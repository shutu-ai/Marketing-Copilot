"""Small route-wide TPS credit broker with no database or message broker."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


class RouteCreditError(RuntimeError):
    pass


@dataclass
class RouteBindLease:
    """One physical SMPP bind slot held for a transport lifetime."""

    broker: "RouteCreditBroker"
    route_id: str
    slot: int
    token: str
    path: Path | None = None
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.broker.release_bind(self)


class RouteCreditBroker:
    """Reserve route TPS credits with a bounded cross-process lease.

    With ``root`` set, each process reserves a small, durable time lease for
    a route.  Only the lease refill takes a file lock, JSON replacement and
    fsync; individual PDUs consume the already-owned in-process schedule.
    This keeps the route-wide limit correct across workers without putting
    filesystem I/O on the PDU hot path.  The same object also owns the
    connection-lifetime bind-slot leases.
    """

    # A crashed process can lose an already-reserved lease, which only leaves
    # capacity unused.  The durable reservation advances the schedule for the
    # whole batch before any PDU uses it, so the batch cannot exceed the
    # configured wall-clock rate.
    # Keep the durable grant small enough that a short burst does not reserve
    # a large tail beyond the configured route rate when several workers each
    # own a local lease.  This still removes one lock/JSON/fsync cycle from the
    # PDU hot path while bounding the unused reservation after a crash.
    CREDIT_LEASE_SIZE = 8

    def __init__(self, root: str | os.PathLike[str] | None = None, *, lock_timeout: float = 10.0):
        self.root = Path(root).resolve() if root else None
        if self.root:
            self.root.mkdir(parents=True, exist_ok=True)
        self.lock_timeout = max(0.1, float(lock_timeout))
        self._lock = asyncio.Lock()
        # Refill operations are infrequent (one per CREDIT_LEASE_SIZE PDUs).
        # Serializing only refills avoids a burst of executor threads waiting
        # on the same short-lived lock file.
        self._file_lock = asyncio.Lock()
        self._bind_file_lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self._next_at: dict[str, float] = {}
        self._rates: dict[str, float] = {}
        self._leases: dict[str, dict[str, float | int]] = {}
        self._local_bind_slots: dict[str, set[int]] = {}
        self._reservations = 0
        self._credit_refills = 0
        self._file_credit_refills = 0
        self._credit_wait_seconds = 0.0
        self._credit_wait_count = 0
        self._credit_granted_units = 0
        self._credit_consumed_units = 0
        self._credit_wasted_units = 0
        self._credit_refill_cost_seconds = 0.0
        self._bind_acquires = 0

    @staticmethod
    def _safe(value: str | int) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value))

    def _paths(self, credit_key: str) -> tuple[Path, Path]:
        assert self.root is not None
        safe = self._safe(credit_key)
        return self.root / f"route-{safe}.json", self.root / f"route-{safe}.lock"

    def _bind_path(self, route_id: str, slot: int) -> Path:
        assert self.root is not None
        return self.root / f"route-{self._safe(route_id)}-bind-{int(slot)}.lease"

    @staticmethod
    def _atomic_write(path: Path, value: dict) -> None:
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(value, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def _reserve_file(self, credit_key: str, rate: float, grant_size: int) -> tuple[float, float]:
        state_path, lock_path = self._paths(credit_key)
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                break
            except (FileExistsError, PermissionError):
                try:
                    if time.time() - lock_path.stat().st_mtime > self.lock_timeout:
                        lock_path.unlink()
                        continue
                except (FileNotFoundError, PermissionError):
                    continue
                if time.monotonic() >= deadline:
                    raise RouteCreditError(f"timed out acquiring route credit lock: {credit_key}")
                time.sleep(0.001)
        try:
            now = time.time()
            current = {}
            if state_path.exists():
                try:
                    current = json.loads(state_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    current = {}
            next_at = max(now, float(current.get("next_at", 0.0)))
            interval = 1.0 / max(0.0001, float(rate))
            self._atomic_write(state_path, {
                "route_id": str(credit_key),
                "rate_tps": float(rate),
                "grant_size": int(grant_size),
                "next_at": next_at + interval * int(grant_size),
                "updated_at": now,
            })
            return next_at, interval
        finally:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _credit_key(route_id: str | int, *, tps_scope: str = "route_shared",
                    bind_slot: int | None = None) -> str:
        scope = str(tps_scope or "route_shared").strip().lower()
        if scope == "per_bind":
            return f"{route_id}:bind:{int(bind_slot or 0)}"
        return str(route_id)

    async def acquire(self, route_id: str | int, rate_tps: float = 0.0, *,
                      tps_scope: str = "route_shared", bind_slot: int | None = None) -> float:
        route = str(route_id)
        rate = float(rate_tps or 0.0)
        key = self._credit_key(route, tps_scope=tps_scope, bind_slot=bind_slot)
        if rate <= 0:
            with self._thread_lock:
                self._reservations += 1
            return 0.0
        async with self._lock:
            lease = self._leases.get(key)
            if not lease or float(lease["rate"]) != rate or int(lease["remaining"]) <= 0:
                if lease:
                    self._credit_wasted_units += max(0, int(lease.get("remaining", 0) or 0))
                refill_started = time.perf_counter()
                if self.root:
                    async with self._file_lock:
                        start_at, interval = await asyncio.to_thread(
                            self._reserve_file, key, rate, self.CREDIT_LEASE_SIZE,
                        )
                    self._file_credit_refills += 1
                else:
                    now = time.time()
                    previous = self._next_at.get(key, now)
                    start_at = max(now, previous)
                    interval = 1.0 / max(0.0001, rate)
                    self._next_at[key] = start_at + interval * self.CREDIT_LEASE_SIZE
                self._credit_refill_cost_seconds += max(0.0, time.perf_counter() - refill_started)
                self._credit_refills += 1
                self._credit_granted_units += self.CREDIT_LEASE_SIZE
                lease = {
                    "rate": rate,
                    "interval": interval,
                    "next_at": start_at + interval,
                    "remaining": self.CREDIT_LEASE_SIZE - 1,
                }
                self._leases[key] = lease
            else:
                start_at = float(lease["next_at"])
                lease["next_at"] = start_at + float(lease["interval"])
                lease["remaining"] = int(lease["remaining"]) - 1
            now = time.time()
            delay = max(0.0, start_at - now)
            self._next_at[key] = float(lease["next_at"])
            self._rates[key] = rate
            self._reservations += 1
            self._credit_consumed_units += 1
        if delay:
            wait_started = time.monotonic()
            await asyncio.sleep(delay)
            self._credit_wait_seconds += max(0.0, time.monotonic() - wait_started)
            self._credit_wait_count += 1
        return delay

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def _acquire_bind_file(self, route_id: str, pool_size: int, timeout: float) -> RouteBindLease:
        deadline = time.monotonic() + max(0.1, float(timeout))
        while True:
            for slot in range(1, max(1, int(pool_size)) + 1):
                path = self._bind_path(route_id, slot)
                token = uuid.uuid4().hex
                try:
                    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                except FileExistsError:
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                        pid = int(payload.get("pid", 0))
                        if not self._pid_alive(pid):
                            path.unlink()
                    except (OSError, ValueError, TypeError):
                        try:
                            if time.time() - path.stat().st_mtime > self.lock_timeout:
                                path.unlink()
                        except (FileNotFoundError, OSError):
                            pass
                    continue
                except OSError:
                    continue
                try:
                    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                        json.dump({
                            "route_id": str(route_id), "slot": int(slot),
                            "pid": os.getpid(), "token": token, "created_at": time.time(),
                        }, handle, sort_keys=True, separators=(",", ":"))
                        handle.write("\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                    self._bind_acquires += 1
                    return RouteBindLease(self, str(route_id), slot, token, path)
                except Exception:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                    raise
            if time.monotonic() >= deadline:
                raise RouteCreditError(
                    f"timed out acquiring SMPP bind slot route={route_id} pool_size={pool_size}"
                )
            time.sleep(0.02)

    def _acquire_bind_local(self, route_id: str, pool_size: int, timeout: float) -> RouteBindLease:
        deadline = time.monotonic() + max(0.1, float(timeout))
        while True:
            with self._thread_lock:
                slots = self._local_bind_slots.setdefault(str(route_id), set())
                for slot in range(1, max(1, int(pool_size)) + 1):
                    if slot not in slots:
                        slots.add(slot)
                        self._bind_acquires += 1
                        return RouteBindLease(self, str(route_id), slot, uuid.uuid4().hex)
            if time.monotonic() >= deadline:
                raise RouteCreditError(
                    f"timed out acquiring SMPP bind slot route={route_id} pool_size={pool_size}"
                )
            time.sleep(0.02)

    async def acquire_bind(self, route_id: str | int, pool_size: int = 1, *,
                           timeout: float | None = None) -> RouteBindLease:
        route = str(route_id)
        wait_timeout = self.lock_timeout if timeout is None else max(0.1, float(timeout))
        async with self._bind_file_lock:
            if self.root:
                return await asyncio.to_thread(
                    self._acquire_bind_file, route, max(1, int(pool_size)), wait_timeout,
                )
            return await asyncio.to_thread(
                self._acquire_bind_local, route, max(1, int(pool_size)), wait_timeout,
            )

    def release_bind(self, lease: RouteBindLease) -> None:
        if lease.path is None:
            with self._thread_lock:
                self._local_bind_slots.get(str(lease.route_id), set()).discard(int(lease.slot))
            return
        try:
            payload = json.loads(lease.path.read_text(encoding="utf-8"))
            if str(payload.get("token")) == str(lease.token):
                lease.path.unlink()
        except FileNotFoundError:
            pass
        except (OSError, ValueError, TypeError):
            # A malformed/stale lease is reclaimed by the next acquirer.
            pass

    def snapshot(self) -> dict:
        with self._thread_lock:
            reservations = self._reservations
        return {
            "root": str(self.root) if self.root else None,
            "reservations": reservations,
            "rates": dict(self._rates),
            "next_at": dict(self._next_at),
            "credit_lease_size": self.CREDIT_LEASE_SIZE,
            "credit_refills": int(self._credit_refills),
            "file_credit_refills": int(self._file_credit_refills),
            "credit_wait_seconds": round(float(self._credit_wait_seconds), 6),
            "credit_wait_count": int(self._credit_wait_count),
            "credit_granted_units": int(self._credit_granted_units),
            "credit_consumed_units": int(self._credit_consumed_units),
            "credit_remaining_units": int(sum(
                max(0, int(item.get("remaining", 0) or 0))
                for item in self._leases.values()
            )),
            "credit_wasted_units": int(
                self._credit_wasted_units
                + sum(max(0, int(item.get("remaining", 0) or 0)) for item in self._leases.values())
            ),
            "credit_refill_cost_seconds": round(float(self._credit_refill_cost_seconds), 6),
            "bind_acquires": int(self._bind_acquires),
        }
