"""Small route-wide TPS credit broker with no database or message broker."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
from pathlib import Path


class RouteCreditError(RuntimeError):
    pass


class RouteCreditBroker:
    """Reserve one route credit at a time.

    With ``root`` set, reservations are coordinated by a short-lived atomic
    lock file and a route state file, so independent worker processes share
    one wall-clock schedule.  Without a root the same contract is useful for
    unit tests and in-process workers.
    """

    # File-backed reservations are shared by independent processes.  A small
    # conservative margin keeps the wall-clock schedule below the configured
    # route rate even when lock acquisition and filesystem timestamps bunch
    # several reservations together.
    SCHEDULE_SAFETY_FACTOR = 1.15

    def __init__(self, root: str | os.PathLike[str] | None = None, *, lock_timeout: float = 10.0):
        self.root = Path(root).resolve() if root else None
        if self.root:
            self.root.mkdir(parents=True, exist_ok=True)
        self.lock_timeout = max(1.0, float(lock_timeout))
        self._lock = asyncio.Lock()
        # File-backed reservations are still shared across processes, but
        # serializing this process's own reservations prevents a burst of
        # asyncio.to_thread calls from occupying every executor thread while
        # they wait on the same short-lived lock file.
        self._file_lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self._next_at: dict[str, float] = {}
        self._rates: dict[str, float] = {}
        self._reservations = 0

    def _paths(self, route_id: str) -> tuple[Path, Path]:
        assert self.root is not None
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(route_id))
        return self.root / f"route-{safe}.json", self.root / f"route-{safe}.lock"

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

    def _reserve_file(self, route_id: str, rate: float) -> float:
        state_path, lock_path = self._paths(route_id)
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
                    raise RouteCreditError(f"timed out acquiring route credit lock: {route_id}")
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
            interval = self.SCHEDULE_SAFETY_FACTOR / max(0.0001, float(rate))
            self._atomic_write(state_path, {
                "route_id": str(route_id),
                "rate_tps": float(rate),
                "next_at": next_at + interval,
                "updated_at": now,
            })
            return max(0.0, next_at - now)
        finally:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    async def acquire(self, route_id: str | int, rate_tps: float = 0.0) -> float:
        route = str(route_id)
        rate = float(rate_tps or 0.0)
        if rate <= 0:
            with self._thread_lock:
                self._reservations += 1
            return 0.0
        if self.root:
            async with self._file_lock:
                delay = await asyncio.to_thread(self._reserve_file, route, rate)
        else:
            async with self._lock:
                now = time.monotonic()
                next_at = max(now, self._next_at.get(route, 0.0))
                self._next_at[route] = next_at + 1.0 / rate
                self._rates[route] = rate
                self._reservations += 1
                delay = max(0.0, next_at - now)
        if delay:
            await asyncio.sleep(delay)
        return delay

    def snapshot(self) -> dict:
        with self._thread_lock:
            reservations = self._reservations
        return {
            "root": str(self.root) if self.root else None,
            "reservations": reservations,
            "rates": dict(self._rates),
            "next_at": dict(self._next_at),
        }
