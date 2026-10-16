"""Worker process ownership, restart and lease fencing for logical shards."""

from __future__ import annotations

import json
import multiprocessing
import os
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Callable

from .diagnostics import write_process_metadata
from .partition import PartitionMap


class OwnerConflict(RuntimeError):
    """A live owner already holds a partition lease."""


class OwnerLeaseStore:
    """Small atomic lease files; the supervisor is the only normal writer."""

    def __init__(self, root: str | os.PathLike[str], *, lease_seconds: float = 30.0):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lease_seconds = max(1.0, float(lease_seconds))

    def _path(self, partition_id: int) -> Path:
        return self.root / f"partition-{int(partition_id):04d}.json"

    def _read(self, partition_id: int) -> dict | None:
        path = self._path(partition_id)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, ValueError):
            return None

    def _write(self, partition_id: int, value: dict) -> None:
        path = self._path(partition_id)
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

    def acquire(self, partition_id: int, owner_id: str, *, map_revision: int,
                requested_epoch: int = 0) -> dict:
        partition = int(partition_id)
        owner = str(owner_id)
        now = time.time()
        current = self._read(partition)
        if current:
            active = float(current.get("expires_at", 0.0)) > now
            same = current.get("owner_id") == owner
            if active and not same:
                raise OwnerConflict(f"partition {partition} owned by {current.get('owner_id')}")
            current_epoch = int(current.get("owner_epoch", 0))
        else:
            current_epoch = 0
        epoch = max(int(requested_epoch), current_epoch + (0 if current and current.get("owner_id") == owner else 1), 1)
        lease = {
            "partition_id": partition,
            "owner_id": owner,
            "owner_epoch": epoch,
            "map_revision": int(map_revision),
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": now + self.lease_seconds,
        }
        self._write(partition, lease)
        return lease

    def heartbeat(self, lease: dict) -> dict:
        current = self._read(int(lease["partition_id"]))
        if not current or current.get("owner_id") != lease.get("owner_id") or int(current.get("owner_epoch", 0)) != int(lease.get("owner_epoch", 0)):
            raise OwnerConflict("owner lease was fenced")
        now = time.time()
        renewed = dict(current, heartbeat_at=now, expires_at=now + self.lease_seconds)
        self._write(int(lease["partition_id"]), renewed)
        return renewed

    def release(self, lease: dict) -> bool:
        partition = int(lease["partition_id"])
        current = self._read(partition)
        if not current:
            return False
        if current.get("owner_id") != lease.get("owner_id") or int(current.get("owner_epoch", 0)) != int(lease.get("owner_epoch", 0)):
            return False
        released = dict(current, expires_at=0.0, released_at=time.time())
        self._write(partition, released)
        return True

    def snapshot(self, partition_id: int) -> dict | None:
        return self._read(int(partition_id))


def _idle_worker(worker_id: int, partitions: tuple[int, ...], control: Connection) -> None:
    """Default worker used by ownership tests until the V2 worker is attached."""

    del worker_id, partitions
    try:
        while True:
            if control.poll(0.25):
                message = control.recv()
                if message == "stop":
                    return
    except (EOFError, OSError):
        return


@dataclass
class WorkerHandle:
    worker_id: int
    partitions: tuple[int, ...]
    process: object
    control: Connection
    leases: tuple[dict, ...]


class WorkerSupervisor:
    """Own all worker processes and never starts a second owner for a shard."""

    def __init__(self, root: str | os.PathLike[str], *, worker_count: int = 4,
                 virtual_partition_count: int = 256, worker_target: Callable | None = None,
                 worker_args: tuple = (), lease_seconds: float = 30.0,
                 start_method: str | None = "spawn", mp_context=None,
                 diagnostics_root: str | os.PathLike[str] | None = None,
                 run_id: str = ""):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.map = PartitionMap.load_or_create(
            self.root / "partition-map.json",
            virtual_partition_count=virtual_partition_count,
            worker_count=worker_count,
        )
        self.leases = OwnerLeaseStore(self.root / "leases", lease_seconds=lease_seconds)
        self.worker_target = worker_target or _idle_worker
        self.worker_args = tuple(worker_args)
        self._mp_context = mp_context or multiprocessing.get_context(start_method or "spawn")
        self._start_method = self._mp_context.get_start_method()
        self._diagnostics_root = str(Path(diagnostics_root).resolve()) if diagnostics_root else ""
        self._run_id = str(run_id or self.root.name)
        self._handles: dict[int, WorkerHandle] = {}
        self._handles_lock = threading.RLock()
        self._owner_prefix = f"supervisor-{os.getpid()}-{uuid.uuid4().hex[:10]}"
        self._monitor_stop = threading.Event()
        self._monitor_thread: threading.Thread | None = None
        self._restart_count = 0
        self._restart_failures: dict[int, int] = {}
        self._restart_not_before: dict[int, float] = {}
        self._last_exitcodes: dict[int, int | None] = {}

    def _context(self):
        return self._mp_context

    def _launch(self, worker_id: int) -> WorkerHandle:
        partitions = self.map.partitions_for_worker(worker_id)
        # A restart is a new owner even when it is launched by the same
        # supervisor process.  The fresh id forces the lease epoch forward.
        owner_id = f"{self._owner_prefix}-{uuid.uuid4().hex[:10]}-w{worker_id}"
        acquired: list[dict] = []
        try:
            for partition in partitions:
                assignment = self.map.assignment(partition)
                acquired.append(self.leases.acquire(
                    partition, owner_id, map_revision=self.map.map_revision,
                    requested_epoch=assignment.owner_epoch,
                ))
        except Exception:
            for lease in acquired:
                self.leases.release(lease)
            raise
        parent, child = self._context().Pipe()
        process = self._context().Process(
            target=self.worker_target,
            args=(worker_id, partitions, child, *self.worker_args),
            name=f"mc-vnext-worker-{worker_id}",
            daemon=True,
        )
        process.start()
        child.close()
        write_process_metadata(
            self._diagnostics_root,
            f"worker-supervisor-{worker_id}",
            run_id=self._run_id,
            start_method=self._start_method,
            phase="child_started",
            status="running",
            progress={"worker_id": int(worker_id), "child_pid": getattr(process, "pid", None)},
        )
        return WorkerHandle(worker_id, partitions, process, parent, tuple(acquired))

    def start(self) -> None:
        with self._handles_lock:
            if self._handles:
                return
            self._monitor_stop.clear()
        try:
            for worker_id in range(self.map.worker_count):
                with self._handles_lock:
                    self._handles[worker_id] = self._launch(worker_id)
        except Exception:
            self.stop()
            raise
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, name="mc-vnext-worker-supervisor", daemon=True,
        )
        self._monitor_thread.start()

    def _monitor_loop(self) -> None:
        while not self._monitor_stop.wait(0.25):
            with self._handles_lock:
                if self._monitor_stop.is_set():
                    return
                dead = [
                    worker_id for worker_id in range(self.map.worker_count)
                    if worker_id not in self._handles
                    or not getattr(self._handles[worker_id].process, "is_alive", lambda: False)()
                ]
            for worker_id in dead:
                if self._monitor_stop.is_set():
                    return
                if time.monotonic() < self._restart_not_before.get(worker_id, 0.0):
                    continue
                try:
                    self.restart_worker(worker_id)
                    self._restart_count += 1
                    self._restart_failures[worker_id] = 0
                    self._restart_not_before.pop(worker_id, None)
                except Exception:
                    # A lease or process failure is retried with a bounded
                    # backoff; no second owner is launched on failure.
                    failures = self._restart_failures.get(worker_id, 0) + 1
                    self._restart_failures[worker_id] = failures
                    delay = min(5.0, 0.25 * (2 ** min(failures - 1, 5)))
                    self._restart_not_before[worker_id] = time.monotonic() + delay
                    continue

    def stop(self, timeout: float = 5.0) -> None:
        self._monitor_stop.set()
        monitor = self._monitor_thread
        self._monitor_thread = None
        if monitor and monitor.is_alive() and monitor is not threading.current_thread():
            monitor.join(timeout=1.0)
        with self._handles_lock:
            handles = list(self._handles.values())
            self._handles.clear()
        for handle in handles:
            try:
                handle.control.send("stop")
            except (EOFError, OSError):
                pass
        for handle in handles:
            try:
                handle.process.join(timeout=max(0.0, float(timeout)))
            except Exception:
                pass
            if getattr(handle.process, "is_alive", lambda: False)():
                try:
                    handle.process.terminate()
                    handle.process.join(timeout=2.0)
                except Exception:
                    pass
            self._last_exitcodes[handle.worker_id] = getattr(handle.process, "exitcode", None)
            write_process_metadata(
                self._diagnostics_root,
                f"worker-{handle.worker_id}",
                run_id=self._run_id,
                start_method=self._start_method,
                phase="parent_observed_stop",
                status="stopped",
                exitcode=getattr(handle.process, "exitcode", None),
                process_pid=getattr(handle.process, "pid", None),
            )
            for lease in handle.leases:
                self.leases.release(lease)
            try:
                handle.control.close()
            except Exception:
                pass

    def restart_worker(self, worker_id: int, timeout: float = 5.0) -> WorkerHandle:
        worker = int(worker_id)
        with self._handles_lock:
            if self._monitor_stop.is_set() and worker not in self._handles:
                raise RuntimeError("worker supervisor is stopped")
            old = self._handles.pop(worker, None)
        if old is not None:
            try:
                old.control.send("stop")
            except (EOFError, OSError):
                pass
            old.process.join(timeout=max(0.0, float(timeout)))
            self._last_exitcodes[worker] = getattr(old.process, "exitcode", None)
            if getattr(old.process, "is_alive", lambda: False)():
                old.process.terminate()
                old.process.join(timeout=2.0)
            self._last_exitcodes[worker] = getattr(old.process, "exitcode", None)
            write_process_metadata(
                self._diagnostics_root,
                f"worker-{worker}",
                run_id=self._run_id,
                start_method=self._start_method,
                phase="parent_observed_restart",
                status="stopped",
                exitcode=getattr(old.process, "exitcode", None),
                process_pid=getattr(old.process, "pid", None),
            )
            for lease in old.leases:
                self.leases.release(lease)
            old.control.close()
        handle = self._launch(worker)
        with self._handles_lock:
            if self._monitor_stop.is_set():
                for lease in handle.leases:
                    self.leases.release(lease)
                try:
                    handle.process.terminate()
                except Exception:
                    pass
                raise RuntimeError("worker supervisor stopped during restart")
            self._handles[worker] = handle
        return handle

    def send(self, worker_id: int, message: object) -> None:
        with self._handles_lock:
            self._handles[int(worker_id)].control.send(message)

    def status(self) -> dict:
        with self._handles_lock:
            handles = list(self._handles.values())
            restart_count = self._restart_count
        return {
            "map_revision": self.map.map_revision,
            "worker_count": self.map.worker_count,
            "start_method": self._start_method,
            "automatic_restarts": restart_count,
            "restart_failures": {str(key): int(value) for key, value in self._restart_failures.items() if value},
            "restart_not_before": {
                str(key): round(max(0.0, value - time.monotonic()), 3)
                for key, value in self._restart_not_before.items()
                if value > time.monotonic()
            },
            "last_exitcodes": {str(key): value for key, value in self._last_exitcodes.items()},
            "workers": [
                {
                    "worker_id": handle.worker_id,
                    "partitions": list(handle.partitions),
                    "pid": getattr(handle.process, "pid", None),
                    "exitcode": getattr(handle.process, "exitcode", None),
                    "alive": bool(getattr(handle.process, "is_alive", lambda: False)()),
                    "owner_epochs": {str(item["partition_id"]): int(item["owner_epoch"]) for item in handle.leases},
                }
                for handle in sorted(handles, key=lambda item: item.worker_id)
            ],
        }

    def assert_no_dual_owners(self) -> None:
        seen: dict[int, str] = {}
        with self._handles_lock:
            handles = list(self._handles.values())
        for handle in handles:
            for lease in handle.leases:
                partition = int(lease["partition_id"])
                owner = str(lease["owner_id"])
                if partition in seen and seen[partition] != owner:
                    raise OwnerConflict(f"dual owner detected for partition {partition}")
                seen[partition] = owner
