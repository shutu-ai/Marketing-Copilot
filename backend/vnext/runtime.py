"""VNext runtime composition: ingress, supervisor, workers and writer."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from pathlib import Path

from .ingress import IngressRouter
from .diagnostics import enable_native_fault_capture, read_process_metadata, write_process_metadata
from .projection_writer import ProjectionWriterProcess, ProgressJournal
from .route_credits import RouteCreditBroker
from .spool import TaskSpool
from .supervisor import WorkerSupervisor
from .worker import ShardWorker


def _runtime_worker_entry(worker_id: int, partitions: tuple[int, ...], control, context: dict) -> None:
    diagnostic_root = str(context.get("diagnostics_root") or "")
    run_id = str(context.get("run_id") or "")
    start_method = str(context.get("start_method") or "spawn")
    native_fault_path = enable_native_fault_capture(
        diagnostic_root, f"worker-{int(worker_id)}"
    )

    def diagnostic(**kwargs) -> None:
        try:
            write_process_metadata(
                diagnostic_root,
                f"worker-{int(worker_id)}",
                run_id=run_id,
                start_method=start_method,
                **kwargs,
            )
        except Exception:
            pass

    diagnostic(
        phase="starting",
        status="starting",
        progress={"worker_id": int(worker_id)},
        extra={"native_fault_log": str(native_fault_path) if native_fault_path else ""},
    )
    spool = TaskSpool(
        context["spool_root"], virtual_partition_count=int(context["virtual_partition_count"]),
        max_bytes=int(context["max_spool_bytes"]),
    )
    credits = RouteCreditBroker(context["credits_root"])
    journal = ProgressJournal(Path(context["progress_root"]) / f"worker-{worker_id}.jsonl")
    projection_queue = context.get("projection_queue")
    worker_status_path = Path(context["progress_root"]) / f"worker-{worker_id}.status.json"
    worker_ref = {"value": None}

    def publish_status(active: bool) -> None:
        current = worker_ref["value"]
        payload = {
            "worker_id": int(worker_id),
            "active": bool(active),
            "updated_at": time.time(),
            "snapshot": current.snapshot() if current is not None else {},
        }
        temporary = worker_status_path.with_name(f".{worker_status_path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            os.replace(temporary, worker_status_path)
        except OSError:
            pass
        snapshot = current.snapshot() if current is not None else {}
        diagnostic(
            phase="active" if active else "idle",
            status="running",
            progress={
                "active": bool(active),
                "taken": int((snapshot.get("counters") or {}).get("taken", 0)),
                "socket_written": int((snapshot.get("counters") or {}).get("socket_written", 0)),
                "inflight": int(snapshot.get("inflight", 0)),
            },
        )

    def emit(event: dict) -> None:
        journal.append(event)
        if projection_queue is not None:
            try:
                projection_queue.put_nowait(event)
            except queue.Full:
                # The journal is the durable source of truth.  IPC is only a
                # low-latency wake-up hint and must never kill a worker when
                # the single DB owner is temporarily behind.
                pass

    control_signature = None

    def load_controls() -> list[dict] | None:
        nonlocal control_signature
        state_path = Path(context["control_state_path"])
        try:
            stat = state_path.stat()
            signature = (int(stat.st_mtime_ns), int(stat.st_size))
            if signature == control_signature:
                return None
            state = json.loads(state_path.read_text(encoding="utf-8"))
            control_signature = signature
            return list(state.get("campaigns", []))
        except (OSError, ValueError, TypeError):
            return None

    worker = ShardWorker(
        worker_id, tuple(partitions), spool, context["route_configs"], credits=credits,
        progress_sink=emit, max_inflight=int(context["max_inflight"]),
        prefetch_per_partition=int(context["prefetch_per_partition"]),
        control_loader=load_controls,
    )
    worker_ref["value"] = worker

    async def run() -> None:
        try:
            while True:
                if control.poll():
                    message = control.recv()
                    if message == "stop":
                        return
                worker.refresh_campaign_states(force=True)
                before = worker.snapshot()["counters"].get("taken", 0)
                publish_status(True)
                await worker.run_until_idle(close_transports=False)
                publish_status(False)
                after = worker.snapshot()["counters"].get("taken", 0)
                if after == before:
                    await asyncio.sleep(0.1)
        finally:
            await worker.close_transports()

    try:
        diagnostic(phase="running", status="running")
        asyncio.run(run())
    except (EOFError, OSError):
        diagnostic(phase="control_closed", status="stopped")
        return
    except BaseException as exc:
        diagnostic(
            phase="exception", status="error",
            extra={"last_error": f"{type(exc).__name__}: {exc}"},
        )
        raise
    finally:
        diagnostic(phase="stopped", status="stopped")


class VNextRuntime:
    """Own the one VNext lifecycle without starting legacy send services."""

    def __init__(self, root: str | os.PathLike[str], route_configs: dict[int | str, dict], *,
                 worker_count: int = 4, virtual_partition_count: int = 256,
                 max_spool_bytes: int = 512 * 1024 * 1024, projection_queue_size: int = 8192,
                 max_inflight: int = 128, prefetch_per_partition: int = 64,
                 start_method: str = "spawn", projection_flush_interval_seconds: float | None = None,
                 run_id: str = ""):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.spool_root = self.root / "spool"
        self.credits_root = self.root / "route-credits"
        self.progress_root = self.root / "progress"
        self.control_state_path = self.root / "control-state.json"
        self.virtual_partition_count = int(virtual_partition_count)
        self.max_spool_bytes = int(max_spool_bytes)
        self.spool = TaskSpool(
            self.spool_root, virtual_partition_count=self.virtual_partition_count,
            max_bytes=self.max_spool_bytes,
        )
        self.ingress = IngressRouter(self.spool)
        self.route_configs = {str(key): dict(value) for key, value in route_configs.items()}
        self.worker_count = max(1, min(32, int(worker_count)))
        self.max_inflight = max(1, int(max_inflight))
        self.prefetch_per_partition = max(1, int(prefetch_per_partition))
        self._mp_context = multiprocessing.get_context(start_method or "spawn")
        self.start_method = self._mp_context.get_start_method()
        if projection_flush_interval_seconds is None:
            try:
                projection_flush_interval_seconds = float(
                    os.environ.get("MC_VNEXT_PROJECTION_FLUSH_MS", "100")
                ) / 1000.0
            except (TypeError, ValueError):
                projection_flush_interval_seconds = 0.1
        self.projection_flush_interval_seconds = min(
            5.0, max(0.01, float(projection_flush_interval_seconds))
        )
        self.run_id = str(run_id or os.environ.get("MC_RUN_ID") or self.root.name)
        self.diagnostics_root = self.root / "diagnostics"
        self.projection = ProjectionWriterProcess(
            self.root / "projection", queue_size=projection_queue_size,
            journal_paths=[self.progress_root / f"worker-{worker_id}.jsonl" for worker_id in range(max(1, int(worker_count)))],
            flush_interval_seconds=self.projection_flush_interval_seconds,
            start_method=self.start_method,
            mp_context=self._mp_context,
            diagnostics_root=self.diagnostics_root,
            run_id=self.run_id,
        )
        self.supervisor: WorkerSupervisor | None = None
        self._started = False
        self._control_state: dict[tuple[str, int], str] = {}
        self._control_version = 0
        self._control_lock = threading.RLock()
        self._projection_watchdog_stop = threading.Event()
        self._projection_watchdog_thread: threading.Thread | None = None
        self._projection_restart_failures = 0
        self._projection_restart_not_before = 0.0
        write_process_metadata(
            self.diagnostics_root,
            "runtime-parent",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="initialized",
            status="initialized",
        )

    def _load_control_state(self) -> dict:
        if not self.control_state_path.exists():
            return {"schema_version": 1, "version": 0, "campaigns": []}
        try:
            value = json.loads(self.control_state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {"schema_version": 1, "version": 0, "campaigns": []}
        controls = {}
        for item in value.get("campaigns", []):
            try:
                controls[(str(item["campaign_type"]), int(item["campaign_id"]))] = str(item["state"])
            except (KeyError, TypeError, ValueError):
                continue
        self._control_state = controls
        self._control_version = int(value.get("version", 0) or 0)
        return value

    def _persist_control_state(self) -> None:
        payload = {
            "schema_version": 1,
            "version": int(self._control_version),
            "updated_at": time.time(),
            "campaigns": [
                {"campaign_type": key[0], "campaign_id": key[1], "state": value}
                for key, value in sorted(self._control_state.items())
            ],
        }
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.control_state_path.name}.{os.getpid()}.",
            dir=str(self.control_state_path.parent),
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.control_state_path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _projection_watchdog(self) -> None:
        while not self._projection_watchdog_stop.wait(0.25):
            if not self._started:
                continue
            if time.monotonic() < self._projection_restart_not_before:
                continue
            try:
                if not self.projection.snapshot().get("alive", False):
                    restarted = self.projection.restart()
                    if restarted:
                        self._projection_restart_failures = 0
                        self._projection_restart_not_before = 0.0
            except (OSError, RuntimeError):
                self._projection_restart_failures += 1
                self._projection_restart_not_before = time.monotonic() + min(
                    5.0, 0.25 * (2 ** min(self._projection_restart_failures - 1, 5))
                )
                continue

    def start(self) -> None:
        if self._started:
            return
        self.progress_root.mkdir(parents=True, exist_ok=True)
        with self._control_lock:
            self._load_control_state()
            if not self.control_state_path.exists():
                self._persist_control_state()
        self.projection.start()
        context = {
            "spool_root": str(self.spool_root),
            "credits_root": str(self.credits_root),
            "progress_root": str(self.progress_root),
            "control_state_path": str(self.control_state_path),
            "virtual_partition_count": self.virtual_partition_count,
            "max_spool_bytes": self.max_spool_bytes,
            "route_configs": self.route_configs,
            "max_inflight": self.max_inflight,
            "prefetch_per_partition": self.prefetch_per_partition,
            "projection_queue": self.projection.queue,
            "diagnostics_root": str(self.diagnostics_root),
            "run_id": self.run_id,
            "start_method": self.start_method,
        }
        self.supervisor = WorkerSupervisor(
            self.root, worker_count=self.worker_count,
            virtual_partition_count=self.virtual_partition_count,
            worker_target=_runtime_worker_entry, worker_args=(context,),
            start_method=self.start_method, mp_context=self._mp_context,
            diagnostics_root=self.diagnostics_root, run_id=self.run_id,
        )
        self.supervisor.start()
        self._started = True
        write_process_metadata(
            self.diagnostics_root,
            "runtime-parent",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="started",
            status="running",
            progress={"worker_count": self.worker_count, "projection_pid": self.projection.process.pid},
        )
        self._projection_watchdog_stop.clear()
        self._projection_watchdog_thread = threading.Thread(
            target=self._projection_watchdog, name="mc-vnext-projection-watchdog", daemon=True,
        )
        self._projection_watchdog_thread.start()

    def _worker_progress(self) -> list[dict]:
        values = []
        for path in sorted(self.progress_root.glob("worker-*.status.json")):
            try:
                values.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError, TypeError):
                continue
        return values

    def stop(self) -> None:
        if self.supervisor:
            self.supervisor.stop()
        if self._started:
            self._projection_watchdog_stop.set()
            watchdog = self._projection_watchdog_thread
            self._projection_watchdog_thread = None
            if watchdog and watchdog.is_alive() and watchdog is not threading.current_thread():
                watchdog.join(timeout=1.0)
            # Let the dedicated DB owner drain its durable progress queue.  A
            # worker journal remains the recovery source if the bounded
            # shutdown deadline is ever exceeded.
            self.projection.stop(timeout=30.0)
        self.supervisor = None
        self._started = False
        write_process_metadata(
            self.diagnostics_root,
            "runtime-parent",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="stopped",
            status="stopped",
        )

    def enqueue_chunk(self, *args, **kwargs):
        if not self._started:
            raise RuntimeError("VNext runtime is not started")
        return self.ingress.enqueue_chunk(*args, **kwargs)

    def set_campaign_state(self, campaign_type: str, campaign_id: int, state: str) -> None:
        with self._control_lock:
            self._load_control_state()
            self._control_state[(str(campaign_type), int(campaign_id))] = str(state)
            self._control_version += 1
            self._persist_control_state()

    def status(self) -> dict:
        with self._control_lock:
            self._load_control_state()
        # The detailed status API remains a full reconciliation boundary, but
        # it reuses the long-lived ingress owner instead of reconstructing a
        # complete TaskSpool index on every request.
        spool = self.spool.status()
        journal_bytes = 0
        journal_paths = []
        for path in sorted(self.progress_root.glob("worker-*.jsonl")):
            try:
                journal_bytes += int(path.stat().st_size)
                journal_paths.append({"path": str(path), "bytes": int(path.stat().st_size)})
            except OSError:
                continue
        route_credits = {}
        for path in sorted(self.credits_root.glob("route-*.json")):
            try:
                route_credits[path.name] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                continue
        return {
            "started": self._started,
            "run_id": self.run_id,
            "start_method": self.start_method,
            "spool": spool,
            "supervisor": self.supervisor.status() if self.supervisor else None,
            "projection": self.projection.snapshot(),
            "worker_progress": self._worker_progress(),
            "journal": {"retained_bytes": journal_bytes, "paths": journal_paths},
            "route_credits": route_credits,
            "control_version": int(self._control_version),
            "controls": [
                {"campaign_type": key[0], "campaign_id": key[1], "state": value}
                for key, value in self._control_state.items()
            ],
            "processes": read_process_metadata(self.diagnostics_root),
            "projection_restart_failures": int(self._projection_restart_failures),
            "projection_restart_backoff_seconds": round(
                max(0.0, self._projection_restart_not_before - time.monotonic()), 3
            ),
        }

    def _idle_snapshot(self) -> dict:
        """Return only bounded cross-process signals for the hot idle loop."""

        return {
            "spool": self.spool.lightweight_snapshot(),
            "supervisor": self.supervisor.status() if self.supervisor else None,
            "projection": self.projection.snapshot(),
            "worker_progress": self._worker_progress(),
        }

    async def wait_for_idle(self, timeout: float = 30.0) -> dict:
        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            status = self._idle_snapshot()
            workers = status.get("worker_progress") or []
            workers_idle = bool(workers) and all(
                not bool(item.get("active"))
                and int((item.get("snapshot") or {}).get("inflight", 0)) == 0
                and all(
                    int(transport.get("window_inflight", 0)) == 0
                    for transport in (item.get("snapshot") or {}).get("transports", [])
                )
                for item in workers
            )
            projection = status.get("projection") or {}
            journals_caught_up = True
            for path, offset in (projection.get("journal_offsets") or {}).items():
                try:
                    journals_caught_up = journals_caught_up and int(offset) >= Path(path).stat().st_size
                except OSError:
                    journals_caught_up = False
            if status["spool"]["pending_records"] == 0 and workers_idle and journals_caught_up:
                return self.status()
            await asyncio.sleep(0.05)
        return self.status()
