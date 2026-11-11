"""VNext runtime composition: ingress, supervisor, workers and writer."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from pathlib import Path

from .business_day import BusinessClock, FileWallClock, ensure_clock
from .critical_path_profiler import CriticalPathProfiler
from .ingress import IngressRouter
from .diagnostics import enable_native_fault_capture, read_process_metadata, write_process_metadata
from .process_lifecycle import set_parent_death_signal
from .projection_writer import ProjectionBackpressure, ProjectionWriterProcess, ProgressJournal
from .progress import ProgressPersistence
from .pressure import StageCredit
from .route_credits import RouteCreditBroker
from .spool import DEFAULT_MAX_SPOOL_BYTES, TaskSpool
from .supervisor import WorkerSupervisor
from .worker import ShardWorker


logger = logging.getLogger(__name__)


def _restartable_progress_failure(snapshot: dict) -> bool:
    """Return whether a blocked worker can be recycled without redispatch."""

    progress = snapshot.get("progress") or {}
    return False


def _plan_partition_rebalance(
    assignments: dict[int, int], pending_by_partition: dict[int | str, int],
    worker_count: int,
) -> dict[int, int]:
    """Return a small, hysteretic owner change for materially skewed load."""

    count = max(1, int(worker_count))
    if count < 2:
        return {}
    owners = {int(partition): int(worker) for partition, worker in assignments.items()}
    pending = {
        int(partition): max(0, int(value or 0))
        for partition, value in pending_by_partition.items()
    }
    loads = [0] * count
    for partition, worker in owners.items():
        if 0 <= worker < count:
            loads[worker] += pending.get(partition, 0)
    total = sum(loads)
    if total < max(256, count * 64):
        return {}
    average = total / count
    if max(loads) <= max(1024, average * 1.5):
        return {}

    # Keep one maintenance pass local to two workers; moving every target at
    # once would restart the whole pool just to correct one hot owner.
    source = max(range(count), key=loads.__getitem__)
    target = min(range(count), key=loads.__getitem__)
    if source == target:
        return {}
    if loads[source] <= max(1024, average * 1.2):
        return {}
    if loads[source] - loads[target] < max(512, average * 0.25):
        return {}
    candidates = [
        partition for partition, worker in owners.items()
        if worker == source and pending.get(partition, 0) > 0
    ]
    # An indivisible single hot shard cannot be balanced safely; leaving it
    # put avoids moving the same shard between workers on every pass.
    if len(candidates) < 2:
        return {}
    desired = max(1.0, min(loads[source] - average, average - loads[target]))
    partition = min(
        candidates,
        key=lambda item: (
            abs(pending.get(item, 0) - desired),
            -pending.get(item, 0),
            item,
        ),
    )
    return {partition: target}


def _runtime_worker_entry(worker_id: int, partitions: tuple[int, ...], control, context: dict) -> None:
    set_parent_death_signal()
    diagnostic_root = str(context.get("diagnostics_root") or "")
    run_id = str(context.get("run_id") or "")
    start_method = str(context.get("start_method") or "spawn")
    profiler = CriticalPathProfiler(
        context.get("profile_root"),
        f"worker-{int(worker_id)}",
        interval_seconds=float(context.get("profile_interval_seconds", 1.0) or 1.0),
    )
    native_fault_path = enable_native_fault_capture(
        diagnostic_root, f"worker-{int(worker_id)}"
    )

    def diagnostic(**kwargs) -> None:
        started_ns = time.perf_counter_ns()
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
        finally:
            profiler.observe_ns("diagnostic_write", time.perf_counter_ns() - started_ns)
            profiler.publish_if_due()

    diagnostic(
        phase="starting",
        status="starting",
        progress={"worker_id": int(worker_id)},
        extra={"native_fault_log": str(native_fault_path) if native_fault_path else ""},
    )
    test_clock_file = str(context.get("test_clock_file") or "").strip()
    worker_clock = (
        BusinessClock(FileWallClock(test_clock_file), context.get("business_timezone"))
        if test_clock_file else None
    )
    spool = TaskSpool(
        context["spool_root"], virtual_partition_count=int(context["virtual_partition_count"]),
        max_bytes=int(context["max_spool_bytes"]), clock=worker_clock,
        business_timezone=context.get("business_timezone") if not worker_clock else None,
    )
    credits = RouteCreditBroker(context["credits_root"])
    journal = ProgressJournal(
        Path(context["progress_root"]) / f"worker-{worker_id}.jsonl",
        max_bytes=int(context.get("max_journal_bytes", 512 * 1024 * 1024)),
        profiler=profiler,
    )
    projection_queue = context.get("projection_queue")
    worker_status_path = Path(context["progress_root"]) / f"worker-{worker_id}.status.json"
    progress_store = ProgressPersistence(
        journal,
        name=f"worker-{int(worker_id)}",
        max_pending_items=int(context.get("progress_queue_items", 131_072)),
        max_pending_bytes=int(
            context.get("progress_queue_bytes", 64 * 1024 * 1024)
        ),
        batch_size=int(context.get("progress_batch_size", 256)),
        flush_interval_seconds=float(
            context.get("progress_flush_interval_seconds", 0.05)
        ),
        aggregate_path=(
            Path(context["progress_root"])
            / f"worker-{int(worker_id)}.progress-aggregate.json"
        ),
        aggregate_interval_seconds=float(
            context.get("progress_aggregate_interval_seconds", 1.0)
        ),
        profiler=profiler,
    )
    worker_ref = {"value": None}
    status_state = {
        "active": False,
        "draining": False,
        "phase": "starting",
        "loop_heartbeat_at": 0.0,
    }
    status_lock = threading.Lock()
    status_stop = threading.Event()

    def publish_status(active: bool) -> None:
        current = worker_ref["value"]
        snapshot = current.snapshot() if current is not None else {}
        progress = snapshot.get("progress") or {}
        busy = bool(int(snapshot.get("inflight", 0) or 0))
        busy = busy or bool(status_state.get("draining"))
        busy = busy or any(
            int(transport.get("window_inflight", 0) or 0)
            or int(transport.get("response_callback_pending", 0) or 0)
            for transport in snapshot.get("transports", [])
        )
        busy = busy or int(progress.get("pending_durability", 0) or 0) > 0
        payload = {
            "worker_id": int(worker_id),
            "process_pid": int(os.getpid()),
            "run_id": run_id,
            # ``active`` describes real protocol/progress work, not the fact
            # that the idle polling loop is currently running one pass.
            "active": busy,
            "updated_at": time.time(),
            "phase": str(status_state.get("phase") or ""),
            "loop_heartbeat_at": float(status_state.get("loop_heartbeat_at") or 0.0),
            "recovery_policy": "checkpoint_before_socket_drop_uncertain",
            "snapshot": snapshot,
        }
        temporary = worker_status_path.with_name(f".{worker_status_path.name}.{os.getpid()}.tmp")
        status_started_ns = time.perf_counter_ns()
        try:
            with status_lock:
                temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
                os.replace(temporary, worker_status_path)
        except OSError:
            pass
        profiler.observe_ns("worker_status_write", time.perf_counter_ns() - status_started_ns)
        diagnostic(
            phase="active" if busy else "idle",
            status="running",
            progress={
                "active": bool(active),
                "taken": int((snapshot.get("counters") or {}).get("taken", 0)),
                "socket_written": int((snapshot.get("counters") or {}).get("socket_written", 0)),
                "inflight": int(snapshot.get("inflight", 0)),
            },
        )

    def projection_hint(events: list[dict]) -> None:
        if projection_queue is not None and events:
            try:
                # One wake-up hint is enough: the projection writer reads the
                # durable journal, not the hint payload.  Sending one hint per
                # batch keeps IPC overhead independent of the batch size.
                projection_queue.put_nowait(events[-1])
                profiler.inc("projection_hint_put")
            except queue.Full:
                # The journal is the durable source of truth.  IPC is only a
                # low-latency wake-up hint and must never kill a worker when
                # the single DB owner is temporarily behind.
                profiler.inc("projection_hint_queue_full")
        profiler.publish_if_due()

    progress_store.add_durability_listener(projection_hint)
    progress_store.start()

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

    worker_count = max(1, int(context.get("worker_count", 1)))
    route_configs = {}
    for key, original in context["route_configs"].items():
        config = dict(original)
        pool_size = max(1, int(config.get("pool_size", 1) or 1))
        # A worker may own more than one physical bind when pool_size is
        # larger than the business-worker count.  The shared bind lease still
        # enforces the route-wide physical cap across all worker processes.
        config["_worker_bind_target"] = max(1, math.ceil(pool_size / worker_count))
        route_configs[str(key)] = config
    worker = ShardWorker(
        worker_id, tuple(partitions), spool, route_configs, credits=credits,
        max_inflight=int(context["max_inflight"]),
        prefetch_per_partition=int(context["prefetch_per_partition"]),
        control_loader=load_controls,
        progress_store=progress_store,
        profiler=profiler,
    )
    worker_ref["value"] = worker

    status_interval = max(
        0.2, float(context.get("status_interval_seconds", 1.0))
    )
    # An idle worker still needs to notice newly appended spool records, but
    # it must not rescan the entire manifest ten times per second.
    idle_poll_interval = max(
        0.1, float(context.get("idle_poll_interval_seconds", 0.5))
    )

    def status_heartbeat() -> None:
        while not status_stop.wait(status_interval):
            publish_status(bool(status_state.get("active")))

    async def run() -> None:
        stop_requested = False

        def handle_control() -> bool:
            nonlocal stop_requested
            # This callback runs on the async dispatch loop, so it proves the
            # loop is advancing without turning the status thread into a fake
            # liveness signal.
            status_state["loop_heartbeat_at"] = time.time()
            while control.poll():
                message = control.recv()
                if message == "stop":
                    stop_requested = True
                    worker.stop()
                    return True
                if (
                    isinstance(message, dict)
                    and message.get("type") == "reload_route_rates"
                ):
                    result = worker.apply_route_rates(message.get("routes") or {})
                    diagnostic(
                        phase="smpp_rate_reload",
                        status="running",
                        progress={
                            "updated_routes": len(result.get("updated_routes") or []),
                            "updated_transports": int(
                                result.get("updated_transports", 0) or 0
                            ),
                        },
                    )
            return stop_requested

        async def drain_before_exit() -> None:
            status_state["active"] = False
            status_state["phase"] = "transport_drain"
            drain = await worker.drain_transports()
            if not drain.get("drained"):
                diagnostic(
                    phase="response_drain_timeout",
                    status="degraded",
                    extra={
                        "pending": int(drain.get("pending", 0) or 0),
                        "callback_pending": int(
                            drain.get("callback_pending", 0) or 0
                        ),
                    },
                )
            # Closing resolves outstanding responses before the final journal drain.
            await worker.close_transports()

        heartbeat = threading.Thread(
            target=status_heartbeat,
            name=f"mc-vnext-worker-status-{int(worker_id)}",
            daemon=True,
        )
        heartbeat.start()
        try:
            prewarm_route_ids = tuple(context.get("prewarm_route_ids") or ())
            if prewarm_route_ids:
                for result in await worker.prewarm_transports(
                    prewarm_route_ids,
                    worker_count=worker_count,
                ):
                    if not result.get("connected"):
                        diagnostic(
                            phase="smpp_prewarm_failed",
                            status="degraded",
                            extra={
                                "route_id": result.get("route_id"),
                                "last_error": result.get("error", ""),
                            },
                        )
            while True:
                if handle_control():
                    await drain_before_exit()
                    return
                status_state["loop_heartbeat_at"] = time.time()
                status_state["phase"] = "maintenance"
                worker.refresh_campaign_states(force=True)
                # A long-lived bind must recover even while the spool is
                # empty; otherwise a dropped idle socket would remain absent
                # until the next business record happened to arrive.
                await worker.maintain_transports()
                before = worker.snapshot()["counters"].get("taken", 0)
                status_state["active"] = True
                status_state["phase"] = "dispatch"
                publish_status(True)
                worker_snapshot = await worker.run_until_idle(
                    close_transports=False,
                    control_handler=handle_control,
                )
                if stop_requested:
                    await drain_before_exit()
                    return
                if _restartable_progress_failure(worker_snapshot):
                    progress = worker_snapshot.get("progress") or {}
                    diagnostic(
                        phase="progress_failure_restart",
                        status="degraded",
                        extra={
                            "reason": str(
                                progress.get("blocked_reason")
                                or "progress persistence stopped accepting work"
                            ),
                            "writer_errors": int(progress.get("writer_errors", 0) or 0),
                        },
                    )
                    # The spool cursor was fenced before socket I/O.  Recycle
                    # the worker and drop the uncertain terminal result so the
                    # next worker can continue with later records.
                    worker.stop()
                    await drain_before_exit()
                    return
                status_state["phase"] = "transport_drain"
                drain = await worker.drain_transports()
                if not drain.get("drained"):
                    diagnostic(
                        phase="response_drain_timeout",
                        status="degraded",
                        extra={
                            "pending": int(drain.get("pending", 0) or 0),
                            "callback_pending": int(
                                drain.get("callback_pending", 0) or 0
                            ),
                        },
                    )
                if not bool(context.get("keep_smpp_binds", True)):
                    await worker.close_transports()
                status_state["draining"] = True
                status_state["phase"] = "progress_drain"
                progress_drain = await worker.drain_progress(timeout=30.0)
                status_state["draining"] = False
                if not progress_drain.get("drained"):
                    diagnostic(
                        phase="progress_drain_timeout",
                        status="degraded",
                        extra={
                            "pending_events": int(
                                progress_drain.get("pending_events", 0) or 0
                            ),
                            "unresolved_events": int(
                                progress_drain.get("unresolved_events", 0) or 0
                            ),
                        },
                    )
                worker.release_reader_caches()
                status_state["active"] = False
                status_state["phase"] = "idle"
                publish_status(False)
                after = worker.snapshot()["counters"].get("taken", 0)
                if after == before:
                    await asyncio.sleep(idle_poll_interval)
        finally:
            status_state["active"] = False
            status_state["phase"] = "stopping"
            await worker.close_transports()
            status_state["draining"] = True
            await worker.drain_progress(timeout=30.0)
            await worker.close_progress(timeout=30.0)
            status_state["draining"] = False
            status_stop.set()
            if heartbeat.is_alive() and heartbeat is not threading.current_thread():
                heartbeat.join(timeout=2.0)
            publish_status(False)

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
        profiler.publish(force=True)


class VNextRuntime:
    """Own the one VNext lifecycle without starting legacy send services."""

    def __init__(self, root: str | os.PathLike[str], route_configs: dict[int | str, dict], *,
                 worker_count: int = 4, virtual_partition_count: int = 256,
                 max_spool_bytes: int = DEFAULT_MAX_SPOOL_BYTES, projection_queue_size: int = 8192,
                 max_inflight: int = 128, prefetch_per_partition: int = 64,
                 retention_days: int | None = None,
                 projection_batch_size: int = 512,
                 start_method: str = "spawn", projection_flush_interval_seconds: float | None = None,
                 run_id: str = "", clock: BusinessClock | object | None = None,
                 business_timezone: str | object | None = None,
                  max_journal_bytes: int | None = None,
                  progress_queue_items: int = 131_072,
                  progress_queue_bytes: int = 64 * 1024 * 1024,
                  progress_batch_size: int = 256,
                  progress_flush_interval_seconds: float = 0.05,
                  progress_aggregate_interval_seconds: float = 1.0,
                  status_interval_seconds: float = 1.0,
                  idle_poll_interval_seconds: float = 0.5,
                  profile_root: str | os.PathLike[str] | None = None,
                  profile_interval_seconds: float = 1.0,
                  keep_smpp_binds: bool = True,
                  prewarm_smpp_binds: bool = False):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.spool_root = self.root / "spool"
        self.credits_root = self.root / "route-credits"
        self.progress_root = self.root / "progress"
        self.control_state_path = self.root / "control-state.json"
        self.virtual_partition_count = int(virtual_partition_count)
        self.max_spool_bytes = int(max_spool_bytes)
        self.test_clock_file = str(os.environ.get("MC_VNEXT_TEST_CLOCK_FILE") or "").strip()
        if self.test_clock_file:
            self.clock = BusinessClock(
                FileWallClock(self.test_clock_file), business_timezone,
            )
        else:
            self.clock = ensure_clock(clock, business_timezone)
        self.max_journal_bytes = max(
            16 * 1024 * 1024,
            int(max_journal_bytes or max_spool_bytes),
        )
        self.spool = TaskSpool(
            self.spool_root, virtual_partition_count=self.virtual_partition_count,
            max_bytes=self.max_spool_bytes,
            clock=self.clock,
        )
        # Repair stale manifest byte counters before admitting new work.  This
        # is the startup compensation for a prior crash or interrupted GC.
        self.spool.reconcile_capacity(persist=True)
        self.ingress_credit = StageCredit(
            "ingress",
            limit_items=max(1024, min(1_000_000, self.max_spool_bytes // 1024)),
            limit_bytes=self.max_spool_bytes,
            high_water=0.95,
            low_water=0.70,
        )
        self.ingress = IngressRouter(self.spool, ingress_credit=self.ingress_credit)
        self.route_configs = {str(key): dict(value) for key, value in route_configs.items()}
        self._route_config_lock = threading.RLock()
        self.worker_count = max(1, min(32, int(worker_count)))
        self.keep_smpp_binds = bool(keep_smpp_binds)
        self.prewarm_smpp_binds = bool(prewarm_smpp_binds)
        self.prewarm_route_ids = tuple(
            key for key, _config in sorted(
                self.route_configs.items(),
                key=lambda item: (
                    int(item[1].get("priority", 100) or 100),
                    int(item[1].get("route_id", item[0]) or 0),
                ),
            )
        ) if self.prewarm_smpp_binds else ()
        self.max_inflight = max(1, int(max_inflight))
        self.prefetch_per_partition = max(1, int(prefetch_per_partition))
        self.projection_batch_size = max(1, min(4096, int(projection_batch_size)))
        self.progress_queue_items = max(16, int(progress_queue_items))
        self.progress_queue_bytes = max(4096, int(progress_queue_bytes))
        self.progress_batch_size = max(1, min(4096, int(progress_batch_size)))
        self.progress_flush_interval_seconds = max(
            0.01, float(progress_flush_interval_seconds)
        )
        self.progress_aggregate_interval_seconds = max(
            0.1, float(progress_aggregate_interval_seconds)
        )
        self.status_interval_seconds = max(0.2, float(status_interval_seconds))
        self.idle_poll_interval_seconds = max(
            0.1, float(idle_poll_interval_seconds)
        )
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
        self.profile_root = Path(profile_root).resolve() if profile_root else None
        self.profile_interval_seconds = max(0.1, float(profile_interval_seconds))
        self.diagnostics_root = self.root / "diagnostics"
        self.projection = ProjectionWriterProcess(
            self.root / "projection", queue_size=projection_queue_size,
            batch_size=self.projection_batch_size,
            journal_paths=[self.progress_root / f"worker-{worker_id}.jsonl" for worker_id in range(max(1, int(worker_count)))],
            flush_interval_seconds=self.projection_flush_interval_seconds,
            start_method=self.start_method,
            mp_context=self._mp_context,
            diagnostics_root=self.diagnostics_root,
            run_id=self.run_id,
            profile_root=self.profile_root,
            profile_interval_seconds=self.profile_interval_seconds,
            retention_days=retention_days,
        )
        self.supervisor: WorkerSupervisor | None = None
        self._started = False
        self.started_at = 0.0
        self._control_state: dict[tuple[str, int], str] = {}
        self._control_version = 0
        self._control_lock = threading.RLock()
        self._projection_watchdog_stop = threading.Event()
        self._projection_watchdog_thread: threading.Thread | None = None
        self._projection_restart_failures = 0
        self._projection_restart_not_before = 0.0
        try:
            self._projection_restart_max_failures = max(
                1,
                min(
                    100,
                    int(os.environ.get("MC_VNEXT_PROJECTION_RESTART_MAX_FAILURES", "5")),
                ),
            )
        except (TypeError, ValueError):
            self._projection_restart_max_failures = 5
        try:
            self._projection_restart_cooldown_seconds = max(
                5.0,
                min(
                    86400.0,
                    float(os.environ.get("MC_VNEXT_PROJECTION_RESTART_COOLDOWN_SECONDS", "60")),
                ),
            )
        except (TypeError, ValueError):
            self._projection_restart_cooldown_seconds = 60.0
        try:
            self._projection_restart_max_backoff_seconds = max(
                1.0,
                min(
                    3600.0,
                    float(os.environ.get("MC_VNEXT_PROJECTION_RESTART_MAX_BACKOFF_SECONDS", "30")),
                ),
            )
        except (TypeError, ValueError):
            self._projection_restart_max_backoff_seconds = 30.0
        self._projection_degraded = False
        self._projection_last_error = ""
        self._daily_epoch_stop = threading.Event()
        self._daily_epoch_thread: threading.Thread | None = None
        self._worker_watchdog_lock = threading.Lock()
        self._worker_watchdog_state: dict[str, dict] = {}
        self._worker_watchdog_last_check = 0.0
        self._worker_watchdog_last_restart: dict[int, float] = {}
        self._worker_watchdog_restart_cooldown_seconds = 60.0
        self._worker_watchdog_status_timeout_seconds = max(
            5.0, self.status_interval_seconds * 4.0
        )
        self._worker_watchdog_loop_timeout_seconds = max(
            30.0, self.status_interval_seconds * 30.0
        )
        self._partition_rebalance_last_at = 0.0
        self._partition_rebalance_cooldown_seconds = 30.0
        self._partition_rebalance_warmup_seconds = 30.0
        self._partition_rebalance_state: dict = {
            "state": "idle", "moved": {}, "updated_at": 0.0,
        }
        try:
            self.spool_gc_interval_seconds = max(
                5.0,
                min(
                    3600.0,
                    float(os.environ.get("MC_VNEXT_SPOOL_GC_INTERVAL_SECONDS", "60")),
                ),
            )
        except (TypeError, ValueError):
            self.spool_gc_interval_seconds = 60.0
        self._spool_gc_lock = threading.Lock()
        self._spool_gc_last_at = 0.0
        self._spool_gc_last_result: dict = {
            "state": "not_run",
            "interval_seconds": self.spool_gc_interval_seconds,
        }
        write_process_metadata(
            self.diagnostics_root,
            "runtime-parent",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="initialized",
            status="initialized",
        )

    def rollover_if_needed(self) -> dict:
        """Run the cheap wall-clock fence and return its durable transition."""

        return self.spool.rollover_if_needed()

    def _workers_heartbeat_ready_for_gc(self) -> bool:
        """Require a current-run heartbeat before unlinking expired segments."""

        if not self._started or self.supervisor is None:
            return True
        workers = [
            item for item in self._worker_progress()
            if str(item.get("run_id") or "") == self.run_id
        ]
        if len(workers) < max(1, int(self.worker_count)):
            return False
        freshness = max(5.0, self.status_interval_seconds * 4.0)
        now = time.time()
        for item in workers:
            if now - float(item.get("updated_at") or 0.0) > freshness:
                return False
        return True

    def _run_spool_gc(self, *, force: bool = False, trigger: str = "interval") -> dict:
        now = time.monotonic()
        if not force and now - self._spool_gc_last_at < self.spool_gc_interval_seconds:
            return dict(self._spool_gc_last_result)
        if not self._workers_heartbeat_ready_for_gc():
            result = {
                "state": "waiting_for_workers",
                "trigger": trigger,
                "at": time.time(),
            }
            self._spool_gc_last_result = result
            return dict(result)
        if not self._spool_gc_lock.acquire(blocking=False):
            return dict(self._spool_gc_last_result)
        try:
            # Expiry fencing makes old payloads safe; an open Windows handle
            # simply causes this bounded batch to retry on the next tick.
            # shortcut: 32 segments per maintenance tick, increase only if GC lag is observed.
            results = self.spool.cleanup_closed_days(
                max_segments=None if force else 32,
            )
            self._spool_gc_last_at = now
            result = {
                "state": "completed",
                "trigger": trigger,
                "at": time.time(),
                "results": results,
            }
            self._spool_gc_last_result = result
            return dict(result)
        except Exception as exc:
            result = {
                "state": "error",
                "trigger": trigger,
                "at": time.time(),
                "error": f"{type(exc).__name__}: {exc}",
            }
            self._spool_gc_last_result = result
            logger.exception("VNext spool cleanup failed")
            return dict(result)
        finally:
            self._spool_gc_lock.release()

    def _daily_epoch_loop(self) -> None:
        while not self._daily_epoch_stop.wait(0.25):
            try:
                transition = self.rollover_if_needed()
                self._run_spool_gc(
                    trigger="rollover" if transition.get("changed") else "interval",
                )
            except (OSError, RuntimeError, ValueError):
                # The next tick retries; a delayed maintenance tick cannot
                # reopen an old socket because workers check the spool fence.
                pass
            try:
                self._run_worker_watchdog()
            except Exception:
                # A monitoring failure must not stop the daily fence/GC loop.
                logger.exception("VNext worker watchdog failed")
            try:
                self._run_partition_rebalancer()
            except Exception:
                # Rebalancing is an optimization; a failed pass must never
                # stop the business-day fence or worker watchdog.
                logger.exception("VNext partition rebalancer failed")

    def _set_worker_watchdog_state(
        self, worker_id: int, state: str, reason: str, **details,
    ) -> None:
        key = str(int(worker_id))
        payload = {
            "worker_id": int(worker_id),
            "state": str(state),
            "reason": str(reason),
            "updated_at": time.time(),
            **details,
        }
        with self._worker_watchdog_lock:
            previous = dict(self._worker_watchdog_state.get(key) or {})
            self._worker_watchdog_state[key] = payload
        if (
            previous.get("state") == payload["state"]
            and previous.get("reason") == payload["reason"]
        ):
            return
        status = "degraded" if state in {"blocked", "restart_failed", "stale"} else "running"
        write_process_metadata(
            self.diagnostics_root,
            f"worker-watchdog-{int(worker_id)}",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="worker_watchdog",
            status=status,
            progress={"worker_id": int(worker_id)},
            extra={"state": str(state), "reason": str(reason), **details},
        )
        if state in {"blocked", "restart_failed"}:
            logger.warning(
                "VNext worker watchdog worker=%s state=%s reason=%s",
                worker_id, state, reason,
            )

    def _watchdog_restart_worker(self, worker_id: int, reason: str) -> None:
        now = time.monotonic()
        last = self._worker_watchdog_last_restart.get(int(worker_id), 0.0)
        if now - last < self._worker_watchdog_restart_cooldown_seconds:
            self._set_worker_watchdog_state(
                worker_id,
                "restart_cooldown",
                reason,
                retry_after_seconds=round(
                    self._worker_watchdog_restart_cooldown_seconds - (now - last), 3
                ),
            )
            return
        if self.supervisor is None:
            self._set_worker_watchdog_state(worker_id, "stale", "supervisor_unavailable")
            return
        try:
            self.supervisor.restart_worker(int(worker_id))
        except Exception as exc:
            self._worker_watchdog_last_restart[int(worker_id)] = now
            self._set_worker_watchdog_state(
                worker_id,
                "restart_failed",
                reason,
                error=f"{type(exc).__name__}: {exc}",
            )
            return
        self._worker_watchdog_last_restart[int(worker_id)] = now
        self._set_worker_watchdog_state(
            worker_id, "restarted", reason,
        )

    def _run_worker_watchdog(self) -> None:
        """Detect live workers whose main loop stopped advancing.

        A fresh heartbeat alone is insufficient: the child heartbeat thread
        can survive while the async dispatch loop is stuck.  The VNext send
        fence makes recycling at-most-once: the current task is abandoned and
        later records continue without redispatching a checkpointed task.
        """

        if not self._started or self.supervisor is None:
            return
        now = time.monotonic()
        if now - self._worker_watchdog_last_check < 1.0:
            return
        self._worker_watchdog_last_check = now
        supervisor = self.supervisor.status()
        live = {
            int(item["worker_id"]): item
            for item in supervisor.get("workers", [])
            if isinstance(item, dict) and "worker_id" in item
        }
        statuses = {}
        for item in self._worker_progress():
            try:
                statuses[int(item["worker_id"])] = item
            except (KeyError, TypeError, ValueError):
                continue
        wall_now = time.time()
        for worker_id in range(self.worker_count):
            process = live.get(worker_id)
            if not process or not bool(process.get("alive")):
                self._set_worker_watchdog_state(
                    worker_id, "process_restart_pending", "supervisor_owns_process_restart",
                )
                continue
            item = statuses.get(worker_id)
            if not item or str(item.get("run_id") or "") != self.run_id:
                self._set_worker_watchdog_state(
                    worker_id, "status_missing", "current_run_status_not_available",
                )
                continue
            status_pid = int(item.get("process_pid") or 0)
            process_pid = int(process.get("pid") or 0)
            if not status_pid or not process_pid or status_pid != process_pid:
                self._set_worker_watchdog_state(
                    worker_id, "status_missing", "worker_status_belongs_to_another_process",
                )
                continue
            snapshot = item.get("snapshot") or {}
            progress = snapshot.get("progress") or {}
            phase = str(item.get("phase") or "")
            recovery_supported = str(item.get("recovery_policy") or "") == (
                "checkpoint_before_socket_drop_uncertain"
            )
            status_age = max(0.0, wall_now - float(item.get("updated_at") or wall_now))
            loop_at = float(item.get("loop_heartbeat_at") or 0.0)
            loop_age = max(0.0, wall_now - loop_at) if loop_at else None

            progress_failure = bool(
                not bool(progress.get("accepting", True))
                or int(progress.get("writer_errors", 0) or 0) > 0
                or int(progress.get("unresolved_events", 0) or 0) > 0
                or int(snapshot.get("progress_lost_events", 0) or 0) > 0
            )
            if progress_failure:
                self._set_worker_watchdog_state(
                    worker_id, "blocked", "progress_persistence_requires_reconciliation",
                    status_age_seconds=round(status_age, 3),
                )
                continue

            if status_age > self._worker_watchdog_status_timeout_seconds:
                if recovery_supported:
                    self._watchdog_restart_worker(
                        worker_id, "worker_status_heartbeat_stale_drop_current_task",
                    )
                else:
                    self._set_worker_watchdog_state(
                        worker_id,
                        "stale",
                        "worker_status_predates_safe_drop_recovery_policy",
                        status_age_seconds=round(status_age, 3),
                    )
                continue

            if phase in {"idle", "maintenance"} and loop_age is not None and loop_age > self._worker_watchdog_loop_timeout_seconds:
                if recovery_supported:
                    self._watchdog_restart_worker(
                        worker_id, "worker_main_loop_stalled_drop_current_task",
                    )
                else:
                    self._set_worker_watchdog_state(
                        worker_id,
                        "blocked",
                        "worker_status_predates_safe_drop_recovery_policy",
                        loop_age_seconds=round(loop_age, 3),
                    )
                continue

            self._set_worker_watchdog_state(
                worker_id,
                "healthy" if recovery_supported else "unmonitored",
                "heartbeat_and_main_loop_advancing" if recovery_supported else phase or "watchdog_policy_missing",
                status_age_seconds=round(status_age, 3),
                loop_age_seconds=round(loop_age, 3) if loop_age is not None else None,
            )

    def _run_partition_rebalancer(self) -> None:
        """Move whole virtual partitions when worker backlog is materially skewed."""

        if not self._started or self.supervisor is None or self.worker_count < 2:
            return
        now = time.monotonic()
        if time.time() - self.started_at < self._partition_rebalance_warmup_seconds:
            return
        if now - self._partition_rebalance_last_at < self._partition_rebalance_cooldown_seconds:
            return
        self._partition_rebalance_last_at = now
        try:
            spool = self.spool.lightweight_snapshot()
            supervisor = self.supervisor.status()
            assignments = {
                partition: self.supervisor.map.assignment(partition).worker_id
                for partition in range(self.virtual_partition_count)
            }
            plan = _plan_partition_rebalance(
                assignments,
                spool.get("pending_by_partition") or {},
                self.worker_count,
            )
            if not plan:
                return
            affected = {
                assignments[partition] for partition in plan
            } | {plan[partition] for partition in plan}
            live = {
                int(item.get("worker_id")): item
                for item in supervisor.get("workers", [])
                if isinstance(item, dict) and item.get("worker_id") is not None
            }
            statuses = {
                int(item.get("worker_id")): item
                for item in self._worker_progress()
                if isinstance(item, dict) and item.get("worker_id") is not None
            }
            freshness = max(5.0, self.status_interval_seconds * 4.0)
            wall_now = time.time()
            for worker_id in affected:
                process = live.get(worker_id) or {}
                item = statuses.get(worker_id) or {}
                progress = (item.get("snapshot") or {}).get("progress") or {}
                if not bool(process.get("alive")):
                    return
                if wall_now - float(item.get("updated_at") or 0.0) > freshness:
                    return
                if (
                    not bool(progress.get("accepting", True))
                    or int(progress.get("writer_errors", 0) or 0) > 0
                    or int(progress.get("unresolved_events", 0) or 0) > 0
                ):
                    return
            result = self.supervisor.reassign_partitions(plan, timeout=15.0)
            self._partition_rebalance_state = {
                "state": "rebalanced" if result.get("ok") else "deferred",
                "reason": str(result.get("reason") or ""),
                "moved": {str(key): int(value) for key, value in plan.items()},
                "updated_at": time.time(),
            }
            if result.get("ok") and result.get("changed"):
                logger.info(
                    "VNext partition rebalance moved %s partitions: %s",
                    len(plan), result.get("moved") or {},
                )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError):
            self._partition_rebalance_state = {
                "state": "error", "moved": {}, "updated_at": time.time(),
            }
            logger.exception("VNext partition rebalance failed")

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
            try:
                snapshot = self.projection.snapshot()
                if snapshot.get("recovery_required"):
                    self._projection_degraded = True
                    self._projection_last_error = str(
                        snapshot.get("recovery_error")
                        or "projection recovery is required"
                    )
                    # A persistent marker is a circuit breaker: reopening the
                    # same database cannot repair a corrupt WAL.
                    continue
                if snapshot.get("ready", False):
                    # A successful start closes the current failure streak.
                    self._projection_restart_failures = 0
                    self._projection_restart_not_before = 0.0
                    self._projection_degraded = False
                    self._projection_last_error = ""
                    continue
                if snapshot.get("alive", False):
                    if snapshot.get("status_unhealthy"):
                        self._projection_degraded = True
                        self._projection_last_error = str(
                            snapshot.get("last_error")
                            or "projection child reported an unhealthy status"
                        )
                    elif snapshot.get("status_stale") or snapshot.get("core_dumping"):
                        self._projection_degraded = True
                        self._projection_last_error = (
                            "projection child is dumping core" if snapshot.get("core_dumping")
                            else "projection child status heartbeat is stale"
                        )
                    # The child may still be opening DuckDB or replaying its
                    # journals.  Do not start a competing child while it is
                    # alive; this is deliberately outside the send path.
                    continue
                if time.monotonic() < self._projection_restart_not_before:
                    continue
                restarted = self.projection.restart()
                if restarted:
                    # Process.start() only means that the child was forked. It
                    # may still fail while opening DuckDB, so count this as a
                    # bounded recovery attempt and apply a circuit cooldown
                    # after repeated native/database failures.
                    self._projection_restart_failures += 1
                    self._projection_degraded = True
                    exitcode = snapshot.get("exitcode")
                    self._projection_last_error = (
                        f"projection child exited exitcode={exitcode}"
                    )
                    failures = self._projection_restart_failures
                    if failures >= self._projection_restart_max_failures:
                        delay = self._projection_restart_cooldown_seconds
                    else:
                        delay = min(
                            self._projection_restart_max_backoff_seconds,
                            0.25 * (2 ** min(failures - 1, 8)),
                        )
                    self._projection_restart_not_before = time.monotonic() + delay
                    with contextlib.suppress(Exception):
                        write_process_metadata(
                            self.diagnostics_root,
                            "runtime-parent",
                            run_id=self.run_id,
                            start_method=self.start_method,
                            phase="projection_restart",
                            status="degraded",
                            extra={
                                "last_error": self._projection_last_error,
                                "restart_failures": failures,
                                "next_retry_seconds": round(delay, 3),
                            },
                        )
            except Exception as exc:
                # A watchdog error must not silently terminate the recovery
                # thread (for example an old child/status shape raising
                # AttributeError).  The sender remains independent while the
                # next bounded check retries projection recovery.
                self._projection_restart_failures += 1
                self._projection_degraded = True
                self._projection_last_error = f"{type(exc).__name__}: {exc}"
                failures = self._projection_restart_failures
                delay = min(
                    self._projection_restart_max_backoff_seconds,
                    self._projection_restart_cooldown_seconds
                    if failures >= self._projection_restart_max_failures
                    else 0.25 * (2 ** min(failures - 1, 8)),
                )
                self._projection_restart_not_before = time.monotonic() + delay
                with contextlib.suppress(Exception):
                    write_process_metadata(
                        self.diagnostics_root,
                        "runtime-parent",
                        run_id=self.run_id,
                        start_method=self.start_method,
                        phase="projection_watchdog_error",
                        status="degraded",
                        extra={
                            "last_error": self._projection_last_error,
                            "restart_failures": failures,
                            "next_retry_seconds": round(delay, 3),
                        },
                    )
                continue

    def start(self) -> None:
        if self._started:
            return
        self.progress_root.mkdir(parents=True, exist_ok=True)
        self.rollover_if_needed()
        # Bound startup work; the resident loop continues the same plan in the
        # background without holding up ingress for every historical segment.
        self._run_spool_gc(trigger="startup")
        with self._control_lock:
            self._load_control_state()
            if not self.control_state_path.exists():
                self._persist_control_state()
        self._projection_restart_failures = 0
        self._projection_restart_not_before = 0.0
        self._projection_degraded = False
        self._projection_last_error = ""
        self._worker_watchdog_last_check = 0.0
        self._worker_watchdog_last_restart.clear()
        with self._worker_watchdog_lock:
            self._worker_watchdog_state.clear()
        if self.projection.recovery_required():
            self._projection_degraded = True
            marker = self.projection.snapshot()
            self._projection_last_error = str(
                marker.get("recovery_error") or "projection recovery is required"
            )
        else:
            self.projection.start()
        context = {
            "spool_root": str(self.spool_root),
            "credits_root": str(self.credits_root),
            "progress_root": str(self.progress_root),
            "control_state_path": str(self.control_state_path),
            "virtual_partition_count": self.virtual_partition_count,
            "max_spool_bytes": self.max_spool_bytes,
            "max_journal_bytes": self.max_journal_bytes,
            "business_timezone": self.clock.timezone_name,
            "test_clock_file": self.test_clock_file,
            "route_configs": self.route_configs,
            "worker_count": self.worker_count,
            "max_inflight": self.max_inflight,
            "prefetch_per_partition": self.prefetch_per_partition,
            "projection_queue": self.projection.queue,
            "diagnostics_root": str(self.diagnostics_root),
            "run_id": self.run_id,
            "start_method": self.start_method,
            "profile_root": str(self.profile_root) if self.profile_root else "",
            "profile_interval_seconds": self.profile_interval_seconds,
            "progress_queue_items": self.progress_queue_items,
            "progress_queue_bytes": self.progress_queue_bytes,
            "progress_batch_size": self.progress_batch_size,
            "progress_flush_interval_seconds": self.progress_flush_interval_seconds,
            "progress_aggregate_interval_seconds": (
                self.progress_aggregate_interval_seconds
            ),
            "status_interval_seconds": self.status_interval_seconds,
            "idle_poll_interval_seconds": self.idle_poll_interval_seconds,
            "keep_smpp_binds": self.keep_smpp_binds,
            "prewarm_route_ids": self.prewarm_route_ids,
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
        self.started_at = time.time()
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
        self._daily_epoch_stop.clear()
        self._daily_epoch_thread = threading.Thread(
            target=self._daily_epoch_loop, name="mc-vnext-daily-epoch", daemon=True,
        )
        self._daily_epoch_thread.start()

    def apply_retention_days(self, value: int) -> dict:
        """Apply task aging through the projection owner control channel."""

        setter = getattr(self.projection, "set_retention_days", None)
        if not callable(setter):
            return {"ok": False, "reason": "projection_control_unavailable"}
        applied = bool(setter(value))
        return {
            "ok": applied,
            "reason": "applied" if applied else "projection_control_deferred",
        }

    def reload_route_rates(self, route_configs: dict[int | str, dict]) -> dict:
        """Apply only TPS settings to live workers without closing SMPP binds."""

        with self._route_config_lock:
            if not self._started or self.supervisor is None:
                return {
                    "ok": False,
                    "reason": "runtime_inactive",
                    "updated_routes": [],
                }
            updates: dict[str, dict] = {}
            missing_routes: list[str] = []
            for raw_route_id, values in (route_configs or {}).items():
                key = str(raw_route_id)
                current = self.route_configs.get(key)
                if current is None:
                    missing_routes.append(key)
                    continue
                if not isinstance(values, dict):
                    missing_routes.append(key)
                    continue
                scope = str(values.get("tps_scope", current.get("tps_scope", "route_shared")))
                scope = scope.strip().lower()
                if scope not in {"route_shared", "per_bind"}:
                    return {
                        "ok": False,
                        "reason": f"invalid_tps_scope:{scope}",
                        "updated_routes": [],
                    }
                try:
                    tps = max(0.0, float(values.get("tps", current.get("tps", 0.0)) or 0.0))
                except (TypeError, ValueError):
                    return {
                        "ok": False,
                        "reason": f"invalid_tps:{key}",
                        "updated_routes": [],
                    }
                current["tps"] = tps
                current["tps_scope"] = scope
                updates[key] = {
                    "route_id": int(current.get("route_id", key)),
                    "tps": tps,
                    "tps_scope": scope,
                }
            if missing_routes:
                return {
                    "ok": False,
                    "reason": "route_not_active",
                    "updated_routes": [],
                    "missing_routes": sorted(set(missing_routes)),
                }
            if not updates:
                return {"ok": False, "reason": "no_active_routes", "updated_routes": []}
            broadcast = self.supervisor.broadcast({
                "type": "reload_route_rates",
                "routes": updates,
            })
            failed = broadcast.get("failed_worker_ids") or []
            return {
                "ok": not failed,
                "reason": "worker_broadcast_failed" if failed else "applied",
                "updated_routes": sorted(updates),
                "sent_worker_ids": broadcast.get("sent_worker_ids") or [],
                "failed_worker_ids": failed,
            }

    def _worker_progress(self) -> list[dict]:
        values = []
        for path in sorted(self.progress_root.glob("worker-*.status.json")):
            try:
                values.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError, TypeError):
                continue
        return values

    def _worker_watchdog_snapshot(self) -> dict[str, dict]:
        with self._worker_watchdog_lock:
            return {
                key: dict(value) for key, value in self._worker_watchdog_state.items()
            }

    def capacity_snapshot(self) -> dict:
        """Return the bounded read-only snapshot needed by capacity monitoring.

        Capacity polling must not enter ``status()``, which intentionally
        performs a full reconciliation and scans retained journal segments.
        The lightweight spool snapshot uses maintained manifest and cursor
        metadata instead of walking every in-memory spool record.
        """

        spool = self.spool.lightweight_snapshot()
        return {
            "started": self._started,
            "run_id": self.run_id,
            "start_method": self.start_method,
            "spool": spool,
            "supervisor": self.supervisor.status() if self.supervisor else None,
            "projection": self.projection.snapshot(),
            "worker_progress": self._worker_progress(),
            "worker_watchdog": self._worker_watchdog_snapshot(),
            "partition_rebalance": dict(self._partition_rebalance_state),
            "spool_gc": dict(self._spool_gc_last_result),
            "daily_epoch": {
                "business_day": spool.get("business_day") or self.spool.current_business_day,
                "business_epoch": int(spool.get("business_epoch") or self.spool.business_epoch),
                "timezone": self.clock.timezone_name,
            },
            "snapshot_mode": "capacity_lightweight",
        }

    def campaign_counter_snapshot(self) -> dict:
        """Return durable per-campaign counters without replaying journals.

        Worker status files already contain the counters maintained by each
        worker's :class:`ProgressPersistence`.  The control plane can read
        those small snapshots without opening DuckDB, scanning the spool, or
        parsing the append-only progress journals.  This method is therefore
        deliberately read-only and kept separate from the dispatch path.
        """

        counters_by_campaign: dict[tuple[str, int], dict[str, int]] = {}
        workers = self._worker_progress()
        counter_fields = {
            "accepted_tasks", "tasks_taken", "socket_written_events",
            "socket_written_pdus", "write_failed", "unknown",
            "response_success", "response_error", "cancelled", "expired",
            "durable_events",
        }
        for worker in workers:
            snapshot = worker.get("snapshot") or {}
            for item in snapshot.get("campaign_counters") or []:
                try:
                    campaign_type = str(item.get("campaign_type") or "")
                    campaign_id = int(item.get("campaign_id") or 0)
                except (TypeError, ValueError):
                    continue
                key = (campaign_type, campaign_id)
                target = counters_by_campaign.setdefault(
                    key,
                    {field: 0 for field in counter_fields},
                )
                for field in counter_fields:
                    value = item.get(field)
                    if isinstance(value, (int, float)):
                        target[field] += int(value)

        campaigns = []
        for (campaign_type, campaign_id), counters in sorted(counters_by_campaign.items()):
            campaigns.append({
                "campaign_type": campaign_type,
                "campaign_id": campaign_id,
                **counters,
            })
        expected = max(1, int(self.worker_count))
        return {
            "campaigns": campaigns,
            "workers_seen": len(workers),
            "workers_expected": expected,
            "complete": len(workers) >= expected,
            "source": "worker_status_aggregate",
        }

    def stop(self) -> None:
        if self._started:
            # Stop maintenance first so the watchdog cannot race a supervisor
            # shutdown and launch a replacement while the parent is stopping.
            self._daily_epoch_stop.set()
            daily_thread = self._daily_epoch_thread
            self._daily_epoch_thread = None
            if daily_thread and daily_thread.is_alive() and daily_thread is not threading.current_thread():
                daily_thread.join(timeout=1.0)
        if self.supervisor:
            self.supervisor.stop()
            self.supervisor = None
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
            # Reclamation is deliberately after worker shutdown.  Closed-day
            # evidence remains in journals/projection; only old spool payloads
            # are eligible for deletion.
            self._run_spool_gc(force=True, trigger="shutdown")
        self._started = False
        self.started_at = 0.0
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
        campaign_name = str(kwargs.pop("campaign_name", "") or "")
        self.rollover_if_needed()
        result = self.ingress.enqueue_chunk(*args, **kwargs)
        # The spool append is the admission/durability boundary.  These
        # zero-wait IPC hints only feed the background projection's logical
        # task index; a full projection queue never rejects an accepted SMS
        # and never blocks the worker/send path.
        if result.accepted_records and self.projection is not None:
            hints = [
                {
                    "event_kind": "task_index",
                    "logical_task_id": str(record.event_id),
                    "event_id": str(record.event_id),
                    "partition_id": int(record.partition_id),
                    "ordinal": int(record.ordinal),
                    "campaign_type": str(record.campaign_type),
                    "campaign_id": int(record.campaign_id),
                    "route_id": int(record.route_id),
                    "normalized_msisdn": str(record.normalized_msisdn),
                    "language": str(record.language or ""),
                    "campaign_name": campaign_name,
                    "state": "queued",
                    "parts_written": 0,
                    "parts_total": int(record.part_count_hint or 0),
                    "message": str(record.message or ""),
                    "error": "",
                    "created_at": float(record.created_at),
                    "business_day": str(record.business_day or self.spool.current_business_day),
                    "expires_at": float(record.expires_at or 0.0),
                    "event_at": time.time(),
                }
                for record in result.accepted_records
            ]
            try:
                # One IPC item per uploaded chunk keeps the read-model hint
                # overhead bounded even for a large customer list.
                self.projection.submit({
                    "event_kind": "task_index_batch",
                    "events": hints,
                })
            except ProjectionBackpressure:
                # Worker progress remains authoritative and will populate the
                # same row when the task reaches dispatch.  Do not turn a
                # diagnostic read-model delay into ingress pressure.
                pass
        return result

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
        for logical_path in sorted(self.progress_root.glob("worker-*.jsonl")):
            journal = ProgressJournal(logical_path)
            storage = journal.storage_snapshot()
            journal_bytes += int(storage["total_bytes"])
            journal_paths.extend(
                {
                    "path": str(path),
                    "bytes": int(path.stat().st_size),
                }
                for path in journal.segment_paths()
                if path.is_file()
            )
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
            "worker_watchdog": self._worker_watchdog_snapshot(),
            "journal": {"retained_bytes": journal_bytes, "paths": journal_paths},
            "daily_epoch": {
                "business_day": self.spool.current_business_day,
                "business_epoch": self.spool.business_epoch,
                "timezone": self.clock.timezone_name,
                "closed_days": dict(self.spool._manifest.get("closed_days", {})),
            },
            "ingress_credit": self.ingress_credit.snapshot(),
            "route_credits": route_credits,
            "smpp_lifecycle": {
                "keep_binds": self.keep_smpp_binds,
                "prewarm_binds": self.prewarm_smpp_binds,
                "prewarm_route_ids": list(self.prewarm_route_ids),
            },
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
            "projection_degraded": bool(self._projection_degraded),
            "projection_last_error": str(self._projection_last_error or ""),
            "projection_restart_max_failures": int(self._projection_restart_max_failures),
            "projection_restart_cooldown_seconds": float(
                self._projection_restart_cooldown_seconds
            ),
            "spool_gc": dict(self._spool_gc_last_result),
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
