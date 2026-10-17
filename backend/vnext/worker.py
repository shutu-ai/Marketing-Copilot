"""Bounded, fair shard workers that never write the application DuckDB."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Awaitable, Callable

from .critical_path_profiler import CriticalPathProfiler
from .metrics import VNextMetrics
from .route_credits import RouteCreditBroker
from .smpp_transport import AsyncSmppTransport, SocketWriteResult
from .spool import SpoolRecord, TaskSpool


@dataclass(frozen=True)
class WorkerProgress:
    event_id: str
    partition_id: int
    ordinal: int
    campaign_type: str
    campaign_id: int
    route_id: int
    normalized_msisdn: str
    state: str
    parts_written: int
    parts_total: int
    error: str = ""
    created_at: float = 0.0
    message: str = ""
    business_day: str = ""
    expires_at: float = 0.0

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "partition_id": self.partition_id,
            "ordinal": self.ordinal,
            "campaign_type": self.campaign_type,
            "campaign_id": self.campaign_id,
            "route_id": self.route_id,
            "normalized_msisdn": self.normalized_msisdn,
            "state": self.state,
            "parts_written": self.parts_written,
            "parts_total": self.parts_total,
            "error": self.error,
            "created_at": self.created_at or time.time(),
            "message": self.message,
            "business_day": self.business_day,
            "expires_at": float(self.expires_at or 0.0),
        }


class ShardWorker:
    """Consume assigned logical shards with bounded fair concurrency."""

    def __init__(self, worker_id: int, partitions: tuple[int, ...], spool: TaskSpool,
                 route_configs: dict[int | str, dict], *, credits: RouteCreditBroker | None = None,
                 transport_factory: Callable | None = None,
                 progress_sink: Callable[[dict], Awaitable[None] | None] | None = None,
                 control_loader: Callable[[], list[dict] | None] | None = None,
                 journal_admission: Callable[[SpoolRecord], bool] | None = None,
                 max_inflight: int = 64, prefetch_per_partition: int = 32,
                  metrics: VNextMetrics | None = None,
                  profiler: CriticalPathProfiler | None = None):
        self.worker_id = int(worker_id)
        self.partitions = tuple(int(item) for item in partitions)
        self.spool = spool
        self.route_configs = {str(key): dict(value) for key, value in route_configs.items()}
        self.credits = credits or RouteCreditBroker()
        self.transport_factory = transport_factory or (lambda config, broker: AsyncSmppTransport(config, credits=broker))
        self.progress_sink = progress_sink
        self.control_loader = control_loader
        self.journal_admission = journal_admission
        self.max_inflight = max(1, int(max_inflight))
        self.prefetch_per_partition = max(1, int(prefetch_per_partition))
        self.metrics = metrics or VNextMetrics()
        self.profiler = profiler
        self._stop = asyncio.Event()
        self._paused_campaigns: set[tuple[str, int]] = set()
        self._cancelled_campaigns: set[tuple[str, int]] = set()
        self._transports: dict[str, list[object]] = {}
        self._transport_creation_locks: dict[str, asyncio.Lock] = {}
        self._transport_cursor: dict[str, int] = {}
        self._number_locks: dict[str, asyncio.Lock] = {}
        self._progress_events: deque[dict] = deque(maxlen=10_000)
        self._group_cursor = 0
        self._last_control_refresh = 0.0
        self._inflight = 0

    def stop(self) -> None:
        self._stop.set()

    def set_campaign_state(self, campaign_type: str, campaign_id: int, state: str) -> None:
        key = (str(campaign_type), int(campaign_id))
        normalized = str(state).lower()
        if normalized in {"pause", "paused"}:
            self._paused_campaigns.add(key)
        elif normalized in {"running", "resume", "resumed"}:
            self._paused_campaigns.discard(key)
            self._cancelled_campaigns.discard(key)
        if normalized in {"cancel", "cancelled", "stopped"}:
            self._paused_campaigns.add(key)
            self._cancelled_campaigns.add(key)

    def refresh_campaign_states(self, *, force: bool = False) -> None:
        if not self.control_loader:
            return
        now = time.monotonic()
        if not force and now - self._last_control_refresh < 0.05:
            return
        self._last_control_refresh = now
        values = self.control_loader()
        if values is None:
            return
        self._paused_campaigns.clear()
        self._cancelled_campaigns.clear()
        for item in values:
            try:
                self.set_campaign_state(item["campaign_type"], int(item["campaign_id"]), item["state"])
            except (KeyError, TypeError, ValueError):
                continue

    def _eligible(self, record: SpoolRecord) -> bool:
        self.refresh_campaign_states()
        key = (record.campaign_type, record.campaign_id)
        return key in self._cancelled_campaigns or key not in self._paused_campaigns

    async def _transport(self, route_id: int):
        key = str(int(route_id))
        config = self.route_configs.get(key)
        if not config:
            return None
        lock = self._transport_creation_locks.setdefault(key, asyncio.Lock())
        async with lock:
            transports = self._transports.setdefault(key, [])
            target = max(1, int(config.get("_worker_bind_target", 1) or 1))
            while len(transports) < target:
                transport_config = dict(config)
                transport_config.setdefault("route_id", key)
                value = self.transport_factory(transport_config, self.credits)
                if inspect.isawaitable(value):
                    value = await value
                set_response_sink = getattr(value, "set_response_sink", None)
                if set_response_sink:
                    set_response_sink(self._response_event)
                set_profiler = getattr(value, "set_profiler", None)
                if set_profiler:
                    set_profiler(self.profiler)
                transports.append(value)
            cursor = self._transport_cursor.get(key, 0)
            value = transports[cursor % len(transports)]
            self._transport_cursor[key] = cursor + 1
            return value

    async def _emit(self, progress: WorkerProgress, **overrides) -> None:
        event = progress.as_dict()
        event.update({
            "taken": int(not progress.error.startswith("dispatch checkpoint failed")),
            "socket_written": int(progress.state == "socket_written"),
            "write_failed": int(progress.state == "write_failed"),
            "unknown": int(progress.state == "unknown"),
            "response_success": 0,
            "response_error": 0,
            "expired": int(progress.state == "expired"),
            "accepted": int(not progress.state.startswith("submit_sm_resp_")),
        })
        event.update(overrides)
        self._progress_events.append(event)
        self.metrics.record(event)
        if self.profiler:
            self.profiler.inc("progress_events")
            self.profiler.inc(f"progress_state_{progress.state}")
            if progress.state.startswith("submit_sm_resp_") or progress.state == "unknown":
                self.profiler.inc("response_events_emitted")
        if not self.progress_sink:
            if self.profiler:
                self.profiler.publish_if_due()
            return
        result = self.progress_sink(event)
        if inspect.isawaitable(result):
            await result
        if self.profiler:
            self.profiler.publish_if_due()

    async def _response_event(self, response: dict) -> None:
        context = dict(response.get("context") or {})
        event_id = str(context.get("event_id") or "")
        if not event_id:
            return
        if response.get("unknown"):
            state = "unknown"
        else:
            state = "submit_sm_resp_success" if bool(response.get("success")) else "submit_sm_resp_error"
        success = bool(response.get("success"))
        if self.profiler:
            self.profiler.inc("response_callback_events")
        progress = WorkerProgress(
            event_id=f"{event_id}:resp:{int(response.get('sequence') or 0)}",
            partition_id=int(context.get("partition_id") or 0),
            ordinal=int(context.get("ordinal") or 0),
            campaign_type=str(context.get("campaign_type") or ""),
            campaign_id=int(context.get("campaign_id") or 0),
            route_id=int(context.get("route_id") or 0),
            normalized_msisdn=str(context.get("normalized_msisdn") or ""),
            state=state,
            parts_written=0,
            parts_total=int(context.get("parts_total") or 1),
            error=str(response.get("error") or response.get("reason") or ""),
            created_at=float(context.get("created_at") or time.time()),
            business_day=str(context.get("business_day") or ""),
            expires_at=float(context.get("expires_at") or 0.0),
        )
        await self._emit(
            progress,
            taken=0,
            accepted=0,
            socket_written=0,
            write_failed=0,
            unknown=int(bool(response.get("unknown"))),
            expired=0,
            response_success=int(success and not response.get("unknown")),
            response_error=int(not success and not response.get("unknown")),
        )

    async def _expire(self, record: SpoolRecord, reason: str) -> None:
        try:
            self.spool.expire_record(record)
        except Exception as exc:
            await self._emit(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, "write_failed", 0, record.part_count_hint,
                f"expiry checkpoint failed: {type(exc).__name__}: {exc}",
                created_at=record.created_at, message=record.message,
                business_day=record.business_day, expires_at=record.expires_at,
            ))
            return
        await self._emit(WorkerProgress(
            record.event_id, record.partition_id, record.ordinal,
            record.campaign_type, record.campaign_id, record.route_id,
            record.normalized_msisdn, "expired", 0, record.part_count_hint,
            reason, created_at=record.created_at, message=record.message,
            business_day=record.business_day, expires_at=record.expires_at,
        ))

    async def _process(self, record: SpoolRecord) -> SpoolRecord | None:
        self.refresh_campaign_states(force=True)
        key = (record.campaign_type, record.campaign_id)
        if key in self._paused_campaigns and key not in self._cancelled_campaigns:
            # Do not checkpoint a record that became paused while it was in a
            # bounded prefetch buffer.  The next polling cycle will discover
            # it again from the durable spool.
            return None
        if not self.spool.is_record_current(record):
            await self._expire(record, "business day cutoff reached before dispatch")
            return None
        if self.journal_admission and not self.journal_admission(record):
            # A result journal at its hard limit is a safe admission stop, not
            # a reason to write a PDU whose outcome could not be recorded.
            return record
        # Binding is a connection-lifecycle operation, not a dispatch fence.
        # Acquire a physical route slot before advancing the durable cursor so
        # a worker that is waiting behind pool_size can never lose a record.
        transport = await self._transport(record.route_id)
        if transport is None:
            return record
        connect = getattr(transport, "connect", None)
        if connect:
            try:
                result = connect()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                # The record remains before the spool cursor.  The caller
                # blocks this partition for this pass and retries it from the
                # durable cursor on the next pass.
                return record
        if not self.spool.is_record_current(record):
            await self._expire(record, "business day cutoff reached after bind")
            return None
        # Persist dispatch_taken before any socket call.  A crash after this
        # point is intentionally at-most-once-after-dispatch.
        try:
            self.spool.checkpoint(record.partition_id, record.ordinal)
        except Exception as exc:
            await self._emit(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, "write_failed", 0, record.part_count_hint,
                f"dispatch checkpoint failed: {type(exc).__name__}: {exc}",
                message=record.message,
            ))
            return None
        if self.profiler:
            self.profiler.inc("task_taken")
            self.profiler.mark("last_task_taken_monotonic")
            self.profiler.publish_if_due()
        self.refresh_campaign_states(force=True)
        if key in self._paused_campaigns and key not in self._cancelled_campaigns:
            return
        if not self.spool.is_record_current(record):
            await self._expire(record, "business day cutoff reached before socket write")
            return None
        if key in self._cancelled_campaigns:
            await self._emit(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, "cancelled", 0, record.part_count_hint,
                "campaign cancelled", message=record.message,
            ))
            return None
        lock = self._number_locks.setdefault(record.normalized_msisdn, asyncio.Lock())
        async with lock:
            try:
                kwargs = {
                    "destination_addr": record.normalized_msisdn,
                    "message": record.message,
                    "flash_sms": record.flash_sms,
                    "reference": (record.ordinal % 255) or 1,
                    "source_addr": record.source_addr or None,
                    "before_write": lambda: self.spool.is_record_current(record),
                    "progress_context": {
                        "event_id": record.event_id,
                        "partition_id": record.partition_id,
                        "ordinal": record.ordinal,
                        "campaign_type": record.campaign_type,
                        "campaign_id": record.campaign_id,
                        "route_id": record.route_id,
                        "normalized_msisdn": record.normalized_msisdn,
                        "parts_total": record.part_count_hint,
                        "created_at": record.created_at,
                        "business_day": record.business_day,
                        "expires_at": record.expires_at,
                    },
                }
                send_task = transport.send_task
                try:
                    parameters = inspect.signature(send_task).parameters
                    accepts_kwargs = any(
                        item.kind is inspect.Parameter.VAR_KEYWORD
                        for item in parameters.values()
                    )
                except (TypeError, ValueError):
                    parameters = {}
                    accepts_kwargs = True
                if not accepts_kwargs:
                    for optional_name in ("before_write", "progress_context"):
                        if optional_name not in parameters:
                            kwargs.pop(optional_name, None)
                result = await send_task(**kwargs)
            except Exception as exc:
                result = SocketWriteResult(
                    "unknown", 0, max(1, record.part_count_hint),
                    f"{type(exc).__name__}: {exc}",
                )
            await self._emit(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, result.state, result.parts_written,
                result.parts_total, result.error, created_at=record.created_at,
                message=record.message, business_day=record.business_day,
                expires_at=record.expires_at,
            ))
        return None

    def _seed_buffers(self):
        iterators = {partition: iter(self.spool.iter_from(partition, self.spool.cursor(partition))) for partition in self.partitions}
        buffers: dict[tuple[int, str, int, int], deque[SpoolRecord]] = defaultdict(deque)
        counts: dict[int, int] = defaultdict(int)
        exhausted: set[int] = set()
        group_order: list[tuple[int, str, int, int]] = []

        def fill() -> None:
            for partition in self.partitions:
                if partition in exhausted:
                    continue
                while counts[partition] < self.prefetch_per_partition:
                    try:
                        record = next(iterators[partition])
                    except StopIteration:
                        exhausted.add(partition)
                        break
                    group = (partition, record.campaign_type, record.campaign_id, record.route_id)
                    if group not in buffers:
                        group_order.append(group)
                    buffers[group].append(record)
                    counts[partition] += 1

        return iterators, buffers, counts, exhausted, group_order, fill

    async def close_transports(self) -> None:
        transports = [transport for values in self._transports.values() for transport in values]
        for transport in transports:
            close = getattr(transport, "close", None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result
        self._transports.clear()
        self._transport_cursor.clear()

    def release_reader_caches(self) -> None:
        """Release bounded-pass reader state after all tasks are quiescent."""

        if self._inflight:
            return
        self._number_locks.clear()
        release = getattr(self.spool, "release_reader_cache", None)
        if release:
            release()

    async def drain_transports(self, timeout: float | None = None) -> dict:
        """Wait for the bounded response window before releasing bind leases.

        A completed send pass only proves that bytes crossed the socket.  The
        reader task owns the durable response event, so production callers and
        acceptance harnesses must share this bounded lifecycle step.
        """

        if timeout is None:
            configured = [
                float(config.get("timeout", 10.0) or 10.0)
                for config in self.route_configs.values()
            ]
            timeout = min(60.0, max([1.0, *configured]) + 1.0)
        timeout = max(0.1, float(timeout))
        deadline = time.monotonic() + timeout
        while True:
            snapshots = self.snapshot().get("transports", [])
            pending = sum(int(item.get("window_inflight", 0) or 0) for item in snapshots)
            if pending == 0:
                return {"drained": True, "pending": 0, "timeout_seconds": timeout}
            if time.monotonic() >= deadline:
                return {
                    "drained": False,
                    "pending": pending,
                    "timeout_seconds": timeout,
                }
            await asyncio.sleep(0.01)

    async def run_until_idle(self, *, max_tasks: int | None = None,
                             close_transports: bool = True) -> dict:
        iterators, buffers, counts, exhausted, group_order, fill = self._seed_buffers()
        pending: set[asyncio.Task] = set()
        blocked_partitions: set[int] = set()
        processed = 0
        fill()
        try:
            while not self._stop.is_set() and (group_order or pending) and (max_tasks is None or processed < int(max_tasks)):
                for partition in blocked_partitions:
                    exhausted.add(partition)
                fill()
                eligible_group = None
                for offset in range(len(group_order)):
                    index = (self._group_cursor + offset) % len(group_order)
                    group = group_order[index]
                    queue = buffers[group]
                    if queue and group[0] not in blocked_partitions and self._eligible(queue[0]):
                        eligible_group = group
                        self._group_cursor = (index + 1) % len(group_order)
                        break
                if eligible_group is not None and len(pending) < self.max_inflight:
                    record = buffers[eligible_group].popleft()
                    counts[record.partition_id] -= 1
                    pending.add(asyncio.create_task(self._process(record)))
                    self._inflight = len(pending)
                    processed += 1
                    continue
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    self._inflight = len(pending)
                    for task in done:
                        deferred = task.result()
                        if deferred is not None:
                            group = (
                                deferred.partition_id, deferred.campaign_type,
                                deferred.campaign_id, deferred.route_id,
                            )
                            buffers[group].appendleft(deferred)
                            counts[deferred.partition_id] += 1
                            blocked_partitions.add(deferred.partition_id)
                    continue
                if all(not buffers[group] for group in group_order):
                    fill()
                    if not any(buffers[group] for group in group_order):
                        break
                else:
                    # Every buffered group is paused/cancelled.  Do not busy
                    # loop or advance its cursor; control can resume it later.
                    break
            if pending:
                await asyncio.gather(*pending)
        finally:
            self._inflight = 0
            if close_transports:
                await self.close_transports()
        return self.snapshot()

    def snapshot(self) -> dict:
        counters = self.metrics.snapshot()["counters"]
        return {
            "worker_id": self.worker_id,
            "partitions": list(self.partitions),
            "counters": counters,
            "progress_events": len(self._progress_events),
            "inflight": int(self._inflight),
            "transports": [
                transport.snapshot()
                for values in self._transports.values()
                for transport in values
                if hasattr(transport, "snapshot")
            ],
        }
