"""Bounded, fair shard workers that never write the application DuckDB."""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Awaitable, Callable

from .critical_path_profiler import CriticalPathProfiler
from .metrics import VNextMetrics
from .progress import (
    ProgressPersistence,
    ProgressPersistenceBlocked,
    ProgressReservation,
)
from .projection_writer import ProgressJournal
from .route_credits import RouteCreditBroker
from .smpp_transport import AsyncSmppTransport, SocketWriteResult, count_parts
from .spool import SpoolRecord, TaskSpool


_ROUTE_RECONNECT_FIELDS = frozenset({
    "host", "port", "system_id", "password", "source_addr", "bind_type",
    "address_ton", "address_npi", "data_coding", "pool_size", "window_size",
    "enquire_interval", "timeout", "reconnect_max_seconds",
    "delivery_receipt_mode", "_worker_bind_target",
})


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
    language: str = ""

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
            "language": self.language,
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
                  profiler: CriticalPathProfiler | None = None,
                  progress_store: ProgressPersistence | None = None):
        self.worker_id = int(worker_id)
        self.partitions = tuple(int(item) for item in partitions)
        self.spool = spool
        self.route_configs = {str(key): dict(value) for key, value in route_configs.items()}
        self.credits = credits or RouteCreditBroker()
        self.transport_factory = transport_factory or (lambda config, broker: AsyncSmppTransport(config, credits=broker))
        self.progress_sink = progress_sink
        self.control_loader = control_loader
        self.journal_admission = journal_admission
        self.progress_store = progress_store
        self._expiry_waiters = {}
        self._expiry_waiters_lock = threading.Lock()
        if self.progress_store is not None:
            self.progress_store.add_durability_listener(self._expiry_durable)
        self.max_inflight = max(1, int(max_inflight))
        self.prefetch_per_partition = max(1, int(prefetch_per_partition))
        self.metrics = metrics or VNextMetrics()
        self.profiler = profiler
        self._stop = asyncio.Event()
        self._paused_campaigns: set[tuple[str, int]] = set()
        self._cancelled_campaigns: set[tuple[str, int]] = set()
        self._campaign_generations: dict[tuple[str, int], int] = {}
        self._transports: dict[str, list[object]] = {}
        self._transports_lock = threading.Lock()
        self._campaign_lock = threading.Lock()
        self._active_by_campaign: Counter = Counter()
        self._campaign_reservations: dict[tuple[str, int], list[ProgressReservation]] = defaultdict(list)
        self._transport_creation_locks: dict[str, asyncio.Lock] = {}
        self._transport_cursor: dict[str, int] = {}
        self._number_locks: dict[str, asyncio.Lock] = {}
        self._progress_events: deque[dict] = deque(maxlen=10_000)
        self._campaign_events: dict[tuple[str, int], Counter] = {}
        self._campaign_events_at: dict[tuple[str, int], float] = {}
        self._progress_submit_failures = 0
        self._progress_lost_events = 0
        self._last_progress_error = ""
        self._last_admission_block = ""
        self._checkpoint_count = 0
        self._control_counters = Counter()
        self._group_cursor = 0
        self._last_control_refresh = 0.0
        self._applied_control_version = -1
        self._idle_after_control_version = -1
        self._inflight = 0
        self._active_by_route: Counter[str] = Counter()
        self._route_reconfigure_pending: set[str] = set()
        self._route_reconfigure_task: asyncio.Task | None = None
        self._route_reconfigure_generation: Counter[str] = Counter()
        self._route_reconfigure_error = ""
        self._route_rebind_required: set[str] = set()

    def stop(self) -> None:
        self._stop.set()

    def set_campaign_state(self, campaign_type: str, campaign_id: int, state: str,
                           generation: int | None = None) -> None:
        key = (str(campaign_type), int(campaign_id))
        normalized = str(state).lower()
        with self._campaign_lock:
            if generation is not None:
                self._campaign_generations[key] = int(generation)
            was_paused = key in self._paused_campaigns
            was_cancelled = key in self._cancelled_campaigns
            if normalized in {"pause", "paused"}:
                self._paused_campaigns.add(key)
                if not was_paused:
                    self._control_counters["pause"] += 1
            elif normalized in {"running", "resume", "resumed"}:
                self._paused_campaigns.discard(key)
                self._cancelled_campaigns.discard(key)
                if was_paused:
                    self._control_counters["resume"] += 1
            if normalized in {"cancel", "cancelled", "stopped"}:
                self._paused_campaigns.add(key)
                self._cancelled_campaigns.add(key)
                if not was_cancelled:
                    self._control_counters["cancel"] += 1

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
        version = None
        if isinstance(values, dict):
            version = int(values.get("version", 0))
            values = values.get("campaigns", [])
        with self._campaign_lock:
            self._paused_campaigns.clear()
            self._cancelled_campaigns.clear()
            self._campaign_generations.clear()
        for item in values:
            try:
                generation = item.get("generation")
                self.set_campaign_state(
                    item["campaign_type"], int(item["campaign_id"]), item["state"],
                    None if generation is None else int(generation),
                )
            except (KeyError, TypeError, ValueError):
                continue
        if version is not None:
            self._applied_control_version = version

    def apply_route_rates(self, route_rates: dict) -> dict:
        """Update safe route rate settings without recreating SMPP binds."""

        updated_routes: list[str] = []
        updated_transports = 0
        for raw_route_id, values in (route_rates or {}).items():
            key = str(raw_route_id)
            config = self.route_configs.get(key)
            if config is None or not isinstance(values, dict):
                continue
            scope = str(values.get("tps_scope", config.get("tps_scope", "route_shared")))
            scope = scope.strip().lower()
            if scope not in {"route_shared", "per_bind"}:
                continue
            try:
                tps = max(0.0, float(values.get("tps", config.get("tps", 0.0)) or 0.0))
            except (TypeError, ValueError):
                continue
            config["tps"] = tps
            config["tps_scope"] = scope
            updated_routes.append(key)
            with self._transports_lock:
                transports = list(self._transports.get(key, ()))
            for transport in transports:
                updater = getattr(transport, "update_rate_limit", None)
                if updater is None:
                    continue
                updater(tps, scope)
                updated_transports += 1
        return {
            "updated_routes": sorted(set(updated_routes)),
            "updated_transports": int(updated_transports),
        }

    def apply_route_configs(self, route_configs: dict) -> dict:
        """Apply a complete route snapshot without restarting other routes."""

        incoming: dict[str, dict] = {}
        for raw_key, original in (route_configs or {}).items():
            if not isinstance(original, dict):
                continue
            key = str(raw_key)
            config = dict(original)
            config.setdefault("route_id", int(config.get("route_id", key)))
            config["_worker_bind_target"] = max(
                1,
                int(
                    config.get(
                        "_worker_bind_target",
                        config.get("pool_size", 1),
                    ) or 1
                ),
            )
            incoming[key] = config

        previous = self.route_configs
        reconnect_routes: set[str] = set()
        rate_updates: dict[str, dict] = {}
        for key in set(previous) | set(incoming):
            old = previous.get(key)
            new = incoming.get(key)
            if old is None or new is None:
                if old is not None:
                    reconnect_routes.add(key)
                continue
            if any(old.get(field) != new.get(field) for field in _ROUTE_RECONNECT_FIELDS):
                reconnect_routes.add(key)
            if (
                old.get("tps") != new.get("tps")
                or old.get("tps_scope") != new.get("tps_scope")
            ):
                rate_updates[key] = new

        self.route_configs = incoming
        for key, config in rate_updates.items():
            try:
                tps = max(0.0, float(config.get("tps", 0.0) or 0.0))
                scope = str(config.get("tps_scope", "per_bind"))
            except (TypeError, ValueError):
                continue
            with self._transports_lock:
                transports = list(self._transports.get(key, ()))
            for transport in transports:
                updater = getattr(transport, "update_rate_limit", None)
                if updater is not None:
                    updater(tps, scope)

        self._route_reconfigure_pending.update(reconnect_routes)
        for key in reconnect_routes:
            self._route_reconfigure_generation[key] += 1
        return {
            "updated_routes": sorted(incoming),
            "removed_routes": sorted(set(previous) - set(incoming)),
            "reconfigure_routes": sorted(reconnect_routes),
            "updated_transports": sum(
                len(self._transports.get(key, ())) for key in rate_updates
            ),
        }

    def schedule_route_reconfigure(self) -> None:
        """Start the route-local drain/rebind task from the event-loop thread."""

        if not self._route_reconfigure_pending:
            return
        if self._route_reconfigure_task and not self._route_reconfigure_task.done():
            return
        self._route_reconfigure_task = asyncio.create_task(
            self._reconfigure_routes(), name=f"vnext-worker-{self.worker_id}-route-reconfigure",
        )

    async def _close_route_transports(self, route_key: str) -> None:
        with self._transports_lock:
            transports = list(self._transports.pop(route_key, ()))
            self._transport_cursor.pop(route_key, None)
        for transport in transports:
            close = getattr(transport, "close", None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result

    async def _reconfigure_routes(self) -> None:
        try:
            while self._route_reconfigure_pending:
                routes = set(self._route_reconfigure_pending)
                deadline = time.monotonic() + 30.0
                while (
                    any(self._active_by_route.get(route, 0) for route in routes)
                    and time.monotonic() < deadline
                ):
                    await asyncio.sleep(0.01)
                while time.monotonic() < deadline:
                    with self._transports_lock:
                        transports = [item for route in routes for item in self._transports.get(route, ())]
                    snapshots = [item.inflight_snapshot() for item in transports if hasattr(item, "inflight_snapshot")]
                    if not any(int(item.get("window_inflight", 0) or 0) or int(item.get("response_callback_pending", 0) or 0) for item in snapshots):
                        break
                    await asyncio.sleep(0.01)
                for route in routes:
                    generation = self._route_reconfigure_generation[route]
                    with self._transports_lock:
                        was_connected = any(bool(getattr(item, "connected", False)) for item in self._transports.get(route, ()))
                    if was_connected:
                        self._route_rebind_required.add(route)
                    # A timed-out send is closed at-most-once; its durable
                    # progress path records the resulting unknown/write state.
                    await self._close_route_transports(route)
                    if route in self._route_rebind_required and route in self.route_configs:
                        try:
                            transport = await self._transport(int(route))
                            await transport.connect()
                            self._route_reconfigure_error = ""
                        except Exception as exc:
                            self._route_reconfigure_error = f"route {route}: {type(exc).__name__}: {exc}"
                            await asyncio.sleep(1.0)
                            continue
                    if generation == self._route_reconfigure_generation[route]:
                        self._route_reconfigure_pending.discard(route)
                        self._route_rebind_required.discard(route)
        except Exception as exc:
            self._route_reconfigure_error = f"reconfigure failed: {type(exc).__name__}: {exc}"
        finally:
            self._route_reconfigure_task = None

    async def wait_route_reconfigure(self) -> None:
        self.schedule_route_reconfigure()
        task = self._route_reconfigure_task
        if task is not None:
            await task

    def _route_inflight_limit(self, route_id: int) -> int:
        config = self.route_configs.get(str(int(route_id)))
        if not config:
            return self.max_inflight
        try:
            binds = max(
                1,
                int(
                    config.get(
                        "_worker_bind_target",
                        config.get("pool_size", 1),
                    ) or 1
                ),
            )
            window = max(1, int(config.get("window_size", 1) or 1))
        except (TypeError, ValueError):
            return self.max_inflight
        return max(1, min(self.max_inflight, binds * window))

    def _eligible(self, record: SpoolRecord) -> bool:
        key = (record.campaign_type, record.campaign_id)
        return (self._is_stale_generation(record)
                or key in self._cancelled_campaigns or key not in self._paused_campaigns
                or not self.spool.is_record_current(record))

    def _settlement_bypasses_dispatch_window(self, record: SpoolRecord) -> bool:
        """Let terminal/stale records settle even when an earlier gap remains."""

        key = (record.campaign_type, record.campaign_id)
        return (
            self._is_stale_generation(record)
            or key in self._cancelled_campaigns
            or not self.spool.is_record_current(record)
        )

    def _is_stale_generation(self, record: SpoolRecord) -> bool:
        key = (record.campaign_type, record.campaign_id)
        with self._campaign_lock:
            generation = self._campaign_generations.get(key)
        return generation is not None and int(record.campaign_generation) < int(generation)

    async def _transport(self, route_id: int):
        key = str(int(route_id))
        config = self.route_configs.get(key)
        if not config:
            return None
        lock = self._transport_creation_locks.setdefault(key, asyncio.Lock())
        async with lock:
            with self._transports_lock:
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
                    set_response_sink(
                        self._response_event_nowait
                        if self.progress_store is not None
                        else self._response_event_legacy
                    )
                set_profiler = getattr(value, "set_profiler", None)
                if set_profiler:
                    set_profiler(self.profiler)
                with self._transports_lock:
                    transports.append(value)
            cursor = self._transport_cursor.get(key, 0)
            value = transports[cursor % len(transports)]
            self._transport_cursor[key] = cursor + 1
            return value

    async def prewarm_transports(
        self, route_ids: list[int | str] | tuple[int | str, ...], *, worker_count: int
    ) -> list[dict]:
        """Establish the assigned SMPP binds before the first business record.

        A route's physical pool is shared across worker processes.  Assigning
        the first ``pool_size`` workers a prewarm slot prevents every worker
        from racing for a single-bind backup route while still making the
        configured pool visible and warm at service start.
        """

        count = max(1, int(worker_count))
        results: list[dict] = []
        for raw_route_id in route_ids:
            key = str(raw_route_id)
            config = self.route_configs.get(key)
            if not config:
                continue
            pool_size = max(1, int(config.get("pool_size", 1) or 1))
            if self.worker_id >= min(pool_size, count):
                continue
            target = max(1, (pool_size + count - 1) // count)
            for _ in range(target):
                transport = await self._transport(int(key))
                if transport is None:
                    continue
                connect = getattr(transport, "connect", None)
                if connect is None:
                    continue
                try:
                    result = connect()
                    if inspect.isawaitable(result):
                        await result
                    results.append({"route_id": int(key), "connected": True})
                except Exception as exc:
                    # Keep the worker alive.  The maintenance pass will retry
                    # with the transport's bounded reconnect backoff, and a
                    # later business record can still trigger an on-demand
                    # connection if the SMSC is temporarily unavailable.
                    results.append({
                        "route_id": int(key),
                        "connected": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    })
        return results

    async def maintain_transports(self) -> list[dict]:
        """Reconnect existing bind objects after an SMSC-side disconnect."""

        with self._transports_lock:
            transports = [
                transport for values in self._transports.values() for transport in values
            ]
        results: list[dict] = []
        for transport in transports:
            connect = getattr(transport, "connect", None)
            if connect is None:
                continue
            connected = getattr(transport, "connected", None)
            if connected is None and hasattr(transport, "snapshot"):
                try:
                    connected = bool(transport.snapshot().get("connected"))
                except Exception:
                    connected = False
            if connected:
                continue
            try:
                result = connect()
                if inspect.isawaitable(result):
                    await result
                results.append({
                    "route_id": str(getattr(transport, "route_id", "")),
                    "connected": True,
                })
            except Exception as exc:
                results.append({
                    "route_id": str(getattr(transport, "route_id", "")),
                    "connected": False,
                    "error": f"{type(exc).__name__}: {exc}",
                })
        return results

    def _build_event(self, progress: WorkerProgress, **overrides) -> dict:
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
        # ``created_at`` is the source-record timestamp.  Keep an explicit
        # observation timestamp for rate windows and operational audit; these
        # are different concepts and must not be conflated.
        event["event_at"] = float(event.get("event_at") or time.time())
        event["error"] = str(event.get("error") or "")[:512]
        return event

    def _record_progress_event(self, event: dict) -> None:
        self._progress_events.append(event)
        self.metrics.record(event)
        key = (
            str(event.get("campaign_type") or ""),
            int(event.get("campaign_id") or 0),
        )
        with self._campaign_lock:
            counters = self._campaign_events.get(key)
            if counters is None:
                if len(self._campaign_events) >= 256:
                    oldest = min(
                        self._campaign_events_at.items(),
                        key=lambda item: float(item[1]),
                    )[0]
                    self._campaign_events.pop(oldest, None)
                    self._campaign_events_at.pop(oldest, None)
                counters = Counter()
                self._campaign_events[key] = counters
            counters["events"] += 1
            if event.get("accepted"):
                counters["accepted_tasks"] += 1
            if event.get("taken"):
                counters["tasks_taken"] += 1
            state = str(event.get("state") or "")
            if state == "socket_written":
                counters["socket_written_events"] += 1
                counters["socket_written_pdus"] += max(
                    0, int(event.get("parts_written", 0) or 0)
                )
            elif state == "write_failed":
                counters["write_failed"] += 1
            elif state == "unknown":
                counters["unknown"] += 1
            elif state == "submit_sm_resp_success":
                counters["response_success"] += 1
            elif state == "submit_sm_resp_error":
                counters["response_error"] += 1
            elif state == "cancelled":
                counters["cancelled"] += 1
            elif state == "expired":
                counters["expired"] += 1
            self._campaign_events_at[key] = time.time()
        if self.profiler:
            self.profiler.inc("progress_events")
            self.profiler.inc(f"progress_state_{state}")
            if state.startswith("submit_sm_resp_") or state == "unknown":
                self.profiler.inc("response_events_emitted")

    def _submit_progress(
        self, event: dict, reservation: ProgressReservation | None
    ) -> bool:
        if self.progress_store is None:
            return False
        if self.progress_store.submit(event, reservation):
            self._last_admission_block = ""
            return True
        self._progress_submit_failures += 1
        self._progress_lost_events += 1
        snapshot = self.progress_store.snapshot()
        self._last_progress_error = str(
            snapshot.get("blocked_reason")
            or snapshot.get("last_error")
            or "progress persistence rejected an event"
        )
        return False

    async def _emit(
        self, progress: WorkerProgress,
        reservation: ProgressReservation | None = None, **overrides,
    ) -> None:
        event = self._build_event(progress, **overrides)
        self._record_progress_event(event)
        if self.progress_store is not None:
            self._submit_progress(event, reservation)
            if self.profiler:
                self.profiler.publish_if_due()
            return
        if not self.progress_sink:
            if self.profiler:
                self.profiler.publish_if_due()
            return
        result = self.progress_sink(event)
        if inspect.isawaitable(result):
            await result
        if self.profiler:
            self.profiler.publish_if_due()

    def _emit_nowait(
        self, progress: WorkerProgress,
        reservation: ProgressReservation | None = None, **overrides,
    ) -> bool:
        event = self._build_event(progress, **overrides)
        self._record_progress_event(event)
        if self.progress_store is not None:
            return self._submit_progress(event, reservation)
        return False

    async def _emit_result(
        self, progress: WorkerProgress,
        reservation: ProgressReservation | None = None, **overrides,
    ) -> bool:
        """Emit a terminal task event through the configured sink."""

        if self.progress_store is None:
            await self._emit(progress, reservation=reservation, **overrides)
            return True
        return self._emit_nowait(progress, reservation=reservation, **overrides)

    @staticmethod
    def _release_claim(reservation: object | None) -> None:
        release = getattr(reservation, "release_claim", None)
        if release:
            try:
                release()
            except Exception:
                pass

    def _response_progress(self, response: dict) -> WorkerProgress | None:
        context = dict(response.get("context") or {})
        event_id = str(context.get("event_id") or "")
        if not event_id:
            self._release_claim(response.get("progress_reservation"))
            return None
        if response.get("unknown"):
            state = "unknown"
        else:
            state = "submit_sm_resp_success" if bool(response.get("success")) else "submit_sm_resp_error"
        if self.profiler:
            self.profiler.inc("response_callback_events")
        return WorkerProgress(
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
            language=str(context.get("language") or ""),
        )

    def _response_event_nowait(self, response: dict) -> None:
        progress = self._response_progress(response)
        if progress is None:
            return
        success = bool(response.get("success"))
        accepted = self._emit_nowait(
            progress,
            reservation=response.get("progress_reservation"),
            taken=0, accepted=0, socket_written=0, write_failed=0,
            unknown=int(bool(response.get("unknown"))), expired=0,
            response_success=int(success and not response.get("unknown")),
            response_error=int(not success and not response.get("unknown")),
        )
        if not accepted:
            # The event never entered the writer queue; return the claim so a
            # later close does not wait for a response slot that cannot arrive.
            self._release_claim(response.get("progress_reservation"))

    async def _response_event_legacy(self, response: dict) -> None:
        progress = self._response_progress(response)
        if progress is None:
            return
        success = bool(response.get("success"))
        await self._emit(
            progress,
            taken=0, accepted=0, socket_written=0, write_failed=0,
            unknown=int(bool(response.get("unknown"))), expired=0,
            response_success=int(success and not response.get("unknown")),
            response_error=int(not success and not response.get("unknown")),
        )

    async def _expire(
        self, record: SpoolRecord, reason: str,
        reservation: ProgressReservation | None = None,
        state: str = "expired",
    ) -> bool:
        progress = WorkerProgress(
            record.event_id, record.partition_id, record.ordinal,
            record.campaign_type, record.campaign_id, record.route_id,
            record.normalized_msisdn, state, 0, record.part_count_hint,
            reason, created_at=record.created_at, message=record.message,
            business_day=record.business_day, expires_at=record.expires_at,
            language=record.language,
        )
        waiter = None
        created = False
        if self.progress_store is not None:
            with self._expiry_waiters_lock:
                waiter = self._expiry_waiters.get(record.event_id)
                if waiter is None:
                    waiter = concurrent.futures.Future()
                    self._expiry_waiters[record.event_id] = waiter
                    created = True
        if self.progress_store is None or created:
            if not await self._emit_result(progress, reservation=reservation):
                if waiter is not None:
                    with self._expiry_waiters_lock:
                        self._expiry_waiters.pop(record.event_id, None)
                return False
        if self.progress_store is not None:
            try:
                await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(waiter)), timeout=10.0)
            except asyncio.TimeoutError:
                return False
        self.spool.expire_record(record)
        if waiter is not None:
            with self._expiry_waiters_lock:
                if self._expiry_waiters.get(record.event_id) is waiter:
                    self._expiry_waiters.pop(record.event_id, None)
        return True

    def _expiry_durable(self, events: list[dict]) -> None:
        with self._expiry_waiters_lock:
            for event in events:
                if event.get("state") not in {"expired", "cancelled"}:
                    continue
                waiter = self._expiry_waiters.get(event.get("event_id"))
                if waiter is not None and not waiter.done():
                    waiter.set_result(True)

    @staticmethod
    def _progress_templates(
        record: SpoolRecord, parts_total: int,
    ) -> tuple[WorkerProgress, WorkerProgress]:
        send = WorkerProgress(
            record.event_id, record.partition_id, record.ordinal,
            record.campaign_type, record.campaign_id, record.route_id,
            record.normalized_msisdn, "socket_written",
            parts_total, parts_total, "x" * 512,
            created_at=record.created_at, message=record.message,
            business_day=record.business_day, expires_at=record.expires_at,
            language=record.language,
        )
        response = WorkerProgress(
            f"{record.event_id}:resp:2147483647",
            record.partition_id, record.ordinal,
            record.campaign_type, record.campaign_id, record.route_id,
            record.normalized_msisdn, "submit_sm_resp_success",
            0, parts_total, "x" * 512,
            created_at=record.created_at, business_day=record.business_day,
            expires_at=record.expires_at,
            language=record.language,
        )
        return send, response

    def _reserve_for_record(
        self, record: SpoolRecord, parts_total: int,
    ) -> ProgressReservation | None:
        if self.progress_store is None:
            return None
        send, response = self._progress_templates(record, parts_total)
        send_event = self._build_event(send)
        response_event = self._build_event(
            response, taken=0, accepted=0, socket_written=0,
            write_failed=0, unknown=0, expired=0,
            response_success=1, response_error=0,
        )
        per_slot = max(
            len(ProgressJournal._encode(send_event)),
            len(ProgressJournal._encode(response_event)),
        ) + 4096
        items = max(1, int(parts_total)) + 1
        return self.progress_store.reserve(
            items=items, bytes_total=per_slot * items,
        )

    async def _process(self, record: SpoolRecord) -> SpoolRecord | None:
        # The dispatch loop already refreshes controls before scheduling.  Keep
        # this pass throttled; the forced fence immediately before checkpoint
        # remains the last-word check before any socket write.
        self.refresh_campaign_states()
        key = (record.campaign_type, record.campaign_id)
        if self._is_stale_generation(record):
            return None if await self._expire(
                record, "campaign generation superseded", state="cancelled",
            ) else record
        if (key in self._paused_campaigns and key not in self._cancelled_campaigns
                and self.spool.is_record_current(record)):
            # Do not checkpoint a record that became paused while it was in a
            # bounded prefetch buffer.  The next polling cycle will discover
            # it again from the durable spool.
            return None
        if not self.spool.is_record_current(record):
            if self.progress_store is not None:
                progress = self.progress_store.snapshot()
                if not bool(progress.get("accepting", True)):
                    self._last_admission_block = str(
                        progress.get("blocked_reason")
                        or "progress persistence is not accepting new work"
                    )
                    # Do not advance an expired record's cursor when its
                    # terminal event cannot enter the durable journal.
                    return record
            return None if await self._expire(record, "business day cutoff reached before dispatch") else record
        if key in self._cancelled_campaigns:
            return None if await self._expire(record, "campaign cancelled", state="cancelled") else record
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
        if self._is_stale_generation(record):
            return None if await self._expire(
                record, "campaign generation superseded", state="cancelled",
            ) else record
        if not self.spool.is_record_current(record):
            return None if await self._expire(record, "business day cutoff reached after bind") else record
        parts_total = count_parts(record.message)
        try:
            reservation = self._reserve_for_record(record, parts_total)
            if reservation is not None:
                with self._campaign_lock:
                    self._campaign_reservations[key].append(reservation)
            self._last_admission_block = ""
        except ProgressPersistenceBlocked as exc:
            # Fail closed before dispatch_taken: no socket write without a
            # reserved progress slot and journal headroom.
            self._last_admission_block = str(exc)
            return record
        if self.progress_store is None and self.journal_admission and not self.journal_admission(record):
            # Legacy callers retain the original conservative fence.
            return record
        self.refresh_campaign_states(force=True)
        if key in self._paused_campaigns and key not in self._cancelled_campaigns:
            # No checkpoint happened yet, so the durable spool can safely
            # redeliver this record after resume.
            if reservation is not None:
                reservation.release_unwritten(reservation.slots_open)
            return None
        if key in self._cancelled_campaigns or self._is_stale_generation(record):
            settled = await self._expire(record, "campaign cancelled", reservation, state="cancelled")
            if reservation is not None:
                reservation.release_unwritten(reservation.slots_open)
            return None if settled else record
        # Persist dispatch_taken before any socket call.  A crash after this
        # point is intentionally at-most-once-after-dispatch.
        try:
            self.spool.checkpoint(record.partition_id, record.ordinal)
            with self._campaign_lock:
                self._checkpoint_count += 1
        except Exception as exc:
            await self._emit_result(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, "write_failed", 0, parts_total,
                f"dispatch checkpoint failed: {type(exc).__name__}: {exc}",
                message=record.message, language=record.language,
            ), reservation=reservation)
            if reservation is not None:
                reservation.release_unwritten(reservation.slots_open)
            return None
        if self.profiler:
            self.profiler.inc("task_taken")
            self.profiler.mark("last_task_taken_monotonic")
            self.profiler.publish_if_due()
        lock = self._number_locks.setdefault(record.normalized_msisdn, asyncio.Lock())
        async with lock:
            try:
                cutoff_grace_deadline = 0.0
                try:
                    # shortcut: a bounded 5s maximum grace, make configurable
                    # only if measured multipart sends need a longer window.
                    cutoff_grace_seconds = max(
                        0.5,
                        min(5.0, float(getattr(transport, "timeout", 5.0) or 5.0)),
                    )
                except (TypeError, ValueError):
                    cutoff_grace_seconds = 5.0

                def before_write(parts_written: int = 0) -> bool:
                    nonlocal cutoff_grace_deadline

                    current = self.spool.is_record_current(record)
                    self.refresh_campaign_states()
                    stopped = (
                        key in self._cancelled_campaigns
                        or key in self._paused_campaigns
                        or self._is_stale_generation(record)
                    )
                    if current and not stopped:
                        return True
                    if int(parts_written or 0) <= 0:
                        return False
                    if cutoff_grace_deadline <= 0.0:
                        cutoff_grace_deadline = time.monotonic() + cutoff_grace_seconds
                    return time.monotonic() <= cutoff_grace_deadline

                kwargs = {
                    "destination_addr": record.normalized_msisdn,
                    "message": record.message,
                    "flash_sms": record.flash_sms,
                    "reference": (record.ordinal % 255) or 1,
                    "source_addr": record.source_addr or None,
                    "before_write": before_write,
                    "progress_context": {
                        "event_id": record.event_id,
                        "partition_id": record.partition_id,
                        "ordinal": record.ordinal,
                        "campaign_type": record.campaign_type,
                        "campaign_id": record.campaign_id,
                        "route_id": record.route_id,
                        "normalized_msisdn": record.normalized_msisdn,
                        "parts_total": parts_total,
                        "created_at": record.created_at,
                        "business_day": record.business_day,
                        "expires_at": record.expires_at,
                        "language": record.language,
                    },
                }
                if reservation is not None:
                    kwargs["progress_reservation"] = reservation
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
                    for optional_name in (
                        "before_write", "progress_context",
                        "progress_reservation",
                    ):
                        if optional_name not in parameters:
                            kwargs.pop(optional_name, None)
                result = await send_task(**kwargs)
            except Exception as exc:
                result = SocketWriteResult(
                    "unknown", 0, parts_total,
                    f"{type(exc).__name__}: {exc}",
                )
            if reservation is not None:
                # Parts that never reached the claim boundary cannot produce a
                # response event.  Claimed parts stay reserved until the
                # response or an explicit unknown event arrives.
                reservation.release_unwritten(
                    parts_total - reservation.slots_claimed
                )
            accepted = await self._emit_result(WorkerProgress(
                record.event_id, record.partition_id, record.ordinal,
                record.campaign_type, record.campaign_id, record.route_id,
                record.normalized_msisdn, result.state, result.parts_written,
                result.parts_total, result.error, created_at=record.created_at,
                message=record.message, business_day=record.business_day,
                expires_at=record.expires_at,
                language=record.language,
            ), reservation=reservation)
            if not accepted and reservation is not None:
                reservation.release_unwritten(reservation.slots_open)
        return None

    def _seed_buffers(self):
        iterators = {
            partition: iter(self.spool.iter_from(
                partition, self.spool.cursor(partition), include_expired=True,
            ))
            for partition in self.partitions
        }
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
        with self._transports_lock:
            transports = [
                transport for values in self._transports.values()
                for transport in values
            ]
            self._transports.clear()
            self._transport_cursor.clear()
        for transport in transports:
            close = getattr(transport, "close", None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result

    async def drain_progress(self, timeout: float | None = None) -> dict:
        """Wait for queued progress events to reach the journal fsync."""

        if self.progress_store is None:
            return {"enabled": False, "drained": True, "pending_events": 0}
        return await asyncio.to_thread(
            self.progress_store.drain,
            30.0 if timeout is None else max(0.0, float(timeout)),
        )

    async def close_progress(self, timeout: float | None = None) -> dict:
        """Flush and stop the per-worker progress writer."""

        if self.progress_store is None:
            return {"enabled": False, "drained": True, "pending_events": 0}
        return await asyncio.to_thread(
            self.progress_store.close,
            30.0 if timeout is None else max(0.0, float(timeout)),
        )

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
            with self._transports_lock:
                transports = [
                    transport for values in self._transports.values()
                    for transport in values
                ]
            snapshots = [
                transport.inflight_snapshot()
                if hasattr(transport, "inflight_snapshot")
                else transport.snapshot()
                for transport in transports
                if hasattr(transport, "snapshot")
            ]
            pending = sum(int(item.get("window_inflight", 0) or 0) for item in snapshots)
            callbacks = sum(
                int(item.get("response_callback_pending", 0) or 0)
                for item in snapshots
            )
            if pending == 0 and callbacks == 0:
                return {
                    "drained": True, "pending": 0,
                    "callback_pending": 0, "timeout_seconds": timeout,
                }
            if time.monotonic() >= deadline:
                return {
                    "drained": False,
                    "pending": pending,
                    "callback_pending": callbacks,
                    "timeout_seconds": timeout,
                }
            await asyncio.sleep(0.01)

    async def run_until_idle(self, *, max_tasks: int | None = None,
                             close_transports: bool = True,
                             control_handler: Callable[[], bool] | None = None) -> dict:
        iterators, buffers, counts, exhausted, group_order, fill = self._seed_buffers()
        pending: set[asyncio.Task] = set()
        pending_routes: dict[asyncio.Task, str] = {}
        pending_campaigns: dict[asyncio.Task, tuple[str, int]] = {}
        blocked_groups: set[tuple[int, str, int, int]] = set()
        processed = 0
        control_requested = False
        fill()
        self._active_by_route.clear()
        try:
            while not self._stop.is_set() and (group_order or pending) and (max_tasks is None or processed < int(max_tasks)):
                if control_handler is not None and control_handler():
                    control_requested = True
                    break
                fill()
                self.refresh_campaign_states()
                eligible_group = None
                for offset in range(len(group_order)):
                    index = (self._group_cursor + offset) % len(group_order)
                    group = group_order[index]
                    queue = buffers[group]
                    route_key = str(group[3])
                    if (
                        queue
                        and group not in blocked_groups
                        and (
                            self.spool.dispatch_window_open(
                                group[0], queue[0].ordinal,
                                max_gap=max(
                                    256, self.max_inflight + self.prefetch_per_partition,
                                ),
                            )
                            or (
                                self._settlement_bypasses_dispatch_window(queue[0])
                                and self.spool.dispatch_window_open(
                                    group[0], queue[0].ordinal,
                                    allow_out_of_order=True,
                                )
                            )
                        )
                        and route_key not in self._route_reconfigure_pending
                        and self._active_by_route[route_key] < self._route_inflight_limit(group[3])
                        and self._eligible(queue[0])
                    ):
                        eligible_group = group
                        self._group_cursor = (index + 1) % len(group_order)
                        break
                if eligible_group is not None and len(pending) < self.max_inflight:
                    record = buffers[eligible_group].popleft()
                    counts[record.partition_id] -= 1
                    task = asyncio.create_task(self._process(record))
                    pending.add(task)
                    campaign_key = (record.campaign_type, record.campaign_id)
                    pending_campaigns[task] = campaign_key
                    with self._campaign_lock:
                        self._active_by_campaign[campaign_key] += 1
                    route_key = str(record.route_id)
                    pending_routes[task] = route_key
                    self._active_by_route[route_key] += 1
                    self._inflight = len(pending)
                    processed += 1
                    continue
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    self._inflight = len(pending)
                    for task in done:
                        campaign_key = pending_campaigns.pop(task)
                        with self._campaign_lock:
                            self._active_by_campaign[campaign_key] -= 1
                            if not self._active_by_campaign[campaign_key]:
                                del self._active_by_campaign[campaign_key]
                        route_key = pending_routes.pop(task, "")
                        if route_key:
                            self._active_by_route[route_key] = max(
                                0, self._active_by_route[route_key] - 1,
                            )
                        deferred = task.result()
                        if deferred is not None:
                            group = (
                                deferred.partition_id, deferred.campaign_type,
                                deferred.campaign_id, deferred.route_id,
                            )
                            buffers[group].appendleft(deferred)
                            counts[deferred.partition_id] += 1
                            blocked_groups.add(group)
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
            if (not self._stop.is_set() and not control_requested
                    and len(exhausted) == len(self.partitions)
                    and not any(queue and (group[1], group[2]) in self._cancelled_campaigns
                                for group, queue in buffers.items())):
                self._idle_after_control_version = self._applied_control_version
            else:
                self._idle_after_control_version = -1
        finally:
            with self._campaign_lock:
                for campaign_key in pending_campaigns.values():
                    self._active_by_campaign[campaign_key] -= 1
                    if not self._active_by_campaign[campaign_key]:
                        del self._active_by_campaign[campaign_key]
            self._inflight = 0
            self._active_by_route.clear()
            if close_transports:
                await self.close_transports()
        return self.snapshot()

    def snapshot(self) -> dict:
        counters = self.metrics.snapshot(include_samples=False)["counters"]
        with self._transports_lock:
            transports = [
                transport for values in self._transports.values()
                for transport in values
            ]
        transport_snapshots = [
            transport.snapshot()
            for transport in transports
            if hasattr(transport, "snapshot")
        ]
        control_snapshot = self._control_snapshot()
        progress = (
            self.progress_store.snapshot()
            if self.progress_store is not None
            else {
                "enabled": False,
                "accepting": True,
                "pending_durability": 0,
                "queue_depth": 0,
                "writer_errors": 0,
            }
        )
        if self.progress_store is not None:
            campaigns = self.progress_store.campaign_snapshot()
            campaign_source = "worker-progress-store"
        else:
            campaigns = []
            with self._campaign_lock:
                campaign_values = sorted(self._campaign_events.items())
            for (campaign_type, campaign_id), values in campaign_values:
                payload = {
                    "campaign_type": campaign_type,
                    "campaign_id": campaign_id,
                }
                payload.update(
                    {name: int(amount) for name, amount in values.items()}
                )
                campaigns.append(payload)
            campaign_source = "worker-memory"
        healthy = bool(
            progress.get("accepting", True)
            and int(progress.get("writer_errors", 0) or 0) == 0
            and int(progress.get("unresolved_events", 0) or 0) == 0
            and self._progress_submit_failures == 0
            and sum(
                int(item.get("response_callback_errors", 0) or 0)
                for item in transport_snapshots
            ) == 0
        )
        return {
            "worker_id": self.worker_id,
            "partitions": list(self.partitions),
            "route_ids": sorted(int(key) for key in self.route_configs),
            "route_pool_size": sum(
                max(1, int(config.get("pool_size", 1) or 1))
                for config in self.route_configs.values()
            ),
            "counters": counters,
            "route_credits": self.credits.snapshot() if hasattr(self.credits, "snapshot") else {},
            "progress_events": len(self._progress_events),
            "inflight": int(self._inflight),
            "progress": progress,
            "campaign_counters": campaigns,
            "campaign_counter_source": campaign_source,
            "progress_submit_failures": int(self._progress_submit_failures),
            "progress_lost_events": int(self._progress_lost_events),
            "last_progress_error": str(self._last_progress_error),
            "admission_blocked_reason": str(self._last_admission_block),
            "route_inflight": {
                key: int(value) for key, value in self._active_by_route.items() if value
            },
            "route_inflight_limits": {
                key: self._route_inflight_limit(int(key)) for key in self.route_configs
            },
            "route_reconfigure_pending": sorted(self._route_reconfigure_pending),
            **control_snapshot,
            "healthy": healthy,
            "transports": transport_snapshots,
        }

    def _control_snapshot(self) -> dict:
        with self._campaign_lock:
            reservations = {}
            for key, values in list(self._campaign_reservations.items()):
                remaining = [value for value in values if value.slots_open]
                if remaining:
                    self._campaign_reservations[key] = remaining
                    reservations[key] = len(remaining)
                else:
                    del self._campaign_reservations[key]
            return {
                "applied_control_version": self._applied_control_version,
                "idle_after_control_version": self._idle_after_control_version,
                "campaign_generations": {
                    f"{key[0]}:{key[1]}": int(value)
                    for key, value in self._campaign_generations.items()
                },
                "campaign_inflight": [
                    {"campaign_type": key[0], "campaign_id": key[1],
                     "active": int(self._active_by_campaign.get(key, 0)),
                     "reservations": int(reservations.get(key, 0))}
                    for key in self._active_by_campaign.keys() | reservations.keys()
                ],
                "checkpoint_count": int(self._checkpoint_count),
                "control_counters": {
                    name: int(self._control_counters.get(name, 0))
                    for name in ("pause", "resume", "cancel")
                },
            }

    def response_callback_errors(self) -> int:
        with self._transports_lock:
            transports = [
                transport for values in self._transports.values()
                for transport in values
            ]
        return sum(
            int((transport.snapshot() or {}).get("response_callback_errors", 0) or 0)
            for transport in transports
            if hasattr(transport, "snapshot")
        )
