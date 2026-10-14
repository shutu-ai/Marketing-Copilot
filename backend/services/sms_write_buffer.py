"""Bounded asynchronous write-behind buffer for the hot SMS delivery path."""

import logging
import os
import heapq
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from collections import Counter
from typing import Any, Optional

from database import is_transient_write_error, write_transaction
from services import result_journal_service

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = max(100, min(1000, int(os.environ.get("MC_SMS_WRITE_BATCH_SIZE", "1000"))))
# Keep the operator-facing collection batch independent from the actual
# transaction size.  A large collection window improves batching, while a
# smaller transaction bounds the time for which DuckDB's single writer gate is
# held and lets other short writes make progress between chunks.
TRANSACTION_BATCH_SIZE = max(
    50, min(500, int(os.environ.get("MC_SMS_WRITE_TRANSACTION_SIZE", "250")))
)
FLUSH_INTERVAL = max(0.05, min(0.2, float(os.environ.get("MC_SMS_WRITE_FLUSH_MS", "100")) / 1000.0))
QUEUE_CAPACITY = max(MAX_BATCH_SIZE * 2, int(os.environ.get("MC_SMS_WRITE_QUEUE_CAPACITY", "100000")))


@dataclass
class WriteItem:
    kind: str
    payload: dict[str, Any]
    event_id: str = ""
    journal_path: str = ""
    sequence: int = 0
    attempts: int = 0
    next_attempt_at: float = 0.0
    created_at: float = field(default_factory=time.time)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[BaseException] = None
    db_committed: bool = False
    metrics_recorded: bool = False
    terminal_state: str = ""
    terminal_error: str = ""


_KIND_QUEUE = {
    "send_intent": "task_status",
    "part_result": "smpp_response",
    "submitted": "smpp_response",
    # Fast Send coalesces many normal successes into one journal event and
    # one bounded database transaction.  Failures still use the ordinary
    # task-status path and remain individually durable.
    "fast_checkpoint": "smpp_response",
    "send_failure": "task_status",
    "message_id": "message_id",
    "receipt": "dlr",
}
_queues: dict[str, queue.Queue[WriteItem]] = {
    name: queue.Queue(maxsize=QUEUE_CAPACITY)
    for name in ("smpp_response", "task_status", "message_id", "dlr")
}
_queue_names = tuple(_queues)
_available = threading.Semaphore(0)
_guard = threading.RLock()
_stop = threading.Event()
_thread: Optional[threading.Thread] = None
_next_queue = 0
_sequence = 0
_pending: dict[str, WriteItem] = {}
_quarantined: dict[str, str] = {}
_retry_heap: list[tuple[float, int, WriteItem]] = []
_pending_condition = threading.Condition(_guard)
_loaded_paths: set[str] = set()
MAX_RETRIES = max(3, min(100, int(os.environ.get("MC_SMS_WRITE_MAX_RETRIES", "10"))))
try:
    TRANSIENT_LOG_INTERVAL = max(
        5.0, float(os.environ.get("MC_SMS_WRITE_TRANSIENT_LOG_INTERVAL_SECONDS", "60"))
    )
except (TypeError, ValueError):
    TRANSIENT_LOG_INTERVAL = 60.0
_last_transient_log_at = 0.0
_transient_log_suppressed = 0
_last_ack_log_at = 0.0
_ack_log_suppressed = 0


def _observe_result_stage(stage: str, started: float, item_count: int = 0) -> None:
    """Record non-sensitive result-batch stage timings."""
    try:
        from services import metrics_service
        metrics_service.observe_histogram(
            "marketing_copilot_sms_result_stage_seconds",
            max(0.0, time.perf_counter() - started),
            labels={"stage": str(stage)},
        )
        metrics_service.observe_histogram(
            "marketing_copilot_sms_result_stage_items",
            max(0, int(item_count or 0)),
            labels={"stage": str(stage)},
            buckets=(1, 10, 50, 100, 250, 500, 1000, 5000),
        )
    except Exception:
        logger.debug("Failed to record SMS result batch stage metrics", exc_info=True)


class _PartialCommitError(RuntimeError):
    """A later transaction chunk failed after earlier chunks committed."""

    def __init__(self, error: BaseException, committed_items: list[WriteItem]):
        self.error = error
        self.committed_items = list(committed_items)
        super().__init__(str(error))


def start() -> None:
    global _thread
    with _guard:
        if _thread and _thread.is_alive():
            return
        result_journal_service.start()
        state = result_journal_service.load_state()
        journal_path = state["path"]
        if journal_path not in _loaded_paths:
            for record in state["pending"]:
                _restore_event(record, journal_path)
            for item in state["quarantined"]:
                event_id = str(item["event"].get("event_id") or "")
                _quarantined[event_id] = str(item["terminal"].get("error") or "quarantined")
            _loaded_paths.add(journal_path)
        _stop.clear()
        _thread = threading.Thread(target=_run, name="sms-db-write-buffer", daemon=True)
        _thread.start()


def _next_sequence() -> int:
    global _sequence
    _sequence += 1
    return _sequence


def _restore_event(record: dict[str, Any], journal_path: str) -> None:
    event_id = str(record.get("event_id") or "")
    kind = str(record.get("kind") or "")
    if not event_id or kind not in _KIND_QUEUE or event_id in _pending:
        return
    item = WriteItem(
        kind=kind,
        payload=dict(record.get("payload") or {}),
        event_id=event_id,
        journal_path=journal_path,
        sequence=_next_sequence(),
        created_at=float(record.get("created_at") or time.time()),
    )
    _pending[event_id] = item
    try:
        _queues[_KIND_QUEUE[kind]].put_nowait(item)
        _available.release()
    except queue.Full:
        item.next_attempt_at = time.monotonic() + FLUSH_INTERVAL
        heapq.heappush(_retry_heap, (item.next_attempt_at, item.sequence, item))


def submit(kind: str, payload: dict[str, Any], wait: bool = False,
           timeout: float = 10.0) -> str:
    start()
    queue_name = _KIND_QUEUE.get(kind)
    if queue_name is None:
        raise ValueError(f"unknown SMS buffered write kind: {kind}")
    event_id, journal_path = result_journal_service.append_event(
        kind, payload, timeout=timeout
    )
    with _pending_condition:
        item = WriteItem(
            kind=kind, payload=payload, event_id=event_id,
            journal_path=journal_path, sequence=_next_sequence(),
        )
        _pending[event_id] = item
        try:
            _queues[queue_name].put(item, timeout=timeout)
            _available.release()
        except queue.Full:
            # The event is already durable. Keep it in the retry heap so a
            # temporary in-memory capacity shortage cannot lose the outcome.
            item.next_attempt_at = time.monotonic() + FLUSH_INTERVAL
            heapq.heappush(_retry_heap, (item.next_attempt_at, item.sequence, item))
            _pending_condition.notify_all()
    try:
        from services import metrics_service
        metrics_service.inc("marketing_copilot_sms_write_enqueued_total", labels={"kind": kind})
    except Exception:
        pass
    if wait:
        if not item.done.wait(timeout):
            raise TimeoutError(f"SMS write buffer timed out for {kind}")
        if item.error:
            raise item.error
    return event_id


def flush(timeout: float = 10.0) -> None:
    start()
    deadline = time.monotonic() + timeout
    with _pending_condition:
        target_sequence = _sequence
        while any(item.sequence <= target_sequence for item in _pending.values()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("SMS write buffer flush timed out")
            _pending_condition.wait(min(remaining, 0.1))
        failed = [
            f"{event_id}:{error}" for event_id, error in _quarantined.items()
            if event_id
        ]
        if failed:
            raise RuntimeError(
                "SMS write buffer contains quarantined durable events: "
                + "; ".join(failed[:5])
            )


def stop(timeout: float = 10.0) -> None:
    global _thread
    with _guard:
        thread = _thread
    if not thread:
        return
    flush_error = None
    try:
        flush(timeout)
    except BaseException as exc:
        flush_error = exc
        logger.exception("SMS write buffer stopped with unresolved durable events")
    _stop.set()
    _available.release()
    thread.join(timeout=timeout)
    with _guard:
        if _thread is thread and not thread.is_alive():
            _thread = None
    result_journal_service.stop(timeout)
    if flush_error:
        raise flush_error


def status() -> dict[str, Any]:
    thread = _thread
    memory_queue_depths = {name: target.qsize() for name, target in _queues.items()}
    with _guard:
        pending_items = list(_pending.values())
        pending_counts = Counter(_KIND_QUEUE[item.kind] for item in pending_items)
        queue_depths = {name: int(pending_counts.get(name, 0)) for name in _queues}
        oldest = min((item.created_at for item in pending_items), default=0.0)
        pending_durable = len(pending_items)
        retry_depth = len(_retry_heap)
        quarantined = len(_quarantined)
    return {
        "running": bool(thread and thread.is_alive()),
        # Durable pending includes queued, currently committing, and delayed
        # retries.  It is the operational backlog; qsize alone can transiently
        # show zero while a large batch is still inside DuckDB.
        "queue_depth": pending_durable,
        "queue_depths": queue_depths,
        "memory_queue_depths": memory_queue_depths,
        "queue_capacity": QUEUE_CAPACITY,
        "batch_size": MAX_BATCH_SIZE,
        "transaction_batch_size": TRANSACTION_BATCH_SIZE,
        "flush_interval_ms": round(FLUSH_INTERVAL * 1000),
        "pending_durable": pending_durable,
        "pending_oldest_seconds": max(0.0, time.time() - oldest) if oldest else 0.0,
        "retry_depth": retry_depth,
        "quarantined": quarantined,
        "journal": result_journal_service.status(),
    }


def _commit(items: list[WriteItem]) -> list[WriteItem]:
    actionable = list(items)
    if not actionable:
        return []

    committed = []
    for offset in range(0, len(actionable), TRANSACTION_BATCH_SIZE):
        chunk = actionable[offset:offset + TRANSACTION_BATCH_SIZE]
        started = time.monotonic()
        try:
            _commit_transaction_batch(chunk)
            for item in chunk:
                item.db_committed = True
            committed.extend(chunk)
        except BaseException as exc:
            _record_batch_metrics([], time.monotonic() - started, exc)
            if committed:
                raise _PartialCommitError(exc, committed) from exc
            raise
        _record_batch_metrics(chunk, time.monotonic() - started, None)
    return committed


def _commit_transaction_batch(actionable: list[WriteItem]) -> None:
    if not actionable:
        return

    def commit_batch(conn):
        from services import sms_task_service
        values = ",".join("(?,?,?,CURRENT_TIMESTAMP)" for _ in actionable)
        params = []
        for item in actionable:
            params.extend([
                item.event_id,
                item.kind,
                int(item.payload.get("task_id") or
                    (item.payload.get("task") or {}).get("id") or 0),
            ])
        stage_started = time.perf_counter()
        inserted = conn.execute(
            f"""INSERT INTO sms_result_events(event_id,kind,task_id,created_at)
                VALUES {values} ON CONFLICT(event_id) DO NOTHING
                RETURNING event_id""",
            params,
        ).fetchall()
        _observe_result_stage("event_id_fence_insert", stage_started, len(actionable))
        inserted_ids = {str(row["event_id"]) for row in inserted}
        fresh = [item for item in actionable if item.event_id in inserted_ids]
        if fresh:
            stage_started = time.perf_counter()
            sms_task_service.apply_buffered_writes(conn, fresh)
            _observe_result_stage("apply_buffered_writes", stage_started, len(fresh))

    write_transaction(
        commit_batch, attempts=3, operation="sms_result_batch", lock_timeout=10.0
    )


def _mark_committed(items: list[WriteItem], timeout: float = 10.0) -> None:
    by_path: dict[str, list[str]] = {}
    for item in items:
        by_path.setdefault(item.journal_path, []).append(item.event_id)
    for journal_path, event_ids in by_path.items():
        result_journal_service.append_terminal(
            event_ids, "committed", journal_path, timeout=timeout
        )
    with _pending_condition:
        for item in items:
            _pending.pop(item.event_id, None)
            item.error = None
            item.done.set()
        _pending_condition.notify_all()


def _quarantine(items: list[WriteItem], error: BaseException,
                timeout: float = 10.0) -> None:
    message = str(error)[:2000]
    by_path: dict[str, list[str]] = {}
    for item in items:
        by_path.setdefault(item.journal_path, []).append(item.event_id)
    for journal_path, event_ids in by_path.items():
        result_journal_service.append_terminal(
            event_ids, "quarantined", journal_path, error=message, timeout=timeout
        )
    with _pending_condition:
        for item in items:
            _pending.pop(item.event_id, None)
            _quarantined[item.event_id] = message
            item.error = error
            item.done.set()
        _pending_condition.notify_all()


def _schedule_retry(items: list[WriteItem]) -> None:
    with _pending_condition:
        for item in items:
            item.attempts += 1
            delay = min(30.0, 0.05 * (2 ** min(item.attempts - 1, 9)))
            item.next_attempt_at = time.monotonic() + delay
            heapq.heappush(_retry_heap, (item.next_attempt_at, item.sequence, item))
        _pending_condition.notify_all()


def _release_due_retries() -> None:
    now = time.monotonic()
    with _pending_condition:
        while _retry_heap and _retry_heap[0][0] <= now:
            _when, _seq, item = heapq.heappop(_retry_heap)
            if item.event_id not in _pending:
                continue
            try:
                _queues[_KIND_QUEUE[item.kind]].put_nowait(item)
                _available.release()
            except queue.Full:
                item.next_attempt_at = now + FLUSH_INTERVAL
                heapq.heappush(
                    _retry_heap, (item.next_attempt_at, item.sequence, item)
                )
                break


def _record_batch_metrics(items: list[WriteItem], duration: float,
                         error: Optional[BaseException]) -> None:
    actionable = list(items)
    try:
        from collections import Counter
        from services import metrics_service

        if error is not None:
            metrics_service.inc("marketing_copilot_sms_write_batches_failed_total")
        newly_committed = [
            item for item in actionable if item.db_committed and not item.metrics_recorded
        ]
        if not newly_committed:
            return
        # Called once immediately after each successful transaction chunk.
        # All batch counts, durations and item observations share that unit.
        metrics_service.inc(
            "marketing_copilot_sms_write_batches_committed_total",
        )
        metrics_service.observe_histogram("marketing_copilot_sms_write_batch_seconds", duration)
        metrics_service.observe_histogram(
            "marketing_copilot_sms_write_batch_items", len(newly_committed),
            buckets=(1, 10, 50, 100, 250, 500, 1000),
        )
        for kind, count in Counter(item.kind for item in newly_committed).items():
            metrics_service.inc(
                "marketing_copilot_sms_write_committed_total",
                amount=count,
                labels={"kind": kind},
            )
        for item in newly_committed:
            item.metrics_recorded = True
    except Exception:
        logger.debug("Failed to record SMS write batch metrics", exc_info=True)


def _log_batch_failure(error: BaseException, retry_items: list[WriteItem]) -> None:
    """Keep transient contention visible without flooding the application log."""
    global _last_transient_log_at, _transient_log_suppressed
    if not is_transient_write_error(error):
        logger.exception(
            "SMS buffered database batch failed permanently size=%s next_attempt=%s",
            len(retry_items), max((item.attempts for item in retry_items), default=0) + 1,
        )
        return
    now = time.monotonic()
    suppressed = 0
    if now - _last_transient_log_at >= TRANSIENT_LOG_INTERVAL:
        suppressed = _transient_log_suppressed
        _transient_log_suppressed = 0
        _last_transient_log_at = now
        logger.warning(
            "SMS buffered database batch deferred after transient DuckDB contention "
            "size=%s next_attempt=%s suppressed_since_last=%s error=%s",
            len(retry_items), max((item.attempts for item in retry_items), default=0) + 1,
            suppressed, error,
        )
    else:
        _transient_log_suppressed += 1


def _log_journal_ack_failure(error: Exception, items: list[WriteItem]) -> None:
    """Rate-limit journal terminal-marker errors; their DB commit is already durable."""
    global _last_ack_log_at, _ack_log_suppressed
    now = time.monotonic()
    if now - _last_ack_log_at < TRANSIENT_LOG_INTERVAL:
        _ack_log_suppressed += 1
        return
    suppressed = _ack_log_suppressed
    _ack_log_suppressed = 0
    _last_ack_log_at = now
    logger.error(
        "SMS result journal acknowledgement deferred size=%s suppressed_since_last=%s error=%s",
        len(items), suppressed, error,
    )


def _ack_terminal_items(items: list[WriteItem]) -> None:
    """Group terminal markers and spend at most one second waiting per pass.

    A stalled journal must not turn a 1000-item collection into 1000 sequential
    fsync waits. On failure defer the remaining groups, letting fresh database
    work and shutdown make progress. Event-id fencing already protects the
    committed database changes while their terminal markers are pending.
    """
    groups = {}
    for item in items:
        if item.terminal_state:
            key = (item.journal_path, item.terminal_state, item.terminal_error)
            groups.setdefault(key, []).append(item)
    batches = list(groups.values())
    deadline = time.monotonic() + 1.0
    for index, batch in enumerate(batches):
        remaining = deadline - time.monotonic()
        if _stop.is_set() or remaining <= 0:
            _schedule_retry([item for group in batches[index:] for item in group])
            return
        try:
            if batch[0].terminal_state == "committed":
                _mark_committed(batch, timeout=remaining)
            else:
                _quarantine(batch, RuntimeError(batch[0].terminal_error), timeout=remaining)
        except Exception as exc:
            _log_journal_ack_failure(exc, batch)
            try:
                from services import metrics_service
                metrics_service.inc("marketing_copilot_sms_write_journal_ack_deferred_total")
            except Exception:
                pass
            _schedule_retry([item for group in batches[index:] for item in group])
            return


def _run() -> None:
    global _next_queue
    # Pending events are already durable in result_journal_service.  Once stop
    # is requested, finish the current commit and exit; unresolved items must
    # not keep the daemon thread alive forever after a prolonged DB outage.
    # They will be replayed on the next start.
    while not _stop.is_set():
        _release_due_retries()
        if not _available.acquire(timeout=FLUSH_INTERVAL):
            continue
        items = []
        deadline = time.monotonic() + FLUSH_INTERVAL
        while len(items) < MAX_BATCH_SIZE:
            item = None
            for offset in range(len(_queue_names)):
                index = (_next_queue + offset) % len(_queue_names)
                target = _queues[_queue_names[index]]
                try:
                    item = target.get_nowait()
                    _next_queue = (index + 1) % len(_queue_names)
                    break
                except queue.Empty:
                    continue
            if item is not None:
                items.append(item)
            remaining = deadline - time.monotonic()
            if len(items) >= MAX_BATCH_SIZE or remaining <= 0:
                break
            if not _available.acquire(timeout=remaining):
                break
        if not items:
            continue
        work_items = [item for item in items if not item.terminal_state]
        if not work_items:
            _ack_terminal_items(items)
            for item in items:
                _queues[_KIND_QUEUE[item.kind]].task_done()
            continue
        committed_items = []
        try:
            committed_items = _commit(work_items) or []
        except BaseException as exc:
            error = getattr(exc, "error", exc)
            committed_items = list(getattr(exc, "committed_items", committed_items))
            for item in committed_items:
                item.terminal_state = "committed"
            committed_ids = {item.event_id for item in committed_items}
            retry_items = [item for item in work_items if item.event_id not in committed_ids]
            _log_batch_failure(error, retry_items)
            # A lock timeout is a transient availability condition, not a
            # terminal data error.  Keep it in the durable journal and retry
            # indefinitely with a bounded delay.  Only non-transient errors
            # are quarantined after MAX_RETRIES.
            transient = is_transient_write_error(exc)
            if transient:
                try:
                    from services import metrics_service
                    metrics_service.inc("marketing_copilot_sms_write_transient_deferred_total")
                except Exception:
                    pass
            if retry_items:
                if not transient and max(item.attempts for item in retry_items) + 1 >= MAX_RETRIES:
                    for item in retry_items:
                        item.terminal_state = "quarantined"
                        item.terminal_error = str(error)[:2000]
                else:
                    _schedule_retry(retry_items)
        else:
            for item in committed_items:
                item.terminal_state = "committed"
        _ack_terminal_items(items)
        for item in items:
            _queues[_KIND_QUEUE[item.kind]].task_done()
