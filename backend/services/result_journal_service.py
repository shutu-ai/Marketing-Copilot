"""Durable append-only handoff journal for SMPP outcomes.

The SMPP reader must not acknowledge an outcome merely because it entered an
in-memory queue.  This module group-commits small JSON records to a local file
and returns only after ``fsync``.  DuckDB consumers append a terminal marker
after their idempotent transaction commits; uncommitted events are replayed on
startup.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from database import get_database_file

logger = logging.getLogger(__name__)

_FORMAT_VERSION = 1
_FSYNC_INTERVAL = max(
    0.001, min(0.1, float(os.environ.get("MC_SMS_RESULT_JOURNAL_FSYNC_MS", "2")) / 1000.0)
)
_FSYNC_BATCH = max(1, min(2000, int(os.environ.get("MC_SMS_RESULT_JOURNAL_FSYNC_BATCH", "250"))))
_QUEUE_CAPACITY = max(1000, int(os.environ.get("MC_SMS_RESULT_JOURNAL_QUEUE_CAPACITY", "100000")))
_COMPACT_BYTES = max(
    1024 * 1024,
    int(os.environ.get("MC_SMS_RESULT_JOURNAL_COMPACT_BYTES", str(256 * 1024 * 1024))),
)
_COMPACT_INTERVAL = max(
    60.0, float(os.environ.get("MC_SMS_RESULT_JOURNAL_COMPACT_INTERVAL_SECONDS", "300"))
)


class JournalCorruptionError(RuntimeError):
    pass


@dataclass
class _AppendRequest:
    path: str
    records: list[dict[str, Any]]
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[BaseException] = None


_queue: queue.Queue[_AppendRequest] = queue.Queue(maxsize=_QUEUE_CAPACITY)
_guard = threading.RLock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_last_error = ""
_last_fsync_at = 0.0
_last_compaction_at = 0.0
_last_compaction_bytes = 0
_last_compaction_error = ""


def _path() -> str:
    configured = os.environ.get("MC_SMS_RESULT_JOURNAL_PATH", "").strip()
    if configured:
        return os.path.abspath(configured)
    return os.path.abspath(f"{get_database_file()}.sms-results.jsonl")


def _canonical(record: dict[str, Any]) -> bytes:
    return json.dumps(
        record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def _encoded_line(record: dict[str, Any]) -> bytes:
    body = dict(record)
    body["version"] = _FORMAT_VERSION
    checksum = f"{zlib.crc32(_canonical(body)) & 0xffffffff:08x}"
    envelope = {"record": body, "crc32": checksum}
    return _canonical(envelope) + b"\n"


def _verify_line(raw: bytes, line_number: int, journal_path: str) -> dict[str, Any]:
    try:
        envelope = json.loads(raw.decode("utf-8"))
        record = envelope["record"]
        actual = f"{zlib.crc32(_canonical(record)) & 0xffffffff:08x}"
        if actual != envelope.get("crc32"):
            raise ValueError("CRC mismatch")
        if int(record.get("version") or 0) != _FORMAT_VERSION:
            raise ValueError(f"unsupported version {record.get('version')}")
        return record
    except Exception as exc:
        raise JournalCorruptionError(
            f"SMS result journal is corrupt at {journal_path}:{line_number}: {exc}"
        ) from exc


def start() -> None:
    global _thread
    with _guard:
        if _thread and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(target=_writer_loop, name="sms-result-journal", daemon=True)
        _thread.start()


def _append(records: list[dict[str, Any]], journal_path: Optional[str] = None,
            timeout: float = 10.0) -> str:
    start()
    target = os.path.abspath(journal_path or _path())
    request = _AppendRequest(path=target, records=records)
    started = time.perf_counter()
    deadline = time.monotonic() + max(0.0, timeout)
    _queue.put(request, timeout=max(0.0, deadline - time.monotonic()))
    if not request.done.wait(max(0.0, deadline - time.monotonic())):
        raise TimeoutError("SMS result journal fsync timed out")
    if request.error:
        raise request.error
    _observe_journal_metric("append_wait_seconds", time.perf_counter() - started)
    return target


def _observe_journal_metric(stage: str, duration: float, item_count: int = 0) -> None:
    """Record bounded journal timings without making metrics a durability dependency."""
    try:
        from services import metrics_service
        metrics_service.observe_histogram(
            "marketing_copilot_sms_result_stage_seconds",
            duration,
            labels={"stage": f"journal_{stage}"},
        )
        if item_count:
            metrics_service.observe_histogram(
                "marketing_copilot_sms_result_stage_items",
                item_count,
                labels={"stage": f"journal_{stage}"},
                buckets=(1, 10, 50, 100, 250, 500, 1000, 5000),
            )
    except Exception:
        pass


def append_event(kind: str, payload: dict[str, Any], timeout: float = 10.0,
                 event_id: Optional[str] = None) -> tuple[str, str]:
    event_id = event_id or uuid.uuid4().hex
    record = {
        "type": "event",
        "event_id": event_id,
        "kind": str(kind),
        "payload": payload,
        "created_at": time.time(),
    }
    return event_id, _append([record], timeout=timeout)


def append_terminal(event_ids: list[str], state: str, journal_path: str,
                    error: str = "", timeout: float = 10.0) -> None:
    if not event_ids:
        return
    _append([{
        "type": "terminal",
        "event_ids": list(dict.fromkeys(str(value) for value in event_ids)),
        "state": str(state),
        "error": str(error)[:2000],
        "created_at": time.time(),
    }], journal_path=journal_path, timeout=timeout)


def load_state(journal_path: Optional[str] = None) -> dict[str, Any]:
    target = os.path.abspath(journal_path or _path())
    if not os.path.isfile(target):
        return {"path": target, "pending": [], "quarantined": [], "records": 0}
    events: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    terminal: dict[str, dict[str, Any]] = {}
    records = 0
    with open(target, "rb") as handle:
        for line_number, raw in enumerate(handle, 1):
            if not raw.strip():
                continue
            record = _verify_line(raw, line_number, target)
            records += 1
            if record.get("type") == "event":
                event_id = str(record.get("event_id") or "")
                if event_id and event_id not in events:
                    events[event_id] = record
                    order.append(event_id)
            elif record.get("type") == "terminal":
                for event_id in record.get("event_ids") or []:
                    terminal[str(event_id)] = record
    pending = [events[event_id] for event_id in order if event_id not in terminal]
    quarantined = [
        {"event": events[event_id], "terminal": terminal[event_id]}
        for event_id in order
        if event_id in terminal and terminal[event_id].get("state") == "quarantined"
    ]
    return {
        "path": target,
        "pending": pending,
        "quarantined": quarantined,
        "records": records,
    }


def status() -> dict[str, Any]:
    thread = _thread
    target = _path()
    try:
        file_bytes = os.path.getsize(target)
    except OSError:
        file_bytes = 0
    return {
        "running": bool(thread and thread.is_alive()),
        "queue_depth": _queue.qsize(),
        "queue_capacity": _QUEUE_CAPACITY,
        "path": _path(),
        "fsync_interval_ms": round(_FSYNC_INTERVAL * 1000),
        "fsync_batch": _FSYNC_BATCH,
        "file_bytes": file_bytes,
        "healthy": not bool(_last_error or _last_compaction_error),
        "last_error": _last_error,
        "last_fsync_at": _last_fsync_at,
        "compact_bytes": _COMPACT_BYTES,
        "last_compaction_at": _last_compaction_at,
        "last_compaction_bytes": _last_compaction_bytes,
        "last_compaction_error": _last_compaction_error,
    }


def stop(timeout: float = 10.0) -> None:
    global _thread
    with _guard:
        thread = _thread
    if not thread:
        return
    deadline = time.monotonic() + timeout
    while _queue.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.01)
    _stop.set()
    thread.join(timeout=max(0.0, deadline - time.monotonic()))
    with _guard:
        if _thread is thread and not thread.is_alive():
            _thread = None


def _writer_loop() -> None:
    global _last_error, _last_fsync_at
    while not _stop.is_set() or not _queue.empty():
        try:
            first = _queue.get(timeout=0.1)
        except queue.Empty:
            continue
        requests = [first]
        deadline = time.monotonic() + _FSYNC_INTERVAL
        while len(requests) < _FSYNC_BATCH:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                requests.append(_queue.get(timeout=remaining))
            except queue.Empty:
                break
        error: Optional[BaseException] = None
        fsync_started = time.perf_counter()
        try:
            by_path: dict[str, list[dict[str, Any]]] = {}
            for request in requests:
                by_path.setdefault(request.path, []).extend(request.records)
            for target, records in by_path.items():
                Path(target).parent.mkdir(parents=True, exist_ok=True)
                with open(target, "ab", buffering=0) as handle:
                    for record in records:
                        handle.write(_encoded_line(record))
                    os.fsync(handle.fileno())
                _maybe_compact(target)
            _last_error = ""
            _last_fsync_at = time.time()
            _observe_journal_metric(
                "fsync_seconds", time.perf_counter() - fsync_started,
                sum(len(records) for records in by_path.values()),
            )
        except BaseException as exc:
            error = exc
            _last_error = str(exc)[:2000]
            logger.exception("SMS result journal fsync failed")
        finally:
            for request in requests:
                request.error = error
                request.done.set()
                _queue.task_done()


def _maybe_compact(target: str) -> None:
    """Atomically discard committed history while retaining replay state.

    This runs only on the single journal writer thread and after the append is
    fsynced. Requests still waiting in memory are written after replacement,
    so no append can race the compaction.
    """
    global _last_compaction_at, _last_compaction_bytes, _last_compaction_error
    now = time.time()
    if now - _last_compaction_at < _COMPACT_INTERVAL:
        return
    try:
        before = os.path.getsize(target)
    except OSError:
        return
    if before < _COMPACT_BYTES:
        return
    temporary = f"{target}.compact-{os.getpid()}.tmp"
    try:
        state = load_state(target)
        records: list[dict[str, Any]] = list(state["pending"])
        for item in state["quarantined"]:
            records.append(item["event"])
            records.append(item["terminal"])
        with open(temporary, "wb", buffering=0) as handle:
            for record in records:
                handle.write(_encoded_line(record))
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _last_compaction_at = now
        _last_compaction_bytes = before
        _last_compaction_error = ""
        logger.info(
            "SMS result journal compacted path=%s before_bytes=%s after_bytes=%s retained_records=%s",
            target, before, os.path.getsize(target), len(records),
        )
    except BaseException as exc:
        _last_compaction_error = str(exc)[:2000]
        logger.exception("SMS result journal compaction failed path=%s", target)
        try:
            if os.path.isfile(temporary):
                os.unlink(temporary)
        except OSError:
            pass
