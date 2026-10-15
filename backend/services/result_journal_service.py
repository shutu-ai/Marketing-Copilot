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
import sqlite3
import tempfile
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from database import get_database_file
from services import result_capacity_service as capacity

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
    lines: object = None
    credit: object = None
    retain_credit: bool = False
    guard: object = field(default_factory=threading.Lock)


_queue: queue.Queue[_AppendRequest] = queue.Queue(maxsize=_QUEUE_CAPACITY)
_guard = threading.RLock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_last_error = ""
_last_fsync_at = 0.0
_last_compaction_at = 0.0
_last_compaction_bytes = 0
_last_compaction_error = ""
_scan_lock = threading.Lock()
_evidence_lock = threading.Lock()
_terminal_slots = threading.BoundedSemaphore(1)
_scan_peak_line = 0
_disk_limit = int(os.environ.get("MC_RESULT_JOURNAL_DISK_BYTES", 1024 * 1024 * 1024))
_index_work_limit = int(os.environ.get("MC_RESULT_JOURNAL_INDEX_BYTES", str(_disk_limit)))
_terminal_disk_reserve = 16 * 1024 * 1024
# Duplicated deliberately at the Journal boundary: importing the write
# buffer here would create a recovery-cycle import. Unknown kinds are protocol
# corruption, not future events that may be silently skipped.
_DURABLE_KINDS = frozenset({
    "send_intent", "part_result", "submitted", "fast_checkpoint",
    "send_failure", "message_id", "receipt",
})


def preserve_rejected(kind, payload, reason):
    """Bounded emergency evidence; never acknowledge a rejected receipt.

    Its presence is a persistent startup fence. Full/error means refuse the
    input (DLR SYSERR / response listener disconnect), never truncate/ACK it.
    """
    capacity.ledger.fail(reason)
    path = _path() + ".rejected.jsonl"
    with _evidence_lock:
        if os.path.exists(path) and os.path.getsize(path) >= 32 * 1024 * 1024:
            raise capacity.ResultCapacityError("emergency evidence disk full; input refused")
        fd, temporary = tempfile.mkstemp(prefix="result-reject-", dir=str(Path(path).parent))
        try:
            with os.fdopen(fd, "wb") as out:
                total = 0
                # Oversize/malformed payloads must never be re-materialized in
                # the emergency path. The rejection itself is durable,
                # explicit and fail-closed; the original caller retains the
                # full in-memory object until its safe unknown/connection
                # policy handles it.
                summary = {
                    "kind": str(kind), "reason": str(reason)[:2000],
                    "payload_type": type(payload).__name__,
                    "payload_object_bytes": sys.getsizeof(payload),
                    "evidence": "payload_rejected_before_durable_journal",
                }
                for chunk in json.JSONEncoder(ensure_ascii=True, separators=(",", ":"),
                                              default=str).iterencode(summary):
                    raw = chunk.encode("utf-8")
                    total += len(raw)
                    if total > 32 * 1024 * 1024:
                        raise capacity.ResultCapacityError("oversize emergency input refused")
                    out.write(raw)
                out.write(b"\n")
                out.flush()
                os.fsync(out.fileno())
            current = os.path.getsize(path) if os.path.exists(path) else 0
            if current + total + 1 > 32 * 1024 * 1024:
                raise capacity.ResultCapacityError("emergency evidence disk full; input refused")
            with open(path, "ab", buffering=0) as out, open(temporary, "rb") as source:
                while True:
                    raw = source.read(65536)
                    if not raw:
                        break
                    out.write(raw)
                os.fsync(out.fileno())
        finally:
            os.unlink(temporary)


def preserve_rejected_lines(lines, reason):
    """Persist already-framed evidence without decoding/recopying payloads."""
    path = _path() + ".rejected.jsonl"
    with _evidence_lock:
        current = os.path.getsize(path) if os.path.exists(path) else 0
        amount = sum(len(line) for line in (lines or []))
        if current + amount > 32 * 1024 * 1024:
            raise capacity.ResultCapacityError("emergency evidence disk full; input refused")
        with open(path, "ab", buffering=0) as out:
            for line in lines or []:
                out.write(line)
            out.write(_encoded_line({"type": "rejected", "reason": str(reason)[:2000]}))
            os.fsync(out.fileno())
    capacity.ledger.fail(reason)


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
    if capacity.enabled():
        capacity.validate(record)
    body = dict(record)
    body["version"] = _FORMAT_VERSION
    checksum = f"{zlib.crc32(_canonical(body)) & 0xffffffff:08x}"
    envelope = {"record": body, "crc32": checksum}
    return _canonical(envelope) + b"\n"


def _verify_line(raw: bytes, line_number: int, journal_path: str) -> dict[str, Any]:
    try:
        envelope = json.loads(raw.decode("utf-8"))
        record = envelope["record"]
        if capacity.enabled():
            capacity.validate(record)
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
            timeout: float = 10.0, credit=None, retain_credit=False) -> str:
    start()
    target = os.path.abspath(journal_path or _path())
    fair = capacity.enabled()
    terminal = bool(records and records[0].get("type") == "terminal")
    lines = None
    if fair:
        try:
            capacity.validate(records)
            lines = [_encoded_line(record) for record in records]
            size = sum(map(len, lines))
            maximum = capacity.MAX_EVENT
            if records and records[0].get("kind") == "part_result":
                maximum = capacity.MAX_PART_EVENT
            elif records and records[0].get("type") == "event" and credit and credit.parent:
                maximum = capacity.MAX_FINAL_EVENT
            elif terminal:
                maximum = capacity.MAX_FINAL_EVENT
            if size > maximum:
                raise capacity.ResultCapacityError("framed result envelope exceeded")
            if not terminal and credit is None:
                credit = capacity.ledger.event(size)
                if credit is None:
                    raise capacity.ResultCapacityError("result emergency bytes exhausted")
            if terminal:
                credit = capacity.ledger.acquire(capacity.charge(size), "terminal")
                if credit is None:
                    raise capacity.ResultCapacityError("terminal lane bytes busy")
        except BaseException as exc:
            if not terminal:
                preserve_rejected("journal", records, exc)
            raise
    request = _AppendRequest(path=target, records=[] if fair else records)
    # Keep legacy constructor/test compatibility; fair requests hold encoded
    # bytes only. The caller/WriteItem owns the single decoded payload.
    request.lines, request.credit, request.retain_credit = lines, credit, retain_credit
    started = time.perf_counter()
    deadline = time.monotonic() + max(0.0, timeout)
    try:
        _queue.put(request, timeout=max(0.0, deadline - time.monotonic()))
    except BaseException as exc:
        if fair:
            # Queue saturation is a rejected durable handoff, never a silent
            # loss. Keep only the already-framed bounded evidence and fence
            # new fair sends until the operator reconciles it.
            try:
                preserve_rejected_lines(lines, exc)
            except Exception:
                capacity.ledger.fail(exc)
        capacity.ledger.release(credit)
        raise
    capacity.ledger.transfer(credit, "queued")
    if not request.done.wait(max(0.0, deadline - time.monotonic())):
        if fair:
            capacity.ledger.fail("journal fsync timeout; durability uncertain")
            with request.guard:
                request.retain_credit = False
                if request.done.is_set():
                    capacity.ledger.release(credit)
        raise TimeoutError("SMS result journal fsync timed out")
    if request.error:
        if fair and request.lines:
            try:
                preserve_rejected_lines(request.lines, request.error)
            except Exception:
                capacity.ledger.fail(request.error)
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
                 event_id: Optional[str] = None, _credit=None, _handoff=False) -> tuple[str, str]:
    event_id = event_id or uuid.uuid4().hex
    record = {
        "type": "event",
        "event_id": event_id,
        "kind": str(kind),
        "payload": payload,
        "created_at": time.time(),
    }
    if capacity.enabled() and _handoff:
        try:
            parent = capacity.owner_for(payload) if kind != "receipt" else None
            maximum = capacity.MAX_PART_EVENT if kind == "part_result" else (
                capacity.MAX_FINAL_EVENT if parent else capacity.MAX_EVENT)
            aggregate = (kind == "fast_checkpoint"
                         and isinstance(payload.get("items"), list)
                         and len(payload["items"]) > 1)
            _credit = capacity.ledger.event(
                maximum, parent, owner="dispatch" if aggregate else "emergency"
            )
            if _credit is None:
                raise capacity.ResultCapacityError("result emergency bytes exhausted")
            capacity.validate(record)
            size = len(_encoded_line(record))
            if size > maximum:
                raise capacity.ResultCapacityError("framed result envelope exceeded")
            capacity.ledger.resize(_credit, size)
        except BaseException as exc:
            capacity.ledger.release(_credit)
            preserve_rejected(kind, payload, exc)
            raise
    result = event_id, _append([record], timeout=timeout, credit=_credit,
                              retain_credit=_credit is not None)
    return (*result, _credit) if _handoff else result


def append_terminal(event_ids: list[str], state: str, journal_path: str,
                    error: str = "", timeout: float = 10.0) -> None:
    if not event_ids:
        return
    # A separately serialized terminal lane can always settle full event debt.
    # Bound each marker regardless of collection/transaction batch settings.
    if capacity.enabled() and len(event_ids) > 250:
        for offset in range(0, len(event_ids), 250):
            append_terminal(event_ids[offset:offset + 250], state, journal_path, error, timeout)
        return
    record = {
        "type": "terminal",
        "event_ids": list(dict.fromkeys(str(value) for value in event_ids)),
        "state": str(state),
        "error": str(error)[:2000],
        "created_at": time.time(),
    }
    if capacity.enabled():
        if not _terminal_slots.acquire(timeout=timeout):
            raise TimeoutError("terminal lane busy")
        try:
            _append([record], journal_path=journal_path, timeout=timeout)
        finally:
            _terminal_slots.release()
    else:
        _append([record], journal_path=journal_path, timeout=timeout)


class _JournalIndex:
    """Disposable disk index, never an authority or a second DuckDB owner.

    Compatible v1 JSONL remains the authority. SQLite cache is 2 MiB and temp
    sorts spill to disk. Each row is decoded singly. First event / last marker
    semantics exactly match the original reader, including duplicate markers.
    """
    def __init__(self, target):
        self.target = target
        fd, self.name = tempfile.mkstemp(prefix="result-index-", suffix=".sqlite",
                                        dir=str(Path(target).parent))
        os.close(fd)
        self.conn = sqlite3.connect(self.name, check_same_thread=False)
        self.conn.execute("PRAGMA cache_size=-2048")
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.executescript("""
            CREATE TABLE events(id TEXT PRIMARY KEY, ordinal INTEGER, raw BLOB);
            CREATE INDEX event_order ON events(ordinal);
            CREATE TABLE markers(ordinal INTEGER PRIMARY KEY, raw BLOB);
            CREATE TABLE terminal(id TEXT PRIMARY KEY, ordinal INTEGER, state TEXT);
        """)
        self.records = 0
        self.scanned_bytes = 0
        global _scan_peak_line
        try:
            if os.path.isfile(target):
                with open(target, "rb") as handle:
                    # Recovery is a bounded streaming scan. Larger/unsupported
                    # data blocks recovery intact instead of being truncated.
                    line_number = 0
                    while True:
                        raw = handle.readline(capacity.MAX_JSON_BYTES + 1)
                        if not raw:
                            break
                        line_number += 1
                        self.scanned_bytes += len(raw)
                        _scan_peak_line = max(_scan_peak_line, len(raw))
                        if len(raw) > capacity.MAX_JSON_BYTES:
                            raise JournalCorruptionError("historical line exceeds bounded reader envelope; journal retained")
                        if not raw.strip():
                            continue
                        record = _verify_line(raw, line_number, target)
                        self.records += 1
                        if self.records % 256 == 0 and os.path.getsize(self.name) > _index_work_limit:
                            raise JournalCorruptionError(
                                "journal recovery index exceeds bounded work disk; journal retained"
                            )
                        if record.get("type") == "event":
                            event_id = str(record.get("event_id") or "")
                            kind = str(record.get("kind") or "")
                            if capacity.enabled() and (not event_id or kind not in _DURABLE_KINDS):
                                raise JournalCorruptionError(
                                    "malformed or unknown durable event; journal retained"
                                )
                            self.conn.execute("INSERT OR IGNORE INTO events VALUES(?,?,?)",
                                              (event_id, line_number, raw))
                        elif record.get("type") == "terminal":
                            state = str(record.get("state") or "")
                            if capacity.enabled() and state not in {"committed", "quarantined"}:
                                raise JournalCorruptionError(
                                    "unknown terminal state; journal retained"
                                )
                            self.conn.execute("INSERT INTO markers VALUES(?,?)", (line_number, raw))
                            self.conn.executemany("INSERT OR REPLACE INTO terminal VALUES(?,?,?)",
                                ((str(event_id), line_number, str(record.get("state") or ""))
                                 for event_id in record.get("event_ids") or []))
                        else:
                            raise JournalCorruptionError("unknown journal record type; journal retained")
                self.conn.commit()
                if os.path.getsize(self.name) > _index_work_limit:
                    raise JournalCorruptionError(
                        "journal recovery index exceeds bounded work disk; journal retained"
                    )
        except BaseException:
            self.close()
            raise

    def query(self, quarantined=False, count=False):
        where = "t.state='quarantined'" if quarantined else "t.id IS NULL"
        fields = "COUNT(*)" if count else "e.raw,m.raw"
        return self.conn.execute(f"""SELECT {fields} FROM events e
            LEFT JOIN terminal t ON t.id=e.id
            LEFT JOIN markers m ON m.ordinal=t.ordinal
            WHERE {where}""" + ("" if count else " ORDER BY e.ordinal"))

    def close(self):
        conn, self.conn = getattr(self, "conn", None), None
        if conn:
            conn.close()
        name = getattr(self, "name", "")
        if name and os.path.exists(name):
            os.unlink(name)

    def __del__(self):
        self.close()


class _Records:
    def __init__(self, index, quarantined=False):
        self.index, self.quarantined = index, quarantined

    def __len__(self):
        return self.index.query(self.quarantined, count=True).fetchone()[0]

    def __iter__(self):
        for raw, marker in self.index.query(self.quarantined):
            record = _verify_line(raw, 0, self.index.target)
            yield ({"event": record, "terminal": _verify_line(marker, 0, self.index.target)}
                   if self.quarantined else record)

    def __eq__(self, other):
        # Small legacy diagnostics/tests; the pipeline itself always iterates.
        return list(self) == other


def load_state(journal_path: Optional[str] = None) -> dict[str, Any]:
    target = os.path.abspath(journal_path or _path())
    if not os.path.isfile(target):
        return {"path": target, "pending": [], "quarantined": [], "records": 0}
    index = _JournalIndex(target)
    return {"path": target, "pending": _Records(index),
            "quarantined": _Records(index, True), "records": index.records}


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
        "disk_limit_bytes": _disk_limit,
        "index_work_limit_bytes": _index_work_limit,
        "scan_peak_line_bytes": _scan_peak_line,
        "rejected_evidence": os.path.exists(target + ".rejected.jsonl"),
        "capacity": capacity.ledger.status() if capacity.enabled() else {"mode": "legacy"},
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
            by_path = {}
            for request in requests:
                capacity.ledger.transfer(request.credit, "in_writer")
                by_path.setdefault(request.path, []).append(request)
            for target, path_requests in by_path.items():
                Path(target).parent.mkdir(parents=True, exist_ok=True)
                if capacity.enabled() and os.path.exists(target) and os.path.getsize(target) >= _disk_limit:
                    _maybe_compact(target, force=True)
                with open(target, "ab", buffering=0) as handle:
                    for request in path_requests:
                        for line in request.lines if request.lines is not None else (
                                _encoded_line(record) for record in request.records):
                            if capacity.enabled():
                                terminal = request.credit and request.credit.owner == "terminal"
                                ceiling = _disk_limit + (_terminal_disk_reserve if terminal else 0)
                                if handle.tell() + len(line) > ceiling:
                                    raise capacity.ResultCapacityError("result journal disk budget exhausted")
                            handle.write(line)
                    os.fsync(handle.fileno())
                _maybe_compact(target)
            _last_error = ""
            _last_fsync_at = time.time()
            _observe_journal_metric(
                "fsync_seconds", time.perf_counter() - fsync_started,
                len(requests),
            )
        except BaseException as exc:
            error = exc
            _last_error = str(exc)[:2000]
            if capacity.enabled():
                capacity.ledger.fail(exc)
            logger.exception("SMS result journal fsync failed")
        finally:
            for request in requests:
                if error and capacity.enabled() and request.lines:
                    # Keep the request's credit live while bounded evidence is
                    # written. The caller must never observe a released byte
                    # credit while its failed framed payload is still held.
                    try:
                        preserve_rejected_lines(request.lines, error)
                    except Exception:
                        capacity.ledger.fail(error)
                    request.lines = None
                with request.guard:
                    request.error = error
                    if error or not request.retain_credit:
                        capacity.ledger.release(request.credit)
                    else:
                        capacity.ledger.transfer(request.credit, "durable_handoff")
                    if not error:
                        request.lines = None
                    request.done.set()
                _queue.task_done()


def _maybe_compact(target: str, force=False) -> None:
    """Atomically discard committed history while retaining replay state.

    This runs only on the single journal writer thread and after the append is
    fsynced. Requests still waiting in memory are written after replacement,
    so no append can race the compaction.
    """
    global _last_compaction_at, _last_compaction_bytes, _last_compaction_error
    now = time.time()
    if not force and now - _last_compaction_at < _COMPACT_INTERVAL:
        return
    try:
        before = os.path.getsize(target)
    except OSError:
        return
    if not force and before < _COMPACT_BYTES:
        return
    temporary = f"{target}.compact-{os.getpid()}.tmp"
    try:
        state = load_state(target)
        def retained():
            yield from state["pending"]
            for item in state["quarantined"]:
                yield item["event"]
                # Emit only this id: reusing a multi-id original marker after
                # another id's later marker can change last-marker semantics.
                yield {**item["terminal"], "event_ids": [item["event"]["event_id"]]}
        count = 0
        written = 0
        with open(temporary, "wb", buffering=0) as handle:
            for record in retained():
                line = _encoded_line(record)
                written += len(line)
                if capacity.enabled() and written > _disk_limit:
                    raise capacity.ResultCapacityError(
                        "result journal compacted state exceeds disk budget"
                    )
                handle.write(line)
                count += 1
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(str(Path(target).parent))
        _last_compaction_at = now
        _last_compaction_bytes = before
        _last_compaction_error = ""
        logger.info(
            "SMS result journal compacted path=%s before_bytes=%s after_bytes=%s retained_records=%s",
            target, before, os.path.getsize(target), count,
        )
    except BaseException as exc:
        _last_compaction_error = str(exc)[:2000]
        logger.exception("SMS result journal compaction failed path=%s", target)
        try:
            if os.path.isfile(temporary):
                os.unlink(temporary)
        except OSError:
            pass
    finally:
        # The SQLite index is disposable; do not retain its on-disk BLOB copy
        # after the streaming compaction pass completes.
        index = getattr(state.get("pending") if "state" in locals() else None, "index", None)
        if index is not None:
            index.close()


def _fsync_directory(directory):
    if os.name != "nt":
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
