"""Bounded asynchronous write-behind buffer for the hot SMS delivery path."""

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from database import get_connection

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = max(100, min(1000, int(os.environ.get("MC_SMS_WRITE_BATCH_SIZE", "250"))))
FLUSH_INTERVAL = max(0.05, min(0.2, float(os.environ.get("MC_SMS_WRITE_FLUSH_MS", "100")) / 1000.0))
QUEUE_CAPACITY = max(MAX_BATCH_SIZE * 2, int(os.environ.get("MC_SMS_WRITE_QUEUE_CAPACITY", "100000")))


@dataclass
class WriteItem:
    kind: str
    payload: dict[str, Any]
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[BaseException] = None


_KIND_QUEUE = {
    "submitted": "smpp_response",
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


def start() -> None:
    global _thread
    with _guard:
        if _thread and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(target=_run, name="sms-db-write-buffer", daemon=True)
        _thread.start()


def submit(kind: str, payload: dict[str, Any], wait: bool = False,
           timeout: float = 10.0) -> None:
    start()
    queue_name = _KIND_QUEUE.get(kind)
    if queue_name is None:
        raise ValueError(f"unknown SMS buffered write kind: {kind}")
    item = WriteItem(kind=kind, payload=payload)
    _queues[queue_name].put(item, timeout=timeout)
    _available.release()
    if wait:
        if not item.done.wait(timeout):
            raise TimeoutError(f"SMS write buffer timed out for {kind}")
        if item.error:
            raise item.error


def flush(timeout: float = 10.0) -> None:
    start()
    deadline = time.monotonic() + timeout
    barriers = []
    for queue_name, target in _queues.items():
        item = WriteItem(kind="barrier", payload={"queue": queue_name})
        target.put(item, timeout=max(0.0, deadline - time.monotonic()))
        _available.release()
        barriers.append(item)
    for item in barriers:
        if not item.done.wait(max(0.0, deadline - time.monotonic())):
            raise TimeoutError("SMS write buffer flush timed out")
        if item.error:
            raise item.error


def stop(timeout: float = 10.0) -> None:
    global _thread
    with _guard:
        thread = _thread
    if not thread:
        return
    flush(timeout)
    _stop.set()
    _available.release()
    thread.join(timeout=timeout)
    with _guard:
        if _thread is thread and not thread.is_alive():
            _thread = None


def status() -> dict[str, Any]:
    thread = _thread
    queue_depths = {name: target.qsize() for name, target in _queues.items()}
    return {
        "running": bool(thread and thread.is_alive()),
        "queue_depth": sum(queue_depths.values()),
        "queue_depths": queue_depths,
        "queue_capacity": QUEUE_CAPACITY,
        "batch_size": MAX_BATCH_SIZE,
        "flush_interval_ms": round(FLUSH_INTERVAL * 1000),
    }


def _commit(items: list[WriteItem]) -> None:
    actionable = [item for item in items if item.kind != "barrier"]
    if not actionable:
        return
    conn = get_connection()
    try:
        from services import sms_task_service
        sms_task_service.apply_buffered_writes(conn, actionable)
        conn.commit()
    except BaseException:
        try:
            conn.conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _run() -> None:
    global _next_queue
    while not _stop.is_set() or any(not target.empty() for target in _queues.values()):
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
        error = None
        for attempt in range(1, 4):
            try:
                _commit(items)
                error = None
                break
            except BaseException as exc:
                error = exc
                logger.exception(
                    "SMS buffered database batch failed size=%s attempt=%s",
                    len(items), attempt,
                )
                if attempt < 3:
                    time.sleep(0.05 * attempt)
        for item in items:
            item.error = error
            item.done.set()
            queue_name = item.payload.get("queue") if item.kind == "barrier" else _KIND_QUEUE[item.kind]
            _queues[queue_name].task_done()
