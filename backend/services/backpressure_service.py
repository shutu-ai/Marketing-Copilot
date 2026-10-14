"""Hysteretic single-host capacity guards for ingestion and delivery pipelines."""

import os
import shutil
import threading
import time

from database import get_database_file, read_transaction, write_lock_status


TASK_HIGH = max(1000, int(os.environ.get("MC_BATCH_INGEST_HIGH_WATERMARK", "50000")))
TASK_LOW = max(0, min(TASK_HIGH, int(os.environ.get("MC_BATCH_INGEST_LOW_WATERMARK", "20000"))))
WRITE_HIGH = max(1000, int(os.environ.get("MC_WRITE_QUEUE_HIGH_WATERMARK", "80000")))
WRITE_LOW = max(0, min(WRITE_HIGH, int(os.environ.get("MC_WRITE_QUEUE_LOW_WATERMARK", "30000"))))
CHUNK_HIGH = max(1000, int(os.environ.get("MC_CHUNK_HIGH_WATERMARK", "100000")))
CHUNK_LOW = max(0, min(CHUNK_HIGH, int(os.environ.get("MC_CHUNK_LOW_WATERMARK", "50000"))))
DISK_HIGH = max(50.0, min(99.0, float(os.environ.get("MC_DISK_HIGH_WATERMARK_PCT", "95"))))
DISK_LOW = max(40.0, min(DISK_HIGH, float(os.environ.get("MC_DISK_LOW_WATERMARK_PCT", "90"))))
MIN_FREE_BYTES = max(0, int(float(os.environ.get("MC_DISK_MIN_FREE_GB", "1")) * 1024 ** 3))

_RAMP_ENABLED = str(os.environ.get("MC_REALTIME_INGEST_RAMP_ENABLED", "1")).strip().lower() in {
    "1", "true", "yes", "on",
}
try:
    _RAMP_INITIAL_TPS = max(
        25.0, min(5000.0, float(os.environ.get("MC_REALTIME_INGEST_RAMP_INITIAL_TPS", "250")))
    )
except (TypeError, ValueError):
    _RAMP_INITIAL_TPS = 250.0
try:
    _RAMP_MAX_TPS = max(
        _RAMP_INITIAL_TPS,
        min(10000.0, float(os.environ.get("MC_REALTIME_INGEST_RAMP_MAX_TPS", "2000"))),
    )
except (TypeError, ValueError):
    _RAMP_MAX_TPS = max(_RAMP_INITIAL_TPS, 2000.0)
try:
    _RAMP_MIN_TPS = max(
        5.0, min(_RAMP_INITIAL_TPS, float(os.environ.get("MC_REALTIME_INGEST_RAMP_MIN_TPS", "25")))
    )
except (TypeError, ValueError):
    _RAMP_MIN_TPS = 25.0
try:
    _RAMP_BURST_SECONDS = max(
        0.1, min(5.0, float(os.environ.get("MC_REALTIME_INGEST_RAMP_BURST_SECONDS", "1")))
    )
except (TypeError, ValueError):
    _RAMP_BURST_SECONDS = 1.0

_lock = threading.Lock()
_paused = {"tasks": False, "writes": False, "chunks": False, "disk": False,
           "results": False}
_depth_cache = {"at": 0.0, "database": "", "tasks": 0, "chunks": 0}
_disk_cache = {"at": 0.0, "database": "", "value": None}
_ramp_lock = threading.Lock()
_ramp = {
    "rate": _RAMP_INITIAL_TPS,
    "tokens": 0.0,
    "last_at": time.monotonic(),
    "last_resume_at": 0.0,
    "paused": False,
    "seen_task_pause": False,
    "admitted": 0,
    "throttled": 0,
}


def _database_depths() -> tuple[int, int]:
    now = time.monotonic()
    database_path = os.path.abspath(get_database_file())
    with _lock:
        if _depth_cache["database"] == database_path and now - _depth_cache["at"] < 1.0:
            return int(_depth_cache["tasks"]), int(_depth_cache["chunks"])
    tasks, chunks = read_transaction(
        lambda conn: (
            conn.execute(
                """SELECT COUNT(*) count FROM sms_tasks
                   WHERE status IN ('queued','retrying','sending')"""
            ).fetchone()["count"],
            conn.execute(
                "SELECT COUNT(*) count FROM ingestion_chunks WHERE status IN ('pending','failed')"
            ).fetchone()["count"],
        ),
        operation="backpressure.depths",
    )
    with _lock:
        _depth_cache.update({"at": now, "database": database_path,
                             "tasks": int(tasks), "chunks": int(chunks)})
    return int(tasks), int(chunks)


def _task_depth() -> int:
    return _database_depths()[0]


def _chunk_depth() -> int:
    return _database_depths()[1]


def _disk_state() -> dict:
    target = os.path.dirname(os.path.abspath(get_database_file())) or os.getcwd()
    now = time.monotonic()
    with _lock:
        if (_disk_cache["database"] == target and _disk_cache["value"] is not None
                and now - _disk_cache["at"] < 1.0):
            return dict(_disk_cache["value"])
    usage = shutil.disk_usage(target)
    used_pct = (usage.used / usage.total * 100.0) if usage.total else 100.0
    value = {"path": target, "used_pct": round(used_pct, 2), "free_bytes": usage.free}
    with _lock:
        _disk_cache.update({"at": now, "database": target, "value": value})
    return dict(value)


def snapshot(include_task_depth: bool = True) -> dict:
    from services import sms_write_buffer

    writes = sms_write_buffer.status()
    disk = _disk_state()
    task_depth, chunk_depth = _database_depths()
    values = {
        "task_depth": task_depth if include_task_depth else None,
        "chunk_depth": chunk_depth,
        "write_depth": int(writes["queue_depth"]),
        "write_depths": writes.get("queue_depths", {}),
        "disk": disk,
        "result_journal": writes.get("journal", {}),
        "write_retry_depth": int(writes.get("retry_depth", 0)),
        "write_quarantined": int(writes.get("quarantined", 0)),
        "write_oldest_seconds": float(writes.get("pending_oldest_seconds", 0.0)),
    }
    with _lock:
        values["paused"] = dict(_paused)
    values["ingestion_ramp"] = _ramp_snapshot()
    return values


def _ramp_snapshot() -> dict:
    with _ramp_lock:
        return {
            "enabled": _RAMP_ENABLED,
            "rate_tps": round(float(_ramp["rate"]), 3),
            "tokens": round(float(_ramp["tokens"]), 3),
            "paused": bool(_ramp["paused"]),
            "seen_task_pause": bool(_ramp["seen_task_pause"]),
            "last_resume_at": float(_ramp["last_resume_at"]),
            "admitted": int(_ramp["admitted"]),
            "throttled": int(_ramp["throttled"]),
        }


def _ingestion_pressure(state: dict) -> float:
    """Return a bounded pressure score for recovery throttling only.

    The hysteretic high/low watermarks remain the hard stop.  This score only
    slows a resumed stream when the result writer or process writer gate is
    still showing pressure, and degrades to zero when no runtime signal exists.
    """
    score = 0.0
    write_depth = float(state.get("write_depth") or 0)
    if WRITE_LOW > 0 and write_depth > WRITE_LOW:
        score = max(
            score,
            min(2.0, 0.5 + (write_depth - WRITE_LOW) / float(WRITE_LOW)),
        )
    oldest = float(state.get("write_oldest_seconds") or 0.0)
    if oldest > 1.0:
        score = max(score, min(2.0, oldest / 5.0))
    retry_depth = float(state.get("write_retry_depth") or 0)
    if retry_depth:
        score = max(score, min(2.0, 1.0 + retry_depth / max(1.0, WRITE_LOW)))
    try:
        lock = write_lock_status()
        if lock.get("held"):
            score = max(score, min(2.0, float(lock.get("age_seconds") or 0.0) / 2.0))
    except Exception:
        pass
    return score


def _ramp_admit(work_units: int, state: dict, stop_event=None, interval: float = 0.25) -> bool:
    """Admit a chunk through a shared recovery token bucket.

    The bucket is activated only after the existing task high-watermark pause
    has been observed.  Normal startup and steady-state ingestion therefore
    keep the historical behavior, while all workers share one recovery budget.
    """
    units = max(1, int(work_units or 0))
    if not _RAMP_ENABLED:
        return stop_event is None or not stop_event.is_set()
    with _ramp_lock:
        if _ramp["paused"]:
            _ramp["paused"] = False
            _ramp["rate"] = _RAMP_INITIAL_TPS
            _ramp["tokens"] = 0.0
            _ramp["last_resume_at"] = time.monotonic()
            _ramp["last_at"] = _ramp["last_resume_at"]
        if not _ramp["seen_task_pause"]:
            return stop_event is None or not stop_event.is_set()

    while True:
        active, current = reasons(producing_chunks=True)
        if active:
            with _ramp_lock:
                if current.get("paused", {}).get("tasks"):
                    _ramp["paused"] = True
                    _ramp["seen_task_pause"] = True
                    _ramp["rate"] = _RAMP_INITIAL_TPS
                    _ramp["tokens"] = 0.0
            if stop_event is not None and stop_event.is_set():
                return False
            if hasattr(stop_event, "wait") and stop_event is not None:
                stop_event.wait(max(0.02, min(0.25, interval)))
            else:
                time.sleep(max(0.02, min(0.25, interval)))
            continue

        now = time.monotonic()
        pressure = _ingestion_pressure(current)
        with _ramp_lock:
            elapsed = max(0.0, now - float(_ramp["last_at"]))
            _ramp["last_at"] = now
            rate = float(_ramp["rate"])
            _ramp["tokens"] = min(
                max(units, rate * _RAMP_BURST_SECONDS),
                float(_ramp["tokens"]) + elapsed * rate,
            )
            if pressure > 1.0:
                rate = max(_RAMP_MIN_TPS, rate * 0.5)
            elif pressure > 0.0:
                rate = max(_RAMP_MIN_TPS, rate * 0.8)
            elif elapsed >= 0.5:
                rate = min(_RAMP_MAX_TPS, rate * (1.0 + min(0.25, elapsed * 0.2)))
            _ramp["rate"] = rate
            if _ramp["tokens"] >= units:
                _ramp["tokens"] -= units
                _ramp["admitted"] += units
                return stop_event is None or not stop_event.is_set()
            wait_seconds = (units - float(_ramp["tokens"])) / max(rate, _RAMP_MIN_TPS)
            _ramp["throttled"] += 1
        if stop_event is not None and stop_event.is_set():
            return False
        if hasattr(stop_event, "wait") and stop_event is not None:
            stop_event.wait(max(0.02, min(0.25, wait_seconds)))
        else:
            time.sleep(max(0.02, min(0.25, wait_seconds)))


def reasons(producing_chunks: bool = False) -> tuple[list[str], dict]:
    state = snapshot()
    disk = state["disk"]
    with _lock:
        _paused["tasks"] = (
            state["task_depth"] > (TASK_LOW if _paused["tasks"] else TASK_HIGH)
        )
        _paused["writes"] = (
            state["write_depth"] > (WRITE_LOW if _paused["writes"] else WRITE_HIGH)
        )
        journal = state.get("result_journal", {})
        _paused["results"] = (
            not journal.get("healthy", True)
            or int(state.get("write_quarantined", 0)) > 0
        )
        if producing_chunks:
            _paused["chunks"] = (
                state["chunk_depth"] > (CHUNK_LOW if _paused["chunks"] else CHUNK_HIGH)
            )
        else:
            _paused["chunks"] = False
        if _paused["disk"]:
            _paused["disk"] = disk["used_pct"] > DISK_LOW or disk["free_bytes"] < MIN_FREE_BYTES
        else:
            _paused["disk"] = disk["used_pct"] >= DISK_HIGH or disk["free_bytes"] < MIN_FREE_BYTES
        active = [name for name, paused in _paused.items() if paused]
        state["paused"] = dict(_paused)
    return active, state


def wait_for_ingestion(stop_event=None, producing_chunks: bool = False,
                       interval: float = 0.25, work_units: int = 0) -> bool:
    while True:
        active, state = reasons(producing_chunks=producing_chunks)
        if state.get("paused", {}).get("tasks"):
            with _ramp_lock:
                _ramp["paused"] = True
                _ramp["seen_task_pause"] = True
        if not active:
            if work_units:
                return _ramp_admit(work_units, state, stop_event=stop_event, interval=interval)
            return stop_event is None or not stop_event.is_set()
        if stop_event is not None and stop_event.is_set():
            return False
        if hasattr(stop_event, "wait") and stop_event is not None:
            stop_event.wait(max(0.05, interval))
        else:
            time.sleep(max(0.05, interval))


def wait_for_delivery(stop_event=None, interval: float = 0.05) -> bool:
    """Pause new claims while preserving capacity to drain received results."""
    while True:
        active, _state = reasons(producing_chunks=False)
        blocking = [reason for reason in active if reason in {"writes", "disk", "results"}]
        if not blocking:
            return stop_event is None or not stop_event.is_set()
        if stop_event is not None and stop_event.is_set():
            return False
        time.sleep(max(0.02, interval))
