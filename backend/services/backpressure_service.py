"""Hysteretic single-host capacity guards for ingestion and delivery pipelines."""

import os
import shutil
import threading
import time

from database import get_connection, get_database_file


TASK_HIGH = max(1000, int(os.environ.get("MC_BATCH_INGEST_HIGH_WATERMARK", "50000")))
TASK_LOW = max(0, min(TASK_HIGH, int(os.environ.get("MC_BATCH_INGEST_LOW_WATERMARK", "20000"))))
WRITE_HIGH = max(1000, int(os.environ.get("MC_WRITE_QUEUE_HIGH_WATERMARK", "80000")))
WRITE_LOW = max(0, min(WRITE_HIGH, int(os.environ.get("MC_WRITE_QUEUE_LOW_WATERMARK", "30000"))))
CHUNK_HIGH = max(1000, int(os.environ.get("MC_CHUNK_HIGH_WATERMARK", "100000")))
CHUNK_LOW = max(0, min(CHUNK_HIGH, int(os.environ.get("MC_CHUNK_LOW_WATERMARK", "50000"))))
DISK_HIGH = max(50.0, min(99.0, float(os.environ.get("MC_DISK_HIGH_WATERMARK_PCT", "95"))))
DISK_LOW = max(40.0, min(DISK_HIGH, float(os.environ.get("MC_DISK_LOW_WATERMARK_PCT", "90"))))
MIN_FREE_BYTES = max(0, int(float(os.environ.get("MC_DISK_MIN_FREE_GB", "1")) * 1024 ** 3))

_lock = threading.Lock()
_paused = {"tasks": False, "writes": False, "chunks": False, "disk": False}
_depth_cache = {"at": 0.0, "database": "", "tasks": 0, "chunks": 0}


def _database_depths() -> tuple[int, int]:
    now = time.monotonic()
    database_path = os.path.abspath(get_database_file())
    with _lock:
        if _depth_cache["database"] == database_path and now - _depth_cache["at"] < 1.0:
            return int(_depth_cache["tasks"]), int(_depth_cache["chunks"])
    conn = get_connection()
    try:
        tasks = conn.execute(
            """SELECT COUNT(*) count FROM sms_tasks
               WHERE status IN ('queued','retrying','sending')"""
        ).fetchone()["count"]
        chunks = conn.execute(
            "SELECT COUNT(*) count FROM ingestion_chunks WHERE status IN ('pending','failed')"
        ).fetchone()["count"]
    finally:
        conn.close()
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
    usage = shutil.disk_usage(target)
    used_pct = (usage.used / usage.total * 100.0) if usage.total else 100.0
    return {"path": target, "used_pct": round(used_pct, 2), "free_bytes": usage.free}


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
    }
    with _lock:
        values["paused"] = dict(_paused)
    return values


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
                       interval: float = 0.25) -> bool:
    while True:
        active, _state = reasons(producing_chunks=producing_chunks)
        if not active:
            return stop_event is None or not stop_event.is_set()
        if stop_event is not None and stop_event.is_set():
            return False
        time.sleep(max(0.05, interval))
