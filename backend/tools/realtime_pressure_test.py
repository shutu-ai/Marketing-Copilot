"""Deterministic realtime inbox pressure and scheduler/worker isolation check."""

import json
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import ingestion_job_service, realtime_campaign_service, realtime_ingestion_worker


def _campaign(root: Path, name: str, prefix: str) -> tuple[int, Path, Path]:
    watch = root / f"{name}-watch"
    campaign_dir = root / f"{name}-campaign"
    watch.mkdir()
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,campaign_dir,watch_dir,watch_filename_prefix,send_days)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        (name, "in_progress", True, str(campaign_dir), str(watch), prefix, "[]"),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    return campaign_id, watch, campaign_dir


def _add_files(watch: Path, prefix: str, now: datetime, start: int, count: int) -> None:
    old_timestamp = (now - timedelta(seconds=30)).timestamp()
    stamp = now.strftime("%Y%m%d%H%M%S")
    for index in range(start, start + count):
        path = watch / f"{prefix}{stamp}_{index:03d}.csv"
        path.write_text("msisdn\n0900000001\n", encoding="utf-8")
        os.utime(path, (old_timestamp, old_timestamp))


def _pending(campaign_id: int) -> int:
    conn = database.get_connection()
    try:
        return int(conn.execute(
            """SELECT COUNT(*) FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND status IN ('pending','processing','retrying')""",
            (campaign_id,),
        ).fetchone()[0])
    finally:
        conn.close()


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="mc-realtime-pressure-"))
    database.set_database_file(str(root / "pressure.duckdb"))
    database.init_db()
    now = datetime.now().replace(microsecond=0)

    pressure_id, pressure_watch, _ = _campaign(root, "pressure", "Pressure_")
    batches = []
    for target, delta in ((10, 10), (50, 40), (100, 50)):
        _add_files(pressure_watch, "Pressure_", now, target - delta, delta)
        started = time.perf_counter()
        result = realtime_campaign_service.sync_watch_dirs_to_inboxes(now=now, campaign_id=pressure_id)
        batches.append({
            "target": target,
            "moved": result["moved"],
            "pending": _pending(pressure_id),
            "sync_seconds": round(time.perf_counter() - started, 4),
        })
    # With the production campaign cap of 100, the next file remains in the
    # source watch directory and is not copied into the inbox.
    _add_files(pressure_watch, "Pressure_", now, 100, 1)
    blocked = realtime_campaign_service.sync_watch_dirs_to_inboxes(now=now, campaign_id=pressure_id)
    assert blocked["moved"] == 0
    pending_at_limit = _pending(pressure_id)
    assert pending_at_limit == 100
    # Keep the pressure scenario independent from the isolation scenario; the
    # rows are terminal test fixtures, so the worker below cannot consume one
    # of the 100 capacity samples while it is being delayed.
    conn = database.get_connection()
    conn.execute(
        "UPDATE ingestion_files SET status='completed' WHERE campaign_type='realtime' AND campaign_id=?",
        (pressure_id,),
    )
    conn.commit()
    conn.close()

    # Use a separate campaign with spare capacity to prove that a slow Worker
    # does not hold the scheduler's watch-directory sync call.
    isolation_id, isolation_watch, _ = _campaign(root, "isolation", "Isolation_")
    _add_files(isolation_watch, "Isolation_", now, 0, 1)
    realtime_campaign_service.sync_watch_dirs_to_inboxes(now=now, campaign_id=isolation_id)
    entered = threading.Event()
    release = threading.Event()
    original_receive = realtime_campaign_service.receive_file

    def slow_receive(*args, **kwargs):
        entered.set()
        release.wait(timeout=10)
        return {"success": True, "task_result": {"created": 0}}

    realtime_campaign_service.receive_file = slow_receive
    worker_thread = threading.Thread(target=realtime_ingestion_worker.process_once, daemon=True)
    worker_thread.start()
    assert entered.wait(timeout=5), "worker did not enter the delayed file handler"
    _add_files(isolation_watch, "Isolation_", now, 1, 1)
    started = time.perf_counter()
    isolation_sync = realtime_campaign_service.sync_watch_dirs_to_inboxes(now=now, campaign_id=isolation_id)
    isolation_seconds = time.perf_counter() - started
    release.set()
    worker_thread.join(timeout=5)
    realtime_campaign_service.receive_file = original_receive
    assert not worker_thread.is_alive()
    assert isolation_sync["moved"] == 1
    assert isolation_seconds < 1.0, isolation_seconds

    print(json.dumps({
        "pressure_batches": batches,
        "backpressure_at_100": {"moved": blocked["moved"], "pending": pending_at_limit},
        "scheduler_during_slow_worker": {
            "moved": isolation_sync["moved"],
            "sync_seconds": round(isolation_seconds, 4),
        },
        "result": "passed",
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
