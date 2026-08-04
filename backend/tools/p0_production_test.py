"""Production-oriented P0 recovery, backup and concurrent-claim checks."""
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path


def main():
    root = Path(tempfile.mkdtemp(prefix="mc-p0-production-"))
    os.environ["DUCKDB_PATH"] = str(root / "production.duckdb")
    os.environ["MC_BACKUP_DIR"] = str(root / "backups")
    os.environ["MC_BACKUP_KEEP"] = "2"
    os.environ["MC_DB_CONFIG_FILE"] = str(root / "db_config.json")
    from database import get_connection, init_db
    from services.runtime_service import backup_database
    from services import sms_task_service

    init_db()
    conn = get_connection()
    conn.execute("INSERT INTO app_settings(key,value) VALUES ('recovery_marker','before-backup')")
    conn.commit(); conn.close()
    backups = []
    for index in range(3):
        backups.append(Path(backup_database()))
        if index < 2:
            # Backup names have one-second precision.
            import time; time.sleep(1.05)
    kept = sorted((root / "backups").glob("*.duckdb"))
    assert len(kept) == 2 and not backups[0].exists(), kept
    import duckdb
    restored = duckdb.connect(str(kept[-1]), read_only=True)
    marker = restored.execute("SELECT value FROM app_settings WHERE key='recovery_marker'").fetchone()[0]
    restored.close()
    assert marker == "before-backup"

    conn = get_connection()
    stale_at = (datetime.now() - timedelta(minutes=10)).isoformat()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,attempts,updated_at)
        VALUES ('realtime',1,'crash','251900009999','x','crash-recovery','sending',1,?)""", (stale_at,))
    for index in range(40):
        conn.execute("""INSERT INTO sms_tasks
            (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
            VALUES ('realtime',1,'concurrent',?,?,?,'queued')""",
            (f"25191{index:08d}", "x", f"concurrent-{index}"))
    conn.commit(); conn.close()
    sms_task_service.recover_stale_tasks()
    conn = get_connection()
    recovered_status = conn.execute("SELECT status FROM sms_tasks WHERE dedupe_key='crash-recovery'").fetchone()["status"]
    conn.close()
    assert recovered_status == "retrying"

    claimed, guard = [], threading.Lock()
    def consume():
        while True:
            task = sms_task_service.claim_next_task()
            if not task:
                return
            with guard:
                claimed.append(task["id"])
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: consume(), range(8)))
    assert len(claimed) == 41, len(claimed)
    assert len(set(claimed)) == len(claimed)
    conn = get_connection()
    attempts = conn.execute("SELECT attempts FROM sms_tasks WHERE dedupe_key='crash-recovery'").fetchone()["attempts"]
    conn.close()
    assert attempts == 2
    print("P0 PRODUCTION RECOVERY PASS")


if __name__ == "__main__":
    main()
