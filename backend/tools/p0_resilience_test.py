"""Fast deterministic checks for P0 persistence, idempotency and failure policy."""
import os
import json
import tempfile
from pathlib import Path


def main():
    root = Path(tempfile.mkdtemp(prefix="mc-p0-"))
    os.environ["DUCKDB_PATH"] = str(root / "test.duckdb")
    os.environ["MC_INSTANCE_LOCK"] = str(root / "instance.lock")
    from database import init_db, get_connection
    from services import operations_service, sms_task_service
    from services.runtime_service import acquire_instance_lock, release_instance_lock

    init_db()
    sample = root / "input.csv"
    sample.write_text("msisdn\n0900000000\n", encoding="utf-8")
    first_id, first_duplicate = operations_service.register_file("realtime", 1, str(sample), sample.name)
    second_id, second_duplicate = operations_service.register_file("realtime", 1, str(sample), "renamed.csv")
    assert not first_duplicate and second_duplicate and first_id == second_id
    duplicate_inbox = root / "duplicate.csv"
    duplicate_inbox.write_text("msisdn\n0900000000\n", encoding="utf-8")
    duplicate_path = operations_service.archive_duplicate(
        str(duplicate_inbox), str(root / "campaign"), duplicate_inbox.name)
    assert not duplicate_inbox.exists() and Path(duplicate_path).is_file()

    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',1,'p0','251900000000','test','p0-claim','queued')""")
    conn.commit(); conn.close()
    claimed = sms_task_service.claim_next_task()
    assert claimed and claimed["status"] == "sending" and claimed["attempts"] == 1
    assert sms_task_service.claim_next_task() is None

    conn = get_connection()
    conn.execute("UPDATE sms_tasks SET status='submitted',smsc_message_ids=? WHERE id=?",
                 (json.dumps(["p0-msg"]), claimed["id"]))
    conn.commit(); conn.close()
    sms_task_service.record_receipt("p0-msg", "UNDELIV", "temporary test")
    conn = get_connection()
    task = conn.execute("SELECT status,receipt_retry_count FROM sms_tasks WHERE id=?", (claimed["id"],)).fetchone()
    conn.close()
    assert task["status"] == "retrying" and task["receipt_retry_count"] == 1, dict(task)

    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,smsc_message_ids)
        VALUES ('realtime',1,'p0','251900000001','test','p0-submit-race','sending',?)""",
        (json.dumps(["p0-race-msg"]),))
    race_id = conn.execute("SELECT id FROM sms_tasks WHERE dedupe_key='p0-submit-race'").fetchone()["id"]
    conn.commit(); conn.close()
    sms_task_service.record_receipt("p0-race-msg", "DELIVRD", "early delivery receipt")
    sms_task_service.mark_submitted(race_id, ["p0-race-msg"])
    sms_task_service.record_receipt("p0-race-msg", "ACCEPTD", "late non-terminal receipt")
    conn = get_connection()
    race_task = conn.execute("SELECT status,smsc_message_ids FROM sms_tasks WHERE id=?", (race_id,)).fetchone()
    conn.close()
    assert race_task["status"] == "delivered", dict(race_task)
    assert json.loads(race_task["smsc_message_ids"]) == ["p0-race-msg"]

    # Regression: SMSC message ids can be reused after a simulator restart.
    # A receipt for a reused id must update the newest task, not be matched to
    # an older task or dropped as a duplicate of the older task's receipt.
    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',1,'p0','251900000002','test','p0-reuse-old','sending')""")
    old_id = conn.execute("SELECT id FROM sms_tasks WHERE dedupe_key='p0-reuse-old'").fetchone()["id"]
    conn.commit(); conn.close()
    sms_task_service.mark_submitted(old_id, ["p0-reuse-msg"])
    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',1,'p0','251900000003','test','p0-reuse-new','sending')""")
    new_id = conn.execute("SELECT id FROM sms_tasks WHERE dedupe_key='p0-reuse-new'").fetchone()["id"]
    conn.commit(); conn.close()
    sms_task_service.mark_submitted(new_id, ["p0-reuse-msg"])
    sms_task_service.record_receipt("p0-reuse-msg", "DELIVRD", "reused id receipt")
    conn = get_connection()
    new_task = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (new_id,)).fetchone()
    old_task = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (old_id,)).fetchone()
    conn.close()
    assert new_task["status"] == "delivered", dict(new_task)
    assert old_task["status"] == "submitted", dict(old_task)
    # A repeated identical receipt is idempotent and must not error.
    sms_task_service.record_receipt("p0-reuse-msg", "DELIVRD", "duplicate receipt")
    conn = get_connection()
    new_task = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (new_id,)).fetchone()
    conn.close()
    assert new_task["status"] == "delivered", dict(new_task)

    acquire_instance_lock()
    try:
        try:
            acquire_instance_lock()
            raise AssertionError("second instance lock unexpectedly succeeded")
        except RuntimeError:
            pass
    finally:
        release_instance_lock()
    print("P0 RESILIENCE PASS")


if __name__ == "__main__":
    main()
