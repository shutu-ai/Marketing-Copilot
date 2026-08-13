"""Deterministic P1 operations-completeness regression checks."""
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path


def main():
    root = Path(tempfile.mkdtemp(prefix="mc-p1-"))
    os.environ["DUCKDB_PATH"] = str(root / "p1.duckdb")
    os.environ["MC_SYSTEM_MODE"] = "development"
    from database import get_connection, init_db
    from models.sms_content import SmsTemplateCreate, SmsTemplateUpdate
    from services import p1_service, sms_content_service, sms_task_service

    init_db()
    campaign_dir = root / "campaign"
    campaign_dir.mkdir()
    watch_dir = campaign_dir / "watch"
    watch_dir.mkdir()
    whitelist = campaign_dir / "whitelist.csv"
    whitelist.write_text("msisdn\n251900000001\n", encoding="utf-8")
    conn = get_connection()
    conn.execute("""INSERT INTO smpp_configs
        (name,host,port,system_id,password,enabled)
        VALUES ('P1 SMPP','127.0.0.1',2775,'p1','p1',1)""")
    smpp_id = conn.execute("SELECT id FROM smpp_configs WHERE name='P1 SMPP'").fetchone()["id"]
    conn.execute("""INSERT INTO realtime_campaigns
        (name,status,start_time,end_time,customer_list_path,row_count,campaign_dir,
         watch_dir,watch_filename_prefix,smpp_config_id)
        VALUES ('P1 realtime','in_progress','',?, ?,1,?,?,?,?)""",
        ((datetime.now() + timedelta(days=1)).isoformat(), str(whitelist), str(campaign_dir),
         str(watch_dir), 'Android_4G_Switch_Somali_', smpp_id))
    realtime_id = conn.execute("SELECT id FROM realtime_campaigns WHERE name='P1 realtime'").fetchone()["id"]
    conn.execute("INSERT INTO campaigns(name,marketing_type,marketing_campaign_id) VALUES ('P1 templates','realtime',?)", (realtime_id,))
    content_campaign_id = conn.execute("SELECT id FROM campaigns WHERE name='P1 templates'").fetchone()["id"]
    conn.commit(); conn.close()

    template = sms_content_service.create_template(SmsTemplateCreate(
        campaign_id=content_campaign_id, language="English",
        content="Hello {msisdn} from {event_filter_id}", source_addr="P1"))
    assert sms_content_service.validate_template("Hi {msisdn}")["valid"]
    assert not sms_content_service.validate_template("Hi {msisdn")["valid"]
    sms_content_service.update_template(template["id"], SmsTemplateUpdate(content="Updated {msisdn}"))
    versions = sms_content_service.get_template_versions(template["id"])
    assert [v["version_no"] for v in versions] == [2, 1], versions
    restored = sms_content_service.restore_template_version(template["id"], 1)
    assert restored["content"] == "Hello {msisdn} from {event_filter_id}"

    paused = p1_service.campaign_action("realtime", realtime_id, "pause")
    assert paused["status"] == "paused"
    resumed = p1_service.campaign_action("realtime", realtime_id, "resume")
    assert resumed["status"] == "in_progress"

    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,error_msg,failure_class,attempts)
        VALUES ('realtime',?,'P1 realtime','251900000001','x','p1-failed','failed','rejected','delivery_receipt',3)""", (realtime_id,))
    failed_id = conn.execute("SELECT id FROM sms_tasks WHERE dedupe_key='p1-failed'").fetchone()["id"]
    conn.commit(); conn.close()
    result = p1_service.bulk_failed_tasks("retry", [failed_id])
    assert result["count"] == 1
    conn = get_connection(); conn.execute("UPDATE sms_tasks SET status='failed' WHERE id=?", (failed_id,)); conn.commit(); conn.close()
    result = p1_service.bulk_failed_tasks("archive", [failed_id])
    assert result["count"] == 1 and p1_service.list_failure_archive()["total"] == 1

    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,smsc_message_ids)
        VALUES ('realtime',?,'P1 realtime','251900000002','x','p1-dlr','submitted',?)""",
        (realtime_id, json.dumps(["p1-dlr-msg"])))
    dlr_id = conn.execute("SELECT id FROM sms_tasks WHERE dedupe_key='p1-dlr'").fetchone()["id"]
    conn.commit(); conn.close()
    sms_task_service.record_receipt("p1-dlr-msg", "REJECTD", "rejected test")
    timeline = p1_service.task_timeline(dlr_id)
    assert timeline["events"][-1]["event_type"] == "delivery_receipt"
    assert timeline["task"]["status"] == "failed"

    progress = p1_service.campaign_progress("realtime", realtime_id)
    assert progress["tasks"]["total"] == 2
    from services import operations_service
    operations_service.event("configuration", "P1_TEST", "test event", "warning")
    events = p1_service.list_events(False)
    assert events["total"] == 1
    assert p1_service.resolve_event(events["items"][0]["id"])

    conn = get_connection()
    conn.execute("UPDATE realtime_campaigns SET end_time=?,status='in_progress' WHERE id=?",
                 ((datetime.now() - timedelta(minutes=1)).isoformat(), realtime_id))
    conn.execute("""INSERT INTO sms_tasks
        (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',?,'P1 realtime','251900000003','x','p1-pending-at-complete','queued')""", (realtime_id,))
    conn.commit(); conn.close()
    assert p1_service.auto_complete_campaigns() == 1
    assert p1_service.campaign_progress("realtime", realtime_id)["status"] == "completed"
    conn = get_connection()
    pending = conn.execute("SELECT status FROM sms_tasks WHERE dedupe_key='p1-pending-at-complete'").fetchone()
    conn.close()
    assert pending["status"] == "expired", dict(pending)
    audits = p1_service.list_audit_logs()
    assert audits["total"] >= 8, audits
    print("P1 OPERATIONS PASS")


if __name__ == "__main__":
    main()
