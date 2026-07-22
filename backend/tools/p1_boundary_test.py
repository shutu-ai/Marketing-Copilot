"""P1 transition, cancellation, health and export boundary checks."""
import asyncio
import os
import tempfile
from pathlib import Path


def main():
    root = Path(tempfile.mkdtemp(prefix="mc-p1-boundary-"))
    os.environ["DUCKDB_PATH"] = str(root / "p1-boundary.duckdb")
    from database import get_connection, init_db
    from services import p1_service
    from routers.operations import _export_rows, export_report
    init_db()
    conn = get_connection()
    conn.execute("INSERT INTO realtime_campaigns(name,status,campaign_dir) VALUES ('Cancel me','in_progress',?)", (str(root),))
    realtime_id = conn.execute("SELECT id FROM realtime_campaigns WHERE name='Cancel me'").fetchone()["id"]
    conn.execute("""INSERT INTO sms_tasks(campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',?,'Cancel me','251900000001','x','cancel-boundary','queued')""", (realtime_id,))
    conn.execute("INSERT INTO batch_campaigns(name,status,campaign_dir) VALUES ('Batch boundary','in_progress',?)", (str(root),))
    batch_id = conn.execute("SELECT id FROM batch_campaigns WHERE name='Batch boundary'").fetchone()["id"]
    conn.commit(); conn.close()

    assert p1_service.campaign_action("batch", batch_id, "pause")["status"] == "paused"
    assert p1_service.campaign_action("batch", batch_id, "cancel")["status"] == "cancelled"
    assert p1_service.campaign_action("realtime", realtime_id, "cancel")["status"] == "cancelled"
    conn = get_connection()
    assert conn.execute("SELECT status FROM sms_tasks WHERE dedupe_key='cancel-boundary'").fetchone()["status"] == "cancelled"
    conn.close()
    try:
        p1_service.campaign_action("realtime", realtime_id, "resume")
        raise AssertionError("invalid cancelled -> resume transition succeeded")
    except ValueError:
        pass

    checks = p1_service.configuration_health(record_events=True)
    codes = {item["code"] for item in checks}
    assert {"SMPP_ENABLED", "TEMPLATE_ENABLED", "DATABASE_PATH"} <= codes
    events_before = p1_service.list_events(False)["total"]
    p1_service.configuration_health(record_events=True)
    assert p1_service.list_events(False)["total"] == events_before

    rows = _export_rows("audits")
    assert rows and any(row["action"] == "campaign.cancel" for row in rows)
    response = export_report("audits")
    async def read_body():
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.encode("utf-8") if isinstance(chunk, str) else chunk)
        return b"".join(chunks)
    body = asyncio.run(read_body())
    assert body.startswith(b"\xef\xbb\xbf") and b"campaign.cancel" in body
    assert "marketing-copilot-audits.csv" in response.headers["content-disposition"]
    try:
        _export_rows("unknown")
        raise AssertionError("unknown report unexpectedly exported")
    except ValueError:
        pass
    print("P1 BOUNDARY PASS")


if __name__ == "__main__":
    main()
