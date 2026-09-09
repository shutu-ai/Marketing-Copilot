import os
from datetime import datetime, timedelta

import pytest

import database
from services import p1_service, sms_task_service, sms_write_buffer


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "phase2.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.flush()


def _insert_tasks(count: int, status: str = "sending", batch_id=None):
    conn = database.get_connection()
    rows = []
    for index in range(count):
        row = conn.execute(
            """INSERT INTO sms_tasks
               (campaign_type,campaign_id,batch_id,campaign_name,msisdn,content,dedupe_key,status)
               VALUES ('batch',1,?,'phase2',?,'hello',?,?) RETURNING id""",
            (batch_id, f"251900{index:06d}", f"phase2-{status}-{batch_id}-{index}", status),
        ).fetchone()
        rows.append(row["id"])
    conn.commit()
    conn.close()
    return rows


def test_delivery_hot_path_is_committed_in_bounded_batches(isolated_db, monkeypatch):
    task_ids = _insert_tasks(300)
    commits = []
    original_commit = database.WrappedConnection.commit

    def tracked_commit(self):
        commits.append(1)
        return original_commit(self)

    monkeypatch.setattr(database.WrappedConnection, "commit", tracked_commit)
    for index, task_id in enumerate(task_ids):
        sms_task_service.mark_submitted(task_id, [f"phase2-msg-{index}"], wait=False)
    sms_task_service.flush_pending_writes()

    conn = database.get_connection()
    accepted = conn.execute(
        "SELECT COUNT(*) count FROM sms_tasks WHERE status='accepted'"
    ).fetchone()["count"]
    mappings = conn.execute("SELECT COUNT(*) count FROM sms_task_message_ids").fetchone()["count"]
    submitted_detail = conn.execute(
        "SELECT COUNT(*) count FROM task_events WHERE event_type='submitted'"
    ).fetchone()["count"]
    submitted_batch = conn.execute(
        """SELECT COALESCE(SUM(event_count),0) count FROM task_event_batches
           WHERE event_type='submitted'"""
    ).fetchone()["count"]
    conn.close()
    assert accepted == 300
    assert mappings == 300
    assert submitted_detail == 0
    assert submitted_batch == 300
    assert 1 <= len(commits) <= 3


def test_hot_writes_use_independent_bounded_queues(isolated_db):
    state = sms_write_buffer.status()
    assert set(state["queue_depths"]) == {
        "smpp_response", "task_status", "message_id", "dlr",
    }
    assert 100 <= state["batch_size"] <= 1000
    assert 50 <= state["flush_interval_ms"] <= 200


def test_delivery_receipts_increment_batch_progress_without_task_count_scan(isolated_db):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('dlr-progress','in_progress') RETURNING id"
    ).fetchone()["id"]
    batch_id = conn.execute(
        """INSERT INTO batch_campaign_batches
           (campaign_id,batch_index,start_row,end_row,total_count,task_count,status)
           VALUES (?,0,0,2,2,2,'queued') RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    task_ids = _insert_tasks(2, batch_id=batch_id)

    for index, task_id in enumerate(task_ids):
        message_id = f"phase2-dlr-{index}"
        sms_task_service.mark_submitted(task_id, [message_id], wait=False)
        sms_task_service.record_receipt(message_id, "DELIVRD", "ok", wait=False)
    sms_task_service.flush_pending_writes()

    conn = database.get_connection()
    batch = conn.execute(
        "SELECT task_count,delivered_count,failed_count,status FROM batch_campaign_batches WHERE id=?",
        (batch_id,),
    ).fetchone()
    conn.close()
    assert batch["task_count"] == 2
    assert batch["delivered_count"] == 2
    assert batch["failed_count"] == 0
    assert batch["status"] == "sent"


def test_accepted_submission_counts_as_delivered_without_double_counting_manual_confirmation(isolated_db):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('accepted-progress','in_progress') RETURNING id"
    ).fetchone()["id"]
    batch_id = conn.execute(
        """INSERT INTO batch_campaign_batches
           (campaign_id,batch_index,start_row,end_row,total_count,task_count,status)
           VALUES (?,0,0,1,1,1,'queued') RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"]
    stats_date = datetime.now().strftime("%Y-%m-%d")
    conn.execute(
        "INSERT INTO batch_campaign_daily_stats(campaign_id,stats_date,total_prospects,sent_count) VALUES (?,?,1,0)",
        (campaign_id, stats_date),
    )
    conn.commit()
    conn.close()
    task_id = _insert_tasks(1, batch_id=batch_id)[0]

    sms_task_service.mark_submitted(task_id, ["accepted-progress-message"], "optional", wait=False)
    sms_task_service.flush_pending_writes()

    conn = database.get_connection()
    task = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    batch = conn.execute("SELECT delivered_count,status FROM batch_campaign_batches WHERE id=?", (batch_id,)).fetchone()
    conn.close()
    assert task["status"] == "accepted"
    assert batch["delivered_count"] == 1
    assert batch["status"] == "sent"

    assert sms_task_service.mark_delivered(task_id)[0] is True
    conn = database.get_connection()
    batch = conn.execute("SELECT delivered_count FROM batch_campaign_batches WHERE id=?", (batch_id,)).fetchone()
    conn.close()
    assert batch["delivered_count"] == 1


def test_late_failure_receipt_does_not_corrupt_completed_batch_counters(isolated_db):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('late-dlr','in_progress') RETURNING id"
    ).fetchone()["id"]
    batch_id = conn.execute(
        """INSERT INTO batch_campaign_batches
           (campaign_id,batch_index,start_row,end_row,total_count,task_count,status)
           VALUES (?,0,0,1,1,1,'queued') RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    task_id = _insert_tasks(1, batch_id=batch_id)[0]

    sms_task_service.mark_submitted(task_id, ["late-dlr-message"], wait=False)
    sms_task_service.record_receipt("late-dlr-message", "DELIVRD", "ok", wait=False)
    sms_task_service.record_receipt("late-dlr-message", "UNDELIV", "late", wait=False)
    sms_task_service.flush_pending_writes()

    conn = database.get_connection()
    task = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    batch = conn.execute(
        "SELECT delivered_count,failed_count,status FROM batch_campaign_batches WHERE id=?",
        (batch_id,),
    ).fetchone()
    conn.close()
    assert task["status"] == "delivered"
    assert batch["delivered_count"] == 1
    assert batch["failed_count"] == 0
    assert batch["status"] == "sent"


def test_created_and_claimed_events_are_aggregated(isolated_db):
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO batch_campaigns(name,status,phone_prefix)
           VALUES ('events','in_progress','251') RETURNING id"""
    ).fetchone()["id"]
    content_campaign_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES ('events-content','batch',?) RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,'Hi {msisdn}')",
        (content_campaign_id, "english"),
    )
    conn.commit()
    campaign = dict(conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone())
    conn.close()

    result = sms_task_service.enqueue_rows(
        "batch", campaign,
        [{"msisdn": f"09{index:08d}", "language": "english"} for index in range(120)],
        "phase2-events.csv", validate_schedule=False,
    )
    assert result["created"] == 120
    assert len(sms_task_service.claim_next_tasks(20)) == 20

    conn = database.get_connection()
    detail_events = conn.execute(
        "SELECT COUNT(*) count FROM task_events WHERE event_type IN ('created','claimed')"
    ).fetchone()["count"]
    batches = conn.execute(
        "SELECT event_type,event_count FROM task_event_batches ORDER BY id"
    ).fetchall()
    conn.close()
    assert detail_events == 0
    assert [(row["event_type"], row["event_count"]) for row in batches] == [
        ("created", 120), ("claimed", 20),
    ]


def test_retention_cleanup_is_chunked(isolated_db, monkeypatch):
    monkeypatch.setenv("MC_TASK_CLEANUP_BATCH", "1000")
    cutoff = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,created_at)
           SELECT 'batch',1,'cleanup','251'||CAST(i AS VARCHAR),'x','cleanup-'||CAST(i AS VARCHAR),
                  'delivered',?
           FROM range(1200) t(i)""",
        (cutoff,),
    )
    conn.commit()
    conn.close()

    assert sms_task_service.cleanup_task_retention() == 1000
    conn = database.get_connection()
    remaining = conn.execute("SELECT COUNT(*) count FROM sms_tasks").fetchone()["count"]
    conn.close()
    assert remaining == 200
    assert sms_task_service.cleanup_task_retention() == 200


def test_task_list_uses_stable_keyset_cursor(isolated_db):
    _insert_tasks(8, status="queued")
    first = sms_task_service.list_tasks(page_size=3)
    second = sms_task_service.list_tasks(page_size=3, cursor=first["next_cursor"])
    first_ids = [item["id"] for item in first["items"]]
    second_ids = [item["id"] for item in second["items"]]
    assert first["has_more"] is True
    assert second["has_more"] is True
    assert first_ids == sorted(first_ids, reverse=True)
    assert second_ids == sorted(second_ids, reverse=True)
    assert set(first_ids).isdisjoint(second_ids)
    assert max(second_ids) < min(first_ids)


def test_failure_views_use_id_keyset_cursors(isolated_db):
    task_ids = _insert_tasks(7, status="failed")
    conn = database.get_connection()
    for task_id in task_ids:
        conn.execute(
            """INSERT INTO failed_task_archive
               (task_id,campaign_type,campaign_id,msisdn,content,final_status,failure_class)
               VALUES (?,'batch',1,?,'hello','failed','transport')""",
            (task_id, f"251{task_id}"),
        )
    conn.commit()
    conn.close()

    failed_first = p1_service.list_failed_tasks(page_size=3)
    failed_second = p1_service.list_failed_tasks(
        page_size=3, cursor=failed_first["next_cursor"]
    )
    archive_first = p1_service.list_failure_archive(page_size=3)
    archive_second = p1_service.list_failure_archive(
        page_size=3, cursor=archive_first["next_cursor"]
    )
    assert {row["id"] for row in failed_first["items"]}.isdisjoint(
        row["id"] for row in failed_second["items"]
    )
    assert {row["id"] for row in archive_first["items"]}.isdisjoint(
        row["id"] for row in archive_second["items"]
    )
