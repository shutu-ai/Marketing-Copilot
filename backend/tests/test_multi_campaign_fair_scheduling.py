import csv
import os
import queue
import time
from datetime import datetime

import pytest

import database
from services import (
    batch_campaign_service,
    campaign_capacity_service,
    realtime_campaign_service,
    sms_task_service,
)


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "fair.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()
    campaign_capacity_service._deficits.clear()
    campaign_capacity_service._virtual_service.clear()
    campaign_capacity_service._rate_tokens.clear()
    campaign_capacity_service._rate_updated.clear()
    yield tmp_path


def _write_csv(path, count):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["msisdn", "language"])
        for index in range(count):
            writer.writerow([f"251900{index:06d}", "english"])


def _batch_campaign(tmp_path, name, count, batch_size=10):
    source = tmp_path / f"{name}.csv"
    _write_csv(source, count)
    campaign_dir = tmp_path / name
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO batch_campaigns
           (name,status,customer_list_path,customer_list_filename,row_count,batch_size,
            campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?,?,?,?) RETURNING id""",
        (name, "in_progress", str(source), source.name, count, batch_size,
         str(campaign_dir), "251"),
    ).fetchone()["id"]
    content_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        (f"{name}-content", "batch", campaign_id),
    ).fetchone()["id"]
    template_id = conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?) RETURNING id",
        (content_id, "english", "Hello {msisdn}"),
    ).fetchone()["id"]
    conn.execute("UPDATE batch_campaigns SET sms_template_id=? WHERE id=?", (template_id, campaign_id))
    conn.commit()
    conn.close()
    return campaign_id


def test_batch_manifest_quantum_does_not_make_large_activity_own_generation(isolated_db):
    large = _batch_campaign(isolated_db, "large", 80, batch_size=10)
    small = _batch_campaign(isolated_db, "small", 3, batch_size=3)

    first = batch_campaign_service.generate_batches(large, 10, max_batches=1)
    assert first["success"] is True and first["complete"] is False
    assert first["total_batches"] == 1

    generated_small = batch_campaign_service.generate_batches(small, 3)
    assert generated_small["complete"] is True
    processed_small = batch_campaign_service.process_pending_batches(small, max_batches=1)
    assert processed_small["processed"] == 1

    # The large source advances one durable quantum at a time and remains
    # invisible until its manifest is complete; no long writer transaction is
    # needed to let the small campaign create real work.
    second = batch_campaign_service.generate_batches(large, 10, max_batches=1)
    assert second["success"] is True
    conn = database.get_connection()
    try:
        large_state = conn.execute(
            "SELECT generated_batches,status FROM batch_campaign_manifests WHERE campaign_id=?",
            (large,),
        ).fetchone()
        small_tasks = conn.execute(
            "SELECT COUNT(*) count FROM sms_tasks WHERE campaign_type='batch' AND campaign_id=?",
            (small,),
        ).fetchone()["count"]
    finally:
        conn.close()
    assert large_state["generated_batches"] == 2
    assert large_state["status"] == "building"
    assert small_tasks == 3


def _insert_queued_task(campaign_id, index, content="short"):
    conn = database.get_connection()
    task_id = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,
            task_date,next_attempt_at,smpp_config_id,transport_smpp_config_id)
           VALUES ('batch',?,?,?,?,?,'queued',CURRENT_DATE,CURRENT_TIMESTAMP,0,0)
           RETURNING id""",
        (campaign_id, f"activity-{campaign_id}", f"2519{campaign_id:04d}{index:04d}",
         content, f"fair-{campaign_id}-{index}"),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    return task_id


def test_sms_candidate_cursor_reaches_campaign_outside_active_window(isolated_db, monkeypatch):
    monkeypatch.setenv("MC_FAIR_ACTIVE_CAMPAIGNS", "4")
    monkeypatch.setenv("MC_FAIR_CLAIM_PER_CAMPAIGN", "1")
    conn = database.get_connection()
    conn.executemany(
        "INSERT INTO batch_campaigns(name,status,send_days) VALUES (?,?,?)",
        [(f"fair-campaign-{campaign_id}", "in_progress", "[]")
         for campaign_id in range(1, 131)],
    )
    conn.commit()
    conn.close()
    for campaign_id in range(1, 131):
        _insert_queued_task(campaign_id, 0)

    reached = None
    for _ in range(8):
        claimed = sms_task_service.claim_next_tasks(1)
        if claimed and claimed[0]["campaign_id"] > 4:
            reached = claimed[0]["campaign_id"]
            break
    assert reached is not None

    conn = database.get_connection()
    try:
        state = conn.execute(
            "SELECT cursor_key,rounds FROM fair_scheduler_state WHERE stage='sms_claim'"
        ).fetchone()
    finally:
        conn.close()
    assert state and int(state["rounds"]) > 0


def _realtime_campaign(tmp_path, name, files=3):
    campaign_dir = tmp_path / name
    watch_dir = tmp_path / f"{name}-watch"
    campaign_dir.mkdir()
    watch_dir.mkdir()
    now = datetime.now()
    for index in range(files):
        path = watch_dir / f"Android_{now:%Y%m%d%H%M%S}_{index:02d}.csv"
        path.write_text("msisdn,language\n251900000001,english\n", encoding="utf-8")
        old = time.time() - 10
        os.utime(path, (old, old))
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,campaign_dir,watch_dir,watch_filename_prefix,phone_prefix)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        (name, "in_progress", False, str(campaign_dir), str(watch_dir), "Android_", "251"),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    return campaign_id


def test_realtime_watch_admission_rotates_campaigns_before_queue_is_full(isolated_db, monkeypatch):
    campaign_ids = [_realtime_campaign(isolated_db, f"realtime-{index}") for index in range(3)]
    monkeypatch.setattr(realtime_campaign_service, "WATCH_DISCOVERY_QUANTUM", 1)
    monkeypatch.setattr(realtime_campaign_service, "WATCH_STABILITY_SECONDS", 0.5)
    realtime_campaign_service._watch_scan_state.clear()
    realtime_campaign_service._watch_pending_stability.clear()
    monkeypatch.setattr(
        realtime_campaign_service, "_watch_copy_queue", queue.Queue(maxsize=2)
    )

    first = realtime_campaign_service.sync_watch_dirs_to_inboxes(
        now=datetime.now(), force=True, enqueue_only=True,
    )
    assert first["enqueued"] == 2
    first_jobs = list(realtime_campaign_service._watch_copy_queue.queue)
    assert {int(item["campaign_id"]) for item in first_jobs} == set(campaign_ids[:2])

    released = realtime_campaign_service._watch_copy_queue.get_nowait()
    realtime_campaign_service.ingestion_job_service.release_realtime_capacity(
        released["reservation_token"]
    )

    second = realtime_campaign_service.sync_watch_dirs_to_inboxes(
        now=datetime.now(), force=True, enqueue_only=True,
    )
    assert second["enqueued"] == 1
    second_jobs = list(realtime_campaign_service._watch_copy_queue.queue)
    assert any(int(item["campaign_id"]) == campaign_ids[1] for item in second_jobs)

    released = realtime_campaign_service._watch_copy_queue.get_nowait()
    realtime_campaign_service.ingestion_job_service.release_realtime_capacity(
        released["reservation_token"]
    )
    third = realtime_campaign_service.sync_watch_dirs_to_inboxes(
        now=datetime.now(), force=True, enqueue_only=True,
    )
    assert third["enqueued"] == 1
    third_jobs = list(realtime_campaign_service._watch_copy_queue.queue)
    assert any(int(item["campaign_id"]) == campaign_ids[2] for item in third_jobs)
