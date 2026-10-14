import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest

import database
from services import (backpressure_service, ingestion_job_service, sms_task_service,
                      sms_write_buffer, task_archive_service)


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "phase3.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.flush()


def test_stable_business_key_uses_filename_not_temporary_path(isolated_db):
    first = sms_task_service.stable_task_dedupe_key(
        "realtime", 7, "251900000001", "2026-08-14", str(isolated_db / "a" / "events.csv")
    )
    moved = sms_task_service.stable_task_dedupe_key(
        "realtime", 7, "251900000001", "2026-08-14", str(isolated_db / "b" / "events.csv")
    )
    renamed = sms_task_service.stable_task_dedupe_key(
        "realtime", 7, "251900000001", "2026-08-14", str(isolated_db / "b" / "renamed.csv")
    )
    assert first == moved
    assert renamed != first


def test_chunk_manifest_resumes_completed_work_and_recovers_expired_lease(isolated_db):
    source = isolated_db / "targets.csv"
    source.write_text("msisdn\n0900000001\n0900000002\n", encoding="utf-8")
    file_id, duplicate = ingestion_job_service.register_file(
        "batch", 11, source.name, str(source), total_rows=2, chunk_size=1,
        chunks=[
            {"chunk_index": 0, "start_row": 0, "end_row": 1},
            {"chunk_index": 1, "start_row": 1, "end_row": 2},
        ],
    )
    assert duplicate is False
    first = ingestion_job_service.claim_chunk(file_id=file_id, chunk_index=0)
    ingestion_job_service.complete_chunk(first["id"], {"created": 1})
    assert ingestion_job_service.claim_chunk(file_id=file_id, chunk_index=0)["status"] == "completed"

    second = ingestion_job_service.claim_chunk(file_id=file_id, chunk_index=1)
    conn = database.get_connection()
    conn.execute(
        "UPDATE ingestion_chunks SET lease_expires_at='2000-01-01 00:00:00' WHERE id=?",
        (second["id"],),
    )
    conn.commit()
    conn.close()
    assert ingestion_job_service.recover_expired_chunks() == 1
    recovered = ingestion_job_service.claim_chunk(file_id=file_id, chunk_index=1)
    assert recovered["attempts"] == 2


def test_concurrent_task_claims_do_not_conflict_or_duplicate_rows(isolated_db):
    conn = database.get_connection()
    conn.execute(
        "INSERT INTO batch_campaigns(id,name,status,send_days) VALUES (1,'claim-lock','in_progress','[]')"
    )
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,task_date)
           SELECT 'batch',1,'claim-lock','251900'||CAST(i AS VARCHAR),'hello',
                  'claim-lock-'||CAST(i AS VARCHAR),'queued',CURRENT_DATE
           FROM range(96) t(i)"""
    )
    conn.commit()
    conn.close()

    def claim_batch(_index):
        return sms_task_service.claim_next_tasks(6)

    with ThreadPoolExecutor(max_workers=8) as pool:
        claimed = [task for batch in pool.map(claim_batch, range(16)) for task in batch]

    ids = [task["id"] for task in claimed]
    assert len(ids) == 96
    assert len(set(ids)) == 96


def test_sql_staging_deduplicates_and_joins_exclusions(isolated_db):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status,phone_prefix) VALUES ('stage','in_progress','251') RETURNING id"
    ).fetchone()["id"]
    content_id = conn.execute(
        "INSERT INTO campaigns(name,marketing_type,marketing_campaign_id) VALUES ('stage','batch',?) RETURNING id",
        (campaign_id,),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,'Hi {msisdn}')",
        (content_id, "english"),
    )
    conn.execute("INSERT INTO exclusion_users(msisdn,enabled) VALUES ('251900000002',1)")
    conn.commit()
    campaign = dict(conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone())
    conn.close()
    result = sms_task_service.enqueue_rows(
        "batch", campaign,
        [
            {"msisdn": "0900000001", "language": "english"},
            {"msisdn": "+251 900000001", "language": "english"},
            {"msisdn": "0900000002", "language": "english"},
            {"msisdn": "", "language": "english"},
        ],
        "stable.csv", validate_schedule=False,
    )
    assert result["created"] == 1
    assert result["duplicate"] == 1
    assert result["excluded"] == 1
    assert result["invalid"] == 1


def test_terminal_partition_is_archived_before_hot_rows_are_deleted(isolated_db):
    old = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,task_date,created_at)
           SELECT 'batch',9,'archive','2519'||CAST(i AS VARCHAR),'x','archive-'||CAST(i AS VARCHAR),
                  'delivered','2026-08-01',? FROM range(3) t(i)""",
        (old,),
    )
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,task_date,created_at)
           VALUES ('batch',9,'archive','251999','x','archive-active','queued','2026-08-01',?)""",
        (old,),
    )
    conn.commit()
    conn.close()
    result = task_archive_service.archive_terminal_tasks(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1000
    )
    assert result["archived"] == 3
    assert os.path.isfile(os.path.join(result["root_path"], "tasks.parquet"))
    history = task_archive_service.list_archived_tasks("batch", 9, page_size=2)
    assert len(history["items"]) == 2
    assert history["has_more"] is True
    conn = database.get_connection()
    remaining = conn.execute("SELECT status FROM sms_tasks").fetchall()
    manifest = conn.execute("SELECT task_count,status FROM task_archive_manifests").fetchone()
    conn.close()
    assert [row["status"] for row in remaining] == ["queued"]
    assert manifest["task_count"] == 3
    assert manifest["status"] == "completed"


def test_backpressure_uses_high_low_hysteresis(isolated_db, monkeypatch):
    with backpressure_service._lock:
        for key in backpressure_service._paused:
            backpressure_service._paused[key] = False
    states = iter([
        {"task_depth": backpressure_service.TASK_HIGH + 1, "chunk_depth": 0,
         "write_depth": 0, "write_depths": {},
         "disk": {"used_pct": 10.0, "free_bytes": 10 ** 12}},
        {"task_depth": backpressure_service.TASK_LOW + 1, "chunk_depth": 0,
         "write_depth": 0, "write_depths": {},
         "disk": {"used_pct": 10.0, "free_bytes": 10 ** 12}},
        {"task_depth": backpressure_service.TASK_LOW, "chunk_depth": 0,
         "write_depth": 0, "write_depths": {},
         "disk": {"used_pct": 10.0, "free_bytes": 10 ** 12}},
    ])
    monkeypatch.setattr(backpressure_service, "snapshot", lambda: next(states))
    assert "tasks" in backpressure_service.reasons()[0]
    assert "tasks" in backpressure_service.reasons()[0]
    assert "tasks" not in backpressure_service.reasons()[0]


@pytest.mark.performance
@pytest.mark.skipif(
    os.environ.get("RUN_TEN_MILLION_CAPACITY") != "1",
    reason="set RUN_TEN_MILLION_CAPACITY=1 for the ten-million-row capacity gate",
)
def test_ten_million_terminal_rows_keep_keyset_memory_bounded(isolated_db):
    import psutil

    process = psutil.Process()
    before = process.memory_info().rss
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,task_date)
           SELECT 'batch',1,'ten-million','251'||CAST(i AS VARCHAR),'x',
                  'ten-million-'||CAST(i AS VARCHAR),'delivered','2026-08-01'
           FROM range(10000000) t(i)"""
    )
    conn.commit()
    count = conn.execute("SELECT COUNT(*) count FROM sms_tasks").fetchone()["count"]
    conn.close()
    started = time.monotonic()
    page = sms_task_service.list_tasks(page_size=100)
    elapsed = time.monotonic() - started
    after = process.memory_info().rss
    assert count == 10_000_000
    assert len(page["items"]) == 100
    assert page["has_more"] is True
    assert elapsed < 15
    assert after - before < 768 * 1024 * 1024
