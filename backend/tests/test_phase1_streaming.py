import csv
import os
import threading
import time
from datetime import datetime

import pandas as pd
import pytest

import database
from models.batch_campaign import BatchCampaignUpdate
from models.realtime_campaign import RealtimeCampaignUpdate
from services import batch_campaign_service, realtime_campaign_service, sms_task_service
from services.smpp_gateway import gateway


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "test.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()
    yield tmp_path
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()


def _write_csv(path, count):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["msisdn", "language"])
        for index in range(count):
            writer.writerow([f"09{index:08d}", "english"])


def _create_batch(tmp_path, count=1200, batch_size=500, status="in_progress"):
    source = tmp_path / "customers.csv"
    _write_csv(source, count)
    campaign_dir = tmp_path / "batch-campaign"
    campaign_dir.mkdir(exist_ok=True)
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO batch_campaigns
           (name,status,customer_list_path,row_count,batch_size,phone_prefix,campaign_dir)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        ("phase1-batch", status, str(source), count, batch_size, "251", str(campaign_dir)),
    ).fetchone()["id"]
    content_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        ("phase1-content", "batch", campaign_id),
    ).fetchone()["id"]
    template_id = conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?) RETURNING id",
        (content_id, "english", "Hello {msisdn}"),
    ).fetchone()["id"]
    smpp_id = conn.execute(
        """INSERT INTO smpp_configs(name,host,system_id,password)
           VALUES (?,?,?,?) RETURNING id""",
        ("phase1-smpp", "127.0.0.1", "test", "test"),
    ).fetchone()["id"]
    conn.execute(
        "UPDATE batch_campaigns SET sms_template_id=?,smpp_config_id=? WHERE id=?",
        (template_id, smpp_id, campaign_id),
    )
    conn.commit()
    conn.close()
    return campaign_id, source


def _task_msisdns(campaign_type, campaign_id):
    conn = database.get_connection()
    rows = conn.execute(
        "SELECT msisdn FROM sms_tasks WHERE campaign_type=? AND campaign_id=? ORDER BY id",
        (campaign_type, campaign_id),
    ).fetchall()
    conn.close()
    return [row["msisdn"] for row in rows]


def test_batch_csv_1200_rows_are_500_500_200(isolated_db):
    campaign_id, _source = _create_batch(isolated_db)
    generated = batch_campaign_service.generate_batches(campaign_id, 500)
    assert [item["count"] for item in generated["batches"]] == [500, 500, 200]

    result = batch_campaign_service.process_pending_batches(campaign_id)
    assert result["processed"] == 3
    msisdns = _task_msisdns("batch", campaign_id)
    assert len(msisdns) == 1200
    assert len(set(msisdns)) == 1200


def test_batch_size_change_before_start_has_no_gaps_or_duplicates(isolated_db):
    campaign_id, _source = _create_batch(isolated_db, status="not_started")
    first = batch_campaign_service.generate_batches(campaign_id, 500)
    assert first["total_batches"] == 3

    updated = batch_campaign_service.update(
        campaign_id,
        BatchCampaignUpdate(batch_size=300, status="in_progress"),
    )
    assert updated["batch_size"] == 300
    assert [item["total_count"] for item in batch_campaign_service.get_batches(campaign_id)] == [300, 300, 300, 300]

    result = batch_campaign_service.process_pending_batches(campaign_id)
    assert result["processed"] == 4
    msisdns = _task_msisdns("batch", campaign_id)
    assert len(msisdns) == 1200
    assert len(set(msisdns)) == 1200


def test_batch_ingestion_waits_for_low_watermark(monkeypatch):
    queue_depths = iter([
        batch_campaign_service.INGEST_QUEUE_HIGH_WATERMARK,
        batch_campaign_service.INGEST_QUEUE_HIGH_WATERMARK - 1,
        batch_campaign_service.INGEST_QUEUE_LOW_WATERMARK,
    ])
    sleeps = []
    monkeypatch.setattr(batch_campaign_service, "_queued_task_count", lambda: next(queue_depths))
    monkeypatch.setattr(batch_campaign_service.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert batch_campaign_service._wait_for_ingest_capacity() is True
    assert sleeps == [0.25, 0.25]


def _create_realtime(tmp_path, count=10):
    source = tmp_path / "realtime.csv"
    _write_csv(source, count)
    campaign_dir = tmp_path / "realtime-campaign"
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?) RETURNING id""",
        ("phase1-realtime", "in_progress", False, str(campaign_dir), "251"),
    ).fetchone()["id"]
    content_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        ("phase1-realtime-content", "realtime", campaign_id),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?)",
        (content_id, "english", "Hello {msisdn}"),
    )
    conn.commit()
    conn.close()
    return campaign_id, source, campaign_dir


def test_realtime_failed_second_chunk_retry_does_not_duplicate_tasks_or_output(isolated_db, monkeypatch):
    campaign_id, source, campaign_dir = _create_realtime(isolated_db)
    monkeypatch.setattr(realtime_campaign_service, "CHUNK_SIZE", 5)
    original_enqueue = sms_task_service.enqueue_rows
    calls = {"count": 0}

    def fail_once(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("simulated second chunk failure")
        return original_enqueue(*args, **kwargs)

    monkeypatch.setattr(sms_task_service, "enqueue_rows", fail_once)
    with pytest.raises(RuntimeError, match="second chunk"):
        realtime_campaign_service.receive_file(campaign_id, str(source), source.name)

    monkeypatch.setattr(sms_task_service, "enqueue_rows", original_enqueue)
    result = realtime_campaign_service.receive_file(campaign_id, str(source), source.name)
    assert result["success"] is True
    assert len(_task_msisdns("realtime", campaign_id)) == 10

    output = campaign_dir / "output" / f"{datetime.now():%Y%m%d}.csv"
    output_rows = pd.read_csv(output, dtype=str, encoding="utf-8-sig")
    assert len(output_rows) == 10
    assert output_rows["msisdn"].nunique() == 10


def test_csv_whitelist_excel_input_releases_temp_resources(isolated_db, monkeypatch):
    whitelist = isolated_db / "excel-whitelist.csv"
    _write_csv(whitelist, 10)
    source = isolated_db / "realtime-input.xlsx"
    pd.DataFrame({
        "msisdn": [f"09{index:08d}" for index in range(10)],
        "language": ["english"] * 10,
    }).to_excel(source, index=False)
    campaign_dir = isolated_db / "excel-realtime-campaign"
    campaign_dir.mkdir()

    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,customer_list_path,row_count,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        ("excel-realtime", "in_progress", True, str(whitelist), 10, str(campaign_dir), "251"),
    ).fetchone()["id"]
    content_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        ("excel-realtime-content", "realtime", campaign_id),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?)",
        (content_id, "english", "Hello {msisdn}"),
    )
    conn.commit()
    conn.close()

    original_tempdir = realtime_campaign_service.tempfile.TemporaryDirectory
    tracked = []

    class TrackedTemporaryDirectory:
        def __init__(self, *args, **kwargs):
            self._inner = original_tempdir(*args, **kwargs)
            self.name = self._inner.name
            self.cleaned = False
            tracked.append(self)

        def cleanup(self):
            self.cleaned = True
            self._inner.cleanup()

    monkeypatch.setattr(
        realtime_campaign_service.tempfile,
        "TemporaryDirectory",
        TrackedTemporaryDirectory,
    )
    result = realtime_campaign_service.receive_file(campaign_id, str(source), source.name)

    assert result["success"] is True
    assert result["matched_count"] == 10
    assert len(_task_msisdns("realtime", campaign_id)) == 10
    assert tracked and all(item.cleaned for item in tracked)


def test_cached_activity_snapshot_is_invalidated_when_campaign_stops(isolated_db):
    campaign_id, _source, _campaign_dir = _create_realtime(isolated_db, count=1)
    task = {
        "campaign_type": "realtime",
        "campaign_id": campaign_id,
        "task_date": datetime.now().date().isoformat(),
    }
    assert sms_task_service.validate_task_schedule(task)[0] is True
    assert sms_task_service.validate_task_schedule(task)[0] is True

    updated = realtime_campaign_service.update(campaign_id, RealtimeCampaignUpdate(status="completed"))
    assert updated["status"] == "completed"
    active, reason = sms_task_service.validate_task_schedule(task)
    assert active is False
    assert reason == "campaign is not enabled"


@pytest.mark.performance
@pytest.mark.skipif(os.environ.get("RUN_LARGE_CAPACITY") != "1", reason="set RUN_LARGE_CAPACITY=1")
def test_million_row_csv_receive_file_is_memory_bounded(isolated_db, monkeypatch):
    psutil = pytest.importorskip("psutil")
    process = psutil.Process()
    whitelist = isolated_db / "million-whitelist.csv"
    _write_csv(whitelist, 1_000_000)
    campaign_dir = isolated_db / "capacity-realtime"
    campaign_dir.mkdir()
    source = isolated_db / "realtime-input.csv"
    _write_csv(source, 10_000)

    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,customer_list_path,row_count,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        ("capacity-realtime", "in_progress", True, str(whitelist), 1_000_000, str(campaign_dir), "251"),
    ).fetchone()["id"]
    content_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        ("capacity-content", "realtime", campaign_id),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?)",
        (content_id, "english", "Hello {msisdn}"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(realtime_campaign_service, "CHUNK_SIZE", 5000)
    monkeypatch.setattr(gateway, "send", lambda *_args, **_kwargs: ["stub-message-id"])
    samples = []
    sampling = True

    def sample_rss():
        while sampling:
            samples.append(process.memory_info().rss)
            time.sleep(0.02)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    baseline = process.memory_info().rss
    sampler.start()
    try:
        result = realtime_campaign_service.receive_file(campaign_id, str(source), source.name)
    finally:
        sampling = False
        sampler.join(timeout=2)

    assert result["success"] is True
    assert result["matched_count"] == 10_000
    assert len(_task_msisdns("realtime", campaign_id)) == 10_000
    assert max(samples or [baseline]) - baseline < 256 * 1024 * 1024
