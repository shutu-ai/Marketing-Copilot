from pathlib import Path
import sys
import threading

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import backpressure_service, realtime_ingestion_worker, sms_task_service


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "hotpath.duckdb"))
    database.init_db()
    yield tmp_path


def _seed_active_batch_task():
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('hotpath','in_progress') RETURNING id"
    ).fetchone()["id"]
    task_id = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
           VALUES ('batch',?,'hotpath','25190001','hello','hotpath-1','queued')
           RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    return campaign_id, task_id


def test_claim_and_submit_authorize_do_not_rescan_route_backlog(isolated_db, monkeypatch):
    _campaign_id, _task_id = _seed_active_batch_task()
    counts = []
    original_execute = database.WrappedConnection.execute

    def tracked_execute(self, sql, params=None):
        normalized = " ".join(str(sql).upper().split())
        if "COUNT(*)" in normalized and "SMS_TASKS" in normalized:
            counts.append(normalized)
        return original_execute(self, sql, params)

    monkeypatch.setattr(database.WrappedConnection, "execute", tracked_execute)
    claimed = sms_task_service.claim_next_tasks(1)
    assert len(claimed) == 1
    attempt_token = sms_task_service.record_send_intent(
        claimed[0], smpp_config_id=7, slot=0, planned_parts=1,
    )
    assert attempt_token
    assert counts == []


def test_realtime_worker_waits_before_claiming_when_capacity_is_paused(monkeypatch):
    calls = []

    class FakeStop:
        def wait(self, _timeout):
            return False

        def is_set(self):
            return False

    monkeypatch.setattr(realtime_ingestion_worker, "_stop", FakeStop())
    monkeypatch.setattr(
        realtime_ingestion_worker.backpressure_service,
        "wait_for_ingestion",
        lambda **kwargs: calls.append(kwargs) or False,
    )
    monkeypatch.setattr(
        realtime_ingestion_worker,
        "process_once",
        lambda _worker_id: pytest.fail("worker claimed while ingestion was paused"),
    )

    realtime_ingestion_worker._run(1)

    assert calls and calls[0]["producing_chunks"] is True


def test_ingestion_capacity_wait_resumes_after_low_watermark(monkeypatch):
    states = iter([
        (["tasks"], {"paused": {"tasks": True}}),
        ([], {"paused": {"tasks": False}}),
    ])
    sleeps = []
    monkeypatch.setattr(backpressure_service, "reasons", lambda **_kwargs: next(states))
    monkeypatch.setattr(backpressure_service.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert backpressure_service.wait_for_ingestion(producing_chunks=True, interval=0.01)
    assert sleeps == [0.05]


def test_ingestion_recovery_ramp_admits_shared_chunk_budget(monkeypatch):
    monkeypatch.setattr(
        backpressure_service,
        "_ramp",
        {
            "rate": backpressure_service._RAMP_INITIAL_TPS,
            "tokens": 0.0,
            "last_at": backpressure_service.time.monotonic(),
            "last_resume_at": 0.0,
            "paused": False,
            "seen_task_pause": False,
            "admitted": 0,
            "throttled": 0,
        },
    )
    states = iter([
        (["tasks"], {"paused": {"tasks": True}}),
        ([], {"paused": {"tasks": False}}),
        ([], {"paused": {"tasks": False}}),
        *([([], {"paused": {"tasks": False}})] * 8),
    ])
    monkeypatch.setattr(backpressure_service, "reasons", lambda **_kwargs: next(states))

    assert backpressure_service.wait_for_ingestion(
        producing_chunks=True, work_units=1, interval=0.01,
    )
    ramp = backpressure_service._ramp_snapshot()
    assert ramp["seen_task_pause"] is True
    assert ramp["admitted"] == 1
    assert ramp["rate_tps"] >= backpressure_service._RAMP_INITIAL_TPS


def test_ingestion_ramp_arms_from_cold_writer_pressure_and_bounds_large_chunks(monkeypatch):
    monkeypatch.setattr(
        backpressure_service,
        "_ramp",
        {
            "rate": backpressure_service._RAMP_INITIAL_TPS,
            "tokens": 0.0,
            "last_at": backpressure_service.time.monotonic(),
            "last_resume_at": 0.0,
            "paused": False,
            "seen_task_pause": False,
            "seen_pressure": False,
            "admitted": 0,
            "throttled": 0,
        },
    )
    pressured = {
        "paused": {"tasks": False, "writes": False, "chunks": False, "disk": False, "results": False},
        "write_depth": backpressure_service.WRITE_LOW + 1,
        "write_oldest_seconds": 0.0,
        "write_retry_depth": 0,
    }
    monkeypatch.setattr(backpressure_service, "reasons", lambda **_kwargs: ([], dict(pressured)))
    monkeypatch.setattr(backpressure_service, "write_lock_status", lambda: {"held": False})

    assert backpressure_service.wait_for_ingestion(producing_chunks=True)
    ramp = backpressure_service._ramp_snapshot()
    assert ramp["seen_pressure"] is True
    capacity = ramp["batch_capacity"]
    assert capacity == int(backpressure_service._RAMP_INITIAL_TPS * backpressure_service._RAMP_BURST_SECONDS)
    assert all(
        backpressure_service.ingestion_batch_size(units) <= capacity
        for units in (1, 750, 1500, 5000)
    )


def test_ingestion_ramp_reduces_stale_permit_without_spinning(monkeypatch):
    monkeypatch.setattr(
        backpressure_service,
        "_ramp",
        {
            "rate": backpressure_service._RAMP_INITIAL_TPS,
            "tokens": 0.0,
            "last_at": backpressure_service.time.monotonic(),
            "last_resume_at": 0.0,
            "paused": False,
            "seen_task_pause": False,
            "seen_pressure": True,
            "admitted": 0,
            "throttled": 0,
        },
    )
    monkeypatch.setattr(backpressure_service, "reasons", lambda **_kwargs: ([], {}))
    monkeypatch.setattr(backpressure_service, "_ingestion_pressure", lambda _state: 2.0)

    granted = backpressure_service.admit_ingestion_batch(500, interval=0.01)

    assert 0 < granted <= int(backpressure_service._RAMP_INITIAL_TPS * 0.5)
    assert backpressure_service._ramp["admitted"] == granted


def test_ramp_wait_uses_cached_depths_without_opening_read_connection(isolated_db, monkeypatch):
    database_path = str(Path(database.get_database_file()).absolute())
    monkeypatch.setattr(
        backpressure_service,
        "_depth_cache",
        {
            "at": backpressure_service.time.monotonic(),
            "database": database_path,
            "tasks": 0,
            "chunks": 0,
        },
    )
    monkeypatch.setattr(
        backpressure_service,
        "_ramp",
        {
            "rate": backpressure_service._RAMP_INITIAL_TPS,
            "tokens": 1.0,
            "last_at": backpressure_service.time.monotonic(),
            "last_resume_at": 0.0,
            "paused": False,
            "seen_task_pause": False,
            "seen_pressure": True,
            "admitted": 0,
            "throttled": 0,
        },
    )
    monkeypatch.setattr(
        backpressure_service,
        "read_transaction",
        lambda *_args, **_kwargs: pytest.fail("ramp wait must not refresh DuckDB depths"),
    )

    assert backpressure_service.admit_ingestion_batch(1, interval=0.01) == 1


def test_stopped_capacity_wait_does_not_write_retry_state(isolated_db, monkeypatch):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO realtime_campaigns(name,status,campaign_dir) VALUES ('realtime','in_progress',?) RETURNING id",
        (str(isolated_db),),
    ).fetchone()["id"]
    conn.commit()
    conn.close()

    stop_event = threading.Event()
    monkeypatch.setattr(realtime_ingestion_worker, "_stop", stop_event)
    monkeypatch.setattr(
        realtime_ingestion_worker.realtime_campaign_service,
        "receive_file",
        lambda *args, **kwargs: {"success": False, "status": "stopped"},
    )
    monkeypatch.setattr(
        realtime_ingestion_worker,
        "_mark_retry",
        lambda *args, **kwargs: pytest.fail("capacity pause must not transition file to retrying"),
    )

    result = realtime_ingestion_worker._process_claimed_file(
        {
            "id": 1,
            "campaign_id": campaign_id,
            "filename": "paused.csv",
            "file_path": str(isolated_db / "paused.csv"),
            "lease_token": "lease",
            "retry_count": 0,
            "max_retry_count": 3,
        },
        "worker-test",
    )

    assert result is True
