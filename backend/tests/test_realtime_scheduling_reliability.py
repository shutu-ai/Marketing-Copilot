"""Focused P0/P1 reliability regressions for the realtime scheduling path."""

from datetime import datetime, timedelta
from pathlib import Path
import threading
import time

import pytest

import database
from services import realtime_campaign_service as campaigns
from services import sms_task_service, sms_write_buffer
from services.smpp_gateway import (
    _SmppConnection, SmppPartialSubmitError, SmppSubmissionNotAllowedError,
)


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "realtime-reliability.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.flush()


def _insert_task(campaign_id: int, status: str, claim: str = "", **extra) -> int:
    conn = database.get_connection()
    values = {
        "campaign_type": extra.get("campaign_type", "batch"),
        "campaign_id": campaign_id,
        "campaign_name": "reliability",
        "msisdn": "251900000001",
        "content": "hello",
        "dedupe_key": f"reliability-{time.time_ns()}",
        "status": status,
        "claim_token": claim,
    }
    row = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,claim_token)
           VALUES (?,?,?,?,?,?,?,?) RETURNING id""",
        tuple(values[key] for key in (
            "campaign_type", "campaign_id", "campaign_name", "msisdn",
            "content", "dedupe_key", "status", "claim_token",
        )),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


@pytest.mark.parametrize("status", [
    "accepted", "submitted", "delivered", "sending", "unknown", "partial",
])
def test_normal_retry_rejects_submission_evidence(isolated_db, status):
    task_id = _insert_task(10, status, "claim-old")
    conn = database.get_connection()
    conn.execute(
        """UPDATE sms_tasks SET smsc_message_ids='[\"sms-old\"]',
           submitted_at='2026-10-13 10:00:00',attempts=2 WHERE id=?""",
        (task_id,),
    )
    conn.commit()
    conn.close()

    with pytest.raises(sms_task_service.TaskRetryRejected, match="not allowed"):
        sms_task_service.retry_task(task_id)

    conn = database.get_connection()
    row = conn.execute(
        "SELECT status,attempts,claim_token,smsc_message_ids,submitted_at FROM sms_tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    conn.close()
    assert dict(row) == {
        "status": status, "attempts": 2, "claim_token": "claim-old",
        "smsc_message_ids": '["sms-old"]', "submitted_at": "2026-10-13 10:00:00",
    }


def test_definite_failure_retry_is_a_single_cas(isolated_db):
    task_id = _insert_task(11, "failed")
    conn = database.get_connection()
    conn.execute(
        "UPDATE sms_tasks SET failure_class='submit_sm_resp',error_msg='rejected' WHERE id=?",
        (task_id,),
    )
    conn.commit()
    conn.close()

    assert sms_task_service.retry_task(task_id) is True
    with pytest.raises(sms_task_service.TaskRetryRejected):
        sms_task_service.retry_task(task_id)
    conn = database.get_connection()
    row = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    events = conn.execute(
        "SELECT COUNT(*) count FROM task_events WHERE task_id=? AND event_type='manual_retry'",
        (task_id,),
    ).fetchone()["count"]
    conn.close()
    assert row["status"] == "queued"
    assert events == 1


def _insert_realtime_campaign(tmp_path, end_time="23:59", start_time="00:00") -> int:
    today = datetime.now().date()
    conn = database.get_connection()
    row = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,start_time,end_time,send_start_time,send_end_time,
            send_days,campaign_dir,watch_dir,watch_filename_prefix)
           VALUES (?,?,?,?,?,?,?,?,?,?,?) RETURNING id""",
        (
            "realtime-reliability", "in_progress", False,
            today.isoformat() + " 00:00:00",
            (today + timedelta(days=1)).isoformat() + " 00:00:00",
            start_time, end_time, "[]", str(tmp_path / "campaign"),
            str(tmp_path / "watch"), "Android_",
        ),
    ).fetchone()
    conn.commit()
    conn.close()
    (tmp_path / "campaign" / "inbox").mkdir(parents=True)
    (tmp_path / "watch").mkdir(parents=True)
    return int(row["id"])


def test_finalizer_preserves_authorized_send_for_late_response(isolated_db, monkeypatch):
    campaign_id = _insert_realtime_campaign(isolated_db, end_time="18:00")
    claim = "claim-in-flight"
    task_id = _insert_task(campaign_id, "sending", claim, campaign_type="realtime")
    attempt = "attempt-in-flight"
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO sms_send_attempts
           (attempt_token,task_id,claim_token,source_namespace,planned_parts,state)
           VALUES (?,?,?,'smpp:1',1,'authorized')""",
        (attempt, task_id, claim),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(campaigns, "_daily_finalize_date", None)
    campaigns._daily_finalized_windows.clear()
    campaigns._daily_started_windows.clear()

    now = datetime.now().replace(hour=20, minute=0, second=0, microsecond=0)
    assert campaigns.finalize_daily_tasks(now) == 0

    database.write_transaction(
        lambda conn: sms_task_service._apply_submitted_batch(conn, [{
            "task_id": task_id, "message_ids": ["sms-late"],
            "delivery_receipt_mode": "optional", "transport_smpp_config_id": 1,
            "submit_sm_parts": 1, "claim_token": claim,
            "attempt_token": attempt, "source_namespace": "smpp:1",
        }]),
        operation="test.late_submit",
    )
    conn = database.get_connection()
    row = conn.execute(
        "SELECT status,claim_token,smsc_message_ids FROM sms_tasks WHERE id=?", (task_id,)
    ).fetchone()
    conn.close()
    assert dict(row) == {
        "status": "accepted", "claim_token": claim, "smsc_message_ids": '["sms-late"]',
    }


def test_smp_socket_is_not_written_after_window_gate_rejects(isolated_db, monkeypatch):
    now = datetime.now()
    campaign_id = _insert_realtime_campaign(
        isolated_db,
        start_time=(now + timedelta(minutes=1)).strftime("%H:%M"),
        end_time=(now + timedelta(minutes=2)).strftime("%H:%M"),
    )
    claim = "claim-closed"
    task_id = _insert_task(campaign_id, "sending", claim, campaign_type="realtime")

    class FakeClient:
        def __init__(self):
            self.sent = []

        def send_message(self, **kwargs):
            self.sent.append(kwargs)
            return type("Pdu", (), {"sequence": 1})()

    connection = _SmppConnection({
        "id": 1, "host": "127.0.0.1", "port": 2775,
        "window_size": 1, "tps": 1000, "timeout": 1,
        "delivery_receipt_mode": "disabled",
    }, 1)
    fake = FakeClient()
    connection.client = fake
    monkeypatch.setattr(connection, "connect", lambda: None)
    task = {
        "id": task_id, "campaign_type": "realtime", "campaign_id": campaign_id,
        "task_date": datetime.now().date().isoformat(), "claim_token": claim,
        "content": "hello", "msisdn": "251900000001",
    }
    with pytest.raises(SmppSubmissionNotAllowedError):
        connection.send(task)
    assert fake.sent == []


def test_sync_multipart_rejection_is_marked_partial_after_prior_acceptance(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.sequences = iter([201, 202])
            self.sent = []

        def send_message(self, **kwargs):
            sequence = next(self.sequences)
            self.sent.append(sequence)
            return type("Pdu", (), {"sequence": sequence})()

    connection = _SmppConnection({
        "id": 1, "host": "127.0.0.1", "port": 2775,
        "window_size": 1, "tps": 1000, "timeout": 1,
        "delivery_receipt_mode": "optional",
    }, 1)
    connection.client = FakeClient()
    monkeypatch.setattr(connection, "connect", lambda: None)
    monkeypatch.setattr(
        sms_task_service, "record_send_intent",
        lambda task, *args: task.update(_attempt_token="partial-attempt") or "partial-attempt",
    )
    monkeypatch.setattr(sms_task_service, "append_message_id", lambda *args, **kwargs: None)
    task = {"id": 47, "content": "a" * 200, "msisdn": "251900000005"}
    errors = []

    def send():
        try:
            connection.send(task)
        except Exception as exc:  # the assertion below checks the typed result
            errors.append(exc)

    sender = threading.Thread(target=send)
    sender.start()
    for _ in range(100):
        if len(connection.client.sent) == 2:
            break
        time.sleep(0.01)
    connection._store_outcome(type("Pdu", (), {"sequence": 201, "status": 0, "message_id": "sms-47-1"})(), True)
    connection._store_outcome(type("Pdu", (), {"sequence": 202, "status": 8, "message_id": ""})(), False)
    sender.join(timeout=1)

    assert isinstance(errors[0], SmppPartialSubmitError)
    assert errors[0].accepted_parts == 1


def test_atomic_watch_copy_never_publishes_changed_source(tmp_path, monkeypatch):
    source = tmp_path / "source.csv"
    dest = tmp_path / "inbox" / "source.csv"
    dest.parent.mkdir()
    source.write_text("msisdn\n251900000001\n", encoding="utf-8")
    original_copy = campaigns.shutil.copyfile

    def copy_then_mutate(src, target):
        result = original_copy(src, target)
        Path(src).write_text("msisdn\n251900000002\n", encoding="utf-8")
        return result

    monkeypatch.setattr(campaigns.shutil, "copyfile", copy_then_mutate)
    with pytest.raises(campaigns.WatchFileChanged):
        campaigns._copy_watch_file_atomic(str(source), str(dest))
    assert not dest.exists()
    assert not list(dest.parent.glob("*.part"))


def test_watch_registration_failure_removes_published_inbox_file(tmp_path, monkeypatch):
    source = tmp_path / "Android_20261013120000.csv"
    dest = tmp_path / "inbox" / source.name
    dest.parent.mkdir()
    source.write_text("msisdn\n251900000001\n", encoding="utf-8")
    monkeypatch.setattr(campaigns.realtime_whitelist_service, "active_version", lambda _id: None)
    monkeypatch.setattr(
        campaigns.ingestion_job_service, "register_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db registration failed")),
    )
    released = []
    monkeypatch.setattr(
        campaigns.ingestion_job_service, "release_realtime_capacity",
        lambda token: released.append(token),
    )

    assert campaigns._copy_watch_job({
        "campaign_id": 1, "filename": source.name, "src": str(source),
        "dest": str(dest), "campaign_dir": str(tmp_path),
        "reservation_token": "reservation-1",
    }) is False
    assert source.exists()
    assert not dest.exists()
    assert released == ["reservation-1"]


def test_pending_stability_recheck_does_not_wait_for_full_rescan(isolated_db, monkeypatch):
    campaign_id = _insert_realtime_campaign(isolated_db)
    monkeypatch.setattr(campaigns, "WATCH_STABILITY_SECONDS", 1.0)
    monkeypatch.setattr(campaigns, "WATCH_PENDING_RECHECK_SECONDS", 0.01)
    campaigns._watch_scan_state.clear()
    campaigns._watch_pending_stability.clear()
    name = f"Android_{datetime.now():%Y%m%d%H%M%S}.csv"
    source = isolated_db / "watch" / name
    source.write_text("msisdn\n251900000001\n", encoding="utf-8")

    assert campaigns.sync_watch_dirs_to_inboxes(force=True)["moved"] == 0
    time.sleep(1.1)
    result = campaigns.sync_watch_dirs_to_inboxes()
    assert result["moved"] == 1
    assert (isolated_db / "campaign" / "inbox" / name).exists()


def test_schema_resolution_never_falls_back_to_first_column():
    with pytest.raises(campaigns.InputSchemaError, match="missing explicit MSISDN"):
        campaigns._resolve_msisdn_column(["campaign", "language"])
    assert campaigns._resolve_msisdn_column([" name ", "MSISDN", "language"]) == "MSISDN"
