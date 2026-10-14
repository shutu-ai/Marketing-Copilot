from datetime import datetime
import threading
import time

import pytest

import database
from services import p1_service, sms_task_service
from services.smpp_gateway import _SmppConnection, SmppSubmissionNotAllowedError


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "multi-campaign.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    yield tmp_path


def _campaign(status="in_progress"):
    conn = database.get_connection()
    row = conn.execute(
        """INSERT INTO batch_campaigns
           (name,status,start_time,end_time,send_start_time,send_end_time,send_days)
           VALUES (?,?,?,?,?,?,?) RETURNING id""",
        (f"campaign-{datetime.now().timestamp()}", status, "", "", "", "", "[]"),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


def _task(campaign_id, ordinal, status="queued"):
    conn = database.get_connection()
    row = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,task_date,next_attempt_at)
           VALUES ('batch',?,?,?,?,?, ?,CURRENT_DATE,CURRENT_TIMESTAMP) RETURNING id""",
        (campaign_id, f"campaign-{campaign_id}", f"2519{campaign_id:04d}{ordinal:04d}",
         "hello", f"isolation-{campaign_id}-{ordinal}", status),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


def _status(task_id):
    conn = database.get_connection()
    try:
        return dict(conn.execute(
            "SELECT status,claim_token,dispatch_generation FROM sms_tasks WHERE id=?",
            (task_id,),
        ).fetchone())
    finally:
        conn.close()


def test_pause_requeues_unstarted_work_and_keeps_other_campaigns_claimable(isolated_db):
    campaign_a = _campaign()
    campaign_b = _campaign()
    campaign_c = _campaign()
    for campaign_id in (campaign_a, campaign_b, campaign_c):
        for ordinal in range(3):
            _task(campaign_id, ordinal)

    result = p1_service.campaign_action("batch", campaign_a, "pause")
    assert result["status"] == "paused"
    claimed = sms_task_service.claim_next_tasks(20)
    assert claimed
    assert {int(task["campaign_id"]) for task in claimed} == {campaign_b, campaign_c}

    # The paused activity's durable queue remains available for resume; the
    # assertion is made against all rows rather than an in-memory snapshot.
    conn = database.get_connection()
    try:
        rows = conn.execute(
            "SELECT status FROM sms_tasks WHERE campaign_id=?", (campaign_a,)
        ).fetchall()
    finally:
        conn.close()
    assert {row["status"] for row in rows} == {"queued"}

    resumed = p1_service.campaign_action("batch", campaign_a, "resume")
    assert resumed["status"] == "in_progress"


def test_pause_fence_denies_pre_authorization_and_cancel_keeps_authorized_attempt(isolated_db):
    campaign_a = _campaign()
    campaign_b = _campaign()
    task_a = _task(campaign_a, 0)
    task_b = _task(campaign_b, 0)

    claimed_a = sms_task_service.claim_next_tasks(1)[0]
    assert claimed_a["id"] == task_a
    p1_service.campaign_action("batch", campaign_a, "pause")
    assert sms_task_service.record_send_intent(
        claimed_a, smpp_config_id=7, slot=0, planned_parts=1,
    ) == ""
    assert _status(task_a)["status"] == "queued"

    claimed_b = sms_task_service.claim_next_tasks(1)[0]
    assert claimed_b["id"] == task_b
    attempt = sms_task_service.record_send_intent(
        claimed_b, smpp_config_id=7, slot=0, planned_parts=1,
    )
    assert attempt
    p1_service.campaign_action("batch", campaign_b, "cancel")
    assert _status(task_b)["status"] == "sending"
    conn = database.get_connection()
    try:
        assert conn.execute(
            "SELECT state FROM sms_send_attempts WHERE attempt_token=?", (attempt,)
        ).fetchone()["state"] == "authorized"
    finally:
        conn.close()


def test_slot_queue_generation_invalidation_is_activity_scoped():
    connection = _SmppConnection({"id": 7, "window_size": 1, "timeout": 1}, 1)
    connection.invalidate_campaign("batch", 10, 2)
    assert connection._is_invalidated_campaign_task({
        "campaign_type": "batch", "campaign_id": 10, "dispatch_generation": 1,
    }) is True
    assert connection._is_invalidated_campaign_task({
        "campaign_type": "batch", "campaign_id": 11, "dispatch_generation": 0,
    }) is False


def test_fake_socket_continues_b_after_a_pause_without_real_smsc(isolated_db, monkeypatch):
    campaign_a = _campaign()
    campaign_b = _campaign()
    _task(campaign_a, 0)
    _task(campaign_b, 0)
    claimed = sms_task_service.claim_next_tasks(2)
    by_campaign = {int(task["campaign_id"]): task for task in claimed}
    task_a, task_b = by_campaign[campaign_a], by_campaign[campaign_b]
    p1_service.campaign_action("batch", campaign_a, "pause")

    class FakeClient:
        def __init__(self):
            self.sent = []

        def send_message(self, **kwargs):
            self.sent.append(kwargs)
            return type("Pdu", (), {"sequence": 9001})()

    connection = _SmppConnection({
        "id": 7, "host": "127.0.0.1", "port": 2775,
        "window_size": 1, "tps": 1000, "timeout": 1,
        "delivery_receipt_mode": "disabled",
    }, 1)
    fake = FakeClient()
    connection.client = fake
    monkeypatch.setattr(connection, "connect", lambda: None)
    with pytest.raises(SmppSubmissionNotAllowedError):
        connection.send(task_a)

    errors = []
    def send_b():
        try:
            connection.send(task_b)
        except Exception as exc:
            errors.append(exc)

    sender = threading.Thread(target=send_b, daemon=True)
    sender.start()
    for _ in range(100):
        if fake.sent and 9001 in connection._sequence_tasks:
            break
        time.sleep(0.01)
    connection._store_outcome(
        type("Pdu", (), {"sequence": 9001, "status": 0, "message_id": "b-9001"})(),
        True,
    )
    sender.join(timeout=1)
    assert not errors
    assert len(fake.sent) == 1
