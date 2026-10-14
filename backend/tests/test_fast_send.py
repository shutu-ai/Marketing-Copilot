from __future__ import annotations

import time
import threading
from types import SimpleNamespace

import pytest

import database
from services import fast_send_service, sms_task_service, sms_write_buffer, task_worker
from services.smpp_gateway import _SmppConnection


@pytest.fixture()
def fast_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_SMS_SEND_MODE", "fast")
    monkeypatch.delenv("MC_ALLOW_MULTI_INSTANCE", raising=False)
    try:
        sms_write_buffer.stop(timeout=2)
    except Exception:
        pass
    fast_send_service.reset_for_tests()
    database.set_database_file(str(tmp_path / "fast.duckdb"))
    database.init_db()
    assert fast_send_service.freeze_mode() == "fast"
    yield tmp_path
    fast_send_service.stop(timeout=2)
    try:
        sms_write_buffer.flush(timeout=2)
    except Exception:
        pass
    try:
        sms_write_buffer.stop(timeout=2)
    except Exception:
        pass
    fast_send_service.reset_for_tests()


def _campaign_and_tasks(count=2):
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('fast', 'in_progress') RETURNING id"
    ).fetchone()["id"]
    conn.executemany(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,task_date,next_attempt_at)
           VALUES ('batch',?,'fast',?,?,?,'queued',CURRENT_DATE,CURRENT_TIMESTAMP)""",
        [
            (campaign_id, f"2519000000{i:02d}", "hello", f"fast-{i}")
            for i in range(count)
        ],
    )
    conn.commit()
    conn.close()
    return campaign_id


def test_send_mode_defaults_to_reliable(monkeypatch):
    monkeypatch.delenv("MC_SMS_SEND_MODE", raising=False)
    monkeypatch.delenv("MC_ALLOW_MULTI_INSTANCE", raising=False)
    fast_send_service.reset_for_tests()
    assert fast_send_service.freeze_mode() == fast_send_service.RELIABLE_MODE
    fast_send_service.reset_for_tests()


def test_fast_mode_rejects_multi_instance(monkeypatch):
    monkeypatch.setenv("MC_SMS_SEND_MODE", "fast")
    monkeypatch.setenv("MC_ALLOW_MULTI_INSTANCE", "1")
    fast_send_service.reset_for_tests()
    with pytest.raises(RuntimeError, match="single-process instance lock"):
        fast_send_service.freeze_mode()
    fast_send_service.reset_for_tests()


def test_fast_claim_has_durable_batch_and_uncertain_attempts(fast_db):
    _campaign_and_tasks(3)
    tasks = sms_task_service.claim_next_fast_batch(3)

    assert len(tasks) == 3
    assert {task["_fast_mode"] for task in tasks} == {True}
    assert len({task["_fast_batch_id"] for task in tasks}) == 1
    conn = database.get_connection()
    try:
        batch = conn.execute(
            "SELECT mode,state,task_count FROM sms_fast_batches"
        ).fetchone()
        items = conn.execute(
            "SELECT state,planned_parts,attempt_token FROM sms_fast_batch_items ORDER BY task_id"
        ).fetchall()
        attempts = conn.execute(
            "SELECT state,planned_parts FROM sms_send_attempts ORDER BY task_id"
        ).fetchall()
    finally:
        conn.close()
    assert dict(batch) == {"mode": "fast", "state": "claimed", "task_count": 3}
    assert [row["state"] for row in items] == ["claimed"] * 3
    assert [row["planned_parts"] for row in items] == [0] * 3
    assert [row["state"] for row in attempts] == ["authorized"] * 3


def test_fast_claim_publishes_local_fence_after_db_commit(fast_db, monkeypatch):
    """Claim and control must not wait on each other through opposite locks."""
    campaign_id = _campaign_and_tasks(1)
    observation_started = threading.Event()
    release_observation = threading.Event()
    claim_result = []
    claim_error = []

    def delayed_observation(task):
        observation_started.set()
        assert release_observation.wait(timeout=2)

    monkeypatch.setattr(
        fast_send_service, "observe_claimed_task", delayed_observation,
    )

    def claim():
        try:
            claim_result.extend(sms_task_service.claim_next_fast_batch(1))
        except Exception as exc:  # pragma: no cover - surfaced below
            claim_error.append(exc)

    claim_thread = threading.Thread(target=claim, daemon=True)
    claim_thread.start()
    assert observation_started.wait(timeout=2)

    transition_result = []
    transition_error = []

    def pause():
        try:
            transition_result.append(
                sms_task_service.transition_campaign_status(
                    "batch", campaign_id, "in_progress", "paused",
                )
            )
        except Exception as exc:  # pragma: no cover - surfaced below
            transition_error.append(exc)

    transition_thread = threading.Thread(target=pause, daemon=True)
    transition_thread.start()
    transition_thread.join(timeout=1)
    release_observation.set()
    claim_thread.join(timeout=2)

    assert not transition_thread.is_alive(), "control is blocked behind claim observation"
    assert not claim_thread.is_alive(), "claim did not finish after observation release"
    assert not transition_error
    assert not claim_error
    assert len(claim_result) == 1
    assert transition_result and transition_result[0]["campaign"]["status"] == "paused"


def test_fast_checkpoint_is_one_durable_event_for_normal_success(fast_db, monkeypatch):
    _campaign_and_tasks(1)
    task = sms_task_service.claim_next_fast_batch(1)[0]
    payload = {
        "task_id": task["id"],
        "batch_id": task["_fast_batch_id"],
        "lease_id": task["_fast_lease_id"],
        "claim_token": task["claim_token"],
        "attempt_token": task["_attempt_token"],
        "source_namespace": "smpp:1",
        "message_ids": [],
        "delivery_receipt_mode": "disabled",
        "transport_smpp_config_id": 1,
        "submit_sm_parts": 3,
        "accepted_parts": 3,
    }
    database.write_transaction(
        lambda conn: sms_task_service._apply_fast_checkpoint(conn, [payload]),
        operation="test.fast.checkpoint",
    )
    conn = database.get_connection()
    try:
        task_row = conn.execute(
            "SELECT status,submit_sm_parts,smsc_message_ids FROM sms_tasks WHERE id=?",
            (task["id"],),
        ).fetchone()
        item = conn.execute(
            "SELECT state,planned_parts,accepted_parts FROM sms_fast_batch_items WHERE task_id=?",
            (task["id"],),
        ).fetchone()
        attempt = conn.execute(
            "SELECT state,accepted_parts FROM sms_send_attempts WHERE task_id=?",
            (task["id"],),
        ).fetchone()
    finally:
        conn.close()
    assert dict(task_row) == {
        "status": "accepted", "submit_sm_parts": 3, "smsc_message_ids": "[]",
    }
    assert dict(item) == {"state": "checkpointed", "planned_parts": 3, "accepted_parts": 3}
    assert dict(attempt) == {"state": "accepted", "accepted_parts": 3}


def test_fast_successes_share_one_checkpoint_event(fast_db):
    _campaign_and_tasks(3)
    tasks = sms_task_service.claim_next_fast_batch(3)
    for task in tasks:
        task.update({
            "_delivery_receipt_mode": "disabled",
            "_transport_smpp_config_id": 1,
            "_source_namespace": "smpp:1",
            "_submit_sm_parts": 1,
            "_fast_planned_parts": 1,
        })
        assert fast_send_service.record_success(task, [])

    assert fast_send_service.status()["checkpoint_pending"] == 3
    assert fast_send_service._flush_once()
    sms_write_buffer.flush(timeout=2)
    conn = database.get_connection()
    try:
        checkpoint_count = conn.execute(
            "SELECT COUNT(*) count FROM sms_result_events WHERE kind='fast_checkpoint'"
        ).fetchone()["count"]
        statuses = conn.execute(
            "SELECT status,COUNT(*) count FROM sms_tasks GROUP BY status"
        ).fetchall()
    finally:
        conn.close()
    assert checkpoint_count == 1
    assert {row["status"]: row["count"] for row in statuses} == {"accepted": 3}


def test_fast_startup_recovery_marks_claim_unknown_and_does_not_reclaim(fast_db):
    _campaign_and_tasks(1)
    task = sms_task_service.claim_next_fast_batch(1)[0]

    assert sms_task_service.recover_fast_claims_on_startup() == 1
    assert sms_task_service.claim_next_fast_batch(1) == []
    conn = database.get_connection()
    try:
        row = conn.execute(
            "SELECT status,failure_class FROM sms_tasks WHERE id=?", (task["id"],)
        ).fetchone()
        attempt = conn.execute(
            "SELECT state FROM sms_send_attempts WHERE task_id=?", (task["id"],)
        ).fetchone()
        item = conn.execute(
            "SELECT state FROM sms_fast_batch_items WHERE task_id=?", (task["id"],)
        ).fetchone()
    finally:
        conn.close()
    assert dict(row) == {"status": "unknown", "failure_class": "delivery_unknown"}
    assert attempt["state"] == "unknown"
    assert item["state"] == "unknown"


def test_fast_socket_path_does_not_call_per_task_record_send_intent(fast_db, monkeypatch):
    _campaign_and_tasks(1)
    task = sms_task_service.claim_next_fast_batch(1)[0]
    task.update({
        "_delivery_receipt_mode": "disabled",
        "_transport_smpp_config_id": 1,
    })

    class FakeClient:
        def __init__(self):
            self.sent = []

        def send_message(self, **kwargs):
            self.sent.append(kwargs)
            return SimpleNamespace(sequence=101)

    connection = _SmppConnection({
        "id": 1, "host": "127.0.0.1", "port": 2775, "window_size": 1,
        "tps": 10000, "timeout": 1, "delivery_receipt_mode": "disabled",
    }, 1)
    connection.client = FakeClient()
    monkeypatch.setattr(connection, "connect", lambda: None)
    monkeypatch.setattr(
        sms_task_service, "record_send_intent",
        lambda *args, **kwargs: pytest.fail("Fast path performed per-task DB authorization"),
    )
    completed = []
    connection.send_async(task, task_worker._async_send_complete)
    for _ in range(100):
        if connection.client.sent:
            break
        time.sleep(0.01)
    assert connection.client.sent
    connection._store_outcome(
        SimpleNamespace(sequence=101, status=0, message_id=""), True,
    )
    for _ in range(100):
        if completed:
            break
        time.sleep(0.01)
    for _ in range(100):
        if fast_send_service.status()["checkpoint_pending"]:
            break
        time.sleep(0.01)
    assert fast_send_service.status()["checkpoint_pending"] == 1
    connection.disconnect()


def test_fast_campaign_barrier_rejects_socket_after_pause(fast_db):
    task = {
        "_fast_mode": True, "_fast_batch_id": "batch-1",
        "campaign_type": "batch", "campaign_id": 1,
        "dispatch_generation": 0,
    }
    fast_send_service.observe_claimed_task({**task, "_fast_campaign_status": "in_progress"})
    with fast_send_service.campaign_control_barrier("batch", 1):
        fast_send_service.publish_campaign_state("batch", 1, "paused", 1)
        with pytest.raises(fast_send_service.FastSendAdmissionDenied):
            with fast_send_service.socket_admission(task):
                pass


def test_fast_pause_requeues_claimed_but_not_started_work(fast_db):
    campaign_id = _campaign_and_tasks(1)
    task = sms_task_service.claim_next_fast_batch(1)[0]

    result = sms_task_service.transition_campaign_status(
        "batch", campaign_id, "in_progress", "paused",
    )
    assert result["tasks"]["requeued"] == 1
    conn = database.get_connection()
    try:
        row = conn.execute(
            "SELECT status,claim_token FROM sms_tasks WHERE id=?", (task["id"],)
        ).fetchone()
        item = conn.execute(
            "SELECT state FROM sms_fast_batch_items WHERE task_id=?", (task["id"],)
        ).fetchone()
        attempt = conn.execute(
            "SELECT state FROM sms_send_attempts WHERE task_id=?", (task["id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "queued"
    assert row["claim_token"] == ""
    assert item["state"] == "deferred"
    assert attempt["state"] == "cancelled"


def test_fast_pause_keeps_socket_admitted_work_owned(fast_db):
    campaign_id = _campaign_and_tasks(1)
    task = sms_task_service.claim_next_fast_batch(1)[0]
    fast_send_service.mark_socket_admitted(task)

    result = sms_task_service.transition_campaign_status(
        "batch", campaign_id, "in_progress", "paused",
    )
    assert result["tasks"]["requeued"] == 0
    conn = database.get_connection()
    try:
        row = conn.execute(
            "SELECT status,claim_token FROM sms_tasks WHERE id=?", (task["id"],)
        ).fetchone()
        item = conn.execute(
            "SELECT state FROM sms_fast_batch_items WHERE task_id=?", (task["id"],)
        ).fetchone()
        attempt = conn.execute(
            "SELECT state FROM sms_send_attempts WHERE task_id=?", (task["id"],)
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "sending"
    assert row["claim_token"] == task["claim_token"]
    assert item["state"] == "claimed"
    assert attempt["state"] == "authorized"

