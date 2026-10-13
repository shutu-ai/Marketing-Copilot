"""Focused regressions for bounded SMPP backpressure and RCA evidence."""

from __future__ import annotations

import threading
import time

from services import smpp_gateway
from tools.mixed_campaign_capacity_acceptance import MixedCampaignRunner


def test_send_async_waits_for_transient_queue_pressure(monkeypatch):
    monkeypatch.setenv("MC_SMPP_SEND_QUEUE_CAPACITY", "1")
    connection = smpp_gateway._SmppConnection(
        {"id": 901, "tps": 100, "timeout": 1}, slot=1
    )
    monkeypatch.setattr(connection, "_start_async_sender", lambda: None)
    monkeypatch.setattr(connection, "_start_async_reaper", lambda: None)
    connection._send_queue.put(("occupied", None))

    def release_one():
        time.sleep(0.05)
        connection._send_queue.get_nowait()
        connection._send_queue.task_done()

    releaser = threading.Thread(target=release_one)
    releaser.start()
    started = time.monotonic()
    connection.send_async({"id": 1, "content": "test"}, lambda *_args: None)
    elapsed = time.monotonic() - started
    releaser.join(timeout=1)

    assert elapsed >= 0.04
    queued_task, _callback = connection._send_queue.get_nowait()
    assert queued_task["id"] == 1
    connection._send_queue.task_done()


def test_retry_diagnostics_flags_unknown_or_partial_requeue():
    runner = object.__new__(MixedCampaignRunner)
    rows = [
        {
            "id": 1, "campaign_type": "realtime", "campaign_id": 10,
            "status": "retrying", "attempts": 2, "next_attempt_at": "later",
            "failure_class": "delivery_unknown", "last_smpp_status": -1,
            "error_msg": "submit_sm_resp timeout", "smpp_config_id": 7,
            "transport_smpp_config_id": None,
        },
        {
            "id": 2, "campaign_type": "batch", "campaign_id": 11,
            "status": "retrying", "attempts": 1, "next_attempt_at": "later",
            "failure_class": "partial_submit", "last_smpp_status": 11,
            "error_msg": "partial", "smpp_config_id": 8,
            "transport_smpp_config_id": None,
        },
    ]
    datasets = {
        "task_events": [
            {"task_id": 1, "event_type": "send_failure", "from_status": "sending",
             "to_status": "unknown", "message": "timeout", "detail":
             '{"failure_class":"delivery_unknown"}', "created_at": "now"},
            {"task_id": 2, "event_type": "send_failure", "from_status": "sending",
             "to_status": "partial", "message": "partial", "detail":
             '{"failure_class":"partial_submit"}', "created_at": "now"},
        ],
        "sms_send_attempts": [
            {"task_id": 2, "attempt_token": "a", "planned_parts": 2,
             "accepted_parts": 1, "failed_parts": 1, "state": "partial",
             "created_at": "now", "updated_at": "now"},
        ],
        "sms_send_parts": [],
    }

    def query(sql: str, params=()):
        if "FROM task_events" in sql:
            return datasets["task_events"]
        if "FROM sms_send_attempts" in sql:
            return datasets["sms_send_attempts"]
        if "FROM sms_send_parts" in sql:
            return datasets["sms_send_parts"]
        raise AssertionError(sql)

    runner._db_query = query
    report = runner._retry_diagnostics(rows)

    assert report["safety_status"] == "FAIL"
    assert report["unknown_partial_retry_violations"]
    assert report["partial_fence_violations"]
    assert report["event_integrity"]["invalid_rows"] == 0
