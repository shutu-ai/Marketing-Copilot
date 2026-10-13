from types import SimpleNamespace
import threading
import time

import smpplib.consts

from services import metrics_service, sms_task_service, smpp_gateway
from services.smpp_gateway import _SmppConnection


class FakeClient:
    def __init__(self):
        self.sequences = iter([101, 102, 103])
        self.sent = []

    def send_message(self, **kwargs):
        sequence = next(self.sequences)
        self.sent.append((sequence, kwargs))
        return SimpleNamespace(sequence=sequence)


def _connection(monkeypatch, window_size=2, delivery_receipt_mode="disabled"):
    connection = _SmppConnection({
        "id": 1,
        "host": "127.0.0.1",
        "port": 2775,
        "window_size": window_size,
        "tps": 10000,
        "timeout": 1,
        "delivery_receipt_mode": delivery_receipt_mode,
    }, 1)
    connection.client = FakeClient()
    monkeypatch.setattr(connection, "connect", lambda: None)
    # Gateway unit tests intentionally have no database.  The durable handoff
    # itself is covered by the write-buffer reliability tests; keep this unit
    # focused on SMPP window/response behaviour while still supplying the
    # attempt metadata that the gateway expects after recording an intent.
    def record_send_intent(task, smpp_config_id, slot, planned_parts,
                           connection_generation=""):
        task["_attempt_token"] = f"test-attempt-{task['id']}"
        task["_source_namespace"] = f"smpp:{smpp_config_id}"
        return task["_attempt_token"]

    monkeypatch.setattr(sms_task_service, "record_send_intent", record_send_intent)
    monkeypatch.setattr(sms_task_service, "record_part_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(sms_task_service, "append_message_id", lambda *args, **kwargs: None)
    return connection


def _counter(name, labels):
    return next(
        (
            item["value"]
            for item in metrics_service.snapshot()["counters"]
            if item["name"] == name and item["labels"] == labels
        ),
        0,
    )


def test_async_submit_returns_before_submit_response(monkeypatch):
    connection = _connection(monkeypatch)
    completed = []
    append_calls = []
    sent_before = _counter(
        "marketing_copilot_smpp_submit_sm_sent_total", {"config_id": "1", "slot": "1"}
    )
    response_before = _counter(
        "marketing_copilot_smpp_submit_sm_resp_total",
        {"config_id": "1", "result": "success", "slot": "1"},
    )
    monkeypatch.setattr(
        sms_task_service,
        "append_message_id",
        lambda *args, **kwargs: append_calls.append((args, kwargs)),
    )
    task = {"id": 42, "content": "hello", "msisdn": "251900000001"}

    connection.send_async(task, lambda task, ids, error: completed.append((task, ids, error)))

    for _ in range(100):
        if connection._async_submissions:
            break
        time.sleep(0.01)
    assert completed == []
    assert len(connection._async_submissions) == 1
    assert connection.status()["inflight"] == 1
    assert _counter(
        "marketing_copilot_smpp_submit_sm_sent_total", {"config_id": "1", "slot": "1"}
    ) == sent_before + 1
    assert any(
        item["name"] == "marketing_copilot_smpp_submit_sm_sent_per_second_1m"
        and item["labels"] == {"config_id": "1", "slot": "1"}
        and item["value"] > 0
        for item in metrics_service.snapshot()["rates"]
    )

    connection._store_outcome(SimpleNamespace(sequence=101, status=0, message_id="sms-42"), True)

    assert completed == [(task, ["sms-42"], None)]
    assert append_calls == []
    assert connection.status()["inflight"] == 0
    assert _counter(
        "marketing_copilot_smpp_submit_sm_resp_total",
        {"config_id": "1", "result": "success", "slot": "1"},
    ) == response_before + 1
    assert any(
        item["name"] == "marketing_copilot_smpp_submit_sm_resp_per_second_1m"
        and item["labels"] == {"config_id": "1", "result": "success", "slot": "1"}
        and item["value"] > 0
        for item in metrics_service.snapshot()["rates"]
    )
    assert connection._window.acquire(blocking=False)
    connection._window.release()


def test_async_submit_waits_for_all_multipart_responses(monkeypatch):
    connection = _connection(monkeypatch, window_size=1)
    completed = []
    task = {"id": 43, "content": "a" * 200, "msisdn": "251900000002"}

    connection.send_async(task, lambda task, ids, error: completed.append((task, ids, error)))
    for _ in range(100):
        if connection._async_submissions:
            break
        time.sleep(0.01)
    assert len(connection._async_submissions) == 1

    connection._store_outcome(SimpleNamespace(sequence=101, status=0, message_id="sms-43-1"), True)
    assert completed == []

    connection._store_outcome(SimpleNamespace(sequence=102, status=0, message_id="sms-43-2"), True)

    assert completed == [(task, ["sms-43-1", "sms-43-2"], None)]
    assert task["_submit_sm_parts"] == 2


def test_submit_response_status_is_counted_without_success_log(caplog, monkeypatch):
    connection = _connection(monkeypatch)
    caplog.set_level("INFO")
    task = {"id": 46, "content": "hello", "msisdn": "251900000004"}

    connection.send_async(task, lambda task, ids, error: None)
    for _ in range(100):
        if connection._sequence_tasks:
            break
        time.sleep(0.01)
    connection._store_outcome(
        SimpleNamespace(sequence=101, status=0x58, message_id=""), False,
    )

    status_counter = _counter(
        "marketing_copilot_smpp_submit_sm_resp_status_total",
        {"config_id": "1", "result": "failure", "slot": "1", "status": "0x00000058"},
    )
    assert status_counter >= 1
    assert not any(record.name == "mc.detail" and "SMPP response" in record.message
                   for record in caplog.records)
    assert any("status=0x00000058" in record.message for record in caplog.records)


def test_existing_sync_send_still_waits_and_returns_message_ids(monkeypatch):
    connection = _connection(monkeypatch, delivery_receipt_mode="optional")
    append_calls = []
    monkeypatch.setattr(
        sms_task_service,
        "append_message_id",
        lambda *args, **kwargs: append_calls.append((args, kwargs)),
    )
    task = {"id": 44, "content": "hello", "msisdn": "251900000003"}
    result = []

    sender = threading.Thread(target=lambda: result.append(connection.send(task)))
    sender.start()
    for _ in range(20):
        if connection.client.sent:
            break
        time.sleep(0.01)

    assert sender.is_alive()
    connection._store_outcome(SimpleNamespace(sequence=101, status=0, message_id="sms-44"), True)
    sender.join(timeout=1)

    assert not sender.is_alive()
    assert result == [["sms-44"]]
    # The source-scoped per-part result path supersedes the legacy standalone
    # message-id callback.  It is monkeypatched by this gateway-only unit test.
    assert append_calls == []


def test_sync_flash_send_uses_class_zero_dcs_and_keeps_delivery_mode(monkeypatch):
    connection = _connection(monkeypatch, delivery_receipt_mode="optional")
    completed = []
    task = {"id": 47, "content": "Hello flash", "flash_sms": True,
            "msisdn": "251900000005"}

    sender = threading.Thread(
        target=lambda: completed.append(connection.send(task)), daemon=True,
    )
    sender.start()
    for _ in range(100):
        if connection.client.sent:
            break
        time.sleep(0.01)

    assert connection.client.sent
    _sequence, kwargs = connection.client.sent[0]
    assert kwargs["short_message"] == b"Hello flash"
    assert kwargs["data_coding"] == 0xF0
    assert kwargs["esm_class"] == 0
    assert kwargs["registered_delivery"] is True

    connection._store_outcome(SimpleNamespace(sequence=101, status=0, message_id="flash-47"), True)
    sender.join(timeout=1)
    assert not sender.is_alive()
    assert completed == [["flash-47"]]


def test_async_flash_multipart_pdu_params_match_normal_except_dcs(monkeypatch):
    connection = _connection(monkeypatch, window_size=2, delivery_receipt_mode="disabled")
    monkeypatch.setattr(smpp_gateway.random, "randint", lambda _start, _end: 9)
    text = "你" * 71 + "😀"
    task = {"id": 48, "content": text, "flash_sms": True,
            "msisdn": "251900000006"}
    normal_task = {"id": 49, "content": text, "msisdn": "251900000007"}
    from services.smpp_gateway import _make_task_parts

    normal_parts, normal_coding, normal_esm_class = _make_task_parts(normal_task)
    completed = []
    connection.send_async(task, lambda sent_task, ids, error: completed.append((ids, error)))

    for _ in range(200):
        if len(connection.client.sent) >= len(normal_parts):
            break
        time.sleep(0.01)

    assert len(connection.client.sent) == len(normal_parts)
    assert [item[1]["short_message"] for item in connection.client.sent] == normal_parts
    assert all(item[1]["data_coding"] == 0x18 for item in connection.client.sent)
    assert all(item[1]["esm_class"] == normal_esm_class for item in connection.client.sent)
    assert normal_coding == smpplib.consts.SMPP_ENCODING_ISO10646
    assert all(item[1]["registered_delivery"] is False for item in connection.client.sent)

    for sequence, _kwargs in connection.client.sent:
        connection._store_outcome(
            SimpleNamespace(sequence=sequence, status=0, message_id=f"flash-{sequence}"),
            True,
        )
    for _ in range(100):
        if completed:
            break
        time.sleep(0.01)
    assert completed and completed[0][1] is None


def test_disabled_receipt_mode_does_not_persist_submitted_message_ids(monkeypatch):
    from services import sms_write_buffer

    calls = []
    monkeypatch.setattr(
        sms_write_buffer,
        "submit",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    sms_task_service.mark_submitted(
        45, ["sms-45"], "disabled", wait=False, submit_sm_parts=1,
    )

    assert calls[0][0][0] == "submitted"
    assert calls[0][0][1]["message_ids"] == []
