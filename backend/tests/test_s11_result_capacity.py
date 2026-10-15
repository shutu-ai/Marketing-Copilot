"""S1.1 hard-byte and recovery fences."""

import json
import threading

import pytest

from services import bounded_dispatch_service as dispatch
from services import result_capacity_service as capacity
from services import result_journal_service as journal
from services import sms_write_buffer as buffer


@pytest.fixture
def fair(monkeypatch, tmp_path):
    monkeypatch.setattr(dispatch, "_frozen_mode", "fair")
    monkeypatch.setattr(capacity, "ledger", capacity.Ledger(
        limit=4 * capacity.charge(capacity.MAX_EVENT),
        emergency=2 * capacity.charge(capacity.MAX_EVENT),
    ))
    path = str(tmp_path / "results.jsonl")
    monkeypatch.setenv("MC_SMS_RESULT_JOURNAL_PATH", path)
    monkeypatch.setattr(journal, "_thread", None)
    monkeypatch.setattr(journal, "_stop", threading.Event())
    yield path
    journal.stop(timeout=2)


def test_variable_payload_is_rejected_before_json_materialization(fair):
    # 64 KiB strings are individually legal, but the aggregate envelope is
    # above the 256 KiB hard line budget. This must fail before _encoded_line.
    value = {"items": ["é" * 64000 for _ in range(8)]}
    with pytest.raises(capacity.ResultCapacityError):
        capacity.validate(value)
    state = capacity.ledger.status()
    assert state["live"] == 0
    assert state["used_bytes"].get("normal", 0) <= state["limit_bytes"]


def test_unknown_durable_kind_blocks_replay(fair, monkeypatch):
    monkeypatch.setattr(buffer, "_pending", {})
    with pytest.raises(capacity.ResultCapacityError, match="replay blocked"):
        buffer._restore_event({"event_id": "e1", "kind": "future_kind", "payload": {}}, fair)
    assert capacity.ledger.status()["failure"]


def test_aggregate_payload_rejected_before_event_encoding(fair, monkeypatch):
    value = {"items": ["x" * 32000 for _ in range(8)]}
    original = journal._encoded_line
    event_calls = []

    def guarded(record):
        if record.get("type") == "event":
            event_calls.append(True)
            raise AssertionError("oversized event was materialized")
        return original(record)

    monkeypatch.setattr(journal, "_encoded_line", guarded)
    with pytest.raises(capacity.ResultCapacityError):
        journal.append_event("submitted", value, _handoff=True)
    assert event_calls == []


def test_fsync_handoff_keeps_credit_until_terminal(fair):
    event_id, path, credit = journal.append_event(
        "submitted", {"task_id": 7, "message_ids": []}, _handoff=True,
    )
    assert credit is not None
    assert credit.stage == "durable_handoff"
    assert capacity.ledger.status()["live"] == 1
    journal.append_terminal([event_id], "committed", path)
    capacity.ledger.release(credit)
    assert capacity.ledger.status()["live"] == 0
    assert journal.load_state(path)["pending"] == []


def test_aggregate_fast_checkpoint_uses_dispatch_budget(fair):
    event_id, path, credit = journal.append_event(
        "fast_checkpoint",
        {"items": [
            {"task_id": 7, "claim_token": "c7"},
            {"task_id": 8, "claim_token": "c8"},
        ]},
        _handoff=True,
    )
    assert event_id and path
    assert credit.owner == "dispatch"
    assert capacity.ledger.status()["used_bytes"].get("emergency", 0) == 0
    assert capacity.ledger.safe()
    journal.append_terminal([event_id], "committed", path)
    capacity.ledger.release(credit)
    assert capacity.ledger.status()["live"] == 0


def test_terminal_lane_does_not_consume_normal_dispatch_budget(fair):
    normal = capacity.ledger.acquire(capacity.charge(capacity.MAX_EVENT), "dispatch")
    assert normal is not None
    terminal = capacity.ledger.acquire(capacity.charge(capacity.MAX_EVENT), "terminal")
    assert terminal is not None
    state = capacity.ledger.status()
    assert state["used_bytes"]["normal"] <= state["limit_bytes"]
    assert state["used_bytes"]["terminal"] <= capacity.ledger.terminal_limit
    capacity.ledger.release(normal)
    capacity.ledger.release(terminal)


def test_journal_line_has_crc_and_bounded_framing(fair):
    record = {"type": "event", "event_id": "e2", "kind": "submitted",
              "payload": {"task_id": 3, "message_ids": ["m1"]}}
    raw = journal._encoded_line(record)
    assert len(raw) < capacity.MAX_JSON_BYTES
    envelope = json.loads(raw)
    assert envelope["record"]["version"] == 1
    assert journal._verify_line(raw, 1, fair)["event_id"] == "e2"


def test_unknown_event_and_terminal_state_block_fair_recovery(fair):
    raw_event = journal._encoded_line({
        "type": "event", "event_id": "bad-kind", "kind": "future_kind",
        "payload": {},
    })
    with open(fair, "wb") as handle:
        handle.write(raw_event)
    with pytest.raises(journal.JournalCorruptionError, match="unknown durable"):
        journal.load_state(fair)

    raw_terminal = journal._encoded_line({
        "type": "terminal", "event_ids": ["e"], "state": "future_state",
    })
    with open(fair, "wb") as handle:
        handle.write(raw_terminal)
    with pytest.raises(journal.JournalCorruptionError, match="unknown terminal"):
        journal.load_state(fair)


def test_pressure_rejects_without_loss_then_terminal_releases_credit(fair, monkeypatch):
    monkeypatch.setattr(capacity, "ledger", capacity.Ledger(
        limit=2 * capacity.charge(capacity.MAX_EVENT),
        emergency=capacity.charge(capacity.MAX_EVENT),
    ))
    first_id, path, first_credit = journal.append_event(
        "submitted", {"task_id": 11, "message_ids": []}, _handoff=True,
    )
    with pytest.raises(capacity.ResultCapacityError):
        journal.append_event("submitted", {"task_id": 12, "message_ids": []}, _handoff=True)
    assert capacity.ledger.status()["failure"]
    journal.append_terminal([first_id], "committed", path)
    capacity.ledger.release(first_credit)
    assert capacity.ledger.status()["live"] == 0
