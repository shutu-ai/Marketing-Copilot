import time

import pytest

import database
from services import (
    campaign_capacity_service, result_journal_service, sms_task_service,
    sms_write_buffer,
)


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "sms-reliability.duckdb"))
    database.init_db()
    campaign_capacity_service._deficits.clear()
    campaign_capacity_service._virtual_service.clear()
    campaign_capacity_service._rate_tokens.clear()
    campaign_capacity_service._rate_updated.clear()
    yield tmp_path
    sms_write_buffer.flush()


def _insert_task(campaign_id, index, content="hello", status="queued",
                 claim_token=""):
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO batch_campaigns(id,name,status,send_days)
           VALUES (?,?,?,?) ON CONFLICT(id) DO NOTHING""",
        (campaign_id, f"reliability-{campaign_id}", "in_progress", "[]"),
    )
    row = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,claim_token)
           VALUES ('batch',?,'reliability',?,?,?, ?,?) RETURNING id""",
        (
            campaign_id, f"25190{campaign_id:03d}{index:04d}", content,
            f"reliability-{campaign_id}-{index}-{time.time_ns()}", status,
            claim_token,
        ),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


def test_claim_and_event_are_one_transaction(isolated_db, monkeypatch):
    task_id = _insert_task(101, 1)

    def fail_event(*_args, **_kwargs):
        raise RuntimeError("injected event failure")

    monkeypatch.setattr(sms_task_service, "_add_task_event_batch", fail_event)
    with pytest.raises(RuntimeError, match="injected event failure"):
        sms_task_service.claim_next_tasks(1)

    conn = database.get_connection()
    row = conn.execute(
        "SELECT status,attempts,claim_token FROM sms_tasks WHERE id=?", (task_id,)
    ).fetchone()
    conn.close()
    assert dict(row) == {"status": "queued", "attempts": 0, "claim_token": ""}


def test_claim_does_not_write_advisory_fairness_row(isolated_db, monkeypatch):
    task_id = _insert_task(102, 1)

    def fail_advisory_write(*_args, **_kwargs):
        raise AssertionError("advisory fairness must not be part of task claim")

    monkeypatch.setattr(
        campaign_capacity_service, "record_activity_event", fail_advisory_write,
    )

    claimed = sms_task_service.claim_next_tasks(1)

    assert [int(task["id"]) for task in claimed] == [task_id]


def test_fair_claim_does_not_let_large_campaign_hide_small_campaign(isolated_db):
    for index in range(100):
        _insert_task(201, index)
    for index in range(5):
        _insert_task(202, index)

    claimed = sms_task_service.claim_next_tasks(20)
    counts = {}
    for task in claimed:
        counts[task["campaign_id"]] = counts.get(task["campaign_id"], 0) + 1

    assert counts[201] > 0
    assert counts[202] == 5


def test_fair_claim_charges_actual_multipart_cost(isolated_db):
    for index in range(30):
        _insert_task(301, index, content="short")
        _insert_task(302, index, content="界" * 300)

    claimed = sms_task_service.claim_next_tasks(24)
    parts = {301: 0, 302: 0}
    tasks = {301: 0, 302: 0}
    for task in claimed:
        campaign_id = int(task["campaign_id"])
        parts[campaign_id] += campaign_capacity_service.task_parts(task)
        tasks[campaign_id] += 1

    assert tasks[301] > tasks[302]
    assert abs(parts[301] - parts[302]) <= 5


def test_old_claim_callback_cannot_overwrite_new_attempt(isolated_db):
    task_id = _insert_task(401, 1, status="sending", claim_token="new-token")

    database.write_transaction(
        lambda conn: sms_task_service._apply_submitted_batch(conn, [{
            "task_id": task_id,
            "message_ids": ["stale-message"],
            "delivery_receipt_mode": "required",
            "transport_smpp_config_id": 1,
            "submit_sm_parts": 1,
            "claim_token": "old-token",
            "attempt_token": "old-attempt",
            "source_namespace": "smpp:1",
        }]),
        operation="test.old_callback",
    )

    conn = database.get_connection()
    task = conn.execute(
        "SELECT status,smsc_message_ids FROM sms_tasks WHERE id=?", (task_id,)
    ).fetchone()
    bindings = conn.execute(
        "SELECT COUNT(*) count FROM sms_message_bindings_v2 WHERE task_id=?", (task_id,)
    ).fetchone()["count"]
    conn.close()
    assert task["status"] == "sending"
    assert task["smsc_message_ids"] == "[]"
    assert bindings == 0


def test_stale_sending_becomes_unknown_not_automatically_retried(isolated_db):
    task_id = _insert_task(501, 1, status="sending", claim_token="claim-501")
    conn = database.get_connection()
    conn.execute(
        "UPDATE sms_tasks SET updated_at=CURRENT_TIMESTAMP - INTERVAL '10 minutes' WHERE id=?",
        (task_id,),
    )
    conn.commit()
    conn.close()

    assert sms_task_service.recover_stale_tasks() == 1
    conn = database.get_connection()
    row = conn.execute(
        "SELECT status,failure_class FROM sms_tasks WHERE id=?", (task_id,)
    ).fetchone()
    conn.close()
    assert row["status"] == "unknown"
    assert row["failure_class"] == "delivery_unknown"


def test_same_message_id_isolated_by_smpp_source(isolated_db):
    task_a = _insert_task(601, 1, status="sending", claim_token="claim-a")
    task_b = _insert_task(602, 1, status="sending", claim_token="claim-b")

    def write(conn):
        sms_task_service._apply_submitted_batch(conn, [
            {
                "task_id": task_a, "message_ids": ["same-id"],
                "delivery_receipt_mode": "required", "transport_smpp_config_id": 1,
                "submit_sm_parts": 1, "claim_token": "claim-a",
                "attempt_token": "attempt-a", "source_namespace": "smpp:1",
            },
            {
                "task_id": task_b, "message_ids": ["same-id"],
                "delivery_receipt_mode": "required", "transport_smpp_config_id": 2,
                "submit_sm_parts": 1, "claim_token": "claim-b",
                "attempt_token": "attempt-b", "source_namespace": "smpp:2",
            },
        ])
        assert sms_task_service._find_task_by_message_id(
            conn, "same-id", "smpp:1"
        )["id"] == task_a
        assert sms_task_service._find_task_by_message_id(
            conn, "same-id", "smpp:2"
        )["id"] == task_b

    database.write_transaction(write, operation="test.source_mapping")


def test_buffer_retries_whole_failed_transaction_without_losing_result(
        isolated_db, monkeypatch):
    task_id = _insert_task(701, 1, status="sending", claim_token="claim-701")
    original = sms_task_service.apply_buffered_writes
    calls = {"count": 0}

    def transient_then_success(conn, items):
        calls["count"] += 1
        if calls["count"] <= 3:
            raise TimeoutError("DuckDB write lock timeout after 5.0s during test")
        return original(conn, items)

    monkeypatch.setattr(sms_task_service, "apply_buffered_writes", transient_then_success)
    sms_task_service.mark_submitted(
        task_id, [], "disabled", wait=False, claim_token="claim-701",
    )
    sms_write_buffer.flush(timeout=15)

    conn = database.get_connection()
    status = conn.execute(
        "SELECT status FROM sms_tasks WHERE id=?", (task_id,)
    ).fetchone()["status"]
    event_count = conn.execute(
        "SELECT COUNT(*) count FROM sms_result_events WHERE task_id=?", (task_id,)
    ).fetchone()["count"]
    conn.close()
    assert calls["count"] >= 4
    assert status == "accepted"
    assert event_count == 1


def test_campaign_parts_rate_limit_bounds_each_scheduler_burst(isolated_db):
    candidates = [
        {
            "id": index, "campaign_type": "batch", "campaign_id": 801,
            "target_smpp_config_id": 1, "content": "short",
            "scheduling_weight": 1.0, "max_parts_tps": 2.0,
        }
        for index in range(10)
    ]
    selected = campaign_capacity_service.select(candidates, 10)
    assert len(selected) == 2


def test_low_rate_policy_does_not_starve_one_multipart_task(isolated_db):
    selected = campaign_capacity_service.select([{
        "id": 1, "campaign_type": "batch", "campaign_id": 802,
        "target_smpp_config_id": 1, "content": "x" * 400,
        "scheduling_weight": 1.0, "max_parts_tps": 1.0,
    }], 1)
    assert len(selected) == 1


def test_capacity_policy_and_active_campaign_snapshot(isolated_db):
    conn = database.get_connection()
    conn.execute("INSERT INTO batch_campaigns(id,name) VALUES (901,'capacity-test')")
    conn.commit()
    conn.close()
    _insert_task(901, 1)

    policy = campaign_capacity_service.upsert_policy("batch", 901, {
        "weight": 2.0,
        "min_parts_tps": 0.0,
        "max_parts_tps": 50.0,
        "max_outstanding_parts": 100,
        "max_pending_tasks": 1000,
        "max_ingest_files": None,
        "enabled": True,
    })
    result = campaign_capacity_service.snapshot()

    assert policy["weight"] == 2.0
    item = next(row for row in result["items"] if row["campaign_id"] == 901)
    assert item["queued"] == 1
    assert item["policy"]["max_parts_tps"] == 50.0


def test_result_journal_compaction_retains_only_replayable_state(
        isolated_db, monkeypatch):
    journal_path = str(isolated_db / "compact-results.jsonl")
    monkeypatch.setenv("MC_SMS_RESULT_JOURNAL_PATH", journal_path)
    monkeypatch.setattr(result_journal_service, "_COMPACT_BYTES", 1)
    monkeypatch.setattr(result_journal_service, "_COMPACT_INTERVAL", 0.0)
    monkeypatch.setattr(result_journal_service, "_last_compaction_at", 0.0)

    event_id, path = result_journal_service.append_event("task_status", {"task_id": 999})
    result_journal_service.append_terminal([event_id], "committed", path)
    state = result_journal_service.load_state(path)

    assert state["pending"] == []
    assert state["quarantined"] == []
    assert state["records"] == 0
