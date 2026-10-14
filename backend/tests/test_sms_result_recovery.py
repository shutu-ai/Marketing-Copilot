"""Regression coverage for delayed results and stalled journal acknowledgements."""

import queue
import threading
from collections import defaultdict
from types import SimpleNamespace

import duckdb
import pytest

import database
from services import metrics_service, result_journal_service, sms_task_service, sms_write_buffer


@pytest.fixture
def buffer_state(monkeypatch):
    buffer = sms_write_buffer
    monkeypatch.setattr(buffer, "_pending", {})
    monkeypatch.setattr(buffer, "_quarantined", {})
    monkeypatch.setattr(buffer, "_retry_heap", [])
    monkeypatch.setattr(buffer, "_stop", threading.Event())
    monkeypatch.setattr(buffer, "_pending_condition", threading.Condition())
    monkeypatch.setattr(metrics_service, "_gauges", {})
    monkeypatch.setattr(metrics_service, "_counters", defaultdict(float))
    return buffer


def _items(buffer, count, terminal="committed", path="memory", error=""):
    result = [buffer.WriteItem(
        "submitted", {}, event_id=str(index), sequence=index,
        journal_path=path, terminal_state=terminal, terminal_error=error,
        db_committed=terminal == "committed",
    ) for index in range(count)]
    buffer._pending.update((item.event_id, item) for item in result)
    return result


def _counter(name):
    return sum(value for (key, _labels), value in metrics_service._counters.items()
               if key == "marketing_copilot_sms_write_" + name)


@pytest.fixture
def result_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "result-recovery.duckdb"))
    database.init_db()
    conn = database.get_connection()
    task_id = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
           VALUES ('batch',1,'recovery','25190001','hello','recovery-1','sending')
           RETURNING id"""
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    yield task_id
    sms_write_buffer.flush()


def test_ack_outage_groups_1000_events_and_recovers(buffer_state, monkeypatch):
    buffer = buffer_state
    items = _items(buffer, 1000)
    calls = []

    def fail(ids, state, path, **kwargs):
        calls.append((list(ids), kwargs["timeout"]))
        raise TimeoutError("injected journal stall")

    monkeypatch.setattr(result_journal_service, "append_terminal", fail)
    buffer._ack_terminal_items(items)
    assert len(calls) == 1
    assert len(calls[0][0]) == 1000
    assert 0 < calls[0][1] <= 1.0
    assert len(buffer._retry_heap) == 1000
    assert all(not item.done.is_set() for item in items)

    monkeypatch.setattr(result_journal_service, "append_terminal", lambda *a, **k: None)
    buffer._ack_terminal_items(items)
    assert not buffer._pending
    assert all(item.done.is_set() and item.error is None for item in items)


def test_ack_groups_share_one_wait_budget(buffer_state, monkeypatch):
    buffer = buffer_state
    clock = [0.0]
    monkeypatch.setattr(buffer.time, "monotonic", lambda: clock[0])
    items = _items(buffer, 3)
    for index, item in enumerate(items):
        item.journal_path = str(index)
    timeouts = []

    def acknowledge(ids, state, path, **kwargs):
        timeouts.append(kwargs["timeout"])
        clock[0] += 0.6

    monkeypatch.setattr(result_journal_service, "append_terminal", acknowledge)
    buffer._ack_terminal_items(items)
    assert timeouts == pytest.approx([1.0, 0.4])
    assert items[0].done.is_set() and items[1].done.is_set()
    assert not items[2].done.is_set()
    assert [entry[2] for entry in buffer._retry_heap] == [items[2]]


def test_quarantine_groups_preserve_errors(buffer_state, monkeypatch):
    buffer = buffer_state
    items = _items(buffer, 3, terminal="quarantined", error="invalid payload")
    items[-1].terminal_error = "another error"
    calls = []
    monkeypatch.setattr(result_journal_service, "append_terminal",
                        lambda ids, state, path, **kw: calls.append((ids, state, kw)))
    buffer._ack_terminal_items(items)
    assert [len(call[0]) for call in calls] == [2, 1]
    assert all(call[1] == "quarantined" for call in calls)
    assert not buffer._pending
    assert buffer._quarantined == {"0": "invalid payload", "1": "invalid payload",
                                   "2": "another error"}
    assert all(item.done.is_set() and isinstance(item.error, RuntimeError) for item in items)


def test_new_db_work_progresses_before_stalled_ack(buffer_state, monkeypatch):
    buffer = buffer_state
    monkeypatch.setattr(buffer, "MAX_BATCH_SIZE", 2)
    monkeypatch.setattr(buffer, "_queues", {name: queue.Queue() for name in buffer._queue_names})
    monkeypatch.setattr(buffer, "_available", threading.Semaphore(0))
    items = _items(buffer, 2)
    items[1].terminal_state = ""
    items[1].db_committed = False
    order = []
    monkeypatch.setattr(buffer, "_commit_transaction_batch", lambda chunk: order.append("db"))

    def stalled_ack(*args, **kwargs):
        order.append("ack")
        buffer._stop.set()
        raise TimeoutError("injected journal stall")

    monkeypatch.setattr(result_journal_service, "append_terminal", stalled_ack)
    for item in items:
        buffer._queues["smpp_response"].put(item)
        buffer._available.release()
    buffer._run()
    assert order == ["db", "ack"]
    assert len(buffer._retry_heap) == 2
    assert all(item.db_committed for item in items)
    assert buffer._queues["smpp_response"].unfinished_tasks == 0


def test_metrics_observe_each_actual_transaction_once(buffer_state, monkeypatch):
    buffer = buffer_state
    monkeypatch.setattr(buffer, "TRANSACTION_BATCH_SIZE", 250)
    clock = [0.0]
    monkeypatch.setattr(buffer.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(buffer, "_commit_transaction_batch",
                        lambda chunk: clock.__setitem__(0, clock[0] + 1.0))
    items = _items(buffer, 1000, terminal="")
    buffer._commit(items)
    buffer._record_batch_metrics(items[:250], 99.0, None)
    assert _counter("batches_committed_total") == 4
    assert _counter("batch_seconds_count") == 4
    assert _counter("batch_seconds_sum") == 4
    assert _counter("batch_items_count") == 4
    assert _counter("batch_items_sum") == 1000
    assert _counter("committed_total") == 1000


def test_db_commit_before_terminal_marker_replays_once(result_db, monkeypatch):
    payload = {
        "task_id": result_db,
        "message_ids": [],
        "delivery_receipt_mode": "disabled",
        "claim_token": "",
        "attempt_token": "",
        "submit_sm_parts": 1,
    }
    event_id, journal_path = result_journal_service.append_event("submitted", payload)
    committed = sms_write_buffer.WriteItem(
        "submitted", payload, event_id=event_id, journal_path=journal_path,
        sequence=1, db_committed=True, terminal_state="committed",
    )
    sms_write_buffer._commit_transaction_batch([committed])

    conn = database.get_connection()
    assert conn.execute(
        "SELECT status FROM sms_tasks WHERE id=?", (result_db,)
    ).fetchone()["status"] == "accepted"
    conn.close()

    original_append_terminal = result_journal_service.append_terminal
    monkeypatch.setattr(
        result_journal_service,
        "append_terminal",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("injected marker crash")),
    )
    sms_write_buffer._pending = {event_id: committed}
    sms_write_buffer._retry_heap = []
    sms_write_buffer._ack_terminal_items([committed])
    assert result_journal_service.load_state(journal_path)["pending"]

    monkeypatch.setattr(result_journal_service, "append_terminal", original_append_terminal)
    replay = sms_write_buffer.WriteItem(
        "submitted", payload, event_id=event_id, journal_path=journal_path,
        sequence=2, db_committed=False, terminal_state="committed",
    )
    sms_write_buffer._pending = {event_id: replay}
    sms_write_buffer._commit_transaction_batch([replay])
    sms_write_buffer._ack_terminal_items([replay])

    state = result_journal_service.load_state(journal_path)
    conn = database.get_connection()
    task = conn.execute(
        "SELECT status FROM sms_tasks WHERE id=?", (result_db,)
    ).fetchone()
    event_count = conn.execute(
        "SELECT COUNT(*) count FROM sms_result_events WHERE event_id=?", (event_id,)
    ).fetchone()["count"]
    summary_count = conn.execute(
        "SELECT COALESCE(SUM(event_count),0) count FROM task_event_batches WHERE event_type='submitted'"
    ).fetchone()["count"]
    conn.close()
    assert state["pending"] == []
    assert task["status"] == "accepted"
    assert event_count == 1
    assert summary_count == 1


def test_partial_commit_metrics_exclude_failed_chunk(buffer_state, monkeypatch):
    buffer = buffer_state
    monkeypatch.setattr(buffer, "TRANSACTION_BATCH_SIZE", 250)
    clock = [0.0]
    calls = []
    monkeypatch.setattr(buffer.time, "monotonic", lambda: clock[0])

    def commit(chunk):
        calls.append(chunk)
        clock[0] += 1.0 if len(calls) == 1 else 5.0
        if len(calls) == 2:
            raise TimeoutError("DuckDB write lock timeout during test")

    monkeypatch.setattr(buffer, "_commit_transaction_batch", commit)
    items = _items(buffer, 600, terminal="")
    with pytest.raises(buffer._PartialCommitError) as caught:
        buffer._commit(items)
    assert len(caught.value.committed_items) == 250
    assert _counter("batches_committed_total") == 1
    assert _counter("batches_failed_total") == 1
    assert _counter("batch_seconds_sum") == 1
    assert _counter("committed_total") == 250


def test_journal_queue_and_fsync_share_timeout(monkeypatch):
    clock = [0.0]
    waits = []
    monkeypatch.setattr(result_journal_service, "start", lambda: None)
    monkeypatch.setattr(result_journal_service.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(result_journal_service, "_AppendRequest",
                        lambda **kw: SimpleNamespace(error=None, done=SimpleNamespace(
                            wait=lambda timeout: waits.append(timeout) or False)))

    def put(request, timeout):
        assert timeout == 1.0
        clock[0] += 0.75

    monkeypatch.setattr(result_journal_service, "_queue", SimpleNamespace(put=put))
    with pytest.raises(TimeoutError, match="fsync timed out"):
        result_journal_service._append([], "memory", timeout=1.0)
    assert waits == [0.25]


@pytest.fixture
def task_db(monkeypatch):
    raw = duckdb.connect(":memory:")
    raw.execute("""CREATE TABLE sms_tasks (
        id INTEGER, batch_id INTEGER, campaign_type VARCHAR, campaign_id INTEGER,
        status VARCHAR, claim_token VARCHAR, smsc_message_ids VARCHAR,
        error_msg VARCHAR, failure_class VARCHAR, last_smpp_status INTEGER,
        transport_smpp_config_id INTEGER, submit_sm_parts INTEGER,
        submitted_at VARCHAR, delivered_at VARCHAR, updated_at TIMESTAMP,
        next_attempt_at TIMESTAMP)""")
    raw.execute("""INSERT INTO sms_tasks VALUES
        (1,NULL,'batch',1,'unknown','current','[]','stale','delivery_unknown',
         -1,NULL,1,NULL,NULL,CURRENT_TIMESTAMP,NULL)""")
    raw.execute("""CREATE TABLE sms_result_events (
        event_id VARCHAR PRIMARY KEY, kind VARCHAR, task_id INTEGER,
        created_at TIMESTAMP)""")
    conn = database.WrappedConnection(raw)
    monkeypatch.setattr(sms_task_service, "_mapped_message_ids", lambda *a: [])
    monkeypatch.setattr(sms_task_service, "_add_task_event", lambda *a: None)
    monkeypatch.setattr(sms_task_service, "_add_task_event_batch", lambda *a: None)
    monkeypatch.setattr(sms_task_service, "_insert_message_bindings", lambda *a: None)
    yield conn
    conn.close()


@pytest.mark.parametrize("retryable,expected", [(False, "failed"), (True, "retrying")])
def test_definite_late_rejection_reconciles_unknown(task_db, retryable, expected):
    sms_task_service._apply_send_failure(task_db, {
        "id": 1, "attempts": 1, "claim_token": "current",
    }, "explicit rejection", status_code=8, retryable=retryable,
       failure_class="submit_sm_resp")
    row = task_db.execute("SELECT status,error_msg,last_smpp_status FROM sms_tasks").fetchone()
    assert row["status"] == expected
    assert row["error_msg"] == "explicit rejection"
    assert row["last_smpp_status"] == 8


def test_late_failure_is_applied_before_event_is_fenced(task_db, monkeypatch):
    def commit(callback, **kwargs):
        task_db.execute("BEGIN TRANSACTION")
        try:
            callback(task_db)
            task_db.commit()
        except BaseException:
            task_db.rollback()
            raise

    monkeypatch.setattr(sms_write_buffer, "write_transaction", commit)
    events = []
    monkeypatch.setattr(sms_task_service, "_add_task_event", lambda *args: events.append(args))
    item = sms_write_buffer.WriteItem("send_failure", {
        "task": {"id": 1, "attempts": 1, "claim_token": "current"},
        "error": "explicit rejection", "status_code": 8, "retryable": False,
        "failure_class": "submit_sm_resp",
    }, event_id="late-failure")
    sms_write_buffer._commit_transaction_batch([item])
    sms_write_buffer._commit_transaction_batch([item])
    assert task_db.execute("SELECT status FROM sms_tasks").fetchone()["status"] == "failed"
    assert task_db.execute("SELECT COUNT(*) n FROM sms_result_events").fetchone()["n"] == 1
    assert len(events) == 1
    assert events[0][3:5] == ("unknown", "failed")


@pytest.mark.parametrize("token,code", [("old", 8), ("current", None), ("current", -1)])
def test_old_claim_or_uncertain_failure_cannot_requeue_unknown(task_db, token, code):
    sms_task_service._apply_send_failure(task_db, {
        "id": 1, "attempts": 1, "claim_token": token,
    }, "delayed error", status_code=code)
    assert task_db.execute("SELECT status FROM sms_tasks").fetchone()["status"] == "unknown"


def test_known_partial_send_does_not_requeue_unknown(task_db):
    sms_task_service._apply_send_failure(task_db, {
        "id": 1, "attempts": 1, "claim_token": "current", "_accepted_parts": 1,
    }, "partial send", status_code=-1, retryable=True)
    row = task_db.execute("SELECT status,failure_class FROM sms_tasks").fetchone()
    assert dict(row) == {"status": "partial", "failure_class": "partial_submit"}


@pytest.mark.parametrize("mode", ["disabled", "optional", "required"])
def test_old_success_does_not_clear_new_claim_metadata(task_db, mode):
    payload = {"task_id": 1, "claim_token": "old", "message_ids": [],
               "delivery_receipt_mode": mode, "submit_sm_parts": 1}
    if mode == "disabled":
        sms_task_service._apply_disabled_submitted_batch(task_db, [payload])
    else:
        sms_task_service._apply_submitted_batch(task_db, [payload])
    row = task_db.execute("SELECT status,failure_class,last_smpp_status FROM sms_tasks").fetchone()
    assert dict(row) == {"status": "unknown", "failure_class": "delivery_unknown",
                         "last_smpp_status": -1}


@pytest.mark.parametrize("mode,expected", [("disabled", "accepted"),
                                          ("optional", "accepted"),
                                          ("required", "submitted")])
def test_late_success_clears_stale_failure_metadata(task_db, mode, expected):
    payload = {"task_id": 1, "claim_token": "current", "message_ids": [],
               "delivery_receipt_mode": mode, "transport_smpp_config_id": 2,
               "submit_sm_parts": 1}
    if mode == "disabled":
        sms_task_service._apply_disabled_submitted_batch(task_db, [payload])
    else:
        sms_task_service._apply_submitted_batch(task_db, [payload])
    row = task_db.execute(
        "SELECT status,error_msg,failure_class,last_smpp_status FROM sms_tasks").fetchone()
    assert dict(row) == {"status": expected, "error_msg": "", "failure_class": "",
                         "last_smpp_status": None}
