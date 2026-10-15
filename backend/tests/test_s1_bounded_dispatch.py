"""S1 admission/ownership oracles; socket fairness intentionally is not asserted."""

from dataclasses import replace
import os
import queue
import subprocess
import sys
import threading

import pytest

import database
from services import bounded_dispatch_service as dispatch
from services import campaign_capacity_service, fast_send_service, sms_task_service, task_worker
from services.smpp_gateway import SmppGateway, _SmppConnection, SmppSubmissionNotAllowedError


@pytest.fixture(autouse=True)
def isolated_mode(monkeypatch):
    monkeypatch.setattr(dispatch, "_frozen_mode", None)
    monkeypatch.setattr(fast_send_service, "_frozen_mode", None)
    monkeypatch.setenv("MC_CAMPAIGN_DISPATCH_MODE", "legacy")
    monkeypatch.setenv("MC_SMS_SEND_MODE", "reliable")
    monkeypatch.setattr(campaign_capacity_service, "_deficits", {})
    monkeypatch.setattr(campaign_capacity_service, "_virtual_service", {})
    monkeypatch.setattr(campaign_capacity_service, "_rate_tokens", {})
    monkeypatch.setattr(campaign_capacity_service, "_rate_updated", {})


@pytest.fixture
def db(tmp_path, monkeypatch):
    database.set_database_file(str(tmp_path / "s1.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    from services import sms_write_buffer
    monkeypatch.setattr(sms_write_buffer, "status", lambda: {
        "pending_durable": 0, "quarantined": 0, "journal": {"healthy": True}})
    monkeypatch.setattr(fast_send_service, "status", lambda: {"checkpoint_pending": 0})
    monkeypatch.setattr(task_worker.gateway, "_enabled_configs", lambda: [
        {"id": 7, "updated_at": "rev1", "delivery_receipt_mode": "disabled"},
        {"id": 8, "updated_at": "rev1", "delivery_receipt_mode": "disabled"}])
    yield tmp_path


def descriptor(task_id=1, campaign=1, route=7, content="hello"):
    return dict(id=task_id, campaign_type="batch", campaign_id=campaign,
                dispatch_generation=0, content=content, claim_token=f"claim-{task_id}",
                smpp_config_id=route)


def seed(campaign, count=3, route=7, kind="batch"):
    def write(conn):
        table = "batch_campaigns" if kind == "batch" else "realtime_campaigns"
        conn.execute(f"INSERT INTO {table}(id,name,status,send_days) VALUES (?,?,'in_progress','[]')",
                     [campaign, f"S1-{kind}-{campaign}"])
        for i in range(count):
            conn.execute("""INSERT INTO sms_tasks(campaign_type,campaign_id,campaign_name,
                msisdn,content,dedupe_key,smpp_config_id,task_date,next_attempt_at)
                VALUES (?,?,?,'0000000000','hello',?,?,CURRENT_DATE,CURRENT_TIMESTAMP)""",
                         [kind, campaign, f"S1-{campaign}", f"{kind}-{campaign}-{i}", route])
    database.write_transaction(write)


def rows():
    return database.read_transaction(lambda conn: [dict(row) for row in conn.execute(
        "SELECT * FROM sms_tasks ORDER BY id").fetchall()])


def assert_conserved(pool):
    state = pool.status()
    assert state["credits"].get("acquired", 0) - state["credits"].get("released", 0) == state["live"]
    for unit in ("tasks", "parts", "bytes", "result_events", "result_bytes"):
        assert sum(group.get(unit, 0) for group in state["groups"]) == state["used"].get(unit, 0)
        assert 0 <= state["used"].get(unit, 0) <= state["limits"][unit]
    for unit in ("prepare_tasks", "prepare_bytes"):
        assert 0 <= state["used"].get(unit, 0) <= state["limits"][unit]


def test_flag_default_frozen_and_independent(monkeypatch):
    monkeypatch.delenv("MC_CAMPAIGN_DISPATCH_MODE")
    monkeypatch.setenv("MC_FAIR_TASK_SCHEDULER_ENABLED", "0")
    assert dispatch.freeze_mode() == "legacy"
    monkeypatch.setenv("MC_CAMPAIGN_DISPATCH_MODE", "fair")
    assert dispatch.mode() == dispatch.freeze_mode() == "legacy"
    monkeypatch.setattr(dispatch, "_frozen_mode", None)
    assert dispatch.freeze_mode() == "fair"
    assert not campaign_capacity_service.enabled()


@pytest.mark.parametrize("value", ["", "fifo", "FAIR-ish"])
def test_invalid_flag_refuses_start(value, monkeypatch):
    monkeypatch.setenv("MC_CAMPAIGN_DISPATCH_MODE", value)
    with pytest.raises(ValueError):
        dispatch.freeze_mode()


@pytest.mark.parametrize("unit,value", [
    ("tasks", 1), ("parts", 1), ("bytes", 20000),
    ("group_tasks", 1), ("group_parts", 1), ("group_bytes", 20000),
    ("campaign_tasks", 1), ("route_tasks", 1),
    ("result_events", 6), ("result_bytes", 50000),
])
def test_each_credit_limit_is_enforced(unit, value):
    pool = dispatch.ReadyPool(replace(dispatch.Limits(), **{unit: value}))
    first = pool.reserve(descriptor(), 7, 1)
    assert first
    assert pool.reserve(descriptor(2), 7, 1) is None
    assert_conserved(pool)
    assert pool.release(first)
    assert not pool.release(first)
    assert pool.status()["live"] == 0
    assert_conserved(pool)


def test_route_zero_cannot_bypass_physical_credits():
    pool = dispatch.ReadyPool()
    with pytest.raises(ValueError):
        pool.reserve(descriptor(), 0, 1)


def test_campaign_does_not_multiply_share_by_route():
    pool = dispatch.ReadyPool(replace(dispatch.Limits(), campaign_tasks=1))
    assert pool.reserve(descriptor(), 7, 1)
    assert pool.reserve(descriptor(2), 8, 1) is None
    assert pool.reserve(descriptor(3, campaign=2), 8, 1)
    assert_conserved(pool)


@pytest.mark.parametrize("kind", ["batch", "realtime"])
@pytest.mark.parametrize("route", [7, 8])
@pytest.mark.parametrize("send_mode", ["reliable", "fast"])
def test_a_full_stays_in_db_b_c_claim_and_prepare(db, monkeypatch, route, kind, send_mode):
    monkeypatch.setenv("MC_SMS_SEND_MODE", send_mode)
    for campaign in (1, 2, 3):
        seed(campaign, route=7 if campaign == 1 else route, kind=kind)
    pool = dispatch.ReadyPool(replace(dispatch.Limits(), campaign_tasks=1, group_tasks=1))
    assert task_worker._fair_claim_once(pool) == 3
    # All three shares remain live; another dispatcher pass must not claim A.
    assert task_worker._fair_claim_once(pool) == 0
    data = rows()
    assert sum(row["status"] == "sending" for row in data) == 3
    assert sum(row["status"] == "queued" and row["attempts"] == 0 for row in data) == 6
    prepared = [pool.get(timeout=.1) for _ in range(3)]
    assert {task["campaign_id"] for task in prepared} == {1, 2, 3}
    assert all(task["_dispatch_reservation"].group[2] > 0 for task in prepared)
    assert_conserved(pool)
    pool.close()
    pool.abandon()
    assert_conserved(pool)


def test_candidate_keyset_reaches_beyond_window(db):
    for campaign in range(1, 8):
        seed(campaign, count=1)
    observed = set()
    for _ in range(4):
        batch = sms_task_service.fair_dispatch_candidates(2, 128 * 1024)
        assert len(batch) <= 2
        observed.update(task["campaign_id"] for task in batch)
    assert observed == set(range(1, 8))
    assert {row["status"] for row in rows()} == {"queued"}


def test_claim_commit_failure_releases_without_consuming_attempt(db, monkeypatch):
    seed(1, count=1)
    original = sms_task_service.write_transaction
    def fail(callback, **kwargs):
        if kwargs.get("operation") == "sms_tasks.claim":
            def rollback(conn):
                callback(conn)
                raise RuntimeError("injected commit failure")
            return original(rollback, **kwargs)
        return original(callback, **kwargs)
    monkeypatch.setattr(sms_task_service, "write_transaction", fail)
    pool = dispatch.ReadyPool()
    with pytest.raises(RuntimeError, match="commit failure"):
        task_worker._fair_claim_once(pool)
    assert rows()[0]["status"] == "queued" and rows()[0]["attempts"] == 0
    assert pool.qsize() == pool.status()["live"] == 0
    assert_conserved(pool)


@pytest.mark.parametrize("send_mode", ["reliable", "fast"])
def test_publish_failure_preserves_claim_for_conservative_recovery(db, monkeypatch, send_mode):
    monkeypatch.setenv("MC_SMS_SEND_MODE", send_mode)
    seed(1, count=1)
    pool = dispatch.ReadyPool()
    def fail(*args):
        raise RuntimeError("injected publish failure")
    monkeypatch.setattr(pool, "publish", fail)
    with pytest.raises(RuntimeError, match="publish failure"):
        task_worker._fair_claim_once(pool)
    assert rows()[0]["status"] == "sending"
    assert rows()[0]["claim_token"]
    assert pool.qsize() == pool.status()["live"] == 0
    if send_mode == "fast":
        sms_task_service.recover_fast_claims_on_startup()
    else:
        database.write_transaction(lambda conn: conn.execute(
            "UPDATE sms_tasks SET updated_at=CURRENT_TIMESTAMP - INTERVAL '6 minutes'"))
        sms_task_service.recover_stale_tasks()
    assert rows()[0]["status"] == "unknown"
    assert sms_task_service.claim_next_tasks(1) == []
    assert_conserved(pool)


def test_process_exit_between_claim_commit_and_publish(tmp_path):
    # Child is the only owner. Parent opens only after waitpid has completed.
    path = tmp_path / "crash.duckdb"
    script = """
import os, database
from services import sms_task_service
database.set_database_file(os.environ['S1_CRASH_DB'])
database.init_db()
c=database.get_connection()
c.execute("INSERT INTO batch_campaigns(id,name,status,send_days) VALUES (1,'crash','in_progress','[]')")
c.execute("INSERT INTO sms_tasks(campaign_type,campaign_id,msisdn,content,dedupe_key,task_date,next_attempt_at) VALUES ('batch',1,'0000000000','hello','crash-key',CURRENT_DATE,CURRENT_TIMESTAMP)")
c.commit(); c.close()
t=sms_task_service.fair_dispatch_candidates(1,131072)
assert sms_task_service.claim_next_tasks(1,send_mode='reliable',_reserved_tasks=t)
os._exit(73)
"""
    env = dict(os.environ, S1_CRASH_DB=str(path), PYTHONPATH=str(database.__file__.rsplit(os.sep, 1)[0]))
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, timeout=30)
    assert result.returncode == 73, result.stderr.decode(errors="replace")
    database.set_database_file(str(path))
    assert rows()[0]["status"] == "sending"
    database.write_transaction(lambda conn: conn.execute(
        "UPDATE sms_tasks SET updated_at=CURRENT_TIMESTAMP - INTERVAL '6 minutes'"))
    sms_task_service.recover_stale_tasks()
    assert rows()[0]["status"] == "unknown"
    assert sms_task_service.claim_next_tasks(1) == []


def test_saturated_slot_is_nonblocking_without_callback_or_reclaim(monkeypatch):
    connection = _SmppConnection({"id": 7}, 1)
    connection._send_queue = queue.Queue(maxsize=1)
    connection._send_queue.put(({}, None))
    monkeypatch.setattr(connection, "_start_async_sender", lambda: None)
    monkeypatch.setattr(connection, "_start_async_reaper", lambda: None)
    assert connection.try_send_async({}, lambda *args: pytest.fail("queue full is not a result")) is False


def test_retry_ready_releases_prepare_and_other_group_runs():
    pool = dispatch.ReadyPool(replace(dispatch.Limits(), prepare_tasks=1))
    for campaign in (1, 2):
        task = descriptor(campaign, campaign)
        item = pool.reserve(task, 7, 1)
        pool.publish(item, task)
    a = pool.get()
    assert pool.retry_ready(a["_dispatch_reservation"])
    b = pool.get()
    assert b["campaign_id"] == 2
    assert pool.status()["used"]["prepare_tasks"] == 1
    assert_conserved(pool)
    pool.close()
    pool.abandon()
    assert_conserved(pool)


def test_duplicate_and_late_callback_do_not_finalize_or_release_twice(monkeypatch):
    pool = dispatch.ReadyPool()
    task = descriptor()
    item = pool.reserve(task, 7, 1)
    pool.publish(item, task)
    pool.get()
    calls = []
    monkeypatch.setattr(sms_task_service, "mark_submitted", lambda *a, **kw: calls.append(kw))
    threads = [threading.Thread(target=task_worker._async_send_complete, args=(task, ["synthetic"], None))
               for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive()
    assert len(calls) == 1
    assert pool.status()["live"] == 1  # fsync/DB drain is a separate boundary
    pool.retire_results(pool.result_candidates())
    task_worker._async_send_complete(task, ["synthetic"], None)
    assert len(calls) == 1
    assert_conserved(pool)


def test_earlier_empty_buffer_sample_cannot_retire_later_completion():
    pool = dispatch.ReadyPool()
    task = descriptor()
    item = pool.reserve(task, 7, 1)
    pool.publish(item, task)
    pool.get()
    before = pool.result_candidates()
    assert pool.begin_completion(item)
    pool.completed(item)
    pool.retire_results(before)
    assert pool.status()["live"] == 1
    pool.retire_results(pool.result_candidates())
    assert pool.status()["live"] == 0


def test_result_handoff_failure_pins_credit_and_closes_admission(monkeypatch):
    pool = dispatch.ReadyPool()
    task = descriptor()
    item = pool.reserve(task, 7, 1)
    pool.publish(item, task)
    pool.get()
    def fail(*a, **kw):
        raise OSError("fsync failed")
    monkeypatch.setattr(sms_task_service, "mark_submitted", fail)
    task_worker._async_send_complete(task, [], None)
    assert pool.status()["failure"] == "result_handoff_failed"
    assert pool.reserve(descriptor(2, 2), 8, 1) is None
    assert_conserved(pool)


@pytest.mark.parametrize("reason", ["disk", "results", "database_maintenance", "writes"])
def test_common_fault_stops_fair_socket_admission(reason, monkeypatch):
    from services import backpressure_service
    pool = dispatch.ReadyPool()
    task = descriptor()
    item = pool.reserve(task, 7, 1)
    pool.publish(item, task)
    monkeypatch.setattr(backpressure_service, "reasons", lambda **kw: ([reason], {}))
    with pytest.raises(fast_send_service.FastSendAdmissionDenied):
        dispatch.assert_delivery_safe(task)


def test_direct_and_rabbitmq_cannot_bypass_fair_admission(monkeypatch):
    from services import rabbitmq_service, sms_sender
    monkeypatch.setenv("MC_CAMPAIGN_DISPATCH_MODE", "fair")
    gateway = SmppGateway()
    with pytest.raises(SmppSubmissionNotAllowedError):
        gateway.send({})
    with pytest.raises(SmppSubmissionNotAllowedError):
        gateway.send_async({}, None)
    with pytest.raises(RuntimeError, match="fair"):
        sms_sender.process_message("campaign.test", {})
    with pytest.raises(RuntimeError, match="fair"):
        rabbitmq_service.start_consumer(None)


def test_route_revision_change_rejected_before_connect(monkeypatch):
    gateway = SmppGateway()
    pool = dispatch.ReadyPool()
    task = descriptor()
    item = pool.reserve(task, 7, 1)
    pool.publish(item, task)
    pool.get()
    task["_dispatch_route_revision"] = "old"
    monkeypatch.setattr(gateway, "_enabled_configs", lambda: [{"id": 7, "updated_at": "new"}])
    monkeypatch.setattr(gateway, "_connection_for", lambda *a, **kw: pytest.fail("must not connect"))
    with pytest.raises(SmppSubmissionNotAllowedError, match="route changed"):
        gateway.try_send_async(task, None)


def test_shutdown_retains_live_epoch_and_refuses_restart(monkeypatch):
    class Alive:
        def is_alive(self):
            return True
        def join(self, **kwargs):
            pass
    old = Alive()
    monkeypatch.setattr(task_worker, "_workers", {0: old})
    monkeypatch.setattr(task_worker, "_supervisor", None)
    monkeypatch.setattr(task_worker, "_claim_dispatcher", None)
    monkeypatch.setattr(task_worker, "_task_queue", dispatch.ReadyPool())
    monkeypatch.setattr(task_worker, "_stop", threading.Event())
    monkeypatch.setattr(task_worker.gateway, "disconnect", lambda: None)
    with pytest.raises(RuntimeError, match="old epoch"):
        task_worker.stop_worker(.5)
    assert task_worker._workers[0] is old
    with pytest.raises(RuntimeError, match="epoch"):
        task_worker.start_worker()
