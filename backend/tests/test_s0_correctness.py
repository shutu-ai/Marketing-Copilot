"""Deterministic S0 reproductions. No real SMSC or external network is used."""
from contextlib import contextmanager
from types import SimpleNamespace
import threading
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pytest
import duckdb

import database
from services import fast_send_service as fast, ingestion_job_service as jobs
from services import realtime_ingestion_state as state, sms_task_service as tasks
from services import sms_write_buffer, smpp_gateway


@pytest.fixture
def s0_db(tmp_path, monkeypatch):
    sms_write_buffer.stop(timeout=2)
    fast.reset_for_tests()
    monkeypatch.setenv("MC_SMS_SEND_MODE", "reliable")
    monkeypatch.setenv("MC_STATE_DIR", str(tmp_path / "state"))
    database.set_database_file(str(tmp_path / "s0.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.stop(timeout=2)
    fast.reset_for_tests()


def _task(status="queued", content="hello"):
    def write(conn):
        cid = conn.execute("INSERT INTO batch_campaigns(name,status) VALUES ('s0','in_progress') RETURNING id").fetchone()["id"]
        tid = conn.execute("""INSERT INTO sms_tasks(campaign_type,campaign_id,msisdn,content,dedupe_key,status)
            VALUES ('batch',?,'900000001',?,?,?) RETURNING id""", (cid, content, f"s0-{cid}", status)).fetchone()["id"]
        return cid, tid
    return database.write_transaction(write)


def _file(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("msisdn\n900000001\n")
    cid = database.write_transaction(lambda c: c.execute("INSERT INTO realtime_campaigns(name,status,use_whitelist) VALUES ('s0','in_progress',false) RETURNING id").fetchone()["id"])
    fid, _ = jobs.register_file("realtime", cid, path.name, str(path))
    job = jobs.claim_file("realtime", "s0")
    chunk = state.prepare_chunk(fid, 0, 0, 1, job["lease_token"])
    return job, chunk


@pytest.mark.parametrize("raw", ["completedcompleted", "processingcompleted", "completed-unproven", "garbage"])
@pytest.mark.parametrize("operation", ["prepare", "complete", "file_complete"])
def test_invalid_chunk_never_proves_completion(s0_db, raw, operation):
    job, chunk = _file(s0_db)
    database.write_transaction(lambda c: c.execute("UPDATE ingestion_chunks SET status=?,lease_token='new-owner' WHERE id=?", (raw, chunk["id"])))
    with pytest.raises((ValueError, RuntimeError)):
        if operation == "prepare":
            state.prepare_chunk(job["id"], 0, 0, 1, job["lease_token"])
        elif operation == "complete":
            state.complete_chunk(job["id"], chunk, {"created": 100}, job["lease_token"])
        else:
            state.transition(job["id"], None, job["lease_token"], "completed")
    def read(c):
        assert c.execute("SELECT status FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()["status"] == raw
        assert c.execute("SELECT completed_chunks FROM ingestion_files WHERE id=?", (job["id"],)).fetchone()["completed_chunks"] == 0
        assert c.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 0
    database.read_transaction(read)


@pytest.mark.parametrize("status", ["unknown", "partial", "submitted", "accepted"])
@pytest.mark.parametrize("dlr", ["UNKNOWN", "UNDELIV", "EXPIRED", "ENROUTE", "DELIVRD"])
def test_receipt_cannot_reopen_uncertain_or_written_task(s0_db, status, dlr):
    _, tid = _task(status)
    def apply(c):
        c.execute("UPDATE sms_tasks SET smsc_message_ids='[\"m1\",\"m2\"]' WHERE id=?", (tid,))
        tasks._insert_message_bindings(c, [("smpp:1", "m1", tid, "", 1)])
        tasks._apply_receipt(c, "m1", dlr, "deterministic S0 injection", source_namespace="smpp:1")
        # Same receipt twice must not double-count events or counters.
        tasks._apply_receipt(c, "m1", dlr, "duplicate", source_namespace="smpp:1")
        row = c.execute("SELECT status,receipt_retry_count FROM sms_tasks WHERE id=?", (tid,)).fetchone()
        assert row["status"] not in {"queued", "retrying"}
        assert row["receipt_retry_count"] == 0
        if status in {"unknown", "partial"}:
            assert row["status"] == status
    database.write_transaction(apply)


def test_old_attempt_receipt_cannot_modify_new_claim(s0_db):
    cid, tid = _task()
    old = tasks.claim_next_tasks(1)[0]
    token = tasks.record_send_intent(old, 1, 1, 1)
    def seed(c):
        tasks._apply_part_result(c, tid, old["claim_token"], token, "smpp:1", 1, 101, True, 0, "old-message")
        c.execute("UPDATE sms_tasks SET claim_token='new-claim',status='sending',smsc_message_ids='[]' WHERE id=?", (tid,))
    database.write_transaction(seed)
    def receipt(c):
        tasks._apply_receipt(c, "old-message", "DELIVRD", "late", source_namespace="smpp:1")
        assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (tid,)).fetchone()["status"] == "sending"
    database.write_transaction(receipt)


@pytest.mark.parametrize("target", ["paused", "cancelled", "completed", "window"])
@pytest.mark.parametrize("phase", ["after_authorize", "throttle", "socket_lock", "between_parts"])
@pytest.mark.parametrize("send_path", ["reliable-async", "reliable-sync", "fast-async"])
def test_stop_linearizes_at_socket(s0_db, monkeypatch, target, phase, send_path):
    async_send = send_path != "reliable-sync"
    if send_path == "fast-async":
        monkeypatch.setenv("MC_SMS_SEND_MODE", "fast")
        fast.reset_for_tests()
    cid, _ = _task(content="a" * (200 if phase == "between_parts" else 10))
    task = (tasks.claim_next_fast_batch(1) if send_path == "fast-async" else tasks.claim_next_tasks(1))[0]
    conn = smpp_gateway._SmppConnection({"id": 1, "host": "127.0.0.1", "port": 1, "tps": 10000, "timeout": 1, "delivery_receipt_mode": "disabled"}, 1)
    monkeypatch.setattr(conn, "connect", lambda: None)
    monkeypatch.setattr(conn, "_start_async_reaper", lambda: None)
    sent, errors = [], []
    reached, release = threading.Event(), threading.Event()
    def barrier():
        reached.set()
        assert release.wait(5)
    def send_message(**kwargs):
        sent.append(kwargs)
        return SimpleNamespace(sequence=len(sent))
    conn.client = SimpleNamespace(send_message=send_message)
    authorize = tasks.record_send_intent
    def intent(*args, **kwargs):
        value = authorize(*args, **kwargs)
        if phase == "after_authorize":
            barrier()
        return value
    monkeypatch.setattr(tasks, "record_send_intent", intent)
    prepare = fast.prepare_task
    def fast_intent(*args, **kwargs):
        value = prepare(*args, **kwargs)
        if phase == "after_authorize":
            barrier()
        return value
    monkeypatch.setattr(fast, "prepare_task", fast_intent)
    throttle_calls = []
    def throttle():
        throttle_calls.append(1)
        if phase == "throttle" or (phase == "between_parts" and len(throttle_calls) == 2):
            barrier()
        return True
    monkeypatch.setattr(conn, "_throttle", throttle)
    if phase == "socket_lock":
        lock = threading.Lock()
        @contextmanager
        def delayed_lock():
            barrier()
            with lock:
                yield
        conn._send_lock = delayed_lock()
    def run():
        try:
            if async_send:
                conn._send_windowed_async(task, lambda _task, ids, error: errors.append(error))
            else:
                conn.send(task)
        except Exception as exc:
            errors.append(exc)
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    assert reached.wait(5), errors
    try:
        if target == "window":
            task["_fast_window_deadline_epoch"] = time.time() - 1
        else:
            result = tasks.transition_campaign_status("batch", cid, "in_progress", target)
            assert result["campaign"]["status"] == target
        before = len(sent)
    finally:
        release.set()
    worker.join(5)
    assert not worker.is_alive()
    assert len(sent) == before, "submit_sm was written after stop returned successfully"
    assert errors and errors[0] is not None
    if before:
        assert isinstance(errors[0], smpp_gateway.SmppDeliveryUnknownError)
    else:
        assert isinstance(errors[0], smpp_gateway.SmppSubmissionNotAllowedError)
    if before:
        tasks.mark_send_failure(task, str(errors[0]), -1, retryable=False, failure_class="delivery_unknown")
        def verify_evidence(c):
            assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (task["id"],)).fetchone()["status"] in {"unknown", "partial"}
            assert c.execute("SELECT COUNT(*) n FROM sms_send_attempts WHERE task_id=?", (task["id"],)).fetchone()["n"] == 1
        database.read_transaction(verify_evidence)


@pytest.mark.parametrize("mode", ["reliable", "fast"])
def test_window_expires_while_waiting_for_socket(s0_db, monkeypatch, mode):
    cid, _ = _task()
    task = tasks.claim_next_tasks(1)[0]
    assert tasks.record_send_intent(task, 1, 1, 2)
    if mode == "fast":
        task["_fast_mode"] = True
    task["_fast_window_deadline_epoch"] = time.time() - 1
    with pytest.raises(fast.FastSendAdmissionDenied, match="window"):
        with fast.socket_admission(task):
            pytest.fail("closed window reached socket")


def test_old_claim_cannot_authorize_and_definite_reject_can_retry(s0_db):
    _, tid = _task()
    task = tasks.claim_next_tasks(1)[0]
    stale = dict(task, claim_token="old-claim")
    assert not tasks.record_send_intent(stale, 1, 1, 1)
    assert tasks.record_send_intent(task, 1, 1, 1)
    tasks.mark_send_failure(task, "SMSC explicitly rejected all parts", 0x58, retryable=True)
    database.write_transaction(lambda c: c.execute("UPDATE sms_tasks SET next_attempt_at=CURRENT_TIMESTAMP WHERE id=?", (tid,)))
    claimed = tasks.claim_next_tasks(1)
    assert len(claimed) == 1 and claimed[0]["claim_token"] != task["claim_token"]


def test_receipt_namespace_and_duplicate_success_preserve_counters(s0_db):
    _, tid = _task("submitted")
    def apply(c):
        c.execute("UPDATE sms_tasks SET smsc_message_ids='[\"m1\",\"m2\"]' WHERE id=?", (tid,))
        tasks._insert_message_bindings(c, [("smpp:1", "m1", tid, "", 1), ("smpp:1", "m2", tid, "", 2)])
        tasks._apply_receipt(c, "m1", "DELIVRD", "wrong source", task_id=tid, source_namespace="smpp:2")
        tasks._apply_receipt(c, "m2", "DELIVRD", "one part", source_namespace="smpp:1")
        assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (tid,)).fetchone()["status"] == "submitted"
        tasks._apply_receipt(c, "m1", "DELIVRD", "all parts", source_namespace="smpp:1")
        tasks._apply_receipt(c, "m1", "DELIVRD", "duplicate", source_namespace="smpp:1")
        tasks._apply_receipt(c, "m1", "UNKNOWN", "late negative", source_namespace="smpp:1")
        assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (tid,)).fetchone()["status"] == "delivered"
        assert c.execute("SELECT COUNT(*) n FROM task_events WHERE task_id=? AND to_status='delivered'", (tid,)).fetchone()["n"] == 1
    database.write_transaction(apply)


def test_modern_multipart_receipt_needs_all_planned_parts(s0_db):
    _, tid = _task()
    task = tasks.claim_next_tasks(1)[0]
    token = tasks.record_send_intent(task, 1, 1, 2)
    def apply(c):
        tasks._apply_part_result(c, tid, task["claim_token"], token, "smpp:1", 1, 101, True, 0, "p1")
        c.execute("UPDATE sms_tasks SET status='submitted',smsc_message_ids='[\"p1\"]' WHERE id=?", (tid,))
        tasks._apply_receipt(c, "p1", "DELIVRD", "only accepted part", source_namespace="smpp:1")
        assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (tid,)).fetchone()["status"] == "submitted"
        assert c.execute("SELECT COUNT(*) n FROM sms_send_parts WHERE attempt_token=?", (token,)).fetchone()["n"] == 2
    database.write_transaction(apply)


@pytest.mark.parametrize("entry", ["batch_update", "realtime_update", "realtime_window_update"])
def test_direct_control_entry_closes_reliable_socket(s0_db, entry):
    from services import batch_campaign_service, realtime_campaign_service
    from models.batch_campaign import BatchCampaignUpdate
    from models.realtime_campaign import RealtimeCampaignUpdate
    if entry == "batch_update":
        cid, _ = _task()
        service, model = batch_campaign_service, BatchCampaignUpdate
    else:
        job, _ = _file(s0_db)
        cid = job["campaign_id"]
        database.write_transaction(lambda c: c.execute("""INSERT INTO sms_tasks(campaign_type,campaign_id,msisdn,content,dedupe_key,task_date)
            VALUES ('realtime',?,'900000002','hello','direct-control',CURRENT_DATE)""", (cid,)))
        service, model = realtime_campaign_service, RealtimeCampaignUpdate
    task = tasks.claim_next_tasks(1)[0]
    assert tasks.record_send_intent(task, 1, 1, 1)
    if entry == "realtime_window_update":
        update = model(end_time="2000-01-01 00:00:00")
    else:
        update = model(status="paused")
    assert service.update(cid, update)
    with pytest.raises(fast.FastSendAdmissionDenied):
        with fast.socket_admission(task):
            pytest.fail("direct control API returned while socket gate remained open")


def test_ingest_only_fixed_manifest_exact_business_keys(s0_db, monkeypatch):
    """Ten sources, 100 valid unique rows plus a duplicate each; zero sends."""
    def seed(c):
        cid = c.execute("INSERT INTO realtime_campaigns(name,status,use_whitelist) VALUES ('manifest','in_progress',false) RETURNING id").fetchone()["id"]
        template_cid = c.execute("INSERT INTO campaigns(name,marketing_type,marketing_campaign_id) VALUES ('manifest','realtime',?) RETURNING id", (cid,)).fetchone()["id"]
        c.execute("INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,'en','hello {msisdn}')", (template_cid,))
        return dict(c.execute("SELECT * FROM realtime_campaigns WHERE id=?", (cid,)).fetchone())
    campaign = database.write_transaction(seed)
    manifest, expected = [], set()
    date = datetime.now().astimezone().date().isoformat()
    for index in range(10):
        path = s0_db / f"fixed-{index:02d}.csv"
        rows = [{"msisdn": f"900{index:02d}{j:04d}", "language": "en"} for j in range(100)]
        rows.append(rows[0])
        path.write_text("msisdn,language\n" + "".join(f"{row['msisdn']},en\n" for row in rows))
        fid, _ = jobs.register_file("realtime", campaign["id"], path.name, str(path))
        job = jobs.claim_file("realtime", f"manifest-{index}")
        assert job["id"] == fid
        manifest.append({"file_id": fid, "filename": path.name, "sha256": job["file_hash"], "valid_rows": 100, "raw_rows": 101, "rows": rows, "job": dict(job)})
        expected.update(tasks.stable_task_dedupe_key("realtime", campaign["id"], row["msisdn"], date, f"{path.name}:{job['file_hash']}") for row in rows)
    def forbid_socket(*args, **kwargs):
        pytest.fail("ingest-only performed socket I/O")
    monkeypatch.setattr(smpp_gateway._SmppConnection, "send", forbid_socket)
    monkeypatch.setattr(smpp_gateway._SmppConnection, "send_async", forbid_socket)
    def ingest(item):
        job = item["job"]
        chunk = state.prepare_chunk(job["id"], 0, 0, 101, job["lease_token"])
        kwargs = dict(ingestion_lease=(job["id"], job["lease_token"]), ingestion_chunk=chunk)
        # Projection outage cannot roll back task/checkpoint authority.
        result = tasks.enqueue_rows("realtime", campaign, item["rows"], item["filename"], **kwargs)
        assert result["created"] == 100
        replay = tasks.enqueue_rows("realtime", campaign, item["rows"], item["filename"], **kwargs)
        assert replay["created"] == 0
        with pytest.raises(state.LeaseLost):
            state.complete_chunk(job["id"], chunk, {"created": 999}, "stale-file-token")
        state.transition(job["id"], None, job["lease_token"], "completed", rows=101)
    monkeypatch.setattr(state, "project_processed_file", lambda *a, **kw: None)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(ingest, manifest))
    def verify(c):
        actual = {r["dedupe_key"] for r in c.execute("SELECT dedupe_key FROM sms_tasks").fetchall()}
        assert expected - actual == actual - expected == set()
        assert len(actual) == 1000
        assert c.execute("SELECT COUNT(*) n FROM ingestion_files WHERE status='completed' AND created_tasks=100 AND completed_chunks=1").fetchone()["n"] == 10
        assert c.execute("SELECT COUNT(*) n FROM ingestion_chunks WHERE status='completed' AND created_tasks=100 AND duplicate_rows=1").fetchone()["n"] == 10
        assert c.execute("SELECT COUNT(*) n FROM sms_send_attempts").fetchone()["n"] == 0
        assert c.execute("SELECT COUNT(*) n FROM processed_files").fetchone()["n"] == 0
    database.read_transaction(verify)
    (s0_db / "manifest.json").write_text(json.dumps(manifest, default=str, indent=2))
    repair = state.reconcile_processed_files(repair=True)
    assert repair["consistent"]
    database.read_transaction(verify_authority_after_projection_repair)


def verify_authority_after_projection_repair(c):
    assert c.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 1000
    assert c.execute("SELECT COUNT(*) n FROM processed_files WHERE status='completed'").fetchone()["n"] == 10


@pytest.mark.parametrize("failure", ["catalog", "occ"])
def test_result_metadata_error_never_false_acknowledges_rolled_back_result(s0_db, monkeypatch, failure):
    from services import campaign_capacity_service
    _, tid = _task()
    task = tasks.claim_next_tasks(1)[0]
    token = tasks.record_send_intent(task, 1, 1, 1)
    item = sms_write_buffer.WriteItem("submitted", {
        "task_id": tid, "claim_token": task["claim_token"], "attempt_token": token,
        "delivery_receipt_mode": "disabled", "submit_sm_parts": 1,
    }, event_id=f"s0-metadata-{failure}")
    database.write_transaction(lambda c: c.execute("CREATE TABLE s0_result_hot(id INTEGER PRIMARY KEY,value INTEGER)"))
    database.write_transaction(lambda c: c.execute("INSERT INTO s0_result_hot VALUES (1,0)"))
    calls = []
    def metadata(conn, *args, **kwargs):
        calls.append(conn)
        if len(calls) > 1:
            return
        if failure == "catalog":
            conn.execute("UPDATE s0_missing_metadata_table SET value=1")
        else:
            # A second in-process connection, same owner, writes after the
            # result transaction's snapshot: a real DuckDB OCC conflict.
            other = duckdb.connect(database.get_database_file(), config=database.duckdb_connection_config())
            try:
                other.execute("UPDATE s0_result_hot SET value=value+1 WHERE id=1")
            finally:
                other.close()
            conn.execute("UPDATE s0_result_hot SET value=value+1 WHERE id=1")
    monkeypatch.setattr(campaign_capacity_service, "record_activity_event", metadata)
    if failure == "catalog":
        with pytest.raises(duckdb.CatalogException):
            sms_write_buffer._commit([item])
        assert not item.db_committed
        expected, event_count = "sending", 0
    else:
        sms_write_buffer._commit([item])
        assert item.db_committed
        assert len(calls) == 2
        expected, event_count = "accepted", 1
    def verify(c):
        assert c.execute("SELECT status FROM sms_tasks WHERE id=?", (tid,)).fetchone()["status"] == expected
        assert c.execute("SELECT COUNT(*) n FROM sms_result_events WHERE event_id=?", (item.event_id,)).fetchone()["n"] == event_count
    database.read_transaction(verify)
