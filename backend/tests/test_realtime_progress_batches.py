"""Both realtime optimization batches, including recovery/fencing regressions."""
import threading

import duckdb
import pandas as pd
import pytest

import database
from services import campaign_capacity_service
from services import ingestion_job_service as jobs
from services import operations_service, realtime_campaign_service as campaigns
from services import realtime_ingestion_state as state
from services import realtime_ingestion_retry_service as retry
from services import realtime_ingestion_worker as worker, sms_task_service
from services import metrics_service
from services import backpressure_service


@pytest.fixture
def job(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DATABASE_FILE", str(tmp_path / "test.duckdb"))
    database.init_db()
    directory = tmp_path / "campaign"
    directory.mkdir()
    path = directory / "inbox.csv"
    path.write_text("msisdn,language\n090001,first\n90001,second\n90002,en\nabc,no\n90001,third\n", encoding="utf-8")
    conn = database.get_connection()
    try:
        cid = conn.execute("INSERT INTO realtime_campaigns(name,status,use_whitelist,campaign_dir) VALUES ('test','in_progress',false,?) RETURNING id", (str(directory),)).fetchone()["id"]
        conn.commit()
    finally:
        conn.close()
    fid, _ = jobs.register_file("realtime", cid, path.name, str(path))
    claimed = jobs.claim_file("realtime", "test-worker")
    pid, _ = operations_service.register_file("realtime", cid, str(path), path.name)
    return dict(claimed, processed_id=pid, path=path, directory=directory)


def rows(job):
    conn = database.get_connection()
    try:
        return (dict(conn.execute("SELECT * FROM ingestion_files WHERE id=?", (job["id"],)).fetchone()),
                dict(conn.execute("SELECT * FROM processed_files WHERE id=?", (job["processed_id"],)).fetchone()))
    finally:
        conn.close()


def test_claim_file_does_not_write_advisory_fairness_row(job, monkeypatch):
    conn = database.get_connection()
    try:
        conn.execute(
            """UPDATE ingestion_files SET status='pending',lease_token='',lease_expires_at=NULL,
               worker_id='' WHERE id=?""",
            (job["id"],),
        )
        conn.commit()
    finally:
        conn.close()

    def fail_advisory_write(*_args, **_kwargs):
        raise AssertionError("advisory fairness must not be part of file claim")

    monkeypatch.setattr(
        campaign_capacity_service, "record_activity_event", fail_advisory_write,
    )

    claimed = jobs.claim_file("realtime", "claim-regression-worker")

    assert claimed and int(claimed["id"]) == int(job["id"])


def test_progress_is_one_atomic_transaction_and_releases_gate(job):
    report = state.Progress(job["id"], job["processed_id"], job["lease_token"])
    assert report("matching", 10, 3, 1, 2)
    file, processed = rows(job)
    assert file["processed_rows"] == processed["processed_rows"] == 10
    conn = database.get_connection()
    conn.execute("DROP TABLE processed_files")
    conn.commit()
    conn.close()
    with pytest.raises(duckdb.CatalogException):
        report("matching", 99, force=True)
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT processed_rows FROM ingestion_files WHERE id=?", (job["id"],)).fetchone()["processed_rows"] == 10
    finally:
        conn.close()
    acquired = []
    def acquire():
        ok = database._write_lock.acquire(timeout=0.1)
        acquired.append(ok)
        if ok:
            database._write_lock.release()
    thread = threading.Thread(target=acquire)
    thread.start()
    thread.join()
    assert acquired == [True]


def test_phase_alternation_cannot_bypass_file_throttle(job, monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(state.time, "monotonic", lambda: clock[0])
    report = state.Progress(job["id"], job["processed_id"], job["lease_token"])
    assert report("matching", 1)
    for i in range(1, 10):
        clock[0] = 10 + i / 10
        assert report("enqueueing_tasks" if i % 2 else "matching", i + 1) is False
    assert rows(job)[0]["processed_rows"] == 1
    clock[0] = 12.1
    assert report("matching", 20)
    assert rows(job)[0]["processed_rows"] == 20


def test_transient_display_write_does_not_fail_file_or_build_whitelist(job, monkeypatch):
    report = state.Progress(job["id"], job["processed_id"], job["lease_token"])
    original = state.transaction
    monkeypatch.setattr(state, "transaction", lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("DuckDB write lock timeout")))
    assert report("loading_index", 22) is False
    assert report.latest[1] == 22
    monkeypatch.setattr(state, "transaction", original)
    assert report("finalizing", 22, force=True)
    assert rows(job)[0]["processed_rows"] == 22
    assert retry.classify(TimeoutError("DuckDB write lock timeout")).error_class == "TRANSIENT_DB"


def test_old_token_cannot_report_retry_complete_or_create_tasks(job):
    conn = database.get_connection()
    conn.execute("UPDATE ingestion_files SET lease_token='new-owner' WHERE id=?", (job["id"],))
    conn.commit()
    conn.close()
    with pytest.raises(state.LeaseLost):
        state.Progress(job["id"], job["processed_id"], job["lease_token"])("matching", 900)
    for status in ("retrying", "completed", "failed"):
        with pytest.raises(state.LeaseLost):
            state.transition(job["id"], job["processed_id"], job["lease_token"], status)
    with pytest.raises(state.LeaseLost):
        state.prepare_chunk(job["id"], 0, 0, 5, job["lease_token"])
    assert rows(job)[0]["status"] == "processing"
    assert rows(job)[1]["processed_rows"] == 0


def test_retry_replay_only_increments_once_and_clears_chunk_lease(job):
    state.prepare_chunk(job["id"], 0, 0, 5, job["lease_token"])
    state.transition(job["id"], job["processed_id"], job["lease_token"], "retrying", error_class="TRANSIENT_DB", error="conflict", delay=60)
    with pytest.raises(state.LeaseLost):
        state.transition(job["id"], job["processed_id"], job["lease_token"], "retrying", delay=60)
    file, processed = rows(job)
    assert file["retry_count"] == processed["retry_count"] == 1
    assert file["status"] == processed["status"] == "retrying"
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT status FROM ingestion_chunks WHERE file_id=?", (job["id"],)).fetchone()["status"] == "pending"
    finally:
        conn.close()


def test_runtime_recovery_respects_healthy_heartbeat_and_fences_old_owner(job):
    state.prepare_chunk(job["id"], 0, 0, 5, job["lease_token"])
    state.FileLease(job["id"], job["lease_token"]).renew()
    assert state.recover_expired_files() == 0
    conn = database.get_connection()
    conn.execute("UPDATE ingestion_files SET lease_expires_at='2000-01-01' WHERE id=?", (job["id"],))
    conn.commit()
    conn.close()
    assert state.recover_expired_files() == 1
    new = jobs.claim_file("realtime", "new-worker")
    assert new["lease_token"] != job["lease_token"]
    assert state.prepare_chunk(new["id"], 0, 0, 5, new["lease_token"])["status"] == "processing"
    with pytest.raises(state.LeaseLost):
        state.FileLease(job["id"], job["lease_token"]).renew()


@pytest.mark.parametrize("whitelist", [True, False])
def test_chunk_bulk_dedup_keeps_first_row_and_cross_chunk_counts(whitelist):
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE seen_msisdns(msisdn TEXT PRIMARY KEY,matched INTEGER NOT NULL)")
        languages = {"90001": "en"}
        first = campaigns._batch_seen_msisdns(conn, pd.Series(["90001", "90001", "", "90002"]), languages, whitelist)
        second = campaigns._batch_seen_msisdns(conn, pd.Series(["90001", "90003", "90003"]), languages, whitelist)
        assert first == ([0] if whitelist else [0, 3], 1, 2 if whitelist else 3)
        assert second == ([] if whitelist else [1], 0, 1 if whitelist else 3)
        assert conn.execute("SELECT COUNT(*),SUM(matched) FROM seen_msisdns").fetchone() == (3, 1 if whitelist else 3)
    finally:
        conn.close()


def test_completed_chunks_replay_after_archive_commit_failure_without_reenqueue(job, monkeypatch):
    monkeypatch.setattr(campaigns, "_realtime_chunk_size", lambda: 2)
    calls = []
    def enqueue(*args, **kwargs):
        calls.extend(args[2])
        return {"success": True, "created": len(args[2]), "created_msisdns": []}
    monkeypatch.setattr(sms_task_service, "enqueue_rows", enqueue)
    original = state.transition
    monkeypatch.setattr(state, "transition", lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("commit failed")))
    with pytest.raises(TimeoutError):
        campaigns.receive_file(job["campaign_id"], str(job["path"]), job["filename"], job["id"], worker_id="test-worker", lease_token=job["lease_token"])
    assert not job["path"].exists()
    assert (job["directory"] / "received" / job["filename"]).exists()
    assert len(calls) == 2 and calls[0]["language"] == "first"
    monkeypatch.setattr(state, "transition", original)
    result = campaigns.receive_file(job["campaign_id"], str(job["path"]), job["filename"], job["id"], worker_id="test-worker", lease_token=job["lease_token"])
    assert result["success"] and result["task_result"]["created"] == 2
    assert result["total_msisdn"] == 2
    assert len(calls) == 2
    file, processed = rows(job)
    assert file["status"] == processed["status"] == "completed"
    with pytest.raises(state.LeaseLost):
        state.Progress(job["id"], job["processed_id"], job["lease_token"])("matching")


def test_failed_recovery_write_does_not_kill_worker_or_quarantine_input(job, monkeypatch):
    monkeypatch.setattr(worker, "_claim_realtime_file", lambda _: job)
    monkeypatch.setattr(campaigns, "receive_file", lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("DuckDB write lock timeout")))
    monkeypatch.setattr(state, "transition", lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("still busy")))
    assert worker.process_once("test-worker") is True
    assert job["path"].is_file()
    assert rows(job)[0]["status"] == "processing"


def _template(job):
    conn = database.get_connection()
    try:
        cid = conn.execute(
            "INSERT INTO campaigns(name,marketing_type,marketing_campaign_id) VALUES ('content','realtime',?) RETURNING id",
            (job["campaign_id"],),
        ).fetchone()["id"]
        conn.execute("INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,'en','Hello {msisdn}')", (cid,))
        conn.commit()
        return dict(conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (job["campaign_id"],)).fetchone())
    finally:
        conn.close()


def test_task_creation_and_chunk_checkpoint_commit_together(job, monkeypatch):
    campaign = _template(job)
    chunk = state.prepare_chunk(job["id"], 0, 0, 2, job["lease_token"])
    original = state.complete_chunk_in_transaction
    monkeypatch.setattr(state, "complete_chunk_in_transaction", lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("checkpoint conflict")))
    arguments = dict(ingestion_lease=(job["id"], job["lease_token"]), ingestion_chunk=chunk)
    with pytest.raises(TimeoutError):
        sms_task_service.enqueue_rows("realtime", campaign, [{"msisdn": "90001"}], job["filename"], **arguments)
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 0
        assert conn.execute("SELECT status FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()["status"] == "processing"
    finally:
        conn.close()
    monkeypatch.setattr(state, "complete_chunk_in_transaction", original)
    result = sms_task_service.enqueue_rows("realtime", campaign, [{"msisdn": "90001"}], job["filename"], **arguments)
    assert result["created"] == 1 and result["chunk_committed"]
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 1
        assert conn.execute("SELECT status,created_tasks FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()["created_tasks"] == 1
    finally:
        conn.close()


def test_actual_enqueue_rejects_stale_owner_after_template_expansion(job, monkeypatch):
    campaign = _template(job)
    original = sms_task_service._select_template
    def steal(template_map, language):
        conn = database.get_connection()
        conn.execute("UPDATE ingestion_files SET lease_token='replacement' WHERE id=?", (job["id"],))
        conn.commit()
        conn.close()
        return original(template_map, language)
    monkeypatch.setattr(sms_task_service, "_select_template", steal)
    with pytest.raises(state.LeaseLost):
        sms_task_service.enqueue_rows("realtime", campaign, [{"msisdn": "90001"}], job["filename"], ingestion_lease=(job["id"], job["lease_token"]))
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 0
    finally:
        conn.close()


def test_runtime_maintenance_runs_recovery_without_restart(job, monkeypatch):
    conn = database.get_connection()
    conn.execute("UPDATE ingestion_files SET lease_expires_at='2000-01-01' WHERE id=?", (job["id"],))
    conn.commit()
    conn.close()
    monkeypatch.setattr(worker, "_last_lease_recovery_at", 0)
    monkeypatch.setattr(worker, "_discard_expired_queued_files", lambda **kw: None)
    monkeypatch.setattr(worker, "_discard_inactive_campaign_files", lambda: None)
    for name in ("migrate_legacy_versions", "repair_active_binding", "repair_active_failed", "build_pending"):
        monkeypatch.setattr(worker.realtime_whitelist_service, name, lambda **kw: None)
    worker._run_maintenance()
    assert rows(job)[0]["status"] == "pending"


def test_bulk_dedup_uses_one_insert_for_2500_rows():
    class CountWrites:
        def __init__(self):
            self.conn = duckdb.connect(":memory:")
            self.conn.execute("CREATE TABLE seen_msisdns(msisdn TEXT PRIMARY KEY,matched INTEGER NOT NULL)")
            self.writes = 0
        def execute(self, sql, *args):
            self.writes += sql.lstrip().upper().startswith("INSERT")
            return self.conn.execute(sql, *args)
        def register(self, *args):
            return self.conn.register(*args)
        def unregister(self, *args):
            return self.conn.unregister(*args)
    wrapped = CountWrites()
    try:
        result = campaigns._batch_seen_msisdns(wrapped, pd.Series([str(900000+i) for i in range(2500)]), {}, True)
        assert result == ([], 0, 0)
        assert wrapped.writes == 1
        assert wrapped.conn.execute("SELECT COUNT(*) FROM seen_msisdns").fetchone()[0] == 2500
    finally:
        wrapped.conn.close()


def test_active_phase_metrics_clear_after_file_leaves_processing(job):
    report = state.Progress(job["id"], job["processed_id"], job["lease_token"])
    report("matching", 5)
    metrics_service.observe_runtime_state()
    def active_matching():
        return next(x["value"] for x in metrics_service.snapshot()["gauges"]
                    if x["name"] == "realtime_inbox_active_phase_files" and x["labels"].get("phase") == "matching")
    assert active_matching() == 1
    state.transition(job["id"], job["processed_id"], job["lease_token"], "retrying", delay=60)
    metrics_service.observe_runtime_state()
    assert active_matching() == 0


def test_four_ingestion_owners_create_tasks_and_checkpoints_without_conflicts(job):
    from concurrent.futures import ThreadPoolExecutor
    campaign = _template(job)
    claimed = []
    for index in range(4):
        source = job["directory"] / f"input-{index}.csv"
        source.write_text("msisdn\n90001\n90002\n", encoding="utf-8")
        jobs.register_file("realtime", campaign["id"], source.name, str(source))
        item = jobs.claim_file("realtime", f"parallel-{index}")
        pid, _ = state.register_processed(campaign["id"], str(source), source.name, item["id"], item["lease_token"])
        claimed.append(dict(item, processed_id=pid))
    def process(item):
        chunk = state.prepare_chunk(item["id"], 0, 0, 2, item["lease_token"])
        state.Progress(item["id"], item["processed_id"], item["lease_token"])("matching", 2)
        result = sms_task_service.enqueue_rows("realtime", campaign, [{"msisdn": "90001"}, {"msisdn": "90002"}], item["filename"],
                                              ingestion_lease=(item["id"], item["lease_token"]), ingestion_chunk=chunk)
        state.transition(item["id"], item["processed_id"], item["lease_token"], "completed", rows=2, created=result["created"])
        return result["created"]
    with ThreadPoolExecutor(max_workers=4) as executor:
        assert list(executor.map(process, claimed)) == [2, 2, 2, 2]
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 8
        assert conn.execute("SELECT COUNT(*) n FROM ingestion_chunks WHERE status='completed'").fetchone()["n"] == 4
    finally:
        conn.close()


def test_real_csv_pipeline_keeps_first_language_and_durable_task_totals(job, monkeypatch):
    _template(job)
    monkeypatch.setattr(campaigns, "_realtime_chunk_size", lambda: 2)
    result = campaigns.receive_file(job["campaign_id"], str(job["path"]), job["filename"], job["id"], worker_id="test", lease_token=job["lease_token"])
    assert result["success"]
    assert result["total_msisdn"] == result["matched_count"] == result["task_result"]["created"] == 2
    assert result["task_result"]["invalid"] == 1
    conn = database.get_connection()
    try:
        tasks = conn.execute("SELECT language FROM sms_tasks ORDER BY id").fetchall()
        assert [x["language"] for x in tasks] == ["first", "en"]
        assert conn.execute("SELECT SUM(created_tasks) n FROM ingestion_chunks").fetchone()["n"] == 2
        assert conn.execute("SELECT COUNT(*) n FROM realtime_campaign_files").fetchone()["n"] == 1
    finally:
        conn.close()


def test_realtime_recovery_budget_splits_large_chunk_and_preserves_checkpoint(job, monkeypatch):
    _template(job)
    job["path"].write_text(
        "msisdn,language\n" + "\n".join(
            f"9000{index},en" for index in range(1, 6)
        ) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(campaigns, "_realtime_chunk_size", lambda: 10)
    calls = []
    progress_flags = []
    original_enqueue = sms_task_service.enqueue_rows

    def traced_enqueue(*args, **kwargs):
        calls.append(len(args[2]))
        progress_flags.append(bool(kwargs.get("ingestion_chunk_progress")))
        return original_enqueue(*args, **kwargs)

    monkeypatch.setattr(sms_task_service, "enqueue_rows", traced_enqueue)
    monkeypatch.setattr(
        backpressure_service,
        "admit_ingestion_batch",
        lambda units, **_kwargs: min(int(units), 2),
    )

    result = campaigns.receive_file(
        job["campaign_id"], str(job["path"]), job["filename"],
        job["id"], worker_id="test-worker", lease_token=job["lease_token"],
    )

    assert result["success"] is True
    assert result["task_result"]["created"] == 5
    assert calls == [2, 2, 1]
    assert progress_flags == [True, True, True]
    conn = database.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) n FROM sms_tasks").fetchone()["n"] == 5
        chunk = conn.execute(
            "SELECT status,created_tasks FROM ingestion_chunks WHERE file_id=?",
            (job["id"],),
        ).fetchone()
        assert chunk["status"] == "completed"
        assert chunk["created_tasks"] == 5
    finally:
        conn.close()


def test_duplicate_worker_completion_preserves_original_file_totals(job, monkeypatch):
    conn = database.get_connection()
    conn.execute("UPDATE processed_files SET status='completed',total_rows=100,created_tasks=9 WHERE id=?", (job["processed_id"],))
    conn.commit()
    conn.close()
    monkeypatch.setattr(worker, "_claim_realtime_file", lambda _: job)
    monkeypatch.setattr(campaigns, "receive_file", lambda *a, **kw: {"success": True, "duplicate_file": True})
    assert worker.process_once("test-worker") is True
    file, processed = rows(job)
    assert file["status"] == processed["status"] == "completed"
    assert processed["total_rows"] == 100 and processed["created_tasks"] == 9
