from __future__ import annotations

import time

import pytest

from vnext.projection_writer import (
    ProjectionWriter,
    ProjectionBackpressure,
    ProjectionWriterBusy,
    ProjectionWriterOwnerBusy,
    ProjectionWriterProcess,
    ProgressJournal,
)


def event(event_id: str, state: str = "socket_written", ordinal: int = 1) -> dict:
    return {
        "event_id": event_id, "partition_id": 1, "ordinal": ordinal,
        "campaign_type": "batch", "campaign_id": 10, "route_id": 2,
        "normalized_msisdn": "251900000001", "state": state,
        "parts_written": 1 if state == "socket_written" else 0, "parts_total": 1,
        "error": "" if state == "socket_written" else "route down",
        "taken": 1, "socket_written": int(state == "socket_written"),
        "write_failed": int(state == "write_failed"), "unknown": int(state == "unknown"),
        "created_at": time.time(),
    }


def test_projection_owner_lock_prevents_competing_duckdb_writer(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_VNEXT_PROJECTION_LOCK_WAIT_SECONDS", "0")
    db = tmp_path / "projection.duckdb"
    first = ProjectionWriter(db, batch_size=8)
    second = ProjectionWriter(db, batch_size=8)
    first.start()
    try:
        with pytest.raises(ProjectionWriterOwnerBusy):
            second.start()
        assert second.snapshot()["owner_lock_held"] is False
        assert first.snapshot()["owner_lock_held"] is True
    finally:
        second.stop()
        first.stop()


def test_projection_writer_batches_idempotently_and_replays_journal(tmp_path):
    db = tmp_path / "projection.duckdb"
    journal = ProgressJournal(tmp_path / "progress.jsonl")
    journal.append_many([event("a"), event("b", "write_failed", 2)])
    writer = ProjectionWriter(db, batch_size=2)
    assert writer.replay(journal) == 2
    writer.submit(event("a"))
    assert writer.flush() == 1
    assert writer.snapshot()["duplicates"] == 1
    writer.stop()

    import duckdb
    conn = duckdb.connect(str(db), read_only=True)
    try:
        row = conn.execute(
            "SELECT taken,socket_written,write_failed FROM vnext_projection"
        ).fetchone()
        assert row == (2, 1, 1)
        assert conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0] == 2
    finally:
        conn.close()


def test_task_index_keeps_one_latest_row_without_changing_event_aggregates(tmp_path):
    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=8)
    writer.start()
    try:
        queued = event("logical-1", state="queued", ordinal=1)
        queued.update({
            "event_kind": "task_index",
            "logical_task_id": "logical-1",
            "normalized_msisdn": "251908341307",
            "message": "hello",
            "created_at": 10.0,
            "event_at": 10.0,
        })
        sent = event("logical-1", state="socket_written", ordinal=1)
        sent.update({
            "normalized_msisdn": "251908341307",
            "message": "hello",
            "created_at": 10.0,
            "event_at": 11.0,
        })
        writer.submit(queued)
        writer.submit(sent)
        assert writer.flush() == 2
        task = writer._conn.execute(
            "SELECT event_id,normalized_msisdn,state,message,last_event_at FROM vnext_task_index"
        ).fetchone()
        assert task == ("logical-1", "251908341307", "socket_written", "hello", 11.0)
        assert writer._conn.execute(
            "SELECT COUNT(*) FROM vnext_progress_events"
        ).fetchone()[0] == 1
        assert writer._conn.execute(
            "SELECT socket_written FROM vnext_projection"
        ).fetchone()[0] == 1
    finally:
        writer.stop()


def test_task_index_sidecar_is_readable_while_projection_writer_is_open(tmp_path):
    from vnext.task_index import connect_reader, is_ready

    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=8)
    writer.start()
    try:
        item = event("sidecar-task", state="queued")
        item.update({
            "event_kind": "task_index",
            "logical_task_id": "sidecar-task",
            "normalized_msisdn": "251908341307",
            "message": "hello",
            "business_day": "2026-10-20",
            "event_at": 10.0,
            "created_at": 10.0,
        })
        writer.submit(item)
        assert writer.flush() == 1
        reader = connect_reader(writer.task_index_path)
        assert reader is not None
        try:
            assert is_ready(reader) is True
            row = reader.execute(
                "SELECT event_id,normalized_msisdn FROM vnext_task_index"
            ).fetchone()
            assert tuple(row) == ("sidecar-task", "251908341307")
        finally:
            reader.close()
    finally:
        writer.stop()


def test_projection_writer_ages_duckdb_and_sidecar_in_bounded_window(tmp_path):
    from vnext.task_index import connect_reader

    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=8)
    writer.start()
    try:
        old = event("old-task", state="socket_written")
        old.update({
            "business_day": "2026-10-10",
            "created_at": 100.0,
            "event_at": 101.0,
            "logical_task_id": "old-task",
        })
        current = event("current-task", state="socket_written")
        current.update({
            "business_day": "2026-10-20",
            "created_at": 200.0,
            "event_at": 201.0,
            "logical_task_id": "current-task",
        })
        writer.submit(old)
        writer.submit(current)
        assert writer.flush() == 2

        result = writer.age_expired(
            retention_days=7,
            max_rows=100,
            reference_day="2026-10-21",
        )
        assert result["cutoff_business_day"] == "2026-10-14"
        assert result["task_index_deleted"] == 1
        assert result["progress_events_deleted"] == 1
        assert result["projection_rows_deleted"] == 1
        assert result["sidecar_rows_deleted"] == 1

        assert writer._conn.execute(
            "SELECT event_id FROM vnext_task_index ORDER BY event_id"
        ).fetchall() == [("current-task",)]
        assert writer._conn.execute(
            "SELECT event_id FROM vnext_progress_events ORDER BY event_id"
        ).fetchall() == [("current-task",)]
        assert writer._conn.execute(
            "SELECT COUNT(*) FROM vnext_projection"
        ).fetchone()[0] == 1

        reader = connect_reader(writer.task_index_path)
        assert reader is not None
        try:
            assert [tuple(row) for row in reader.execute(
                "SELECT event_id FROM vnext_task_index ORDER BY event_id"
            ).fetchall()] == [("current-task",)]
        finally:
            reader.close()
    finally:
        writer.stop()


def test_task_index_sidecar_can_be_rebuilt_offline(tmp_path):
    from vnext.task_index import connect_reader, is_ready, rebuild_from_duckdb

    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=8)
    writer.start()
    item = event("rebuild-task", state="queued")
    item.update({
        "event_kind": "task_index",
        "logical_task_id": "rebuild-task",
        "normalized_msisdn": "251908341307",
        "message": "hello",
        "business_day": "2026-10-20",
        "event_at": 10.0,
        "created_at": 10.0,
    })
    writer.submit(item)
    assert writer.flush() == 1
    writer.stop()
    for path in tmp_path.glob("vnext-task-index.sqlite3*"):
        path.unlink()

    result = rebuild_from_duckdb(tmp_path / "projection.duckdb")
    assert result["ready"] is True
    reader = connect_reader(tmp_path / "vnext-task-index.sqlite3")
    assert reader is not None
    try:
        assert is_ready(reader) is True
        assert tuple(reader.execute(
            "SELECT event_id,normalized_msisdn FROM vnext_task_index"
        ).fetchone()) == ("rebuild-task", "251908341307")
    finally:
        reader.close()


def test_projection_snapshot_publishes_day_level_metric_contract(tmp_path):
    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=10)
    writer.start()
    try:
        first = event("day-a")
        first["business_day"] = "2026-10-19"
        second = event("day-b", state="submit_sm_resp_success", ordinal=2)
        second.update({
            "business_day": "2026-10-19",
            "response_success": 1,
            "socket_written": 0,
        })
        writer.submit(first)
        writer.submit(second)
        assert writer.flush() == 2
        daily = writer.snapshot()["daily_totals"]
        assert daily == [{
            "business_day": "2026-10-19",
            "queued": 0,
            "taken": 2,
            "socket_written": 1,
            "socket_written_pdus": 1,
            "write_failed": 0,
            "unknown": 0,
            "cancelled": 0,
            "accepted": 0,
            "expired": 0,
            "response_success": 1,
            "response_error": 0,
        }]
        campaign_daily = writer.snapshot()["campaign_daily_totals"]
        assert campaign_daily == [{
            "business_day": "2026-10-19",
            "campaign_type": "batch",
            "campaign_id": 10,
            "queued": 0,
            "taken": 2,
            "socket_written": 1,
            "socket_written_pdus": 1,
            "write_failed": 0,
            "unknown": 0,
            "cancelled": 0,
            "expired": 0,
            "accepted": 0,
            "response_success": 1,
            "response_error": 0,
        }]
    finally:
        writer.stop()


def test_projection_pause_is_local_and_catches_up_after_resume(tmp_path):
    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=10)
    writer.start()
    writer.submit(event("a"))
    writer.pause(0.05)
    with pytest.raises(ProjectionWriterBusy):
        writer.flush()
    assert writer.snapshot()["queue_depth"] == 1
    time.sleep(0.07)
    assert writer.flush() == 1
    writer.stop()


def test_projection_process_drains_all_events_before_sentinel(tmp_path):
    process = ProjectionWriterProcess(tmp_path / "process", batch_size=16)
    process.start()
    try:
        for index in range(300):
            process.submit(event(f"event-{index}", ordinal=index + 1), timeout=2)
    finally:
        process.stop(timeout=10)

    snapshot = process.snapshot()
    assert snapshot["alive"] is False
    assert snapshot["applied"] == 300
    assert snapshot["queue_depth"] == 0


def test_projection_process_flushes_low_rate_event_before_batch_size(tmp_path):
    process = ProjectionWriterProcess(
        tmp_path / "low-rate", batch_size=128, flush_interval_seconds=0.05,
    )
    process.start()
    try:
        process.submit(event("low-rate"), timeout=2)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and process.snapshot().get("applied", 0) < 1:
            time.sleep(0.02)
        assert process.snapshot()["applied"] == 1
        assert process.snapshot()["flush_interval_seconds"] == 0.05
    finally:
        process.stop(timeout=10)


def test_projection_process_materializes_task_index_batch_without_journals(tmp_path):
    process = ProjectionWriterProcess(
        tmp_path / "task-index-batch", batch_size=16, flush_interval_seconds=0.05,
    )
    process.start()
    try:
        process.submit({
            "event_kind": "task_index_batch",
            "events": [
                {
                    "event_id": "task-index-1",
                    "partition_id": 1,
                    "ordinal": 1,
                    "campaign_type": "batch",
                    "campaign_id": 10,
                    "route_id": 2,
                    "normalized_msisdn": "251900000001",
                    "campaign_name": "batch campaign",
                    "state": "queued",
                    "message": "hello",
                    "created_at": time.time(),
                    "event_at": time.time(),
                    "business_day": "2026-10-20",
                },
                {
                    "event_id": "task-index-2",
                    "partition_id": 1,
                    "ordinal": 2,
                    "campaign_type": "realtime",
                    "campaign_id": 11,
                    "route_id": 3,
                    "normalized_msisdn": "251900000002",
                    "campaign_name": "realtime campaign",
                    "state": "queued",
                    "message": "hi",
                    "created_at": time.time(),
                    "event_at": time.time(),
                    "business_day": "2026-10-20",
                },
            ],
        }, timeout=2)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if process.snapshot().get("queue_depth", 0) == 0:
                break
            time.sleep(0.02)
    finally:
        process.stop(timeout=10)

    import duckdb

    connection = duckdb.connect(str(process.db_path), read_only=True)
    try:
        rows = connection.execute(
            "SELECT event_id, campaign_type, normalized_msisdn "
            "FROM vnext_task_index ORDER BY event_id"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        ("task-index-1", "batch", "251900000001"),
        ("task-index-2", "realtime", "251900000002"),
    ]


def test_vectorized_staging_matches_executemany_and_reports_stage_metrics(tmp_path):
    values = [event(f"vector-{index}", ordinal=index) for index in range(1, 129)]
    values[3]["message"] = None
    values[7]["error"] = None
    summaries = {}
    for mode in ("executemany", "vectorized"):
        writer = ProjectionWriter(
            tmp_path / f"{mode}.duckdb", batch_size=128, staging_mode=mode,
        )
        writer.start()
        try:
            for item in values:
                writer.submit(item)
            assert writer.flush() == len(values)
            summary = writer._conn.execute(
                "SELECT SUM(taken),SUM(socket_written),COUNT(*) FROM vnext_projection"
            ).fetchone()
            summaries[mode] = summary
            metrics = writer.snapshot()["metrics"]
            assert metrics["batches_total"] == 1
            assert metrics["events_new"] == len(values)
            assert metrics["events_replayed"] == 0
            assert metrics["rows_prepare_ns"] >= 0
            assert metrics["staging_prepare_rows_ns"] == metrics["rows_prepare_ns"]
            assert metrics["staging_clear_table_ns"] >= 0
            assert metrics["staging_transpose_ns"] >= 0
            assert metrics["staging_bind_execute_ns"] >= 0
            assert metrics["staging_load_ns"] >= 0
            assert metrics["staging_total_ns"] == metrics["staging_load_ns"]
            assert metrics["staging_batch_count"] == 1
            assert metrics["staging_event_count"] == len(values)
            assert metrics["db_commit_ns"] >= 0
            assert metrics["batch_size_p50"] == len(values)
            assert metrics["staging_batch_ms_p50"] >= 0
            assert metrics["staging_event_us_p95"] >= 0
        finally:
            writer.stop()
    assert summaries["vectorized"] == summaries["executemany"] == (128, 128, 1)


def test_journal_generation_recovery_reclaims_only_acknowledged_segments(tmp_path):
    journal = ProgressJournal(
        tmp_path / "worker-0.jsonl", max_event_bytes=1_024,
        max_bytes=2_400, segment_bytes=1_200,
    )
    values = [event(f"segment-{index}", ordinal=index) for index in range(1, 30)]
    appended = []
    for item in values:
        if not journal.can_append(item):
            break
        journal.append(item)
        appended.append(item)
    assert len(appended) >= 3
    segments = journal.segment_paths()
    assert len(segments) >= 2
    active = segments[-1]
    first = segments[0]
    offsets = {str(first): first.stat().st_size}
    assert journal.reclaim(offsets) == [str(first)]
    assert active.exists()
    assert all(path != first for path in journal.segment_paths())
    assert journal.storage_snapshot(offsets)["reclaimable_bytes"] == 0
    next_index = len(appended)
    while next_index < len(values) and journal.can_append(values[next_index]):
        journal.append(values[next_index])
        next_index += 1
    overflow = event("segment-overflow-0", ordinal=1000)
    overflow_index = 1
    while journal.can_append(overflow):
        journal.append(overflow)
        overflow = event(f"segment-overflow-{overflow_index}", ordinal=1000 + overflow_index)
        overflow_index += 1
    assert journal.storage_snapshot(offsets)["blocked"] is True
    with pytest.raises(ProjectionBackpressure):
        journal.append(overflow)
