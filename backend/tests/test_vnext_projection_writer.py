from __future__ import annotations

import time

import pytest

from vnext.projection_writer import (
    ProjectionWriter,
    ProjectionBackpressure,
    ProjectionWriterBusy,
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
            assert metrics["staging_load_ns"] >= 0
            assert metrics["db_commit_ns"] >= 0
            assert metrics["batch_size_p50"] == len(values)
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
