from __future__ import annotations

import time

import pytest

from vnext.projection_writer import (
    ProjectionWriter,
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
