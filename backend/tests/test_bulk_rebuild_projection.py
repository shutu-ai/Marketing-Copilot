from __future__ import annotations

import json
import sqlite3
import sys

import duckdb

import tools.bulk_rebuild_projection as bulk_rebuild


def _event(event_id: str, *, state: str = "socket_written", event_at: float = 1.0) -> dict:
    return {
        "event_id": event_id,
        "logical_task_id": event_id.split(":resp:", 1)[0],
        "partition_id": 1,
        "ordinal": int(event_at),
        "campaign_type": "batch",
        "campaign_id": 10,
        "route_id": 2,
        "normalized_msisdn": "251900000001",
        "language": "",
        "campaign_name": "bulk-test",
        "state": state,
        "parts_written": 1,
        "parts_total": 1,
        "message": "",
        "error": "",
        "created_at": event_at,
        "event_at": event_at,
        "business_day": "2026-10-21",
        "socket_written": 1 if state == "socket_written" else 0,
    }


def test_seed_historical_progress_uses_unindexed_raw_staging(tmp_path):
    source = tmp_path / "seed.duckdb"
    source_connection = duckdb.connect(str(source))
    source_connection.execute(
        """CREATE TABLE vnext_progress_events(
            event_id VARCHAR, partition_id INTEGER, ordinal BIGINT,
            campaign_type VARCHAR, campaign_id BIGINT, route_id BIGINT,
            normalized_msisdn VARCHAR, language VARCHAR, state VARCHAR,
            parts_written INTEGER, parts_total INTEGER, message VARCHAR,
            error VARCHAR, created_at DOUBLE, business_day VARCHAR,
            expires_at DOUBLE
        )"""
    )
    source_connection.execute(
        "INSERT INTO vnext_progress_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("historic:resp:1", 1, 2, "batch", 10, 20, "2519", "", "socket_written", 1, 1, "m\x00sg", "", 1.0, "2026-10-28", 0.0),
    )
    source_connection.close()

    target = duckdb.connect(":memory:")
    bulk_rebuild._create_raw_table(target)
    assert bulk_rebuild._seed_historical_progress(target, source) == 1
    assert target.execute(
        "SELECT event_id,logical_task_id,taken,accepted,message FROM _mc_replay_events"
    ).fetchone() == ("historic:resp:1", "historic", 1, 1, "msg")
    target.close()


def test_bulk_rebuild_creates_canonical_tables_and_sidecar(tmp_path, monkeypatch):
    source = tmp_path / "source"
    output = tmp_path / "output"
    progress = source / "progress"
    progress.mkdir(parents=True)
    events = [
        _event("task-1"),
        _event("task-1:resp:1", state="submit_sm_resp_success", event_at=2.0),
        _event("task-1"),  # duplicate evidence must be ignored
    ]
    events[0]["message"] = "safe\x00text"
    events[1]["message"] = "safe\x00text"
    (progress / "worker-0.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in events),
        encoding="utf-8",
    )

    monkeypatch.setattr(bulk_rebuild, "_service_is_active", lambda _: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bulk_rebuild_projection",
            "--root",
            str(source),
            "--output-root",
            str(output),
            "--workers",
            "1",
            "--batch-size",
            "2",
            "--checkpoint-every-events",
            "2",
            "--progress-seconds",
            "999",
        ],
    )

    assert bulk_rebuild.main() == 0

    db = output / "projection" / "vnext-projection.duckdb"
    with duckdb.connect(str(db), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM vnext_projection").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone() == (1,)
        assert connection.execute("SELECT message FROM vnext_task_index").fetchone() == ("safetext",)
        assert connection.execute("SELECT queued,taken,socket_written FROM vnext_projection").fetchone() == (0, 0, 1)

    with sqlite3.connect(output / "projection" / "vnext-task-index.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone() == (1,)

    state = json.loads((output / "bulk-rebuild-state.json").read_text(encoding="utf-8"))
    assert state["ok"] is True
    assert state["checkpointed"] is True
