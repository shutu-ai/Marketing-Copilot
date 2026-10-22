from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

import tools.maintain_projection as maintain_projection


def _event(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "partition_id": 1,
        "ordinal": 1,
        "campaign_type": "batch",
        "campaign_id": 10,
        "route_id": 2,
        "normalized_msisdn": "251900000001",
        "language": "",
        "state": "socket_written",
        "parts_written": 1,
        "parts_total": 1,
        "message": "",
        "error": "",
        "created_at": 1.0,
        "business_day": "2026-10-21",
        "expires_at": 0.0,
        "socket_written": 1,
    }


def test_separate_output_root_replays_without_reclaiming_source_journal(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    output = tmp_path / "output"
    progress = source / "progress"
    progress.mkdir(parents=True)
    journal = progress / "worker-0.jsonl"
    journal.write_text(
        json.dumps(_event("maintenance-1"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(maintain_projection, "_service_is_active", lambda _: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "maintain_projection",
            "--root",
            str(source),
            "--output-root",
            str(output),
            "--workers",
            "1",
            "--batch-size",
            "512",
            "--no-reclaim",
            "--no-checkpoint",
            "--progress-seconds",
            "999",
        ],
    )

    assert maintain_projection.main() == 0
    assert journal.exists()

    db = output / "projection" / "vnext-projection.duckdb"
    with duckdb.connect(str(db), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone() == (1,)


def test_separate_output_root_requires_no_reclaim(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"
    (source / "progress").mkdir(parents=True)
    monkeypatch.setattr(maintain_projection, "_service_is_active", lambda _: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "maintain_projection",
            "--root",
            str(source),
            "--output-root",
            str(tmp_path / "output"),
        ],
    )

    assert maintain_projection.main() == 2
    assert "output_root_requires_no_reclaim" in capsys.readouterr().err
