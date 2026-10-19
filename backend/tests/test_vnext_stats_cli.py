from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from tools.vnext_stats import _RateTracker, _build_snapshot


REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "backend" / "tools" / "vnext_stats.py"


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _fake_runtime_root(tmp_path: Path) -> Path:
    root = tmp_path / "runtime"
    now = time.time()
    _write(
        root / "diagnostics" / "processes" / f"runtime-parent-{os.getpid()}.json",
        {
            "role": "runtime-parent",
            "pid": os.getpid(),
            "phase": "started",
            "status": "running",
            "run_id": "pilot-1",
            "updated_at": now,
            "last_progress": {"worker_count": 2},
        },
    )
    worker_snapshot = {
        "worker_id": 0,
        "route_pool_size": 2,
        "inflight": 0,
        "checkpoint_count": 12,
        "control_counters": {"pause": 1, "resume": 0, "cancel": 0},
        "counters": {
            "taken": 12,
            "accepted": 12,
            "socket_written": 12,
            "write_failed": 0,
        },
        "progress": {
            "enabled": True,
            "accepting": True,
            "blocked_reason": "",
            "pending_durability": 0,
            "queue_depth": 0,
            "writer_oldest_age_ms": 0.0,
            "writer_errors": 0,
            "unresolved_events": 0,
            "writer_batches": 4,
            "writer_fsync_total_ms": 12.5,
            "durable_response_success": 12,
            "durable_response_error": 0,
            "durable_unknown": 0,
            "durable_socket_written_pdus": 12,
            "durable_socket_written_events": 12,
            "durable_accepted_tasks": 12,
            "durable_tasks_taken": 12,
            "durable_write_failed": 0,
            "durable_expired": 0,
            "durable_cancelled": 0,
            "durable_events": 24,
        },
        "campaign_counters": [
            {
                "campaign_type": "batch",
                "campaign_id": 101,
                "accepted_tasks": 6,
                "socket_written_pdus": 6,
                "response_success": 6,
                "response_error": 0,
                "unknown": 0,
                "pending": {},
            },
            {
                "campaign_type": "batch",
                "campaign_id": 102,
                "accepted_tasks": 6,
                "socket_written_pdus": 6,
                "response_success": 6,
                "response_error": 0,
                "unknown": 0,
                "pending": {},
            },
        ],
        "transports": [
            {
                "route_id": "1",
                "connected": True,
                "bind_active": 1,
                "pool_size": 2,
                "window_inflight": 0,
                "window_inflight_peak": 2,
                "socket_written_parts": 12,
                "response_observed_success": 12,
                "response_observed_error": 0,
                "response_observed_unknown": 0,
                "response_callback_errors": 0,
                "reconnect_count": 0,
                "effective_total_limit_tps": 400,
                "response_latency_p95_ms": 4.2,
            }
        ],
    }
    _write(
        root / "progress" / "worker-0.status.json",
        {
            "worker_id": 0,
            "active": False,
            "updated_at": now,
            "snapshot": worker_snapshot,
        },
    )
    _write(
        root / "progress" / "worker-0.progress-aggregate.json",
        {
            "schema_version": 1,
            "worker": "worker-0",
            "updated_at": now,
            "complete": True,
            "applied_segments": {},
            "campaigns": [
                {
                    "campaign_type": "batch",
                    "campaign_id": 101,
                    "accepted_tasks": 6,
                    "tasks_taken": 6,
                    "socket_written_events": 6,
                    "socket_written_pdus": 6,
                    "response_success": 6,
                    "response_error": 0,
                    "unknown": 0,
                    "durable_events": 12,
                },
                {
                    "campaign_type": "batch",
                    "campaign_id": 102,
                    "accepted_tasks": 6,
                    "tasks_taken": 6,
                    "socket_written_events": 6,
                    "socket_written_pdus": 6,
                    "response_success": 6,
                    "response_error": 0,
                    "unknown": 0,
                    "durable_events": 12,
                },
            ],
        },
    )
    _write(
        root / "projection" / "projection-writer-status.json",
        {
            "applied": 24,
            "queue_depth": 0,
            "journal_total_bytes": 4096,
            "journal_committed_offset_bytes": 4096,
            "journal_backlog_bytes": 0,
            "journal_pending_events": 0,
        },
    )
    _write(
        root / "control-state.json",
        {
            "schema_version": 1,
            "version": 1,
            "campaigns": [
                {"campaign_type": "batch", "campaign_id": 101, "state": "paused"},
                {"campaign_type": "batch", "campaign_id": 102, "state": "running"},
            ],
        },
    )
    _write(
        root / "spool" / "manifest.json",
        {
            "schema_version": 1,
            "virtual_partition_count": 2,
            "current_business_day": "2026-10-19",
            "business_epoch": 3,
            "total_records": 13,
            "total_bytes": 13 * 1024,
            "segments": {
                "0": [
                    {
                        "business_day": "2026-10-19",
                        "records": 10,
                        "first_ordinal": 1,
                        "last_ordinal": 10,
                    }
                ],
                "1": [],
            },
        },
    )
    _write(
        root / "spool" / "cursors" / "partition-0000.json",
        {"partition_id": 0, "cursor": 7},
    )
    _write(
        root / "spool" / "taken" / "partition-0000.json",
        {"partition_id": 0, "taken": [9]},
    )
    _write(
        root / "spool" / "cursors" / "partition-0001.json",
        {"partition_id": 1, "cursor": 0},
    )
    return root


def _run_cli(*args: str):
    return subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )


def test_stats_cli_json_snapshot(tmp_path):
    root = _fake_runtime_root(tmp_path)
    result = _run_cli("--root", str(root), "--once", "--json")
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["runtime"]["state"] == "RUNNING"
    assert value["run_id"] == "pilot-1"
    assert value["spool"]["pending"] == 2
    assert value["smpp"]["written_pdus"] == 12
    assert value["smpp"]["socket_written_observed_pdus"] == 12
    assert value["smpp"]["durable_socket_written_pdus"] == 12
    assert value["socket_write_rate_pdu_per_second"] is None
    assert value["journal_durable_rate_pdu_per_second"] is None
    assert value["response_observed_rate_pdu_per_second"] is None
    assert value["rate_pdu_per_second"] is None
    assert value["smpp"]["binds_configured"] == 2
    assert value["workers"]["checkpoint_count"] == 12
    assert value["workers"]["control_counters"]["pause"] == 1
    assert value["responses"]["durable_success"] == 12
    assert value["journal"]["durable_events"] == 24
    assert value["projection"]["caught_up"] is True
    assert value["health"] == "OK"
    assert value["stale"] is False
    assert "campaign_projected" in value["incomplete_metrics"]
    assert {row["campaign_id"] for row in value["campaigns"]} == {101, 102}
    states = {
        row["campaign_id"]: row["state"] for row in value["campaigns"]
    }
    assert states == {101: "paused", 102: "running"}


def test_stats_cli_campaign_filter_and_plain_output(tmp_path):
    root = _fake_runtime_root(tmp_path)
    result = _run_cli(
        "--root", str(root), "--once", "--no-color",
        "--campaign", "batch:101",
    )
    assert result.returncode == 0, result.stderr
    assert "VNEXT | run=pilot-1" in result.stdout
    assert "batch:101" in result.stdout
    assert "batch:102" not in result.stdout
    assert "\x1b[" not in result.stdout


def test_stats_cli_missing_root_is_an_error(tmp_path):
    result = _run_cli("--root", str(tmp_path / "missing"), "--once")
    assert result.returncode == 2
    assert "does not exist" in result.stderr


def test_stats_cli_stale_status_is_explicit(tmp_path):
    root = _fake_runtime_root(tmp_path)
    status_path = root / "progress" / "worker-0.status.json"
    value = json.loads(status_path.read_text(encoding="utf-8"))
    value["updated_at"] = time.time() - 60
    status_path.write_text(json.dumps(value), encoding="utf-8")
    result = _run_cli("--root", str(root), "--once", "--json")
    assert result.returncode == 0, result.stderr
    snapshot = json.loads(result.stdout)
    assert snapshot["stale"] is True
    assert snapshot["health"] == "STALE"


def test_stats_cli_callback_error_is_blocked(tmp_path):
    root = _fake_runtime_root(tmp_path)
    status_path = root / "progress" / "worker-0.status.json"
    value = json.loads(status_path.read_text(encoding="utf-8"))
    value["snapshot"]["transports"][0]["response_callback_errors"] = 1
    status_path.write_text(json.dumps(value), encoding="utf-8")
    result = _run_cli("--root", str(root), "--once", "--json")
    assert result.returncode == 0, result.stderr
    snapshot = json.loads(result.stdout)
    assert snapshot["health"] == "BLOCKED"
    assert snapshot["responses"]["callback_errors"] == 1
    assert "response_callback_errors" in snapshot["incomplete_metrics"]


def test_rate_tracker_never_reports_reset_as_negative_or_multiplied():
    tracker = _RateTracker(5.0)
    assert tracker.update(100, 0.0) is None
    assert tracker.update(110, 2.0) == 5.0
    assert tracker.update(5, 4.0) is None
    assert tracker.counter_reset is True
    assert tracker.update(15, 6.0) == 5.0
    assert tracker.update(20, 7.0) == 5.0


def test_rate_tracker_resets_when_process_generation_changes():
    tracker = _RateTracker(5.0)
    assert tracker.update(100, 0.0, identity="run-1/worker-0/pid-10") is None
    assert tracker.update(110, 2.0, identity="run-1/worker-0/pid-10") == 5.0
    assert tracker.update(3, 4.0, identity="run-1/worker-0/pid-11") is None
    assert tracker.counter_reset is True
    assert tracker.last_reset_reason == "counter_identity_changed"


def test_dual_rate_snapshot_separates_socket_and_durable_progress(tmp_path):
    root = _fake_runtime_root(tmp_path)
    now = time.time()
    _write(
        root / "diagnostics" / "processes" / "worker-0-111.json",
        {
            "role": "worker-0",
            "pid": 111,
            "run_id": "pilot-1",
            "phase": "running",
            "updated_at": now,
        },
    )
    trackers = {
        "socket_write": _RateTracker(5.0),
        "journal_durable": _RateTracker(5.0),
        "response_observed": _RateTracker(5.0),
    }
    first = _build_snapshot(root, rate_trackers=trackers, interval=2.0)
    assert first["socket_write_rate_pdu_per_second"] is None
    assert first["journal_durable_rate_pdu_per_second"] is None

    status_path = root / "progress" / "worker-0.status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["updated_at"] = time.time()
    status["snapshot"]["transports"][0]["socket_written_parts"] = 32
    status["snapshot"]["transports"][0]["response_observed_success"] = 28
    status["snapshot"]["transports"][0]["response_observed_total"] = 28
    status_path.write_text(json.dumps(status), encoding="utf-8")
    progress_path = root / "progress" / "worker-0.progress-aggregate.json"
    aggregate = json.loads(progress_path.read_text(encoding="utf-8"))
    for campaign in aggregate["campaigns"]:
        campaign["socket_written_pdus"] = 8
    progress_path.write_text(json.dumps(aggregate), encoding="utf-8")
    worker_progress = status["snapshot"]["progress"]
    worker_progress["durable_socket_written_pdus"] = 16
    status_path.write_text(json.dumps(status), encoding="utf-8")
    time.sleep(0.01)

    second = _build_snapshot(root, rate_trackers=trackers, interval=2.0)
    assert second["socket_write_rate_pdu_per_second"] is not None
    assert second["journal_durable_rate_pdu_per_second"] is not None
    assert second["response_observed_rate_pdu_per_second"] is not None
    assert second["socket_write_rate_pdu_per_second"] > second[
        "journal_durable_rate_pdu_per_second"
    ]
    assert second["rate_pdu_per_second"] == second[
        "journal_durable_rate_pdu_per_second"
    ]


def test_rate_snapshot_marks_missing_transport_counter_as_incomplete(tmp_path):
    root = _fake_runtime_root(tmp_path)
    status_path = root / "progress" / "worker-0.status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    del status["snapshot"]["transports"][0]["socket_written_parts"]
    status_path.write_text(json.dumps(status), encoding="utf-8")
    result = _run_cli("--root", str(root), "--once", "--json")
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["socket_write_rate_pdu_per_second"] is None
    assert any(
        reason.startswith("socket_written_parts:")
        for reason in value["rate_unavailable_reasons"]
    )
    assert "socket_write_rate" in value["incomplete_metrics"]


def test_stats_cli_watch_json_emits_ndjson(tmp_path):
    root = _fake_runtime_root(tmp_path)
    process = subprocess.Popen(
        [
            sys.executable, str(CLI_PATH), "--root", str(root),
            "--watch", "--json", "--interval", "0.2",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    try:
        lines = []
        deadline = time.monotonic() + 15.0
        while len(lines) < 2 and time.monotonic() < deadline:
            line = process.stdout.readline()
            if line:
                lines.append(line)
        assert len(lines) >= 2
        for line in lines[:2]:
            value = json.loads(line)
            assert value["schema_version"] == 1
            assert value["runtime"]["state"] == "RUNNING"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
