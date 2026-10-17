from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import database
import vnext.runtime as runtime_module
from tools.vnext_runtime_canary_acceptance import CanaryRunner
from vnext.diagnostics import (
    disable_native_fault_capture,
    enable_native_fault_capture,
    write_process_metadata,
)
from vnext.projection_writer import ProgressJournal
from vnext.runtime import VNextRuntime
from vnext.spool import SpoolRecord, TaskSpool


def _record(event_id: str, ordinal: int) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1,
        event_id=event_id,
        partition_id=0,
        ordinal=ordinal,
        campaign_type="audit",
        campaign_id=1,
        campaign_generation=1,
        route_id=1,
        normalized_msisdn=f"251900{ordinal:06d}",
        payload_ref=f"audit:{ordinal}",
        message="synthetic",
        part_count_hint=1,
        source_id="audit",
        created_at=1.0,
    )


def test_watchdog_does_not_classify_wall_clock_age_as_stall():
    previous = database._stall_watchdog_threshold
    database._stall_watchdog_threshold = 30.0
    try:
        assert database._stall_watchdog_reason({"held": False, "age_seconds": 300.0}) is None
        assert database._stall_watchdog_reason({"held": True, "age_seconds": 29.9}) is None
        assert database._stall_watchdog_reason({"held": True, "age_seconds": 30.0}) == "no_progress_stall"
    finally:
        database._stall_watchdog_threshold = previous


def test_watchdog_enabled_switch_is_effective_before_startup(tmp_path):
    database.stop_stall_watchdog()
    disabled = tmp_path / "disabled"
    assert database.start_stall_watchdog(disabled, threshold_seconds=1, enabled=False) is False
    assert not disabled.exists()

    enabled = tmp_path / "enabled"
    try:
        assert database.start_stall_watchdog(enabled, threshold_seconds=1, enabled=True) is True
        assert (enabled / "watchdog-config.json").exists()
        assert (enabled / "application-threads-timeout.txt").exists()
    finally:
        database.stop_stall_watchdog()


def test_native_fault_capture_is_independent_and_persistent(tmp_path):
    root = tmp_path / "diagnostics"
    path = enable_native_fault_capture(root, "audit-root")
    try:
        assert path is not None and path.exists()
        api_path = enable_native_fault_capture(root, "api-main")
        assert api_path == path
        metadata_path = write_process_metadata(root, "audit-root", phase="running")
        assert metadata_path is not None
        metadata = __import__("json").loads(metadata_path.read_text(encoding="utf-8"))
        assert metadata["native_fault_capture"] is True
        assert "native_module_versions" in metadata
        disable_native_fault_capture("api-main")
        metadata_path = write_process_metadata(root, "audit-root", phase="still-running")
        metadata = __import__("json").loads(metadata_path.read_text(encoding="utf-8"))
        assert metadata["native_fault_capture"] is True
    finally:
        disable_native_fault_capture("audit-root")


def test_parent_observer_preserves_child_native_capture_fact(tmp_path):
    root = tmp_path / "diagnostics"
    path = write_process_metadata(
        root,
        "worker-0",
        process_pid=4242,
        extra={"native_fault_capture": True, "native_fault_log": "worker.log"},
    )
    assert path is not None
    write_process_metadata(root, "worker-0", process_pid=4242, phase="stopped")
    metadata = __import__("json").loads(path.read_text(encoding="utf-8"))
    assert metadata["native_fault_capture"] is True
    assert metadata["native_fault_log"] == "worker.log"


def test_lightweight_snapshot_does_not_scan_segment_contents(tmp_path, monkeypatch):
    root = tmp_path / "spool"
    spool = TaskSpool(root, virtual_partition_count=2, max_bytes=100_000, fsync=False)
    spool.append_many([_record("a", 1), _record("b", 2)])

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("lightweight snapshot scanned a segment")

    monkeypatch.setattr(spool, "_scan_segment", fail_scan)
    snapshot = spool.lightweight_snapshot()
    assert snapshot["snapshot_mode"] == "lightweight"
    assert snapshot["pending_records"] == 2
    assert snapshot["total_records"] == 2


def test_runtime_status_reuses_long_lived_spool_owner(tmp_path, monkeypatch):
    runtime = VNextRuntime(
        tmp_path / "runtime", {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=1, virtual_partition_count=4,
    )

    def fail_constructor(*_args, **_kwargs):
        raise AssertionError("status reconstructed TaskSpool")

    monkeypatch.setattr(runtime_module, "TaskSpool", fail_constructor)
    assert runtime.status()["spool"]["pending_records"] == 0


def test_wait_for_idle_uses_lightweight_path_before_final_status(tmp_path, monkeypatch):
    runtime = VNextRuntime(
        tmp_path / "runtime", {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=1, virtual_partition_count=4,
    )
    final_status = {"spool": {"pending_records": 0}, "marker": "final"}
    monkeypatch.setattr(runtime, "status", lambda: final_status)
    monkeypatch.setattr(
        runtime,
        "_idle_snapshot",
        lambda: {
            "spool": {"pending_records": 0},
            "worker_progress": [{"active": False, "snapshot": {"inflight": 0, "transports": []}}],
            "projection": {"journal_offsets": {}},
        },
    )
    assert asyncio.run(runtime.wait_for_idle(timeout=0.1)) == final_status


def test_loopback_summary_uses_bounded_counter_reconciliation(tmp_path):
    runner = object.__new__(CanaryRunner)
    runner.run_root = Path(tmp_path)
    runner.expected_destinations = {7: {"2511", "2512"}}
    runner.smsc = SimpleNamespace(smsc=SimpleNamespace(received=[
        {"destination": "2511"},
        {"destination": "2511"},
        {"destination": "9999"},
    ]))
    runner._received_counts = Counter()
    runner._received_observer_index = 0
    runner._received_baselines = {}
    summary = runner.loopback_summary(7)
    assert summary["received"] == 2
    assert summary["duplicates"] == ["2511"]
    assert summary["missing"] == ["2512"]
    assert summary["unexpected"] == ["9999"]
    assert summary["unexpected_count"] == 1


def test_loopback_summary_isolated_between_campaigns(tmp_path):
    runner = object.__new__(CanaryRunner)
    runner.run_root = Path(tmp_path)
    runner.expected_destinations = {1: {"2511"}, 2: {"2512"}}
    runner.smsc = SimpleNamespace(smsc=SimpleNamespace(received=[
        {"destination": "2511"},
        {"destination": "2512"},
    ]))
    runner._received_counts = Counter()
    runner._received_observer_index = 0
    runner._received_baselines = {2: Counter({"2511": 1})}
    first = runner.loopback_summary(1)
    second = runner.loopback_summary(2)
    assert first["received"] == 1
    assert first["unexpected_count"] == 1
    assert second["received"] == 1
    assert second["unexpected_count"] == 0


def test_canary_api_uses_300_second_http_timeout():
    runner = object.__new__(CanaryRunner)
    runner.api_port = 19090
    observed = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"{}"

    class _Opener:
        def open(self, _request, *, timeout):
            observed["timeout"] = timeout
            return _Response()

    runner._url_opener = _Opener()
    assert runner.api("GET", "/api/vnext/status") == {}
    assert observed["timeout"] == 300.0


def test_progress_observer_reads_journal_incrementally(tmp_path):
    journal = ProgressJournal(tmp_path / "vnext" / "progress" / "worker-0.jsonl")
    journal.append({"event_id": "one", "campaign_id": 1, "state": "socket_written"})
    runner = object.__new__(CanaryRunner)
    runner.run_root = Path(tmp_path)
    runner._progress_offsets = {}
    runner._progress_events_by_id = {}
    assert len(runner.progress_events()) == 1
    first_offset = runner._progress_offsets[str(journal.path)]
    journal.append({"event_id": "two", "campaign_id": 1, "state": "socket_written"})
    assert {item["event_id"] for item in runner.progress_events()} == {"one", "two"}
    assert runner._progress_offsets[str(journal.path)] > first_offset


def test_r2_phase_marker_is_explicit_in_acceptance_runner():
    source = Path(__file__).resolve().parents[1] / "tools" / "vnext_runtime_canary_acceptance.py"
    assert '"r2_campaign_begin"' in source.read_text(encoding="utf-8")
