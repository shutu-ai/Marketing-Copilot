import json

from tools.vnext_streaming_acceptance import _aggregate_critical_path_profiles, _profile_sample_row
from vnext.critical_path_profiler import CriticalPathProfiler
from vnext.projection_writer import ProgressJournal


def test_critical_path_profiler_publishes_bounded_snapshot(tmp_path):
    profiler = CriticalPathProfiler(tmp_path / "profiles", "worker-1", interval_seconds=0.1)
    profiler.inc("socket_written_pdu", 3)
    profiler.observe_ns("response_callback", 1_000_000)
    profiler.observe_ns("response_callback", 3_000_000)
    profiler.mark("last_socket_write_monotonic", 12.5)
    profiler.publish(force=True)

    path = tmp_path / "profiles" / "profiles" / f"worker-1-{profiler.pid}.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    assert value["counters"]["socket_written_pdu"] == 3
    assert value["durations"]["response_callback"]["p95_ms"] == 2.9
    assert value["timestamps"]["last_socket_write_monotonic"] == 12.5


def test_progress_journal_profiles_append_and_fsync(tmp_path):
    profiler = CriticalPathProfiler(tmp_path / "profiles", "worker-0", interval_seconds=0.1)
    journal = ProgressJournal(tmp_path / "worker-0.jsonl", profiler=profiler)
    journal.append({"event_id": "e1", "state": "socket_written"})
    profiler.publish(force=True)
    value = _aggregate_critical_path_profiles(tmp_path / "profiles")
    assert value["counters"]["journal_append_count"] == 1
    assert value["counters"]["journal_fsync_count"] == 1
    assert value["counters"]["journal_append_event_count"] == 1
    assert value["durations"]["journal_fsync"]["count"] == 1


def test_profile_sample_row_has_stage_latency_columns():
    row = _profile_sample_row({
        "at_seconds": 1.0,
        "projection_applied": 4,
        "projection_journal_backlog_bytes": 10,
        "projection_journal_backlog_events_estimate": 2,
        "critical_path": {
            "counters": {"socket_written_pdu": 4, "response_pdus_received": 3,
                         "journal_events_durable": 6},
            "durations": {"response_callback": {"p95_ms_max": 1.25}},
        },
    })
    assert row["socket_written_pdu"] == 4
    assert row["projection_committed_events"] == 4
    assert row["response_callback_p95_ms"] == 1.25
