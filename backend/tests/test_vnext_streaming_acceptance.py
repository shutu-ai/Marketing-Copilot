import json
import os
import threading
import time
from pathlib import Path

import pytest

from tools import vnext_streaming_acceptance as acceptance
from vnext.partition import PartitionMap
from tools.vnext_streaming_acceptance import _projection_caught_up


class _Projection:
    def __init__(self, status: dict):
        self._status = status

    def snapshot(self) -> dict:
        return dict(self._status)


class _DurableProjection(_Projection):
    def __init__(self, root: Path, status: dict):
        super().__init__(status)
        self.status_path = root / "projection-writer-status.json"
        self.offsets_path = root / "journal-offsets.json"
        self.status_path.write_text(json.dumps(status), encoding="utf-8")


@pytest.mark.parametrize("workers", [8, 12, 16, 24])
def test_worker_matrix_values_are_accepted_without_clamping(workers):
    assert acceptance._parse_worker_count(str(workers)) == workers
    assert acceptance._validate_worker_count(workers) == workers


@pytest.mark.parametrize("workers", [0, -1, 1, 7, 25, 32])
def test_worker_matrix_rejects_unsupported_values(workers):
    with pytest.raises(acceptance.argparse.ArgumentTypeError):
        acceptance._parse_worker_count(str(workers))
    with pytest.raises(ValueError, match="--workers must be one of"):
        acceptance._validate_worker_count(workers)


@pytest.mark.parametrize("workers", [8, 12, 16, 24])
def test_partition_assignment_evidence_covers_all_partitions(workers, tmp_path: Path):
    partition_map = PartitionMap.create(tmp_path / "partition-map.json", 256, workers)
    evidence = acceptance._partition_assignment_evidence(partition_map, workers)

    assert evidence["valid"] is True
    assert evidence["virtual_partition_count"] == 256
    assert evidence["assigned_partition_count"] == 256
    assert evidence["unique_partition_count"] == 256
    assert evidence["missing_partition_count"] == 0
    assert evidence["duplicate_partition_count"] == 0
    assert sum(evidence["owner_distribution"].values()) == 256


def test_worker_scaling_does_not_expand_shared_physical_bind_target():
    assert [
        acceptance._worker_bind_target(4, workers)
        for workers in (8, 12, 16, 24)
    ] == [1, 1, 1, 1]


def test_worker_lifecycle_heartbeat_records_shutdown_contract(tmp_path: Path):
    stop_event = threading.Event()
    feeder_done = threading.Event()
    lifecycle = acceptance._WorkerLifecycle(5, (5, 17), str(tmp_path / "lifecycle"))
    lifecycle.attach_events(stop_event=stop_event, feeder_done=feeder_done)
    lifecycle.update(
        "DRAIN_RESPONSES_ENTER",
        detail="diagnostic-test",
        local_inflight=2,
        response_pending=3,
        global_pending={"scope": "all_workers", "records": 0},
    )
    feeder_done.set()
    stop_event.set()
    lifecycle.set_events(stop_event=stop_event, feeder_done=feeder_done)
    lifecycle.close()

    payload = json.loads((tmp_path / "lifecycle" / "worker-5.json").read_text(encoding="utf-8"))
    assert payload["worker_id"] == 5
    assert payload["pid"] == os.getpid()
    assert "pid_start_time" in payload
    assert payload["phase"] == "PROCESS_EXIT"
    assert payload["partitions"] == [5, 17]
    assert payload["stop_event_is_set"] is True
    assert payload["feeder_done_is_set"] is True
    assert payload["global_pending"]["scope"] == "all_workers"
    assert payload["response_pending"] == 3
    assert payload["faulthandler_enabled"] is True


def test_worker_lifecycle_stall_dump_writes_thread_evidence(tmp_path: Path):
    lifecycle = acceptance._WorkerLifecycle(2, (2,), str(tmp_path / "lifecycle"))
    lifecycle._dump_stall("RUN_UNTIL_IDLE_ENTER", 16.0)
    lifecycle.close()

    stack_text = (tmp_path / "lifecycle" / "worker-2-stacks.log").read_text(encoding="utf-8")
    assert "lifecycle stall phase=RUN_UNTIL_IDLE_ENTER" in stack_text
    assert "THREAD name=" in stack_text


def test_process_rows_accepts_sles_ps_and_ignores_headers_or_short_lines():
    rows = acceptance._parse_process_rows(
        "  PID   PPID %CPU   RSS\n"
        "  101      1  2.5  4096\n"
        "102 101 0.0 2048 extra\n"
        "bad row\n"
    )
    assert rows[101] == {"ppid": 1, "cpu_percent": 2.5, "rss_kib": 4096, "state": "?"}
    assert rows[102]["ppid"] == 101
    assert 0 not in rows


def test_process_sampler_selects_children_and_handles_a_disappeared_pid(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        acceptance,
        "_read_proc_rows",
        lambda: (
            {
                10: {"ppid": 1, "cpu_ticks": 10, "rss_kib": 100, "state": "S"},
                11: {"ppid": 10, "cpu_ticks": 20, "rss_kib": 200, "state": "S"},
            },
            {"source": "proc"},
        ),
    )
    monkeypatch.setattr(
        acceptance,
        "_read_ps_rows",
        lambda pids=None: ({}, {"source": "ps", "error": "pid disappeared"}),
    )
    value = acceptance._process_tree_metrics(
        [10, 999], label="application", diagnostic_path=tmp_path / "sampler.jsonl"
    )
    assert value["sampler_ok"] is True
    assert value["sampled_pid_count"] == 2
    assert value["rss_kib"] == 300
    assert value["sampling_source"] == "proc"
    assert "pid disappeared" in (value["diagnostic"]["primary"]["ps_cross_check"]["error"])


def test_process_sampler_failure_is_not_reported_as_zero(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(acceptance, "_read_proc_rows", lambda: (_ for _ in ()).throw(RuntimeError("proc denied")))
    monkeypatch.setattr(acceptance, "_read_psutil_rows", lambda: (_ for _ in ()).throw(RuntimeError("psutil missing")))
    monkeypatch.setattr(
        acceptance,
        "_read_ps_rows",
        lambda pids=None: ({}, {"source": "ps", "error": "ps exited 1"}),
    )
    value = acceptance._process_tree_metrics([10], diagnostic_path=tmp_path / "sampler.jsonl")
    assert value["sampler_ok"] is False
    assert value["rss_kib"] is None
    assert value["cpu_percent"] is None
    assert "ps exited 1" in value["sampler_error"]


def test_projection_completion_uses_durable_offsets_when_status_is_stale(tmp_path: Path):
    journal = tmp_path / "worker-0.jsonl"
    journal.write_text("x" * 100 + "\n", encoding="ascii")
    status = {
        "alive": True,
        "journal_committed_offset_bytes": 0,
        "journal_backlog_bytes": 1,
        "journal_backlog_events_estimate": 1,
        "journal_pending_events": 1,
        "queue_depth": 1,
    }
    projection = _DurableProjection(tmp_path, status)
    projection.offsets_path.write_text(
        json.dumps({str(journal.resolve()): journal.stat().st_size}), encoding="utf-8"
    )
    old = time.time() - 30
    os.utime(projection.status_path, (old, old))
    assert _projection_caught_up(projection, [journal]) is True
    evidence = acceptance._projection_completion_evidence(projection, [journal])
    assert evidence["state"] == "COMPLETED"
    assert evidence["status_age_ms"] > 20000
    assert evidence["durable"]["backlog_bytes"] == 0


def test_projection_completion_does_not_skip_partial_journal_line(tmp_path: Path):
    journal = tmp_path / "worker-0.jsonl"
    journal.write_text("x" * 100 + "partial", encoding="ascii")
    projection = _DurableProjection(
        tmp_path,
        {
            "alive": True,
            "journal_committed_offset_bytes": 100,
            "journal_backlog_bytes": 0,
            "journal_backlog_events_estimate": 0,
            "journal_pending_events": 0,
            "queue_depth": 0,
        },
    )
    projection.offsets_path.write_text(
        json.dumps({str(journal.resolve()): 100}), encoding="utf-8"
    )
    assert _projection_caught_up(projection, [journal]) is False


def test_projection_completion_without_durable_offsets_marks_stale_status(tmp_path: Path):
    journal = tmp_path / "worker-0.jsonl"
    journal.write_text("x" * 100 + "\n", encoding="ascii")
    projection = _DurableProjection(
        tmp_path,
        {
            "alive": True,
            "journal_committed_offset_bytes": 0,
            "journal_backlog_bytes": 0,
            "journal_backlog_events_estimate": 0,
            "journal_pending_events": 0,
            "queue_depth": 0,
        },
    )
    projection.offsets_path.unlink(missing_ok=True)
    old = time.time() - 30
    os.utime(projection.status_path, (old, old))
    evidence = acceptance._projection_completion_evidence(projection, [journal])
    assert evidence["complete"] is False
    assert evidence["state"] == "STALE_STATUS"


def test_projection_caught_up_requires_committed_journal_eof(tmp_path: Path):
    journal = tmp_path / "worker-0.jsonl"
    journal.write_text("x" * 100, encoding="ascii")
    status = {
        "alive": True,
        "projection_lag_seconds": 0.0,
        "journal_committed_offset_bytes": 0,
        "journal_backlog_bytes": 0,
        "journal_backlog_events_estimate": 0,
        "journal_pending_events": 0,
        "queue_depth": 0,
    }

    assert not _projection_caught_up(_Projection(status), [journal])

    status["journal_committed_offset_bytes"] = 100
    assert _projection_caught_up(_Projection(status), [journal])

    status["alive"] = False
    assert _projection_caught_up(_Projection(status), [journal])

    status["journal_backlog_events_estimate"] = 1
    assert not _projection_caught_up(_Projection(status), [journal])


def test_throughput_observability_retains_worker_and_bind_evidence():
    value = acceptance._throughput_observability(
        route_config={
            "route_id": 1,
            "tps": 200,
            "tps_scope": "per_bind",
            "pool_size": 4,
            "window_size": 50,
        },
        feed_stats=acceptance.Counter({"campaign_1_accepted": 2, "batches": 1}),
        smsc=type(
            "SMSC",
            (),
            {"snapshot": lambda self: {"received": 2, "response_success": 2}},
        )(),
        worker_results=[
            {
                "worker_id": 0,
                "counters": {"taken": 2, "socket_written": 2, "response_success": 2},
                "route_credits": {
                    "credit_wait_seconds": 1.5,
                    "credit_refill_cost_seconds": 0.1,
                    "credit_wait_count": 2,
                },
                "transports": [
                    {
                        "socket_written_parts": 2,
                        "response_success": 2,
                        "bind_active_seconds": 1.0,
                    }
                ],
            }
        ],
        critical_path={"counters": {}, "durations": {}},
    )
    assert value["workers"][0]["task_taken"] == 2
    assert value["workers"][0]["socket_written_pdu"] == 2
    assert value["binds"][0]["socket_pdu_per_bind_active_second"] == 2.0
    assert value["credits"]["credit_wait_seconds"] == 1.5
