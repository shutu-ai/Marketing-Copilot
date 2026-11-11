from __future__ import annotations

import asyncio
import time

from vnext.runtime import VNextRuntime, _restartable_progress_failure


def test_blocked_progress_failures_are_restartable_without_redispatch():
    snapshot = {
        "progress": {
            "accepting": False,
            "pending_durability": 0,
            "reservations_items": 0,
            "reservations_bytes": 0,
            "retry_batch_events": 0,
            "unresolved_events": 0,
        },
        "progress_lost_events": 0,
        "inflight": 0,
        "transports": [{"window_inflight": 0, "response_callback_pending": 0}],
    }
    assert _restartable_progress_failure(snapshot) is False

    snapshot["progress_lost_events"] = 1
    assert _restartable_progress_failure(snapshot) is False
    snapshot["progress_lost_events"] = 0
    snapshot["progress"]["unresolved_events"] = 1
    assert _restartable_progress_failure(snapshot) is False


def test_worker_watchdog_restarts_stalled_worker_without_redispatch(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "watchdog-runtime",
        {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=1, virtual_partition_count=8,
    )

    class Supervisor:
        def __init__(self):
            self.restarts = []

        def status(self):
            return {"workers": [{"worker_id": 0, "alive": True, "pid": 1234}]}

        def restart_worker(self, worker_id):
            self.restarts.append(int(worker_id))

    supervisor = Supervisor()
    runtime.supervisor = supervisor
    runtime._started = True
    runtime._worker_watchdog_loop_timeout_seconds = 0.1
    now = time.time()
    runtime._worker_progress = lambda: [{
        "worker_id": 0,
        "process_pid": 1234,
        "run_id": runtime.run_id,
        "updated_at": now,
        "phase": "dispatch",
        "loop_heartbeat_at": now - 10,
        "recovery_policy": "checkpoint_before_socket_drop_uncertain",
        "snapshot": {
            "progress": {"accepting": True},
            "inflight": 1,
            "transports": [{"window_inflight": 1}],
        },
    }]

    runtime._run_worker_watchdog()

    assert supervisor.restarts == []
    assert runtime._worker_watchdog_state["0"]["state"] == "healthy"


def test_runtime_starts_projection_and_worker_supervisor_then_stops(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "runtime", {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=2, virtual_partition_count=8, projection_queue_size=32,
    )
    assert runtime.start_method == "spawn"
    assert runtime.projection.start_method == "spawn"
    try:
        runtime.start()
        status = runtime.status()
        assert status["started"] is True
        assert status["start_method"] == "spawn"
        assert status["supervisor"]["start_method"] == "spawn"
        assert len(status["supervisor"]["workers"]) == 2
        runtime.set_campaign_state("batch", 7, "paused")
        assert runtime.status()["controls"] == [{"campaign_type": "batch", "campaign_id": 7, "state": "paused"}]
    finally:
        runtime.stop()
    assert runtime.status()["started"] is False


def test_runtime_capacity_snapshot_avoids_full_spool_reconciliation(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "capacity-runtime", {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=1, virtual_partition_count=8,
    )
    runtime.spool.status = lambda: (_ for _ in ()).throw(
        AssertionError("capacity snapshot must not call the full spool status")
    )

    snapshot = runtime.capacity_snapshot()

    assert snapshot["snapshot_mode"] == "capacity_lightweight"
    assert snapshot["spool"]["snapshot_mode"] == "lightweight"
