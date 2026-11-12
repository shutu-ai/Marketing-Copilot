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


def test_campaign_stop_confirmation_requires_current_worker_and_durable_drain(tmp_path):
    runtime = VNextRuntime(tmp_path / "stop", {1: {"route_id": 1}}, worker_count=1, virtual_partition_count=8)
    runtime._started = True
    runtime._control_state = {("realtime", 6): "cancelled"}
    runtime._control_version = 4
    runtime._control_generations = {("realtime", 6): 4}
    runtime._load_control_state = lambda: None
    class Supervisor:
        def status(self):
            return {"workers": [{"worker_id": 0, "alive": True, "pid": 123}]}
    runtime.supervisor = Supervisor()
    snapshot = {"applied_control_version": 4, "healthy": True, "campaign_inflight": [],
                "idle_after_control_version": -1,
                "campaign_generations": {"realtime:6": 4},
                "progress": {"pending_events_by_campaign": []}}
    report = {"worker_id": 0, "process_pid": 123, "run_id": runtime.run_id,
              "updated_at": time.time(), "snapshot": snapshot}
    runtime._worker_progress = lambda: [report]
    assert runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]
    snapshot["campaign_inflight"] = [{"campaign_type": "realtime", "campaign_id": 6,
                                      "active": 1, "reservations": 1}]
    assert not runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]
    snapshot["campaign_inflight"] = [{"campaign_type": "realtime", "campaign_id": 6,
                                      "active": 1, "reservations": 0}]
    assert runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]
    snapshot["campaign_inflight"] = []
    snapshot["progress"]["pending_events_by_campaign"] = [{"campaign_type": "realtime", "campaign_id": 6, "count": 1}]
    assert not runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]
    snapshot["progress"]["pending_events_by_campaign"] = []
    snapshot["campaign_generations"]["realtime:6"] = 3
    assert not runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]
    snapshot["campaign_generations"]["realtime:6"] = 4
    report["process_pid"] = 999
    assert not runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]


def test_campaign_stop_confirmation_accepts_maintenance_pause(tmp_path):
    runtime = VNextRuntime(tmp_path / "paused-stop", {1: {"route_id": 1}}, worker_count=1, virtual_partition_count=8)
    runtime._started = True
    runtime._control_state = {("realtime", 6): "paused"}
    runtime._control_version = 4
    runtime._control_generations = {("realtime", 6): 4}
    runtime._load_control_state = lambda: None

    class Supervisor:
        def status(self):
            return {"workers": [{"worker_id": 0, "alive": True, "pid": 123}]}

    runtime.supervisor = Supervisor()
    snapshot = {
        "applied_control_version": 4, "healthy": True,
        "campaign_inflight": [], "campaign_generations": {"realtime:6": 4},
        "progress": {"pending_events_by_campaign": []},
    }
    runtime._worker_progress = lambda: [{
        "worker_id": 0, "process_pid": 123, "run_id": runtime.run_id,
        "updated_at": time.time(), "snapshot": snapshot,
    }]

    assert runtime.campaign_stop_confirmation("realtime", 6)["confirmed"]


def test_restart_reconciles_campaign_controls_before_worker_start(tmp_path):
    root = tmp_path / "recover"
    first = VNextRuntime(root, {1: {"route_id": 1}}, worker_count=1, virtual_partition_count=8)
    first.set_campaign_state("realtime", 6, "running")
    restarted = VNextRuntime(root, {1: {"route_id": 1}}, worker_count=1, virtual_partition_count=8)
    restarted.reconcile_realtime_controls([{"id": 6, "status": "ended"}])
    assert restarted._load_control_state()["campaigns"] == [
        {"campaign_type": "realtime", "campaign_id": 6, "state": "cancelled", "generation": 0}
    ]


def test_stale_control_publish_cannot_replace_newer_campaign_generation(tmp_path):
    runtime = VNextRuntime(tmp_path / "generation", {1: {"route_id": 1}}, worker_count=1, virtual_partition_count=8)
    runtime.set_campaign_state("realtime", 6, "cancelled", 5)
    runtime.set_campaign_state("realtime", 6, "running", 4)
    state = runtime._load_control_state()
    assert state["campaigns"] == [
        {"campaign_type": "realtime", "campaign_id": 6, "state": "cancelled", "generation": 5}
    ]

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


def test_runtime_spool_maintenance_fence_quiesces_worker(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "maintenance-fence", {},
        worker_count=1, virtual_partition_count=1, projection_queue_size=32,
    )
    try:
        runtime.start()
        request_id = "test-maintenance-request"
        runtime._write_spool_maintenance_request(request_id)
        runtime.supervisor.broadcast({
            "type": "spool_maintenance", "request_id": request_id,
        })
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            workers = [
                item for item in runtime._worker_progress()
                if str(item.get("run_id") or "") == runtime.run_id
            ]
            if workers and all(
                not bool(item.get("active"))
                and bool(item.get("spool_maintenance"))
                and item.get("spool_maintenance_id") == request_id
                and str(item.get("phase") or "") == "idle"
                for item in workers
            ):
                break
            time.sleep(0.05)
        assert workers and all(
            not bool(item.get("active"))
            and bool(item.get("spool_maintenance"))
            and item.get("spool_maintenance_id") == request_id
            for item in workers
        )
    finally:
        runtime._write_spool_maintenance_request("")
        runtime.stop()


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


def test_spool_gc_skips_recovery_when_no_closed_day_gap_exists(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "gc-no-gap", {1: {"route_id": 1}},
        worker_count=1, virtual_partition_count=1,
    )
    runtime._started = True
    runtime.supervisor = object()
    runtime._workers_heartbeat_ready_for_gc = lambda: True
    runtime.spool.cleanup_closed_days = lambda **kwargs: []
    recovered = []
    runtime._recover_expired_spool_gaps = lambda: recovered.append(True)

    result = runtime._run_spool_gc()

    assert result["state"] == "completed"
    assert recovered == []
    assert runtime._spool_gap_recovery_last_result["state"] == "not_run"


def test_spool_gc_backs_off_a_unchanged_blocked_gap(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "gc-backoff", {1: {"route_id": 1}},
        worker_count=1, virtual_partition_count=1,
    )
    runtime._started = True
    runtime.supervisor = object()
    runtime._workers_heartbeat_ready_for_gc = lambda: True
    runtime.spool.closed_day_gap_snapshot = lambda: {
        "partitions": [0], "signature": ((0, 1, 10),),
    }
    runtime.spool.cleanup_closed_days = lambda **kwargs: []
    runtime._enter_spool_maintenance = lambda: "request-1"
    runtime._leave_spool_maintenance = lambda request_id: None
    recovered = []
    runtime._recover_expired_spool_gaps = lambda: (
        recovered.append(True)
        or {"state": "blocked", "reason": "task_index_unavailable"}
    )

    first = runtime._run_spool_gc()
    runtime._spool_gc_last_at = 0.0
    second = runtime._run_spool_gc()

    assert first["state"] == "completed"
    assert second["state"] == "completed"
    assert recovered == [True]
    assert runtime._spool_gap_recovery_last_result["state"] == "blocked"


def test_spool_maintenance_requires_matching_request_id(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "maintenance-generation", {1: {"route_id": 1}},
        worker_count=1, virtual_partition_count=1,
    )
    runtime._started = True
    class Supervisor:
        @staticmethod
        def status():
            return {
                "workers": [
                    {"worker_id": 0, "pid": 1234, "alive": True},
                ],
            }

    runtime.supervisor = Supervisor()
    report = {
        "worker_id": 0,
        "process_pid": 1234,
        "run_id": runtime.run_id,
        "updated_at": time.time(),
        "active": False,
        "spool_maintenance": True,
        "spool_maintenance_id": "old-request",
        "snapshot": {"inflight": 0, "progress": {"pending_durability": 0}},
    }
    runtime._worker_progress = lambda: [report]

    assert not runtime._workers_ready_for_spool_maintenance("new-request")
    report["spool_maintenance_id"] = "new-request"
    assert runtime._workers_ready_for_spool_maintenance("new-request")
    report["process_pid"] = 4321
    assert not runtime._workers_ready_for_spool_maintenance("new-request")


def test_spool_gc_does_not_repeat_fence_for_unchanged_structural_gap(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "gc-structural-block", {1: {"route_id": 1}},
        worker_count=1, virtual_partition_count=1,
    )
    runtime._started = True
    runtime.supervisor = object()
    runtime._workers_heartbeat_ready_for_gc = lambda: True
    runtime.spool.closed_day_gap_snapshot = lambda: {
        "partitions": [0], "signature": ((0, 1, 10, ("orphan",)),),
    }
    runtime.spool.cleanup_closed_days = lambda **kwargs: []
    runtime.spool.reconcile_closed_day_gaps = lambda: {
        "state": "partial", "blocked_partitions": {"0": "unsettled_orphan"},
    }
    entered = []
    runtime._enter_spool_maintenance = lambda: entered.append(True) or "request-1"
    runtime._leave_spool_maintenance = lambda request_id: None
    runtime._recover_expired_spool_gaps = lambda: {
        "state": "completed", "recovered": 0,
    }

    runtime._run_spool_gc()
    runtime._spool_gc_last_at = 0.0
    runtime._spool_gap_retry_not_before = 0.0
    runtime._run_spool_gc()

    assert entered == [True]
    assert runtime._spool_gap_recovery_last_result["cursor_gaps"]["reason"] == (
        "gap_evidence_unchanged"
    )
