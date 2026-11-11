from __future__ import annotations

import threading

from vnext.partition import PartitionMap
from vnext.supervisor import WorkerSupervisor


def test_supervisor_assigns_each_partition_once_and_restarts_with_new_epoch(tmp_path):
    supervisor = WorkerSupervisor(
        tmp_path / "runtime", worker_count=2, virtual_partition_count=8,
        start_method="spawn",
    )
    try:
        supervisor.start()
        supervisor.assert_no_dual_owners()
        before = supervisor.status()
        old_epoch = before["workers"][0]["owner_epochs"]["0"]
        restarted = supervisor.restart_worker(0)
        assert restarted.process.is_alive()
        supervisor.assert_no_dual_owners()
        after = supervisor.status()
        assert after["workers"][0]["owner_epochs"]["0"] > old_epoch
        assert sum(len(item["partitions"]) for item in after["workers"]) == 8
    finally:
        supervisor.stop()


def test_supervisor_reassigns_partitions_after_graceful_owner_stop(tmp_path):
    supervisor = WorkerSupervisor(
        tmp_path / "runtime", worker_count=2, virtual_partition_count=8,
        start_method="spawn",
    )
    try:
        supervisor.start()
        before = supervisor.map.assignment(1).owner_epoch
        result = supervisor.reassign_partitions({1: 0})
        assert result["ok"] is True
        assert supervisor.map.assignment(1).worker_id == 0
        assert supervisor.map.assignment(1).owner_epoch > before
        reloaded = PartitionMap.load_or_create(
            tmp_path / "runtime" / "partition-map.json",
            virtual_partition_count=8, worker_count=2,
        )
        assert reloaded.assignment(1).worker_id == 0
        supervisor.assert_no_dual_owners()
    finally:
        supervisor.stop()


def test_stop_during_reassignment_does_not_launch_replacements(tmp_path, monkeypatch):
    supervisor = WorkerSupervisor(tmp_path / "runtime", worker_count=2, virtual_partition_count=8)
    supervisor.start()
    entered = threading.Event()
    release = threading.Event()
    original_join = supervisor._handles[0].process.join

    def delayed_join(timeout=None):
        entered.set()
        assert release.wait(10)
        return original_join(timeout)

    monkeypatch.setattr(supervisor._handles[0].process, "join", delayed_join)
    result = []
    rebalance = threading.Thread(target=lambda: result.append(supervisor.reassign_partitions({1: 0})))
    rebalance.start()
    try:
        assert entered.wait(10)
        stopping = threading.Thread(target=supervisor.stop)
        stopping.start()
        assert supervisor._monitor_stop.wait(10)
        release.set()
        rebalance.join(20)
        stopping.join(20)
        assert not rebalance.is_alive() and not stopping.is_alive()
        assert result[0]["reason"] == "supervisor_stopping"
        assert not supervisor._handles
        assert supervisor.map.assignment(1).worker_id == 1
    finally:
        release.set()
        rebalance.join(20)
        supervisor.stop()
