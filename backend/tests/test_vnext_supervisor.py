from __future__ import annotations

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
