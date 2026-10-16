from __future__ import annotations

import asyncio
import time

from vnext.ingress import IngressRouter
from vnext.projection_writer import ProjectionWriterProcess, ProgressJournal
from vnext.smpp_transport import SocketWriteResult
from vnext.spool import TaskSpool
from vnext.worker import ShardWorker
from vnext.runtime import VNextRuntime


class _Transport:
    async def send_task(self, **_kwargs):
        return SocketWriteResult("socket_written", 1, 1)

    async def close(self):
        return None


def _transport_factory(_config, _credits):
    return _Transport()


def _event(event_id: str, ordinal: int) -> dict:
    return {
        "event_id": event_id,
        "partition_id": 0,
        "ordinal": ordinal,
        "campaign_type": "batch",
        "campaign_id": 1,
        "route_id": 1,
        "normalized_msisdn": f"251900000{ordinal:04d}",
        "state": "socket_written",
        "parts_written": 1,
        "parts_total": 1,
        "taken": 1,
        "socket_written": 1,
        "created_at": time.time(),
    }


def _wait_until(predicate, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("condition did not become true before timeout")


def test_long_lived_worker_refreshes_online_manifest_after_idle(tmp_path):
    root = tmp_path / "spool"
    writer_spool = TaskSpool(root, virtual_partition_count=4, max_bytes=100_000)
    reader_spool = TaskSpool(root, virtual_partition_count=4, max_bytes=100_000)
    ingress = IngressRouter(writer_spool)
    progress = []
    worker = ShardWorker(
        0, (0, 1, 2, 3), reader_spool, {1: {"route_id": 1}},
        transport_factory=_transport_factory, progress_sink=progress.append,
        max_inflight=2, prefetch_per_partition=4,
    )

    asyncio.run(worker.run_until_idle())
    result = ingress.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "live-1"}],
        campaign_type="batch", campaign_id=1, route_id=1,
        phone_prefix="251", source_id="live-1",
    )
    assert result.accepted == 1
    asyncio.run(worker.run_until_idle())
    assert len(progress) == 1
    assert progress[0]["state"] == "socket_written"
    assert reader_spool.status()["pending_records"] == 0


def test_projection_process_tails_journal_and_replays_after_crash(tmp_path):
    journal = ProgressJournal(tmp_path / "progress" / "worker-0.jsonl")
    journal.append_many([_event(f"before-{index}", index) for index in range(1, 21)])
    process = ProjectionWriterProcess(
        tmp_path / "projection", queue_size=1, batch_size=4, journal_paths=[journal.path],
    )
    process.start()
    _wait_until(lambda: process.snapshot().get("applied", 0) >= 20)
    process.process.terminate()
    process.process.join(timeout=5)
    journal.append_many([_event(f"after-{index}", index) for index in range(21, 26)])
    assert process.restart() is True
    _wait_until(lambda: process.snapshot().get("applied", 0) >= 5)
    process.stop(timeout=10)

    import duckdb

    conn = duckdb.connect(str(tmp_path / "projection" / "vnext-projection.duckdb"), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0] == 25
        assert conn.execute("SELECT SUM(socket_written) FROM vnext_projection").fetchone()[0] == 25
    finally:
        conn.close()


def test_runtime_control_state_survives_restart(tmp_path):
    route = {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}}
    first = VNextRuntime(tmp_path / "runtime", route, worker_count=1, virtual_partition_count=8)
    first.start()
    first.set_campaign_state("batch", 91, "paused")
    first.stop()

    second = VNextRuntime(tmp_path / "runtime", route, worker_count=1, virtual_partition_count=8)
    second.start()
    try:
        assert second.status()["controls"] == [
            {"campaign_type": "batch", "campaign_id": 91, "state": "paused"}
        ]
    finally:
        second.stop()


def test_pause_does_not_checkpoint_prefetched_record_until_resume(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
    ingress = IngressRouter(spool)
    ingress.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "pause"}],
        campaign_type="batch", campaign_id=7, route_id=1,
        phone_prefix="251", source_id="pause",
    )
    progress = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=_transport_factory, progress_sink=progress.append,
    )
    worker.set_campaign_state("batch", 7, "paused")
    asyncio.run(worker.run_until_idle())
    assert spool.status()["pending_records"] == 1
    assert progress == []
    worker.set_campaign_state("batch", 7, "running")
    asyncio.run(worker.run_until_idle())
    assert spool.status()["pending_records"] == 0
    assert progress[0]["state"] == "socket_written"
