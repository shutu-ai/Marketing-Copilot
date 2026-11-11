from __future__ import annotations

import asyncio

from vnext.smpp_transport import SocketWriteResult
from vnext.spool import SpoolRecord, TaskSpool
from vnext.worker import ShardWorker


def _record(event_id: str, ordinal: int, campaign_id: int, route_id: int, partition: int) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1, event_id=event_id, partition_id=partition, ordinal=ordinal,
        campaign_type="batch", campaign_id=campaign_id, campaign_generation=1,
        route_id=route_id, normalized_msisdn=f"251900000{ordinal:04d}",
        payload_ref="synthetic", message=event_id, part_count_hint=1,
        source_id="synthetic", created_at=1.0,
    )


class FakeTransport:
    def __init__(self, config, _credits, events):
        self.config = config
        self.events = events

    async def send_task(self, **kwargs):
        await asyncio.sleep(0.001 if self.config["route_id"] == "2" else 0.004)
        if self.config.get("fail"):
            return SocketWriteResult("write_failed", 0, 1, "route down")
        self.events.append((self.config["route_id"], kwargs["destination_addr"]))
        return SocketWriteResult("socket_written", 1, 1)

    async def close(self):
        return None

    def snapshot(self):
        return {"route_id": self.config["route_id"]}


def test_worker_is_fair_and_route_failure_does_not_block_other_route(tmp_path):
    return asyncio.run(_test_worker_is_fair_and_route_failure_does_not_block_other_route(tmp_path))


async def _test_worker_is_fair_and_route_failure_does_not_block_other_route(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=4, max_bytes=100_000)
    records = []
    ordinal = 0
    for index in range(1, 11):
        ordinal += 1
        records.append(_record(f"a-{index}", ordinal, 1, 1, 0))
        ordinal += 1
        records.append(_record(f"b-{index}", ordinal, 2, 2, 0))
    spool.append_many(records)
    events = []

    def factory(config, credits):
        return FakeTransport(config, credits, events)

    progress = []
    worker = ShardWorker(
        0, (0,), spool,
        {1: {"route_id": 1, "fail": True}, 2: {"route_id": 2}},
        transport_factory=factory, progress_sink=progress.append,
        max_inflight=8, prefetch_per_partition=8,
    )
    result = await worker.run_until_idle()
    assert result["counters"]["socket_written"] == 10
    assert result["counters"]["write_failed"] == 10
    assert {str(route) for route, _destination in events} == {"2"}
    assert spool.cursor(0) == 20
    # Campaign 2 must get service before all Campaign 1 failures finish.
    first_success = next(index for index, item in enumerate(progress) if item["state"] == "socket_written")
    assert any(item["campaign_id"] == 2 for item in progress[:first_success + 1])


def test_slow_route_isolated_by_route_inflight_limit(tmp_path):
    return asyncio.run(_test_slow_route_isolated_by_route_inflight_limit(tmp_path))


async def _test_slow_route_isolated_by_route_inflight_limit(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([
        _record("slow", 1, 1, 1, 0),
        _record("fast-1", 2, 2, 2, 0),
        _record("fast-2", 3, 2, 2, 0),
    ])
    started = asyncio.Event()
    release = asyncio.Event()
    events = []

    class Transport(FakeTransport):
        async def send_task(self, **kwargs):
            if str(self.config["route_id"]) == "1":
                started.set()
                await release.wait()
            events.append(self.config["route_id"])
            return SocketWriteResult("socket_written", 1, 1)

    worker = ShardWorker(
        0, (0,), spool,
        {
            1: {"route_id": 1, "pool_size": 1, "window_size": 1},
            2: {"route_id": 2, "pool_size": 1, "window_size": 2},
        },
        transport_factory=lambda config, credits: Transport(config, credits, events),
        max_inflight=3, prefetch_per_partition=3,
    )
    task = asyncio.create_task(worker.run_until_idle())
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await asyncio.sleep(0.01)
    assert 2 in events
    release.set()
    await asyncio.wait_for(task, timeout=1.0)


def test_route_failure_does_not_block_other_route_in_same_partition(tmp_path):
    return asyncio.run(_test_route_failure_does_not_block_other_route_in_same_partition(tmp_path))


async def _test_route_failure_does_not_block_other_route_in_same_partition(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([
        _record("unavailable", 1, 1, 1, 0),
        _record("available", 2, 2, 2, 0),
    ])
    events = []

    class Unavailable(FakeTransport):
        async def connect(self):
            raise ConnectionError("route unavailable")

    def factory(config, credits):
        transport = Unavailable(config, credits, events)
        if str(config["route_id"]) == "2":
            transport.connect = None
        return transport

    worker = ShardWorker(
        0, (0,), spool,
        {1: {"route_id": 1}, 2: {"route_id": 2}},
        transport_factory=factory, max_inflight=2, prefetch_per_partition=2,
    )
    await worker.run_until_idle()
    assert events and events[0][0] == 2


def test_worker_bind_target_can_use_multiple_physical_transports(tmp_path):
    return asyncio.run(_test_worker_bind_target_can_use_multiple_physical_transports(tmp_path))


async def _test_worker_bind_target_can_use_multiple_physical_transports(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([
        _record("one", 1, 1, 1, 0),
        _record("two", 2, 1, 1, 0),
    ])
    created = []

    def factory(config, credits):
        transport = FakeTransport(config, credits, [])
        created.append(transport)
        return transport

    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1, "_worker_bind_target": 2}},
        transport_factory=factory, max_inflight=2, prefetch_per_partition=2,
    )
    snapshot = await worker.run_until_idle(close_transports=False)

    assert len(created) == 2
    assert len(snapshot["transports"]) == 2
    await worker.close_transports()


def test_worker_stop_prevents_new_dispatch_at_shutdown_boundary(tmp_path):
    return asyncio.run(_test_worker_stop_prevents_new_dispatch_at_shutdown_boundary(tmp_path))


async def _test_worker_stop_prevents_new_dispatch_at_shutdown_boundary(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([_record("not-dispatched", 1, 1, 1, 0)])
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda config, credits: FakeTransport(config, credits, []),
    )

    worker.stop()
    snapshot = await worker.run_until_idle(close_transports=False)

    assert snapshot["counters"].get("task_taken", 0) == 0
    assert snapshot["counters"].get("socket_written", 0) == 0
    assert spool.status()["pending_records"] == 1
    await worker.close_transports()


def test_new_partition_owner_refreshes_out_of_order_checkpoints(tmp_path):
    root = tmp_path / "spool"
    original = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    original.append_many([_record(str(i), i, 1, 1, 0) for i in range(1, 5)])
    successor = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)

    original.checkpoint(0, 2)
    original.checkpoint(0, 4)

    assert [record.ordinal for record in successor.iter_from(0)] == [1, 3]
    successor.checkpoint(0, 1)
    successor.checkpoint(0, 3)
    assert successor.cursor(0) == 4
