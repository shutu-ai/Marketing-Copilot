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
