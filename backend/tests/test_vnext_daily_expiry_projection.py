from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import time

from vnext.business_day import BusinessClock
from vnext.ingress import IngressRouter
from vnext.pressure import StageCredit
from vnext.projection_writer import ProjectionWriter
from vnext.spool import SpoolRecord, TaskSpool
from vnext.worker import ShardWorker
from vnext.smpp_transport import SocketWriteResult


class FakeWallClock:
    def __init__(self, value: datetime):
        self.value = value

    def now(self):
        return self.value

    def advance(self, **kwargs):
        self.value += timedelta(**kwargs)


def _record(event_id: str, *, day: str = "", ordinal: int = 0, partition: int = 0):
    return SpoolRecord(
        schema_version=1,
        event_id=event_id,
        partition_id=partition,
        ordinal=ordinal,
        campaign_type="batch",
        campaign_id=7,
        campaign_generation=1,
        route_id=1,
        normalized_msisdn="251900000001",
        payload_ref="synthetic",
        message=event_id,
        part_count_hint=1,
        source_id="synthetic",
        created_at=1.0,
        business_day=day,
    )


def test_business_day_namespace_fences_old_records_and_reclaims_closed_segments(tmp_path):
    wall = FakeWallClock(datetime(2026, 10, 17, 23, 59, 59, tzinfo=timezone.utc))
    spool = TaskSpool(
        tmp_path / "spool", virtual_partition_count=4, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"), segment_max_records=1,
    )
    ingress = IngressRouter(spool)
    first = ingress.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "yesterday"}],
        campaign_type="batch", campaign_id=1, route_id=1,
        phone_prefix="251", source_id="same-source",
    )
    assert first.accepted == 1
    old_record = next(item for item in spool.iter_from(0))
    old_day = old_record.business_day
    before = spool.status()["total_bytes"]

    wall.advance(seconds=1)
    transition = spool.rollover_if_needed()
    assert transition["changed"] is True
    assert spool.current_business_day == "2026-10-18"
    assert list(spool.iter_from(0)) == []
    assert spool.is_record_current(old_record) is False

    second = ingress.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "today"}],
        campaign_type="batch", campaign_id=1, route_id=1,
        phone_prefix="251", source_id="same-source",
    )
    assert second.accepted == 1
    new_record = next(item for item in spool.iter_from(0))
    assert new_record.business_day != old_day
    assert new_record.event_id != old_record.event_id

    reclaimed = spool.reclaim_business_day(old_day)
    assert reclaimed["reclaimed"] is True
    assert reclaimed["reclaimed_bytes"] > 0
    assert spool.status()["total_bytes"] < before + 5000
    reopened = TaskSpool(
        tmp_path / "spool", virtual_partition_count=4, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    assert [item.message for item in reopened.iter_from(0)] == ["today"]


def test_legacy_record_without_business_day_is_fail_closed_after_restart(tmp_path):
    root = tmp_path / "legacy"
    spool = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    spool.append(_record("legacy"))
    segment = next((root / "partition-0000").glob("*.jsonl"))
    value = json.loads(segment.read_text(encoding="utf-8"))
    value.pop("business_day", None)
    value.pop("expires_at", None)
    legacy = SpoolRecord.from_dict(value, verify_crc=False).with_crc().to_dict()
    segment.write_text(json.dumps(legacy, sort_keys=True) + "\n", encoding="utf-8")

    recovered = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    assert list(recovered.iter_from(0)) == []
    assert recovered.status()["pending_records"] == 0


def test_worker_expiry_checkpoint_is_durable_and_never_calls_transport(tmp_path):
    wall = FakeWallClock(datetime(2026, 10, 17, 23, 59, 59, tzinfo=timezone.utc))
    spool = TaskSpool(
        tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    stored, _ = spool.append(_record("old"))
    wall.advance(seconds=1)
    sent = []

    class Transport:
        async def send_task(self, **_kwargs):
            sent.append(True)
            return SocketWriteResult("socket_written", 1, 1)

    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda *_args: Transport(),
        progress_sink=lambda event: events.append(event),
    )
    events = []
    asyncio.run(worker._process(stored))
    assert sent == []
    assert events[0]["state"] == "expired"
    assert spool.status()["pending_records"] == 0
    reopened = TaskSpool(
        tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    assert list(reopened.iter_from(0)) == []


def test_stage_credit_release_before_wait_is_not_lost():
    async def scenario():
        credit = StageCredit("race", limit_items=1, limit_bytes=100)
        held = credit.try_reserve(1, 10)
        waiter = asyncio.create_task(credit.reserve(1, 10, timeout=1))
        credit.release(held)
        return await waiter

    reservation = asyncio.run(scenario())
    assert reservation.items == 1


def _event(event_id: str, day: str, ordinal: int, state: str = "socket_written") -> dict:
    return {
        "event_id": event_id,
        "partition_id": 0,
        "ordinal": ordinal,
        "campaign_type": "batch",
        "campaign_id": 7,
        "route_id": 1,
        "normalized_msisdn": f"251900000{ordinal:03d}",
        "state": state,
        "parts_written": int(state == "socket_written"),
        "parts_total": 1,
        "socket_written": int(state == "socket_written"),
        "taken": 1,
        "business_day": day,
        "created_at": time.time(),
    }


def test_projection_set_based_batch_deduplicates_and_isolates_days(tmp_path):
    writer = ProjectionWriter(tmp_path / "projection.duckdb", batch_size=16)
    writer.start()
    try:
        first = _event("a", "2026-10-17", 1)
        writer.submit(first)
        writer.submit(first)
        writer.submit(_event("b", "2026-10-17", 2))
        writer.submit(_event("c", "2026-10-18", 1))
        assert writer.flush() == 4
        assert writer.snapshot()["last_batch_new"] == 3
        assert writer.snapshot()["last_batch_duplicates"] == 1
        day_one = writer.daily_summary("2026-10-17")
        day_two = writer.daily_summary("2026-10-18")
        assert len(day_one) == len(day_two) == 1
        assert day_one[0]["socket_written"] == 2
        assert day_two[0]["socket_written"] == 1
        assert day_one[0]["business_day"] != day_two[0]["business_day"]
    finally:
        writer.stop()
