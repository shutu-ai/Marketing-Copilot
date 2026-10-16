from __future__ import annotations

import asyncio

import pytest

from vnext.ingress import IngressRouter
from vnext.pressure import ProgressWatchdog, StageCredit
from vnext.spool import TaskSpool


def test_ingress_deduplicates_source_rows_and_uses_msisdn_partition(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=16, max_bytes=100_000)
    router = IngressRouter(spool)
    rows = [
        {"msisdn": "0900000001", "message": "Hi {msisdn}"},
        {"msisdn": "+251900000002", "message": "Hi {msisdn}"},
    ]
    first = router.enqueue_chunk(
        rows, campaign_type="batch", campaign_id=1, phone_prefix="251", source_id="file-a",
    )
    second = router.enqueue_chunk(
        rows, campaign_type="batch", campaign_id=1, phone_prefix="251", source_id="file-a",
    )
    assert first.accepted == 2
    assert second.accepted == 0 and second.duplicate == 2
    stored = [item for partition in range(16) for item in spool.iter_from(partition)]
    assert {item.normalized_msisdn for item in stored} == {"251900000001", "251900000002"}
    assert all("251" in item.message for item in stored)


def test_same_msisdn_is_stable_across_campaigns_without_cross_campaign_dedupe(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=16, max_bytes=100_000)
    router = IngressRouter(spool)
    first = router.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "one"}],
        campaign_type="batch", campaign_id=1, phone_prefix="251", source_id="a",
    )
    second = router.enqueue_chunk(
        [{"msisdn": "0900000001", "message": "two"}],
        campaign_type="batch", campaign_id=2, phone_prefix="251", source_id="b",
    )
    assert first.accepted == second.accepted == 1
    records = [item for partition in range(16) for item in spool.iter_from(partition)]
    assert len(records) == 2
    assert records[0].partition_id == records[1].partition_id
    assert {item.campaign_id for item in records} == {1, 2}


def test_stage_credit_hysteresis_and_watchdog_do_not_flag_legal_pause():
    credit = StageCredit("spool:0", limit_items=10, limit_bytes=1000, high_water=0.8, low_water=0.4, max_age=1)
    reservation = credit.try_reserve(8, 800)
    assert reservation
    assert credit.try_reserve(1, 10) is None
    credit.release(reservation)
    assert credit.snapshot()["resume_count"] == 1
    watchdog = ProgressWatchdog(stall_seconds=0.01)
    watchdog.register(credit)
    credit.blocked_reason = "user_pause"
    credit.try_reserve(1, 10) is not None
    assert watchdog.snapshot()["stalled"] == []


def test_stage_credit_waits_for_release_without_polling_loop():
    async def scenario():
        credit = StageCredit("projection", limit_items=1, limit_bytes=100)
        held = credit.try_reserve(1, 10)
        waiter = asyncio.create_task(credit.reserve(1, 10, timeout=1))
        await asyncio.sleep(0.01)
        assert not waiter.done()
        credit.release(held)
        return await waiter

    reservation = asyncio.run(scenario())
    assert reservation.items == 1
