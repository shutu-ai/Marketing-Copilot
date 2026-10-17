from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import queue
import time

import pytest

from vnext.business_day import BusinessClock
from vnext.ingress import IngressRouter
from vnext.projection_writer import (
    ProjectionBackpressure,
    ProjectionWriterProcess,
    ProgressJournal,
)
from vnext.smpp_transport import SocketWriteResult
from vnext.spool import SpoolCapacityError, SpoolRecord, TaskSpool
from vnext.worker import ShardWorker


def _record(event_id: str, *, partition: int = 0, route_id: int = 1, campaign_id: int = 1) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1,
        event_id=event_id,
        partition_id=partition,
        ordinal=0,
        campaign_type="batch",
        campaign_id=campaign_id,
        campaign_generation=1,
        route_id=route_id,
        normalized_msisdn=f"251900000{abs(hash(event_id)) % 10000:04d}",
        payload_ref="t7-synthetic",
        message=event_id,
        part_count_hint=1,
        source_id="t7-synthetic",
        created_at=time.time(),
    )


def _projection_event(event_id: str, business_day: str = "2026-10-17") -> dict:
    return {
        "event_id": event_id,
        "partition_id": 0,
        "ordinal": 1,
        "campaign_type": "batch",
        "campaign_id": 1,
        "route_id": 1,
        "normalized_msisdn": "251900000001",
        "state": "socket_written",
        "parts_written": 1,
        "parts_total": 1,
        "message": "t7",
        "error": "",
        "created_at": time.time(),
        "business_day": business_day,
        "expires_at": 1_800_000_000.0,
        "queued": 1,
        "taken": 1,
        "accepted": 1,
        "socket_written": 1,
        "write_failed": 0,
        "unknown": 0,
        "cancelled": 0,
        "expired": 0,
        "response_success": 0,
        "response_error": 0,
    }


class _Transport:
    def __init__(self, config, _credits, sent):
        self.config = config
        self.sent = sent

    async def send_task(self, **kwargs):
        await asyncio.sleep(float(self.config.get("delay", 0.0)))
        if self.config.get("fail"):
            return SocketWriteResult("write_failed", 0, 1, "route unavailable")
        self.sent.append((str(self.config.get("route_id")), kwargs["destination_addr"]))
        return SocketWriteResult("socket_written", 1, 1)

    async def close(self):
        return None

    def snapshot(self):
        return {"route_id": str(self.config.get("route_id")), "window_inflight": 0}


def _worker_factory(sent):
    def factory(config, credits):
        return _Transport(config, credits, sent)
    return factory


def test_t7_4_feeder_pause_then_resume_has_no_duplicate_source_events(tmp_path):
    async def scenario():
        spool = TaskSpool(tmp_path / "spool", virtual_partition_count=4, max_bytes=100_000)
        ingress = IngressRouter(spool)
        sent = []
        progress = []
        worker = ShardWorker(
            0, (0, 1, 2, 3), spool, {1: {"route_id": 1}},
            transport_factory=_worker_factory(sent), progress_sink=progress.append,
            max_inflight=4, prefetch_per_partition=4,
        )
        first = ingress.enqueue_chunk(
            [{"msisdn": "0900000001", "message": "first"}],
            campaign_type="batch", campaign_id=1, route_id=1,
            phone_prefix="251", source_id="t7-feeder-first",
        )
        assert first.accepted == 1
        await worker.run_until_idle(close_transports=False)
        await asyncio.sleep(0.05)  # bounded stand-in for the long feeder pause
        second = ingress.enqueue_chunk(
            [{"msisdn": "0900000002", "message": "second"}],
            campaign_type="batch", campaign_id=1, route_id=1,
            phone_prefix="251", source_id="t7-feeder-second",
        )
        assert second.accepted == 1
        await worker.run_until_idle(close_transports=False)
        await worker.close_transports()
        assert len(sent) == 2
        assert len({item["event_id"] for item in progress}) == 2
        assert spool.status()["pending_records"] == 0

    asyncio.run(scenario())


def test_t7_5_route_a_failure_keeps_route_b_progressing(tmp_path):
    async def scenario():
        spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
        spool.append_many([
            _record("route-a-1", route_id=1, campaign_id=1),
            _record("route-b-1", route_id=2, campaign_id=2),
            _record("route-a-2", route_id=1, campaign_id=1),
            _record("route-b-2", route_id=2, campaign_id=2),
        ])
        sent = []
        progress = []

        def factory(config, credits):
            return _Transport(dict(config, fail=str(config.get("route_id")) == "1"), credits, sent)

        worker = ShardWorker(
            0, (0,), spool,
            {1: {"route_id": 1, "fail": True}, 2: {"route_id": 2}},
            transport_factory=factory, progress_sink=progress.append,
            max_inflight=4, prefetch_per_partition=4,
        )
        result = await worker.run_until_idle()
        assert result["counters"]["write_failed"] == 2
        assert result["counters"]["socket_written"] == 2
        assert {route for route, _ in sent} == {"2"}
        first_success = next(index for index, item in enumerate(progress) if item["state"] == "socket_written")
        assert any(item["campaign_id"] == 2 for item in progress[:first_success + 1])

    asyncio.run(scenario())


def test_t7_6_full_projection_hint_queue_keeps_journal_authoritative(tmp_path):
    async def scenario():
        spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
        spool.append_many([_record("queue-1"), _record("queue-2")])
        journal = ProgressJournal(tmp_path / "progress" / "worker-0.jsonl")
        hints = queue.Queue(maxsize=1)
        hints.put_nowait({"hint": "already-full"})

        def sink(event):
            journal.append(event)
            try:
                hints.put_nowait(event)
            except queue.Full:
                pass

        sent = []
        worker = ShardWorker(
            0, (0,), spool, {1: {"route_id": 1}},
            transport_factory=_worker_factory(sent), progress_sink=sink,
            max_inflight=2, prefetch_per_partition=2,
        )
        await worker.run_until_idle()
        events = journal.read()
        assert len(events) == 2
        assert {item["state"] for item in events} == {"socket_written"}
        assert hints.qsize() == 1
        assert len(sent) == 2

    asyncio.run(scenario())


def test_t7_7_journal_hard_limit_stops_admission_and_recovers_after_gc(tmp_path):
    journal = ProgressJournal(
        tmp_path / "progress" / "worker-0.jsonl", max_event_bytes=1_024,
        max_bytes=2_400, segment_bytes=1_200,
    )
    for index in range(100):
        item = _projection_event(f"filled-{index}")
        if not journal.can_append(item):
            break
        journal.append(item)
    segments = journal.segment_paths()
    assert len(segments) >= 2
    blocked = _projection_event("blocked")
    assert journal.can_append(blocked) is False
    with pytest.raises(ProjectionBackpressure):
        journal.append(blocked)

    offsets = {str(path): path.stat().st_size for path in segments[:-1]}
    assert journal.reclaim(offsets)
    assert journal.can_append(blocked) is True

    async def scenario():
        spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000)
        spool.append(_record("after-gc"))
        sent = []
        progress = []

        def admission(record):
            return journal.can_append(_projection_event(record.event_id))

        worker = ShardWorker(
            0, (0,), spool, {1: {"route_id": 1}},
            transport_factory=_worker_factory(sent), progress_sink=lambda event: (journal.append(event), progress.append(event)),
            journal_admission=admission,
        )
        await worker.run_until_idle()
        assert sent
        assert any(item["state"] == "socket_written" for item in progress)

    asyncio.run(scenario())


def test_t7_8_spool_capacity_releases_on_day_reclaim_and_new_day_admits(tmp_path):
    now = [datetime(2026, 10, 17, 23, 59, 0, tzinfo=timezone.utc)]
    clock = BusinessClock(lambda: now[0], "UTC")
    spool = TaskSpool(
        tmp_path / "spool", virtual_partition_count=1,
        segment_max_records=2, segment_max_bytes=4096, max_bytes=1_500, clock=clock,
    )
    inserted = 0
    while True:
        try:
            spool.append(_record(f"old-{inserted}"))
            inserted += 1
        except SpoolCapacityError:
            break
    assert inserted >= 1
    now[0] = datetime(2026, 10, 18, 0, 0, 1, tzinfo=timezone.utc)
    transition = spool.rollover_if_needed()
    assert transition["changed"] is True
    reclaimed = spool.cleanup_closed_days(force=True)
    assert sum(int(item.get("reclaimed_records", 0) or 0) for item in reclaimed) >= inserted
    accepted, inserted_new = spool.append(_record("new-day"))
    assert inserted_new is True
    assert accepted.business_day == "2026-10-18"


def test_t7_9_cross_day_cursor_and_writer_restart_do_not_redispatch_or_double_count(tmp_path):
    now = [datetime(2026, 10, 17, 23, 59, 59, tzinfo=timezone.utc)]
    clock = BusinessClock(lambda: now[0], "UTC")
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000, clock=clock)
    old_record, _ = spool.append(_record("old-day"))
    spool.checkpoint(old_record.partition_id, old_record.ordinal)  # crash after dispatch_taken, before socket write
    now[0] = datetime(2026, 10, 18, 0, 0, 1, tzinfo=timezone.utc)
    assert spool.rollover_if_needed()["changed"] is True

    async def worker_scenario():
        sent = []
        worker = ShardWorker(
            0, (0,), spool, {1: {"route_id": 1}},
            transport_factory=_worker_factory(sent), max_inflight=1, prefetch_per_partition=2,
        )
        await worker.run_until_idle(close_transports=False)
        assert sent == []
        spool.append(_record("new-day"))
        await worker.run_until_idle()
        assert len(sent) == 1
        assert spool.status()["pending_records"] == 0

    asyncio.run(worker_scenario())

    journal = ProgressJournal(tmp_path / "progress" / "worker-0.jsonl")
    old_event = _projection_event("projection-old", "2026-10-17")
    new_event = _projection_event("projection-new", "2026-10-18")
    journal.append(old_event)
    process = ProjectionWriterProcess(tmp_path / "projection", batch_size=2, journal_paths=[journal.path])
    process.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and process.snapshot().get("applied", 0) < 1:
        time.sleep(0.02)
    assert process.snapshot().get("applied", 0) >= 1
    process.process.terminate()
    process.process.join(timeout=5)
    journal.append(old_event)
    journal.append(new_event)
    assert process.restart() is True
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and process.snapshot().get("applied", 0) < 1:
        time.sleep(0.02)
    process.stop(timeout=10)

    import duckdb
    connection = duckdb.connect(str(tmp_path / "projection" / "vnext-projection.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0] == 2
        assert connection.execute("SELECT SUM(socket_written) FROM vnext_projection").fetchone()[0] == 2
    finally:
        connection.close()
