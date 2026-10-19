from __future__ import annotations

import asyncio
import json
import os
import time

import pytest

from vnext.progress import (
    ProgressPersistence,
    ProgressPersistenceBlocked,
)
from vnext.projection_writer import ProjectionBackpressure, ProgressJournal
from vnext.smpp_transport import (
    BIND_TRANSCEIVER,
    HEADER,
    RESPONSE_MASK,
    SUBMIT_SM,
    AsyncSmppTransport,
    read_pdu,
)
from vnext.spool import SpoolRecord, TaskSpool
from vnext.worker import ShardWorker


async def _responsive_loopback_server(delay: float = 0.0):
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            command, _status, sequence, _body = await read_pdu(reader)
            assert command == BIND_TRANSCEIVER
            writer.write(
                HEADER.pack(22, BIND_TRANSCEIVER | RESPONSE_MASK, 0, sequence)
                + b"LOCAL\x00"
            )
            await writer.drain()
            while True:
                command, _status, sequence, _body = await read_pdu(reader)
                if command != SUBMIT_SM:
                    break
                if delay:
                    await asyncio.sleep(delay)
                writer.write(
                    HEADER.pack(19, SUBMIT_SM | RESPONSE_MASK, 0, sequence)
                    + b"id\x00"
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server


async def _silent_submit_loopback_server():
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            command, _status, sequence, _body = await read_pdu(reader)
            assert command == BIND_TRANSCEIVER
            writer.write(
                HEADER.pack(22, BIND_TRANSCEIVER | RESPONSE_MASK, 0, sequence)
                + b"LOCAL\x00"
            )
            await writer.drain()
            while True:
                command, _status, _sequence, _body = await read_pdu(reader)
                if command != SUBMIT_SM:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    return await asyncio.start_server(handler, "127.0.0.1", 0)


def _event(event_id: str, state: str = "submit_sm_resp_success", **extra) -> dict:
    value = {
        "event_id": event_id,
        "partition_id": 0,
        "ordinal": 1,
        "campaign_type": "batch",
        "campaign_id": 101,
        "route_id": 1,
        "normalized_msisdn": "251900000001",
        "state": state,
        "parts_written": 0,
        "parts_total": 1,
        "error": "",
        "created_at": 1.0,
        "message": "",
        "business_day": "2026-10-19",
        "expires_at": 0.0,
        "taken": 0,
        "accepted": int(not state.startswith("submit_sm_resp_")),
        "socket_written": int(state == "socket_written"),
        "write_failed": int(state == "write_failed"),
        "unknown": int(state == "unknown"),
        "expired": 0,
        "response_success": int(state == "submit_sm_resp_success"),
        "response_error": int(state == "submit_sm_resp_error"),
    }
    value.update(extra)
    return value


def test_reader_observes_responses_while_journal_fsync_is_slow(tmp_path, monkeypatch):
    return asyncio.run(
        _test_reader_observes_responses_while_journal_fsync_is_slow(
            tmp_path, monkeypatch
        )
    )


async def _test_reader_observes_responses_while_journal_fsync_is_slow(
    tmp_path, monkeypatch
):
    journal = ProgressJournal(tmp_path / "worker-0.jsonl", max_bytes=16 * 1024 * 1024)
    original_append_many = journal.append_many

    def slow_append_many(events):
        time.sleep(0.6)
        original_append_many(events)

    monkeypatch.setattr(journal, "append_many", slow_append_many)
    store = ProgressPersistence(
        journal,
        name="worker-0",
        max_pending_items=4096,
        max_pending_bytes=16 * 1024 * 1024,
        batch_size=256,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "worker-0.progress-aggregate.json",
        aggregate_interval_seconds=0.1,
        max_replay_events=1000,
    )
    store.start()
    server = await _responsive_loopback_server()
    port = server.sockets[0].getsockname()[1]

    observed: list[float] = []

    def callback(event):
        reservation = event.get("progress_reservation")
        accepted = store.submit(
            _event(
                f"task-{event['sequence']}:resp:{event['sequence']}",
            ),
            reservation,
        )
        assert accepted
        observed.append(time.monotonic())

    transport = AsyncSmppTransport(
        {
            "route_id": 1, "host": "127.0.0.1", "port": port,
            "system_id": "test", "password": "test", "source_addr": "VNEXT",
            "window_size": 50, "tps": 0,
        },
        response_callback=callback,
    )
    try:
        reservations = [
            store.reserve(items=2, bytes_total=2048) for _ in range(50)
        ]
        started = time.monotonic()
        results = await asyncio.wait_for(
            asyncio.gather(*[
                transport.send_task(
                    destination_addr=f"251900000{index:03d}",
                    message=f"m-{index}",
                    progress_context={"event_id": f"task-{index + 1}"},
                    progress_reservation=reservations[index],
                )
                for index in range(50)
            ]),
            timeout=10,
        )
        assert all(result.state == "socket_written" for result in results)
        deadline = time.monotonic() + 2.0
        while len(observed) < 50 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        observed_elapsed = time.monotonic() - started
        assert len(observed) == 50
        # The reader must not be serialized behind the injected fsync delay.
        assert observed_elapsed < 0.45, observed_elapsed
        assert transport.response_observed_success == 50
        snapshot = store.snapshot()
        assert snapshot["pending_durability"] > 0
        drain = await asyncio.to_thread(store.drain, 5.0)
        assert drain["drained"] is True
        assert store.snapshot()["writer_events_durable"] == 50
    finally:
        await transport.close()
        server.close()
        await server.wait_closed()
        await asyncio.to_thread(store.close, 5.0)


def test_reservation_budget_fails_closed_before_dispatch(tmp_path):
    journal = ProgressJournal(tmp_path / "worker-0.jsonl")
    store = ProgressPersistence(
        journal,
        max_pending_items=4,
        max_pending_bytes=4096,
        batch_size=4,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "worker-0.progress-aggregate.json",
    )
    store.start()
    try:
        reservation = store.reserve(items=4, bytes_total=4096)
        assert reservation.slots_open == 4
        with pytest.raises(ProgressPersistenceBlocked):
            store.reserve(items=1, bytes_total=1024)
        assert journal.size() == 0
        reservation.release_unwritten(4)
        replacement = store.reserve(items=4, bytes_total=4096)
        assert replacement.slots_open == 4
        replacement.release_unwritten(4)
    finally:
        store.close(timeout=2)


def test_journal_failure_marks_unresolved_and_never_fakes_durability(
    tmp_path, monkeypatch,
):
    journal = ProgressJournal(tmp_path / "worker-0.jsonl")

    def broken_append_many(_events):
        raise OSError("injected fsync failure")

    monkeypatch.setattr(journal, "append_many", broken_append_many)
    store = ProgressPersistence(
        journal,
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "worker-0.progress-aggregate.json",
    )
    store.start()
    try:
        reservation = store.reserve(items=1, bytes_total=2048)
        assert store.submit(_event("one"), reservation)
        deadline = time.monotonic() + 2.0
        while store.snapshot()["writer_errors"] == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        snapshot = store.snapshot()
        assert snapshot["accepting"] is False
        assert snapshot["writer_errors"] >= 1
        assert snapshot["unresolved_events"] >= 1
        assert snapshot["writer_events_durable"] == 0
        drain = store.drain(timeout=0.1)
        assert drain["drained"] is False
        marker = tmp_path / "worker-0.progress-aggregate.failure.json"
        assert marker.is_file()
        marker_value = json.loads(marker.read_text(encoding="utf-8"))
        assert marker_value["unresolved_events"] >= 1
    finally:
        store.close(timeout=2)


def test_aggregate_replay_does_not_double_count_after_restart(tmp_path):
    journal_path = tmp_path / "worker-0.jsonl"
    aggregate_path = tmp_path / "worker-0.progress-aggregate.json"
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=aggregate_path,
        aggregate_interval_seconds=0.1,
    )
    store.start()
    try:
        reservation = store.reserve(items=2, bytes_total=4096)
        assert store.submit(
            _event("task-1", state="socket_written", parts_written=1), reservation
        )
        assert store.submit(_event("task-1:resp:1"), reservation)
        assert store.drain(timeout=5)["drained"] is True
    finally:
        store.close(timeout=5)

    # A durable event that arrived after the last aggregate checkpoint must be
    # replayed exactly once by the next process.
    ProgressJournal(journal_path).append(_event("task-2"))

    restarted = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=aggregate_path,
        aggregate_interval_seconds=0.1,
    )
    restarted.start()
    try:
        campaigns = restarted.campaign_snapshot()
        assert len(campaigns) == 1
        assert campaigns[0]["durable_events"] == 3
        assert campaigns[0]["response_success"] == 2
        assert campaigns[0]["socket_written_pdus"] == 1
        assert restarted.snapshot()["aggregate_complete"] is True
    finally:
        restarted.close(timeout=5)


def _record(
    event_id: str, ordinal: int, campaign_id: int, message: str = "hello",
    parts: int = 1,
) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1,
        event_id=event_id,
        partition_id=0,
        ordinal=ordinal,
        campaign_type="batch",
        campaign_id=campaign_id,
        campaign_generation=1,
        route_id=1,
        normalized_msisdn=f"2519000{ordinal:05d}",
        payload_ref="synthetic",
        message=message,
        part_count_hint=parts,
        source_id="synthetic",
        created_at=1.0,
    )


class _ReservedFakeTransport:
    def __init__(self, config, _credits, sent: list[dict], delay: float = 0.0):
        self.config = dict(config)
        self.sent = sent
        self.response_sink = None
        self.delay = float(delay)
        self.sequence = 0

    def set_response_sink(self, callback):
        self.response_sink = callback

    async def connect(self):
        return None

    async def send_task(self, **kwargs):
        from vnext.smpp_transport import SocketWriteResult, count_parts

        message = kwargs["message"]
        context = dict(kwargs["progress_context"])
        reservation = kwargs.get("progress_reservation")
        parts = count_parts(message)
        written = 0
        for _index in range(parts):
            if reservation is not None and not reservation.claim_response_slot():
                return SocketWriteResult(
                    "unknown" if written else "write_failed",
                    written, parts, "reservation exhausted",
                )
            self.sequence += 1
            written += 1
            self.sent.append(dict(context))
            if self.response_sink is not None:
                self.response_sink({
                    "route_id": self.config.get("route_id", 1),
                    "sequence": self.sequence,
                    "status": 0,
                    "success": True,
                    "latency_seconds": self.delay,
                    "body_length": 3,
                    "context": context,
                    "progress_reservation": reservation,
                })
        return SocketWriteResult("socket_written", written, parts)

    async def close(self):
        return None

    def inflight_snapshot(self):
        return {
            "route_id": self.config.get("route_id", 1),
            "connected": True,
            "window_inflight": 0,
            "response_callback_pending": 0,
        }

    def snapshot(self):
        return {
            **self.inflight_snapshot(),
            "response_callback_errors": 0,
            "response_observed_success": len(self.sent),
        }


def test_worker_records_multipart_send_and_responses_durably(tmp_path):
    return asyncio.run(_test_worker_records_multipart_send_and_responses_durably(tmp_path))


async def _test_worker_records_multipart_send_and_responses_durably(tmp_path):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    journal = ProgressJournal(journal_path, max_bytes=16 * 1024 * 1024)
    store = ProgressPersistence(
        journal,
        max_pending_items=4096,
        max_pending_bytes=16 * 1024 * 1024,
        batch_size=64,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=1_000_000)
    spool.append_many([_record("multipart", 1, 101, message="x" * 300, parts=3)])
    sent: list[dict] = []
    worker = ShardWorker(
        0, (0,), spool,
        {1: {"route_id": 1}},
        transport_factory=lambda config, credits: _ReservedFakeTransport(
            config, credits, sent
        ),
        progress_store=store,
        max_inflight=2,
        prefetch_per_partition=2,
    )
    try:
        snapshot = await worker.run_until_idle(close_transports=False)
        await worker.drain_transports()
        await worker.close_transports()
        drain = await worker.drain_progress(timeout=5)
        assert drain["drained"] is True, drain
        events = ProgressJournal(journal_path).read()
        states = [event["state"] for event in events]
        assert states.count("socket_written") == 1
        assert states.count("submit_sm_resp_success") == 3
        assert sum(
            int(event["parts_written"])
            for event in events
            if event["state"] == "socket_written"
        ) == 3
        assert all(
            int(event["parts_total"]) == 3
            for event in events
            if event["state"].startswith("submit_sm_resp_")
        )
        campaign = store.campaign_snapshot()[0]
        assert campaign["accepted_tasks"] == 1
        assert campaign["socket_written_pdus"] == 3
        assert campaign["response_success"] == 3
        assert store.snapshot()["reservations_items"] == 0
        assert store.snapshot()["pending_durability"] == 0
        assert snapshot["healthy"] is True
        assert len(sent) == 3
    finally:
        await worker.close_progress(timeout=5)

def test_paused_campaign_stops_while_other_campaign_continues(tmp_path):
    return asyncio.run(_test_paused_campaign_stops_while_other_campaign_continues(tmp_path))


async def _test_paused_campaign_stops_while_other_campaign_continues(tmp_path):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    store = ProgressPersistence(
        ProgressJournal(journal_path, max_bytes=16 * 1024 * 1024),
        max_pending_items=4096,
        max_pending_bytes=16 * 1024 * 1024,
        batch_size=64,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=1_000_000)
    spool.append_many([
        _record("a-1", 1, 1),
        _record("b-1", 2, 2),
    ])
    sent: list[dict] = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda config, credits: _ReservedFakeTransport(
            config, credits, sent
        ),
        progress_store=store,
        max_inflight=2,
        prefetch_per_partition=2,
    )
    worker.set_campaign_state("batch", 1, "paused")
    try:
        await worker.run_until_idle(close_transports=False)
        await worker.drain_transports()
        await worker.close_transports()
        await worker.drain_progress(timeout=5)
        assert [item["campaign_id"] for item in sent] == [2]
        events = ProgressJournal(journal_path).read()
        assert {event["campaign_id"] for event in events} == {2}
        # Campaign A remains before the contiguous cursor; Campaign B was
        # dispatched out of order through the durable taken set.
        assert spool.cursor(0) == 0
        assert spool.status()["taken_pending"]["0"] == [2]
        assert spool.status()["pending_records"] == 1
        # Resume A and prove the deferred record is dispatched.
        worker.set_campaign_state("batch", 1, "running")
        await worker.run_until_idle(close_transports=False)
        await worker.drain_transports()
        await worker.close_transports()
        await worker.drain_progress(timeout=5)
        assert [item["campaign_id"] for item in sent] == [2, 1]
        assert spool.status()["pending_records"] == 0

        # Cancel a new record: it must be consumed without a socket write.
        spool.append_many([_record("a-2", 3, 1)])
        worker.set_campaign_state("batch", 1, "cancelled")
        await worker.run_until_idle(close_transports=False)
        await worker.close_transports()
        await worker.drain_progress(timeout=5)
        events = ProgressJournal(journal_path).read()
        assert any(
            event["event_id"] == "a-2" and event["state"] == "cancelled"
            for event in events
        )
        assert [item["campaign_id"] for item in sent] == [2, 1]
        assert spool.status()["pending_records"] == 0
    finally:
        await worker.close_progress(timeout=5)


def test_worker_defers_before_checkpoint_when_progress_budget_is_exhausted(tmp_path):
    return asyncio.run(
        _test_worker_defers_before_checkpoint_when_progress_budget_is_exhausted(tmp_path)
    )


async def _test_worker_defers_before_checkpoint_when_progress_budget_is_exhausted(
    tmp_path,
):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=1,
        max_pending_bytes=4096,
        batch_size=16,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=1_000_000)
    spool.append_many([_record("blocked", 1, 1)])
    sent: list[dict] = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda config, credits: _ReservedFakeTransport(
            config, credits, sent
        ),
        progress_store=store,
    )
    try:
        snapshot = await worker.run_until_idle(close_transports=False)
        assert sent == []
        assert spool.cursor(0) == 0
        assert spool.status()["pending_records"] == 1
        assert "item budget" in snapshot["admission_blocked_reason"]
    finally:
        await worker.close_progress(timeout=5)


def test_worker_resumes_automatically_after_progress_backpressure_releases(tmp_path):
    return asyncio.run(
        _test_worker_resumes_automatically_after_progress_backpressure_releases(
            tmp_path
        )
    )


async def _test_worker_resumes_automatically_after_progress_backpressure_releases(
    tmp_path,
):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=2,
        max_pending_bytes=64 * 1024,
        batch_size=16,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    external = store.reserve(items=1, bytes_total=1024)
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=1_000_000)
    spool.append_many([_record("retry", 1, 1)])
    sent: list[dict] = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda config, credits: _ReservedFakeTransport(
            config, credits, sent
        ),
        progress_store=store,
    )
    try:
        snapshot = await worker.run_until_idle(close_transports=False)
        assert sent == []
        assert spool.cursor(0) == 0
        assert "item budget" in snapshot["admission_blocked_reason"]
        external.release_unwritten(1)
        await worker.run_until_idle(close_transports=False)
        await worker.drain_transports()
        await worker.close_transports()
        await worker.drain_progress(timeout=5)
        assert [item["campaign_id"] for item in sent] == [1]
        assert spool.status()["pending_records"] == 0
    finally:
        await worker.close_progress(timeout=5)


def test_transport_close_persists_unknown_response_before_writer_shutdown(tmp_path):
    return asyncio.run(
        _test_transport_close_persists_unknown_response_before_writer_shutdown(
            tmp_path
        )
    )


async def _test_transport_close_persists_unknown_response_before_writer_shutdown(
    tmp_path,
):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    server = await _silent_submit_loopback_server()
    port = server.sockets[0].getsockname()[1]

    def callback(event):
        reservation = event.get("progress_reservation")
        state = "unknown" if event.get("unknown") else "submit_sm_resp_success"
        store.submit(
            _event(
                f"task:resp:{event.get('sequence')}",
                state=state,
                unknown=int(state == "unknown"),
                response_success=int(state != "unknown"),
            ),
            reservation,
        )

    transport = AsyncSmppTransport(
        {
            "route_id": 1, "host": "127.0.0.1", "port": port,
            "system_id": "test", "password": "test", "source_addr": "VNEXT",
            "window_size": 4, "tps": 0, "timeout": 5,
        },
        response_callback=callback,
    )
    reservation = store.reserve(items=2, bytes_total=4096)
    try:
        result = await transport.send_task(
            destination_addr="251900000001",
            message="hello",
            progress_context={"event_id": "task"},
            progress_reservation=reservation,
        )
        assert result.state == "socket_written"
        assert store.submit(
            _event(
                "task", state="socket_written", parts_written=1,
                taken=1, accepted=1,
            ),
            reservation,
        )
        await asyncio.sleep(0.05)
        assert transport.snapshot()["window_inflight"] == 1
        await transport.close()
        assert transport.snapshot()["window_inflight"] == 0
        assert transport.response_observed_unknown == 1
        drain = await asyncio.to_thread(store.drain, 5.0)
        assert drain["drained"] is True
        assert store.snapshot()["durable_unknown"] == 1
        assert reservation.slots_open == 0
        events = ProgressJournal(journal_path).read()
        assert [event["state"] for event in events] == [
            "socket_written", "unknown",
        ]
    finally:
        await transport.close()
        server.close()
        await server.wait_closed()
        await asyncio.to_thread(store.close, 5.0)


def test_aggregate_survives_segment_rotation_and_reclaim(tmp_path):
    journal_path = tmp_path / "worker-0.jsonl"
    aggregate_path = tmp_path / "worker-0.progress-aggregate.json"
    journal = ProgressJournal(
        journal_path, max_event_bytes=1024, segment_bytes=2048,
    )
    store = ProgressPersistence(
        journal,
        max_pending_items=256,
        max_pending_bytes=1024 * 1024,
        batch_size=16,
        flush_interval_seconds=0.01,
        aggregate_path=aggregate_path,
        aggregate_interval_seconds=0.1,
    )
    store.start()
    try:
        for index in range(40):
            reservation = store.reserve(items=1, bytes_total=2048)
            assert store.submit(
                _event(
                    f"event-{index}",
                    state="submit_sm_resp_success",
                    response_success=1,
                ),
                reservation,
            )
        assert store.drain(timeout=10)["drained"] is True
    finally:
        store.close(timeout=10)
    paths = ProgressJournal(journal_path).segment_paths()
    assert len(paths) > 1
    reclaim_offsets = {
        str(path): path.stat().st_size for path in paths[:-1]
    }
    removed = ProgressJournal(journal_path).reclaim(reclaim_offsets)
    assert len(removed) == len(paths) - 1

    restarted = ProgressPersistence(
        ProgressJournal(
            journal_path, max_event_bytes=1024, segment_bytes=2048,
        ),
        max_pending_items=256,
        max_pending_bytes=1024 * 1024,
        batch_size=16,
        flush_interval_seconds=0.01,
        aggregate_path=aggregate_path,
        aggregate_interval_seconds=0.1,
    )
    restarted.start()
    try:
        snapshot = restarted.snapshot()
        assert snapshot["aggregate_complete"] is True
        assert snapshot["durable_events"] == 40
        assert snapshot["durable_response_success"] == 40
    finally:
        restarted.close(timeout=10)


def test_startup_repairs_partial_journal_tail_before_append(tmp_path):
    journal_path = tmp_path / "worker-0.jsonl"
    ProgressJournal(journal_path).append(_event("one"))
    with journal_path.open("ab") as handle:
        handle.write(b'{"partial":')
        handle.flush()
        os.fsync(handle.fileno())
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "worker-0.progress-aggregate.json",
    )
    store.start()
    try:
        reservation = store.reserve(items=1, bytes_total=2048)
        assert store.submit(_event("two"), reservation)
        assert store.drain(timeout=5)["drained"] is True
    finally:
        store.close(timeout=5)
    events = ProgressJournal(journal_path).read()
    assert {event["event_id"] for event in events} == {"one", "two"}
    assert store.snapshot()["durable_events"] >= 2


def test_journal_capacity_backpressure_retries_without_permanent_failure(
    tmp_path, monkeypatch,
):
    journal = ProgressJournal(tmp_path / "worker-0.jsonl")
    original_append_many = journal.append_many
    attempts = {"count": 0}

    def flaky_append_many(events):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ProjectionBackpressure("injected capacity backpressure")
        return original_append_many(events)

    monkeypatch.setattr(journal, "append_many", flaky_append_many)
    store = ProgressPersistence(
        journal,
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.02,
        aggregate_path=tmp_path / "worker-0.progress-aggregate.json",
    )
    store.start()
    try:
        reservation = store.reserve(items=1, bytes_total=2048)
        assert store.submit(_event("retry-capacity"), reservation)
        drain = store.drain(timeout=5)
        assert drain["drained"] is True, drain
        snapshot = store.snapshot()
        assert snapshot["accepting"] is True
        assert snapshot["writer_errors"] == 0
        assert snapshot["writer_events_durable"] == 1
        assert attempts["count"] == 2
    finally:
        store.close(timeout=5)


def test_checkpointed_record_is_not_redispatched_after_restart(tmp_path):
    return asyncio.run(
        _test_checkpointed_record_is_not_redispatched_after_restart(tmp_path)
    )


async def _test_checkpointed_record_is_not_redispatched_after_restart(tmp_path):
    journal_path = tmp_path / "progress" / "worker-0.jsonl"
    journal_path.parent.mkdir(parents=True)
    store = ProgressPersistence(
        ProgressJournal(journal_path),
        max_pending_items=64,
        max_pending_bytes=64 * 1024,
        batch_size=8,
        flush_interval_seconds=0.01,
        aggregate_path=tmp_path / "progress" / "worker-0.progress-aggregate.json",
    )
    store.start()
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=1_000_000)
    spool.append_many([_record("once", 1, 1)])
    # Simulate a crash immediately after the durable dispatch checkpoint and
    # before the socket write.  Restart must not authorize a resend.
    spool.checkpoint(0, 1)
    sent: list[dict] = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}},
        transport_factory=lambda config, credits: _ReservedFakeTransport(
            config, credits, sent
        ),
        progress_store=store,
    )
    try:
        await worker.run_until_idle(close_transports=False)
        await worker.close_transports()
        await worker.drain_progress(timeout=5)
        assert sent == []
        assert spool.status()["pending_records"] == 0
        assert ProgressJournal(journal_path).read() == []
    finally:
        await worker.close_progress(timeout=5)
