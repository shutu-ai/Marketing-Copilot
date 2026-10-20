from __future__ import annotations

import asyncio
import pytest

from vnext.route_credits import RouteCreditBroker, RouteCreditError
from vnext.smpp_transport import (
    BIND_RECEIVER,
    BIND_TRANSMITTER,
    BIND_TRANSCEIVER,
    ENQUIRE_LINK,
    HEADER,
    RESPONSE_MASK,
    SUBMIT_SM,
    AsyncSmppTransport,
    build_submit_sm,
    build_bind,
    read_pdu,
)
from tools.smsc_simulator import parse_submit_sm


async def _loopback_server():
    received = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            command, _status, sequence, _body = await read_pdu(reader)
            assert command == BIND_TRANSCEIVER
            writer.write(HEADER.pack(22, BIND_TRANSCEIVER | RESPONSE_MASK, 0, sequence) + b"LOCAL\x00")
            await writer.drain()
            while True:
                command, _status, sequence, body = await read_pdu(reader)
                if command == SUBMIT_SM:
                    received.append((sequence, body))
                    writer.write(HEADER.pack(19, SUBMIT_SM | RESPONSE_MASK, 0, sequence) + b"id\x00")
                    await writer.drain()
                else:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, received


async def _edge_case_loopback_server():
    observed = {"enquire_response": None}

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            command, _status, sequence, _body = await read_pdu(reader)
            assert command == BIND_TRANSCEIVER
            writer.write(
                HEADER.pack(22, BIND_TRANSCEIVER | RESPONSE_MASK, 0, sequence)
                + b"LOCAL\x00"
            )
            await writer.drain()
            writer.write(HEADER.pack(16, ENQUIRE_LINK, 0, 777))
            await writer.drain()
            sequences = []
            responded = False
            while len(sequences) < 2 or observed["enquire_response"] is None:
                command, _status, sequence, _body = await read_pdu(reader)
                if command == (ENQUIRE_LINK | RESPONSE_MASK):
                    observed["enquire_response"] = (command, sequence)
                    continue
                assert command == SUBMIT_SM
                sequences.append(sequence)
                if len(sequences) < 2 or responded:
                    continue
                responded = True
                for response_sequence in reversed(sequences):
                    writer.write(
                        HEADER.pack(
                            19, SUBMIT_SM | RESPONSE_MASK, 0, response_sequence,
                        )
                        + b"id\x00"
                    )
                await writer.drain()
                writer.write(
                    HEADER.pack(
                        19, SUBMIT_SM | RESPONSE_MASK, 0, sequences[0],
                    )
                    + b"id\x00"
                )
                writer.write(
                    HEADER.pack(19, SUBMIT_SM | RESPONSE_MASK, 0, 999999)
                    + b"id\x00"
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

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, observed


def test_async_transport_reports_socket_written_without_waiting_for_response():
    return asyncio.run(_test_async_transport_reports_socket_written_without_waiting_for_response())


async def _test_async_transport_reports_socket_written_without_waiting_for_response():
    server, received = await _loopback_server()
    port = server.sockets[0].getsockname()[1]
    transport = AsyncSmppTransport({
        "route_id": 1, "host": "127.0.0.1", "port": port,
        "system_id": "test", "password": "test", "source_addr": "VNEXT",
        "window_size": 4, "tps": 0,
    })
    try:
        results = await asyncio.wait_for(asyncio.gather(*[
            transport.send_task(destination_addr=f"251900000{index:03d}", message=f"m-{index}")
            for index in range(100)
        ]), timeout=15)
        assert all(result.state == "socket_written" for result in results), (
            [(result.state, result.error) for result in results if result.state != "socket_written"]
            + [transport.snapshot(), {"received": len(received)}]
        )
        assert sum(result.parts_written for result in results) == 100
        await asyncio.sleep(0.1)
        assert len(received) == 100
        assert transport.response_success == 100
        assert transport.snapshot()["socket_written_parts_1m"] == 100
        assert transport.snapshot()["response_success_1m"] == 100
        assert transport.snapshot()["window_inflight"] == 0
    finally:
        await transport.close()
        server.close()
        await server.wait_closed()


def test_submit_sm_multipart_contains_standard_concat_udh():
    packet = build_submit_sm(
        7, source_addr="VNEXT", destination_addr="251900000001",
        message="x" * 300, part=1, total_parts=3, reference=9,
    )
    length, command, status, sequence = HEADER.unpack(packet[:16])
    assert length == len(packet)
    assert command == SUBMIT_SM
    assert status == 0 and sequence == 7
    assert b"\x05\x00\x03\x09\x03\x01" in packet


@pytest.mark.parametrize(
    ("message", "configured_coding", "expected_coding"),
    [
        ("hello", None, 0xF0),
        ("hello", 0x08, 0xF0),
        ("እንደምን", None, 0x18),
        ("እንደምን", 0x00, 0x18),
    ],
)
def test_flash_submit_sm_uses_class_zero_dcs_and_preserves_esm(
    message, configured_coding, expected_coding,
):
    packet = build_submit_sm(
        8, source_addr="VNEXT", destination_addr="251900000001",
        message=message, flash_sms=True, data_coding=configured_coding,
        registered_delivery=0,
    )

    fields = parse_submit_sm(packet[16:])
    assert fields["data_coding"] == expected_coding
    assert fields["esm_class"] == 0
    assert fields["text"] == message


def test_smpp_bind_and_submit_honor_route_protocol_fields():
    _length, command, _status, _sequence = HEADER.unpack(
        build_bind(1, "sid", "pwd", bind_type="transmitter")[:16]
    )
    assert command == BIND_TRANSMITTER
    _length, command, _status, _sequence = HEADER.unpack(
        build_bind(1, "sid", "pwd", bind_type="receiver")[:16]
    )
    assert command == BIND_RECEIVER

    packet = build_submit_sm(
        9, source_addr="VNEXT", destination_addr="251900000001", message="hello",
        source_ton=5, source_npi=6, destination_ton=2, destination_npi=3,
        data_coding=8, registered_delivery=0,
    )
    assert b"\x05\x06VNEXT\x00\x02\x03" in packet
    assert b"\x00\x00\x08\x00\x05hello" in packet


def test_file_route_credit_schedule_is_shared_by_two_brokers(tmp_path):
    return asyncio.run(_test_file_route_credit_schedule_is_shared_by_two_brokers(tmp_path))


async def _test_file_route_credit_schedule_is_shared_by_two_brokers(tmp_path):
    first = RouteCreditBroker(tmp_path / "credits")
    second = RouteCreditBroker(tmp_path / "credits")
    await asyncio.gather(*[
        broker.acquire("route-a", 50)
        for broker in (first, second)
        for _ in range(5)
    ])
    state = (tmp_path / "credits" / "route-route-a.json").read_text(encoding="utf-8")
    value = __import__("json").loads(state)
    assert value["next_at"] - value["updated_at"] >= 0.15
    assert first.snapshot()["file_credit_refills"] == 1
    assert second.snapshot()["file_credit_refills"] == 1


def test_file_route_credit_broker_handles_four_process_like_bursts(tmp_path):
    return asyncio.run(_test_file_route_credit_broker_handles_four_process_like_bursts(tmp_path))


async def _test_file_route_credit_broker_handles_four_process_like_bursts(tmp_path):
    brokers = [RouteCreditBroker(tmp_path / "credits", lock_timeout=10) for _ in range(4)]
    await asyncio.gather(*[
        broker.acquire("route-burst", 80)
        for broker in brokers
        for _ in range(32)
    ])


def test_route_credit_batching_and_per_bind_scope_are_explicit(tmp_path):
    return asyncio.run(_test_route_credit_batching_and_per_bind_scope_are_explicit(tmp_path))


async def _test_route_credit_batching_and_per_bind_scope_are_explicit(tmp_path):
    broker = RouteCreditBroker(tmp_path / "credits")
    for _ in range(65):
        await broker.acquire("route-batched", 100, tps_scope="route_shared")
    snapshot = broker.snapshot()
    assert snapshot["reservations"] == 65
    assert snapshot["file_credit_refills"] == 9
    assert snapshot["credit_lease_size"] == 8

    per_bind = RouteCreditBroker(tmp_path / "per-bind")
    await asyncio.gather(*[
        per_bind.acquire("route-scoped", 100, tps_scope="per_bind", bind_slot=slot)
        for slot in (1, 2)
        for _ in range(32)
    ])
    assert len(list((tmp_path / "per-bind").glob("route-route-scoped_bind_*.json"))) == 2


def test_local_bind_pool_is_bounded_and_reusable():
    return asyncio.run(_test_local_bind_pool_is_bounded_and_reusable())


async def _test_local_bind_pool_is_bounded_and_reusable():
    broker = RouteCreditBroker()
    first = await broker.acquire_bind("route", 2, timeout=0.1)
    second = await broker.acquire_bind("route", 2, timeout=0.1)
    with pytest.raises(RouteCreditError, match="timed out acquiring SMPP bind slot"):
        await broker.acquire_bind("route", 2, timeout=0.1)
    first.release()
    replacement = await broker.acquire_bind("route", 2, timeout=0.1)
    assert replacement.slot == first.slot
    second.release()
    replacement.release()


def test_out_of_order_duplicate_and_unknown_responses_keep_window_consistent():
    return asyncio.run(
        _test_out_of_order_duplicate_and_unknown_responses_keep_window_consistent()
    )


async def _test_out_of_order_duplicate_and_unknown_responses_keep_window_consistent():
    server, observed = await _edge_case_loopback_server()
    port = server.sockets[0].getsockname()[1]
    callback_events = []

    def callback(event):
        callback_events.append(event)

    transport = AsyncSmppTransport(
        {
            "route_id": 1, "host": "127.0.0.1", "port": port,
            "system_id": "test", "password": "test", "source_addr": "VNEXT",
            "window_size": 2, "tps": 0, "timeout": 5, "enquire_interval": 0,
        },
        response_callback=callback,
    )
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                transport.send_task(
                    destination_addr="251900000001", message="one",
                ),
                transport.send_task(
                    destination_addr="251900000002", message="two",
                ),
            ),
            timeout=10,
        )
        assert [result.state for result in results] == [
            "socket_written", "socket_written",
        ]
        deadline = asyncio.get_running_loop().time() + 3.0
        while (
            transport.unexpected_response_pdus < 2
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.01)
        assert observed["enquire_response"] == (
            ENQUIRE_LINK | RESPONSE_MASK, 777,
        )
        assert len(callback_events) == 2
        assert all(event["success"] for event in callback_events)
        assert transport.response_success == 2
        assert transport.unknown_parts == 2
        assert transport.unexpected_response_pdus == 2
        assert transport.snapshot()["window_inflight"] == 0
    finally:
        await transport.close()
        server.close()
        await server.wait_closed()
