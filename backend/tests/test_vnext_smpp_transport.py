from __future__ import annotations

import asyncio
import pytest

from vnext.route_credits import RouteCreditBroker
from vnext.smpp_transport import (
    BIND_TRANSCEIVER,
    HEADER,
    RESPONSE_MASK,
    SUBMIT_SM,
    AsyncSmppTransport,
    build_submit_sm,
    read_pdu,
)


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
