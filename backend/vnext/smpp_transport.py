"""Minimal asyncio SMPP 3.4 transport for the VNext socket boundary."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import struct
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from .route_credits import RouteBindLease, RouteCreditBroker


HEADER = struct.Struct("!IIII")
BIND_RECEIVER = 0x00000001
BIND_TRANSMITTER = 0x00000002
BIND_TRANSCEIVER = 0x00000009
SUBMIT_SM = 0x00000004
ENQUIRE_LINK = 0x00000015
UNBIND = 0x00000006
RESPONSE_MASK = 0x80000000
ESME_ROK = 0


class SmppTransportError(RuntimeError):
    pass


class SmppWriteUnknown(SmppTransportError):
    """The connection failed after a write may have crossed the boundary."""


def cstring(value: str) -> bytes:
    return str(value or "").encode("ascii", errors="replace") + b"\x00"


def _split_text(message: str) -> tuple[list[bytes], int]:
    text = str(message or "")
    use_ucs2 = any(ord(character) > 0x7F for character in text)
    if use_ucs2:
        encoded = text.encode("utf-16-be")
        unit = 134  # 140 octets minus the six-octet concatenation UDH.
        if len(encoded) <= 140:
            return [encoded], 8
        return [encoded[index:index + unit] for index in range(0, len(encoded), unit)], 8
    encoded = text.encode("ascii", errors="replace")
    if len(encoded) <= 140:
        return [encoded], 0
    return [encoded[index:index + 134] for index in range(0, len(encoded), 134)], 0


def build_bind(sequence: int, system_id: str, password: str, system_type: str = "",
               bind_type: str = "transceiver") -> bytes:
    command_id = {
        "receiver": BIND_RECEIVER,
        "transmitter": BIND_TRANSMITTER,
        "transceiver": BIND_TRANSCEIVER,
    }.get(str(bind_type or "transceiver").strip().lower(), BIND_TRANSCEIVER)
    body = b"".join([
        cstring(system_id), cstring(password), cstring(system_type),
        bytes((0x34, 0, 0)), cstring(""),
    ])
    return HEADER.pack(16 + len(body), command_id, 0, int(sequence)) + body


def build_submit_sm(sequence: int, *, source_addr: str, destination_addr: str,
                    message: str, part: int = 1, total_parts: int = 1,
                    reference: int = 1, flash_sms: bool = False,
                    source_ton: int = 0, source_npi: int = 0,
                    destination_ton: int = 1, destination_npi: int = 1,
                    data_coding: int | None = None,
                    registered_delivery: int = 1) -> bytes:
    parts, detected_coding = _split_text(message)
    if len(parts) != int(total_parts):
        raise ValueError("total_parts does not match encoded message parts")
    effective_coding = detected_coding if data_coding is None else int(data_coding)
    body_message = parts[int(part) - 1]
    esm_class = 0x20 if flash_sms else 0
    if total_parts > 1:
        body_message = bytes((0x05, 0x00, 0x03, int(reference) & 0xFF,
                              int(total_parts) & 0xFF, int(part) & 0xFF)) + body_message
        esm_class |= 0x40
    if len(body_message) > 255:
        raise ValueError("SMPP short_message exceeds 255 octets")
    body = b"".join([
        cstring(""), bytes((int(source_ton) & 0xFF, int(source_npi) & 0xFF)), cstring(source_addr),
        bytes((int(destination_ton) & 0xFF, int(destination_npi) & 0xFF)), cstring(destination_addr),
        bytes((esm_class, 0, 0)), cstring(""), cstring(""),
        bytes((int(registered_delivery) & 0xFF, 0, int(effective_coding) & 0xFF, 0, len(body_message))), body_message,
    ])
    return HEADER.pack(16 + len(body), SUBMIT_SM, 0, int(sequence)) + body


async def read_pdu(reader: asyncio.StreamReader) -> tuple[int, int, int, bytes]:
    header = await reader.readexactly(16)
    length, command_id, status, sequence = HEADER.unpack(header)
    if length < 16 or length > 1024 * 1024:
        raise SmppTransportError(f"invalid SMPP PDU length {length}")
    return command_id, status, sequence, await reader.readexactly(length - 16)


@dataclass(frozen=True)
class SocketWriteResult:
    state: str
    parts_written: int
    parts_total: int
    error: str = ""


@dataclass
class _Pending:
    sequence: int
    sent_at: float
    route_id: str
    context: dict | None = None


class AsyncSmppTransport:
    """One non-blocking SMPP bind with a continuously running response reader."""

    def __init__(self, config: dict, *, credits: RouteCreditBroker | None = None,
                 response_callback: Callable[[dict], Awaitable[None] | None] | None = None):
        self.config = dict(config)
        self.route_id = str(self.config.get("route_id", self.config.get("id", "default")))
        self.host = str(self.config.get("host", "127.0.0.1"))
        self.port = int(self.config.get("port", 0))
        self.system_id = str(self.config.get("system_id", "test"))
        self.password = str(self.config.get("password", "test"))
        self.source_addr = str(self.config.get("source_addr", "VNEXT"))
        self.bind_type = str(self.config.get("bind_type", "transceiver") or "transceiver").strip().lower()
        if self.bind_type not in {"receiver", "transmitter", "transceiver"}:
            self.bind_type = "transceiver"
        self.pool_size = max(1, int(self.config.get("pool_size", 1) or 1))
        self.window_size = max(1, int(self.config.get("window_size", 50)))
        self.tps = max(0.0, float(self.config.get("tps", self.config.get("rate_tps", 0.0)) or 0.0))
        self.tps_scope = str(self.config.get("tps_scope", "route_shared") or "route_shared").strip().lower()
        if self.tps_scope not in {"route_shared", "per_bind"}:
            self.tps_scope = "route_shared"
        self.source_ton = int(self.config.get("address_ton", 0) or 0)
        self.source_npi = int(self.config.get("address_npi", 0) or 0)
        self.data_coding = int(self.config.get("data_coding", 0) or 0)
        self.delivery_receipt_mode = str(
            self.config.get("delivery_receipt_mode", "optional") or "optional"
        ).strip().lower()
        if self.delivery_receipt_mode not in {"required", "optional", "disabled"}:
            self.delivery_receipt_mode = "optional"
        self.timeout = max(0.2, float(self.config.get("timeout", 10.0)))
        self.enquire_interval = max(0.0, float(self.config.get("enquire_interval", 30.0) or 0.0))
        self.reconnect_max_seconds = max(
            0.0, float(self.config.get("reconnect_max_seconds", 60.0) or 0.0)
        )
        self.credits = credits or RouteCreditBroker()
        self.response_callback = response_callback
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._window = asyncio.Semaphore(self.window_size)
        self._pending: dict[int, _Pending] = {}
        self._sequence = 1
        self._connected = False
        self._closing = False
        self._bind_lease: RouteBindLease | None = None
        self.bind_slot: int | None = None
        self._reconnect_failures = 0
        self._reconnect_not_before = 0.0
        self.socket_written_parts = 0
        self.response_success = 0
        self.response_error = 0
        self.unknown_parts = 0
        self.write_failures = 0
        self.last_error = ""

    def set_response_sink(self, callback: Callable[[dict], Awaitable[None] | None] | None) -> None:
        """Attach the current worker's durable response sink."""

        self.response_callback = callback

    async def _admission_allowed(self, callback) -> bool:
        if callback is None:
            return True
        try:
            value = callback()
            if inspect.isawaitable(value):
                value = await value
            return bool(value)
        except Exception as exc:
            self.last_error = f"socket admission check failed: {type(exc).__name__}: {exc}"
            return False

    @property
    def connected(self) -> bool:
        return bool(self._connected and self.writer and not self.writer.is_closing())

    def _next_sequence(self) -> int:
        self._sequence = (self._sequence % 0x7FFFFFFE) + 1
        return self._sequence

    def _schedule_reconnect(self) -> None:
        self._reconnect_failures += 1
        if self.reconnect_max_seconds <= 0:
            self._reconnect_not_before = 0.0
            return
        delay = min(
            self.reconnect_max_seconds,
            0.25 * (2 ** min(self._reconnect_failures - 1, 8)),
        )
        self._reconnect_not_before = time.monotonic() + delay

    async def _heartbeat(self) -> None:
        try:
            while self.connected and self.enquire_interval > 0:
                await asyncio.sleep(self.enquire_interval)
                if not self.connected or self.writer is None:
                    return
                sequence = self._next_sequence()
                async with self._write_lock:
                    self.writer.write(HEADER.pack(16, ENQUIRE_LINK, 0, sequence))
                    await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            await self.close()

    async def connect(self) -> None:
        if self.connected:
            return
        async with self._connect_lock:
            if self.connected:
                return
            try:
                wait = self._reconnect_not_before - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._bind_lease = await self.credits.acquire_bind(
                    self.route_id, self.pool_size,
                    timeout=max(self.timeout, self.reconnect_max_seconds, 30.0),
                )
                self.bind_slot = self._bind_lease.slot
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port), timeout=self.timeout
                )
                sequence = 1
                async with self._write_lock:
                    self.writer.write(build_bind(
                        sequence, self.system_id, self.password, bind_type=self.bind_type,
                    ))
                    await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)
                command_id, status, response_sequence, _ = await asyncio.wait_for(
                    read_pdu(self.reader), timeout=self.timeout
                )
                bind_command = {
                    "receiver": BIND_RECEIVER,
                    "transmitter": BIND_TRANSMITTER,
                    "transceiver": BIND_TRANSCEIVER,
                }[self.bind_type]
                if command_id != (bind_command | RESPONSE_MASK) or response_sequence != sequence or status != ESME_ROK:
                    raise SmppTransportError(f"bind failed status=0x{status:08x} command=0x{command_id:08x}")
                self._connected = True
                self._closing = False
                self._reconnect_failures = 0
                self._reconnect_not_before = 0.0
                self._reader_task = asyncio.create_task(self._read_responses(), name=f"vnext-smpp-reader-{self.route_id}")
                if self.enquire_interval > 0:
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat(), name=f"vnext-smpp-heartbeat-{self.route_id}"
                    )
            except Exception:
                await self.close()
                self._schedule_reconnect()
                raise

    async def _read_responses(self) -> None:
        assert self.reader is not None
        try:
            while True:
                command_id, status, sequence, body = await read_pdu(self.reader)
                if command_id == (SUBMIT_SM | RESPONSE_MASK):
                    pending = self._pending.pop(sequence, None)
                    if pending is None:
                        self.unknown_parts += 1
                        continue
                    if status == ESME_ROK:
                        self.response_success += 1
                    else:
                        self.response_error += 1
                    self._window.release()
                    event = {
                        "route_id": self.route_id,
                        "sequence": sequence,
                        "status": int(status),
                        "success": status == ESME_ROK,
                        "latency_seconds": max(0.0, time.monotonic() - pending.sent_at),
                        "body_length": len(body),
                        "context": dict(pending.context or {}),
                    }
                    if self.response_callback:
                        result = self.response_callback(event)
                        if asyncio.iscoroutine(result):
                            await result
                elif command_id == ENQUIRE_LINK:
                    if self.writer and not self.writer.is_closing():
                        async with self._write_lock:
                            self.writer.write(HEADER.pack(16, ENQUIRE_LINK | RESPONSE_MASK, 0, sequence))
                            await self.writer.drain()
                elif command_id == (ENQUIRE_LINK | RESPONSE_MASK):
                    # Heartbeat response; no PDU window credit is involved.
                    continue
                elif command_id == (UNBIND | RESPONSE_MASK):
                    return
        except (asyncio.IncompleteReadError, ConnectionError, OSError, SmppTransportError) as exc:
            if not self._closing:
                self.last_error = f"{type(exc).__name__}: {exc}"
        finally:
            if not self._closing:
                self._connected = False
                stale_writer = self.writer
                outstanding = list(self._pending.values())
                self._pending.clear()
                for _ in outstanding:
                    self._window.release()
                self.unknown_parts += len(outstanding)
                if stale_writer is not None and not stale_writer.is_closing():
                    stale_writer.close()
                self.reader = None
                self.writer = None
                lease = self._bind_lease
                self._bind_lease = None
                self.bind_slot = None
                if lease is not None:
                    lease.release()

    async def send_task(self, *, destination_addr: str, message: str, flash_sms: bool = False,
                        reference: int = 1, source_addr: str | None = None,
                        before_write: Callable[[], bool | Awaitable[bool]] | None = None,
                        progress_context: dict | None = None) -> SocketWriteResult:
        parts, _coding = _split_text(message)
        if not parts:
            return SocketWriteResult("write_failed", 0, 0, "empty message")
        if self.bind_type == "receiver":
            return SocketWriteResult(
                "write_failed", 0, len(parts),
                "receiver bind cannot submit_sm; choose transmitter or transceiver",
            )
        try:
            await self.connect()
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.write_failures += 1
            return SocketWriteResult("write_failed", 0, len(parts), self.last_error)
        written = 0
        for index in range(1, len(parts) + 1):
            await self.credits.acquire(
                self.route_id, self.tps, tps_scope=self.tps_scope, bind_slot=self.bind_slot,
            )
            if not await self._admission_allowed(before_write):
                if before_write is not None:
                    state = "unknown" if written else "expired"
                    return SocketWriteResult(
                        state, written, len(parts),
                        self.last_error or "business day cutoff reached before socket write",
                    )
            try:
                await asyncio.wait_for(self._window.acquire(), timeout=self.timeout)
            except asyncio.TimeoutError:
                self.write_failures += 1
                self.last_error = "SMPP response window did not release before timeout"
                return SocketWriteResult("write_failed", written, len(parts), self.last_error)
            if not await self._admission_allowed(before_write):
                if before_write is not None:
                    self._window.release()
                    state = "unknown" if written else "expired"
                    return SocketWriteResult(
                        state, written, len(parts),
                        self.last_error or "business day cutoff reached before socket write",
                    )
            sequence = self._next_sequence()
            packet = build_submit_sm(
                sequence, source_addr=str(source_addr or self.source_addr),
                destination_addr=str(destination_addr), message=message,
                part=index, total_parts=len(parts), reference=reference,
                flash_sms=flash_sms,
                source_ton=self.source_ton, source_npi=self.source_npi,
                data_coding=(self.data_coding or None),
                registered_delivery=0 if self.delivery_receipt_mode == "disabled" else 1,
            )
            self._pending[sequence] = _Pending(
                sequence, time.monotonic(), self.route_id,
                dict(progress_context or {}),
            )
            try:
                assert self.writer is not None
                async with self._write_lock:
                    if not await self._admission_allowed(before_write):
                        if before_write is not None:
                            self._pending.pop(sequence, None)
                            self._window.release()
                            state = "unknown" if written else "expired"
                            return SocketWriteResult(
                                state, written, len(parts),
                                "business day cutoff reached immediately before socket write",
                            )
                    self.writer.write(packet)
                    await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)
                self.socket_written_parts += 1
                written += 1
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                self._pending.pop(sequence, None)
                self._window.release()
                self.write_failures += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                state = "unknown" if written or self.writer and self.writer.is_closing() else "write_failed"
                return SocketWriteResult(state, written, len(parts), self.last_error)
        return SocketWriteResult("socket_written", written, len(parts))

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat and heartbeat is not asyncio.current_task():
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat
        task = self._reader_task
        self._reader_task = None
        if task and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self.writer and not self.writer.is_closing():
            try:
                async with self._write_lock:
                    self.writer.write(HEADER.pack(16, UNBIND, 0, self._next_sequence()))
                    await self.writer.drain()
            except Exception:
                pass
            self.writer.close()
            with contextlib.suppress(Exception):
                await self.writer.wait_closed()
        outstanding = len(self._pending)
        self._pending.clear()
        for _ in range(outstanding):
            self._window.release()
        self.reader = None
        self.writer = None
        lease = self._bind_lease
        self._bind_lease = None
        self.bind_slot = None
        if lease is not None:
            lease.release()
        self._closing = False

    def snapshot(self) -> dict:
        return {
            "route_id": self.route_id,
            "connected": self.connected,
            "bind_type": self.bind_type,
            "bind_slot": self.bind_slot,
            "pool_size": self.pool_size,
            "window_size": self.window_size,
            "tps": self.tps,
            "tps_scope": self.tps_scope,
            "effective_total_limit_tps": self.tps * self.pool_size if self.tps_scope == "per_bind" else self.tps,
            "tps_unit": "submit_sm_parts/s",
            "delivery_receipt_mode": self.delivery_receipt_mode,
            "enquire_interval": self.enquire_interval,
            "reconnect_max_seconds": self.reconnect_max_seconds,
            "window_inflight": len(self._pending),
            "socket_written_parts": self.socket_written_parts,
            "response_success": self.response_success,
            "response_error": self.response_error,
            "unknown_parts": self.unknown_parts,
            "write_failures": self.write_failures,
            "reconnect_failures": self._reconnect_failures,
            "reconnect_backoff_seconds": round(
                max(0.0, self._reconnect_not_before - time.monotonic()), 3
            ),
            "last_error": self.last_error,
        }
