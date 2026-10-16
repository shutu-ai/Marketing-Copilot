"""Minimal SMPP 3.4 SMSC simulator for local end-to-end testing.

It accepts bind_transceiver/transmitter/receiver, submit_sm, enquire_link and
unbind. Every submit_sm receives a successful submit_sm_resp followed by a
DELIVRD deliver_sm receipt.
"""

import argparse
import asyncio
from collections import deque
from contextlib import suppress
import hashlib
import itertools
import json
import logging
import random
import signal
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HEADER = struct.Struct("!IIII")
ESME_ROK = 0
BIND_RECEIVER = 0x00000001
BIND_TRANSMITTER = 0x00000002
SUBMIT_SM = 0x00000004
DELIVER_SM = 0x00000005
UNBIND = 0x00000006
BIND_TRANSCEIVER = 0x00000009
ENQUIRE_LINK = 0x00000015
GENERIC_NACK = 0x80000000
RESPONSE_MASK = 0x80000000

COMMAND_NAMES = {
    BIND_RECEIVER: "bind_receiver", BIND_TRANSMITTER: "bind_transmitter",
    BIND_TRANSCEIVER: "bind_transceiver", SUBMIT_SM: "submit_sm",
    DELIVER_SM: "deliver_sm", UNBIND: "unbind", ENQUIRE_LINK: "enquire_link",
}


def cstring(value: str) -> bytes:
    return value.encode("ascii", errors="replace") + b"\x00"


def tlv(tag: int, value: bytes) -> bytes:
    return struct.pack("!HH", tag, len(value)) + value


class BodyReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def byte(self) -> int:
        if self.pos >= len(self.data):
            raise ValueError("unexpected end of PDU body")
        value = self.data[self.pos]
        self.pos += 1
        return value

    def cstring(self) -> str:
        end = self.data.find(b"\x00", self.pos)
        if end < 0:
            raise ValueError("unterminated C string")
        value = self.data[self.pos:end].decode("ascii", errors="replace")
        self.pos = end + 1
        return value

    def bytes(self, size: int) -> bytes:
        value = self.data[self.pos:self.pos + size]
        if len(value) != size:
            raise ValueError("short PDU field")
        self.pos += size
        return value


def decode_short_message(data: bytes, data_coding: int, esm_class: int):
    concat = None
    payload = data
    if esm_class & 0x40 and data:
        udh_length = data[0]
        udh = data[1:1 + udh_length]
        payload = data[1 + udh_length:]
        if len(udh) >= 5 and udh[:2] == b"\x00\x03":
            concat = {"reference": udh[2], "total": udh[3], "part": udh[4]}
    if data_coding == 8:
        text = payload.decode("utf-16-be", errors="replace")
    else:
        gsm_overrides = {
            0x00: "@", 0x02: "$", 0x0A: "\n", 0x0D: "\r", 0x11: "_",
            0x1C: "Æ", 0x1D: "æ", 0x1E: "ß", 0x1F: "É",
            0x40: "¡", 0x5B: "Ä", 0x5C: "Ö", 0x5D: "Ñ", 0x5E: "Ü",
            0x5F: "§", 0x60: "¿", 0x7B: "ä", 0x7C: "ö", 0x7D: "ñ",
            0x7E: "ü", 0x7F: "à",
        }
        chars = []
        for value in payload:
            if value in gsm_overrides:
                chars.append(gsm_overrides[value])
            elif 0x20 <= value <= 0x7A:
                chars.append(chr(value))
            else:
                chars.append("�")
        text = "".join(chars)
    return text, concat


def parse_submit_sm(body: bytes) -> dict:
    r = BodyReader(body)
    service_type = r.cstring()
    source_ton, source_npi, source_addr = r.byte(), r.byte(), r.cstring()
    dest_ton, dest_npi, destination_addr = r.byte(), r.byte(), r.cstring()
    esm_class, protocol_id, priority_flag = r.byte(), r.byte(), r.byte()
    schedule_delivery_time, validity_period = r.cstring(), r.cstring()
    registered_delivery = r.byte()
    replace_if_present = r.byte()
    data_coding, default_msg_id, sm_length = r.byte(), r.byte(), r.byte()
    short_message = r.bytes(sm_length)
    text, concat = decode_short_message(short_message, data_coding, esm_class)
    return {
        "service_type": service_type,
        "source_addr": source_addr,
        "source_ton": source_ton,
        "source_npi": source_npi,
        "destination_addr": destination_addr,
        "dest_ton": dest_ton,
        "dest_npi": dest_npi,
        "esm_class": esm_class,
        "protocol_id": protocol_id,
        "priority_flag": priority_flag,
        "schedule_delivery_time": schedule_delivery_time,
        "validity_period": validity_period,
        "registered_delivery": registered_delivery,
        "replace_if_present": replace_if_present,
        "data_coding": data_coding,
        "default_msg_id": default_msg_id,
        "text": text,
        "concat": concat,
        "raw_hex": short_message.hex(),
    }


class AuditWriter:
    """Bounded asynchronous JSONL writer for protocol-boundary audit events.

    The responder never waits for disk I/O.  ``put_nowait`` is deliberate: an
    audit queue overflow is a harness failure, not permission to silently lose
    evidence and report a throughput pass.
    """

    def __init__(self, path: Path, maxsize: int, batch_size: int, flush_interval: float):
        self.path = path
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=max(1, int(maxsize)))
        self.batch_size = max(1, int(batch_size))
        self.flush_interval = max(0.001, float(flush_interval))
        self.task: asyncio.Task | None = None
        self.stopping = False
        self.error: str | None = None
        self.enqueued = 0
        self.written = 0
        self.max_backlog = 0

    async def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.task = asyncio.create_task(self._run(), name="smsc-audit-writer")

    def enqueue(self, event: dict) -> bool:
        if self.error:
            return False
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            return False
        self.enqueued += 1
        self.max_backlog = max(self.max_backlog, self.queue.qsize())
        return True

    async def _run(self) -> None:
        while not self.stopping or not self.queue.empty():
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                continue
            batch = [first]
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.writelines(
                        json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n"
                        for item in batch
                    )
                    handle.flush()
                self.written += len(batch)
            except Exception as exc:  # pragma: no cover - exercised by integration environments
                self.error = f"{type(exc).__name__}: {exc}"
            finally:
                for _ in batch:
                    self.queue.task_done()

    async def close(self) -> None:
        self.stopping = True
        if self.task:
            await self.task
            self.task = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "queue_capacity": self.queue.maxsize,
            "batch_size": self.batch_size,
            "enqueued": self.enqueued,
            "written": self.written,
            "backlog": self.queue.qsize(),
            "max_backlog": self.max_backlog,
            "overflow": 0,
            "write_error": self.error,
        }


class SimulatorStats:
    """Low-overhead aggregate counters and bounded latency samples."""

    def __init__(self) -> None:
        self.received = 0
        self.response_sent = 0
        self.response_success = 0
        self.response_failed = 0
        self.response_dropped = 0
        self.bind_attempts = 0
        self.bind_count = 0
        self.inflight = 0
        self.max_inflight = 0
        self.connected = 0
        self.disconnected = 0
        self.dlr_generated = 0
        self.errors = 0
        self.error_messages: deque[str] = deque(maxlen=20)
        self.response_latencies_ms: deque[float] = deque(maxlen=100_000)
        self.parse_to_response_latencies_ms: deque[float] = deque(maxlen=100_000)

    def enter(self) -> None:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)

    def leave(self) -> None:
        self.inflight = max(0, self.inflight - 1)

    def error(self, message: str) -> None:
        self.errors += 1
        self.error_messages.append(message)

    @staticmethod
    def _percentiles(values: deque[float]) -> dict[str, float | None]:
        if not values:
            return {"p50": None, "p95": None, "p99": None}
        ordered = sorted(values)

        def value(percentile: float) -> float:
            index = (len(ordered) - 1) * percentile
            lower = int(index)
            upper = min(len(ordered) - 1, lower + 1)
            fraction = index - lower
            return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)

        return {"p50": value(0.50), "p95": value(0.95), "p99": value(0.99)}

    def snapshot(self) -> dict[str, Any]:
        return {
            "received": self.received,
            "response_sent": self.response_sent,
            "response_success": self.response_success,
            "response_failed": self.response_failed,
            "response_dropped": self.response_dropped,
            "bind_attempts": self.bind_attempts,
            "bind_count": self.bind_count,
            "inflight": self.inflight,
            "max_inflight": self.max_inflight,
            "connected": self.connected,
            "disconnected": self.disconnected,
            "dlr_generated": self.dlr_generated,
            "error_count": self.errors,
            "errors": list(self.error_messages),
            "response_latency_ms": self._percentiles(self.response_latencies_ms),
            "parse_to_response_write_ms": self._percentiles(self.parse_to_response_latencies_ms),
            "latency_sample_count": len(self.response_latencies_ms),
        }


@dataclass
class Session:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    server: "SmscSimulator"
    peer: str
    bound: bool = False
    system_id: str = ""
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    multipart: dict = field(default_factory=dict)
    closed: bool = False
    submit_slots: asyncio.Semaphore = field(init=False)
    submit_tasks: set[asyncio.Task] = field(default_factory=set, init=False)
    receipt_tasks: set[asyncio.Task] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        self.submit_slots = asyncio.Semaphore(self.server.max_inflight_per_connection)

    async def send_pdu(self, command_id: int, sequence: int, body: bytes = b"", status: int = 0):
        if self.closed or self.writer.is_closing():
            raise ConnectionError(f"SMPP session {self.peer} is closed")
        packet = HEADER.pack(16 + len(body), command_id, status, sequence) + body
        async with self.write_lock:
            self.writer.write(packet)
            await self.writer.drain()

    async def handle_bind(self, command_id: int, sequence: int, body: bytes):
        r = BodyReader(body)
        system_id, password = r.cstring(), r.cstring()
        self.server.stats.bind_attempts += 1
        status = ESME_ROK
        if self.server.system_id and system_id != self.server.system_id:
            status = 0x0000000F
        elif self.server.password and password != self.server.password:
            status = 0x0000000E
        self.bound = status == ESME_ROK
        self.system_id = system_id
        if self.bound:
            self.server.stats.bind_count += 1
        self.server.record_event({
            "event_type": "bind",
            "peer": self.peer,
            "status": status,
            "success": self.bound,
        })
        self.server.log_fault(
            f"[{self.peer}] BIND result={'OK' if self.bound else hex(status)}"
        )
        await self.send_pdu(command_id | RESPONSE_MASK, sequence, cstring("LOCAL-SMSC"), status)

    async def handle_submit(self, sequence: int, body: bytes):
        received_at = time.monotonic()
        message = parse_submit_sm(body)
        self.server.stats.received += 1
        submit_index = self.server.next_submit_index()
        await self.server.wait_for_tps()
        message_id = f"{next(self.server.message_ids):010d}"
        response_status = self.server.response_status(submit_index)
        response_dropped = self.server.should_drop_response(submit_index)
        disconnect_after = self.server.should_disconnect(submit_index)
        disconnect_before_response = self.server.should_disconnect_before_response(submit_index)
        event = {
            "event_type": "submit_sm",
            "peer": self.peer,
            "sequence": sequence,
            "submit_index": submit_index,
            "message_id": message_id,
            "response_status": response_status,
            "response_dropped": response_dropped,
            "disconnect_after": disconnect_after,
            "disconnect_before_response": disconnect_before_response,
            "source_digest": hashlib.sha256(
                str(message.get("source_addr", "")).encode("utf-8")
            ).hexdigest()[:16],
            "destination_digest": hashlib.sha256(
                str(message.get("destination_addr", "")).encode("utf-8")
            ).hexdigest()[:16],
            **message,
        }
        self.server.record_event(event)
        self.server.log_fault(
            f"[{self.peer}] submit_sm index={submit_index} sequence={sequence} "
            f"status=0x{response_status:08x} dropped={response_dropped}"
        )
        concat = message.get("concat")
        if concat:
            key = (message["source_addr"], message["destination_addr"], concat["reference"])
            parts = self.multipart.setdefault(key, {})
            parts[concat["part"]] = message["text"]
            if len(parts) == concat["total"]:
                self.server.log_fault(
                    f"[{self.peer}] reassembled multipart parts={concat['total']}"
                )
                self.multipart.pop(key, None)

        if disconnect_before_response:
            # The PDU crossed the protocol boundary, but the response is
            # deliberately not written. This is the controlled ambiguous
            # write needed to prove UNKNOWN persistence and no whole-task
            # retry on a fresh connection.
            self.server.log_fault(
                f"[{self.peer}] DISCONNECT before submit_sm_resp "
                f"submit_index={submit_index} sequence={sequence}"
            )
            self.request_close()
            response_latency_ms = max(0.0, (time.monotonic() - received_at) * 1000.0)
            self.server.stats.response_latencies_ms.append(response_latency_ms)
            self.server.stats.parse_to_response_latencies_ms.append(response_latency_ms)
            self.server.record_event({
                "event_type": "submit_sm_resp",
                "peer": self.peer,
                "sequence": sequence,
                "submit_index": submit_index,
                "message_id": message_id,
                "status": response_status,
                "sent": False,
                "dropped": False,
                "disconnect_before_response": True,
                "latency_ms": round(response_latency_ms, 3),
                "error": "controlled disconnect before response",
            })
            return

        if self.server.submit_response_delay:
            await asyncio.sleep(self.server.submit_response_delay)
        response_sent = False
        response_error = None
        if not response_dropped:
            try:
                await self.send_pdu(
                    SUBMIT_SM | RESPONSE_MASK,
                    sequence,
                    cstring(message_id) if response_status == 0 else b"",
                    response_status,
                )
                response_sent = True
                self.server.stats.response_sent += 1
                if response_status == 0:
                    self.server.stats.response_success += 1
                else:
                    self.server.stats.response_failed += 1
            except Exception as exc:
                response_error = f"{type(exc).__name__}: {exc}"
                self.server.record_error(response_error)
        else:
            self.server.stats.response_dropped += 1
            self.server.log_fault(f"[{self.peer}] DROP submit_sm_resp sequence={sequence}")

        response_latency_ms = max(0.0, (time.monotonic() - received_at) * 1000.0)
        self.server.stats.response_latencies_ms.append(response_latency_ms)
        self.server.stats.parse_to_response_latencies_ms.append(response_latency_ms)
        self.server.record_event({
            "event_type": "submit_sm_resp",
            "peer": self.peer,
            "sequence": sequence,
            "submit_index": submit_index,
            "message_id": message_id,
            "status": response_status,
            "sent": response_sent,
            "dropped": response_dropped,
            "latency_ms": round(response_latency_ms, 3),
            "error": response_error,
        })
        if (
            response_status == 0
            and response_sent
            and self.server.dlr_enabled
            and message["registered_delivery"]
            and not self.server.should_drop_receipt(submit_index)
        ):
            task = asyncio.create_task(
                self.send_delivery_receipt(message, message_id),
                name=f"smsc-dlr-{submit_index}",
            )
            self.receipt_tasks.add(task)
            task.add_done_callback(self._receipt_done)
        if disconnect_after:
            self.server.log_fault(f"[{self.peer}] DISCONNECT after submit_index={submit_index}")
            self.request_close()

    def _receipt_done(self, task: asyncio.Task) -> None:
        self.receipt_tasks.discard(task)
        with suppress(asyncio.CancelledError):
            error = task.exception()
            if error:
                self.server.record_error(f"DLR task failed: {type(error).__name__}: {error}")

    async def send_delivery_receipt(self, message: dict, message_id: str):
        await asyncio.sleep(self.server.receipt_delay)
        submit_date = datetime.now().strftime("%y%m%d%H%M")
        receipt_text = (
            f"id:{message_id} sub:001 dlvrd:001 submit date:{submit_date} "
            f"done date:{submit_date} stat:{self.server.receipt_state} err:000 text:{message['text'][:20]}"
        ).encode("ascii", errors="replace")
        body = b"".join([
            cstring(""), bytes([1, 1]), cstring(message["destination_addr"]),
            bytes([1, 1]), cstring(message["source_addr"]),
            bytes([0x04, 0, 0]), cstring(""), cstring(""),
            bytes([0, 0, 0, 0, len(receipt_text)]), receipt_text,
            tlv(0x001E, cstring(message_id)),
            tlv(0x0427, bytes([self.server.receipt_state_code])),
        ])
        sequence = next(self.server.server_sequences)
        await self.send_pdu(DELIVER_SM, sequence, body)
        self.server.stats.dlr_generated += 1
        self.server.record_event({
            "event_type": "dlr",
            "peer": self.peer,
            "message_id": message_id,
            "state": self.server.receipt_state,
            "sequence": sequence,
        })
        self.server.log_fault(
            f"[{self.peer}] DELIVERY_RECEIPT state={self.server.receipt_state}"
        )

    def request_close(self) -> None:
        self.closed = True
        self.writer.close()

    async def _drain_tasks(self) -> None:
        pending = list(self.submit_tasks | self.receipt_tasks)
        if not pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=self.server.shutdown_drain_seconds,
            )
        except asyncio.TimeoutError:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    def _submit_done(self, task: asyncio.Task) -> None:
        self.submit_tasks.discard(task)
        self.submit_slots.release()
        self.server.stats.leave()
        with suppress(asyncio.CancelledError):
            error = task.exception()
            if error:
                self.server.record_error(
                    f"submit handler failed: {type(error).__name__}: {error}"
                )

    async def run(self):
        try:
            while True:
                header = await self.reader.readexactly(16)
                length, command_id, status, sequence = HEADER.unpack(header)
                if length < 16 or length > self.server.max_pdu_size:
                    raise ValueError(f"invalid PDU length {length}")
                body = await self.reader.readexactly(length - 16)
                request_id = command_id & ~RESPONSE_MASK
                if command_id in {BIND_RECEIVER, BIND_TRANSMITTER, BIND_TRANSCEIVER}:
                    await self.handle_bind(command_id, sequence, body)
                elif command_id == SUBMIT_SM:
                    if not self.bound:
                        await self.send_pdu(SUBMIT_SM | RESPONSE_MASK, sequence, b"", 0x00000004)
                    else:
                        # Acquiring before creating the task keeps the number of
                        # in-flight handlers bounded and lets TCP backpressure
                        # the producer when the connection is full.
                        await self.submit_slots.acquire()
                        self.server.stats.enter()
                        task = asyncio.create_task(
                            self.handle_submit(sequence, body),
                            name=f"smsc-submit-{sequence}",
                        )
                        self.submit_tasks.add(task)
                        task.add_done_callback(self._submit_done)
                elif command_id == ENQUIRE_LINK:
                    self.server.log_fault(f"[{self.peer}] ENQUIRE_LINK sequence={sequence}")
                    await self.send_pdu(ENQUIRE_LINK | RESPONSE_MASK, sequence)
                elif command_id == UNBIND:
                    self.server.log_fault(f"[{self.peer}] UNBIND")
                    await self.send_pdu(UNBIND | RESPONSE_MASK, sequence)
                    break
                elif command_id == (DELIVER_SM | RESPONSE_MASK):
                    self.server.log_fault(
                        f"[{self.peer}] deliver_sm_resp sequence={sequence} status={status}"
                    )
                elif command_id & RESPONSE_MASK:
                    self.server.log_fault(
                        f"[{self.peer}] RESPONSE command=0x{command_id:08x} "
                        f"sequence={sequence} status={status}"
                    )
                else:
                    self.server.log_fault(
                        f"[{self.peer}] UNSUPPORTED {COMMAND_NAMES.get(request_id, hex(command_id))}"
                    )
                    await self.send_pdu(GENERIC_NACK, sequence, status=0x00000003)
        except asyncio.IncompleteReadError:
            pass
        except ConnectionError:
            # Client dropped the TCP connection abruptly (e.g. WinError 64 /
            # WSAECONNRESET on Windows). This is a normal teardown path, not a
            # protocol failure, so do not log a traceback.
            logging.debug("Session %s connection reset by peer", self.peer)
        except Exception as exc:
            logging.exception("Session %s failed: %s", self.peer, exc)
        finally:
            # Drain already accepted submit handlers before closing the socket.
            # This preserves response delivery on a graceful UNBIND while the
            # bounded handler set still prevents unbounded task creation.
            await self._drain_tasks()
            self.closed = True
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except ConnectionError:
                # The reset can also surface while awaiting close, which would
                # otherwise leak as an unhandled exception in the callback.
                pass
            self.server.stats.disconnected += 1
            self.server.log_fault(f"[{self.peer}] DISCONNECTED")


class SmscSimulator:
    def __init__(self, host: str, port: int, system_id: str, password: str, receipt_delay: float,
                 submit_status: int = 0, receipt_state: str = "DELIVRD", drop_receipts: bool = False,
                 tps: float = 0.0, submit_response_delay: float = 0.0,
                 reject_ratio: float = 0.0, drop_response_ratio: float = 0.0,
                 drop_receipt_ratio: float = 0.0, disconnect_after: int = 0,
                 partial_submit_after: int = 0, partial_submit_status: int = 0x0000000B,
                 seed: int = 1, event_log: str = "", mode: str = "fast",
                 audit_mode: str | None = None, audit_queue_size: int = 8192,
                 audit_batch_size: int = 128, audit_flush_interval: float = 0.05,
                 max_inflight_per_connection: int = 256, metrics_output: str = "",
                 *, disconnect_before_response: int = 0):
        self.host, self.port = host, port
        self.system_id, self.password = system_id, password
        self.receipt_delay = receipt_delay
        self.submit_status = submit_status
        self.receipt_state = receipt_state.upper()
        self.receipt_state_code = {"DELIVRD": 2, "EXPIRED": 3, "UNDELIV": 5, "ACCEPTD": 6,
                                   "UNKNOWN": 7, "REJECTD": 8}.get(self.receipt_state, 7)
        self.drop_receipts = drop_receipts
        self.mode = str(mode or "fast").strip().lower()
        if self.mode not in {"fast", "fault"}:
            raise ValueError("simulator mode must be fast or fault")
        self.audit_mode = str(
            audit_mode or ("fast" if self.mode == "fast" else "full")
        ).strip().lower()
        if self.audit_mode not in {"fast", "full", "off"}:
            raise ValueError("simulator audit mode must be fast, full, or off")
        self.dlr_enabled = self.mode == "fault"
        self.tps = max(0.0, float(tps if tps is not None else 0.0))
        self.submit_response_delay = max(
            0.0, float(submit_response_delay if submit_response_delay is not None else 0.0)
        )
        self.reject_ratio = min(1.0, max(0.0, float(reject_ratio or 0.0)))
        self.drop_response_ratio = min(1.0, max(0.0, float(drop_response_ratio or 0.0)))
        self.drop_receipt_ratio = min(1.0, max(0.0, float(drop_receipt_ratio or 0.0)))
        self.disconnect_after = max(0, int(disconnect_after or 0))
        self.disconnect_before_response = max(0, int(disconnect_before_response or 0))
        self.partial_submit_after = max(0, int(partial_submit_after or 0))
        self.partial_submit_status = int(partial_submit_status)
        self.random = random.Random(int(seed))
        self.event_log = Path(event_log) if event_log else None
        if self.event_log:
            self.event_log.parent.mkdir(parents=True, exist_ok=True)
        self.submit_counter = itertools.count(1)
        self._tps_lock = asyncio.Lock()
        self._last_submit_at = 0.0
        self.max_pdu_size = 1024 * 1024
        self.max_inflight_per_connection = max(1, int(max_inflight_per_connection))
        self.shutdown_drain_seconds = 10.0
        self.audit_queue_size = max(1, int(audit_queue_size))
        self.audit_batch_size = max(1, int(audit_batch_size))
        self.audit_flush_interval = max(0.001, float(audit_flush_interval))
        self.metrics_output = Path(metrics_output) if metrics_output else None
        self.stats = SimulatorStats()
        self._audit_writer: AuditWriter | None = None
        self._audit_incomplete = False
        self._audit_overflow = 0
        self._stop_event: asyncio.Event | None = None
        self._server: asyncio.AbstractServer | None = None
        self._session_tasks: set[asyncio.Task] = set()
        self._sessions: dict[int, Session] = {}
        # Start from a time-based base so message ids are unique across
        # simulator restarts; reusing ids (0000000001, 0000000002, ...) makes
        # the Marketing Copilot backend match receipts to old tasks.
        self.message_ids = itertools.count(int(time.time() * 1000))
        self.server_sequences = itertools.count(1_000_000)

    def record_event(self, event: dict) -> None:
        """Queue one machine-readable event without blocking the responder."""
        if not self.event_log or self.audit_mode == "off":
            return
        record = {"time": datetime.now(timezone.utc).isoformat(), **event}
        if self.audit_mode == "fast" and record.get("event_type") == "submit_sm":
            # Keep protocol and accounting evidence, but never put phone
            # numbers, message bodies or raw payloads in the fast-path log.
            concat = record.get("concat") or {}
            record = {
                key: record[key]
                for key in (
                    "time", "event_type", "peer", "sequence", "submit_index",
                    "message_id", "response_status", "response_dropped",
                    "disconnect_after", "disconnect_before_response",
                    "data_coding", "esm_class",
                    "source_digest", "destination_digest",
                )
                if key in record
            }
            if concat:
                record["concat"] = {
                    key: concat[key] for key in ("reference", "total", "part") if key in concat
                }
        if self._audit_writer is not None:
            if not self._audit_writer.enqueue(record):
                self._audit_overflow += 1
                self._audit_incomplete = True
                self.record_error("simulator audit queue overflow or writer failure")
            return
        # Preserve the small unit-test/API contract for callers that record an
        # event before the async server has started.
        try:
            self.event_log.parent.mkdir(parents=True, exist_ok=True)
            with self.event_log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            self._audit_incomplete = True
            self.record_error(f"simulator audit write failed: {type(exc).__name__}: {exc}")

    def record_error(self, message: str) -> None:
        self.stats.error(message)
        logging.error("SMSC simulator: %s", message)

    def log_fault(self, message: str) -> None:
        if self.mode == "fault":
            print(message, flush=True)

    async def _start_audit(self) -> None:
        if self.event_log and self.audit_mode != "off":
            self._audit_writer = AuditWriter(
                self.event_log,
                self.audit_queue_size,
                self.audit_batch_size,
                self.audit_flush_interval,
            )
            await self._audit_writer.start()

    async def _close_audit(self) -> None:
        if self._audit_writer:
            await self._audit_writer.close()
            if self._audit_writer.error:
                self._audit_incomplete = True

    def _audit_snapshot(self) -> dict[str, Any]:
        if not self._audit_writer:
            return {
                "enabled": bool(self.event_log and self.audit_mode != "off"),
                "queue_capacity": self.audit_queue_size,
                "batch_size": self.audit_batch_size,
                "enqueued": 0,
                "written": 0,
                "backlog": 0,
                "max_backlog": 0,
                "overflow": self._audit_overflow,
                "write_error": None,
            }
        snapshot = self._audit_writer.snapshot()
        snapshot["overflow"] = self._audit_overflow
        return snapshot

    def metrics_snapshot(self) -> dict[str, Any]:
        audit = self._audit_snapshot()
        audit_incomplete = bool(
            self._audit_incomplete
            or audit.get("overflow")
            or audit.get("write_error")
            or audit.get("backlog")
        )
        return {
            "simulator_mode": self.mode,
            "audit_mode": self.audit_mode,
            "simulator_tps": self.tps,
            "submit_response_delay": self.submit_response_delay,
            "dlr_enabled": self.dlr_enabled,
            "faults": {
                "reject_ratio": self.reject_ratio,
                "drop_response_ratio": self.drop_response_ratio,
                "drop_receipt_ratio": self.drop_receipt_ratio,
                "disconnect_after": self.disconnect_after,
                "disconnect_before_response": self.disconnect_before_response,
                "partial_submit_after": self.partial_submit_after,
            },
            "max_inflight_per_connection": self.max_inflight_per_connection,
            "stats": self.stats.snapshot(),
            "audit": audit,
            "audit_status": "SIMULATOR_AUDIT_INCOMPLETE" if audit_incomplete else "VERIFIED",
            "acceptance_status": "TEST_HARNESS_FAIL" if audit_incomplete else "READY",
        }

    def _write_metrics(self) -> None:
        if not self.metrics_output:
            return
        try:
            self.metrics_output.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.metrics_output.with_suffix(self.metrics_output.suffix + ".tmp")
            tmp.write_text(
                json.dumps(self.metrics_snapshot(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            tmp.replace(self.metrics_output)
        except Exception as exc:  # pragma: no cover - environment-specific
            self._audit_incomplete = True
            self.record_error(f"simulator metrics write failed: {type(exc).__name__}: {exc}")

    def next_submit_index(self) -> int:
        return next(self.submit_counter)

    async def wait_for_tps(self) -> None:
        if self.tps <= 0:
            return
        interval = 1.0 / self.tps
        async with self._tps_lock:
            now = time.monotonic()
            due = max(now, self._last_submit_at + interval)
            self._last_submit_at = due
        wait = due - now
        if wait > 0:
            await asyncio.sleep(wait)

    def response_status(self, submit_index: int) -> int:
        if self.submit_status:
            return self.submit_status
        if self.partial_submit_after and submit_index > self.partial_submit_after:
            return self.partial_submit_status
        if self.reject_ratio and self.random.random() < self.reject_ratio:
            return self.partial_submit_status
        return 0

    def should_drop_response(self, submit_index: int) -> bool:
        return bool(self.drop_response_ratio and self.random.random() < self.drop_response_ratio)

    def should_drop_receipt(self, submit_index: int) -> bool:
        return bool(self.drop_receipt_ratio and self.random.random() < self.drop_receipt_ratio)

    def should_disconnect(self, submit_index: int) -> bool:
        return bool(self.disconnect_after and submit_index >= self.disconnect_after)

    def should_disconnect_before_response(self, submit_index: int) -> bool:
        """Trigger one controlled ambiguous-write disconnect.

        ``disconnect_after`` keeps its legacy post-response behavior. This
        separate hook models a response lost after the PDU crossed the SMSC
        boundary, which is the case that must become durable UNKNOWN rather
        than an automatic retry.
        """
        return bool(
            self.disconnect_before_response
            and int(submit_index) == self.disconnect_before_response
        )

    async def client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        self.record_event({"event_type": "connected", "peer": peer})
        self.stats.connected += 1
        session = Session(reader, writer, self, peer)
        task = asyncio.current_task()
        if task:
            self._session_tasks.add(task)
        self._sessions[id(session)] = session
        self.log_fault(f"[{peer}] CONNECTED")
        try:
            await session.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.record_error(f"session {peer} crashed: {type(exc).__name__}: {exc}")
            writer.close()
        finally:
            self._sessions.pop(id(session), None)
            if task:
                self._session_tasks.discard(task)

    async def run(self):
        await self._start_audit()
        self._stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        signals = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, "SIGBREAK"):
            signals.append(signal.SIGBREAK)
        def stop_from_signal(_signum, _frame):
            loop.call_soon_threadsafe(self._stop_event.set)

        for signum in signals:
            if signum is None:
                continue
            with suppress(NotImplementedError, RuntimeError, ValueError):
                signal.signal(signum, stop_from_signal)
        server = await asyncio.start_server(self.client_connected, self.host, self.port)
        self._server = server
        addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
        print(f"Local SMSC simulator listening on {addresses}", flush=True)
        print(
            "Simulator config: "
            f"mode={self.mode} simulator_tps={self.tps} "
            f"response_delay={self.submit_response_delay} dlr_enabled={self.dlr_enabled} "
            f"audit_mode={self.audit_mode} max_inflight_per_connection={self.max_inflight_per_connection}",
            flush=True,
        )
        try:
            await self._stop_event.wait()
        finally:
            server.close()
            await server.wait_closed()
            for session in list(self._sessions.values()):
                session.request_close()
            sessions = list(self._session_tasks)
            if sessions:
                await asyncio.gather(*sessions, return_exceptions=True)
            self.stats.inflight = 0
            self.record_event({"event_type": "simulator_summary", **self.metrics_snapshot()})
            await self._close_audit()
            self._write_metrics()


def main():
    parser = argparse.ArgumentParser(description="Local SMPP 3.4 SMSC simulator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2775)
    parser.add_argument("--system-id", default="test")
    parser.add_argument("--password", default="test")
    parser.add_argument("--receipt-delay", type=float, default=0.5)
    parser.add_argument(
        "--simulator-mode", "--mode", dest="mode", choices=("fast", "fault"), default="fast",
        help="fast is the unthrottled throughput mode; fault preserves injection behaviour",
    )
    parser.add_argument(
        "--simulator-tps", "--tps", dest="simulator_tps", type=float, default=0.0,
        help="Artificial simulator submit pacing; 0 disables pacing",
    )
    parser.add_argument(
        "--simulator-response-delay", "--submit-response-delay",
        dest="simulator_response_delay", type=float, default=0.0,
    )
    parser.add_argument("--submit-status", type=lambda value: int(value, 0), default=0)
    parser.add_argument("--reject-ratio", type=float, default=0.0)
    parser.add_argument("--drop-response-ratio", type=float, default=0.0)
    parser.add_argument("--drop-receipt-ratio", type=float, default=0.0)
    parser.add_argument("--disconnect-after", type=int, default=0,
                        help="Close the SMPP socket after this global submit_sm index")
    parser.add_argument(
        "--disconnect-before-response", type=int, default=0,
        help="Close after receiving this submit_sm but before writing its response",
    )
    parser.add_argument("--partial-submit-after", type=int, default=0,
                        help="Accept this many submit_sm PDUs, then reject later parts")
    parser.add_argument("--partial-submit-status", type=lambda value: int(value, 0), default=0x0000000B)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--event-log", default="")
    parser.add_argument("--metrics-output", default="")
    parser.add_argument(
        "--simulator-audit", "--audit-mode", dest="audit_mode",
        choices=("fast", "full", "off"), default=None,
    )
    parser.add_argument("--audit-queue-size", type=int, default=8192)
    parser.add_argument("--audit-batch-size", type=int, default=128)
    parser.add_argument("--audit-flush-interval", type=float, default=0.05)
    parser.add_argument("--max-inflight-per-connection", type=int, default=256)
    parser.add_argument("--receipt-state", default="DELIVRD")
    parser.add_argument("--drop-receipts", action="store_true")
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("The local SMSC simulator must bind to loopback")
    simulator = SmscSimulator(
        args.host, args.port, args.system_id, args.password, args.receipt_delay,
        args.submit_status, args.receipt_state, args.drop_receipts,
        args.simulator_tps, args.simulator_response_delay, args.reject_ratio,
        args.drop_response_ratio, args.drop_receipt_ratio, args.disconnect_after,
        args.partial_submit_after, args.partial_submit_status, args.seed, args.event_log,
        args.mode, args.audit_mode, args.audit_queue_size, args.audit_batch_size,
        args.audit_flush_interval, args.max_inflight_per_connection, args.metrics_output,
        disconnect_before_response=args.disconnect_before_response,
    )
    try:
        asyncio.run(simulator.run())
    except KeyboardInterrupt:
        print("SMSC simulator stopped")


if __name__ == "__main__":
    main()
