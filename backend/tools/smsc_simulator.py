"""Minimal SMPP 3.4 SMSC simulator for local end-to-end testing.

It accepts bind_transceiver/transmitter/receiver, submit_sm, enquire_link and
unbind. Every submit_sm receives a successful submit_sm_resp followed by a
DELIVRD deliver_sm receipt.
"""

import argparse
import asyncio
import itertools
import json
import logging
import signal
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone


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

    async def send_pdu(self, command_id: int, sequence: int, body: bytes = b"", status: int = 0):
        packet = HEADER.pack(16 + len(body), command_id, status, sequence) + body
        async with self.write_lock:
            self.writer.write(packet)
            await self.writer.drain()

    async def handle_bind(self, command_id: int, sequence: int, body: bytes):
        r = BodyReader(body)
        system_id, password = r.cstring(), r.cstring()
        status = ESME_ROK
        if self.server.system_id and system_id != self.server.system_id:
            status = 0x0000000F
        elif self.server.password and password != self.server.password:
            status = 0x0000000E
        self.bound = status == ESME_ROK
        self.system_id = system_id
        print(f"[{self.peer}] BIND system_id={system_id!r} result={'OK' if self.bound else hex(status)}", flush=True)
        await self.send_pdu(command_id | RESPONSE_MASK, sequence, cstring("LOCAL-SMSC"), status)

    async def handle_submit(self, sequence: int, body: bytes):
        if not self.bound:
            await self.send_pdu(SUBMIT_SM | RESPONSE_MASK, sequence, b"", 0x00000004)
            return
        message = parse_submit_sm(body)
        message_id = f"{next(self.server.message_ids):010d}"
        event = {
            "time": datetime.now(timezone.utc).isoformat(),
            "peer": self.peer,
            "sequence": sequence,
            "message_id": message_id,
            **message,
        }
        print("\n=== SMSC RECEIVED submit_sm ===", flush=True)
        print(json.dumps(event, ensure_ascii=False, indent=2), flush=True)
        concat = message.get("concat")
        if concat:
            key = (message["source_addr"], message["destination_addr"], concat["reference"])
            parts = self.multipart.setdefault(key, {})
            parts[concat["part"]] = message["text"]
            if len(parts) == concat["total"]:
                full_text = "".join(parts[i] for i in range(1, concat["total"] + 1))
                print(f"=== REASSEMBLED multipart SMS ({concat['total']} parts) ===\n{full_text}\n", flush=True)
                self.multipart.pop(key, None)

        await self.send_pdu(SUBMIT_SM | RESPONSE_MASK, sequence, cstring(message_id), self.server.submit_status)
        if self.server.submit_status == 0 and message["registered_delivery"] and not self.server.drop_receipts:
            asyncio.create_task(self.send_delivery_receipt(message, message_id))

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
        print(f"[{self.peer}] DELIVERY_RECEIPT message_id={message_id} state={self.server.receipt_state}", flush=True)

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
                    await self.handle_submit(sequence, body)
                elif command_id == ENQUIRE_LINK:
                    print(f"[{self.peer}] ENQUIRE_LINK sequence={sequence}", flush=True)
                    await self.send_pdu(ENQUIRE_LINK | RESPONSE_MASK, sequence)
                elif command_id == UNBIND:
                    print(f"[{self.peer}] UNBIND", flush=True)
                    await self.send_pdu(UNBIND | RESPONSE_MASK, sequence)
                    break
                elif command_id == (DELIVER_SM | RESPONSE_MASK):
                    print(f"[{self.peer}] deliver_sm_resp sequence={sequence} status={status}", flush=True)
                elif command_id & RESPONSE_MASK:
                    print(f"[{self.peer}] RESPONSE command=0x{command_id:08x} sequence={sequence} status={status}", flush=True)
                else:
                    print(f"[{self.peer}] UNSUPPORTED {COMMAND_NAMES.get(request_id, hex(command_id))}", flush=True)
                    await self.send_pdu(GENERIC_NACK, sequence, status=0x00000003)
        except asyncio.IncompleteReadError:
            pass
        except Exception as exc:
            logging.exception("Session %s failed: %s", self.peer, exc)
        finally:
            self.writer.close()
            await self.writer.wait_closed()
            print(f"[{self.peer}] DISCONNECTED", flush=True)


class SmscSimulator:
    def __init__(self, host: str, port: int, system_id: str, password: str, receipt_delay: float,
                 submit_status: int = 0, receipt_state: str = "DELIVRD", drop_receipts: bool = False):
        self.host, self.port = host, port
        self.system_id, self.password = system_id, password
        self.receipt_delay = receipt_delay
        self.submit_status = submit_status
        self.receipt_state = receipt_state.upper()
        self.receipt_state_code = {"DELIVRD": 2, "EXPIRED": 3, "UNDELIV": 5, "ACCEPTD": 6,
                                   "UNKNOWN": 7, "REJECTD": 8}.get(self.receipt_state, 7)
        self.drop_receipts = drop_receipts
        self.max_pdu_size = 1024 * 1024
        self.message_ids = itertools.count(1)
        self.server_sequences = itertools.count(1_000_000)

    async def client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        print(f"[{peer}] CONNECTED", flush=True)
        await Session(reader, writer, self, peer).run()

    async def run(self):
        server = await asyncio.start_server(self.client_connected, self.host, self.port)
        addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
        print(f"Local SMSC simulator listening on {addresses}", flush=True)
        print(f"Credentials: system_id={self.system_id!r}, password={self.password!r}", flush=True)
        print("Press Ctrl+C to stop.\n", flush=True)
        async with server:
            await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Local SMPP 3.4 SMSC simulator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2775)
    parser.add_argument("--system-id", default="test")
    parser.add_argument("--password", default="test")
    parser.add_argument("--receipt-delay", type=float, default=0.5)
    parser.add_argument("--submit-status", type=lambda value: int(value, 0), default=0)
    parser.add_argument("--receipt-state", default="DELIVRD")
    parser.add_argument("--drop-receipts", action="store_true")
    args = parser.parse_args()
    simulator = SmscSimulator(args.host, args.port, args.system_id, args.password, args.receipt_delay,
                              args.submit_status, args.receipt_state, args.drop_receipts)
    try:
        asyncio.run(simulator.run())
    except KeyboardInterrupt:
        print("SMSC simulator stopped")


if __name__ == "__main__":
    main()
