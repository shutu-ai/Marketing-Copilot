#!/usr/bin/env python3
"""Standalone SMPP Flash SMS test for a deployed Marketing Copilot server.

Edit the values in ``SMPP_CONFIG``, ``TEST_DESTINATION`` and ``TEST_MESSAGE``
below, then run this file with the application's Python runtime:

    python3 bin/smpp_flash_test.py

This utility is intentionally kept under the repository's root ``bin``
directory.  It is a manual test utility and is not part of the application
deployment package.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from typing import Any

import smpplib.client
import smpplib.consts
import smpplib.gsm


# ============================================================================
# Edit this section before sending a real test message.
# Values are copied from SMPP.PNG.  The password is not visible in the image,
# so it must be filled in locally.  The script refuses to send while the
# password or destination number is empty.
# ============================================================================
SMPP_CONFIG = {
    "connection_name": "Exhibition_Center",
    "host": "10.204.181.73",
    "port": 15019,
    "system_id": "7127",
    "password": "",  # Required: fill in the SMPP password.
    "source_addr": "Znexus",
    "bind_type": "transceiver",
    "source_addr_ton": 0,
    "source_addr_npi": 0,
    "destination_addr_ton": 1,
    "destination_addr_npi": 1,
    "data_coding": 0,  # 0 - GSM 7-bit in the screenshot.
    "timeout": 10.0,
    "request_delivery_receipt": False,  # DLR policy: do not request.
    "receipt_timeout": 60.0,
    # Reference values from the connection screen.  A standalone test uses
    # one bind, so pool/window/TPS settings are not used for this single SMS.
    "pool_size": 4,
    "window_size": 60,
    "enquire_interval": 30,
    "tps": 200,
    "priority": 100,
    "reconnect_max_seconds": 60,
}

TEST_DESTINATION = ""  # Required: fill in the recipient number.
TEST_MESSAGE = "Marketing Copilot flash SMS test"


def _iter_utf16be_units(text: str):
    """Yield complete UTF-16BE code-unit sequences for each text character."""

    index = 0
    while index < len(text):
        character = text[index]
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDBFF:
            if index + 1 >= len(text) or not 0xDC00 <= ord(text[index + 1]) <= 0xDFFF:
                raise UnicodeEncodeError(
                    "utf-16-be", text, index, index + 1, "surrogates not allowed"
                )
            yield bytes(
                (
                    code_point >> 8,
                    code_point & 0xFF,
                    ord(text[index + 1]) >> 8,
                    ord(text[index + 1]) & 0xFF,
                )
            )
            index += 2
            continue
        if 0xDC00 <= code_point <= 0xDFFF:
            raise UnicodeEncodeError(
                "utf-16-be", text, index, index + 1, "surrogates not allowed"
            )
        if code_point <= 0xFFFF:
            yield bytes((code_point >> 8, code_point & 0xFF))
            index += 1
            continue

        code_point -= 0x10000
        high_surrogate = 0xD800 + (code_point >> 10)
        low_surrogate = 0xDC00 + (code_point & 0x3FF)
        yield bytes(
            (
                high_surrogate >> 8,
                high_surrogate & 0xFF,
                low_surrogate >> 8,
                low_surrogate & 0xFF,
            )
        )
        index += 1


def _make_ucs2_parts(text: str):
    """Fallback for an environment where smpplib cannot load utf-16-be."""

    part_size = smpplib.consts.UCS2_PART_SIZE
    encoded_parts: list[bytes] = []
    current_part = bytearray()
    for encoded_character in _iter_utf16be_units(text):
        if current_part and len(current_part) + len(encoded_character) > part_size:
            encoded_parts.append(bytes(current_part))
            current_part.clear()
        current_part.extend(encoded_character)

    if current_part or not encoded_parts:
        encoded_parts.append(bytes(current_part))
    if len(encoded_parts) > 255:
        raise smpplib.gsm.exceptions.MessageTooLong()

    if len(encoded_parts) == 1:
        return (
            encoded_parts,
            smpplib.consts.SMPP_ENCODING_ISO10646,
            smpplib.consts.SMPP_MSGTYPE_DEFAULT,
        )

    reference = os.getpid() & 0xFF
    header = bytes((0x05, 0x00, 0x03, reference, len(encoded_parts)))
    parts = [
        header + bytes((part_index,)) + part
        for part_index, part in enumerate(encoded_parts, start=1)
    ]
    return (
        parts,
        smpplib.consts.SMPP_ENCODING_ISO10646,
        smpplib.consts.SMPP_GSMFEAT_UDHI,
    )


def _make_parts(text: str):
    """Split text using the same smpplib rules as the application gateway."""

    try:
        return smpplib.gsm.make_parts(text)
    except LookupError as exc:
        if "utf-16-be" not in str(exc).lower():
            raise
        return _make_ucs2_parts(text)


def _flash_data_coding(normal_coding: int) -> int:
    """Return the SMPP message-class-0 DCS for the detected alphabet."""

    if int(normal_coding) == smpplib.consts.SMPP_ENCODING_ISO10646:
        return 0x18  # UCS-2, message class 0.
    return 0xF0  # GSM default alphabet, message class 0.


def _normalise_destination(value: str) -> str:
    destination = str(value or "").strip()
    if destination.startswith("+"):
        destination = destination[1:]
    if not destination:
        raise ValueError("TEST_DESTINATION must contain the recipient number")
    if not destination.isdigit():
        raise ValueError("TEST_DESTINATION must contain digits; remove a leading +")
    return destination


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Marketing Copilot standalone SMPP Flash SMS test"
    )
    parser.add_argument(
        "--destination",
        default=None,
        help="Temporarily override TEST_DESTINATION",
    )
    parser.add_argument(
        "--message",
        default=None,
        help="Temporarily override TEST_MESSAGE",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show encoding and parts without connecting or sending",
    )
    parser.add_argument(
        "--wait-for-receipt",
        action="store_true",
        help="Request and wait for delivery receipts for this run",
    )
    return parser.parse_args()


def _print_message_plan(
    destination: str,
    message: str,
    *,
    wait_for_receipt: bool,
) -> tuple[list[bytes], int, int]:
    parts, normal_coding, esm_class = _make_parts(message)
    flash_coding = _flash_data_coding(normal_coding)
    print(
        f"SMPP: {SMPP_CONFIG['connection_name']} "
        f"{SMPP_CONFIG['host']}:{SMPP_CONFIG['port']}"
    )
    print(f"destination: {destination or '(not set)'}")
    print(f"message: {message}")
    print(
        "encoding: "
        f"normal=0x{int(normal_coding):02x}, "
        f"flash_dcs=0x{flash_coding:02x}, "
        f"esm_class=0x{int(esm_class):02x}, "
        f"parts={len(parts)}, "
        f"registered_delivery={1 if wait_for_receipt else 0}"
    )
    for index, part in enumerate(parts, start=1):
        print(f"  part {index}: {len(part)} octets")
    return parts, flash_coding, esm_class


def _status_text(status: Any) -> str:
    try:
        numeric_status = int(status or 0)
    except (TypeError, ValueError):
        return str(status)
    return f"0x{numeric_status:08x}"


def send_flash_sms(
    *,
    destination: str,
    message: str,
    password: str,
    wait_for_receipt: bool,
) -> None:
    destination = _normalise_destination(destination)
    message = str(message or "")
    if not message:
        raise ValueError("TEST_MESSAGE must contain the message text")
    password = str(password or "")
    if not password:
        raise ValueError(
            "SMPP_CONFIG['password'] is empty; the password is not visible in SMPP.PNG"
        )

    parts, flash_coding, esm_class = _print_message_plan(
        destination,
        message,
        wait_for_receipt=wait_for_receipt,
    )
    bind_type = str(SMPP_CONFIG.get("bind_type") or "transceiver").strip().lower()
    if bind_type == "receiver":
        raise ValueError(
            "receiver bind cannot submit_sm; use transmitter or transceiver"
        )
    if bind_type not in {"transmitter", "transceiver"}:
        raise ValueError(f"unsupported bind_type: {bind_type}")

    timeout = float(SMPP_CONFIG.get("timeout", 10.0))
    submitted = threading.Event()
    delivered = threading.Event()
    stopping = threading.Event()
    state: dict[str, Any] = {
        "responses": [],
        "message_ids": [],
        "receipt_ids": [],
        "submit_error": None,
        "listener_error": None,
    }

    client = smpplib.client.Client(
        str(SMPP_CONFIG["host"]),
        int(SMPP_CONFIG["port"]),
        timeout=timeout,
        allow_unknown_opt_params=True,
    )

    def on_sent(pdu, **_kwargs):
        status = getattr(pdu, "status", 0)
        sequence = getattr(pdu, "sequence", "?")
        message_id = getattr(pdu, "message_id", "")
        state["responses"].append(sequence)
        if int(status or 0) != 0:
            state["submit_error"] = (
                f"submit_sm_resp rejected: status={_status_text(status)}, "
                f"sequence={sequence}"
            )
        else:
            state["message_ids"].append(str(message_id))
            print(
                f"submit_sm_resp: sequence={sequence}, "
                f"message_id={message_id}, status={_status_text(status)}",
                flush=True,
            )
        if state["submit_error"] or len(state["responses"]) >= len(parts):
            submitted.set()

    def on_received(pdu, **_kwargs):
        message_id = getattr(pdu, "receipted_message_id", "")
        message_state = getattr(pdu, "message_state", "")
        state["receipt_ids"].append(str(message_id))
        print(
            f"delivery_receipt: message_id={message_id}, state={message_state}",
            flush=True,
        )
        if len(state["receipt_ids"]) >= len(parts):
            delivered.set()
        return smpplib.consts.SMPP_ESME_ROK

    client.set_message_sent_handler(on_sent)
    if wait_for_receipt:
        client.set_message_received_handler(on_received)

    listener: threading.Thread | None = None
    try:
        print(
            f"connecting to {SMPP_CONFIG['host']}:{SMPP_CONFIG['port']} ...",
            flush=True,
        )
        client.connect()
        bind_method = getattr(client, f"bind_{bind_type}")
        bind_method(
            system_id=str(SMPP_CONFIG["system_id"]),
            password=password,
        )
        print(f"SMPP bind succeeded: {bind_type}", flush=True)

        def listen_until_stopped():
            try:
                client.listen()
            except Exception as exc:  # Depends on the remote socket.
                if not stopping.is_set():
                    state["listener_error"] = exc
                    submitted.set()
                    delivered.set()

        listener = threading.Thread(
            target=listen_until_stopped,
            name="smpp-flash-test-listener",
            daemon=True,
        )
        listener.start()

        registered_delivery = 1 if wait_for_receipt else 0
        for part in parts:
            client.send_message(
                source_addr_ton=int(SMPP_CONFIG.get("source_addr_ton", 0)),
                source_addr_npi=int(SMPP_CONFIG.get("source_addr_npi", 0)),
                source_addr=str(SMPP_CONFIG["source_addr"]),
                dest_addr_ton=int(
                    SMPP_CONFIG.get("destination_addr_ton", smpplib.consts.SMPP_TON_INTL)
                ),
                dest_addr_npi=int(
                    SMPP_CONFIG.get("destination_addr_npi", smpplib.consts.SMPP_NPI_ISDN)
                ),
                destination_addr=destination,
                short_message=part,
                data_coding=flash_coding,
                esm_class=esm_class,
                registered_delivery=registered_delivery,
            )

        if not submitted.wait(timeout):
            raise TimeoutError(
                f"submit_sm_resp timeout: "
                f"{len(state['responses'])}/{len(parts)} received"
            )
        if state["listener_error"]:
            raise RuntimeError(f"SMPP listener failed: {state['listener_error']}")
        if state["submit_error"]:
            raise RuntimeError(state["submit_error"])

        if wait_for_receipt:
            receipt_timeout = float(SMPP_CONFIG.get("receipt_timeout", 60.0))
            if not delivered.wait(receipt_timeout):
                raise TimeoutError(
                    f"delivery receipt timeout: "
                    f"{len(state['receipt_ids'])}/{len(parts)} received"
                )
            print(
                f"PASS: {len(parts)} submit_sm_resp and "
                f"{len(state['receipt_ids'])} delivery receipt(s) received",
                flush=True,
            )
        else:
            print(
                f"PASS: SMSC accepted {len(parts)} submit_sm_resp; "
                "delivery receipt was not requested",
                flush=True,
            )
    finally:
        stopping.set()
        try:
            # Disconnecting wakes smpplib.listen(). Suppress its expected
            # shutdown socket message without hiding a send failure.
            client.logger.disabled = True
            client.disconnect()
        except Exception:
            pass
        if listener is not None:
            listener.join(timeout=2)


def main() -> int:
    args = _parse_args()
    destination = TEST_DESTINATION if args.destination is None else args.destination
    message = TEST_MESSAGE if args.message is None else args.message
    wait_for_receipt = bool(SMPP_CONFIG.get("request_delivery_receipt", False))
    wait_for_receipt = wait_for_receipt or bool(args.wait_for_receipt)

    try:
        if args.dry_run:
            _print_message_plan(
                str(destination or ""),
                str(message or ""),
                wait_for_receipt=wait_for_receipt,
            )
            print("DRY-RUN: no connection was opened and no SMS was sent")
            return 0

        password = str(
            SMPP_CONFIG.get("password")
            or os.environ.get("SMPP_TEST_PASSWORD", "")
        )
        send_flash_sms(
            destination=str(destination or ""),
            message=str(message or ""),
            password=password,
            wait_for_receipt=wait_for_receipt,
        )
        return 0
    except KeyboardInterrupt:
        print("cancelled", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
