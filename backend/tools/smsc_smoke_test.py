"""Send one SMS to an SMPP endpoint and validate submit_sm_resp.

Delivery-receipt confirmation is opt-in because throughput/capacity smoke tests
only need to verify that the SMSC accepted the submit_sm request.
"""

import argparse
import threading

import smpplib.client
import smpplib.consts
import smpplib.gsm


def main():
    parser = argparse.ArgumentParser(description="Smoke test a local SMPP endpoint")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2775)
    parser.add_argument("--system-id", default="test")
    parser.add_argument("--password", default="test")
    parser.add_argument("--source", default="telebirr")
    parser.add_argument("--destination", default="251900001240")
    parser.add_argument("--message", default="Marketing Copilot local SMPP test")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument(
        "--wait-for-receipt", action="store_true",
        help="also request and wait for delivery receipts",
    )
    args = parser.parse_args()

    submitted = threading.Event()
    delivered = threading.Event()
    stopping = threading.Event()
    parts, coding, esm_class = smpplib.gsm.make_parts(args.message)
    result = {"message_ids": set(), "receipt_ids": set(), "state": None, "expected": len(parts)}
    client = smpplib.client.Client(args.host, args.port, timeout=2, allow_unknown_opt_params=True)

    def on_sent(pdu, **kwargs):
        result["message_ids"].add(str(pdu.message_id))
        print(f"submit_sm_resp: sequence={pdu.sequence}, message_id={pdu.message_id}", flush=True)
        if len(result["message_ids"]) >= result["expected"]:
            submitted.set()

    def on_received(pdu, **kwargs):
        result["state"] = pdu.message_state
        result["receipt_ids"].add(str(pdu.receipted_message_id))
        print(
            f"delivery_receipt: message_id={pdu.receipted_message_id}, state={pdu.message_state}",
            flush=True,
        )
        if len(result["receipt_ids"]) >= result["expected"]:
            delivered.set()
        return smpplib.consts.SMPP_ESME_ROK

    client.set_message_sent_handler(on_sent)
    client.set_message_received_handler(on_received)
    client.connect()
    client.bind_transceiver(system_id=args.system_id, password=args.password)
    def listen_until_stopped():
        try:
            client.listen()
        except Exception:
            if not stopping.is_set():
                raise

    listener = threading.Thread(target=listen_until_stopped, daemon=True)
    listener.start()

    for part in parts:
        client.send_message(
            source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
            source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
            source_addr=args.source,
            dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
            dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
            destination_addr=args.destination,
            short_message=part,
            data_coding=coding,
            esm_class=esm_class,
            registered_delivery=args.wait_for_receipt,
        )

    if not submitted.wait(args.timeout):
        raise SystemExit("FAILED: submit_sm_resp timeout")
    if args.wait_for_receipt:
        if not delivered.wait(args.timeout):
            raise SystemExit("FAILED: delivery receipt timeout")
        print(f"PASS: {len(parts)} submit_sm_resp and DELIVRD receipt(s) received", flush=True)
    else:
        print(f"PASS: {len(parts)} submit_sm_resp received; delivery receipt not required", flush=True)
    stopping.set()
    # smpplib.listen() blocks in recv(). Closing the socket wakes it with an
    # expected Windows WSAENOTSOCK error; suppress shutdown-only library logs.
    client.logger.disabled = True
    client.disconnect()
    listener.join(timeout=2)


if __name__ == "__main__":
    main()
