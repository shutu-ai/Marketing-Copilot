"""Service-level VNext business-day and bounded recovery acceptance checks.

This tool is intentionally loopback-only.  It uses the real VNextRuntime,
spawned workers, the single ProjectionWriter, and an opt-in file-backed
BusinessClock so a test can cross midnight without changing the EulerOS clock.
It does not connect to a production SMSC or application database.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import time

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.runtime import VNextRuntime
from vnext.smpp_transport import (
    BIND_RECEIVER,
    BIND_TRANSMITTER,
    BIND_TRANSCEIVER,
    ENQUIRE_LINK,
    HEADER,
    RESPONSE_MASK,
    SUBMIT_SM,
    UNBIND,
    cstring,
    read_pdu,
)


def _destination(body: bytes) -> str:
    offset = 0
    for _ in range(3):
        end = body.find(b"\x00", offset)
        if end < 0:
            raise ValueError("malformed submit_sm destination")
        value = body[offset:end].decode("ascii", errors="replace")
        offset = end + 1
        if _ == 0:
            offset += 2
        elif _ == 1:
            offset += 2
        else:
            return value
    raise ValueError("missing destination")


class LoopbackSMSC:
    def __init__(self):
        self.server = None
        self.received: list[dict] = []
        self.responses = 0
        self.errors: list[str] = []

    async def _handle(self, reader, writer):
        bound = False
        try:
            while True:
                command, _status, sequence, body = await read_pdu(reader)
                if command in {BIND_RECEIVER, BIND_TRANSMITTER, BIND_TRANSCEIVER}:
                    bound = True
                    response = cstring("VNEXT-T6-SMSC")
                    writer.write(HEADER.pack(16 + len(response), command | RESPONSE_MASK, 0, sequence) + response)
                    await writer.drain()
                elif command == SUBMIT_SM:
                    self.received.append({
                        "destination": _destination(body),
                        "monotonic": time.monotonic(),
                    })
                    response = cstring(f"T6-{len(self.received):08d}")
                    writer.write(HEADER.pack(16 + len(response), command | RESPONSE_MASK, 0, sequence) + response)
                    await writer.drain()
                    self.responses += 1
                elif command == ENQUIRE_LINK:
                    writer.write(HEADER.pack(16, command | RESPONSE_MASK, 0, sequence))
                    await writer.drain()
                elif command == UNBIND:
                    writer.write(HEADER.pack(16, command | RESPONSE_MASK, 0, sequence))
                    await writer.drain()
                    break
                else:
                    self.errors.append(f"unsupported command=0x{command:08x}")
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except Exception as exc:  # pragma: no cover - forensic path
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return int(self.server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()


def _write_clock(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps({"now": value}, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _rows(prefix: str, count: int) -> list[dict]:
    return [
        {"msisdn": f"090{prefix}{index:05d}", "message": f"T6 {prefix} {index}"}
        for index in range(count)
    ]


def _normalized(prefix: str, count: int) -> set[str]:
    return {f"25190{prefix}{index:05d}" for index in range(count)}


def _projection_days(path: Path) -> dict:
    try:
        import duckdb
        connection = duckdb.connect(str(path), read_only=True)
        try:
            rows = connection.execute(
                "SELECT business_day,campaign_type,campaign_id,SUM(socket_written) AS socket_written "
                "FROM vnext_projection GROUP BY 1,2,3 ORDER BY 1,2,3"
            ).fetchall()
        finally:
            connection.close()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    return {
        "rows": [
            {
                "business_day": str(day), "campaign_type": str(campaign_type),
                "campaign_id": int(campaign_id), "socket_written": int(socket_written),
            }
            for day, campaign_type, campaign_id, socket_written in rows
        ]
    }


async def run_daily(args: argparse.Namespace) -> dict:
    root = Path(args.run_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    clock_path = root / "clock.json"
    _write_clock(clock_path, "2026-10-17T23:59:59+00:00")
    os.environ["MC_VNEXT_TEST_CLOCK_FILE"] = str(clock_path)
    os.environ["MC_VNEXT_BUSINESS_TIMEZONE"] = "UTC"

    smsc = LoopbackSMSC()
    port = await smsc.start()
    route = {
        1: {
            "route_id": 1, "host": "127.0.0.1", "port": port,
            "system_id": "test", "password": "test", "source_addr": "VNEXT",
            "pool_size": 1, "window_size": 4, "tps": 0.0,
            "tps_scope": "route_shared", "timeout": 5,
        }
    }
    runtime_kwargs = {
        "worker_count": 2, "virtual_partition_count": 8,
        "max_spool_bytes": 16 * 1024 * 1024, "projection_queue_size": 64,
        "max_inflight": 8, "prefetch_per_partition": 4,
        "business_timezone": "UTC", "run_id": root.name,
    }
    old_rows = _rows("01", args.old_count)
    new_rows = _rows("02", args.new_count)
    old_destinations = _normalized("01", args.old_count)
    new_destinations = _normalized("02", args.new_count)
    runtime = VNextRuntime(root, route, **runtime_kwargs)
    old_day = "2026-10-17"
    new_day = "2026-10-18"
    transition = {}
    old_enqueue = {}
    new_enqueue = {}
    cutover_monotonic = None
    restart_monotonic = None
    try:
        runtime.start()
        runtime.set_campaign_state("batch", 1, "paused")
        old_enqueue = runtime.enqueue_chunk(
            old_rows, campaign_type="batch", campaign_id=1, route_id=1,
            phone_prefix="251", source_id="t6-old-day", payload_ref="t6-old",
        ).as_dict()
        _write_clock(clock_path, "2026-10-18T00:00:00+00:00")
        transition = runtime.rollover_if_needed()
        cutover_monotonic = time.monotonic()
        runtime.stop()

        restart_monotonic = time.monotonic()
        runtime = VNextRuntime(root, route, **runtime_kwargs)
        runtime.start()
        runtime.set_campaign_state("batch", 1, "running")
        new_enqueue = runtime.enqueue_chunk(
            new_rows, campaign_type="batch", campaign_id=2, route_id=1,
            phone_prefix="251", source_id="t6-new-day", payload_ref="t6-new",
        ).as_dict()
        idle_status = await runtime.wait_for_idle(timeout=args.timeout)
    finally:
        runtime.stop()
        await smsc.stop()
        os.environ.pop("MC_VNEXT_TEST_CLOCK_FILE", None)

    old_after_cutoff = sum(
        item["destination"] in old_destinations
        and cutover_monotonic is not None
        and item["monotonic"] >= cutover_monotonic
        for item in smsc.received
    )
    old_after_restart = sum(
        item["destination"] in old_destinations
        and restart_monotonic is not None
        and item["monotonic"] >= restart_monotonic
        for item in smsc.received
    )
    new_received = sum(item["destination"] in new_destinations for item in smsc.received)
    old_received = sum(item["destination"] in old_destinations for item in smsc.received)
    projection = _projection_days(root / "projection" / "vnext-projection.duckdb")
    rows_by_day = projection.get("rows", []) if isinstance(projection, dict) else []
    old_day_rows = [item for item in rows_by_day if item["business_day"] == old_day]
    new_day_rows = [item for item in rows_by_day if item["business_day"] == new_day]
    new_day_stats_contamination = sum(item["campaign_id"] == 1 for item in new_day_rows)
    passed = (
        not smsc.errors
        and transition.get("changed") is True
        and old_enqueue.get("accepted") == args.old_count
        and new_enqueue.get("accepted") == args.new_count
        and old_after_cutoff == 0
        and old_after_restart == 0
        and old_received == 0
        and new_received == args.new_count
        and new_day_stats_contamination == 0
        and not projection.get("error")
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "scenario": "service_level_business_day_rollover_restart",
        "business_timezone": "UTC",
        "clock_injection": "MC_VNEXT_TEST_CLOCK_FILE",
        "old_day": old_day,
        "new_day": new_day,
        "transition": transition,
        "old_enqueue": old_enqueue,
        "new_enqueue": new_enqueue,
        "old_day_new_pdu_after_cutoff": int(old_after_cutoff),
        "old_day_redispatch_after_restart": int(old_after_restart),
        "new_day_stats_contamination": int(new_day_stats_contamination),
        "old_day_socket_received": int(old_received),
        "new_day_socket_received": int(new_received),
        "idle_status": idle_status,
        "projection": projection,
        "smsc_errors": smsc.errors,
        "evidence_flags": {
            "service_level_executed": True,
            "cutoff_executed": True,
            "restart_at_new_day_executed": True,
            "old_day_socket_after_cutoff_measured": True,
            "old_day_redispatch_after_restart_measured": True,
            "stats_contamination_measured": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--old-count", type=int, default=8)
    parser.add_argument("--new-count", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()
    result = asyncio.run(run_daily(args))
    output = Path(args.run_root).resolve() / "daily-result.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
