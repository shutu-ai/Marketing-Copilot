"""Loopback-only VNext socket-write acceptance runner.

The runner starts a tiny SMPP 3.4 server on 127.0.0.1, feeds the real VNext
ingress/spool/worker code from multiple processes, then reconciles worker
``socket_written`` events with bytes observed at the simulator protocol
boundary. It never opens the application DuckDB in a worker.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
import time
from collections import Counter

TOOLS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = TOOLS_DIR.parent
REPO_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.ingress import IngressRouter
from vnext.partition import PartitionMap
from vnext.projection_writer import ProjectionWriterProcess, ProgressJournal
from vnext.route_credits import RouteCreditBroker
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
from vnext.spool import TaskSpool
from vnext.worker import ShardWorker


def _read_cstring(body: bytes, offset: int) -> tuple[str, int]:
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated SMPP C string")
    return body[offset:end].decode("ascii", errors="replace"), end + 1


def _parse_destination(body: bytes) -> str:
    offset = 0
    _service_type, offset = _read_cstring(body, offset)
    offset += 2
    _source, offset = _read_cstring(body, offset + 0)
    offset += 2
    destination, _offset = _read_cstring(body, offset)
    return destination


class LoopbackSMSC:
    def __init__(self, *, response_delay: float = 0.0):
        self.server = None
        self.received: list[dict] = []
        self.responses = 0
        self.response_success = 0
        self.binds = 0
        self.active_binds = 0
        self.max_active_binds = 0
        self.errors: list[str] = []
        self.response_delay = max(0.0, float(response_delay))

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        bound = False
        try:
            while True:
                command, _status, sequence, body = await read_pdu(reader)
                if command in {BIND_RECEIVER, BIND_TRANSMITTER, BIND_TRANSCEIVER}:
                    if not bound:
                        bound = True
                        self.binds += 1
                        self.active_binds += 1
                        self.max_active_binds = max(self.max_active_binds, self.active_binds)
                    response = cstring("VNEXT-SMSC")
                    writer.write(HEADER.pack(16 + len(response), command | RESPONSE_MASK, 0, sequence) + response)
                    await writer.drain()
                elif command == SUBMIT_SM:
                    destination = _parse_destination(body)
                    item = {
                        "sequence": int(sequence),
                        "destination": destination,
                        "destination_digest": hashlib.sha256(destination.encode("utf-8")).hexdigest()[:16],
                        "received_at": time.time(),
                    }
                    self.received.append(item)
                    if self.response_delay:
                        await asyncio.sleep(self.response_delay)
                    response = cstring(f"VNEXT-{len(self.received):010d}")
                    writer.write(HEADER.pack(16 + len(response), command | RESPONSE_MASK, 0, sequence) + response)
                    await writer.drain()
                    self.responses += 1
                    self.response_success += 1
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
        except Exception as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            if bound:
                self.active_binds = max(0, self.active_binds - 1)
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


def _worker_process(worker_id: int, partitions: tuple[int, ...], spool_root: str,
                    credits_root: str, route_config: dict, journal_path: str,
                    result_path: str, max_inflight: int, prefetch: int,
                    worker_count: int) -> None:
    spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=512 * 1024 * 1024)
    credits = RouteCreditBroker(credits_root)
    journal = ProgressJournal(journal_path)
    worker_route_config = dict(route_config)
    worker_route_config["_worker_bind_target"] = max(
        1,
        (int(route_config.get("pool_size", 1)) + max(1, int(worker_count)) - 1)
        // max(1, int(worker_count)),
    )
    worker = ShardWorker(
        worker_id, tuple(partitions), spool,
        {str(route_config["route_id"]): worker_route_config},
        credits=credits, progress_sink=journal.append,
        max_inflight=max_inflight, prefetch_per_partition=prefetch,
    )
    result = asyncio.run(worker.run_until_idle())
    Path(result_path).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


def _campaign_plan(count: int, campaign_counts: str = "") -> list[tuple[int, int]]:
    if str(campaign_counts or "").strip():
        values = [int(item.strip()) for item in str(campaign_counts).split(",") if item.strip()]
        if len(values) < 2 or any(item <= 0 for item in values):
            raise ValueError("--campaign-counts requires at least two positive integers")
        if sum(values) != int(count):
            raise ValueError("--campaign-counts must sum to --count")
        return [(index, value) for index, value in enumerate(values, start=1)]
    total = max(1, int(count))
    if total == 5500:
        return [(1, 2500), (2, 2500), (3, 250), (4, 250)]
    return [(1, total)]


async def run_gate(*, count: int, run_root: Path, workers: int = 4,
                   pool_size: int | None = None, route_tps: float = 0.0,
                   tps_scope: str = "route_shared", window_size: int = 50,
                   timeout: float = 300.0, campaign_counts: str = "") -> dict:
    run_root = Path(run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    spool_root = run_root / "spool"
    credits_root = run_root / "route-credits"
    spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=512 * 1024 * 1024)
    ingress = IngressRouter(spool)
    planned = _campaign_plan(count, campaign_counts)
    expected: dict[tuple[str, int, int], dict] = {}
    generated = 0
    for campaign_id, campaign_count in planned:
        rows = []
        for index in range(campaign_count):
            msisdn = f"090{campaign_id:02d}{index:07d}"
            rows.append({"msisdn": msisdn, "message": f"VNext campaign={campaign_id} event={index}"})
            normalized = f"251{msisdn[1:]}"
            expected[("batch", campaign_id, index + 1)] = {
                "campaign_id": campaign_id, "index": index,
                "destination": normalized,
            }
        result = ingress.enqueue_chunk(
            rows, campaign_type="batch", campaign_id=campaign_id,
            phone_prefix="251", source_id=f"synthetic-campaign-{campaign_id}",
            payload_ref=f"synthetic:{campaign_id}", route_id=1,
        )
        generated += campaign_count
        if result.accepted != campaign_count:
            raise RuntimeError(f"ingress did not durably accept campaign {campaign_id}: {result.as_dict()}")

    smsc = LoopbackSMSC()
    port = await smsc.start()
    partition_map = PartitionMap.create(run_root / "partition-map.json", 256, workers)
    route_config = {
        "route_id": 1, "host": "127.0.0.1", "port": port,
        "system_id": "test", "password": "test", "source_addr": "VNEXT",
        "pool_size": max(1, int(pool_size or workers)),
        "window_size": max(1, int(window_size)), "tps": max(0.0, float(route_tps)),
        "tps_scope": str(tps_scope or "route_shared"), "timeout": 10,
    }
    context = multiprocessing.get_context("spawn")
    processes = []
    journal_paths = [run_root / "progress" / f"worker-{worker_id}.jsonl" for worker_id in range(workers)]
    projection = ProjectionWriterProcess(
        run_root / "projection", batch_size=512, journal_paths=journal_paths,
    )
    # Start the durable Journal tailer before any worker is allowed to write a
    # PDU.  This is the concurrent send+projection gate; the journal remains
    # the source of truth if IPC is full or the writer is restarted.
    projection.start()
    projection_started_before_workers = True
    for worker_id in range(workers):
        journal_path = journal_paths[worker_id]
        result_path = run_root / "workers" / f"worker-{worker_id}.json"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.parent.mkdir(parents=True, exist_ok=True)
        process = context.Process(
            target=_worker_process,
            args=(worker_id, partition_map.partitions_for_worker(worker_id), str(spool_root),
                  str(credits_root), route_config, str(journal_path), str(result_path),
                  128, 64, workers),
            name=f"mc-vnext-acceptance-worker-{worker_id}", daemon=True,
        )
        process.start()
        processes.append(process)
    started = time.monotonic()
    timed_out = False
    try:
        while any(process.is_alive() for process in processes):
            if time.monotonic() - started > max(1.0, float(timeout)):
                timed_out = True
                break
            await asyncio.sleep(0.05)
    finally:
        if timed_out:
            for process in processes:
                if process.is_alive():
                    process.terminate()
        for process in processes:
            process.join(timeout=5)
        await smsc.stop()
        progress = []
        for worker_id in range(workers):
            journal = ProgressJournal(journal_paths[worker_id])
            progress.extend(journal.read())
        expected_projection = len(progress)
        projection_deadline = time.monotonic() + max(
            60.0, min(300.0, expected_projection / 25.0 + 60.0)
        )
        while time.monotonic() < projection_deadline:
            snapshot = projection.snapshot()
            if int(snapshot.get("applied", 0) or 0) >= expected_projection:
                break
            if snapshot.get("alive") is False:
                break
            await asyncio.sleep(0.05)
        projection.stop(timeout=300)

    worker_results = []
    for worker_id in range(workers):
        path = run_root / "workers" / f"worker-{worker_id}.json"
        if path.exists():
            worker_results.append(json.loads(path.read_text(encoding="utf-8")))
    progress.sort(key=lambda item: (float(item.get("created_at", 0)), int(item.get("ordinal", 0))))
    projection_status = projection.snapshot()

    states = Counter(str(item.get("state", "")) for item in progress)
    reconciled_spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=512 * 1024 * 1024)
    per_campaign = Counter()
    per_campaign_received = Counter()
    campaign_counters: dict[int, Counter] = {int(campaign_id): Counter() for campaign_id, _ in planned}
    for item in progress:
        campaign_id = int(item.get("campaign_id") or 0)
        counters = campaign_counters.setdefault(campaign_id, Counter())
        state = str(item.get("state") or "")
        counters["projection"] += 1
        if state.startswith("submit_sm_resp_"):
            counters["submit_sm_resp_success"] += int(state == "submit_sm_resp_success")
            counters["submit_sm_resp_error"] += int(state == "submit_sm_resp_error")
            continue
        counters["accepted"] += 1
        counters["taken"] += int(item.get("taken", 0) or 0)
        counters["socket_written"] += int(item.get("socket_written", state == "socket_written") or 0)
        counters["expired"] += int(item.get("expired", state == "expired") or 0)
        counters["unknown"] += int(item.get("unknown", state == "unknown") or 0)
        if item.get("state") == "socket_written":
            per_campaign[int(item["campaign_id"])] += 1
    expected_destinations = Counter(
        str(item["destination"]) for item in expected.values()
    )
    expected_campaign_by_destination = {
        str(item["destination"]): int(item["campaign_id"])
        for item in expected.values()
    }
    for item in smsc.received:
        destination = str(item["destination"])
        campaign_id = expected_campaign_by_destination.get(destination)
        if campaign_id is not None:
            per_campaign_received[campaign_id] += 1
    received_destinations = [str(item["destination"]) for item in smsc.received]
    received_counts = Counter(received_destinations)
    duplicate_destinations = sorted(
        destination for destination, value in received_counts.items()
        if value > expected_destinations.get(destination, 0)
    )
    missing_destinations = sorted(
        destination for destination, value in expected_destinations.items()
        for _ in range(max(0, value - received_counts.get(destination, 0)))
    )
    unexpected_destinations = sorted(
        destination for destination, value in received_counts.items()
        if value > expected_destinations.get(destination, 0)
    )
    fairness = {
        str(campaign_id): {
            "expected": campaign_count,
            "socket_written": per_campaign[campaign_id],
            "smpp_received": per_campaign_received[campaign_id],
        }
        for campaign_id, campaign_count in planned
    }
    passed = (
        not timed_out and not smsc.errors and generated == len(expected)
        and states["socket_written"] == generated
        and len(smsc.received) == generated
        and smsc.response_success == generated
        and reconciled_spool.status()["pending_records"] == 0
        and int(projection_status.get("applied", 0)) == len(progress)
        and smsc.max_active_binds <= route_config["pool_size"]
        and not duplicate_destinations and not missing_destinations and not unexpected_destinations
        and all(item["socket_written"] == item["expected"] for item in fairness.values())
    )
    gate_name = {10: "P10", 100: "P100", 1000: "P1K", 5500: "P5500"}.get(count, f"P{count}")
    return {
        "gate": gate_name,
        "status": "PASS" if passed else "FAIL",
        "run_root": str(run_root),
        "workers": workers,
        "pool_size": route_config["pool_size"],
        "window_size": route_config["window_size"],
        "route_tps": route_tps,
        "tps_scope": route_config["tps_scope"],
        "configured_effective_total_tps": (
            float(route_tps) * route_config["pool_size"]
            if route_config["tps_scope"] == "per_bind" else float(route_tps)
        ),
        "duration_seconds": round(time.monotonic() - started, 3),
        "generated": generated,
        "spool": reconciled_spool.status(),
        "states": dict(states),
        "socket_written": states["socket_written"],
        "smpp": {
            "binds": smsc.binds, "active_binds": smsc.active_binds,
            "max_active_binds": smsc.max_active_binds,
            "submit_sm_received": len(smsc.received),
            "submit_sm_resp_success": smsc.response_success,
            "errors": smsc.errors,
        },
        "fairness": fairness,
        "campaign_counters": {
            str(campaign_id): {
                "generated": int(dict(planned).get(campaign_id, 0)),
                **{key: int(value) for key, value in counters.items()},
            }
            for campaign_id, counters in sorted(campaign_counters.items())
        },
        "duplicate_wire": len(duplicate_destinations),
        "campaign_plan": [
            {"campaign_id": int(campaign_id), "count": int(campaign_count)}
            for campaign_id, campaign_count in planned
        ],
        "identity_key_schema": ["business_day", "campaign_type", "campaign_id", "event_id", "part_index"],
        "business_day_checks": {
            "executed": False,
            "reason": "not_exercised_by_same_day_loopback_gate",
        },
        "old_business_day_socket_writes_after_cutoff": None,
        "old_business_day_re_dispatch_after_restart": None,
        "day_stats_cross_contamination": None,
        "duplicate_destinations": duplicate_destinations,
        "missing_destinations": missing_destinations[:20],
        "missing_count": len(missing_destinations),
        "unexpected_destinations": unexpected_destinations[:20],
        "worker_results": worker_results,
        "projection": projection_status,
        "timeout": timed_out,
        "execution_mode": "concurrent_worker_journal_projection",
        "projection_started_before_workers": projection_started_before_workers,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", choices=("p10", "p100", "p1k", "p5500"), default="p100")
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--pool-size", type=int, default=0,
                        help="physical bind cap; defaults to workers for the legacy synthetic gate")
    parser.add_argument("--route-tps", type=float, default=0.0)
    parser.add_argument("--tps-scope", choices=("route_shared", "per_bind"), default="route_shared")
    parser.add_argument("--window-size", type=int, default=50,
                        help="PDU window per physical bind")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--campaign-counts", default="",
                        help="comma-separated campaign sizes; must sum to --count")
    parser.add_argument("--run-root", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--quiet", action="store_true", help="print only the bounded result summary")
    args = parser.parse_args()
    count = args.count or {"p10": 10, "p100": 100, "p1k": 1000, "p5500": 5500}[args.gate]
    root = Path(args.run_root).resolve() if args.run_root else Path(__import__("tempfile").mkdtemp(prefix="mc-vnext-loopback-"))
    result = asyncio.run(run_gate(
        count=count, run_root=root, workers=max(1, min(8, args.workers)),
        pool_size=max(1, min(20, args.pool_size)) if args.pool_size else None,
        route_tps=max(0.0, args.route_tps), tps_scope=args.tps_scope,
        window_size=max(1, min(1000, args.window_size)), timeout=args.timeout,
        campaign_counts=args.campaign_counts,
    ))
    output = Path(args.output).resolve() if args.output else root / "result.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    if args.quiet:
        print(json.dumps({
            "gate": result["gate"], "status": result["status"],
            "duration_seconds": result["duration_seconds"],
            "generated": result["generated"], "socket_written": result["socket_written"],
            "submit_sm_received": result["smpp"]["submit_sm_received"],
            "max_active_binds": result["smpp"]["max_active_binds"],
            "submit_sm_resp_success": result["smpp"]["submit_sm_resp_success"],
            "projection_applied": result["projection"].get("applied"),
            "pending": result["spool"].get("pending_records"),
            "missing": result["missing_count"],
        }, ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
