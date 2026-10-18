"""Layered throughput bottleneck-isolation harness for the VNext SMPP path.

The harness deliberately composes the production classes without changing
their implementation.  L1 is transport/credit/window only, L2 adds the real
spool and worker, and L3 adds the real fsyncing progress journal.  It is
intended to run on the target's native filesystem; the caller supplies the
source and result roots explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import sys
import time
import traceback
from pathlib import Path
from typing import Any


TOTAL_CAMPAIGNS = 4
ROUTE_ID = "1"
POOL_SIZE = 4
WINDOW_SIZE = 50
TPS = 200.0
TPS_SCOPE = "per_bind"
MESSAGE = "vnext layered throughput isolation"


def _install_source(source_root: str | os.PathLike[str]) -> Path:
    root = Path(source_root).resolve()
    backend = root / "backend"
    if not backend.is_dir():
        raise FileNotFoundError(f"backend directory not found under {root}")
    value = str(backend)
    if value not in sys.path:
        sys.path.insert(0, value)
    return root


def _rate(count: int, start: float | None, end: float | None) -> float | str:
    if start is None or end is None or end <= start:
        return "NOT_MEASURED"
    return round(float(count) / max(0.000001, end - start), 3)


def _min_timestamp(rows: list[dict], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return min(values) if values else None


def _max_timestamp(rows: list[dict], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return max(values) if values else None


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _route_config(port: int) -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": int(port),
        "system_id": "vnext-layered",
        "password": "vnext-layered",
        "source_addr": "VNEXT",
        "route_id": ROUTE_ID,
        "pool_size": POOL_SIZE,
        "window_size": WINDOW_SIZE,
        "tps": TPS,
        "tps_scope": TPS_SCOPE,
        "timeout": 10.0,
        "enquire_interval": 0.0,
        "reconnect_max_seconds": 0.0,
        "_worker_bind_target": POOL_SIZE,
    }


def _smpp_symbols(smpp: Any) -> Any:
    """Build the simulator's small symbol contract from production SMPP code."""

    return type(
        "LayeredSmppSymbols",
        (),
        {
            "BIND_RECEIVER": smpp.BIND_RECEIVER,
            "BIND_TRANSMITTER": smpp.BIND_TRANSMITTER,
            "BIND_TRANSCEIVER": smpp.BIND_TRANSCEIVER,
            "ENQUIRE_LINK": smpp.ENQUIRE_LINK,
            "HEADER": smpp.HEADER,
            "RESPONSE_MASK": smpp.RESPONSE_MASK,
            "SUBMIT_SM": smpp.SUBMIT_SM,
            "UNBIND": smpp.UNBIND,
            "cstring": staticmethod(smpp.cstring),
            "read_pdu": staticmethod(smpp.read_pdu),
            "parse_destination": staticmethod(_parse_destination),
        },
    )


def _parse_destination(body: bytes) -> str:
    offset = 0
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated service_type")
    offset = end + 1 + 2
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated source_addr")
    offset = end + 1 + 2
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated destination_addr")
    return body[offset:end].decode("ascii", errors="replace")


def _destination(campaign: int, index: int) -> str:
    return f"25190{campaign:02d}{index:07d}"


def _context(campaign: int, index: int, event_id: str = "") -> dict[str, Any]:
    return {
        "event_id": event_id or f"layered:{campaign}:{index}",
        "partition_id": (campaign * 31 + index) % 256,
        "ordinal": index + 1,
        "campaign_type": "batch",
        "campaign_id": campaign,
        "route_id": int(ROUTE_ID),
        "normalized_msisdn": _destination(campaign, index),
        "parts_total": 1,
        "created_at": time.time(),
    }


async def _drain_transports(transports: list[Any], timeout: float = 60.0) -> dict[str, Any]:
    deadline = time.monotonic() + float(timeout)
    while True:
        pending = sum(
            int(item.snapshot().get("window_inflight", 0) or 0)
            for item in transports
        )
        if pending == 0:
            return {"drained": True, "pending": 0, "timeout_seconds": timeout}
        if time.monotonic() >= deadline:
            return {"drained": False, "pending": pending, "timeout_seconds": timeout}
        await asyncio.sleep(0.01)


def _profiler(run_root: Path, role: str, enabled: bool):
    from vnext.critical_path_profiler import CriticalPathProfiler

    return CriticalPathProfiler(
        run_root if enabled else None,
        role,
        interval_seconds=0.1,
        sample_limit=4096,
    )


def _common_result(
    *,
    layer: str,
    count: int,
    run_root: Path,
    source_root: Path,
    test_start: float,
    test_end: float,
    socket_written: int,
    responses: int,
    bind_rows: list[dict[str, Any]],
    simulator: dict[str, Any],
    credits: dict[str, Any],
    profiler: Any,
    ingestion: dict[str, Any] | None = None,
    worker: dict[str, Any] | None = None,
    journal: dict[str, Any] | None = None,
    pass_rows: list[dict[str, Any]] | None = None,
    instrumentation: bool = True,
) -> dict[str, Any]:
    first_socket = _min_timestamp(bind_rows, "first_socket_write_monotonic")
    last_socket = _max_timestamp(bind_rows, "last_socket_write_monotonic")
    first_response = _min_timestamp(bind_rows, "first_response_monotonic")
    last_response = _max_timestamp(bind_rows, "last_response_monotonic")
    wall_seconds = max(0.000001, test_end - test_start)
    profile = profiler.snapshot()
    profiler.publish(force=True)
    close_seconds = round(
        sum(float(row.get("close_seconds", 0.0) or 0.0) for row in (pass_rows or [])),
        6,
    )
    connect_seconds = round(
        sum(float(row.get("connect_setup_seconds", 0.0) or 0.0) for row in (pass_rows or [])),
        6,
    )
    lifecycle_seconds = close_seconds + connect_seconds
    result = {
        "schema_version": 1,
        "status": "PASS",
        "layer": layer,
        "count_expected": int(count),
        "source_root": str(source_root),
        "run_root": str(run_root),
        "parameters": {
            "campaigns": TOTAL_CAMPAIGNS,
            "workers": 1 if layer in {"L2", "L3"} else "NOT_APPLICABLE",
            "pool_size": POOL_SIZE,
            "window_size": WINDOW_SIZE,
            "per_bind_tps": TPS,
            "tps_scope": TPS_SCOPE,
            "response_delay_seconds": 0.0,
            "instrumentation": bool(instrumentation),
        },
        "counts": {
            "accepted": int((ingestion or {}).get("accepted", count)),
            "socket_written": int(socket_written),
            "simulator_received": int(simulator.get("received", 0) or 0),
            "responses": int(responses),
            "simulator_responses": int(simulator.get("responses", 0) or 0),
            "journal_events_durable": (
                int((journal or {}).get("events_durable", 0))
                if journal is not None else "NOT_MEASURED"
            ),
            "projection_committed_events": "NOT_MEASURED",
            "unknown_response": int(sum(int(row.get("unknown_parts", 0) or 0) for row in bind_rows)),
            "write_failed": int(sum(int(row.get("write_failures", 0) or 0) for row in bind_rows)),
            "duplicate_pdu": int(simulator.get("duplicate_destinations", 0) or 0),
        },
        "timing": {
            "test_start_monotonic": test_start,
            "test_end_monotonic": test_end,
            "first_socket_write_monotonic": first_socket,
            "last_socket_write_monotonic": last_socket,
            "first_response_monotonic": first_response,
            "last_response_monotonic": last_response,
            "wall_seconds": round(wall_seconds, 6),
            "sample_counts": {
                "socket_written": int(socket_written),
                "responses": int(responses),
                "binds": len(bind_rows),
                "journal_events": int((journal or {}).get("events_durable", 0)) if journal else "NOT_MEASURED",
            },
        },
        "rates": {
            "stage_active_pdu_rate": _rate(socket_written, first_socket, last_socket),
            "response_phase_pdu_rate": _rate(responses, first_socket, last_response),
            "e2e_wall_pdu_rate": _rate(socket_written, test_start, test_end),
            "projection_committed_events_rate": "NOT_MEASURED",
        },
        "bind_lifecycle": {
            "bind_count": len(bind_rows),
            "bind_acquired": int(sum(int(row.get("bind_acquired_count", 0) or 0) for row in bind_rows)),
            "bind_released": int(sum(int(row.get("bind_released_count", 0) or 0) for row in bind_rows)),
            "max_window_inflight": max(
                [int(row.get("window_inflight_peak", 0) or 0) for row in bind_rows] or [0]
            ),
            "sum_bind_active_seconds": round(
                sum(float(row.get("bind_active_seconds", 0.0) or 0.0) for row in bind_rows), 6
            ),
            "connect_setup_seconds": connect_seconds,
            "connect_setup_semantics": (
                "sum of pass-start to first socket-write proxies; includes worker scheduling "
                "and credit wait, not a pure TCP bind duration"
            ),
            "close_seconds": close_seconds,
            "close_rebind_seconds": round(lifecycle_seconds, 6),
            "close_rebind_wall_share": round(lifecycle_seconds / wall_seconds, 6),
            "pass_rows": pass_rows or [],
        },
        "simulator": simulator,
        "route_credit": credits,
        "worker": worker or "NOT_MEASURED",
        "journal": journal or "NOT_MEASURED",
        "critical_path_profiler": profile if instrumentation else "DISABLED",
        "staging_bind_execute": "NOT_MEASURED",
        "staging_p95_origin": "NOT_MEASURED",
    }
    validation_errors: list[str] = []
    counts = result["counts"]
    if int(counts["accepted"]) != int(count):
        validation_errors.append(f"accepted={counts['accepted']} expected={count}")
    if int(counts["socket_written"]) != int(count):
        validation_errors.append(f"socket_written={counts['socket_written']} expected={count}")
    if int(counts["simulator_received"]) != int(count):
        validation_errors.append(f"simulator_received={counts['simulator_received']} expected={count}")
    if int(counts["responses"]) != int(count):
        validation_errors.append(f"responses={counts['responses']} expected={count}")
    if int(counts["simulator_responses"]) != int(count):
        validation_errors.append(f"simulator_responses={counts['simulator_responses']} expected={count}")
    if int(counts["unknown_response"]) != 0:
        validation_errors.append(f"unknown_response={counts['unknown_response']}")
    if int(counts["write_failed"]) != 0:
        validation_errors.append(f"write_failed={counts['write_failed']}")
    if int(counts["duplicate_pdu"]) != 0:
        validation_errors.append(f"duplicate_pdu={counts['duplicate_pdu']}")
    if simulator.get("errors"):
        validation_errors.append(f"simulator_errors={simulator['errors']}")
    if int(simulator.get("active_binds", 0) or 0) != 0:
        validation_errors.append(f"active_binds={simulator['active_binds']}")
    if journal is not None:
        expected_journal = int(count) * 2
        if int(journal.get("events_durable", 0) or 0) != expected_journal:
            validation_errors.append(
                f"journal_events_durable={journal.get('events_durable')} expected={expected_journal}"
            )
        if int(journal.get("response_events_durable", 0) or 0) != int(count):
            validation_errors.append(
                f"response_events_durable={journal.get('response_events_durable')} expected={count}"
            )
        if not journal.get("event_count_matches_sink", False):
            validation_errors.append("journal_event_count_does_not_match_sink")
    result["validation"] = {
        "passed": not validation_errors,
        "errors": validation_errors,
    }
    result["status"] = "PASS" if not validation_errors else "FAIL"
    return result


def _simulator_snapshot(simulator: Any) -> dict[str, Any]:
    snapshot = dict(simulator.snapshot())
    # Keep the evidence compact but retain enough information to prove the
    # duplicate check when the loopback tracks every destination.
    snapshot["per_destination_counts"] = dict(simulator.destination_counts)
    snapshot["destination_tracking_truncated"] = bool(simulator.destination_tracking_truncated)
    snapshot["duplicate_destinations"] = sum(
        max(0, int(value) - 1) for value in simulator.destination_counts.values()
    )
    snapshot["errors"] = list(simulator.errors)
    return snapshot


async def _run_l1(
    *,
    count: int,
    run_root: Path,
    source_root: Path,
    instrumentation: bool,
) -> dict[str, Any]:
    from tools.vnext_streaming_acceptance import CountingLoopbackSMSC
    from vnext import smpp_transport as smpp
    from vnext.route_credits import RouteCreditBroker
    from vnext.smpp_transport import AsyncSmppTransport

    profiler = _profiler(run_root, "L1-main", instrumentation)
    credits = RouteCreditBroker(run_root / "route-credits")
    simulator = CountingLoopbackSMSC(_smpp_symbols(smpp), response_delay_seconds=0.0)
    transports: list[AsyncSmppTransport] = []
    response_events: list[dict[str, Any]] = []
    test_start = time.monotonic()
    try:
        port = await simulator.start()
        config = _route_config(port)
        connect_started = time.monotonic()
        for _ in range(POOL_SIZE):
            transport = AsyncSmppTransport(
                config, credits=credits,
                response_callback=response_events.append,
                profiler=profiler,
            )
            await transport.connect()
            transports.append(transport)
        connected_at = time.monotonic()
        per_bind = count // POOL_SIZE

        async def send_bind(transport: AsyncSmppTransport, campaign: int) -> None:
            for base in range(0, per_bind, 64):
                tasks = []
                for index in range(base, min(per_bind, base + 64)):
                    tasks.append(transport.send_task(
                        destination_addr=_destination(campaign, index),
                        message=MESSAGE,
                        reference=(index % 255) or 1,
                        source_addr="VNEXT",
                        progress_context=_context(campaign, index),
                    ))
                await asyncio.gather(*tasks)

        await asyncio.gather(*(
            send_bind(transport, campaign)
            for campaign, transport in enumerate(transports, 1)
        ))
        drained = await _drain_transports(transports)
        if not drained["drained"]:
            raise TimeoutError(f"L1 response drain timed out: {drained}")
        bind_rows: list[dict[str, Any]] = []
        close_started = time.monotonic()
        for transport in transports:
            await transport.close()
            bind_rows.append(transport.snapshot())
        close_seconds = time.monotonic() - close_started
        test_end = time.monotonic()
        await asyncio.sleep(0.05)
        await simulator.stop()
        sim = _simulator_snapshot(simulator)
        result = _common_result(
            layer="L1", count=count, run_root=run_root, source_root=source_root,
            test_start=test_start, test_end=test_end,
            socket_written=sum(int(row.get("socket_written_parts", 0) or 0) for row in bind_rows),
            responses=len(response_events), bind_rows=bind_rows, simulator=sim,
            credits=credits.snapshot(), profiler=profiler,
            pass_rows=[{
                "pass_index": 1,
                "pending_before": count,
                "pending_after": 0,
                "tasks_requested": count,
                "connect_setup_seconds": round(connected_at - connect_started, 6),
                "close_seconds": round(close_seconds, 6),
                "bind_count": len(bind_rows),
                "socket_written": sum(int(row.get("socket_written_parts", 0) or 0) for row in bind_rows),
                "responses": len(response_events),
            }],
            instrumentation=instrumentation,
        )
        result["diagnostic"] = {
            "response_sink": "in_memory",
            "worker": "NOT_INCLUDED",
            "spool": "NOT_INCLUDED",
            "journal": "NOT_INCLUDED",
            "projection": "NOT_INCLUDED",
        }
        return result
    finally:
        for transport in transports:
            if transport.connected:
                await transport.close()
        await simulator.stop()


async def _run_worker_layer(
    *,
    layer: str,
    count: int,
    run_root: Path,
    source_root: Path,
    pass_size: int,
    keep_transports: bool,
    instrumentation: bool,
) -> dict[str, Any]:
    from tools.vnext_streaming_acceptance import CountingLoopbackSMSC
    from vnext import smpp_transport as smpp
    from vnext.critical_path_profiler import CriticalPathProfiler
    from vnext.ingress import IngressRouter
    from vnext.projection_writer import ProgressJournal
    from vnext.route_credits import RouteCreditBroker
    from vnext.spool import TaskSpool
    from vnext.worker import ShardWorker

    profiler = _profiler(run_root, f"{layer}-main", instrumentation)
    credits = RouteCreditBroker(run_root / "route-credits")
    spool = TaskSpool(
        run_root / "spool",
        virtual_partition_count=256,
        segment_max_records=1000,
        segment_max_bytes=4 * 1024 * 1024,
        max_bytes=1024 * 1024 * 1024,
        fsync=True,
    )
    ingress = IngressRouter(spool)
    per_campaign = count // TOTAL_CAMPAIGNS
    ingestion = {"accepted": 0, "duplicate": 0, "rejected": 0}
    for campaign in range(1, TOTAL_CAMPAIGNS + 1):
        rows = [
            {"msisdn": _destination(campaign, index), "message": MESSAGE}
            for index in range(per_campaign)
        ]
        accepted = ingress.enqueue_chunk(
            rows,
            campaign_type="batch",
            campaign_id=campaign,
            campaign_generation=1,
            route_id=int(ROUTE_ID),
            phone_prefix="",
            source_id=f"layered-{layer.lower()}-{campaign}",
            payload_ref=f"layered-{layer.lower()}-{campaign}",
            row_ordinal_start=0,
        )
        ingestion["accepted"] += int(accepted.accepted)
        ingestion["duplicate"] += int(accepted.duplicate)
        ingestion["rejected"] += int(accepted.rejected)
    if ingestion["accepted"] != count:
        raise AssertionError(f"ingress accepted {ingestion['accepted']} of {count}")

    journal: ProgressJournal | None = None
    durable_events: list[dict[str, Any]] = []
    if layer == "L3":
        journal = ProgressJournal(
            run_root / "progress.jsonl",
            max_bytes=512 * 1024 * 1024,
            segment_bytes=64 * 1024 * 1024,
            profiler=profiler,
        )

        def progress_sink(event: dict[str, Any]) -> None:
            journal.append(event)
            durable_events.append(event)
    else:
        diagnostic_events: list[dict[str, Any]] = []

        def progress_sink(event: dict[str, Any]) -> None:
            diagnostic_events.append(event)

    simulator = CountingLoopbackSMSC(_smpp_symbols(smpp), response_delay_seconds=0.0)
    test_start = time.monotonic()
    bind_rows: list[dict[str, Any]] = []
    pass_rows: list[dict[str, Any]] = []
    worker: ShardWorker | None = None
    try:
        port = await simulator.start()
        worker = ShardWorker(
            0,
            tuple(range(256)),
            spool,
            {ROUTE_ID: _route_config(port)},
            credits=credits,
            progress_sink=progress_sink,
            max_inflight=64,
            prefetch_per_partition=32,
            profiler=profiler,
        )
        target_pass_size = int(pass_size) if int(pass_size) > 0 else None
        pass_index = 0
        previous_socket_written = 0
        previous_responses = 0
        while True:
            pending_before = int(spool.lightweight_snapshot().get("pending_records", 0) or 0)
            if pending_before <= 0:
                break
            pass_index += 1
            pass_started = time.monotonic()
            await worker.run_until_idle(
                max_tasks=target_pass_size,
                close_transports=False,
            )
            drained = await worker.drain_transports(timeout=60.0)
            if not drained.get("drained"):
                raise TimeoutError(f"{layer} response drain timed out: {drained}")
            transport_objects = [
                transport
                for values in worker._transports.values()
                for transport in values
            ]
            live_rows = [transport.snapshot() for transport in transport_objects]
            first_socket = _min_timestamp(live_rows, "first_socket_write_monotonic")
            connect_setup = (
                max(0.0, first_socket - pass_started)
                if first_socket is not None and pass_index == 1 else 0.0
            )
            pending_after = int(spool.lightweight_snapshot().get("pending_records", 0) or 0)
            live_socket_written = sum(
                int(row.get("socket_written_parts", 0) or 0) for row in live_rows
            )
            live_responses = sum(
                int(row.get("response_success", 0) or 0) for row in live_rows
            )
            if keep_transports:
                close_seconds = 0.0
                bind_count = len(live_rows) if pass_index == 1 else 0
                socket_written_delta = live_socket_written - previous_socket_written
                response_delta = live_responses - previous_responses
                previous_socket_written = live_socket_written
                previous_responses = live_responses
            else:
                close_started = time.monotonic()
                await worker.close_transports()
                close_seconds = time.monotonic() - close_started
                closed_rows = [transport.snapshot() for transport in transport_objects]
                for row in closed_rows:
                    row["pass_index"] = pass_index
                    bind_rows.append(row)
                bind_count = len(closed_rows)
                socket_written_delta = sum(
                    int(row.get("socket_written_parts", 0) or 0) for row in closed_rows
                )
                response_delta = sum(
                    int(row.get("response_success", 0) or 0) for row in closed_rows
                )
            worker.release_reader_caches()
            pass_rows.append({
                "pass_index": pass_index,
                "pending_before": pending_before,
                "pending_after": pending_after,
                "tasks_requested": target_pass_size or pending_before,
                "connect_setup_seconds": round(connect_setup, 6),
                "close_seconds": round(close_seconds, 6),
                "bind_count": bind_count,
                "socket_written": socket_written_delta,
                "responses": response_delta,
            })
            if pending_after >= pending_before:
                raise RuntimeError(
                    f"{layer} made no spool progress in pass {pass_index}: "
                    f"before={pending_before} after={pending_after}"
                )
        if keep_transports:
            transport_objects = [
                transport
                for values in worker._transports.values()
                for transport in values
            ]
            close_started = time.monotonic()
            await worker.close_transports()
            close_seconds = time.monotonic() - close_started
            closed_rows = [transport.snapshot() for transport in transport_objects]
            for row in closed_rows:
                row["pass_index"] = pass_index + 1
                bind_rows.append(row)
            if pass_rows:
                pass_rows[-1]["close_seconds"] = round(close_seconds, 6)
        final_worker = worker.snapshot()
        test_end = time.monotonic()
        await asyncio.sleep(0.05)
        await simulator.stop()
        sim = _simulator_snapshot(simulator)
        journal_info: dict[str, Any] | None = None
        if journal is not None:
            events = journal.read()
            response_events = sum(1 for event in events if ":resp:" in str(event.get("event_id") or ""))
            journal_info = {
                "path": str(journal.path),
                "events_durable": len(events),
                "response_events_durable": response_events,
                "bytes": journal.size(),
                "storage": journal.storage_snapshot(),
                "read_metrics": journal.read_metrics(),
                "event_count_matches_sink": len(events) == len(durable_events),
            }
        response_count = int(sim.get("responses", 0) or 0)
        result = _common_result(
            layer=layer, count=count, run_root=run_root, source_root=source_root,
            test_start=test_start, test_end=test_end,
            socket_written=sum(int(row.get("socket_written_parts", 0) or 0) for row in bind_rows),
            responses=response_count, bind_rows=bind_rows, simulator=sim,
            credits=credits.snapshot(), profiler=profiler,
            ingestion=ingestion, worker=final_worker, journal=journal_info,
            pass_rows=pass_rows, instrumentation=instrumentation,
        )
        result["diagnostic"] = {
            "bounded_pass_size": target_pass_size or "ALL_AVAILABLE",
            "keep_transports_until_done": bool(keep_transports),
            "connect_setup_semantics": (
                "pass-start to first socket-write proxy; includes worker scheduling and credit wait"
            ),
            "response_sink": "ProgressJournal" if layer == "L3" else "in_memory",
            "spool_pending_after": int(spool.lightweight_snapshot().get("pending_records", 0) or 0),
            "spool_status": spool.status(),
            "journal_projection": "NOT_INCLUDED",
        }
        return result
    finally:
        if worker is not None:
            await worker.close_transports()
        await simulator.stop()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    source_root = _install_source(args.source_root)
    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    count = int(args.count)
    if count <= 0 or count % TOTAL_CAMPAIGNS != 0 or count % POOL_SIZE != 0:
        raise ValueError("count must be positive and divisible by 4")
    layer = str(args.layer).upper()
    if layer == "L1":
        return await _run_l1(
            count=count, run_root=run_root, source_root=source_root,
            instrumentation=bool(args.instrumentation),
        )
    if layer in {"L2", "L3"}:
        return await _run_worker_layer(
            layer=layer, count=count, run_root=run_root, source_root=source_root,
            pass_size=int(args.pass_size), keep_transports=bool(args.keep_transports),
            instrumentation=bool(args.instrumentation),
        )
    raise ValueError(f"unsupported layer {layer}; use L1, L2, or L3")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=("L1", "L2", "L3"), required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument(
        "--pass-size", type=int, default=250,
        help="bounded worker pass size for L2/L3; 0 means one unbounded pass",
    )
    parser.add_argument(
        "--keep-transports", action="store_true",
        help="diagnostic control: keep binds across bounded passes until final drain",
    )
    parser.add_argument(
        "--no-instrumentation", dest="instrumentation", action="store_false",
        help="disable CriticalPathProfiler (default is enabled)",
    )
    parser.set_defaults(instrumentation=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    run_root = Path(args.run_root).resolve()
    request = {
        "layer": args.layer,
        "count": args.count,
        "source_root": str(Path(args.source_root).resolve()),
        "run_root": str(run_root),
        "pass_size": args.pass_size,
        "keep_transports": bool(args.keep_transports),
        "instrumentation": bool(args.instrumentation),
        "python": sys.version,
        "platform": platform.platform(),
        "pid": os.getpid(),
        "started_epoch": time.time(),
    }
    _write_json(run_root / "request.json", request)
    try:
        result = asyncio.run(_run(args))
    except Exception as exc:  # Keep a machine-readable forensic result.
        result = {
            "schema_version": 1,
            "status": "FAIL",
            "layer": args.layer,
            "count_expected": args.count,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        _write_json(run_root / "result.json", result)
        return 1
    _write_json(run_root / "result.json", result)
    digest = hashlib.sha256((run_root / "result.json").read_bytes()).hexdigest()
    _write_json(run_root / "result.sha256.json", {"sha256": digest})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
