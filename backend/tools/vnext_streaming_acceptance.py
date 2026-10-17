"""Bounded, loopback-only streaming acceptance harness for VNext.

This is a test-run artifact.  It reuses the frozen VNext ingress, spool,
worker, SMPP transport, Journal, and single ProjectionWriter implementation;
the only extra behavior here is a bounded multi-campaign feeder and metrics
collection for T3/T4.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import os
from collections import Counter
from pathlib import Path
import shutil
import subprocess
import time


def _install_source(source_root: str) -> None:
    root = Path(source_root).resolve()
    backend = root / "backend"
    if str(backend) not in os.sys.path:
        os.sys.path.insert(0, str(backend))


def _parse_counts(value: str) -> list[int]:
    counts = [int(item.strip()) for item in str(value).split(",") if item.strip()]
    if len(counts) < 4 or any(item <= 0 for item in counts):
        raise ValueError("--counts requires at least four positive integers")
    return counts


def _parse_ids(value: str) -> set[int]:
    if not value.strip():
        return set()
    return {int(item.strip()) for item in value.split(",") if item.strip()}


def _directory_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _process_tree_metrics(roots: list[int]) -> tuple[float, int, int]:
    """Return aggregate CPU%, RSS KiB, and process count for this Run."""

    try:
        output = subprocess.check_output(
            ["ps", "-eo", "pid=,ppid=,pcpu=,rss="],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return 0.0, 0, 0
    rows: dict[int, tuple[int, float, int]] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) != 4:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
            cpu, rss = float(parts[2]), int(parts[3])
        except ValueError:
            continue
        rows[pid] = (ppid, cpu, rss)
    selected = set(int(pid) for pid in roots if int(pid) > 0)
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _cpu, _rss) in rows.items():
            if pid not in selected and ppid in selected:
                selected.add(pid)
                changed = True
    return (
        round(sum(rows[pid][1] for pid in selected if pid in rows), 3),
        sum(rows[pid][2] for pid in selected if pid in rows),
        len(selected),
    )


def _destination_campaign(destination: str) -> int | None:
    # The feeder uses 25190{campaign:02d}{index:07d} synthetic destinations.
    try:
        if destination.startswith("25190") and len(destination) >= 7:
            return int(destination[5:7])
    except ValueError:
        pass
    return None


class CountingLoopbackSMSC:
    """Minimal loopback SMPP server with bounded event memory."""

    def __init__(self, smpp, *, max_destination_keys: int = 100_000):
        self._smpp = smpp
        self.server = None
        self.total_received = 0
        self.responses = 0
        self.response_success = 0
        self.binds = 0
        self.active_binds = 0
        self.max_active_binds = 0
        self.errors: list[str] = []
        self.destination_counts: Counter[str] = Counter()
        self.campaign_pdus: Counter[int] = Counter()
        self.max_destination_keys = max(1, int(max_destination_keys))
        self.destination_tracking_truncated = False

    async def _handle(self, reader, writer):
        bound = False
        try:
            while True:
                command, _status, sequence, body = await self._smpp.read_pdu(reader)
                if command in {
                    self._smpp.BIND_RECEIVER,
                    self._smpp.BIND_TRANSMITTER,
                    self._smpp.BIND_TRANSCEIVER,
                }:
                    if not bound:
                        bound = True
                        self.binds += 1
                        self.active_binds += 1
                        self.max_active_binds = max(self.max_active_binds, self.active_binds)
                    response = self._smpp.cstring("VNEXT-STREAM-SMSC")
                    writer.write(
                        self._smpp.HEADER.pack(
                            16 + len(response), command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                        + response
                    )
                    await writer.drain()
                elif command == self._smpp.SUBMIT_SM:
                    destination = self._smpp.parse_destination(body)
                    self.total_received += 1
                    if (
                        destination in self.destination_counts
                        or len(self.destination_counts) < self.max_destination_keys
                    ):
                        self.destination_counts[destination] += 1
                    else:
                        self.destination_tracking_truncated = True
                    campaign = _destination_campaign(destination)
                    if campaign is not None:
                        self.campaign_pdus[campaign] += 1
                    response = self._smpp.cstring(f"VNEXT-{self.total_received:012d}")
                    writer.write(
                        self._smpp.HEADER.pack(
                            16 + len(response), command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                        + response
                    )
                    await writer.drain()
                    self.responses += 1
                    self.response_success += 1
                elif command == self._smpp.ENQUIRE_LINK:
                    writer.write(
                        self._smpp.HEADER.pack(
                            16, command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                    )
                    await writer.drain()
                elif command == self._smpp.UNBIND:
                    writer.write(
                        self._smpp.HEADER.pack(
                            16, command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                    )
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


async def _drain_transport_responses(worker, *, timeout: float = 60.0) -> None:
    """Wait for every written PDU's response before releasing bind leases.

    ``ShardWorker.run_until_idle`` can finish as soon as socket writes have
    been queued, while the SMPP reader task is still consuming responses.
    Closing the transport at that point clears its pending response window and
    makes a high-rate loopback run look short on ``submit_sm_resp`` events.
    The streaming harness must drain that transport-level window before it
    releases a physical bind slot for another worker.
    """

    deadline = time.monotonic() + max(1.0, float(timeout))
    while True:
        snapshots = worker.snapshot().get("transports", [])
        pending = sum(int(item.get("window_inflight", 0) or 0) for item in snapshots)
        if pending == 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"SMPP response drain exceeded {timeout:.1f}s; pending={pending}"
            )
        await asyncio.sleep(0.01)


def _release_completed_worker_caches(spool, worker) -> None:
    """Release reader-only caches after a completed worker pass.

    Ingress owns the authoritative spool index.  A worker only needs records
    for its current bounded pass; retaining every decoded record and every
    completed destination lock in each spawned worker multiplies memory by
    the worker count during a long steady run.
    """

    event_index = getattr(spool, "_event_index", None)
    if isinstance(event_index, dict):
        event_index.clear()
    number_locks = getattr(worker, "_number_locks", None)
    if isinstance(number_locks, dict):
        number_locks.clear()


def _worker_process(
    source_root: str,
    worker_id: int,
    partitions: tuple[int, ...],
    spool_root: str,
    credits_root: str,
    route_config: dict,
    journal_path: str,
    result_path: str,
    worker_count: int,
    feeder_done,
    stop_event,
) -> None:
    _install_source(source_root)
    from vnext.projection_writer import ProgressJournal
    from vnext.route_credits import RouteCreditBroker
    from vnext.spool import TaskSpool
    from vnext.worker import ShardWorker

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
        worker_id,
        tuple(partitions),
        spool,
        {str(route_config["route_id"]): worker_route_config},
        credits=credits,
        progress_sink=journal.append,
        max_inflight=128,
        prefetch_per_partition=64,
    )

    async def loop() -> None:
        while True:
            # Each pass may become idle while another worker still owns the
            # bounded physical bind pool.  Release the leases at the pass
            # boundary so a worker with newly admitted partitions cannot be
            # starved behind an idle worker.  The production runtime uses the
            # same bounded lifecycle; this is not a quota bypass.
            await worker.run_until_idle(close_transports=False)
            await _drain_transport_responses(worker)
            await worker.close_transports()
            _release_completed_worker_caches(spool, worker)
            # The feeder/worker idle loop must not rebuild every segment index.
            # Full status is reserved for the final reconciliation boundary.
            status = spool.lightweight_snapshot()
            if stop_event.is_set() and int(status.get("pending_records", 0) or 0) == 0:
                break
            await asyncio.sleep(0.02)
        await worker.close_transports()
        return worker.snapshot()

    result = asyncio.run(loop())
    Path(result_path).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


def _sample(
    *, run_root: Path, process_roots: list[int], spool, projection, smsc, started: float
) -> dict:
    cpu, rss_kib, process_count = _process_tree_metrics(process_roots)
    try:
        projection_status = projection.snapshot()
    except Exception as exc:  # pragma: no cover - diagnostic path
        projection_status = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    spool_status = spool.lightweight_snapshot()
    return {
        "at_seconds": round(time.monotonic() - started, 3),
        "cpu_percent_sum": cpu,
        "rss_kib_sum": rss_kib,
        "process_count": process_count,
        "run_bytes": _directory_bytes(run_root),
        "disk_free_bytes": shutil.disk_usage(run_root).free,
        "spool_bytes": int(spool_status.get("bytes", 0) or 0),
        "spool_pending_records": int(spool_status.get("pending_records", 0) or 0),
        "journal_bytes": sum(
            path.stat().st_size
            for path in run_root.rglob("worker-*.jsonl")
            if path.is_file()
        ),
        "projection_applied": int(projection_status.get("applied", 0) or 0),
        "projection_lag_seconds": float(projection_status.get("projection_lag_seconds", 0.0) or 0.0),
        "projection_queue_depth": int(projection_status.get("queue_depth", 0) or 0),
        "smpp_received": int(getattr(smsc, "total_received", 0)),
        "smpp_response_success": smsc.response_success,
    }


async def run(args: argparse.Namespace) -> dict:
    _install_source(args.source_root)
    from vnext.ingress import IngressRouter
    from vnext.partition import PartitionMap
    from vnext.projection_writer import ProgressJournal, ProjectionWriterProcess
    from vnext.route_credits import RouteCreditBroker  # noqa: F401 - import contract check
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
    smpp = type(
        "SmppSymbols",
        (),
        {
            "BIND_RECEIVER": BIND_RECEIVER,
            "BIND_TRANSMITTER": BIND_TRANSMITTER,
            "BIND_TRANSCEIVER": BIND_TRANSCEIVER,
            "ENQUIRE_LINK": ENQUIRE_LINK,
            "HEADER": HEADER,
            "RESPONSE_MASK": RESPONSE_MASK,
            "SUBMIT_SM": SUBMIT_SM,
            "UNBIND": UNBIND,
            "cstring": staticmethod(cstring),
            "read_pdu": staticmethod(read_pdu),
            "parse_destination": staticmethod(_parse_destination),
        },
    )

    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    spool_root = run_root / "spool"
    credits_root = run_root / "route-credits"
    spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=args.spool_max_bytes)
    ingress = IngressRouter(spool)
    counts = _parse_counts(args.counts)
    campaign_ids = list(range(1, len(counts) + 1))
    multipart_campaigns = _parse_ids(args.multipart_campaigns)
    total_tasks = sum(counts)

    # Reuse the exact loopback protocol server already used by the frozen
    # P10/P100/P1K/P5500 acceptance tool; this harness only adds streaming.
    smsc = CountingLoopbackSMSC(smpp)
    port = await smsc.start()
    partition_map = PartitionMap.create(run_root / "partition-map.json", 256, args.workers)
    route_config = {
        "route_id": 1,
        "host": "127.0.0.1",
        "port": port,
        "system_id": "test",
        "password": "test",
        "source_addr": "VNEXT",
        "pool_size": args.pool_size,
        "window_size": args.window_size,
        "tps": args.route_tps,
        "tps_scope": args.tps_scope,
        "timeout": 10,
    }
    context = multiprocessing.get_context("spawn")
    feeder_done = context.Event()
    stop_event = context.Event()
    journal_paths = [run_root / "progress" / f"worker-{worker_id}.jsonl" for worker_id in range(args.workers)]
    projection = ProjectionWriterProcess(
        run_root / "projection", batch_size=512, journal_paths=journal_paths
    )
    projection.start()
    processes = []
    for worker_id in range(args.workers):
        journal_path = journal_paths[worker_id]
        result_path = run_root / "workers" / f"worker-{worker_id}.json"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.parent.mkdir(parents=True, exist_ok=True)
        process = context.Process(
            target=_worker_process,
            args=(
                str(Path(args.source_root).resolve()),
                worker_id,
                partition_map.partitions_for_worker(worker_id),
                str(spool_root),
                str(credits_root),
                route_config,
                str(journal_path),
                str(result_path),
                args.workers,
                feeder_done,
                stop_event,
            ),
            name=f"mc-vnext-stream-worker-{worker_id}",
            daemon=True,
        )
        process.start()
        processes.append(process)

    started = time.monotonic()
    samples: list[dict] = []
    feed_stats = Counter()
    offsets = {campaign_id: 0 for campaign_id in campaign_ids}
    remaining = {campaign_id: count for campaign_id, count in zip(campaign_ids, counts)}
    steady_deadline = (
        time.monotonic() + max(1.0, float(args.steady_seconds))
        if args.steady_seconds > 0 else None
    )
    feed_stopped_at = None

    async def feed() -> None:
        while (
            (steady_deadline is not None and time.monotonic() < steady_deadline)
            or (steady_deadline is None and any(value > 0 for value in remaining.values()))
        ):
            progress = False
            while int(spool.lightweight_snapshot().get("pending_records", 0) or 0) >= args.max_pending_records:
                feed_stats["backpressure_waits"] += 1
                await asyncio.sleep(0.5)
                if steady_deadline is not None and time.monotonic() >= steady_deadline:
                    break
            for campaign_id in campaign_ids:
                if steady_deadline is None and remaining[campaign_id] <= 0:
                    continue
                if steady_deadline is not None and time.monotonic() >= steady_deadline:
                    break
                progress = True
                batch_count = (
                    args.batch_size if steady_deadline is not None
                    else min(args.batch_size, remaining[campaign_id])
                )
                offset = offsets[campaign_id]
                long_message = campaign_id in multipart_campaigns
                rows = []
                for index in range(offset, offset + batch_count):
                    message = f"VNext stream campaign={campaign_id} task={index}"
                    if long_message:
                        message += " " + ("MULTIPART-" * 24)
                    rows.append({
                        "msisdn": f"090{campaign_id:02d}{index:07d}",
                        "message": message,
                    })
                result = ingress.enqueue_chunk(
                    rows,
                    campaign_type="batch",
                    campaign_id=campaign_id,
                    campaign_generation=1,
                    route_id=1,
                    phone_prefix="251",
                    source_id=f"stream-{campaign_id}-{offset}",
                    payload_ref=f"synthetic-stream:{campaign_id}:{offset}",
                    row_ordinal_start=offset,
                )
                if result.accepted != batch_count:
                    raise RuntimeError(
                        f"ingress accepted {result.accepted}/{batch_count} for campaign {campaign_id}: {result.as_dict()}"
                    )
                offsets[campaign_id] += batch_count
                if steady_deadline is None:
                    remaining[campaign_id] -= batch_count
                feed_stats["batches"] += 1
                feed_stats[f"campaign_{campaign_id}_accepted"] += result.accepted
                await asyncio.sleep(args.feed_interval)
            if not progress:
                break
        feeder_done.set()

    feed_task = asyncio.create_task(feed())
    guard_reason = ""
    try:
        next_sample = 0.0
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= next_sample:
                sample = _sample(
                    run_root=run_root,
                    process_roots=[os.getpid()]
                    + [process.pid for process in processes if process.pid]
                    + [int(getattr(projection, "pid", 0) or 0)],
                    spool=spool,
                    projection=projection,
                    smsc=smsc,
                    started=started,
                )
                samples.append(sample)
                next_sample += max(0.1, args.sample_interval)
                if sample["run_bytes"] > args.max_run_bytes:
                    guard_reason = "run_disk_guard_exceeded"
                    break
                if sample["rss_kib_sum"] > args.max_rss_kib:
                    guard_reason = "rss_guard_exceeded"
                    break
            if feed_task.done():
                await feed_task
                if feed_stopped_at is None:
                    feed_stopped_at = time.monotonic()
                status = spool.lightweight_snapshot()
                if int(status.get("pending_records", 0) or 0) == 0:
                    break
            await asyncio.sleep(0.1)
    finally:
        if not feed_task.done():
            feed_task.cancel()
            feeder_done.set()
        if guard_reason:
            stop_event.set()
        else:
            stop_event.set()
        for process in processes:
            await asyncio.to_thread(process.join, 15)
        for process in processes:
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 5)
        await smsc.stop()
        projection.stop(timeout=300)

    progress = []
    for journal_path in journal_paths:
        progress.extend(ProgressJournal(journal_path).read())
    task_events = [item for item in progress if not str(item.get("state", "")).startswith("submit_sm_resp_")]
    socket_events = [item for item in task_events if item.get("state") == "socket_written"]
    response_events = [item for item in progress if item.get("state") == "submit_sm_resp_success"]
    campaign_tasks = Counter(int(item.get("campaign_id") or 0) for item in task_events)
    campaign_socket_tasks = Counter(int(item.get("campaign_id") or 0) for item in socket_events)
    campaign_pdus = Counter()
    for item in socket_events:
        campaign_pdus[int(item.get("campaign_id") or 0)] += int(
            item.get("parts_written") or item.get("parts_total") or 1
        )
    received_campaign_pdus = Counter(getattr(smsc, "campaign_pdus", Counter()))
    smpp_total = int(getattr(smsc, "total_received", 0))
    destination_counts = Counter(getattr(smsc, "destination_counts", Counter()))
    duplicate_destination_pdu_count = sum(value - 1 for value in destination_counts.values() if value > 1)
    spool_status = spool.status()
    projection_status = projection.snapshot()
    journal_bytes = sum(path.stat().st_size for path in journal_paths if path.is_file())
    worker_results = []
    for process_index in range(args.workers):
        result_path = run_root / "workers" / f"worker-{process_index}.json"
        if result_path.is_file():
            worker_results.append(json.loads(result_path.read_text(encoding="utf-8")))
    expected_campaigns = {
        str(campaign_id): {
            "tasks": int(feed_stats[f"campaign_{campaign_id}_accepted"]),
            "socket_written_tasks": int(campaign_socket_tasks[campaign_id]),
            "pdu_written": int(campaign_pdus[campaign_id]),
            "smpp_pdu_received": int(received_campaign_pdus[campaign_id]),
        }
        for campaign_id, count in zip(campaign_ids, counts)
    }
    generated_tasks = sum(
        int(feed_stats[f"campaign_{campaign_id}_accepted"])
        for campaign_id in campaign_ids
    )
    finite_complete = args.steady_seconds <= 0 and generated_tasks == total_tasks
    steady_complete = (
        args.steady_seconds > 0
        and (time.monotonic() - started) >= float(args.steady_seconds)
        and generated_tasks > 0
    )
    passed = (
        not guard_reason
        and not smsc.errors
        and (finite_complete or steady_complete)
        and len(task_events) == generated_tasks
        and len(socket_events) == generated_tasks
        # submit_sm_resp_success is a transport-level counter in the loopback
        # server; the frozen worker journal intentionally contains one task
        # event, not one event per multipart response PDU.
        and smsc.response_success == smpp_total
        and smpp_total == sum(campaign_pdus.values())
        and smsc.max_active_binds <= args.pool_size
        and not spool_status.get("pending_records")
        and int(projection_status.get("applied", 0) or 0) == len(progress)
        and all(item["socket_written_tasks"] == item["tasks"] for item in expected_campaigns.values())
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "failure_reason": guard_reason or (smsc.errors[0] if smsc.errors else ""),
        "source_root": str(Path(args.source_root).resolve()),
        "run_root": str(run_root),
        "started_epoch": time.time() - (time.monotonic() - started),
        "duration_seconds": round(time.monotonic() - started, 3),
        "workers": args.workers,
        "pool_size": args.pool_size,
        "route_tps": args.route_tps,
        "tps_scope": args.tps_scope,
        "configured_effective_total_tps": args.route_tps * args.pool_size if args.tps_scope == "per_bind" else args.route_tps,
        "window_size": args.window_size,
        "counts": counts,
        "total_tasks": generated_tasks,
        "accepted_message_count": generated_tasks,
        "socket_written_task_count": len(socket_events),
        "socket_written_pdu_count": sum(campaign_pdus.values()),
        "submit_sm_resp_success": smsc.response_success,
        "projection_events_applied": int(projection_status.get("applied", 0) or 0),
        "projection_lag_seconds": float(projection_status.get("projection_lag_seconds", 0.0) or 0.0),
        "journal_bytes": journal_bytes,
        "drain_seconds": round(
            max(0.0, (time.monotonic() - feed_stopped_at))
            if feed_stopped_at is not None else 0.0, 3
        ),
        "steady_seconds_requested": float(args.steady_seconds),
        "producer": {
            "batches": int(feed_stats["batches"]),
            "backpressure_waits": int(feed_stats["backpressure_waits"]),
            "counts_by_campaign": {
                str(campaign_id): int(feed_stats[f"campaign_{campaign_id}_accepted"])
                for campaign_id in campaign_ids
            },
        },
        "spool": spool_status,
        "smpp": {
            "binds": smsc.binds,
            "active_binds": smsc.active_binds,
            "max_active_binds": smsc.max_active_binds,
            "submit_sm_pdu_received": smpp_total,
            "submit_sm_resp_success": smsc.response_success,
            "duplicate_destination_pdu_count": duplicate_destination_pdu_count,
            "destination_tracking_truncated": bool(smsc.destination_tracking_truncated),
            "errors": smsc.errors,
        },
        "campaigns": expected_campaigns,
        "projection": projection_status,
        "worker_results": worker_results,
        "resource_samples": samples,
        "guard": {
            "max_run_bytes": args.max_run_bytes,
            "max_rss_kib": args.max_rss_kib,
            "max_pending_records": args.max_pending_records,
            "sample_interval_seconds": args.sample_interval,
            "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "nice": os.getpriority(os.PRIO_PROCESS, 0) if hasattr(os, "getpriority") else None,
        },
        "execution_mode": "bounded_streaming_feeder_concurrent_worker_journal_projection",
        "test_harness_scope": "synthetic_loopback_only_not_production_api",
    }


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--counts", default="20000,20000,5000,5000")
    parser.add_argument("--multipart-campaigns", default="",
                        help="campaign ids for the separate multipart experiment")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--route-tps", type=float, default=200.0)
    parser.add_argument("--tps-scope", choices=("route_shared", "per_bind"), default="per_bind")
    parser.add_argument("--window-size", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--feed-interval", type=float, default=0.05)
    parser.add_argument("--max-pending-records", type=int, default=20000)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--steady-seconds", type=float, default=0.0,
                        help="keep the bounded producer active for this duration before drain")
    parser.add_argument("--max-run-bytes", type=int, default=5 * 1024 * 1024 * 1024)
    parser.add_argument("--max-rss-kib", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--spool-max-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    args.workers = max(1, min(8, args.workers))
    args.pool_size = max(1, min(20, args.pool_size))
    args.window_size = max(1, min(1000, args.window_size))
    result = asyncio.run(run(args))
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "status": result["status"],
        "duration_seconds": result["duration_seconds"],
        "total_tasks": result["total_tasks"],
        "accepted_message_count": result["accepted_message_count"],
        "socket_written_task_count": result["socket_written_task_count"],
        "socket_written_pdu_count": result["socket_written_pdu_count"],
        "submit_sm_resp_success": result["submit_sm_resp_success"],
        "projection_events_applied": result["projection_events_applied"],
        "max_active_binds": result["smpp"]["max_active_binds"],
        "failure_reason": result["failure_reason"],
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
