"""Bounded, loopback-only streaming acceptance harness for VNext.

This is a test-run artifact.  It reuses the frozen VNext ingress, spool,
worker, SMPP transport, Journal, and single ProjectionWriter implementation;
the only extra behavior here is a bounded multi-campaign feeder and metrics
collection for T3/T4.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import multiprocessing
import os
from collections import Counter
from pathlib import Path
import shutil
import subprocess
import time


RESOURCE_PROFILE_RSS_KIB = {
    # LOW_MEM_2G retains the historical harness-inclusive guard for fault
    # injection.  Production profiles guard application-only RSS so the
    # synthetic feeder's bounded bookkeeping is reported separately.
    "LOW_MEM_2G": 2 * 1024 * 1024,
    "PROD_8G": 6 * 1024 * 1024,
    "PROD_16G": 12 * 1024 * 1024,
}


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


def _journal_bytes(journal_paths: list[Path]) -> int:
    total = 0
    for path in journal_paths:
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def _projection_caught_up(projection, journal_paths: list[Path]) -> bool:
    """Require the writer to commit the current journal EOF before stopping.

    A zero ``projection_lag_seconds`` only says that the last applied event was
    recent.  It does not prove that the writer has consumed the journal tail.
    The parent therefore compares the live journal bytes with the writer's
    committed offset in addition to checking the writer's backlog counters.
    """

    try:
        status = projection.snapshot()
    except Exception:
        return False
    committed_offset = status.get("journal_committed_offset_bytes")
    if committed_offset is None:
        return False
    try:
        committed_offset = int(committed_offset)
    except (TypeError, ValueError):
        return False
    return (
        _journal_bytes(journal_paths) <= committed_offset
        and int(status.get("journal_backlog_bytes", 0) or 0) <= 0
        and int(status.get("journal_backlog_events_estimate", 0) or 0) <= 0
        and int(status.get("journal_pending_events", 0) or 0) <= 0
        and int(status.get("queue_depth", 0) or 0) <= 0
    )


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

    def __init__(self, smpp, *, max_destination_keys: int = 100_000,
                 response_delay_seconds: float = 0.0):
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
        self.response_delay_seconds = max(0.0, float(response_delay_seconds))

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
                    if self.response_delay_seconds:
                        await asyncio.sleep(self.response_delay_seconds)
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

    result = await worker.drain_transports(timeout=timeout)
    if not result.get("drained"):
        raise TimeoutError(
            f"SMPP response drain exceeded {timeout:.1f}s; "
            f"pending={int(result.get('pending', 0) or 0)}"
        )


def _release_completed_worker_caches(spool, worker) -> None:
    """Release reader-only caches after a completed worker pass.

    Ingress owns the authoritative spool index.  A worker only needs records
    for its current bounded pass; retaining every decoded record and every
    completed destination lock in each spawned worker multiplies memory by
    the worker count during a long steady run.
    """

    release = getattr(worker, "release_reader_caches", None)
    if release:
        release()
        return
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
    profile_root: str = "",
    profile_interval_seconds: float = 1.0,
) -> None:
    _install_source(source_root)
    from vnext.critical_path_profiler import CriticalPathProfiler
    from vnext.projection_writer import ProgressJournal
    from vnext.route_credits import RouteCreditBroker
    from vnext.spool import TaskSpool
    from vnext.worker import ShardWorker

    spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=512 * 1024 * 1024)
    credits = RouteCreditBroker(credits_root)
    profiler = CriticalPathProfiler(
        profile_root,
        f"worker-{int(worker_id)}",
        interval_seconds=profile_interval_seconds,
    )
    journal = ProgressJournal(journal_path, profiler=profiler)
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
        profiler=profiler,
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
    profiler.publish(force=True)
    result["process_pid"] = os.getpid()
    result["process_role"] = "stream-worker"
    if profiler.enabled:
        result["critical_path"] = profiler.snapshot()
    Path(result_path).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


def _sample(
    *, run_root: Path, process_roots: list[int], application_roots: list[int] | None = None,
    process_roles: dict | None = None, spool, projection, smsc, started: float,
    profile_root: Path | None = None,
) -> dict:
    application_roots = list(application_roots or process_roots)
    harness_cpu, harness_rss_kib, harness_process_count = _process_tree_metrics(process_roots)
    application_cpu, application_rss_kib, application_process_count = _process_tree_metrics(
        application_roots
    )
    try:
        projection_status = projection.snapshot()
    except Exception as exc:  # pragma: no cover - diagnostic path
        projection_status = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    spool_status = spool.lightweight_snapshot()
    return {
        "at_seconds": round(time.monotonic() - started, 3),
        # Backward-compatible aliases retain the historical harness-inclusive
        # values; the explicit fields are used for production profile gates.
        "cpu_percent_sum": harness_cpu,
        "rss_kib_sum": harness_rss_kib,
        "process_count": harness_process_count,
        "harness_cpu_percent": harness_cpu,
        "harness_rss_kib": harness_rss_kib,
        "harness_process_count": harness_process_count,
        "application_cpu_percent": application_cpu,
        "application_rss_kib": application_rss_kib,
        "application_process_count": application_process_count,
        "process_roles": process_roles or {},
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
        "projection_journal_committed_offset_bytes": int(
            projection_status.get("journal_committed_offset_bytes", 0) or 0
        ),
        "projection_journal_backlog_bytes": int(
            projection_status.get("journal_backlog_bytes", 0) or 0
        ),
        "projection_journal_backlog_events_estimate": int(
            projection_status.get("journal_backlog_events_estimate", 0) or 0
        ),
        "smpp_received": int(getattr(smsc, "total_received", 0)),
        "smpp_response_success": smsc.response_success,
    }


def _read_critical_path_profiles(profile_root: Path | None) -> list[dict]:
    if profile_root is None:
        return []
    values = []
    for path in sorted((profile_root / "profiles").glob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        if isinstance(value, dict):
            values.append(value)
    return values


def _aggregate_critical_path_profiles(profile_root: Path | None) -> dict:
    profiles = _read_critical_path_profiles(profile_root)
    counters: Counter[str] = Counter()
    gauges: dict[str, object] = {}
    timestamps: dict[str, float] = {}
    duration_values: dict[str, list[dict]] = {}
    for profile in profiles:
        counters.update({
            str(key): int(value or 0)
            for key, value in (profile.get("counters") or {}).items()
            if isinstance(value, (int, float))
        })
        gauges.update(profile.get("gauges") or {})
        for key, value in (profile.get("timestamps") or {}).items():
            try:
                timestamps[str(key)] = max(timestamps.get(str(key), 0.0), float(value))
            except (TypeError, ValueError):
                continue
        for name, value in (profile.get("durations") or {}).items():
            if isinstance(value, dict):
                duration_values.setdefault(str(name), []).append(value)
    durations = {}
    for name, values in duration_values.items():
        durations[name] = {
            "count": int(counters.get(f"{name}_count", 0)),
            "p50_ms_max": max(float(item.get("p50_ms", 0.0) or 0.0) for item in values),
            "p95_ms_max": max(float(item.get("p95_ms", 0.0) or 0.0) for item in values),
            "p99_ms_max": max(float(item.get("p99_ms", 0.0) or 0.0) for item in values),
            "max_ms_max": max(float(item.get("max_ms", 0.0) or 0.0) for item in values),
            "total_ns": int(counters.get(f"{name}_total_ns", 0)),
        }
    return {
        "enabled": bool(profiles),
        "profile_count": len(profiles),
        "roles": [str(item.get("role") or "") for item in profiles],
        "counters": dict(sorted(counters.items())),
        "gauges": dict(sorted(gauges.items())),
        "timestamps": dict(sorted(timestamps.items())),
        "durations": dict(sorted(durations.items())),
        "profiles": profiles,
    }


def _profile_sample_row(sample: dict) -> dict:
    critical = sample.get("critical_path") or {}
    counters = critical.get("counters") or {}
    durations = critical.get("durations") or {}

    def duration_p95(name: str):
        value = (durations.get(name) or {}).get("p95_ms_max")
        return round(float(value), 3) if value is not None else ""

    return {
        "at_seconds": sample.get("at_seconds", 0.0),
        "socket_written_pdu": counters.get("socket_written_pdu", 0),
        "response_pdus_received": counters.get("response_pdus_received", 0),
        "journal_events_durable": counters.get("journal_events_durable", 0),
        "projection_committed_events": sample.get("projection_applied", 0),
        "journal_backlog_bytes": sample.get("projection_journal_backlog_bytes", 0),
        "journal_backlog_events_estimate": sample.get("projection_journal_backlog_events_estimate", 0),
        "response_callback_p95_ms": duration_p95("response_callback"),
        "journal_append_p95_ms": duration_p95("journal_append"),
        "journal_fsync_p95_ms": duration_p95("journal_fsync"),
        "tps_credit_wait_p95_ms": duration_p95("tps_credit_wait"),
        "response_window_wait_p95_ms": duration_p95("response_window_wait"),
        "socket_write_drain_p95_ms": duration_p95("socket_write_drain"),
        "projection_status_write_p95_ms": duration_p95("process_status_write"),
    }


def _write_critical_path_samples(run_root: Path, samples: list[dict]) -> str:
    path = run_root / "critical-path-samples.csv"
    fields = list(_profile_sample_row(samples[0]).keys()) if samples else ["at_seconds"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for sample in samples:
            writer.writerow(_profile_sample_row(sample))
    return str(path)


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
    profile_root = Path(args.profile_root).resolve() if args.profile_root else None
    if profile_root:
        profile_root.mkdir(parents=True, exist_ok=True)
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
    smsc = CountingLoopbackSMSC(
        smpp, response_delay_seconds=max(0.0, float(args.response_delay_ms)) / 1000.0
    )
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
    projection_kwargs = {
        "batch_size": args.projection_batch_size,
        "journal_paths": journal_paths,
        "staging_mode": args.projection_staging_mode,
    }
    if profile_root:
        projection_kwargs.update({
            "profile_root": str(profile_root),
            "profile_interval_seconds": args.profile_interval,
        })
    try:
        projection = ProjectionWriterProcess(
            run_root / "projection",
            **projection_kwargs,
            status_interval_seconds=args.projection_status_interval,
        )
    except TypeError as exc:
        # The frozen f4ab3373 baseline predates the bounded status interval.
        # Keep the harness identical while allowing that source tree to run
        # with its historical per-loop status behavior.
        if "status_interval_seconds" not in str(exc):
            raise
        projection = ProjectionWriterProcess(run_root / "projection", **projection_kwargs)
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
                str(args.profile_root) if args.profile_root else "",
                args.profile_interval,
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
    smpp_drained_at = None
    projection_caught_up_at = None
    projection_drain_deadline = None

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
                    application_roots=[process.pid for process in processes if process.pid]
                    + [int(getattr(projection, "pid", 0) or 0)],
                    process_roles={
                        "harness": [os.getpid()],
                        "workers": [process.pid for process in processes if process.pid],
                        "projection_writer": [int(getattr(projection, "pid", 0) or 0)],
                    },
                    spool=spool,
                    projection=projection,
                    smsc=smsc,
                    started=started,
                    profile_root=profile_root,
                )
                sample["critical_path"] = _aggregate_critical_path_profiles(profile_root)
                samples.append(sample)
                next_sample += max(0.1, args.sample_interval)
                if sample["run_bytes"] > args.max_run_bytes:
                    guard_reason = "run_disk_guard_exceeded"
                    break
                guard_rss_kib = (
                    sample["application_rss_kib"]
                    if args.rss_guard_scope == "application"
                    else sample["harness_rss_kib"]
                )
                if guard_rss_kib > args.max_rss_kib:
                    guard_reason = "rss_guard_exceeded"
                    break
            if feed_task.done():
                await feed_task
                if feed_stopped_at is None:
                    feed_stopped_at = time.monotonic()
                    projection_drain_deadline = (
                        feed_stopped_at + max(1.0, float(args.projection_drain_timeout))
                    )
                status = spool.lightweight_snapshot()
                if int(status.get("pending_records", 0) or 0) == 0:
                    if (
                        smpp_drained_at is None
                        and smsc.total_received > 0
                        and smsc.response_success == smsc.total_received
                    ):
                        smpp_drained_at = time.monotonic()
                    if _projection_caught_up(projection, journal_paths):
                        if projection_caught_up_at is None:
                            projection_caught_up_at = time.monotonic()
                        elif time.monotonic() - projection_caught_up_at >= 0.5:
                            break
                    else:
                        projection_caught_up_at = None
                else:
                    projection_caught_up_at = None
                if (
                    projection_drain_deadline is not None
                    and time.monotonic() >= projection_drain_deadline
                ):
                    guard_reason = "projection_drain_timeout"
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
    write_failed_events = [item for item in task_events if item.get("state") == "write_failed"]
    response_events = [item for item in progress if ":resp:" in str(item.get("event_id", ""))]
    response_success_events = [
        item for item in response_events if item.get("state") == "submit_sm_resp_success"
    ]
    response_unknown_events = [item for item in response_events if item.get("state") == "unknown"]
    campaign_tasks = Counter(int(item.get("campaign_id") or 0) for item in task_events)
    campaign_socket_tasks = Counter(int(item.get("campaign_id") or 0) for item in socket_events)
    campaign_write_failed = Counter(int(item.get("campaign_id") or 0) for item in write_failed_events)
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
    journal_bytes = sum(
        path.stat().st_size
        for path in run_root.rglob("worker-*.jsonl")
        if path.is_file()
    )
    worker_results = []
    for process_index in range(args.workers):
        result_path = run_root / "workers" / f"worker-{process_index}.json"
        if result_path.is_file():
            worker_results.append(json.loads(result_path.read_text(encoding="utf-8")))
    expected_campaigns = {
        str(campaign_id): {
            "tasks": int(feed_stats[f"campaign_{campaign_id}_accepted"]),
            "socket_written_tasks": int(campaign_socket_tasks[campaign_id]),
            "write_failed_tasks": int(campaign_write_failed[campaign_id]),
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
    failure_reason = guard_reason or (smsc.errors[0] if smsc.errors else "")
    if not failure_reason and write_failed_events:
        failure_reason = "task_write_failed"
    if not failure_reason and len(response_events) != smpp_total:
        failure_reason = "response_event_count_mismatch"
    passed = (
        not guard_reason
        and not smsc.errors
        and (finite_complete or steady_complete)
        and len(task_events) == generated_tasks
        and len(socket_events) == generated_tasks
        and smsc.response_success == smpp_total
        and smpp_total == sum(campaign_pdus.values())
        and len(response_events) == smpp_total
        and len(response_success_events) == smsc.response_success
        and not response_unknown_events
        and not write_failed_events
        and smsc.max_active_binds <= args.pool_size
        and not spool_status.get("pending_records")
        and int(projection_status.get("applied", 0) or 0) == len(progress)
        and all(item["socket_written_tasks"] == item["tasks"] for item in expected_campaigns.values())
    )
    samples_sorted = sorted(samples, key=lambda item: float(item.get("at_seconds", 0.0)))
    critical_path = _aggregate_critical_path_profiles(profile_root)
    critical_path_samples_path = (
        _write_critical_path_samples(run_root, samples) if profile_root else None
    )

    def _peak(key: str) -> int:
        return max((int(item.get(key, 0) or 0) for item in samples_sorted), default=0)

    def _slope_last_180(key: str) -> float:
        if not samples_sorted:
            return 0.0
        end = samples_sorted[-1]
        start = next(
            (item for item in reversed(samples_sorted)
             if float(end.get("at_seconds", 0.0)) - float(item.get("at_seconds", 0.0)) >= 180.0),
            samples_sorted[0],
        )
        seconds = float(end.get("at_seconds", 0.0)) - float(start.get("at_seconds", 0.0))
        if seconds <= 0:
            return 0.0
        return round((float(end.get(key, 0) or 0) - float(start.get(key, 0) or 0)) / seconds, 3)

    def _counter_value(sample: dict, name: str) -> int:
        return int(((sample.get("critical_path") or {}).get("counters") or {}).get(name, 0) or 0)

    def _rate_summary(value_getter) -> dict:
        rates = []
        previous = None
        for sample in samples_sorted:
            if previous is not None:
                seconds = float(sample.get("at_seconds", 0.0)) - float(previous.get("at_seconds", 0.0))
                if seconds > 0:
                    rates.append(
                        max(0.0, (float(value_getter(sample)) - float(value_getter(previous))) / seconds)
                    )
            previous = sample
        if not rates:
            return {"sample_count": 0, "p50": None, "p95": None, "max": None}
        ordered = sorted(rates)

        def percentile(percentile: float) -> float:
            index = (len(ordered) - 1) * percentile
            lower = int(index)
            upper = min(len(ordered) - 1, lower + 1)
            fraction = index - lower
            return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)

        return {
            "sample_count": len(rates),
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "max": round(max(ordered), 3),
        }

    timeline_timestamps = critical_path.get("timestamps") or {}

    def _timeline_value(name: str):
        value = timeline_timestamps.get(name)
        return round(float(value), 6) if value is not None else None

    def _after_feed_stop(name: str):
        value = timeline_timestamps.get(name)
        if value is None or feed_stopped_at is None:
            return None
        return round(max(0.0, float(value) - float(feed_stopped_at)), 3)

    producer_active_seconds = (
        round(max(0.0, feed_stopped_at - started), 3) if feed_stopped_at is not None else None
    )
    smpp_drain_seconds = (
        round(max(0.0, smpp_drained_at - feed_stopped_at), 3)
        if feed_stopped_at is not None and smpp_drained_at is not None else None
    )
    projection_drain_seconds = (
        round(max(0.0, projection_caught_up_at - feed_stopped_at), 3)
        if feed_stopped_at is not None and projection_caught_up_at is not None else None
    )
    last_timestamps = {
        "feed_start_monotonic": round(started, 6),
        "feed_stop_monotonic": round(feed_stopped_at, 6) if feed_stopped_at is not None else None,
        "last_task_taken_monotonic": _timeline_value("last_task_taken_monotonic"),
        "last_socket_write_monotonic": _timeline_value("last_socket_write_monotonic"),
        "last_smpp_response_received_monotonic": _timeline_value("last_smpp_response_received_monotonic"),
        "last_response_journal_durable_monotonic": _timeline_value("last_response_journal_durable_monotonic"),
        "last_projection_db_commit_monotonic": _timeline_value("last_projection_db_commit_monotonic"),
        "last_projection_offset_durable_monotonic": _timeline_value("last_projection_offset_durable_monotonic"),
    }
    return {
        "status": "PASS" if passed else "FAIL",
        "failure_reason": failure_reason,
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
        "projection_batch_size": args.projection_batch_size,
        "projection_staging_mode": args.projection_staging_mode,
        "projection_status_interval_seconds": args.projection_status_interval,
        "resource_profile": args.resource_profile,
        "rss_guard_scope": args.rss_guard_scope,
        "response_delay_ms": float(args.response_delay_ms),
        "counts": counts,
        "total_tasks": generated_tasks,
        "accepted_message_count": generated_tasks,
        "socket_written_task_count": len(socket_events),
        "write_failed_task_count": len(write_failed_events),
        "socket_written_pdu_count": sum(campaign_pdus.values()),
        "journal_event_count": len(progress),
        "journal_expected_event_count": len(progress),
        "response_event_count": len(response_events),
        "durable_response_event_count": len(response_events),
        "response_success_event_count": len(response_success_events),
        "response_unknown_event_count": len(response_unknown_events),
        "events_per_task": round(len(progress) / generated_tasks, 6) if generated_tasks else 0.0,
        "events_per_pdu": round(len(progress) / sum(campaign_pdus.values()), 6) if sum(campaign_pdus.values()) else 0.0,
        "submit_sm_resp_success": smsc.response_success,
        "projection_events_applied": int(projection_status.get("applied", 0) or 0),
        "duckdb_unique_event_rows": int(
            (projection_status.get("metrics") or {}).get("events_new", 0) or 0
        ),
        "projection_lag_seconds": float(projection_status.get("projection_lag_seconds", 0.0) or 0.0),
        "projection_caught_up_before_stop": projection_caught_up_at is not None,
        "projection_drain_timeout_seconds": float(args.projection_drain_timeout),
        "journal_bytes": journal_bytes,
        "producer_active_seconds": producer_active_seconds,
        "smpp_drain_seconds": smpp_drain_seconds,
        "projection_drain_seconds": projection_drain_seconds,
        "drain_seconds": projection_drain_seconds,
        "critical_path_timeline": {
            **last_timestamps,
            "feed_active_seconds": producer_active_seconds,
            "pending_send_drain_seconds": _after_feed_stop("last_socket_write_monotonic"),
            "smpp_response_drain_seconds": _after_feed_stop("last_smpp_response_received_monotonic"),
            "response_journal_drain_seconds": _after_feed_stop("last_response_journal_durable_monotonic"),
            "projection_commit_drain_seconds": _after_feed_stop("last_projection_db_commit_monotonic"),
            "offset_durable_drain_seconds": _after_feed_stop("last_projection_offset_durable_monotonic"),
            "journal_backlog_bytes_timeline": [
                {
                    "at_seconds": item.get("at_seconds"),
                    "bytes": item.get("projection_journal_backlog_bytes"),
                }
                for item in samples_sorted
            ],
            "journal_backlog_events_estimate_timeline": [
                {
                    "at_seconds": item.get("at_seconds"),
                    "events": item.get("projection_journal_backlog_events_estimate"),
                }
                for item in samples_sorted
            ],
        },
        "critical_path_rates": {
            "socket_written_pdu_per_second": _rate_summary(
                lambda item: _counter_value(item, "socket_written_pdu")
            ),
            "response_received_pdu_per_second": _rate_summary(
                lambda item: _counter_value(item, "response_pdus_received")
            ),
            "journal_durable_events_per_second": _rate_summary(
                lambda item: _counter_value(item, "journal_events_durable")
            ),
            "projection_committed_events_per_second": _rate_summary(
                lambda item: item.get("projection_applied", 0)
            ),
        },
        "critical_path": critical_path,
        "critical_path_samples_csv": critical_path_samples_path,
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
            "submit_sm_resp_error": int(
                len([item for item in response_events if item.get("state") == "submit_sm_resp_error"])
            ),
            "duplicate_destination_pdu_count": duplicate_destination_pdu_count,
            "destination_tracking_truncated": bool(smsc.destination_tracking_truncated),
            "errors": smsc.errors,
        },
        "campaigns": expected_campaigns,
        "projection": projection_status,
        "worker_results": worker_results,
        "resource_samples": samples,
        "resource_peaks": {
            "application_rss_kib": _peak("application_rss_kib"),
            "harness_rss_kib": _peak("harness_rss_kib"),
            "application_rss_slope_last_180_kib_per_second": _slope_last_180(
                "application_rss_kib"
            ),
            "harness_rss_slope_last_180_kib_per_second": _slope_last_180(
                "harness_rss_kib"
            ),
        },
        "guard": {
            "max_run_bytes": args.max_run_bytes,
            "max_rss_kib": args.max_rss_kib,
            "max_pending_records": args.max_pending_records,
            "sample_interval_seconds": args.sample_interval,
            "resource_profile": args.resource_profile,
            "rss_guard_scope": args.rss_guard_scope,
            "application_rss_limit_kib": args.max_rss_kib
                if args.rss_guard_scope == "application" else None,
            "harness_rss_limit_kib": args.max_rss_kib
                if args.rss_guard_scope == "harness" else None,
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
    parser.add_argument("--projection-batch-size", type=int, default=512)
    parser.add_argument("--projection-staging-mode", choices=("vectorized", "executemany"), default="vectorized")
    parser.add_argument("--projection-status-interval", type=float, default=1.0,
                        help="status/diagnostic write interval in seconds")
    parser.add_argument("--instrumentation", choices=("off", "on"), default="off",
                        help="enable bounded critical-path profiles and per-second CSV")
    parser.add_argument("--profile-root", default="",
                        help="profile output root; defaults to <run-root>/critical-path when enabled")
    parser.add_argument("--profile-interval", type=float, default=1.0,
                        help="maximum profile snapshot frequency in seconds")
    parser.add_argument("--response-delay-ms", type=float, default=0.0,
                        help="delay each loopback submit_sm_resp to exercise response draining")
    parser.add_argument("--feed-interval", type=float, default=0.05)
    parser.add_argument("--max-pending-records", type=int, default=20000)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--steady-seconds", type=float, default=0.0,
                        help="keep the bounded producer active for this duration before drain")
    parser.add_argument("--projection-drain-timeout", type=float, default=600.0,
                        help="maximum seconds to wait for journal EOF catch-up after feed stop")
    parser.add_argument("--max-run-bytes", type=int, default=5 * 1024 * 1024 * 1024)
    parser.add_argument("--resource-profile", choices=tuple(RESOURCE_PROFILE_RSS_KIB),
                        default="LOW_MEM_2G")
    parser.add_argument("--rss-guard-scope", choices=("application", "harness"), default="")
    parser.add_argument("--max-rss-kib", type=int, default=None)
    parser.add_argument("--spool-max-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    args.workers = max(1, min(8, args.workers))
    args.pool_size = max(1, min(20, args.pool_size))
    args.window_size = max(1, min(1000, args.window_size))
    args.projection_batch_size = max(1, min(4096, args.projection_batch_size))
    args.projection_status_interval = min(60.0, max(0.1, float(args.projection_status_interval)))
    args.profile_interval = min(60.0, max(0.1, float(args.profile_interval)))
    if args.instrumentation == "on" and not args.profile_root:
        args.profile_root = str(Path(args.run_root).resolve() / "critical-path")
    elif args.profile_root:
        args.profile_root = str(Path(args.profile_root).resolve())
    args.projection_drain_timeout = max(1.0, min(3600.0, args.projection_drain_timeout))
    args.resource_profile = str(args.resource_profile).upper()
    if args.max_rss_kib is None:
        args.max_rss_kib = RESOURCE_PROFILE_RSS_KIB[args.resource_profile]
    if not args.rss_guard_scope:
        args.rss_guard_scope = "harness" if args.resource_profile == "LOW_MEM_2G" else "application"
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
        "journal_event_count": result["journal_event_count"],
        "response_event_count": result["response_event_count"],
        "response_unknown_event_count": result["response_unknown_event_count"],
        "submit_sm_resp_success": result["submit_sm_resp_success"],
        "projection_events_applied": result["projection_events_applied"],
        "max_active_binds": result["smpp"]["max_active_binds"],
        "failure_reason": result["failure_reason"],
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
