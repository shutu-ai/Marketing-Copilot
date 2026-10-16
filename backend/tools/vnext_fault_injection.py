"""Bounded VNext fault-injection checks for crash, Writer pause and control."""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import time

from pathlib import Path as _Path
import sys

BACKEND_DIR = _Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.projection_writer import ProjectionWriter, ProjectionWriterBusy, ProgressJournal
from vnext.smpp_transport import SocketWriteResult
from vnext.spool import SpoolRecord, TaskSpool
from vnext.worker import ShardWorker


def _record(index: int) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1, event_id=f"fault-{index}", partition_id=0, ordinal=index,
        campaign_type="batch", campaign_id=1, campaign_generation=1, route_id=1,
        normalized_msisdn=f"251900000{index:04d}", payload_ref="fault", message=f"fault-{index}",
        part_count_hint=1, source_id="fault", created_at=time.time(),
    )


class _SuccessTransport:
    async def send_task(self, **_kwargs):
        await asyncio.sleep(0)
        return SocketWriteResult("socket_written", 1, 1)

    async def close(self):
        return None

    def snapshot(self):
        return {"route_id": "1"}


def _success_factory(_config, _credits):
    return _SuccessTransport()


def _crash_worker(spool_root: str, journal_path: str, crash_after: int) -> None:
    spool = TaskSpool(spool_root, virtual_partition_count=1, max_bytes=100_000)
    journal = ProgressJournal(journal_path)
    seen = 0

    def sink(event):
        nonlocal seen
        journal.append(event)
        seen += 1
        if seen >= int(crash_after):
            os._exit(17)

    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}}, transport_factory=_success_factory,
        progress_sink=sink, max_inflight=1, prefetch_per_partition=4,
    )
    asyncio.run(worker.run_until_idle())


def run_restart(root: Path) -> dict:
    spool_root = root / "spool"
    spool = TaskSpool(spool_root, virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([_record(index) for index in range(1, 21)])
    context = multiprocessing.get_context("spawn")
    journal_path = root / "crash-progress.jsonl"
    process = context.Process(target=_crash_worker, args=(str(spool_root), str(journal_path), 5), daemon=True)
    process.start()
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
    crashed_events = ProgressJournal(journal_path).read()
    recovered_spool = TaskSpool(spool_root, virtual_partition_count=1, max_bytes=100_000)
    remaining_events = []
    worker = ShardWorker(
        0, (0,), recovered_spool, {1: {"route_id": 1}}, transport_factory=_success_factory,
        progress_sink=remaining_events.append, max_inflight=1, prefetch_per_partition=4,
    )
    asyncio.run(worker.run_until_idle())
    all_events = crashed_events + remaining_events
    event_ids = [item["event_id"] for item in all_events]
    passed = (
        process.exitcode == 17 and len(all_events) == 20
        and len(set(event_ids)) == 20
        and all(item["state"] == "socket_written" for item in all_events)
        and recovered_spool.status()["pending_records"] == 0
    )
    return {
        "scenario": "worker_crash_restart",
        "status": "PASS" if passed else "FAIL",
        "crash_exit_code": process.exitcode,
        "crashed_events": len(crashed_events),
        "recovered_events": len(remaining_events),
        "unique_events": len(set(event_ids)),
        "pending_after_recovery": recovered_spool.status()["pending_records"],
    }


def run_writer_pause(root: Path) -> dict:
    writer = ProjectionWriter(root / "projection.duckdb", batch_size=8)
    writer.start()
    events = [{
        "event_id": f"writer-{index}", "partition_id": 0, "ordinal": index,
        "campaign_type": "batch", "campaign_id": 1, "route_id": 1,
        "normalized_msisdn": f"251900000{index:04d}", "state": "socket_written",
        "parts_written": 1, "parts_total": 1, "taken": 1, "socket_written": 1,
        "created_at": time.time(),
    } for index in range(1, 21)]
    for item in events:
        writer.submit(item)
    writer.pause(0.1)
    blocked = False
    try:
        writer.flush()
    except ProjectionWriterBusy:
        blocked = True
    time.sleep(0.12)
    applied = 0
    while writer.snapshot()["queue_depth"]:
        applied += writer.flush()
    snapshot = writer.snapshot()
    writer.stop()
    passed = blocked and applied == 20 and snapshot["queue_depth"] == 0
    return {"scenario": "projection_writer_pause_resume", "status": "PASS" if passed else "FAIL", "blocked": blocked, "applied": applied, "snapshot": snapshot}


def run_control(root: Path) -> dict:
    spool = TaskSpool(root / "control-spool", virtual_partition_count=1, max_bytes=100_000)
    spool.append_many([_record(index) for index in range(1, 6)])
    progress = []
    worker = ShardWorker(
        0, (0,), spool, {1: {"route_id": 1}}, transport_factory=_success_factory,
        progress_sink=progress.append, max_inflight=2, prefetch_per_partition=4,
    )
    worker.set_campaign_state("batch", 1, "paused")
    asyncio.run(worker.run_until_idle())
    paused_count = len(progress)
    worker.set_campaign_state("batch", 1, "running")
    asyncio.run(worker.run_until_idle())
    passed = paused_count == 0 and len(progress) == 5 and all(item["state"] == "socket_written" for item in progress)
    return {"scenario": "pause_control_fence", "status": "PASS" if passed else "FAIL", "paused_writes": paused_count, "writes_after_resume": len(progress)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("restart", "writer-pause", "control"), default="restart")
    parser.add_argument("--run-root", default="")
    args = parser.parse_args()
    root = Path(args.run_root).resolve() if args.run_root else Path(tempfile.mkdtemp(prefix="mc-vnext-fault-"))
    root.mkdir(parents=True, exist_ok=True)
    result = {
        "restart": run_restart,
        "writer-pause": run_writer_pause,
        "control": run_control,
    }[args.scenario](root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
