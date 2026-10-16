"""Minimal local VNext fork/spawn diagnostic with loopback SMPP.

This tool is intentionally smaller than the real-main acceptance runner.  It
starts the runtime before online ingress, appends a second round after an idle
period, restarts the runtime, and records only bounded counters and process
metadata.  It never emits route credentials, message bodies, or MSISDNs.
"""

from __future__ import annotations

import argparse
import asyncio
import faulthandler
import json
import os
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

BACKEND_DIR = Path(os.environ.get("MC_APP_BACKEND_DIR", str(Path(__file__).resolve().parents[1]))).resolve()
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.diagnostics import read_process_metadata
from vnext.runtime import VNextRuntime
from vnext_loopback_acceptance import LoopbackSMSC


def _journal_summary(root: Path) -> dict:
    states: Counter[str] = Counter()
    event_ids: set[str] = set()
    total = 0
    for path in sorted((root / "progress").glob("worker-*.jsonl")):
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                event = json.loads(line)
                total += 1
                states[str(event.get("state") or "unknown")] += 1
                event_id = str(event.get("event_id") or "")
                if event_id:
                    event_ids.add(event_id)
        except (OSError, ValueError, TypeError):
            continue
    return {
        "events": total,
        "unique_event_ids": len(event_ids),
        "duplicate_event_ids": max(0, total - len(event_ids)),
        "states": dict(sorted(states.items())),
    }


async def _run(root: Path, *, start_method: str, count: int, cycles: int) -> dict:
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    smsc = LoopbackSMSC()
    port = await smsc.start()
    route = {
        "route_id": 1,
        "host": "127.0.0.1",
        "port": port,
        "system_id": "native-diag",
        "password": "native-diag",
        "source_addr": "NATIVE-DIAG",
        "window_size": 16,
        "tps": 0.0,
        "timeout": 2,
    }
    accepted = 0
    controls_after_restart: list[dict] = []
    lifecycle: list[dict] = []
    runtime = None
    try:
        for cycle in range(max(1, int(cycles))):
            runtime = VNextRuntime(
                root,
                {1: route},
                worker_count=2,
                virtual_partition_count=16,
                projection_queue_size=4,
                max_inflight=8,
                prefetch_per_partition=4,
                start_method=start_method,
                run_id=f"native-sigsegv-{start_method}",
            )
            runtime.start()
            rows = [
                {"msisdn": f"090000{cycle:02d}{index:04d}", "message": f"diag-{cycle}-{index}"}
                for index in range(max(1, int(count)))
            ]
            result = runtime.enqueue_chunk(
                rows,
                campaign_type="batch",
                campaign_id=9100 + cycle,
                campaign_generation=1,
                route_id=1,
                phone_prefix="251",
                source_id=f"native-diag-{cycle}",
                payload_ref=f"native-diag-{cycle}",
            )
            accepted += int(result.accepted)
            idle = await runtime.wait_for_idle(timeout=30)
            status_before_stop = runtime.status()
            lifecycle.append({
                "cycle": cycle,
                "accepted": int(result.accepted),
                "idle_pending_records": int(idle["spool"]["pending_records"]),
                "worker_pids": [item.get("pid") for item in (status_before_stop.get("supervisor") or {}).get("workers", [])],
                "projection_pid": status_before_stop.get("projection", {}).get("pid"),
                "start_method": status_before_stop.get("start_method"),
            })
            if cycle == 0:
                runtime.set_campaign_state("batch", 9199, "paused")
            runtime.stop()
            if cycle == 0:
                runtime = VNextRuntime(
                    root,
                    {1: route},
                    worker_count=2,
                    virtual_partition_count=16,
                    projection_queue_size=4,
                    max_inflight=8,
                    prefetch_per_partition=4,
                    start_method=start_method,
                    run_id=f"native-sigsegv-{start_method}",
                )
                runtime.start()
                controls_after_restart = runtime.status().get("controls", [])
                runtime.stop()
            runtime = None
    finally:
        if runtime is not None:
            runtime.stop()
        await smsc.stop()

    journal = _journal_summary(root)
    metadata = read_process_metadata(root / "diagnostics")
    socket_written = int(journal["states"].get("socket_written", 0))
    response_success = int(smsc.response_success)
    control_restored = any(
        item.get("campaign_type") == "batch"
        and int(item.get("campaign_id", 0)) == 9199
        and item.get("state") == "paused"
        for item in controls_after_restart
    )
    return {
        "status": "PASS" if (
            accepted == socket_written == len(smsc.received) == response_success
            and journal["duplicate_event_ids"] == 0
            and control_restored
        ) else "FAIL",
        "start_method": start_method,
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "python": sys.version.split()[0],
        "run_root": str(root),
        "cycles": len(lifecycle),
        "accepted": accepted,
        "smpp_received": len(smsc.received),
        "submit_sm_response_success": response_success,
        "journal": journal,
        "control_restored_after_restart": control_restored,
        "lifecycle": lifecycle,
        "processes": metadata,
        "smsc_errors": list(smsc.errors),
        "captured_at": time.time(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--start-method", choices=("fork", "spawn"), default="spawn")
    parser.add_argument("--count", type=int, default=16)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--output", required=True)
    parser.add_argument("--faulthandler-log", default="")
    args = parser.parse_args()
    root = Path(args.run_root).resolve()
    output = Path(args.output).resolve()
    fault_path = Path(args.faulthandler_log).resolve() if args.faulthandler_log else root / "diagnostics" / "faulthandler.log"
    output.parent.mkdir(parents=True, exist_ok=True)
    fault_path.parent.mkdir(parents=True, exist_ok=True)
    result: dict
    try:
        with fault_path.open("w", encoding="utf-8") as handle:
            faulthandler.enable(handle, all_threads=True)
            result = asyncio.run(_run(root, start_method=args.start_method, count=args.count, cycles=args.cycles))
            faulthandler.disable()
    except BaseException as exc:
        result = {
            "status": "ERROR",
            "start_method": args.start_method,
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "run_root": str(root),
            "exception": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(limit=20),
        }
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
