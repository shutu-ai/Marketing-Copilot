"""Capture the pre-closure VNext runtime canary failures.

This is intentionally a small, loopback-only reproducer used by the C0 RCA.
It starts the existing SMPP simulator, starts the real ``VNextRuntime`` before
ingress, then checks whether a later online append is discovered and whether a
persisted campaign pause survives a runtime restart.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
import sys

BACKEND_DIR = Path(os.environ.get("MC_APP_BACKEND_DIR", str(Path(__file__).resolve().parents[1]))).resolve()
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.runtime import VNextRuntime
from vnext_loopback_acceptance import LoopbackSMSC


async def run(root: Path, count: int = 10) -> dict:
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    smsc = LoopbackSMSC()
    port = await smsc.start()
    route = {
        "route_id": 1,
        "host": "127.0.0.1",
        "port": port,
        "system_id": "c0",
        "password": "c0",
        "source_addr": "C0",
        "window_size": 10,
        "tps": 0.0,
        "timeout": 2,
    }
    runtime = VNextRuntime(
        root,
        {1: route},
        worker_count=1,
        virtual_partition_count=16,
        projection_queue_size=2,
        max_inflight=4,
        prefetch_per_partition=4,
    )
    online = {}
    controls_before = []
    controls_after = []
    try:
        runtime.start()
        controls_before = runtime.status()["controls"]
        ready_deadline = time.monotonic() + 5.0
        while time.monotonic() < ready_deadline:
            workers = (runtime.status().get("supervisor") or {}).get("workers", [])
            if workers and all(item.get("alive") for item in workers):
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)
        rows = [
            {"msisdn": f"0900000{index:03d}", "message": f"c0-{index}"}
            for index in range(count)
        ]
        accepted = runtime.enqueue_chunk(
            rows,
            campaign_type="batch",
            campaign_id=901,
            campaign_generation=1,
            route_id=1,
            phone_prefix="251",
            source_id="c0-online",
            payload_ref="c0-online",
        ).accepted
        await asyncio.sleep(2.0)
        online_status = runtime.status()
        online = {
            "accepted": accepted,
            "smpp_received": len(smsc.received),
            "socket_written": sum(
                1 for path in runtime.progress_root.glob("worker-*.jsonl")
                for line in path.read_text(encoding="utf-8").splitlines()
                if line and json.loads(line).get("state") == "socket_written"
            ),
            "pending_records": online_status["spool"]["pending_records"],
        }
        runtime.set_campaign_state("batch", 902, "paused")
    finally:
        runtime.stop()

    restarted = VNextRuntime(
        root,
        {1: route},
        worker_count=1,
        virtual_partition_count=16,
        projection_queue_size=2,
        max_inflight=4,
        prefetch_per_partition=4,
    )
    try:
        restarted.start()
        controls_after = restarted.status()["controls"]
    finally:
        restarted.stop()
        await smsc.stop()

    return {
        "status": "FAIL_REPRODUCED" if (
            online.get("smpp_received", 0) < online.get("accepted", count)
            or not any(item.get("campaign_id") == 902 for item in controls_after)
        ) else "NO_FAILURE",
        "root": str(root),
        "online_append": online,
        "controls_before_restart": controls_before,
        "controls_after_restart": controls_after,
        "captured_at": time.time(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", default="")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    root = Path(args.run_root).resolve() if args.run_root else Path(tempfile.mkdtemp(prefix="mc-vnext-c0-"))
    result = asyncio.run(run(root, max(1, args.count)))
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "FAIL_REPRODUCED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
