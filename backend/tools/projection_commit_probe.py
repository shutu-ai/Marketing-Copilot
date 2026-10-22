"""Isolated commit/checkpoint/recovery probe; synthetic data only.

Run from a native Linux filesystem for performance or stability evidence.
MC_PROJECTION_SOURCE can point at a baseline backend for an identical A/B run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time


sys.path.insert(0, os.environ.get("MC_PROJECTION_SOURCE", str(Path(__file__).resolve().parents[1])))
from vnext.projection_writer import ProjectionWriter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--events", type=int, default=65536)
    parser.add_argument("--checkpoint-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    database = root / "projection.duckdb"
    if args.child:
        writer = ProjectionWriter(database, batch_size=512)
        writer._checkpoint_bytes = args.checkpoint_bytes
        writer.start()
        writer._conn.execute(f"SET checkpoint_threshold='{args.checkpoint_bytes}B'")
        rng = random.Random(20261021)
        alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 中文短信ሰላም"
        started = time.monotonic()
        checkpoints = 0
        previous_wal = 0
        for first in range(0, args.events, 512):
            for ordinal in range(first, min(first + 512, args.events)):
                message = "".join(rng.choices(alphabet, k=384))
                writer.submit({
                    "event_id": f"probe-{ordinal}", "partition_id": ordinal % 16,
                    "ordinal": ordinal, "campaign_type": "batch",
                    "campaign_id": ordinal % 8, "route_id": 1,
                    "normalized_msisdn": "synthetic",
                    "language": "mixed", "state": "socket_written",
                    "parts_written": 3, "parts_total": 3,
                    "message": message, "error": "",
                    "created_at": 1792570000.0 + ordinal / 1000,
                    "business_day": "2026-10-21", "expires_at": 0,
                    "taken": 1, "accepted": 1, "socket_written": 1,
                })
            writer.flush()
            wal_path = Path(f"{database}.wal")
            wal = wal_path.stat().st_size if wal_path.exists() else 0
            if wal < previous_wal or wal == 0:
                checkpoints += 1
            previous_wal = wal
            if first % 8192 == 0:
                print(json.dumps({"applied": writer._applied, "seconds": round(time.monotonic()-started, 3)}), flush=True)
        assert not writer.snapshot().get("checkpoint_error"), writer.snapshot()
        print(json.dumps({
            "events": args.events, "seconds": round(time.monotonic() - started, 3),
            "checkpoint_cycles_observed": checkpoints,
            "events_per_second": round(args.events / (time.monotonic() - started), 2),
        }), flush=True)
        # Deliberately omit close: reopening must recover the committed WAL.
        os._exit(0)
    if database.exists():
        raise SystemExit("Use a new root; existing databases are never overwritten")
    result = subprocess.run([sys.executable, __file__, *sys.argv[1:], "--child"])
    if result.returncode:
        raise SystemExit(f"writer exited {result.returncode}")
    import duckdb
    with duckdb.connect(str(database)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0]
        totals = conn.execute("SELECT SUM(taken),SUM(socket_written),SUM(socket_written_pdus) FROM vnext_projection").fetchone()
        assert count == args.events, (count, args.events)
        assert totals == (args.events, args.events, 3 * args.events), totals
        digest = hashlib.sha256()
        for row in conn.execute("SELECT event_id,message FROM vnext_progress_events ORDER BY event_id").fetchall():
            digest.update(json.dumps(row, ensure_ascii=False).encode("utf-8"))
    print(json.dumps({"status": "PASS", "recovered_events": count, "sha256": digest.hexdigest()}), flush=True)


if __name__ == "__main__":
    main()
