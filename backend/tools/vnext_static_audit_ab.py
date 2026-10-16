"""Low-risk local A/B for the VNext idle observer.

The full side reproduces the pre-fix ``TaskSpool(...)`` construction on every
query.  The lightweight side reuses one owner and reads only the manifest plus
changed cursor/taken metadata.  The tool uses synthetic records only and never
connects to a real SMSC or database.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

BACKEND_DIR = Path(os.environ.get("MC_APP_BACKEND_DIR", str(Path(__file__).resolve().parents[1]))).resolve()
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext import spool as spool_module
from vnext.spool import SpoolRecord, TaskSpool


def _seed(root: Path, records: int, partitions: int) -> None:
    spool = TaskSpool(root, virtual_partition_count=partitions, fsync=False)
    ordinals = {partition: 1 for partition in range(partitions)}
    values = []
    for index in range(max(1, int(records))):
        partition = index % partitions
        ordinal = ordinals[partition]
        ordinals[partition] += 1
        values.append(SpoolRecord(
            schema_version=1,
            event_id=f"audit-{index:08d}",
            partition_id=partition,
            ordinal=ordinal,
            campaign_type="audit",
            campaign_id=1,
            campaign_generation=1,
            route_id=1,
            normalized_msisdn=f"251900{index:07d}",
            payload_ref=f"audit:{index}",
            message="audit synthetic payload",
            part_count_hint=1,
            source_id="static-audit",
            created_at=time.time(),
        ))
    spool.append_many(values)


def _run_queries(root: Path, queries: int, partitions: int, mode: str) -> dict:
    counters = {
        "spool_constructions": 0,
        "segment_scans": 0,
        "segment_bytes_scanned": 0,
        "json_loads": 0,
    }
    original_loads = spool_module.json.loads
    original_init = TaskSpool.__init__
    original_scan = TaskSpool._scan_segment

    def counted_loads(*args, **kwargs):
        counters["json_loads"] += 1
        return original_loads(*args, **kwargs)

    def counted_init(self, *args, **kwargs):
        counters["spool_constructions"] += 1
        return original_init(self, *args, **kwargs)

    def counted_scan(self, path, *, start=0):
        counters["segment_scans"] += 1
        try:
            counters["segment_bytes_scanned"] += max(0, int(path.stat().st_size) - int(start))
        except OSError:
            pass
        return original_scan(self, path, start=start)

    spool_module.json.loads = counted_loads
    TaskSpool.__init__ = counted_init
    TaskSpool._scan_segment = counted_scan
    started = time.perf_counter()
    try:
        if mode == "full":
            last = None
            for _ in range(max(1, int(queries))):
                last = TaskSpool(root, virtual_partition_count=partitions, fsync=False).status()
        else:
            owner = TaskSpool(root, virtual_partition_count=partitions, fsync=False)
            last = None
            for _ in range(max(1, int(queries))):
                last = owner.lightweight_snapshot()
    finally:
        spool_module.json.loads = original_loads
        TaskSpool.__init__ = original_init
        TaskSpool._scan_segment = original_scan
    return {
        "mode": mode,
        "queries": max(1, int(queries)),
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "counters": counters,
        "pending_records": int((last or {}).get("pending_records", -1)),
        "total_records": int((last or {}).get("total_records", -1)),
        "snapshot_mode": (last or {}).get("snapshot_mode", "full"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", default="")
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--queries", default="10,100,1000")
    args = parser.parse_args()
    root = Path(args.run_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    partitions = max(1, min(256, int(args.partitions)))
    _seed(root / "spool", max(1, int(args.records)), partitions)
    values = []
    for raw in str(args.queries).split(","):
        try:
            value = max(1, int(raw.strip()))
        except ValueError:
            continue
        values.append({
            "queries": value,
            "full": _run_queries(root / "spool", value, partitions, "full"),
            "lightweight": _run_queries(root / "spool", value, partitions, "lightweight"),
        })
    try:
        filesystem = subprocess.check_output(["df", "-T", "."], text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        filesystem = "UNAVAILABLE"
    result = {
        "tool": "vnext_static_audit_ab",
        "status": "PASS" if values and all(
            item["full"]["pending_records"] == item["lightweight"]["pending_records"]
            and item["full"]["total_records"] == item["lightweight"]["total_records"]
            for item in values
        ) else "FAIL",
        "run_root": str(root),
        "records": max(1, int(args.records)),
        "partitions": partitions,
        "filesystem_df_T": filesystem,
        "python": sys.version,
        "queries": values,
        "captured_at": time.time(),
    }
    output = Path(args.output).resolve() if args.output else root / "idle-ab.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
