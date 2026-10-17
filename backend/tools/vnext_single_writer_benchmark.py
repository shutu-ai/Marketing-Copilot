"""Isolated A0-A3 benchmark for the single VNext projection writer.

The benchmark intentionally uses the same 25-field event shape, event-id
dedupe, aggregate upsert, DuckDB transaction and bounded batch path as the
production writer.  Each case owns a separate database under ``--run-root``;
no runtime database, journal or spool is read or modified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.projection_writer import ProjectionWriter


def _percentile(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = index - lower
    return int(round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction))


def _event(index: int, *, duplicate_period: int | None = None, campaign_id: int = 10) -> dict:
    event_index = index
    if duplicate_period and index > 0 and index % duplicate_period == 0:
        event_index = index - 1
    state = "socket_written" if index % 17 else "submit_sm_resp_success"
    return {
        "event_id": f"bench-{campaign_id}-{event_index:08d}",
        "partition_id": index % 8,
        "ordinal": index + 1,
        "campaign_type": "batch",
        "campaign_id": campaign_id,
        "route_id": 1,
        "normalized_msisdn": f"251900{index:07d}",
        "state": state,
        "parts_written": 1,
        "parts_total": 1,
        "message": "benchmark payload with nullable-compatible text",
        "error": None if index % 23 else "",
        "created_at": 1_760_000_000.0 + index,
        "business_day": "2026-10-17",
        "expires_at": 1_760_086_400.0,
        "queued": 1,
        "taken": 1,
        "accepted": 1,
        "socket_written": int(state == "socket_written"),
        "write_failed": 0,
        "unknown": 0,
        "cancelled": 0,
        "expired": 0,
        "response_success": int(state == "submit_sm_resp_success"),
        "response_error": 0,
    }


def _jsonable(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _projection_digest(db_path: Path) -> tuple[str, int, int, list[dict]]:
    import duckdb

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        event_rows = connection.execute(
            "SELECT * FROM vnext_progress_events WHERE campaign_id=10 ORDER BY event_id"
        ).fetchall()
        event_columns = [str(item[0]) for item in connection.description]
        projection_rows = connection.execute(
            "SELECT * FROM vnext_projection WHERE campaign_id=10 ORDER BY business_day,campaign_type,campaign_id,partition_id,route_id"
        ).fetchall()
        projection_columns = [str(item[0]) for item in connection.description]
    finally:
        connection.close()
    payload = {
        "events": [
            {key: _jsonable(value) for key, value in zip(event_columns, row)}
            for row in event_rows
        ],
        "projection": [
            {key: _jsonable(value) for key, value in zip(projection_columns, row)}
            for row in projection_rows
        ],
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return digest, len(event_rows), len(projection_rows), payload["projection"]


def _metrics_delta(before: dict, after: dict) -> dict:
    keys = {
        "rows_prepare_ns", "staging_load_ns", "new_event_dedupe_ns",
        "events_insert_ns", "aggregate_upsert_ns", "db_commit_ns",
        "batch_end_to_end_ns", "batches_total", "events_read", "events_new",
        "events_replayed", "sql_calls", "executemany_calls",
        "vectorized_staging_calls",
    }
    return {key: int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0) for key in sorted(keys)}


def _run_case(case_root: Path, *, label: str, batch_size: int, events: int, duplicate_fraction: float) -> dict:
    case_root.mkdir(parents=True, exist_ok=True)
    db_path = case_root / "projection.duckdb"
    if label in {"A0", "A1"}:
        staging_mode = "executemany"
    else:
        staging_mode = "vectorized"
    reuse_staging = label in {"A1", "A3"}
    duplicate_period = None
    if duplicate_fraction > 0:
        duplicate_period = max(2, round(1.0 / min(0.49, duplicate_fraction)))
    writer = ProjectionWriter(
        db_path,
        batch_size=batch_size,
        staging_mode=staging_mode,
        reuse_staging=reuse_staging,
    )
    writer.start()
    try:
        warmup = [_event(index, campaign_id=99) for index in range(min(64, batch_size))]
        for item in warmup:
            writer.submit(item)
        writer.flush()
        metrics_before = dict(writer._metrics)
        started_ns = time.perf_counter_ns()
        flush_sizes: list[int] = []
        for start in range(0, events, batch_size):
            current = [_event(index, duplicate_period=duplicate_period) for index in range(start, min(events, start + batch_size))]
            for item in current:
                writer.submit(item)
            flush_sizes.append(writer.flush())
        elapsed_ns = time.perf_counter_ns() - started_ns
        metrics_after = dict(writer._metrics)
        snapshot = writer.snapshot()
    finally:
        writer.stop()
    digest, unique_events, aggregate_rows, projection = _projection_digest(db_path)
    delta = _metrics_delta(metrics_before, metrics_after)
    elapsed_seconds = elapsed_ns / 1_000_000_000
    return {
        "label": label,
        "batch_size": batch_size,
        "staging_mode": staging_mode,
        "reuse_staging": reuse_staging,
        "events": events,
        "duplicate_period": duplicate_period,
        "duplicate_fraction_requested": duplicate_fraction,
        "wall_seconds": round(elapsed_seconds, 6),
        "events_per_second": round(events / elapsed_seconds, 3) if elapsed_seconds else 0.0,
        "batch_size_p50": _percentile(flush_sizes, 0.50),
        "batch_size_p95": _percentile(flush_sizes, 0.95),
        "metrics": delta,
        "writer_snapshot": snapshot,
        "unique_event_rows": unique_events,
        "aggregate_rows": aggregate_rows,
        "result_sha256": digest,
        "projection_rows": projection,
        "db_path": str(db_path),
    }


def _parse_batch_sizes(raw: str) -> list[int]:
    values = []
    for item in str(raw).split(","):
        value = int(item.strip())
        if value <= 0:
            raise ValueError("batch sizes must be positive")
        values.append(min(4096, value))
    return sorted(set(values))


def run(args: argparse.Namespace) -> dict:
    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    batch_sizes = _parse_batch_sizes(args.batch_sizes)
    labels = ("A0", "A1", "A2", "A3")
    cases = []
    for batch_size in batch_sizes:
        for label in labels:
            cases.append(_run_case(
                run_root / f"{label.lower()}-batch-{batch_size}",
                label=label,
                batch_size=batch_size,
                events=max(1, int(args.events)),
                duplicate_fraction=max(0.0, min(0.49, float(args.duplicate_fraction))),
            ))
    references = {
        str(batch_size): next(
            item["result_sha256"] for item in cases
            if item["batch_size"] == batch_size and item["label"] == "A0"
        )
        for batch_size in batch_sizes
    }
    for item in cases:
        item["matches_a0_result"] = item["result_sha256"] == references[str(item["batch_size"])]
    return {
        "schema": "vnext-single-writer-benchmark-v1",
        "python": sys.version,
        "platform": platform.platform(),
        "pid": os.getpid(),
        "cwd": str(Path.cwd()),
        "run_root": str(run_root),
        "duckdb_version": __import__("duckdb").__version__,
        "events": max(1, int(args.events)),
        "duplicate_fraction": max(0.0, min(0.49, float(args.duplicate_fraction))),
        "batch_sizes": batch_sizes,
        "cases": cases,
        "all_correct": all(item["matches_a0_result"] for item in cases),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--events", type=int, default=4096)
    parser.add_argument("--batch-sizes", default="256,512,1024,2048")
    parser.add_argument("--duplicate-fraction", type=float, default=0.10)
    args = parser.parse_args()
    result = run(args)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "all_correct": result["all_correct"],
        "duckdb_version": result["duckdb_version"],
        "cases": len(result["cases"]),
        "output": str(output),
    }, sort_keys=True))
    return 0 if result["all_correct"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
