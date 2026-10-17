"""Isolated A0-A3 benchmark for the single VNext projection writer.

The benchmark intentionally uses the same 25-field event shape, event-id
dedupe, aggregate upsert, DuckDB transaction and bounded batch path as the
production writer.  Each case owns a separate database under ``--run-root``;
no runtime database, journal or spool is read or modified.
"""

from __future__ import annotations

import argparse
from collections import Counter
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


def _event(
    index: int,
    *,
    duplicate_period: int | None = None,
    campaign_count: int = 4,
    events_per_sms: int = 2,
    business_day_count: int = 2,
    campaign_id_override: int | None = None,
) -> dict:
    events_per_sms = max(1, int(events_per_sms))
    sms_index = index // events_per_sms
    event_slot = index % events_per_sms
    event_index = sms_index
    if duplicate_period and sms_index > 0 and sms_index % duplicate_period == 0:
        event_index = sms_index - 1
    campaign_id = int(campaign_id_override or (10 + (event_index % max(1, campaign_count))))
    business_day = f"2026-10-{17 + (event_index % max(1, business_day_count)):02d}"
    is_response = event_slot > 0
    state = "submit_sm_resp_success" if is_response else "socket_written"
    suffix = f":resp:{event_slot}" if is_response else ""
    return {
        "event_id": f"bench-{campaign_id}-{event_index:08d}{suffix}",
        "partition_id": event_index % 8,
        "ordinal": event_index * events_per_sms + event_slot + 1,
        "campaign_type": "batch",
        "campaign_id": campaign_id,
        "route_id": 1,
        "normalized_msisdn": f"251900{event_index:07d}",
        "state": state,
        "parts_written": 0 if is_response else 1,
        "parts_total": 1,
        "message": "benchmark payload with task and durable response event",
        "error": "",
        "created_at": 1_760_000_000.0 + event_index,
        "business_day": business_day,
        "expires_at": 1_760_086_400.0,
        "queued": int(not is_response),
        "taken": int(not is_response),
        "accepted": int(not is_response),
        "socket_written": int(not is_response),
        "write_failed": 0,
        "unknown": 0,
        "cancelled": 0,
        "expired": 0,
        "response_success": int(is_response),
        "response_error": 0,
    }


def _jsonable(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _projection_digest(db_path: Path) -> tuple[str, int, int, list[dict], dict, list[dict]]:
    import duckdb

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        event_rows = connection.execute(
            "SELECT * FROM vnext_progress_events WHERE campaign_id<>99 ORDER BY event_id"
        ).fetchall()
        event_columns = [str(item[0]) for item in connection.description]
        projection_rows = connection.execute(
            "SELECT * FROM vnext_projection WHERE campaign_id<>99 ORDER BY business_day,campaign_type,campaign_id,partition_id,route_id"
        ).fetchall()
        projection_columns = [str(item[0]) for item in connection.description]
    finally:
        connection.close()
    event_payload = [
            {key: _jsonable(value) for key, value in zip(event_columns, row)}
            for row in event_rows
        ]
    projection_payload = [
            {key: _jsonable(value) for key, value in zip(projection_columns, row)}
            for row in projection_rows
        ]
    state_counts = Counter(str(item.get("state") or "") for item in event_payload)
    daily_summary: dict[str, dict[str, int]] = {}
    for item in projection_payload:
        day = str(item.get("business_day") or "")
        summary = daily_summary.setdefault(day, {"rows": 0, "queued": 0, "socket_written": 0, "response_success": 0})
        summary["rows"] += 1
        summary["queued"] += int(item.get("queued") or 0)
        summary["socket_written"] += int(item.get("socket_written") or 0)
        summary["response_success"] += int(item.get("response_success") or 0)
    payload = {
        "events": event_payload,
        "projection": projection_payload,
        "state_counts": dict(sorted(state_counts.items())),
        "daily_summary": {key: daily_summary[key] for key in sorted(daily_summary)},
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return (
        digest,
        len(event_rows),
        len(projection_rows),
        payload["projection"],
        payload["state_counts"],
        [dict(day=key, **daily_summary[key]) for key in sorted(daily_summary)],
    )


def _metrics_delta(before: dict, after: dict) -> dict:
    keys = {
        "rows_prepare_ns", "staging_load_ns", "new_event_dedupe_ns",
        "events_insert_ns", "aggregate_upsert_ns", "db_commit_ns",
        "batch_end_to_end_ns", "batches_total", "events_read", "events_new",
        "events_replayed", "sql_calls", "executemany_calls",
        "vectorized_staging_calls",
    }
    return {key: int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0) for key in sorted(keys)}


def _run_case(
    case_root: Path,
    *,
    label: str,
    batch_size: int,
    events: int,
    duplicate_fraction: float,
    campaign_count: int,
    events_per_sms: int,
    business_day_count: int,
) -> dict:
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
        warmup = [
            _event(
                index,
                campaign_id_override=99,
                campaign_count=campaign_count,
                events_per_sms=events_per_sms,
                business_day_count=business_day_count,
            )
            for index in range(min(64, batch_size))
        ]
        for item in warmup:
            writer.submit(item)
        writer.flush()
        metrics_before = dict(writer._metrics)
        started_ns = time.perf_counter_ns()
        flush_sizes: list[int] = []
        for start in range(0, events, batch_size):
            current = [
                _event(
                    index,
                    duplicate_period=duplicate_period,
                    campaign_count=campaign_count,
                    events_per_sms=events_per_sms,
                    business_day_count=business_day_count,
                )
                for index in range(start, min(events, start + batch_size))
            ]
            for item in current:
                writer.submit(item)
            flush_sizes.append(writer.flush())
        elapsed_ns = time.perf_counter_ns() - started_ns
        metrics_after = dict(writer._metrics)
        snapshot = writer.snapshot()
    finally:
        writer.stop()
    (
        digest,
        unique_events,
        aggregate_rows,
        projection,
        state_counts,
        daily_summary,
    ) = _projection_digest(db_path)
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
        "campaign_count": campaign_count,
        "events_per_sms": events_per_sms,
        "business_day_count": business_day_count,
        "wall_seconds": round(elapsed_seconds, 6),
        "events_per_second": round(events / elapsed_seconds, 3) if elapsed_seconds else 0.0,
        "batch_size_p50": _percentile(flush_sizes, 0.50),
        "batch_size_p95": _percentile(flush_sizes, 0.95),
        "metrics": delta,
        "writer_snapshot": snapshot,
        "unique_event_rows": unique_events,
        "aggregate_rows": aggregate_rows,
        "task_event_count": int(state_counts.get("socket_written", 0)),
        "response_event_count": int(state_counts.get("submit_sm_resp_success", 0)),
        "state_counts": state_counts,
        "daily_summary": daily_summary,
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


def _parse_labels(raw: str) -> list[str]:
    allowed = {"A0", "A1", "A2", "A3"}
    labels = [item.strip().upper() for item in str(raw).split(",") if item.strip()]
    if not labels or any(item not in allowed for item in labels):
        raise ValueError("labels must be a non-empty subset of A0,A1,A2,A3")
    return list(dict.fromkeys(labels))


def run(args: argparse.Namespace) -> dict:
    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    batch_sizes = _parse_batch_sizes(args.batch_sizes)
    labels = _parse_labels(args.labels)
    campaign_count = max(1, min(64, int(args.campaigns)))
    events_per_sms = max(1, min(8, int(args.events_per_sms)))
    business_day_count = max(1, min(31, int(args.business_days)))
    cases = []
    for batch_size in batch_sizes:
        for label in labels:
            cases.append(_run_case(
                run_root / f"{label.lower()}-batch-{batch_size}",
                label=label,
                batch_size=batch_size,
                events=max(1, int(args.events)),
                duplicate_fraction=max(0.0, min(0.49, float(args.duplicate_fraction))),
                campaign_count=campaign_count,
                events_per_sms=events_per_sms,
                business_day_count=business_day_count,
            ))
    references = {
        str(batch_size): next(
            (item["result_sha256"] for item in cases
             if item["batch_size"] == batch_size and item["label"] == "A0"),
            "",
        )
        for batch_size in batch_sizes
    }
    for item in cases:
        reference = references[str(item["batch_size"])]
        item["matches_a0_result"] = (
            item["result_sha256"] == reference if reference else None
        )
    return {
        "schema": "vnext-single-writer-benchmark-v2",
        "python": sys.version,
        "platform": platform.platform(),
        "pid": os.getpid(),
        "cwd": str(Path.cwd()),
        "run_root": str(run_root),
        "duckdb_version": __import__("duckdb").__version__,
        "events": max(1, int(args.events)),
        "duplicate_fraction": max(0.0, min(0.49, float(args.duplicate_fraction))),
        "labels": labels,
        "campaign_count": campaign_count,
        "events_per_sms": events_per_sms,
        "business_day_count": business_day_count,
        "batch_sizes": batch_sizes,
        "cases": cases,
        "all_correct": (
            all(item["matches_a0_result"] for item in cases)
            if any(references.values())
            else all(bool(item.get("result_sha256")) for item in cases)
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--events", type=int, default=4096)
    parser.add_argument("--batch-sizes", default="256,512,1024,2048")
    parser.add_argument("--duplicate-fraction", type=float, default=0.10)
    parser.add_argument("--labels", default="A0,A1,A2,A3")
    parser.add_argument("--campaigns", type=int, default=4)
    parser.add_argument("--events-per-sms", type=int, default=2)
    parser.add_argument("--business-days", type=int, default=2)
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
