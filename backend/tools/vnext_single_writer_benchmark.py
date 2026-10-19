"""Isolated A0-A4 staging benchmark for the single VNext projection writer.

The benchmark intentionally uses the same 26-field event shape, event-id
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
import random
from pathlib import Path
import platform
try:
    import resource
except ImportError:  # pragma: no cover - Windows only
    resource = None
import sys
import time

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.critical_path_profiler import CriticalPathProfiler
from vnext.projection_writer import ProjectionWriter


STAGING_COLUMNS = (
    "event_id", "partition_id", "ordinal", "campaign_type", "campaign_id",
    "route_id", "normalized_msisdn", "language", "state", "parts_written", "parts_total",
    "message", "error", "created_at", "business_day", "expires_at", "queued",
    "taken", "accepted", "socket_written", "write_failed", "unknown", "cancelled",
    "expired", "response_success", "response_error",
)
STAGING_TYPES = (
    "VARCHAR", "INTEGER", "BIGINT", "VARCHAR", "BIGINT", "BIGINT", "VARCHAR",
    "VARCHAR", "VARCHAR", "INTEGER", "INTEGER", "VARCHAR", "VARCHAR", "DOUBLE", "VARCHAR",
    "DOUBLE", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
    "BIGINT", "BIGINT", "BIGINT",
)


class StagingAdapterSkipped(RuntimeError):
    """The optional adapter cannot be exercised in the current environment."""


def _record_adapter_timing(writer: ProjectionWriter, metric_key: str, elapsed_ns: int) -> None:
    elapsed_ns = max(0, int(elapsed_ns))
    writer._metrics[metric_key] += elapsed_ns
    if writer.profiler:
        writer.profiler.observe_ns(metric_key.removesuffix("_ns"), elapsed_ns)


def _pandas_frame(rows: list[tuple]):
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - depends on the target image
        raise StagingAdapterSkipped(f"pandas unavailable: {exc}") from exc

    frame = pd.DataFrame.from_records(rows, columns=STAGING_COLUMNS)
    nullable_dtypes = {}
    for index, column in enumerate(STAGING_COLUMNS):
        duckdb_type = STAGING_TYPES[index]
        if duckdb_type == "VARCHAR":
            nullable_dtypes[column] = "string"
        elif duckdb_type == "INTEGER":
            nullable_dtypes[column] = "Int32"
        elif duckdb_type == "BIGINT":
            nullable_dtypes[column] = "Int64"
        elif duckdb_type == "DOUBLE":
            nullable_dtypes[column] = "Float64"
    return frame.astype(nullable_dtypes)


def _stage_pandas(writer: ProjectionWriter, rows: list[tuple], execute, _executemany) -> None:
    relation_name = f"_vnext_pandas_stage_{os.getpid()}_{id(rows)}"
    relation_started_ns = time.perf_counter_ns()
    frame = _pandas_frame(rows)
    writer._conn.register(relation_name, frame)
    _record_adapter_timing(
        writer, "staging_relation_create_ns", time.perf_counter_ns() - relation_started_ns
    )
    insert_started_ns = time.perf_counter_ns()
    try:
        execute(f'INSERT INTO _vnext_projection_batch SELECT * FROM "{relation_name}"')
    finally:
        writer._conn.unregister(relation_name)
    _record_adapter_timing(
        writer, "staging_relation_insert_ns", time.perf_counter_ns() - insert_started_ns
    )


def _stage_pyarrow(writer: ProjectionWriter, rows: list[tuple], execute, _executemany) -> None:
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - depends on the target image
        raise StagingAdapterSkipped(f"pyarrow unavailable: {exc}") from exc

    relation_name = f"_vnext_arrow_stage_{os.getpid()}_{id(rows)}"
    relation_started_ns = time.perf_counter_ns()
    arrow_types = {
        "VARCHAR": pa.string(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "DOUBLE": pa.float64(),
    }
    arrays = [
        pa.array([row[index] for row in rows], type=arrow_types[duckdb_type])
        for index, duckdb_type in enumerate(STAGING_TYPES)
    ]
    table = pa.Table.from_arrays(arrays, names=list(STAGING_COLUMNS))
    writer._conn.register(relation_name, table)
    _record_adapter_timing(
        writer, "staging_relation_create_ns", time.perf_counter_ns() - relation_started_ns
    )
    insert_started_ns = time.perf_counter_ns()
    try:
        execute(f'INSERT INTO _vnext_projection_batch SELECT * FROM "{relation_name}"')
    finally:
        writer._conn.unregister(relation_name)
    _record_adapter_timing(
        writer, "staging_relation_insert_ns", time.perf_counter_ns() - insert_started_ns
    )


def _stage_jsonl(writer: ProjectionWriter, rows: list[tuple], execute, _executemany) -> None:
    path = writer.case_root / "_projection_staging.ndjson"
    relation_started_ns = time.perf_counter_ns()
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(
                dict(zip(STAGING_COLUMNS, row)),
                ensure_ascii=False,
                separators=(",", ":"),
            ))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    _record_adapter_timing(
        writer, "staging_relation_create_ns", time.perf_counter_ns() - relation_started_ns
    )

    column_spec = "{" + ", ".join(
        f"{name}: '{duckdb_type}'"
        for name, duckdb_type in zip(STAGING_COLUMNS, STAGING_TYPES)
    ) + "}"
    insert_started_ns = time.perf_counter_ns()
    try:
        execute(
            "INSERT INTO _vnext_projection_batch SELECT * "
            f"FROM read_json_auto(?, columns={column_spec})",
            [str(path)],
        )
    finally:
        path.unlink(missing_ok=True)
    _record_adapter_timing(
        writer, "staging_relation_insert_ns", time.perf_counter_ns() - insert_started_ns
    )


class BenchmarkProjectionWriter(ProjectionWriter):
    """Use production downstream DML with an isolated staging adapter."""

    def __init__(self, *args, arm: str, case_root: Path, **kwargs):
        self.arm = str(arm).upper()
        self.case_root = Path(case_root).resolve()
        self.case_root.mkdir(parents=True, exist_ok=True)
        super().__init__(*args, staging_mode="vectorized", reuse_staging=True, **kwargs)

    def _clear_staging(self, execute, batch_table_sql: str) -> None:
        if self.arm != "A4":
            return super()._clear_staging(execute, batch_table_sql)
        # A4 changes only the order of reuse operations.  Its row input and
        # every downstream statement remain the production A0 path.
        if not self._batch_table_created:
            execute("DROP TABLE IF EXISTS _vnext_projection_batch")
            execute(batch_table_sql)
            self._batch_table_created = True
        execute("DROP TABLE IF EXISTS _vnext_projection_new")
        execute("DELETE FROM _vnext_projection_batch")

    def _stage_rows(self, rows: list[tuple], execute, executemany) -> None:
        if self.arm in {"A0", "A4"}:
            return super()._stage_rows(rows, execute, executemany)
        if self.arm == "A1":
            return _stage_pandas(self, rows, execute, executemany)
        if self.arm == "A2":
            return _stage_pyarrow(self, rows, execute, executemany)
        if self.arm == "A3":
            return _stage_jsonl(self, rows, execute, executemany)
        raise ValueError(f"unsupported benchmark arm: {self.arm}")


def _adapter_capability(label: str) -> tuple[bool, str]:
    if label == "A1":
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            return False, f"pandas unavailable: {exc}"
    elif label == "A2":
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            return False, f"pyarrow unavailable: {exc}"
    return True, ""


def _rss_kib() -> int:
    try:
        value = Path("/proc/self/status").read_text(encoding="utf-8")
        for line in value.splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    if resource is None:
        return 0
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() != "Darwin" else value // 1024


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
    variant = event_index % 31
    if variant == 3:
        message = "中文批量消息 🚀 — same destination across campaigns"
    elif variant == 5:
        message = ""
    elif variant == 7:
        message = None
    elif variant == 11:
        message = "边界/emoji/nullable " + ("长" * 512)
    else:
        message = "benchmark payload with task and durable response event"
    error = None if variant == 13 else ""
    return {
        "event_id": f"bench-{campaign_id}-{event_index:08d}{suffix}",
        "partition_id": event_index % 8,
        "ordinal": event_index * events_per_sms + event_slot + 1,
        "campaign_type": "batch",
        "campaign_id": campaign_id,
        "route_id": 1,
        "normalized_msisdn": f"251900{event_index % 97:07d}",
        "state": state,
        "parts_written": 0 if is_response else 1,
        "parts_total": 1,
        "message": message,
        "error": error,
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
        "rows_prepare_ns", "staging_prepare_rows_ns", "staging_clear_table_ns",
        "staging_transpose_ns", "staging_bind_execute_ns",
        "staging_relation_create_ns", "staging_relation_insert_ns",
        "staging_load_ns", "staging_total_ns", "staging_batch_count",
        "staging_event_count", "new_event_dedupe_ns",
        "events_insert_ns", "aggregate_upsert_ns", "db_commit_ns",
        "batch_end_to_end_ns", "batches_total", "events_read", "events_new",
        "events_replayed", "sql_calls", "executemany_calls",
        "vectorized_staging_calls",
    }
    return {key: int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0) for key in sorted(keys)}


def _staging_breakdown(metrics: dict) -> dict:
    values = {
        key: int(metrics.get(key, 0) or 0)
        for key in (
            "staging_prepare_rows_ns", "staging_clear_table_ns", "staging_transpose_ns",
            "staging_bind_execute_ns", "staging_relation_create_ns",
            "staging_relation_insert_ns",
        )
    }
    total = int(metrics.get("staging_total_ns", 0) or 0)
    accounted = sum(values.values())
    values.update({
        "staging_total_ns": total,
        "accounted_ns": accounted,
        "unattributed_ns": total - accounted,
        "percent_of_staging_total": {
            key: round(value * 100 / total, 3) if total else 0.0
            for key, value in values.items()
        },
    })
    return values


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
    instrumentation: str = "off",
) -> dict:
    label = str(label).upper()
    supported, support_reason = _adapter_capability(label)
    if not supported:
        return {
            "status": "SKIPPED",
            "label": label,
            "batch_size": batch_size,
            "events": events,
            "supported": False,
            "skip_reason": support_reason,
            "result_sha256": "",
            "db_path": "",
        }
    case_root.mkdir(parents=True, exist_ok=True)
    db_path = case_root / "projection.duckdb"
    for path in (db_path, db_path.with_suffix(".duckdb.wal")):
        path.unlink(missing_ok=True)
    duplicate_period = None
    if duplicate_fraction > 0:
        duplicate_period = max(2, round(1.0 / min(0.49, duplicate_fraction)))
    writer = BenchmarkProjectionWriter(
        db_path,
        arm=label,
        case_root=case_root,
        batch_size=batch_size,
        profiler=(
            CriticalPathProfiler(case_root / "critical-path", f"projection-{label}", interval_seconds=1.0)
            if instrumentation == "on" else None
        ),
    )
    metrics_before = {}
    metrics_after = {}
    elapsed_ns = 0
    flush_sizes: list[int] = []
    snapshot = {}
    staging_schema = []
    rss_before = _rss_kib()
    try:
        writer.start()
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
        snapshot = writer.snapshot(force_metrics=True)
        schema_rows = writer._conn.execute("DESCRIBE _vnext_projection_batch").fetchall()
        staging_schema = [
            {"name": str(row[0]), "type": str(row[1])}
            for row in schema_rows
        ]
    except StagingAdapterSkipped as exc:
        return {
            "status": "SKIPPED",
            "label": label,
            "batch_size": batch_size,
            "events": events,
            "supported": False,
            "skip_reason": str(exc),
            "result_sha256": "",
            "db_path": str(db_path),
        }
    except Exception as exc:
        return {
            "status": "FAILED",
            "label": label,
            "batch_size": batch_size,
            "events": events,
            "supported": True,
            "error": f"{type(exc).__name__}: {exc}",
            "result_sha256": "",
            "db_path": str(db_path),
        }
    finally:
        writer.stop()
        if writer.profiler:
            writer.profiler.publish(force=True)
    rss_after = _rss_kib()
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
        "status": "PASS",
        "label": label,
        "batch_size": batch_size,
        "supported": True,
        "instrumentation": instrumentation,
        "staging_mode": "vectorized",
        "reuse_staging": True,
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
        "staging_breakdown": _staging_breakdown(delta),
        "staging_schema": staging_schema,
        "writer_snapshot": snapshot,
        "rss_before_kib": rss_before,
        "rss_after_kib": rss_after,
        "rss_peak_process_kib": max(rss_before, rss_after),
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
    allowed = {"A0", "A1", "A2", "A3", "A4"}
    labels = [item.strip().upper() for item in str(raw).split(",") if item.strip()]
    if not labels or any(item not in allowed for item in labels):
        raise ValueError("labels must be a non-empty subset of A0,A1,A2,A3,A4")
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
    rng = random.Random(int(args.seed))
    for round_index in range(max(1, int(args.rounds))):
        for batch_size in batch_sizes:
            order = list(labels)
            rng.shuffle(order)
            for label in order:
                cases.append(_run_case(
                    run_root / f"round-{round_index + 1:02d}-{label.lower()}-batch-{batch_size}",
                    label=label,
                    batch_size=batch_size,
                    events=max(1, int(args.events)),
                    duplicate_fraction=max(0.0, min(0.49, float(args.duplicate_fraction))),
                    campaign_count=campaign_count,
                    events_per_sms=events_per_sms,
                    business_day_count=business_day_count,
                    instrumentation=str(args.instrumentation),
                ) | {"round": round_index + 1})
    references = {
        (item["round"], item["batch_size"]): next(
            (candidate["result_sha256"] for candidate in cases
             if candidate["round"] == item["round"]
             and candidate["batch_size"] == item["batch_size"]
             and candidate["label"] == "A0"),
            "",
        )
        for item in cases
    }
    for item in cases:
        reference = references[(item["round"], item["batch_size"])]
        item["matches_a0_result"] = (
            item.get("status") == "PASS" and item["result_sha256"] == reference
            if reference else None
        )
    evaluated = [item for item in cases if item.get("status") == "PASS"]
    return {
        "schema": "vnext-staging-adapter-benchmark-v3",
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
        "rounds": max(1, int(args.rounds)),
        "seed": int(args.seed),
        "instrumentation": str(args.instrumentation),
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "nice": os.getpriority(os.PRIO_PROCESS, 0) if hasattr(os, "getpriority") else None,
        "cases": cases,
        "all_correct": (
            bool(evaluated)
            and not any(item.get("status") == "FAILED" for item in cases)
            and (
                all(item.get("matches_a0_result") for item in evaluated)
                if any(references.values())
                else all(bool(item.get("result_sha256")) for item in evaluated)
            )
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--events", type=int, default=11000)
    parser.add_argument("--batch-sizes", default="256,512,1024,2048")
    parser.add_argument("--duplicate-fraction", type=float, default=0.10)
    parser.add_argument("--labels", default="A0,A1,A2,A3,A4")
    parser.add_argument("--campaigns", type=int, default=4)
    parser.add_argument("--events-per-sms", type=int, default=2)
    parser.add_argument("--business-days", type=int, default=2)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20261018)
    parser.add_argument("--instrumentation", choices=("off", "on"), default="off")
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
