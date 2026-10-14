"""Isolated live DuckDB claim/authorization benchmark.

This is intentionally a one-shot harness: it creates its own database, does
not start the application, and reports measured samples rather than inferred
percentiles.  Run it once against a baseline source tree and once against the
working tree with the same task count and iteration count.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

import database
from services import sms_task_service


def _percentile(samples: list[float], quantile: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _seed(task_count: int, history_count: int) -> None:
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('hotpath-benchmark','in_progress') RETURNING id"
    ).fetchone()["id"]
    if history_count:
        conn.execute(
            """INSERT INTO sms_tasks
               (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
                status,smpp_config_id)
               SELECT 'batch', ?, 'hotpath-history',
                      CAST(1519000000 + i AS VARCHAR), 'history',
                      'hotpath-history-' || CAST(i AS VARCHAR), 'accepted', 7
               FROM range(?) AS history(i)""",
            (campaign_id, int(history_count)),
        )
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,smpp_config_id)
           SELECT 'batch', ?, 'hotpath-benchmark',
                  CAST(2519000000 + i AS VARCHAR), 'hello',
                  'hotpath-benchmark-' || CAST(i AS VARCHAR), 'queued', 7
           FROM range(?) AS values(i)""",
        (campaign_id, int(task_count)),
    )
    conn.commit()
    conn.close()


def run(db_path: str, task_count: int, iterations: int, history_count: int,
        planned_parts: int = 1) -> dict:
    database.set_database_file(db_path)
    database.init_db()
    _seed(task_count, history_count)

    count_queries = 0
    statement_samples = {name: [] for name in ("select", "count", "insert", "update", "other", "commit")}
    lock_hold_samples = []
    original_execute = database.WrappedConnection.execute
    original_commit = database.WrappedConnection.commit
    original_release = database.WrappedConnection._release_write_lock

    def statement_kind(normalized: str) -> str:
        if "COUNT(*)" in normalized:
            return "count"
        if normalized.startswith("SELECT") or (
            normalized.startswith("WITH") and "SELECT" in normalized
        ):
            return "select"
        for keyword in ("INSERT", "UPDATE"):
            if normalized.startswith(keyword):
                return keyword.lower()
        return "other"

    def tracked_execute(connection, sql, params=None):
        nonlocal count_queries
        normalized = " ".join(str(sql).upper().split())
        if "COUNT(*)" in normalized and "SMS_TASKS" in normalized:
            count_queries += 1
        started = time.perf_counter()
        try:
            return original_execute(connection, sql, params)
        finally:
            statement_samples[statement_kind(normalized)].append(
                time.perf_counter() - started
            )

    def tracked_commit(connection):
        started = time.perf_counter()
        try:
            return original_commit(connection)
        finally:
            statement_samples["commit"].append(time.perf_counter() - started)

    def tracked_release(connection):
        status = database.write_lock_status()
        if status.get("held"):
            lock_hold_samples.append(float(status.get("age_seconds") or 0.0))
        return original_release(connection)

    database.WrappedConnection.execute = tracked_execute
    database.WrappedConnection.commit = tracked_commit
    database.WrappedConnection._release_write_lock = tracked_release
    claim_samples = []
    authorize_samples = []
    try:
        for _ in range(min(int(iterations), int(task_count))):
            started = time.perf_counter()
            claimed = sms_task_service.claim_next_tasks(1)
            claim_samples.append(time.perf_counter() - started)
            if not claimed:
                break
            started = time.perf_counter()
            attempt = sms_task_service.record_send_intent(
                claimed[0], smpp_config_id=7, slot=0, planned_parts=planned_parts,
            )
            authorize_samples.append(time.perf_counter() - started)
            if not attempt:
                raise RuntimeError("send authorization unexpectedly denied")
    finally:
        database.WrappedConnection.execute = original_execute
        database.WrappedConnection.commit = original_commit
        database.WrappedConnection._release_write_lock = original_release

    def summary(samples: list[float]) -> dict:
        return {
            "count": len(samples),
            "mean_ms": round(statistics.fmean(samples) * 1000, 3) if samples else 0.0,
            "p50_ms": round(_percentile(samples, 0.50) * 1000, 3),
            "p95_ms": round(_percentile(samples, 0.95) * 1000, 3),
            "p99_ms": round(_percentile(samples, 0.99) * 1000, 3),
            "max_ms": round(max(samples) * 1000, 3) if samples else 0.0,
        }

    result = {
        "database": str(Path(db_path).resolve()),
        "task_count": int(task_count),
        "history_count": int(history_count),
        "iterations_requested": int(iterations),
        "planned_parts": max(1, int(planned_parts)),
        "claim": summary(claim_samples),
        "submit_authorize": summary(authorize_samples),
        "sms_tasks_count_queries": count_queries,
    }
    result["statement_timing"] = {
        kind: summary(samples) for kind, samples in statement_samples.items()
    }
    result["write_lock_hold"] = summary(lock_hold_samples)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--task-count", type=int, default=3000)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--history-count", type=int, default=0)
    parser.add_argument("--planned-parts", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(
        run(args.db, args.task_count, args.iterations, args.history_count, args.planned_parts),
        indent=2,
    ))


if __name__ == "__main__":
    main()
