"""Build a VNext projection in bulk without indexed hot-path upserts.

The normal projection writer is deliberately incremental and is optimized for
small online commits.  A long offline replay has a different failure mode on
older DuckDB releases: repeatedly maintaining several indexed tables, temporary
relations, and ``ON CONFLICT`` statements can eventually enter a native storage
failure even when every individual batch is small.

This command is an operator-controlled recovery path.  It reads the immutable
progress journals while the service is stopped, loads an unindexed staging table
in bounded transactions, derives the canonical tables once, and builds the
indexes only after the data is complete.  It never mutates the source journals.
It never opens the task spool or an SMPP transport, so rebuilding the read model
cannot authorize a new SMS submission.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from tools.maintain_projection import (  # noqa: E402
    _journal_paths,
    _read_quantum,
    _service_is_active,
    _storage,
)
from vnext.projection_writer import (  # noqa: E402
    ProgressJournal,
    _safe_text,
    _load_offsets,
    _persist_offsets,
)
from vnext.task_index import rebuild_from_duckdb  # noqa: E402


RAW_COLUMNS = (
    "ingest_seq",
    "event_kind",
    "event_id",
    "logical_task_id",
    "partition_id",
    "ordinal",
    "campaign_type",
    "campaign_id",
    "route_id",
    "normalized_msisdn",
    "language",
    "campaign_name",
    "state",
    "parts_written",
    "parts_total",
    "message",
    "error",
    "created_at",
    "business_day",
    "expires_at",
    "event_at",
    "queued",
    "taken",
    "accepted",
    "socket_written",
    "write_failed",
    "unknown",
    "cancelled",
    "expired",
    "response_success",
    "response_error",
)
RAW_COLUMN_SQL = ",".join(RAW_COLUMNS)
RAW_PLACEHOLDERS = ",".join("?" for _ in RAW_COLUMNS)

PROGRESS_COLUMNS = (
    "event_id",
    "partition_id",
    "ordinal",
    "campaign_type",
    "campaign_id",
    "route_id",
    "normalized_msisdn",
    "language",
    "state",
    "parts_written",
    "parts_total",
    "message",
    "error",
    "created_at",
    "business_day",
    "expires_at",
)
PROGRESS_COLUMN_SQL = ",".join(PROGRESS_COLUMNS)

TASK_INDEX_COLUMNS = (
    "event_id",
    "partition_id",
    "ordinal",
    "campaign_type",
    "campaign_id",
    "route_id",
    "normalized_msisdn",
    "language",
    "campaign_name",
    "state",
    "parts_written",
    "parts_total",
    "message",
    "error",
    "created_at",
    "business_day",
    "expires_at",
    "last_event_at",
)
TASK_INDEX_COLUMN_SQL = ",".join(TASK_INDEX_COLUMNS)

RAW_TABLE = "_mc_replay_events"
UNIQUE_PROGRESS_TABLE = "_mc_unique_progress"


def _int_value(event: dict, key: str, default: int = 0) -> int:
    value = event.get(key, default)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return int(default)


def _float_value(event: dict, key: str, default: float) -> float:
    value = event.get(key, default)
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return float(default)


def _logical_task_id(event: dict) -> str:
    explicit = str(event.get("logical_task_id") or "").strip()
    if explicit:
        return explicit
    return str(event.get("event_id") or "").split(":resp:", 1)[0]


def _raw_row(event: dict, ingest_seq: int) -> tuple:
    state = str(event.get("state") or "")
    created_at = _float_value(event, "created_at", time.time())
    return (
        int(ingest_seq),
        _safe_text(event.get("event_kind")),
        _safe_text(event.get("event_id")),
        _logical_task_id(event),
        _int_value(event, "partition_id"),
        _int_value(event, "ordinal"),
        _safe_text(event.get("campaign_type")),
        _int_value(event, "campaign_id"),
        _int_value(event, "route_id"),
        _safe_text(event.get("normalized_msisdn")),
        _safe_text(event.get("language")),
        _safe_text(event.get("campaign_name")),
        state,
        _int_value(event, "parts_written"),
        _int_value(event, "parts_total"),
        _safe_text(event.get("message")),
        _safe_text(event.get("error")),
        created_at,
        _safe_text(event.get("business_day")),
        _float_value(event, "expires_at", 0.0),
        _float_value(event, "event_at", created_at),
        _int_value(event, "queued"),
        _int_value(event, "taken"),
        _int_value(event, "accepted"),
        _int_value(event, "socket_written", int(state == "socket_written")),
        _int_value(event, "write_failed", int(state == "write_failed")),
        _int_value(event, "unknown", int(state == "unknown")),
        _int_value(event, "cancelled", int(state == "cancelled")),
        _int_value(event, "expired", int(state == "expired")),
        _int_value(event, "response_success", int(state == "submit_sm_resp_success")),
        _int_value(event, "response_error", int(state == "submit_sm_resp_error")),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-rebuild a VNext projection from immutable progress journals."
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--seed-projection",
        type=Path,
        default=None,
        help="read historical progress rows from a WAL-free projection base into raw staging",
    )
    parser.add_argument(
        "--service-unit",
        default=os.environ.get("MC_SERVICE_UNIT", "marketing-copilot.service"),
    )
    parser.add_argument("--workers", type=int, default=int(os.environ.get("MC_VNEXT_WORKERS", "4")))
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("MC_VNEXT_PROJECTION_BULK_BATCH_SIZE", "4096")),
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=int(os.environ.get("MC_VNEXT_PROJECTION_MAINTENANCE_THREADS", "1")),
    )
    parser.add_argument(
        "--memory-limit",
        default=os.environ.get("MC_VNEXT_PROJECTION_MAINTENANCE_MEMORY_LIMIT", "16GB"),
    )
    parser.add_argument(
        "--checkpoint-every-events",
        type=int,
        default=int(os.environ.get("MC_VNEXT_PROJECTION_MAINTENANCE_CHECKPOINT_EVENTS", "100000")),
    )
    parser.add_argument("--progress-seconds", type=float, default=30.0)
    parser.add_argument(
        "--resume-output",
        action="store_true",
        help="resume the raw-table load from output-root journal-offsets.json",
    )
    parser.add_argument(
        "--keep-staging",
        action="store_true",
        help="keep the raw staging table after finalization for forensic inspection",
    )
    return parser.parse_args()


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _seed_historical_progress(connection, source_path: Path) -> int:
    """Load old progress evidence without importing its indexed tables."""

    connection.execute(
        f"ATTACH {_sql_literal(source_path)} AS seed_projection (READ_ONLY)"
    )
    try:
        count = int(
            connection.execute(
                "SELECT COUNT(*) FROM seed_projection.vnext_progress_events"
            ).fetchone()[0]
        )
        connection.execute(
            f"""
            INSERT INTO {RAW_TABLE}({RAW_COLUMN_SQL})
            SELECT
                CAST(ROW_NUMBER() OVER () - 1 AS BIGINT),
                'progress',
                replace(COALESCE(event_id, ''), chr(0), ''),
                split_part(replace(COALESCE(event_id, ''), chr(0), ''), ':resp:', 1),
                COALESCE(partition_id, 0), COALESCE(ordinal, 0),
                replace(COALESCE(campaign_type, ''), chr(0), ''),
                COALESCE(campaign_id, 0), COALESCE(route_id, 0),
                replace(COALESCE(normalized_msisdn, ''), chr(0), ''),
                replace(COALESCE(language, ''), chr(0), ''),
                '',
                replace(COALESCE(state, ''), chr(0), ''),
                COALESCE(parts_written, 0), COALESCE(parts_total, 0),
                replace(COALESCE(message, ''), chr(0), ''),
                replace(COALESCE(error, ''), chr(0), ''),
                COALESCE(created_at, 0),
                replace(COALESCE(business_day, ''), chr(0), ''),
                COALESCE(expires_at, 0), COALESCE(created_at, 0),
                CASE WHEN state = 'queued' THEN 1 ELSE 0 END,
                CASE WHEN state = 'socket_written' THEN 1 ELSE 0 END,
                CASE WHEN state = 'socket_written' THEN 1 ELSE 0 END,
                CASE WHEN state = 'socket_written' THEN 1 ELSE 0 END,
                CASE WHEN state = 'write_failed' THEN 1 ELSE 0 END,
                CASE WHEN state = 'unknown' THEN 1 ELSE 0 END,
                CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END,
                CASE WHEN state = 'expired' THEN 1 ELSE 0 END,
                CASE WHEN state = 'submit_sm_resp_success' THEN 1 ELSE 0 END,
                CASE WHEN state = 'submit_sm_resp_error' THEN 1 ELSE 0 END
            FROM seed_projection.vnext_progress_events
            """
        )
        return count
    finally:
        connection.execute("DETACH seed_projection")


def _connect(database: Path, temp_directory: Path, threads: int, memory_limit: str):
    import duckdb

    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database))
    connection.execute(f"SET threads TO {max(1, min(8, int(threads)))}")
    connection.execute(f"SET memory_limit={_sql_literal(memory_limit)}")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(f"SET temp_directory={_sql_literal(str(temp_directory))}")
    # Avoid an implicit mixed-transaction checkpoint.  This command performs
    # explicit checkpoints only after a complete INSERT transaction or after
    # all derived tables have been built.
    try:
        connection.execute("SET wal_autocheckpoint='1TB'")
    except Exception:
        connection.execute("SET debug_skip_checkpoint_on_commit=true")
    return connection


def _create_raw_table(connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            ingest_seq BIGINT,
            event_kind VARCHAR,
            event_id VARCHAR,
            logical_task_id VARCHAR,
            partition_id INTEGER,
            ordinal BIGINT,
            campaign_type VARCHAR,
            campaign_id BIGINT,
            route_id BIGINT,
            normalized_msisdn VARCHAR,
            language VARCHAR,
            campaign_name VARCHAR,
            state VARCHAR,
            parts_written INTEGER,
            parts_total INTEGER,
            message VARCHAR,
            error VARCHAR,
            created_at DOUBLE,
            business_day VARCHAR,
            expires_at DOUBLE,
            event_at DOUBLE,
            queued BIGINT,
            taken BIGINT,
            accepted BIGINT,
            socket_written BIGINT,
            write_failed BIGINT,
            unknown BIGINT,
            cancelled BIGINT,
            expired BIGINT,
            response_success BIGINT,
            response_error BIGINT
        )
        """
    )


def _drop_if_exists(connection, *tables: str) -> None:
    for table in tables:
        connection.execute(f"DROP TABLE IF EXISTS {table}")


def _derive_tables(connection) -> None:
    """Create canonical tables from the unindexed raw relation."""

    _drop_if_exists(connection, UNIQUE_PROGRESS_TABLE, "vnext_progress_events", "vnext_projection", "vnext_task_index")
    ranked_columns = RAW_COLUMN_SQL
    connection.execute(
        f"""
        CREATE TABLE {UNIQUE_PROGRESS_TABLE} AS
        SELECT {ranked_columns}
        FROM (
            SELECT {ranked_columns},
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id ORDER BY ingest_seq
                   ) AS _rn
            FROM {RAW_TABLE}
            WHERE COALESCE(event_kind, '') <> 'task_index'
              AND COALESCE(event_id, '') <> ''
        ) AS ranked
        WHERE _rn = 1
        """
    )
    connection.execute(
        f"""
        CREATE TABLE vnext_progress_events AS
        SELECT {PROGRESS_COLUMN_SQL}
        FROM {UNIQUE_PROGRESS_TABLE}
        """
    )
    connection.execute(
        f"""
        CREATE TABLE vnext_projection AS
        SELECT business_day,campaign_type,campaign_id,partition_id,route_id,
               SUM(queued) AS queued,
               SUM(taken) AS taken,
               SUM(socket_written) AS socket_written,
               SUM(CASE WHEN state='socket_written' THEN parts_written ELSE 0 END)
                   AS socket_written_pdus,
               SUM(write_failed) AS write_failed,
               SUM(unknown) AS unknown,
               SUM(cancelled) AS cancelled,
               SUM(accepted) AS accepted,
               SUM(expired) AS expired,
               SUM(response_success) AS response_success,
               SUM(response_error) AS response_error,
               MAX(ordinal) AS last_ordinal,
               MAX(created_at) AS updated_at
        FROM {UNIQUE_PROGRESS_TABLE}
        GROUP BY business_day,campaign_type,campaign_id,partition_id,route_id
        """
    )
    connection.execute(
        f"""
        CREATE TABLE vnext_task_index AS
        SELECT logical_task_id AS event_id,
               partition_id,ordinal,campaign_type,campaign_id,route_id,
               normalized_msisdn,language,campaign_name,state,
               parts_written,parts_total,message,error,created_at,
               business_day,expires_at,event_at AS last_event_at
        FROM (
            SELECT {ranked_columns},
                   ROW_NUMBER() OVER (
                       PARTITION BY logical_task_id
                       ORDER BY event_at DESC, ingest_seq DESC
                   ) AS _rn
            FROM {RAW_TABLE}
            WHERE COALESCE(logical_task_id, '') <> ''
        ) AS ranked
        WHERE _rn = 1
        """
    )


def _create_indexes(connection) -> None:
    connection.execute(
        "ALTER TABLE vnext_progress_events ADD PRIMARY KEY (event_id)"
    )
    connection.execute(
        "ALTER TABLE vnext_projection ADD PRIMARY KEY "
        "(business_day,campaign_type,campaign_id,partition_id,route_id)"
    )
    connection.execute("ALTER TABLE vnext_task_index ADD PRIMARY KEY (event_id)")
    connection.execute(
        """CREATE INDEX idx_vnext_progress_day_order
           ON vnext_progress_events(business_day,created_at,ordinal,event_id)"""
    )
    connection.execute(
        """CREATE INDEX idx_vnext_progress_day_state_order
           ON vnext_progress_events(business_day,state,created_at,ordinal,event_id)"""
    )
    for name, columns in (
        ("idx_vnext_task_day_order", "business_day,created_at,ordinal,event_id"),
        ("idx_vnext_task_day_type_order", "business_day,campaign_type,created_at,ordinal,event_id"),
        ("idx_vnext_task_day_status_order", "business_day,state,created_at,ordinal,event_id"),
        ("idx_vnext_task_day_msisdn_order", "business_day,normalized_msisdn,created_at,ordinal,event_id"),
        ("idx_vnext_task_campaign_order", "campaign_type,campaign_id,created_at,ordinal,event_id"),
    ):
        connection.execute(
            f"CREATE INDEX {name} ON vnext_task_index({columns})"
        )


def _write_state(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    args = _parse_args()
    source_root = args.root.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()
    projection_root = output_root / "projection"
    db_path = projection_root / "vnext-projection.duckdb"
    offsets_path = projection_root / "journal-offsets.json"
    task_index_path = projection_root / "vnext-task-index.sqlite3"
    state_path = output_root / "bulk-rebuild-state.json"
    temp_directory = projection_root / "vnext-projection.duckdb.tmp"

    if _service_is_active(str(args.service_unit)):
        print(json.dumps({"ok": False, "error": "service_active"}), file=sys.stderr)
        return 2
    if output_root == source_root:
        print(json.dumps({"ok": False, "error": "output_must_be_separate"}), file=sys.stderr)
        return 2
    if output_root.exists() and not args.resume_output:
        existing = [path for path in output_root.iterdir() if path.name != ".keep"]
        if existing:
            print(
                json.dumps(
                    {"ok": False, "error": "output_root_not_empty", "paths": [str(path) for path in existing]},
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            return 2
    projection_root.mkdir(parents=True, exist_ok=True)

    journals = tuple(
        ProgressJournal(path)
        for path in _journal_paths(source_root / "progress", max(1, int(args.workers)))
    )
    offsets = _load_offsets(offsets_path)
    before = _storage(journals, offsets)
    batch_size = max(256, min(20_000, int(args.batch_size)))
    checkpoint_every = max(0, int(args.checkpoint_every_events))
    next_checkpoint = checkpoint_every
    cursor = 0
    ingest_seq = 0
    applied = 0
    seeded_rows = 0
    started = time.monotonic()
    last_report = started
    connection = None
    phase = "load"

    try:
        import duckdb

        connection = _connect(
            db_path,
            temp_directory,
            int(args.threads),
            str(args.memory_limit),
        )
        _create_raw_table(connection)
        if args.seed_projection is not None:
            seed_projection = args.seed_projection.expanduser().resolve()
            if not seed_projection.is_file():
                raise FileNotFoundError(seed_projection)
            seeded_rows = _seed_historical_progress(connection, seed_projection)
            ingest_seq = seeded_rows
        if args.resume_output:
            ingest_seq = int(
                connection.execute(
                    f"SELECT COALESCE(MAX(ingest_seq) + 1, 0) FROM {RAW_TABLE}"
                ).fetchone()[0]
            )
        else:
            connection.execute("CHECKPOINT")

        while True:
            events, candidates, cursor = _read_quantum(
                journals, offsets, cursor, batch_size
            )
            if not events:
                break
            rows = [_raw_row(event, ingest_seq + index) for index, event in enumerate(events)]
            connection.execute("BEGIN TRANSACTION")
            connection.executemany(
                f"INSERT INTO {RAW_TABLE}({RAW_COLUMN_SQL}) VALUES ({RAW_PLACEHOLDERS})",
                rows,
            )
            connection.execute("COMMIT")
            ingest_seq += len(rows)
            applied += len(rows)
            offsets.update(candidates)
            _persist_offsets(offsets_path, offsets)
            if (
                checkpoint_every > 0
                and applied >= next_checkpoint
            ):
                connection.execute("CHECKPOINT")
                while next_checkpoint <= applied:
                    next_checkpoint += checkpoint_every
            now = time.monotonic()
            if now - last_report >= max(0.5, float(args.progress_seconds)):
                current = _storage(journals, offsets)
                print(
                    json.dumps(
                        {
                            "phase": "load",
                            "applied": applied,
                            "backlog_bytes": current["backlog_bytes"],
                            "backlog_events_estimate": current["backlog_events_estimate"],
                            "elapsed_seconds": round(now - started, 1),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                _write_state(
                    state_path,
                    {"phase": "load", "applied": applied, "offsets": str(offsets_path)},
                )
                last_report = now

        connection.execute("CHECKPOINT")
        phase = "derive"
        _write_state(state_path, {"phase": phase, "applied": applied})
        _derive_tables(connection)
        connection.execute("CHECKPOINT")
        phase = "index"
        _write_state(state_path, {"phase": phase, "applied": applied})
        _create_indexes(connection)
        connection.execute("CHECKPOINT")
        connection.close()
        connection = None

        # The SQLite read model is rebuilt only after the canonical DuckDB
        # table is complete and closed.  It is not part of the DuckDB write
        # transaction and cannot affect the production sender path.
        task_index_rebuilt = rebuild_from_duckdb(db_path, task_index_path)

        connection = duckdb.connect(str(db_path))
        connection.execute("SET threads TO 1")
        if not args.keep_staging:
            _drop_if_exists(connection, UNIQUE_PROGRESS_TABLE, RAW_TABLE)
            connection.execute("CHECKPOINT")
        counts = {
            "progress_events": int(connection.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0]),
            "projection_rows": int(connection.execute("SELECT COUNT(*) FROM vnext_projection").fetchone()[0]),
            "task_index_rows": int(connection.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0]),
        }
        connection.close()
        connection = None
        if not args.keep_staging:
            shutil.rmtree(temp_directory, ignore_errors=True)
        after = _storage(journals, offsets)
        result = {
            "ok": after["backlog_bytes"] == 0,
            "db_path": str(db_path),
            "output_root": str(output_root),
            "before": before,
            "after": after,
            "loaded_rows": applied,
            "seeded_rows": seeded_rows,
            "counts": counts,
            "task_index_rebuilt": task_index_rebuilt,
            "checkpointed": True,
            "staging_kept": bool(args.keep_staging),
            "elapsed_seconds": round(time.monotonic() - started, 1),
        }
        _write_state(state_path, result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0 if result["ok"] else 1
    except BaseException as exc:
        result = {
            "ok": False,
            "phase": phase,
            "error": f"{type(exc).__name__}: {exc}",
            "before": before,
            "current": _storage(journals, offsets),
            "loaded_rows": applied,
            "seeded_rows": seeded_rows,
        }
        _write_state(state_path, result)
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr, flush=True)
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
