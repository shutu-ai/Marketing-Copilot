"""Build and verify the VNext-only storage cutover.

This command is intentionally offline.  It copies the stopped application's
business database to a new path, runs the expensive schema/data maintenance
once, imports historical SMS tasks into the VNext logical-task index, merges
any already-persisted VNext progress evidence, and only then marks the new
business database as startup-ready.

The old source file is never opened by the running application after the
operator switches ``MC_CONTROL_DB_PATH`` to the target.  Keep the source as
an offline rollback archive until acceptance is complete.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import time
from pathlib import Path

import duckdb


CANONICAL_MARKERS = (
    "db_schema_columns_v1",
    "db_realtime_cleanup_v1",
    "db_processed_files_unique_v1",
    "db_data_normalization_v1",
    "db_receipt_indexes_v2",
    "sms_task_message_ids_backfill_v1",
    "db_canonical_ready_v1",
)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _epoch(value: object) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, dt.date):
        parsed = dt.datetime.combine(value, dt.time())
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            return float(text)
        except (TypeError, ValueError):
            try:
                parsed = dt.datetime.fromisoformat(text)
            except ValueError:
                return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _business_day(task_date: object, created_at: object) -> str:
    if task_date:
        return str(task_date)[:10]
    if isinstance(created_at, (dt.datetime, dt.date)):
        return str(created_at)[:10]
    text = str(created_at or "")
    return text[:10] if len(text) >= 10 and text[4:5] == "-" else ""


def _normalize_business_day(value: object) -> str:
    """Normalize an optional cutover day before it reaches SQL parameters."""

    text = str(value or "").strip()
    if not text:
        return ""
    return dt.date.fromisoformat(text[:10]).isoformat()


def _normalizer():
    from vnext.partition import normalize_msisdn

    return normalize_msisdn


def _ensure_target_business_db(target: Path) -> dict:
    """Run the normal schema/maintenance pass once on the copied database."""

    # The migration command is the only code path allowed to open the source
    # business database.  Runtime processes use MC_CONTROL_DB_PATH instead.
    os.environ["DUCKDB_PATH"] = str(target)
    os.environ["MC_CONTROL_DB_PATH"] = str(target)
    import database

    database.set_database_file(str(target))
    database.init_db()
    connection = database.get_connection()
    try:
        # A copied source may already contain the ready marker.  Clear it
        # while the target is still in the cutover window so a failed later
        # projection/spool rebuild cannot be mistaken for a deployable store.
        connection.execute(
            "DELETE FROM app_settings WHERE key=?",
            ("db_canonical_ready_v1",),
        )
        connection.commit()
        tables = int(connection.execute(
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_schema='main' AND table_type='BASE TABLE'"""
        ).fetchone()[0])
        task_count = int(connection.execute("SELECT COUNT(*) FROM sms_tasks").fetchone()[0])
        return {"table_count": tables, "sms_task_count": task_count}
    finally:
        connection.close()


def _connect_target_business_db(target: Path):
    """Open a cutover connection with the same offline extension config.

    ``_ensure_target_business_db`` leaves the database module's owner
    connection alive for the duration of the cutover.  Opening the same file
    with DuckDB's default configuration afterwards can make WAL replay fail
    with a mixed-configuration error, especially on offline SLES hosts.
    Reuse the application connection settings for every direct maintenance
    connection to the canonical business target.
    """

    import database

    return duckdb.connect(
        str(target),
        config=database.prepare_duckdb_connection(),
    )


def _prune_target_sms_tasks(target: Path, min_business_day: str) -> dict:
    """Remove pre-cutover task data from the copied canonical DB only.

    The legacy source is intentionally left untouched as an offline rollback
    archive.  The new control DB must not retain millions of obsolete task
    rows or their task-linked operational records, otherwise an accidental
    legacy query could still make the cutover look partially migrated.
    """

    cutoff = _normalize_business_day(min_business_day)
    if not cutoff:
        return {
            "cutoff_business_day": "",
            "expired_task_rows_removed": 0,
            "task_linked_rows_removed": {},
            "remaining_sms_task_rows": None,
        }

    connection = _connect_target_business_db(target)
    try:
        connection.execute(
            """CREATE TEMPORARY TABLE _cutover_expired_task_ids AS
               SELECT id
                 FROM sms_tasks
                WHERE TRY_CAST(task_date AS DATE) < CAST(? AS DATE)
                   OR (task_date IS NULL
                       AND TRY_CAST(created_at AS TIMESTAMP) < CAST(? AS TIMESTAMP))""",
            (cutoff, f"{cutoff} 00:00:00"),
        )
        expired_count = int(connection.execute(
            "SELECT COUNT(*) FROM _cutover_expired_task_ids"
        ).fetchone()[0])
        connection.execute(
            """CREATE TEMPORARY TABLE _cutover_expired_attempt_tokens AS
               SELECT attempt_token
                 FROM sms_send_attempts
                WHERE task_id IN (SELECT id FROM _cutover_expired_task_ids)"""
        )

        removals: dict[str, int] = {}
        delete_statements = (
            (
                "sms_send_parts",
                "attempt_token IN (SELECT attempt_token FROM _cutover_expired_attempt_tokens)",
            ),
            ("sms_message_bindings_v2", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("sms_task_message_ids", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("delivery_receipts", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("task_events", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("sms_result_events", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("sms_fast_batch_items", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("sms_send_attempts", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("failed_task_archive", "task_id IN (SELECT id FROM _cutover_expired_task_ids)"),
            ("sms_tasks", "id IN (SELECT id FROM _cutover_expired_task_ids)"),
        )
        for table, predicate in delete_statements:
            before = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            if before:
                connection.execute(f"DELETE FROM {table} WHERE {predicate}")
            after = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            removals[table] = max(0, before - after)

        before_manifests = int(connection.execute(
            "SELECT COUNT(*) FROM task_archive_manifests"
        ).fetchone()[0])
        connection.execute(
            "DELETE FROM task_archive_manifests WHERE task_date < CAST(? AS DATE)",
            (cutoff,),
        )
        after_manifests = int(connection.execute(
            "SELECT COUNT(*) FROM task_archive_manifests"
        ).fetchone()[0])
        removals["task_archive_manifests"] = max(0, before_manifests - after_manifests)
        connection.commit()
        remaining = int(connection.execute("SELECT COUNT(*) FROM sms_tasks").fetchone()[0])
        return {
            "cutoff_business_day": cutoff,
            "expired_task_rows_removed": expired_count,
            "task_linked_rows_removed": removals,
            "remaining_sms_task_rows": remaining,
        }
    finally:
        connection.close()


def _mark_canonical_ready(target: Path) -> dict:
    """Publish the startup-ready marker only after every cutover phase passes."""

    connection = _connect_target_business_db(target)
    try:
        for key in CANONICAL_MARKERS:
            connection.execute(
                """INSERT INTO app_settings(key,value) VALUES (?, 'completed')
                   ON CONFLICT(key) DO UPDATE SET value='completed'""",
                (key,),
            )
        connection.commit()
        return {
            "ready": True,
            "markers": list(CANONICAL_MARKERS),
        }
    finally:
        connection.close()


def _source_columns(source) -> set[str]:
    return {
        str(row[0])
        for row in source.execute("DESCRIBE sms_tasks").fetchall()
    }


def _source_column(columns: set[str], name: str) -> str:
    """Return a qualified legacy column or a NULL expression."""

    if name not in columns:
        return "NULL"
    return f"legacy.sms_tasks.{_quote_identifier(name)}"


def _sql_epoch(expression: str) -> str:
    """Match the Python epoch conversion without a per-row Python callback."""

    text = f"TRIM(COALESCE(CAST({expression} AS VARCHAR), ''))"
    return f"""CASE
        WHEN {text} = '' THEN 0.0
        WHEN TRY_CAST({text} AS DOUBLE) IS NOT NULL THEN TRY_CAST({text} AS DOUBLE)
        ELSE COALESCE(EPOCH(TRY_CAST({text} AS TIMESTAMP)), 0.0)
    END"""


def _sql_normalized_msisdn() -> str:
    """Match normalize_msisdn(value) for the empty-prefix cutover."""

    digits = "digits"
    return f"""CASE
        WHEN {digits} = '' THEN ''
        WHEN {digits} LIKE '00%' THEN
            CASE WHEN SUBSTR({digits}, 3, 1) = '0'
                 THEN SUBSTR({digits}, 4)
                 ELSE SUBSTR({digits}, 3)
            END
        WHEN {digits} LIKE '0%' THEN SUBSTR({digits}, 2)
        ELSE {digits}
    END"""


def _merge_progress_events(target, min_business_day: str = "") -> int:
    """Materialize existing VNext evidence into one row per logical task."""

    cutoff = _normalize_business_day(min_business_day)
    progress_filter = ""
    progress_params: tuple[str, ...] = ()
    if cutoff:
        # A previous interrupted migration may have left an old partial
        # projection.  Remove it from the new target before rebuilding the
        # retained window; the source/rollback database is never touched.
        for table in ("vnext_task_index", "vnext_progress_events", "vnext_projection"):
            target.execute(
                f"DELETE FROM {table} WHERE business_day='' OR business_day < ?",
                (cutoff,),
            )
        target.commit()
        progress_filter = "WHERE business_day <> '' AND business_day >= ?"
        progress_params = (cutoff,)

    result = target.execute(
        f"""
        INSERT INTO vnext_task_index(
            event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
            normalized_msisdn,language,campaign_name,state,parts_written,parts_total,
            message,error,created_at,business_day,expires_at,last_event_at
        )
        WITH ranked AS (
            SELECT
                split_part(event_id, ':resp:', 1) AS event_id,
                partition_id, ordinal, campaign_type, campaign_id, route_id,
                normalized_msisdn, language, '' AS campaign_name, state,
                parts_written, parts_total, message, error, created_at,
                business_day, expires_at, created_at AS last_event_at,
                row_number() OVER (
                    PARTITION BY split_part(event_id, ':resp:', 1)
                    ORDER BY created_at DESC,
                             CASE WHEN event_id LIKE '%:resp:%' THEN 1 ELSE 0 END DESC,
                             event_id DESC
                ) AS rank_no
            FROM vnext_progress_events
            {progress_filter}
        )
        SELECT event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
               normalized_msisdn,language,campaign_name,state,parts_written,parts_total,
               message,error,created_at,business_day,expires_at,last_event_at
          FROM ranked
         WHERE rank_no=1
        ON CONFLICT(event_id) DO UPDATE SET
            partition_id=excluded.partition_id,
            ordinal=excluded.ordinal,
            campaign_type=excluded.campaign_type,
            campaign_id=excluded.campaign_id,
            route_id=excluded.route_id,
            normalized_msisdn=COALESCE(NULLIF(excluded.normalized_msisdn,''),vnext_task_index.normalized_msisdn),
            language=COALESCE(NULLIF(excluded.language,''),vnext_task_index.language),
            state=excluded.state,
            parts_written=excluded.parts_written,
            parts_total=GREATEST(excluded.parts_total,vnext_task_index.parts_total),
            message=COALESCE(NULLIF(excluded.message,''),vnext_task_index.message),
            error=excluded.error,
            created_at=LEAST(vnext_task_index.created_at,excluded.created_at),
            business_day=COALESCE(NULLIF(excluded.business_day,''),vnext_task_index.business_day),
            expires_at=GREATEST(vnext_task_index.expires_at,excluded.expires_at),
            last_event_at=GREATEST(vnext_task_index.last_event_at,excluded.last_event_at)
        WHERE excluded.last_event_at >= vnext_task_index.last_event_at
        """,
        progress_params,
    )
    del result
    return int(target.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0])


def _legacy_cutoff_clause(columns: set[str], min_business_day: str) -> tuple[str, tuple[str, ...]]:
    cutoff = _normalize_business_day(min_business_day)
    if not cutoff:
        return "TRUE", ()
    task_date = _source_column(columns, "task_date")
    created_at = _source_column(columns, "created_at")
    if "task_date" in columns:
        task_day = f"TRY_CAST({task_date} AS DATE)"
        if "created_at" in columns:
            created_time = f"TRY_CAST({created_at} AS TIMESTAMP)"
            return (
                f"({task_day} >= CAST(? AS DATE) OR ({task_day} IS NULL "
                f"AND {created_time} >= CAST(? AS TIMESTAMP)))",
                (cutoff, f"{cutoff} 00:00:00"),
            )
        return f"{task_day} >= CAST(? AS DATE)", (cutoff,)
    if "created_at" in columns:
        return (
            f"TRY_CAST({created_at} AS TIMESTAMP) >= CAST(? AS TIMESTAMP)",
            (f"{cutoff} 00:00:00",),
        )
    raise RuntimeError(
        "legacy sms_tasks has no task_date or created_at column for the cutover cutoff"
    )


def _migrate_sms_tasks(
    source_path: Path,
    target_path: Path,
    batch_size: int,
    min_business_day: str = "",
) -> dict:
    from vnext.projection_writer import ProjectionWriter

    # Bootstrap the new projection schema once without starting a worker or a
    # second long-lived owner.  Existing VNext progress evidence is preserved.
    target_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap = ProjectionWriter(str(target_path))
    bootstrap.start()
    bootstrap.stop()
    target = duckdb.connect(str(target_path))
    attached = False
    try:
        from vnext.projection_writer import ensure_task_index_schema

        ensure_task_index_schema(target)
        source_literal = str(source_path).replace("'", "''")
        target.execute(f"ATTACH '{source_literal}' AS legacy (READ_ONLY)")
        attached = True
        columns = {
            str(row[0])
            for row in target.execute("DESCRIBE legacy.sms_tasks").fetchall()
        }
        if "id" not in columns:
            raise RuntimeError("legacy sms_tasks table has no id column")

        id_column = _source_column(columns, "id")
        cutoff_clause, cutoff_params = _legacy_cutoff_clause(columns, min_business_day)
        min_id, max_id, source_count = target.execute(
            f"""SELECT MIN({id_column}), MAX({id_column}), COUNT(*)
                  FROM legacy.sms_tasks
                 WHERE {cutoff_clause}""",
            cutoff_params,
        ).fetchone()

        # A failed earlier cutover can leave committed historical rows in the
        # projection.  They are rebuilt from the source below; VNext evidence
        # rows remain intact.
        target.execute(
            "DELETE FROM vnext_task_index WHERE event_id LIKE 'historical:%'"
        )
        target.commit()

        migrated = int(source_count or 0)
        if min_id is not None and max_id is not None and migrated:
            campaign_type = _source_column(columns, "campaign_type")
            campaign_id = _source_column(columns, "campaign_id")
            campaign_name = _source_column(columns, "campaign_name")
            msisdn = _source_column(columns, "msisdn")
            language = _source_column(columns, "language")
            status = _source_column(columns, "status")
            content = _source_column(columns, "content")
            error_msg = _source_column(columns, "error_msg")
            smpp_config_id = _source_column(columns, "smpp_config_id")
            transport_smpp_config_id = _source_column(columns, "transport_smpp_config_id")
            submit_sm_parts = _source_column(columns, "submit_sm_parts")
            created_at = _source_column(columns, "created_at")
            updated_at = _source_column(columns, "updated_at")
            task_date = _source_column(columns, "task_date")

            chunk_span = max(250_000, int(batch_size) * 20)
            lower = int(min_id) - 1
            upper_limit = int(max_id)
            while lower < upper_limit:
                upper = min(lower + chunk_span, upper_limit)
                target.execute(
                    f"""WITH source_rows AS (
                        SELECT
                            {id_column} AS task_id,
                            COALESCE(CAST({campaign_type} AS VARCHAR), '') AS campaign_type,
                            COALESCE(TRY_CAST({campaign_id} AS BIGINT), 0) AS campaign_id,
                            COALESCE(CAST({campaign_name} AS VARCHAR), '') AS campaign_name,
                            regexp_replace(
                                COALESCE(CAST({msisdn} AS VARCHAR), ''),
                                '[^0-9]', '', 'g'
                            ) AS digits,
                            COALESCE(CAST({language} AS VARCHAR), '') AS language,
                            COALESCE(NULLIF(CAST({status} AS VARCHAR), ''), 'unknown') AS state,
                            COALESCE(CAST({content} AS VARCHAR), '') AS message,
                            COALESCE(CAST({error_msg} AS VARCHAR), '') AS error,
                            COALESCE(NULLIF(TRY_CAST({transport_smpp_config_id} AS BIGINT), 0),
                                     NULLIF(TRY_CAST({smpp_config_id} AS BIGINT), 0), 0) AS route_id,
                            GREATEST(COALESCE(TRY_CAST({submit_sm_parts} AS BIGINT), 0), 0)
                                AS parts_total,
                            TRIM(COALESCE(CAST({created_at} AS VARCHAR), '')) AS created_text,
                            TRIM(COALESCE(CAST({updated_at} AS VARCHAR), '')) AS updated_text,
                            TRIM(COALESCE(CAST({task_date} AS VARCHAR), '')) AS task_day
                        FROM legacy.sms_tasks
                        WHERE {id_column} > ? AND {id_column} <= ?
                          AND {cutoff_clause}
                    ), normalized AS (
                        SELECT
                            task_id, campaign_type, campaign_id, campaign_name, language,
                            state, message, error, route_id, parts_total,
                            {_sql_epoch('created_text')} AS created_at,
                            {_sql_epoch('updated_text')} AS updated_at,
                            CASE WHEN task_day <> '' THEN SUBSTR(task_day, 1, 10)
                                 WHEN regexp_matches(created_text, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')
                                 THEN SUBSTR(created_text, 1, 10)
                                 ELSE ''
                            END AS business_day,
                            {_sql_normalized_msisdn()} AS normalized_msisdn
                        FROM source_rows
                    )
                    INSERT INTO vnext_task_index(
                        event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                        normalized_msisdn,language,campaign_name,state,parts_written,parts_total,
                        message,error,created_at,business_day,expires_at,last_event_at
                    )
                    SELECT
                        'historical:' || CAST(task_id AS VARCHAR), 0, task_id,
                        campaign_type, campaign_id, route_id, normalized_msisdn, language,
                        campaign_name, state, 0, parts_total, message, error, created_at,
                        business_day, 0.0, COALESCE(NULLIF(updated_at, 0.0), created_at)
                    FROM normalized
                    ON CONFLICT(event_id) DO NOTHING""",
                    (lower, upper, *cutoff_params),
                )
                target.commit()
                print(
                    f"vectorized sms_tasks migration through id {upper}/{upper_limit}",
                    flush=True,
                )
                lower = upper

        return {
            "source_sms_task_rows_seen": migrated,
            "target_task_index_rows": int(target.execute(
                "SELECT COUNT(*) FROM vnext_task_index"
            ).fetchone()[0]),
        }
    finally:
        if attached:
            try:
                target.execute("DETACH legacy")
            except duckdb.Error:
                pass
        target.close()


def _rebuild_task_index_snapshot(projection_path: Path, batch_size: int) -> dict:
    from vnext.task_index import rebuild_from_duckdb

    return rebuild_from_duckdb(
        projection_path,
        batch_size=max(1000, int(batch_size)),
    )


def _rebuild_spool_index(spool_root: Path) -> dict:
    """Perform the one-time full spool scan outside application startup."""

    manifest_path = spool_root / "manifest.json"
    if not manifest_path.is_file():
        return {"root": str(spool_root), "ready": False, "reason": "manifest_missing"}
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        partitions = int(manifest.get("virtual_partition_count", 0) or 0)
    except (OSError, ValueError, TypeError) as exc:
        raise RuntimeError(f"invalid spool manifest: {manifest_path}: {exc}") from exc
    if partitions <= 0:
        raise RuntimeError(f"invalid spool virtual_partition_count: {partitions}")
    from vnext.spool import TaskSpool

    spool = TaskSpool(spool_root, virtual_partition_count=partitions)
    result = spool.rebuild_recovery_index()
    result["root"] = str(spool_root)
    return result


def _copy_business_db(source: Path, target: Path, replace: bool) -> None:
    source = source.resolve()
    target = target.resolve()
    if source == target:
        raise ValueError("source and target database paths must differ")
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if not replace:
            raise FileExistsError(f"target already exists: {target} (use --replace)")
        target.unlink()
    shutil.copy2(source, target)


def run(args) -> dict:
    source = Path(args.source).expanduser().resolve()
    business_target = Path(
        getattr(args, "control_target", "") or getattr(args, "business_target", "")
    ).expanduser().resolve()
    projection_target = Path(args.projection_target).expanduser().resolve()
    started = time.perf_counter()
    _copy_business_db(source, business_target, bool(args.replace))
    business = _ensure_target_business_db(business_target)
    min_business_day = _normalize_business_day(
        getattr(args, "min_business_day", "")
    )
    business_prune = _prune_target_sms_tasks(
        business_target, min_business_day,
    )
    if business_prune.get("remaining_sms_task_rows") is not None:
        business["sms_task_count_before_cutover_prune"] = business.get("sms_task_count")
        business["sms_task_count"] = business_prune["remaining_sms_task_rows"]
    task_index = _migrate_sms_tasks(
        source,
        projection_target,
        max(1000, int(args.batch_size)),
        min_business_day=min_business_day,
    )
    projection = duckdb.connect(str(projection_target))
    try:
        progress_rows = _merge_progress_events(
            projection, min_business_day=min_business_day,
        )
        projection.commit()
    finally:
        projection.close()
    task_index_snapshot = _rebuild_task_index_snapshot(
        projection_target, max(1000, int(args.batch_size))
    )
    spool_index = None
    if getattr(args, "spool_root", ""):
        spool_index = _rebuild_spool_index(
            Path(args.spool_root).expanduser().resolve()
        )
    canonical = _mark_canonical_ready(business_target)
    result = {
        "source": str(source),
        "min_business_day": min_business_day,
        "business_target": str(business_target),
        "projection_target": str(projection_target),
        "business": business,
        "business_cutover_prune": business_prune,
        "task_index": task_index,
        "projection_task_index_rows_after_progress_merge": progress_rows,
        "task_index_snapshot": task_index_snapshot,
        "spool_recovery_index": spool_index,
        "canonical": canonical,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if args.manifest:
        manifest = Path(args.manifest).expanduser().resolve()
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="stopped legacy/business DuckDB snapshot")
    parser.add_argument(
        "--control-target", "--business-target", dest="control_target", required=True,
        help="new canonical control-plane DuckDB path",
    )
    parser.add_argument("--projection-target", required=True, help="new VNext projection DuckDB path")
    parser.add_argument("--spool-root", default="", help="optional stopped-service spool root to index once")
    parser.add_argument(
        "--min-business-day",
        default="",
        help="do not import task/projection records before this ISO business day",
    )
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--manifest", default="")
    parser.add_argument("--replace", action="store_true", help="replace an existing target after path validation")
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
