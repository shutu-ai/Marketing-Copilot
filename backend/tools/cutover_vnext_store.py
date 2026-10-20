"""Build and verify the VNext-only storage cutover.

This command is intentionally offline.  It copies the stopped application's
business database to a new path, runs the expensive schema/data maintenance
once, imports historical SMS tasks into the VNext logical-task index, merges
any already-persisted VNext progress evidence, and only then marks the new
business database as startup-ready.

The old source file is never opened by the running application after the
operator switches ``MC_DB_CONFIG_FILE``/``DUCKDB_PATH`` to the target.  Keep
the source as an offline rollback archive until acceptance is complete.
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


def _normalizer():
    from vnext.partition import normalize_msisdn

    return normalize_msisdn


def _ensure_target_business_db(target: Path) -> dict:
    """Run the normal schema/maintenance pass once on the copied database."""

    os.environ["DUCKDB_PATH"] = str(target)
    import database

    database.set_database_file(str(target))
    database.init_db()
    connection = database.get_connection()
    try:
        for key in CANONICAL_MARKERS:
            connection.execute(
                """INSERT INTO app_settings(key,value) VALUES (?, 'completed')
                   ON CONFLICT(key) DO UPDATE SET value='completed'""",
                (key,),
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


def _source_columns(source) -> set[str]:
    return {
        str(row[0])
        for row in source.execute("DESCRIBE sms_tasks").fetchall()
    }


def _source_task_rows(source, last_id: int, batch_size: int, columns: set[str]):
    fields = {
        "id": "id",
        "campaign_type": "campaign_type",
        "campaign_id": "campaign_id",
        "campaign_name": "campaign_name",
        "msisdn": "msisdn",
        "language": "language",
        "status": "status",
        "content": "content",
        "error_msg": "error_msg",
        "smpp_config_id": "smpp_config_id",
        "transport_smpp_config_id": "transport_smpp_config_id",
        "submit_sm_parts": "submit_sm_parts",
        "created_at": "created_at",
        "updated_at": "updated_at",
        "task_date": "task_date",
    }
    select = []
    for alias, column in fields.items():
        if column in columns:
            select.append(f"{_quote_identifier(column)} AS {_quote_identifier(alias)}")
        else:
            select.append(f"NULL AS {_quote_identifier(alias)}")
    return source.execute(
        f"""SELECT {', '.join(select)}
               FROM sms_tasks
              WHERE id > ?
              ORDER BY id
              LIMIT ?""",
        (int(last_id), int(batch_size)),
    ).fetchall()


def _merge_progress_events(target) -> int:
    """Materialize existing VNext evidence into one row per logical task."""

    result = target.execute(
        """
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
        """
    )
    del result
    return int(target.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0])


def _migrate_sms_tasks(source_path: Path, target_path: Path, batch_size: int) -> dict:
    normalizer = _normalizer()
    from vnext.projection_writer import ProjectionWriter

    # Bootstrap the new projection schema once without starting a worker or a
    # second long-lived owner.  Existing VNext progress evidence is preserved.
    target_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap = ProjectionWriter(str(target_path))
    bootstrap.start()
    bootstrap.stop()
    source = duckdb.connect(str(source_path), read_only=True)
    target = duckdb.connect(str(target_path))
    try:
        from vnext.projection_writer import ensure_task_index_schema

        ensure_task_index_schema(target)
        columns = _source_columns(source)
        last_id = 0
        migrated = 0
        while True:
            rows = _source_task_rows(source, last_id, batch_size, columns)
            if not rows:
                break
            values = []
            for row in rows:
                item = dict(zip(
                    (
                        "id", "campaign_type", "campaign_id", "campaign_name", "msisdn",
                        "language", "status", "content", "error_msg", "smpp_config_id",
                        "transport_smpp_config_id", "submit_sm_parts", "created_at", "updated_at",
                        "task_date",
                    ),
                    row,
                ))
                task_id = int(item["id"] or 0)
                last_id = max(last_id, task_id)
                created_at = _epoch(item["created_at"])
                updated_at = _epoch(item["updated_at"]) or created_at
                values.append((
                    f"historical:{task_id}", 0, task_id,
                    str(item["campaign_type"] or ""), int(item["campaign_id"] or 0),
                    int(item["transport_smpp_config_id"] or item["smpp_config_id"] or 0),
                    normalizer(item["msisdn"]), str(item["language"] or ""),
                    str(item["campaign_name"] or ""), str(item["status"] or "unknown"),
                    0, max(0, int(item["submit_sm_parts"] or 0)),
                    str(item["content"] or ""), str(item["error_msg"] or ""),
                    created_at, _business_day(item["task_date"], item["created_at"]), 0.0, updated_at,
                ))
            target.executemany(
                """INSERT INTO vnext_task_index(
                    event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                    normalized_msisdn,language,campaign_name,state,parts_written,parts_total,
                    message,error,created_at,business_day,expires_at,last_event_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(event_id) DO NOTHING""",
                values,
            )
            migrated += len(values)
            if len(rows) < batch_size:
                break
        target.commit()
        return {
            "source_sms_task_rows_seen": migrated,
            "target_task_index_rows": int(target.execute(
                "SELECT COUNT(*) FROM vnext_task_index"
            ).fetchone()[0]),
        }
    finally:
        target.close()
        source.close()


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
    business_target = Path(args.business_target).expanduser().resolve()
    projection_target = Path(args.projection_target).expanduser().resolve()
    started = time.perf_counter()
    _copy_business_db(source, business_target, bool(args.replace))
    business = _ensure_target_business_db(business_target)
    task_index = _migrate_sms_tasks(source, projection_target, max(1000, int(args.batch_size)))
    projection = duckdb.connect(str(projection_target))
    try:
        progress_rows = _merge_progress_events(projection)
        projection.commit()
    finally:
        projection.close()
    result = {
        "source": str(source),
        "business_target": str(business_target),
        "projection_target": str(projection_target),
        "business": business,
        "task_index": task_index,
        "projection_task_index_rows_after_progress_merge": progress_rows,
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
    parser.add_argument("--business-target", required=True, help="new runtime business DuckDB path")
    parser.add_argument("--projection-target", required=True, help="new VNext projection DuckDB path")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--manifest", default="")
    parser.add_argument("--replace", action="store_true", help="replace an existing target after path validation")
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
