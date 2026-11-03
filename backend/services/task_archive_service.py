"""Archive terminal SMS task partitions to Parquet before hot-table deletion."""

import os
import logging
import shutil
import tempfile
from datetime import datetime, timedelta
from typing import Optional

from database import get_connection, get_database_file, write_transaction


TERMINAL_STATUSES = (
    "delivered", "failed", "expired", "cancelled", "archived",
    "accepted", "unknown", "partial", "submitted",
)
ACTIVE_TASK_STATUSES = ("queued", "sending", "retrying", "submitted")
logger = logging.getLogger(__name__)


def _archive_root() -> str:
    configured = os.environ.get("MC_TASK_ARCHIVE_DIR", "").strip()
    if configured:
        return os.path.abspath(configured)
    return os.path.join(os.path.dirname(os.path.abspath(get_database_file())), "archives", "tasks")


def _sql_path(path: str) -> str:
    return "'" + os.path.abspath(path).replace("'", "''") + "'"


def delete_task_artifacts(conn, task_ids, *, delete_tasks: bool = True) -> dict:
    """Delete send evidence together with its task rows in dependency order."""

    ids = sorted({int(task_id) for task_id in task_ids if task_id is not None})
    if not ids:
        return {"tasks": 0, "attempts": 0, "parts": 0, "fast_items": 0}
    placeholders = ",".join("?" for _ in ids)
    attempt_rows = conn.execute(
        f"SELECT attempt_token FROM sms_send_attempts WHERE task_id IN ({placeholders})",
        ids,
    ).fetchall()
    attempt_tokens = [str(row["attempt_token"]) for row in attempt_rows if row["attempt_token"]]
    batch_rows = conn.execute(
        f"SELECT DISTINCT batch_id FROM sms_fast_batch_items WHERE task_id IN ({placeholders})",
        ids,
    ).fetchall()
    batch_ids = [str(row["batch_id"]) for row in batch_rows if row["batch_id"]]

    parts_removed = 0
    if attempt_tokens:
        attempt_placeholders = ",".join("?" for _ in attempt_tokens)
        parts_removed = len(conn.execute(
            f"DELETE FROM sms_send_parts WHERE attempt_token IN ({attempt_placeholders}) RETURNING attempt_token",
            attempt_tokens,
        ).fetchall())
        conn.execute(
            f"DELETE FROM sms_message_bindings_v2 WHERE attempt_token IN ({attempt_placeholders})",
            attempt_tokens,
        )
    conn.execute(f"DELETE FROM sms_message_bindings_v2 WHERE task_id IN ({placeholders})", ids)
    fast_items_removed = len(conn.execute(
        f"DELETE FROM sms_fast_batch_items WHERE task_id IN ({placeholders}) RETURNING task_id",
        ids,
    ).fetchall())
    conn.execute(f"DELETE FROM sms_result_events WHERE task_id IN ({placeholders})", ids)
    if attempt_tokens:
        conn.execute(
            f"DELETE FROM sms_send_attempts WHERE attempt_token IN ({attempt_placeholders})",
            attempt_tokens,
        )
    conn.execute(f"DELETE FROM sms_task_message_ids WHERE task_id IN ({placeholders})", ids)
    conn.execute(f"DELETE FROM delivery_receipts WHERE task_id IN ({placeholders})", ids)
    conn.execute(f"DELETE FROM task_events WHERE task_id IN ({placeholders})", ids)
    if batch_ids:
        batch_placeholders = ",".join("?" for _ in batch_ids)
        conn.execute(
            f"""DELETE FROM sms_fast_batches
                WHERE batch_id IN ({batch_placeholders})
                  AND NOT EXISTS (
                    SELECT 1 FROM sms_fast_batch_items i WHERE i.batch_id=sms_fast_batches.batch_id
                  )""",
            batch_ids,
        )
    tasks_removed = 0
    if delete_tasks:
        tasks_removed = len(conn.execute(
            f"DELETE FROM sms_tasks WHERE id IN ({placeholders}) RETURNING id", ids
        ).fetchall())
    return {
        "tasks": tasks_removed,
        "attempts": len(attempt_tokens),
        "parts": parts_removed,
        "fast_items": fast_items_removed,
    }


def cleanup_orphan_task_artifacts(retention_days: int | None = None, *, limit: int = 1000) -> dict:
    """Remove stale evidence left by pre-retention task deletion paths."""

    from services import settings_service

    days = settings_service.get_data_retention_days(retention_days)
    batch_limit = max(100, min(10000, int(limit)))
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    def cleanup(conn):
        attempt_rows = conn.execute(
            f"""SELECT a.attempt_token FROM sms_send_attempts a
                LEFT JOIN sms_tasks t ON t.id=a.task_id
                WHERE a.created_at < ?
                  AND (t.id IS NULL OR (t.status NOT IN ({','.join(repr(s) for s in ACTIVE_TASK_STATUSES)})
                       AND TRY_CAST(t.updated_at AS TIMESTAMP) < TRY_CAST(? AS TIMESTAMP)))
                  AND (a.lease_until IS NULL OR TRY_CAST(a.lease_until AS TIMESTAMPTZ) IS NULL
                       OR TRY_CAST(a.lease_until AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP)
                ORDER BY a.created_at LIMIT ?""",
            (cutoff, cutoff, batch_limit),
        ).fetchall()
        attempt_tokens = [str(row["attempt_token"]) for row in attempt_rows if row["attempt_token"]]
        attempts_removed = 0
        parts_removed = 0
        if attempt_tokens:
            placeholders = ",".join("?" for _ in attempt_tokens)
            parts_removed = len(conn.execute(
                f"DELETE FROM sms_send_parts WHERE attempt_token IN ({placeholders}) RETURNING attempt_token",
                attempt_tokens,
            ).fetchall())
            conn.execute(
                f"DELETE FROM sms_message_bindings_v2 WHERE attempt_token IN ({placeholders})",
                attempt_tokens,
            )
            attempts_removed = len(conn.execute(
                f"DELETE FROM sms_send_attempts WHERE attempt_token IN ({placeholders}) RETURNING attempt_token",
                attempt_tokens,
            ).fetchall())
        orphan_part_rows = conn.execute(
            """SELECT p.attempt_token,p.part_no FROM sms_send_parts p
               WHERE p.updated_at < ?
                 AND NOT EXISTS (SELECT 1 FROM sms_send_attempts a
                                WHERE a.attempt_token=p.attempt_token)
               ORDER BY p.updated_at LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
        orphan_parts = 0
        if orphan_part_rows:
            conn.executemany(
                "DELETE FROM sms_send_parts WHERE attempt_token=? AND part_no=?",
                [(row["attempt_token"], row["part_no"]) for row in orphan_part_rows],
            )
            orphan_parts = len(orphan_part_rows)

        def delete_keyed(table: str, columns: tuple[str, ...], query: str, params: tuple) -> int:
            rows = conn.execute(query, [*params, batch_limit]).fetchall()
            if not rows:
                return 0
            where = " AND ".join(f"{column}=?" for column in columns)
            conn.executemany(
                f"DELETE FROM {table} WHERE {where}",
                [tuple(row[column] for column in columns) for row in rows],
            )
            return len(rows)

        orphan_bindings = delete_keyed(
            "sms_message_bindings_v2",
            ("source_namespace", "smsc_message_id", "task_id", "attempt_token", "part_no"),
            """SELECT b.source_namespace,b.smsc_message_id,b.task_id,b.attempt_token,b.part_no
               FROM sms_message_bindings_v2 b
               WHERE b.created_at < ?
                 AND (NOT EXISTS (SELECT 1 FROM sms_tasks t WHERE t.id=b.task_id)
                      OR NOT EXISTS (SELECT 1 FROM sms_send_attempts a
                                     WHERE a.attempt_token=b.attempt_token))
               ORDER BY b.created_at LIMIT ?""",
            (cutoff,),
        )
        orphan_events = delete_keyed(
            "sms_result_events", ("event_id",),
            """SELECT e.event_id FROM sms_result_events e
               WHERE e.created_at < ?
                 AND (e.task_id IS NULL OR NOT EXISTS (
                    SELECT 1 FROM sms_tasks t WHERE t.id=e.task_id
                 ))
               ORDER BY e.created_at LIMIT ?""",
            (cutoff,),
        )
        orphan_message_ids = delete_keyed(
            "sms_task_message_ids", ("smsc_message_id",),
            """SELECT m.smsc_message_id FROM sms_task_message_ids m
               WHERE m.created_at < ?
                 AND NOT EXISTS (SELECT 1 FROM sms_tasks t WHERE t.id=m.task_id)
               ORDER BY m.created_at LIMIT ?""",
            (cutoff,),
        )
        orphan_receipts = delete_keyed(
            "delivery_receipts", ("id",),
            """SELECT r.id FROM delivery_receipts r
               WHERE r.received_at < ?
                 AND (r.task_id IS NULL OR NOT EXISTS (
                    SELECT 1 FROM sms_tasks t WHERE t.id=r.task_id
                 ))
               ORDER BY r.received_at LIMIT ?""",
            (cutoff,),
        )
        orphan_task_events = delete_keyed(
            "task_events", ("id",),
            """SELECT e.id FROM task_events e
               WHERE e.created_at < ?
                 AND NOT EXISTS (SELECT 1 FROM sms_tasks t WHERE t.id=e.task_id)
               ORDER BY e.created_at LIMIT ?""",
            (cutoff,),
        )
        fast_item_rows = conn.execute(
            """SELECT i.batch_id FROM sms_fast_batch_items i
               WHERE i.created_at < ?
                 AND NOT EXISTS (SELECT 1 FROM sms_tasks t WHERE t.id=i.task_id)
               ORDER BY i.created_at LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
        fast_batch_ids = [str(row["batch_id"]) for row in fast_item_rows if row["batch_id"]]
        fast_items_removed = 0
        if fast_batch_ids:
            item_placeholders = ",".join("?" for _ in fast_batch_ids)
            fast_items_removed = len(conn.execute(
                f"""DELETE FROM sms_fast_batch_items i
                    WHERE i.batch_id IN ({item_placeholders})
                      AND i.created_at < ?
                      AND NOT EXISTS (SELECT 1 FROM sms_tasks t WHERE t.id=i.task_id)
                    RETURNING batch_id""",
                [*fast_batch_ids, cutoff],
            ).fetchall())
        stale_batch_rows = conn.execute(
            """SELECT b.batch_id FROM sms_fast_batches b
               WHERE b.updated_at < ?
                 AND NOT EXISTS (SELECT 1 FROM sms_fast_batch_items i WHERE i.batch_id=b.batch_id)
                 AND (b.state<>'claimed' OR b.lease_until IS NULL
                      OR TRY_CAST(b.lease_until AS TIMESTAMPTZ) IS NULL
                      OR TRY_CAST(b.lease_until AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP)
               ORDER BY b.updated_at LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
        stale_batch_ids = [str(row["batch_id"]) for row in stale_batch_rows if row["batch_id"]]
        fast_batches_removed = 0
        if stale_batch_ids:
            placeholders = ",".join("?" for _ in stale_batch_ids)
            fast_batches_removed = len(conn.execute(
                f"DELETE FROM sms_fast_batches WHERE batch_id IN ({placeholders}) RETURNING batch_id",
                stale_batch_ids,
            ).fetchall())
        return {
            "attempts": attempts_removed,
            "parts": parts_removed + orphan_parts,
            "bindings": orphan_bindings,
            "result_events": orphan_events,
            "message_ids": orphan_message_ids,
            "receipts": orphan_receipts,
            "task_events": orphan_task_events,
            "fast_items": fast_items_removed,
            "fast_batches": fast_batches_removed,
        }

    result = write_transaction(
        cleanup,
        attempts=3,
        operation="task_artifacts.orphan_cleanup",
        lock_timeout=1.0,
    )
    return {"retention_days": days, **result}


def _copy_query(conn, query: str, path: str) -> None:
    conn.execute(
        f"COPY ({query}) TO {_sql_path(path)} "
        "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 100000)"
    )


def archive_terminal_tasks(cutoff: str, limit: int = 5000) -> dict:
    """Archive one date/campaign partition and atomically remove exactly those rows."""
    limit = max(1000, min(20000, int(limit)))
    terminal_sql = ",".join(repr(status) for status in TERMINAL_STATUSES)
    conn = get_connection()
    temporary_dir = None
    try:
        partition = conn.execute(
            """SELECT task_date,campaign_type,campaign_id FROM sms_tasks
               WHERE created_at<? AND status IN (""" + terminal_sql + """)
               ORDER BY id LIMIT 1""",
            (cutoff,),
        ).fetchone()
        if not partition:
            return {"archived": 0, "archive_key": "", "root_path": ""}
        rows = conn.execute(
            """SELECT id FROM sms_tasks
               WHERE created_at<? AND status IN (""" + terminal_sql + """ )
                 AND task_date=? AND campaign_type=? AND campaign_id=?
               ORDER BY id LIMIT ?""",
            (cutoff, partition["task_date"], partition["campaign_type"],
             partition["campaign_id"], limit),
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if not ids:
            return {"archived": 0, "archive_key": "", "root_path": ""}
        first_id, last_id = ids[0], ids[-1]
        archive_key = (
            f"{partition['task_date']}-{partition['campaign_type']}-"
            f"{partition['campaign_id']}-{first_id}-{last_id}"
        )
        root = _archive_root()
        partition_dir = os.path.join(
            root, f"task_date={partition['task_date']}",
            f"campaign_type={partition['campaign_type']}",
            f"campaign_id={partition['campaign_id']}",
        )
        final_dir = os.path.join(partition_dir, archive_key)
        os.makedirs(partition_dir, exist_ok=True)
        placeholders = ",".join(str(task_id) for task_id in ids)
        if not os.path.isdir(final_dir):
            temporary_dir = tempfile.mkdtemp(prefix=f".{archive_key}-", dir=partition_dir)
            _copy_query(conn, f"SELECT * FROM sms_tasks WHERE id IN ({placeholders}) ORDER BY id",
                        os.path.join(temporary_dir, "tasks.parquet"))
            _copy_query(conn, f"SELECT * FROM sms_task_message_ids WHERE task_id IN ({placeholders}) ORDER BY task_id",
                        os.path.join(temporary_dir, "message_ids.parquet"))
            _copy_query(conn, f"SELECT * FROM delivery_receipts WHERE task_id IN ({placeholders}) ORDER BY task_id,id",
                        os.path.join(temporary_dir, "receipts.parquet"))
            _copy_query(conn, f"SELECT * FROM task_events WHERE task_id IN ({placeholders}) ORDER BY task_id,id",
                        os.path.join(temporary_dir, "events.parquet"))
            os.replace(temporary_dir, final_dir)
            temporary_dir = None
        for filename in ("tasks.parquet", "message_ids.parquet", "receipts.parquet", "events.parquet"):
            if not os.path.isfile(os.path.join(final_dir, filename)):
                raise RuntimeError(f"incomplete task archive {archive_key}: missing {filename}")
        verification = conn.execute(
            f"SELECT COUNT(*) count,MIN(id) first_id,MAX(id) last_id "
            f"FROM read_parquet({_sql_path(os.path.join(final_dir, 'tasks.parquet'))})"
        ).fetchone()
        if (int(verification["count"] or 0) != len(ids)
                or int(verification["first_id"] or 0) != first_id
                or int(verification["last_id"] or 0) != last_id):
            raise RuntimeError(f"task archive verification failed for {archive_key}")
        conn.execute(
            """INSERT INTO task_archive_manifests
               (archive_key,task_date,campaign_type,campaign_id,first_task_id,last_task_id,
                task_count,root_path,status)
               VALUES (?,?,?,?,?,?,?,?, 'completed')
               ON CONFLICT (archive_key) DO NOTHING""",
            (archive_key, partition["task_date"], partition["campaign_type"],
             partition["campaign_id"], first_id, last_id, len(ids), final_dir),
        )
        delete_task_artifacts(conn, ids)
        conn.commit()
        return {"archived": len(ids), "archive_key": archive_key, "root_path": final_dir}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        if temporary_dir:
            root = os.path.abspath(_archive_root())
            candidate = os.path.abspath(temporary_dir)
            if os.path.commonpath([root, candidate]) == root:
                shutil.rmtree(candidate, ignore_errors=True)
        raise
    finally:
        conn.close()


def cleanup_archives(retention_days: int | None = None, *, limit: int = 1000) -> dict:
    """Remove old local Parquet archives after the configured data window."""

    from services import settings_service

    days = settings_service.get_data_retention_days(retention_days)
    batch_limit = max(50, min(5000, int(limit)))
    cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()
    root = os.path.abspath(_archive_root())
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id,root_path FROM task_archive_manifests
               WHERE task_date < CAST(? AS DATE)
               ORDER BY id LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
    finally:
        conn.close()

    removable_ids = []
    removed_dirs = 0
    for row in rows:
        candidate = os.path.abspath(str(row["root_path"] or ""))
        try:
            inside_root = os.path.commonpath([root, candidate]) == root
        except ValueError:
            inside_root = False
        if not inside_root or candidate == root:
            logger.warning("Skipping unsafe task archive path=%s", candidate)
            continue
        try:
            if os.path.lexists(candidate) and not os.path.islink(candidate):
                shutil.rmtree(candidate)
                removed_dirs += 1
            if os.path.lexists(candidate):
                continue
            removable_ids.append(int(row["id"]))
            parent = os.path.dirname(candidate)
            while parent and parent != root:
                try:
                    os.rmdir(parent)
                except OSError:
                    break
                parent = os.path.dirname(parent)
        except OSError:
            logger.warning("Unable to remove task archive path=%s", candidate, exc_info=True)

    removed_manifests = 0
    if removable_ids:
        placeholders = ",".join("?" for _ in removable_ids)
        removed_manifests = write_transaction(
            lambda conn: conn.execute(
                f"""DELETE FROM task_archive_manifests
                    WHERE id IN ({placeholders})
                      AND task_date < CAST(? AS DATE)
                    RETURNING id""",
                [*removable_ids, cutoff],
            ).fetchall(),
            attempts=3,
            operation="task_archive.retention_cleanup",
            lock_timeout=1.0,
        )
    result = {
        "retention_days": days,
        "archives_removed": removed_dirs,
        "manifests_removed": len(removed_manifests or []),
    }
    if result["manifests_removed"]:
        logger.info("Task archive retention cleanup completed result=%s", result)
    return result


def list_archives(campaign_type: str = "", campaign_id: int = 0,
                  cursor: Optional[int] = None, page_size: int = 50) -> dict:
    conditions, params = [], []
    if campaign_type:
        conditions.append("campaign_type=?")
        params.append(campaign_type)
    if campaign_id:
        conditions.append("campaign_id=?")
        params.append(campaign_id)
    if cursor:
        conditions.append("id<?")
        params.append(cursor)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    conn = get_connection()
    try:
        rows = conn.execute(
            f"SELECT * FROM task_archive_manifests {where} ORDER BY id DESC LIMIT ?",
            params + [page_size + 1],
        ).fetchall()
        has_more = len(rows) > page_size
        items = [dict(row) for row in rows[:page_size]]
        return {"items": items, "has_more": has_more,
                "next_cursor": items[-1]["id"] if has_more and items else None}
    finally:
        conn.close()


def list_archived_tasks(campaign_type: str = "", campaign_id: int = 0,
                        cursor: Optional[int] = None, page_size: int = 50) -> dict:
    conditions, params = ["status='completed'"], []
    if campaign_type:
        conditions.append("campaign_type=?")
        params.append(campaign_type)
    if campaign_id:
        conditions.append("campaign_id=?")
        params.append(campaign_id)
    conn = get_connection()
    try:
        manifests = conn.execute(
            "SELECT root_path FROM task_archive_manifests WHERE " + " AND ".join(conditions),
            params,
        ).fetchall()
        paths = [os.path.join(row["root_path"], "tasks.parquet") for row in manifests]
        paths = [path for path in paths if os.path.isfile(path)]
        if not paths:
            return {"items": [], "has_more": False, "next_cursor": None}
        cursor_where = "WHERE id<?" if cursor else ""
        query_params = [paths] + ([cursor] if cursor else []) + [page_size + 1]
        rows = conn.execute(
            f"""SELECT id,campaign_type,campaign_id,campaign_name,smpp_config_id,
                       msisdn,language,content,status,attempts,error_msg,updated_at
                FROM read_parquet(?,union_by_name=true) {cursor_where}
                ORDER BY id DESC LIMIT ?""",
            query_params,
        ).fetchall()
        has_more = len(rows) > page_size
        items = [dict(row) for row in rows[:page_size]]
        return {"items": items, "has_more": has_more,
                "next_cursor": items[-1]["id"] if has_more and items else None}
    finally:
        conn.close()
