"""Archive terminal SMS task partitions to Parquet before hot-table deletion."""

import os
import shutil
import tempfile
from typing import Optional

from database import get_connection, get_database_file


TERMINAL_STATUSES = ("delivered", "failed", "expired", "cancelled")


def _archive_root() -> str:
    configured = os.environ.get("MC_TASK_ARCHIVE_DIR", "").strip()
    if configured:
        return os.path.abspath(configured)
    return os.path.join(os.path.dirname(os.path.abspath(get_database_file())), "archives", "tasks")


def _sql_path(path: str) -> str:
    return "'" + os.path.abspath(path).replace("'", "''") + "'"


def _copy_query(conn, query: str, path: str) -> None:
    conn.execute(
        f"COPY ({query}) TO {_sql_path(path)} "
        "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 100000)"
    )


def archive_terminal_tasks(cutoff: str, limit: int = 5000) -> dict:
    """Archive one date/campaign partition and atomically remove exactly those rows."""
    limit = max(1000, min(20000, int(limit)))
    conn = get_connection()
    temporary_dir = None
    try:
        partition = conn.execute(
            """SELECT task_date,campaign_type,campaign_id FROM sms_tasks
               WHERE created_at<? AND status IN ('delivered','failed','expired','cancelled')
               ORDER BY id LIMIT 1""",
            (cutoff,),
        ).fetchone()
        if not partition:
            return {"archived": 0, "archive_key": "", "root_path": ""}
        rows = conn.execute(
            """SELECT id FROM sms_tasks
               WHERE created_at<? AND status IN ('delivered','failed','expired','cancelled')
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
        conn.execute(f"DELETE FROM sms_task_message_ids WHERE task_id IN ({placeholders})")
        conn.execute(f"DELETE FROM delivery_receipts WHERE task_id IN ({placeholders})")
        conn.execute(f"DELETE FROM task_events WHERE task_id IN ({placeholders})")
        conn.execute(f"DELETE FROM sms_tasks WHERE id IN ({placeholders})")
        conn.commit()
        return {"archived": len(ids), "archive_key": archive_key, "root_path": final_dir}
    except Exception:
        try:
            conn.conn.rollback()
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
