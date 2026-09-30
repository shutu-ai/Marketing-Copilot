"""Persistent file manifests and resumable ingestion chunk leases."""

import os
import uuid
from datetime import datetime, timedelta
from typing import Iterable, Optional

from database import get_connection


DEFAULT_LEASE_SECONDS = max(30, int(os.environ.get("MC_INGEST_CHUNK_LEASE_SECONDS", "300")))
FILE_LEASE_SECONDS = max(30, int(os.environ.get("MC_INGEST_FILE_LEASE_SECONDS", "600")))


def get_file(campaign_type: str, campaign_id: int, filename: str) -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT * FROM ingestion_files
               WHERE campaign_type=? AND campaign_id=? AND filename=?""",
            (campaign_type, campaign_id, os.path.basename(filename)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def register_file(campaign_type: str, campaign_id: int, filename: str, file_path: str,
                  total_rows: int = 0, chunk_size: int = 5000,
                  chunks: Optional[Iterable[dict]] = None, reset: bool = False,
                  whitelist_version_id: Optional[int] = None) -> tuple[int, bool]:
    """Register by stable business identity; a renamed file is a new manifest."""
    safe_filename = os.path.basename(filename)
    absolute_path = os.path.abspath(file_path) if file_path else ""
    file_size = os.path.getsize(absolute_path) if absolute_path and os.path.isfile(absolute_path) else 0
    conn = get_connection()
    try:
        existing = conn.execute(
            """SELECT * FROM ingestion_files
               WHERE campaign_type=? AND campaign_id=? AND filename=?""",
            (campaign_type, campaign_id, safe_filename),
        ).fetchone()
        if existing and existing["status"] == "completed" and not reset:
            return existing["id"], True
        if existing and not reset and existing["file_path"] == absolute_path \
                and int(existing.get("file_size", 0) or 0) == file_size:
            return existing["id"], False
        if existing:
            file_id = existing["id"]
            conn.execute(
                """UPDATE ingestion_files SET file_path=?,file_size=?,total_rows=?,chunk_size=?,
                   total_chunks=0,completed_chunks=0,created_tasks=0,status='pending',
                   error_detail='',processing_phase='queued',processed_rows=0,matched_rows=0,
                   current_chunk_index=0,worker_id='',lease_token='',lease_expires_at=NULL,
                   retry_count=0,last_error_class='',last_error_at=NULL,next_retry_at=NULL,
                   whitelist_version_id=COALESCE(?,whitelist_version_id),
                   updated_at=CURRENT_TIMESTAMP,completed_at=NULL WHERE id=?""",
                (absolute_path, file_size, int(total_rows or 0), max(1, int(chunk_size)), whitelist_version_id, file_id),
            )
            if reset:
                conn.execute("DELETE FROM ingestion_chunks WHERE file_id=?", (file_id,))
        else:
            file_id = conn.execute(
                """INSERT INTO ingestion_files
                   (campaign_type,campaign_id,filename,file_path,file_size,total_rows,chunk_size,whitelist_version_id)
                   VALUES (?,?,?,?,?,?,?,?) RETURNING id""",
                (campaign_type, campaign_id, safe_filename, absolute_path, file_size,
                 int(total_rows or 0), max(1, int(chunk_size)), whitelist_version_id),
            ).fetchone()["id"]
        chunk_rows = list(chunks or [])
        for index, item in enumerate(chunk_rows):
            conn.execute(
                """INSERT INTO ingestion_chunks
                   (file_id,source_batch_id,chunk_index,start_row,end_row)
                   VALUES (?,?,?,?,?) ON CONFLICT (file_id,chunk_index) DO NOTHING""",
                (file_id, item.get("source_batch_id"), int(item.get("chunk_index", index)),
                 int(item.get("start_row", 0)), int(item.get("end_row", 0))),
            )
        if chunk_rows:
            conn.execute(
                "UPDATE ingestion_files SET total_chunks=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (len(chunk_rows), file_id),
            )
        conn.commit()
        return file_id, False
    finally:
        conn.close()


def claim_file(campaign_type: str = "realtime", worker_id: str = "",
               lease_seconds: int = FILE_LEASE_SECONDS) -> Optional[dict]:
    """Atomically lease one pending ingestion file for a single worker."""
    token = uuid.uuid4().hex
    worker_id = worker_id or f"realtime-ingestion-{os.getpid()}"
    lease_until = (datetime.now() + timedelta(seconds=max(30, lease_seconds))).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT id FROM ingestion_files
               WHERE campaign_type=? AND status IN ('pending','retrying')
                 AND (next_retry_at IS NULL OR CAST(next_retry_at AS TIMESTAMP)<=CURRENT_TIMESTAMP)
               ORDER BY created_at,id LIMIT 1""", (campaign_type,)
        ).fetchone()
        if not row:
            return None
        claimed = conn.execute(
            """UPDATE ingestion_files SET status='processing',worker_id=?,lease_token=?,
               lease_expires_at=?,processing_started_at=COALESCE(processing_started_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
               last_progress_at=CURRENT_TIMESTAMP,processing_phase='queued',updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN ('pending','retrying')
                 AND (next_retry_at IS NULL OR CAST(next_retry_at AS TIMESTAMP)<=CURRENT_TIMESTAMP)
               RETURNING *""", (worker_id, token, lease_until, row["id"]),
        ).fetchone()
        if claimed:
            conn.commit()
            return dict(claimed)
        return None
    finally:
        conn.close()


def update_progress(file_id: int, phase: str, processed_rows: Optional[int] = None,
                    matched_rows: Optional[int] = None, chunk_index: Optional[int] = None,
                    total_chunks: Optional[int] = None, worker_id: Optional[str] = None,
                    whitelist_version_id: Optional[int] = None) -> None:
    fields = ["processing_phase=?", "last_progress_at=CURRENT_TIMESTAMP", "updated_at=CURRENT_TIMESTAMP"]
    values = [phase]
    for column, value in (("processed_rows", processed_rows), ("matched_rows", matched_rows),
                          ("current_chunk_index", chunk_index), ("total_chunks", total_chunks),
                          ("worker_id", worker_id), ("whitelist_version_id", whitelist_version_id)):
        if value is not None:
            fields.append(f"{column}=?")
            values.append(value)
    values.append(file_id)
    conn = get_connection()
    try:
        conn.execute(f"UPDATE ingestion_files SET {','.join(fields)} WHERE id=?", values)
        conn.commit()
    finally:
        conn.close()


def schedule_retry(file_id: int, error_class: str, error, delay_seconds: int,
                   allow_unbounded: bool = False) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT retry_count,max_retry_count FROM ingestion_files WHERE id=?", (file_id,)).fetchone()
        if not row:
            return {"scheduled": False, "retry_count": 0, "max_retry_count": 0}
        retry_count = int(row["retry_count"] or 0) + 1
        max_retry_count = int(row["max_retry_count"] or 4)
        if retry_count > max_retry_count and not allow_unbounded:
            return {"scheduled": False, "retry_count": retry_count, "max_retry_count": max_retry_count}
        conn.execute(
            """UPDATE ingestion_files SET status='retrying',retry_count=?,last_error_class=?,
               last_error_at=CURRENT_TIMESTAMP,next_retry_at=CAST(CURRENT_TIMESTAMP + (? * INTERVAL '1 second') AS VARCHAR),
               processing_phase='queued',lease_token='',lease_expires_at=NULL,worker_id='',
               updated_at=CURRENT_TIMESTAMP,error_detail=? WHERE id=?""",
            (retry_count, error_class, int(max(1, delay_seconds)), str(error)[:4000], file_id),
        )
        conn.commit()
        return {"scheduled": True, "retry_count": retry_count, "max_retry_count": max_retry_count}
    finally:
        conn.close()


def requeue_waiting_whitelist_files(campaign_id: int) -> int:
    """Immediately retry realtime files held only by a whitelist dependency."""
    conn = get_connection()
    try:
        count = conn.execute(
            """SELECT COUNT(*) AS count FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class='WAITING_DEPENDENCY'
                 AND status IN ('retrying','failed','quarantined')""",
            (campaign_id,),
        ).fetchone()["count"]
        result = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',retry_count=0,
                      last_error_class='',last_error_at=NULL,next_retry_at=NULL,error_detail='',
                      lease_token='',lease_expires_at=NULL,worker_id='',updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class='WAITING_DEPENDENCY'
                 AND status IN ('retrying','failed','quarantined')""",
            (campaign_id,),
        )
        conn.commit()
        return int(count or 0)
    finally:
        conn.close()


def count_waiting_whitelist_files(campaign_id: int) -> int:
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS count FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class='WAITING_DEPENDENCY'
                 AND status IN ('pending','retrying','failed','quarantined')""",
            (campaign_id,),
        ).fetchone()
        return int(row["count"] or 0)
    finally:
        conn.close()


def fail_file(file_id: int, error_class: str, error: str, status: str = "failed") -> None:
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE ingestion_files SET status=?,processing_phase=?,last_error_class=?,
               last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,lease_token='',lease_expires_at=NULL,
               worker_id='',error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (status, "quarantined" if status == "quarantined" else "failed", error_class, str(error)[:4000], file_id),
        )
        conn.commit()
    finally:
        conn.close()


def expire_file(file_id: int, error: str, include_processing: bool = False) -> bool:
    """Mark an out-of-date realtime file terminal so it no longer occupies the queue."""
    conn = get_connection()
    try:
        active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
        conn.execute(
            """UPDATE ingestion_files SET status='failed',processing_phase='failed',
               last_error_class='EXPIRED_FILE',last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,
               lease_token='',lease_expires_at=NULL,worker_id='',error_detail=?,
               completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN """ + active_statuses,
            (str(error)[:4000], file_id),
        )
        row = conn.execute(
            "SELECT status,last_error_class FROM ingestion_files WHERE id=?", (file_id,),
        ).fetchone()
        expired = bool(row and row["status"] == "failed" and row["last_error_class"] == "EXPIRED_FILE")
        if expired:
            conn.execute(
                """UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
                   error_detail=?,updated_at=CURRENT_TIMESTAMP
                   WHERE file_id=? AND status IN ('pending','processing','retrying')""",
                (str(error)[:4000], file_id),
            )
        conn.commit()
        return expired
    finally:
        conn.close()


def expire_files(file_ids: Iterable[int], error: str,
                 include_processing: bool = False) -> set[int]:
    """Expire a batch of queued files in one database update."""
    ids = sorted({int(file_id) for file_id in file_ids})
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE ingestion_files SET status='failed',processing_phase='failed',
               last_error_class='EXPIRED_FILE',last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,
               lease_token='',lease_expires_at=NULL,worker_id='',error_detail=?,
               completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id IN (""" + placeholders + ") AND status IN " + active_statuses,
            (str(error)[:4000], *ids),
        )
        rows = conn.execute(
            """SELECT id FROM ingestion_files WHERE id IN (""" + placeholders + ")
               AND status='failed' AND last_error_class='EXPIRED_FILE'",
            ids,
        ).fetchall()
        expired_ids = {int(row["id"]) for row in rows}
        if expired_ids:
            expired_placeholders = ",".join("?" for _ in expired_ids)
            conn.execute(
                """UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
                   error_detail=?,updated_at=CURRENT_TIMESTAMP
                   WHERE file_id IN (""" + expired_placeholders + ")
                     AND status IN ('pending','processing','retrying')",
                (str(error)[:4000], *sorted(expired_ids)),
            )
        conn.commit()
        return expired_ids
    finally:
        conn.close()


def complete_file(file_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE ingestion_files SET status='completed',processing_phase='completed',
               next_retry_at=NULL,lease_token='',lease_expires_at=NULL,worker_id='',
               completed_at=CURRENT_TIMESTAMP,last_progress_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id=?""", (file_id,),
        )
        conn.commit()
    finally:
        conn.close()


def requeue_file(file_id: int) -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM ingestion_files WHERE id=?", (file_id,)).fetchone()
        if not row or row["status"] not in ("failed", "quarantined", "retrying"):
            return dict(row) if row else None
        updated = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',retry_count=0,
               last_error_class='',last_error_at=NULL,next_retry_at=NULL,error_detail='',
               lease_token='',lease_expires_at=NULL,worker_id='',updated_at=CURRENT_TIMESTAMP
               WHERE id=? RETURNING *""", (file_id,),
        ).fetchone()
        conn.commit()
        return dict(updated) if updated else None
    finally:
        conn.close()


def ensure_chunk(file_id: int, chunk_index: int, start_row: int, end_row: int,
                 source_batch_id: Optional[int] = None) -> dict:
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO ingestion_chunks
               (file_id,source_batch_id,chunk_index,start_row,end_row)
               VALUES (?,?,?,?,?) ON CONFLICT (file_id,chunk_index) DO NOTHING""",
            (file_id, source_batch_id, chunk_index, start_row, end_row),
        )
        conn.execute(
            """UPDATE ingestion_files SET total_chunks=GREATEST(total_chunks,?),
               total_rows=GREATEST(total_rows,?),updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (chunk_index + 1, end_row, file_id),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM ingestion_chunks WHERE file_id=? AND chunk_index=?",
            (file_id, chunk_index),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def claim_chunk(file_id: Optional[int] = None, chunk_index: Optional[int] = None,
                source_batch_id: Optional[int] = None,
                lease_seconds: int = DEFAULT_LEASE_SECONDS) -> Optional[dict]:
    token = uuid.uuid4().hex
    lease_until = (datetime.now() + timedelta(seconds=max(30, lease_seconds))).strftime("%Y-%m-%d %H:%M:%S")
    if source_batch_id is not None:
        where, params = "source_batch_id=?", [source_batch_id]
    else:
        where, params = "file_id=? AND chunk_index=?", [file_id, chunk_index]
    conn = get_connection()
    try:
        row = conn.execute(
            f"SELECT * FROM ingestion_chunks WHERE {where}", params
        ).fetchone()
        if not row or row["status"] == "completed":
            return dict(row) if row else None
        if row["status"] == "processing" and row["lease_expires_at"]:
            lease = datetime.fromisoformat(str(row["lease_expires_at"]).replace("Z", "+00:00")).replace(tzinfo=None)
            if lease > datetime.now():
                return None
        claimed = conn.execute(
            """UPDATE ingestion_chunks SET status='processing',attempts=attempts+1,
               lease_token=?,lease_expires_at=?,error_detail='',updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN ('pending','failed','processing') RETURNING *""",
            (token, lease_until, row["id"]),
        ).fetchone()
        if claimed:
            conn.execute(
                "UPDATE ingestion_files SET status='processing',updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (claimed["file_id"],),
            )
            conn.commit()
            return dict(claimed)
        return None
    finally:
        conn.close()


def complete_chunk_in_transaction(conn, chunk_id: int, result: dict) -> None:
    row = conn.execute("SELECT file_id,status FROM ingestion_chunks WHERE id=?", (chunk_id,)).fetchone()
    if not row or row["status"] == "completed":
        return
    conn.execute(
        """UPDATE ingestion_chunks SET status='completed',lease_token='',lease_expires_at=NULL,
           created_tasks=?,duplicate_rows=?,excluded_rows=?,invalid_rows=?,no_template_rows=?,
           completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (int(result.get("created", 0)), int(result.get("duplicate", 0)),
         int(result.get("excluded", 0)), int(result.get("invalid", 0)),
         int(result.get("no_template", 0)), chunk_id),
    )
    conn.execute(
        """UPDATE ingestion_files SET
           completed_chunks=(SELECT COUNT(*) FROM ingestion_chunks WHERE file_id=? AND status='completed'),
           created_tasks=(SELECT COALESCE(SUM(created_tasks),0) FROM ingestion_chunks WHERE file_id=?),
           updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (row["file_id"], row["file_id"], row["file_id"]),
    )


def complete_chunk(chunk_id: int, result: dict) -> None:
    conn = get_connection()
    try:
        complete_chunk_in_transaction(conn, chunk_id, result)
        conn.commit()
    finally:
        conn.close()


def fail_chunk_in_transaction(conn, chunk_id: int, error) -> None:
    row = conn.execute("SELECT file_id FROM ingestion_chunks WHERE id=?", (chunk_id,)).fetchone()
    if row:
        conn.execute(
            """UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
               error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (str(error)[:4000], chunk_id),
        )
        conn.execute(
            "UPDATE ingestion_files SET status='failed',error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (str(error)[:4000], row["file_id"]),
        )


def fail_chunk(chunk_id: int, error) -> None:
    conn = get_connection()
    try:
        fail_chunk_in_transaction(conn, chunk_id, error)
        conn.commit()
    finally:
        conn.close()


def finish_file(file_id: int, status: str = "completed", error: str = "") -> None:
    conn = get_connection()
    try:
        counts = conn.execute(
            """SELECT COUNT(*) total,
                      SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) completed,
                      COALESCE(SUM(created_tasks),0) created
               FROM ingestion_chunks WHERE file_id=?""",
            (file_id,),
        ).fetchone()
        final_status = status
        if status == "completed" and counts["total"] and counts["completed"] < counts["total"]:
            final_status = "processing"
        conn.execute(
            """UPDATE ingestion_files SET status=?,total_chunks=?,completed_chunks=?,created_tasks=?,
               error_detail=?,updated_at=CURRENT_TIMESTAMP,
               completed_at=CASE WHEN ?='completed' THEN CAST(CURRENT_TIMESTAMP AS VARCHAR) ELSE completed_at END
               WHERE id=?""",
            (final_status, counts["total"], counts["completed"] or 0, counts["created"],
             str(error)[:4000], final_status, file_id),
        )
        conn.commit()
    finally:
        conn.close()


def recover_expired_chunks() -> int:
    conn = get_connection()
    try:
        rows = conn.execute(
            """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL,
               error_detail='lease expired; recovered after restart',updated_at=CURRENT_TIMESTAMP
               WHERE status='processing' AND CAST(lease_expires_at AS TIMESTAMP)<=CURRENT_TIMESTAMP
               RETURNING source_batch_id"""
        ).fetchall()
        batch_ids = [row["source_batch_id"] for row in rows if row["source_batch_id"]]
        if batch_ids:
            placeholders = ",".join("?" for _ in batch_ids)
            conn.execute(
                f"UPDATE batch_campaign_batches SET status='pending',updated_at=CURRENT_TIMESTAMP "
                f"WHERE id IN ({placeholders}) AND status='processing'",
                batch_ids,
            )
        if rows:
            conn.commit()
        return len(rows)
    finally:
        conn.close()


def recover_expired_files() -> int:
    conn = get_connection()
    try:
        rows = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',
               lease_token='',lease_expires_at=NULL,worker_id='',
               error_detail='file lease expired; recovered after restart',updated_at=CURRENT_TIMESTAMP
               WHERE status='processing' AND lease_expires_at IS NOT NULL
                 AND CAST(lease_expires_at AS TIMESTAMP)<=CURRENT_TIMESTAMP RETURNING id"""
        ).fetchall()
        if rows:
            conn.commit()
        return len(rows)
    finally:
        conn.close()


def status() -> dict:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT status,COUNT(*) count FROM ingestion_chunks GROUP BY status"
        ).fetchall()
        counts = {row["status"]: int(row["count"]) for row in rows}
        return {"chunks": counts, "pending": counts.get("pending", 0) + counts.get("failed", 0)}
    finally:
        conn.close()
