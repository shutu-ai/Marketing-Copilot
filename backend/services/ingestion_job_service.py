"""Persistent file manifests and resumable ingestion chunk leases."""

import os
import uuid
from datetime import datetime, timedelta
from typing import Iterable, Optional

from database import get_connection


DEFAULT_LEASE_SECONDS = max(30, int(os.environ.get("MC_INGEST_CHUNK_LEASE_SECONDS", "300")))


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
                  chunks: Optional[Iterable[dict]] = None, reset: bool = False) -> tuple[int, bool]:
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
        if existing:
            file_id = existing["id"]
            conn.execute(
                """UPDATE ingestion_files SET file_path=?,file_size=?,total_rows=?,chunk_size=?,
                   total_chunks=0,completed_chunks=0,created_tasks=0,status='pending',
                   error_detail='',updated_at=CURRENT_TIMESTAMP,completed_at=NULL WHERE id=?""",
                (absolute_path, file_size, int(total_rows or 0), max(1, int(chunk_size)), file_id),
            )
            if reset:
                conn.execute("DELETE FROM ingestion_chunks WHERE file_id=?", (file_id,))
        else:
            file_id = conn.execute(
                """INSERT INTO ingestion_files
                   (campaign_type,campaign_id,filename,file_path,file_size,total_rows,chunk_size)
                   VALUES (?,?,?,?,?,?,?) RETURNING id""",
                (campaign_type, campaign_id, safe_filename, absolute_path, file_size,
                 int(total_rows or 0), max(1, int(chunk_size))),
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
