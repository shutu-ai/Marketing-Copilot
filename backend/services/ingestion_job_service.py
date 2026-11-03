"""Persistent file manifests and resumable ingestion chunk leases."""

import hashlib
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Iterable, Optional

from database import read_transaction, write_transaction


logger = logging.getLogger(__name__)


DEFAULT_LEASE_SECONDS = max(30, int(os.environ.get("MC_INGEST_CHUNK_LEASE_SECONDS", "300")))
FILE_LEASE_SECONDS = max(30, int(os.environ.get("MC_INGEST_FILE_LEASE_SECONDS", "600")))

# ``CURRENT_TIMESTAMP`` is a TIMESTAMPTZ in DuckDB.  Retry timestamps are
# stored as text for compatibility with existing manifests, so they must be
# parsed back as TIMESTAMPTZ before comparing them.  Casting a value such as
# ``2026-09-30 13:55:00+03`` to plain TIMESTAMP silently converts it to UTC
# wall-clock time and makes a future retry look immediately due.
_RETRY_DUE_SQL = "(next_retry_at IS NULL OR TRY_CAST(next_retry_at AS TIMESTAMPTZ) <= CURRENT_TIMESTAMP)"


def _file_hash(path: str) -> str:
    """Hash an input before entering the short authoritative DB transaction."""
    if not path or not os.path.isfile(path):
        return ""
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def get_file(campaign_type: str, campaign_id: int, filename: str) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            """SELECT * FROM ingestion_files
               WHERE campaign_type=? AND campaign_id=? AND filename=?""",
            (campaign_type, campaign_id, os.path.basename(filename)),
        ).fetchone(),
        operation="ingestion_file.get",
    )
    return dict(row) if row else None


def register_file(campaign_type: str, campaign_id: int, filename: str, file_path: str,
                  total_rows: int = 0, chunk_size: int = 5000,
                  chunks: Optional[Iterable[dict]] = None, reset: bool = False,
                  whitelist_version_id: Optional[int] = None,
                  reservation_token: str = "", manifest_id: Optional[int] = None,
                  file_hash: Optional[str] = None) -> tuple[int, bool]:
    """Register by stable business identity; a renamed file is a new manifest."""
    safe_filename = os.path.basename(filename)
    absolute_path = os.path.abspath(file_path) if file_path else ""
    file_size = os.path.getsize(absolute_path) if absolute_path and os.path.isfile(absolute_path) else 0
    source_hash = str(file_hash or "").strip() or _file_hash(absolute_path)
    def write(conn):
        if reservation_token:
            reservation = conn.execute(
                """SELECT token FROM realtime_inbox_reservations
                   WHERE token=? AND campaign_id=? AND filename=?
                     AND TRY_CAST(expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP""",
                (reservation_token, int(campaign_id), safe_filename),
            ).fetchone()
            if not reservation:
                raise RuntimeError("realtime inbox capacity reservation is missing or expired")
        existing = conn.execute(
            """SELECT * FROM ingestion_files
               WHERE campaign_type=? AND campaign_id=? AND filename=?""",
            (campaign_type, campaign_id, safe_filename),
        ).fetchone()
        same_manifest = (
            manifest_id is None
            or existing is None
            or existing.get("manifest_id") in (None, int(manifest_id))
        )
        existing_hash = str(existing.get("file_hash") or "") if existing else ""
        same_content = not source_hash or not existing_hash or source_hash == existing_hash
        if existing and existing["status"] == "completed" and not reset and same_manifest and same_content:
            if reservation_token:
                conn.execute(
                    "DELETE FROM realtime_inbox_reservations WHERE token=?",
                    (reservation_token,),
                )
            return existing["id"], True
        if existing and not reset and same_manifest and same_content and existing["file_path"] == absolute_path \
                and int(existing.get("file_size", 0) or 0) == file_size:
            if reservation_token:
                conn.execute(
                    "DELETE FROM realtime_inbox_reservations WHERE token=?",
                    (reservation_token,),
                )
            return existing["id"], False
        if existing:
            file_id = existing["id"]
            conn.execute(
                """UPDATE ingestion_files SET file_path=?,file_size=?,file_hash=?,manifest_id=?,total_rows=?,chunk_size=?,
                   total_chunks=0,completed_chunks=0,created_tasks=0,status='pending',
                   error_detail='',processing_phase='queued',processed_rows=0,matched_rows=0,
                   current_chunk_index=0,worker_id='',lease_token='',lease_expires_at=NULL,
                   retry_count=0,last_error_class='',last_error_at=NULL,next_retry_at=NULL,
                   whitelist_version_id=COALESCE(?,whitelist_version_id),
                   state_version=COALESCE(state_version,0)+1,
                   updated_at=CURRENT_TIMESTAMP,completed_at=NULL WHERE id=?""",
                (absolute_path, file_size, source_hash, manifest_id, int(total_rows or 0),
                 max(1, int(chunk_size)), whitelist_version_id, file_id),
            )
            if reset or (source_hash and existing_hash and source_hash != existing_hash):
                conn.execute("DELETE FROM ingestion_chunks WHERE file_id=?", (file_id,))
        else:
            file_id = conn.execute(
                """INSERT INTO ingestion_files
                   (campaign_type,campaign_id,filename,file_path,file_size,file_hash,manifest_id,total_rows,chunk_size,whitelist_version_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?) RETURNING id""",
                 (campaign_type, campaign_id, safe_filename, absolute_path, file_size, source_hash,
                  manifest_id, int(total_rows or 0), max(1, int(chunk_size)), whitelist_version_id),
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
                "UPDATE ingestion_files SET total_chunks=?,state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (len(chunk_rows), file_id),
            )
        if reservation_token:
            conn.execute(
                "DELETE FROM realtime_inbox_reservations WHERE token=?",
                (reservation_token,),
            )
        return file_id, False

    return write_transaction(write, operation="ingestion_file.register")


def reserve_realtime_capacity(campaign_id: int, filename: str, file_size: int,
                              max_campaign_files: int, max_total_files: int,
                              max_total_bytes: int, source: str = "") -> dict:
    """Atomically reserve one realtime inbox slot across every intake path."""
    token = uuid.uuid4().hex
    safe_filename = os.path.basename(filename)
    file_size = max(0, int(file_size or 0))

    def reserve(conn):
        conn.execute(
            """DELETE FROM realtime_inbox_reservations
               WHERE token IN (
                 SELECT token FROM realtime_inbox_reservations
                 WHERE TRY_CAST(expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP
                 ORDER BY expires_at LIMIT 1000
               )"""
        )
        existing = conn.execute(
            """SELECT id FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=? AND filename=?""",
            (int(campaign_id), safe_filename),
        ).fetchone()
        if existing:
            return {"ok": False, "duplicate": True, "reason": "file is already registered"}
        reserved = conn.execute(
            """SELECT token FROM realtime_inbox_reservations
               WHERE campaign_id=? AND filename=?""",
            (int(campaign_id), safe_filename),
        ).fetchone()
        if reserved:
            return {"ok": False, "duplicate": True, "reason": "file is already reserved"}
        active = conn.execute(
            """SELECT
                 SUM(CASE WHEN campaign_id=? THEN 1 ELSE 0 END) campaign_count,
                 COUNT(*) total_count,COALESCE(SUM(file_size),0) total_bytes
               FROM ingestion_files
               WHERE campaign_type='realtime'
                 AND status IN ('pending','processing','retrying')""",
            (int(campaign_id),),
        ).fetchone()
        reservations = conn.execute(
            """SELECT
                 SUM(CASE WHEN campaign_id=? THEN 1 ELSE 0 END) campaign_count,
                 COUNT(*) total_count,COALESCE(SUM(file_size),0) total_bytes
               FROM realtime_inbox_reservations""",
            (int(campaign_id),),
        ).fetchone()
        policy = conn.execute(
            """SELECT max_pending_tasks,max_ingest_files
               FROM campaign_capacity_policies
               WHERE campaign_type='realtime' AND campaign_id=? AND enabled=1""",
            (int(campaign_id),),
        ).fetchone()
        effective_campaign_files = int(max_campaign_files)
        if policy and policy["max_ingest_files"] is not None:
            effective_campaign_files = min(
                effective_campaign_files, max(1, int(policy["max_ingest_files"]))
            )
        if policy and policy["max_pending_tasks"] is not None:
            pending_tasks = conn.execute(
                """SELECT COUNT(*) count FROM sms_tasks
                   WHERE campaign_type='realtime' AND campaign_id=?
                     AND status IN ('queued','retrying','sending')""",
                (int(campaign_id),),
            ).fetchone()["count"]
            if int(pending_tasks or 0) >= int(policy["max_pending_tasks"]):
                return {"ok": False, "duplicate": False,
                        "reason": "campaign pending SMS task limit reached"}
        campaign_count = int(active["campaign_count"] or 0) + int(reservations["campaign_count"] or 0)
        total_count = int(active["total_count"] or 0) + int(reservations["total_count"] or 0)
        total_bytes = int(active["total_bytes"] or 0) + int(reservations["total_bytes"] or 0)
        if campaign_count >= effective_campaign_files:
            return {"ok": False, "duplicate": False,
                    "reason": "campaign realtime inbox pending limit reached"}
        if total_count >= int(max_total_files):
            return {"ok": False, "duplicate": False,
                    "reason": "global realtime inbox pending limit reached"}
        if total_bytes + file_size > int(max_total_bytes):
            return {"ok": False, "duplicate": False,
                    "reason": "global realtime inbox byte limit reached"}
        conn.execute(
            """INSERT INTO realtime_inbox_reservations
               (token,campaign_id,filename,file_size,source,expires_at)
               VALUES (?,?,?,?,?,CAST(CURRENT_TIMESTAMP + INTERVAL '10 minutes' AS VARCHAR))""",
            (token, int(campaign_id), safe_filename, file_size, str(source or "")),
        )
        return {"ok": True, "duplicate": False, "reason": "", "token": token}

    return write_transaction(reserve, operation="realtime_inbox.reserve")


def release_realtime_capacity(token: str) -> None:
    if not token:
        return
    write_transaction(
        lambda conn: conn.execute(
            "DELETE FROM realtime_inbox_reservations WHERE token=?", (str(token),)
        ),
        operation="realtime_inbox.release",
    )


def claim_file(campaign_type: str = "realtime", worker_id: str = "",
               lease_seconds: int = FILE_LEASE_SECONDS,
               campaign_id: Optional[int] = None) -> Optional[dict]:
    """Atomically lease one pending ingestion file for a single worker.

    ``campaign_id`` is optional for backward compatibility.  Realtime workers
    use it to claim from one campaign selected by their round-robin scheduler;
    other callers retain the original global queue behavior.
    """
    token = uuid.uuid4().hex
    worker_id = worker_id or f"realtime-ingestion-{os.getpid()}"
    def claim(conn):
        campaign_clause = "" if campaign_id is None else " AND campaign_id=?"
        select_params = [campaign_type]
        if campaign_id is not None:
            select_params.append(int(campaign_id))
        row = conn.execute(
            """SELECT id FROM ingestion_files
               WHERE campaign_type=? AND status IN ('pending','retrying')
                 AND """ + _RETRY_DUE_SQL + """
                 """ + campaign_clause + """
                 AND (campaign_type<>'realtime' OR EXISTS (
                      SELECT 1 FROM realtime_campaigns c
                      WHERE c.id=ingestion_files.campaign_id AND c.status='in_progress'
                 ))
               -- Realtime filenames carry a fixed-width YYYYMMDDHHMMSS token,
               -- so lexical order is chronological for the watched files.
               ORDER BY CASE WHEN campaign_type='realtime' THEN filename ELSE created_at END,id LIMIT 1""", select_params
        ).fetchone()
        if not row:
            return None
        claim_params = [worker_id, token, max(30, lease_seconds), row["id"]]
        if campaign_id is not None:
            claim_params.append(int(campaign_id))
        claimed = conn.execute(
            """UPDATE ingestion_files SET status='processing',worker_id=?,lease_token=?,
               lease_expires_at=CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR),
               processing_started_at=COALESCE(processing_started_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
               last_progress_at=CURRENT_TIMESTAMP,processing_phase='queued',
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN ('pending','retrying')
                 AND """ + _RETRY_DUE_SQL + """
                 """ + (" AND campaign_id=?" if campaign_id is not None else "") + """
                 AND (campaign_type<>'realtime' OR EXISTS (
                      SELECT 1 FROM realtime_campaigns c
                      WHERE c.id=ingestion_files.campaign_id AND c.status='in_progress'
                 ))
               RETURNING *""", claim_params,
        ).fetchone()
        if claimed:
            # ``campaign_fairness_state`` is advisory display state.  Keep it
            # out of the fenced lease transaction: concurrent ingestion
            # workers claiming files for one campaign would otherwise turn a
            # telemetry UPSERT into an OCC conflict on the file claim.
            return dict(claimed)
        return None
    return write_transaction(
        claim,
        attempts=5,
        operation="realtime.ingestion.file_claim",
        lock_timeout=1.0,
    )


def pending_campaign_ids(campaign_type: str = "realtime") -> list[int]:
    """Return active campaigns that currently have a claimable file.

    This is intentionally a small read-only query.  The caller chooses the
    next campaign and then calls ``claim_file(..., campaign_id=...)`` so the
    actual lease remains an atomic UPDATE transaction.
    """
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT DISTINCT f.campaign_id
               FROM ingestion_files f
               WHERE f.campaign_type=?
                 AND f.campaign_id IS NOT NULL
                 AND f.status IN ('pending','retrying')
                 AND """ + _RETRY_DUE_SQL + """
                 AND (f.campaign_type<>'realtime' OR EXISTS (
                      SELECT 1 FROM realtime_campaigns c
                      WHERE c.id=f.campaign_id AND c.status='in_progress'
                 ))
               ORDER BY f.campaign_id""",
            (campaign_type,),
        ).fetchall(),
        operation="realtime.ingestion.pending_campaigns",
    )
    return [int(row["campaign_id"]) for row in rows]


def update_progress(file_id: int, phase: str, processed_rows: Optional[int] = None,
                    matched_rows: Optional[int] = None, chunk_index: Optional[int] = None,
                    total_chunks: Optional[int] = None, worker_id: Optional[str] = None,
                    whitelist_version_id: Optional[int] = None) -> None:
    fields = ["processing_phase=?", "state_version=COALESCE(state_version,0)+1",
              "last_progress_at=CURRENT_TIMESTAMP", "updated_at=CURRENT_TIMESTAMP"]
    values = [phase]
    for column, value in (("processed_rows", processed_rows), ("matched_rows", matched_rows),
                          ("current_chunk_index", chunk_index), ("total_chunks", total_chunks),
                          ("worker_id", worker_id), ("whitelist_version_id", whitelist_version_id)):
        if value is not None:
            fields.append(f"{column}=?")
            values.append(value)
    values.append(file_id)
    def write(conn):
        conn.execute(f"UPDATE ingestion_files SET {','.join(fields)} WHERE id=?", values)
    write_transaction(write, attempts=1, operation="realtime.ingestion.progress", lock_timeout=0.5)


def schedule_retry(file_id: int, error_class: str, error, delay_seconds: int,
                   allow_unbounded: bool = False) -> dict:
    def write(conn):
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
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP,error_detail=? WHERE id=?""",
            (retry_count, error_class, int(max(1, delay_seconds)), str(error)[:4000], file_id),
        )
        return {"scheduled": True, "retry_count": retry_count, "max_retry_count": max_retry_count}
    return write_transaction(write, operation="realtime.ingestion.schedule_retry")


def requeue_waiting_whitelist_files(campaign_id: int) -> int:
    """Immediately retry realtime files held only by a whitelist dependency."""
    def write(conn):
        count = conn.execute(
            """SELECT COUNT(*) AS count FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class IN ('WAITING_DEPENDENCY','WHITELIST_INDEX')
                 AND status IN ('retrying','failed','quarantined')""",
            (campaign_id,),
        ).fetchone()["count"]
        result = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',retry_count=0,
                      last_error_class='',last_error_at=NULL,next_retry_at=NULL,error_detail='',
                      lease_token='',lease_expires_at=NULL,worker_id='',
                      state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class IN ('WAITING_DEPENDENCY','WHITELIST_INDEX')
                 AND status IN ('retrying','failed','quarantined')""",
            (campaign_id,),
        )
        return int(count or 0)
    return write_transaction(write, operation="realtime.ingestion.requeue_whitelist")


def count_waiting_whitelist_files(campaign_id: int) -> int:
    row = read_transaction(
        lambda conn: conn.execute(
            """SELECT COUNT(*) AS count FROM ingestion_files
               WHERE campaign_type='realtime' AND campaign_id=?
                 AND last_error_class='WAITING_DEPENDENCY'
                 AND status IN ('pending','retrying','failed','quarantined')""",
            (campaign_id,),
        ).fetchone(),
        operation="realtime.ingestion.count_whitelist_waiters",
    )
    return int(row["count"] or 0)


def fail_file(file_id: int, error_class: str, error: str, status: str = "failed") -> None:
    def write(conn):
        conn.execute(
            """UPDATE ingestion_files SET status=?,processing_phase=?,last_error_class=?,
               last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,lease_token='',lease_expires_at=NULL,
               worker_id='',error_detail=?,state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (status, "quarantined" if status == "quarantined" else "failed", error_class, str(error)[:4000], file_id),
        )
    write_transaction(write, operation="realtime.ingestion.fail_file")


def expire_file(file_id: int, error: str, include_processing: bool = False) -> bool:
    """Mark an out-of-date realtime file terminal so it no longer occupies the queue."""
    def write(conn):
        active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
        conn.execute(
            """UPDATE ingestion_files SET status='failed',processing_phase='failed',
               last_error_class='EXPIRED_FILE',last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,
               lease_token='',lease_expires_at=NULL,worker_id='',error_detail=?,
               completed_at=CURRENT_TIMESTAMP,state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP
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
        return expired
    return write_transaction(write, operation="realtime.ingestion.expire_file")


def expire_files(file_ids: Iterable[int], error: str,
                 include_processing: bool = False) -> set[int]:
    """Expire a batch of queued files in one database update."""
    ids = sorted({int(file_id) for file_id in file_ids})
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
    def write(conn):
        conn.execute(
            """UPDATE ingestion_files SET status='failed',processing_phase='failed',
               last_error_class='EXPIRED_FILE',last_error_at=CURRENT_TIMESTAMP,next_retry_at=NULL,
               lease_token='',lease_expires_at=NULL,worker_id='',error_detail=?,
               completed_at=CURRENT_TIMESTAMP,state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP
               WHERE id IN (""" + placeholders + ") AND status IN " + active_statuses,
            (str(error)[:4000], *ids),
        )
        rows = conn.execute(
            f"""SELECT id FROM ingestion_files WHERE id IN ({placeholders})
               AND status='failed' AND last_error_class='EXPIRED_FILE'""",
            ids,
        ).fetchall()
        expired_ids = {int(row["id"]) for row in rows}
        if expired_ids:
            expired_placeholders = ",".join("?" for _ in expired_ids)
            conn.execute(
                f"""UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
                   error_detail=?,updated_at=CURRENT_TIMESTAMP
                   WHERE file_id IN ({expired_placeholders})
                     AND status IN ('pending','processing','retrying')""",
                (str(error)[:4000], *sorted(expired_ids)),
            )
        return expired_ids
    return write_transaction(write, operation="realtime.ingestion.expire_files")


def complete_file(file_id: int) -> None:
    def write(conn):
        conn.execute(
            """UPDATE ingestion_files SET status='completed',processing_phase='completed',
               next_retry_at=NULL,lease_token='',lease_expires_at=NULL,worker_id='',
               completed_at=CURRENT_TIMESTAMP,last_progress_at=CURRENT_TIMESTAMP,
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE id=?""", (file_id,),
        )
    write_transaction(write, operation="realtime.ingestion.complete_file")


def requeue_file(file_id: int) -> Optional[dict]:
    def write(conn):
        row = conn.execute("SELECT * FROM ingestion_files WHERE id=?", (file_id,)).fetchone()
        if not row or row["status"] not in ("failed", "quarantined", "retrying"):
            return dict(row) if row else None
        updated = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',retry_count=0,
               last_error_class='',last_error_at=NULL,next_retry_at=NULL,error_detail='',
               lease_token='',lease_expires_at=NULL,worker_id='',
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE id=? RETURNING *""", (file_id,),
        ).fetchone()
        return dict(updated) if updated else None
    return write_transaction(write, operation="realtime.ingestion.requeue_file")


def ensure_chunk(file_id: int, chunk_index: int, start_row: int, end_row: int,
                 source_batch_id: Optional[int] = None) -> dict:
    def write(conn):
        conn.execute(
            """INSERT INTO ingestion_chunks
               (file_id,source_batch_id,chunk_index,start_row,end_row)
               VALUES (?,?,?,?,?) ON CONFLICT (file_id,chunk_index) DO NOTHING""",
            (file_id, source_batch_id, chunk_index, start_row, end_row),
        )
        conn.execute(
            """UPDATE ingestion_files SET total_chunks=GREATEST(total_chunks,?),
               total_rows=GREATEST(total_rows,?),state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (chunk_index + 1, end_row, file_id),
        )
        row = conn.execute(
            "SELECT * FROM ingestion_chunks WHERE file_id=? AND chunk_index=?",
            (file_id, chunk_index),
        ).fetchone()
        return dict(row)
    return write_transaction(write, operation="realtime.ingestion.ensure_chunk")


def claim_chunk(file_id: Optional[int] = None, chunk_index: Optional[int] = None,
                source_batch_id: Optional[int] = None,
                lease_seconds: int = DEFAULT_LEASE_SECONDS) -> Optional[dict]:
    token = uuid.uuid4().hex
    lease_until = (datetime.now() + timedelta(seconds=max(30, lease_seconds))).strftime("%Y-%m-%d %H:%M:%S")
    if source_batch_id is not None:
        where, params = "source_batch_id=?", [source_batch_id]
    else:
        where, params = "file_id=? AND chunk_index=?", [file_id, chunk_index]
    def write(conn):
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
                "UPDATE ingestion_files SET status='processing',state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (claimed["file_id"],),
            )
            return dict(claimed)
        return None
    return write_transaction(
        write,
        attempts=3,
        operation="realtime.ingestion.chunk_claim",
        lock_timeout=1.0,
    )


def record_chunk_progress_in_transaction(conn, chunk_id: int, result: dict) -> None:
    """Persist one idempotent-enqueue progress unit while the chunk is fenced."""
    row = conn.execute("SELECT file_id,status FROM ingestion_chunks WHERE id=?", (chunk_id,)).fetchone()
    if not row or row["status"] == "completed":
        return
    conn.execute(
        """UPDATE ingestion_chunks SET
           created_tasks=COALESCE(created_tasks,0)+?,
           duplicate_rows=COALESCE(duplicate_rows,0)+?,
           excluded_rows=COALESCE(excluded_rows,0)+?,
           invalid_rows=COALESCE(invalid_rows,0)+?,
           no_template_rows=COALESCE(no_template_rows,0)+?,
           updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='processing'""",
        (int(result.get("created", 0)), int(result.get("duplicate", 0)),
         int(result.get("excluded", 0)), int(result.get("invalid", 0)),
         int(result.get("no_template", 0)), chunk_id),
    )
    conn.execute(
        """UPDATE ingestion_files SET created_tasks=(
             SELECT COALESCE(SUM(created_tasks),0) FROM ingestion_chunks WHERE file_id=?
           ), state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (row["file_id"], row["file_id"]),
    )


def is_completed_chunk_status(status) -> bool:
    """Only the exact durable enum value proves checkpoint completion."""
    return status == "completed"


def complete_chunk_in_transaction(conn, chunk_id: int, result: dict) -> None:
    row = conn.execute(
        """SELECT file_id,status,created_tasks,duplicate_rows,excluded_rows,
                  invalid_rows,no_template_rows FROM ingestion_chunks WHERE id=?""",
        (chunk_id,),
    ).fetchone()
    if not row or row["status"] == "completed":
        return
    if row["status"] != "processing":
        raise ValueError(f"Cannot complete ingestion chunk {chunk_id}: status={row['status']!r}")
    conn.execute(
        """UPDATE ingestion_chunks SET status='completed',lease_token='',lease_expires_at=NULL,
           created_tasks=?,duplicate_rows=?,excluded_rows=?,invalid_rows=?,no_template_rows=?,
           completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (max(int(row["created_tasks"] or 0), int(result.get("created", 0))),
         max(int(row["duplicate_rows"] or 0), int(result.get("duplicate", 0))),
         max(int(row["excluded_rows"] or 0), int(result.get("excluded", 0))),
         max(int(row["invalid_rows"] or 0), int(result.get("invalid", 0))),
         max(int(row["no_template_rows"] or 0), int(result.get("no_template", 0))), chunk_id),
    )
    conn.execute(
        """UPDATE ingestion_files SET
           completed_chunks=(SELECT COUNT(*) FROM ingestion_chunks WHERE file_id=? AND status='completed'),
           created_tasks=(SELECT COALESCE(SUM(created_tasks),0) FROM ingestion_chunks WHERE file_id=?),
           state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (row["file_id"], row["file_id"], row["file_id"]),
    )


def complete_chunk(chunk_id: int, result: dict) -> None:
    write_transaction(
        lambda conn: complete_chunk_in_transaction(conn, chunk_id, result),
        operation="realtime.ingestion.chunk_complete",
    )


def fail_chunk_in_transaction(conn, chunk_id: int, error) -> None:
    row = conn.execute("SELECT file_id FROM ingestion_chunks WHERE id=?", (chunk_id,)).fetchone()
    if row:
        conn.execute(
            """UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
               error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (str(error)[:4000], chunk_id),
        )
        conn.execute(
            "UPDATE ingestion_files SET status='failed',error_detail=?,state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (str(error)[:4000], row["file_id"]),
        )


def fail_chunk(chunk_id: int, error) -> None:
    write_transaction(
        lambda conn: fail_chunk_in_transaction(conn, chunk_id, error),
        operation="realtime.ingestion.chunk_fail",
    )


def finish_file(file_id: int, status: str = "completed", error: str = "") -> None:
    def write(conn):
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
               error_detail=?,state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP,
               completed_at=CASE WHEN ?='completed' THEN CAST(CURRENT_TIMESTAMP AS VARCHAR) ELSE completed_at END
               WHERE id=?""",
            (final_status, counts["total"], counts["completed"] or 0, counts["created"],
             str(error)[:4000], final_status, file_id),
        )
    write_transaction(write, operation="realtime.ingestion.finish_file")


def recover_expired_chunks() -> int:
    def write(conn):
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
        return len(rows)
    return write_transaction(write, operation="realtime.ingestion.recover_chunks")


def recover_expired_files() -> int:
    def write(conn):
        rows = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',
               lease_token='',lease_expires_at=NULL,worker_id='',
               error_detail='file lease expired; recovered after restart',
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE status='processing' AND lease_expires_at IS NOT NULL
                 AND CAST(lease_expires_at AS TIMESTAMP)<=CURRENT_TIMESTAMP RETURNING id"""
        ).fetchall()
        return len(rows)
    return write_transaction(write, operation="realtime.ingestion.recover_files")


def delete_campaign_records(conn, campaign_type: str, campaign_id: int) -> dict:
    """Delete all ingestion state owned by a campaign inside its transaction."""

    campaign_type = str(campaign_type)
    campaign_id = int(campaign_id)
    conn.execute(
        """DELETE FROM ingestion_chunks
           WHERE file_id IN (
             SELECT id FROM ingestion_files WHERE campaign_type=? AND campaign_id=?
           )""",
        (campaign_type, campaign_id),
    )
    files = conn.execute(
        """SELECT filename FROM ingestion_files
           WHERE campaign_type=? AND campaign_id=?""",
        (campaign_type, campaign_id),
    ).fetchall()
    conn.execute(
        "DELETE FROM ingestion_files WHERE campaign_type=? AND campaign_id=?",
        (campaign_type, campaign_id),
    )
    conn.execute(
        "DELETE FROM processed_files WHERE campaign_type=? AND campaign_id=?",
        (campaign_type, campaign_id),
    )
    conn.execute(
        "DELETE FROM campaign_fairness_state WHERE campaign_type=? AND campaign_id=?",
        (campaign_type, campaign_id),
    )
    conn.execute(
        "DELETE FROM campaign_capacity_policies WHERE campaign_type=? AND campaign_id=?",
        (campaign_type, campaign_id),
    )
    if campaign_type == "realtime":
        conn.execute(
            "DELETE FROM realtime_inbox_reservations WHERE campaign_id=?",
            (campaign_id,),
        )
    return {"files": len(files), "campaign_type": campaign_type, "campaign_id": campaign_id}


def has_live_leases(campaign_type: str, campaign_id: int) -> bool:
    """Return whether a worker still owns a non-expired file/chunk lease."""

    row = read_transaction(
        lambda conn: conn.execute(
            """SELECT 1 FROM ingestion_files
               WHERE campaign_type=? AND campaign_id=?
                 AND (
                   (status='processing' AND TRY_CAST(lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP)
                   OR EXISTS (
                     SELECT 1 FROM ingestion_chunks c
                     WHERE c.file_id=ingestion_files.id AND c.status='processing'
                       AND TRY_CAST(c.lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP
                   )
                 ) LIMIT 1""",
            (str(campaign_type), int(campaign_id)),
        ).fetchone(),
        operation="ingestion.retention.live_leases",
    )
    return bool(row)


def cleanup_retention(retention_days: int | None = None, *, limit: int = 1000) -> dict:
    """Remove old terminal/orphan ingestion state in bounded transactions.

    Active campaigns keep queued and retryable files. Deleted/deleting
    campaigns are safe to reclaim once no worker lease remains.
    """

    from services import settings_service

    days = settings_service.get_data_retention_days(retention_days)
    batch_limit = max(100, min(10000, int(limit)))
    cutoff = f"CURRENT_TIMESTAMP - ({days} * INTERVAL '1 day')"

    def write(conn):
        candidate_rows = conn.execute(
            f"""SELECT f.id,f.campaign_type,f.campaign_id,f.filename,f.file_path,
                       CASE WHEN f.campaign_type='realtime' THEN rc.status ELSE bc.status END AS campaign_status
                FROM ingestion_files f
                LEFT JOIN realtime_campaigns rc
                  ON f.campaign_type='realtime' AND rc.id=f.campaign_id
                LEFT JOIN batch_campaigns bc
                  ON f.campaign_type='batch' AND bc.id=f.campaign_id
                WHERE TRY_CAST(f.updated_at AS TIMESTAMPTZ) < {cutoff}
                  AND (f.status IN ('completed','failed','quarantined') OR
                       (
                         (f.campaign_type='realtime' AND NOT EXISTS (
                           SELECT 1 FROM realtime_campaigns c
                           WHERE c.id=f.campaign_id
                             AND c.status IN ('in_progress','not_started','paused')
                         )) OR
                         (f.campaign_type='batch' AND NOT EXISTS (
                           SELECT 1 FROM batch_campaigns c
                           WHERE c.id=f.campaign_id
                             AND c.status IN ('in_progress','not_started','paused')
                         ))
                       ))
                  AND (f.status<>'processing' OR f.lease_expires_at IS NULL
                       OR TRY_CAST(f.lease_expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP)
                  AND NOT EXISTS (
                        SELECT 1 FROM ingestion_chunks live_chunk
                        WHERE live_chunk.file_id=f.id
                          AND live_chunk.status='processing'
                          AND TRY_CAST(live_chunk.lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP
                  )
                ORDER BY f.id LIMIT ?""",
            (batch_limit * 4,),
        ).fetchall()
        rows = []
        for row in candidate_rows:
            active_campaign = row["campaign_status"] in ("in_progress", "not_started", "paused")
            if active_campaign and row["file_path"] and os.path.isfile(row["file_path"]):
                continue
            rows.append(row)
            if len(rows) >= batch_limit:
                break
        file_ids = [int(row["id"]) for row in rows]
        deleted_files = 0
        deleted_chunks = 0
        if file_ids:
            placeholders = ",".join("?" for _ in file_ids)
            deleted_chunks = len(conn.execute(
                f"DELETE FROM ingestion_chunks WHERE file_id IN ({placeholders}) RETURNING id",
                file_ids,
            ).fetchall())
            conn.execute(
                f"DELETE FROM ingestion_files WHERE id IN ({placeholders})",
                file_ids,
            )
            deleted_files = len(file_ids)
            conn.execute("DROP TABLE IF EXISTS _mc_retention_file_keys")
            conn.execute(
                """CREATE TEMPORARY TABLE _mc_retention_file_keys(
                       campaign_type VARCHAR, campaign_id BIGINT, filename VARCHAR
                   )"""
            )
            try:
                conn.executemany(
                    "INSERT INTO _mc_retention_file_keys VALUES (?,?,?)",
                    [(row["campaign_type"], int(row["campaign_id"]), row["filename"]) for row in rows],
                )
                conn.execute(
                    """DELETE FROM processed_files p
                       WHERE EXISTS (
                         SELECT 1 FROM _mc_retention_file_keys k
                         WHERE k.campaign_type=p.campaign_type
                           AND k.campaign_id=p.campaign_id
                           AND k.filename=p.filename
                       )"""
                )
            finally:
                conn.execute("DROP TABLE IF EXISTS _mc_retention_file_keys")

        old_processed_candidates = conn.execute(
            f"""SELECT p.id,p.file_path,
                       CASE WHEN p.campaign_type='realtime' THEN rc.status ELSE bc.status END AS campaign_status
                FROM processed_files p
                LEFT JOIN realtime_campaigns rc
                  ON p.campaign_type='realtime' AND rc.id=p.campaign_id
                LEFT JOIN batch_campaigns bc
                  ON p.campaign_type='batch' AND bc.id=p.campaign_id
                WHERE TRY_CAST(COALESCE(p.completed_at,p.detected_at) AS TIMESTAMPTZ) < {cutoff}
                  AND (
                    p.status NOT IN ('pending','processing','retrying') OR
                    (p.campaign_type='realtime' AND NOT EXISTS (
                       SELECT 1 FROM realtime_campaigns c
                       WHERE c.id=p.campaign_id
                         AND c.status IN ('in_progress','not_started','paused')
                    )) OR
                    (p.campaign_type='batch' AND NOT EXISTS (
                       SELECT 1 FROM batch_campaigns c
                       WHERE c.id=p.campaign_id
                         AND c.status IN ('in_progress','not_started','paused')
                    ))
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM ingestion_files live_file
                    WHERE live_file.campaign_type=p.campaign_type
                      AND live_file.campaign_id=p.campaign_id
                      AND live_file.filename=p.filename
                      AND (
                        (live_file.status='processing' AND TRY_CAST(live_file.lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP)
                        OR EXISTS (
                          SELECT 1 FROM ingestion_chunks live_chunk
                          WHERE live_chunk.file_id=live_file.id
                            AND live_chunk.status='processing'
                            AND TRY_CAST(live_chunk.lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP
                        )
                      )
                  )
                ORDER BY p.id LIMIT ?""",
            (batch_limit * 4,),
        ).fetchall()
        old_processed = [
            row for row in old_processed_candidates
            if row["campaign_status"] not in ("in_progress", "not_started", "paused")
            or not row["file_path"]
            or not os.path.isfile(row["file_path"])
        ][:batch_limit]
        processed_ids = [int(row["id"]) for row in old_processed]
        if processed_ids:
            placeholders = ",".join("?" for _ in processed_ids)
            conn.execute(f"DELETE FROM processed_files WHERE id IN ({placeholders})", processed_ids)

        orphan_chunks = conn.execute(
            f"""SELECT id FROM ingestion_chunks c
                WHERE NOT EXISTS (SELECT 1 FROM ingestion_files f WHERE f.id=c.file_id)
                ORDER BY id LIMIT ?""",
            (batch_limit,),
        ).fetchall()
        orphan_chunk_ids = [int(row["id"]) for row in orphan_chunks]
        if orphan_chunk_ids:
            placeholders = ",".join("?" for _ in orphan_chunk_ids)
            conn.execute(f"DELETE FROM ingestion_chunks WHERE id IN ({placeholders})", orphan_chunk_ids)

        return {
            "retention_days": days,
            "ingestion_files": deleted_files,
            "ingestion_chunks": deleted_chunks + len(orphan_chunk_ids),
            "processed_files": len(processed_ids),
        }

    result = write_transaction(
        write,
        attempts=3,
        operation="ingestion.retention.cleanup",
        lock_timeout=1.0,
    )
    if any(result[key] for key in ("ingestion_files", "ingestion_chunks", "processed_files")):
        logger.info("Ingestion retention cleanup completed result=%s", result)
    return result


def status() -> dict:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT status,COUNT(*) count FROM ingestion_chunks GROUP BY status"
        ).fetchall(),
        operation="realtime.ingestion.status",
    )
    counts = {row["status"]: int(row["count"]) for row in rows}
    return {"chunks": counts, "pending": counts.get("pending", 0) + counts.get("failed", 0)}
