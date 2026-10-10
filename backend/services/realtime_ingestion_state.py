"""Short, fenced transactions for realtime file state (never file IO)."""

import logging
import os
import random
import threading
import time
import uuid

import duckdb

from database import get_connection
from services import ingestion_job_service, metrics_service

logger = logging.getLogger(__name__)


class LeaseLost(RuntimeError):
    """The caller must abandon this file without writing another terminal state."""


def transient(error):
    return isinstance(error, (TimeoutError, duckdb.TransactionException))


def transaction(callback, attempts=3, lock_timeout=None):
    """Retry the whole transaction with a new connection, sleeping only after close."""
    for attempt in range(attempts):
        conn = get_connection()
        conn.write_lock_timeout = lock_timeout
        try:
            conn.execute("BEGIN TRANSACTION")
            result = callback(conn)
            conn.commit()
            return result
        except BaseException as exc:
            conn.rollback()
            if not transient(exc) or attempt + 1 == attempts:
                raise
        finally:
            conn.close()
        metrics_service.inc("realtime_state_transaction_retries_total")
        time.sleep(0.025 * (attempt + 1) + random.uniform(0, 0.025))


def assert_owned(conn, file_id, token):
    if not token:
        return
    row = conn.execute(
        """SELECT id FROM ingestion_files WHERE id=? AND status='processing'
           AND lease_token=? AND TRY_CAST(lease_expires_at AS TIMESTAMPTZ)>CURRENT_TIMESTAMP""",
        (file_id, token),
    ).fetchone()
    if not row:
        raise LeaseLost(f"realtime file lease lost: {file_id}")


def check_owned(file_id, token):
    if token:
        conn = get_connection()
        try:
            assert_owned(conn, file_id, token)
        finally:
            conn.close()


def register_processed(campaign_id, path, filename, file_id, token):
    from services import operations_service
    digest, size = operations_service.file_digest(path), os.path.getsize(path)
    if os.path.splitext(filename)[1].lower() not in operations_service.ALLOWED_EXTENSIONS:
        raise ValueError("unsupported file type")
    def write(conn):
        assert_owned(conn, file_id, token)
        row = conn.execute(
            "SELECT id,status FROM processed_files WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
            (campaign_id, os.path.basename(filename)),
        ).fetchone()
        if row:
            if row["status"] == "completed":
                return row["id"], True
            conn.execute(
                """UPDATE processed_files SET file_hash=?,file_path=?,file_size=?,status='validating',
                   error_detail='',completed_at=NULL WHERE id=?""", (digest, path, size, row["id"]),
            )
            return row["id"], False
        row = conn.execute(
            """INSERT INTO processed_files(campaign_type,campaign_id,filename,file_hash,file_path,file_size,status)
               VALUES ('realtime',?,?,?,?,?,'validating') RETURNING id""",
            (campaign_id, os.path.basename(filename), digest, path, size),
        ).fetchone()
        return row["id"], False
    return transaction(write)


def finish_processed(file_id, processed_id, token, status, path, error):
    def write(conn):
        assert_owned(conn, file_id, token)
        conn.execute(
            """UPDATE processed_files SET status=?,processing_phase=?,file_path=?,error_detail=?,
               completed_at=CURRENT_TIMESTAMP,last_progress_at=CURRENT_TIMESTAMP WHERE id=?""",
            (status, status, path, str(error)[:4000], processed_id),
        )
    transaction(write)


class FileLease:
    """Renew independently of slow parsing/matching; connections belong to the heartbeat."""
    def __init__(self, file_id, token):
        self.file_id, self.token = file_id, token
        self.stop = threading.Event()
        self.thread = None

    def renew(self):
        def write(conn):
            assert_owned(conn, self.file_id, self.token)
            conn.execute(
                """UPDATE ingestion_files SET
                   lease_expires_at=CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR)
                   WHERE id=? AND lease_token=?""",
                (ingestion_job_service.FILE_LEASE_SECONDS, self.file_id, self.token),
            )
            # A healthy file owner also owns its currently processing chunk.
            conn.execute(
                """UPDATE ingestion_chunks SET
                   lease_expires_at=CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR)
                   WHERE file_id=? AND status='processing'""",
                (ingestion_job_service.FILE_LEASE_SECONDS, self.file_id),
            )
        transaction(write, attempts=1, lock_timeout=0.5)

    def _run(self):
        interval = min(30, ingestion_job_service.FILE_LEASE_SECONDS / 3)
        while not self.stop.wait(interval):
            try:
                self.renew()
            except LeaseLost:
                return
            except Exception:
                metrics_service.inc("realtime_file_lease_renew_failures_total")
                logger.warning("Realtime lease heartbeat deferred file_id=%s", self.file_id, exc_info=True)

    def __enter__(self):
        if self.token:
            self.thread = threading.Thread(target=self._run, name=f"realtime-file-lease-{self.file_id}", daemon=True)
            self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=2)


class Progress:
    def __init__(self, file_id, processed_id, token="", worker_id="", whitelist_version_id=None):
        self.file_id, self.processed_id, self.token = file_id, processed_id, token
        self.worker_id, self.version_id = worker_id, whitelist_version_id
        try:
            self.interval = max(0.1, float(os.environ.get("MC_REALTIME_PROGRESS_INTERVAL_SECONDS", "2")))
        except ValueError:
            self.interval = 2.0
        self.last_attempt = float("-inf")
        self.latest = None
        self.phase, self.phase_started = None, time.monotonic()

    def __call__(self, phase, processed_rows=0, matched_rows=0, chunk_index=0, total_chunks=0, force=False):
        now = time.monotonic()
        if self.phase is not None:
            metrics_service.observe("realtime_file_stage_seconds", now-self.phase_started, {"phase": self.phase})
        self.phase, self.phase_started = phase, now
        self.latest = (phase, processed_rows, matched_rows, chunk_index, total_chunks)
        if not force and now-self.last_attempt < self.interval:
            return False
        self.last_attempt = now
        try:
            transaction(self._write, attempts=3 if force else 1, lock_timeout=None if force else 0.1)
        except Exception as exc:
            if force or not transient(exc):
                raise
            # Display state can be coalesced; never retry the whole input file for this.
            metrics_service.inc("realtime_file_progress_deferred_total")
            return False
        metrics_service.inc("realtime_file_progress_commits_total")
        metrics_service.set_gauge("realtime_ingestion_worker_last_progress_timestamp", time.time())
        self.latest = None
        return True

    def _write(self, conn):
        assert_owned(conn, self.file_id, self.token)
        phase, rows, matched, chunk, chunks = self.latest
        updated = conn.execute(
            """UPDATE ingestion_files SET processing_phase=?,processed_rows=?,matched_rows=?,
               current_chunk_index=?,total_chunks=GREATEST(total_chunks,?),worker_id=?,
               whitelist_version_id=COALESCE(?,whitelist_version_id),
               last_progress_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN ('pending','processing') RETURNING id""",
            (phase, rows, matched, chunk, chunks, self.worker_id, self.version_id, self.file_id),
        ).fetchone()
        if not updated:
            raise LeaseLost(f"realtime file no longer active: {self.file_id}")
        updated_processed = conn.execute(
            """UPDATE processed_files SET processing_phase=?,processed_rows=?,matched_rows=?,
               current_chunk_index=?,total_chunks=GREATEST(total_chunks,?),worker_id=?,
               whitelist_version_id=COALESCE(?,whitelist_version_id),last_progress_at=CURRENT_TIMESTAMP,
               processing_started_at=COALESCE(processing_started_at,CAST(CURRENT_TIMESTAMP AS VARCHAR))
               WHERE id=? RETURNING id""",
            (phase, rows, matched, chunk, chunks, self.worker_id, self.version_id, self.processed_id),
        ).fetchone()
        if not updated_processed:
            raise LookupError(f"processed file record missing: {self.processed_id}")


def prepare_chunk(file_id, index, start, end, token=""):
    def write(conn):
        assert_owned(conn, file_id, token)
        conn.execute(
            """INSERT INTO ingestion_chunks(file_id,chunk_index,start_row,end_row) VALUES (?,?,?,?)
               ON CONFLICT (file_id,chunk_index) DO NOTHING""", (file_id, index, start, end),
        )
        conn.execute(
            """UPDATE ingestion_files SET total_chunks=GREATEST(total_chunks,?),
               total_rows=GREATEST(total_rows,?),updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (index+1, end, file_id),
        )
        row = conn.execute("SELECT * FROM ingestion_chunks WHERE file_id=? AND chunk_index=?", (file_id, index)).fetchone()
        if row["status"] == "completed":
            return dict(row)
        claimed = conn.execute(
            """UPDATE ingestion_chunks SET status='processing',attempts=attempts+1,lease_token=?,
               lease_expires_at=CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR),
               error_detail='',updated_at=CURRENT_TIMESTAMP WHERE id=?
               AND (status IN ('pending','failed') OR TRY_CAST(lease_expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP)
               RETURNING *""", (uuid.uuid4().hex, ingestion_job_service.FILE_LEASE_SECONDS, row["id"]),
        ).fetchone()
        if not claimed:
            raise RuntimeError(f"realtime chunk {index} lease is held by another worker")
        return dict(claimed)
    return transaction(write)


def complete_chunk_in_transaction(conn, file_id, chunk, result, token=""):
    assert_owned(conn, file_id, token)
    row = conn.execute("SELECT status,lease_token FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()
    if not row or row["status"] != "processing" or row["lease_token"] != chunk["lease_token"]:
        raise LeaseLost(f"realtime chunk lease lost: {chunk['id']}")
    ingestion_job_service.complete_chunk_in_transaction(conn, chunk["id"], result)


def complete_chunk(file_id, chunk, result, token=""):
    transaction(lambda conn: complete_chunk_in_transaction(conn, file_id, chunk, result, token))


def fail_chunk(file_id, chunk, error, token=""):
    """Release only the current chunk lease without stealing file ownership."""
    def write(conn):
        assert_owned(conn, file_id, token)
        updated = conn.execute(
            """UPDATE ingestion_chunks SET status='failed',lease_token='',lease_expires_at=NULL,
                      error_detail=?,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='processing' AND lease_token=? RETURNING id""",
            (str(error)[:4000], chunk["id"], chunk["lease_token"]),
        ).fetchone()
        if not updated:
            raise LeaseLost(f"realtime chunk lease lost: {chunk['id']}")
    transaction(write)


def transition(file_id, processed_id, token, status, error_class="", error="", delay=0,
               path="", rows=0, created=0, campaign_id=None, filename="", total_msisdn=0, matched=0):
    """Terminal/retry state in both tables, increment retries only once per lease."""
    def write(conn):
        assert_owned(conn, file_id, token)
        if status == "completed":
            incomplete = conn.execute("SELECT COUNT(*) n FROM ingestion_chunks WHERE file_id=? AND status<>'completed'", (file_id,)).fetchone()["n"]
            if incomplete:
                raise RuntimeError("Cannot complete realtime file with incomplete chunks")
            if campaign_id is not None:
                conn.execute(
                    """INSERT INTO realtime_campaign_files(campaign_id,filename,file_path,total_msisdn,matched_count)
                       SELECT ?,?,?,?,? WHERE NOT EXISTS
                       (SELECT 1 FROM realtime_campaign_files WHERE campaign_id=? AND filename=?)""",
                    (campaign_id, filename, path, total_msisdn, matched, campaign_id, filename),
                )
        phase = "queued" if status == "retrying" else status
        conn.execute(
            """UPDATE ingestion_files SET status=?,processing_phase=?,lease_token='',lease_expires_at=NULL,
               worker_id='',error_detail=?,last_error_class=?,updated_at=CURRENT_TIMESTAMP,
               retry_count=retry_count+?,next_retry_at=CASE WHEN ?='retrying'
               THEN CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR) ELSE NULL END,
               completed_at=CASE WHEN ?='retrying' THEN NULL ELSE CAST(CURRENT_TIMESTAMP AS VARCHAR) END,
               last_progress_at=CURRENT_TIMESTAMP WHERE id=?""",
            (status, phase, str(error)[:4000], error_class, int(status == "retrying"), status, max(1, delay), status, file_id),
        )
        if processed_id:
            conn.execute(
                """UPDATE processed_files SET status=?,processing_phase=?,error_detail=?,last_error_class=?,
                   retry_count=COALESCE(retry_count,0)+?,next_retry_at=CASE WHEN ?='retrying'
                   THEN CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR) ELSE NULL END,
                   last_progress_at=CURRENT_TIMESTAMP,last_error_at=CASE WHEN ?<>'' THEN CAST(CURRENT_TIMESTAMP AS VARCHAR) ELSE last_error_at END,
                   completed_at=CASE WHEN ?='retrying' THEN NULL ELSE CAST(CURRENT_TIMESTAMP AS VARCHAR) END,
                   file_path=CASE WHEN ?='' THEN file_path ELSE ? END,
                   total_rows=CASE WHEN ?='completed' THEN ? ELSE total_rows END,
                   created_tasks=CASE WHEN ?='completed' THEN ? ELSE created_tasks END WHERE id=?""",
                (status, phase, str(error)[:4000], error_class, int(status == "retrying"), status, max(1, delay),
                 error_class, status, path, path, status, rows, status, created, processed_id),
            )
        # Clear abandoned chunks now; completed checkpoints remain intact for replay.
        if status != "completed":
            conn.execute(
                """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL,
                   error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE file_id=? AND status='processing'""",
                (str(error)[:4000], file_id),
            )
    transaction(write)


def recover_expired_files():
    def write(conn):
        expired = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',lease_token='',
               lease_expires_at=NULL,worker_id='',error_detail='file lease expired; recovered at runtime',
               updated_at=CURRENT_TIMESTAMP WHERE campaign_type='realtime' AND status='processing'
               AND TRY_CAST(lease_expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP RETURNING id,campaign_id,filename""",
        ).fetchall()
        for row in expired:
            conn.execute(
                """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL
                   WHERE file_id=? AND status='processing'""", (row["id"],),
            )
            conn.execute(
                """UPDATE processed_files SET status='retrying',processing_phase='queued',worker_id=''
                   WHERE campaign_type='realtime' AND campaign_id=? AND filename=? AND status<>'completed'""",
                (row["campaign_id"], row["filename"]),
            )
        return len(expired)
    return transaction(write)
