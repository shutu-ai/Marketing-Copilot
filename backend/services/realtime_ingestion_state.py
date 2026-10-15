"""Short, fenced transactions for realtime file state (never file IO)."""

from contextlib import contextmanager
import logging
import os
import threading
import time
import uuid

import database as database_module
from database import is_transient_write_error, read_transaction, write_transaction
from services import ingestion_job_service, metrics_service

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows uses msvcrt below.
    fcntl = None

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX uses fcntl above.
    msvcrt = None

logger = logging.getLogger(__name__)
_PROCESSED_FILES_THREAD_LOCK = threading.Lock()


@contextmanager
def _processed_files_guard():
    """Serialize the non-authoritative processed-file mirror across workers.

    DuckDB 1.2.0 can corrupt a unique-index update when independent OCC
    transactions concurrently update this mirror table.  The authoritative
    ingestion lease/chunk tables remain OCC-concurrent; only this audit/UI
    mirror uses a short cross-process file lock.
    """
    lock_path = f"{database_module.DATABASE_FILE}.processed-files.lock"
    with _PROCESSED_FILES_THREAD_LOCK:
        handle = open(lock_path, "a+b")
        try:
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            yield
        finally:
            try:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                elif msvcrt is not None:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                handle.close()


class LeaseLost(RuntimeError):
    """The caller must abandon this file without writing another terminal state."""


def transient(error):
    # A progress update may be coalesced on any bounded admission timeout,
    # while actual transaction retries are delegated to the database
    # coordinator (which excludes external-owner/I/O failures).
    return isinstance(error, TimeoutError) or is_transient_write_error(error)


def transaction(callback, attempts=5, lock_timeout=None):
    """Run the DB-only state transaction through the shared coordinator."""
    return write_transaction(
        callback,
        attempts=max(1, min(5, int(attempts or 1))),
        operation="realtime.ingestion.state",
        lock_timeout=lock_timeout,
    )


def processed_transaction(callback, attempts=5, lock_timeout=None):
    """Run a short projection transaction under the mirror-only guard.

    Authoritative ingestion writes must use :func:`transaction` directly.
    This helper is intentionally reserved for the non-authoritative
    ``processed_files`` projection so a missing or contended mirror can never
    roll back a durable lease, chunk checkpoint, or task enqueue.
    """
    with _processed_files_guard():
        return transaction(callback, attempts=attempts, lock_timeout=lock_timeout)


def _projection_snapshot(row, *, status=None, path=None, error=None) -> dict:
    """Convert an authoritative ingestion row into a mirror snapshot."""
    snapshot = dict(row)
    if status is not None:
        snapshot["status"] = status
        snapshot["processing_phase"] = (
            "queued" if status == "retrying" else status
        )
    if path is not None:
        snapshot["file_path"] = path
    if error is not None:
        snapshot["error_detail"] = str(error)[:4000]
    snapshot["campaign_type"] = snapshot.get("campaign_type") or "realtime"
    snapshot["file_hash"] = str(snapshot.get("file_hash") or "")
    snapshot["file_path"] = str(snapshot.get("file_path") or "")
    snapshot["file_size"] = int(snapshot.get("file_size") or 0)
    snapshot["source_state_version"] = int(snapshot.get("state_version") or 0)
    return snapshot


def _project_processed_snapshot(snapshot: dict) -> int | None:
    """Upsert a mirror row without allowing an older snapshot to regress it.

    The guard is deliberately held only for this short projection write.  The
    version fence is still required because a delayed projector can arrive
    after a newer authoritative terminal state has already been projected.
    """
    columns = (
        "campaign_type", "campaign_id", "filename", "file_hash", "file_path",
        "file_size", "status", "total_rows", "created_tasks", "error_detail",
        "whitelist_version_id", "processing_started_at", "last_progress_at",
        "processing_phase", "processed_rows", "matched_rows", "current_chunk_index",
        "total_chunks", "worker_id", "retry_count", "last_error_class", "last_error_at",
        "next_retry_at", "max_retry_count", "source_state_version", "completed_at",
    )
    values = [
        snapshot.get("campaign_type") or "realtime",
        int(snapshot.get("campaign_id") or 0),
        os.path.basename(str(snapshot.get("filename") or "")),
        snapshot.get("file_hash") or "",
        snapshot.get("file_path") or "",
        int(snapshot.get("file_size") or 0),
        snapshot.get("status") or "detected",
        int(snapshot.get("total_rows") or 0),
        int(snapshot.get("created_tasks") or 0),
        str(snapshot.get("error_detail") or "")[:4000],
        snapshot.get("whitelist_version_id"),
        snapshot.get("processing_started_at"),
        snapshot.get("last_progress_at"),
        snapshot.get("processing_phase") or "queued",
        int(snapshot.get("processed_rows") or 0),
        int(snapshot.get("matched_rows") or 0),
        int(snapshot.get("current_chunk_index") or 0),
        int(snapshot.get("total_chunks") or 0),
        snapshot.get("worker_id") or "",
        int(snapshot.get("retry_count") or 0),
        snapshot.get("last_error_class") or "",
        snapshot.get("last_error_at"),
        snapshot.get("next_retry_at"),
        int(snapshot.get("max_retry_count") or 4),
        int(snapshot.get("source_state_version") or 0),
        snapshot.get("completed_at"),
    ]

    assignments = ",".join(
        f"{column}=?" for column in columns[3:]
    )

    def write(conn):
        key = (values[0], values[1], values[2])
        version = values[24]
        updated = conn.execute(
            f"""UPDATE processed_files SET {assignments}
                WHERE campaign_type=? AND campaign_id=? AND filename=?
                  AND COALESCE(source_state_version,-1)<=?
                RETURNING id""",
            [*values[3:], *key, version],
        ).fetchone()
        if updated:
            return int(updated["id"])

        inserted = conn.execute(
            f"""INSERT INTO processed_files ({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                ON CONFLICT (campaign_type,campaign_id,filename) DO NOTHING
                RETURNING id""",
            values,
        ).fetchone()
        if inserted:
            return int(inserted["id"])
        row = conn.execute(
            """SELECT id FROM processed_files
               WHERE campaign_type=? AND campaign_id=? AND filename=?""",
            key,
        ).fetchone()
        return int(row["id"]) if row else None

    return processed_transaction(write, attempts=5, lock_timeout=1.0)


def project_processed_file(file_id: int, *, status=None, path=None, error=None) -> int | None:
    """Best-effort projection of the current authoritative file state.

    Callers use this after committing authoritative state.  Projection errors
    are observable and repairable, but are never allowed to fail the business
    operation that already committed.
    """
    try:
        row = read_transaction(
            lambda conn: conn.execute(
                "SELECT * FROM ingestion_files WHERE id=?", (file_id,)
            ).fetchone(),
            operation="realtime.ingestion.projection_source",
        )
    except Exception as exc:
        metrics_service.inc("realtime_processed_projection_failures_total")
        logger.warning(
            "Realtime processed-file projection source read deferred file_id=%s error=%s",
            file_id, exc, exc_info=True,
        )
        return None
    if not row:
        return None
    snapshot = _projection_snapshot(row, status=status, path=path, error=error)
    try:
        return _project_processed_snapshot(snapshot)
    except Exception as exc:
        metrics_service.inc("realtime_processed_projection_failures_total")
        logger.warning(
            "Realtime processed-file projection deferred file_id=%s error=%s",
            file_id, exc, exc_info=True,
        )
        return None


def reconcile_processed_files(*, campaign_id=None, file_id=None, filename="", repair=False,
                              limit=None) -> dict:
    """Check or repair the display projection from authoritative ingestion rows."""
    clauses = ["f.campaign_type='realtime'"]
    params = []
    if campaign_id is not None:
        clauses.append("f.campaign_id=?")
        params.append(int(campaign_id))
    if file_id is not None:
        clauses.append("f.id=?")
        params.append(int(file_id))
    if filename:
        clauses.append("f.filename=?")
        params.append(os.path.basename(filename))
    limit_sql = ""
    query_params = list(params)
    if limit is not None:
        limit_sql = " LIMIT ?"
        query_params.append(max(1, int(limit)))
    rows = read_transaction(
        lambda conn: conn.execute(
            f"""SELECT f.*, p.id AS processed_id,
                       p.source_state_version AS projected_state_version,
                       p.status AS projected_status
                FROM ingestion_files f
                LEFT JOIN processed_files p
                  ON p.campaign_type=f.campaign_type
                 AND p.campaign_id=f.campaign_id
                 AND p.filename=f.filename
                WHERE {' AND '.join(clauses)}
                ORDER BY f.id""" + limit_sql,
            query_params,
        ).fetchall(),
        operation="realtime.ingestion.projection_reconcile",
    )
    issues = []
    repaired = []
    for row in rows:
        item = dict(row)
        current_version = int(item.get("state_version") or 0)
        projected_version = item.get("projected_state_version")
        issue = (
            item.get("processed_id") is None
            or projected_version is None
            or int(projected_version) < current_version
            or str(item.get("projected_status") or "") != str(item.get("status") or "")
        )
        if issue:
            metrics_service.inc("realtime_processed_projection_drift_total")
            issues.append({
                "file_id": int(item["id"]),
                "campaign_id": int(item["campaign_id"]),
                "filename": item["filename"],
                "authority_status": item.get("status"),
                "authority_state_version": current_version,
                "projected_state_version": projected_version,
                "projected_status": item.get("projected_status"),
            })
            if repair:
                projected_id = _project_processed_snapshot(_projection_snapshot(item))
                metrics_service.inc("realtime_processed_projection_repairs_total")
                repaired.append({"file_id": int(item["id"]), "processed_id": projected_id})
    return {
        "scope": {"campaign_id": campaign_id, "file_id": file_id, "filename": filename or ""},
        "checked": len(rows),
        "issues": issues,
        "repaired": repaired,
        "consistent": not issues if not repair else not reconcile_processed_files(
            campaign_id=campaign_id, file_id=file_id, filename=filename, repair=False, limit=limit
        )["issues"],
    }


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
        read_transaction(
            lambda conn: assert_owned(conn, file_id, token),
            operation="realtime.ingestion.lease_check",
        )


def register_processed(campaign_id, path, filename, file_id, token):
    from services import operations_service
    digest, size = operations_service.file_digest(path), os.path.getsize(path)
    if os.path.splitext(filename)[1].lower() not in operations_service.ALLOWED_EXTENSIONS:
        raise ValueError("unsupported file type")
    def write(conn):
        assert_owned(conn, file_id, token)
        updated = conn.execute(
            """UPDATE ingestion_files SET file_hash=?,file_path=?,file_size=?,
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP
               WHERE id=? RETURNING *""",
            (digest, os.path.abspath(path), size, file_id),
        ).fetchone()
        if not updated:
            raise LeaseLost(f"realtime file disappeared: {file_id}")
        return dict(updated)

    # The file hash is calculated before this short authority transaction.  A
    # mirror insert/update is intentionally a separate best-effort projection.
    authoritative = transaction(write, attempts=5)
    processed_id = None
    try:
        processed_id = _project_processed_snapshot(
            _projection_snapshot(authoritative, status="validating", path=path)
        )
    except Exception as exc:
        metrics_service.inc("realtime_processed_projection_failures_total")
        logger.warning(
            "Realtime processed-file registration projection deferred file_id=%s error=%s",
            file_id, exc, exc_info=True,
        )
    return processed_id, False


def finish_processed(file_id, processed_id, token, status, path, error):
    # This is an early display hint used before the worker performs its
    # authoritative retry/terminal transition.  It must not become a second
    # source of truth or make that transition depend on the mirror row.
    check_owned(file_id, token)
    project_processed_file(file_id, status=status, path=path, error=error)


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
            transaction(
                self._write, attempts=5 if force else 1,
                lock_timeout=None if force else 0.1,
            )
        except Exception as exc:
            if force or not transient(exc):
                raise
            # Display state can be coalesced; never retry the whole input file for this.
            metrics_service.inc("realtime_file_progress_deferred_total")
            return False
        # Projection is deliberately after the authority commit.  A missing
        # row/table only defers UI repair; it cannot roll back progress.
        project_processed_file(self.file_id)
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
               state_version=COALESCE(state_version,0)+1,
               last_progress_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status IN ('pending','processing') RETURNING id""",
            (phase, rows, matched, chunk, chunks, self.worker_id, self.version_id, self.file_id),
        ).fetchone()
        if not updated:
            raise LeaseLost(f"realtime file no longer active: {self.file_id}")


def prepare_chunk(file_id, index, start, end, token=""):
    def write(conn):
        assert_owned(conn, file_id, token)
        conn.execute(
            """INSERT INTO ingestion_chunks(file_id,chunk_index,start_row,end_row) VALUES (?,?,?,?)
               ON CONFLICT (file_id,chunk_index) DO NOTHING""", (file_id, index, start, end),
        )
        conn.execute(
            """UPDATE ingestion_files SET total_chunks=GREATEST(total_chunks,?),
               total_rows=GREATEST(total_rows,?),state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP WHERE id=?""",
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
    result = transaction(write)
    project_processed_file(file_id)
    return result


def complete_chunk_in_transaction(conn, file_id, chunk, result, token=""):
    assert_owned(conn, file_id, token)
    row = conn.execute("SELECT status,lease_token FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()
    if not row or row["status"] != "processing" or row["lease_token"] != chunk["lease_token"]:
        raise LeaseLost(f"realtime chunk lease lost: {chunk['id']}")
    ingestion_job_service.complete_chunk_in_transaction(conn, chunk["id"], result)


def record_chunk_progress_in_transaction(conn, file_id, chunk, result, token=""):
    assert_owned(conn, file_id, token)
    row = conn.execute("SELECT status,lease_token FROM ingestion_chunks WHERE id=?", (chunk["id"],)).fetchone()
    if not row or row["status"] != "processing" or row["lease_token"] != chunk["lease_token"]:
        raise LeaseLost(f"realtime chunk lease lost: {chunk['id']}")
    ingestion_job_service.record_chunk_progress_in_transaction(conn, chunk["id"], result)


def complete_chunk(file_id, chunk, result, token=""):
    transaction(lambda conn: complete_chunk_in_transaction(conn, file_id, chunk, result, token))
    project_processed_file(file_id)


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
    project_processed_file(file_id)


def transition(file_id, processed_id, token, status, error_class="", error="", delay=0,
               path="", rows=0, created=0, campaign_id=None, filename="", total_msisdn=0, matched=0):
    """Commit terminal/retry state in authority, then refresh the mirror."""
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
               worker_id='',error_detail=?,last_error_class=?,state_version=COALESCE(state_version,0)+1,
               updated_at=CURRENT_TIMESTAMP,
               retry_count=retry_count+?,next_retry_at=CASE WHEN ?='retrying'
               THEN CAST(CURRENT_TIMESTAMP+(? * INTERVAL '1 second') AS VARCHAR) ELSE NULL END,
               completed_at=CASE WHEN ?='retrying' THEN NULL ELSE CAST(CURRENT_TIMESTAMP AS VARCHAR) END,
               last_progress_at=CURRENT_TIMESTAMP WHERE id=?""",
            (status, phase, str(error)[:4000], error_class, int(status == "retrying"), status, max(1, delay), status, file_id),
        )
        # Clear abandoned chunks now; completed checkpoints remain intact for replay.
        if status != "completed":
            conn.execute(
                """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL,
                   error_detail=?,updated_at=CURRENT_TIMESTAMP WHERE file_id=? AND status='processing'""",
                (str(error)[:4000], file_id),
            )
    transaction(write)
    project_processed_file(file_id, path=path or None, error=error or None)


def recover_expired_files():
    def write(conn):
        expired = conn.execute(
            """UPDATE ingestion_files SET status='pending',processing_phase='queued',lease_token='',
               lease_expires_at=NULL,worker_id='',error_detail='file lease expired; recovered at runtime',
               state_version=COALESCE(state_version,0)+1,updated_at=CURRENT_TIMESTAMP WHERE campaign_type='realtime' AND status='processing'
               AND TRY_CAST(lease_expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP RETURNING id,campaign_id,filename""",
        ).fetchall()
        for row in expired:
            conn.execute(
                """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL
                   WHERE file_id=? AND status='processing'""", (row["id"],),
            )
        return [int(row["id"]) for row in expired]
    expired_ids = transaction(write)
    for file_id in expired_ids:
        project_processed_file(file_id)
    return len(expired_ids)
