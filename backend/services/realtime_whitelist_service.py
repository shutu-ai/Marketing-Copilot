"""Versioned, disk-backed whitelist indexes for realtime campaigns."""

import csv
import logging
import os
import shutil
import tempfile
import time
from typing import Optional

import duckdb
import pandas as pd

from database import read_transaction, write_transaction
from services import ingestion_job_service, operations_service, sms_task_service, settings_service

logger = logging.getLogger(__name__)
INDEX_DIRNAME = "index"
BUILD_CHUNK_SIZE = max(1000, int(os.environ.get("MC_REALTIME_WHITELIST_BUILD_CHUNK", "10000")))


def _build_chunk_size() -> int:
    try:
        value = settings_service.get_setting(
            "realtime_whitelist_build_chunk_size",
            os.environ.get("MC_REALTIME_WHITELIST_BUILD_CHUNK", str(BUILD_CHUNK_SIZE)),
        )
        return max(1000, min(100000, int(value)))
    except (TypeError, ValueError):
        return BUILD_CHUNK_SIZE


def _build_timeout_seconds() -> int:
    try:
        value = settings_service.get_setting(
            "realtime_index_build_timeout",
            os.environ.get("MC_REALTIME_INDEX_BUILD_TIMEOUT", "7200"),
        )
        return max(0, int(value))
    except (TypeError, ValueError):
        return 7200


def _metrics_observe(name: str, value: float, labels=None) -> None:
    try:
        from services import metrics_service
        metrics_service.observe(name, value, labels)
    except Exception:
        logger.debug("Unable to record realtime metric %s", name, exc_info=True)


def _metrics_inc(name: str, amount: float = 1, labels=None) -> None:
    try:
        from services import metrics_service
        metrics_service.inc(name, amount, labels)
    except Exception:
        logger.debug("Unable to record realtime metric %s", name, exc_info=True)


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier returned from an uploaded CSV header."""
    return '"' + str(value).replace('"', '""') + '"'


def _create_csv_raw_table(db, path: str) -> list[str]:
    """Load a CSV into a DuckDB temp table and return its source columns."""
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
    first_line = sample.split("\n")[0].strip()
    if "|" in first_line:
        delimiter = "|"
    else:
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
        except csv.Error:
            delimiter = ","
    db.execute(
        """CREATE TEMP TABLE raw_whitelist AS
           SELECT * FROM read_csv_auto(?, header=true, all_varchar=true, delim=?)""",
        (path, delimiter),
    )
    return [row[1] for row in db.execute("PRAGMA table_info('raw_whitelist')").fetchall()]


def _build_csv_vectorized(db, path: str) -> tuple[int, int]:
    """Normalize and deduplicate a CSV inside DuckDB's vectorized engine.

    The legacy normalizer removes non-digits, strips one leading zero, and
    keeps the first language seen for a number.  The source-order column and
    arg_min() preserve those semantics without a Python loop per row.
    """
    columns = _create_csv_raw_table(db, path)
    if not columns:
        return 0, 0
    msisdn_column = "msisdn" if "msisdn" in columns else columns[0]
    msisdn = _quote_identifier(msisdn_column)
    language_column = "language" if "language" in columns else None
    language = _quote_identifier(language_column) if language_column else None
    normalized_msisdn = (
        "regexp_replace(COALESCE(CAST({0} AS VARCHAR), ''), '[^\\p{{Nd}}]', '', 'g')"
        .format(msisdn)
    )
    normalized_msisdn = (
        "CASE WHEN starts_with({0}, '0') THEN substr({0}, 2) ELSE {0} END"
        .format(normalized_msisdn)
    )
    if language:
        normalized_language = f"""COALESCE(
            CASE WHEN lower(trim(CAST({language} AS VARCHAR)))
                      IN ('nan', 'none', 'nat', '<na>')
                 THEN ''
                 ELSE trim(CAST({language} AS VARCHAR))
            END,
            ''
        )"""
    else:
        normalized_language = "''"
    source_rows = int(db.execute("SELECT COUNT(*) FROM raw_whitelist").fetchone()[0])
    db.execute(
        f"""INSERT INTO whitelist_msisdns(msisdn, language)
            WITH normalized AS (
                SELECT row_number() OVER () AS source_order,
                       {normalized_msisdn} AS msisdn,
                       {normalized_language} AS language
                FROM raw_whitelist
            ), deduplicated AS (
                SELECT msisdn, arg_min(language, source_order) AS language
                FROM normalized
                WHERE msisdn <> ''
                GROUP BY msisdn
            )
            SELECT msisdn, language FROM deduplicated""",
    )
    normalized_rows = int(db.execute("SELECT COUNT(*) FROM whitelist_msisdns").fetchone()[0])
    return source_rows, normalized_rows


def _source_row_count(path: str) -> int:
    """Count source records with bounded memory for the async upload response."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        with open(path, "r", encoding="utf-8-sig", newline="") as handle:
            sample = handle.read(8192)
            first_line = sample.split("\n")[0].strip()
            if "|" in first_line:
                delimiter = "|"
            else:
                try:
                    delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
                except csv.Error:
                    delimiter = ","
            handle.seek(0)
            return max(0, sum(1 for _ in csv.reader(handle, delimiter=delimiter)) - 1)
    try:
        return int(len(pd.read_excel(path, usecols=[0], dtype=str)))
    except Exception:
        return 0


def active_version(campaign_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            """SELECT v.* FROM realtime_whitelist_versions v
               JOIN realtime_campaigns c ON c.active_whitelist_version_id=v.id
               WHERE c.id=? AND v.status='ready'""", (campaign_id,),
        ).fetchone(),
        operation="whitelist.active_version",
    )
    return dict(row) if row else None


def get_version(version_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE id=?", (version_id,)
        ).fetchone(),
        operation="whitelist.version.get",
    )
    return dict(row) if row else None


def latest_version(campaign_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE campaign_id=? ORDER BY version_no DESC LIMIT 1",
            (campaign_id,),
        ).fetchone(),
        operation="whitelist.version.latest",
    )
    return dict(row) if row else None


def get_index_status(campaign: dict) -> dict:
    campaign_id = int(campaign["id"])
    waiting_files = ingestion_job_service.count_waiting_whitelist_files(campaign_id)
    if not campaign.get("use_whitelist", True):
        return {"status": "disabled", "waiting_file_count": waiting_files}

    active = get_version(campaign["active_whitelist_version_id"]) if campaign.get("active_whitelist_version_id") else None
    latest = latest_version(campaign_id)
    version = latest or active
    status = "missing"
    error_source = version
    if active and active.get("status") == "ready":
        status = "ready" if os.path.isfile(active.get("index_path") or "") else "missing_index"
        version = active
        # A previously failed, newer version may remain in the history after an
        # old release switched the campaign back. It must not make the active
        # version look broken in the detail page.
        error_source = active if status == "ready" else (latest or active)
    elif version and version.get("status") == "building":
        status = "building"
    elif version and version.get("status") == "failed":
        status = "failed"
    elif version and version.get("status") == "ready":
        status = "inactive"
    elif campaign.get("customer_list_path"):
        status = "queued" if os.path.isfile(campaign["customer_list_path"]) else "source_missing"

    return {
        "status": status,
        "version_no": version.get("version_no") if version else None,
        "active_version_no": active.get("version_no") if active else None,
        "row_count": int(version.get("row_count") or 0) if version else int(campaign.get("row_count") or 0),
        "normalized_count": int(version.get("normalized_count") or 0) if version else 0,
        "index_path": version.get("index_path") if version else "",
        "error_detail": (error_source or {}).get("error_detail") or "",
        "created_at": version.get("created_at") if version else None,
        "ready_at": active.get("ready_at") if active else None,
        "waiting_file_count": waiting_files,
    }


def create_version(campaign_id: int, source_filename: str, source_path: str,
                   source_hash: str) -> tuple[dict, bool]:
    def write(conn):
        existing = conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE campaign_id=? AND source_hash=?",
            (campaign_id, source_hash),
        ).fetchone()
        if existing and existing["status"] in ("building", "ready"):
            return dict(existing), True
        if existing:
            conn.execute(
                """UPDATE realtime_whitelist_versions SET source_filename=?,source_path=?,status='building',
                   error_detail='',ready_at=NULL,retired_at=NULL WHERE id=?""",
                (os.path.basename(source_filename), os.path.abspath(source_path), existing["id"]),
            )
            row = conn.execute("SELECT * FROM realtime_whitelist_versions WHERE id=?", (existing["id"],)).fetchone()
            return dict(row), False
        version_no = conn.execute(
            "SELECT COALESCE(MAX(version_no),0)+1 AS version_no FROM realtime_whitelist_versions WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()["version_no"]
        row = conn.execute(
            """INSERT INTO realtime_whitelist_versions
               (campaign_id,version_no,source_filename,source_path,source_hash,status)
               VALUES (?,?,?,?,?,'building') RETURNING *""",
            (campaign_id, int(version_no), os.path.basename(source_filename), os.path.abspath(source_path), source_hash),
        ).fetchone()
        return dict(row), False

    return write_transaction(write, operation="whitelist.version.create")


def _set_status(version_id: int, status: str, **fields) -> None:
    assignments = ["status=?"]
    values = [status]
    for key, value in fields.items():
        assignments.append(f"{key}=?")
        values.append(value)
    values.append(version_id)
    def write(conn):
        conn.execute(f"UPDATE realtime_whitelist_versions SET {','.join(assignments)} WHERE id=?", values)

    write_transaction(write, operation="whitelist.version.status")


def schedule_active_rebuild(version_id: int, error_detail: str = "") -> bool:
    """Put the currently active whitelist version back into the build queue.

    This deliberately keeps the campaign bound to ``version_id``.  It is a
    repair of the authoritative version, never a fallback to an older index.
    Returning False means the version is no longer active or its source is no
    longer available, so callers can fail the waiting file explicitly.
    """
    version = get_version(version_id)
    if not version:
        return False
    campaign = read_transaction(
        lambda conn: conn.execute(
            "SELECT active_whitelist_version_id,customer_list_path FROM realtime_campaigns WHERE id=?",
            (version["campaign_id"],),
        ).fetchone(),
        operation="whitelist.rebuild.campaign",
    )
    if not campaign:
        return False
    active_version_id = int(campaign["active_whitelist_version_id"] or 0)
    if active_version_id not in (0, int(version_id)):
        return False
    if active_version_id == 0:
        # A first whitelist build can fail before the campaign receives an
        # active binding. Only repair the newest failed version in that case;
        # never resurrect an older historical version.
        latest = latest_version(version["campaign_id"])
        if not latest or int(latest["id"]) != int(version_id):
            return False
    source_path = version.get("source_path") or ""
    if not os.path.isfile(source_path):
        # An upgrade or manual cleanup can remove the historical path. It is
        # safe to restore it only when the campaign's current source has the
        # exact hash recorded for this same version; never substitute another
        # whitelist silently.
        candidate = campaign["customer_list_path"] or ""
        try:
            same_source = (
                os.path.isfile(candidate)
                and operations_service.file_digest(candidate) == version.get("source_hash")
            )
        except Exception:
            same_source = False
        if not same_source:
            logger.error(
                "Active whitelist rebuild skipped: source missing or hash mismatch campaign=%s version=%s source=%s",
                version["campaign_id"], version_id, source_path,
            )
            return False
        write_transaction(
            lambda conn: conn.execute(
                "UPDATE realtime_whitelist_versions SET source_path=? WHERE id=?",
                (candidate, version_id),
            ),
            attempts=3,
            operation="whitelist.version.restore_source",
            base_delay=0.1,
        )
    if version.get("status") == "building":
        return True
    _set_status(
        version_id,
        "building",
        # Do not keep rendering the previous failure while the repair is in
        # progress. The reason remains in the service log and event record;
        # a new build failure will write a fresh error detail.
        error_detail="",
        ready_at=None,
        retired_at=None,
    )
    logger.warning(
        "Realtime whitelist rebuild scheduled campaign=%s version=%s reason=%s",
        version["campaign_id"], version_id, str(error_detail or "")[:500],
    )
    return True


def is_transient_database_error(error) -> bool:
    """Return True for short-lived DuckDB concurrency/lock failures.

    These errors describe contention in the application database, not damage
    to a whitelist index.  Callers must retry the current operation instead of
    rebuilding an otherwise unchanged multi-million-row whitelist.
    """
    message = str(error or "").lower()
    return any(marker in message for marker in (
        "write-write conflict",
        "transactioncontext",
        "transaction conflict",
        "database is locked",
        "could not set lock",
        "conflicting lock",
        "write lock timeout",
        "lock timeout",
    ))


def _is_repairable_error(error) -> bool:
    """Return True only for index/storage failures that justify a rebuild."""
    message = str(error or "").lower()
    return any(marker in message for marker in (
        "cannot read whitelist index", "cannot read active whitelist index",
        "catalog error", "io error", "corrupt",
    ))


def recover_transient_failure(version_id: int) -> str:
    """Recover a failed version after a transient database conflict.

    If the index file was fully materialized before the metadata transaction
    conflicted, validate it and restore the version to ``ready``.  If no valid
    index exists, put the version back into ``building`` so the normal build
    worker retries the incomplete build.  Return ``ready`` or ``building``;
    return an empty string when the version cannot be recovered.
    """
    version = get_version(version_id)
    if not version or version.get("status") != "failed":
        return ""
    index_path = version.get("index_path") or ""
    if index_path and os.path.isfile(index_path):
        index = None
        try:
            index = duckdb.connect(index_path, read_only=True)
            index.execute("SELECT 1 FROM whitelist_msisdns LIMIT 1").fetchone()
            _set_status(version_id, "ready", error_detail="")
            logger.warning(
                "Realtime whitelist transient failure recovered campaign=%s version=%s using existing index",
                version["campaign_id"], version_id,
            )
            return "ready"
        except Exception:
            logger.info(
                "Existing whitelist index is not usable after transient failure campaign=%s version=%s",
                version["campaign_id"], version_id, exc_info=True,
            )
        finally:
            if index:
                index.close()
    _set_status(version_id, "building", error_detail="")
    logger.warning(
        "Realtime whitelist transient failure queued for build retry campaign=%s version=%s",
        version["campaign_id"], version_id,
    )
    return "building"


def repair_active_failed(limit: int = 1) -> int:
    """Queue repair for active failed indexes without changing the binding."""
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT v.id,v.error_detail FROM realtime_whitelist_versions v
               JOIN realtime_campaigns c ON c.id=v.campaign_id
               WHERE v.status='failed'
                 AND (c.active_whitelist_version_id=v.id
                      OR (c.active_whitelist_version_id IS NULL AND v.version_no=(
                          SELECT MAX(v2.version_no) FROM realtime_whitelist_versions v2
                          WHERE v2.campaign_id=v.campaign_id)))
               ORDER BY v.created_at,v.id LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall(),
        operation="whitelist.repair.failed_candidates",
    )
    repaired = 0
    for row in rows:
        if is_transient_database_error(row["error_detail"]):
            try:
                if recover_transient_failure(row["id"]):
                    repaired += 1
            except Exception:
                logger.exception("Unable to recover transient whitelist failure version=%s", row["id"])
            continue
        if not _is_repairable_error(row["error_detail"]):
            continue
        try:
            if schedule_active_rebuild(row["id"], row["error_detail"]):
                repaired += 1
        except Exception:
            logger.exception("Unable to repair active failed whitelist version=%s", row["id"])
    if repaired:
        logger.warning("Realtime active whitelist repairs queued count=%s", repaired)
    return repaired


def repair_active_binding(limit: int = 10) -> int:
    """Restore a newer ready index as active when the binding is stale.

    A failed version must not hide a newer ready version. This is a binding
    repair, not a rollback: the candidate must have a higher version number
    than the failed active version (or the campaign must have no binding).
    """
    campaigns = read_transaction(
        lambda conn: conn.execute(
            """SELECT id,active_whitelist_version_id FROM realtime_campaigns
               WHERE use_whitelist=TRUE ORDER BY id LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall(),
        operation="whitelist.repair.binding_candidates",
    )
    restored = 0
    for campaign in campaigns:
        campaign_id = int(campaign["id"])
        active_id = int(campaign["active_whitelist_version_id"] or 0)
        active = get_version(active_id) if active_id else None
        if active and active.get("status") == "ready" and os.path.isfile(active.get("index_path") or ""):
            continue
        candidates = read_transaction(
            lambda conn: conn.execute(
                """SELECT * FROM realtime_whitelist_versions
                   WHERE campaign_id=? AND status IN ('ready','retired','failed')
                   ORDER BY version_no DESC""",
                (campaign_id,),
            ).fetchall(),
            operation="whitelist.repair.binding_versions",
        )
        candidate = None
        for row in candidates:
            index_path = row["index_path"] or ""
            if not os.path.isfile(index_path):
                continue
            probe = None
            try:
                probe = duckdb.connect(index_path, read_only=True)
                probe.execute("SELECT 1 FROM whitelist_msisdns LIMIT 1").fetchone()
                candidate = dict(row)
                break
            except Exception:
                continue
            finally:
                if probe:
                    probe.close()
        if not candidate:
            continue
        if not active:
            latest = latest_version(campaign_id)
            if latest and int(candidate["version_no"]) < int(latest["version_no"]):
                # A newer failed version exists; do not silently fall back to
                # an older ready index. Repair the newest version instead.
                continue
        if active and int(candidate["version_no"]) <= int(active.get("version_no") or 0):
            # Never fall back to an older version when the current version is
            # damaged or missing. It must be repaired from its own source.
            continue
        def restore(conn):
            conn.execute(
                """UPDATE realtime_whitelist_versions
                   SET status='ready',retired_at=NULL,error_detail=''
                   WHERE id=?""",
                (candidate["id"],),
            )
            conn.execute(
                """UPDATE realtime_campaigns SET active_whitelist_version_id=?,
                   customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (candidate["id"], candidate["source_filename"], candidate["source_path"],
                 int(candidate["row_count"] or 0), campaign_id),
            )
            if active_id:
                conn.execute(
                    """UPDATE ingestion_files SET whitelist_version_id=?,status='pending',
                       processing_phase='queued',retry_count=0,last_error_class='',last_error_at=NULL,
                       next_retry_at=NULL,error_detail='',lease_token='',lease_expires_at=NULL,
                       worker_id='',updated_at=CURRENT_TIMESTAMP
                       WHERE campaign_type='realtime' AND campaign_id=? AND whitelist_version_id=?
                         AND status IN ('pending','retrying','failed','quarantined')""",
                    (candidate["id"], campaign_id, active_id),
                )
            else:
                conn.execute(
                    """UPDATE ingestion_files SET whitelist_version_id=?,status='pending',
                       processing_phase='queued',retry_count=0,last_error_class='',last_error_at=NULL,
                       next_retry_at=NULL,error_detail='',lease_token='',lease_expires_at=NULL,
                       worker_id='',updated_at=CURRENT_TIMESTAMP
                       WHERE campaign_type='realtime' AND campaign_id=?
                         AND status IN ('pending','retrying','failed','quarantined')
                         AND (whitelist_version_id IS NULL
                              OR last_error_class IN ('WAITING_DEPENDENCY','WHITELIST_INDEX'))""",
                    (candidate["id"], campaign_id),
                )
        write_transaction(
            restore,
            attempts=3,
            operation="whitelist.repair.binding_restore",
        )
        restored += 1
        logger.warning(
            "Realtime active whitelist binding restored campaign=%s from_version=%s to_version=%s",
            campaign_id, active_id or "none", candidate["id"],
        )
    return restored


def _configure(conn) -> None:
    try:
        conn.execute("SET memory_limit = ?", (settings_service.get_setting(
            "realtime_whitelist_build_memory_limit",
            os.environ.get("MC_REALTIME_WHITELIST_BUILD_MEMORY_LIMIT", "4GB"),
        ),))
    except Exception:
        pass
    try:
        threads = max(1, int(settings_service.get_setting(
            "realtime_whitelist_build_threads",
            os.environ.get("MC_REALTIME_WHITELIST_BUILD_THREADS", "1"),
        )))
        conn.execute("SET threads = ?", (threads,))
    except Exception:
        pass


def build_version(version_id: int) -> dict:
    version = get_version(version_id)
    if not version:
        raise ValueError("whitelist version not found")
    if version["status"] == "ready" and os.path.isfile(version["index_path"]):
        return version
    if not os.path.isfile(version["source_path"]):
        message = "whitelist source file is missing"
        _set_status(version_id, "failed", error_detail=message)
        raise FileNotFoundError(message)
    campaign_row = read_transaction(
        lambda conn: conn.execute(
            "SELECT campaign_dir FROM realtime_campaigns WHERE id=?",
            (version["campaign_id"],),
        ).fetchone(),
        operation="whitelist.build.campaign_dir",
    )
    campaign_dir = campaign_row["campaign_dir"] if campaign_row else ""
    index_dir = os.path.join(campaign_dir, "whitelist", INDEX_DIRNAME)
    os.makedirs(index_dir, exist_ok=True)
    final_path = os.path.join(index_dir, f"whitelist-v{int(version['version_no']):06d}.duckdb")
    fd, temp_path = tempfile.mkstemp(prefix=f"whitelist-v{version['version_no']:06d}-", suffix=".duckdb", dir=index_dir)
    os.close(fd)
    os.unlink(temp_path)
    started = time.monotonic()
    timeout_seconds = _build_timeout_seconds()
    rows = normalized = 0
    try:
        db = duckdb.connect(temp_path)
        try:
            _configure(db)
            # Allow the vectorized aggregation to spill to the campaign disk
            # directory instead of competing with the process memory limit.
            try:
                db.execute("SET temp_directory = ?", (index_dir,))
            except Exception:
                pass
            db.execute("CREATE TABLE whitelist_msisdns (msisdn TEXT PRIMARY KEY, language TEXT NOT NULL DEFAULT '')")
            ext = os.path.splitext(version["source_path"])[1].lower()
            if ext == ".csv":
                rows, normalized = _build_csv_vectorized(db, version["source_path"])
                if timeout_seconds and time.monotonic() - started > timeout_seconds:
                    raise TimeoutError(f"whitelist index build exceeded {timeout_seconds} seconds")
            else:
                reader = [pd.read_excel(version["source_path"], dtype=str)]
                for chunk in reader:
                    if timeout_seconds and time.monotonic() - started > timeout_seconds:
                        raise TimeoutError(f"whitelist index build exceeded {timeout_seconds} seconds")
                    msisdn_col = "msisdn" if "msisdn" in chunk.columns else chunk.columns[0]
                    has_language = "language" in chunk.columns
                    batch = []
                    rows += len(chunk)
                    for idx, value in enumerate(chunk[msisdn_col]):
                        norm = sms_task_service.normalize_msisdn(value)
                        if not norm:
                            continue
                        language = str(chunk["language"].iloc[idx]).strip() if has_language else ""
                        if language.lower() in {"nan", "none", "nat", "<na>"}:
                            language = ""
                        batch.append((norm, language))
                    if batch:
                        db.executemany(
                            "INSERT INTO whitelist_msisdns(msisdn,language) VALUES (?,?) ON CONFLICT DO NOTHING",
                            batch,
                        )
                        normalized += len(batch)
                        db.commit()
            count = db.execute("SELECT COUNT(*) AS count FROM whitelist_msisdns").fetchone()[0]
            # normalized_count is the deduplicated index cardinality, not the
            # number of source rows that survived normalization.
            normalized = int(count)
        finally:
            db.close()
        if os.path.exists(final_path):
            os.unlink(final_path)
        os.replace(temp_path, final_path)
        def write(conn):
            conn.execute(
                """UPDATE realtime_whitelist_versions SET status='ready',row_count=?,normalized_count=?,
                   index_path=?,error_detail='',ready_at=CAST(CURRENT_TIMESTAMP AS VARCHAR) WHERE id=?""",
                (int(rows), int(count), final_path, version_id),
            )
            active_row = conn.execute(
                """SELECT c.active_whitelist_version_id,v.version_no AS active_version_no
                   FROM realtime_campaigns c
                   LEFT JOIN realtime_whitelist_versions v
                     ON v.id=c.active_whitelist_version_id
                   WHERE c.id=?""",
                (version["campaign_id"],),
            ).fetchone()
            active_version_no = int(active_row["active_version_no"] or 0) if active_row else 0
            if not active_row or not active_row["active_whitelist_version_id"] \
                    or active_version_no <= int(version["version_no"]):
                conn.execute(
                    """UPDATE realtime_campaigns SET active_whitelist_version_id=?,customer_list_filename=?,
                       customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                    (version_id, version["source_filename"], version["source_path"], int(rows), version["campaign_id"]),
                )
            return True

        write_transaction(
            write,
            attempts=3,
            operation="whitelist.version.ready",
            base_delay=0.1,
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)
        _metrics_observe("realtime_whitelist_index_build_seconds", elapsed_ms / 1000.0,
                         {"campaign_id": version["campaign_id"]})
        _metrics_inc("realtime_whitelist_index_rows", count,
                     {"campaign_id": version["campaign_id"]})
        logger.info("Realtime whitelist index ready campaign=%s version=%s rows=%s normalized=%s elapsed_ms=%s",
                    version["campaign_id"], version_id, rows, count, elapsed_ms)
        try:
            requeued = ingestion_job_service.requeue_waiting_whitelist_files(version["campaign_id"])
            if requeued:
                logger.info("Realtime whitelist ready campaign=%s version=%s waiting_files_requeued=%s",
                            version["campaign_id"], version_id, requeued)
        except Exception:
            logger.exception("Unable to requeue files after whitelist became ready campaign=%s version=%s",
                             version["campaign_id"], version_id)
        return get_version(version_id) or version
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        _metrics_observe("realtime_whitelist_index_build_seconds", elapsed_ms / 1000.0,
                         {"campaign_id": version["campaign_id"], "result": "failed"})
        _metrics_inc("realtime_whitelist_index_build_errors_total", labels={"campaign_id": version["campaign_id"]})
        _set_status(version_id, "failed", error_detail=str(exc)[:4000])
        logger.exception("Realtime whitelist index failed campaign=%s version=%s", version["campaign_id"], version_id)
        raise
    finally:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass


def build_pending(limit: int = 1) -> int:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT id FROM realtime_whitelist_versions WHERE status='building' ORDER BY created_at,id LIMIT ?",
            (max(1, limit),),
        ).fetchall(),
        operation="whitelist.build.pending",
    )
    built = 0
    for row in rows:
        try:
            build_version(row["id"])
            built += 1
        except Exception:
            continue
    return built


def migrate_legacy_versions(limit: int = 1) -> int:
    """Create build jobs for campaigns that predate versioned indexes.

    Hashing/copying is intentionally performed by the ingestion worker, never by
    the scheduler or HTTP request, so an old 4M-row whitelist cannot pause intake.
    """
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT id,campaign_dir,customer_list_filename,customer_list_path
               FROM realtime_campaigns
               WHERE use_whitelist=TRUE AND active_whitelist_version_id IS NULL
                 AND COALESCE(customer_list_path,'')<>''
                 AND NOT EXISTS (SELECT 1 FROM realtime_whitelist_versions v
                                 WHERE v.campaign_id=realtime_campaigns.id AND v.status='building')
               ORDER BY updated_at,id LIMIT ?""", (max(1, limit),)
        ).fetchall(),
        operation="whitelist.migrate.legacy_candidates",
    )
    migrated = 0
    for row in rows:
        source_path = row["customer_list_path"]
        if not source_path or not os.path.isfile(source_path):
            continue
        campaign_dir = row["campaign_dir"] or os.path.dirname(source_path)
        source_dir = os.path.join(campaign_dir, "whitelist", "sources")
        os.makedirs(source_dir, exist_ok=True)
        try:
            source_hash = operations_service.file_digest(source_path)
            existing = get_version_by_hash(row["id"], source_hash)
            if existing:
                version = existing
            else:
                version, _ = create_version(
                    row["id"], row["customer_list_filename"] or os.path.basename(source_path),
                    source_path, source_hash,
                )
            stable_path = os.path.join(
                source_dir, f"whitelist-v{int(version['version_no']):06d}{os.path.splitext(source_path)[1].lower()}"
            )
            if os.path.abspath(source_path) != os.path.abspath(stable_path) and not os.path.isfile(stable_path):
                shutil.copy2(source_path, stable_path)
            write_transaction(
                lambda conn: conn.execute(
                    "UPDATE realtime_whitelist_versions SET source_path=? WHERE id=?",
                    (stable_path, version["id"]),
                ),
                attempts=3,
                operation="whitelist.version.migrate_source",
                base_delay=0.1,
            )
            migrated += 1
            logger.info("Realtime legacy whitelist migration queued campaign=%s version=%s", row["id"], version["id"])
        except Exception:
            logger.exception("Realtime legacy whitelist migration failed campaign=%s", row["id"])
    return migrated


def cleanup_versions(campaign_id: Optional[int] = None) -> dict:
    """Retire and later remove unused version files without breaking bound jobs."""
    try:
        retention_days = max(1, int(settings_service.get_setting(
            "realtime_whitelist_retention_days",
            os.environ.get("MC_REALTIME_WHITELIST_RETENTION_DAYS", "7"),
        )))
    except (TypeError, ValueError):
        retention_days = 7
    def snapshot(conn):
        where = "WHERE v.status IN ('ready','retired')"
        params = []
        if campaign_id:
            where += " AND v.campaign_id=?"
            params.append(campaign_id)
        candidates = conn.execute(
            f"""SELECT v.*,c.active_whitelist_version_id FROM realtime_whitelist_versions v
                JOIN realtime_campaigns c ON c.id=v.campaign_id {where}""", params
        ).fetchall()
        ready_ids = []
        removable = []
        for row in candidates:
            if row["id"] == row["active_whitelist_version_id"]:
                continue
            bound = conn.execute(
                """SELECT COUNT(*) count FROM ingestion_files
                   WHERE whitelist_version_id=? AND status NOT IN ('completed','failed','quarantined')""",
                (row["id"],),
            ).fetchone()["count"]
            if int(bound or 0):
                continue
            if row["status"] == "ready":
                ready_ids.append(row["id"])
                continue
            old_enough = conn.execute(
                """SELECT CAST(COALESCE(retired_at,created_at) AS TIMESTAMP)
                          < CURRENT_TIMESTAMP - (? * INTERVAL '1 day') old_enough
                   FROM realtime_whitelist_versions WHERE id=?""",
                (retention_days, row["id"]),
            ).fetchone()["old_enough"]
            if old_enough:
                removable.append((row["id"], row["index_path"], row["source_path"]))
        return ready_ids, removable

    ready_ids, removable = read_transaction(
        snapshot,
        operation="whitelist.cleanup.snapshot",
    )

    # Retired files are removed outside the database admission. Only clear
    # paths for rows whose filesystem cleanup completed, so an I/O failure is
    # recoverable on the next retention pass.
    removable_ids = []
    for version_id, index_path, source_path in removable:
        cleanup_ok = True
        for path in (index_path, source_path):
            if not path or not os.path.isfile(path):
                continue
            try:
                os.remove(path)
            except OSError:
                cleanup_ok = False
                logger.warning("Unable to remove retired whitelist file path=%s", path)
        if cleanup_ok:
            removable_ids.append(version_id)

    def finalize(conn):
        retired = 0
        for version_id in ready_ids:
            conn.execute(
                """UPDATE realtime_whitelist_versions SET status='retired',retired_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status='ready' AND id<>COALESCE((
                       SELECT active_whitelist_version_id FROM realtime_campaigns c
                       JOIN realtime_whitelist_versions v ON v.campaign_id=c.id WHERE v.id=?
                   ),0)""",
                (version_id, version_id),
            )
            retired += 1
        removed = 0
        for version_id in removable_ids:
            conn.execute(
                """UPDATE realtime_whitelist_versions AS v SET index_path='',source_path='',error_detail=?
                   WHERE v.id=? AND v.status='retired'
                     AND v.id<>COALESCE((SELECT active_whitelist_version_id FROM realtime_campaigns c
                                        WHERE c.id=v.campaign_id),0)
                     AND NOT EXISTS (SELECT 1 FROM ingestion_files f
                                     WHERE f.whitelist_version_id=v.id
                                       AND f.status NOT IN ('completed','failed','quarantined'))""",
                (f"retired files removed after {retention_days} days", version_id),
            )
            removed += 1
        return {"retired": retired, "removed": removed}

    return write_transaction(
        finalize,
        attempts=3,
        operation="whitelist.cleanup.finalize",
    )


def upload(campaign_id: int, file_path: str, original_filename: str) -> dict:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone(),
        operation="whitelist.upload.campaign",
    )
    if not row:
        return {"success": False, "message": "Campaign not found"}
    campaign = dict(row)
    campaign_dir = campaign.get("campaign_dir") or os.path.join(os.path.dirname(file_path), f"campaign-{campaign_id}")
    source_dir = os.path.join(campaign_dir, "whitelist", "sources")
    os.makedirs(source_dir, exist_ok=True)
    source_hash = operations_service.file_digest(file_path)
    source_rows = _source_row_count(file_path)
    existing = get_version_by_hash(campaign_id, source_hash)
    if existing and existing["status"] in ("building", "ready"):
        if existing["status"] == "ready":
            def write(conn):
                conn.execute(
                    "UPDATE realtime_campaigns SET active_whitelist_version_id=?,customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (existing["id"], existing["source_filename"], existing["source_path"], existing["row_count"], campaign_id),
                )
            write_transaction(write, operation="whitelist.version.activate")
        return {**campaign, "success": True, "whitelist_version_id": existing["id"],
                "whitelist_status": existing["status"], "row_count": existing["row_count"]}
    # Materialize the durable source before publishing the ``building`` row.
    # The scheduler can pick a new version immediately after create_version;
    # recording the request's temporary UploadFile path first lets that worker
    # race the router's finally block and observe FileNotFoundError.
    fd, source_path = tempfile.mkstemp(
        prefix="whitelist-upload-",
        suffix=os.path.splitext(original_filename)[1].lower(),
        dir=source_dir,
    )
    os.close(fd)
    try:
        shutil.copy2(file_path, source_path)
    except Exception:
        try:
            os.unlink(source_path)
        except OSError:
            pass
        raise
    version, _duplicate = create_version(campaign_id, original_filename, source_path, source_hash)
    source_path = version.get("source_path") or source_path
    def write(conn):
        conn.execute("UPDATE realtime_whitelist_versions SET source_path=?,row_count=? WHERE id=?",
                     (source_path, source_rows, version["id"]))
        conn.execute("UPDATE realtime_campaigns SET customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                     (original_filename, source_path, source_rows, campaign_id))
    write_transaction(write, operation="whitelist.upload")
    logger.info("Realtime whitelist queued campaign=%s version=%s file=%s", campaign_id, version["id"], os.path.basename(original_filename))
    return {**campaign, "success": True, "whitelist_version_id": version["id"],
            "whitelist_status": "building", "row_count": source_rows, "customer_list_path": source_path,
            "customer_list_filename": original_filename}


def get_version_by_hash(campaign_id: int, source_hash: str) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE campaign_id=? AND source_hash=?",
            (campaign_id, source_hash),
        ).fetchone(),
        operation="whitelist.version.by_hash",
    )
    return dict(row) if row else None
