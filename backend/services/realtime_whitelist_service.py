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

from database import get_connection
from services import operations_service, sms_task_service, settings_service

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
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT v.* FROM realtime_whitelist_versions v
               JOIN realtime_campaigns c ON c.active_whitelist_version_id=v.id
               WHERE c.id=? AND v.status='ready'""", (campaign_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_version(version_id: int) -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM realtime_whitelist_versions WHERE id=?", (version_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def invalidate_version(version_id: int, error_detail: str) -> Optional[dict]:
    """Mark a broken index failed and restore the newest usable ready version."""
    conn = get_connection()
    try:
        version = conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE id=?", (version_id,)
        ).fetchone()
        if not version:
            return None
        campaign_id = version["campaign_id"]
        campaign = conn.execute(
            "SELECT active_whitelist_version_id FROM realtime_campaigns WHERE id=?",
            (campaign_id,),
        ).fetchone()
        conn.execute(
            "UPDATE realtime_whitelist_versions SET status='failed',error_detail=? WHERE id=?",
            (str(error_detail)[:4000], version_id),
        )
        if not campaign or campaign["active_whitelist_version_id"] != version_id:
            conn.commit()
            return None
        fallback = conn.execute(
            """SELECT * FROM realtime_whitelist_versions
               WHERE campaign_id=? AND status='ready' AND id<>?
                 AND COALESCE(index_path,'')<>''
               ORDER BY version_no DESC LIMIT 1""",
            (campaign_id, version_id),
        ).fetchone()
        if fallback:
            conn.execute(
                """UPDATE realtime_campaigns SET active_whitelist_version_id=?,
                   customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (fallback["id"], fallback["source_filename"], fallback["source_path"],
                 fallback["row_count"], campaign_id),
            )
        else:
            conn.execute(
                "UPDATE realtime_campaigns SET active_whitelist_version_id=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (campaign_id,),
            )
        conn.commit()
        return dict(fallback) if fallback else None
    finally:
        conn.close()


def latest_version(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM realtime_whitelist_versions WHERE campaign_id=? ORDER BY version_no DESC LIMIT 1",
            (campaign_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_version(campaign_id: int, source_filename: str, source_path: str,
                   source_hash: str) -> tuple[dict, bool]:
    conn = get_connection()
    try:
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
            conn.commit()
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
        conn.commit()
        return dict(row), False
    finally:
        conn.close()


def _set_status(version_id: int, status: str, **fields) -> None:
    assignments = ["status=?"]
    values = [status]
    for key, value in fields.items():
        assignments.append(f"{key}=?")
        values.append(value)
    values.append(version_id)
    conn = get_connection()
    try:
        conn.execute(f"UPDATE realtime_whitelist_versions SET {','.join(assignments)} WHERE id=?", values)
        conn.commit()
    finally:
        conn.close()


def _configure(conn) -> None:
    try:
        conn.execute("SET memory_limit = ?", (settings_service.get_setting(
            "realtime_whitelist_build_memory_limit",
            os.environ.get("MC_REALTIME_WHITELIST_BUILD_MEMORY_LIMIT", "512MB"),
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
    campaign = get_connection()
    try:
        campaign_row = campaign.execute("SELECT campaign_dir FROM realtime_campaigns WHERE id=?", (version["campaign_id"],)).fetchone()
    finally:
        campaign.close()
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
        conn = get_connection()
        try:
            conn.execute(
                """UPDATE realtime_whitelist_versions SET status='ready',row_count=?,normalized_count=?,
                   index_path=?,error_detail='',ready_at=CAST(CURRENT_TIMESTAMP AS VARCHAR) WHERE id=?""",
                (int(rows), int(count), final_path, version_id),
            )
            conn.execute(
                """UPDATE realtime_campaigns SET active_whitelist_version_id=?,customer_list_filename=?,
                   customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (version_id, version["source_filename"], version["source_path"], int(rows), version["campaign_id"]),
            )
            conn.commit()
        finally:
            conn.close()
        elapsed_ms = int((time.monotonic() - started) * 1000)
        _metrics_observe("realtime_whitelist_index_build_seconds", elapsed_ms / 1000.0,
                         {"campaign_id": version["campaign_id"]})
        _metrics_inc("realtime_whitelist_index_rows", count,
                     {"campaign_id": version["campaign_id"]})
        logger.info("Realtime whitelist index ready campaign=%s version=%s rows=%s normalized=%s elapsed_ms=%s",
                    version["campaign_id"], version_id, rows, count, elapsed_ms)
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
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id FROM realtime_whitelist_versions WHERE status='building' ORDER BY created_at,id LIMIT ?",
            (max(1, limit),),
        ).fetchall()
    finally:
        conn.close()
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
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id,campaign_dir,customer_list_filename,customer_list_path
               FROM realtime_campaigns
               WHERE use_whitelist=TRUE AND active_whitelist_version_id IS NULL
                 AND COALESCE(customer_list_path,'')<>''
                 AND NOT EXISTS (SELECT 1 FROM realtime_whitelist_versions v
                                 WHERE v.campaign_id=realtime_campaigns.id AND v.status='building')
               ORDER BY updated_at,id LIMIT ?""", (max(1, limit),)
        ).fetchall()
    finally:
        conn.close()
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
            conn = get_connection()
            try:
                conn.execute("UPDATE realtime_whitelist_versions SET source_path=? WHERE id=?", (stable_path, version["id"]))
                conn.commit()
            finally:
                conn.close()
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
    conn = get_connection()
    try:
        where = "WHERE v.status IN ('ready','retired')"
        params = []
        if campaign_id:
            where += " AND v.campaign_id=?"
            params.append(campaign_id)
        candidates = conn.execute(
            f"""SELECT v.*,c.active_whitelist_version_id FROM realtime_whitelist_versions v
                JOIN realtime_campaigns c ON c.id=v.campaign_id {where}""", params
        ).fetchall()
        retired = removed = 0
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
                conn.execute(
                    "UPDATE realtime_whitelist_versions SET status='retired',retired_at=CURRENT_TIMESTAMP WHERE id=?",
                    (row["id"],),
                )
                retired += 1
                continue
            old_enough = conn.execute(
                "SELECT CAST(COALESCE(retired_at,created_at) AS TIMESTAMP) < CURRENT_TIMESTAMP - (? * INTERVAL '1 day') old_enough",
                (retention_days,),
            ).fetchone()["old_enough"]
            if not old_enough:
                continue
            for path in (row["index_path"], row["source_path"]):
                if path and os.path.isfile(path):
                    try:
                        os.remove(path)
                    except OSError:
                        logger.warning("Unable to remove retired whitelist file path=%s", path)
            conn.execute(
                "UPDATE realtime_whitelist_versions SET index_path='',source_path='',error_detail=? WHERE id=?",
                (f"retired files removed after {retention_days} days", row["id"]),
            )
            removed += 1
        if retired or removed:
            conn.commit()
        return {"retired": retired, "removed": removed}
    finally:
        conn.close()


def upload(campaign_id: int, file_path: str, original_filename: str) -> dict:
    campaign = get_connection()
    try:
        row = campaign.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    finally:
        campaign.close()
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
            conn = get_connection()
            try:
                conn.execute(
                    "UPDATE realtime_campaigns SET active_whitelist_version_id=?,customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (existing["id"], existing["source_filename"], existing["source_path"], existing["row_count"], campaign_id),
                )
                conn.commit()
            finally:
                conn.close()
        return {**campaign, "success": True, "whitelist_version_id": existing["id"],
                "whitelist_status": existing["status"], "row_count": existing["row_count"]}
    version, _duplicate = create_version(campaign_id, original_filename, file_path, source_hash)
    source_path = os.path.join(source_dir, f"whitelist-v{int(version['version_no']):06d}{os.path.splitext(original_filename)[1].lower()}")
    shutil.copy2(file_path, source_path)
    conn = get_connection()
    try:
        conn.execute("UPDATE realtime_whitelist_versions SET source_path=?,row_count=? WHERE id=?",
                     (source_path, source_rows, version["id"]))
        conn.execute("UPDATE realtime_campaigns SET customer_list_filename=?,customer_list_path=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                     (original_filename, source_path, source_rows, campaign_id))
        conn.commit()
    finally:
        conn.close()
    logger.info("Realtime whitelist queued campaign=%s version=%s file=%s", campaign_id, version["id"], os.path.basename(original_filename))
    return {**campaign, "success": True, "whitelist_version_id": version["id"],
            "whitelist_status": "building", "row_count": source_rows, "customer_list_path": source_path,
            "customer_list_filename": original_filename}


def get_version_by_hash(campaign_id: int, source_hash: str) -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM realtime_whitelist_versions WHERE campaign_id=? AND source_hash=?",
                           (campaign_id, source_hash)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
