import os
import json
import re
import math
import stat
import shutil
import hashlib
import logging
import queue
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import csv
import time
import duckdb
import tempfile
import threading
from database import get_connection, read_transaction, write_transaction
from models.realtime_campaign import RealtimeCampaignCreate, RealtimeCampaignUpdate
from services import settings_service
from services import metrics_service
from services import sms_task_service
from services import (operations_service, campaign_name_service, ingestion_job_service,
                      backpressure_service, realtime_whitelist_service, realtime_ingestion_state)

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")

CHUNK_SIZE = 5000  # fallback; the effective value is read from app settings
MAX_PENDING_FILES_PER_CAMPAIGN = max(1, int(os.environ.get("MC_REALTIME_MAX_PENDING_FILES_PER_CAMPAIGN", "100")))
MAX_INBOX_FILES = max(1, int(os.environ.get("MC_REALTIME_MAX_INBOX_FILES", "1000")))
MAX_INBOX_BYTES = max(1, int(os.environ.get("MC_REALTIME_MAX_INBOX_GB", "10")) * 1024 ** 3)
try:
    WATCH_RESCAN_SECONDS = max(3.0, float(os.environ.get("MC_REALTIME_WATCH_RESCAN_SECONDS", "30")))
except (TypeError, ValueError):
    WATCH_RESCAN_SECONDS = 30.0
try:
    WATCH_STABILITY_SECONDS = max(
        0.5, float(os.environ.get("MC_REALTIME_WATCH_STABILITY_SECONDS", "5"))
    )
except (TypeError, ValueError):
    WATCH_STABILITY_SECONDS = 5.0
try:
    WATCH_PENDING_RECHECK_SECONDS = max(
        0.2, float(os.environ.get("MC_REALTIME_WATCH_PENDING_RECHECK_SECONDS", "1"))
    )
except (TypeError, ValueError):
    WATCH_PENDING_RECHECK_SECONDS = 1.0
WATCH_REQUIRE_READY = str(os.environ.get("MC_REALTIME_REQUIRE_READY", "0")).strip().lower() in {
    "1", "true", "yes", "on",
}
try:
    WATCH_COPY_QUEUE_SIZE = max(1, min(256, int(os.environ.get("MC_REALTIME_COPY_QUEUE_SIZE", "32"))))
except (TypeError, ValueError):
    WATCH_COPY_QUEUE_SIZE = 32
try:
    WATCH_COPY_WORKERS = max(1, min(8, int(os.environ.get("MC_REALTIME_COPY_WORKERS", "2"))))
except (TypeError, ValueError):
    WATCH_COPY_WORKERS = 2
try:
    WATCH_DISCOVERY_QUANTUM = max(
        1, min(64, int(os.environ.get("MC_REALTIME_DISCOVERY_QUANTUM", "2")))
    )
except (TypeError, ValueError):
    WATCH_DISCOVERY_QUANTUM = 2
_daily_finalize_lock = threading.Lock()
_daily_finalize_date = None
_daily_finalized_windows = set()
_daily_started_windows = set()
_backpressure_log_lock = threading.Lock()
_backpressure_log_state = {}
_watch_skip_log_lock = threading.Lock()
_watch_skip_log_state = {}
_watch_scan_lock = threading.Lock()
_watch_scan_state = {}
_watch_pending_stability = {}
_watch_copy_queue = queue.Queue(maxsize=WATCH_COPY_QUEUE_SIZE)
_watch_copy_stop = threading.Event()
_watch_copy_threads = []
_watch_copy_start_lock = threading.Lock()
_watch_campaign_cursor = 0
_sftp_campaign_cursor = 0


class WatchFileChanged(RuntimeError):
    """The producer changed a file while it was being handed off."""


class InputSchemaError(ValueError):
    """The input does not contain an explicit, auditable recipient column."""


def _run_campaign_write(operation: str, writer):
    return write_transaction(
        writer,
        attempts=3,
        operation=f"realtime_campaign.{operation}",
        base_delay=0.1,
    )


def _realtime_chunk_size() -> int:
    """Return the current bounded input chunk size without requiring a restart."""
    # Keep the module fallback monkeypatchable for the existing streaming tests.
    if CHUNK_SIZE != 5000:
        return max(1, min(100000, int(CHUNK_SIZE)))
    try:
        configured = settings_service.get_setting(
            "realtime_match_chunk_size",
            os.environ.get("MC_REALTIME_MATCH_CHUNK_SIZE", str(CHUNK_SIZE)),
        )
        return max(500, min(100000, int(configured)))
    except (TypeError, ValueError):
        return CHUNK_SIZE


def _clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none", "<na>"} else text


def _watch_snapshot(path: str) -> tuple[int, int, int]:
    info = os.stat(path)
    return (
        int(info.st_size),
        int(getattr(info, "st_mtime_ns", int(info.st_mtime * 1_000_000_000))),
        int(getattr(info, "st_ino", 0) or 0),
    )


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _copy_watch_file_atomic(src: str, dest: str) -> tuple[int, int, int]:
    """Copy through an inbox-local staging file and publish atomically.

    The source is checked before and after the copy, and the copied bytes are
    hashed against a second source read.  This prevents a changed source from
    being published as a formal inbox file.  The staging file is always in
    the inbox directory, so ``os.replace`` is a same-filesystem publication.
    """
    started = time.monotonic()
    before = _watch_snapshot(src)
    if os.path.exists(dest):
        raise FileExistsError(dest)
    inbox_dir = os.path.dirname(dest)
    fd, staged = tempfile.mkstemp(
        prefix=f".{os.path.basename(dest)}.", suffix=".part", dir=inbox_dir,
    )
    os.close(fd)
    try:
        shutil.copyfile(src, staged)
        with open(staged, "r+b") as handle:
            os.fsync(handle.fileno())
        copied_hash = _sha256_file(staged)
        after = _watch_snapshot(src)
        source_hash = _sha256_file(src)
        final_source = _watch_snapshot(src)
        if before != after or after != final_source or before[0] != os.path.getsize(staged):
            raise WatchFileChanged(f"source changed during copy: {src}")
        if copied_hash != source_hash:
            raise WatchFileChanged(f"source content changed during copy: {src}")
        os.replace(staged, dest)
        return final_source
    finally:
        metrics_service.observe_histogram(
            "realtime_watch_atomic_copy_seconds", time.monotonic() - started,
        )
        try:
            if os.path.exists(staged):
                os.unlink(staged)
        except OSError:
            logger.warning("Unable to clean watch staging file=%s", staged, exc_info=True)


def _pending_watch_key(campaign_id: int, src: str) -> tuple[int, str]:
    return int(campaign_id), os.path.abspath(src)


def _track_watch_stability(campaign_id: int, src: str, snapshot: tuple[int, int, int],
                           producer_ready: bool = False) -> bool:
    """Return whether a source may be copied under the configured policy.

    A producer completion marker is authoritative.  Without one this is an
    explicitly heuristic compatibility path: an old enough mtime is only a
    hint and the atomic copy still rejects changes observed during copying.
    """
    key = _pending_watch_key(campaign_id, src)
    now = time.monotonic()
    if producer_ready:
        with _watch_scan_lock:
            _watch_pending_stability.pop(key, None)
        metrics_service.inc(
            "realtime_watch_stability_ready_total",
            labels={"campaign_id": campaign_id},
        )
        return True
    if WATCH_REQUIRE_READY:
        with _watch_scan_lock:
            current = _watch_pending_stability.get(key)
            if not current or current["snapshot"] != snapshot:
                _watch_pending_stability[key] = {
                    "snapshot": snapshot, "first_seen": now,
                    "next_check": now + WATCH_PENDING_RECHECK_SECONDS,
                }
                metrics_service.set_gauge(
                    "realtime_watch_first_seen_timestamp", time.time(),
                    labels={"campaign_id": campaign_id},
                )
            else:
                current["next_check"] = now + WATCH_PENDING_RECHECK_SECONDS
        metrics_service.inc(
            "realtime_watch_stability_pending_total",
            labels={"campaign_id": campaign_id, "mode": "ready_required"},
        )
        return False
    age = max(0.0, time.time() - (snapshot[1] / 1_000_000_000))
    if age >= WATCH_STABILITY_SECONDS:
        with _watch_scan_lock:
            _watch_pending_stability.pop(key, None)
        metrics_service.inc(
            "realtime_watch_stability_accepted_total",
            labels={"campaign_id": campaign_id, "mode": "mtime_heuristic"},
        )
        return True
    with _watch_scan_lock:
        current = _watch_pending_stability.get(key)
        if not current or current["snapshot"] != snapshot:
            _watch_pending_stability[key] = {
                "snapshot": snapshot, "first_seen": now,
                "next_check": now + WATCH_PENDING_RECHECK_SECONDS,
            }
            metrics_service.set_gauge(
                "realtime_watch_first_seen_timestamp", time.time(),
                labels={"campaign_id": campaign_id},
            )
        else:
            current["next_check"] = now + WATCH_PENDING_RECHECK_SECONDS
    metrics_service.inc(
        "realtime_watch_stability_pending_total",
        labels={"campaign_id": campaign_id, "mode": "mtime_heuristic"},
    )
    return False


def _pending_watch_names(campaign_id: int, watch_dir: str) -> list[str]:
    prefix = (int(campaign_id), os.path.abspath(watch_dir))
    with _watch_scan_lock:
        return sorted(
            os.path.basename(path)
            for (cid, path), state in _watch_pending_stability.items()
            if cid == prefix[0] and os.path.dirname(path) == prefix[1]
            and state.get("next_check", 0) <= time.monotonic()
        )


def _csv_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
    # Detect delimiter: check for pipe (|) in first line, fall back to Sniffer
    first_line = sample.split("\n")[0].strip()
    if "|" in first_line and first_line.count("|") >= 1:
        delimiter = "|"
    else:
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
        except csv.Error:
            delimiter = ","
    return delimiter


def _read_csv(path: str, chunksize: int = 0):
    """Read CSV file. If chunksize > 0, returns a chunk iterator instead of a DataFrame."""
    delimiter = _csv_delimiter(path)
    if chunksize:
        return pd.read_csv(path, dtype=str, sep=delimiter, chunksize=chunksize)
    return pd.read_csv(path, dtype=str, sep=delimiter)


def _read_csv_columns(path: str):
    return pd.read_csv(path, dtype=str, sep=_csv_delimiter(path), nrows=0).columns


_MSISDN_ALIASES = {
    "msisdn": "msisdn",
    "msisdn_number": "msisdn",
    "mobile": "msisdn",
    "mobile_number": "msisdn",
    "phone": "msisdn",
    "phone_number": "msisdn",
    "recipient": "msisdn",
}


def _configured_msisdn_column() -> str:
    configured = os.environ.get("MC_REALTIME_MSISDN_COLUMN", "")
    if not configured:
        configured = settings_service.get_setting("realtime_msisdn_column", "") or ""
    return str(configured).lstrip("\ufeff").strip().lower()


def _resolve_msisdn_column(columns) -> str:
    """Resolve a recipient column from an explicit name or audited alias."""
    original = list(columns) if columns is not None else []
    if not original:
        raise InputSchemaError("input has no header row")
    normalized = [str(value).lstrip("\ufeff").strip().lower() for value in original]
    if any(not value for value in normalized):
        raise InputSchemaError("input contains an empty column name")
    duplicate_suffixes = {
        value.split(".", 1)[0]
        for value in normalized
        if re.match(r"^(?:msisdn|msisdn_number|mobile|mobile_number|phone|phone_number|recipient)\.\d+$", value)
    }
    if duplicate_suffixes:
        raise InputSchemaError(
            "input contains duplicate recipient column names: "
            + ", ".join(sorted(duplicate_suffixes))
        )
    duplicates = {value for value in normalized if normalized.count(value) > 1}
    if duplicates:
        raise InputSchemaError(
            "input contains duplicate column names: " + ", ".join(sorted(duplicates))
        )
    configured = _configured_msisdn_column()
    if configured:
        matches = [index for index, value in enumerate(normalized) if value == configured]
        if len(matches) != 1:
            raise InputSchemaError(
                f"configured MSISDN column '{configured}' is missing or duplicated"
            )
        return original[matches[0]]
    matches = [index for index, value in enumerate(normalized) if value in _MSISDN_ALIASES]
    if len(matches) != 1:
        if not matches:
            raise InputSchemaError(
                "missing explicit MSISDN column; accepted names: "
                + ", ".join(sorted(_MSISDN_ALIASES))
            )
        raise InputSchemaError("multiple possible MSISDN columns; configure one explicitly")
    return original[matches[0]]


def _get_base_dir() -> str:
    return settings_service.get_setting("realtime_base_dir") or settings_service.get_setting("batch_base_dir") or os.path.join(
        os.path.dirname(__file__), "..", "uploads", "batch_campaigns"
    )


def _sanitize_dirname(name: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    s = s.rstrip(". ")
    if not s:
        s = "campaign"
    return s


def _ensure_campaign_dir(campaign: dict) -> str:
    base = _get_base_dir()
    dirname = _sanitize_dirname(campaign.get("name", f"campaign_{campaign['id']}"))
    return os.path.join(base, dirname)


def _fmt_days(days: Optional[list]) -> str:
    return json.dumps(days, ensure_ascii=False) if days else "[]"


def _parse_days(val) -> list:
    if not val:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return []


def _enrich(c: dict) -> dict:
    c["send_days"] = _parse_days(c.get("send_days", ""))
    return c


def public_campaign(campaign: Optional[dict]) -> Optional[dict]:
    """Redact SSH credentials before returning a campaign from the API."""
    if not campaign:
        return campaign
    result = dict(campaign)
    if result.get("sftp_password"):
        result["sftp_password"] = "********"
    result.pop("sftp_identity_file", None)
    return result


def get_sftp_password(campaign_id: int) -> str:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT sftp_password FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        return (row["sftp_password"] or "") if row else ""
    finally:
        conn.close()


def get_all() -> list:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_campaigns ORDER BY created_at DESC"
        ).fetchall(),
        operation="realtime_campaign.list",
    )
    return [_enrich(dict(r)) for r in rows]


def list_campaigns(page: int = 1, page_size: int = 50) -> dict:
    def read(conn):
        total = conn.execute("SELECT COUNT(*) cnt FROM realtime_campaigns").fetchone()["cnt"]
        offset = max(0, page - 1) * page_size
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (page_size, offset),
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="realtime_campaign.list_page")
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_enrich(dict(r)) for r in rows],
    }


def get_one(campaign_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone(),
        operation="realtime_campaign.get",
    )
    return _enrich(dict(row)) if row else None


def create(data: RealtimeCampaignCreate) -> dict:
    name = campaign_name_service.ensure_unique_name(data.name)
    if not (data.phone_prefix or "").strip():
        raise ValueError("请输入号码前缀")
    use_custom_directory = bool(data.use_custom_directory and (data.campaign_dir or "").strip())
    campaign_dir = (data.campaign_dir or "").strip() if use_custom_directory else ""

    def write(conn):
        cursor = conn.execute(
            "INSERT INTO realtime_campaigns (name, use_whitelist, start_time, end_time, send_start_time, send_end_time, send_days, campaign_dir, use_custom_directory, watch_source, watch_dir, sftp_host, sftp_port, sftp_username, sftp_password, watch_filename_prefix, phone_prefix, sms_template_id, smpp_config_id, flash_sms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, data.use_whitelist if data.use_whitelist is not None else True,
             data.start_time or "", data.end_time or "", data.send_start_time or "",
             data.send_end_time or "", _fmt_days(data.send_days), campaign_dir,
             use_custom_directory, data.watch_source or "local", data.watch_dir or "",
             data.sftp_host or "", data.sftp_port or 22, data.sftp_username or "",
             data.sftp_password or "", data.watch_filename_prefix or "Android_4G_Switch_Somali_",
             data.phone_prefix or "", data.sms_template_id, data.smpp_config_id,
             bool(data.flash_sms)),
        )
        return dict(conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (cursor.lastrowid,)
        ).fetchone())

    campaign = _enrich(_run_campaign_write("create", write))
    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        _run_campaign_write(
            "create.directory",
            lambda conn: conn.execute(
                "UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?",
                (campaign_dir, campaign["id"]),
            ),
        )
        campaign["campaign_dir"] = campaign_dir
    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "received"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    return campaign


def update(campaign_id: int, data: RealtimeCampaignUpdate) -> Optional[dict]:
    probe = get_connection()
    try:
        existing = probe.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
    finally:
        probe.close()
    if not existing:
        return None
    existing_dict = dict(existing)
    if data.name is not None and data.name.strip() != (existing_dict.get("name") or "").strip():
        data.name = campaign_name_service.ensure_unique_name(data.name, "realtime", campaign_id)

    auto_campaign_dir = ""
    validation_campaign = dict(existing_dict)
    if data.status == "in_progress":
        if not (validation_campaign.get("campaign_dir") or "").strip() and not bool(validation_campaign.get("use_custom_directory")):
            auto_campaign_dir = _ensure_campaign_dir(validation_campaign)
            validation_campaign["campaign_dir"] = auto_campaign_dir
        validation_errors = operations_service.validate_campaign(
            "realtime", {**validation_campaign, "id": campaign_id}
        )
        if validation_errors:
            raise ValueError("; ".join(validation_errors))
        if validation_campaign.get("use_whitelist", True) and not validation_campaign.get("row_count", 0):
            raise ValueError("No whitelist uploaded - cannot start campaign")
    if existing_dict.get("status") != "not_started":
        only_allowed = all(
            getattr(data, field, None) is None
            for field in ["name", "start_time", "campaign_dir", "flash_sms"]
        )
        if not only_allowed:
            raise ValueError("Campaign has already started - cannot edit")

    fields = {}
    for field in ["name", "use_whitelist", "start_time", "end_time", "status", "send_start_time", "send_end_time", "campaign_dir", "sms_template_id", "smpp_config_id", "flash_sms", "watch_source", "watch_dir", "sftp_host", "sftp_port", "sftp_username", "sftp_password", "watch_filename_prefix", "phone_prefix", "use_custom_directory"]:
        value = getattr(data, field, None)
        if value is not None:
            if field == "sftp_password" and value in ("", "********"):
                continue
            fields[field] = value
    if auto_campaign_dir:
        fields["campaign_dir"] = auto_campaign_dir
    if data.send_days is not None:
        fields["send_days"] = _fmt_days(data.send_days)
    if data.use_custom_directory is not None:
        if data.use_custom_directory:
            campaign_dir = (data.campaign_dir or "").strip()
            if not campaign_dir:
                raise ValueError("Custom campaign directory is required")
            fields["campaign_dir"] = campaign_dir
        else:
            fields["campaign_dir"] = _ensure_campaign_dir({**existing_dict, **fields})
    if data.use_custom_directory is not None and existing_dict.get("customer_list_path"):
        new_dir = fields.get("campaign_dir", "")
        old_dir = (existing_dict.get("campaign_dir") or "").strip()
        if new_dir and os.path.normcase(os.path.normpath(new_dir)) != os.path.normcase(os.path.normpath(old_dir)):
            raise ValueError("Cannot change campaign directory after a whitelist has been uploaded")
    new_smpp_id = fields.get("smpp_config_id")
    new_flash_sms = fields.get("flash_sms")

    def write(conn):
        current = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        if not current:
            return None
        if new_smpp_id is not None:
            smpp = conn.execute(
                "SELECT id FROM smpp_configs WHERE id=? AND enabled=1", (new_smpp_id,)
            ).fetchone()
            if not smpp:
                raise ValueError("Associated SMPP configuration is missing or disabled")
        if fields:
            set_clause = ", ".join(f"{key}=?" for key in fields)
            conn.execute(
                f"UPDATE realtime_campaigns SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
                (*fields.values(), campaign_id),
            )
        if new_smpp_id is not None:
            conn.execute(
                """UPDATE sms_tasks SET smpp_config_id=?, updated_at=CURRENT_TIMESTAMP
                   WHERE campaign_type='realtime' AND campaign_id=? AND status IN ('queued','retrying','sending')""",
                (new_smpp_id, campaign_id),
            )
        if new_flash_sms is not None:
            conn.execute(
                """UPDATE sms_tasks SET flash_sms=?, updated_at=CURRENT_TIMESTAMP
                   WHERE campaign_type='realtime' AND campaign_id=? AND status IN ('queued','retrying','sending')""",
                (bool(new_flash_sms), campaign_id),
            )
        row = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        return dict(row) if row else None

    updated = _run_campaign_write("update", write)
    if updated is None:
        return None
    sms_task_service.invalidate_campaign_cache("realtime", campaign_id)
    result = _enrich(updated)
    if data.use_custom_directory is not None:
        os.makedirs(os.path.join(result["campaign_dir"], "whitelist"), exist_ok=True)
        os.makedirs(os.path.join(result["campaign_dir"], "received"), exist_ok=True)
        os.makedirs(os.path.join(result["campaign_dir"], "output"), exist_ok=True)
    return result


def delete(campaign_id: int) -> bool:
    probe = get_connection()
    try:
        row = probe.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
    finally:
        probe.close()
    if not row:
        return False
    _run_campaign_write(
        "delete",
        lambda conn: (
            conn.execute("DELETE FROM realtime_campaign_files WHERE campaign_id=?", (campaign_id,)),
            conn.execute("DELETE FROM realtime_campaigns WHERE id=?", (campaign_id,)),
        ),
    )
    campaign_dir = dict(row).get("campaign_dir", "")
    if campaign_dir and os.path.isdir(campaign_dir):
        try:
            shutil.rmtree(campaign_dir)
        except Exception:
            logger.warning("Unable to remove deleted realtime campaign directory=%s", campaign_dir, exc_info=True)
    sms_task_service.invalidate_campaign_cache("realtime", campaign_id)
    return True


def copy(campaign_id: int) -> Optional[dict]:
    def write(conn):
        row = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        if not row:
            return None
        orig = dict(row)
        cursor = conn.execute(
            "INSERT INTO realtime_campaigns (name, use_whitelist, start_time, end_time, send_start_time, send_end_time, send_days, sms_template_id, smpp_config_id, flash_sms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (orig["name"] + " (Copy)", orig.get("use_whitelist", True),
             orig["start_time"], orig["end_time"], orig.get("send_start_time", ""),
             orig.get("send_end_time", ""), orig.get("send_days", "[]"),
             orig.get("sms_template_id"), orig.get("smpp_config_id"),
             bool(orig.get("flash_sms"))),
        )
        new_row = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=?", (cursor.lastrowid,)
        ).fetchone()
        return {"original": orig, "campaign": dict(new_row)}

    result = _run_campaign_write("copy", write)
    if result is None:
        return None
    orig = result["original"]
    new_campaign = _enrich(result["campaign"])
    campaign_dir = _ensure_campaign_dir(new_campaign)
    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "received"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    old_dir = orig.get("campaign_dir") or _ensure_campaign_dir(orig)
    old_whitelist = os.path.join(old_dir, "whitelist")
    if os.path.isdir(old_whitelist):
        for fname in os.listdir(old_whitelist):
            src = os.path.join(old_whitelist, fname)
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(campaign_dir, "whitelist", fname))
    _run_campaign_write(
        "copy.directory",
        lambda conn: conn.execute(
            "UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?",
            (campaign_dir, new_campaign["id"]),
        ),
    )
    new_campaign["campaign_dir"] = campaign_dir
    return new_campaign


def upload_customer_list(campaign_id: int, file_path: str, original_filename: str) -> Optional[dict]:
    campaign = get_one(campaign_id)
    if not campaign:
        return None
    campaign_dir = campaign.get("campaign_dir", "")
    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
        os.makedirs(os.path.join(campaign_dir, "received"), exist_ok=True)
        os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
        _run_campaign_write(
            "upload.directory",
            lambda conn: conn.execute(
                "UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?",
                (campaign_dir, campaign_id),
            ),
        )
    _ensure_realtime_dirs(campaign_dir)
    result = realtime_whitelist_service.upload(campaign_id, file_path, original_filename)
    if not result.get("success"):
        return None
    conn = get_connection()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    sms_task_service.invalidate_campaign_cache("realtime", campaign_id)
    return {**(dict(row) if row else {}),
            "whitelist_version_id": result.get("whitelist_version_id"),
            "whitelist_status": result.get("whitelist_status", "building"),
            "row_count": result.get("row_count", (dict(row) if row else {}).get("row_count", 0)),
            "customer_list_filename": result.get("customer_list_filename", (dict(row) if row else {}).get("customer_list_filename", "")),
            "customer_list_path": result.get("customer_list_path", (dict(row) if row else {}).get("customer_list_path", "")),
            }


def queue_file(campaign_id: int, file_path: str, original_filename: str) -> dict:
    """Persist an input file and enqueue it; processing is performed by the worker."""
    campaign = get_one(campaign_id)
    if not campaign:
        return {"success": False, "message": "Campaign not found"}
    active, reason = sms_task_service.campaign_is_active(campaign)
    if not active:
        return {"success": False, "message": reason}
    campaign_dir = campaign.get("campaign_dir") or _ensure_campaign_dir(campaign)
    if not campaign.get("campaign_dir"):
        _run_campaign_write(
            "upload.directory",
            lambda conn: conn.execute(
                "UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?",
                (campaign_dir, campaign_id),
            ),
        )
    _ensure_realtime_dirs(campaign_dir)
    inbox_dir = os.path.join(campaign_dir, "inbox")
    os.makedirs(inbox_dir, exist_ok=True)
    safe_name = os.path.basename(original_filename)
    dest = os.path.join(inbox_dir, safe_name)
    if os.path.exists(dest):
        return {"success": True, "queued": True, "duplicate_file": True, "filename": safe_name,
                "message": "file is already queued"}
    reservation = _reserve_inbox_capacity(
        campaign_id, safe_name, os.path.getsize(file_path), "upload",
    )
    if not reservation.get("ok"):
        if reservation.get("duplicate"):
            return {"success": True, "queued": True, "duplicate_file": True,
                    "filename": safe_name, "message": reservation["reason"]}
        logger.warning("Realtime inbox backpressure campaign=%s file=%s reason=%s",
                       campaign_id, safe_name, reservation["reason"])
        return {"success": False, "status": "backpressure", "retryable": True,
                "message": reservation["reason"]}
    reservation_token = reservation["token"]
    consumed = False
    published = False
    try:
        _copy_watch_file_atomic(file_path, dest)
        published = True
        file_id, duplicate = ingestion_job_service.register_file(
            "realtime", campaign_id, safe_name, dest, chunk_size=_realtime_chunk_size(),
            whitelist_version_id=(realtime_whitelist_service.active_version(campaign_id) or {}).get("id"),
            reservation_token=reservation_token,
        )
        consumed = True
        try:
            os.unlink(file_path)
        except FileNotFoundError:
            pass
    finally:
        if published and not consumed:
            try:
                os.unlink(dest)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning(
                    "Unable to remove unpublished upload inbox file=%s", dest, exc_info=True,
                )
        if not consumed:
            ingestion_job_service.release_realtime_capacity(reservation_token)
    if duplicate:
        duplicate_path = operations_service.archive_duplicate(dest, campaign_dir, safe_name)
        return {"success": True, "queued": False, "duplicate_file": True,
                "duplicate_path": duplicate_path, "filename": safe_name}
    logger.info("Realtime file queued campaign=%s file=%s ingestion_file_id=%s", campaign_id, safe_name, file_id)
    return {"success": True, "queued": True, "filename": safe_name,
            "ingestion_file_id": file_id, "status": "queued", "processing_phase": "queued"}


def _count_output_rows(campaign_dir: str, date_prefix: str = "") -> int:
    output_dir = os.path.join(campaign_dir, "output")
    if not os.path.isdir(output_dir):
        return 0
    total = 0
    for fname in os.listdir(output_dir):
        if not fname.endswith(".csv"):
            continue
        if date_prefix and not fname.startswith(date_prefix):
            continue
        path = os.path.join(output_dir, fname)
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=["msisdn"])
            total += len(df)
        except Exception:
            pass
    return total


_REALTIME_TASK_STATUS_KEYS = (
    "queued", "sending", "submitted", "accepted", "delivered",
    "retrying", "failed", "expired", "cancelled", "archived",
)


def _realtime_task_status_counts(conn, campaign_id: int, date_value: Optional[str] = None) -> dict:
    """Return one consistent task-state view for realtime campaign metrics.

    Counts are based on sms_tasks, not the output CSV snapshot. ``delivered``
    means a confirmed delivery receipt; ``accepted`` means the SMSC accepted
    the submission and is kept separate from confirmed delivery.
    """
    where = "campaign_type='realtime' AND campaign_id=?"
    params = [campaign_id]
    if date_value:
        where += " AND CAST(created_at AS DATE)=CAST(? AS DATE)"
        params.append(date_value)
    rows = conn.execute(
        f"SELECT status, COUNT(*) AS count FROM sms_tasks WHERE {where} GROUP BY status",
        params,
    ).fetchall()
    raw = {str(row["status"] or ""): int(row["count"] or 0) for row in rows}
    result = {key: raw.get(key, 0) for key in _REALTIME_TASK_STATUS_KEYS}
    result["task_created"] = sum(raw.values())
    result["accepted_or_delivered"] = result["accepted"] + result["delivered"]
    result["reached"] = result["delivered"]
    return result


def get_detail(campaign_id: int) -> Optional[dict]:
    campaign = get_one(campaign_id)
    if not campaign:
        return None
    conn = get_connection()
    try:
        task_counts = _realtime_task_status_counts(conn, campaign_id)
    finally:
        conn.close()
    campaign["reached_users"] = task_counts["delivered"]
    campaign["task_created_users"] = task_counts["task_created"]
    campaign["accepted_users"] = task_counts["accepted"]
    campaign["delivered_users"] = task_counts["delivered"]
    campaign["whitelist_index"] = realtime_whitelist_service.get_index_status(campaign)
    return campaign


def _append_realtime_output(campaign_dir: str, rows: list[dict]) -> None:
    """Append only the current matched chunk to today's output file."""
    if not campaign_dir or not rows:
        return
    output_dir = os.path.join(campaign_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{datetime.now():%Y%m%d}.csv")
    pd.DataFrame(rows, columns=["msisdn", "send_time"]).to_csv(
        output_path, index=False, mode="a",
        header=not os.path.isfile(output_path), encoding="utf-8-sig",
    )


def _task_msisdn_for_campaign(value, campaign: dict) -> str:
    normalized = sms_task_service.normalize_msisdn(value)
    prefix = (campaign.get("phone_prefix") or "").replace(" ", "").replace("\t", "").replace("\r", "").replace("\n", "")
    return prefix + normalized if prefix and not normalized.startswith(prefix) else normalized


def _inbox_capacity_snapshot(campaign_id: int) -> dict:
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT
                 SUM(CASE WHEN campaign_id=? THEN 1 ELSE 0 END) AS campaign_count,
                 COUNT(*) AS total_count,
                 COALESCE(SUM(file_size),0) AS total_bytes
               FROM ingestion_files
               WHERE campaign_type='realtime' AND status IN ('pending','processing','retrying')""",
            (campaign_id,),
        ).fetchone()
    finally:
        conn.close()
    return {
        "campaign_count": int(row["campaign_count"] or 0),
        "total_count": int(row["total_count"] or 0),
        "total_bytes": int(row["total_bytes"] or 0),
    }


def _inbox_capacity_available(campaign_id: int, file_size: int = 0,
                              capacity: Optional[dict] = None) -> tuple[bool, str]:
    capacity = capacity or _inbox_capacity_snapshot(campaign_id)
    campaign_count = capacity["campaign_count"]
    total_count = capacity["total_count"]
    total_bytes = capacity["total_bytes"]
    if int(campaign_count or 0) >= MAX_PENDING_FILES_PER_CAMPAIGN:
        return False, "campaign realtime inbox pending limit reached"
    if int(total_count or 0) >= MAX_INBOX_FILES:
        return False, "global realtime inbox pending limit reached"
    if int(total_bytes or 0) + int(file_size or 0) > MAX_INBOX_BYTES:
        return False, "global realtime inbox byte limit reached"
    return True, ""


def _record_inbox_capacity_file(capacity: dict, file_size: int) -> None:
    capacity["campaign_count"] += 1
    capacity["total_count"] += 1
    capacity["total_bytes"] += max(0, int(file_size or 0))


def _reserve_inbox_capacity(campaign_id: int, filename: str, file_size: int,
                            source: str) -> dict:
    return ingestion_job_service.reserve_realtime_capacity(
        campaign_id, filename, file_size,
        MAX_PENDING_FILES_PER_CAMPAIGN, MAX_INBOX_FILES, MAX_INBOX_BYTES,
        source=source,
    )


def _copy_watch_job(job: dict) -> bool:
    campaign_id = int(job["campaign_id"])
    fname = str(job["filename"])
    reservation_token = str(job.get("reservation_token") or "")
    consumed = False
    published = False
    started = time.monotonic()
    dest = job["dest"]
    try:
        _copy_watch_file_atomic(job["src"], dest)
        published = True
        whitelist_version_id = (realtime_whitelist_service.active_version(campaign_id) or {}).get("id")
        file_id, duplicate = ingestion_job_service.register_file(
            "realtime", campaign_id, fname, dest,
            chunk_size=_realtime_chunk_size(),
            whitelist_version_id=whitelist_version_id,
            reservation_token=reservation_token,
        )
        consumed = True
        metrics_service.set_gauge(
            "realtime_watch_last_registered_timestamp", time.time(),
            labels={"campaign_id": campaign_id},
        )
        if duplicate:
            duplicate_path = operations_service.archive_duplicate(
                dest, job["campaign_dir"], fname,
            )
            logger.info(
                "Watch duplicate archived campaign=%s file=%s path=%s",
                campaign_id, fname, duplicate_path,
            )
        else:
            logger.info(
                "Watch file copied campaign=%s src=%s dest=%s ingestion_file_id=%s",
                campaign_id, job["src"], dest, file_id,
            )
        return True
    except (WatchFileChanged, FileNotFoundError, OSError) as exc:
        # Keep the source for a later stability check; only the inbox-local
        # staging file is removed by _copy_watch_file_atomic.
        logger.info(
            "Watch file handoff deferred campaign=%s file=%s reason=%s",
            campaign_id, fname, exc,
        )
        metrics_service.inc(
            "realtime_watch_copy_deferred_total",
            labels={"reason": type(exc).__name__},
        )
    except Exception:
        logger.exception("Watch file handoff failed campaign=%s file=%s", campaign_id, fname)
        metrics_service.inc("realtime_watch_copy_failures_total")
    finally:
        metrics_service.observe_histogram(
            "realtime_watch_handoff_seconds", time.monotonic() - started,
            labels={"campaign_id": campaign_id},
        )
        if published and not consumed:
            try:
                os.unlink(dest)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning(
                    "Unable to remove unpublished watch inbox file=%s", dest, exc_info=True,
                )
        if not consumed and reservation_token:
            try:
                ingestion_job_service.release_realtime_capacity(reservation_token)
            except Exception:
                logger.exception(
                    "Unable to release watch reservation campaign=%s file=%s",
                    campaign_id, fname,
                )
    return False


def _watch_copy_worker() -> None:
    while not _watch_copy_stop.is_set():
        try:
            job = _watch_copy_queue.get(timeout=0.2)
        except queue.Empty:
            continue
        try:
            _copy_watch_job(job)
        finally:
            _watch_copy_queue.task_done()
            metrics_service.set_gauge(
                "realtime_watch_copy_queue_depth", _watch_copy_queue.qsize(),
            )


def start_watch_copy_workers() -> None:
    with _watch_copy_start_lock:
        if any(thread.is_alive() for thread in _watch_copy_threads):
            return
        _watch_copy_stop.clear()
        _watch_copy_threads[:] = [
            threading.Thread(
                target=_watch_copy_worker,
                name=f"realtime-watch-copy-{index + 1}",
                daemon=True,
            )
            for index in range(WATCH_COPY_WORKERS)
        ]
        for thread in _watch_copy_threads:
            thread.start()


def stop_watch_copy_workers() -> None:
    _watch_copy_stop.set()
    for thread in list(_watch_copy_threads):
        if thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2)
    while True:
        try:
            job = _watch_copy_queue.get_nowait()
        except queue.Empty:
            break
        try:
            token = job.get("reservation_token")
            if token:
                ingestion_job_service.release_realtime_capacity(token)
        finally:
            _watch_copy_queue.task_done()
    _watch_copy_threads.clear()


def _log_inbox_backpressure(campaign_id: int, rejected: int, reason: str,
                            source: str = "local") -> None:
    """Rate-limit repeated capacity warnings while retaining suppressed counts."""
    key = (source, int(campaign_id), reason)
    now = time.monotonic()
    with _backpressure_log_lock:
        last, suppressed = _backpressure_log_state.get(key, (0.0, 0))
        if now - last < 30.0:
            _backpressure_log_state[key] = (last, suppressed + int(rejected or 0))
            return
        _backpressure_log_state[key] = (now, 0)
    logger.warning(
        "%s realtime inbox backpressure campaign=%s rejected=%s suppressed=%s reason=%s",
        source.upper(), campaign_id, int(rejected or 0), suppressed, reason,
    )


def _lookup_whitelist_languages(conn, msisdns, query_size: int = 1000) -> dict:
    """Load only the current input chunk's whitelist matches into memory."""
    values = list({value for value in msisdns if value})
    result = {}
    query_size = max(100, min(1000, int(query_size)))
    for start in range(0, len(values), query_size):
        current = values[start:start + query_size]
        values_sql = ",".join("(?)" for _ in current)
        rows = conn.execute(
            f"""WITH current_chunk(msisdn) AS (VALUES {values_sql})
                SELECT w.msisdn,w.language FROM whitelist_msisdns w
                JOIN current_chunk c ON c.msisdn=w.msisdn""",
            current,
        ).fetchall()
        result.update({row[0]: row[1] for row in rows})
    return result


def _is_transient_whitelist_index_error(error: Exception) -> bool:
    return realtime_whitelist_service.is_transient_database_error(error)


def _is_repairable_whitelist_error(error) -> bool:
    """Identify index failures that can be repaired from the same source file."""
    message = str(error or "").lower()
    return any(marker in message for marker in (
        "cannot read whitelist index",
        "cannot read active whitelist index",
        "catalog error",
        "io error",
        "corrupt",
    ))


def _schedule_active_whitelist_rebuild(campaign_id: int, version_id: Optional[int], error) -> bool:
    if not version_id:
        return False
    try:
        scheduled = realtime_whitelist_service.schedule_active_rebuild(version_id, str(error))
    except Exception:
        logger.exception(
            "Unable to schedule active whitelist rebuild campaign=%s version=%s",
            campaign_id, version_id,
        )
        return False
    if scheduled:
        operations_service.event(
            "realtime_ingestion", "WHITELIST_INDEX_REBUILD_SCHEDULED",
            f"Rebuilding active whitelist index: {str(error)[:1000]}",
            severity="warning",
            context={"campaign_id": campaign_id, "version_id": version_id},
        )
    return scheduled


def _open_whitelist_index(path: str):
    """Open and minimally validate a persistent whitelist index.

    A version file can be replaced while a worker is starting a read-only
    connection. DuckDB reports that short race as a lock or write conflict;
    retry the open before treating the index as damaged.
    """
    last_error = None
    for attempt in range(3):
        conn = None
        try:
            conn = duckdb.connect(path, read_only=True)
            # Connecting to a damaged DuckDB file can succeed before the first
            # table access. Validate the schema before consuming any input rows.
            conn.execute("SELECT 1 FROM whitelist_msisdns LIMIT 1").fetchone()
            return conn
        except Exception as exc:
            last_error = exc
            if conn:
                conn.close()
            if not _is_transient_whitelist_index_error(exc) or attempt >= 2:
                raise
            time.sleep(0.2 * (attempt + 1))
    raise last_error


def _batch_seen_msisdns(conn, normalized, languages, use_whitelist):
    """One chunk relation/write, retaining first input positions across chunks."""
    stage = pd.DataFrame({"ordinal": range(len(normalized)), "msisdn": list(normalized)})
    valid = stage["msisdn"].astype(bool)
    invalid = int((~valid).sum())
    stage = stage.loc[valid].copy()
    stage["matched"] = stage["msisdn"].isin(languages).astype(int) if use_whitelist else 1
    matched_rows = int(stage["matched"].sum())
    stage = stage.drop_duplicates("msisdn", keep="first")
    if stage.empty:
        return [], invalid, matched_rows
    conn.register("chunk_seen_input", stage)
    try:
        inserted = conn.execute(
            """INSERT INTO seen_msisdns(msisdn,matched)
               SELECT msisdn,matched FROM chunk_seen_input
               ON CONFLICT DO NOTHING RETURNING msisdn""",
        ).fetchall()
        new_numbers = {row[0] for row in inserted}
        positions = stage.loc[stage["msisdn"].isin(new_numbers) & (stage["matched"] == 1), "ordinal"]
        return list(positions), invalid, matched_rows
    finally:
        conn.unregister("chunk_seen_input")


def receive_file(campaign_id: int, file_path: str, original_filename: str,
                 ingestion_file_id: Optional[int] = None,
                 whitelist_version_id: Optional[int] = None,
                 worker_id: str = "", lease_token: str = "", stop_event=None) -> dict:
    conn = get_connection()
    campaign = conn.execute(
        "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
    ).fetchone()
    if not campaign:
        conn.close()
        return {"success": False, "message": "Campaign not found"}
    campaign = dict(campaign)
    # Do not retain a long-lived DuckDB snapshot while chunk state and tasks are
    # committed through their own short transactions.
    conn.close()
    realtime_ingestion_state.check_owned(ingestion_file_id, lease_token)
    # A previous attempt may have moved the file but failed its final DB commit.
    # Replay completed chunk checkpoints from received, not a missing inbox path.
    recovered_archive = False
    if not os.path.isfile(file_path):
        archived = os.path.join(campaign.get("campaign_dir", ""), "received", os.path.basename(original_filename))
        if os.path.isfile(archived):
            file_path = archived
            recovered_archive = True
    try:
        if lease_token:
            file_record_id, duplicate_file = realtime_ingestion_state.register_processed(
                campaign_id, file_path, original_filename, ingestion_file_id, lease_token,
            )
        else:
            file_record_id, duplicate_file = operations_service.register_file(
                "realtime", campaign_id, file_path, original_filename)
    except realtime_ingestion_state.LeaseLost:
        raise
    except Exception as exc:
        if realtime_ingestion_state.transient(exc):
            raise
        return {"success": False, "message": str(exc), "error_class": "INVALID_INPUT"}
    if duplicate_file:
        duplicate_path = "" if recovered_archive else operations_service.archive_duplicate(
            file_path, campaign.get("campaign_dir", ""), original_filename)
        return {"success": True, "duplicate_file": True, "duplicate_path": duplicate_path,
                "message": "file already processed"}
    chunk_size = _realtime_chunk_size()
    if ingestion_file_id is None:
        ingestion_file_id, _ = ingestion_job_service.register_file(
            "realtime", campaign_id, original_filename, file_path,
            chunk_size=chunk_size, reset=False, whitelist_version_id=whitelist_version_id,
        )
    if whitelist_version_id is None:
        whitelist_version_id = (realtime_whitelist_service.active_version(campaign_id) or {}).get("id")
    progress = realtime_ingestion_state.Progress(
        ingestion_file_id, file_record_id, lease_token, worker_id, whitelist_version_id,
    )
    def finish_processed(status, path, error):
        realtime_ingestion_state.finish_processed(ingestion_file_id, file_record_id, lease_token, status, path, error)
    progress("validating")
    active, inactive_reason = sms_task_service.campaign_is_active(campaign)
    if not active:
        # A worker can claim a file just as an operator closes the campaign.
        # Treat terminal campaign closure as expiration, not an input failure.
        finish_processed("failed", file_path, inactive_reason)
        terminal = inactive_reason in {"campaign is not enabled", "campaign has ended"}
        return {
            "success": False,
            "status": "expired" if terminal else "inactive",
            "error_class": "EXPIRED_FILE" if terminal else "INVALID_CAMPAIGN",
            "retryable": False,
            "message": inactive_reason,
        }
    whitelist_version = realtime_whitelist_service.get_version(whitelist_version_id) if whitelist_version_id else None
    latest_whitelist_version = realtime_whitelist_service.latest_version(campaign_id)
    whitelist_path = campaign.get("customer_list_path", "")
    if campaign.get("use_whitelist", True) and not whitelist_version and latest_whitelist_version \
            and latest_whitelist_version.get("status") == "building":
        progress("waiting_for_whitelist")
        return {"success": False, "status": "waiting_for_whitelist", "retryable": True,
                "message": "whitelist index is still building"}
    if campaign.get("use_whitelist", True) and whitelist_version:
        whitelist_status = whitelist_version.get("status")
        if whitelist_status == "failed":
            error_detail = whitelist_version.get("error_detail") or "whitelist index build failed"
            if _is_transient_whitelist_index_error(error_detail):
                recovered = realtime_whitelist_service.recover_transient_failure(whitelist_version_id)
                if recovered == "ready":
                    whitelist_version = realtime_whitelist_service.get_version(whitelist_version_id)
                    whitelist_status = (whitelist_version or {}).get("status")
                else:
                    # A transient conflict with no complete index is a build
                    # retry, not a terminal whitelist failure.  Keep the file
                    # pending until the normal whitelist builder completes.
                    progress("waiting_for_whitelist")
                    return {
                        "success": False,
                        "status": "waiting_for_whitelist",
                        "retryable": True,
                        "message": f"whitelist index temporarily unavailable: {error_detail}",
                    }
            if whitelist_status == "failed" and _is_repairable_whitelist_error(error_detail) and _schedule_active_whitelist_rebuild(
                campaign_id, whitelist_version_id, error_detail,
            ):
                progress("waiting_for_whitelist")
                return {
                    "success": False,
                    "status": "waiting_for_whitelist",
                    "retryable": True,
                    "message": f"active whitelist index repair scheduled: {error_detail}",
                }
            if whitelist_status == "failed":
                terminal_error = f"whitelist index build failed: {error_detail}"
                finish_processed("failed", file_path, terminal_error)
                return {
                    "success": False,
                    "status": "failed",
                    "error_class": "WHITELIST_INDEX",
                    "retryable": False,
                    "message": terminal_error,
                }
        if whitelist_status == "ready" and not os.path.isfile(whitelist_version.get("index_path", "")):
            if _schedule_active_whitelist_rebuild(
                campaign_id, whitelist_version_id, "active whitelist index file is missing",
            ):
                progress("waiting_for_whitelist")
                return {
                    "success": False,
                    "status": "waiting_for_whitelist",
                    "retryable": True,
                    "message": "active whitelist index repair scheduled: index file is missing",
                }
        if whitelist_status != "ready" or not os.path.isfile(whitelist_version.get("index_path", "")):
            progress("waiting_for_whitelist")
            return {"success": False, "status": "waiting_for_whitelist", "retryable": True,
                    "message": "whitelist index is still building"}
        whitelist_path = whitelist_version["index_path"]
    elif campaign.get("use_whitelist", True) and (not whitelist_path or not os.path.isfile(whitelist_path)):
        finish_processed("failed", file_path, "No whitelist uploaded")
        return {"success": False, "status": "waiting_for_whitelist", "retryable": True,
                "message": "No ready whitelist for this campaign"}

    ext = os.path.splitext(file_path)[1].lower()
    progress("reading_input")
    reader = None
    try:
        if ext in (".csv", ".txt"):
            reader = _read_csv(file_path, chunksize=chunk_size)
            msisdn_col = _resolve_msisdn_column(_read_csv_columns(file_path))
        else:
            df = pd.read_excel(file_path, dtype=str)
            msisdn_col = _resolve_msisdn_column(df.columns)
    except Exception as e:
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        realtime_ingestion_state.check_owned(ingestion_file_id, lease_token)
        target = operations_service.quarantine(file_path, campaign.get("campaign_dir", ""), original_filename)
        finish_processed("quarantined", target, e)
        operations_service.event("file", "READ_FAILED", str(e), context={"campaign_id": campaign_id, "file": original_filename})
        return {"success": False, "status": "quarantined", "error_class": "INVALID_INPUT",
                "quarantine_path": target, "message": f"Cannot read file: {e}"}

    # Load only the persistent versioned index. Legacy campaigns are migrated by
    # the realtime worker before they are allowed to process new files.
    whitelist_conn = None
    # Main database progress failures are not whitelist-index failures.
    progress("loading_index")
    try:
        if not campaign.get("use_whitelist", True):
            whitelist_languages = {}
            whitelist_set = set()
        elif whitelist_version:
            whitelist_conn = _open_whitelist_index(whitelist_path)
            whitelist_languages = None
            whitelist_set = None
        else:
            return {"success": False, "status": "waiting_for_whitelist", "retryable": True,
                    "message": "No ready whitelist index for this campaign"}
    except Exception as e:
        if whitelist_conn:
            whitelist_conn.close()
        if _is_transient_whitelist_index_error(e):
            progress("waiting_for_whitelist")
            return {
                "success": False,
                "status": "waiting_for_whitelist",
                "retryable": True,
                "message": f"whitelist index temporarily busy: {e}",
            }
        rebuild_scheduled = _schedule_active_whitelist_rebuild(campaign_id, whitelist_version_id, e)
        # The active whitelist is authoritative. Never mark it failed or
        # substitute an older version when its index cannot be read. Keep the
        # file pending so the index can be repaired/rebuilt and retried.
        progress("waiting_for_whitelist")
        if not rebuild_scheduled:
            operations_service.event(
                "realtime_ingestion", "WHITELIST_INDEX_UNAVAILABLE",
                f"Cannot read active whitelist index: {e}",
                severity="error",
                context={
                    "campaign_id": campaign_id,
                    "version_id": whitelist_version_id,
                    "index_path": whitelist_path,
                    "filename": original_filename,
                },
            )
        return {
            "success": False,
            "status": "waiting_for_whitelist",
            "error_class": "WHITELIST_INDEX",
            "retryable": True,
            "message": f"active whitelist index repair scheduled: {e}" if rebuild_scheduled
            else f"Cannot read active whitelist index: {e}",
        }

    # Match input MSISDNs against whitelist. CSV chunks are fully matched,
    # enqueued and written before the next chunk is read. A disk-backed unique
    # table preserves cross-chunk de-duplication without retaining the file in
    # Python memory.
    if ext in (".csv", ".txt"):
        total_rows = 0
        matched_row_count = 0
        task_result = {"success": True, "created": 0, "duplicate": 0,
                       "excluded": 0, "invalid": 0, "no_template": 0}
        unique_fd, unique_path = tempfile.mkstemp(prefix="mc-realtime-", suffix=".duckdb")
        os.close(unique_fd)
        os.unlink(unique_path)
        unique_conn = duckdb.connect(unique_path)
        current_job = None
        try:
            unique_conn.execute("CREATE TABLE seen_msisdns (msisdn TEXT PRIMARY KEY, matched INTEGER NOT NULL)")
            for chunk_index, chunk in enumerate(reader):
                expired_reason = _realtime_ingestion_expired_reason(campaign, filename=original_filename)
                if expired_reason:
                    return {"success": False, "status": "expired", "message": expired_reason}
                if not backpressure_service.wait_for_ingestion(
                    stop_event=stop_event, producing_chunks=True,
                ):
                    return {
                        "success": False,
                        "status": "stopped",
                        "message": "realtime ingestion stopped while waiting for capacity",
                    }
                chunk_start = total_rows
                total_rows += len(chunk)
                progress("matching", total_rows, matched_row_count, chunk_index, chunk_index + 1)
                current_job = realtime_ingestion_state.prepare_chunk(
                    ingestion_file_id, chunk_index, chunk_start, total_rows, lease_token,
                )
                if current_job is None:
                    raise RuntimeError(f"realtime chunk {chunk_index} lease is held by another worker")
                chunk_completed = current_job.get("status") == "completed"
                chunk_normalized = chunk[msisdn_col].map(sms_task_service.normalize_msisdn)
                chunk_whitelist_languages = (
                    _lookup_whitelist_languages(whitelist_conn, chunk_normalized)
                    if whitelist_conn is not None else whitelist_languages
                )
                chunk_result = {}
                positions, invalid, chunk_matched_rows = _batch_seen_msisdns(
                    unique_conn, chunk_normalized, chunk_whitelist_languages,
                    campaign.get("use_whitelist", True),
                )
                task_result["invalid"] += invalid
                selected = chunk.iloc[positions].fillna("").copy()
                selected["msisdn"] = [chunk_normalized.iloc[idx] for idx in positions]
                matched_records = selected.to_dict("records")
                if campaign.get("use_whitelist", True):
                    for row_dict in matched_records:
                        row_dict["language"] = chunk_whitelist_languages[row_dict["msisdn"]]
                matched_row_count += chunk_matched_rows
                if matched_records and not chunk_completed:
                    expired_reason = _realtime_ingestion_expired_reason(campaign, filename=original_filename)
                    if expired_reason:
                        return {"success": False, "status": "expired", "message": expired_reason}
                    realtime_ingestion_state.check_owned(ingestion_file_id, lease_token)
                    chunk_result = sms_task_service.enqueue_rows(
                        "realtime", campaign, matched_records, original_filename,
                        ingestion_lease=(ingestion_file_id, lease_token), ingestion_chunk=current_job,
                    )
                    for key in ("created", "duplicate", "excluded", "invalid", "no_template"):
                        task_result[key] += chunk_result.get(key, 0)
                    if not chunk_result.get("success"):
                        raise RuntimeError(chunk_result.get("message", "Unable to enqueue realtime chunk"))
                    created_msisdns = set(chunk_result.get("created_msisdns", []))
                    _append_realtime_output(campaign.get("campaign_dir", ""), [
                        {"msisdn": row["msisdn"], "send_time": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")}
                        for row in matched_records
                        if _task_msisdn_for_campaign(row["msisdn"], campaign) in created_msisdns
                    ])
                    del created_msisdns
                elif chunk_completed:
                    for key, column in (
                        ("created", "created_tasks"), ("duplicate", "duplicate_rows"),
                        ("excluded", "excluded_rows"), ("no_template", "no_template_rows"),
                    ):
                        task_result[key] += int(current_job.get(column, 0) or 0)
                if not chunk_completed:
                    if not chunk_result.get("chunk_committed"):
                        realtime_ingestion_state.complete_chunk(
                            ingestion_file_id, current_job, chunk_result if matched_records else {}, lease_token,
                        )
                    progress("enqueueing_tasks", total_rows, matched_row_count, chunk_index, chunk_index + 1)
                current_job = None
                del matched_records, chunk_normalized, chunk_whitelist_languages, chunk
            counts = unique_conn.execute(
                "SELECT COUNT(*) AS total, SUM(matched) AS matched FROM seen_msisdns"
            ).fetchone()
            total_msisdn = int(counts[0] or 0)
            matched_count = int(counts[1] or 0)
        except Exception as exc:
            # Release the current chunk immediately so a direct retry does not
            # wait for its lease to expire. Keep file ownership intact; the
            # worker's outer retry transition remains authoritative.
            if current_job and current_job.get("status") != "completed":
                try:
                    realtime_ingestion_state.fail_chunk(
                        ingestion_file_id, current_job, exc, lease_token,
                    )
                except Exception:
                    logger.warning(
                        "Unable to release failed realtime chunk file_id=%s chunk_id=%s",
                        ingestion_file_id, current_job.get("id"), exc_info=True,
                    )
            raise
        finally:
            unique_conn.close()
            if reader is not None:
                try:
                    reader.close()
                except Exception:
                    pass
            if os.path.exists(unique_path):
                try:
                    os.unlink(unique_path)
                except OSError:
                    pass
            if whitelist_conn:
                whitelist_conn.close()
    else:
        if not backpressure_service.wait_for_ingestion(
            stop_event=stop_event, producing_chunks=True,
        ):
            return {
                "success": False,
                "status": "stopped",
                "message": "realtime ingestion stopped while waiting for capacity",
            }
        excel_job = realtime_ingestion_state.prepare_chunk(
            ingestion_file_id, 0, 0, len(df), lease_token,
        )
        if excel_job is None:
            raise RuntimeError("realtime Excel chunk lease is held by another worker")
        try:
            progress("matching", 0, 0, 0, 1)
            normalized_msisdns = df[msisdn_col].map(sms_task_service.normalize_msisdn)
            received_msisdns = {value for value in normalized_msisdns if value}
            total_msisdn = len(received_msisdns)
            total_rows = len(df)
            if whitelist_conn is not None:
                chunk_whitelist_languages = _lookup_whitelist_languages(
                    whitelist_conn, normalized_msisdns
                )
                matched = received_msisdns & set(chunk_whitelist_languages)
            else:
                chunk_whitelist_languages = whitelist_languages
                matched = (
                    received_msisdns
                    if not campaign.get("use_whitelist", True)
                    else received_msisdns & whitelist_set
                )
            matched_count = len(matched)
            matched_rows = df[normalized_msisdns.isin(matched)].copy()
            matched_rows["msisdn"] = normalized_msisdns[normalized_msisdns.isin(matched)]
            matched_records = matched_rows.fillna("").to_dict("records")
            for record in matched_records:
                msisdn = record.get("msisdn")
                record["language"] = (
                    record.get("language", "")
                    if not campaign.get("use_whitelist", True)
                    else chunk_whitelist_languages[msisdn]
                )
            if excel_job.get("status") == "completed":
                task_result = {
                    "success": True,
                    "created": int(excel_job.get("created_tasks", 0) or 0),
                    "duplicate": int(excel_job.get("duplicate_rows", 0) or 0),
                    "excluded": int(excel_job.get("excluded_rows", 0) or 0),
                    "invalid": int(excel_job.get("invalid_rows", 0) or 0),
                    "no_template": int(excel_job.get("no_template_rows", 0) or 0),
                    "created_msisdns": [],
                }
            else:
                expired_reason = _realtime_ingestion_expired_reason(campaign, filename=original_filename)
                if expired_reason:
                    return {"success": False, "status": "expired", "message": expired_reason}
                task_result = sms_task_service.enqueue_rows(
                    "realtime", campaign, matched_records, original_filename,
                    ingestion_lease=(ingestion_file_id, lease_token), ingestion_chunk=excel_job,
                )
            created_msisdns = set(task_result.get("created_msisdns", []))
            _append_realtime_output(campaign.get("campaign_dir", ""), [
                {"msisdn": record["msisdn"], "send_time": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")}
                for record in matched_records
                if _task_msisdn_for_campaign(record["msisdn"], campaign) in created_msisdns
            ])
            del created_msisdns
            if excel_job.get("status") != "completed" and not task_result.get("chunk_committed"):
                realtime_ingestion_state.complete_chunk(ingestion_file_id, excel_job, task_result, lease_token)
        except Exception as exc:
            if excel_job.get("status") != "completed":
                try:
                    realtime_ingestion_state.fail_chunk(
                        ingestion_file_id, excel_job, exc, lease_token,
                    )
                except Exception:
                    logger.warning(
                        "Unable to release failed realtime Excel chunk file_id=%s chunk_id=%s",
                        ingestion_file_id, excel_job.get("id"), exc_info=True,
                    )
            raise
        finally:
            if whitelist_conn:
                whitelist_conn.close()

    expired_reason = _realtime_ingestion_expired_reason(campaign, filename=original_filename)
    if expired_reason:
        return {"success": False, "status": "expired", "message": expired_reason}

    campaign_dir = campaign.get("campaign_dir", "")
    progress("finalizing", total_rows, matched_count, 0, 0, force=True)
    received_dir = os.path.join(campaign_dir, "received")
    os.makedirs(received_dir, exist_ok=True)
    dest = os.path.join(received_dir, original_filename)
    realtime_ingestion_state.check_owned(ingestion_file_id, lease_token)
    if os.path.abspath(file_path) != os.path.abspath(dest):
        shutil.move(file_path, dest)
    try:
        os.utime(dest, None)
    except OSError:
        pass

    realtime_ingestion_state.transition(
        ingestion_file_id, file_record_id, lease_token, "completed", path=dest,
        rows=total_rows, created=task_result.get("created", 0), campaign_id=campaign_id,
        filename=original_filename, total_msisdn=total_msisdn, matched=matched_count,
    )
    logger.info(
        "Realtime file processed campaign=%s file=%s rows=%s unique_msisdn=%s matched=%s tasks_created=%s dest=%s",
        campaign_id, original_filename, total_rows, total_msisdn, matched_count, task_result.get("created", 0), dest,
    )
    return {
        "success": True,
        "filename": original_filename,
        "total_msisdn": total_msisdn,
        "matched_count": matched_count,
        "task_result": task_result,
        "file_record_id": file_record_id,
    }


def get_realtime_files(campaign_id: int, page: int = 1, page_size: int = 50) -> dict:
    conn = get_connection()
    total = conn.execute(
        "SELECT COUNT(*) cnt FROM realtime_campaign_files WHERE campaign_id=?",
        (campaign_id,),
    ).fetchone()["cnt"]
    rows = conn.execute(
        """SELECT * FROM realtime_campaign_files
           WHERE campaign_id=?
           ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        (campaign_id, page_size, max(0, page - 1) * page_size),
    ).fetchall()
    filenames = [str(row["filename"] or "") for row in rows if row["filename"]]
    task_counts = {}
    if filenames:
        placeholders = ",".join("?" for _ in filenames)
        task_rows = conn.execute(
            f"""SELECT source_file, status, COUNT(*) AS count
                FROM sms_tasks
                WHERE campaign_type='realtime' AND campaign_id=?
                  AND source_file IN ({placeholders})
                GROUP BY source_file, status""",
            [campaign_id, *filenames],
        ).fetchall()
        for task_row in task_rows:
            task_counts.setdefault(str(task_row["source_file"] or ""), {})[
                str(task_row["status"] or "")
            ] = int(task_row["count"] or 0)
    conn.close()
    items = []
    for row in rows:
        item = dict(row)
        raw = task_counts.get(str(item.get("filename") or ""), {})
        counts = {key: int(raw.get(key, 0)) for key in _REALTIME_TASK_STATUS_KEYS}
        item.update({
            "task_count": sum(raw.values()),
            "accepted_count": counts["accepted"],
            "delivered_count": counts["delivered"],
            "queued_count": counts["queued"],
            "sending_count": counts["sending"],
            "submitted_count": counts["submitted"],
            "retrying_count": counts["retrying"],
            "failed_count": counts["failed"],
            "expired_count": counts["expired"],
            "cancelled_count": counts["cancelled"],
        })
        items.append(item)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
    }


def get_realtime_watch_files(
    campaign_id: int,
    page: int = 1,
    page_size: int = 50,
    extension: str = "csv",
    status: str = "",
    reason: str = "",
) -> dict:
    """List watch-directory files without parsing the entire directory first."""
    campaign = get_one(campaign_id)
    if not campaign:
        return {"total": 0, "page": page, "page_size": page_size, "items": []}

    extension_filter = str(extension or "").strip().lower().lstrip(".")
    status_filter = str(status or "").strip().lower()
    reason_filter = str(reason or "").strip().lower()
    supported_extensions = {".csv", ".txt", ".xlsx", ".xls"}

    def matches_filters(item: dict) -> bool:
        if extension_filter and str(item.get("_ext") or "").lower().lstrip(".") != extension_filter:
            return False
        if status_filter and str(item.get("status") or "").lower() != status_filter:
            return False
        if reason_filter and reason_filter not in str(item.get("skip_reason") or "").lower():
            return False
        return True

    processed_records = _processed_file_records(campaign_id)

    if campaign.get("watch_source", "local") == "sftp":
        from services import sftp_watch_service
        files = []
        client = sftp = None
        try:
            client, sftp, entries = sftp_watch_service.list_directory(campaign)
            prefix = campaign.get("watch_filename_prefix") or ""
            remote_label = f"sftp://{campaign.get('sftp_host')}:{campaign.get('sftp_port')}{campaign.get('watch_dir')}"
            for entry in sorted(entries, key=lambda item: item.filename):
                name = entry.filename
                ext = os.path.splitext(name)[1].lower()
                if (
                    not stat.S_ISREG(entry.st_mode)
                    or ext not in supported_extensions
                    or (extension_filter and ext.lstrip(".") != extension_filter)
                ):
                    continue
                match_prefix = bool(prefix and name.startswith(prefix))
                match_schedule = _watch_file_matches_campaign(name, campaign)
                processed_record = processed_records.get(name)
                age = datetime.now().timestamp() - (entry.st_mtime or 0)
                if processed_record:
                    status, label, reason = "processed", "Processed", f"Already processed: {name}"
                elif not match_prefix:
                    status, label, reason = "ignored", "Ignored", "Filename prefix does not match"
                elif not match_schedule:
                    status, label, reason = "ignored", "Ignored", "Filename timestamp does not match campaign schedule"
                elif age < 5:
                    status, label, reason = "pending", "Writing", "Waiting for remote file to become stable"
                else:
                    status, label, reason = "pending", "Pending", ""
                item = {
                    "filename": name, "directory_path": remote_label,
                    "file_size": entry.st_size or 0, "row_count": 0,
                    "_ext": ext,
                    "match_prefix": match_prefix, "match_schedule": match_schedule,
                    "status": status, "status_label": label, "skip_reason": reason,
                    "created_at": "", "updated_at": "",
                }
                if matches_filters(item):
                    item.pop("_ext", None)
                    files.append(item)
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()
        total = len(files)
        offset = max(0, page - 1) * page_size
        return {"total": total, "page": page, "page_size": page_size,
                "items": files[offset:offset + page_size]}

    watch_dir = os.path.abspath(campaign.get("watch_dir") or "")
    if not os.path.isdir(watch_dir):
        return {"total": 0, "page": page, "page_size": page_size, "items": []}

    prefix = campaign.get("watch_filename_prefix") or ""
    files = []
    directory_entries = []
    try:
        with os.scandir(watch_dir) as entries:
            for entry in entries:
                try:
                    entry_stat = entry.stat()
                except OSError:
                    continue
                if not stat.S_ISREG(entry_stat.st_mode):
                    continue
                name = entry.name
                ext = os.path.splitext(name)[1].lower()
                if ext not in (".csv", ".txt", ".xlsx", ".xls"):
                    continue
                if extension_filter and ext.lstrip(".") != extension_filter:
                    continue
                directory_entries.append((name, entry.path, ext, entry_stat.st_size, entry_stat.st_mtime))
    except OSError:
        return {"total": 0, "page": page, "page_size": page_size, "items": []}

    for name, path, ext, file_size, file_mtime in sorted(directory_entries, key=lambda item: item[0]):

        match_prefix = bool(prefix and name.startswith(prefix))
        match_schedule = _watch_file_matches_campaign(name, campaign)
        processed_record = processed_records.get(name)
        if processed_record:
            status = "processed"
            status_label = "已处理过"
            skip_reason = f"同名文件已处理: {processed_record.get('filename')}"
        elif not match_prefix:
            status = "ignored"
            status_label = "前缀不匹配"
            skip_reason = "文件名不符合当前活动前缀"
        elif not match_schedule:
            status = "ignored"
            status_label = "发送规则不匹配"
            skip_reason = "文件名时间不在当前活动发送日期或时间段内"
        else:
            writing = datetime.now().timestamp() - file_mtime < 2
            status = "pending"
            status_label = "写入中" if writing else "待处理"
            skip_reason = "文件仍在写入稳定等待期内" if writing else ""

        item = {
            "filename": name,
            "directory_path": watch_dir,
            "file_size": file_size,
            "row_count": 0,
            "_path": path,
            "_ext": ext,
            "_processed_total_rows": (
                int(processed_record.get("total_rows") or 0) if processed_record else None
            ),
            "match_prefix": match_prefix,
            "match_schedule": match_schedule,
            "status": status,
            "status_label": status_label,
            "skip_reason": skip_reason,
            "created_at": "",
            "updated_at": "",
        }
        if matches_filters(item):
            files.append(item)

    total = len(files)
    offset = max(0, page - 1) * page_size
    page_items = files[offset:offset + page_size]
    # Row counts are useful in the UI, but parsing every historical file made
    # this endpoint unbounded. Calculate them only for the visible page and
    # stream CSV rows instead of loading each file into a DataFrame.
    for item in page_items:
        if item["_ext"] != ".csv" or item["status"] == "ignored":
            if item["_processed_total_rows"] is not None:
                item["row_count"] = item["_processed_total_rows"]
            item.pop("_path", None)
            item.pop("_ext", None)
            item.pop("_processed_total_rows", None)
            continue
        if item["_processed_total_rows"] is not None:
            item["row_count"] = item["_processed_total_rows"]
            item.pop("_path", None)
            item.pop("_ext", None)
            item.pop("_processed_total_rows", None)
            continue
        try:
            with open(item["_path"], "r", encoding="utf-8-sig", newline="") as handle:
                item["row_count"] = max(0, sum(1 for _ in csv.reader(handle)) - 1)
        except (OSError, UnicodeError, csv.Error):
            item["row_count"] = 0
        item.pop("_path", None)
        item.pop("_ext", None)
        item.pop("_processed_total_rows", None)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": page_items,
    }


def get_realtime_sent_records(campaign_id: int, page: int = 1, page_size: int = 50) -> dict:
    campaign = get_one(campaign_id)
    if not campaign:
        return {"total": 0, "page": page, "page_size": page_size, "items": []}
    output_dir = os.path.join(campaign.get("campaign_dir", ""), "output")
    output_dates = set()
    if os.path.isdir(output_dir):
        output_dates = {
            os.path.splitext(fname)[0]
            for fname in os.listdir(output_dir)
            if re.match(r"^\d{8}\.csv$", fname)
        }

    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT CAST(created_at AS DATE) AS report_date, status, COUNT(*) AS count
               FROM sms_tasks
               WHERE campaign_type='realtime' AND campaign_id=?
               GROUP BY CAST(created_at AS DATE), status""",
            (campaign_id,),
        ).fetchall()
    finally:
        conn.close()

    by_date = {}
    for row in rows:
        date = str(row["report_date"])[:10]
        date_key = date.replace("-", "")
        by_date.setdefault(date_key, {})[str(row["status"] or "")] = int(row["count"] or 0)

    records = []
    for date_key in sorted(output_dates | set(by_date), reverse=True):
        raw = by_date.get(date_key, {})
        counts = {key: int(raw.get(key, 0)) for key in _REALTIME_TASK_STATUS_KEYS}
        task_count = sum(raw.values())
        delivered_count = counts["delivered"]
        records.append({
            "date": f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}" if len(date_key) == 8 else date_key,
            "filename": f"{date_key}.csv",
            # Backward-compatible field: it now has one strict meaning.
            "user_count": delivered_count,
            "task_count": task_count,
            "accepted_count": counts["accepted"],
            "delivered_count": delivered_count,
            "queued_count": counts["queued"],
            "sending_count": counts["sending"],
            "submitted_count": counts["submitted"],
            "retrying_count": counts["retrying"],
            "failed_count": counts["failed"],
            "expired_count": counts["expired"],
            "cancelled_count": counts["cancelled"],
        })
    total = len(records)
    offset = max(0, page - 1) * page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": records[offset:offset + page_size],
    }


def get_realtime_stats(campaign_id: int) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    campaign = conn.execute(
        "SELECT campaign_dir FROM realtime_campaigns WHERE id=?",
        (campaign_id,),
    ).fetchone()
    row = conn.execute(
        "SELECT COUNT(*) AS file_count, COALESCE(SUM(total_msisdn),0) AS total_msisdn_sum, COALESCE(SUM(matched_count),0) AS total_matched FROM realtime_campaign_files WHERE campaign_id=?",
        (campaign_id,)
    ).fetchone()
    today_row = conn.execute(
        """SELECT COUNT(*) AS file_count, COALESCE(SUM(total_msisdn),0) AS total_msisdn_sum,
                  COALESCE(SUM(matched_count),0) AS total_matched
           FROM realtime_campaign_files
           WHERE campaign_id=? AND CAST(created_at AS DATE)=CAST(? AS DATE)""",
        (campaign_id, today),
    ).fetchone()
    task_counts = _realtime_task_status_counts(conn, campaign_id)
    today_task_counts = _realtime_task_status_counts(conn, campaign_id, today)
    conn.close()
    result = dict(row) if row else {"file_count": 0, "total_msisdn_sum": 0, "total_matched": 0}
    today_result = dict(today_row) if today_row else {
        "file_count": 0, "total_msisdn_sum": 0, "total_matched": 0,
    }
    result["today_file_count"] = int(today_result.get("file_count") or 0)
    result["today_msisdn_sum"] = int(today_result.get("total_msisdn_sum") or 0)
    result["today_matched"] = int(today_result.get("total_matched") or 0)
    for key, value in task_counts.items():
        result[f"{key}_users"] = value
    for key, value in today_task_counts.items():
        result[f"today_{key}_users"] = value
    # Keep these aliases stable for existing clients while making their
    # meaning explicit: only confirmed delivery receipts count as reached.
    result["today_reached_users"] = today_task_counts["delivered"]
    result["today_users"] = today_task_counts["delivered"]
    result["reached_users"] = task_counts["delivered"]
    return result


def _ensure_realtime_dirs(campaign_dir: str):
    for dirname in ("whitelist", "inbox", "received", "output"):
        os.makedirs(os.path.join(campaign_dir, dirname), exist_ok=True)


def _watch_file_timestamp(filename: str, prefix: str = "") -> Optional[datetime]:
    prefix = prefix or "Android_4G_Switch_Somali_"
    if not filename.startswith(prefix):
        return None
    token = filename[len(prefix):len(prefix) + 14]
    if len(token) != 14 or not token.isdigit():
        return None
    try:
        return datetime.strptime(token, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _watch_file_matches_campaign(filename: str, campaign: dict, now: Optional[datetime] = None,
                                 file_time: Optional[datetime] = None) -> bool:
    prefix = campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_"
    file_time = file_time or _watch_file_timestamp(filename, prefix)
    if not file_time:
        return False
    now = now or datetime.now()
    if file_time.date() != now.date():
        return False

    days = campaign.get("send_days") or []
    if isinstance(days, str):
        try:
            days = json.loads(days or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    if days:
        # API/UI use JavaScript's convention: Sunday=0 ... Saturday=6.
        api_weekday = (file_time.weekday() + 1) % 7
        if api_weekday not in {int(v) for v in days}:
            return False

    current = file_time.strftime("%H:%M")
    window_start = (campaign.get("send_start_time") or "")[:5]
    window_end = (campaign.get("send_end_time") or "")[:5]
    if window_start and window_end:
        in_window = (window_start <= current <= window_end) if window_start <= window_end else (
            current >= window_start or current <= window_end
        )
        if not in_window:
            return False
    elif window_start and current < window_start:
        return False
    elif window_end and current > window_end:
        return False
    return True


def _processed_file_record(campaign_id: int, filename: str) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM processed_files WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
        (campaign_id, filename),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _processed_file_records(campaign_id: int) -> dict[str, dict]:
    """Load processed-file metadata once for a watch-directory listing."""
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT filename, status, total_rows, file_size, file_path
               FROM processed_files
               WHERE campaign_type='realtime' AND campaign_id=?""",
            (campaign_id,),
        ).fetchall(),
        operation="realtime_campaign.processed_file_index",
    )
    return {
        str(row["filename"]): dict(row)
        for row in rows
        if row["filename"]
    }


def _completed_processed_filenames(campaign_id: int) -> set[str]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT filename FROM processed_files
               WHERE campaign_type='realtime' AND campaign_id=? AND status='completed'""",
            (campaign_id,),
        ).fetchall()
    finally:
        conn.close()
    return {str(row["filename"]) for row in rows if row["filename"]}


def _log_watch_skip_summary(campaign_id: int, skipped: int, reason: str = "already processed") -> None:
    key = (int(campaign_id), reason)
    now = time.monotonic()
    with _watch_skip_log_lock:
        last, suppressed = _watch_skip_log_state.get(key, (0.0, 0))
        if now - last < 30.0:
            _watch_skip_log_state[key] = (last, suppressed + int(skipped or 0))
            return
        _watch_skip_log_state[key] = (now, 0)
    logger.info(
        "Watch files skipped campaign=%s count=%s suppressed=%s reason=%s",
        campaign_id, int(skipped or 0), suppressed, reason,
    )


def _watch_scan_due(campaign: dict, watch_dir: str, now: datetime, force: bool = False) -> bool:
    """Avoid rescanning an unchanged local watch directory every scheduler tick.

    The source system creates new files in the directory, so the directory
    mtime is a cheap change hint.  The date/minute buckets ensure that a file
    already present before the campaign window becomes eligible when the
    filename timestamp reaches the current window.  A bounded rescan remains
    as a safety net for filesystems with unreliable directory mtimes.
    """
    try:
        directory_mtime_ns = os.stat(watch_dir).st_mtime_ns
    except OSError:
        return True
    key = (int(campaign.get("id") or 0), os.path.abspath(watch_dir))
    now_mono = time.monotonic()
    pending_due = bool(_pending_watch_names(int(campaign.get("id") or 0), watch_dir))
    state = {
        "directory_mtime_ns": directory_mtime_ns,
        "date": now.date(),
        "minute": now.replace(second=0, microsecond=0),
        "last_scan": now_mono,
    }
    with _watch_scan_lock:
        previous = _watch_scan_state.get(key)
        changed = (
            not previous
            or previous["directory_mtime_ns"] != directory_mtime_ns
            or previous["date"] != state["date"]
            or previous["minute"] != state["minute"]
        )
        if not force and pending_due and not changed:
            state["last_scan"] = now_mono
            _watch_scan_state[key] = state
            campaign["_watch_scan_mode"] = "pending"
            metrics_service.inc(
                "realtime_watch_pending_rescans_total",
                labels={"campaign_id": campaign.get("id", 0)},
            )
            metrics_service.set_gauge(
                "realtime_watch_last_pending_rescan_timestamp", time.time(),
                labels={"campaign_id": campaign.get("id", 0)},
            )
            return True
        if not force and not changed and now_mono - previous["last_scan"] < WATCH_RESCAN_SECONDS:
            return False
        _watch_scan_state[key] = state
        campaign["_watch_scan_mode"] = "full"
        metrics_service.inc(
            "realtime_watch_full_scans_total",
            labels={"campaign_id": campaign.get("id", 0)},
        )
        metrics_service.set_gauge(
            "realtime_watch_last_full_scan_timestamp", time.time(),
            labels={"campaign_id": campaign.get("id", 0)},
        )
        return True


def _daily_window_ended(campaign: dict, now: Optional[datetime] = None) -> bool:
    """Return whether the current local calendar day has passed its send window."""
    now = now or datetime.now()
    current = now.strftime("%H:%M")
    start = (campaign.get("send_start_time") or "")[:5]
    end = (campaign.get("send_end_time") or "")[:5]
    if not end:
        return False
    if start and start > end:
        return current > end and current < start
    return current > end


def _realtime_ingestion_expired_reason(campaign: dict, now: Optional[datetime] = None,
                                      filename: str = "") -> str:
    now = now or datetime.now()
    file_time = _watch_file_timestamp(
        filename, campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_",
    ) if filename else None
    if file_time and file_time.date() != now.date():
        return "file timestamp is outside the current day"
    active, reason = sms_task_service.campaign_is_active(campaign, now)
    if not active and (
        reason in {"campaign is not enabled", "campaign has ended"}
        or _daily_window_ended(campaign, now)
    ):
        if reason in {"campaign is not enabled", "campaign has ended"}:
            return reason
        return "daily send window ended"
    return ""


def finalize_daily_tasks(now: Optional[datetime] = None) -> int:
    """Close realtime work at a daily boundary and prevent cross-day carryover."""
    global _daily_finalize_date, _daily_finalized_windows, _daily_started_windows
    now = now or datetime.now()
    today = now.date().isoformat()
    with _daily_finalize_lock:
        new_day = _daily_finalize_date != today
        finalized_windows = set() if new_day else _daily_finalized_windows
        started_windows = set() if new_day else _daily_started_windows
        conn = get_connection()
        try:
            campaigns = conn.execute(
                "SELECT id,status,send_start_time,send_end_time,send_days FROM realtime_campaigns"
            ).fetchall()
            due_campaigns = []
            started_campaigns = []
            for row in campaigns:
                campaign = dict(row)
                key = (
                    int(campaign["id"]), today,
                    (campaign.get("send_start_time") or "")[:5],
                    (campaign.get("send_end_time") or "")[:5],
                )
                if campaign.get("status") == "in_progress" and key not in started_windows:
                    active, _ = sms_task_service.campaign_is_active(campaign, now)
                    if active:
                        started_windows.add(key)
                        started_campaigns.append(campaign)
                if _daily_window_ended(campaign, now) and key not in finalized_windows:
                    due_campaigns.append((campaign, key))
            # This is the hard cross-day guard. It also repairs tasks missed by
            # the scheduler at midnight; task_date is independently enforced by
            # the worker. Run it once per local calendar day.
            if not new_day and not due_campaigns and not started_campaigns:
                return 0
            cancelled = 0
            stale_cancelled = 0
            campaign_cancelled = {}
            if new_day:
                stale_count = conn.execute(
                    """SELECT COUNT(*) AS count FROM sms_tasks
                       WHERE campaign_type='realtime' AND task_date < CAST(? AS DATE)
                         AND (status IN ('queued','retrying') OR
                              (status='sending' AND NOT EXISTS (
                                  SELECT 1 FROM sms_send_attempts a
                                  WHERE a.task_id=sms_tasks.id
                                    AND a.claim_token=sms_tasks.claim_token
                                    AND a.state IN ('intent','authorized','responding','partial')
                              )))""",
                    (today,),
                ).fetchone()["count"]
                if int(stale_count or 0):
                    conn.execute(
                        """UPDATE sms_tasks SET status='cancelled', error_msg='previous day task cancelled',
                           claim_token='',updated_at=CURRENT_TIMESTAMP
                           WHERE campaign_type='realtime' AND task_date < CAST(? AS DATE)
                             AND (status IN ('queued','retrying') OR
                                  (status='sending' AND NOT EXISTS (
                                      SELECT 1 FROM sms_send_attempts a
                                      WHERE a.task_id=sms_tasks.id
                                        AND a.claim_token=sms_tasks.claim_token
                                        AND a.state IN ('intent','authorized','responding','partial')
                                  )))""",
                        (today,),
                    )
                    stale_cancelled = int(stale_count)
                    cancelled += stale_cancelled
            for campaign, key in due_campaigns:
                status_rows = conn.execute(
                    """SELECT status,COUNT(*) AS count FROM sms_tasks
                       WHERE campaign_type='realtime' AND campaign_id=?
                         AND task_date=CAST(? AS DATE)
                         AND (status IN ('queued','retrying') OR
                              (status='sending' AND NOT EXISTS (
                                  SELECT 1 FROM sms_send_attempts a
                                  WHERE a.task_id=sms_tasks.id
                                    AND a.claim_token=sms_tasks.claim_token
                                    AND a.state IN ('intent','authorized','responding','partial')
                              )))
                       GROUP BY status""",
                    (campaign["id"], today),
                ).fetchall()
                status_counts = {str(row["status"]): int(row["count"] or 0) for row in status_rows}
                daily_count = sum(status_counts.values())
                if int(daily_count or 0):
                    conn.execute(
                        """UPDATE sms_tasks SET status='cancelled', error_msg='daily send window ended',
                           claim_token='',updated_at=CURRENT_TIMESTAMP
                           WHERE campaign_type='realtime' AND campaign_id=?
                             AND task_date=CAST(? AS DATE)
                             AND (status IN ('queued','retrying') OR
                                  (status='sending' AND NOT EXISTS (
                                      SELECT 1 FROM sms_send_attempts a
                                      WHERE a.task_id=sms_tasks.id
                                        AND a.claim_token=sms_tasks.claim_token
                                        AND a.state IN ('intent','authorized','responding','partial')
                                  )))""",
                        (campaign["id"], today),
                    )
                    cancelled += int(daily_count)
                campaign_cancelled[int(campaign["id"])] = {
                    "queued": status_counts.get("queued", 0),
                    "retrying": status_counts.get("retrying", 0),
                    "sending": status_counts.get("sending", 0),
                    "cancelled": int(daily_count or 0),
                }
                finalized_windows.add(key)
            if cancelled:
                conn.commit()
            _daily_finalize_date = today
            _daily_finalized_windows = finalized_windows
            _daily_started_windows = started_windows
        finally:
            conn.close()
        for campaign in started_campaigns:
            logger.info(
                "Realtime daily window started campaign=%s date=%s send_start=%s send_end=%s",
                campaign["id"], today,
                (campaign.get("send_start_time") or "")[:5],
                (campaign.get("send_end_time") or "")[:5],
            )
        # Expire queued ingestion records and remove their input files once at
        # the boundary.  The scheduler skips closed windows after this point.
        discarded = 0
        if due_campaigns:
            discarded = cleanup_closed_window_ingestion_files(now)
        for campaign, _ in due_campaigns:
            counts = campaign_cancelled.get(int(campaign["id"]), {})
            logger.info(
                "Realtime daily window finalized campaign=%s date=%s "
                "queued=%s retrying=%s sending=%s cancelled_tasks=%s "
                "discarded_files_total=%s intake_scanning=false sending_stopped=true",
                campaign["id"], today,
                counts.get("queued", 0), counts.get("retrying", 0),
                counts.get("sending", 0), counts.get("cancelled", 0),
                discarded,
            )
        if stale_cancelled:
            logger.info(
                "Realtime previous-day tasks cancelled date=%s cancelled_tasks=%s",
                today, stale_cancelled,
            )
    if cancelled and not due_campaigns:
        logger.info("Realtime daily tasks cancelled=%s", cancelled)
    return cancelled


def has_active_realtime_campaigns(now: Optional[datetime] = None) -> bool:
    """Return whether the scheduler needs to scan realtime watch/inbox paths."""
    now = now or datetime.now()
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE status='in_progress'"
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        campaign = dict(row)
        if _daily_window_ended(campaign, now):
            continue
        active, _ = sms_task_service.campaign_is_active(campaign, now)
        if active:
            return True
    return False


def cleanup_closed_window_ingestion_files(now: Optional[datetime] = None,
                                         include_processing: bool = False) -> int:
    """Discard unstarted realtime file jobs after that day's send window closes."""
    now = now or datetime.now()
    conn = get_connection()
    try:
        campaigns = conn.execute(
            """SELECT id,send_start_time,send_end_time,campaign_dir
               FROM realtime_campaigns WHERE campaign_dir<>''"""
        ).fetchall()
    finally:
        conn.close()

    discarded = 0
    for row in campaigns:
        campaign = dict(row)
        if not _daily_window_ended(campaign, now):
            continue
        conn = get_connection()
        try:
            active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
            jobs = [dict(item) for item in conn.execute(
                """SELECT id,campaign_id,filename,file_path FROM ingestion_files
                   WHERE campaign_type='realtime' AND campaign_id=?
                     AND status IN """ + active_statuses,
                (campaign["id"],),
            ).fetchall()]
        finally:
            conn.close()

        reason = "daily send window ended"
        expired_jobs = []
        if jobs:
            expired_ids = ingestion_job_service.expire_files(
                (job["id"] for job in jobs), reason,
                include_processing=include_processing,
            )
            expired_jobs = [job for job in jobs if job["id"] in expired_ids]
            if expired_jobs:
                conn = get_connection()
                try:
                    conn.executemany(
                        """UPDATE processed_files SET status='failed',file_path='',error_detail=?,
                           processing_phase='failed',last_progress_at=CURRENT_TIMESTAMP,
                           completed_at=CAST(CURRENT_TIMESTAMP AS VARCHAR)
                           WHERE campaign_type='realtime' AND campaign_id=? AND filename=? AND status<>'completed'""",
                        ((reason, job["campaign_id"], job["filename"]) for job in expired_jobs),
                    )
                    conn.commit()
                finally:
                    conn.close()
        for job in expired_jobs:
            try:
                path = job.get("file_path", "")
                if path and os.path.isfile(path):
                    os.remove(path)
            except OSError as exc:
                logger.warning(
                    "Realtime expired file deletion failed campaign=%s file=%s error=%s",
                    campaign["id"], job["filename"], exc,
                )
        discarded += len(expired_jobs)
    if discarded:
        logger.info("Realtime closed-window ingestion jobs discarded count=%s", discarded)
    return discarded

def sync_watch_dirs_to_inboxes(now=None, campaign_id=None, force: bool = False,
                               enqueue_only: bool = False):
    global _watch_campaign_cursor
    conn = get_connection()
    if campaign_id:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=? AND status='in_progress' AND COALESCE(watch_dir,'')<>'' AND COALESCE(watch_source,'local')='local'",
            (campaign_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE status='in_progress' AND COALESCE(watch_dir,'')<>'' AND COALESCE(watch_source,'local')='local'"
        ).fetchall()
    conn.close()
    if now is None:
        now = datetime.now()
    rows = list(rows)
    if campaign_id is None and len(rows) > 1:
        rows.sort(key=lambda item: int(item["id"]))
        start = _watch_campaign_cursor % len(rows)
        rows = rows[start:] + rows[:start]
        _watch_campaign_cursor = (_watch_campaign_cursor + 1) % len(rows)
    total_moved = 0
    total_enqueued = 0
    for row in rows:
        campaign = dict(row)
        watch_dir = campaign.get("watch_dir", "")
        if not watch_dir or not os.path.isdir(watch_dir):
            continue
        # Keep the listener alive, but pause this campaign outside its
        # current window. Filename time remains authoritative below.
        active, reason = sms_task_service.campaign_is_active(campaign, now)
        if not active:
            logger.info("Watch intake paused campaign=%s reason=%s", campaign.get("id"), reason)
            continue
        if not _watch_scan_due(campaign, watch_dir, now, force=force):
            continue
        campaign_dir = campaign.get("campaign_dir", "")
        if not campaign_dir:
            campaign_dir = _ensure_campaign_dir(campaign)
            _ensure_realtime_dirs(campaign_dir)
            conn2 = get_connection()
            conn2.execute("UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign["id"]))
            conn2.commit()
            conn2.close()
        inbox_dir = os.path.join(campaign_dir, "inbox")
        os.makedirs(inbox_dir, exist_ok=True)
        prefix = campaign.get("watch_filename_prefix", "Android_4G_Switch_Somali_")
        if not prefix:
            prefix = "Android_4G_Switch_Somali_"
        capacity = _inbox_capacity_snapshot(campaign["id"])
        completed_files = _completed_processed_filenames(campaign["id"])
        already_processed_skipped = 0
        historical_skipped = 0
        campaign_window_skipped = 0
        backpressure_rejected = 0
        backpressure_reason = ""
        admitted = 0
        # The watched filename contains a fixed-width YYYYMMDDHHMMSS token;
        # lexical order therefore matches file time order.
        pending_only = campaign.pop("_watch_scan_mode", "full") == "pending"
        try:
            if pending_only:
                entries = [
                    (name, os.path.join(watch_dir, name))
                    for name in _pending_watch_names(campaign["id"], watch_dir)
                ]
            else:
                entries = sorted(os.scandir(watch_dir), key=lambda entry: entry.name)
        except OSError:
            continue
        ready_names = set()
        if not pending_only:
            try:
                ready_names = {
                    item.name[:-6]
                    for item in entries
                    if item.name.endswith(".ready")
                }
            except AttributeError:
                ready_names = set()
        for entry in entries:
            if admitted >= WATCH_DISCOVERY_QUANTUM:
                metrics_service.inc(
                    "realtime_watch_campaign_quantum_yield_total",
                    labels={"campaign_id": campaign["id"]},
                )
                break
            if isinstance(entry, tuple):
                fname, entry_path = entry
                entry_stat = None
            else:
                fname = entry.name
                entry_path = entry.path
                entry_stat = None
            if not fname.startswith(prefix):
                continue
            if fname.startswith(".") or fname.endswith((".part", ".ready")):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in (".csv", ".txt", ".xlsx", ".xls"):
                continue
            # Filename time is authoritative.  Reject historical/malformed
            # files before stat(), database lookups, copying, or whitelist
            # work; otherwise the scheduler repeatedly reprocesses old files.
            file_time = _watch_file_timestamp(fname, prefix)
            if not file_time:
                continue
            if file_time.date() != now.date():
                historical_skipped += 1
                continue
            if not _watch_file_matches_campaign(fname, campaign, now, file_time=file_time):
                campaign_window_skipped += 1
                continue
            src = entry_path
            try:
                if entry_stat is None:
                    entry_stat = os.stat(src)
                if not stat.S_ISREG(entry_stat.st_mode):
                    continue
                snapshot = _watch_snapshot(src)
            except OSError:
                continue
            producer_ready = fname in ready_names or os.path.exists(src + ".ready")
            if not _track_watch_stability(campaign["id"], src, snapshot, producer_ready):
                continue
            dest = os.path.join(inbox_dir, fname)
            if os.path.exists(dest):
                continue
            if fname in completed_files:
                already_processed_skipped += 1
                continue
            try:
                file_size = os.path.getsize(src)
                reservation = _reserve_inbox_capacity(
                    campaign["id"], fname, file_size, "local",
                )
                if not reservation.get("ok"):
                    if reservation.get("duplicate"):
                        already_processed_skipped += 1
                        continue
                    backpressure_rejected += 1
                    backpressure_reason = reservation["reason"]
                    break
                reservation_token = reservation["token"]
                copied = False
                if enqueue_only:
                    try:
                        _watch_copy_queue.put_nowait({
                            "campaign_id": campaign["id"], "filename": fname,
                            "src": src, "dest": dest,
                            "campaign_dir": campaign_dir,
                            "reservation_token": reservation_token,
                        })
                    except queue.Full:
                        ingestion_job_service.release_realtime_capacity(reservation_token)
                        backpressure_rejected += 1
                        backpressure_reason = "realtime watch copy queue is full"
                        break
                    total_enqueued += 1
                    metrics_service.set_gauge(
                        "realtime_watch_copy_queue_depth", _watch_copy_queue.qsize(),
                    )
                else:
                    copied = _copy_watch_job({
                        "campaign_id": campaign["id"], "filename": fname,
                        "src": src, "dest": dest, "campaign_dir": campaign_dir,
                        "reservation_token": reservation_token,
                    })
                    if copied:
                        total_moved += 1
                        metrics_service.inc("realtime_watch_files_published_total")
                if enqueue_only or copied:
                    admitted += 1
                    _record_inbox_capacity_file(capacity, file_size)
            except OSError:
                ingestion_job_service.release_realtime_capacity(reservation_token)
        if backpressure_rejected:
            _log_inbox_backpressure(campaign["id"], backpressure_rejected, backpressure_reason)
        if historical_skipped:
            _log_watch_skip_summary(campaign["id"], historical_skipped, "historical filename timestamp")
        if campaign_window_skipped:
            _log_watch_skip_summary(campaign["id"], campaign_window_skipped, "filename outside campaign window")
        if already_processed_skipped:
            _log_watch_skip_summary(campaign["id"], already_processed_skipped)
    if total_moved or total_enqueued:
        logger.info("Watch sync complete moved=%s enqueued=%s", total_moved, total_enqueued)
    return {"moved": total_moved, "enqueued": total_enqueued}


def sync_sftp_watch_dirs_to_inboxes(now=None):
    """Poll configured SFTP folders and atomically download eligible files."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM realtime_campaigns WHERE status='in_progress' "
        "AND watch_source='sftp' AND COALESCE(watch_dir,'')<>''"
    ).fetchall()
    conn.close()
    # Local-watch deployments do not need the optional Paramiko dependency;
    # avoid turning the SFTP poller into a repeating error loop when there are
    # no configured remote campaigns.  A configured SFTP campaign still gets
    # the same explicit dependency error below.
    if not rows:
        return {"downloaded": 0}
    from services import sftp_watch_service

    now = now or datetime.now()
    global _sftp_campaign_cursor
    rows = list(rows)
    if len(rows) > 1:
        rows.sort(key=lambda item: int(item["id"]))
        start = _sftp_campaign_cursor % len(rows)
        rows = rows[start:] + rows[:start]
        _sftp_campaign_cursor = (_sftp_campaign_cursor + 1) % len(rows)
    total_downloaded = 0
    for row in rows:
        campaign = dict(row)
        client = sftp = None
        backpressure_rejected = 0
        backpressure_reason = ""
        already_processed_skipped = 0
        admitted = 0
        try:
            active, reason = sms_task_service.campaign_is_active(campaign, now)
            if not active:
                continue
            client, sftp, entries = sftp_watch_service.list_directory(campaign)
            campaign_dir = campaign.get("campaign_dir", "") or _ensure_campaign_dir(campaign)
            _ensure_realtime_dirs(campaign_dir)
            if not campaign.get("campaign_dir"):
                update_conn = get_connection()
                update_conn.execute("UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign["id"]))
                update_conn.commit()
                update_conn.close()
            inbox_dir = os.path.join(campaign_dir, "inbox")
            os.makedirs(inbox_dir, exist_ok=True)
            prefix = campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_"
            remote_dir = campaign["watch_dir"].rstrip("/") or "/"
            capacity = _inbox_capacity_snapshot(campaign["id"])
            completed_files = _completed_processed_filenames(campaign["id"])
            already_processed_skipped = 0
            # Keep SFTP intake order consistent with local watch intake.
            for entry in sorted(entries, key=lambda item: item.filename):
                if admitted >= WATCH_DISCOVERY_QUANTUM:
                    metrics_service.inc(
                        "realtime_sftp_campaign_quantum_yield_total",
                        labels={"campaign_id": campaign["id"]},
                    )
                    break
                fname = entry.filename
                if fname in (".", "..") or os.path.basename(fname) != fname:
                    continue
                if not stat.S_ISREG(entry.st_mode) or not fname.startswith(prefix):
                    continue
                if os.path.splitext(fname)[1].lower() not in (".csv", ".txt", ".xlsx", ".xls"):
                    continue
                if not _watch_file_matches_campaign(fname, campaign, now):
                    continue
                remote_path = f"{remote_dir}/{fname}" if remote_dir != "/" else f"/{fname}"
                if not sftp_watch_service.file_is_stable(campaign["id"], remote_path, entry):
                    continue
                dest = os.path.join(inbox_dir, fname)
                if os.path.exists(dest):
                    continue
                if fname in completed_files:
                    already_processed_skipped += 1
                    continue
                file_size = entry.st_size or 0
                reservation = _reserve_inbox_capacity(
                    campaign["id"], fname, file_size, "sftp",
                )
                if not reservation.get("ok"):
                    if reservation.get("duplicate"):
                        already_processed_skipped += 1
                        continue
                    backpressure_rejected += 1
                    backpressure_reason = reservation["reason"]
                    break
                reservation_token = reservation["token"]
                consumed = False
                published = False
                temp_fd, temp_path = tempfile.mkstemp(prefix=".sftp-", suffix=".part", dir=inbox_dir)
                os.close(temp_fd)
                try:
                    sftp.get(remote_path, temp_path)
                    if os.path.getsize(temp_path) != (entry.st_size or 0):
                        raise IOError("remote file changed while it was being downloaded")
                    if os.path.exists(dest):
                        continue
                    os.replace(temp_path, dest)
                    published = True
                    os.utime(dest, None)
                    whitelist_version_id = (realtime_whitelist_service.active_version(campaign["id"]) or {}).get("id")
                    ingestion_job_service.register_file(
                        "realtime", campaign["id"], fname, dest,
                        chunk_size=_realtime_chunk_size(), whitelist_version_id=whitelist_version_id,
                        reservation_token=reservation_token,
                    )
                    consumed = True
                    admitted += 1
                    _record_inbox_capacity_file(capacity, file_size)
                    total_downloaded += 1
                    logger.info("SFTP watch file downloaded campaign=%s host=%s file=%s", campaign["id"], campaign.get("sftp_host"), fname)
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                    if published and not consumed:
                        try:
                            os.unlink(dest)
                        except FileNotFoundError:
                            pass
                        except OSError:
                            logger.warning(
                                "Unable to remove unpublished SFTP inbox file=%s",
                                dest, exc_info=True,
                            )
                    if not consumed:
                        ingestion_job_service.release_realtime_capacity(reservation_token)
        except Exception as exc:
            logger.warning("SFTP watch poll failed campaign=%s host=%s error=%s", campaign.get("id"), campaign.get("sftp_host"), exc)
        finally:
            if sftp:
                sftp.close()
            if client:
                client.close()
        if backpressure_rejected:
            _log_inbox_backpressure(
                campaign["id"], backpressure_rejected, backpressure_reason, source="sftp",
            )
        if already_processed_skipped:
            _log_watch_skip_summary(campaign["id"], already_processed_skipped)
    return {"downloaded": total_downloaded}


def process_realtime_inboxes(campaign_id: Optional[int] = None):
    cleanup_closed_window_ingestion_files()
    conn = get_connection()
    if campaign_id:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=? AND status='in_progress' AND campaign_dir<>''",
            (campaign_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE status='in_progress' AND campaign_dir<>''"
        ).fetchall()
    conn.close()
    results = {"processed": 0, "queued": 0, "errors": []}
    for row in rows:
        campaign = dict(row)
        campaign_dir = campaign.get("campaign_dir", "")
        if not campaign_dir:
            continue
        inbox_dir = os.path.join(campaign_dir, "inbox")
        if _daily_window_ended(campaign):
            continue
        if not os.path.isdir(inbox_dir):
            continue
        processing_conn = get_connection()
        try:
            processing_files = {
                item["filename"]
                for item in processing_conn.execute(
                    """SELECT filename FROM ingestion_files
                       WHERE campaign_type='realtime' AND campaign_id=? AND status='processing'""",
                    (campaign["id"],),
                ).fetchall()
            }
        finally:
            processing_conn.close()
        for fname in sorted(os.listdir(inbox_dir)):
            src = os.path.join(inbox_dir, fname)
            if not os.path.isfile(src):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in (".csv", ".txt", ".xlsx", ".xls"):
                continue
            active, reason = sms_task_service.campaign_is_active(campaign, datetime.now())
            file_time = _watch_file_timestamp(fname, campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_")
            if file_time and file_time.date() != datetime.now().date():
                if fname in processing_files:
                    # A worker may have claimed this file just before the date changed.
                    # Let its pre-processing expiry check finish the job.
                    continue
                try:
                    os.remove(src)
                    logger.info(
                        "Realtime expired inbox file deleted campaign=%s file=%s",
                        campaign["id"], fname,
                    )
                except OSError as exc:
                    results["errors"].append({"filename": fname, "message": str(exc)})
                    logger.warning(
                        "Realtime expired inbox file deletion failed campaign=%s file=%s error=%s",
                        campaign["id"], fname, exc,
                    )
                continue
            if not active and _daily_window_ended(campaign):
                if fname in processing_files:
                    continue
                try:
                    os.remove(src)
                    logger.info(
                        "Realtime closed-window inbox file deleted campaign=%s file=%s",
                        campaign["id"], fname,
                    )
                except OSError as exc:
                    results["errors"].append({"filename": fname, "message": str(exc)})
                    logger.warning(
                        "Realtime closed-window inbox file deletion failed campaign=%s file=%s error=%s",
                        campaign["id"], fname, exc,
                    )
                continue
            if not active and reason in {"after daily send window", "campaign has ended", "campaign is not enabled"}:
                try:
                    target = operations_service.quarantine(src, campaign.get("campaign_dir", ""), fname)
                    results["errors"].append({"filename": fname, "message": reason})
                    logger.info("Realtime inbox file archived campaign=%s file=%s reason=%s", campaign["id"], fname, reason)
                except OSError as exc:
                    results["errors"].append({"filename": fname, "message": str(exc)})
                continue
            if not active:
                continue
            try:
                existing = ingestion_job_service.get_file("realtime", campaign["id"], fname)
                if existing:
                    continue
                reservation = _reserve_inbox_capacity(
                    campaign["id"], fname, os.path.getsize(src), "inbox-recovery",
                )
                if not reservation.get("ok"):
                    if reservation.get("duplicate"):
                        continue
                    results["errors"].append({
                        "filename": fname, "message": reservation["reason"],
                    })
                    _log_inbox_backpressure(
                        campaign["id"], 1, reservation["reason"], source="inbox",
                    )
                    continue
                version_id = (realtime_whitelist_service.active_version(campaign["id"]) or {}).get("id")
                reservation_token = reservation["token"]
                consumed = False
                try:
                    _file_id, duplicate = ingestion_job_service.register_file(
                        "realtime", campaign["id"], fname, src, chunk_size=_realtime_chunk_size(),
                        whitelist_version_id=version_id,
                        reservation_token=reservation_token,
                    )
                    consumed = True
                finally:
                    if not consumed:
                        ingestion_job_service.release_realtime_capacity(reservation_token)
                if not duplicate:
                    results["queued"] += 1
                    depth_conn = get_connection()
                    try:
                        depth = depth_conn.execute(
                            "SELECT COUNT(*) count FROM ingestion_files WHERE campaign_type='realtime' AND status IN ('pending','processing','retrying')"
                        ).fetchone()["count"]
                    finally:
                        depth_conn.close()
                    logger.info("Realtime file queued campaign=%s file=%s queue_depth=%s",
                                campaign["id"], fname, int(depth or 0))
            except Exception as e:
                results["errors"].append({"filename": fname, "message": str(e)})
                logger.exception("Realtime inbox file registration failed campaign=%s file=%s", campaign["id"], fname)
    logger.info("Realtime inbox sweep queued=%s errors=%s", results["queued"], len(results["errors"]))
    return results


def cleanup_realtime_retention():
    input_retention_days = max(1, int(settings_service.get_setting("realtime_file_retention_days", os.environ.get("MC_REALTIME_FILE_RETENTION_DAYS", "1"))))
    output_retention_days = max(1, int(settings_service.get_setting("realtime_output_retention_days", os.environ.get("MC_REALTIME_OUTPUT_RETENTION_DAYS", "30"))))
    input_cutoff_ts = (datetime.now() - timedelta(days=input_retention_days)).timestamp()
    output_cutoff_ts = (datetime.now() - timedelta(days=output_retention_days)).timestamp()
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, campaign_dir FROM realtime_campaigns WHERE campaign_dir<>''"
    ).fetchall()
    protected_paths = {
        os.path.abspath(row["file_path"])
        for row in conn.execute(
            """SELECT file_path FROM ingestion_files
               WHERE campaign_type='realtime' AND status IN ('pending','processing','retrying')
                 AND COALESCE(file_path,'')<>''"""
        ).fetchall()
    }
    deleted = 0
    for row in rows:
        campaign_dir = row["campaign_dir"]
        for dirname in ("inbox", "received", "duplicates", "quarantine", "output"):
            cutoff_ts = output_cutoff_ts if dirname == "output" else input_cutoff_ts
            path = os.path.join(campaign_dir, dirname)
            if not os.path.isdir(path):
                continue
            for fname in os.listdir(path):
                file_path = os.path.join(path, fname)
                if not os.path.isfile(file_path):
                    continue
                if os.path.abspath(file_path) in protected_paths:
                    continue
                try:
                    if os.path.getmtime(file_path) < cutoff_ts:
                        os.remove(file_path)
                        deleted += 1
                except OSError:
                    pass
    db_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "DELETE FROM realtime_campaign_files WHERE created_at < ?", (db_cutoff,),
    )
    conn.commit()
    conn.close()
    logger.info(
        "Realtime file retention cleanup completed input_retention_days=%s output_retention_days=%s deleted=%s",
        input_retention_days, output_retention_days, deleted,
    )
