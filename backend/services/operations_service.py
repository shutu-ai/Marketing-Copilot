import hashlib
import json
import os
import shutil
from datetime import datetime

from database import get_connection

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
MAX_FILE_BYTES = int(os.environ.get("MC_MAX_UPLOAD_BYTES", str(2 * 1024 * 1024 * 1024)))


def file_digest(path: str) -> str:
    digest, size = hashlib.sha256(), 0
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(chunk)
            if size > MAX_FILE_BYTES:
                raise ValueError(f"file exceeds {MAX_FILE_BYTES} byte limit")
            digest.update(chunk)
    return digest.hexdigest()


def register_file(campaign_type: str, campaign_id: int, path: str, filename: str) -> tuple[int, bool]:
    if os.path.splitext(filename)[1].lower() not in ALLOWED_EXTENSIONS:
        raise ValueError("unsupported file type")
    digest = file_digest(path)
    safe_filename = os.path.basename(filename)
    conn = get_connection()
    existing = conn.execute(
        "SELECT id, status FROM processed_files WHERE campaign_type=? AND campaign_id=? AND filename=?",
        (campaign_type, campaign_id, safe_filename)).fetchone()
    if existing:
        if existing["status"] != "completed":
            conn.execute(
                """UPDATE processed_files
                   SET file_hash=?, file_path=?, file_size=?, status='validating', error_detail='',
                       completed_at=NULL
                   WHERE id=?""",
                (digest, os.path.abspath(path), os.path.getsize(path), existing["id"]),
            )
            conn.commit()
            conn.close()
            return existing["id"], False
        conn.close()
        return existing["id"], True
    try:
        cursor = conn.execute(
            """INSERT INTO processed_files
               (campaign_type,campaign_id,filename,file_hash,file_path,file_size,status)
               VALUES (?,?,?,?,?,?,'validating')""",
            (campaign_type, campaign_id, safe_filename, digest, os.path.abspath(path), os.path.getsize(path)))
    except Exception as exc:
        if "duplicate" not in str(exc).lower() and "unique" not in str(exc).lower():
            conn.close()
            raise
        existing = conn.execute(
            "SELECT id FROM processed_files WHERE campaign_type=? AND campaign_id=? AND filename=?",
            (campaign_type, campaign_id, safe_filename)).fetchone()
        conn.close()
        if existing:
            return existing["id"], True
        raise
    conn.commit()
    result = cursor.lastrowid
    conn.close()
    return result, False


def finish_file(file_id: int, status: str, path: str = "", total_rows: int = 0,
                created_tasks: int = 0, error: str = ""):
    conn = get_connection()
    conn.execute(
        """UPDATE processed_files SET status=?,file_path=?,total_rows=?,created_tasks=?,error_detail=?,
           processing_phase=?,last_progress_at=CURRENT_TIMESTAMP,
           completed_at=CASE WHEN ? IN ('completed','failed','quarantined') THEN CAST(CURRENT_TIMESTAMP AS VARCHAR) ELSE completed_at END
           WHERE id=?""",
        (status, path, total_rows, created_tasks, str(error)[:4000],
         "completed" if status == "completed" else ("quarantined" if status == "quarantined" else "failed"),
         status, file_id))
    conn.commit()
    conn.close()


def update_file_progress(file_id: int, phase: str, processed_rows: int = 0,
                         matched_rows: int = 0, chunk_index: int = 0,
                         total_chunks: int = 0, worker_id: str = "",
                         whitelist_version_id=None):
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE processed_files SET processing_phase=?,processed_rows=?,matched_rows=?,
               current_chunk_index=?,total_chunks=?,worker_id=?,last_progress_at=CURRENT_TIMESTAMP,
               processing_started_at=COALESCE(processing_started_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
               whitelist_version_id=COALESCE(?,whitelist_version_id) WHERE id=?""",
            (phase, int(processed_rows or 0), int(matched_rows or 0), int(chunk_index or 0),
             int(total_chunks or 0), worker_id or "", whitelist_version_id, file_id),
        )
        conn.commit()
    finally:
        conn.close()


def mark_file_retry(file_id: int, error_class: str, error: str, next_retry_at=None):
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE processed_files SET status='retrying',retry_count=COALESCE(retry_count,0)+1,
               last_error_class=?,last_error_at=CURRENT_TIMESTAMP,
               next_retry_at=?,error_detail=?,processing_phase='queued',last_progress_at=CURRENT_TIMESTAMP,
               completed_at=NULL
               WHERE id=?""",
            (error_class, next_retry_at, str(error)[:4000], file_id),
        )
        conn.commit()
    finally:
        conn.close()


def quarantine(path: str, campaign_dir: str, filename: str) -> str:
    target_dir = os.path.join(campaign_dir, "quarantine")
    os.makedirs(target_dir, exist_ok=True)
    safe_name = os.path.basename(filename)
    stem, ext = os.path.splitext(safe_name)
    target = os.path.join(target_dir, safe_name)
    if os.path.exists(target):
        target = os.path.join(target_dir, f"{stem}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}{ext}")
    shutil.move(path, target)
    try:
        os.utime(target, None)
    except OSError:
        pass
    return target


def archive_duplicate(path: str, campaign_dir: str, filename: str) -> str:
    """Move a duplicate out of a watched inbox so it is not scanned forever."""
    if not os.path.isfile(path):
        return ""
    target_dir = os.path.join(campaign_dir, "duplicates")
    os.makedirs(target_dir, exist_ok=True)
    safe_name = os.path.basename(filename)
    stem, ext = os.path.splitext(safe_name)
    target = os.path.join(target_dir, safe_name)
    if os.path.exists(target):
        target = os.path.join(target_dir, f"{stem}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}{ext}")
    shutil.move(path, target)
    try:
        os.utime(target, None)
    except OSError:
        pass
    return target


def event(category: str, code: str, message: str, severity: str = "error", context=None):
    conn = get_connection()
    existing = conn.execute(
        "SELECT id FROM system_events WHERE category=? AND code=? AND resolved=0 LIMIT 1",
        (category, code),
    ).fetchone()
    if not existing:
        conn.execute("INSERT INTO system_events(category,severity,code,message,context) VALUES (?,?,?,?,?)",
                     (category, severity, code, str(message)[:2000], json.dumps(context or {}, ensure_ascii=False)))
    conn.commit()
    conn.close()


def audit(action: str, resource: str = "", resource_id="", detail=None,
          username: str = "system", ip_address: str = ""):
    conn = get_connection()
    conn.execute(
        """INSERT INTO audit_logs(username,action,resource,resource_id,detail,ip_address)
           VALUES (?,?,?,?,?,?)""",
        (username, action, resource, str(resource_id or ""),
         json.dumps(detail or {}, ensure_ascii=False, default=str), ip_address),
    )
    conn.commit()
    conn.close()


def validate_campaign(campaign_type: str, campaign: dict) -> list[str]:
    errors = []
    if campaign_type != "realtime" or campaign.get("use_whitelist", True):
        if not campaign.get("customer_list_path") or not os.path.isfile(campaign["customer_list_path"]):
            errors.append("campaign whitelist is missing" if campaign_type == "realtime" else "campaign target file is missing")
    if not campaign.get("campaign_dir"):
        errors.append("campaign directory is missing")
    else:
        try:
            os.makedirs(campaign["campaign_dir"], exist_ok=True)
            probe = os.path.join(campaign["campaign_dir"], ".write-test")
            with open(probe, "w", encoding="utf-8") as stream: stream.write("ok")
            os.unlink(probe)
        except OSError as exc:
            errors.append(f"campaign directory is not writable: {exc}")
    if campaign_type == "realtime":
        watch_dir = (campaign.get("watch_dir") or "").strip()
        if not watch_dir:
            errors.append("realtime watch directory is missing")
        elif not os.path.isdir(watch_dir):
            errors.append(f"realtime watch directory does not exist: {watch_dir}")
        if not (campaign.get("watch_filename_prefix") or "").strip():
            errors.append("realtime watch filename prefix is missing")
    conn = get_connection()
    template = conn.execute(
        """SELECT t.id FROM sms_templates t JOIN campaigns c ON c.id=t.campaign_id
           WHERE c.marketing_type=? AND c.marketing_campaign_id=? AND c.enabled=1 AND t.enabled=1 LIMIT 1""",
        (campaign_type, campaign["id"])).fetchone()
    if not template and campaign.get("sms_template_id"):
        template = conn.execute(
            """SELECT t.id FROM sms_templates t JOIN campaigns c ON c.id=t.campaign_id
               WHERE t.id=? AND c.enabled=1 AND t.enabled=1 LIMIT 1""",
            (campaign["sms_template_id"],),
        ).fetchone()
    if campaign_type in ("realtime", "batch"):
        smpp_id = campaign.get("smpp_config_id")
        if smpp_id:
            smpp = conn.execute(
                "SELECT id FROM smpp_configs WHERE id=? AND enabled=1", (smpp_id,)
            ).fetchone()
            if not smpp:
                errors.append("associated SMPP configuration is missing or disabled")
        else:
            errors.append(f"{campaign_type} campaign must be associated with an SMPP")
    else:
        smpp = conn.execute("SELECT id FROM smpp_configs WHERE enabled=1 LIMIT 1").fetchone()
    conn.close()
    if not template: errors.append("no enabled SMS template")
    from services.settings_service import get_setting
    if campaign_type not in ("realtime", "batch") and get_setting("system_mode", "production") == "production" and not smpp:
        errors.append("no enabled SMPP configuration")
    return errors
