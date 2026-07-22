import hashlib
import json
import os
import shutil
from datetime import datetime

from database import get_connection

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
MAX_FILE_BYTES = int(os.environ.get("MC_MAX_UPLOAD_BYTES", str(100 * 1024 * 1024)))


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
    conn = get_connection()
    existing = conn.execute(
        "SELECT id FROM processed_files WHERE campaign_type=? AND campaign_id=? AND file_hash=?",
        (campaign_type, campaign_id, digest)).fetchone()
    if existing:
        conn.close()
        return existing["id"], True
    try:
        cursor = conn.execute(
            """INSERT INTO processed_files
               (campaign_type,campaign_id,filename,file_hash,file_path,file_size,status)
               VALUES (?,?,?,?,?,?,'validating')""",
            (campaign_type, campaign_id, os.path.basename(filename), digest, os.path.abspath(path), os.path.getsize(path)))
    except Exception as exc:
        if "duplicate" not in str(exc).lower() and "unique" not in str(exc).lower():
            conn.close()
            raise
        existing = conn.execute(
            "SELECT id FROM processed_files WHERE campaign_type=? AND campaign_id=? AND file_hash=?",
            (campaign_type, campaign_id, digest)).fetchone()
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
           completed_at=CURRENT_TIMESTAMP WHERE id=?""",
        (status, path, total_rows, created_tasks, str(error)[:4000], file_id))
    conn.commit()
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
    if not campaign.get("customer_list_path") or not os.path.isfile(campaign["customer_list_path"]):
        errors.append("customer/whitelist file is missing")
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
    conn = get_connection()
    template = conn.execute(
        """SELECT t.id FROM sms_templates t JOIN campaigns c ON c.id=t.campaign_id
           WHERE c.marketing_type=? AND c.marketing_campaign_id=? AND c.enabled=1 AND t.enabled=1 LIMIT 1""",
        (campaign_type, campaign["id"])).fetchone()
    smpp = conn.execute("SELECT id FROM smpp_configs WHERE enabled=1 LIMIT 1").fetchone()
    conn.close()
    if not template: errors.append("no enabled SMS template")
    if os.environ.get("MC_SYSTEM_MODE", "production") == "production" and not smpp:
        errors.append("no enabled SMPP configuration")
    return errors
