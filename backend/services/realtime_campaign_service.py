import os
import json
import re
import math
import shutil
import logging
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import csv
import time
from database import get_connection
from models.realtime_campaign import RealtimeCampaignCreate, RealtimeCampaignUpdate
from services import settings_service
from services import sms_task_service
from services import operations_service

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")

CHUNK_SIZE = 100000  # rows per chunk for large CSV files


def _read_csv(path: str, chunksize: int = 0):
    """Read CSV file. If chunksize > 0, returns a chunk iterator instead of a DataFrame."""
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
    # Detect delimiter: check for pipe (|) in first line, fall back to Sniffer
    first_line = sample.split("\n")[0].strip()
    if "|" in first_line and first_line.count("|") >= 1:
        delimiter = "|"
    else:
        delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
    if chunksize:
        return pd.read_csv(path, dtype=str, sep=delimiter, chunksize=chunksize)
    return pd.read_csv(path, dtype=str, sep=delimiter)


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


def get_all() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM realtime_campaigns ORDER BY created_at DESC").fetchall()
    conn.close()
    return [_enrich(dict(r)) for r in rows]


def list_campaigns(page: int = 1, page_size: int = 50) -> dict:
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) cnt FROM realtime_campaigns").fetchone()["cnt"]
    offset = max(0, page - 1) * page_size
    rows = conn.execute(
        "SELECT * FROM realtime_campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (page_size, offset),
    ).fetchall()
    conn.close()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_enrich(dict(r)) for r in rows],
    }


def get_one(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return _enrich(dict(row)) if row else None


def create(data: RealtimeCampaignCreate) -> dict:
    conn = get_connection()
    use_custom_directory = bool(data.use_custom_directory and (data.campaign_dir or "").strip())
    campaign_dir = (data.campaign_dir or "").strip() if use_custom_directory else ""
    cursor = conn.execute(
        "INSERT INTO realtime_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, campaign_dir, use_custom_directory, watch_dir, watch_filename_prefix, phone_prefix, sms_template_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (data.name, data.start_time or "", data.end_time or "",
         data.send_start_time or "", data.send_end_time or "",
         _fmt_days(data.send_days), campaign_dir, use_custom_directory,
         data.watch_dir or "", data.watch_filename_prefix or "Android_4G_Switch_Somali_",
         data.phone_prefix or "", data.sms_template_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    campaign = _enrich(dict(row))

    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        conn2 = get_connection()
        conn2.execute("UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign["id"]))
        conn2.commit()
        conn2.close()
        campaign["campaign_dir"] = campaign_dir

    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "received"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    return campaign


def update(campaign_id: int, data: RealtimeCampaignUpdate) -> Optional[dict]:
    conn = get_connection()
    existing = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not existing:
        conn.close()
        return None
    existing_dict = dict(existing)
    if data.status == "in_progress":
        validation_errors = operations_service.validate_campaign("realtime", {**existing_dict, "id": campaign_id})
        if validation_errors:
            conn.close()
            raise ValueError("; ".join(validation_errors))
    if data.status == "in_progress" and not existing_dict.get("row_count", 0):
        conn.close()
        raise ValueError("No whitelist uploaded — cannot start campaign")
    if existing_dict.get("status") != "not_started":
        only_allowed = all(getattr(data, f, None) is None for f in ["name", "start_time", "campaign_dir"])
        if not only_allowed:
            conn.close()
            raise ValueError("Campaign has already started — cannot edit")
    fields = {}
    for field in ["name", "start_time", "end_time", "status", "send_start_time", "send_end_time", "campaign_dir", "sms_template_id", "watch_dir", "watch_filename_prefix", "phone_prefix", "use_custom_directory"]:
        val = getattr(data, field, None)
        if val is not None:
            fields[field] = val
    if data.send_days is not None:
        fields["send_days"] = _fmt_days(data.send_days)

    if fields:
        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE realtime_campaigns SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
            (*fields.values(), campaign_id)
        )
        conn.commit()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return _enrich(dict(row))


def delete(campaign_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if row:
        campaign_dir = dict(row).get("campaign_dir", "")
        if campaign_dir and os.path.isdir(campaign_dir):
            try:
                shutil.rmtree(campaign_dir)
            except Exception:
                pass
    conn.execute("DELETE FROM realtime_campaign_files WHERE campaign_id=?", (campaign_id,))
    cursor = conn.execute("DELETE FROM realtime_campaigns WHERE id=?", (campaign_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def copy(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        conn.close()
        return None
    orig = dict(row)
    new_name = orig["name"] + " (Copy)"
    cursor = conn.execute(
        "INSERT INTO realtime_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, sms_template_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (new_name, orig["start_time"], orig["end_time"],
         orig.get("send_start_time", ""), orig.get("send_end_time", ""),
         orig.get("send_days", "[]"), orig.get("sms_template_id"))
    )
    conn.commit()
    new_row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    new_campaign = _enrich(dict(new_row))
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
    conn2 = get_connection()
    conn2.execute("UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, new_campaign["id"]))
    conn2.commit()
    conn2.close()
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
        conn = get_connection()
        conn.execute("UPDATE realtime_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign_id))
        conn.commit()
        conn.close()
    whitelist_dir = os.path.join(campaign_dir, "whitelist")
    os.makedirs(whitelist_dir, exist_ok=True)
    ext = os.path.splitext(original_filename)[1].lower()
    dest = os.path.join(whitelist_dir, f"whitelist{ext}")
    shutil.move(file_path, dest)
    try:
        if ext == ".csv":
            row_count = sum(len(chunk) for chunk in _read_csv(dest, chunksize=CHUNK_SIZE))
        else:
            df = pd.read_excel(dest, dtype=str)
            row_count = len(df)
    except Exception as exc:
        row_count = 0
        logger.error("Whitelist upload failed campaign=%s file=%s error=%s", campaign_id, original_filename, exc)
    else:
        logger.info("Whitelist uploaded campaign=%s file=%s rows=%s dest=%s", campaign_id, original_filename, row_count, dest)
    conn = get_connection()
    conn.execute(
        "UPDATE realtime_campaigns SET customer_list_filename=?, customer_list_path=?, row_count=?, updated_at=datetime('now','localtime') WHERE id=?",
        (original_filename, dest, row_count, campaign_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


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


def get_detail(campaign_id: int) -> Optional[dict]:
    campaign = get_one(campaign_id)
    if not campaign:
        return None
    campaign["reached_users"] = _count_output_rows(campaign.get("campaign_dir", ""))
    return campaign


def receive_file(campaign_id: int, file_path: str, original_filename: str) -> dict:
    conn = get_connection()
    campaign = conn.execute(
        "SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)
    ).fetchone()
    if not campaign:
        conn.close()
        return {"success": False, "message": "Campaign not found"}
    campaign = dict(campaign)
    try:
        file_record_id, duplicate_file = operations_service.register_file(
            "realtime", campaign_id, file_path, original_filename)
    except Exception as exc:
        conn.close()
        return {"success": False, "message": str(exc)}
    if duplicate_file:
        conn.close()
        duplicate_path = operations_service.archive_duplicate(
            file_path, campaign.get("campaign_dir", ""), original_filename)
        return {"success": True, "duplicate_file": True, "duplicate_path": duplicate_path,
                "message": "file already processed"}
    active, inactive_reason = sms_task_service.campaign_is_active(campaign)
    if not active:
        conn.close()
        operations_service.finish_file(file_record_id, "failed", file_path, error=inactive_reason)
        return {"success": False, "message": inactive_reason}
    whitelist_path = campaign.get("customer_list_path", "")
    if not whitelist_path or not os.path.isfile(whitelist_path):
        conn.close()
        operations_service.finish_file(file_record_id, "failed", file_path, error="No whitelist uploaded")
        return {"success": False, "message": "No whitelist uploaded for this campaign"}

    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext in (".csv", ".txt"):
            reader = _read_csv(file_path, chunksize=CHUNK_SIZE)
        else:
            df = pd.read_excel(file_path, dtype=str)
    except Exception as e:
        conn.close()
        target = operations_service.quarantine(file_path, campaign.get("campaign_dir", ""), original_filename)
        operations_service.finish_file(file_record_id, "quarantined", target, error=e)
        operations_service.event("file", "READ_FAILED", str(e), context={"campaign_id": campaign_id, "file": original_filename})
        return {"success": False, "message": f"Cannot read file: {e}"}

    # Load whitelist MSISDNs into a set (chunked for large CSV files)
    wl_ext = os.path.splitext(whitelist_path)[1].lower()
    try:
        if wl_ext == ".csv":
            whitelist_set = set()
            for wl_chunk in _read_csv(whitelist_path, chunksize=CHUNK_SIZE):
                wl_msisdn_col = "msisdn" if "msisdn" in wl_chunk.columns else wl_chunk.columns[0]
                for value in wl_chunk[wl_msisdn_col].dropna():
                    norm = sms_task_service.normalize_msisdn(value)
                    if norm:
                        whitelist_set.add(norm)
        else:
            wl_df = pd.read_excel(whitelist_path, dtype=str)
            wl_msisdn_col = "msisdn" if "msisdn" in wl_df.columns else wl_df.columns[0]
            whitelist_set = {
                sms_task_service.normalize_msisdn(value)
                for value in wl_df[wl_msisdn_col].dropna()
                if sms_task_service.normalize_msisdn(value)
            }
    except Exception as e:
        conn.close()
        operations_service.finish_file(file_record_id, "failed", file_path, error=f"Cannot read whitelist: {e}")
        return {"success": False, "message": f"Cannot read whitelist: {e}"}

    # Match input MSISDNs against whitelist (chunked for CSV, full load for Excel)
    if ext in (".csv", ".txt"):
        received_msisdns = set()
        matched_row_dicts = []
        total_rows = 0
        msisdn_col = None
        for chunk in reader:
            total_rows += len(chunk)
            if msisdn_col is None:
                msisdn_col = "msisdn" if "msisdn" in chunk.columns else chunk.columns[0]
            chunk_normalized = chunk[msisdn_col].map(sms_task_service.normalize_msisdn)
            for idx in range(len(chunk)):
                norm = chunk_normalized.iloc[idx]
                if norm:
                    received_msisdns.add(norm)
                    if norm in whitelist_set:
                        row_dict = chunk.iloc[idx].fillna("").to_dict()
                        row_dict["msisdn"] = norm
                        matched_row_dicts.append(row_dict)
        total_msisdn = len(received_msisdns)
        matched = received_msisdns & whitelist_set
        matched_count = len(matched)
        task_result = sms_task_service.enqueue_rows(
            "realtime", campaign, matched_row_dicts, original_filename
        )
    else:
        msisdn_col = "msisdn" if "msisdn" in df.columns else df.columns[0]
        normalized_msisdns = df[msisdn_col].map(sms_task_service.normalize_msisdn)
        received_msisdns = {value for value in normalized_msisdns if value}
        total_msisdn = len(received_msisdns)
        total_rows = len(df)
        matched = received_msisdns & whitelist_set
        matched_count = len(matched)
        matched_rows = df[normalized_msisdns.isin(matched)].copy()
        matched_rows["msisdn"] = normalized_msisdns[normalized_msisdns.isin(matched)]
        task_result = sms_task_service.enqueue_rows(
            "realtime", campaign, matched_rows.fillna("").to_dict("records"), original_filename
        )

    campaign_dir = campaign.get("campaign_dir", "")
    received_dir = os.path.join(campaign_dir, "received")
    os.makedirs(received_dir, exist_ok=True)
    dest = os.path.join(received_dir, original_filename)
    shutil.move(file_path, dest)
    try:
        os.utime(dest, None)
    except OSError:
        pass

    # Write matched users to output CSV
    if matched_count > 0 and campaign_dir:
        output_dir = os.path.join(campaign_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        out_date = datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(output_dir, f"{out_date}.csv")
        matched_df = pd.DataFrame({"msisdn": list(matched)})
        now_ts = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        matched_df["send_time"] = now_ts
        matched_df.to_csv(output_path, index=False, mode="a",
                          header=not os.path.isfile(output_path), encoding="utf-8-sig")

    conn.execute(
        "INSERT INTO realtime_campaign_files (campaign_id, filename, file_path, total_msisdn, matched_count) VALUES (?,?,?,?,?)",
        (campaign_id, original_filename, dest, total_msisdn, matched_count)
    )
    conn.commit()
    conn.close()
    operations_service.finish_file(file_record_id, "completed", dest, total_rows, task_result.get("created", 0))
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
    conn.close()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [dict(r) for r in rows],
    }


def get_realtime_sent_records(campaign_id: int, page: int = 1, page_size: int = 50) -> dict:
    campaign = get_one(campaign_id)
    if not campaign:
        return {"total": 0, "page": page, "page_size": page_size, "items": []}
    output_dir = os.path.join(campaign.get("campaign_dir", ""), "output")
    if not os.path.isdir(output_dir):
        return {"total": 0, "page": page, "page_size": page_size, "items": []}
    records = []
    for fname in sorted(os.listdir(output_dir), reverse=True):
        if not re.match(r"^\d{8}\.csv$", fname):
            continue
        file_path = os.path.join(output_dir, fname)
        user_count = 0
        try:
            df = pd.read_csv(file_path, dtype=str, encoding="utf-8-sig")
            user_count = len(df)
        except Exception:
            pass
        date_raw = os.path.splitext(fname)[0]
        try:
            date = datetime.strptime(date_raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            date = date_raw
        records.append({"date": date, "filename": fname, "user_count": user_count})
    records.sort(key=lambda item: item["filename"], reverse=True)
    total = len(records)
    offset = max(0, page - 1) * page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": records[offset:offset + page_size],
    }


def get_realtime_stats(campaign_id: int) -> dict:
    conn = get_connection()
    campaign = conn.execute(
        "SELECT campaign_dir FROM realtime_campaigns WHERE id=?",
        (campaign_id,),
    ).fetchone()
    row = conn.execute(
        "SELECT COUNT(*) AS file_count, COALESCE(SUM(total_msisdn),0) AS total_msisdn_sum, COALESCE(SUM(matched_count),0) AS total_matched FROM realtime_campaign_files WHERE campaign_id=?",
        (campaign_id,)
    ).fetchone()
    conn.close()
    result = dict(row) if row else {"file_count": 0, "total_msisdn_sum": 0, "total_matched": 0}
    campaign_dir = campaign["campaign_dir"] if campaign else ""
    result["today_users"] = _count_output_rows(campaign_dir, datetime.now().strftime("%Y%m%d"))
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


def _watch_file_matches_campaign(filename: str, campaign: dict, now: Optional[datetime] = None) -> bool:
    prefix = campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_"
    file_time = _watch_file_timestamp(filename, prefix)
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


def sync_watch_dirs_to_inboxes(now=None, campaign_id=None):
    conn = get_connection()
    if campaign_id:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=? AND status='in_progress' AND COALESCE(watch_dir,'')<>''",
            (campaign_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE status='in_progress' AND COALESCE(watch_dir,'')<>''"
        ).fetchall()
    conn.close()
    if now is None:
        now = datetime.now()
    total_moved = 0
    for row in rows:
        campaign = dict(row)
        watch_dir = campaign.get("watch_dir", "")
        if not watch_dir or not os.path.isdir(watch_dir):
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
        for fname in os.listdir(watch_dir):
            src = os.path.join(watch_dir, fname)
            if not os.path.isfile(src):
                continue
            if not fname.startswith(prefix):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in (".csv", ".txt", ".xlsx", ".xls"):
                continue
            if not _watch_file_matches_campaign(fname, campaign, now):
                logger.info("Watch file skipped campaign=%s file=%s reason=filename time does not match campaign", campaign.get("id"), fname)
                continue
            # Skip files still being written (modified < 5 seconds ago)
            try:
                mtime = os.path.getmtime(src)
                if now.timestamp() - mtime < 5:
                    continue
            except OSError:
                continue
            dest = os.path.join(inbox_dir, fname)
            if os.path.exists(dest):
                continue
            processed = _processed_file_record(campaign["id"], fname)
            if processed and processed.get("status") == "completed":
                logger.info("Watch file skipped campaign=%s file=%s reason=already processed", campaign.get("id"), fname)
                continue
            try:
                shutil.copy2(src, dest)
                try:
                    os.utime(dest, None)
                except OSError:
                    pass
                total_moved += 1
                logger.info("Watch file copied campaign=%s src=%s dest=%s", campaign.get("id"), src, dest)
            except OSError:
                pass
    if total_moved:
        logger.info("Watch sync complete moved=%s", total_moved)
    return {"moved": total_moved}


def process_realtime_inboxes(campaign_id: Optional[int] = None):
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
    results = {"processed": 0, "errors": []}
    for row in rows:
        campaign = dict(row)
        campaign_dir = campaign.get("campaign_dir", "")
        if not campaign_dir:
            continue
        inbox_dir = os.path.join(campaign_dir, "inbox")
        if not os.path.isdir(inbox_dir):
            continue
        for fname in sorted(os.listdir(inbox_dir)):
            src = os.path.join(inbox_dir, fname)
            if not os.path.isfile(src):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in (".csv", ".txt", ".xlsx", ".xls"):
                continue
            try:
                result = receive_file(campaign["id"], src, fname)
                if result.get("success"):
                    results["processed"] += 1
                    logger.info(
                        "Inbox file processed campaign=%s file=%s total_msisdn=%s matched=%s",
                        campaign["id"], fname, result.get("total_msisdn", 0), result.get("matched_count", 0),
                    )
                else:
                    results["errors"].append({"filename": fname, "message": result.get("message", "unknown")})
                    logger.error(
                        "Inbox file rejected campaign=%s file=%s reason=%s",
                        campaign["id"], fname, result.get("message", "unknown"),
                    )
            except Exception as e:
                results["errors"].append({"filename": fname, "message": str(e)})
                logger.exception("Inbox file failed campaign=%s file=%s", campaign["id"], fname)
    logger.info("Realtime inbox sweep processed=%s errors=%s", results["processed"], len(results["errors"]))
    return results


def cleanup_realtime_retention():
    input_retention_days = max(1, int(os.environ.get("MC_REALTIME_FILE_RETENTION_DAYS", "1")))
    output_retention_days = max(1, int(os.environ.get("MC_REALTIME_OUTPUT_RETENTION_DAYS", "30")))
    input_cutoff_ts = (datetime.now() - timedelta(days=input_retention_days)).timestamp()
    output_cutoff_ts = (datetime.now() - timedelta(days=output_retention_days)).timestamp()
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, campaign_dir FROM realtime_campaigns WHERE campaign_dir<>''"
    ).fetchall()
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
