import os
import json
import re
import math
import shutil
from datetime import datetime
from typing import Optional
import pandas as pd
import csv
from database import get_connection
from models.realtime_campaign import RealtimeCampaignCreate, RealtimeCampaignUpdate
from services import settings_service
from services import sms_task_service
from services import operations_service


def _read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
    delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
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


def get_one(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return _enrich(dict(row)) if row else None


def create(data: RealtimeCampaignCreate) -> dict:
    conn = get_connection()
    campaign_dir = data.campaign_dir or ""
    cursor = conn.execute(
        "INSERT INTO realtime_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, campaign_dir, sms_template_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (data.name, data.start_time or "", data.end_time or "",
         data.send_start_time or "", data.send_end_time or "",
         _fmt_days(data.send_days), campaign_dir, data.sms_template_id)
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
        only_allowed = all(getattr(data, f, None) is None for f in ["name", "start_time", "end_time", "send_start_time", "send_end_time", "send_days", "campaign_dir"])
        if not only_allowed:
            conn.close()
            raise ValueError("Campaign has already started — cannot edit")
    fields = {}
    for field in ["name", "start_time", "end_time", "status", "send_start_time", "send_end_time", "campaign_dir", "sms_template_id"]:
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
            df = _read_csv(dest)
        else:
            df = pd.read_excel(dest, dtype=str)
        row_count = len(df)
    except Exception:
        row_count = 0
    conn = get_connection()
    conn.execute(
        "UPDATE realtime_campaigns SET customer_list_filename=?, customer_list_path=?, row_count=?, updated_at=datetime('now','localtime') WHERE id=?",
        (original_filename, dest, row_count, campaign_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _count_reached_users(campaign_dir: str) -> int:
    output_dir = os.path.join(campaign_dir, "output")
    if not os.path.isdir(output_dir):
        return 0
    unique = set()
    for fname in os.listdir(output_dir):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(output_dir, fname)
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=["msisdn"])
            unique.update(df["msisdn"].dropna().str.strip().tolist())
        except Exception:
            pass
    return len(unique)


def get_detail(campaign_id: int) -> Optional[dict]:
    campaign = get_one(campaign_id)
    if not campaign:
        return None
    campaign["reached_users"] = _count_reached_users(campaign.get("campaign_dir", ""))
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
        if ext == ".csv":
            df = _read_csv(file_path)
        else:
            df = pd.read_excel(file_path, dtype=str)
    except Exception as e:
        conn.close()
        target = operations_service.quarantine(file_path, campaign.get("campaign_dir", ""), original_filename)
        operations_service.finish_file(file_record_id, "quarantined", target, error=e)
        operations_service.event("file", "READ_FAILED", str(e), context={"campaign_id": campaign_id, "file": original_filename})
        return {"success": False, "message": f"Cannot read file: {e}"}

    msisdn_col = "msisdn" if "msisdn" in df.columns else df.columns[0]
    normalized_msisdns = df[msisdn_col].map(sms_task_service.normalize_msisdn)
    received_msisdns = {value for value in normalized_msisdns if value}
    total_msisdn = len(received_msisdns)

    wl_ext = os.path.splitext(whitelist_path)[1].lower()
    try:
        if wl_ext == ".csv":
            wl_df = _read_csv(whitelist_path)
        else:
            wl_df = pd.read_excel(whitelist_path, dtype=str)
    except Exception as e:
        conn.close()
        operations_service.finish_file(file_record_id, "failed", file_path, error=f"Cannot read whitelist: {e}")
        return {"success": False, "message": f"Cannot read whitelist: {e}"}

    wl_msisdn_col = "msisdn" if "msisdn" in wl_df.columns else wl_df.columns[0]
    whitelist_set = {
        sms_task_service.normalize_msisdn(value)
        for value in wl_df[wl_msisdn_col].dropna()
        if sms_task_service.normalize_msisdn(value)
    }
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
    operations_service.finish_file(file_record_id, "completed", dest, len(df), task_result.get("created", 0))
    return {
        "success": True,
        "filename": original_filename,
        "total_msisdn": total_msisdn,
        "matched_count": matched_count,
        "task_result": task_result,
        "file_record_id": file_record_id,
    }


def get_realtime_files(campaign_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM realtime_campaign_files WHERE campaign_id=? ORDER BY created_at DESC",
        (campaign_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_realtime_stats(campaign_id: int) -> dict:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS file_count, COALESCE(SUM(total_msisdn),0) AS total_msisdn_sum, COALESCE(SUM(matched_count),0) AS total_matched FROM realtime_campaign_files WHERE campaign_id=?",
        (campaign_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {"file_count": 0, "total_msisdn_sum": 0, "total_matched": 0}
