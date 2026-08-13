import os
import json
import math
import re
import logging
from datetime import datetime
from database import get_connection
from models.batch_campaign import BatchCampaignCreate, BatchCampaignUpdate
from typing import Optional
import shutil
import pandas as pd
from services import settings_service
from services import sms_task_service, operations_service, campaign_name_service

logger = logging.getLogger(__name__)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploads", "campaigns")
ALLOWED_LANGUAGES = {"amharic", "tigrigna", "somali", "english", "oromign", "en"}


def _detect_csv_delimiter(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
        if "|" in first_line:
            return "|"
        return ","
    except Exception:
        return ","


def _validate_customer_list(df: pd.DataFrame) -> int:
    columns = set(df.columns)
    errors = []
    if "msisdn" not in columns:
        errors.append("Customer list file must contain an msisdn column")
    if errors:
        raise ValueError("; ".join(errors))

    if df.empty:
        raise ValueError("Customer list file is empty")

    msisdn_col = "msisdn"
    valid_rows = 0
    for idx, value in enumerate(df[msisdn_col]):
        normalized = sms_task_service.normalize_msisdn(value)
        if not normalized:
            errors.append(f"Row {idx + 2}: msisdn is empty or invalid")
        else:
            valid_rows += 1

    if "language" in columns:
        language_col = "language"
        allowed_label = ", ".join(sorted(ALLOWED_LANGUAGES))
        for idx, value in enumerate(df[language_col]):
            language = str(value).strip().lower() if pd.notna(value) else ""
            if not language or language not in ALLOWED_LANGUAGES:
                errors.append(f"Row {idx + 2}: language '{str(value).strip()}' is invalid; allowed: {allowed_label}")

    if errors:
        raise ValueError("\n".join(errors[:20]))
    return valid_rows


def _get_batch_base_dir() -> str:
    return settings_service.get_setting("batch_base_dir") or os.path.join(
        os.path.dirname(__file__), "..", "uploads", "batch_campaigns"
    )


def _sanitize_dirname(name: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    s = s.rstrip(". ")
    if not s:
        s = "campaign"
    return s


def _ensure_campaign_dir(campaign: dict) -> str:
    base = _get_batch_base_dir()
    dirname = _sanitize_dirname(campaign.get("name", f"campaign_{campaign['id']}"))
    campaign_dir = os.path.join(base, dirname)
    return campaign_dir


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


def _get_campaign_templates(batch_campaign_id: int) -> list:
    conn = get_connection()
    rows = conn.execute("""
        SELECT st.id, st.language, st.content, st.source_addr
        FROM sms_templates st
        JOIN campaigns c ON c.id = st.campaign_id
        WHERE c.marketing_campaign_id = ? AND c.marketing_type = 'batch'
        ORDER BY st.language
    """, (batch_campaign_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _enrich(c: dict) -> dict:
    c["send_days"] = _parse_days(c.get("send_days", ""))
    return c


def get_all() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM batch_campaigns ORDER BY created_at DESC").fetchall()
    conn.close()
    return [_enrich(dict(r)) for r in rows]


def list_campaigns(page: int = 1, page_size: int = 50) -> dict:
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) cnt FROM batch_campaigns").fetchone()["cnt"]
    offset = max(0, page - 1) * page_size
    rows = conn.execute(
        "SELECT * FROM batch_campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?",
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
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return _enrich(dict(row)) if row else None


def create(data: BatchCampaignCreate) -> dict:
    name = campaign_name_service.ensure_unique_name(data.name)
    if not (data.phone_prefix or "").strip():
        raise ValueError("\u8bf7\u8f93\u5165\u53f7\u7801\u524d\u7f00")
    conn = get_connection()
    use_custom_directory = bool(data.use_custom_directory and (data.campaign_dir or "").strip())
    campaign_dir = (data.campaign_dir or "").strip() if use_custom_directory else ""
    cursor = conn.execute(
        "INSERT INTO batch_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, batch_size, campaign_dir, use_custom_directory, sms_template_id, smpp_config_id, phone_prefix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, data.start_time or "", data.end_time or "",
          data.send_start_time or "", data.send_end_time or "",
          _fmt_days(data.send_days), data.batch_size or 500, campaign_dir,
          use_custom_directory, data.sms_template_id, data.smpp_config_id,
          data.phone_prefix or "")
    )
    conn.commit()
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    campaign = _enrich(dict(row))

    # If no campaign_dir provided, auto-compute
    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        conn2 = get_connection()
        conn2.execute("UPDATE batch_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign["id"]))
        conn2.commit()
        conn2.close()
        campaign["campaign_dir"] = campaign_dir

    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    return campaign


def update(campaign_id: int, data: BatchCampaignUpdate) -> Optional[dict]:
    conn = get_connection()
    existing = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not existing:
        conn.close()
        return None
    existing_dict = dict(existing)
    if data.name is not None and data.name.strip() != (existing_dict.get("name") or "").strip():
        data.name = campaign_name_service.ensure_unique_name(data.name, "batch", campaign_id)
    if data.use_custom_directory is not None and existing_dict.get("customer_list_path"):
        if data.use_custom_directory:
            new_dir = (data.campaign_dir or "").strip()
        else:
            new_dir = _ensure_campaign_dir(existing_dict)
        old_dir = (existing_dict.get("campaign_dir") or "").strip()
        if new_dir and os.path.normcase(os.path.normpath(new_dir)) != os.path.normcase(os.path.normpath(old_dir)):
            conn.close()
            raise ValueError("Cannot change campaign directory after a target list has been uploaded")
    if data.status == "in_progress":
        validation_errors = operations_service.validate_campaign("batch", {**existing_dict, "id": campaign_id})
        if validation_errors:
            conn.close()
            raise ValueError("; ".join(validation_errors))
    if data.status == "in_progress":
        if not existing_dict.get("row_count", 0):
            conn.close()
            raise ValueError("请先上传潜客清单")
        if not existing_dict.get("sms_template_id"):
            conn.close()
            raise ValueError("请先配置短信模板")
    if existing_dict.get("status") == "ended":
        only_allowed = all(getattr(data, f, None) is None for f in ["start_time", "batch_size", "campaign_dir", "use_custom_directory"])
        if not only_allowed:
            conn.close()
            raise ValueError("Ended campaign can only edit name, end time, send window, send days, and SMS template")
    elif existing_dict.get("status") != "not_started":
        only_allowed = all(getattr(data, f, None) is None for f in ["name", "start_time", "end_time", "send_start_time", "send_end_time", "send_days", "batch_size", "campaign_dir", "use_custom_directory"])
        if not only_allowed:
            conn.close()
            raise ValueError("Campaign has already started — cannot edit")
    fields = {}
    for field in ["name", "start_time", "end_time", "status", "send_start_time", "send_end_time", "batch_size", "campaign_dir", "use_custom_directory", "sms_template_id", "smpp_config_id", "phone_prefix"]:
        val = getattr(data, field, None)
        if val is not None:
            fields[field] = val
    if data.send_days is not None:
        fields["send_days"] = _fmt_days(data.send_days)
    new_smpp_id = fields.get("smpp_config_id")
    if new_smpp_id is not None:
        smpp = conn.execute(
            "SELECT id FROM smpp_configs WHERE id=? AND enabled=1", (new_smpp_id,)
        ).fetchone()
        if not smpp:
            conn.close()
            raise ValueError("Associated SMPP configuration is missing or disabled")
    if data.use_custom_directory is not None:
        if data.use_custom_directory:
            campaign_dir = (data.campaign_dir or "").strip()
            if not campaign_dir:
                conn.close()
                raise ValueError("Custom campaign directory is required")
            fields["campaign_dir"] = campaign_dir
        else:
            fields["campaign_dir"] = _ensure_campaign_dir({**existing_dict, **fields})

    if fields:
        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE batch_campaigns SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
            (*fields.values(), campaign_id)
        )
        conn.commit()
    if new_smpp_id is not None:
        # Re-route pending SMS tasks of this campaign to the newly associated SMSC
        conn.execute(
            """UPDATE sms_tasks SET smpp_config_id=?, updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type='batch' AND campaign_id=? AND status IN ('queued','retrying','sending')""",
            (new_smpp_id, campaign_id),
        )
        conn.commit()
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    updated_campaign = _enrich(dict(row))
    if data.use_custom_directory is not None:
        os.makedirs(os.path.join(updated_campaign["campaign_dir"], "whitelist"), exist_ok=True)
        os.makedirs(os.path.join(updated_campaign["campaign_dir"], "output"), exist_ok=True)

    # Auto: generate batches on start (no auto-send)
    if data.status == "in_progress":
        batch_size = existing_dict.get("batch_size") or 500
        generate_batches(campaign_id, batch_size)

    return updated_campaign


def get_prospects(campaign_id: int, search: str = "", field: str = "", value: str = "", page: int = 1, page_size: int = 50) -> dict:
    campaign = get_one(campaign_id)
    if not campaign or not campaign.get("customer_list_path"):
        return {"headers": [], "rows": [], "total": 0, "page": 1, "page_size": page_size}

    path = campaign["customer_list_path"]
    if not os.path.exists(path):
        return {"headers": [], "rows": [], "total": 0, "page": 1, "page_size": page_size}

    try:
        sep = _detect_csv_delimiter(path)
        df = pd.read_csv(path, dtype=str, sep=sep).fillna("")
    except Exception:
        return {"headers": [], "rows": [], "total": 0, "page": 1, "page_size": page_size}

    # Apply filters
    if search:
        mask = df.apply(lambda row: row.astype(str).str.contains(search, case=False, na=False).any(), axis=1)
        df = df[mask]

    if field and value:
        if field in df.columns:
            df = df[df[field].astype(str).str.contains(value, case=False, na=False)]

    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    page_data = df.iloc[start:end].to_dict(orient="records")

    return {
        "headers": list(df.columns),
        "rows": page_data,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def delete(campaign_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if row:
        campaign_dir = dict(row).get("campaign_dir", "")
        if campaign_dir and os.path.isdir(campaign_dir):
            try:
                shutil.rmtree(campaign_dir)
            except Exception:
                pass
    conn.execute("DELETE FROM batch_campaign_daily_stats WHERE campaign_id=?", (campaign_id,))
    conn.execute("DELETE FROM batch_campaign_batches WHERE campaign_id=?", (campaign_id,))
    conn.execute("DELETE FROM batch_campaigns WHERE id=?", (campaign_id,))
    conn.commit()
    conn.close()
    # DuckDB does not provide a reliable DB-API rowcount for DELETE and may
    # return -1 even when the row was removed. The lookup above is the source
    # of truth for whether this campaign existed.
    return row is not None


def copy(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        conn.close()
        return None
    orig = dict(row)
    new_name = orig["name"] + " (Copy)"
    cursor = conn.execute(
        "INSERT INTO batch_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, batch_size, sms_template_id, smpp_config_id, phone_prefix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (new_name, orig["start_time"], orig["end_time"],
         orig.get("send_start_time", ""), orig.get("send_end_time", ""),
         orig.get("send_days", "[]"), orig.get("batch_size", 500),
         orig.get("sms_template_id"), orig.get("smpp_config_id"), orig.get("phone_prefix", ""))
    )
    conn.commit()
    new_row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    new_campaign = _enrich(dict(new_row))

    # Create directory and copy whitelist
    campaign_dir = _ensure_campaign_dir(new_campaign)
    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    old_dir = orig.get("campaign_dir") or _ensure_campaign_dir(orig)
    old_whitelist = os.path.join(old_dir, "whitelist")
    if os.path.isdir(old_whitelist):
        for fname in os.listdir(old_whitelist):
            src = os.path.join(old_whitelist, fname)
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(campaign_dir, "whitelist", fname))

    conn2 = get_connection()
    conn2.execute("UPDATE batch_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, new_campaign["id"]))
    conn2.commit()
    conn2.close()
    new_campaign["campaign_dir"] = campaign_dir
    return new_campaign


def get_daily_stats(campaign_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM batch_campaign_daily_stats WHERE campaign_id=? ORDER BY stats_date DESC",
        (campaign_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reset_batches_for_today(campaign_id: int) -> bool:
    """Mark all batches pending again for the current day (daily-repeat send).

    Only resets when this campaign has not enqueued anything today, so each
    eligible day produces exactly one batch run. Queued/retrying tasks left over
    from previous days are expired first so they cannot send late and duplicate
    the new day's batch.
    """
    conn = get_connection()
    today = datetime.now().strftime("%Y-%m-%d")
    existing = conn.execute(
        "SELECT 1 FROM batch_campaign_daily_stats WHERE campaign_id=? AND stats_date=? LIMIT 1",
        (campaign_id, today),
    ).fetchone()
    if existing:
        conn.close()
        return False
    conn.execute(
        """UPDATE sms_tasks SET status='expired', error_msg='superseded by next daily batch',
           updated_at=CURRENT_TIMESTAMP
           WHERE campaign_type='batch' AND campaign_id=? AND status IN ('queued','retrying')
             AND CAST(created_at AS DATE) < ?""",
        (campaign_id, today),
    )
    count = conn.execute(
        """SELECT COUNT(*) cnt FROM batch_campaign_batches
           WHERE campaign_id=? AND status IN ('queued','sent','skipped','failed')""",
        (campaign_id,),
    ).fetchone()["cnt"]
    if not count:
        conn.close()
        return False
    conn.execute(
        "UPDATE batch_campaign_batches SET status='pending', updated_at=CURRENT_TIMESTAMP WHERE campaign_id=?",
        (campaign_id,),
    )
    conn.commit()
    conn.close()
    return True


def reset_daily_batches() -> int:
    """Reset batches of in-progress batch campaigns that have not run yet today."""
    conn = get_connection()
    today = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT c.id FROM batch_campaigns c
           WHERE c.status='in_progress' AND c.customer_list_path <> ''
             AND EXISTS (SELECT 1 FROM batch_campaign_batches b WHERE b.campaign_id=c.id)
             AND NOT EXISTS (SELECT 1 FROM batch_campaign_daily_stats d
                             WHERE d.campaign_id=c.id AND d.stats_date=?)""",
        (today,),
    ).fetchall()
    conn.close()
    reset = 0
    for row in rows:
        if reset_batches_for_today(row["id"]):
            reset += 1
    if reset:
        logger.info("Batch daily reset prepared campaigns=%s", reset)
    return reset


def cancel_overdue_daily_tasks() -> int:
    """Cancel the day's still-unfinished batch tasks once the daily send window closes.

    Tasks already picked up by a worker ('sending') and submitted ones are left alone:
    they are already in flight at the SMSC. The next eligible day's batch run will
    re-send the same users.
    """
    conn = get_connection()
    now = datetime.now()
    current = now.strftime("%H:%M")
    rows = conn.execute(
        "SELECT id, send_start_time, send_end_time FROM batch_campaigns WHERE status='in_progress'"
    ).fetchall()
    cancelled = 0
    for row in rows:
        start = (row["send_start_time"] or "")[:5]
        end = (row["send_end_time"] or "")[:5]
        if start and end:
            if start <= end:
                window_ended = current > end
            else:
                # Window crosses midnight: active while current >= start or current <= end
                window_ended = current > end and current < start
        elif end:
            window_ended = current > end
        else:
            continue
        if not window_ended:
            continue
        pending = conn.execute(
            """SELECT COUNT(*) cnt FROM sms_tasks
               WHERE campaign_type='batch' AND campaign_id=? AND status IN ('queued','retrying')""",
            (row["id"],),
        ).fetchone()["cnt"]
        if not pending:
            continue
        conn.execute(
            """UPDATE sms_tasks SET status='cancelled', error_msg='daily send window ended',
               updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type='batch' AND campaign_id=? AND status IN ('queued','retrying')""",
            (row["id"],),
        )
        cancelled += pending
    conn.commit()
    conn.close()
    if cancelled:
        logger.info("Batch overdue daily tasks cancelled=%s", cancelled)
    return cancelled


def generate_batches(campaign_id: int, batch_size: int = 500) -> dict:
    """Split customer list file into batches."""
    campaign = get_one(campaign_id)
    if not campaign:
        return {"success": False, "message": "Campaign not found"}
    path = campaign.get("customer_list_path", "")
    if not path or not os.path.isfile(path):
        return {"success": False, "message": "No customer list uploaded"}
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path, dtype=str, sep=_detect_csv_delimiter(path))
        else:
            df = pd.read_excel(path, dtype=str)
    except Exception as e:
        return {"success": False, "message": f"Cannot read file: {e}"}
    total = len(df)
    conn = get_connection()
    conn.execute("DELETE FROM batch_campaign_batches WHERE campaign_id=?", (campaign_id,))
    created = []
    for i in range(0, total, batch_size):
        end = min(i + batch_size, total)
        cursor = conn.execute(
            "INSERT INTO batch_campaign_batches (campaign_id, batch_index, start_row, end_row, total_count) VALUES (?,?,?,?,?)",
            (campaign_id, len(created), i, end, end - i)
        )
        created.append({
            "id": cursor.lastrowid, "batch_index": len(created) - 1,
            "start_row": i, "end_row": end, "count": end - i, "status": "pending"
        })
    conn.commit()
    conn.close()
    return {"success": True, "batches": created, "total_batches": len(created), "total_rows": total}


def get_batches(campaign_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM batch_campaign_batches WHERE campaign_id=? ORDER BY batch_index",
        (campaign_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_today_sent_msisdns(campaign_dir: str) -> set:
    """Read today's output file and return set of already-sent msisdns."""
    today_str = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(campaign_dir, "output", f"{today_str}.csv")
    if not os.path.isfile(output_path):
        return set()
    try:
        df = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig")
        if "msisdn" in df.columns:
            return set(df["msisdn"].dropna().str.strip())
    except Exception:
        pass
    return set()


def process_batch(batch_id: int) -> dict:
    conn = get_connection()
    row = conn.execute("SELECT * FROM batch_campaign_batches WHERE id=?", (batch_id,)).fetchone()
    if not row:
        conn.close()
        return {"success": False, "message": "Batch not found"}
    batch = dict(row)
    campaign_id = batch["campaign_id"]

    if batch["status"] not in {"pending", "failed"}:
        conn.close()
        return {"success": False, "message": "Batch is already processing or queued"}

    conn.execute(
        "UPDATE batch_campaign_batches SET status='processing', updated_at=datetime('now','localtime') WHERE id=?",
        (batch_id,)
    )
    conn.commit()
    try:
        campaign = get_one(campaign_id)
        path = campaign.get("customer_list_path", "") if campaign else ""
        if not path or not os.path.isfile(path):
            raise Exception("Customer list file not found")
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            # Read only this batch's row slice instead of re-reading the whole file,
            # so per-batch cost is O(batch_size) rather than O(total rows).
            start = int(batch["start_row"] or 0)
            end = int(batch["end_row"] or start)
            df = pd.read_csv(
                path, dtype=str, sep=_detect_csv_delimiter(path),
                skiprows=range(1, start + 1), nrows=max(0, end - start), header=0,
            )
        else:
            df = pd.read_excel(path, dtype=str)
        batch_rows = df.iloc[batch["start_row"]:batch["end_row"]]

        msisdn_col = "msisdn" if "msisdn" in batch_rows.columns else batch_rows.columns[0]
        if msisdn_col != "msisdn":
            batch_rows = batch_rows.rename(columns={msisdn_col: "msisdn"})
        enqueue_result = sms_task_service.enqueue_rows(
            "batch", campaign, batch_rows.fillna("").to_dict("records"), path, batch_id
        )
        if not enqueue_result.get("success"):
            raise Exception(enqueue_result.get("message", "Unable to enqueue batch"))
        queued = enqueue_result["created"]
        batch_status = "queued" if queued > 0 else "skipped"
        conn.execute(
            "UPDATE batch_campaign_batches SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
            (batch_status, batch_id)
        )

        # Update daily stats
        stats_date = datetime.now().strftime("%Y-%m-%d")
        total_prospects = campaign.get("row_count", 0) if campaign else 0
        existing_stats = conn.execute(
            "SELECT id, sent_count FROM batch_campaign_daily_stats WHERE campaign_id=? AND stats_date=?",
            (campaign_id, stats_date)
        ).fetchone()
        if existing_stats:
            conn.execute(
                "UPDATE batch_campaign_daily_stats SET updated_at=datetime('now','localtime') WHERE id=?",
                (existing_stats["id"],)
            )
        else:
            conn.execute(
                "INSERT INTO batch_campaign_daily_stats (campaign_id, stats_date, total_prospects, sent_count) VALUES (?, ?, ?, ?)",
                (campaign_id, stats_date, total_prospects, 0)
            )
        conn.commit()
        conn.close()
        return {"success": True, "message": f"Queued {queued} messages",
                "queued_count": queued, "filter_result": enqueue_result}
    except Exception as e:
        conn.execute(
            "UPDATE batch_campaign_batches SET status='failed', updated_at=datetime('now','localtime') WHERE id=?",
            (batch_id,)
        )
        conn.commit()
        conn.close()
        return {"success": False, "message": str(e)}


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
    campaign["daily_stats"] = get_daily_stats(campaign_id)
    _attach_daily_smpp(campaign_id, campaign["daily_stats"])
    campaign["batches"] = get_batches(campaign_id)
    conn = get_connection()
    reached = conn.execute(
        """SELECT COUNT(DISTINCT msisdn) cnt FROM sms_tasks
           WHERE campaign_type='batch' AND campaign_id=? AND status='delivered'""",
        (campaign_id,),
    ).fetchone()
    conn.close()
    campaign["reached_users"] = reached["cnt"] if reached else 0
    campaign["sms_templates"] = _get_campaign_templates(campaign_id)
    return campaign


def _attach_daily_smpp(campaign_id: int, daily_stats: list) -> None:
    """Attach the SMSC label(s) used per stats date, derived from sms_tasks."""
    if not daily_stats:
        return
    conn = get_connection()
    rows = conn.execute(
        """SELECT CAST(created_at AS DATE) AS d, smpp_config_id
           FROM sms_tasks
           WHERE campaign_type='batch' AND campaign_id=?
           GROUP BY d, smpp_config_id""",
        (campaign_id,),
    ).fetchall()
    configs = {
        r["id"]: r
        for r in conn.execute("SELECT id, name, host, port FROM smpp_configs").fetchall()
    }
    conn.close()

    def label(config_id):
        if config_id is None:
            return "默认"
        cfg = configs.get(config_id)
        if cfg:
            return f"{cfg['name']} ({cfg['host']}:{cfg['port']})"
        return f"#{config_id}"

    by_date = {}
    for r in rows:
        by_date.setdefault(str(r["d"]), []).append(r["smpp_config_id"])
    for item in daily_stats:
        ids = by_date.get(item["stats_date"], [])
        item["smpp_label"] = "、".join(dict.fromkeys(label(i) for i in ids)) or "-"


def upload_customer_list(campaign_id: int, file_path: str, original_filename: str) -> Optional[dict]:
    campaign = get_one(campaign_id)
    if not campaign:
        return None

    campaign_dir = campaign.get("campaign_dir", "")
    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
        os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
        conn = get_connection()
        conn.execute("UPDATE batch_campaigns SET campaign_dir=? WHERE id=?", (campaign_dir, campaign_id))
        conn.commit()
        conn.close()

    whitelist_dir = os.path.join(campaign_dir, "whitelist")
    os.makedirs(whitelist_dir, exist_ok=True)
    ext = os.path.splitext(original_filename)[1].lower()

    try:
        sep = _detect_csv_delimiter(file_path)
        df = pd.read_csv(file_path, dtype=str, sep=sep)
        row_count = _validate_customer_list(df)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Cannot read customer list file: {exc}") from exc

    dest = os.path.join(whitelist_dir, f"whitelist{ext}")
    shutil.move(file_path, dest)

    conn = get_connection()
    conn.execute(
        "UPDATE batch_campaigns SET customer_list_filename=?, customer_list_path=?, row_count=?, updated_at=datetime('now','localtime') WHERE id=?",
        (original_filename, dest, row_count, campaign_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()

    # If campaign is in progress, regenerate batches from new list (no auto-send)
    if campaign.get("status") == "in_progress" and row_count > 0:
        batch_size = campaign.get("batch_size") or 500
        generate_batches(campaign_id, batch_size)

    return dict(row) if row else None
