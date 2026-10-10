import os
import json
import math
import re
import logging
from datetime import datetime
from database import get_connection, read_transaction, write_transaction
from models.batch_campaign import BatchCampaignCreate, BatchCampaignUpdate
from typing import Optional
import shutil
import pandas as pd
import time
from services import settings_service
from services import (sms_task_service, operations_service, campaign_name_service,
                      ingestion_job_service, backpressure_service)

logger = logging.getLogger(__name__)
OVERDUE_CANCEL_BATCH_SIZE = max(
    50, min(2000, int(os.environ.get("MC_BATCH_CANCEL_BATCH_SIZE", "250")))
)
OVERDUE_CANCEL_CAMPAIGNS_PER_RUN = max(
    1, min(32, int(os.environ.get("MC_BATCH_CANCEL_CAMPAIGNS_PER_RUN", "4")))
)
_overdue_campaign_cursor = 0

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploads", "campaigns")
ALLOWED_LANGUAGES = {"amharic", "tigrigna", "somali", "english", "oromign", "en"}
INGEST_QUEUE_HIGH_WATERMARK = backpressure_service.TASK_HIGH
INGEST_QUEUE_LOW_WATERMARK = backpressure_service.TASK_LOW


def _run_campaign_write(operation: str, writer):
    return write_transaction(
        writer,
        attempts=3,
        operation=f"batch_campaign.{operation}",
        base_delay=0.1,
    )


def _queued_task_count() -> int:
    return backpressure_service._task_depth()


def _wait_for_ingest_capacity(stop_event=None) -> bool:
    """Pause on task, buffered-write or disk pressure with low-water recovery."""
    queued = _queued_task_count()
    if queued >= INGEST_QUEUE_HIGH_WATERMARK:
        while queued > INGEST_QUEUE_LOW_WATERMARK:
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(0.25)
            queued = _queued_task_count()
    return backpressure_service.wait_for_ingestion(stop_event=stop_event)


def _detect_csv_delimiter(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
        if "|" in first_line:
            return "|"
        return ","
    except Exception:
        return ","


def _canonicalize_customer_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the reserved customer-list headers without changing order.

    ``msisdn`` and ``language`` are logical field names, not case-sensitive
    source headers.  Keep other columns intact because they may be used as
    template variables, but always expose the two reserved fields under their
    canonical lower-case names to the validation and enqueue paths.
    """
    rename = {}
    reserved = {}
    for column in df.columns:
        key = str(column).lstrip("\ufeff").strip().casefold()
        if key not in {"msisdn", "language"}:
            continue
        if key in reserved:
            raise ValueError(
                f"Customer list file contains duplicate {key} columns"
            )
        reserved[key] = column
        rename[column] = key
    return df.rename(columns=rename)


def _read_excel_chunks(path: str, chunksize: int):
    """Stream an XLSX workbook in read-only mode and always close its resources."""
    from openpyxl import load_workbook

    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        rows = sheet.iter_rows(values_only=True)
        header_row = next(rows, None)
        if not header_row:
            return
        headers = [str(value or "").strip() for value in header_row]
        buffer = []
        for values in rows:
            buffer.append(dict(zip(headers, values)))
            if len(buffer) >= chunksize:
                yield pd.DataFrame(buffer, columns=headers)
                buffer.clear()
        if buffer:
            yield pd.DataFrame(buffer, columns=headers)
    finally:
        workbook.close()


def _validate_customer_list_file(path: str, chunksize: int = 10000) -> int:
    ext = os.path.splitext(path)[1].lower()
    source = (
        pd.read_csv(path, dtype=str, sep=_detect_csv_delimiter(path), chunksize=chunksize)
        if ext == ".csv" else _read_excel_chunks(path, chunksize)
    )
    total = 0
    for frame in source:
        total += _validate_customer_list(frame)
        del frame
    if total <= 0:
        raise ValueError("Customer list file is empty")
    return total


def _validate_customer_list(df: pd.DataFrame) -> int:
    df = _canonicalize_customer_list_columns(df)
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
    rows = read_transaction(
        lambda conn: conn.execute("""
            SELECT st.id, st.language, st.content, st.source_addr
            FROM sms_templates st
            JOIN campaigns c ON c.id = st.campaign_id
            WHERE c.marketing_campaign_id = ? AND c.marketing_type = 'batch'
            ORDER BY st.language
        """, (batch_campaign_id,)).fetchall(),
        operation="batch_campaign.templates.list",
    )
    return [dict(r) for r in rows]


def _enrich(c: dict) -> dict:
    c["send_days"] = _parse_days(c.get("send_days", ""))
    return c


def get_all() -> list:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM batch_campaigns ORDER BY created_at DESC"
        ).fetchall(),
        operation="batch_campaign.list",
    )
    return [_enrich(dict(r)) for r in rows]


def list_campaigns(page: int = 1, page_size: int = 50) -> dict:
    def read(conn):
        total = conn.execute("SELECT COUNT(*) cnt FROM batch_campaigns").fetchone()["cnt"]
        offset = max(0, page - 1) * page_size
        rows = conn.execute(
            "SELECT * FROM batch_campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (page_size, offset),
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="batch_campaign.list_page")
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_enrich(dict(r)) for r in rows],
    }


def get_one(campaign_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone(),
        operation="batch_campaign.get",
    )
    return _enrich(dict(row)) if row else None


def create(data: BatchCampaignCreate) -> dict:
    name = campaign_name_service.ensure_unique_name(data.name)
    if not (data.phone_prefix or "").strip():
        raise ValueError("请输入号码前缀")
    use_custom_directory = bool(data.use_custom_directory and (data.campaign_dir or "").strip())
    campaign_dir = (data.campaign_dir or "").strip() if use_custom_directory else ""

    def write(conn):
        cursor = conn.execute(
            "INSERT INTO batch_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, batch_size, campaign_dir, use_custom_directory, sms_template_id, smpp_config_id, phone_prefix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, data.start_time or "", data.end_time or "",
             data.send_start_time or "", data.send_end_time or "",
             _fmt_days(data.send_days), data.batch_size or 500, campaign_dir,
             use_custom_directory, data.sms_template_id, data.smpp_config_id,
             data.phone_prefix or ""),
        )
        return dict(conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (cursor.lastrowid,)
        ).fetchone())

    campaign = _enrich(_run_campaign_write("create", write))
    if not campaign_dir:
        campaign_dir = _ensure_campaign_dir(campaign)
        _run_campaign_write(
            "create.directory",
            lambda conn: conn.execute(
                "UPDATE batch_campaigns SET campaign_dir=? WHERE id=?",
                (campaign_dir, campaign["id"]),
            ),
        )
        campaign["campaign_dir"] = campaign_dir
    os.makedirs(os.path.join(campaign_dir, "whitelist"), exist_ok=True)
    os.makedirs(os.path.join(campaign_dir, "output"), exist_ok=True)
    return campaign


def update(campaign_id: int, data: BatchCampaignUpdate) -> Optional[dict]:
    probe = get_connection()
    try:
        existing = probe.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
    finally:
        probe.close()
    if not existing:
        return None

    existing_dict = dict(existing)
    if data.name is not None and data.name.strip() != (existing_dict.get("name") or "").strip():
        data.name = campaign_name_service.ensure_unique_name(data.name, "batch", campaign_id)
    auto_campaign_dir = ""
    if data.status == "in_progress":
        validation_campaign = dict(existing_dict)
        if not (validation_campaign.get("campaign_dir") or "").strip() and not bool(validation_campaign.get("use_custom_directory")):
            auto_campaign_dir = _ensure_campaign_dir(validation_campaign)
            validation_campaign["campaign_dir"] = auto_campaign_dir
        validation_errors = operations_service.validate_campaign(
            "batch", {**validation_campaign, "id": campaign_id}
        )
        if validation_errors:
            raise ValueError("; ".join(validation_errors))
        if not existing_dict.get("row_count", 0):
            raise ValueError("请先上传潜客清单")
        if not existing_dict.get("sms_template_id"):
            raise ValueError("请先配置短信模板")

    if data.use_custom_directory is not None and existing_dict.get("customer_list_path"):
        new_dir = ((data.campaign_dir or "").strip()
                   if data.use_custom_directory
                   else _ensure_campaign_dir(existing_dict))
        old_dir = (existing_dict.get("campaign_dir") or "").strip()
        if new_dir and os.path.normcase(os.path.normpath(new_dir)) != os.path.normcase(os.path.normpath(old_dir)):
            raise ValueError("Cannot change campaign directory after a target list has been uploaded")
    if existing_dict.get("status") == "ended":
        only_allowed = all(
            getattr(data, f, None) is None
            for f in ["start_time", "batch_size", "campaign_dir", "use_custom_directory"]
        )
        if not only_allowed:
            raise ValueError("Ended campaign can only edit name, end time, send window, send days, and SMS template")
    elif existing_dict.get("status") != "not_started":
        only_allowed = all(
            getattr(data, f, None) is None
            for f in ["name", "start_time", "end_time", "send_start_time", "send_end_time", "send_days", "batch_size", "campaign_dir", "use_custom_directory"]
        )
        if not only_allowed:
            raise ValueError("Campaign has already started - cannot edit")

    fields = {}
    for field in ["name", "start_time", "end_time", "status", "send_start_time", "send_end_time", "batch_size", "campaign_dir", "use_custom_directory", "sms_template_id", "smpp_config_id", "phone_prefix"]:
        value = getattr(data, field, None)
        if value is not None:
            fields[field] = value
    if data.send_days is not None:
        fields["send_days"] = _fmt_days(data.send_days)
    if auto_campaign_dir:
        fields["campaign_dir"] = auto_campaign_dir
    if data.use_custom_directory is not None:
        if data.use_custom_directory:
            campaign_dir = (data.campaign_dir or "").strip()
            if not campaign_dir:
                raise ValueError("Custom campaign directory is required")
            fields["campaign_dir"] = campaign_dir
        else:
            fields["campaign_dir"] = _ensure_campaign_dir({**existing_dict, **fields})
    new_smpp_id = fields.get("smpp_config_id")

    def write(conn):
        current = conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
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
                f"UPDATE batch_campaigns SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
                (*fields.values(), campaign_id),
            )
        if new_smpp_id is not None:
            conn.execute(
                """UPDATE sms_tasks SET smpp_config_id=?, updated_at=CURRENT_TIMESTAMP
                   WHERE campaign_type='batch' AND campaign_id=? AND status IN ('queued','retrying','sending')""",
                (new_smpp_id, campaign_id),
            )
        row = conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        return dict(row) if row else None

    updated_campaign = _run_campaign_write("update", write)
    if updated_campaign is None:
        return None
    sms_task_service.invalidate_campaign_cache("batch", campaign_id)
    updated_campaign = _enrich(updated_campaign)
    if data.use_custom_directory is not None:
        os.makedirs(os.path.join(updated_campaign["campaign_dir"], "whitelist"), exist_ok=True)
        os.makedirs(os.path.join(updated_campaign["campaign_dir"], "output"), exist_ok=True)
    if data.status == "in_progress":
        generate_batches(campaign_id, updated_campaign.get("batch_size") or 500)
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
    probe = get_connection()
    try:
        row = probe.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
    finally:
        probe.close()
    if not row:
        return False

    _run_campaign_write(
        "delete",
        lambda conn: (
            conn.execute("DELETE FROM batch_campaign_daily_stats WHERE campaign_id=?", (campaign_id,)),
            conn.execute("DELETE FROM batch_campaign_batches WHERE campaign_id=?", (campaign_id,)),
            conn.execute("DELETE FROM batch_campaigns WHERE id=?", (campaign_id,)),
        ),
    )
    campaign_dir = dict(row).get("campaign_dir", "")
    if campaign_dir and os.path.isdir(campaign_dir):
        try:
            shutil.rmtree(campaign_dir)
        except Exception:
            logger.warning("Unable to remove deleted batch campaign directory=%s", campaign_dir, exc_info=True)
    sms_task_service.invalidate_campaign_cache("batch", campaign_id)
    return True


def copy(campaign_id: int) -> Optional[dict]:
    def write(conn):
        row = conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()
        if not row:
            return None
        orig = dict(row)
        cursor = conn.execute(
            "INSERT INTO batch_campaigns (name, start_time, end_time, send_start_time, send_end_time, send_days, batch_size, sms_template_id, smpp_config_id, phone_prefix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (orig["name"] + " (Copy)", orig["start_time"], orig["end_time"],
             orig.get("send_start_time", ""), orig.get("send_end_time", ""),
             orig.get("send_days", "[]"), orig.get("batch_size", 500),
             orig.get("sms_template_id"), orig.get("smpp_config_id"),
             orig.get("phone_prefix", "")),
        )
        new_row = conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (cursor.lastrowid,)
        ).fetchone()
        return {"original": orig, "campaign": dict(new_row)}

    result = _run_campaign_write("copy", write)
    if result is None:
        return None
    orig = result["original"]
    new_campaign = _enrich(result["campaign"])
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
    _run_campaign_write(
        "copy.directory",
        lambda conn: conn.execute(
            "UPDATE batch_campaigns SET campaign_dir=? WHERE id=?",
            (campaign_dir, new_campaign["id"]),
        ),
    )
    new_campaign["campaign_dir"] = campaign_dir
    return new_campaign


def get_daily_stats(campaign_id: int) -> list:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM batch_campaign_daily_stats WHERE campaign_id=? ORDER BY stats_date DESC",
            (campaign_id,)
        ).fetchall(),
        operation="batch_campaign.daily_stats",
    )
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
        """UPDATE batch_campaign_batches
           SET status='pending',task_count=0,delivered_count=0,failed_count=0,
               updated_at=CURRENT_TIMESTAMP WHERE campaign_id=?""",
        (campaign_id,),
    )
    conn.execute(
        """UPDATE ingestion_chunks SET status='pending',lease_token='',lease_expires_at=NULL,
           created_tasks=0,duplicate_rows=0,excluded_rows=0,invalid_rows=0,no_template_rows=0,
           error_detail='',completed_at=NULL,updated_at=CURRENT_TIMESTAMP
           WHERE source_batch_id IN
             (SELECT id FROM batch_campaign_batches WHERE campaign_id=?)""",
        (campaign_id,),
    )
    conn.execute(
        """UPDATE ingestion_files SET status='pending',completed_chunks=0,created_tasks=0,
           error_detail='',completed_at=NULL,updated_at=CURRENT_TIMESTAMP
           WHERE campaign_type='batch' AND campaign_id=?""",
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
    now = datetime.now()
    current = now.strftime("%H:%M")
    global _overdue_campaign_cursor

    def load_candidates(conn, after_id):
        return conn.execute(
            """SELECT c.id, c.send_start_time, c.send_end_time,
                      EXISTS(
                          SELECT 1 FROM sms_tasks t
                          WHERE t.campaign_type='batch' AND t.campaign_id=c.id
                            AND t.status IN ('queued','retrying')
                      ) AS has_pending
               FROM batch_campaigns c
               WHERE c.status='in_progress' AND c.id>?
               ORDER BY c.id LIMIT ?""",
            (after_id, OVERDUE_CANCEL_CAMPAIGNS_PER_RUN),
        ).fetchall()

    rows = read_transaction(
        lambda conn: load_candidates(conn, _overdue_campaign_cursor),
        operation="batch_campaign.overdue_candidates",
    )
    if not rows and _overdue_campaign_cursor:
        _overdue_campaign_cursor = 0
        rows = read_transaction(
            lambda conn: load_candidates(conn, 0),
            operation="batch_campaign.overdue_candidates",
        )
    cancelled = 0
    for row in rows:
        _overdue_campaign_cursor = int(row["id"])
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
        if not row["has_pending"]:
            continue

        def cancel(conn, campaign_id=row["id"]):
            task_ids = conn.execute(
                """SELECT id FROM sms_tasks
                   WHERE campaign_type='batch' AND campaign_id=?
                     AND status IN ('queued','retrying')
                   ORDER BY id LIMIT ?""",
                (campaign_id, OVERDUE_CANCEL_BATCH_SIZE),
            ).fetchall()
            ids = [int(task["id"]) for task in task_ids]
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            sql = (
                "UPDATE sms_tasks SET status='cancelled', "
                "error_msg='daily send window ended', updated_at=CURRENT_TIMESTAMP "
                "WHERE campaign_type='batch' AND campaign_id=? "
                "AND status IN ('queued','retrying') AND id IN ("
                + placeholders + ") RETURNING id"
            )
            return conn.execute(
                sql,
                [campaign_id, *ids],
            ).fetchall()

        # One campaign per short transaction prevents a large campaign's
        # overdue cleanup from holding the writer gate while other campaigns
        # and the SMS result writer wait behind it.
        changed = write_transaction(
            cancel,
            attempts=3,
            operation="batch_campaign.cancel_overdue",
            base_delay=0.1,
        )
        cancelled += len(changed or [])
    if cancelled:
        logger.info("Batch overdue daily tasks cancelled=%s", cancelled)
    return cancelled


def generate_batches(campaign_id: int, batch_size: int = 500) -> dict:
    """Split customer list file into batches using one sequential CSV iterator."""
    campaign = get_one(campaign_id)
    pressure, _ = backpressure_service.reasons(producing_chunks=True)
    if pressure:
        return {"success": False, "message": "ingestion backpressure: " + ",".join(pressure)}
    if not campaign:
        return {"success": False, "message": "Campaign not found"}
    path = campaign.get("customer_list_path", "")
    if not path or not os.path.isfile(path):
        return {"success": False, "message": "No customer list uploaded"}
    ext = os.path.splitext(path)[1].lower()
    try:
        source = (
            pd.read_csv(path, dtype=str, sep=_detect_csv_delimiter(path),
                        chunksize=max(1, int(batch_size or 500)))
            if ext == ".csv"
            else _read_excel_chunks(path, max(1, int(batch_size or 500)))
        )
        total = 0
    except Exception as e:
        return {"success": False, "message": f"Cannot read file: {e}"}
    def write(conn):
        source = (
            pd.read_csv(path, dtype=str, sep=_detect_csv_delimiter(path),
                        chunksize=max(1, int(batch_size or 500)))
            if ext == ".csv"
            else _read_excel_chunks(path, max(1, int(batch_size or 500)))
        )
        local_created = []
        local_total = 0
        conn.execute("DELETE FROM batch_campaign_batches WHERE campaign_id=?", (campaign_id,))
        for chunk in source:
            count = len(chunk)
            start = local_total
            local_total += count
            end = local_total
            cursor = conn.execute(
                "INSERT INTO batch_campaign_batches (campaign_id, batch_index, start_row, end_row, total_count) VALUES (?,?,?,?,?)",
                (campaign_id, len(local_created), start, end, count),
            )
            local_created.append({
                "id": cursor.lastrowid, "batch_index": len(local_created),
                "start_row": start, "end_row": end, "count": count, "status": "pending"
            })
            del chunk
        return local_created, local_total

    created, total = _run_campaign_write("generate_batches", write)
    manifest_chunks = [
        {"source_batch_id": item["id"], "chunk_index": item["batch_index"],
         "start_row": item["start_row"], "end_row": item["end_row"]}
        for item in created
    ]
    ingestion_job_service.register_file(
        "batch", campaign_id,
        campaign.get("customer_list_filename") or os.path.basename(path), path,
        total_rows=total, chunk_size=max(1, int(batch_size or 500)),
        chunks=manifest_chunks, reset=True,
    )
    return {"success": True, "batches": created, "total_batches": len(created), "total_rows": total}


def get_batches(campaign_id: int) -> list:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM batch_campaign_batches WHERE campaign_id=? ORDER BY batch_index",
            (campaign_id,)
        ).fetchall(),
        operation="batch_campaign.batches",
    )
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


def process_batch(batch_id: int, batch_rows=None, campaign_override: Optional[dict] = None) -> dict:
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

    if batch_rows is None:
        campaign_probe = get_one(campaign_id)
        if campaign_probe and campaign_probe.get("customer_list_path"):
            conn.close()
            return process_pending_batches(campaign_id, batch_id)

    chunk_job = ingestion_job_service.claim_chunk(source_batch_id=batch_id)
    if chunk_job and chunk_job.get("status") == "completed":
        conn.close()
        return {"success": True, "message": "Chunk already completed", "queued_count": 0,
                "resumed": True}
    if chunk_job is None:
        mapped = conn.execute(
            "SELECT id FROM ingestion_chunks WHERE source_batch_id=?", (batch_id,)
        ).fetchone()
        if mapped:
            conn.close()
            return {"success": False, "message": "Chunk lease is held by another worker"}

    conn.execute(
        "UPDATE batch_campaign_batches SET status='processing', updated_at=datetime('now','localtime') WHERE id=?",
        (batch_id,)
    )
    conn.commit()
    try:
        campaign = campaign_override or get_one(campaign_id)
        path = campaign.get("customer_list_path", "") if campaign else ""
        if not path or not os.path.isfile(path):
            raise Exception("Customer list file not found")
        ext = os.path.splitext(path)[1].lower()
        if batch_rows is None:
            raise RuntimeError("batch rows must be supplied by the sequential ingestion reader")

        batch_rows = _canonicalize_customer_list_columns(batch_rows)
        if "msisdn" not in batch_rows.columns:
            raise ValueError("Customer list file must contain an msisdn column")
        enqueue_result = sms_task_service.enqueue_rows(
            "batch", campaign, batch_rows.fillna("").to_dict("records"),
            campaign.get("customer_list_filename") or os.path.basename(path), batch_id
        )
        if not enqueue_result.get("success"):
            raise Exception(enqueue_result.get("message", "Unable to enqueue batch"))
        queued = enqueue_result["created"]
        batch_status = "queued" if queued > 0 else "skipped"
        conn.execute(
            """UPDATE batch_campaign_batches
               SET status=?,task_count=?,delivered_count=0,failed_count=0,
                   updated_at=datetime('now','localtime') WHERE id=?""",
            (batch_status, queued, batch_id)
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
        if chunk_job:
            ingestion_job_service.complete_chunk_in_transaction(
                conn, chunk_job["id"], enqueue_result
            )
        conn.commit()
        conn.close()
        return {"success": True, "message": f"Queued {queued} messages",
                "queued_count": queued, "filter_result": enqueue_result}
    except Exception as e:
        if is_transient_write_error(e):
            try:
                conn.rollback()
            finally:
                conn.close()
            raise
        conn.execute(
            "UPDATE batch_campaign_batches SET status='failed', updated_at=datetime('now','localtime') WHERE id=?",
            (batch_id,)
        )
        if chunk_job:
            ingestion_job_service.fail_chunk_in_transaction(conn, chunk_job["id"], e)
        conn.commit()
        conn.close()
        return {"success": False, "message": str(e)}


def process_pending_batches(campaign_id: Optional[int] = None, target_batch_id: Optional[int] = None,
                            stop_event=None) -> dict:
    """Process one campaign's batches with a single sequential CSV read.

    Batch metadata remains addressable for the API, but the scheduler no longer
    opens the same CSV once per batch and seeks from row zero each time.
    """
    conn = get_connection()
    if campaign_id is None:
        row = conn.execute(
            """SELECT c.id FROM batch_campaigns c
               JOIN batch_campaign_batches b ON b.campaign_id=c.id
               WHERE c.status='in_progress' AND b.status='pending'
               ORDER BY b.created_at LIMIT 1"""
        ).fetchone()
        campaign_id = row["id"] if row else None
    if campaign_id is None:
        conn.close()
        return {"success": True, "processed": 0}
    campaign_row = conn.execute(
        "SELECT * FROM batch_campaigns WHERE id=? AND status='in_progress'", (campaign_id,)
    ).fetchone()
    batches = conn.execute(
        "SELECT * FROM batch_campaign_batches WHERE campaign_id=? ORDER BY batch_index", (campaign_id,)
    ).fetchall()
    conn.close()
    campaign = _enrich(dict(campaign_row)) if campaign_row else None
    if not campaign or not batches:
        return {"success": False, "message": "Campaign or batches not found", "processed": 0}
    active, reason = sms_task_service.campaign_is_active(campaign)
    if not active:
        return {"success": False, "message": reason, "processed": 0}
    path = campaign.get("customer_list_path", "")
    if not path or not os.path.isfile(path):
        return {"success": False, "message": "Customer list file not found", "processed": 0}

    processed = 0
    ext = os.path.splitext(path)[1].lower()
    try:
        source = (
            pd.read_csv(path, dtype=str, sep=_detect_csv_delimiter(path),
                        chunksize=max(1, int(campaign.get("batch_size") or 500)))
            if ext == ".csv" else
            _read_excel_chunks(path, max(1, int(campaign.get("batch_size") or 500)))
        )
        for batch, frame in zip(batches, source):
            if not _wait_for_ingest_capacity(stop_event):
                return {"success": False, "message": "batch ingestion worker stopping", "processed": processed}
            if target_batch_id is not None:
                if batch["id"] != target_batch_id:
                    continue
            elif batch["status"] != "pending":
                continue
            result = process_batch(batch["id"], batch_rows=frame, campaign_override=campaign)
            if result.get("success"):
                processed += 1
            del frame
    except Exception as exc:
        logger.exception("Sequential batch read failed campaign=%s", campaign_id)
        return {"success": False, "message": str(exc), "processed": processed}
    manifest = ingestion_job_service.get_file(
        "batch", campaign_id,
        campaign.get("customer_list_filename") or os.path.basename(path),
    )
    if manifest:
        ingestion_job_service.finish_file(manifest["id"])
    return {"success": True, "processed": processed}


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
           WHERE campaign_type='batch' AND campaign_id=? AND status IN ('delivered','accepted')""",
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
        write_transaction(
            lambda conn: conn.execute(
                "UPDATE batch_campaigns SET campaign_dir=? WHERE id=?",
                (campaign_dir, campaign_id),
            ),
            operation="batch_campaign.directory",
        )

    whitelist_dir = os.path.join(campaign_dir, "whitelist")
    os.makedirs(whitelist_dir, exist_ok=True)
    ext = os.path.splitext(original_filename)[1].lower()

    try:
        row_count = _validate_customer_list_file(file_path)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Cannot read customer list file: {exc}") from exc

    dest = os.path.join(whitelist_dir, f"whitelist{ext}")
    shutil.move(file_path, dest)

    def write(conn):
        conn.execute(
            "UPDATE batch_campaigns SET customer_list_filename=?, customer_list_path=?, row_count=?, updated_at=datetime('now','localtime') WHERE id=?",
            (original_filename, dest, row_count, campaign_id),
        )
        return conn.execute(
            "SELECT * FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()

    row = write_transaction(write, operation="batch_campaign.upload")
    sms_task_service.invalidate_campaign_cache("batch", campaign_id)

    # If campaign is in progress, regenerate batches from new list (no auto-send)
    if campaign.get("status") == "in_progress" and row_count > 0:
        batch_size = campaign.get("batch_size") or 500
        generate_batches(campaign_id, batch_size)

    return dict(row) if row else None
