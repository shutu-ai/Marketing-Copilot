import hashlib
import json
import re
import uuid
from datetime import datetime
from typing import Iterable, Optional

from database import get_connection


FINAL_STATES = {"delivered", "failed", "expired", "cancelled", "archived"}

DLR_STATE_MAP = {
    "DELIVRD": {"status": "delivered", "label": "Delivered", "retryable": False},
    "DELIVERED": {"status": "delivered", "label": "Delivered", "retryable": False},
    "2": {"status": "delivered", "label": "Delivered", "retryable": False},
    "ACCEPTD": {"status": "submitted", "label": "Accepted by SMSC", "retryable": False},
    "ENROUTE": {"status": "submitted", "label": "En route", "retryable": False},
    "SCHEDULED": {"status": "submitted", "label": "Scheduled", "retryable": False},
    "UNDELIV": {"status": "retrying", "label": "Undeliverable", "retryable": True},
    "EXPIRED": {"status": "retrying", "label": "Expired at SMSC", "retryable": True},
    "UNKNOWN": {"status": "retrying", "label": "Unknown delivery state", "retryable": True},
    "REJECTD": {"status": "failed", "label": "Rejected", "retryable": False},
    "DELETED": {"status": "failed", "label": "Deleted", "retryable": False},
}


def _add_task_event(conn, task_id: int, event_type: str, from_status: str = "", to_status: str = "",
                    message: str = "", detail=None):
    conn.execute(
        """INSERT INTO task_events(task_id,event_type,from_status,to_status,message,detail)
           VALUES (?,?,?,?,?,?)""",
        (task_id, event_type, from_status or "", to_status or "", message or "",
         json.dumps(detail or {}, ensure_ascii=False, default=str)),
    )


def normalize_msisdn(value: object, country_code: str = "251") -> str:
    number = re.sub(r"\D", "", str(value or ""))
    if not number:
        return ""
    if number.startswith(country_code):
        return number
    if number.startswith("0"):
        number = number[1:]
    return country_code + number


def campaign_is_active(campaign: dict, now: Optional[datetime] = None) -> tuple[bool, str]:
    now = now or datetime.now().astimezone()
    if campaign.get("status") != "in_progress":
        return False, "campaign is not enabled"
    try:
        start = campaign.get("start_time") or ""
        end = campaign.get("end_time") or ""
        if start and now.replace(tzinfo=None) < datetime.fromisoformat(start).replace(tzinfo=None):
            return False, "campaign has not started"
        if end and now.replace(tzinfo=None) > datetime.fromisoformat(end).replace(tzinfo=None):
            return False, "campaign has ended"
        days = campaign.get("send_days") or []
        if isinstance(days, str):
            days = json.loads(days or "[]")
        if days and now.weekday() not in {int(v) for v in days}:
            return False, "today is not an allowed send day"
        current = now.strftime("%H:%M")
        if campaign.get("send_start_time") and current < campaign["send_start_time"][:5]:
            return False, "before daily send window"
        if campaign.get("send_end_time") and current > campaign["send_end_time"][:5]:
            return False, "after daily send window"
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        return False, f"invalid campaign schedule: {exc}"
    return True, "active"


def _template_for(conn, campaign_type: str, campaign_id: int, language: str):
    rows = conn.execute(
        """SELECT t.* FROM sms_templates t JOIN campaigns c ON c.id=t.campaign_id
           WHERE c.marketing_type=? AND c.marketing_campaign_id=?
             AND c.enabled=1 AND t.enabled=1
           ORDER BY t.id""",
        (campaign_type, campaign_id),
    ).fetchall()
    templates = [dict(r) for r in rows]
    if not templates:
        return None
    wanted = (language or "").strip().lower()
    for item in templates:
        if item.get("language", "").strip().lower() == wanted:
            return item
    for fallback in ("english", "en", "amharic"):
        for item in templates:
            if item.get("language", "").strip().lower() == fallback:
                return item
    return templates[0]


def enqueue_rows(campaign_type: str, campaign: dict, rows: Iterable[dict], source_file: str = "",
                 batch_id: Optional[int] = None) -> dict:
    active, reason = campaign_is_active(campaign)
    if not active:
        return {"success": False, "message": reason, "created": 0}

    conn = get_connection()
    whitelist_rows = conn.execute(
        "SELECT msisdn, language FROM whitelist_users WHERE status='active'"
    ).fetchall()
    whitelist = {
        normalize_msisdn(r["msisdn"]): (r.get("language") or "") for r in whitelist_rows
    }
    exclusions = {
        normalize_msisdn(r["msisdn"])
        for r in conn.execute("SELECT msisdn FROM exclusion_users WHERE enabled=1").fetchall()
    }
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    result = {"success": True, "created": 0, "duplicate": 0, "not_whitelisted": 0,
              "excluded": 0, "invalid": 0, "no_template": 0}
    seen = set()

    for raw in rows:
        payload = {str(k): "" if v is None else str(v) for k, v in dict(raw).items()}
        msisdn = normalize_msisdn(payload.get("msisdn"))
        if not msisdn:
            result["invalid"] += 1
            continue
        payload["msisdn"] = msisdn
        if msisdn in seen:
            result["duplicate"] += 1
            continue
        seen.add(msisdn)
        if msisdn not in whitelist:
            result["not_whitelisted"] += 1
            continue
        if msisdn in exclusions:
            result["excluded"] += 1
            continue

        language = whitelist[msisdn] or payload.get("language", "")
        template = _template_for(conn, campaign_type, campaign["id"], language)
        if not template:
            result["no_template"] += 1
            continue
        content = template["content"]
        for key, value in payload.items():
            content = content.replace("{" + key + "}", value)
        dedupe_key = hashlib.sha256(
            f"{campaign_type}:{campaign['id']}:{msisdn}:{today}".encode("utf-8")
        ).hexdigest()
        try:
            cursor = conn.execute(
                """INSERT INTO sms_tasks
                   (campaign_type,campaign_id,batch_id,campaign_name,msisdn,language,content,
                    source_addr,payload,source_file,dedupe_key)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (campaign_type, campaign["id"], batch_id, campaign.get("name", ""), msisdn,
                 language, content, template.get("source_addr") or "Marketing",
                 json.dumps(payload, ensure_ascii=False), source_file, dedupe_key),
            )
            _add_task_event(conn, cursor.lastrowid, "created", "", "queued", "task created from file",
                            {"source_file": source_file})
            result["created"] += 1
        except Exception as exc:
            if "duplicate" in str(exc).lower() or "unique" in str(exc).lower():
                result["duplicate"] += 1
            else:
                conn.close()
                raise
    conn.commit()
    conn.close()
    return result


def claim_next_task() -> Optional[dict]:
    conn = get_connection()
    token = uuid.uuid4().hex
    row = conn.execute(
        """UPDATE sms_tasks SET status='sending', attempts=attempts+1, claim_token=?,
           updated_at=CURRENT_TIMESTAMP
           WHERE id=(SELECT id FROM sms_tasks WHERE status IN ('queued','retrying')
             AND CAST(next_attempt_at AS TIMESTAMP)<=CURRENT_TIMESTAMP ORDER BY created_at,id LIMIT 1)
           AND status IN ('queued','retrying') RETURNING *""", (token,)
    ).fetchone()
    if row:
        _add_task_event(conn, row["id"], "claimed", "queued", "sending", "worker claimed task",
                        {"claim_token": token, "attempt": row["attempts"]})
    conn.commit()
    conn.close()
    return dict(row) if row else None


def mark_submitted(task_id: int, message_ids: list[str]):
    conn = get_connection()
    row = conn.execute("SELECT smsc_message_ids FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    existing_ids = []
    if row:
        try:
            existing_ids = json.loads(row["smsc_message_ids"] or "[]")
        except json.JSONDecodeError:
            existing_ids = []
    message_ids = list(dict.fromkeys(existing_ids + list(message_ids or [])))
    conn.execute(
        """UPDATE sms_tasks SET status='submitted', smsc_message_ids=?, error_msg='',
           submitted_at=COALESCE(submitted_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)), updated_at=CURRENT_TIMESTAMP
           WHERE id=? AND status IN ('sending','submitted')""",
        (json.dumps(message_ids), task_id),
    )
    if row:
        current = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
        if current and current["status"] == "submitted":
            _add_task_event(conn, task_id, "submitted", "sending", "submitted", "SMSC accepted task",
                            {"message_ids": message_ids})
    conn.commit()
    conn.close()


def append_message_id(task_id: int, message_id: str):
    conn = get_connection()
    row = conn.execute("SELECT smsc_message_ids FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    if row:
        try:
            values = json.loads(row["smsc_message_ids"] or "[]")
        except json.JSONDecodeError:
            values = []
        if message_id and message_id not in values:
            values.append(message_id)
        conn.execute(
            "UPDATE sms_tasks SET smsc_message_ids=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (json.dumps(values), task_id),
        )
        conn.commit()
    conn.close()


def mark_send_failure(task: dict, error: str, status_code: Optional[int] = None,
                      retryable: bool = True, failure_class: str = "transport"):
    retry = retryable and task["attempts"] < task.get("max_attempts", 3)
    status = "retrying" if retry else "failed"
    delay_minutes = min(30, 2 ** max(0, task["attempts"] - 1))
    conn = get_connection()
    conn.execute(
        f"""UPDATE sms_tasks SET status=?, error_msg=?, last_smpp_status=?, failure_class=?,
            next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '{delay_minutes} minutes',
            updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='sending'""",
        (status, str(error)[:2000], status_code, failure_class, task["id"]),
    )
    current = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task["id"],)).fetchone()
    if current and current["status"] == status:
        _add_task_event(conn, task["id"], "send_failure", task.get("status", "sending"), status,
                        str(error)[:500], {"smpp_status": status_code, "failure_class": failure_class})
    conn.commit()
    conn.close()


def record_receipt(message_id: str, state: str, raw: str):
    normalized_state = str(state or "").upper()
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM sms_tasks WHERE smsc_message_ids LIKE ? ORDER BY id DESC",
        (f'%"{message_id}"%',),
    ).fetchall()
    task = dict(rows[0]) if rows else None
    task_id = task["id"] if task else None
    try:
        conn.execute(
            "INSERT INTO delivery_receipts (task_id,smsc_message_id,message_state,raw_receipt) VALUES (?,?,?,?)",
            (task_id, message_id, normalized_state, raw[:4000]),
        )
    except Exception as exc:
        if "duplicate" not in str(exc).lower() and "unique" not in str(exc).lower():
            conn.close()
            raise
    if task_id:
        was_delivered = task.get("status") == "delivered"
        if normalized_state in {"DELIVRD", "DELIVERED", "2"}:
            try:
                expected_ids = set(json.loads(task.get("smsc_message_ids") or "[]"))
            except json.JSONDecodeError:
                expected_ids = set()
            delivered_rows = conn.execute(
                """SELECT DISTINCT smsc_message_id FROM delivery_receipts
                   WHERE task_id=? AND message_state IN ('DELIVRD','DELIVERED','2')""",
                (task_id,),
            ).fetchall()
            delivered_ids = {r["smsc_message_id"] for r in delivered_rows}
            delivered_ids.add(message_id)
            all_parts_delivered = bool(expected_ids) and expected_ids.issubset(delivered_ids)
            status = "delivered" if all_parts_delivered else "submitted"
            delivered_sql = ", delivered_at=CURRENT_TIMESTAMP" if all_parts_delivered else ""
        elif normalized_state in {"ACCEPTD", "ENROUTE", "SCHEDULED", "1", "6", "7"}:
            status = "submitted"
            delivered_sql = ""
        else:
            temporary_receipt = normalized_state in {"UNKNOWN", "UNDELIV", "EXPIRED"}
            can_retry = temporary_receipt and int(task.get("receipt_retry_count") or 0) < 1
            status = "retrying" if can_retry else "failed"
            delivered_sql = ""
        conn.execute(
            f"UPDATE sms_tasks SET status=?, failure_class=?, receipt_retry_count=COALESCE(receipt_retry_count,0)+?, "
            f"next_attempt_at=CASE WHEN ? THEN CAST(CURRENT_TIMESTAMP + INTERVAL '5 minutes' AS VARCHAR) ELSE next_attempt_at END, "
            f"updated_at=CURRENT_TIMESTAMP{delivered_sql} WHERE id=? AND (status<>'delivered' OR ?='delivered')",
            (status, "delivery_receipt" if status != "delivered" else "", 1 if status == "retrying" else 0,
             status == "retrying", task_id, status),
        )
        mapping = DLR_STATE_MAP.get(normalized_state, {"label": "Unmapped DLR state", "retryable": False})
        _add_task_event(conn, task_id, "delivery_receipt", task.get("status", ""), status,
                        mapping["label"], {"message_id": message_id, "state": normalized_state,
                                           "retryable": mapping.get("retryable", False), "raw": raw[:1000]})
        if status == "delivered" and task.get("batch_id"):
            batch_counts = conn.execute(
                """SELECT COUNT(*) total,
                          SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) delivered
                   FROM sms_tasks WHERE batch_id=?""",
                (task["batch_id"],),
            ).fetchone()
            delivered_count = int(batch_counts["delivered"] or 0)
            if batch_counts["total"] and delivered_count >= batch_counts["total"]:
                conn.execute(
                    "UPDATE batch_campaign_batches SET status='sent', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (task["batch_id"],),
                )
        if status == "delivered" and not was_delivered and task.get("campaign_type") == "batch":
            stats_date = datetime.now().astimezone().strftime("%Y-%m-%d")
            existing = conn.execute(
                "SELECT id FROM batch_campaign_daily_stats WHERE campaign_id=? AND stats_date=?",
                (task["campaign_id"], stats_date),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE batch_campaign_daily_stats SET sent_count=sent_count+1, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (existing["id"],),
                )
    conn.commit()
    conn.close()


def list_tasks(page: int = 1, page_size: int = 50, status: str = "", campaign_type: str = "") -> dict:
    conn = get_connection()
    conditions, params = [], []
    if status:
        conditions.append("status=?")
        params.append(status)
    if campaign_type:
        conditions.append("campaign_type=?")
        params.append(campaign_type)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) cnt FROM sms_tasks {where}", params).fetchone()["cnt"]
    offset = max(0, page - 1) * page_size
    rows = conn.execute(
        f"SELECT * FROM sms_tasks {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [page_size, offset],
    ).fetchall()
    conn.close()
    return {"total": total, "page": page, "page_size": page_size, "items": [dict(r) for r in rows]}


def task_stats() -> dict:
    conn = get_connection()
    rows = conn.execute("SELECT status, COUNT(*) count FROM sms_tasks GROUP BY status").fetchall()
    result = {r["status"]: r["count"] for r in rows}
    result["total"] = sum(result.values())
    conn.close()
    return result


def retry_task(task_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT id FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    if row:
        previous = conn.execute("SELECT status FROM sms_tasks WHERE id=?", (task_id,)).fetchone()["status"]
        conn.execute(
            "UPDATE sms_tasks SET status='queued', attempts=0, error_msg='', next_attempt_at=CURRENT_TIMESTAMP WHERE id=?",
            (task_id,),
        )
        _add_task_event(conn, task_id, "manual_retry", previous, "queued", "manual retry requested")
        conn.commit()
    conn.close()
    return bool(row)


def validate_task_schedule(task: dict) -> tuple[bool, str]:
    table = "batch_campaigns" if task["campaign_type"] == "batch" else "realtime_campaigns"
    conn = get_connection()
    row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (task["campaign_id"],)).fetchone()
    conn.close()
    if not row:
        return False, "campaign no longer exists"
    return campaign_is_active(dict(row))


def defer_task(task_id: int, reason: str, expired: bool = False):
    conn = get_connection()
    if expired:
        conn.execute(
            "UPDATE sms_tasks SET status='expired', error_msg=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (reason, task_id),
        )
    else:
        conn.execute(
            """UPDATE sms_tasks SET status='queued', attempts=GREATEST(attempts-1,0), error_msg=?,
               next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '5 minutes', updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (reason, task_id),
        )
    conn.commit()
    conn.close()


def recover_stale_tasks():
    conn = get_connection()
    conn.execute(
        """UPDATE sms_tasks SET status='retrying', error_msg='worker interrupted; recovered',
           next_attempt_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
           WHERE status='sending' AND CAST(updated_at AS TIMESTAMP) < CURRENT_TIMESTAMP - INTERVAL '5 minutes'"""
    )
    conn.execute(
        """UPDATE sms_tasks SET status='expired', error_msg='delivery receipt timeout',
           updated_at=CURRENT_TIMESTAMP
           WHERE status='submitted' AND CAST(submitted_at AS TIMESTAMP) < CURRENT_TIMESTAMP - INTERVAL '24 hours'"""
    )
    conn.commit()
    conn.close()
