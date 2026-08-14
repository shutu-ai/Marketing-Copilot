import hashlib
import json
import logging
import os
import re
import threading
import uuid
import time
from datetime import datetime
from typing import Iterable, Optional

from database import get_connection
from services import settings_service

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")

_TASK_STATS_CACHE = None
_TASK_STATS_CACHE_AT = 0.0
_TASK_STATS_CACHE_TTL = 30.0
_TASK_STATS_LOCK = threading.Lock()
_TASK_COUNT_CACHE = {}
_EXCLUSION_COUNT_CACHE = None
_EXCLUSION_COUNT_CACHE_AT = 0.0
_EXCLUSION_COUNT_LOCK = threading.Lock()
_CACHE_LOCK = threading.RLock()
_CAMPAIGN_CACHE = {}
_TEMPLATE_CACHE = {}
_CAMPAIGN_CACHE_TTL = 30.0
_CAMPAIGN_CACHE_GENERATION = 0
_TEMPLATE_CACHE_GENERATION = 0

# Keep SQL statements bounded. 2000 rows is inside the requested 1000-5000
# range while leaving room for the task/event columns in DuckDB statements.
DB_INSERT_BATCH_SIZE = 2000


FINAL_STATES = {"delivered", "failed", "expired", "cancelled", "archived", "accepted"}

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

LANGUAGE_ALIASES = {
    "en": "english",
    "eng": "english",
    "am": "amharic",
    "amh": "amharic",
    "ti": "tigrigna",
    "tig": "tigrigna",
    "so": "somali",
    "som": "somali",
    "om": "oromign",
    "orm": "oromign",
}


def normalize_language_key(value) -> str:
    key = (value or "").strip().lower()
    return LANGUAGE_ALIASES.get(key, key)


def invalidate_campaign_cache(campaign_type: str = "", campaign_id: Optional[int] = None) -> None:
    """Invalidate versioned runtime snapshots after campaign/config changes."""
    global _CAMPAIGN_CACHE_GENERATION, _TEMPLATE_CACHE_GENERATION
    with _CACHE_LOCK:
        _CAMPAIGN_CACHE_GENERATION += 1
        _TEMPLATE_CACHE_GENERATION += 1
        if campaign_type and campaign_id is not None:
            key = (campaign_type, int(campaign_id))
            _CAMPAIGN_CACHE.pop(key, None)
            _TEMPLATE_CACHE.pop(key, None)
        else:
            _CAMPAIGN_CACHE.clear()
            _TEMPLATE_CACHE.clear()


def invalidate_template_cache() -> None:
    global _TEMPLATE_CACHE_GENERATION
    with _CACHE_LOCK:
        _TEMPLATE_CACHE_GENERATION += 1
        _TEMPLATE_CACHE.clear()


def _cached_campaign(conn, campaign_type: str, campaign_id: int) -> Optional[dict]:
    key = (campaign_type, int(campaign_id))
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CAMPAIGN_CACHE.get(key)
        if cached and now - cached[0] < _CAMPAIGN_CACHE_TTL:
            return dict(cached[1])
        generation = _CAMPAIGN_CACHE_GENERATION
    table = {"batch": "batch_campaigns", "realtime": "realtime_campaigns"}.get(campaign_type)
    if not table:
        return None
    owns_conn = conn is None
    if owns_conn:
        conn = get_connection()
    row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (campaign_id,)).fetchone()
    if owns_conn:
        conn.close()
    if not row:
        return None
    campaign = dict(row)
    with _CACHE_LOCK:
        if generation == _CAMPAIGN_CACHE_GENERATION:
            _CAMPAIGN_CACHE[key] = (time.monotonic(), campaign)
    return dict(campaign)


def _add_task_event(conn, task_id: int, event_type: str, from_status: str = "", to_status: str = "",
                    message: str = "", detail=None):
    conn.execute(
        """INSERT INTO task_events(task_id,event_type,from_status,to_status,message,detail)
            VALUES (?,?,?,?,?,?)""",
        (task_id, event_type, from_status or "", to_status or "", message or "",
        json.dumps(detail or {}, ensure_ascii=False, default=str)),
    )


def _bulk_insert_tasks(conn, task_rows) -> list:
    """Insert tasks in bounded multi-row statements, returning inserted pairs."""
    if not task_rows:
        return []
    columns = ("campaign_type", "campaign_id", "batch_id", "campaign_name", "msisdn", "language",
               "sms_template_id", "smpp_config_id", "content", "source_addr", "payload",
               "source_file", "task_date", "dedupe_key")
    placeholders = ",".join("?" for _ in columns)
    inserted = []
    for start in range(0, len(task_rows), DB_INSERT_BATCH_SIZE):
        rows = task_rows[start:start + DB_INSERT_BATCH_SIZE]
        values = ",".join(f"({placeholders})" for _ in rows)
        conn.execute(
            f"INSERT INTO sms_tasks ({','.join(columns)}) VALUES {values} "
            "ON CONFLICT (dedupe_key) DO NOTHING RETURNING id, dedupe_key",
            [value for row in rows for value in row],
        )
        inserted.extend((r["id"], r["dedupe_key"]) for r in conn.fetchall())
    return inserted


def _bulk_insert_events(conn, event_rows) -> None:
    if not event_rows:
        return
    columns = ("task_id", "event_type", "from_status", "to_status", "message", "detail")
    placeholders = ",".join("?" for _ in columns)
    for start in range(0, len(event_rows), DB_INSERT_BATCH_SIZE):
        rows = event_rows[start:start + DB_INSERT_BATCH_SIZE]
        values = ",".join(f"({placeholders})" for _ in rows)
        conn.execute(
            f"INSERT INTO task_events ({','.join(columns)}) VALUES {values}",
            [value for row in rows for value in row],
        )


def _add_task_event_batch(conn, campaign_type: str, campaign_id: int,
                          batch_id: Optional[int], event_type: str,
                          from_status: str, to_status: str, task_ids,
                          detail=None) -> None:
    ids = [int(task_id) for task_id in task_ids]
    if not ids:
        return
    _add_task_event_batch_summary(
        conn, campaign_type, campaign_id, batch_id, event_type,
        from_status, to_status, len(ids), min(ids), max(ids), detail,
    )


def _add_task_event_batch_summary(conn, campaign_type: str, campaign_id: int,
                                  batch_id: Optional[int], event_type: str,
                                  from_status: str, to_status: str, event_count: int,
                                  first_task_id: int, last_task_id: int,
                                  detail=None) -> None:
    if not event_count:
        return
    conn.execute(
        """INSERT INTO task_event_batches
           (campaign_type,campaign_id,batch_id,event_type,from_status,to_status,
            event_count,first_task_id,last_task_id,detail)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (campaign_type, campaign_id, batch_id, event_type, from_status, to_status,
         event_count, first_task_id, last_task_id,
         json.dumps(detail or {}, ensure_ascii=False, default=str)),
    )


def cancel_pending_campaign_tasks(conn, campaign_type: str, campaign_id: int,
                                  message: str = "campaign cancelled") -> int:
    """Mark not-yet-submitted campaign tasks terminal so workers cannot claim them."""
    summary = conn.execute(
        """SELECT COUNT(*) count,MIN(id) first_id,MAX(id) last_id FROM sms_tasks
           WHERE campaign_type=? AND campaign_id=?
             AND status IN ('queued','retrying','sending')""",
        (campaign_type, campaign_id),
    ).fetchone()
    count = int(summary["count"] or 0)
    if not count:
        return 0
    conn.execute(
        """UPDATE sms_tasks SET status='cancelled', error_msg=?, claim_token='',
               updated_at=CURRENT_TIMESTAMP
           WHERE campaign_type=? AND campaign_id=?
             AND status IN ('queued','retrying','sending')""",
        (message, campaign_type, campaign_id),
    )
    _add_task_event_batch_summary(
        conn, campaign_type, campaign_id, None,
        "campaign_cancelled", "active", "cancelled", count,
        summary["first_id"], summary["last_id"],
        {"message": message},
    )
    return count


def _insert_message_mapping(conn, task_id: int, message_ids) -> None:
    """Record the latest task that used each SMSC message id.

    SMSC message ids are not guaranteed globally unique (e.g. an SMSC or the
    local simulator can reuse ids after a restart), so the row must point at
    the most recent task instead of being ignored once filled. ``INSERT OR
    REPLACE`` silently does nothing on conflict when the table has a secondary
    index (DuckDB quirk), so an explicit ``ON CONFLICT ... DO UPDATE`` is used.
    """
    rows = [(str(mid), task_id) for mid in (message_ids or []) if str(mid)]
    if not rows:
        return
    values = ",".join("(?,?)" for _ in rows)
    conn.execute(
        f"""INSERT INTO sms_task_message_ids (smsc_message_id, task_id) VALUES {values}
            ON CONFLICT (smsc_message_id) DO UPDATE SET task_id = excluded.task_id""",
        [value for row in rows for value in row],
    )


def _resolve_template_map(conn, campaign_type: str, campaign_id: int, campaign: dict) -> dict:
    """Resolve and cache the campaign's language->template mapping."""
    cache_key = (campaign_type, int(campaign_id))
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _TEMPLATE_CACHE.get(cache_key)
        if cached and now - cached[0] < _CAMPAIGN_CACHE_TTL:
            return cached[1]
        generation = _TEMPLATE_CACHE_GENERATION

    def cache(value: dict) -> dict:
        with _CACHE_LOCK:
            if generation == _TEMPLATE_CACHE_GENERATION:
                _TEMPLATE_CACHE[cache_key] = (time.monotonic(), value)
        return value

    rows = conn.execute(
        """SELECT t.* FROM sms_templates t JOIN campaigns c ON c.id=t.campaign_id
            WHERE c.marketing_type=? AND c.marketing_campaign_id=?
            AND c.enabled=1 AND t.enabled=1
            ORDER BY t.id""",
        (campaign_type, campaign_id),
    ).fetchall()
    templates = [dict(r) for r in rows]
    if templates:
        by_lang = {}
        for item in templates:
            by_lang.setdefault(normalize_language_key(item.get("language", "")), item)
        fallback = None
        for key in ("english", "en", "amharic"):
            if key in by_lang:
                fallback = by_lang[key]
                break
        return cache({"by_lang": by_lang, "fallback": fallback, "default": templates[0], "direct": None})
    selected_template_id = campaign.get("sms_template_id")
    direct = None
    if selected_template_id:
        selected = conn.execute(
            "SELECT * FROM sms_templates WHERE id=? AND enabled=1 LIMIT 1", (selected_template_id,),
        ).fetchone()
        direct = dict(selected) if selected else None
    if not direct:
        fallback_campaign = conn.execute(
            """SELECT id FROM campaigns WHERE marketing_type=? AND marketing_campaign_id=?
                ORDER BY id LIMIT 1""",
            (campaign_type, campaign_id),
        ).fetchone()
        if fallback_campaign:
            fallback_template = conn.execute(
                """SELECT * FROM sms_templates WHERE campaign_id=? AND enabled=1 ORDER BY id LIMIT 1""",
                (fallback_campaign["id"],),
            ).fetchone()
            if fallback_template:
                direct = dict(fallback_template)
    return cache({"by_lang": {}, "fallback": None, "default": None, "direct": direct})


def _select_template(template_map: dict, language: str) -> Optional[dict]:
    if template_map["by_lang"]:
        wanted = normalize_language_key(language)
        item = template_map["by_lang"].get(wanted)
        if item:
            return item
        if template_map["fallback"]:
            return template_map["fallback"]
        return template_map["default"]
    return template_map["direct"]


def normalize_msisdn(value: object) -> str:
    """Clean a phone number for comparison without adding any country code.

    Only non-digits are removed and a single leading 0 is stripped. The final
    number format is decided by the campaign's phone_prefix configuration; the
    system no longer assumes a country code (previously hardcoded to 251).
    """
    number = re.sub(r"\D", "", str(value or ""))
    if not number:
        return ""
    if number.startswith("0"):
        number = number[1:]
    return number


def stable_task_dedupe_key(campaign_type: str, campaign_id: int, msisdn: str,
                           task_date: str, source_file: str) -> str:
    """Build a replay-stable business key without ingestion-time timestamps."""
    source_identity = os.path.basename(str(source_file or "")).strip()
    value = f"{campaign_type}:{int(campaign_id)}:{source_identity}:{task_date}:{msisdn}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _find_excluded_msisdns(conn, msisdns, chunk_size: int = 1000) -> set[str]:
    """Return only exclusions relevant to the current enqueue batch."""
    values = list(msisdns)
    excluded = set()
    for start in range(0, len(values), chunk_size):
        chunk = values[start:start + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT msisdn FROM exclusion_users WHERE enabled=1 AND msisdn IN ({placeholders})",
            chunk,
        ).fetchall()
        excluded.update(row["msisdn"] for row in rows)
    return excluded

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
        # API/UI use JavaScript's convention: Sunday=0 ... Saturday=6.
        api_weekday = (now.weekday() + 1) % 7
        if days and api_weekday not in {int(v) for v in days}:
            return False, "today is not an allowed send day"
        current = now.strftime("%H:%M")
        window_start = (campaign.get("send_start_time") or "")[:5]
        window_end = (campaign.get("send_end_time") or "")[:5]
        if window_start and window_end:
            # An end time earlier than the start time crosses midnight, e.g.
            # 11:19–00:00 permits 11:34 and continues until midnight.
            in_window = (window_start <= current <= window_end) if window_start <= window_end else (
                current >= window_start or current <= window_end
            )
            if not in_window:
                return False, "outside daily send window"
        elif window_start and current < window_start:
            return False, "before daily send window"
        elif window_end and current > window_end:
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
        campaign_table = {"batch": "batch_campaigns", "realtime": "realtime_campaigns"}.get(campaign_type)
        selected_template_id = None
        if campaign_table:
            selected = conn.execute(
                f"SELECT sms_template_id FROM {campaign_table} WHERE id=?", (campaign_id,)
            ).fetchone()
            selected_template_id = selected["sms_template_id"] if selected else None
        detail_logger.info(
            "template lookup campaign_type=%s campaign_id=%s selected_template_id=%s",
            campaign_type, campaign_id, selected_template_id,
        )
        if not selected_template_id:
            # Last resort: look up campaigns table by marketing_campaign_id ignoring enabled flag
            fallback_campaign = conn.execute(
                """SELECT id FROM campaigns WHERE marketing_type=? AND marketing_campaign_id=?
                    ORDER BY id LIMIT 1""",
                (campaign_type, campaign_id),
            ).fetchone()
            if fallback_campaign:
                fallback_template = conn.execute(
                    """SELECT t.* FROM sms_templates t
                        WHERE t.campaign_id=? AND t.enabled=1
                        ORDER BY t.id LIMIT 1""",
                    (fallback_campaign["id"],),
                ).fetchone()
                if fallback_template:
                    detail_logger.info("template fallback campaign_type=%s campaign_id=%s template=%s",
                                       campaign_type, campaign_id, fallback_template["id"])
                    return dict(fallback_template)
            detail_logger.info("template missing campaign_type=%s campaign_id=%s language=%s",
                               campaign_type, campaign_id, language)
            return None
        selected_template = conn.execute(
            """SELECT t.* FROM sms_templates t
                WHERE t.id=? AND t.enabled=1 LIMIT 1""",
            (selected_template_id,),
        ).fetchone()
        detail_logger.info("template direct campaign_type=%s campaign_id=%s template=%s",
                           campaign_type, campaign_id, selected_template["id"] if selected_template else None)
        return dict(selected_template) if selected_template else None
    wanted = normalize_language_key(language)
    for item in templates:
        if normalize_language_key(item.get("language", "")) == wanted:
            return item
    for fallback in ("english", "en", "amharic"):
        for item in templates:
            if normalize_language_key(item.get("language", "")) == fallback:
                return item
    detail_logger.info("template default campaign_type=%s campaign_id=%s template=%s",
                       campaign_type, campaign_id, templates[0]["id"])
    return templates[0]


def enqueue_rows(campaign_type: str, campaign: dict, rows: Iterable[dict], source_file: str = "",
                batch_id: Optional[int] = None, validate_schedule: bool = True) -> dict:
    if validate_schedule:
        active, reason = campaign_is_active(campaign)
        if not active:
            return {"success": False, "message": reason, "created": 0}
    elif campaign.get("status") != "in_progress":
        return {"success": False, "message": "campaign is not enabled", "created": 0}

    conn = get_connection()
    try:
        task_date = datetime.now().astimezone().date().isoformat()
        if campaign_type == "realtime":
            match = re.search(r"(\d{14})", os.path.basename(source_file or ""))
            if match:
                try:
                    task_date = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").date().isoformat()
                except ValueError:
                    pass
        result = {"success": True, "created": 0, "duplicate": 0,
                  "excluded": 0, "invalid": 0, "no_template": 0,
                  "created_msisdns": []}
        phone_prefix = (campaign.get("phone_prefix") or "").replace(" ", "").replace("\t", "").replace("\r", "").replace("\n", "")
        template_map = _resolve_template_map(conn, campaign_type, campaign["id"], campaign)
        stage_rows = []
        for ordinal, raw in enumerate(rows):
            payload = {str(k): "" if v is None else str(v) for k, v in dict(raw).items()}
            stage_rows.append((ordinal, payload.get("msisdn", ""),
                               json.dumps(payload, ensure_ascii=False)))
        conn.execute(
            """CREATE TEMP TABLE ingest_task_stage (
               ordinal BIGINT,raw_msisdn TEXT,payload TEXT)"""
        )
        if stage_rows:
            conn.executemany(
                "INSERT INTO ingest_task_stage VALUES (?,?,?)", stage_rows
            )
        normalized_expression = (
            "regexp_replace(regexp_replace(COALESCE(raw_msisdn,''),'[^0-9]','','g'),'^0','')"
        )
        prefix_sql = "? || normalized" if phone_prefix else "normalized"
        stage_params = [phone_prefix, phone_prefix] + ([phone_prefix] if phone_prefix else [])
        stats = conn.execute(
            f"""WITH cleaned AS (
                    SELECT ordinal,payload,{normalized_expression} normalized
                    FROM ingest_task_stage
                 ), prefixed AS (
                    SELECT ordinal,payload,
                           CASE WHEN normalized='' THEN ''
                                WHEN ?<>'' AND NOT starts_with(normalized,?) THEN {prefix_sql}
                                ELSE normalized END msisdn
                    FROM cleaned
                 ), ranked AS (
                    SELECT *,ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY ordinal) rn
                    FROM prefixed
                 )
                 SELECT SUM(CASE WHEN r.msisdn='' THEN 1 ELSE 0 END) invalid,
                        SUM(CASE WHEN r.msisdn<>'' AND rn>1 THEN 1 ELSE 0 END) duplicate,
                        SUM(CASE WHEN r.msisdn<>'' AND rn=1 AND e.msisdn IS NOT NULL THEN 1 ELSE 0 END) excluded
                 FROM ranked r LEFT JOIN exclusion_users e
                   ON e.msisdn=r.msisdn AND e.enabled=1""",
            stage_params,
        ).fetchone()
        result["invalid"] = int(stats["invalid"] or 0)
        result["duplicate"] = int(stats["duplicate"] or 0)
        result["excluded"] = int(stats["excluded"] or 0)
        prepared_rows = conn.execute(
            f"""WITH cleaned AS (
                    SELECT ordinal,payload,{normalized_expression} normalized
                    FROM ingest_task_stage
                 ), prefixed AS (
                    SELECT ordinal,payload,
                           CASE WHEN normalized='' THEN ''
                                WHEN ?<>'' AND NOT starts_with(normalized,?) THEN {prefix_sql}
                                ELSE normalized END msisdn
                    FROM cleaned
                 ), ranked AS (
                    SELECT *,ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY ordinal) rn
                    FROM prefixed
                 )
                 SELECT r.payload,r.msisdn FROM ranked r
                 LEFT JOIN exclusion_users e ON e.msisdn=r.msisdn AND e.enabled=1
                 WHERE r.msisdn<>'' AND r.rn=1 AND e.msisdn IS NULL
                 ORDER BY r.ordinal""",
            stage_params,
        ).fetchall()
        task_rows = []
        for stage_row in prepared_rows:
            payload = json.loads(stage_row["payload"])
            msisdn = stage_row["msisdn"]
            payload["msisdn"] = msisdn
            language = payload.get("language", "")
            template = _select_template(template_map, language)
            if not template:
                result["no_template"] += 1
                continue
            content = template["content"]
            for key, value in payload.items():
                content = content.replace("{" + key + "}", value)
            dedupe_key = stable_task_dedupe_key(
                campaign_type, campaign["id"], msisdn, task_date, source_file
            )
            task_rows.append((
                campaign_type, campaign["id"], batch_id, campaign.get("name", ""), msisdn,
                language, template["id"], campaign.get("smpp_config_id"), content,
                template.get("source_addr") or "",
                json.dumps(payload, ensure_ascii=False), source_file, task_date, dedupe_key,
            ))
        if task_rows:
            created = _bulk_insert_tasks(conn, task_rows)
            result["duplicate"] += len(task_rows) - len(created)
            result["created"] += len(created)
            created_keys = {dedupe for _task_id, dedupe in created}
            result["created_msisdns"].extend(
                row[4] for row in task_rows if row[-1] in created_keys
            )
            _add_task_event_batch(
                conn, campaign_type, campaign["id"], batch_id,
                "created", "", "queued",
                [task_id for task_id, _dedupe in created],
                {"source_file": source_file},
            )
        conn.commit()
        logger.info(
            "Tasks enqueued campaign=%s:%s file=%s result=%s",
            campaign_type, campaign["id"], source_file, result,
        )
        return result
    finally:
        conn.close()


def claim_next_tasks(limit: int = 20) -> list:
    limit = max(1, int(limit or 20))
    conn = get_connection()
    token = uuid.uuid4().hex
    rows = conn.execute(
        """UPDATE sms_tasks SET status='sending', attempts=attempts+1, claim_token=?,
            updated_at=CURRENT_TIMESTAMP
            WHERE id IN (SELECT id FROM sms_tasks WHERE status IN ('queued','retrying')
            AND CAST(next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
            AND (campaign_type <> 'realtime' OR task_date=CURRENT_DATE) ORDER BY created_at,id LIMIT ?)
            AND status IN ('queued','retrying') RETURNING *""", (token, limit)
    ).fetchall()
    tasks = [dict(r) for r in rows]
    if tasks:
        grouped = {}
        for task in tasks:
            key = (task["campaign_type"], task["campaign_id"], task.get("batch_id"))
            grouped.setdefault(key, []).append(task)
        for (campaign_type, campaign_id, batch_id), items in grouped.items():
            _add_task_event_batch(
                conn, campaign_type, campaign_id, batch_id,
                "claimed", "queued", "sending",
                [task["id"] for task in items],
                {"claim_token": token, "attempt_min": min(task["attempts"] for task in items),
                 "attempt_max": max(task["attempts"] for task in items)},
            )
    conn.commit()
    conn.close()
    return tasks


def claim_next_task() -> Optional[dict]:
    tasks = claim_next_tasks(1)
    return tasks[0] if tasks else None


def apply_buffered_writes(conn, items) -> None:
    """Apply one buffered delivery batch inside a single database transaction."""
    submitted_ids = []
    for item in items:
        payload = item.payload
        if item.kind == "submitted":
            _apply_submitted(conn, **payload)
            submitted_ids.append(int(payload["task_id"]))
        elif item.kind == "message_id":
            _apply_message_id(conn, **payload)
        elif item.kind == "send_failure":
            _apply_send_failure(conn, **payload)
        elif item.kind == "receipt":
            _apply_receipt(conn, **payload)
        else:
            raise ValueError(f"unknown SMS buffered write kind: {item.kind}")
    if submitted_ids:
        placeholders = ",".join("?" for _ in submitted_ids)
        rows = conn.execute(
            f"""SELECT id,campaign_type,campaign_id,batch_id,status FROM sms_tasks
                 WHERE id IN ({placeholders})""",
            submitted_ids,
        ).fetchall()
        grouped = {}
        for row in rows:
            key = (row["campaign_type"], row["campaign_id"], row["batch_id"], row["status"])
            grouped.setdefault(key, []).append(row["id"])
        for (campaign_type, campaign_id, batch_id, status), task_ids in grouped.items():
            _add_task_event_batch(
                conn, campaign_type, campaign_id, batch_id,
                "submitted", "sending", status, task_ids,
                {"buffered": True},
            )


def flush_pending_writes(timeout: float = 10.0) -> None:
    from services import sms_write_buffer
    sms_write_buffer.flush(timeout)


def mark_submitted(task_id: int, message_ids: list[str], delivery_receipt_mode: str = "optional",
                   wait: bool = True):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "submitted",
        {"task_id": task_id, "message_ids": list(message_ids or []),
         "delivery_receipt_mode": delivery_receipt_mode},
        wait=wait,
    )


def _apply_submitted(conn, task_id: int, message_ids: list[str],
                     delivery_receipt_mode: str = "optional"):
    row = conn.execute("SELECT smsc_message_ids FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    existing_ids = []
    if row:
        try:
            existing_ids = json.loads(row["smsc_message_ids"] or "[]")
        except json.JSONDecodeError:
            existing_ids = []
    message_ids = list(dict.fromkeys(existing_ids + list(message_ids or [])))
    mode = delivery_receipt_mode if delivery_receipt_mode in {"required", "optional", "disabled"} else "optional"
    final_status = "submitted" if mode == "required" else "accepted"
    conn.execute(
        f"""UPDATE sms_tasks SET status='{final_status}', smsc_message_ids=?, error_msg='',
            submitted_at=COALESCE(submitted_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)), updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status IN ('sending','submitted')""",
        (json.dumps(message_ids), task_id),
    )
    _insert_message_mapping(conn, task_id, message_ids)
    pending_receipts = conn.execute(
        """SELECT smsc_message_id, message_state, raw_receipt
           FROM delivery_receipts
           WHERE task_id IS NULL AND smsc_message_id IN
             (SELECT UNNEST(from_json(?, '[\"VARCHAR\"]')))""",
        (json.dumps(message_ids),),
    ).fetchall()
    if pending_receipts:
        conn.execute(
            """UPDATE delivery_receipts SET task_id=?
               WHERE task_id IS NULL AND smsc_message_id IN
                 (SELECT UNNEST(from_json(?, '[\"VARCHAR\"]')))""",
            (task_id, json.dumps(message_ids)),
        )
    for receipt in pending_receipts:
        _apply_receipt(
            conn, receipt["smsc_message_id"], receipt["message_state"],
            receipt["raw_receipt"], task_id=task_id, skip_insert=True,
        )


def append_message_id(task_id: int, message_id: str, wait: bool = True):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "message_id", {"task_id": task_id, "message_id": message_id}, wait=wait
    )


def _apply_message_id(conn, task_id: int, message_id: str):
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
        _insert_message_mapping(conn, task_id, [message_id])


def mark_send_failure(task: dict, error: str, status_code: Optional[int] = None,
                      retryable: bool = True, failure_class: str = "transport",
                      wait: bool = True):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "send_failure",
        {"task": dict(task), "error": str(error), "status_code": status_code,
         "retryable": retryable, "failure_class": failure_class},
        wait=wait,
    )


def _apply_send_failure(conn, task: dict, error: str, status_code: Optional[int] = None,
                        retryable: bool = True, failure_class: str = "transport"):
    retry = retryable and task["attempts"] < task.get("max_attempts", 3)
    status = "retrying" if retry else "failed"
    delay_minutes = min(30, 2 ** max(0, task["attempts"] - 1))
    updated = conn.execute(
        f"""UPDATE sms_tasks SET status=?, error_msg=?, last_smpp_status=?, failure_class=?,
            next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '{delay_minutes} minutes',
            updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='sending'
            RETURNING id""",
        (status, str(error)[:2000], status_code, failure_class, task["id"]),
    ).fetchone()
    if updated:
        _add_task_event(conn, task["id"], "send_failure", task.get("status", "sending"), status,
                        str(error)[:500], {"smpp_status": status_code, "failure_class": failure_class})
        if status == "failed" and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=COALESCE(failed_count,0)+1,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (task["batch_id"],),
            )


def requeue_transport_failures() -> int:
    """Put terminal transport failures back into the send queue after recovery."""
    conn = get_connection()
    count = conn.execute(
        "SELECT COUNT(*) count FROM sms_tasks WHERE status='failed' AND failure_class='transport'"
    ).fetchone()["count"]
    if count:
        by_batch = conn.execute(
            """SELECT batch_id,COUNT(*) count FROM sms_tasks
               WHERE status='failed' AND failure_class='transport' AND batch_id IS NOT NULL
               GROUP BY batch_id"""
        ).fetchall()
        conn.execute(
            """UPDATE sms_tasks SET status='queued', attempts=0, error_msg='',
                claim_token='', last_smpp_status=NULL, next_attempt_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
                WHERE status='failed' AND failure_class='transport'"""
        )
        for row in by_batch:
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=GREATEST(COALESCE(failed_count,0)-?,0),
                       updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (row["count"], row["batch_id"]),
            )
        conn.commit()
    conn.close()
    return int(count or 0)


def record_receipt(message_id: str, state: str, raw: str,
                   _task_id: Optional[int] = None, _skip_insert: bool = False,
                   wait: bool = True):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "receipt",
        {"message_id": str(message_id), "state": str(state), "raw": str(raw),
         "task_id": _task_id, "skip_insert": _skip_insert},
        wait=wait,
    )


def _apply_receipt(conn, message_id: str, state: str, raw: str,
                   task_id: Optional[int] = None, skip_insert: bool = False):
    normalized_state = str(state or "").upper()
    if task_id is None:
        task = _find_task_by_message_id(conn, message_id)
        task_id = task["id"] if task else None
    else:
        row = conn.execute("SELECT * FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
        task = dict(row) if row else None
    if not skip_insert:
        inserted = conn.execute(
            """INSERT INTO delivery_receipts
               (task_id,smsc_message_id,message_state,raw_receipt)
               VALUES (?,?,?,?) ON CONFLICT DO NOTHING RETURNING id""",
            (task_id, message_id, normalized_state, raw[:4000]),
        ).fetchone()
        if task_id is not None and not inserted:
            return
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
            status = "delivered" if all_parts_delivered else ("accepted" if task.get("status") == "accepted" else "submitted")
            delivered_sql = ", delivered_at=CURRENT_TIMESTAMP" if all_parts_delivered else ""
        elif normalized_state in {"ACCEPTD", "ENROUTE", "SCHEDULED", "1", "6", "7"}:
            status = "accepted" if task.get("status") == "accepted" else "submitted"
            delivered_sql = ""
        else:
            temporary_receipt = normalized_state in {"UNKNOWN", "UNDELIV", "EXPIRED"}
            can_retry = temporary_receipt and int(task.get("receipt_retry_count") or 0) < 1
            status = "retrying" if can_retry else "failed"
            delivered_sql = ""
        updated = conn.execute(
            f"UPDATE sms_tasks SET status=?, failure_class=?, receipt_retry_count=COALESCE(receipt_retry_count,0)+?, "
            f"next_attempt_at=CASE WHEN ? THEN CAST(CURRENT_TIMESTAMP + INTERVAL '5 minutes' AS VARCHAR) ELSE next_attempt_at END, "
            f"updated_at=CURRENT_TIMESTAMP{delivered_sql} WHERE id=? AND (status<>'delivered' OR ?='delivered') "
            f"RETURNING id",
            (status, "delivery_receipt" if status != "delivered" else "", 1 if status == "retrying" else 0,
            status == "retrying", task_id, status),
        ).fetchone()
        if not updated:
            return
        mapping = DLR_STATE_MAP.get(normalized_state, {"label": "Unmapped DLR state", "retryable": False})
        _add_task_event(conn, task_id, "delivery_receipt", task.get("status", ""), status,
                        mapping["label"], {"message_id": message_id, "state": normalized_state,
                                            "retryable": mapping.get("retryable", False), "raw": raw[:1000]})
        if status == "delivered" and not was_delivered and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET delivered_count=COALESCE(delivered_count,0)+1,
                       failed_count=CASE WHEN ?='failed'
                         THEN GREATEST(COALESCE(failed_count,0)-1,0)
                         ELSE COALESCE(failed_count,0) END,
                   status=CASE
                     WHEN task_count>0 AND COALESCE(delivered_count,0)+1>=task_count
                     THEN 'sent' ELSE status END,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (task.get("status", ""), task["batch_id"]),
            )
        elif status == "failed" and task.get("status") != "failed" and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=COALESCE(failed_count,0)+1,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (task["batch_id"],),
            )
        elif task.get("status") == "failed" and status != "failed" and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=GREATEST(COALESCE(failed_count,0)-1,0),
                       updated_at=CURRENT_TIMESTAMP WHERE id=?""",
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


def _find_task_by_message_id(conn, message_id: str) -> Optional[dict]:
    """Find the task through the indexed SMSC message-id mapping."""
    row = conn.execute(
        """SELECT t.* FROM sms_task_message_ids m
           JOIN sms_tasks t ON t.id=m.task_id
           WHERE m.smsc_message_id=? LIMIT 1""",
        (str(message_id),),
    ).fetchone()
    return dict(row) if row else None


def cleanup_task_retention() -> int:
    retention_days = max(1, int(settings_service.get_setting("sms_task_retention_days", "3")))
    cleanup_batch = max(1000, min(20000, int(os.environ.get("MC_TASK_CLEANUP_BATCH", "5000"))))
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
    archive_enabled = os.environ.get("MC_TASK_ARCHIVE_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
    archived_count = 0
    if archive_enabled:
        from services import task_archive_service
        archived_count = task_archive_service.archive_terminal_tasks(cutoff, cleanup_batch)["archived"]
    conn = get_connection()
    active = "('queued','sending','retrying','submitted','accepted')"
    rows = [] if archive_enabled else conn.execute(
        f"""SELECT id FROM sms_tasks
             WHERE created_at < ? AND status NOT IN {active}
             ORDER BY id LIMIT ?""",
        (cutoff, cleanup_batch),
    ).fetchall()
    ids = [row["id"] for row in rows]
    if ids:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(f"DELETE FROM sms_task_message_ids WHERE task_id IN ({placeholders})", ids)
        conn.execute(f"DELETE FROM delivery_receipts WHERE task_id IN ({placeholders})", ids)
        conn.execute(f"DELETE FROM task_events WHERE task_id IN ({placeholders})", ids)
        conn.execute(f"DELETE FROM sms_tasks WHERE id IN ({placeholders})", ids)
    old_event_rows = conn.execute(
        "SELECT id FROM task_event_batches WHERE created_at < ? ORDER BY id LIMIT ?",
        (cutoff, cleanup_batch),
    ).fetchall()
    old_event_ids = [row["id"] for row in old_event_rows]
    if old_event_ids:
        event_placeholders = ",".join("?" for _ in old_event_ids)
        conn.execute(
            f"DELETE FROM task_event_batches WHERE id IN ({event_placeholders})",
            old_event_ids,
        )
    if ids or old_event_ids:
        conn.commit()
    conn.close()
    if ids:
        logger.info("SMS task retention cleanup deleted=%s retention_days=%s", len(ids), retention_days)
    return archived_count if archive_enabled else len(ids)

def _cached_task_count(conn, where: str, params: list) -> int:
    now = time.monotonic()
    key = (where, tuple(params))
    cached = _TASK_COUNT_CACHE.get(key)
    if cached and now - cached[0] < _TASK_STATS_CACHE_TTL:
        return cached[1]
    with _TASK_STATS_LOCK:
        cached = _TASK_COUNT_CACHE.get(key)
        if cached and now - cached[0] < _TASK_STATS_CACHE_TTL:
            return cached[1]
        total = conn.execute(
            f"SELECT COUNT(*) cnt FROM sms_tasks {where}", params
        ).fetchone()["cnt"]
        _TASK_COUNT_CACHE[key] = (now, total)
        if len(_TASK_COUNT_CACHE) > 128:
            expired = [
                cache_key for cache_key, value in _TASK_COUNT_CACHE.items()
                if now - value[0] >= _TASK_STATS_CACHE_TTL
            ]
            for cache_key in expired:
                _TASK_COUNT_CACHE.pop(cache_key, None)
        return total

def list_tasks(page: int = 1, page_size: int = 50, status: str = "", campaign_type: str = "",
               smpp_config_id: int = -1, cursor: Optional[int] = None) -> dict:
    conn = get_connection()
    conditions, params = [], []
    if status:
        conditions.append("status=?")
        params.append(status)
    if campaign_type:
        conditions.append("campaign_type=?")
        params.append(campaign_type)
    if smpp_config_id >= 0:
        if smpp_config_id == 0:
            conditions.append("smpp_config_id IS NULL")
        else:
            conditions.append("smpp_config_id=?")
            params.append(smpp_config_id)
    count_where = "WHERE " + " AND ".join(conditions) if conditions else ""
    total = task_stats()["total"] if not conditions else _cached_task_count(conn, count_where, params)
    query_conditions = list(conditions)
    query_params = list(params)
    if cursor is not None and int(cursor) > 0:
        query_conditions.append("id<?")
        query_params.append(int(cursor))
    where = "WHERE " + " AND ".join(query_conditions) if query_conditions else ""
    rows = conn.execute(
        f"""SELECT id,campaign_type,campaign_id,campaign_name,smpp_config_id,
                   msisdn,language,content,status,attempts,error_msg,updated_at
            FROM sms_tasks {where} ORDER BY id DESC LIMIT ?""",
        query_params + [page_size + 1],
    ).fetchall()
    conn.close()
    has_more = len(rows) > page_size
    rows = rows[:page_size]
    items = [dict(r) for r in rows]
    next_cursor = items[-1]["id"] if has_more and items else None
    return {"total": total, "page": page, "page_size": page_size, "items": items,
            "next_cursor": next_cursor, "has_more": has_more}


def _cached_exclusion_count(conn) -> int:
    global _EXCLUSION_COUNT_CACHE, _EXCLUSION_COUNT_CACHE_AT
    now = time.monotonic()
    if (
        _EXCLUSION_COUNT_CACHE is not None
        and now - _EXCLUSION_COUNT_CACHE_AT < _TASK_STATS_CACHE_TTL
    ):
        return _EXCLUSION_COUNT_CACHE
    with _EXCLUSION_COUNT_LOCK:
        now = time.monotonic()
        if (
            _EXCLUSION_COUNT_CACHE is not None
            and now - _EXCLUSION_COUNT_CACHE_AT < _TASK_STATS_CACHE_TTL
        ):
            return _EXCLUSION_COUNT_CACHE
        total = conn.execute("SELECT COUNT(*) cnt FROM exclusion_users").fetchone()["cnt"]
        _EXCLUSION_COUNT_CACHE = total
        _EXCLUSION_COUNT_CACHE_AT = now
        return total

def list_exclusions(page: int = 1, page_size: int = 50) -> dict:
    conn = get_connection()
    total = _cached_exclusion_count(conn)
    rows = conn.execute(
        "SELECT id,msisdn,reason,enabled,created_at FROM exclusion_users ORDER BY id DESC LIMIT ? OFFSET ?",
        (page_size, max(0, page - 1) * page_size),
    ).fetchall()
    conn.close()
    return {"total": total, "page": page, "page_size": page_size, "items": [dict(r) for r in rows]}


def task_stats() -> dict:
    global _TASK_STATS_CACHE, _TASK_STATS_CACHE_AT
    now = time.monotonic()
    if _TASK_STATS_CACHE is not None and now - _TASK_STATS_CACHE_AT < _TASK_STATS_CACHE_TTL:
        return dict(_TASK_STATS_CACHE)

    with _TASK_STATS_LOCK:
        now = time.monotonic()
        if _TASK_STATS_CACHE is not None and now - _TASK_STATS_CACHE_AT < _TASK_STATS_CACHE_TTL:
            return dict(_TASK_STATS_CACHE)
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT status, COUNT(*) count FROM sms_tasks GROUP BY status"
            ).fetchall()
            result = {r["status"]: r["count"] for r in rows}
            result["total"] = sum(result.values())
        finally:
            conn.close()
        _TASK_STATS_CACHE = result
        _TASK_STATS_CACHE_AT = now
        return dict(result)

def retry_task(task_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT id,status,batch_id FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    if row:
        previous = row["status"]
        conn.execute(
            """UPDATE sms_tasks SET status='queued', attempts=0, error_msg='', claim_token='',
                smsc_message_ids='[]', submitted_at=NULL, delivered_at=NULL,
                next_attempt_at=CURRENT_TIMESTAMP WHERE id=?""",
            (task_id,),
        )
        if previous in {"failed", "expired", "cancelled"} and row["batch_id"]:
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=GREATEST(COALESCE(failed_count,0)-1,0),
                       updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (row["batch_id"],),
            )
        _add_task_event(conn, task_id, "manual_retry", previous, "queued", "manual retry requested")
        conn.commit()
    conn.close()
    return bool(row)


def mark_delivered(task_id: int) -> tuple[bool, str]:
    """Manually confirm a submitted task as delivered.

    Once the SMSC accepts a submission (status='submitted') the message is
    almost always sent; if the final delivery receipt is lost or delayed, an
    operator can confirm delivery manually. Only ``submitted`` tasks can be
    confirmed; already-delivered tasks are treated as an idempotent success.
    """
    conn = get_connection()
    row = conn.execute("SELECT * FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        conn.close()
        return False, "Task not found"
    task = dict(row)
    if task["status"] == "delivered":
        conn.close()
        return True, "Task already delivered"
    if task["status"] not in {"submitted", "accepted"}:
        conn.close()
        return False, f"Only submitted or accepted tasks can be confirmed as delivered (current: {task['status']})"
    conn.execute(
        """UPDATE sms_tasks SET status='delivered', failure_class='', error_msg='',
            delivered_at=COALESCE(delivered_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
            updated_at=CURRENT_TIMESTAMP WHERE id=?""",
        (task_id,),
    )
    _add_task_event(conn, task_id, "manual_delivered", task["status"], "delivered",
                    "manually confirmed as delivered")
    if task.get("batch_id"):
        conn.execute(
            """UPDATE batch_campaign_batches
               SET delivered_count=COALESCE(delivered_count,0)+1,
                   status=CASE
                     WHEN task_count>0 AND COALESCE(delivered_count,0)+1>=task_count
                     THEN 'sent' ELSE status END,
                   updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (task["batch_id"],),
        )
    if task.get("campaign_type") == "batch":
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
    return True, "Task confirmed as delivered"


def validate_task_schedule(task: dict) -> tuple[bool, str]:
    task_date = task.get("task_date")
    if task.get("campaign_type") == "realtime" and task_date is not None:
        current_date = datetime.now().astimezone().date().isoformat()
        if str(task_date)[:10] != current_date:
            return False, "task belongs to a previous day"
    campaign = _cached_campaign(None, task["campaign_type"], task["campaign_id"])
    if not campaign:
        return False, "campaign no longer exists"
    return campaign_is_active(campaign)


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
            WHERE status='sending' AND CAST(updated_at AS TIMESTAMP WITH TIME ZONE) < CURRENT_TIMESTAMP - INTERVAL '5 minutes'"""
    )
    conn.execute(
        """UPDATE sms_tasks SET status='expired', error_msg='delivery receipt timeout',
            updated_at=CURRENT_TIMESTAMP
            WHERE status='submitted' AND CAST(submitted_at AS TIMESTAMP WITH TIME ZONE) < CURRENT_TIMESTAMP - INTERVAL '24 hours'"""
    )
    conn.commit()
    conn.close()
