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

from database import get_connection, read_transaction, write_transaction
from services import metrics_service, settings_service

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
_TASK_CLAIM_LOCK = threading.Lock()
_CAMPAIGN_CACHE = {}
_TEMPLATE_CACHE = {}
_CAMPAIGN_CACHE_TTL = 30.0
_CAMPAIGN_CACHE_GENERATION = 0
_TEMPLATE_CACHE_GENERATION = 0

# Keep SQL statements bounded. 2000 rows is inside the requested 1000-5000
# range while leaving room for the task/event columns in DuckDB statements.
DB_INSERT_BATCH_SIZE = 2000


FINAL_STATES = {
    "delivered", "failed", "expired", "cancelled", "archived", "accepted",
    "unknown", "partial",
}

# A normal retry is deliberately narrower than the set of terminal states.
# Once a transport has accepted a submit_sm (or we can no longer prove that it
# did not), the task must be reconciled instead of being put back on the send
# queue.  Keep this list close to the service boundary so callers cannot make
# the UI/API safety check optional.
MANUAL_RETRY_FAILURE_CLASSES = {
    "transport",
    "window_exhausted",
    "submit_sm_resp",
    "scheduler",
}


class TaskRetryRejected(RuntimeError):
    """A normal manual retry is unsafe for the task's current evidence."""

    def __init__(self, reason: str):
        self.reason = str(reason)
        super().__init__(self.reason)

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
             AND (status IN ('queued','retrying') OR
                  (status='sending' AND NOT EXISTS (
                      SELECT 1 FROM sms_send_attempts a
                      WHERE a.task_id=sms_tasks.id
                        AND a.claim_token=sms_tasks.claim_token
                        AND a.state IN ('intent','authorized','responding','partial')
                  )))""",
        (campaign_type, campaign_id),
    ).fetchone()
    count = int(summary["count"] or 0)
    if not count:
        return 0
    conn.execute(
        """UPDATE sms_tasks SET status='cancelled', error_msg=?, claim_token='',
               updated_at=CURRENT_TIMESTAMP
           WHERE campaign_type=? AND campaign_id=?
             AND (status IN ('queued','retrying') OR
                  (status='sending' AND NOT EXISTS (
                      SELECT 1 FROM sms_send_attempts a
                      WHERE a.task_id=sms_tasks.id
                        AND a.claim_token=sms_tasks.claim_token
                        AND a.state IN ('intent','authorized','responding','partial')
                  )))""",
        (message, campaign_type, campaign_id),
    )
    _add_task_event_batch_summary(
        conn, campaign_type, campaign_id, None,
        "campaign_cancelled", "active", "cancelled", count,
        summary["first_id"], summary["last_id"],
        {"message": message},
    )
    return count


def _insert_message_mappings(conn, rows) -> None:
    # Legacy compatibility index. Never overwrite an older task: a recycled
    # SMSC id must not silently move a late receipt to a newer campaign.
    latest_task_by_id = {}
    for message_id, task_id in rows:
        message_id = str(message_id)
        if message_id:
            latest_task_by_id[message_id] = int(task_id)
    if not latest_task_by_id:
        return
    values = ",".join("(?,?)" for _ in latest_task_by_id)
    params = [value for row in latest_task_by_id.items() for value in row]
    conn.execute(
        f"""INSERT INTO sms_task_message_ids (smsc_message_id, task_id) VALUES {values}
            ON CONFLICT (smsc_message_id) DO NOTHING""",
        params,
    )


def smpp_source_namespace(config_id: Optional[int]) -> str:
    return f"smpp:{int(config_id)}" if config_id is not None else "legacy"


def _insert_message_bindings(conn, rows) -> None:
    unique = {}
    for source_namespace, message_id, task_id, attempt_token, part_no in rows:
        message_id = str(message_id or "")
        if not message_id:
            continue
        key = (
            str(source_namespace or "legacy"), message_id, int(task_id),
            str(attempt_token or ""), int(part_no or 0),
        )
        unique[key] = key
    if not unique:
        return
    values = ",".join("(?,?,?,?,?,CURRENT_TIMESTAMP)" for _ in unique)
    params = [value for row in unique.values() for value in row]
    conn.execute(
        f"""INSERT INTO sms_message_bindings_v2
              (source_namespace,smsc_message_id,task_id,attempt_token,part_no,created_at)
              VALUES {values} ON CONFLICT DO NOTHING""",
        params,
    )
    # Keep the old lookup populated for archive/backward compatibility only.
    _insert_message_mappings(conn, ((row[1], row[2]) for row in unique.values()))


def _mapped_message_ids(conn, task_id: int) -> list[str]:
    rows = conn.execute(
        """SELECT smsc_message_id FROM sms_message_bindings_v2 WHERE task_id=?
           UNION SELECT smsc_message_id FROM sms_task_message_ids WHERE task_id=?""",
        (int(task_id), int(task_id)),
    ).fetchall()
    return [str(row["smsc_message_id"]) for row in rows if row["smsc_message_id"]]


def _insert_message_mapping(conn, task_id: int, message_ids) -> None:
    """Record the latest task that used each SMSC message id.

    SMSC message ids are not guaranteed globally unique (e.g. an SMSC or the
    local simulator can reuse ids after a restart), so the row must point at
    the most recent task instead of being ignored once filled. ``INSERT OR
    REPLACE`` silently does nothing on conflict when the table has a secondary
    index (DuckDB quirk), so an explicit ``ON CONFLICT ... DO UPDATE`` is used.
    """
    _insert_message_mappings(conn, ((mid, task_id) for mid in (message_ids or [])))


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
            if window_start <= window_end:
                if current < window_start:
                    return False, "before daily send window"
                if current > window_end:
                    return False, "after daily send window"
            elif window_end < current < window_start:
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
                batch_id: Optional[int] = None, validate_schedule: bool = True,
                ingestion_lease: Optional[tuple] = None,
                ingestion_chunk: Optional[dict] = None) -> dict:
    if ingestion_chunk and not ingestion_lease:
        raise ValueError("ingestion_chunk requires ingestion_lease")
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
        # Release the DuckDB writer gate before the read-heavy normalization,
        # whitelist join, and Python template expansion below.  The temporary
        # staging table remains available after commit, while task insertion is
        # handled later as a short, separate write transaction.
        conn.commit()
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
        if ingestion_lease:
            # Fence at the actual task insertion boundary, not before
            # potentially slow parsing/template expansion. No file IO in tx.
            from services import realtime_ingestion_state
            conn.execute("BEGIN TRANSACTION")
            realtime_ingestion_state.assert_owned(conn, *ingestion_lease)
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
        if ingestion_chunk:
            realtime_ingestion_state.complete_chunk_in_transaction(
                conn, ingestion_lease[0], ingestion_chunk, result, ingestion_lease[1],
            )
            result["chunk_committed"] = True
        conn.execute("DROP TABLE IF EXISTS ingest_task_stage")
        conn.commit()
        if result["created"] and campaign_type == "realtime":
            metrics_service.inc(
                "realtime_sms_tasks_created_total", result["created"],
                labels={"campaign_id": campaign["id"]},
            )
            metrics_service.set_gauge(
                "realtime_last_task_created_timestamp", time.time(),
                labels={"campaign_id": campaign["id"]},
            )
        # Keep INFO logs bounded.  The created MSISDN list can contain tens of
        # thousands of sensitive values and is not needed to diagnose queue
        # throughput; callers still receive it in the API result when needed.
        logger.info(
            "Tasks enqueued campaign=%s:%s file=%s created=%s duplicate=%s excluded=%s invalid=%s no_template=%s",
            campaign_type, campaign["id"], source_file,
            result["created"], result["duplicate"], result["excluded"],
            result["invalid"], result["no_template"],
        )
        return result
    finally:
        conn.close()


def claim_next_tasks(limit: int = 20) -> list:
    limit = max(1, int(limit or 20))
    # DuckDB allows concurrent readers, but concurrent UPDATE ... RETURNING
    # claims can conflict before the explicit commit when many task workers
    # select the same queue head. Serialize only this short claim transaction
    # and retry conflicts caused by unrelated writers.
    with _TASK_CLAIM_LOCK:
        token = uuid.uuid4().hex

        def claim(conn):
            from services import campaign_capacity_service
            if campaign_capacity_service.enabled():
                per_campaign = campaign_capacity_service.per_campaign_candidates()
                candidate_limit = (
                    per_campaign * campaign_capacity_service.active_campaign_limit()
                )
                candidates = conn.execute(
                    """WITH active_groups AS (
                           SELECT campaign_type,campaign_id,COALESCE(smpp_config_id,0) route_id,
                                  MIN(created_at) oldest
                           FROM sms_tasks
                           WHERE status IN ('queued','retrying')
                             AND CAST(next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                             AND (campaign_type <> 'realtime' OR task_date=CURRENT_DATE)
                           GROUP BY campaign_type,campaign_id,COALESCE(smpp_config_id,0)
                           ORDER BY oldest,campaign_type,campaign_id,route_id
                           LIMIT ?
                       ), ranked AS (
                           SELECT t.*,COALESCE(t.smpp_config_id,0) target_smpp_config_id,
                                  COALESCE(p.weight,1.0) scheduling_weight,
                                  p.max_parts_tps,p.max_outstanding_parts,
                                  ROW_NUMBER() OVER (
                                    PARTITION BY t.campaign_type,t.campaign_id,
                                                 COALESCE(t.smpp_config_id,0)
                                    ORDER BY t.created_at,t.id
                                  ) fair_row
                           FROM sms_tasks t
                           JOIN active_groups g
                             ON g.campaign_type=t.campaign_type
                            AND g.campaign_id=t.campaign_id
                            AND g.route_id=COALESCE(t.smpp_config_id,0)
                           LEFT JOIN campaign_capacity_policies p
                             ON p.campaign_type=t.campaign_type
                            AND p.campaign_id=t.campaign_id AND p.enabled=1
                           WHERE t.status IN ('queued','retrying')
                             AND CAST(t.next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                             AND (t.campaign_type <> 'realtime' OR t.task_date=CURRENT_DATE)
                       )
                       SELECT * EXCLUDE(fair_row) FROM ranked
                       WHERE fair_row<=?
                       ORDER BY created_at,id LIMIT ?""",
                    (
                        campaign_capacity_service.active_campaign_limit(),
                        per_campaign,
                        candidate_limit,
                    ),
                ).fetchall()
                scheduled = campaign_capacity_service.select(
                    (dict(row) for row in candidates), limit
                )
                selected_ids = [int(task["id"]) for task in scheduled]
            else:
                selected_ids = [
                    int(row["id"]) for row in conn.execute(
                        """SELECT id FROM sms_tasks
                           WHERE status IN ('queued','retrying')
                             AND CAST(next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                             AND (campaign_type <> 'realtime' OR task_date=CURRENT_DATE)
                           ORDER BY created_at,id LIMIT ?""",
                        (limit,),
                    ).fetchall()
                ]
            if not selected_ids:
                return []
            placeholders = ",".join("?" for _ in selected_ids)
            claimed_rows = conn.execute(
                f"""UPDATE sms_tasks SET status='sending', attempts=attempts+1,
                       claim_token=?,updated_at=CURRENT_TIMESTAMP
                    WHERE id IN ({placeholders}) AND status IN ('queued','retrying')
                    RETURNING *""",
                [token, *selected_ids],
            ).fetchall()
            claimed_by_id = {int(row["id"]): dict(row) for row in claimed_rows}
            # DuckDB UPDATE RETURNING order is not a scheduling contract.
            tasks = [claimed_by_id[task_id] for task_id in selected_ids if task_id in claimed_by_id]
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
            return tasks

        return write_transaction(claim, attempts=3, operation="sms_tasks.claim")


def claim_next_task() -> Optional[dict]:
    tasks = claim_next_tasks(1)
    return tasks[0] if tasks else None


def apply_buffered_writes(conn, items) -> None:
    """Apply one buffered delivery batch inside a single database transaction."""
    # Persist intents and per-part facts before folding the task-level result.
    # All changes share the caller's explicit transaction and event-id fence.
    for item in items:
        if item.kind == "send_intent":
            _apply_send_intent(conn, **item.payload)
        elif item.kind == "part_result":
            _apply_part_result(conn, **item.payload)
    # The buffer polls its per-kind queues fairly, so message_id entries can be
    # interleaved with submitted/receipt entries. Collect them across the whole
    # transaction; otherwise each interleaved item breaks the batch and turns
    # bulk id persistence back into one SELECT/UPDATE/UPSERT per message.
    message_id_items = [item for item in items if item.kind == "message_id"]
    submitted_payloads = [item.payload for item in items if item.kind == "submitted"]

    # When delivery receipts are disabled, submit_sm_resp is still processed in
    # memory by the SMPP gateway, but its successful task finalization does not
    # need one UPDATE statement per task.  Keep the durable accepted state and
    # campaign counters, while applying all disabled-mode successes with one
    # set-based UPDATE below.  Explicit submit failures continue through the
    # normal send_failure path and remain durable immediately in this batch.
    payloads_by_task = {}
    for payload in submitted_payloads:
        payloads_by_task.setdefault(int(payload["task_id"]), []).append(payload)
    disabled_task_ids = {
        task_id for task_id, task_payloads in payloads_by_task.items()
        if task_payloads and all(
            (payload.get("delivery_receipt_mode") or "optional") == "disabled"
            for payload in task_payloads
        )
    }
    disabled_payloads = [
        payload for payload in submitted_payloads
        if int(payload["task_id"]) in disabled_task_ids
    ]
    regular_payloads = [
        payload for payload in submitted_payloads
        if int(payload["task_id"]) not in disabled_task_ids
    ]
    regular_message_id_items = [
        item for item in message_id_items
        if int(item.payload["task_id"]) not in disabled_task_ids
    ]

    if disabled_payloads:
        _apply_disabled_submitted_batch(conn, disabled_payloads)
    if regular_payloads:
        # submit_sm_resp callbacks enqueue each part's message id immediately
        # for crash recovery; the later submitted event contains those same ids.
        # Combine both sources so a successful part reaches the indexed mapping
        # table only once per transaction.
        _apply_submitted_batch(conn, regular_payloads, regular_message_id_items)
    elif regular_message_id_items:
        _apply_message_id_batch(conn, regular_message_id_items)

    for item in items:
        payload = item.payload
        if item.kind in {"message_id", "submitted", "send_intent", "part_result"}:
            continue
        if item.kind == "send_failure":
            _apply_send_failure(conn, **payload)
        elif item.kind == "receipt":
            _apply_receipt(conn, **payload)
        else:
            raise ValueError(f"unknown SMS buffered write kind: {item.kind}")


def record_send_intent(task: dict, smpp_config_id: int, slot: int,
                       planned_parts: int, connection_generation: str = "") -> str:
    """Authorize and durably record an attempt before its first socket write.

    The sender queue is intentionally not an authorization boundary.  This
    transaction is the hand-off fence between the task worker and the SMPP
    sender: it re-reads the current claim and campaign schedule, then creates
    the attempt row while the task is still owned by the same claim.  The
    daily finalizer only cancels ``sending`` tasks without an active attempt,
    so a later callback can still be fenced and persisted.

    An empty return value means the task is no longer allowed to begin an
    external submission.  The caller must not write a socket in that case.
    """
    claim_token = str(task.get("claim_token") or "")
    attempt_token = str(task.get("_attempt_token") or "")
    if not attempt_token:
        attempt_token = f"{claim_token or 'external'}:{uuid.uuid4().hex}"
    source_namespace = smpp_source_namespace(smpp_config_id)

    def authorize(conn):
        row = conn.execute(
            "SELECT * FROM sms_tasks WHERE id=? AND status='sending' AND claim_token=?",
            (int(task["id"]), claim_token),
        ).fetchone()
        if not row:
            return False
        current = dict(row)
        if current.get("campaign_type") == "realtime" and current.get("task_date") is not None:
            if str(current["task_date"])[:10] != datetime.now().astimezone().date().isoformat():
                return False
        table = {"batch": "batch_campaigns", "realtime": "realtime_campaigns"}.get(
            current.get("campaign_type")
        )
        if not table:
            return False
        campaign_row = conn.execute(
            f"SELECT * FROM {table} WHERE id=?", (int(current["campaign_id"]),)
        ).fetchone()
        if not campaign_row:
            return False
        active, _reason = campaign_is_active(dict(campaign_row), datetime.now().astimezone())
        if not active:
            return False

        conn.execute(
            """INSERT INTO sms_send_attempts
                  (attempt_token,task_id,claim_token,source_namespace,smpp_config_id,slot,
                   planned_parts,state,lease_until,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,'authorized',
                       CAST(CURRENT_TIMESTAMP + INTERVAL '5 minutes' AS VARCHAR),
                       CAST(CURRENT_TIMESTAMP AS VARCHAR),CAST(CURRENT_TIMESTAMP AS VARCHAR))
               ON CONFLICT(attempt_token) DO UPDATE SET
                   lease_until=excluded.lease_until,
                   updated_at=excluded.updated_at""",
            (attempt_token, int(task["id"]), claim_token, source_namespace,
             int(smpp_config_id), int(slot), max(1, int(planned_parts))),
        )
        for part_no in range(1, max(1, int(planned_parts)) + 1):
            conn.execute(
                """INSERT INTO sms_send_parts
                      (attempt_token,part_no,connection_generation,source_namespace,state,updated_at)
                   VALUES (?,?,?,?,'planned',CURRENT_TIMESTAMP)
                   ON CONFLICT(attempt_token,part_no) DO NOTHING""",
                (attempt_token, part_no, str(connection_generation or ""), source_namespace),
            )
        _add_task_event(
            conn, int(task["id"]), "submit_authorized", "sending", "sending",
            "SMPP submit authorization acquired before socket write",
            {"claim_token": claim_token, "attempt_token": attempt_token,
             "planned_parts": max(1, int(planned_parts))},
        )
        return True

    if not write_transaction(authorize, attempts=3, operation="sms_task.submit_authorize", base_delay=0.05):
        task["_submit_gate_reason"] = (
            "task claim is no longer owned or the campaign send window is closed"
        )
        task["_submit_gate_denied"] = True
        return ""
    task["_attempt_token"] = attempt_token
    task["_source_namespace"] = source_namespace
    return attempt_token


def _apply_send_intent(conn, task_id: int, claim_token: str, attempt_token: str,
                       source_namespace: str, smpp_config_id: int, slot: int,
                       planned_parts: int, connection_generation: str = "") -> None:
    owner = conn.execute(
        """SELECT id FROM sms_tasks WHERE id=? AND status='sending' AND claim_token=?""",
        (int(task_id), str(claim_token or "")),
    ).fetchone()
    if not owner:
        return
    conn.execute(
        """INSERT INTO sms_send_attempts
              (attempt_token,task_id,claim_token,source_namespace,smpp_config_id,slot,
               planned_parts,state,lease_until,created_at,updated_at)
           VALUES (?,?,?,?,?,?,?,'intent',CAST(CURRENT_TIMESTAMP + INTERVAL '5 minutes' AS VARCHAR),
                    CAST(CURRENT_TIMESTAMP AS VARCHAR),CAST(CURRENT_TIMESTAMP AS VARCHAR))
           ON CONFLICT(attempt_token) DO UPDATE SET
               lease_until=excluded.lease_until,
               updated_at=excluded.updated_at""",
        (str(attempt_token), int(task_id), str(claim_token or ""),
         str(source_namespace or "legacy"), int(smpp_config_id), int(slot),
         max(1, int(planned_parts))),
    )
    for part_no in range(1, max(1, int(planned_parts)) + 1):
        conn.execute(
            """INSERT INTO sms_send_parts
                  (attempt_token,part_no,connection_generation,source_namespace,state,updated_at)
               VALUES (?,?,?,?,'planned',CURRENT_TIMESTAMP)
               ON CONFLICT(attempt_token,part_no) DO NOTHING""",
            (str(attempt_token), part_no, str(connection_generation or ""),
             str(source_namespace or "legacy")),
        )


def record_part_result(task: dict, part_no: int, sequence: int, ok: bool,
                       status: int, message_id: str, smpp_config_id: int,
                       connection_generation: str = "") -> None:
    from services import sms_write_buffer
    # Successful no-DLR responses carry no later correlation value and the
    # task-level accepted event is sufficient.  Keep failures durable so an
    # explicit SMSC rejection can never be hidden by this optimization.
    if ok and (task.get("_delivery_receipt_mode") or "optional") == "disabled":
        return
    sms_write_buffer.submit(
        "part_result",
        {
            "task_id": int(task["id"]),
            "claim_token": str(task.get("claim_token") or ""),
            "attempt_token": str(task.get("_attempt_token") or ""),
            "source_namespace": str(
                task.get("_source_namespace") or smpp_source_namespace(smpp_config_id)
            ),
            "part_no": int(part_no),
            "sequence": int(sequence),
            "ok": bool(ok),
            "status": int(status or 0),
            "message_id": str(message_id or ""),
            "connection_generation": str(connection_generation or ""),
            "persist_message_id":
                (task.get("_delivery_receipt_mode") or "optional") != "disabled",
        },
        wait=False,
    )


def _apply_part_result(conn, task_id: int, claim_token: str, attempt_token: str,
                       source_namespace: str, part_no: int, sequence: int,
                       ok: bool, status: int, message_id: str,
                       connection_generation: str = "",
                       persist_message_id: bool = True) -> None:
    if not attempt_token:
        return
    state = "accepted" if ok else "rejected"
    conn.execute(
        """INSERT INTO sms_send_parts
              (attempt_token,part_no,sequence,connection_generation,source_namespace,
               response_status,smsc_message_id,state,sent_at,responded_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
           ON CONFLICT(attempt_token,part_no) DO UPDATE SET
              sequence=excluded.sequence,
              connection_generation=excluded.connection_generation,
              source_namespace=excluded.source_namespace,
              response_status=excluded.response_status,
              smsc_message_id=excluded.smsc_message_id,
              state=excluded.state,
              responded_at=excluded.responded_at,
              updated_at=excluded.updated_at""",
        (str(attempt_token), int(part_no), int(sequence),
         str(connection_generation or ""), str(source_namespace or "legacy"),
         int(status or 0), str(message_id or ""), state),
    )
    conn.execute(
        """UPDATE sms_send_attempts SET
              accepted_parts=(SELECT COUNT(*) FROM sms_send_parts
                              WHERE attempt_token=? AND state='accepted'),
              failed_parts=(SELECT COUNT(*) FROM sms_send_parts
                            WHERE attempt_token=? AND state='rejected'),
              state=CASE
                WHEN (SELECT COUNT(*) FROM sms_send_parts
                      WHERE attempt_token=? AND state='rejected')>0 AND
                     (SELECT COUNT(*) FROM sms_send_parts
                      WHERE attempt_token=? AND state='accepted')>0 THEN 'partial'
                WHEN (SELECT COUNT(*) FROM sms_send_parts
                      WHERE attempt_token=? AND state='rejected')>0 THEN 'rejected'
                ELSE 'responding' END,
              lease_until=CURRENT_TIMESTAMP + INTERVAL '5 minutes',
              updated_at=CURRENT_TIMESTAMP
           WHERE attempt_token=? AND task_id=? AND claim_token=?""",
        (str(attempt_token), str(attempt_token), str(attempt_token),
         str(attempt_token), str(attempt_token), str(attempt_token),
         int(task_id), str(claim_token or "")),
    )
    if ok and message_id and persist_message_id:
        _insert_message_bindings(conn, [(
            source_namespace, message_id, task_id, attempt_token, part_no
        )])


def _apply_disabled_submitted_batch(conn, payloads) -> None:
    """Finalize no-DLR successes with one set-based task update.

    ``submit_sm_resp`` must still be consumed by the protocol layer so the
    SMPP window is released and explicit failures can be handled.  For a
    configuration that does not request delivery receipts, successful message
    ids have no business value.  This path therefore keeps only the compact
    accepted-task state and aggregates the campaign counters, avoiding the
    per-task UPDATE/JSON work of the general submission path.
    """
    if not payloads:
        return

    # A multipart task can produce several completion callbacks in one flush.
    # Retain the last transport metadata while deduplicating the task update.
    latest = {}
    for payload in payloads:
        latest[int(payload["task_id"])] = payload
    task_ids = list(latest)
    placeholders = ",".join("?" for _ in task_ids)
    before_rows = conn.execute(
        f"""SELECT id,batch_id,campaign_type,campaign_id,status,claim_token
            FROM sms_tasks WHERE id IN ({placeholders})
              AND status IN ('sending','submitted','unknown')""",
        task_ids,
    ).fetchall()
    if not before_rows:
        return

    values = ",".join("(?,?,?,?,?)" for _ in task_ids)
    params = []
    for task_id in task_ids:
        payload = latest[task_id]
        params.extend([
            task_id,
            payload.get("transport_smpp_config_id"),
            payload.get("submit_sm_parts"),
            str(payload.get("claim_token") or ""),
            str(payload.get("attempt_token") or ""),
        ])
    updated = conn.execute(
        f"""UPDATE sms_tasks AS t SET
                status='accepted', smsc_message_ids='[]', error_msg='',
                failure_class='', last_smpp_status=NULL,
                transport_smpp_config_id=COALESCE(i.transport_smpp_config_id,
                                                   t.transport_smpp_config_id),
                submit_sm_parts=CASE WHEN i.submit_sm_parts IS NULL
                                     THEN t.submit_sm_parts
                                     ELSE GREATEST(1,i.submit_sm_parts) END,
                submitted_at=COALESCE(submitted_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
                delivered_at=COALESCE(delivered_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
                updated_at=CURRENT_TIMESTAMP
            FROM (VALUES {values}) AS i(task_id,transport_smpp_config_id,submit_sm_parts,
                                        claim_token,attempt_token)
            WHERE t.id=i.task_id AND t.status IN ('sending','submitted','unknown')
              AND t.claim_token=i.claim_token
            RETURNING t.id""",
        params,
    ).fetchall()
    updated_ids = {int(row["id"]) for row in updated}
    if not updated_ids:
        return

    attempt_updates = [
        (str(latest[task_id].get("attempt_token") or ""), task_id,
         str(latest[task_id].get("claim_token") or ""))
        for task_id in updated_ids
        if latest[task_id].get("attempt_token")
    ]
    if attempt_updates:
        conn.executemany(
            """UPDATE sms_send_attempts SET state='accepted',lease_until=NULL,
               updated_at=CURRENT_TIMESTAMP
               WHERE attempt_token=? AND task_id=? AND claim_token=?""",
            attempt_updates,
        )

    rows_by_id = {int(row["id"]): row for row in before_rows}
    batch_delivered_increments = {}
    daily_sent_increments = {}
    event_groups = {}
    for task_id in updated_ids:
        row = rows_by_id.get(task_id)
        if not row:
            continue
        event_key = (
            row["campaign_type"], row["campaign_id"], row["batch_id"], row["status"]
        )
        event_groups.setdefault(event_key, []).append(task_id)
        if row["batch_id"]:
            batch_id = int(row["batch_id"])
            campaign_id = int(row["campaign_id"])
            batch_delivered_increments[batch_id] = (
                batch_delivered_increments.get(batch_id, 0) + 1
            )
            daily_sent_increments[campaign_id] = (
                daily_sent_increments.get(campaign_id, 0) + 1
            )

    for (campaign_type, campaign_id, batch_id, from_status), task_group in event_groups.items():
        _add_task_event_batch(
            conn, campaign_type, campaign_id, batch_id,
            "submitted", from_status, "accepted", task_group,
            {"buffered": True, "no_delivery_receipt": True, "set_based": True},
        )

    if batch_delivered_increments:
        conn.executemany(
            """UPDATE batch_campaign_batches
               SET delivered_count=COALESCE(delivered_count,0)+?,
                   status=CASE
                     WHEN task_count>0 AND COALESCE(delivered_count,0)+?>=task_count
                     THEN 'sent' ELSE status END,
                   updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            [(count, count, batch_id) for batch_id, count in batch_delivered_increments.items()],
        )
        stats_date = datetime.now().astimezone().strftime("%Y-%m-%d")
        conn.executemany(
            """UPDATE batch_campaign_daily_stats
               SET sent_count=COALESCE(sent_count,0)+?, updated_at=CURRENT_TIMESTAMP
               WHERE campaign_id=? AND stats_date=?""",
            [
                (count, campaign_id, stats_date)
                for campaign_id, count in daily_sent_increments.items()
            ],
        )


def flush_pending_writes(timeout: float = 10.0) -> None:
    from services import sms_write_buffer
    sms_write_buffer.flush(timeout)


def create_external_task(campaign_id: int, campaign_name: str, msisdn: str,
                         language: str, content: str, source_addr: str,
                         payload: Optional[dict] = None) -> dict:
    """Create a directly-consumed task so all SMS paths share task accounting."""
    conn = get_connection()
    try:
        row = conn.execute(
            """INSERT INTO sms_tasks
               (campaign_type,campaign_id,campaign_name,msisdn,language,content,
                source_addr,payload,source_file,dedupe_key,status,attempts,max_attempts)
               VALUES ('rabbitmq',?,?,?,?,?,?,?,'rabbitmq',?,'sending',1,1)
               RETURNING *""",
            (int(campaign_id), campaign_name or "", msisdn or "", language or "",
             content or "", source_addr or "", json.dumps(payload or {}, ensure_ascii=False),
             f"rabbitmq:{uuid.uuid4().hex}"),
        ).fetchone()
        conn.commit()
        return dict(row)
    finally:
        conn.close()


def mark_submitted(task_id: int, message_ids: list[str], delivery_receipt_mode: str = "optional",
                   wait: bool = True,
                   transport_smpp_config_id: Optional[int] = None,
                   submit_sm_parts: Optional[int] = None,
                   claim_token: str = "", attempt_token: str = "",
                   source_namespace: str = ""):
    from services import sms_write_buffer
    stored_message_ids = list(message_ids or []) if delivery_receipt_mode != "disabled" else []
    sms_write_buffer.submit(
        "submitted",
        {"task_id": task_id, "message_ids": stored_message_ids,
         "delivery_receipt_mode": delivery_receipt_mode,
         "transport_smpp_config_id": transport_smpp_config_id,
         "submit_sm_parts": submit_sm_parts,
         "claim_token": str(claim_token or ""),
         "attempt_token": str(attempt_token or ""),
         "source_namespace": str(
             source_namespace or smpp_source_namespace(transport_smpp_config_id)
         )},
        wait=wait,
    )


def _apply_submitted(conn, task_id: int, message_ids: list[str],
                     delivery_receipt_mode: str = "optional",
                     transport_smpp_config_id: Optional[int] = None,
                     submit_sm_parts: Optional[int] = None,
                     claim_token: str = "", attempt_token: str = "",
                     source_namespace: str = ""):
    _apply_submitted_batch(conn, [{
        "task_id": task_id, "message_ids": message_ids,
        "delivery_receipt_mode": delivery_receipt_mode,
        "transport_smpp_config_id": transport_smpp_config_id,
        "submit_sm_parts": submit_sm_parts,
        "claim_token": claim_token,
        "attempt_token": attempt_token,
        "source_namespace": source_namespace or smpp_source_namespace(transport_smpp_config_id),
    }])


def _apply_submitted_batch(conn, payloads, message_id_items=()) -> None:
    """Apply submission outcomes while consolidating secondary writes per batch."""
    if not payloads:
        return

    task_ids = list(dict.fromkeys(int(payload["task_id"]) for payload in payloads))
    placeholders = ",".join("?" for _ in task_ids)
    rows = conn.execute(
        f"""SELECT id,smsc_message_ids,batch_id,campaign_type,campaign_id,status,claim_token
            FROM sms_tasks WHERE id IN ({placeholders})""",
        task_ids,
    ).fetchall()
    rows_by_id = {int(row["id"]): row for row in rows}
    grouped = {}
    for payload in payloads:
        task_id = int(payload["task_id"])
        grouped.setdefault(task_id, []).append(payload)

    # Preserve the historical ordering: standalone response ids were upserted
    # first, then submitted outcomes. If an SMSC reuses an id, the later
    # submitted mapping still wins, while both sources share one SQL upsert.
    mapping_rows = []
    for item in message_id_items:
        message_payload = item.payload
        message_id = str(message_payload.get("message_id") or "")
        if message_id:
            mapping_rows.append((
                str(message_payload.get("source_namespace") or "legacy"),
                message_id, int(message_payload["task_id"]),
                str(message_payload.get("attempt_token") or ""),
                int(message_payload.get("part_no") or 0),
            ))
    pending_task_by_message_id = {}
    batch_delivered_increments = {}
    daily_sent_increments = {}
    event_groups = {}
    for task_id, task_payloads in grouped.items():
        row = rows_by_id.get(task_id)
        existing_ids = []
        if row:
            try:
                existing_ids = json.loads(row["smsc_message_ids"] or "[]")
            except (TypeError, json.JSONDecodeError):
                existing_ids = []

        # Multiple outcomes for the same task in one flush are unusual, but
        # merge their message ids and retain the last supplied configuration.
        message_ids = list(existing_ids)
        for payload in task_payloads:
            message_ids.extend(payload.get("message_ids") or [])
        message_ids = list(dict.fromkeys(str(mid) for mid in message_ids if str(mid)))
        payload = task_payloads[-1]
        mode = payload.get("delivery_receipt_mode")
        mode = mode if mode in {"required", "optional", "disabled"} else "optional"
        final_status = "submitted" if mode == "required" else "accepted"
        source_namespace = str(
            payload.get("source_namespace")
            or smpp_source_namespace(payload.get("transport_smpp_config_id"))
        )
        attempt_token = str(payload.get("attempt_token") or "")
        updated = conn.execute(
            """UPDATE sms_tasks SET status=?, smsc_message_ids=?, error_msg='',
                failure_class='', last_smpp_status=NULL,
                transport_smpp_config_id=COALESCE(?,transport_smpp_config_id),
                submit_sm_parts=CASE WHEN ? IS NULL THEN submit_sm_parts ELSE GREATEST(1,?) END,
                submitted_at=COALESCE(submitted_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
                delivered_at=CASE WHEN ? <> 'required'
                                  THEN COALESCE(delivered_at,CAST(CURRENT_TIMESTAMP AS VARCHAR))
                                  ELSE delivered_at END,
                updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND status IN ('sending','submitted','unknown')
                  AND claim_token=?
                RETURNING id""",
            (final_status, json.dumps(message_ids),
             payload.get("transport_smpp_config_id"), payload.get("submit_sm_parts"),
             payload.get("submit_sm_parts"), mode, task_id,
             str(payload.get("claim_token") or "")),
        ).fetchone()
        if not updated:
            continue
        # Only the currently owned claim may publish message-id mappings.
        # A delayed callback from an expired attempt must not make an old SMSC
        # response addressable as if it belonged to the replacement attempt.
        mapping_rows.extend(
            (source_namespace, message_id, task_id, attempt_token, part_no)
            for part_no, message_id in enumerate(message_ids, 1)
        )
        pending_task_by_message_id.update(
            ((source_namespace, message_id), task_id) for message_id in message_ids
        )
        if row:
            event_key = (
                row["campaign_type"], row["campaign_id"], row["batch_id"],
                row["status"], final_status,
            )
            event_groups.setdefault(event_key, []).append(task_id)
        if attempt_token:
            conn.execute(
                """UPDATE sms_send_attempts SET state='accepted',
                   lease_until=NULL,updated_at=CURRENT_TIMESTAMP
                   WHERE attempt_token=? AND task_id=? AND claim_token=?""",
                (attempt_token, task_id, str(payload.get("claim_token") or "")),
            )
        if final_status == "accepted" and row and row["batch_id"]:
            batch_id = int(row["batch_id"])
            campaign_id = int(row["campaign_id"])
            batch_delivered_increments[batch_id] = batch_delivered_increments.get(batch_id, 0) + 1
            daily_sent_increments[campaign_id] = daily_sent_increments.get(campaign_id, 0) + 1

    for (campaign_type, campaign_id, batch_id, from_status, to_status), task_group in event_groups.items():
        _add_task_event_batch(
            conn, campaign_type, campaign_id, batch_id,
            "submitted", from_status, to_status, task_group,
            {"buffered": True, "attempt_fenced": True},
        )

    _insert_message_bindings(conn, mapping_rows)
    if batch_delivered_increments:
        conn.executemany(
            """UPDATE batch_campaign_batches
               SET delivered_count=COALESCE(delivered_count,0)+?,
                   status=CASE
                     WHEN task_count>0 AND COALESCE(delivered_count,0)+?>=task_count
                     THEN 'sent' ELSE status END,
                   updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            [(count, count, batch_id) for batch_id, count in batch_delivered_increments.items()],
        )
        stats_date = datetime.now().astimezone().strftime("%Y-%m-%d")
        conn.executemany(
            """UPDATE batch_campaign_daily_stats
               SET sent_count=COALESCE(sent_count,0)+?, updated_at=CURRENT_TIMESTAMP
               WHERE campaign_id=? AND stats_date=?""",
            [(count, campaign_id, stats_date)
             for campaign_id, count in daily_sent_increments.items()],
        )

    # Receipts may arrive before submit_sm_resp. Preserve that reconciliation,
    # but query all submitted ids together instead of once per task.
    ids_by_source = {}
    for source_namespace, message_id in pending_task_by_message_id:
        ids_by_source.setdefault(source_namespace, []).append(message_id)
    for source_namespace, message_ids in ids_by_source.items():
        for offset in range(0, len(message_ids), 500):
            id_chunk = message_ids[offset:offset + 500]
            id_placeholders = ",".join("?" for _ in id_chunk)
            pending_receipts = conn.execute(
                f"""SELECT smsc_message_id,message_state,raw_receipt,source_namespace
                    FROM delivery_receipts
                    WHERE task_id IS NULL AND source_namespace=?
                      AND smsc_message_id IN ({id_placeholders})""",
                [source_namespace, *id_chunk],
            ).fetchall()
            matched = []
            for receipt in pending_receipts:
                message_id = str(receipt["smsc_message_id"])
                task_id = pending_task_by_message_id.get((source_namespace, message_id))
                if task_id is None:
                    continue
                matched.append((task_id, source_namespace, message_id))
                _apply_receipt(
                    conn, message_id, receipt["message_state"], receipt["raw_receipt"],
                    task_id=task_id, skip_insert=True,
                    source_namespace=source_namespace,
                )
            if matched:
                conn.executemany(
                    """UPDATE delivery_receipts SET task_id=? WHERE task_id IS NULL
                       AND source_namespace=? AND smsc_message_id=?""",
                    matched,
                )


def append_message_id(task_id: int, message_id: str, wait: bool = True,
                      source_namespace: str = "legacy", attempt_token: str = "",
                      part_no: int = 0):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "message_id", {"task_id": task_id, "message_id": message_id,
                       "source_namespace": source_namespace,
                       "attempt_token": attempt_token, "part_no": part_no}, wait=wait
    )


def _apply_message_id(conn, task_id: int, message_id: str):
    _apply_message_id_batch(conn, [{"payload": {"task_id": task_id, "message_id": message_id}}])


def _apply_message_id_batch(conn, items) -> None:
    """Persist SMSC ids in the indexed lookup table as one batch.

    The task's JSON snapshot is written once when submission succeeds or fails;
    updating that wide task row for every response part creates avoidable DuckDB
    row rewrites on the hottest write path.
    """
    mapping_candidates = []
    for item in items:
        payload = item.payload
        task_id = int(payload["task_id"])
        message_id = str(payload.get("message_id") or "")
        if message_id:
            mapping_candidates.append((
                str(payload.get("source_namespace") or "legacy"), message_id, task_id,
                str(payload.get("attempt_token") or ""), int(payload.get("part_no") or 0),
            ))
    _insert_message_bindings(conn, mapping_candidates)


def mark_send_failure(task: dict, error: str, status_code: Optional[int] = None,
                      retryable: bool = True, failure_class: str = "transport",
                      wait: bool = True):
    from services import sms_write_buffer
    durable_task = {
        key: task.get(key) for key in (
            "id", "attempts", "max_attempts", "batch_id", "campaign_type",
            "campaign_id", "status", "claim_token", "smsc_message_ids",
            "_attempt_token", "_source_namespace", "_submit_sm_parts",
            "_transport_smpp_config_id", "_accepted_parts",
        )
    }
    sms_write_buffer.submit(
        "send_failure",
        {"task": durable_task, "error": str(error), "status_code": status_code,
         "retryable": retryable, "failure_class": failure_class},
        wait=wait,
    )


def _apply_send_failure(conn, task: dict, error: str, status_code: Optional[int] = None,
                        retryable: bool = True, failure_class: str = "transport"):
    attempt_token = str(task.get("_attempt_token") or "")
    accepted_parts = int(task.get("_accepted_parts") or 0)
    if attempt_token:
        part_row = conn.execute(
            """SELECT COUNT(*) count FROM sms_send_parts
               WHERE attempt_token=? AND state='accepted'""",
            (attempt_token,),
        ).fetchone()
        accepted_parts = max(accepted_parts, int(part_row["count"] or 0))
    if accepted_parts:
        status = "partial"
        failure_class = "partial_submit"
        retry = False
    elif failure_class == "delivery_unknown" or status_code == -1:
        status = "unknown"
        failure_class = "delivery_unknown"
        retry = False
    else:
        retry = retryable and int(task.get("attempts") or 0) < int(task.get("max_attempts") or 3)
        status = "retrying" if retry else "failed"
    delay_minutes = min(30, 2 ** max(0, task["attempts"] - 1))
    try:
        existing_ids = json.loads(task.get("smsc_message_ids") or "[]")
    except (TypeError, json.JSONDecodeError):
        existing_ids = []
    message_ids = list(dict.fromkeys(
        [str(value) for value in existing_ids if str(value)]
        + _mapped_message_ids(conn, task["id"])
    ))
    # Recovery may have isolated this same claim while the durable result was
    # waiting for the writer. Only a definite rejection or known partial send
    # can reconcile unknown: another transport timeout must not permit resend.
    reconcile_unknown = status != "unknown" and (
        accepted_parts > 0 or (status_code is not None and status_code > 0)
    )
    before = conn.execute(
        "SELECT status FROM sms_tasks WHERE id=? AND claim_token=?",
        (task["id"], str(task.get("claim_token") or "")),
    ).fetchone()
    if not before:
        return
    updated = conn.execute(
        f"""UPDATE sms_tasks SET status=?, error_msg=?, last_smpp_status=?, failure_class=?,
            smsc_message_ids=?,
            next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '{delay_minutes} minutes',
            updated_at=CURRENT_TIMESTAMP WHERE id=?
              AND (status='sending' OR (status='unknown' AND ?))
              AND claim_token=?
            RETURNING id""",
        (status, str(error)[:2000], status_code, failure_class,
          json.dumps(message_ids), task["id"], reconcile_unknown,
          str(task.get("claim_token") or "")),
    ).fetchone()
    if updated:
        _add_task_event(conn, task["id"], "send_failure", before["status"], status,
                        str(error)[:500], {"smpp_status": status_code, "failure_class": failure_class})
        if attempt_token:
            conn.execute(
                """UPDATE sms_send_attempts SET state=?,lease_until=NULL,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE attempt_token=? AND task_id=? AND claim_token=?""",
                (status, attempt_token, int(task["id"]),
                 str(task.get("claim_token") or "")),
            )
        if status in {"failed", "partial", "unknown"} and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=COALESCE(failed_count,0)+1,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (task["batch_id"],),
            )


def requeue_transport_failures() -> int:
    """Put only proven pre-submit transport failures back after recovery.

    This recovery path is intentionally separate from manual retry, but it
    still uses the same evidence fence: no SMSC id, no submit/delivery time,
    and no durable attempt that has started responding.  Ambiguous failures
    remain ``unknown`` and are never requeued here.
    """
    conn = get_connection()
    count = conn.execute(
        """SELECT COUNT(*) count FROM sms_tasks
           WHERE status='failed' AND failure_class='transport'
             AND COALESCE(smsc_message_ids,'[]')='[]'
             AND submitted_at IS NULL AND delivered_at IS NULL
             AND NOT EXISTS (
                 SELECT 1 FROM sms_send_attempts a
                 WHERE a.task_id=sms_tasks.id
                   AND a.state IN ('authorized','responding','partial','accepted')
             )"""
    ).fetchone()["count"]
    if count:
        by_batch = conn.execute(
            """SELECT batch_id,COUNT(*) count FROM sms_tasks
               WHERE status='failed' AND failure_class='transport'
                 AND COALESCE(smsc_message_ids,'[]')='[]'
                 AND submitted_at IS NULL AND delivered_at IS NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM sms_send_attempts a
                     WHERE a.task_id=sms_tasks.id
                       AND a.state IN ('authorized','responding','partial','accepted')
                 )
                 AND batch_id IS NOT NULL
               GROUP BY batch_id"""
        ).fetchall()
        conn.execute(
            """UPDATE sms_tasks SET status='queued', attempts=0, error_msg='',
                claim_token='', last_smpp_status=NULL, next_attempt_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
                WHERE status='failed' AND failure_class='transport'
                  AND COALESCE(smsc_message_ids,'[]')='[]'
                  AND submitted_at IS NULL AND delivered_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM sms_send_attempts a
                      WHERE a.task_id=sms_tasks.id
                        AND a.state IN ('authorized','responding','partial','accepted')
                  )"""
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
                   wait: bool = True, source_namespace: str = "legacy"):
    from services import sms_write_buffer
    sms_write_buffer.submit(
        "receipt",
        {"message_id": str(message_id), "state": str(state), "raw": str(raw),
         "task_id": _task_id, "skip_insert": _skip_insert,
         "source_namespace": str(source_namespace or "legacy")},
        wait=wait,
    )


def _apply_receipt(conn, message_id: str, state: str, raw: str,
                   task_id: Optional[int] = None, skip_insert: bool = False,
                   source_namespace: str = "legacy"):
    normalized_state = str(state or "").upper()
    if task_id is None:
        task = _find_task_by_message_id(conn, message_id, source_namespace)
        task_id = task["id"] if task else None
    else:
        row = conn.execute("SELECT * FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
        task = dict(row) if row else None
    if not skip_insert:
        inserted = conn.execute(
            """INSERT INTO delivery_receipts
               (task_id,source_namespace,smsc_message_id,message_state,raw_receipt)
               VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING RETURNING id""",
            (task_id, str(source_namespace or "legacy"), message_id,
             normalized_state, raw[:4000]),
        ).fetchone()
        if task_id is not None and not inserted:
            return
    if task_id:
        # accepted is a delivered-equivalent terminal state when the SMSC
        # configuration does not require a delivery receipt.
        was_delivered = task.get("status") in {"delivered", "accepted"}
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
            # A delivery receipt for an accepted multipart part must not
            # promote a task that has already been durably classified as
            # partial.  The accepted part can be delivered while other parts
            # were rejected; the business outcome remains partial and must
            # never be rewritten to delivered by the late DLR.
            all_parts_delivered = (
                task.get("status") != "partial"
                and bool(expected_ids)
                and expected_ids.issubset(delivered_ids)
            )
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
        elif status in {"retrying", "failed"} and was_delivered and task.get("batch_id"):
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET delivered_count=GREATEST(COALESCE(delivered_count,0)-1,0),
                       failed_count=CASE WHEN ?='failed'
                         THEN COALESCE(failed_count,0)+1
                         ELSE COALESCE(failed_count,0) END,
                       status=CASE
                         WHEN status='sent' AND task_count>0
                              AND GREATEST(COALESCE(delivered_count,0)-1,0)<task_count
                         THEN 'in_progress' ELSE status END,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (status, task["batch_id"]),
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
        elif status in {"retrying", "failed"} and was_delivered and task.get("campaign_type") == "batch":
            stats_date = datetime.now().astimezone().strftime("%Y-%m-%d")
            existing = conn.execute(
                "SELECT id FROM batch_campaign_daily_stats WHERE campaign_id=? AND stats_date=?",
                (task["campaign_id"], stats_date),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE batch_campaign_daily_stats SET sent_count=GREATEST(sent_count-1,0), updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (existing["id"],),
                )


def _find_task_by_message_id(conn, message_id: str,
                             source_namespace: str = "legacy") -> Optional[dict]:
    """Resolve a receipt only when its SMSC-scoped id has one task owner."""
    rows = conn.execute(
        """SELECT DISTINCT t.* FROM sms_message_bindings_v2 m
           JOIN sms_tasks t ON t.id=m.task_id
           WHERE m.source_namespace=? AND m.smsc_message_id=?
           ORDER BY t.id DESC LIMIT 2""",
        (str(source_namespace or "legacy"), str(message_id)),
    ).fetchall()
    if len(rows) == 1:
        return dict(rows[0])
    if len(rows) > 1:
        logger.error(
            "Ambiguous SMPP receipt source=%s message_id=%s candidates=%s",
            source_namespace, message_id, [row["id"] for row in rows],
        )
        return None
    # Compatibility for mappings created before source namespaces existed.
    if str(source_namespace or "legacy") == "legacy":
        row = conn.execute(
            """SELECT t.* FROM sms_task_message_ids m
               JOIN sms_tasks t ON t.id=m.task_id
               WHERE m.smsc_message_id=? LIMIT 1""",
            (str(message_id),),
        ).fetchone()
        return dict(row) if row else None
    return None


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
    def read(conn):
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
        return total, rows

    total, rows = read_transaction(read, operation="sms_tasks.list")
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
    def read(conn):
        total = _cached_exclusion_count(conn)
        rows = conn.execute(
            "SELECT id,msisdn,reason,enabled,created_at FROM exclusion_users ORDER BY id DESC LIMIT ? OFFSET ?",
            (page_size, max(0, page - 1) * page_size),
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="sms_tasks.exclusions.list")
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
                """SELECT status, COUNT(*) AS count,
                          SUM(CASE WHEN CAST(created_at AS DATE)=CURRENT_DATE THEN 1 ELSE 0 END) AS today_count
                   FROM sms_tasks
                   GROUP BY status"""
            ).fetchall()
            raw = {r["status"]: int(r["count"] or 0) for r in rows}
            today_raw = {r["status"]: int(r["today_count"] or 0) for r in rows}
            result = dict(raw)
            for status in ("queued", "sending", "submitted", "accepted", "delivered",
                           "retrying", "failed", "partial", "unknown", "expired",
                           "cancelled", "archived"):
                result.setdefault(status, 0)
            # Keep delivery semantics strict. ``accepted`` means the SMSC
            # accepted submit_sm; it is not a handset delivery receipt.
            result["accepted_or_delivered"] = raw.get("accepted", 0) + raw.get("delivered", 0)
            result["today_total"] = sum(today_raw.values())
            for status in ("queued", "sending", "submitted", "accepted", "delivered",
                           "retrying", "failed", "partial", "unknown", "expired",
                           "cancelled", "archived"):
                result[f"today_{status}"] = today_raw.get(status, 0)
            result["today_accepted_or_delivered"] = (
                today_raw.get("accepted", 0) + today_raw.get("delivered", 0)
            )
            result["total"] = sum(raw.values())
        finally:
            conn.close()
        _TASK_STATS_CACHE = result
        _TASK_STATS_CACHE_AT = now
        return dict(result)

def retry_task(task_id: int) -> bool:
    def write(conn):
        row = conn.execute(
            """SELECT id,status,batch_id,failure_class,smsc_message_ids,attempts,
                      submitted_at,delivered_at,claim_token
                 FROM sms_tasks WHERE id=?""", (task_id,)
        ).fetchone()
        if not row:
            return False
        previous = row["status"]
        failure_class = str(row["failure_class"] or "")
        if previous != "failed":
            raise TaskRetryRejected(
                f"normal retry is not allowed for status '{previous}'; "
                "only a definite pre-submit failure may be retried"
            )
        if failure_class not in MANUAL_RETRY_FAILURE_CLASSES:
            raise TaskRetryRejected(
                f"failure class '{failure_class or 'unknown'}' is not a definite pre-submit failure"
            )
        try:
            message_ids = json.loads(row["smsc_message_ids"] or "[]")
        except (TypeError, json.JSONDecodeError):
            message_ids = []
        if message_ids or row["submitted_at"] or row["delivered_at"]:
            raise TaskRetryRejected(
                "submission evidence exists; reconcile the attempt instead of using normal retry"
            )

        # The status/failure-class/message-id predicates form the CAS.  A
        # concurrent DLR, callback, or second retry therefore wins or loses
        # atomically; it cannot clear metadata from the newer state.
        updated = conn.execute(
            """UPDATE sms_tasks SET status='queued', attempts=0,
                    error_msg='manual retry requested after definite failure',
                    claim_token='',next_attempt_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND status='failed' AND failure_class=?
                  AND COALESCE(smsc_message_ids,'[]')='[]'
                  AND submitted_at IS NULL AND delivered_at IS NULL
                RETURNING id""",
            (task_id, failure_class),
        ).fetchone()
        if not updated:
            raise TaskRetryRejected(
                "task changed while retry was being requested; refresh before retrying"
            )
        if row["batch_id"]:
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET failed_count=GREATEST(COALESCE(failed_count,0)-1,0),
                       updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (row["batch_id"],),
            )
        _add_task_event(
            conn, task_id, "manual_retry", previous, "queued",
            "manual retry requested after definite pre-submit failure",
            {"previous_failure_class": failure_class, "previous_attempts": row["attempts"]},
        )
        return True

    return write_transaction(
        write,
        attempts=3,
        operation="sms_task.retry",
        base_delay=0.1,
    )


def mark_delivered(task_id: int) -> tuple[bool, str]:
    def write(conn):
        row = conn.execute(
            "SELECT * FROM sms_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if not row:
            return False, "Task not found"
        task = dict(row)
        if task["status"] == "delivered":
            return True, "Task already delivered"
        if task["status"] not in {"submitted", "accepted"}:
            return False, (
                "Only submitted or accepted tasks can be confirmed as delivered "
                f"(current: {task['status']})"
            )
        conn.execute(
            """UPDATE sms_tasks SET status='delivered', failure_class='', error_msg='',
                delivered_at=COALESCE(delivered_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)),
                updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (task_id,),
        )
        _add_task_event(
            conn, task_id, "manual_delivered", task["status"], "delivered",
            "manually confirmed as delivered",
        )
        if task.get("batch_id") and task["status"] not in {"delivered", "accepted"}:
            conn.execute(
                """UPDATE batch_campaign_batches
                   SET delivered_count=COALESCE(delivered_count,0)+1,
                       status=CASE
                         WHEN task_count>0 AND COALESCE(delivered_count,0)+1>=task_count
                         THEN 'sent' ELSE status END,
                       updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (task["batch_id"],),
            )
        if task.get("campaign_type") == "batch" and task["status"] not in {"delivered", "accepted"}:
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
        return True, "Task confirmed as delivered"

    return write_transaction(
        write,
        attempts=3,
        operation="sms_task.mark_delivered",
        base_delay=0.1,
    )


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


def defer_task(task_id: int, reason: str, expired: bool = False,
               claim_token: str = ""):
    def write(conn):
        if expired:
            conn.execute(
                """UPDATE sms_tasks SET status='expired', error_msg=?,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND status='sending' AND claim_token=?""",
                (reason, task_id, str(claim_token or "")),
            )
        else:
            conn.execute(
                """UPDATE sms_tasks SET status='queued', attempts=GREATEST(attempts-1,0),
                    error_msg=?,next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '5 minutes',
                    updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='sending' AND claim_token=?""",
                (reason, task_id, str(claim_token or "")),
            )

    write_transaction(write, operation="sms_tasks.defer")


def recover_stale_tasks():
    def recover(conn):
        stale_rows = conn.execute(
            """SELECT id,campaign_type,campaign_id,batch_id,claim_token,smsc_message_ids
               FROM sms_tasks WHERE status='sending'
                 AND CAST(updated_at AS TIMESTAMP WITH TIME ZONE) <
                     CURRENT_TIMESTAMP - INTERVAL '5 minutes'"""
        ).fetchall()
        recovered = 0
        for row in stale_rows:
            mapped = _mapped_message_ids(conn, int(row["id"]))
            try:
                existing = json.loads(row["smsc_message_ids"] or "[]")
            except (TypeError, json.JSONDecodeError):
                existing = []
            all_ids = list(dict.fromkeys(
                [str(value) for value in existing if str(value)] + mapped
            ))
            updated = conn.execute(
                """UPDATE sms_tasks SET status='unknown',failure_class='delivery_unknown',
                    error_msg='worker interrupted; delivery state requires reconciliation',
                    smsc_message_ids=?,updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='sending' AND claim_token=? RETURNING id""",
                (json.dumps(all_ids), int(row["id"]), str(row["claim_token"] or "")),
            ).fetchone()
            if not updated:
                continue
            recovered += 1
            conn.execute(
                """UPDATE sms_send_attempts SET state='unknown',lease_until=NULL,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE task_id=? AND claim_token=? AND state NOT IN ('accepted','partial')""",
                (int(row["id"]), str(row["claim_token"] or "")),
            )
            _add_task_event(
                conn, int(row["id"]), "recovered_unknown", "sending", "unknown",
                "stale sending attempt isolated; not automatically retried",
                {"claim_token": str(row["claim_token"] or "")},
            )
        conn.execute(
            """UPDATE sms_tasks SET status='expired', error_msg='delivery receipt timeout',
                updated_at=CURRENT_TIMESTAMP
                WHERE status='submitted' AND CAST(submitted_at AS TIMESTAMP WITH TIME ZONE) <
                    CURRENT_TIMESTAMP - INTERVAL '24 hours'"""
        )
        return recovered

    return write_transaction(recover, operation="sms_tasks.recover_stale")
