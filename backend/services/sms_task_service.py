import hashlib
import json
import logging
import os
import re
import threading
import uuid
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Iterable, Optional

from database import get_connection, read_transaction, write_transaction
from services import metrics_service, settings_service
from vnext.business_day import business_timezone

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
_FAIR_DISABLED_WARNING_EMITTED = False

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
    def read(conn):
        return conn.execute(f"SELECT * FROM {table} WHERE id=?", (campaign_id,)).fetchone()

    row = read_transaction(read, operation="sms_tasks.campaign_cache") if conn is None else read(conn)
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
               "sms_template_id", "smpp_config_id", "flash_sms", "content", "source_addr", "payload",
               "source_file", "task_date", "dedupe_key", "dispatch_generation")
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


def _add_task_event_batch_summaries(conn, summaries) -> None:
    """Insert several aggregated event summaries with one statement.

    Result batches can contain tasks from several campaign batches.  The
    summary rows are already aggregated in Python, so issuing one INSERT per
    group only adds writer-gate round trips without improving correctness.
    """
    rows = []
    for summary in summaries:
        (campaign_type, campaign_id, batch_id, event_type, from_status,
         to_status, event_count, first_task_id, last_task_id, detail) = summary
        if not event_count:
            continue
        rows.append((
            campaign_type, int(campaign_id), batch_id, event_type, from_status,
            to_status, int(event_count), int(first_task_id), int(last_task_id),
            json.dumps(detail or {}, ensure_ascii=False, default=str),
        ))
    if not rows:
        return
    values = ",".join("(?,?,?,?,?,?,?,?,?,?)" for _ in rows)
    conn.execute(
        f"""INSERT INTO task_event_batches
           (campaign_type,campaign_id,batch_id,event_type,from_status,to_status,
            event_count,first_task_id,last_task_id,detail)
           VALUES {values}""",
        [value for row in rows for value in row],
    )


def _observe_result_stage(stage: str, started: float, item_count: int = 0) -> None:
    """Expose bounded, non-sensitive timings for result-batch sub-stages."""
    try:
        metrics_service.observe_histogram(
            "marketing_copilot_sms_result_stage_seconds",
            max(0.0, time.perf_counter() - started),
            labels={"stage": str(stage)},
        )
        metrics_service.observe_histogram(
            "marketing_copilot_sms_result_stage_items",
            max(0, int(item_count or 0)),
            labels={"stage": str(stage)},
            buckets=(1, 10, 50, 100, 250, 500, 1000, 5000),
        )
    except Exception:
        # Observability must never change result durability semantics.
        logger.debug("Failed to record SMS result stage metrics", exc_info=True)


def _unstarted_attempt_predicate(task_alias: str = "sms_tasks") -> str:
    return f"""(
        {task_alias}.status IN ('queued','retrying') OR
        ({task_alias}.status='sending' AND NOT EXISTS (
            SELECT 1 FROM sms_send_attempts a
            WHERE a.task_id={task_alias}.id
              AND a.claim_token={task_alias}.claim_token
              AND a.state IN ('intent','authorized','responding','partial')
        )) OR
        ({task_alias}.status='sending' AND {task_alias}.claim_token='')
    )"""


def _fast_pre_submit_claims(conn, campaign_type: str, campaign_id: int):
    from services import fast_send_service
    if not fast_send_service.is_fast():
        return []
    candidates = conn.execute(
        """SELECT t.id,t.claim_token
           FROM sms_tasks t
           JOIN sms_fast_batch_items fi
             ON fi.task_id=t.id AND fi.claim_token=t.claim_token
           WHERE t.campaign_type=? AND t.campaign_id=? AND t.status='sending'
             AND fi.state='claimed' AND fi.planned_parts=0""",
        (campaign_type, campaign_id),
    ).fetchall()
    return list(fast_send_service.safe_pre_submit_claims(candidates))


def _update_fast_pre_submit_tasks(
    conn, claims, target_status: str, generation: int, message: str,
):
    updated = []
    for task_id, claim_token in claims:
        row = conn.execute(
            """UPDATE sms_tasks SET status=?,
                       attempts=CASE WHEN ?='queued' THEN GREATEST(attempts-1,0) ELSE attempts END,
                       claim_token='',dispatch_generation=?,error_msg=?,
                       next_attempt_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='sending' AND claim_token=?
               RETURNING id""",
            (target_status, target_status, int(generation), str(message or ""),
             int(task_id), str(claim_token)),
        ).fetchone()
        if row:
            updated.append((int(task_id), str(claim_token)))
    return updated


def _close_fast_pre_submit_attempts(conn, claims, item_state: str, message: str):
    for task_id, claim_token in claims:
        conn.execute(
            """UPDATE sms_send_attempts AS a SET state='cancelled',lease_until=NULL,
                       updated_at=CURRENT_TIMESTAMP
               FROM sms_fast_batch_items fi
               WHERE fi.task_id=? AND fi.claim_token=? AND a.attempt_token=fi.attempt_token""",
            (int(task_id), str(claim_token)),
        )
        conn.execute(
            """UPDATE sms_fast_batch_items SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP
               WHERE task_id=? AND claim_token=? AND state='claimed' AND planned_parts=0""",
            (item_state, str(message or ""), int(task_id), str(claim_token)),
        )


def reconcile_campaign_state_in_transaction(
    conn, campaign_type: str, campaign_id: int, target_status: str,
    dispatch_generation: int, message: str = "",
) -> dict:
    """Apply the task side of an activity state transition in the same tx.

    ``dispatch_generation`` is the durable stop fence.  A pause requeues only
    claimed work that has no send attempt, while cancel/complete terminalize
    only that same safe subset.  Authorized/responding/partial attempts are
    deliberately retained for result reconciliation and are never inferred
    to be safe to resend.
    """
    from services import vnext_service
    if vnext_service.is_active():
        # Campaign control is fenced by the VNext control-state file.  There
        # is no legacy sms_tasks claim set to reconcile in this mode.
        return {"requeued": 0, "terminated": 0, "pipeline": "vnext"}
    target_status = str(target_status or "")
    generation = int(dispatch_generation or 0)
    if target_status == "paused":
        unstarted = _unstarted_attempt_predicate()
        rows = conn.execute(
            f"""UPDATE sms_tasks
                SET status='queued', attempts=GREATEST(attempts-1,0),
                    claim_token='', dispatch_generation=?,
                    error_msg=?, next_attempt_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE campaign_type=? AND campaign_id=?
                  AND status='sending' AND {unstarted}
                RETURNING id""",
            (generation, message or "campaign paused", campaign_type, campaign_id),
        ).fetchall()
        # Queued/retrying work is still durable; just move its fence to the
        # new generation so it can resume without a five-minute defer delay.
        conn.execute(
            """UPDATE sms_tasks SET dispatch_generation=?,updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type=? AND campaign_id=? AND status IN ('queued','retrying')""",
            (generation, campaign_type, campaign_id),
        )
        fast_claims = _fast_pre_submit_claims(conn, campaign_type, campaign_id)
        fast_updated = _update_fast_pre_submit_tasks(
            conn, fast_claims, "queued", generation,
            message or "campaign paused",
        )
        count = len(rows) + len(fast_updated)
        if count:
            event_ids = [int(row["id"]) for row in rows]
            event_ids.extend(task_id for task_id, _claim_token in fast_updated)
            _add_task_event_batch_summary(
                conn, campaign_type, campaign_id, None,
                "campaign_paused", "sending", "queued", count,
                min(event_ids), max(event_ids),
                {"message": message or "campaign paused"},
            )
        _close_fast_pre_submit_attempts(
            conn, fast_updated, "deferred", message or "campaign paused",
        )
        return {"requeued": count, "terminated": 0}

    if target_status == "in_progress":
        conn.execute(
            """UPDATE sms_tasks SET dispatch_generation=?,updated_at=CURRENT_TIMESTAMP
               WHERE campaign_type=? AND campaign_id=? AND status IN ('queued','retrying')""",
            (generation, campaign_type, campaign_id),
        )
        return {"requeued": 0, "terminated": 0}

    if target_status not in {"cancelled", "completed", "expired"}:
        return {"requeued": 0, "terminated": 0}

    terminal_status = "cancelled" if target_status == "cancelled" else "expired"
    terminal_message = message or (
        "campaign cancelled" if terminal_status == "cancelled" else "campaign completed"
    )
    unstarted = _unstarted_attempt_predicate()
    rows = conn.execute(
        f"""UPDATE sms_tasks
            SET status=?, error_msg=?, claim_token='', dispatch_generation=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE campaign_type=? AND campaign_id=? AND {unstarted}
            RETURNING id""",
        (terminal_status, terminal_message, generation, campaign_type, campaign_id),
    ).fetchall()
    fast_claims = _fast_pre_submit_claims(conn, campaign_type, campaign_id)
    fast_target = "cancelled" if terminal_status == "cancelled" else "expired"
    fast_updated = _update_fast_pre_submit_tasks(
        conn, fast_claims, fast_target, generation, terminal_message,
    )
    count = len(rows) + len(fast_updated)
    if count:
        event_ids = [int(row["id"]) for row in rows]
        event_ids.extend(task_id for task_id, _claim_token in fast_updated)
        _add_task_event_batch_summary(
            conn, campaign_type, campaign_id, None,
            "campaign_terminated", "active", terminal_status, count,
            min(event_ids), max(event_ids), {"message": terminal_message},
        )
    _close_fast_pre_submit_attempts(
        conn, fast_updated, fast_target, terminal_message,
    )
    return {"requeued": 0, "terminated": count}


def transition_campaign_status(
    campaign_type: str, campaign_id: int, expected_status: str, target_status: str,
) -> dict:
    """CAS-update an activity and its dispatch fence in one DB transaction."""
    table = {"batch": "batch_campaigns", "realtime": "realtime_campaigns"}.get(
        str(campaign_type)
    )
    if not table:
        raise ValueError("campaign_type must be batch or realtime")

    def write(conn):
        row = conn.execute(
            f"SELECT * FROM {table} WHERE id=?", (int(campaign_id),)
        ).fetchone()
        if not row:
            raise LookupError("campaign not found")
        current = dict(row)
        if (current.get("status") or "not_started") != expected_status:
            raise ValueError(
                f"campaign changed while {target_status} was being requested"
            )
        next_generation = int(current.get("dispatch_generation") or 0) + 1
        updated = conn.execute(
            f"""UPDATE {table}
                SET status=?, dispatch_generation=?, updated_at=datetime('now','localtime')
                WHERE id=? AND status=?
                RETURNING *""",
            (target_status, next_generation, int(campaign_id), expected_status),
        ).fetchone()
        if not updated:
            raise ValueError("campaign changed while its state was being updated")
        task_summary = reconcile_campaign_state_in_transaction(
            conn, campaign_type, campaign_id, target_status, next_generation,
        )
        return {"campaign": dict(updated), "tasks": task_summary}

    from services import fast_send_service
    barrier = fast_send_service.campaign_control_barrier(campaign_type, campaign_id)
    with barrier:
        result = write_transaction(
            write, attempts=3, operation="campaign.status.transition", base_delay=0.1,
        )
        fast_send_service.publish_campaign_state(
            campaign_type, campaign_id,
            result["campaign"].get("status", ""),
            int(result["campaign"].get("dispatch_generation") or 0),
        )
    invalidate_campaign_cache(campaign_type, campaign_id)
    return result


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


def _business_now(now: Optional[datetime] = None) -> datetime:
    """Normalize a schedule comparison to the deployment's business zone."""

    _name, tz = business_timezone()
    if now is None:
        return datetime.now(timezone.utc).astimezone(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def _parse_business_datetime(value: object, tz) -> datetime:
    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def campaign_is_active(campaign: dict, now: Optional[datetime] = None) -> tuple[bool, str]:
    now = _business_now(now)
    if campaign.get("status") != "in_progress":
        return False, "campaign is not enabled"
    try:
        start = campaign.get("start_time") or ""
        end = campaign.get("end_time") or ""
        if start and now < _parse_business_datetime(start, now.tzinfo):
            return False, "campaign has not started"
        if end and now > _parse_business_datetime(end, now.tzinfo):
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
                ingestion_chunk: Optional[dict] = None,
                 ingestion_chunk_progress: bool = False,
                 row_ordinal_start: int = 0) -> dict:
    from services import vnext_service
    if vnext_service.is_active():
        return vnext_service.enqueue_rows(
            campaign_type, campaign, rows, source_file, batch_id,
            validate_schedule=validate_schedule, ingestion_lease=ingestion_lease,
            ingestion_chunk=ingestion_chunk, ingestion_chunk_progress=ingestion_chunk_progress,
            row_ordinal_start=row_ordinal_start,
        )
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
        task_date = _business_now().date().isoformat()
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
        task_source_identity = source_file
        if campaign_type == "realtime" and ingestion_lease:
            source_row = conn.execute(
                "SELECT file_hash FROM ingestion_files WHERE id=?",
                (ingestion_lease[0],),
            ).fetchone()
            source_hash = str(source_row["file_hash"] or "") if source_row else ""
            if source_hash:
                # Keep the public source_file filename stable, while making
                # same-name/different-content manifests distinct task keys.
                task_source_identity = f"{source_file}:{source_hash}"
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
                campaign_type, campaign["id"], msisdn, task_date, task_source_identity
            )
            task_rows.append((
                campaign_type, campaign["id"], batch_id, campaign.get("name", ""), msisdn,
                language, template["id"], campaign.get("smpp_config_id"),
                bool(campaign.get("flash_sms")), content,
                template.get("source_addr") or "",
                json.dumps(payload, ensure_ascii=False), source_file, task_date, dedupe_key,
                int(campaign.get("dispatch_generation") or 0),
            ))
        # The staging connection is intentionally closed before the durable
        # task/result phase.  Preparation can be CPU-heavy, while the actual
        # task insert, idempotent event append, and ingestion-chunk fence are a
        # short logical transaction that can safely replay from a fresh
        # connection after an OCC conflict.
        conn.close()
        conn = None

        def persist(conn):
            if ingestion_lease:
                # Fence at the actual task insertion boundary, not before
                # potentially slow parsing/template expansion. No file IO in tx.
                from services import realtime_ingestion_state
                realtime_ingestion_state.assert_owned(conn, *ingestion_lease)
            created = _bulk_insert_tasks(conn, task_rows) if task_rows else []
            persist_result = {
                **result,
                "created": len(created),
                "duplicate": int(result.get("duplicate", 0) or 0)
                + len(task_rows) - len(created),
            }
            if task_rows:
                _add_task_event_batch(
                    conn, campaign_type, campaign["id"], batch_id,
                    "created", "", "queued",
                    [task_id for task_id, _dedupe in created],
                    {"source_file": source_file},
                )
            chunk_committed = False
            chunk_progress_committed = False
            if ingestion_chunk:
                from services import realtime_ingestion_state
                if ingestion_chunk_progress:
                    realtime_ingestion_state.record_chunk_progress_in_transaction(
                        conn, ingestion_lease[0], ingestion_chunk,
                        persist_result, ingestion_lease[1],
                    )
                    chunk_progress_committed = True
                else:
                    realtime_ingestion_state.complete_chunk_in_transaction(
                        conn, ingestion_lease[0], ingestion_chunk,
                        persist_result, ingestion_lease[1],
                    )
                    chunk_committed = True
            return created, chunk_committed, chunk_progress_committed

        created, chunk_committed, chunk_progress_committed = write_transaction(
            persist,
            attempts=5,
            operation=f"sms_tasks.enqueue.{campaign_type}",
            lock_timeout=1.0,
        )
        if campaign_type == "realtime" and ingestion_lease:
            # Task/chunk authority is committed first.  The processed-file
            # row is only a repairable display projection and must not be
            # inside this OCC transaction.
            from services import realtime_ingestion_state
            realtime_ingestion_state.project_processed_file(ingestion_lease[0])
        result["duplicate"] += len(task_rows) - len(created)
        result["created"] += len(created)
        created_keys = {dedupe for _task_id, dedupe in created}
        result["created_msisdns"].extend(
            row[4] for row in task_rows if row[-2] in created_keys
        )
        if chunk_committed:
            result["chunk_committed"] = True
        if chunk_progress_committed:
            result["chunk_progress_committed"] = True
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
        if conn is not None:
            conn.close()


def _campaign_eligibility_sql(alias: str) -> str:
    """Return the SQL-side activity fence used by every persistent claim.

    The Python schedule validator remains the final pre-wire check.  This
    predicate keeps stopped or not-yet-open campaigns out of both the
    candidate scan and the conditional UPDATE, so a large paused backlog
    cannot consume claim slots before being deferred.
    """
    now = "CURRENT_TIMESTAMP"
    current_hhmm = f"strftime({now}, '%H:%M')"
    current_day = "strftime(CURRENT_DATE, '%w')"
    return f"""
        {alias}.status='in_progress'
        AND (NULLIF(COALESCE({alias}.start_time,''),'') IS NULL
             OR TRY_CAST({alias}.start_time AS TIMESTAMP) <= {now})
        AND (NULLIF(COALESCE({alias}.end_time,''),'') IS NULL
             OR TRY_CAST({alias}.end_time AS TIMESTAMP) >= {now})
        AND (COALESCE({alias}.send_days,'') IN ('','[]')
             OR regexp_matches({alias}.send_days,
                '(^|[^0-9])' || {current_day} || '([^0-9]|$)'))
        AND (
            COALESCE({alias}.send_start_time,'')=''
            OR COALESCE({alias}.send_end_time,'')=''
            OR (
                {alias}.send_start_time <= {alias}.send_end_time
                AND {current_hhmm} BETWEEN LEFT({alias}.send_start_time,5)
                    AND LEFT({alias}.send_end_time,5)
            )
            OR (
                {alias}.send_start_time > {alias}.send_end_time
                AND ({current_hhmm} >= LEFT({alias}.send_start_time,5)
                     OR {current_hhmm} <= LEFT({alias}.send_end_time,5))
            )
        )
    """


def _task_campaign_fence_sql(task_alias: str = "t") -> str:
    """Return a type-safe EXISTS fence for a task row."""
    return f"""
        (
            ({task_alias}.campaign_type='batch' AND EXISTS (
                SELECT 1 FROM batch_campaigns c
                WHERE c.id={task_alias}.campaign_id
                  AND {task_alias}.dispatch_generation=c.dispatch_generation
                  AND {_campaign_eligibility_sql('c')}
            ))
            OR
            ({task_alias}.campaign_type='realtime' AND EXISTS (
                SELECT 1 FROM realtime_campaigns c
                WHERE c.id={task_alias}.campaign_id
                  AND {task_alias}.dispatch_generation=c.dispatch_generation
                  AND {_campaign_eligibility_sql('c')}
            ))
        )
    """


def _campaign_task_eligibility_sql() -> str:
    """Compatibility alias for callers building a task candidate query."""
    return _task_campaign_fence_sql("t")


def fair_dispatch_candidates(window: int, max_task_bytes: int) -> list:
    """Bounded keyset window; advance past full groups without claiming them.

    This transaction only reads candidates and updates the existing durable
    cursor. DRR selection and memory reservation happen after it commits.
    """
    window = max(1, min(128, int(window)))

    def read(conn):
        state = conn.execute(
            "SELECT cursor_key FROM fair_scheduler_state WHERE stage='s1_dispatch'"
        ).fetchone()
        from services.campaign_capacity_service import _parse_cursor_key, _cursor_key
        previous = _parse_cursor_key(state["cursor_key"] if state else "")
        base = f"""FROM sms_tasks t
            WHERE t.status IN ('queued','retrying')
              AND CAST(t.next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
              AND (t.campaign_type<>'realtime' OR t.task_date=CURRENT_DATE)
              AND {_task_campaign_fence_sql('t')}"""
        route = "COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0)"
        columns = f"t.campaign_type,t.campaign_id,{route}"
        group_sql = f"SELECT {columns} route_id {base}"
        after = f" AND (t.campaign_type,t.campaign_id,{route})>(?,?,?)" if previous else ""
        groups = conn.execute(
            group_sql + after + f" GROUP BY {columns} ORDER BY {columns} LIMIT ?",
            [*(previous or ()), window],
        ).fetchall()
        if not groups and previous:
            groups = conn.execute(
                group_sql + f" GROUP BY {columns} ORDER BY {columns} LIMIT ?", [window],
            ).fetchall()
        if not groups:
            return []
        last = groups[-1]
        conn.execute(
            """INSERT INTO fair_scheduler_state(stage,cursor_key,rounds,updated_at)
               VALUES ('s1_dispatch',?,1,CURRENT_TIMESTAMP)
               ON CONFLICT(stage) DO UPDATE SET cursor_key=excluded.cursor_key,
                 rounds=fair_scheduler_state.rounds+1,updated_at=excluded.updated_at""",
            [_cursor_key((last["campaign_type"], last["campaign_id"], last["route_id"]))],
        )
        # One oldest task per group, with a SQL payload envelope before any
        # Python object is materialized. Oversize work remains authoritative.
        clause = " OR ".join(f"(t.campaign_type=? AND t.campaign_id=? AND {route}=?)" for _ in groups)
        params = [v for group in groups for v in (group["campaign_type"], group["campaign_id"], group["route_id"])]
        rows = conn.execute(
            f"""SELECT * FROM (
                SELECT t.*,p.weight scheduling_weight,p.max_parts_tps,
                  ROW_NUMBER() OVER (PARTITION BY {columns} ORDER BY t.created_at,t.id) rn
                {base.replace('FROM sms_tasks t', 'FROM sms_tasks t LEFT JOIN campaign_capacity_policies p ON p.campaign_type=t.campaign_type AND p.campaign_id=t.campaign_id AND p.enabled=1')}
                  AND ({clause})
                  AND OCTET_LENGTH(encode(COALESCE(t.content,'')))
                    + OCTET_LENGTH(encode(COALESCE(t.payload,'')))<=?
              ) candidates WHERE rn=1 LIMIT ?""",
            [*params, int(max_task_bytes) // 4, window],
        ).fetchall()
        return [dict(row) for row in rows]

    return write_transaction(read, attempts=3, operation="sms_tasks.s1_candidates")


def claim_next_tasks(limit: int = 20, send_mode: Optional[str] = None,
                     *, _reserved_tasks: Optional[list] = None) -> list:
    limit = max(1, int(limit or 20))
    from services import fast_send_service
    selected_mode = send_mode or fast_send_service.mode()
    if selected_mode not in {"reliable", "fast"}:
        raise ValueError(f"unsupported SMS send mode: {selected_mode}")
    fast_mode = selected_mode == "fast"
    # DuckDB allows concurrent readers, but concurrent UPDATE ... RETURNING
    # claims can conflict before the explicit commit when many task workers
    # select the same queue head. Serialize only this short claim transaction
    # and retry conflicts caused by unrelated writers.
    with _TASK_CLAIM_LOCK:
        token = uuid.uuid4().hex
        fast_batch_id = uuid.uuid4().hex if fast_mode else ""
        fast_lease_id = uuid.uuid4().hex if fast_mode else ""

        def claim(conn):
            global _FAIR_DISABLED_WARNING_EMITTED
            from services import campaign_capacity_service
            if _reserved_tasks is not None:
                # Memory admission already succeeded outside this transaction.
                # Reject any descriptor whose durable payload/identity changed.
                selected_ids = []
                for expected in _reserved_tasks:
                    current = conn.execute("SELECT * FROM sms_tasks WHERE id=?", [expected["id"]]).fetchone()
                    fields = ("campaign_type", "campaign_id", "dispatch_generation", "content",
                              "payload", "smpp_config_id", "transport_smpp_config_id", "msisdn",
                              "source_addr", "flash_sms")
                    if current and all(current[field] == expected.get(field) for field in fields):
                        selected_ids.append(int(expected["id"]))
            elif campaign_capacity_service.enabled():
                per_campaign = campaign_capacity_service.per_campaign_candidates()
                active_groups = conn.execute(
                    f"""SELECT t.campaign_type,t.campaign_id,
                              COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0) route_id,
                              MIN(t.created_at) oldest
                       FROM sms_tasks t
                       WHERE t.status IN ('queued','retrying')
                         AND CAST(t.next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                         AND (t.campaign_type <> 'realtime' OR t.task_date=CURRENT_DATE)
                         AND {_task_campaign_fence_sql('t')}
                       GROUP BY t.campaign_type,t.campaign_id,
                                COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0)
                       ORDER BY oldest,t.campaign_type,t.campaign_id,route_id"""
                ).fetchall()
                selected_groups = campaign_capacity_service.rotate_candidate_groups(
                    conn, (dict(row) for row in active_groups),
                    campaign_capacity_service.active_campaign_limit(),
                )
                if not selected_groups:
                    return []
                candidate_limit = per_campaign * max(1, len(selected_groups))
                group_clause = " OR ".join(
                    "(t.campaign_type=? AND t.campaign_id=? AND "
                    "COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0)=?)"
                    for _ in selected_groups
                )
                candidate_params = [value for group in selected_groups for value in group]
                candidate_params.extend([per_campaign, candidate_limit])
                candidate_sql = """WITH ranked AS (
                           SELECT t.*,
                                  COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0)
                                    target_smpp_config_id,
                                  COALESCE(p.weight,1.0) scheduling_weight,
                                  p.max_parts_tps,p.max_outstanding_parts,
                                  ROW_NUMBER() OVER (
                                    PARTITION BY t.campaign_type,t.campaign_id,
                                      COALESCE(t.transport_smpp_config_id,t.smpp_config_id,0)
                                    ORDER BY t.created_at,t.id
                                  ) fair_row
                           FROM sms_tasks t
                           LEFT JOIN campaign_capacity_policies p
                             ON p.campaign_type=t.campaign_type
                            AND p.campaign_id=t.campaign_id AND p.enabled=1
                           WHERE (""" + group_clause + """
                             )
                             AND t.status IN ('queued','retrying')
                             AND CAST(t.next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                             AND (t.campaign_type <> 'realtime' OR t.task_date=CURRENT_DATE)
                             AND """ + _task_campaign_fence_sql("t") + """
                       )
                       SELECT * FROM ranked
                       WHERE fair_row<=?
                       ORDER BY created_at,id LIMIT ?"""
                candidates = conn.execute(
                    candidate_sql,
                    candidate_params,
                ).fetchall()
                scheduled = campaign_capacity_service.select(
                    (dict(row) for row in candidates), limit
                )
                selected_ids = [int(task["id"]) for task in scheduled]
            else:
                if not _FAIR_DISABLED_WARNING_EMITTED:
                    _FAIR_DISABLED_WARNING_EMITTED = True
                    logger.warning(
                        "Fair SMS task scheduler is disabled; claims use global FIFO and "
                        "campaign starvation protection is not active"
                    )
                    metrics_service.inc("marketing_copilot_fair_scheduler_disabled_total")
                selected_ids = [
                    int(row["id"]) for row in conn.execute(
                        f"""SELECT t.id FROM sms_tasks t
                           WHERE t.status IN ('queued','retrying')
                             AND CAST(t.next_attempt_at AS TIMESTAMP WITH TIME ZONE)<=CURRENT_TIMESTAMP
                             AND (t.campaign_type <> 'realtime' OR t.task_date=CURRENT_DATE)
                             AND {_task_campaign_fence_sql('t')}
                           ORDER BY t.created_at,t.id LIMIT ?""",
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
                      AND {_task_campaign_fence_sql('sms_tasks')}
                    RETURNING *""",
                [token, *selected_ids],
            ).fetchall()
            claimed_by_id = {int(row["id"]): dict(row) for row in claimed_rows}
            # DuckDB UPDATE RETURNING order is not a scheduling contract.
            tasks = [claimed_by_id[task_id] for task_id in selected_ids if task_id in claimed_by_id]
            if fast_mode and tasks:
                _create_fast_batch_in_transaction(
                    conn, fast_batch_id, fast_lease_id, token, tasks,
                )
                for task in tasks:
                    task["_fast_mode"] = True
                    task["_fast_batch_id"] = fast_batch_id
                    task["_fast_lease_id"] = fast_lease_id
                    task["_fast_campaign_status"] = "in_progress"
                    task["_attempt_token"] = f"fast:{fast_batch_id}:{task['id']}"
            for task in tasks:
                metrics_service.inc(
                    "marketing_copilot_campaign_tasks_claimed_total",
                    labels={
                        "campaign_type": task.get("campaign_type", ""),
                        "campaign_id": task.get("campaign_id", 0),
                    },
                )
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
                     "attempt_max": max(task["attempts"] for task in items),
                     **({"dispatch_protocol": "s1_bounded_v1"} if _reserved_tasks is not None else {})},
                )
            # ``campaign_fairness_state`` is advisory display state.  The
            # claim scheduler uses candidate rows and the durable cursor, not
            # this table, while the capacity snapshot computes authoritative
            # task counts when requested.  Do not upsert its shared campaign
            # row inside the claim transaction: concurrent task workers would
            # otherwise turn an observability write into an OCC hot-row fence.
            return tasks

        claimed_tasks = write_transaction(
            claim, attempts=3,
            operation="sms_tasks.fast_claim" if fast_mode else "sms_tasks.claim",
        )
        if fast_mode:
            # The durable claim is now committed.  Never acquire a campaign
            # fence from inside the DuckDB writer transaction: campaign
            # control takes the fence first and then the writer gate, so doing
            # it in the opposite order recreates a Claim x Pause deadlock.
            # The SQL generation fence remains authoritative if control wins
            # this race before the local observation is published.
            for task in claimed_tasks:
                fast_send_service.observe_claimed_task(task)
        return claimed_tasks


def claim_next_task() -> Optional[dict]:
    tasks = claim_next_tasks(1)
    return tasks[0] if tasks else None


def claim_next_fast_batch(limit: int = 20) -> list:
    """Claim a bounded Fast batch with a durable uncertain-after-claim fence."""
    from services import fast_send_service
    if fast_send_service.freeze_mode() != "fast":
        raise RuntimeError("claim_next_fast_batch requires MC_SMS_SEND_MODE=fast")
    return claim_next_tasks(limit, send_mode="fast")


def _create_fast_batch_in_transaction(conn, batch_id: str, lease_id: str,
                                      claim_token: str, tasks: list) -> None:
    """Persist compact batch ownership and one attempt identity per task."""
    conn.execute(
        """INSERT INTO sms_fast_batches
           (batch_id,lease_id,claim_token,mode,state,task_count,lease_until)
           VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP + INTERVAL '5 minutes')""",
        (str(batch_id), str(lease_id), str(claim_token), "fast", "claimed", len(tasks)),
    )
    item_values = []
    item_params = []
    attempt_values = []
    attempt_params = []
    campaign_schedule = {}
    for task in tasks:
        task_id = int(task["id"])
        attempt_token = f"fast:{batch_id}:{task_id}"
        route_id = task.get("transport_smpp_config_id") or task.get("smpp_config_id")
        campaign_key = (str(task.get("campaign_type") or ""), int(task.get("campaign_id") or 0))
        if campaign_key not in campaign_schedule:
            table = {"batch": "batch_campaigns", "realtime": "realtime_campaigns"}.get(campaign_key[0])
            campaign_schedule[campaign_key] = None
            if table:
                campaign_schedule[campaign_key] = dict(conn.execute(
                    f"SELECT status,start_time,end_time,send_start_time,send_end_time,send_days FROM {table} WHERE id=?",
                    (campaign_key[1],),
                ).fetchone() or {})
        deadline = _fast_campaign_window_deadline(campaign_schedule[campaign_key])
        task["_socket_schedule"] = campaign_schedule[campaign_key]
        if deadline is not None:
            task["_fast_window_deadline_epoch"] = deadline
        item_values.append("(?,?,?,?,?,?,?,?,?,'claimed','[]','',CURRENT_TIMESTAMP,NULL,CURRENT_TIMESTAMP)")
        item_params.extend([
            str(batch_id), str(lease_id), task_id, str(claim_token), attempt_token,
            int(task.get("dispatch_generation") or 0), route_id, 0, 0,
        ])
        attempt_values.append("(?,?,?,?,?,?,?,?,'authorized',CURRENT_TIMESTAMP + INTERVAL '5 minutes',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        attempt_params.extend([
            attempt_token, task_id, str(claim_token),
            f"smpp:{int(route_id)}" if route_id is not None else "legacy",
            route_id, -1, 0, 0,
        ])
    if item_values:
        conn.execute(
            """INSERT INTO sms_fast_batch_items
               (batch_id,lease_id,task_id,claim_token,attempt_token,
                dispatch_generation,route_id,planned_parts,accepted_parts,state,
                message_ids,last_error,created_at,checkpointed_at,updated_at)
               VALUES """ + ",".join(item_values), item_params,
        )
        conn.execute(
            """INSERT INTO sms_send_attempts
               (attempt_token,task_id,claim_token,source_namespace,smpp_config_id,
                slot,planned_parts,accepted_parts,state,lease_until,created_at,updated_at)
               VALUES """ + ",".join(attempt_values), attempt_params,
        )
    grouped = {}
    for task in tasks:
        key = (task["campaign_type"], task["campaign_id"], task.get("batch_id"))
        grouped.setdefault(key, []).append(task["id"])
    for (campaign_type, campaign_id, batch_id), task_ids in grouped.items():
        _add_task_event_batch(
            conn, campaign_type, campaign_id, batch_id,
            "fast_batch_claimed", "queued", "sending", task_ids,
            {"batch_id": str(batch_id), "lease_id": str(lease_id),
             "claim_token": str(claim_token), "mode": "fast"},
        )


def _fast_campaign_window_deadline(campaign: dict) -> Optional[float]:
    """Compute a local deadline for the already-validated send window.

    This is only a short-lived socket-admission guard.  Durable campaign
    generation/status remains authoritative; the value prevents a bounded
    prefetched task from starting after its current time window closes.
    """
    if not campaign:
        return None
    now = _business_now()
    deadlines = []
    end_time = str(campaign.get("end_time") or "").strip()
    if end_time:
        try:
            parsed = _parse_business_datetime(end_time, now.tzinfo)
            deadlines.append(parsed.timestamp())
        except ValueError:
            pass
    start = str(campaign.get("send_start_time") or "").strip()[:5]
    end = str(campaign.get("send_end_time") or "").strip()[:5]
    if end and ":" in end:
        try:
            start_minutes = int(start[:2]) * 60 + int(start[3:5]) if start else 0
            end_minutes = int(end[:2]) * 60 + int(end[3:5])
            current_minutes = now.hour * 60 + now.minute
            end_date = now.date()
            if start_minutes > end_minutes and current_minutes >= start_minutes:
                from datetime import timedelta
                end_date += timedelta(days=1)
            end_dt = datetime.combine(
                end_date, datetime.min.time(), tzinfo=now.tzinfo,
            ).replace(hour=end_minutes // 60, minute=end_minutes % 60)
            deadlines.append(end_dt.timestamp())
        except (TypeError, ValueError):
            pass
    return min(deadlines) if deadlines else None


def recover_fast_claims_on_startup() -> int:
    """Conservatively isolate Fast claims left across a process restart."""
    def recover(conn):
        rows = conn.execute(
            """SELECT i.batch_id,i.task_id,i.claim_token,i.attempt_token,
                      t.status,t.campaign_type,t.campaign_id
               FROM sms_fast_batch_items i
               JOIN sms_fast_batches b ON b.batch_id=i.batch_id
               JOIN sms_tasks t ON t.id=i.task_id
               WHERE b.mode='fast'
                 AND b.state IN ('claimed','active','checkpoint_pending')
                 AND i.state IN ('claimed','sending','checkpoint_pending')
                 AND t.status='sending'"""
        ).fetchall()
        if not rows:
            return 0
        conn.execute(
            """UPDATE sms_tasks AS t SET
                   status='unknown', failure_class='delivery_unknown',
                   error_msg='Fast Send claim survived restart; submission is unconfirmed',
                   updated_at=CURRENT_TIMESTAMP
               FROM sms_fast_batch_items i
               JOIN sms_fast_batches b ON b.batch_id=i.batch_id
               WHERE t.id=i.task_id AND t.status='sending'
                 AND b.mode='fast'
                 AND b.state IN ('claimed','active','checkpoint_pending')
                 AND i.state IN ('claimed','sending','checkpoint_pending')"""
        )
        conn.execute(
            """UPDATE sms_send_attempts AS a SET
                   state='unknown', lease_until=NULL, updated_at=CURRENT_TIMESTAMP
               FROM sms_fast_batch_items i
               JOIN sms_fast_batches b ON b.batch_id=i.batch_id
               WHERE a.attempt_token=i.attempt_token
                 AND b.mode='fast'
                 AND b.state IN ('claimed','active','checkpoint_pending')
                 AND i.state IN ('claimed','sending','checkpoint_pending')"""
        )
        conn.execute(
            """UPDATE sms_fast_batch_items AS i SET
                   state='unknown',
                   last_error='Fast Send claim survived restart; no automatic resubmit',
                   updated_at=CURRENT_TIMESTAMP
               FROM sms_fast_batches b
               WHERE b.batch_id=i.batch_id AND b.mode='fast'
                 AND b.state IN ('claimed','active','checkpoint_pending')
                 AND i.state IN ('claimed','sending','checkpoint_pending')"""
        )
        touched_batches = {str(row["batch_id"]) for row in rows}
        for batch_id in touched_batches:
            conn.execute(
                """UPDATE sms_fast_batches SET state='recovered_unknown',
                       unresolved_count=(SELECT COUNT(*) FROM sms_fast_batch_items
                                         WHERE batch_id=? AND state='unknown'),
                       updated_at=CURRENT_TIMESTAMP,last_error='restart recovery'
                   WHERE batch_id=?""",
                (batch_id, batch_id),
            )
        for row in rows:
            _add_task_event(
                conn, int(row["task_id"]), "recovered_unknown", "sending", "unknown",
                "Fast Send claim survived restart; not automatically resubmitted",
                {"batch_id": str(row["batch_id"]), "attempt_token": str(row["attempt_token"] or "")},
            )
        return len(rows)

    recovered = write_transaction(
        recover, attempts=3, operation="sms_tasks.fast_startup_recovery",
    )
    if recovered:
        logger.warning("Fast Send isolated %s uncertain-after-claim tasks on startup", recovered)
    return int(recovered or 0)


def abort_fast_task_before_submit(task: dict, reason: str) -> None:
    """Close a Fast attempt that was stopped before its first socket write."""
    def abort(conn):
        row = conn.execute(
            """SELECT t.status,t.claim_token,c.status campaign_status
               FROM sms_tasks t
               LEFT JOIN batch_campaigns c
                 ON t.campaign_type='batch' AND c.id=t.campaign_id
               LEFT JOIN realtime_campaigns r
                 ON t.campaign_type='realtime' AND r.id=t.campaign_id
               WHERE t.id=? AND t.claim_token=?""",
            (int(task["id"]), str(task.get("claim_token") or "")),
        ).fetchone()
        if not row:
            return False
        campaign_status = str(row["campaign_status"] or "")
        if not campaign_status:
            realtime_row = conn.execute(
                """SELECT r.status FROM realtime_campaigns r
                   JOIN sms_tasks t ON t.campaign_type='realtime' AND t.campaign_id=r.id
                   WHERE t.id=?""", (int(task["id"]),),
            ).fetchone()
            campaign_status = str(realtime_row["status"] or "") if realtime_row else ""
        # Requeue is safe only because this method is called by the socket
        # fence before any submit_sm write.  Stopped/ended campaigns remain
        # terminal and are never returned to the queue.
        target = "queued" if campaign_status in {"in_progress", "paused", ""} else (
            "cancelled" if campaign_status == "cancelled" else "expired"
        )
        conn.execute(
            """UPDATE sms_tasks SET status=?, attempts=CASE WHEN ?='queued'
                       THEN GREATEST(attempts-1,0) ELSE attempts END,
                   error_msg=?, next_attempt_at=CURRENT_TIMESTAMP,
                   updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND status='sending' AND claim_token=?""",
            (target, target, str(reason)[:2000], int(task["id"]),
             str(task.get("claim_token") or "")),
        )
        conn.execute(
            """UPDATE sms_send_attempts SET state='cancelled',lease_until=NULL,
                   updated_at=CURRENT_TIMESTAMP
               WHERE attempt_token=? AND task_id=? AND claim_token=?""",
            (str(task.get("_attempt_token") or ""), int(task["id"]),
             str(task.get("claim_token") or "")),
        )
        conn.execute(
            """UPDATE sms_fast_batch_items SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP
               WHERE batch_id=? AND task_id=? AND claim_token=?""",
            ("deferred" if target == "queued" else target,
             str(reason)[:2000], str(task.get("_fast_batch_id") or ""),
             int(task["id"]), str(task.get("claim_token") or "")),
        )
        conn.execute(
            """UPDATE sms_fast_batches SET updated_at=CURRENT_TIMESTAMP,
                   last_error=? WHERE batch_id=?""",
            (str(reason)[:2000], str(task.get("_fast_batch_id") or "")),
        )
        return True

    write_transaction(abort, attempts=3, operation="sms_tasks.fast_abort_pre_submit")


def _apply_fast_checkpoint(conn, payloads) -> None:
    """Fold a bounded group of normal Fast successes into durable state.

    The SMPP response path has already proved that every part in each payload
    was accepted.  This function is called by the existing journal-backed
    writer, so replay is fenced by ``sms_result_events.event_id`` before this
    function runs.  Task updates and Fast batch-item updates therefore remain
    idempotent even when the process stops between database commit and the
    journal terminal marker.
    """
    if not payloads:
        return
    latest = {}
    for payload in payloads:
        task_id = int(payload.get("task_id") or 0)
        if task_id:
            latest[task_id] = dict(payload)
    if not latest:
        return
    submitted = []
    for payload in latest.values():
        submitted.append({
            "task_id": int(payload["task_id"]),
            "message_ids": list(payload.get("message_ids") or []),
            "delivery_receipt_mode": payload.get("delivery_receipt_mode") or "optional",
            "transport_smpp_config_id": payload.get("transport_smpp_config_id"),
            "submit_sm_parts": payload.get("submit_sm_parts"),
            "claim_token": str(payload.get("claim_token") or ""),
            "attempt_token": str(payload.get("attempt_token") or ""),
            "source_namespace": str(payload.get("source_namespace") or "legacy"),
        })
    _apply_submitted_batch(conn, submitted)

    values = []
    params = []
    for payload in latest.values():
        values.append("(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)")
        params.extend([
            str(payload.get("batch_id") or ""),
            str(payload.get("lease_id") or ""),
            int(payload["task_id"]),
            max(1, int(payload.get("submit_sm_parts") or 1)),
            int(payload.get("accepted_parts") or payload.get("submit_sm_parts") or 1),
            json.dumps(list(payload.get("message_ids") or [])),
            str(payload.get("claim_token") or ""),
            str(payload.get("attempt_token") or ""),
        ])
    if values:
        conn.execute(
            """UPDATE sms_fast_batch_items AS i SET
                   planned_parts=v.planned_parts,
                   accepted_parts=v.accepted_parts,
                   state='checkpointed', message_ids=v.message_ids,
                   last_error='', checkpointed_at=CURRENT_TIMESTAMP,
                   updated_at=CURRENT_TIMESTAMP
               FROM (VALUES """ + ",".join(values) + """) AS v(
                   batch_id,lease_id,task_id,planned_parts,accepted_parts,
                   message_ids,claim_token,attempt_token,checkpointed_at)
               WHERE i.batch_id=v.batch_id AND i.task_id=v.task_id
                 AND i.claim_token=v.claim_token
                 AND i.attempt_token=v.attempt_token""",
            params,
        )

    attempt_values = []
    attempt_params = []
    for payload in latest.values():
        attempt_values.append("(?,?,?,?,?)")
        attempt_params.extend([
            str(payload.get("attempt_token") or ""),
            int(payload["task_id"]),
            str(payload.get("claim_token") or ""),
            max(1, int(payload.get("submit_sm_parts") or 1)),
            int(payload.get("accepted_parts") or payload.get("submit_sm_parts") or 1),
        ])
    if attempt_values:
        conn.execute(
            """UPDATE sms_send_attempts AS a SET
                   planned_parts=v.planned_parts,
                   accepted_parts=v.accepted_parts,
                   state='accepted', lease_until=NULL,
                   updated_at=CURRENT_TIMESTAMP
               FROM (VALUES """ + ",".join(attempt_values) + """) AS v(
                   attempt_token,task_id,claim_token,planned_parts,accepted_parts)
               WHERE a.attempt_token=v.attempt_token AND a.task_id=v.task_id
                 AND a.claim_token=v.claim_token""",
            attempt_params,
        )

    batch_ids = sorted({str(item.get("batch_id") or "") for item in latest.values() if item.get("batch_id")})
    for batch_id in batch_ids:
        conn.execute(
            """UPDATE sms_fast_batches AS b SET
                   checkpointed_count=(SELECT COUNT(*) FROM sms_fast_batch_items i
                                       WHERE i.batch_id=b.batch_id AND i.state='checkpointed'),
                   unresolved_count=(SELECT COUNT(*) FROM sms_fast_batch_items i
                                     WHERE i.batch_id=b.batch_id
                                       AND i.state IN ('unknown','partial','failed')),
                   state=CASE WHEN NOT EXISTS (
                              SELECT 1 FROM sms_fast_batch_items i
                              WHERE i.batch_id=b.batch_id
                                AND i.state IN ('claimed','sending','checkpoint_pending')
                          ) THEN 'completed' ELSE 'active' END,
                   checkpointed_at=CASE WHEN NOT EXISTS (
                              SELECT 1 FROM sms_fast_batch_items i
                              WHERE i.batch_id=b.batch_id
                                AND i.state IN ('claimed','sending','checkpoint_pending')
                          ) THEN CAST(CURRENT_TIMESTAMP AS VARCHAR) ELSE b.checkpointed_at END,
                   updated_at=CURRENT_TIMESTAMP
               WHERE b.batch_id=?""",
            (batch_id,),
        )


def apply_buffered_writes(conn, items) -> None:
    """Apply one buffered delivery batch inside a single database transaction."""
    # Persist intents and per-part facts before folding the task-level result.
    # All changes share the caller's explicit transaction and event-id fence.
    stage_started = time.perf_counter()
    intent_count = 0
    fast_checkpoints = []
    for item in items:
        if item.kind == "send_intent":
            _apply_send_intent(conn, **item.payload)
            intent_count += 1
        elif item.kind == "part_result":
            _apply_part_result(conn, **item.payload)
            intent_count += 1
        elif item.kind == "fast_checkpoint":
            fast_checkpoints.extend(item.payload.get("items") or [])
    _observe_result_stage("intent_and_part_result", stage_started, intent_count)
    if fast_checkpoints:
        stage_started = time.perf_counter()
        _apply_fast_checkpoint(conn, fast_checkpoints)
        _observe_result_stage("fast_checkpoint", stage_started, len(fast_checkpoints))
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
        stage_started = time.perf_counter()
        _apply_disabled_submitted_batch(conn, disabled_payloads)
        _observe_result_stage("disabled_submitted", stage_started, len(disabled_payloads))
    if regular_payloads:
        # submit_sm_resp callbacks enqueue each part's message id immediately
        # for crash recovery; the later submitted event contains those same ids.
        # Combine both sources so a successful part reaches the indexed mapping
        # table only once per transaction.
        stage_started = time.perf_counter()
        _apply_submitted_batch(conn, regular_payloads, regular_message_id_items)
        _observe_result_stage("submitted", stage_started, len(regular_payloads))
    elif regular_message_id_items:
        stage_started = time.perf_counter()
        _apply_message_id_batch(conn, regular_message_id_items)
        _observe_result_stage("message_id", stage_started, len(regular_message_id_items))

    stage_started = time.perf_counter()
    secondary_count = 0
    for item in items:
        payload = item.payload
        if item.kind in {
            "message_id", "submitted", "send_intent", "part_result", "fast_checkpoint",
        }:
            continue
        if item.kind == "send_failure":
            _apply_send_failure(conn, **payload)
            secondary_count += 1
        elif item.kind == "receipt":
            _apply_receipt(conn, **payload)
            secondary_count += 1
        else:
            raise ValueError(f"unknown SMS buffered write kind: {item.kind}")
    _observe_result_stage("failure_and_receipt", stage_started, secondary_count)


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
    planned_parts = max(1, int(planned_parts))

    def authorize(conn):
        row = conn.execute(
            """SELECT id,campaign_type,campaign_id,task_date,claim_token,status,
                      dispatch_generation
               FROM sms_tasks
               WHERE id=? AND status='sending' AND claim_token=?""",
            (int(task["id"]), claim_token),
        ).fetchone()
        if not row:
            return False
        current = dict(row)
        if int(task.get("dispatch_generation") or 0) != int(current.get("dispatch_generation") or 0):
            return False
        if current.get("campaign_type") == "realtime" and current.get("task_date") is not None:
            if str(current["task_date"])[:10] != _business_now().date().isoformat():
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
        if int(current.get("dispatch_generation") or 0) != int(
            dict(campaign_row).get("dispatch_generation") or 0
        ):
            return False
        active, _reason = campaign_is_active(dict(campaign_row), _business_now())
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
             int(smpp_config_id), int(slot), planned_parts),
        )
        # Keep every planned part durable before socket I/O, but materialize a
        # multipart range in one statement.  The former one-INSERT-per-part
        # loop multiplied writer-gate work for the normal 3-4-part message.
        conn.execute(
            """INSERT INTO sms_send_parts
                  (attempt_token,part_no,connection_generation,source_namespace,state,updated_at)
               SELECT ?, part_no + 1, ?, ?, 'planned', CURRENT_TIMESTAMP
               FROM range(?) AS planned(part_no)
               ON CONFLICT(attempt_token,part_no) DO NOTHING""",
            (attempt_token, str(connection_generation or ""), source_namespace,
             planned_parts),
        )
        # ``last_submit_at`` is a dashboard/fairness observation, not part of
        # the send fence.  It is recorded once per campaign in the buffered
        # result transaction below, rather than adding one more writer-gate
        # statement to every pre-wire authorization.
        _add_task_event(
            conn, int(task["id"]), "submit_authorized", "sending", "sending",
            "SMPP submit authorization acquired before socket write",
            {"claim_token": claim_token, "attempt_token": attempt_token,
             "planned_parts": planned_parts},
        )
        return dict(campaign_row)

    authorization_started = time.perf_counter()
    authorization_result = "error"
    try:
        authorized = write_transaction(
            authorize, attempts=3, operation="sms_task.submit_authorize", base_delay=0.05,
        )
        authorization_result = "success" if authorized else "denied"
    except TimeoutError:
        authorization_result = "timeout"
        metrics_service.inc(
            "marketing_copilot_smpp_submit_authorize_timeouts_total",
            labels={"config_id": smpp_config_id, "slot": slot},
        )
        raise
    finally:
        metrics_service.observe_histogram(
            "marketing_copilot_smpp_submit_authorize_seconds",
            time.perf_counter() - authorization_started,
            labels={
                "config_id": smpp_config_id,
                "slot": slot,
                "result": authorization_result,
            },
        )
    if not authorized:
        metrics_service.inc(
            "marketing_copilot_smpp_submit_authorize_denied_total",
            labels={"config_id": smpp_config_id, "slot": slot},
        )
        metrics_service.inc(
            "marketing_copilot_campaign_send_invalidated_total",
            labels={
                "campaign_type": task.get("campaign_type", ""),
                "campaign_id": task.get("campaign_id", 0),
            },
        )
        task["_submit_gate_reason"] = (
            "task claim is no longer owned or the campaign send window is closed"
        )
        task["_submit_gate_denied"] = True
        return ""
    metrics_service.inc(
        "marketing_copilot_smpp_submit_authorize_success_total",
        labels={"config_id": smpp_config_id, "slot": slot},
    )
    metrics_service.inc(
        "marketing_copilot_campaign_tasks_authorized_total",
        labels={
            "campaign_type": task.get("campaign_type", ""),
            "campaign_id": task.get("campaign_id", 0),
        },
    )
    task["_attempt_token"] = attempt_token
    task["_source_namespace"] = source_namespace
    # The schedule snapshot came from the authorization transaction. Publish
    # only after commit: control takes campaign gate -> DB, never DB -> gate.
    task["_fast_window_deadline_epoch"] = _fast_campaign_window_deadline(authorized)
    task["_socket_schedule"] = authorized
    from services import fast_send_service
    fast_send_service.observe_claimed_task(task)
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
    if task.get("_fast_mode"):
        from services import fast_send_service
        fast_send_service.record_part_result(
            task, part_no, sequence, ok, status, message_id,
        )
        return
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


def _record_submit_activity(conn, activity_keys) -> None:
    """Persist dashboard submit timestamps at result-batch granularity.

    This metadata is not an authorization or fairness fence. Once its SQL is
    inside the result transaction, any error must escape to the whole-
    transaction retry. WrappedConnection rolls back on statement failure;
    swallowing it would falsely acknowledge a rolled-back result to Journal.
    """
    try:
        from services import campaign_capacity_service
    except Exception:
        return
    for campaign_type, campaign_id, route_id in activity_keys:
        campaign_capacity_service.record_activity_event(
            conn, campaign_type, campaign_id, "submit", route_id=route_id,
            backlog=None,
        )


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
    attempt_fence_sql = ""
    if any(str(payload.get("attempt_token") or "") for payload in latest.values()):
        attempt_fence_sql = """
              AND (i.attempt_token='' OR EXISTS (
                  SELECT 1 FROM sms_send_attempts a
                  WHERE a.attempt_token=i.attempt_token
                    AND a.task_id=t.id AND a.claim_token=i.claim_token
              ))"""
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
              {attempt_fence_sql}
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
        values = ",".join("(?,?,?)" for _ in attempt_updates)
        conn.execute(
            f"""UPDATE sms_send_attempts AS a SET state='accepted',lease_until=NULL,
                   updated_at=CURRENT_TIMESTAMP
                FROM (VALUES {values}) AS i(attempt_token,task_id,claim_token)
                WHERE a.attempt_token=i.attempt_token
                  AND a.task_id=i.task_id AND a.claim_token=i.claim_token""",
            [value for row in attempt_updates for value in row],
        )

    rows_by_id = {int(row["id"]): row for row in before_rows}
    batch_delivered_increments = {}
    daily_sent_increments = {}
    event_groups = {}
    submit_activity = set()
    for task_id in updated_ids:
        row = rows_by_id.get(task_id)
        if not row:
            continue
        event_key = (
            row["campaign_type"], row["campaign_id"], row["batch_id"], row["status"]
        )
        event_groups.setdefault(event_key, []).append(task_id)
        payload = latest[task_id]
        submit_activity.add((
            row["campaign_type"], int(row["campaign_id"]),
            int(payload.get("transport_smpp_config_id") or 0),
        ))
        if row["batch_id"]:
            batch_id = int(row["batch_id"])
            campaign_id = int(row["campaign_id"])
            batch_delivered_increments[batch_id] = (
                batch_delivered_increments.get(batch_id, 0) + 1
            )
            daily_sent_increments[campaign_id] = (
                daily_sent_increments.get(campaign_id, 0) + 1
            )

    event_summaries = []
    for (campaign_type, campaign_id, batch_id, from_status), task_group in event_groups.items():
        event_summaries.append((
            campaign_type, campaign_id, batch_id, "submitted", from_status,
            "accepted", len(task_group), min(task_group), max(task_group),
            {"buffered": True, "no_delivery_receipt": True, "set_based": True},
        ))
    if len(event_summaries) == 1:
        # Keep the existing helper as the compatibility seam for small/legacy
        # callers that provide a minimal in-memory schema in unit tests.
        (event_key, task_group), = event_groups.items()
        campaign_type, campaign_id, batch_id, from_status = event_key
        _add_task_event_batch(
            conn, campaign_type, campaign_id, batch_id, "submitted", from_status,
            "accepted", task_group,
            {"buffered": True, "no_delivery_receipt": True, "set_based": True},
        )
    else:
        _add_task_event_batch_summaries(conn, event_summaries)

    _record_submit_activity(conn, submit_activity)

    if batch_delivered_increments:
        values = ",".join("(?,?)" for _ in batch_delivered_increments)
        conn.execute(
            f"""UPDATE batch_campaign_batches AS b SET
                   delivered_count=COALESCE(b.delivered_count,0)+i.delta,
                   status=CASE
                     WHEN b.task_count>0 AND COALESCE(b.delivered_count,0)+i.delta>=b.task_count
                     THEN 'sent' ELSE b.status END,
                   updated_at=CURRENT_TIMESTAMP
                FROM (VALUES {values}) AS i(batch_id,delta)
                WHERE b.id=i.batch_id""",
            [value for batch_id, count in batch_delivered_increments.items()
             for value in (batch_id, count)],
        )
        stats_date = datetime.now().astimezone().strftime("%Y-%m-%d")
        values = ",".join("(?,?)" for _ in daily_sent_increments)
        conn.execute(
            f"""UPDATE batch_campaign_daily_stats AS s SET
                   sent_count=COALESCE(s.sent_count,0)+i.delta,
                   updated_at=CURRENT_TIMESTAMP
                FROM (VALUES {values}) AS i(campaign_id,delta)
                WHERE s.campaign_id=i.campaign_id AND s.stats_date=?""",
            [value for campaign_id, count in daily_sent_increments.items()
             for value in (campaign_id, count)] + [stats_date],
        )


def flush_pending_writes(timeout: float = 10.0) -> None:
    from services import sms_write_buffer
    sms_write_buffer.flush(timeout)


def create_external_task(campaign_id: int, campaign_name: str, msisdn: str,
                         language: str, content: str, source_addr: str,
                         payload: Optional[dict] = None) -> dict:
    """Create a directly-consumed task so all SMS paths share task accounting."""
    row = write_transaction(
        lambda conn: conn.execute(
            """INSERT INTO sms_tasks
               (campaign_type,campaign_id,campaign_name,msisdn,language,content,
                source_addr,payload,source_file,dedupe_key,status,attempts,max_attempts)
               VALUES ('rabbitmq',?,?,?,?,?,?,?,'rabbitmq',?,'sending',1,1)
               RETURNING *""",
            (int(campaign_id), campaign_name or "", msisdn or "", language or "",
             content or "", source_addr or "", json.dumps(payload or {}, ensure_ascii=False),
             f"rabbitmq:{uuid.uuid4().hex}"),
        ).fetchone(),
        attempts=3,
        operation="sms_tasks.external_create",
    )
    return dict(row)


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
    submit_activity = set()
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
            current = conn.execute(
                "SELECT status,claim_token FROM sms_tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            logger.warning(
                "Buffered submit callback fenced out task=%s current_status=%s "
                "current_claim=%s payload_claim=%s attempt=%s",
                task_id,
                current["status"] if current else "missing",
                current["claim_token"] if current else "",
                str(payload.get("claim_token") or ""),
                attempt_token,
            )
            continue
        submit_activity.add((
            row["campaign_type"], int(row["campaign_id"]),
            int(payload.get("transport_smpp_config_id") or 0),
        ))
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

    _record_submit_activity(conn, submit_activity)

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
            "_transport_smpp_config_id", "_accepted_parts", "_fast_mode",
            "_fast_batch_id", "_fast_lease_id", "_fast_part_results",
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
    # Fast mode keeps successful part responses in memory while the task is
    # still a candidate for a normal success checkpoint.  If the multipart
    # submission fails, materialize that small exceptional evidence before
    # folding the task result so partial is never inferred from a counter
    # alone and can be audited after restart.
    fast_part_results = list(task.get("_fast_part_results") or [])
    if attempt_token and task.get("_fast_mode") and fast_part_results:
        for result in fast_part_results:
            conn.execute(
                """INSERT INTO sms_send_parts
                   (attempt_token,part_no,sequence,connection_generation,
                    source_namespace,response_status,smsc_message_id,state,
                    sent_at,responded_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                   ON CONFLICT(attempt_token,part_no) DO UPDATE SET
                     sequence=excluded.sequence,
                     source_namespace=excluded.source_namespace,
                     response_status=excluded.response_status,
                     smsc_message_id=excluded.smsc_message_id,
                     state=excluded.state,
                     responded_at=excluded.responded_at,
                     updated_at=excluded.updated_at""",
                (
                    attempt_token, int(result.get("part_no") or 0),
                    int(result.get("sequence") or 0), "",
                    str(task.get("_source_namespace") or "legacy"),
                    int(result.get("status") or 0),
                    str(result.get("message_id") or ""),
                    "accepted" if result.get("ok") else "rejected",
                ),
            )
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
        if task.get("_fast_mode") and task.get("_fast_batch_id"):
            item_state = status if status in {
                "retrying", "failed", "partial", "unknown", "cancelled", "expired",
            } else "failed"
            conn.execute(
                """UPDATE sms_fast_batch_items SET state=?,accepted_parts=?,
                       planned_parts=GREATEST(planned_parts,?),last_error=?,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE batch_id=? AND task_id=? AND claim_token=?""",
                (
                    item_state, accepted_parts,
                    int(task.get("_submit_sm_parts") or len(fast_part_results) or 1),
                    str(error)[:2000], str(task.get("_fast_batch_id") or ""),
                    int(task["id"]), str(task.get("claim_token") or ""),
                ),
            )
            conn.execute(
                """UPDATE sms_fast_batches SET unresolved_count=(
                       SELECT COUNT(*) FROM sms_fast_batch_items
                       WHERE batch_id=? AND state IN ('unknown','partial','failed')),
                       updated_at=CURRENT_TIMESTAMP,last_error=? WHERE batch_id=?""",
                (str(task.get("_fast_batch_id") or ""), str(error)[:2000],
                 str(task.get("_fast_batch_id") or "")),
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
    def recover(conn):
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
        count = sum(int(row["count"] or 0) for row in by_batch)
        unbatched = conn.execute(
            """SELECT COUNT(*) count FROM sms_tasks
               WHERE status='failed' AND failure_class='transport'
                 AND COALESCE(smsc_message_ids,'[]')='[]'
                 AND submitted_at IS NULL AND delivered_at IS NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM sms_send_attempts a
                     WHERE a.task_id=sms_tasks.id
                       AND a.state IN ('authorized','responding','partial','accepted')
                 ) AND batch_id IS NULL"""
        ).fetchone()["count"]
        count += int(unbatched or 0)
        if not count:
            return 0
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
        return int(count)

    return write_transaction(
        recover,
        attempts=3,
        operation="sms_tasks.requeue_transport_failures",
    )


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
        # A receipt is evidence of an external submission, never proof that
        # the whole SMS was not written. Resolve its attempt before changing
        # the current task, including the explicit task_id replay path.
        bindings = conn.execute(
            """SELECT m.attempt_token,m.part_no,a.claim_token,a.planned_parts,a.created_at
               FROM sms_message_bindings_v2 m
               LEFT JOIN sms_send_attempts a ON a.attempt_token=m.attempt_token AND a.task_id=m.task_id
               WHERE m.source_namespace=? AND m.smsc_message_id=? AND m.task_id=?""",
            (str(source_namespace or "legacy"), message_id, task_id),
        ).fetchall()
        planned_parts = 0
        if len(bindings) == 1 and bindings[0]["attempt_token"]:
            binding = bindings[0]
            if (binding["claim_token"] is None
                    or binding["claim_token"] != task.get("claim_token")
                    or conn.execute(
                        """SELECT 1 FROM sms_send_attempts WHERE task_id=?
                           AND attempt_token<>? AND created_at>=? LIMIT 1""",
                        (task_id, binding["attempt_token"], binding["created_at"]),
                    ).fetchone()):
                return
            planned_parts = int(binding["planned_parts"] or 0)
        elif (len(bindings) > 1 or conn.execute(
                "SELECT 1 FROM sms_send_attempts WHERE task_id=? LIMIT 1", (task_id,),
        ).fetchone()):
            return
        elif not bindings:
            # Pre-namespace legacy mappings are usable only without modern
            # attempt ownership. An arbitrary task_id is not a message binding.
            if source_namespace != "legacy" or not conn.execute(
                "SELECT 1 FROM sms_task_message_ids WHERE task_id=? AND smsc_message_id=?",
                (task_id, message_id),
            ).fetchone():
                return
        if task.get("status") in {"unknown", "partial", "queued", "retrying", "sending", "cancelled", "expired"}:
            # Retain the receipt fact above while preserving unresolved parts
            # and claims. Submission replay handles receipts that arrived early.
            return
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
                    WHERE task_id=? AND source_namespace=?
                      AND message_state IN ('DELIVRD','DELIVERED','2')""",
                (task_id, str(source_namespace or "legacy")),
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
                and len(expected_ids) >= planned_parts
                and expected_ids.issubset(delivered_ids)
            )
            status = "delivered" if all_parts_delivered else ("accepted" if task.get("status") == "accepted" else "submitted")
            delivered_sql = ", delivered_at=CURRENT_TIMESTAMP" if all_parts_delivered else ""
        elif normalized_state in {"ACCEPTD", "ENROUTE", "SCHEDULED", "1", "6", "7"}:
            status = task.get("status") if task.get("status") in {"accepted", "failed", "delivered"} else "submitted"
            delivered_sql = ""
        else:
            status = "failed"
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
    active = "('queued','sending','retrying','submitted','accepted')"
    def cleanup(conn):
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
        return len(ids)

    deleted_count = write_transaction(
        cleanup,
        attempts=3,
        operation="sms_tasks.retention_cleanup",
    )
    if deleted_count:
        logger.info("SMS task retention cleanup deleted=%s retention_days=%s", deleted_count, retention_days)
    return archived_count if archive_enabled else deleted_count

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
               smpp_config_id: int = -1, cursor: Optional[int] = None,
               msisdn: str = "") -> dict:
    conditions, params = [], []
    normalized_msisdn = normalize_msisdn(msisdn)
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
    if normalized_msisdn:
        conditions.append("msisdn=?")
        params.append(normalized_msisdn)
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
        rows = read_transaction(
            lambda conn: conn.execute(
                """SELECT status, COUNT(*) AS count,
                          SUM(CASE WHEN CAST(created_at AS DATE)=CURRENT_DATE THEN 1 ELSE 0 END) AS today_count
                   FROM sms_tasks
                   GROUP BY status"""
            ).fetchall(),
            operation="sms_tasks.stats",
        )
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
        current_date = _business_now().date().isoformat()
        if str(task_date)[:10] != current_date:
            return False, "task belongs to a previous day"
    campaign = _cached_campaign(None, task["campaign_type"], task["campaign_id"])
    if not campaign:
        return False, "campaign no longer exists"
    return campaign_is_active(campaign)


def defer_task(task_id: int, reason: str, expired: bool = False,
               claim_token: str = "", delay_seconds: int = 300):
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
                    error_msg=?,next_attempt_at=CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                    updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='sending' AND claim_token=?""",
                (reason, max(0, int(delay_seconds or 0)), task_id, str(claim_token or "")),
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
