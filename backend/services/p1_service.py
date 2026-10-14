import json
import os
import logging
from datetime import datetime
from typing import Optional

from database import get_connection, get_database_file, read_transaction, write_transaction
from models.batch_campaign import BatchCampaignUpdate
from models.realtime_campaign import RealtimeCampaignUpdate
from services import batch_campaign_service, operations_service, realtime_campaign_service, sms_task_service

logger = logging.getLogger(__name__)


CAMPAIGN_TRANSITIONS = {
    "not_started": {"start": "in_progress", "cancel": "cancelled"},
    "in_progress": {"pause": "paused", "cancel": "cancelled", "complete": "completed"},
    "paused": {"resume": "in_progress", "cancel": "cancelled", "complete": "completed"},
}


def _campaign_service(campaign_type: str):
    if campaign_type == "batch":
        return batch_campaign_service, BatchCampaignUpdate
    if campaign_type == "realtime":
        return realtime_campaign_service, RealtimeCampaignUpdate
    raise ValueError("campaign_type must be batch or realtime")


def campaign_action(campaign_type: str, campaign_id: int, action: str, automatic: bool = False) -> dict:
    service, update_model = _campaign_service(campaign_type)
    campaign = service.get_one(campaign_id)
    if not campaign:
        raise LookupError("campaign not found")
    current = campaign.get("status") or "not_started"
    target = CAMPAIGN_TRANSITIONS.get(current, {}).get(action)
    if not target:
        raise ValueError(f"cannot {action} campaign from {current}")
    if target == "in_progress" and action == "start":
        # Preserve the existing readiness/template validation for the initial
        # start. The service update itself now applies the generation fence and
        # task reconciliation in its transaction.
        updated = service.update(campaign_id, update_model(status=target))
        task_summary = {"terminated": 0, "requeued": 0}
    else:
        transition = sms_task_service.transition_campaign_status(
            campaign_type, campaign_id, current, target,
        )
        task_summary = transition["tasks"]
        # Fetch the enriched/public representation only after the CAS commit;
        # the activity and its task fence are already one durable state change.
        updated = service.get_one(campaign_id)
    if not updated:
        raise LookupError("campaign not found")
    if campaign_type == "realtime":
        updated = realtime_campaign_service.public_campaign(updated)
    cancelled_task_count = int(task_summary.get("terminated", 0) or 0)
    try:
        from services.smpp_gateway import gateway
        gateway.invalidate_campaign(
            campaign_type, campaign_id,
            int(updated.get("dispatch_generation") or 0),
        )
    except Exception:
        # Queue invalidation is an acceleration; the durable generation fence
        # remains authoritative if the live gateway is not initialized.
        logger.debug("Unable to invalidate live SMPP queues", exc_info=True)
    operations_service.audit(
        "campaign.auto_complete" if automatic else f"campaign.{action}",
        f"{campaign_type}_campaign", campaign_id,
        {"from_status": current, "to_status": target, "cancelled_task_count": cancelled_task_count},
    )
    logger.info(
        "Campaign action campaign_type=%s campaign=%s action=%s from_status=%s "
        "to_status=%s automatic=%s cancelled_tasks=%s",
        campaign_type, campaign_id, action, current, target,
        str(bool(automatic)).lower(), cancelled_task_count,
    )
    if target == "cancelled":
        updated["cancelled_task_count"] = cancelled_task_count
    return updated


def _parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except (TypeError, ValueError):
        return None


def auto_complete_campaigns() -> int:
    now = datetime.now()
    completed = 0
    for campaign_type, service in (("batch", batch_campaign_service), ("realtime", realtime_campaign_service)):
        for campaign in service.get_all():
            if campaign.get("status") != "in_progress":
                continue
            end_time = _parse_time(campaign.get("end_time", ""))
            should_complete = bool(end_time and end_time <= now)
            # Batch campaigns now repeat daily within their date range, so they
            # only auto-complete when the end time has passed (or are ended manually).
            if should_complete:
                campaign_action(campaign_type, campaign["id"], "complete", automatic=True)
                completed += 1
    return completed


def campaign_progress(campaign_type: str, campaign_id: int) -> dict:
    service, _ = _campaign_service(campaign_type)
    campaign = service.get_one(campaign_id)
    if not campaign:
        raise LookupError("campaign not found")
    def read(conn):
        task_rows = conn.execute(
            "SELECT status,COUNT(*) count FROM sms_tasks WHERE campaign_type=? AND campaign_id=? GROUP BY status",
            (campaign_type, campaign_id)).fetchall()
        task_counts = {row["status"]: row["count"] for row in task_rows}
        files = conn.execute(
            """SELECT COUNT(*) total, SUM(CASE WHEN status IN ('completed','quarantined','failed') THEN 1 ELSE 0 END) finished,
               COALESCE(SUM(total_rows),0) row_count, COALESCE(SUM(created_tasks),0) task_count
               FROM processed_files WHERE campaign_type=? AND campaign_id=?""",
            (campaign_type, campaign_id)).fetchone()
        batches = None
        if campaign_type == "batch":
            row = conn.execute(
                """SELECT COUNT(*) total, SUM(CASE WHEN status IN ('sent','skipped','failed') THEN 1 ELSE 0 END) finished
                   FROM batch_campaign_batches WHERE campaign_id=?""", (campaign_id,)).fetchone()
            batches = {"total": int(row["total"] or 0), "finished": int(row["finished"] or 0)}
        return task_counts, files, batches

    task_counts, files, batches = read_transaction(read, operation="operations.campaign_progress")
    task_total = sum(task_counts.values())
    # ``accepted`` is the terminal business state when the SMPP configuration
    # does not require a delivery receipt. Required-receipt tasks remain in
    # ``submitted`` until a delivery receipt moves them to delivered/failed.
    task_terminal = sum(task_counts.get(s, 0) for s in (
        "delivered", "accepted", "failed", "expired", "cancelled", "archived"
    ))
    return {
        "campaign_type": campaign_type, "campaign_id": campaign_id,
        "name": campaign.get("name", ""), "status": campaign.get("status", ""),
        "tasks": {"total": task_total, "finished": task_terminal, "percent": round(task_terminal * 100 / task_total, 1) if task_total else 0, "by_status": task_counts},
        "files": {"total": int(files["total"] or 0), "finished": int(files["finished"] or 0), "rows": int(files["row_count"] or 0), "created_tasks": int(files["task_count"] or 0)},
        "batches": batches,
    }


def all_campaign_progress() -> list:
    result = []
    for campaign_type, service in (("batch", batch_campaign_service), ("realtime", realtime_campaign_service)):
        for campaign in service.get_all():
            result.append(campaign_progress(campaign_type, campaign["id"]))
    return result


def list_campaign_progress(page: int = 1, page_size: int = 50) -> dict:
    def read(conn):
        total = conn.execute(
            """SELECT (SELECT COUNT(*) FROM batch_campaigns) +
                      (SELECT COUNT(*) FROM realtime_campaigns) cnt"""
        ).fetchone()["cnt"]
        rows = conn.execute(
            """SELECT campaign_type, campaign_id FROM (
                   SELECT 'batch' AS campaign_type, id AS campaign_id, created_at FROM batch_campaigns
                   UNION ALL
                   SELECT 'realtime' AS campaign_type, id AS campaign_id, created_at FROM realtime_campaigns
               ) ORDER BY created_at DESC LIMIT ? OFFSET ?""",
            (page_size, max(0, page - 1) * page_size),
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="operations.campaign_progress.list")
    items = [campaign_progress(row["campaign_type"], row["campaign_id"]) for row in rows]
    return {"total": total, "page": page, "page_size": page_size, "items": items}


def bulk_failed_tasks(action: str, task_ids=None, campaign_type: str = "", campaign_id: int = 0) -> dict:
    conditions = ["status IN ('failed','expired')"]
    params = []
    if task_ids:
        conditions.append("id IN (" + ",".join("?" for _ in task_ids) + ")")
        params.extend(task_ids)
    if campaign_type:
        conditions.append("campaign_type=?")
        params.append(campaign_type)
    if campaign_id:
        conditions.append("campaign_id=?")
        params.append(campaign_id)

    def write(conn):
        rows = conn.execute(
            "SELECT * FROM sms_tasks WHERE " + " AND ".join(conditions), params
        ).fetchall()
        if action == "retry":
            retry_by_batch = {}
            for row in rows:
                conn.execute(
                    """UPDATE sms_tasks SET status='queued',attempts=0,error_msg='',failure_class='',
                       next_attempt_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                    (row["id"],),
                )
                _task_event(conn, row["id"], "bulk_retry", row["status"], "queued", "bulk retry requested")
                if row["batch_id"]:
                    retry_by_batch[row["batch_id"]] = retry_by_batch.get(row["batch_id"], 0) + 1
            for batch_id, count in retry_by_batch.items():
                conn.execute(
                    """UPDATE batch_campaign_batches
                       SET failed_count=GREATEST(COALESCE(failed_count,0)-?,0),
                           updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                    (count, batch_id),
                )
        elif action == "archive":
            for raw in rows:
                row = dict(raw)
                conn.execute(
                    """INSERT OR IGNORE INTO failed_task_archive
                       (task_id,campaign_type,campaign_id,campaign_name,msisdn,content,final_status,attempts,failure_class,error_msg,snapshot)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (row["id"], row["campaign_type"], row["campaign_id"], row["campaign_name"], row["msisdn"],
                     row["content"], row["status"], row["attempts"], row.get("failure_class") or "", row.get("error_msg") or "",
                     json.dumps(row, ensure_ascii=False, default=str)),
                )
                conn.execute(
                    "UPDATE sms_tasks SET status='archived',updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (row["id"],),
                )
                _task_event(conn, row["id"], "archived", row["status"], "archived", "failure archived")
        else:
            raise ValueError("action must be retry or archive")
        return [row["id"] for row in rows]

    ids = write_transaction(
        write,
        attempts=3,
        operation=f"tasks.bulk.{action}",
        base_delay=0.1,
    )
    operations_service.audit_async(
        f"tasks.bulk_{action}", "sms_task", "bulk",
        {"count": len(ids), "task_ids": ids},
    )
    return {"success": True, "action": action, "count": len(ids)}


def _task_event(conn, task_id: int, event_type: str, from_status: str, to_status: str, message: str, detail=None):
    conn.execute(
        """INSERT INTO task_events(task_id,event_type,from_status,to_status,message,detail)
           VALUES (?,?,?,?,?,?)""",
        (task_id, event_type, from_status or "", to_status or "", message or "",
         json.dumps(detail or {}, ensure_ascii=False, default=str)))


def list_failure_archive(page: int = 1, page_size: int = 50,
                         cursor: Optional[int] = None) -> dict:
    def read(conn):
        total = conn.execute("SELECT COUNT(*) cnt FROM failed_task_archive").fetchone()["cnt"]
        where, params = ("WHERE id<?", [cursor]) if cursor else ("", [])
        rows = conn.execute(
            f"SELECT * FROM failed_task_archive {where} ORDER BY id DESC LIMIT ?",
            params + [page_size + 1],
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="operations.failure_archive.list")
    has_more = len(rows) > page_size
    items = [dict(row) for row in rows[:page_size]]
    return {"total": total, "page": page, "page_size": page_size, "items": items,
            "next_cursor": items[-1]["id"] if has_more and items else None,
            "has_more": has_more}


def list_failed_tasks(page: int = 1, page_size: int = 50,
                      cursor: Optional[int] = None) -> dict:
    def read(conn):
        total = conn.execute(
            "SELECT COUNT(*) cnt FROM sms_tasks WHERE status IN ('failed','expired')"
        ).fetchone()["cnt"]
        cursor_condition = " AND id<?" if cursor else ""
        params = [cursor] if cursor else []
        rows = conn.execute(
            f"""SELECT * FROM sms_tasks
                WHERE status IN ('failed','expired'){cursor_condition}
                ORDER BY id DESC LIMIT ?""",
            params + [page_size + 1],
        ).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="operations.failed_tasks.list")
    has_more = len(rows) > page_size
    items = [dict(row) for row in rows[:page_size]]
    return {"total": total, "page": page, "page_size": page_size, "items": items,
            "next_cursor": items[-1]["id"] if has_more and items else None,
            "has_more": has_more}


def task_timeline(task_id: int) -> dict:
    def read(conn):
        task = conn.execute("SELECT * FROM sms_tasks WHERE id=?", (task_id,)).fetchone()
        if not task:
            return None
        events = conn.execute("SELECT * FROM task_events WHERE task_id=? ORDER BY created_at,id", (task_id,)).fetchall()
        batch_events = conn.execute(
            """SELECT * FROM task_event_batches
               WHERE campaign_type=? AND campaign_id=?
                 AND (batch_id IS NULL OR batch_id IS NOT DISTINCT FROM ?)
                 AND ? BETWEEN first_task_id AND last_task_id
               ORDER BY created_at,id""",
            (task["campaign_type"], task["campaign_id"], task["batch_id"], task_id),
        ).fetchall()
        receipts = conn.execute("SELECT * FROM delivery_receipts WHERE task_id=? ORDER BY received_at,id", (task_id,)).fetchall()
        return task, events, batch_events, receipts

    result = read_transaction(read, operation="operations.task_timeline")
    if result is None:
        raise LookupError("task not found")
    task, events, batch_events, receipts = result
    return {"task": dict(task), "events": [dict(r) for r in events],
            "batch_events": [dict(r) for r in batch_events],
            "receipts": [dict(r) for r in receipts]}


def list_events(resolved: Optional[bool] = None, severity: str = "", page: int = 1, page_size: int = 50) -> dict:
    conditions, params = [], []
    if resolved is not None:
        conditions.append("resolved=?"); params.append(1 if resolved else 0)
    if severity:
        conditions.append("severity=?"); params.append(severity)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    def read(conn):
        total = conn.execute(f"SELECT COUNT(*) cnt FROM system_events {where}", params).fetchone()["cnt"]
        rows = conn.execute(f"SELECT * FROM system_events {where} ORDER BY resolved,severity DESC,created_at DESC LIMIT ? OFFSET ?",
                            params + [page_size, max(0, page - 1) * page_size]).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="operations.events.list")
    return {"total": total, "items": [dict(r) for r in rows], "page": page, "page_size": page_size}


def resolve_event(event_id: int, resolved: bool = True) -> bool:
    def write(conn):
        row = conn.execute(
            "SELECT id FROM system_events WHERE id=?", (event_id,)
        ).fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE system_events SET resolved=? WHERE id=?",
            (1 if resolved else 0, event_id),
        )
        return True

    resolved_ok = write_transaction(
        write,
        attempts=3,
        operation="system_event.resolve",
        base_delay=0.1,
    )
    if resolved_ok:
        operations_service.audit_async(
            "exception.resolve" if resolved else "exception.reopen",
            "system_event", event_id,
        )
    return resolved_ok


def configuration_health(record_events: bool = False) -> list:
    checks = []
    smpp, templates = read_transaction(
        lambda conn: (
            conn.execute("SELECT COUNT(*) cnt FROM smpp_configs WHERE enabled=1").fetchone()["cnt"],
            conn.execute("SELECT COUNT(*) cnt FROM sms_templates WHERE enabled=1").fetchone()["cnt"],
        ),
        operation="operations.configuration_health.core",
    )
    checks.append({"code": "SMPP_ENABLED", "category": "smpp", "ok": bool(smpp), "severity": "error", "message": "No enabled SMPP configuration"})
    checks.append({"code": "TEMPLATE_ENABLED", "category": "template", "ok": bool(templates), "severity": "error", "message": "No enabled SMS template"})
    db_path = get_database_file()
    checks.append({"code": "DATABASE_PATH", "category": "database", "ok": db_path == ":memory:" or os.path.isdir(os.path.dirname(os.path.abspath(db_path))), "severity": "error", "message": "Database directory does not exist"})
    realtime_watch_dirs = read_transaction(
        lambda conn: [
            dict(row) for row in conn.execute(
                "SELECT id,name,watch_dir FROM realtime_campaigns WHERE COALESCE(watch_dir,'')<>''"
            ).fetchall()
        ],
        operation="operations.configuration_health.realtime",
    )
    for item in realtime_watch_dirs:
        watch_dir = item.get("watch_dir", "")
        checks.append({
            "code": "REALTIME_WATCH_DIRECTORY",
            "category": "file",
            "ok": bool(watch_dir and os.path.isdir(watch_dir)),
            "severity": "warning",
            "message": f"Realtime watch directory does not exist: {watch_dir}",
            "context": {"campaign_id": item["id"], "campaign_name": item["name"]},
        })
    if record_events:
        for check in checks:
            if not check["ok"]:
                operations_service.event(check["category"], check["code"], check["message"], check["severity"], check.get("context"))
    return checks


def list_audit_logs(page: int = 1, page_size: int = 100, action: str = "") -> dict:
    where, params = ("WHERE action LIKE ?", [f"%{action}%"]) if action else ("", [])
    def read(conn):
        total = conn.execute(f"SELECT COUNT(*) cnt FROM audit_logs {where}", params).fetchone()["cnt"]
        rows = conn.execute(f"SELECT * FROM audit_logs {where} ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",
                            params + [page_size, max(0, page - 1) * page_size]).fetchall()
        return total, rows

    total, rows = read_transaction(read, operation="operations.audit_logs.list")
    return {"total": total, "items": [dict(r) for r in rows], "page": page, "page_size": page_size}
