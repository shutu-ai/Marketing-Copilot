import logging
import os
import threading
import time

from database import get_connection
from services import batch_campaign_service, realtime_campaign_service, sms_task_service, p1_service

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None


def _process_batches():
    conn = get_connection()
    unbatched = conn.execute(
        """SELECT c.* FROM batch_campaigns c
           WHERE c.status='in_progress' AND c.customer_list_path <> ''
             AND NOT EXISTS (SELECT 1 FROM batch_campaign_batches b WHERE b.campaign_id=c.id)
           ORDER BY c.created_at LIMIT 1"""
    ).fetchone()
    if unbatched:
        candidate = dict(unbatched)
        active, _ = sms_task_service.campaign_is_active(candidate)
        if active:
            conn.close()
            batch_campaign_service.generate_batches(candidate["id"], candidate.get("batch_size") or 500)
            conn = get_connection()
    rows = conn.execute(
        """SELECT b.id AS batch_id, c.* FROM batch_campaign_batches b
           JOIN batch_campaigns c ON c.id=b.campaign_id
           WHERE b.status='pending' AND c.status='in_progress'
           ORDER BY b.created_at LIMIT 1"""
    ).fetchall()
    conn.close()
    if rows:
        campaign = dict(rows[0])
        active, _ = sms_task_service.campaign_is_active(campaign)
        if active:
            return batch_campaign_service.process_batch(rows[0]["batch_id"])
    return None


def _process_realtime_inboxes():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM realtime_campaigns WHERE status='in_progress'").fetchall()
    conn.close()
    for row in rows:
        campaign = dict(row)
        active, _ = sms_task_service.campaign_is_active(campaign)
        if not active or not campaign.get("campaign_dir"):
            continue
        inbox = os.path.join(campaign["campaign_dir"], "inbox")
        os.makedirs(inbox, exist_ok=True)
        for name in sorted(os.listdir(inbox)):
            path = os.path.join(inbox, name)
            if not os.path.isfile(path) or os.path.splitext(name)[1].lower() not in {".csv", ".xlsx", ".xls"}:
                continue
            if time.time() - os.path.getmtime(path) < 2:
                continue
            result = realtime_campaign_service.receive_file(campaign["id"], path, name)
            if not result.get("success"):
                logger.error("Realtime inbox file failed %s: %s", path, result.get("message"))


def _run():
    ticks = 0
    while not _stop.wait(3):
        try:
            ticks += 1
            _process_batches()
            _process_realtime_inboxes()
            if ticks % 20 == 0:
                p1_service.auto_complete_campaigns()
                from services import metrics_service, task_worker
                from services.smpp_gateway import gateway
                metrics_service.evaluate_alerts(task_worker.worker_status()['running'], gateway.status())
        except Exception:
            logger.exception("Activity scheduler iteration failed")


def start_scheduler():
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_run, name="activity-scheduler", daemon=True)
    _thread.start()
    logger.info("Activity scheduler started")


def stop_scheduler():
    _stop.set()
