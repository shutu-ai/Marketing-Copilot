import logging
import os
import queue
import threading
import time

from database import get_connection
from services import (batch_campaign_service, realtime_campaign_service, sms_task_service,
                      p1_service, ingestion_job_service)

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None
_sftp_thread = None
_batch_worker_stop = threading.Event()
_batch_worker_thread = None
_batch_jobs = queue.Queue(maxsize=1)
_batch_state_lock = threading.Lock()
_batch_job_active = False


def _submit_batch_job(campaign_id: int, generate: bool = False) -> bool:
    with _batch_state_lock:
        if _batch_job_active or not _batch_jobs.empty():
            return False
        try:
            _batch_jobs.put_nowait((int(campaign_id), bool(generate)))
        except queue.Full:
            return False
    logger.debug("Submitted batch ingestion campaign=%s generate=%s", campaign_id, generate)
    return True


def _batch_worker():
    global _batch_job_active
    while not _batch_worker_stop.is_set():
        try:
            campaign_id, generate = _batch_jobs.get(timeout=0.5)
        except queue.Empty:
            continue
        with _batch_state_lock:
            _batch_job_active = True
        try:
            if generate:
                conn = get_connection()
                try:
                    row = conn.execute(
                        "SELECT batch_size FROM batch_campaigns WHERE id=? AND status='in_progress'",
                        (campaign_id,),
                    ).fetchone()
                finally:
                    conn.close()
                if row:
                    batch_campaign_service.generate_batches(campaign_id, row["batch_size"] or 500)
            batch_campaign_service.process_pending_batches(
                campaign_id, stop_event=_batch_worker_stop
            )
        except Exception:
            logger.exception("Batch ingestion worker failed campaign=%s", campaign_id)
        finally:
            with _batch_state_lock:
                _batch_job_active = False
            _batch_jobs.task_done()


def _process_batches():
    conn = get_connection()
    try:
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
                _submit_batch_job(candidate["id"], generate=True)
        else:
            pending = conn.execute(
                """SELECT c.id FROM batch_campaigns c
                   JOIN batch_campaign_batches b ON b.campaign_id=c.id
                   WHERE c.status='in_progress' AND b.status='pending'
                   ORDER BY b.created_at LIMIT 1"""
            ).fetchone()
            if pending:
                _submit_batch_job(pending["id"])
    finally:
        conn.close()
    # Daily-repeat batch campaigns: reset batches once per day while the campaign
    # is still in progress, so the same target list is sent again each eligible day.
    batch_campaign_service.reset_daily_batches()
    # Cancel the day's unfinished tasks once the daily send window has closed.
    batch_campaign_service.cancel_overdue_daily_tasks()


def _run():
    ticks = 0
    while not _stop.wait(3):
        try:
            ticks += 1
            from services import metrics_service
            metrics_service.set_gauge("realtime_scheduler_last_tick_timestamp", time.time())
            _process_batches()
            realtime_campaign_service.finalize_daily_tasks()
            if realtime_campaign_service.has_active_realtime_campaigns():
                realtime_campaign_service.sync_watch_dirs_to_inboxes()
                realtime_result = realtime_campaign_service.process_realtime_inboxes()
                if realtime_result.get("errors"):
                    logger.error("Realtime inbox registration errors=%s", len(realtime_result["errors"]))
            if ticks % 20 == 0:
                ingestion_job_service.recover_expired_chunks()
                realtime_campaign_service.cleanup_realtime_retention()
                from services import realtime_whitelist_service
                realtime_whitelist_service.cleanup_versions()
                sms_task_service.cleanup_task_retention()
                p1_service.auto_complete_campaigns()
                from services import metrics_service, task_worker
                from services.smpp_gateway import gateway
                metrics_service.evaluate_alerts(task_worker.worker_status()['running'], gateway.status())
        except Exception:
            logger.exception("Activity scheduler iteration failed")


def _run_sftp_watch():
    """Poll remote SSH/SFTP sources separately so network timeouts do not stall scheduling."""
    while not _stop.is_set():
        try:
            result = realtime_campaign_service.sync_sftp_watch_dirs_to_inboxes()
            if result.get("downloaded"):
                logger.info("SFTP watch sync complete downloaded=%s", result["downloaded"])
        except Exception:
            logger.exception("SFTP watch sync iteration failed")
        if _stop.wait(15):
            break


def start_scheduler():
    global _thread, _sftp_thread, _batch_worker_thread
    if _thread and _thread.is_alive():
        return
    with _batch_state_lock:
        while True:
            try:
                _batch_jobs.get_nowait()
                _batch_jobs.task_done()
            except queue.Empty:
                break
    _stop.clear()
    _batch_worker_stop.clear()
    recovered = ingestion_job_service.recover_expired_chunks()
    if recovered:
        logger.warning("Recovered %s expired ingestion chunk leases", recovered)
    _batch_worker_thread = threading.Thread(
        target=_batch_worker, name="batch-ingestion-worker", daemon=True
    )
    _batch_worker_thread.start()
    from services import realtime_ingestion_worker
    realtime_ingestion_worker.start_worker()
    _thread = threading.Thread(target=_run, name="activity-scheduler", daemon=True)
    _thread.start()
    _sftp_thread = threading.Thread(target=_run_sftp_watch, name="sftp-watch-poller", daemon=True)
    _sftp_thread.start()
    logger.info("Activity scheduler started")


def stop_scheduler():
    _stop.set()
    _batch_worker_stop.set()
    worker = _batch_worker_thread
    if worker and worker.is_alive():
        worker.join(timeout=2)
    sftp_worker = _sftp_thread
    if sftp_worker and sftp_worker.is_alive():
        sftp_worker.join(timeout=18)
    from services import realtime_ingestion_worker
    realtime_ingestion_worker.stop_worker()
