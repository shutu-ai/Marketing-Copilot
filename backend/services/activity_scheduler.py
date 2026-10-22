import logging
import os
import queue
import threading
import time

from database import is_transient_write_error, read_transaction
from services import (batch_campaign_service, realtime_campaign_service, sms_task_service,
                      p1_service, ingestion_job_service, vnext_service)

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None
_sftp_thread = None
_batch_worker_stop = threading.Event()
_batch_worker_thread = None
_batch_jobs = queue.Queue(maxsize=1)
_batch_state_lock = threading.Lock()
_batch_job_active = False
_batch_campaign_cursor = 0
try:
    _BATCH_RESET_INTERVAL = max(
        30.0, float(os.environ.get("MC_BATCH_RESET_INTERVAL_SECONDS", "60"))
    )
except (TypeError, ValueError):
    _BATCH_RESET_INTERVAL = 60.0
try:
    _BATCH_CANCEL_INTERVAL = max(
        15.0, float(os.environ.get("MC_BATCH_CANCEL_INTERVAL_SECONDS", "30"))
    )
except (TypeError, ValueError):
    _BATCH_CANCEL_INTERVAL = 30.0
_last_batch_reset = 0.0
_last_batch_cancel = 0.0


def _vnext_active() -> bool:
    try:
        from services import vnext_service
        return vnext_service.is_active()
    except Exception:
        return False


def _batch_quantum_seconds() -> float:
    try:
        return max(0.05, float(os.environ.get("MC_BATCH_QUANTUM_SECONDS", "1.5")))
    except (TypeError, ValueError):
        return 1.5


def _run_stage(name, callback):
    """Run one scheduler stage without starving unrelated stages on failure."""
    try:
        return callback()
    except Exception as exc:
        try:
            from services import metrics_service
            metrics_service.inc(
                "marketing_copilot_scheduler_stage_failures_total",
                labels={"stage": name},
            )
        except Exception:
            pass
        if is_transient_write_error(exc):
            logger.warning(
                "Scheduler stage deferred after transient database contention "
                "stage=%s error=%s",
                name, exc,
            )
        else:
            logger.exception("Scheduler stage failed stage=%s", name)
        return None


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
            row = read_transaction(
                lambda conn: conn.execute(
                    "SELECT batch_size FROM batch_campaigns WHERE id=? AND status='in_progress'",
                    (campaign_id,),
                ).fetchone(),
                operation="scheduler.batch_worker.campaign",
            )
            if row:
                # Generation and processing each receive a bounded quantum.
                # The worker returns to the campaign selector after one turn,
                # so a large file cannot occupy the only batch worker forever.
                generated = batch_campaign_service.generate_batches(
                    campaign_id, row["batch_size"] or 500,
                    max_batches=batch_campaign_service._BATCH_INGEST_QUANTUM,
                )
                if generated.get("success"):
                    batch_campaign_service.process_pending_batches(
                        campaign_id,
                        stop_event=_batch_worker_stop,
                        max_batches=batch_campaign_service._BATCH_INGEST_QUANTUM,
                        quantum_seconds=_batch_quantum_seconds(),
                    )
                from services import campaign_capacity_service
                campaign_capacity_service.record_activity_event_transaction(
                    "batch", campaign_id, "dispatch",
                )
        except Exception:
            logger.exception("Batch ingestion worker failed campaign=%s", campaign_id)
        finally:
            with _batch_state_lock:
                _batch_job_active = False
            _batch_jobs.task_done()


def _process_batches():
    global _last_batch_reset, _last_batch_cancel, _batch_campaign_cursor
    campaign_ids = batch_campaign_service.pending_campaign_ids()
    if campaign_ids:
        start = _batch_campaign_cursor % len(campaign_ids)
        ordered = campaign_ids[start:] + campaign_ids[:start]
        _batch_campaign_cursor = (_batch_campaign_cursor + 1) % len(campaign_ids)
        # Activity eligibility is checked again immediately before processing;
        # this filter only avoids spending the turn on a closed campaign.
        rows = read_transaction(
            lambda conn: conn.execute(
                "SELECT * FROM batch_campaigns WHERE id IN (" +
                ",".join("?" for _ in ordered) + ")", ordered,
            ).fetchall(),
            operation="scheduler.batch_campaign_selector",
        )
        by_id = {int(row["id"]): dict(row) for row in rows}
        for campaign_id in ordered:
            candidate = by_id.get(int(campaign_id))
            if not candidate:
                continue
            active, _ = sms_task_service.campaign_is_active(candidate)
            if active and _submit_batch_job(campaign_id, generate=True):
                break
    # These operations are daily/window-boundary maintenance. Running them on
    # every three-second scheduler tick created unnecessary write contention.
    now = time.monotonic()
    if _vnext_active():
        # VNext owns its durable business-day epoch.  Legacy reset/cancel SQL
        # must not be allowed to stand in for the spool cutoff protocol.
        _run_stage("vnext_daily_rollover", vnext_service.rollover_if_needed)
    elif now - _last_batch_reset >= _BATCH_RESET_INTERVAL:
        _last_batch_reset = now
        _run_stage("batch_daily_reset", batch_campaign_service.reset_daily_batches)
    if now - _last_batch_cancel >= _BATCH_CANCEL_INTERVAL:
        _last_batch_cancel = now
        if not _vnext_active():
            _run_stage(
                "batch_overdue_cancel",
                batch_campaign_service.cancel_overdue_daily_tasks,
            )


def _run():
    ticks = 0
    while not _stop.wait(3):
        try:
            ticks += 1
            from services import metrics_service
            metrics_service.set_gauge("realtime_scheduler_last_tick_timestamp", time.time())
            _run_stage("batch_scheduler", _process_batches)
            if not _vnext_active():
                _run_stage("realtime_daily_finalize", realtime_campaign_service.finalize_daily_tasks)
            active = _run_stage(
                "realtime_active_check",
                realtime_campaign_service.has_active_realtime_campaigns,
            )
            if active:
                _run_stage(
                    "realtime_watch_sync",
                    lambda: realtime_campaign_service.sync_watch_dirs_to_inboxes(enqueue_only=True),
                )
                realtime_result = _run_stage(
                    "realtime_inbox_processing",
                    realtime_campaign_service.process_realtime_inboxes,
                ) or {}
                if realtime_result.get("errors"):
                    logger.error("Realtime inbox registration errors=%s", len(realtime_result["errors"]))
            if ticks % 20 == 0:
                _run_stage("recover_expired_chunks", ingestion_job_service.recover_expired_chunks)
                _run_stage("realtime_retention", realtime_campaign_service.cleanup_realtime_retention)
                from services import realtime_whitelist_service
                _run_stage("whitelist_cleanup", realtime_whitelist_service.cleanup_versions)
                if _vnext_active():
                    # Retry a deferred control message without entering the
                    # VNext send path or restarting its workers.
                    _run_stage("vnext_retention_config", vnext_service.apply_retention_days)
                else:
                    _run_stage("task_retention", sms_task_service.cleanup_task_retention)
                _run_stage("campaign_auto_complete", p1_service.auto_complete_campaigns)
                if not _vnext_active():
                    from services import metrics_service, task_worker
                    from services.smpp_gateway import gateway
                    _run_stage(
                        "alert_evaluation",
                        lambda: metrics_service.evaluate_alerts(
                            task_worker.worker_status()['running'], gateway.status()
                        ),
                    )
        except Exception:
            # Keep the scheduler thread alive even if a future stage is added
            # without the per-stage guard above.
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


def _recover_expired_chunks_on_startup() -> int:
    """Recover ingestion leases without making startup fail on contention.

    The SMS result buffer and task workers can have durable work to flush as
    soon as they start.  A short-lived DuckDB writer conflict during startup
    is therefore an availability condition, not an application bootstrap
    failure.  The normal scheduler loop retries this stage periodically.
    """
    try:
        recovered = ingestion_job_service.recover_expired_chunks()
    except Exception as exc:
        if not is_transient_write_error(exc):
            raise
        logger.warning(
            "Startup ingestion recovery deferred after transient database contention "
            "error=%s",
            exc,
        )
        return 0
    if recovered:
        logger.warning("Recovered %s expired ingestion chunk leases", recovered)
    return recovered


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
    _recover_expired_chunks_on_startup()
    _batch_worker_thread = threading.Thread(
        target=_batch_worker, name="batch-ingestion-worker", daemon=True
    )
    _batch_worker_thread.start()
    realtime_campaign_service.start_watch_copy_workers()
    from services import realtime_ingestion_worker
    realtime_ingestion_worker.start_worker()
    _thread = threading.Thread(target=_run, name="activity-scheduler", daemon=True)
    _thread.start()
    _sftp_thread = threading.Thread(target=_run_sftp_watch, name="sftp-watch-poller", daemon=True)
    _sftp_thread.start()
    logger.info("Activity scheduler started")


def stop_scheduler():
    logger.info("Activity scheduler stopping")
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
    realtime_campaign_service.stop_watch_copy_workers()
    logger.info("Activity scheduler stopped")
