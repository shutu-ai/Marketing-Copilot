"""Single-worker realtime file ingestion loop."""

import logging
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta

from database import get_connection
from services import ingestion_job_service, operations_service, realtime_campaign_service, realtime_whitelist_service
from services import realtime_ingestion_retry_service as retry_service

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None
_last_expiry_sweep_date = None
WORKER_ID = "realtime-ingestion-1"


def _processed_id(campaign_id: int, filename: str):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM processed_files WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
            (campaign_id, filename),
        ).fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def _discard_expired_queued_files(include_processing: bool = False):
    """Expire old-date jobs before the worker selects the next queue item."""
    conn = get_connection()
    try:
        active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
        rows = conn.execute(
            """SELECT f.id, f.campaign_id, f.filename, f.file_path,
                      c.campaign_dir, c.watch_filename_prefix
               FROM ingestion_files f
               JOIN realtime_campaigns c ON c.id=f.campaign_id
               WHERE f.campaign_type='realtime' AND f.status IN """ + active_statuses
        ).fetchall()
    finally:
        conn.close()

    today = datetime.now().date()
    expired_jobs = []
    for row in rows:
        job = dict(row)
        file_time = realtime_campaign_service._watch_file_timestamp(
            job["filename"], job.get("watch_filename_prefix") or "Android_4G_Switch_Somali_",
        )
        if not file_time or file_time.date() == today:
            continue
        expired_jobs.append(job)
    if not expired_jobs:
        return

    reason = "file timestamp is outside the current day"
    expired_ids = ingestion_job_service.expire_files(
        (job["id"] for job in expired_jobs), reason,
        include_processing=include_processing,
    )
    if not expired_ids:
        return
    discarded_jobs = [job for job in expired_jobs if job["id"] in expired_ids]
    conn = get_connection()
    try:
        conn.executemany(
            """UPDATE processed_files SET status='failed',file_path='',error_detail=?,
               processing_phase='failed',last_progress_at=CURRENT_TIMESTAMP,
               completed_at=CAST(CURRENT_TIMESTAMP AS VARCHAR)
               WHERE campaign_type='realtime' AND campaign_id=? AND filename=? AND status<>'completed'""",
            ((reason, job["campaign_id"], job["filename"]) for job in discarded_jobs),
        )
        conn.commit()
    finally:
        conn.close()

    for job in discarded_jobs:
        try:
            path = job.get("file_path", "")
            if path and os.path.isfile(path):
                os.remove(path)
        except OSError as exc:
            logger.warning(
                "Realtime expired queued file deletion failed campaign=%s file=%s error=%s",
                job["campaign_id"], job["filename"], exc,
            )
    logger.info("Realtime expired queued files discarded count=%s", len(discarded_jobs))


def _discard_claimed_expired_file(job: dict, campaign: dict, reason: str):
    ingestion_job_service.expire_file(job["id"], reason, include_processing=True)
    processed_id = _processed_id(campaign["id"], job["filename"])
    if processed_id:
        operations_service.finish_file(processed_id, "failed", "", error=reason)
    file_path = job.get("file_path", "")
    try:
        if file_path and os.path.isfile(file_path):
            os.remove(file_path)
    except OSError as exc:
        logger.warning(
            "Realtime expired file deletion failed campaign=%s file=%s error=%s",
            campaign["id"], job["filename"], exc,
        )
    realtime_campaign_service.finalize_daily_tasks()
    logger.info(
        "Realtime expired queued file discarded campaign=%s file=%s reason=%s worker_id=%s",
        campaign["id"], job["filename"], reason, WORKER_ID,
    )


def _cleanup_orphaned_intermediate_files():
    """Remove scratch files left behind by a previous worker process."""
    removed = 0
    temp_dir = tempfile.gettempdir()
    try:
        for filename in os.listdir(temp_dir):
            if not (filename.startswith("mc-realtime-") and filename.endswith(".duckdb")):
                continue
            path = os.path.join(temp_dir, filename)
            try:
                if os.path.isfile(path):
                    os.remove(path)
                    removed += 1
            except OSError as exc:
                logger.warning("Realtime scratch cleanup failed file=%s error=%s", path, exc)
    except OSError as exc:
        logger.warning("Realtime scratch directory scan failed dir=%s error=%s", temp_dir, exc)
    conn = get_connection()
    try:
        campaign_dirs = [
            row["campaign_dir"] for row in conn.execute(
                "SELECT campaign_dir FROM realtime_campaigns WHERE COALESCE(campaign_dir,'')<>''"
            ).fetchall()
        ]
    finally:
        conn.close()
    for campaign_dir in campaign_dirs:
        inbox_dir = os.path.join(campaign_dir, "inbox")
        try:
            for filename in os.listdir(inbox_dir):
                if not (filename.startswith(".sftp-") and filename.endswith(".part")):
                    continue
                path = os.path.join(inbox_dir, filename)
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                        removed += 1
                except OSError as exc:
                    logger.warning("Realtime SFTP temp cleanup failed file=%s error=%s", path, exc)
        except OSError:
            continue
    if removed:
        logger.info("Realtime orphaned intermediate files removed count=%s", removed)


def _mark_retry(processed_id, decision, error, retry_count):
    delay = retry_service.delay_for(decision, retry_count)
    next_retry = (datetime.now() + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
    ingestion_job_service.schedule_retry(
        _current_file_id, decision.error_class, error, delay,
        allow_unbounded=decision.error_class in {"WAITING_DEPENDENCY", "RESOURCE_LIMIT"},
    )
    if processed_id:
        operations_service.mark_file_retry(processed_id, decision.error_class, error, next_retry)
    logger.warning(
        "Realtime file retrying campaign=%s file=%s error_class=%s retry_count=%s next_retry_at=%s error=%s",
        _current_campaign_id, _current_filename, decision.error_class, retry_count + 1, next_retry, str(error)[:500],
    )


def _quarantine(job: dict, processed_id, decision, error, target_path=None):
    path = job.get("file_path", "")
    target = target_path or path
    if path and decision.quarantine:
        try:
            target = operations_service.quarantine(path, _campaign_dir, job["filename"])
        except OSError:
            pass
    ingestion_job_service.fail_file(job["id"], decision.error_class, str(error), "quarantined" if decision.quarantine else "failed")
    if processed_id:
        operations_service.finish_file(processed_id, "quarantined" if decision.quarantine else "failed", target, error=error)
    operations_service.event(
        "realtime_ingestion", decision.error_class, str(error),
        severity="error", context={"campaign_id": _current_campaign_id, "filename": _current_filename},
    )
    logger.error(
        "Realtime file failed campaign=%s file=%s phase=%s error_class=%s error=%s",
        _current_campaign_id, _current_filename, job.get("processing_phase", ""), decision.error_class, str(error)[:1000],
    )


def process_once() -> bool:
    global _current_file_id, _current_campaign_id, _current_filename, _campaign_dir
    global _last_expiry_sweep_date
    today = datetime.now().date()
    if _last_expiry_sweep_date != today:
        _discard_expired_queued_files()
        _last_expiry_sweep_date = today
    job = ingestion_job_service.claim_file("realtime", WORKER_ID)
    if not job:
        return False
    _current_file_id = job["id"]
    _current_campaign_id = job["campaign_id"]
    _current_filename = job["filename"]
    _campaign_dir = ""
    processed_id = None
    started = time.monotonic()
    try:
        conn = get_connection()
        try:
            campaign_row = conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (job["campaign_id"],)).fetchone()
        finally:
            conn.close()
        if not campaign_row:
            raise ValueError("campaign not found")
        campaign = dict(campaign_row)
        _campaign_dir = campaign.get("campaign_dir", "")
        file_time = realtime_campaign_service._watch_file_timestamp(
            job["filename"], campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_",
        )
        expired_reason = realtime_campaign_service._realtime_ingestion_expired_reason(
            campaign, filename=job["filename"],
        )
        if file_time and file_time.date() != datetime.now().date():
            expired_reason = "file timestamp is outside the current day"
        if expired_reason:
            _discard_claimed_expired_file(job, campaign, expired_reason)
            return True
        result = realtime_campaign_service.receive_file(
            campaign["id"], job["file_path"], job["filename"],
            ingestion_file_id=job["id"],
            whitelist_version_id=job.get("whitelist_version_id"), worker_id=WORKER_ID,
        )
        processed_id = _processed_id(campaign["id"], job["filename"])
        if result.get("status") == "expired":
            _discard_claimed_expired_file(
                job, campaign, result.get("message", "daily send window ended"),
            )
            return True
        if result.get("success"):
            ingestion_job_service.complete_file(job["id"])
            elapsed_ms = int((time.monotonic() - started) * 1000)
            from services import metrics_service
            metrics_service.observe("realtime_file_processing_seconds", elapsed_ms / 1000.0,
                                    {"campaign_id": campaign["id"], "result": "success"})
            logger.info(
                "Realtime file completed campaign=%s file=%s elapsed_ms=%s tasks_created=%s whitelist_version=%s worker_id=%s",
                campaign["id"], job["filename"], elapsed_ms, result.get("task_result", {}).get("created", 0),
                job.get("whitelist_version_id") or "", WORKER_ID,
            )
            return True
        decision = retry_service.classify(result.get("message", ""), result.get("status", ""))
        from services import metrics_service
        metrics_service.observe("realtime_file_processing_seconds", (time.monotonic() - started),
                                {"campaign_id": campaign["id"], "result": "failed"})
        metrics_service.inc("realtime_file_processing_errors_total",
                            labels={"campaign_id": campaign["id"], "error_class": decision.error_class})
        if result.get("status") == "waiting_for_whitelist":
            decision = retry_service.ErrorDecision("WAITING_DEPENDENCY", True, decision.delay_seconds)
        current_retry = int(job.get("retry_count", 0) or 0)
        if decision.retryable and (
            decision.error_class in {"WAITING_DEPENDENCY", "RESOURCE_LIMIT"}
            or current_retry < int(job.get("max_retry_count", retry_service.MAX_RETRIES) or retry_service.MAX_RETRIES)
        ):
            _mark_retry(processed_id, decision, result.get("message", "unknown"), current_retry)
        else:
            _quarantine(job, processed_id, decision, result.get("message", "unknown"), result.get("quarantine_path"))
        return True
    except Exception as exc:
        from services import metrics_service
        metrics_service.observe("realtime_file_processing_seconds", (time.monotonic() - started),
                                {"campaign_id": _current_campaign_id, "result": "failed"})
        metrics_service.inc("realtime_file_processing_errors_total",
                            labels={"campaign_id": _current_campaign_id, "error_class": retry_service.classify(exc).error_class})
        processed_id = processed_id or _processed_id(_current_campaign_id, _current_filename)
        decision = retry_service.classify(exc)
        current_retry = int(job.get("retry_count", 0) or 0)
        if decision.retryable and (
            decision.error_class in {"WAITING_DEPENDENCY", "RESOURCE_LIMIT"}
            or current_retry < int(job.get("max_retry_count", retry_service.MAX_RETRIES) or retry_service.MAX_RETRIES)
        ):
            _mark_retry(processed_id, decision, exc, current_retry)
        else:
            _quarantine(job, processed_id, decision, exc)
        return True


def _run():
    ingestion_job_service.recover_expired_files()
    while not _stop.wait(0.5):
        try:
            from services import metrics_service
            metrics_service.set_gauge("realtime_ingestion_worker_last_progress_timestamp", time.time())
            # Index construction is also persisted and resumable at the version
            # level; keeping it in this dedicated worker prevents scheduler ticks
            # from being held by a multi-million-row whitelist.
            realtime_whitelist_service.migrate_legacy_versions(limit=1)
            realtime_whitelist_service.build_pending(limit=1)
            if not process_once():
                continue
            metrics_service.set_gauge("realtime_ingestion_worker_last_progress_timestamp", time.time())
        except Exception:
            logger.exception("Realtime ingestion worker iteration failed")


def start_worker():
    global _thread, _last_expiry_sweep_date
    if _thread and _thread.is_alive():
        return
    try:
        ingestion_job_service.recover_expired_files()
        _discard_expired_queued_files(include_processing=True)
        realtime_campaign_service.cleanup_closed_window_ingestion_files(include_processing=True)
        _cleanup_orphaned_intermediate_files()
        _last_expiry_sweep_date = datetime.now().date()
    except Exception:
        logger.exception("Realtime expired-file startup cleanup failed")
    _stop.clear()
    _thread = threading.Thread(target=_run, name="realtime-ingestion-worker", daemon=True)
    _thread.start()
    logger.info("Realtime ingestion worker started concurrency=1")


def stop_worker():
    _stop.set()
    thread = _thread
    if thread and thread.is_alive():
        thread.join(timeout=2)


def worker_status() -> dict:
    thread = _thread
    return {"running": bool(thread and thread.is_alive()), "worker_id": WORKER_ID}


_current_file_id = None
_current_campaign_id = None
_current_filename = ""
_campaign_dir = ""
