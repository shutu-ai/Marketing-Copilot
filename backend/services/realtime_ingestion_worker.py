"""Single-worker realtime file ingestion loop."""

import logging
import threading
import time
from datetime import datetime, timedelta

from database import get_connection
from services import ingestion_job_service, operations_service, realtime_campaign_service, realtime_whitelist_service
from services import realtime_ingestion_retry_service as retry_service

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None
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


def _mark_retry(processed_id, decision, error, retry_count):
    delay = retry_service.delay_for(decision, retry_count)
    next_retry = (datetime.now() + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
    ingestion_job_service.schedule_retry(
        _current_file_id, decision.error_class, error, delay,
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
        result = realtime_campaign_service.receive_file(
            campaign["id"], job["file_path"], job["filename"],
            ingestion_file_id=job["id"],
            whitelist_version_id=job.get("whitelist_version_id"), worker_id=WORKER_ID,
        )
        processed_id = _processed_id(campaign["id"], job["filename"])
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
        if result.get("retryable"):
            decision = retry_service.ErrorDecision("WAITING_DEPENDENCY", True, decision.delay_seconds)
        current_retry = int(job.get("retry_count", 0) or 0)
        if decision.retryable and current_retry < int(job.get("max_retry_count", retry_service.MAX_RETRIES) or retry_service.MAX_RETRIES):
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
        if decision.retryable and current_retry < int(job.get("max_retry_count", retry_service.MAX_RETRIES) or retry_service.MAX_RETRIES):
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
    global _thread
    if _thread and _thread.is_alive():
        return
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
