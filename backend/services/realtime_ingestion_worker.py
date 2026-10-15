"""Concurrent, lease-based realtime file ingestion workers."""

import logging
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta

from database import read_transaction
from services import backpressure_service
from services import ingestion_job_service, operations_service, realtime_campaign_service, realtime_whitelist_service
from services import realtime_ingestion_state
from services import realtime_ingestion_retry_service as retry_service

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None
_threads = []
_maintenance_thread = None
_claim_lock = threading.Lock()
_maintenance_lock = threading.Lock()
_campaign_cursor = 0
_last_expiry_sweep_date = None
_last_inactive_campaign_sweep_at = 0.0
_INACTIVE_CAMPAIGN_SWEEP_INTERVAL_SECONDS = 5.0
_last_lease_recovery_at = 0.0


def _configured_worker_count() -> int:
    try:
        return max(1, min(16, int(os.environ.get("MC_REALTIME_INGEST_WORKERS", "4"))))
    except (TypeError, ValueError):
        return 4


def _processed_id(campaign_id: int, filename: str):
    try:
        row = read_transaction(
            lambda conn: conn.execute(
                "SELECT id FROM processed_files WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
                (campaign_id, filename),
            ).fetchone(),
            operation="realtime.ingestion.processed_file_lookup",
        )
        return row["id"] if row else None
    except Exception as exc:
        # This is a display-projection lookup only.  A missing/corrupt mirror
        # must never turn a valid ingestion lease into a retry or quarantine.
        logger.warning(
            "Realtime processed-file lookup deferred campaign=%s file=%s error=%s",
            campaign_id, filename, exc,
        )
        return None


def _discard_expired_queued_files(include_processing: bool = False):
    """Expire old-date jobs before the worker selects the next queue item."""
    active_statuses = "('pending','retrying','processing')" if include_processing else "('pending','retrying')"
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT f.id, f.campaign_id, f.filename, f.file_path,
                      c.campaign_dir, c.watch_filename_prefix
               FROM ingestion_files f
               JOIN realtime_campaigns c ON c.id=f.campaign_id
               WHERE f.campaign_type='realtime' AND f.status IN """ + active_statuses
        ).fetchall(),
        operation="realtime.ingestion.expired_file_scan",
    )

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
    for job in discarded_jobs:
        realtime_ingestion_state.project_processed_file(
            job["id"], path="", error=reason,
        )

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


def _discard_inactive_campaign_files() -> None:
    """Batch-cancel queued files whose campaign was disabled or removed."""
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT f.id,f.campaign_id,f.filename,f.file_path,
                      c.id AS matched_campaign_id
               FROM ingestion_files f
               LEFT JOIN realtime_campaigns c ON c.id=f.campaign_id
               WHERE f.campaign_type='realtime'
                 AND f.status IN ('pending','retrying')
                 AND (c.id IS NULL OR c.status <> 'in_progress')"""
        ).fetchall(),
        operation="realtime.ingestion.inactive_file_scan",
    )
    if not rows:
        return

    groups = {}
    for row in rows:
        job = dict(row)
        reason = "campaign not found" if job.get("matched_campaign_id") is None else "campaign is not enabled"
        groups.setdefault(reason, []).append(job)

    discarded_jobs = []
    for reason, jobs in groups.items():
        expired_ids = ingestion_job_service.expire_files(
            (job["id"] for job in jobs), reason,
        )
        discarded_jobs.extend(job for job in jobs if job["id"] in expired_ids)
    if not discarded_jobs:
        return

    for job in discarded_jobs:
        realtime_ingestion_state.project_processed_file(
            job["id"], path="",
            error=("campaign not found" if job.get("matched_campaign_id") is None
                   else "campaign is not enabled"),
        )

    for job in discarded_jobs:
        try:
            path = job.get("file_path", "")
            if path and os.path.isfile(path):
                os.remove(path)
        except OSError as exc:
            logger.warning(
                "Realtime inactive campaign file deletion failed campaign=%s file=%s error=%s",
                job["campaign_id"], job["filename"], exc,
            )
    logger.info(
        "Realtime inactive campaign queued files discarded count=%s",
        len(discarded_jobs),
    )


def _discard_claimed_expired_file(job: dict, campaign: dict, reason: str, worker_id: str):
    processed_id = _processed_id(campaign["id"], job["filename"])
    if job.get("lease_token"):
        realtime_ingestion_state.transition(
            job["id"], processed_id, job["lease_token"], "failed", error_class="EXPIRED_FILE", error=reason,
        )
    else:
        ingestion_job_service.expire_file(job["id"], reason, include_processing=True)
        if processed_id:
            realtime_ingestion_state.project_processed_file(
                job["id"], path="", error=reason,
            )
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
        campaign["id"], job["filename"], reason, worker_id,
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
    campaign_dirs = read_transaction(
        lambda conn: [
            row["campaign_dir"] for row in conn.execute(
                "SELECT campaign_dir FROM realtime_campaigns WHERE COALESCE(campaign_dir,'')<>''"
            ).fetchall()
        ],
        operation="realtime.ingestion.scratch_campaign_dirs",
    )
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


def _mark_retry(job: dict, processed_id, decision, error, retry_count, worker_id: str):
    delay = retry_service.delay_for(decision, retry_count)
    next_retry = (datetime.now() + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
    realtime_ingestion_state.transition(
        job["id"], processed_id, job.get("lease_token", ""), "retrying",
        error_class=decision.error_class, error=error, delay=delay,
    )
    logger.warning(
        "Realtime file retrying campaign=%s file=%s error_class=%s retry_count=%s next_retry_at=%s error=%s",
        job["campaign_id"], job["filename"], decision.error_class, retry_count + 1, next_retry, str(error)[:500],
    )


def _quarantine(job: dict, processed_id, decision, error, campaign_dir: str, worker_id: str, target_path=None):
    realtime_ingestion_state.check_owned(job["id"], job.get("lease_token", ""))
    path = job.get("file_path", "")
    target = target_path or path
    if path and decision.quarantine:
        try:
            target = operations_service.quarantine(path, campaign_dir, job["filename"])
        except OSError:
            pass
    realtime_ingestion_state.transition(
        job["id"], processed_id, job.get("lease_token", ""), "quarantined" if decision.quarantine else "failed",
        error_class=decision.error_class, error=error, path=target,
    )
    operations_service.event(
        "realtime_ingestion", decision.error_class, str(error),
        severity="error", context={
            "campaign_id": job["campaign_id"],
            "filename": job["filename"],
            "worker_id": worker_id,
        },
    )
    logger.error(
        "Realtime file failed campaign=%s file=%s phase=%s error_class=%s error=%s",
        job["campaign_id"], job["filename"], job.get("processing_phase", ""), decision.error_class, str(error)[:1000],
    )


def _claim_realtime_file(worker_id: str):
    """Claim one file using campaign-level round-robin scheduling.

    The worker pool remains shared, but a campaign with a large backlog cannot
    monopolize the global filename-ordered queue.  The claim transaction is
    still serialized locally to avoid DuckDB workers racing on the same queue.
    """
    global _campaign_cursor
    with _claim_lock:
        campaign_ids = ingestion_job_service.pending_campaign_ids("realtime")
        if not campaign_ids:
            return None
        start = _campaign_cursor % len(campaign_ids)
        for offset in range(len(campaign_ids)):
            position = (start + offset) % len(campaign_ids)
            campaign_id = campaign_ids[position]
            job = ingestion_job_service.claim_file(
                "realtime", worker_id, campaign_id=campaign_id,
            )
            if job:
                _campaign_cursor = (position + 1) % len(campaign_ids)
                return job
        # Another local caller may have claimed the candidates between the
        # discovery query and the conditional UPDATE.  Advance the cursor so
        # the next pass does not repeatedly retry the same campaign.
        _campaign_cursor = (start + 1) % len(campaign_ids)
        return None


def process_once(worker_id: str) -> bool:
    # DuckDB claim is a short UPDATE transaction. Serialize only this local
    # claim step so multiple ingestion workers do not race on the same queue
    # head; file parsing and whitelist matching remain concurrent.
    job = _claim_realtime_file(worker_id)
    if not job:
        return False
    with realtime_ingestion_state.FileLease(job["id"], job.get("lease_token", "")):
        try:
            return _process_claimed_file(job, worker_id)
        except realtime_ingestion_state.LeaseLost:
            logger.warning("Realtime file ownership lost file_id=%s worker_id=%s", job["id"], worker_id)
            return True
        except Exception:
            # If even the terminal/retry transaction failed, retain the input.
            # Stop renewing this lease; runtime maintenance will recover it.
            logger.exception("Realtime file recovery deferred until lease expiry file_id=%s", job["id"])
            return True


def _process_claimed_file(job, worker_id):
    campaign_id = job["campaign_id"]
    filename = job["filename"]
    campaign_dir = ""
    processed_id = None
    started = time.monotonic()
    try:
        campaign_row = read_transaction(
            lambda conn: conn.execute(
                "SELECT * FROM realtime_campaigns WHERE id=?", (job["campaign_id"],)
            ).fetchone(),
            operation="realtime.ingestion.campaign_lookup",
        )
        if not campaign_row:
            raise ValueError("campaign not found")
        campaign = dict(campaign_row)
        campaign_dir = campaign.get("campaign_dir", "")
        file_time = realtime_campaign_service._watch_file_timestamp(
            job["filename"], campaign.get("watch_filename_prefix") or "Android_4G_Switch_Somali_",
        )
        expired_reason = realtime_campaign_service._realtime_ingestion_expired_reason(
            campaign, filename=job["filename"],
        )
        if file_time and file_time.date() != datetime.now().date():
            expired_reason = "file timestamp is outside the current day"
        if expired_reason:
            _discard_claimed_expired_file(job, campaign, expired_reason, worker_id)
            return True
        result = realtime_campaign_service.receive_file(
            campaign["id"], job["file_path"], job["filename"],
            ingestion_file_id=job["id"],
            whitelist_version_id=job.get("whitelist_version_id"), worker_id=worker_id,
            lease_token=job.get("lease_token", ""),
            stop_event=_stop,
        )
        processed_id = _processed_id(campaign["id"], job["filename"])
        if result.get("status") == "expired":
            _discard_claimed_expired_file(
                job, campaign, result.get("message", "daily send window ended"), worker_id,
            )
            return True
        if result.get("status") == "stopped":
            # Capacity waiting is a recoverable pause, not a file failure.
            # Leave the processing lease for normal expiry recovery on
            # shutdown; importantly, do not write a retry state transition.
            return True
        if result.get("success"):
            # receive_file commits both final states. Duplicate-file detection
            # has no chunk work, so it only needs the fenced file transition.
            if result.get("duplicate_file"):
                # Do not zero the original completed file's row/task totals.
                realtime_ingestion_state.transition(job["id"], None, job.get("lease_token", ""), "completed")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            from services import metrics_service
            metrics_service.observe_histogram("realtime_file_processing_seconds", elapsed_ms / 1000.0,
                                              {"campaign_id": campaign["id"], "result": "success"})
            logger.info(
                "Realtime file completed campaign=%s file=%s elapsed_ms=%s tasks_created=%s whitelist_version=%s worker_id=%s",
                campaign["id"], job["filename"], elapsed_ms, result.get("task_result", {}).get("created", 0),
                job.get("whitelist_version_id") or "", worker_id,
            )
            return True
        decision = retry_service.classify(result.get("message", ""), result.get("status", ""))
        from services import metrics_service
        metrics_service.observe_histogram("realtime_file_processing_seconds", (time.monotonic() - started),
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
            _mark_retry(job, processed_id, decision, result.get("message", "unknown"), current_retry, worker_id)
        else:
            _quarantine(job, processed_id, decision, result.get("message", "unknown"), campaign_dir, worker_id,
                        result.get("quarantine_path"))
        return True
    except realtime_ingestion_state.LeaseLost:
        raise
    except Exception as exc:
        from services import metrics_service
        metrics_service.observe_histogram("realtime_file_processing_seconds", (time.monotonic() - started),
                                          {"campaign_id": campaign_id, "result": "failed"})
        metrics_service.inc("realtime_file_processing_errors_total",
                            labels={"campaign_id": campaign_id, "error_class": retry_service.classify(exc).error_class})
        processed_id = processed_id or _processed_id(campaign_id, filename)
        decision = retry_service.classify(exc)
        current_retry = int(job.get("retry_count", 0) or 0)
        if decision.retryable and (
            decision.error_class in {"WAITING_DEPENDENCY", "RESOURCE_LIMIT"}
            or current_retry < int(job.get("max_retry_count", retry_service.MAX_RETRIES) or retry_service.MAX_RETRIES)
        ):
            _mark_retry(job, processed_id, decision, exc, current_retry, worker_id)
        else:
            _quarantine(job, processed_id, decision, exc, campaign_dir, worker_id)
        return True


def _run_maintenance():
    """Run shared housekeeping once, without racing other ingestion workers."""
    global _last_expiry_sweep_date, _last_inactive_campaign_sweep_at, _last_lease_recovery_at
    if not _maintenance_lock.acquire(blocking=False):
        return
    try:
        today = datetime.now().date()
        if _last_expiry_sweep_date != today:
            _discard_expired_queued_files()
            _last_expiry_sweep_date = today
        now_monotonic = time.monotonic()
        if now_monotonic - _last_lease_recovery_at >= 30:
            recovered = realtime_ingestion_state.recover_expired_files()
            try:
                projection = realtime_ingestion_state.reconcile_processed_files(
                    repair=True, limit=50,
                )
                if projection.get("issues"):
                    logger.warning(
                        "Realtime processed-file projection repaired drift=%s",
                        len(projection["issues"]),
                    )
            except Exception:
                logger.warning(
                    "Realtime processed-file projection repair deferred",
                    exc_info=True,
                )
            _last_lease_recovery_at = now_monotonic
            if recovered:
                logger.warning("Realtime expired file leases recovered count=%s", recovered)
        if now_monotonic - _last_inactive_campaign_sweep_at >= _INACTIVE_CAMPAIGN_SWEEP_INTERVAL_SECONDS:
            _discard_inactive_campaign_files()
            _last_inactive_campaign_sweep_at = now_monotonic
        # Only one worker may maintain/build whitelist indexes at a time.
        realtime_whitelist_service.migrate_legacy_versions(limit=1)
        realtime_whitelist_service.repair_active_binding(limit=10)
        realtime_whitelist_service.repair_active_failed(limit=1)
        realtime_whitelist_service.build_pending(limit=1)
    finally:
        _maintenance_lock.release()


def _run(worker_index: int):
    worker_id = f"realtime-ingestion-{worker_index}"
    while not _stop.wait(0.5):
        try:
            from services import metrics_service
            metrics_service.set_gauge("realtime_ingestion_worker_last_progress_timestamp", time.time())
            # Do not claim a file while task/write/chunk/disk/result capacity
            # is paused.  This keeps pure capacity pressure out of the
            # claim -> receive_file -> retry DB write loop.
            if not backpressure_service.wait_for_ingestion(
                stop_event=_stop, producing_chunks=True,
            ):
                break
            if not process_once(worker_id):
                continue
            metrics_service.set_gauge("realtime_ingestion_worker_last_progress_timestamp", time.time())
        except Exception:
            logger.exception("Realtime ingestion worker iteration failed worker_id=%s", worker_id)


def _maintenance_run():
    """Keep whitelist builds and repair work off file-processing workers."""
    while not _stop.is_set():
        try:
            _run_maintenance()
        except Exception:
            logger.exception("Realtime ingestion maintenance iteration failed")
        if _stop.wait(1.0):
            break


def start_worker():
    global _thread, _threads, _maintenance_thread, _campaign_cursor
    global _last_expiry_sweep_date, _last_inactive_campaign_sweep_at
    if any(thread.is_alive() for thread in _threads):
        return
    try:
        realtime_ingestion_state.recover_expired_files()
        _discard_expired_queued_files(include_processing=True)
        _discard_inactive_campaign_files()
        realtime_campaign_service.cleanup_closed_window_ingestion_files(include_processing=True)
        _cleanup_orphaned_intermediate_files()
        _last_expiry_sweep_date = datetime.now().date()
        _last_inactive_campaign_sweep_at = time.monotonic()
        realtime_whitelist_service.repair_active_binding(limit=10)
        realtime_whitelist_service.repair_active_failed(limit=5)
    except Exception:
        logger.exception("Realtime expired-file startup cleanup failed")
    _stop.clear()
    _campaign_cursor = 0
    worker_count = _configured_worker_count()
    _threads = [
        threading.Thread(
            target=_run,
            args=(index,),
            name=f"realtime-ingestion-worker-{index}",
            daemon=True,
        )
        for index in range(1, worker_count + 1)
    ]
    for thread in _threads:
        thread.start()
    _maintenance_thread = threading.Thread(
        target=_maintenance_run,
        name="realtime-ingestion-maintenance",
        daemon=True,
    )
    _maintenance_thread.start()
    _thread = _threads[0] if _threads else None
    logger.info("Realtime ingestion worker started concurrency=%s", worker_count)


def stop_worker():
    global _threads, _maintenance_thread
    _stop.set()
    for thread in _threads:
        if thread.is_alive():
            thread.join(timeout=2)
    if _maintenance_thread and _maintenance_thread.is_alive():
        _maintenance_thread.join(timeout=2)
    _maintenance_thread = None
    _threads = []
    logger.info("Realtime ingestion worker stopped")


def worker_status() -> dict:
    running = [thread for thread in _threads if thread.is_alive()]
    return {
        "running": len(running),
        "configured": _configured_worker_count(),
        "worker_ids": [thread.name for thread in running],
        "maintenance_alive": bool(_maintenance_thread and _maintenance_thread.is_alive()),
    }
