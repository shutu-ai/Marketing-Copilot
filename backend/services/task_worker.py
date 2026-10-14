import logging
import os
import queue
import threading
import time

from services import fast_send_service, metrics_service, sms_task_service
from services.settings_service import get_setting
from services.smpp_gateway import (
    gateway, SmppSubmitError, SmppDeliveryUnknownError, SmppPartialSubmitError,
    SmppSubmissionNotAllowedError,
)

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")
_stop = threading.Event()
_guard = threading.RLock()
_workers = {}
_last_active = {}
_supervisor = None
_claim_dispatcher = None
_task_queue = None


def _async_send_complete(task, message_ids, error):
    """Finalize a task after the background SMPP response handler completes."""
    try:
        if error is None:
            if fast_send_service.is_fast_task(task):
                if not fast_send_service.record_success(task, message_ids):
                    # The SMSC result is real, but a failed checkpoint cannot
                    # be presented as accepted.  Isolate it as unknown; the
                    # normal failure path is durable and never requeues this
                    # task automatically.
                    task["_fast_part_results"] = []
                    task["_accepted_parts"] = 0
                    sms_task_service.mark_send_failure(
                        task,
                        "Fast success checkpoint unavailable; submission is unconfirmed",
                        status_code=-1, retryable=False,
                        failure_class="delivery_unknown", wait=False,
                    )
                else:
                    metrics_service.inc("marketing_copilot_tasks_submitted_total")
                return
            sms_task_service.mark_submitted(
                task["id"], message_ids,
                task.get("_delivery_receipt_mode", "optional"),
                transport_smpp_config_id=task.get("_transport_smpp_config_id"),
                submit_sm_parts=task.get("_submit_sm_parts"),
                claim_token=task.get("claim_token", ""),
                attempt_token=task.get("_attempt_token", ""),
                source_namespace=task.get("_source_namespace", ""),
                wait=False,
            )
            metrics_service.inc("marketing_copilot_tasks_submitted_total")
            return

        if isinstance(error, SmppSubmitError):
            if isinstance(error, SmppSubmissionNotAllowedError):
                if fast_send_service.is_fast_task(task) and not task.get("_fast_socket_submitted"):
                    fast_send_service.abort_before_submit(task, str(error))
                    return
                # No submit_sm was written.  Re-check/finalize through the
                # claim fence rather than manufacturing a transport failure
                # or clearing evidence needed by a late callback.
                sms_task_service.defer_task(
                    task["id"], str(error), False, task.get("claim_token", "")
                )
                return
            temporary = error.status in {0x00000008, 0x00000014, 0x00000045, 0x00000058}
            failure_class = (
                "partial_submit" if isinstance(error, SmppPartialSubmitError)
                else "delivery_unknown" if isinstance(error, SmppDeliveryUnknownError)
                else "submit_sm_resp"
            )
            if failure_class in {"partial_submit", "delivery_unknown"}:
                temporary = False
            if isinstance(error, SmppPartialSubmitError):
                task["_accepted_parts"] = error.accepted_parts
            sms_task_service.mark_send_failure(
                task, str(error), error.status, temporary, failure_class, wait=False,
            )
            metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": failure_class})
            return

        sms_task_service.mark_send_failure(task, str(error), wait=False)
        metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": "transport"})
    except Exception:
        logger.exception("Failed to finalize async SMS task %s", task.get("id"))
    finally:
        if fast_send_service.is_fast_task(task):
            fast_send_service.forget_task(task)


def configured_worker_count():
    try:
        value = int(os.environ.get("MC_TASK_WORKERS", get_setting("task_workers", "8")))
    except (TypeError, ValueError):
        value = 8
    return max(1, min(value, 32))


def configured_claim_batch():
    try:
        value = int(os.environ.get("MC_CLAIM_BATCH", get_setting("task_claim_batch", "30")))
    except (TypeError, ValueError):
        value = 30
    return max(1, min(value, 500))


def worker_status():
    with _guard:
        items = [{"index": index + 1, "name": thread.name, "alive": thread.is_alive(),
                  "last_active": _last_active.get(index, 0)}
                 for index, thread in sorted(_workers.items())]
    return {"configured": configured_worker_count(),
            "running": sum(1 for item in items if item["alive"]), "workers": items}


def _claim_fetch_size() -> int:
    """Fetch enough work for the send workers in one DuckDB transaction.

    MC_CLAIM_BATCH remains the operator-facing per-worker batch setting. The
    single claim dispatcher multiplies it by the worker count so DuckDB sees
    one bounded claim transaction instead of N workers competing for the same
    queue head.
    """
    try:
        configured = int(os.environ.get("MC_FAIR_CLAIM_TOTAL", "0") or 0)
    except (TypeError, ValueError):
        configured = 0
    if configured > 0:
        return max(1, min(500, configured))
    # Keep enough work to feed senders, but do not mark hundreds/thousands of
    # tasks as `sending` before any socket has capacity for them.
    return max(configured_claim_batch(), min(200, configured_worker_count() * 2))


def _claim_dispatch_loop(task_queue):
    logger.info(
        "SMS task claim dispatcher started workers=%s fetch_batch=%s queue_capacity=%s",
        configured_worker_count(), _claim_fetch_size(),
        task_queue.maxsize,
    )
    while not _stop.is_set():
        from services import backpressure_service
        if not backpressure_service.wait_for_delivery(_stop):
            break
        metrics_service.set_gauge(
            "marketing_copilot_task_dispatch_queue_depth", task_queue.qsize()
        )
        # Keep a bounded amount of claimed work in memory. Tasks are already
        # marked as sending in DuckDB, so over-prefetching would increase the
        # recovery window without improving throughput.
        if task_queue.qsize() >= max(configured_claim_batch(), task_queue.maxsize // 2):
            _stop.wait(0.05)
            continue

        try:
            claim_started = time.monotonic()
            if fast_send_service.freeze_mode() == fast_send_service.FAST_MODE:
                tasks = sms_task_service.claim_next_fast_batch(_claim_fetch_size())
            else:
                tasks = sms_task_service.claim_next_tasks(_claim_fetch_size(), send_mode="reliable")
            metrics_service.observe_histogram(
                "marketing_copilot_task_claim_seconds",
                time.monotonic() - claim_started,
            )
        except Exception:
            logger.exception("SMS task claim dispatcher failed; backing off")
            _stop.wait(0.2)
            continue

        if not tasks:
            _stop.wait(0.1)
            continue

        for task in tasks:
            while not _stop.is_set():
                enqueue_started = time.monotonic()
                try:
                    task_queue.put(task, timeout=0.2)
                    metrics_service.inc(
                        "marketing_copilot_campaign_tasks_prefetched_total",
                        labels={
                            "campaign_type": task.get("campaign_type", ""),
                            "campaign_id": task.get("campaign_id", 0),
                        },
                    )
                    metrics_service.observe_histogram(
                        "marketing_copilot_task_dispatch_queue_enqueue_wait_seconds",
                        time.monotonic() - enqueue_started,
                    )
                    metrics_service.set_gauge(
                        "marketing_copilot_task_dispatch_queue_depth", task_queue.qsize(),
                    )
                    break
                except queue.Full:
                    metrics_service.observe_histogram(
                        "marketing_copilot_task_dispatch_queue_enqueue_wait_seconds",
                        time.monotonic() - enqueue_started,
                    )
                    metrics_service.inc(
                        "marketing_copilot_task_dispatch_queue_full_total",
                    )
                    continue


def _process_task(task: dict, worker_index: int):
    try:
        metrics_service.inc("marketing_copilot_tasks_claimed_total", labels={"worker": worker_index + 1})
        active, reason = sms_task_service.validate_task_schedule(task)
        if not active:
            expired = reason in {
                "campaign has ended", "campaign no longer exists",
                "task belongs to a previous day",
            }
            sms_task_service.defer_task(
                task["id"], reason, expired, task.get("claim_token", ""),
                delay_seconds=0 if not expired else 300,
            )
            return
        if get_setting("system_mode", "production") == "development":
            message_id = f"DEV-{task['id']}"
            detail_logger.info(
                "[SMS STUB] dev mode simulated send task=%s campaign=%s/%s name=%s msisdn=%s content_len=%d content=%s",
                task["id"], task.get("campaign_type"), task.get("campaign_id"),
                task.get("campaign_name", ""), task.get("msisdn"), len(task.get("content") or ""),
                (task.get("content") or "")[:1000],
            )
            sms_task_service.mark_submitted(
                task["id"], [message_id], task.get("_delivery_receipt_mode", "optional"),
                submit_sm_parts=1, claim_token=task.get("claim_token", ""), wait=False,
            )
            sms_task_service.record_receipt(message_id, "DELIVRD", "development simulation", wait=False)
            metrics_service.inc("marketing_copilot_tasks_submitted_total")
            return

        send_started = time.monotonic()
        gateway.send_async(task, _async_send_complete)
        metrics_service.observe_histogram(
            "marketing_copilot_task_send_async_call_seconds",
            time.monotonic() - send_started,
        )
    except SmppSubmitError as exc:
        temporary = exc.status in {0x00000008, 0x00000014, 0x00000045, 0x00000058}
        failure_class = (
            "partial_submit" if isinstance(exc, SmppPartialSubmitError)
            else "delivery_unknown" if isinstance(exc, SmppDeliveryUnknownError)
            else "submit_sm_resp"
        )
        if failure_class in {"partial_submit", "delivery_unknown"}:
            temporary = False
        sms_task_service.mark_send_failure(task, str(exc), exc.status, temporary, failure_class, wait=False)
        metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": failure_class})
    except Exception as exc:
        logger.exception("SMS task %s failed", task["id"])
        sms_task_service.mark_send_failure(task, str(exc), wait=False)
        metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": "transport"})


def _run(worker_index):
    next_recovery = time.monotonic() + 60.0
    last_connect_check, gateway_was_connected = 0.0, gateway.connected
    try:
        if worker_index == 0:
            sms_task_service.recover_stale_tasks()
        while not _stop.is_set():
            _last_active[worker_index] = time.time()
            if worker_index == 0 and time.monotonic() >= next_recovery:
                next_recovery = time.monotonic() + 60.0
                sms_task_service.recover_stale_tasks()
            if (worker_index == 0 and get_setting("system_mode", "production") == "production"
                    and time.monotonic() >= last_connect_check):
                last_connect_check = time.monotonic() + 5.0
                try:
                    gateway.connect()
                    reconnect_delay = 1.0
                except Exception as exc:
                    logger.warning("SMPP reconnect failed: %s", exc)
                gateway_connected = gateway.connected
                if gateway_connected and not gateway_was_connected:
                    recovered = sms_task_service.requeue_transport_failures()
                    if recovered:
                        logger.warning("Requeued %s transport-failed SMS tasks after SMPP recovery", recovered)
                gateway_was_connected = gateway_connected

            try:
                task = _task_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                _process_task(task, worker_index)
            finally:
                _task_queue.task_done()
    except Exception:
        logger.exception("SMS worker %s terminated unexpectedly", worker_index + 1)


def _spawn(index):
    thread = threading.Thread(target=_run, args=(index,), name=f"sms-task-worker-{index + 1}", daemon=True)
    _workers[index] = thread
    thread.start()


def _supervise():
    while not _stop.wait(2):
        with _guard:
            if not _claim_dispatcher or not _claim_dispatcher.is_alive():
                _start_claim_dispatcher()
            for index in range(configured_worker_count()):
                thread = _workers.get(index)
                if not thread or not thread.is_alive():
                    metrics_service.inc("marketing_copilot_worker_restarts_total", labels={"worker": index + 1})
                    _spawn(index)
        metrics_service.set_gauge("marketing_copilot_workers", worker_status()["running"])


def _start_claim_dispatcher():
    global _claim_dispatcher
    task_queue = _task_queue
    if task_queue is None or (_claim_dispatcher and _claim_dispatcher.is_alive()):
        return
    _claim_dispatcher = threading.Thread(
        target=_claim_dispatch_loop, args=(task_queue,),
        name="sms-task-claim-dispatcher", daemon=True,
    )
    _claim_dispatcher.start()


def start_worker():
    global _supervisor, _claim_dispatcher, _task_queue
    with _guard:
        if any(thread.is_alive() for thread in _workers.values()):
            return
        mode = fast_send_service.freeze_mode()
        from services import realtime_campaign_service
        realtime_campaign_service.finalize_daily_tasks()
        if mode == fast_send_service.FAST_MODE:
            fast_send_service.start()
        _stop.clear(); _workers.clear(); _last_active.clear()
        capacity = max(100, min(1000, configured_worker_count() * 4))
        _task_queue = queue.Queue(maxsize=capacity)
        _claim_dispatcher = None
        _start_claim_dispatcher()
        for index in range(configured_worker_count()):
            _spawn(index)
        _supervisor = threading.Thread(target=_supervise, name="sms-worker-supervisor", daemon=True)
        _supervisor.start()
    metrics_service.set_gauge("marketing_copilot_workers", worker_status()["running"])


def stop_worker(timeout: float = 8.0):
    global _supervisor, _claim_dispatcher, _task_queue
    _stop.set(); gateway.disconnect()
    with _guard:
        threads = list(_workers.values())
    deadline = time.monotonic() + max(0.5, float(timeout))
    for thread in threads:
        if thread is threading.current_thread():
            continue
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            break
        thread.join(timeout=remaining)
    if _supervisor and _supervisor is not threading.current_thread():
        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0:
            _supervisor.join(timeout=remaining)
    if _claim_dispatcher and _claim_dispatcher is not threading.current_thread():
        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0:
            _claim_dispatcher.join(timeout=remaining)
    with _guard:
        _workers.clear(); _last_active.clear()
    if fast_send_service.mode() == fast_send_service.FAST_MODE:
        try:
            fast_send_service.stop(timeout=max(1.0, float(timeout)))
        except Exception:
            logger.exception("Fast Send checkpoint shutdown failed")
    _supervisor = None; _claim_dispatcher = None; _task_queue = None
    metrics_service.set_gauge("marketing_copilot_workers", 0)
