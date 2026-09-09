import logging
import os
import threading
import time

from services import metrics_service, sms_task_service
from services.settings_service import get_setting
from services.smpp_gateway import gateway, SmppSubmitError

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")
_stop = threading.Event()
_guard = threading.RLock()
_workers = {}
_last_active = {}
_supervisor = None


def configured_worker_count():
    try:
        value = int(os.environ.get("MC_TASK_WORKERS", get_setting("task_workers", "1")))
    except (TypeError, ValueError):
        value = 1
    return max(1, min(value, 32))


def configured_claim_batch():
    try:
        value = int(os.environ.get("MC_CLAIM_BATCH", get_setting("task_claim_batch", "20")))
    except (TypeError, ValueError):
        value = 20
    return max(1, min(value, 500))


def worker_status():
    with _guard:
        items = [{"index": index + 1, "name": thread.name, "alive": thread.is_alive(),
                  "last_active": _last_active.get(index, 0)}
                 for index, thread in sorted(_workers.items())]
    return {"configured": configured_worker_count(),
            "running": sum(1 for item in items if item["alive"]), "workers": items}


def _run(worker_index):
    reconnect_after, reconnect_delay, ticks = 0.0, 1.0, 0
    last_connect_check, gateway_was_connected = 0.0, gateway.connected
    try:
        if worker_index == 0:
            sms_task_service.recover_stale_tasks()
        while not _stop.wait(1):
            _last_active[worker_index] = time.time()
            ticks += 1
            if worker_index == 0 and ticks % 60 == 0:
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
            tasks = sms_task_service.claim_next_tasks(configured_claim_batch())
            if not tasks:
                if (worker_index == 0 and get_setting("system_mode", "production") == "production"
                        and not gateway.connected and time.monotonic() >= reconnect_after):
                    try:
                        gateway.connect(); reconnect_delay = 1.0
                    except Exception as exc:
                        logger.warning("SMPP reconnect failed: %s", exc)
                        reconnect_after = time.monotonic() + reconnect_delay
                        reconnect_delay = min(60.0, reconnect_delay * 2)
                continue
            for task in tasks:
                try:
                    metrics_service.inc("marketing_copilot_tasks_claimed_total", labels={"worker": worker_index + 1})
                    active, reason = sms_task_service.validate_task_schedule(task)
                    if not active:
                        expired = reason in {"campaign has ended", "campaign no longer exists", "campaign is not enabled", "task belongs to a previous day", "after daily send window"}
                        sms_task_service.defer_task(task["id"], reason, expired)
                        continue
                    if get_setting("system_mode", "production") == "development":
                        message_id = f"DEV-{task['id']}"
                        detail_logger.info(
                            "[SMS STUB] dev mode simulated send task=%s campaign=%s/%s msisdn=%s content_len=%d content=%s",
                            task["id"], task.get("campaign_type"), task.get("campaign_id"),
                            task.get("msisdn"), len(task.get("content") or ""), (task.get("content") or "")[:1000],
                        )
                        sms_task_service.mark_submitted(
                            task["id"], [message_id], task.get("_delivery_receipt_mode", "optional"),
                            submit_sm_parts=1, wait=False,
                        )
                        sms_task_service.record_receipt(message_id, "DELIVRD", "development simulation", wait=False)
                    else:
                        message_ids = gateway.send(task)
                        sms_task_service.mark_submitted(
                            task["id"], message_ids,
                            task.get("_delivery_receipt_mode", "optional"),
                            transport_smpp_config_id=task.get("_transport_smpp_config_id"),
                            submit_sm_parts=task.get("_submit_sm_parts"),
                            wait=False,
                        )
                    metrics_service.inc("marketing_copilot_tasks_submitted_total")
                except SmppSubmitError as exc:
                    temporary = exc.status in {0x00000008, 0x00000014, 0x00000045, 0x00000058}
                    failure_class = "delivery_unknown" if exc.status == -1 else "submit_sm_resp"
                    sms_task_service.mark_send_failure(task, str(exc), exc.status, temporary, failure_class, wait=False)
                    metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": failure_class})
                except Exception as exc:
                    logger.exception("SMS task %s failed", task["id"])
                    sms_task_service.mark_send_failure(task, str(exc), wait=False)
                    metrics_service.inc("marketing_copilot_task_failures_total", labels={"class": "transport"})
    except Exception:
        logger.exception("SMS worker %s terminated unexpectedly", worker_index + 1)


def _spawn(index):
    thread = threading.Thread(target=_run, args=(index,), name=f"sms-task-worker-{index + 1}", daemon=True)
    _workers[index] = thread
    thread.start()


def _supervise():
    while not _stop.wait(2):
        with _guard:
            for index in range(configured_worker_count()):
                thread = _workers.get(index)
                if not thread or not thread.is_alive():
                    metrics_service.inc("marketing_copilot_worker_restarts_total", labels={"worker": index + 1})
                    _spawn(index)
        metrics_service.set_gauge("marketing_copilot_workers", worker_status()["running"])


def start_worker():
    global _supervisor
    with _guard:
        if any(thread.is_alive() for thread in _workers.values()):
            return
        _stop.clear(); _workers.clear(); _last_active.clear()
        for index in range(configured_worker_count()):
            _spawn(index)
        _supervisor = threading.Thread(target=_supervise, name="sms-worker-supervisor", daemon=True)
        _supervisor.start()
    metrics_service.set_gauge("marketing_copilot_workers", worker_status()["running"])


def stop_worker():
    global _supervisor
    _stop.set(); gateway.disconnect()
    with _guard:
        threads = list(_workers.values())
    for thread in threads:
        if thread is not threading.current_thread(): thread.join(timeout=3)
    if _supervisor and _supervisor is not threading.current_thread(): _supervisor.join(timeout=3)
    with _guard:
        _workers.clear(); _last_active.clear()
    _supervisor = None
    metrics_service.set_gauge("marketing_copilot_workers", 0)
