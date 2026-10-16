"""Opt-in Fast Send coordination and success checkpointing.

Fast Send deliberately keeps the durable ownership boundary in DuckDB, but
removes normal response persistence from the SMPP reader/sender threads.  A
bounded in-memory accumulator emits one durable ``fast_checkpoint`` event for
many successful tasks.  Failures and ambiguous outcomes continue through the
existing durable result path.

This module is intentionally small and process-local.  The application
instance lock is the cross-process safety boundary.  Fast mode refuses to
start when ``MC_ALLOW_MULTI_INSTANCE=1`` is set because a local campaign
barrier cannot prove a global stop fence in that configuration.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

from services import metrics_service

logger = logging.getLogger(__name__)

RELIABLE_MODE = "reliable"
FAST_MODE = "fast"
VALID_MODES = frozenset({RELIABLE_MODE, FAST_MODE})


class FastSendAdmissionDenied(RuntimeError):
    """A Fast task reached a closed or unsafe socket admission fence."""

    pre_submit = True


_mode_lock = threading.RLock()
_frozen_mode: Optional[str] = None


def _configured_mode() -> str:
    value = str(os.environ.get("MC_SMS_SEND_MODE", RELIABLE_MODE) or "").strip().lower()
    if value not in VALID_MODES:
        raise ValueError(
            f"MC_SMS_SEND_MODE must be reliable or fast, got {value!r}"
        )
    return value


def freeze_mode() -> str:
    """Freeze the send mode for this process for the lifetime of the worker."""
    global _frozen_mode
    with _mode_lock:
        configured = _configured_mode()
        if _frozen_mode is None:
            if configured == FAST_MODE and os.environ.get("MC_ALLOW_MULTI_INSTANCE") == "1":
                raise RuntimeError(
                    "Fast Send requires the single-process instance lock; "
                    "unset MC_ALLOW_MULTI_INSTANCE before starting Fast mode"
                )
            _frozen_mode = configured
            metrics_service.set_gauge(
                "marketing_copilot_sms_send_mode_fast", 1 if configured == FAST_MODE else 0
            )
            logger.warning(
                "SMS send mode frozen mode=%s; Fast mode is experimental and does not "
                "automatically recover uncertain submissions",
                configured,
            )
        elif configured != _frozen_mode:
            logger.warning(
                "Ignoring runtime MC_SMS_SEND_MODE change from %s to %s; "
                "send mode is frozen until restart",
                _frozen_mode, configured,
            )
        return _frozen_mode


def mode() -> str:
    with _mode_lock:
        return _frozen_mode or _configured_mode()


def is_fast() -> bool:
    return mode() == FAST_MODE


def reset_for_tests() -> None:
    """Reset process state for isolated unit tests."""
    global _frozen_mode
    stop(timeout=2.0)
    with _mode_lock:
        _frozen_mode = None
    with _fences_lock:
        _fences.clear()
    with _task_state_lock:
        _task_states.clear()


@dataclass
class _CampaignFence:
    lock: threading.RLock
    generation: int = -1
    status: str = ""
    control_requested: bool = False


_fences_lock = threading.RLock()
_fences: dict[tuple[str, int], _CampaignFence] = {}

_task_state_lock = threading.RLock()
_task_states: dict[tuple[int, str], dict[str, bool]] = {}


def _task_key(task_id: int, claim_token: str = "", batch_id: str = ""):
    return int(task_id), str(claim_token or batch_id or "")


def _register_claimed_task(task: dict) -> None:
    key = _task_key(
        task.get("id", 0), task.get("claim_token", ""),
        task.get("_fast_batch_id", ""),
    )
    with _task_state_lock:
        _task_states[key] = {"socket_admitted": False}


def mark_socket_admitted(task: dict) -> None:
    """Record the conservative point after which a claim must not be requeued."""
    task["_fast_socket_admitted"] = True
    key = _task_key(
        task.get("id", 0), task.get("claim_token", ""),
        task.get("_fast_batch_id", ""),
    )
    with _task_state_lock:
        state = _task_states.get(key)
        if state is not None:
            state["socket_admitted"] = True


def safe_pre_submit_claims(rows) -> set[tuple[int, str]]:
    """Return only claims this process can prove never entered socket I/O.

    Missing state is deliberately unsafe: after an in-process hand-off or a
    restart there is no evidence strong enough to justify automatic resend.
    """
    safe = set()
    with _task_state_lock:
        for row in rows or []:
            task_id = int(row["id"])
            claim_token = str(row["claim_token"] or "")
            state = _task_states.get(_task_key(task_id, claim_token))
            if state is not None and not state.get("socket_admitted"):
                safe.add((task_id, claim_token))
    return safe


def forget_task(task: dict) -> None:
    key = _task_key(
        task.get("id", 0), task.get("claim_token", ""),
        task.get("_fast_batch_id", ""),
    )
    with _task_state_lock:
        _task_states.pop(key, None)


def _get_fence(campaign_type: str, campaign_id: int) -> _CampaignFence:
    key = (str(campaign_type or ""), int(campaign_id or 0))
    with _fences_lock:
        fence = _fences.get(key)
        if fence is None:
            fence = _CampaignFence(threading.RLock())
            _fences[key] = fence
        return fence


def observe_claimed_task(task: dict) -> None:
    """Observe durable ownership after commit, outside the database lock."""
    if task.get("_fast_mode"):
        _register_claimed_task(task)
    fence = _get_fence(task.get("campaign_type", ""), task.get("campaign_id", 0))
    with fence.lock:
        generation = int(task.get("dispatch_generation") or 0)
        if fence.generation < 0:
            fence.generation = generation
            fence.status = str(task.get("_fast_campaign_status") or "in_progress")
        elif generation > fence.generation:
            fence.generation = generation
            fence.status = str(task.get("_fast_campaign_status") or "in_progress")


@contextmanager
def campaign_control_barrier(campaign_type: str, campaign_id: int) -> Iterator[None]:
    """Serialize a campaign state transition with the short socket gate.

    The caller holds this context across the short durable campaign-state
    transaction and calls :func:`publish_campaign_state` before releasing it.
    No Fast sender can enter a new socket admission after the barrier request
    is raised.  The lock is never held while waiting for an SMSC response.
    """
    fence = _get_fence(campaign_type, campaign_id)
    fence.control_requested = True
    fence.lock.acquire()
    try:
        yield
    finally:
        fence.control_requested = False
        fence.lock.release()


def publish_campaign_state(campaign_type: str, campaign_id: int,
                           status: str, dispatch_generation: int) -> None:
    fence = _get_fence(campaign_type, campaign_id)
    with fence.lock:
        fence.status = str(status or "")
        fence.generation = max(fence.generation, int(dispatch_generation or 0))


@contextmanager
def socket_admission(task: dict) -> Iterator[None]:
    """Hold the per-campaign linearization gate only around socket I/O."""
    if task.get("_fast_mode") and backpressure_active():
        raise FastSendAdmissionDenied(
            "Fast Send checkpoint backlog is above its safety threshold"
        )
    fence = _get_fence(task.get("campaign_type", ""), task.get("campaign_id", 0))
    with fence.lock:
        task_generation = int(task.get("dispatch_generation") or 0)
        if fence.control_requested:
            raise FastSendAdmissionDenied("campaign control barrier is closing")
        if fence.generation >= 0 and task_generation < fence.generation:
            raise FastSendAdmissionDenied("campaign dispatch generation is stale")
        if fence.status and fence.status != "in_progress":
            raise FastSendAdmissionDenied(
                f"campaign is {fence.status}; socket admission is closed"
            )
        deadline = task.get("_fast_window_deadline_epoch")
        if deadline is not None and time.time() >= float(deadline):
            raise FastSendAdmissionDenied("campaign send window is closed")
        schedule = task.get("_socket_schedule")
        if schedule:
            from services import sms_task_service
            from datetime import datetime
            now = datetime.now().astimezone()
            active, reason = sms_task_service.campaign_is_active(schedule, now)
            if (task.get("campaign_type") == "realtime" and task.get("task_date")
                    and str(task["task_date"])[:10] != now.date().isoformat()):
                active, reason = False, "realtime task date expired"
            if not active:
                raise FastSendAdmissionDenied(reason)
        if task.get("_fast_mode"):
            mark_socket_admitted(task)
        task["_socket_write_started"] = True
        yield


def is_fast_task(task: Optional[dict]) -> bool:
    return bool(task and task.get("_fast_mode") and task.get("_fast_batch_id"))


def prepare_task(task: dict, planned_parts: int, source_namespace: str) -> str:
    """Attach the already-durable Fast attempt identity without a DB write."""
    if not is_fast_task(task):
        raise FastSendAdmissionDenied("task has no durable Fast batch ownership")
    if backpressure_active():
        raise FastSendAdmissionDenied(
            "Fast Send checkpoint backlog is above its safety threshold"
        )
    attempt_token = str(task.get("_attempt_token") or "")
    if not attempt_token:
        raise FastSendAdmissionDenied("Fast task is missing its durable attempt token")
    task["_source_namespace"] = str(source_namespace or "legacy")
    task["_submit_sm_parts"] = max(1, int(planned_parts or 1))
    task["_fast_planned_parts"] = task["_submit_sm_parts"]
    task["_fast_claimed_for_socket"] = True
    return attempt_token


_checkpoint_lock = threading.RLock()
_checkpoint_condition = threading.Condition(_checkpoint_lock)
_checkpoint_pending: "OrderedDict[int, dict]" = OrderedDict()
_checkpoint_thread: Optional[threading.Thread] = None
_checkpoint_stop = threading.Event()
_checkpoint_failed = False
_checkpoint_error = ""
_last_checkpoint_at = 0.0
_last_checkpoint_event_at = 0.0
_unresolved_count = 0


def _int_env(name: str, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(os.environ.get(name, str(default)))))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float, low: float, high: float) -> float:
    try:
        return max(low, min(high, float(os.environ.get(name, str(default)))))
    except (TypeError, ValueError):
        return default


def _checkpoint_limit() -> int:
    return _int_env("MC_FAST_SEND_CHECKPOINT_QUEUE", 10000, 100, 100000)


def _checkpoint_batch_size() -> int:
    return _int_env("MC_FAST_SEND_CHECKPOINT_BATCH", 250, 1, 2000)


def _fair_checkpoint_interval() -> float:
    """Return the bounded fair-mode coalescing interval.

    Fair Result capacity deliberately keeps Fast success checkpoints durable
    and bounded.  The default remains the historical 300 ms interval; the
    operator may increase only the coalescing delay, never remove the queue or
    capacity limits.
    """
    return _float_env(
        "MC_FAST_SEND_FAIR_CHECKPOINT_MS", 300.0, 300.0, 5000.0
    ) / 1000.0


def _checkpoint_interval() -> float:
    return _float_env("MC_FAST_SEND_CHECKPOINT_MS", 100.0, 10.0, 5000.0) / 1000.0


def _pending_durable_limit() -> int:
    return _int_env("MC_FAST_SEND_MAX_DURABLE_BACKLOG", 2000, 100, 100000)


def _checkpoint_wait_seconds() -> float:
    from services import result_capacity_service as capacity
    if capacity.enabled():
        return max(_checkpoint_interval(), _fair_checkpoint_interval())
    return _checkpoint_interval()


def start() -> None:
    """Start Fast checkpointing and reconcile old claimed batches."""
    global _checkpoint_thread, _checkpoint_failed, _checkpoint_error
    if freeze_mode() != FAST_MODE:
        return
    with _checkpoint_lock:
        if _checkpoint_thread and _checkpoint_thread.is_alive():
            return
        _checkpoint_failed = False
        _checkpoint_error = ""
    # The result buffer is started by the application before task_worker.  A
    # synchronous drain here lets a durable checkpoint replay before startup
    # converts still-claimed batches to conservative unknown states.
    from services import sms_write_buffer, sms_task_service
    try:
        sms_write_buffer.flush(timeout=_float_env(
            "MC_FAST_SEND_STARTUP_REPLAY_TIMEOUT", 30.0, 1.0, 300.0
        ))
        sms_task_service.recover_fast_claims_on_startup()
    except Exception as exc:
        _set_checkpoint_failure(exc)
        raise RuntimeError(
            "Fast Send startup recovery could not establish an at-most-once boundary"
        ) from exc
    _checkpoint_stop.clear()
    thread = threading.Thread(
        target=_checkpoint_loop, name="sms-fast-success-checkpoint", daemon=True
    )
    with _checkpoint_lock:
        _checkpoint_thread = thread
    thread.start()
    logger.warning(
        "Fast Send checkpoint pipeline started batch=%s interval_ms=%s queue=%s",
        _checkpoint_batch_size(), round(_checkpoint_wait_seconds() * 1000),
        _checkpoint_limit(),
    )


def stop(timeout: float = 10.0) -> None:
    global _checkpoint_thread
    with _checkpoint_lock:
        thread = _checkpoint_thread
        _checkpoint_stop.set()
        _checkpoint_condition.notify_all()
    if thread and thread is not threading.current_thread():
        thread.join(timeout=max(0.1, float(timeout)))
    deadline = time.monotonic() + max(0.1, float(timeout))
    while time.monotonic() < deadline:
        if not _flush_once():
            break
    with _checkpoint_lock:
        if _checkpoint_thread is thread and (not thread or not thread.is_alive()):
            _checkpoint_thread = None


def _set_checkpoint_failure(error: BaseException) -> None:
    global _checkpoint_failed, _checkpoint_error
    with _checkpoint_lock:
        _checkpoint_failed = True
        _checkpoint_error = str(error)[:2000]
    metrics_service.inc("marketing_copilot_fast_checkpoint_failures_total")
    logger.error("Fast Send checkpoint pipeline entered fail-closed state: %s", error)


def _checkpoint_payload(task: dict, message_ids: list[str]) -> dict:
    ids = list(dict.fromkeys(str(value) for value in message_ids or [] if str(value)))
    results = list(task.get("_fast_part_results") or [])
    accepted_parts = sum(1 for result in results if result.get("ok"))
    planned_parts = max(
        1, int(task.get("_fast_planned_parts") or task.get("_submit_sm_parts") or len(results) or 1)
    )
    if not results and ids:
        accepted_parts = min(planned_parts, len(ids))
    return {
        "_created_at": time.time(),
        "task_id": int(task["id"]),
        "batch_id": str(task["_fast_batch_id"]),
        "lease_id": str(task.get("_fast_lease_id") or ""),
        "claim_token": str(task.get("claim_token") or ""),
        "attempt_token": str(task.get("_attempt_token") or ""),
        "source_namespace": str(task.get("_source_namespace") or "legacy"),
        "message_ids": ids,
        "delivery_receipt_mode": str(task.get("_delivery_receipt_mode") or "optional"),
        "transport_smpp_config_id": task.get("_transport_smpp_config_id"),
        "submit_sm_parts": planned_parts,
        "accepted_parts": accepted_parts,
        "state": "accepted",
    }


def record_success(task: dict, message_ids: list[str]) -> bool:
    """Aggregate a complete, all-parts-successful task in memory."""
    global _unresolved_count
    if not is_fast_task(task):
        return False
    payload = _checkpoint_payload(task, message_ids)
    from services import result_capacity_service as capacity
    task_id = int(payload["task_id"])
    pending_limit = _checkpoint_limit()
    if capacity.enabled():
        reservation = task.get("_dispatch_reservation")
        pool = getattr(reservation, "pool", None)
        limits = getattr(pool, "limits", None)
        result_credit = getattr(reservation, "result_credit", None)
        pending_limit = int(getattr(limits, "tasks", 0) or 0)
        if (reservation is None or result_credit is None
                or getattr(reservation, "state", "") in {"released", "failed"}
                or pending_limit < 1):
            _unresolved_count += 1
            _set_checkpoint_failure(
                RuntimeError("Fair Fast success is missing a live dispatch result reservation")
            )
            return False
    failure = None
    with _checkpoint_condition:
        if task_id not in _checkpoint_pending and len(_checkpoint_pending) >= pending_limit:
            _unresolved_count += 1
            failure = RuntimeError("Fast success checkpoint queue is full")
        else:
            _checkpoint_pending[task_id] = payload
            if len(_checkpoint_pending) >= _checkpoint_batch_size():
                _checkpoint_condition.notify_all()
    if failure:
        _set_checkpoint_failure(failure)
        return False
    metrics_service.inc("marketing_copilot_fast_success_aggregated_total")
    metrics_service.set_gauge(
        "marketing_copilot_fast_checkpoint_pending", len(_checkpoint_pending)
    )
    return True


def record_part_result(task: dict, part_no: int, sequence: int, ok: bool,
                       status: int, message_id: str) -> None:
    """Keep response facts in the task object until success/failure is known."""
    result = {
        "part_no": int(part_no), "sequence": int(sequence), "ok": bool(ok),
        "status": int(status or 0), "message_id": str(message_id or ""),
    }
    task.setdefault("_fast_part_results", []).append(result)
    if not ok:
        task["_fast_failure_seen"] = True
    task["_fast_accepted_parts"] = sum(
        1 for item in task.get("_fast_part_results", []) if item.get("ok")
    )


def backpressure_active() -> bool:
    with _checkpoint_lock:
        if _checkpoint_failed or len(_checkpoint_pending) >= _checkpoint_limit():
            return True
    try:
        from services import sms_write_buffer
        buffered = sms_write_buffer.status()
        if int(buffered.get("pending_durable") or 0) >= _pending_durable_limit():
            return True
        if int((buffered.get("journal") or {}).get("queue_depth") or 0) >= _pending_durable_limit():
            return True
    except Exception:
        # A status read failure is not proof that the durable pipeline is
        # healthy.  Fast mode fails closed until the next status check.
        return True
    return False


def _flush_once() -> bool:
    global _last_checkpoint_at, _last_checkpoint_event_at
    with _checkpoint_lock:
        if not _checkpoint_pending:
            return False
        values = list(_checkpoint_pending.values())[:_checkpoint_batch_size()]
    try:
        from services import sms_write_buffer
        event_id = sms_write_buffer.submit(
            "fast_checkpoint", {"items": values}, wait=False,
        )
    except Exception as exc:
        _set_checkpoint_failure(exc)
        return False
    with _checkpoint_condition:
        for value in values:
            task_id = int(value["task_id"])
            current = _checkpoint_pending.get(task_id)
            if current is value:
                _checkpoint_pending.pop(task_id, None)
        _last_checkpoint_event_at = time.time()
        _last_checkpoint_at = _last_checkpoint_event_at
        _checkpoint_condition.notify_all()
    metrics_service.inc(
        "marketing_copilot_fast_checkpoints_enqueued_total", labels={"items": len(values)}
    )
    metrics_service.set_gauge(
        "marketing_copilot_fast_checkpoint_pending", len(_checkpoint_pending)
    )
    return bool(event_id)


def _checkpoint_loop() -> None:
    while not _checkpoint_stop.is_set():
        with _checkpoint_condition:
            if not _checkpoint_pending:
                _checkpoint_condition.wait(timeout=_checkpoint_wait_seconds())
            else:
                _checkpoint_condition.wait(timeout=_checkpoint_wait_seconds())
            ready = bool(_checkpoint_pending)
        if ready:
            _flush_once()
    while _flush_once():
        pass


def abort_before_submit(task: dict, reason: str) -> None:
    """Return a Fast task to a safe queue/terminal state without socket I/O."""
    from services import sms_task_service
    sms_task_service.abort_fast_task_before_submit(task, reason)


def status() -> dict:
    with _checkpoint_lock:
        pending = len(_checkpoint_pending)
        oldest = next(iter(_checkpoint_pending.values()), None)
        failed = _checkpoint_failed
        error = _checkpoint_error
        last = _last_checkpoint_at
        unresolved = _unresolved_count
        thread = _checkpoint_thread
    oldest_at = float(oldest.get("_created_at") or 0.0) if oldest else 0.0
    return {
        "mode": mode(),
        "frozen": _frozen_mode is not None,
        "running": bool(thread and thread.is_alive()),
        "checkpoint_pending": pending,
        "checkpoint_queue_capacity": _checkpoint_limit(),
        "checkpoint_batch_size": _checkpoint_batch_size(),
        "checkpoint_interval_ms": round(_checkpoint_wait_seconds() * 1000),
        "checkpoint_lag_seconds": max(0.0, time.time() - oldest_at) if oldest_at else 0.0,
        "last_checkpoint_at": last,
        "unresolved_count": unresolved,
        "backpressure": backpressure_active() if is_fast() else False,
        "fail_closed": failed,
        "last_error": error,
        "uncertain_after_claim": {
            "policy": "unknown_no_automatic_resubmit",
            "max_in_memory_success_tasks": _checkpoint_limit(),
        },
    }

