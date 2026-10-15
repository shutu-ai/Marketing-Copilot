import logging
import os
import queue
import random
import threading
import time
import uuid
from typing import Callable

from database import read_transaction
from services import fast_send_service, metrics_service, sms_task_service

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")

# A configured SMSC TPS is a physical route limit, not a per-pool-connection
# limit.  Pool slots therefore share one process-wide reservation clock per
# configuration so campaign weights cannot multiply the same SMSC allowance.
_SHARED_TPS_LOCK = threading.Lock()
_SHARED_TPS_NEXT = {}
_SHARED_TPS_CONFIG = {}


def _internal_tps_multiplier() -> float:
    """Return the bounded local pacing multiplier.

    The configured SMPP TPS is the route ceiling.  A local multiplier may slow
    an isolated test, but it must never raise the process above the configured
    route allowance (which would make the value an undocumented quota bypass).
    """
    try:
        return max(0.01, min(1.0, float(
            os.environ.get("MC_SMPP_INTERNAL_TPS_MULTIPLIER", "1")
        )))
    except (TypeError, ValueError):
        return 1.0


def _encode_utf16be(text: str) -> bytes:
    """Encode text as UTF-16BE without relying on Python's codec registry."""
    return b"".join(_iter_utf16be_units(text))


def _iter_utf16be_units(text: str):
    """Yield complete UTF-16 code-unit sequences for each text character."""
    index = 0
    while index < len(text):
        character = text[index]
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDBFF:
            if index + 1 >= len(text) or not 0xDC00 <= ord(text[index + 1]) <= 0xDFFF:
                raise UnicodeEncodeError(
                    "utf-16-be", text, index, index + 1, "surrogates not allowed"
                )
            yield bytes((code_point >> 8, code_point & 0xFF,
                         ord(text[index + 1]) >> 8, ord(text[index + 1]) & 0xFF))
            index += 2
            continue
        if 0xDC00 <= code_point <= 0xDFFF:
            raise UnicodeEncodeError(
                "utf-16-be", text, index, index + 1, "surrogates not allowed"
            )
        if code_point <= 0xFFFF:
            yield bytes((code_point >> 8, code_point & 0xFF))
            index += 1
            continue

        code_point -= 0x10000
        high_surrogate = 0xD800 + (code_point >> 10)
        low_surrogate = 0xDC00 + (code_point & 0x3FF)
        yield bytes((high_surrogate >> 8, high_surrogate & 0xFF,
                     low_surrogate >> 8, low_surrogate & 0xFF))
        index += 1


def _make_ucs2_parts(text: str):
    """Build SMPP UCS-2 parts without using the utf-16-be codec."""
    import smpplib.consts
    import smpplib.gsm

    part_size = smpplib.consts.UCS2_PART_SIZE
    encoded_parts = []
    current_part = bytearray()
    for encoded_character in _iter_utf16be_units(text):
        if current_part and len(current_part) + len(encoded_character) > part_size:
            encoded_parts.append(bytes(current_part))
            current_part.clear()
        current_part.extend(encoded_character)
    if current_part or not encoded_parts:
        encoded_parts.append(bytes(current_part))

    if len(encoded_parts) > 255:
        raise smpplib.gsm.exceptions.MessageTooLong()

    if len(encoded_parts) == 1:
        return (encoded_parts, smpplib.consts.SMPP_ENCODING_ISO10646,
                smpplib.consts.SMPP_MSGTYPE_DEFAULT)

    reference = random.randint(0, 255)
    header = bytes((0x05, 0x00, 0x03, reference, len(encoded_parts)))
    parts = [header + bytes((part_index,)) + part
             for part_index, part in enumerate(encoded_parts, start=1)]
    return (parts, smpplib.consts.SMPP_ENCODING_ISO10646,
            smpplib.consts.SMPP_GSMFEAT_UDHI)


def _make_parts(text: str):
    """Use smpplib's normal splitter, with a narrow UCS-2 codec fallback."""
    import smpplib.gsm

    try:
        return smpplib.gsm.make_parts(text)
    except LookupError as exc:
        if "utf-16-be" not in str(exc).lower():
            raise
        logger.warning(
            "utf-16-be codec unavailable; using SMPP UCS-2 fallback"
        )
        return _make_ucs2_parts(text)


def _flash_data_coding(coding: int) -> int:
    """Return the GSM message-class-0 DCS for the detected text alphabet."""
    # The Ethio Telecom SMPP v3.4 profile documents 1111xxxx as the GSM
    # message-class control group.  0xF0 is Class 0 with the default alphabet;
    # 0x18 is the widely supported UCS-2 Class 0 form used for local scripts.
    if coding == 8:  # SMPP_ENCODING_ISO10646 / UCS-2
        return 0x18
    return 0xF0


def _make_task_parts(task: dict):
    if task.get("_prepared_smpp_parts") is not None:
        return task["_prepared_smpp_parts"]
    parts, coding, esm_class = _make_parts(task["content"])
    if task.get("flash_sms"):
        coding = _flash_data_coding(coding)
    return parts, coding, esm_class


class SmppSubmitError(RuntimeError):
    def __init__(self, status: int, message: str = ""):
        self.status = int(status or 0)
        super().__init__(message or f"SMPP submit failed with status 0x{self.status:08x}")


class SmppDeliveryUnknownError(SmppSubmitError):
    def __init__(self, message):
        super().__init__(-1, message)

    """Submission may have reached the SMSC; retrying on a backup could duplicate it."""


class SmppWindowExhaustedError(SmppSubmitError):
    """The local SMPP window is full; the task can be retried safely."""

    def __init__(self, message="SMPP submission window exhausted"):
        super().__init__(0x00000045, message)


class SmppSubmissionNotAllowedError(SmppSubmitError):
    """The DB/claim/window fence rejected a submission before socket I/O."""

    def __init__(self, message="SMPP submission is no longer authorized"):
        self.pre_submit = True
        super().__init__(-3, message)


class SmppPartialSubmitError(SmppSubmitError):
    """At least one part was accepted, so retrying the full SMS is unsafe."""

    def __init__(self, status: int, accepted_parts: int):
        self.accepted_parts = int(accepted_parts)
        super().__init__(
            -2,
            f"multipart submission partially accepted: accepted_parts={accepted_parts}, "
            f"failed_status=0x{int(status or 0):08x}",
        )


class _AsyncSubmission:
    def __init__(self, task: dict, task_id: int, sequences: list[int],
                 deadline: float, callback: Callable):
        self.task = task
        self.task_id = task_id
        self.sequences = sequences
        self.deadline = deadline
        self.callback = callback


class _SmppConnection:
    def __init__(self, config: dict, slot: int):
        self.config = dict(config)
        self.slot = slot
        self.client = None
        self.connected = False
        self.state = "disconnected"
        self.last_error = ""
        self.last_connected_at = None
        self._condition = threading.Condition(threading.RLock())
        self._sequence_tasks = {}
        self._sequence_persist_message_ids = {}
        self._outcomes = {}
        self._early_outcomes = {}
        self._sequence_sent_at = {}
        self._sequence_context = {}
        self._async_submissions = {}
        self._listener = None
        self._async_reaper = None
        self._async_reaper_stop = threading.Event()
        self.window_size = max(1, min(100, int(
            self.config.get("window_size") or os.environ.get("MC_SMPP_WINDOW_SIZE", "10")
        )))
        configured_send_queue_capacity = os.environ.get("MC_SMPP_SEND_QUEUE_CAPACITY")
        if not configured_send_queue_capacity or not configured_send_queue_capacity.strip():
            configured_send_queue_capacity = str(max(100, self.window_size * 4))
        send_queue_capacity = max(
            1,
            min(
                5000,
                int(configured_send_queue_capacity),
            ),
        )
        self._send_queue = queue.Queue(maxsize=send_queue_capacity)
        self._send_queue_capacity = send_queue_capacity
        self._sender_thread = None
        self._sender_stop = threading.Event()
        self._sender_start_lock = threading.Lock()
        self._last_submit = 0.0
        self.tps_multiplier = _internal_tps_multiplier()
        self._throttle_lock = threading.Lock()
        self._response_log_lock = threading.Lock()
        self._last_response_warning_at = {}
        # smpplib clients share one socket and sequence generator. Serialize
        # only the short write operation; do not hold this lock while waiting
        # for submit_sm_resp so the configured window can stay in flight.
        self._send_lock = threading.Lock()
        self._window = threading.BoundedSemaphore(self.window_size)
        # S2 Fair mode accounts one credit per submit_sm actually handed to the
        # transport.  Legacy keeps the historical task-wide semaphore.  The two
        # units are deliberately visible separately in status().
        self._pdu_window_lock = threading.Condition(threading.RLock())
        self._pdu_sequence_credits = {}
        self._pdu_unbound_reservations = {}
        self._pdu_retired_generations = set()
        self._pdu_credit_stats = {"acquired": 0, "released": 0, "retired": 0}
        self._connection_generation = uuid.uuid4().hex
        self._fair_handoff_permits = threading.BoundedSemaphore(1)
        self._fair_handoff_lock = threading.Lock()
        self._fair_active_handoffs = 0
        # A control action records the newest durable activity generation here
        # as a best-effort queue accelerator. The database generation/attempt
        # fence remains authoritative if this process is restarted.
        self._campaign_invalidations = {}
        self._campaign_invalidation_lock = threading.Lock()

    def _fair_mode(self) -> bool:
        from services import bounded_dispatch_service
        return bounded_dispatch_service.mode() == "fair"

    def _pdu_used_locked(self) -> int:
        return len(self._pdu_sequence_credits) + len(self._pdu_unbound_reservations)

    def _pdu_retire_locked(self):
        """Clear a disconnected generation; late responses can never pay it."""
        retired = (len(self._pdu_sequence_credits)
                   + len(self._pdu_unbound_reservations))
        self._pdu_sequence_credits.clear()
        self._pdu_unbound_reservations.clear()
        self._pdu_retired_generations.add(self._connection_generation)
        self._pdu_credit_stats["retired"] += retired
        if retired:
            self._pdu_window_lock.notify_all()

    def _pdu_retire(self):
        with self._pdu_window_lock:
            self._pdu_retire_locked()

    def _pdu_reserve(self, generation: str, timeout: float):
        """Reserve one not-yet-written PDU slot without blocking task workers."""
        deadline = time.monotonic() + max(0.0, timeout)
        key = (generation, uuid.uuid4().hex)
        with self._pdu_window_lock:
            while generation not in self._pdu_retired_generations:
                if self._pdu_used_locked() < self.window_size:
                    self._pdu_unbound_reservations[key] = generation
                    self._pdu_credit_stats["acquired"] += 1
                    self._pdu_window_lock.notify_all()
                    return key
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._pdu_window_lock.wait(min(remaining, 0.1))
            return None

    def _pdu_bind(self, reservation, sequence, generation: str) -> bool:
        with self._pdu_window_lock:
            if self._pdu_unbound_reservations.get(reservation) != generation:
                return False
            del self._pdu_unbound_reservations[reservation]
            self._pdu_sequence_credits[(generation, int(sequence))] = reservation
            return True

    def _pdu_release_sequence_locked(self, sequence) -> bool:
        # Called while holding _condition; acquire the independent PDU lock
        # here so a response can safely settle its exact credit.
        with self._pdu_window_lock:
            key = next((item for item in self._pdu_sequence_credits
                        if item[1] == int(sequence)), None)
            if key is None:
                return False
            del self._pdu_sequence_credits[key]
            self._pdu_credit_stats["released"] += 1
            self._pdu_window_lock.notify_all()
            return True

    def _pdu_release_reservation(self, reservation) -> bool:
        with self._pdu_window_lock:
            if reservation not in self._pdu_unbound_reservations:
                return False
            del self._pdu_unbound_reservations[reservation]
            self._pdu_credit_stats["released"] += 1
            self._pdu_window_lock.notify_all()
            return True

    def _pdu_window_status(self) -> dict:
        with self._pdu_window_lock:
            return {
                "unit": "submit_sm_pdu",
                "limit": self.window_size,
                "used": self._pdu_used_locked(),
                "bound_sequences": len(self._pdu_sequence_credits),
                "unbound_write_reservations": len(self._pdu_unbound_reservations),
                "credits": dict(self._pdu_credit_stats),
                "hard_limit": "IMPLEMENTED",
            }

    @staticmethod
    def _text(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").strip("\x00")
        return str(value or "").strip("\x00")

    def _persist_message_ids(self, task: dict) -> bool:
        mode = task.get("_delivery_receipt_mode") or self.config.get("delivery_receipt_mode")
        return (mode or "optional") != "disabled"

    def _metric_labels(self):
        return {"config_id": self.config["id"], "slot": self.slot}

    def invalidate_campaign(self, campaign_type: str, campaign_id: int,
                            dispatch_generation: int) -> None:
        key = (str(campaign_type), int(campaign_id))
        generation = int(dispatch_generation or 0)
        with self._campaign_invalidation_lock:
            self._campaign_invalidations[key] = max(
                generation, int(self._campaign_invalidations.get(key, -1))
            )

    def _is_invalidated_campaign_task(self, task: dict) -> bool:
        key = (str(task.get("campaign_type") or ""), int(task.get("campaign_id") or 0))
        with self._campaign_invalidation_lock:
            generation = self._campaign_invalidations.get(key)
        if generation is None:
            return False
        return int(task.get("dispatch_generation") or 0) < int(generation)

    def _response_latency_locked(self, sequence, outcome):
        """Return response latency once, including the early-response race."""
        if outcome.get("_latency_recorded"):
            return None
        sent_at = self._sequence_sent_at.get(sequence)
        received_at = outcome.get("_received_at")
        if sent_at is None or received_at is None:
            return None
        outcome["_latency_recorded"] = True
        return max(0.0, float(received_at) - float(sent_at))

    def _log_response_failure(self, sequence, task_id, status):
        """Log non-zero submit_sm_resp status codes at a bounded rate."""
        interval = max(
            0.0,
            float(os.environ.get("MC_SMPP_RESPONSE_ERROR_LOG_INTERVAL", "60")),
        )
        now = time.monotonic()
        with self._response_log_lock:
            previous = self._last_response_warning_at.get(status)
            if previous is not None and now - previous < interval:
                return
            self._last_response_warning_at[status] = now
        logger.warning(
            "SMPP submit_sm_resp failure config=%s slot=%s sequence=%s "
            "task=%s status=0x%08x (further same-status logs suppressed for %.0fs)",
            self.config.get("id"), self.slot, sequence, task_id, status, interval,
        )

    def _start_async_sender(self):
        with self._sender_start_lock:
            current = self._sender_thread
            if current and current.is_alive():
                return
            self._sender_stop.clear()
            self._sender_thread = threading.Thread(
                target=self._async_sender_loop,
                name=f"smpp-sender-{self.config['id']}-{self.slot}",
                daemon=True,
            )
            self._sender_thread.start()

    def _async_sender_loop(self):
        while not self._sender_stop.is_set():
            try:
                task, callback = self._send_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            fair_handoff = self._fair_mode()
            if fair_handoff:
                with self._fair_handoff_lock:
                    self._fair_active_handoffs += 1
            try:
                if self._is_invalidated_campaign_task(task):
                    metrics_service.inc(
                        "marketing_copilot_campaign_queue_invalidated_total",
                        labels={
                            "campaign_type": task.get("campaign_type", ""),
                            "campaign_id": task.get("campaign_id", 0),
                        },
                    )
                    self._dispatch_async_completion(
                        task, [],
                        SmppSubmissionNotAllowedError(
                            "campaign dispatch generation was invalidated before socket write"
                        ),
                        callback,
                    )
                    continue
                self._send_windowed_async(task, callback)
            except Exception:
                logger.exception(
                    "SMPP async sender failed config=%s slot=%s task=%s",
                    self.config.get("id"), self.slot, task.get("id"),
                )
                self._dispatch_async_completion(
                    task, [], SmppDeliveryUnknownError("SMPP async sender failed"), callback,
                )
            finally:
                self._send_queue.task_done()
                if fair_handoff:
                    with self._fair_handoff_lock:
                        self._fair_active_handoffs -= 1
                    self._fair_handoff_permits.release()

    def _fail_queued_submissions(self, error: Exception):
        queued = []
        while True:
            try:
                queued.append(self._send_queue.get_nowait())
            except queue.Empty:
                break
        for task, callback in queued:
            self._send_queue.task_done()
            self._dispatch_async_completion(
                task, [],
                SmppSubmissionNotAllowedError(
                    "SMPP queued submission was stopped before socket write"
                ),
                callback,
            )

    def _store_outcome(self, pdu, ok: bool):
        sequence = getattr(pdu, "sequence", None)
        received_at = time.monotonic()
        outcome = {"ok": ok, "status": int(getattr(pdu, "status", 0) or 0),
                   "message_id": self._text(getattr(pdu, "message_id", "")),
                   "_received_at": received_at}
        completion = None
        context = None
        response_latency = None
        with self._condition:
            task_id = self._sequence_tasks.get(sequence)
            context = self._sequence_context.get(sequence)
            target = self._outcomes if sequence in self._sequence_tasks else self._early_outcomes
            target[sequence] = outcome
            response_latency = self._response_latency_locked(sequence, outcome)
            self._condition.notify_all()
        if context is not None:
            sms_task_service.record_part_result(
                context["task"], context["part_no"], int(sequence), ok,
                outcome["status"], outcome["message_id"], self.config["id"],
                self._connection_generation,
            )
        elif (ok and task_id and outcome["message_id"]
              and self._sequence_persist_message_ids.get(sequence, True)):
            # Synchronous compatibility path.
            sms_task_service.append_message_id(
                task_id, outcome["message_id"], wait=False,
                source_namespace=sms_task_service.smpp_source_namespace(self.config["id"]),
            )
        with self._condition:
            if task_id in self._async_submissions:
                completion = self._take_async_completion_locked(task_id)
        result = "success" if ok else "failure"
        if context is not None:
            task = context["task"]
            campaign_labels = {
                "campaign_type": task.get("campaign_type", ""),
                "campaign_id": task.get("campaign_id", 0),
                "config_id": self.config["id"],
                "result": result,
            }
            metrics_service.inc(
                "marketing_copilot_campaign_submit_sm_resp_total",
                labels=campaign_labels,
            )
            metrics_service.record_event(
                "marketing_copilot_campaign_submit_sm_resp",
                labels=campaign_labels,
            )
        metrics_service.record_smpp_response(
            self.config["id"], self.slot, outcome["status"], result,
            latency_seconds=response_latency,
        )
        if outcome["status"] != 0 or not ok:
            self._log_response_failure(sequence, task_id, outcome["status"])
        if completion:
            self._dispatch_async_completion(*completion)

    def _take_async_completion_locked(self, task_id: int):
        submission = self._async_submissions.get(task_id)
        if not submission or any(sequence not in self._outcomes for sequence in submission.sequences):
            return None
        outcomes = []
        for sequence in submission.sequences:
            outcome = self._outcomes.pop(sequence)
            response_latency = self._response_latency_locked(sequence, outcome)
            outcomes.append(outcome)
        for sequence in submission.sequences:
            self._pdu_release_sequence_locked(sequence)
            self._sequence_tasks.pop(sequence, None)
            self._sequence_persist_message_ids.pop(sequence, None)
            self._sequence_sent_at.pop(sequence, None)
            self._sequence_context.pop(sequence, None)
        self._async_submissions.pop(task_id, None)
        if not self._fair_mode():
            self._window.release()
        failed = next((item for item in outcomes if not item["ok"]), None)
        accepted_parts = sum(1 for item in outcomes if item["ok"])
        if fast_send_service.is_fast_task(submission.task):
            submission.task["_fast_part_results"] = [
                {
                    "part_no": index + 1,
                    "sequence": int(sequence),
                    "ok": bool(item.get("ok")),
                    "status": int(item.get("status") or 0),
                    "message_id": str(item.get("message_id") or ""),
                }
                for index, (sequence, item) in enumerate(
                    zip(submission.sequences, outcomes)
                )
            ]
            submission.task["_fast_accepted_parts"] = accepted_parts
            submission.task["_accepted_parts"] = accepted_parts
            submission.task["_fast_socket_submitted"] = True
        error = None
        if failed:
            error = (
                SmppPartialSubmitError(failed["status"], accepted_parts)
                if accepted_parts else SmppSubmitError(failed["status"])
            )
        if error is None:
            submission.task["_submit_sm_parts"] = len(submission.sequences)
            metrics_service.inc(
                "marketing_copilot_smpp_submitted_total",
                amount=len(submission.sequences), labels={"config_id": self.config["id"]},
            )
        message_ids = [item["message_id"] for item in outcomes if item["message_id"]]
        return submission.task, message_ids, error, submission.callback

    def _dispatch_async_completion(self, task, message_ids, error, callback):
        callback_started = time.monotonic()
        try:
            callback(task, message_ids, error)
        except Exception:
            logger.exception("SMPP async completion callback failed task=%s", task.get("id"))
        finally:
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_async_completion_seconds",
                time.monotonic() - callback_started,
                labels=self._metric_labels(),
            )

    def _async_reaper_loop(self):
        while not self._async_reaper_stop.wait(0.1):
            try:
                now = time.monotonic()
                completions = []
                with self._condition:
                    expired = [
                        submission for submission in self._async_submissions.values()
                        if submission.deadline <= now
                    ]
                    for submission in expired:
                        current = self._async_submissions.pop(submission.task_id, None)
                        if current is None:
                            continue
                        for sequence in submission.sequences:
                            self._pdu_release_sequence_locked(sequence)
                            self._sequence_tasks.pop(sequence, None)
                            self._sequence_persist_message_ids.pop(sequence, None)
                            self._sequence_sent_at.pop(sequence, None)
                            self._sequence_context.pop(sequence, None)
                            self._outcomes.pop(sequence, None)
                        if not self._fair_mode():
                            self._window.release()
                        metrics_service.inc(
                            "marketing_copilot_smpp_submit_sm_resp_timeout_total",
                            amount=len(submission.sequences),
                            labels=self._metric_labels(),
                        )
                        completions.append((
                            submission.task,
                            [],
                            SmppDeliveryUnknownError("submit_sm_resp timeout; delivery status unknown"),
                            submission.callback,
                        ))
                    if expired:
                        self._condition.notify_all()
                for completion in completions:
                    self._dispatch_async_completion(*completion)
            except Exception:
                logger.exception("SMPP async response reaper failed; it will retry")
                time.sleep(0.2)

    def _start_async_reaper(self):
        current = self._async_reaper
        if current and current.is_alive():
            return
        self._async_reaper_stop.clear()
        self._async_reaper = threading.Thread(
            target=self._async_reaper_loop,
            name=f"smpp-async-reaper-{self.config['id']}-{self.slot}",
            daemon=True,
        )
        self._async_reaper.start()

    def _fail_async_submissions(self, error: Exception):
        completions = []
        with self._condition:
            submissions = list(self._async_submissions.values())
            self._async_submissions.clear()
            for submission in submissions:
                for sequence in submission.sequences:
                    self._pdu_release_sequence_locked(sequence)
                    self._sequence_tasks.pop(sequence, None)
                    self._sequence_persist_message_ids.pop(sequence, None)
                    self._sequence_sent_at.pop(sequence, None)
                    self._sequence_context.pop(sequence, None)
                    self._outcomes.pop(sequence, None)
                if not self._fair_mode():
                    self._window.release()
                completions.append((submission.task, [], error, submission.callback))
            if submissions:
                self._condition.notify_all()
        for completion in completions:
            self._dispatch_async_completion(*completion)

    def _on_delivery(self, pdu, **kwargs):
        import smpplib.consts
        try:
            message_id = self._text(getattr(pdu, "receipted_message_id", ""))
            state = self._text(getattr(pdu, "message_state", ""))
            raw = self._text(getattr(pdu, "short_message", ""))
            detail_logger.info(
                "SMPP delivery receipt config=%s slot=%s command=%s "
                "sequence=%s message_id=%s state=%s",
                self.config.get("id"), self.slot,
                getattr(pdu, "command", ""), getattr(pdu, "sequence", ""),
                message_id, state,
            )
            if not message_id:
                logger.warning(
                    "SMPP delivery receipt missing message_id config=%s slot=%s "
                    "sequence=%s state=%s",
                    self.config.get("id"), self.slot,
                    getattr(pdu, "sequence", ""), state,
                )
            else:
                sms_task_service.record_receipt(
                    message_id, state, raw, wait=False,
                    source_namespace=sms_task_service.smpp_source_namespace(
                        self.config.get("id")
                    ),
                )
                metrics_service.inc(
                    "marketing_copilot_dlr_total",
                    labels={"state": state or "unknown"},
                )
            return smpplib.consts.SMPP_ESME_ROK
        except Exception:
            logger.exception("Failed to handle SMPP delivery receipt")
            return smpplib.consts.SMPP_ESME_RSYSERR
    def _listen(self, client):
        try:
            client.listen(auto_send_enquire_link=True)
        except Exception as exc:
            if self.connected:
                self.last_error = str(exc)
                logger.exception("SMPP listener stopped config=%s slot=%s", self.config["id"], self.slot)
        finally:
            self.connected = False
            self.state = "disconnected"
            metrics_service.set_gauge("marketing_copilot_smpp_connection", 0,
                                      {"config_id": self.config["id"], "slot": self.slot})

    def connect(self):
        with self._condition:
            if self.connected and self._listener and self._listener.is_alive():
                return
            # A reconnect can be initiated by the dedicated sender thread;
            # do not stop that thread while resetting only the transport.
            self.disconnect(stop_sender=False)
            self.state = "connecting"
            try:
                import smpplib.client
                client = smpplib.client.Client(
                    self.config["host"], int(self.config["port"]), allow_unknown_opt_params=True,
                    timeout=int(self.config.get("timeout") or 10))
                client.set_message_sent_handler(lambda pdu, **kw: self._store_outcome(pdu, True))
                client.set_message_received_handler(self._on_delivery)
                client.set_error_pdu_handler(lambda pdu, **kw: self._store_outcome(pdu, False))
                client.connect()
                bind_type = self.config.get("bind_type") or "transceiver"
                getattr(client, f"bind_{bind_type}", client.bind_transceiver)(
                    system_id=self.config["system_id"], password=self.config["password"])
                self.client, self.connected = client, True
                self._connection_generation = uuid.uuid4().hex
                self.state, self.last_error, self.last_connected_at = "bound", "", time.time()
                self._listener = threading.Thread(target=self._listen, args=(client,),
                                                  name=f"smpp-{self.config['id']}-{self.slot}", daemon=True)
                self._listener.start()
                metrics_service.inc("marketing_copilot_smpp_connect_total", labels={"config_id": self.config["id"]})
                metrics_service.set_gauge("marketing_copilot_smpp_connection", 1,
                                          {"config_id": self.config["id"], "slot": self.slot})
            except Exception as exc:
                self.connected, self.client, self.state, self.last_error = False, None, "error", str(exc)
                metrics_service.inc("marketing_copilot_smpp_connect_failures_total", labels={"config_id": self.config["id"]})
                raise

    def disconnect(self, stop_sender=True):
        if stop_sender:
            self._sender_stop.set()
            sender = self._sender_thread
            if sender and sender is not threading.current_thread():
                sender.join(timeout=1)
            self._sender_thread = None
        self._async_reaper_stop.set()
        reaper = self._async_reaper
        if reaper and reaper is not threading.current_thread():
            reaper.join(timeout=1)
        self._async_reaper = None
        client, self.client = self.client, None
        self.connected, self.state = False, "disconnected"
        if client:
            try: client.unbind()
            except Exception: pass
            try: client.disconnect()
            except Exception: pass
        listener = self._listener
        if listener and listener is not threading.current_thread():
            listener.join(timeout=2)
        self._listener = None
        self._fail_async_submissions(
            SmppDeliveryUnknownError("SMPP connection closed with pending submissions")
        )
        # A failed write has no sequence.  Its unbound credit is proved dead at
        # this generation boundary; late responses are ignored by the cleared
        # sequence maps and can never release a newer generation.
        self._pdu_retire()
        if stop_sender:
            self._fail_queued_submissions(
                SmppDeliveryUnknownError("SMPP connection closed with queued submissions")
            )
        metrics_service.set_gauge("marketing_copilot_smpp_connection", 0,
                                  {"config_id": self.config.get("id", 0), "slot": self.slot})

    def _throttle(self) -> bool:
        configured_tps = max(1.0, float(self.config.get("tps") or 1))
        interval = 1.0 / (configured_tps * self.tps_multiplier)
        route_key = int(self.config.get("id") or 0)
        route_revision = str(self.config.get("updated_at") or "")
        if not route_revision:
            route_revision = f"tps:{configured_tps}:multiplier:{self.tps_multiplier}"
        lock_started = time.monotonic()
        with _SHARED_TPS_LOCK:
            lock_wait = time.monotonic() - lock_started
            now = time.monotonic()
            previous = _SHARED_TPS_CONFIG.get(route_key)
            if previous and previous[0] != route_revision:
                # A dynamic route update must not inherit an arbitrarily long
                # reservation debt from the old interval.  Preserve at most
                # one current-interval reservation, and let already-reserved
                # old callers re-check the shared state below.
                _SHARED_TPS_NEXT[route_key] = max(
                    now,
                    min(_SHARED_TPS_NEXT.get(route_key, now), now + interval),
                )
            _SHARED_TPS_CONFIG[route_key] = (route_revision, interval)
            scheduled_at = max(now, _SHARED_TPS_NEXT.get(route_key, 0.0) + interval)
            _SHARED_TPS_NEXT[route_key] = scheduled_at
            wait = max(0.0, scheduled_at - now)

        # Reserve the next slot while holding the lock, but never sleep while
        # holding it.  Multiple workers can therefore reserve consecutive
        # slots immediately instead of queueing behind a sleeping worker.
        metrics_service.observe_histogram(
            "marketing_copilot_smpp_throttle_lock_wait_seconds",
            lock_wait, labels=self._metric_labels(),
        )
        metrics_service.observe_histogram(
            "marketing_copilot_smpp_throttle_sleep_seconds",
            wait, labels=self._metric_labels(),
        )
        metrics_service.observe_histogram(
            "marketing_copilot_smpp_throttle_wait_seconds",
            wait, labels=self._metric_labels(),
        )
        if wait <= 0:
            return True

        # Re-check the route reservation while waiting.  This makes a live
        # tps update stop-aware and prevents a stale long sleep from surviving
        # a configuration revision.
        slept = 0.0
        while wait > 0:
            chunk_started = time.monotonic()
            if self._sender_stop.wait(min(wait, 0.1)):
                metrics_service.observe_histogram(
                    "marketing_copilot_smpp_throttle_actual_sleep_seconds",
                    slept + (time.monotonic() - chunk_started),
                    labels=self._metric_labels(),
                )
                return False
            slept += time.monotonic() - chunk_started
            with _SHARED_TPS_LOCK:
                current = _SHARED_TPS_CONFIG.get(route_key)
                now = time.monotonic()
                if current and current[0] != route_revision:
                    route_revision, interval = current
                    scheduled_at = max(
                        now, _SHARED_TPS_NEXT.get(route_key, now) + interval,
                    )
                    _SHARED_TPS_NEXT[route_key] = scheduled_at
                wait = max(0.0, scheduled_at - now)
        metrics_service.observe_histogram(
            "marketing_copilot_smpp_throttle_actual_sleep_seconds",
            slept, labels=self._metric_labels(),
        )
        return True

    def send(self, task: dict) -> list[str]:
        with self._window:
            return self._send_windowed(task)

    def send_async(self, task: dict, callback: Callable):
        """Queue a task for the per-connection asynchronous sender.

        The caller only pays the bounded queue operation. A dedicated sender
        thread performs multipart splitting, local TPS pacing and socket
        writes, so task workers are not occupied by the full multipart loop.
        The SMPP window is acquired by that sender and remains held until all
        submit_sm_resp responses arrive or the response timeout expires.
        """
        self._start_async_sender()
        self._start_async_reaper()
        queue_wait_started = time.monotonic()
        queue_wait_timeout = max(
            0.1, min(60.0, float(self.config.get("timeout") or 10) + 2.0)
        )
        try:
            # Queue capacity is an in-process backpressure boundary, not an
            # SMSC rejection.  Waiting briefly lets the sender drain at the
            # configured route TPS instead of converting normal low-TPS load
            # into a retryable 0x45 task failure and another DB claim cycle.
            self._send_queue.put((task, callback), timeout=queue_wait_timeout)
            metrics_service.inc(
                "marketing_copilot_campaign_tasks_slot_queued_total",
                labels={
                    "campaign_type": task.get("campaign_type", ""),
                    "campaign_id": task.get("campaign_id", 0),
                },
            )
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_send_queue_wait_seconds",
                time.monotonic() - queue_wait_started,
                labels=self._metric_labels(),
            )
            metrics_service.set_gauge(
                "marketing_copilot_smpp_send_queue_depth",
                self._send_queue.qsize(), labels=self._metric_labels(),
            )
        except queue.Full:
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_send_queue_wait_seconds",
                time.monotonic() - queue_wait_started,
                labels=self._metric_labels(),
            )
            metrics_service.inc(
                "marketing_copilot_smpp_send_queue_full_total",
                labels=self._metric_labels(),
            )
            self._dispatch_async_completion(
                task, [], SmppWindowExhaustedError("SMPP async send queue full"), callback,
            )

    def try_send_async(self, task: dict, callback: Callable) -> bool:
        """S1 handoff: never wait for Slot FIFO capacity on a shared worker.

        Fair mode additionally bounds each physical Slot to one not-yet-started
        descriptor.  The permit transfers to the dedicated sender thread and is
        released only after the send loop has taken ownership of the task (or
        failed it before a socket write).  This is a shallow handoff fence, not
        a substitute for the PDU credit ledger below.
        """
        self._start_async_sender()
        self._start_async_reaper()
        fair = self._fair_mode()
        acquired = self._fair_handoff_permits.acquire(blocking=False) if fair else True
        if fair and not acquired:
            return False
        try:
            self._send_queue.put_nowait((task, callback))
            if fair:
                metrics_service.set_gauge(
                    "marketing_copilot_smpp_fair_active_handoffs",
                    self._fair_active_handoffs + 1,
                    labels=self._metric_labels(),
                )
            return True
        except queue.Full:
            if fair:
                self._fair_handoff_permits.release()
            return False

    def _send_windowed(self, task: dict) -> list[str]:
        # Synchronous direct send is rejected in Fair mode and retains the
        # historical task-wide window unit.
        fair_window = False
        self.connect()
        import smpplib.consts
        parts, coding, esm_class = _make_task_parts(task)
        source_addr = task.get("source_addr") or self.config.get("source_addr") or "Marketing"
        content = task.get("content") or ""
        task_id = task.get("id")
        persist_message_ids = self._persist_message_ids(task)
        attempt_token = sms_task_service.record_send_intent(
            task, self.config["id"], self.slot, len(parts),
            self._connection_generation,
        )
        if not attempt_token:
            raise SmppSubmissionNotAllowedError(
                task.pop("_submit_gate_reason", None)
                or "task claim or send window is no longer active"
            )
        detail_logger.info(
            "[SMPP SEND] task=%s campaign=%s/%s name=%s msisdn=%s source_addr=%s "
            "config=%s %s:%s slot=%s parts=%d content_len=%d content=%s",
            task_id, task.get("campaign_type"), task.get("campaign_id"),
            task.get("campaign_name", ""), task.get("msisdn"), source_addr,
            self.config.get("id"), self.config.get("host"), self.config.get("port"), self.slot,
            len(parts), len(content), content[:1000],
        )
        sequences = []
        submitted_any = False
        try:
            for part_no, part in enumerate(parts, 1):
                if not self._throttle():
                    raise SmppSubmissionNotAllowedError(
                        "SMPP submission stopped before socket write"
                    )
                from services.bounded_dispatch_service import assert_delivery_safe
                assert_delivery_safe(task)
                send_started = time.monotonic()
                send_lock_started = time.monotonic()
                with self._send_lock:
                    metrics_service.observe_histogram(
                        "marketing_copilot_smpp_send_lock_wait_seconds",
                        time.monotonic() - send_lock_started,
                        labels=self._metric_labels(),
                    )
                    with fast_send_service.socket_admission(task):
                        pdu = self.client.send_message(
                            source_addr_ton=int(self.config.get("address_ton") or smpplib.consts.SMPP_TON_ALNUM),
                            source_addr_npi=int(self.config.get("address_npi") or smpplib.consts.SMPP_NPI_UNK),
                            source_addr=source_addr, dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                            dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN, destination_addr=task["msisdn"],
                            short_message=part, data_coding=coding, esm_class=esm_class,
                            registered_delivery=(self.config.get('delivery_receipt_mode') or 'optional') != 'disabled')
                sent_at = time.monotonic()
                metrics_service.inc(
                    "marketing_copilot_smpp_submit_sm_sent_total",
                    labels=self._metric_labels(),
                )
                metrics_service.record_event(
                    "marketing_copilot_smpp_submit_sm_sent",
                    labels=self._metric_labels(),
                )
                metrics_service.observe_histogram(
                    "marketing_copilot_smpp_submit_sm_send_seconds",
                    sent_at - send_started, labels=self._metric_labels(),
                )
                metrics_service.inc(
                    "marketing_copilot_campaign_socket_parts_total",
                    labels={
                        "campaign_type": task.get("campaign_type", ""),
                        "campaign_id": task.get("campaign_id", 0),
                        "config_id": self.config["id"],
                    },
                )
                metrics_service.record_event(
                    "marketing_copilot_campaign_socket_parts",
                    labels={
                        "campaign_type": task.get("campaign_type", ""),
                        "campaign_id": task.get("campaign_id", 0),
                        "config_id": self.config["id"],
                    },
                )
                submitted_any = True
                sequence = pdu.sequence
                with self._condition:
                    self._sequence_tasks[sequence] = task_id
                    self._sequence_persist_message_ids[sequence] = persist_message_ids
                    self._sequence_sent_at[sequence] = sent_at
                    self._sequence_context[sequence] = {
                        "task": task, "part_no": part_no,
                        "window_generation": generation if fair_window else None,
                    }
                    if sequence in self._early_outcomes:
                        outcome = self._early_outcomes.pop(sequence)
                        self._outcomes[sequence] = outcome
                        response_latency = self._response_latency_locked(sequence, outcome)
                        if response_latency is not None:
                            metrics_service.observe_histogram(
                                "marketing_copilot_smpp_submit_sm_resp_latency_seconds",
                                response_latency, labels=self._metric_labels(),
                            )
                        if (outcome["ok"] and outcome["message_id"] and task_id
                                and persist_message_ids):
                            sms_task_service.append_message_id(
                                task_id, outcome["message_id"], wait=False,
                                source_namespace=sms_task_service.smpp_source_namespace(
                                    self.config["id"]
                                ),
                            )
                    sequences.append(sequence)
            deadline = time.monotonic() + int(self.config.get("timeout") or 10)
            with self._condition:
                while any(seq not in self._outcomes for seq in sequences):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise SmppDeliveryUnknownError("submit_sm_resp timeout; delivery status unknown")
                    self._condition.wait(min(remaining, 0.25))
                outcomes = [self._outcomes.pop(seq) for seq in sequences]
            failed = next((item for item in outcomes if not item["ok"]), None)
            if failed:
                accepted_parts = sum(1 for item in outcomes if item["ok"])
                if accepted_parts:
                    raise SmppPartialSubmitError(failed["status"], accepted_parts)
                raise SmppSubmitError(failed["status"])
            task["_submit_sm_parts"] = len(parts)
            detail_logger.info(
                "[SMPP SEND OK] task=%s sequences=%s message_ids=%s",
                task_id, sequences,
                [item.get("message_id") for item in outcomes if item.get("message_id")],
            )
            metrics_service.inc("marketing_copilot_smpp_submitted_total",
                                amount=len(parts), labels={"config_id": self.config["id"]})
            return [item["message_id"] for item in outcomes if item["message_id"]]
        except fast_send_service.FastSendAdmissionDenied as exc:
            if task.get("_socket_write_started"):
                raise SmppDeliveryUnknownError("campaign fence closed after socket admission") from exc
            raise SmppSubmissionNotAllowedError(str(exc)) from exc
        except SmppSubmissionNotAllowedError as exc:
            if task.get("_socket_write_started"):
                raise SmppDeliveryUnknownError("submission stopped after socket admission") from exc
            raise
        except (SmppSubmitError, SmppDeliveryUnknownError):
            raise
        except Exception as exc:
            if task.get("_socket_write_started"):
                raise SmppDeliveryUnknownError(f"connection lost after submit: {exc}") from exc
            raise
        finally:
            with self._condition:
                for seq in sequences:
                    self._sequence_tasks.pop(seq, None)
                    self._sequence_persist_message_ids.pop(seq, None)
                    self._sequence_sent_at.pop(seq, None)
                    self._sequence_context.pop(seq, None)

    def _send_windowed_async(self, task: dict, callback: Callable):
        fair_window = self._fair_mode()
        if not fair_window:
            wait_timeout = max(1.0, min(60.0, float(self.config.get("timeout") or 10) + 2.0))
            window_wait_started = time.monotonic()
            window_acquired = self._window.acquire(timeout=wait_timeout)
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_window_wait_seconds",
                time.monotonic() - window_wait_started,
                labels=self._metric_labels(),
            )
            if not window_acquired:
                metrics_service.inc(
                    "marketing_copilot_smpp_window_exhausted_total",
                    labels={"config_id": self.config["id"], "slot": self.slot},
                )
                self._dispatch_async_completion(
                    task, [], SmppWindowExhaustedError(), callback,
                )
                return
        reservation = None
        try:
            connect_started = time.monotonic()
            self.connect()
            self._start_async_reaper()
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_connection_ensure_seconds",
                time.monotonic() - connect_started,
                labels=self._metric_labels(),
            )
            import smpplib.consts
            parts_started = time.monotonic()
            parts, coding, esm_class = _make_task_parts(task)
            metrics_service.observe_histogram(
                "marketing_copilot_smpp_make_parts_seconds",
                time.monotonic() - parts_started,
                labels=self._metric_labels(),
            )
            task_id = task.get("id")
            if task_id is None:
                raise ValueError("SMPP async submission requires a task id")
            dispatch_reservation = task.get("_dispatch_reservation")
            if dispatch_reservation is not None and len(parts) != int(
                    dispatch_reservation.cost["parts"]):
                raise SmppSubmissionNotAllowedError("encoded parts differ from reserved cost")
            persist_message_ids = self._persist_message_ids(task)
            generation = self._connection_generation
            if fast_send_service.is_fast_task(task):
                attempt_token = fast_send_service.prepare_task(
                    task, len(parts),
                    sms_task_service.smpp_source_namespace(self.config["id"]),
                )
            else:
                attempt_token = sms_task_service.record_send_intent(
                    task, self.config["id"], self.slot, len(parts),
                    generation,
                )
            if not attempt_token:
                raise SmppSubmissionNotAllowedError(
                    task.pop("_submit_gate_reason", None)
                    or "task claim or send window is no longer active"
                )
            source_addr = task.get("source_addr") or self.config.get("source_addr") or "Marketing"
            detail_logger.info(
                "[SMPP ASYNC SEND] task=%s msisdn=%s config=%s slot=%s parts=%s window_unit=%s",
                task_id, task.get("msisdn"), self.config.get("id"), self.slot, len(parts),
                "pdu" if fair_window else "task",
            )
        except Exception as exc:
            if not fair_window:
                self._window.release()
            error = exc if isinstance(exc, SmppSubmitError) else SmppDeliveryUnknownError(str(exc))
            self._dispatch_async_completion(task, [], error, callback)
            return
        sequences = []
        submitted_any = False
        parts_loop_started = time.monotonic()
        try:
            for part_no, part in enumerate(parts, 1):
                if self._is_invalidated_campaign_task(task):
                    raise SmppSubmissionNotAllowedError(
                        "campaign dispatch generation was invalidated before socket write"
                    )
                if not self._throttle():
                    raise SmppSubmissionNotAllowedError(
                        "SMPP submission stopped before socket write"
                    )
                from services.bounded_dispatch_service import assert_delivery_safe
                assert_delivery_safe(task)
                if fair_window:
                    reservation = self._pdu_reserve(
                        generation, max(0.0, float(self.config.get("timeout") or 10))
                    )
                    if reservation is None:
                        raise SmppWindowExhaustedError(
                            "Fair PDU window remained exhausted before submit_sm"
                        )
                send_started = time.monotonic()
                send_lock_started = time.monotonic()
                with self._send_lock:
                    with fast_send_service.socket_admission(task):
                        metrics_service.observe_histogram(
                            "marketing_copilot_smpp_send_lock_wait_seconds",
                            time.monotonic() - send_lock_started,
                            labels=self._metric_labels(),
                        )
                        pdu = self.client.send_message(
                            source_addr_ton=int(self.config.get("address_ton") or smpplib.consts.SMPP_TON_ALNUM),
                            source_addr_npi=int(self.config.get("address_npi") or smpplib.consts.SMPP_NPI_UNK),
                            source_addr=source_addr, dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                            dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN, destination_addr=task["msisdn"],
                            short_message=part, data_coding=coding, esm_class=esm_class,
                            registered_delivery=(self.config.get("delivery_receipt_mode") or "optional") != "disabled",
                        )
                if fast_send_service.is_fast_task(task):
                    task["_fast_socket_submitted"] = True
                if fair_window:
                    self._pdu_bind(reservation, pdu.sequence, generation)
                sent_at = time.monotonic()
                metrics_service.inc(
                    "marketing_copilot_smpp_submit_sm_sent_total",
                    labels=self._metric_labels(),
                )
                metrics_service.record_event(
                    "marketing_copilot_smpp_submit_sm_sent",
                    labels=self._metric_labels(),
                )
                metrics_service.observe_histogram(
                    "marketing_copilot_smpp_submit_sm_send_seconds",
                    sent_at - send_started, labels=self._metric_labels(),
                )
                metrics_service.inc(
                    "marketing_copilot_campaign_socket_parts_total",
                    labels={
                        "campaign_type": task.get("campaign_type", ""),
                        "campaign_id": task.get("campaign_id", 0),
                        "config_id": self.config["id"],
                    },
                )
                metrics_service.record_event(
                    "marketing_copilot_campaign_socket_parts",
                    labels={
                        "campaign_type": task.get("campaign_type", ""),
                        "campaign_id": task.get("campaign_id", 0),
                        "config_id": self.config["id"],
                    },
                )
                submitted_any = True
                sequence = pdu.sequence
                early_outcome = None
                with self._condition:
                    self._sequence_tasks[sequence] = task_id
                    self._sequence_persist_message_ids[sequence] = persist_message_ids
                    self._sequence_sent_at[sequence] = sent_at
                    self._sequence_context[sequence] = {
                        "task": task, "part_no": part_no,
                        "window_generation": generation if fair_window else None,
                    }
                    if sequence in self._early_outcomes:
                        early_outcome = self._early_outcomes.pop(sequence)
                        self._outcomes[sequence] = early_outcome
                        response_latency = self._response_latency_locked(sequence, early_outcome)
                        if response_latency is not None:
                            metrics_service.observe_histogram(
                                "marketing_copilot_smpp_submit_sm_resp_latency_seconds",
                                response_latency, labels=self._metric_labels(),
                            )
                    sequences.append(sequence)
                if early_outcome is not None:
                    sms_task_service.record_part_result(
                        task, part_no, int(sequence), early_outcome["ok"],
                        early_outcome["status"], early_outcome["message_id"],
                        self.config["id"], self._connection_generation,
                    )

            metrics_service.observe_histogram(
                "marketing_copilot_smpp_submit_parts_loop_seconds",
                time.monotonic() - parts_loop_started,
                labels=self._metric_labels(),
            )

            with self._condition:
                self._async_submissions[task_id] = _AsyncSubmission(
                    task, task_id, sequences,
                    time.monotonic() + int(self.config.get("timeout") or 10), callback,
                )
                completion = self._take_async_completion_locked(task_id)
            if completion:
                self._dispatch_async_completion(*completion)
        except Exception as exc:
            error = exc
            if isinstance(error, fast_send_service.FastSendAdmissionDenied):
                error = SmppSubmissionNotAllowedError(str(error))
            if (task.get("_socket_write_started")
                    and not isinstance(error, SmppSubmitError)):
                error = SmppDeliveryUnknownError(
                    "submit_sm write outcome is ambiguous; automatic resend is unsafe"
                )
            if submitted_any and isinstance(error, SmppSubmissionNotAllowedError):
                error = SmppDeliveryUnknownError(
                    "campaign stop fence closed after one or more submit_sm writes"
                )
            elif submitted_any and not isinstance(error, SmppSubmitError):
                error = SmppDeliveryUnknownError(f"connection lost after submit: {exc}")
            with self._condition:
                for sequence in sequences:
                    self._pdu_release_sequence_locked(sequence)
                    self._sequence_tasks.pop(sequence, None)
                    self._sequence_persist_message_ids.pop(sequence, None)
                    self._sequence_sent_at.pop(sequence, None)
                    self._sequence_context.pop(sequence, None)
                    self._outcomes.pop(sequence, None)
                if self._async_submissions.pop(task_id, None) is None:
                    if fair_window:
                        if task.get("_socket_write_started"):
                            # A failed/ambiguous write keeps its unbound credit
                            # until this generation is isolated and closed.
                            self._pdu_release_reservation(reservation)
                    else:
                        self._window.release()
                elif fair_window:
                    self._pdu_release_reservation(reservation)
            self._dispatch_async_completion(task, [], error, callback)
        finally:
            metrics_service.set_gauge(
                "marketing_copilot_smpp_send_queue_depth",
                self._send_queue.qsize(), labels=self._metric_labels(),
            )

    def status(self):
        configured_route_tps = max(0.0, float(self.config.get("tps") or 0))
        fair = self._fair_mode()
        return {"slot": self.slot, "state": self.state, "connected": self.connected,
                "last_error": self.last_error, "last_connected_at": self.last_connected_at,
                "window_size": self.window_size, "inflight": len(self._sequence_tasks),
                "window_unit": "submit_sm_pdu" if fair else "task",
                "pdu_window": self._pdu_window_status() if fair else None,
                "fair_active_handoffs": self._fair_active_handoffs,
                "send_queue_size": self._send_queue.qsize(),
                "send_queue_capacity": self._send_queue_capacity,
                "configured_route_tps": configured_route_tps,
                "configured_per_bind_tps": None,
                "effective_total_limit_tps": configured_route_tps * self.tps_multiplier,
                "tps_scope": "route_shared",
                "tps_unit": "submit_sm_parts/s"}


class SmppGateway:
    def __init__(self, connection_factory=_SmppConnection):
        self._connection_factory = connection_factory
        self._lock = threading.RLock()
        self._pools = {}
        self._round_robin = {}
        self._fair_served = {}
        self._down_until = {}
        self._last_error = ""
        self._active_config_id = None
        self._config_cache = None
        self._config_cache_version = 0

    def invalidate_config_cache(self):
        with self._lock:
            self._config_cache = None
            self._config_cache_version += 1

    def _enabled_configs(self):
        with self._lock:
            if self._config_cache is not None:
                return [dict(config) for config in self._config_cache]
            generation = self._config_cache_version
        rows = read_transaction(
            lambda conn: conn.execute(
                "SELECT * FROM smpp_configs WHERE enabled=1 ORDER BY priority,id"
            ).fetchall(),
            operation="smpp_gateway.enabled_configs",
        )
        configs = [dict(row) for row in rows]
        with self._lock:
            if generation == self._config_cache_version:
                self._config_cache = configs
        return [dict(config) for config in configs]

    @property
    def connected(self):
        return any(connection.connected for pool in self._pools.values() for connection in pool)

    def _connection_for(self, config: dict, *, connect=True):
        config_id = config["id"]
        with self._lock:
            pool = self._pools.setdefault(config_id, [])
            desired = max(1, min(20, int(config.get("pool_size") or 1)))
            pool[:] = [item for item in pool if item.config.get("updated_at") == config.get("updated_at")]
            if len(pool) < desired:
                connection = self._connection_factory(config, len(pool) + 1)
                pool.append(connection)
            else:
                index = self._round_robin.get(config_id, 0) % len(pool)
                connection = pool[index]
                self._round_robin[config_id] = index + 1
        if connect:
            connection.connect()
        return connection

    def connect(self):
        configs = self._enabled_configs()
        if not configs:
            raise RuntimeError("No enabled SMPP configuration")
        errors = []
        for config in configs:
            if time.monotonic() < self._down_until.get(config["id"], 0):
                continue
            try:
                desired = max(1, min(20, int(config.get("pool_size") or 1)))
                for _ in range(desired):
                    self._connection_for(config)
                self._active_config_id = config["id"]
                return
            except Exception as exc:
                errors.append(f"{config['name']}: {exc}")
                self._mark_down(config, exc)
        raise RuntimeError("All SMSC configurations unavailable: " + "; ".join(errors))

    def _mark_down(self, config: dict, exc: Exception):
        delay = max(1, min(300, int(config.get("reconnect_max_seconds") or 60)))
        self._down_until[config["id"]] = time.monotonic() + delay
        self._last_error = str(exc)
        metrics_service.inc("marketing_copilot_smpp_failures_total", labels={"config_id": config["id"]})
        for connection in self._pools.get(config["id"], []):
            if not connection.connected:
                connection.disconnect()

    def send(self, task: dict) -> list[str]:
        from services import bounded_dispatch_service
        if bounded_dispatch_service.mode() == "fair":
            raise SmppSubmissionNotAllowedError("direct synchronous sending is disabled in fair mode")
        configs = self._enabled_configs()
        if not configs:
            raise RuntimeError("No enabled SMPP configuration")
        target_id = task.get("smpp_config_id")
        if target_id:
            config = next((c for c in configs if c["id"] == target_id), None)
            if not config:
                raise RuntimeError(
                    f"Associated SMPP configuration {target_id} is missing or disabled")
            connection = self._connection_for(config)
            task['_delivery_receipt_mode'] = config.get('delivery_receipt_mode') or 'optional'
            task['_transport_smpp_config_id'] = config['id']
            self._active_config_id = config['id']
            return connection.send(task)
        previous_id = None
        errors = []
        for config in configs:
            if time.monotonic() < self._down_until.get(config["id"], 0):
                continue
            try:
                connection = self._connection_for(config)
                if previous_id is not None and previous_id != config["id"]:
                    metrics_service.inc("marketing_copilot_smpp_failovers_total",
                                        labels={"from_config": previous_id, "to_config": config["id"]})
                task['_delivery_receipt_mode'] = config.get('delivery_receipt_mode') or 'optional'
                task['_transport_smpp_config_id'] = config['id']
                result = connection.send(task)
                self._active_config_id = config["id"]
                return result
            except (SmppSubmitError, SmppDeliveryUnknownError):
                raise
            except Exception as exc:
                previous_id = config["id"]
                errors.append(f"{config['name']}: {exc}")
                self._mark_down(config, exc)
                logger.warning("SMSC unavailable, trying backup config=%s error=%s", config["id"], exc)
        raise RuntimeError("All SMSC configurations unavailable: " + "; ".join(errors))

    def send_async(self, task: dict, callback: Callable):
        """Queue a bounded SMPP submission and complete it from the response path."""
        from services import bounded_dispatch_service
        if bounded_dispatch_service.mode() == "fair":
            raise SmppSubmissionNotAllowedError("fair sending requires a reserved bounded handoff")
        configs = self._enabled_configs()
        if not configs:
            callback(task, [], RuntimeError("No enabled SMPP configuration"))
            return
        target_id = task.get("smpp_config_id")
        candidates = ([next((c for c in configs if c["id"] == target_id), None)]
                      if target_id else configs)
        if target_id and candidates[0] is None:
            callback(task, [], RuntimeError(
                f"Associated SMPP configuration {target_id} is missing or disabled"))
            return

        errors = []
        for config in candidates:
            if config is None or time.monotonic() < self._down_until.get(config["id"], 0):
                continue
            try:
                connection = self._connection_for(config)
                task["_delivery_receipt_mode"] = config.get("delivery_receipt_mode") or "optional"
                task["_transport_smpp_config_id"] = config["id"]
                self._active_config_id = config["id"]
                connection.send_async(task, callback)
                return
            except Exception as exc:
                errors.append(f"{config['name']}: {exc}")
                self._mark_down(config, exc)
                logger.warning("SMSC unavailable for async send config=%s error=%s", config["id"], exc)
                if target_id:
                    break
        callback(task, [], RuntimeError("All SMSC configurations unavailable: " + "; ".join(errors)))

    def resolve_dispatch_route(self, task, configs):
        target = task.get("transport_smpp_config_id") or task.get("smpp_config_id")
        candidates = [c for c in configs if not target or c["id"] == target]
        return next((c for c in candidates
                     if time.monotonic() >= self._down_until.get(c["id"], 0)), None)

    def _fair_pool(self, config: dict):
        """Return the revision-isolated physical pool without RR side effects."""
        config_id = int(config["id"])
        desired = max(1, min(20, int(config.get("pool_size") or 1)))
        with self._lock:
            pool = self._pools.setdefault(config_id, [])
            retained = [item for item in pool
                        if item.config.get("updated_at") == config.get("updated_at")]
            # Removing a stale object must not join its listener on a task
            # worker. Its disconnected generation fails closed; process shutdown
            # and recover() remain the lifecycle cleanup paths.
            pool[:] = retained
            while len(pool) < desired:
                pool.append(self._connection_factory(config, len(pool) + 1))
            return list(pool)

    def try_send_async(self, task: dict, callback: Callable) -> bool:
        reservation = task.get("_dispatch_reservation")
        if reservation is None or reservation.state != "prepare":
            raise SmppSubmissionNotAllowedError("missing active S1 preparation reservation")
        configs = self._enabled_configs()
        config = next((c for c in configs if c["id"] == reservation.group[2]), None)
        if config is None or str(config.get("updated_at") or "") != task.get("_dispatch_route_revision"):
            raise SmppSubmissionNotAllowedError("resolved route changed before handoff")
        # Connection establishment belongs to the existing Slot sender. No
        # connect(), throttle, window wait or FIFO wait on the SMS worker.
        connections = self._fair_pool(config)
        candidates = sorted(
            connections,
            key=lambda item: (
                1 if item._fair_active_handoffs else 0,
                self._fair_served.get(reservation.group, 0),
                self._round_robin.get(config["id"], 0),
                item.slot,
            ),
        )
        for connection in candidates:
            task["_delivery_receipt_mode"] = config.get("delivery_receipt_mode") or "optional"
            task["_transport_smpp_config_id"] = config["id"]
            if connection.try_send_async(task, callback):
                self._fair_served[reservation.group] = self._fair_served.get(reservation.group, 0) + 1
                with self._lock:
                    self._round_robin[config["id"]] = (
                        self._round_robin.get(config["id"], 0) + 1
                    )
                return True
        return False

    def disconnect(self, config_id=None):
        with self._lock:
            targets = [config_id] if config_id is not None else list(self._pools)
            for target in targets:
                for connection in self._pools.pop(target, []):
                    connection.disconnect()
            if config_id is None:
                self._active_config_id = None

    def invalidate_campaign(self, campaign_type: str, campaign_id: int,
                            dispatch_generation: int) -> None:
        """Fast-drop pre-authorization queue entries for one activity."""
        with self._lock:
            pools = [connection for pool in self._pools.values() for connection in pool]
        for connection in pools:
            connection.invalidate_campaign(
                campaign_type, campaign_id, dispatch_generation,
            )

    def recover(self, config_id=None):
        """Immediately rebuild the live pool after an operator confirms recovery."""
        configs = self._enabled_configs()
        targets = [item for item in configs if config_id is None or item["id"] == config_id]
        if config_id is not None and not targets:
            raise RuntimeError(f"SMPP configuration {config_id} is missing or disabled")
        for config in targets:
            self._down_until.pop(config["id"], None)
            self.disconnect(config["id"])
            desired = max(1, min(20, int(config.get("pool_size") or 1)))
            for _ in range(desired):
                self._connection_for(config)
            self._active_config_id = config["id"]
        self._last_error = ""
        return self.status()

    def status(self) -> dict:
        configs = self._enabled_configs()
        pools = []
        for config in configs:
            connections = [item.status() for item in self._pools.get(config["id"], [])]
            pools.append({"config_id": config["id"], "name": config["name"], "priority": config["priority"],
                          "desired_pool_size": config["pool_size"], "connections": connections,
                          "healthy": any(item["connected"] for item in connections),
                          "cooldown_seconds": max(0, round(self._down_until.get(config["id"], 0) - time.monotonic(), 1))})
        return {"state": "bound" if self.connected else "disconnected", "connected": self.connected,
                "config_id": self._active_config_id, "active_config_id": self._active_config_id,
                "enabled_configs": len(configs), "pools": pools, "last_error": self._last_error,
                "dispatch_fairness": "IMPLEMENTED_NOT_VERIFIED",
                "socket_fairness": "NOT_VERIFIED",
                "pdu_window_hard_limit": "NOT_VERIFIED"}


gateway = SmppGateway()
