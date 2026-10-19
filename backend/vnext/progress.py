"""Bounded, ordered progress persistence for one VNext worker.

The SMPP reader must never wait for a journal ``fsync``.  This module owns a
single writer thread per worker, a bounded in-memory queue, and explicit
capacity reservations taken before a PDU may cross the socket boundary.

The journal remains the durable source of truth.  The queue only holds events
whose worst-case journal footprint was reserved up front.  A process crash can
therefore still lose already-observed-but-not-yet-fsynced events; those are
reported as unresolved rather than as durable success.
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Callable

from .critical_path_profiler import CriticalPathProfiler
from .projection_writer import ProjectionBackpressure, ProgressJournal


CAMPAIGN_LIMIT = 256
AGGREGATE_SCHEMA_VERSION = 1


class ProgressPersistenceError(RuntimeError):
    pass


class ProgressPersistenceBlocked(ProgressPersistenceError):
    """The bounded reservation budget cannot accept another dispatch."""


class ProgressReservationError(ProgressPersistenceError):
    """A reservation was used after release or beyond its reserved slots."""


def _campaign_key(event: dict) -> tuple[str, int]:
    return (str(event.get("campaign_type") or ""), int(event.get("campaign_id") or 0))


def _campaign_payload(campaign_type: str, campaign_id: int) -> dict:
    return {
        "campaign_type": str(campaign_type),
        "campaign_id": int(campaign_id),
    }


def _empty_campaign_counters() -> Counter:
    return Counter()


def _apply_event_to_counters(counters: Counter, event: dict) -> None:
    """Apply one durable progress event to a campaign counter set."""

    if event.get("accepted"):
        counters["accepted_tasks"] += 1
    if event.get("taken"):
        counters["tasks_taken"] += 1
    state = str(event.get("state") or "")
    if state == "socket_written":
        counters["socket_written_events"] += 1
        counters["socket_written_pdus"] += max(0, int(event.get("parts_written", 0) or 0))
    elif state == "write_failed":
        counters["write_failed"] += 1
    elif state == "unknown":
        counters["unknown"] += 1
    elif state == "submit_sm_resp_success":
        counters["response_success"] += 1
    elif state == "submit_sm_resp_error":
        counters["response_error"] += 1
    elif state == "cancelled":
        counters["cancelled"] += 1
    elif state == "expired":
        counters["expired"] += 1
    counters["durable_events"] += 1


def _pending_response_kind(event: dict) -> str:
    state = str(event.get("state") or "")
    if state == "submit_sm_resp_success":
        return "response_success"
    if state == "submit_sm_resp_error":
        return "response_error"
    if state == "unknown":
        return "unknown"
    return ""


class ProgressReservation:
    """A bounded pre-dispatch reservation owned by one worker task.

    ``slots_total`` covers one terminal send event plus one possible response
    event per encoded SMS part.  A part is claimed immediately before it may
    cross the socket boundary.  Never-written parts can be released; claimed
    parts stay reserved until the response or an explicit unknown event is
    submitted.
    """

    def __init__(self, store: "ProgressPersistence", token: int, slots_total: int,
                 bytes_per_slot: int):
        self.store = store
        self.token = int(token)
        self.slots_total = max(1, int(slots_total))
        self.bytes_per_slot = max(1, int(bytes_per_slot))
        self.slots_consumed = 0
        self.slots_claimed = 0
        self.slots_released = 0

    @property
    def slots_open(self) -> int:
        with self.store._lock:
            return self._slots_open_locked()

    def _slots_open_locked(self) -> int:
        return max(0, self.slots_total - self.slots_consumed - self.slots_released)

    def claim_response_slot(self) -> bool:
        """Reserve one response slot for a part that is about to be written."""

        with self.store._lock:
            if self._slots_open_locked() <= 1:
                return False
            if self.slots_claimed >= self.slots_total - 1:
                return False
            self.slots_claimed += 1
            return True

    def consume(self) -> bool:
        """Move one reserved slot into the writer queue."""

        with self.store._lock:
            if self._slots_open_locked() <= 0:
                return False
            self.slots_consumed += 1
            self.store._reserved_items = max(0, self.store._reserved_items - 1)
            self.store._reserved_bytes = max(
                0, self.store._reserved_bytes - self.bytes_per_slot
            )
            return True

    def release_claim(self) -> int:
        """Release one claimed slot that will never produce an event."""

        with self.store._lock:
            if self.slots_claimed <= 0:
                return 0
            self.slots_claimed -= 1
            return self._release_locked(1)

    def release_unwritten(self, count: int) -> int:
        """Release response slots for parts that were never claimed."""

        with self.store._lock:
            return self._release_locked(count)

    def _release_locked(self, count: int) -> int:
        available = self._slots_open_locked()
        amount = max(0, min(int(count), available))
        if amount <= 0:
            return 0
        self.slots_released += amount
        self.store._reserved_items = max(0, self.store._reserved_items - amount)
        self.store._reserved_bytes = max(
            0, self.store._reserved_bytes - amount * self.bytes_per_slot
        )
        self.store._notify_locked()
        return amount

    def unresolved_slots(self) -> int:
        """Return claimed slots whose response/unknown event is still open."""

        with self.store._lock:
            return max(0, self.slots_claimed - max(0, self.slots_consumed - 1))

    def snapshot(self) -> dict:
        with self.store._lock:
            return {
                "token": self.token,
                "slots_total": self.slots_total,
                "slots_consumed": self.slots_consumed,
                "slots_claimed": self.slots_claimed,
                "slots_released": self.slots_released,
                "slots_open": self._slots_open_locked(),
                "bytes_per_slot": self.bytes_per_slot,
            }


class ProgressPersistence:
    """One ordered journal writer with bounded memory and reservations."""

    def __init__(
        self,
        journal: ProgressJournal,
        *,
        name: str = "",
        max_pending_items: int = 131_072,
        max_pending_bytes: int = 64 * 1024 * 1024,
        batch_size: int = 256,
        flush_interval_seconds: float = 0.05,
        aggregate_path: str | os.PathLike[str] | None = None,
        aggregate_interval_seconds: float = 1.0,
        max_replay_events: int = 2_000_000,
        profiler: CriticalPathProfiler | None = None,
    ):
        self.journal = journal
        self.name = str(name or journal.path.stem)
        self.max_pending_items = max(1, int(max_pending_items))
        self.max_pending_bytes = max(4096, int(max_pending_bytes))
        self.batch_size = max(1, min(4096, int(batch_size)))
        self.flush_interval_seconds = max(0.01, float(flush_interval_seconds))
        self.aggregate_path = Path(aggregate_path).resolve() if aggregate_path else None
        self.aggregate_interval_seconds = max(
            0.1, float(aggregate_interval_seconds)
        )
        self.max_replay_events = max(0, int(max_replay_events))
        self.profiler = profiler

        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._queue: queue.Queue = queue.Queue(maxsize=self.max_pending_items)
        self._writer_thread: threading.Thread | None = None
        self._retry_batch: list[tuple[dict, int]] | None = None
        self._stop_event = threading.Event()
        self._accepting = True
        self._failure_reason = ""
        self._writer_errors = 0
        self._reserved_items = 0
        self._reserved_bytes = 0
        self._pending_items = 0
        self._pending_bytes = 0
        self._pending_by_campaign: dict[tuple[str, int], Counter] = {}
        self._oldest_pending_at: float | None = None
        self._next_token = 1
        self._writer_batches = 0
        self._writer_events_durable = 0
        self._writer_bytes_durable = 0
        self._fsync_total_ns = 0
        self._last_durable_at = 0.0
        self._last_batch_at = 0.0
        self._last_aggregate_at = 0.0
        self._journal_used_bytes = 0
        self._journal_remaining_bytes = 0
        self._durable_by_campaign: dict[tuple[str, int], Counter] = {}
        self._durable_totals: Counter = Counter()
        self._aggregate_complete = True
        self._aggregate_error = ""
        self._aggregate_last_success_at = 0.0
        self._aggregate_dirty = False
        self._applied_segments: dict[str, int] = {}
        self._unresolved_events = 0
        self._unresolved_bytes = 0
        self._unresolved_since = 0.0
        self._durability_listeners: list[Callable[[list[dict]], None]] = []
        self._failure_path = (
            Path(aggregate_path).with_suffix(".failure.json")
            if aggregate_path else None
        )

        self._journal_used_bytes = self._safe_journal_size()
        self._journal_remaining_bytes = max(
            0, int(self.journal.max_bytes) - self._journal_used_bytes
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        try:
            self.journal.repair_partial_tail()
        except Exception:
            # The first append will retry the repair; a read-only or damaged
            # journal must not prevent the status path from starting.
            pass
        self._load_failure_marker()
        self._load_aggregate_and_replay()
        if self._unresolved_events:
            self._accepting = False
            if not self._failure_reason:
                self._failure_reason = (
                    "unresolved progress failure marker exists; operator "
                    "reconciliation is required"
                )
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name=f"mc-vnext-progress-{self.name}",
            daemon=True,
        )
        self._writer_thread.start()

    def add_durability_listener(
        self, callback: Callable[[list[dict]], None]
    ) -> None:
        with self._lock:
            self._durability_listeners.append(callback)

    def drain(self, timeout: float = 30.0) -> dict:
        deadline = time.monotonic() + max(0.0, float(timeout))
        with self._condition:
            while self._pending_items > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(min(remaining, 0.25))
            drained = self._pending_items == 0
            if drained:
                # A graceful drain is a natural checkpoint boundary; make the
                # durable campaign aggregate reflect every event that just
                # became durable.
                self._persist_aggregate(force=True)
            return {
                "enabled": True,
                "drained": drained,
                "pending_events": self._pending_items,
                "pending_bytes": self._pending_bytes,
                "unresolved_events": self._unresolved_events,
                "writer_errors": self._writer_errors,
                "failure_reason": self._failure_reason,
            }

    def close(self, timeout: float = 30.0) -> dict:
        with self._lock:
            self._accepting = False
        self._stop_event.set()
        with self._lock:
            self._notify_locked()
        thread = self._writer_thread
        if thread and thread.is_alive():
            thread.join(timeout=max(0.0, float(timeout)))
        with self._lock:
            writer_alive = bool(thread and thread.is_alive())
            if thread and thread.is_alive():
                self._writer_errors += 1
                self._failure_reason = (
                    self._failure_reason or "progress writer did not stop in time"
                )
            if not writer_alive:
                self._persist_aggregate(force=True)
            result = {
                "enabled": True,
                "drained": self._pending_items == 0,
                "pending_events": self._pending_items,
                "pending_bytes": self._pending_bytes,
                "unresolved_events": max(
                    self._unresolved_events, self._pending_items
                ),
                "writer_errors": self._writer_errors,
                "failure_reason": self._failure_reason,
            }
            if result["pending_events"] or result["unresolved_events"]:
                self._write_failure_marker(
                    self._failure_reason or "shutdown left undurable progress events",
                    result["unresolved_events"],
                    result["pending_bytes"],
                )
            return result

    # ------------------------------------------------------------------
    # Reservation API used on the worker event loop
    # ------------------------------------------------------------------
    def reserve(self, *, items: int, bytes_total: int) -> ProgressReservation:
        items = max(1, int(items))
        bytes_total = max(items, int(bytes_total))
        with self._lock:
            reason = self._admission_reason_locked(items, bytes_total)
            if reason:
                self._failure_reason = reason
                raise ProgressPersistenceBlocked(reason)
            reservation = ProgressReservation(
                self,
                self._next_token,
                items,
                max(1, (bytes_total + items - 1) // items),
            )
            self._next_token += 1
            self._reserved_items += items
            self._reserved_bytes += reservation.bytes_per_slot * items
            self._failure_reason = ""
            return reservation

    def reserve_for_event(self, event: dict, *, items: int = 1) -> ProgressReservation:
        encoded = ProgressJournal._encode(event)
        per_event = max(256, len(encoded) + 4096)
        return self.reserve(items=items, bytes_total=per_event * int(items))

    def submit(self, event: dict, reservation: ProgressReservation | None = None) -> bool:
        with self._lock:
            if reservation is not None:
                if reservation.store is not self:
                    self._mark_failure_locked("progress reservation belongs to another worker")
                    return False
                if not self._accepting:
                    return False
                if not reservation.consume():
                    self._mark_failure_locked("progress reservation exhausted")
                    return False
                item_bytes = reservation.bytes_per_slot
            else:
                if not self._accepting:
                    return False
                encoded = ProgressJournal._encode(event)
                item_bytes = max(256, len(encoded) + 4096)
                reason = self._admission_reason_locked(1, item_bytes)
                if reason:
                    self._failure_reason = reason
                    return False
            self._pending_items += 1
            self._pending_bytes += item_bytes
            if self._oldest_pending_at is None:
                self._oldest_pending_at = time.monotonic()
            kind = _pending_response_kind(event)
            if kind:
                key = _campaign_key(event)
                counters = self._pending_by_campaign.setdefault(key, Counter())
                counters[kind] += 1
            try:
                self._queue.put_nowait((dict(event), item_bytes))
            except queue.Full:
                self._mark_failure_locked("progress queue unexpectedly full")
                self._unresolved_events += 1
                self._unresolved_bytes += item_bytes
                self._notify_locked()
                return False
            self._notify_locked()
            return True

    def _admission_reason_locked(self, items: int, bytes_total: int) -> str:
        if not self._accepting:
            return self._failure_reason or "progress persistence is not accepting new work"
        if self._pending_items + self._reserved_items + items > self.max_pending_items:
            return "progress queue item budget exhausted"
        if self._pending_bytes + self._reserved_bytes + bytes_total > self.max_pending_bytes:
            return "progress queue byte budget exhausted"
        projected = (
            self._journal_used_bytes
            + self._pending_bytes
            + self._reserved_bytes
            + bytes_total
        )
        if projected > int(self.journal.max_bytes):
            return "journal hard budget exhausted"
        return ""

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------
    def _writer_loop(self) -> None:
        while True:
            if self._retry_batch is not None:
                batch = self._retry_batch
                time.sleep(self.flush_interval_seconds)
            else:
                try:
                    first = self._queue.get(timeout=self.flush_interval_seconds)
                except queue.Empty:
                    if self._stop_event.is_set():
                        break
                    self._refresh_journal_capacity()
                    self._maybe_persist_aggregate()
                    continue
                if first is None:
                    break
                batch: list[tuple[dict, int]] = [first]
                while len(batch) < self.batch_size:
                    try:
                        item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if item is None:
                        self._stop_event.set()
                        break
                    batch.append(item)
            events = [item[0] for item in batch]
            batch_bytes = sum(item[1] for item in batch)
            started_ns = time.perf_counter_ns()
            try:
                self.journal.append_many(events)
            except ProjectionBackpressure as exc:
                with self._lock:
                    self._failure_reason = (
                        f"progress journal capacity backpressure: {exc}"
                    )
                    self._retry_batch = batch
                    self._notify_locked()
                self._refresh_journal_capacity()
                continue
            except Exception as exc:
                with self._lock:
                    self._writer_errors += 1
                    self._mark_failure_locked(
                        f"progress journal append failed: {type(exc).__name__}: {exc}"
                    )
                    self._unresolved_events = max(
                        self._unresolved_events, self._pending_items
                    )
                    self._unresolved_bytes = max(
                        self._unresolved_bytes, self._pending_bytes
                    )
                    self._unresolved_since = time.time()
                    self._write_failure_marker(
                        self._failure_reason,
                        self._unresolved_events,
                        self._unresolved_bytes,
                    )
                    self._notify_locked()
                break
            elapsed_ns = max(0, time.perf_counter_ns() - started_ns)
            with self._lock:
                self._retry_batch = None
                self._failure_reason = ""
                self._writer_batches += 1
                self._writer_events_durable += len(events)
                self._writer_bytes_durable += batch_bytes
                self._fsync_total_ns += elapsed_ns
                self._last_durable_at = time.time()
                self._last_batch_at = time.monotonic()
                self._pending_items = max(0, self._pending_items - len(events))
                self._pending_bytes = max(0, self._pending_bytes - batch_bytes)
                for event in events:
                    kind = _pending_response_kind(event)
                    if kind:
                        key = _campaign_key(event)
                        counters = self._pending_by_campaign.get(key)
                        if counters is not None:
                            counters[kind] = max(0, counters[kind] - 1)
                            if not any(counters.values()):
                                self._pending_by_campaign.pop(key, None)
                    self._apply_durable_event_locked(event)
                if self._pending_items == 0:
                    self._oldest_pending_at = None
                self._notify_locked()
            if self.profiler:
                self.profiler.inc("progress_writer_batches")
                self.profiler.inc("progress_writer_events_durable", len(events))
                self.profiler.observe_ns("progress_writer_batch", elapsed_ns)
                self.profiler.publish_if_due()
            for listener in self._durability_listeners:
                try:
                    listener(events)
                except Exception:
                    continue
            self._refresh_journal_capacity()
            self._maybe_persist_aggregate()
            if self._stop_event.is_set() and self._queue.empty():
                break

    def _apply_durable_event_locked(self, event: dict) -> None:
        key = _campaign_key(event)
        counters = self._durable_by_campaign.get(key)
        if counters is None:
            if len(self._durable_by_campaign) >= CAMPAIGN_LIMIT:
                oldest = min(
                    self._durable_by_campaign.items(),
                    key=lambda item: int(item[1].get("last_event_at", 0) or 0),
                )[0]
                self._durable_by_campaign.pop(oldest, None)
            counters = _empty_campaign_counters()
            self._durable_by_campaign[key] = counters
        _apply_event_to_counters(counters, event)
        _apply_event_to_counters(self._durable_totals, event)
        counters["last_event_at"] = int(time.time())
        self._aggregate_dirty = True

    def _refresh_journal_capacity(self) -> None:
        used = self._safe_journal_size()
        with self._lock:
            self._journal_used_bytes = used
            self._journal_remaining_bytes = max(
                0, int(self.journal.max_bytes) - used
            )
            self._notify_locked()

    def _safe_journal_size(self) -> int:
        try:
            return max(0, int(self.journal.size()))
        except OSError:
            return max(0, int(self._journal_used_bytes))

    # ------------------------------------------------------------------
    # Durable campaign aggregate
    # ------------------------------------------------------------------
    def _maybe_persist_aggregate(self) -> None:
        now = time.monotonic()
        if now - self._last_aggregate_at < self.aggregate_interval_seconds:
            return
        self._last_aggregate_at = now
        self._persist_aggregate(force=False)

    def _persist_aggregate(self, *, force: bool) -> None:
        if self.aggregate_path is None:
            return
        with self._lock:
            if not force and not self._aggregate_dirty:
                return
            # The saved counters and the saved journal cursors must describe
            # the same durable prefix.  Only the writer thread advances the
            # durable counters, so refreshing the cursors first is enough.
            self._update_applied_segments_locked()
            campaigns = []
            for (campaign_type, campaign_id), counters in sorted(
                self._durable_by_campaign.items()
            ):
                payload = _campaign_payload(campaign_type, campaign_id)
                payload.update({key: int(value) for key, value in counters.items()})
                campaigns.append(payload)
            checkpoint = dict(self._applied_segments)
            payload = {
                "schema_version": AGGREGATE_SCHEMA_VERSION,
                "worker": self.name,
                "updated_at": time.time(),
                "campaigns": campaigns,
                "applied_segments": checkpoint,
                "complete": bool(self._aggregate_complete),
                "incomplete_reason": "" if self._aggregate_complete else self._aggregate_error,
            }
        try:
            self.aggregate_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{self.aggregate_path.name}.",
                dir=str(self.aggregate_path.parent),
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                    json.dump(
                        payload, handle, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    )
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.aggregate_path)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
            with self._lock:
                self._aggregate_error = ""
                self._aggregate_last_success_at = time.time()
                self._aggregate_dirty = False
        except (OSError, TypeError, ValueError) as exc:
            with self._lock:
                self._aggregate_complete = False
                self._aggregate_error = (
                    f"aggregate checkpoint failed: {type(exc).__name__}: {exc}"
                )

    def _update_applied_segments_locked(self) -> None:
        try:
            paths = self.journal.segment_paths()
        except OSError:
            return
        for path in paths:
            try:
                self._applied_segments[str(path)] = int(path.stat().st_size)
            except OSError:
                continue

    def _load_aggregate_and_replay(self) -> None:
        aggregate_exists = bool(
            self.aggregate_path is not None and self.aggregate_path.is_file()
        )
        if aggregate_exists:
            try:
                value = json.loads(
                    self.aggregate_path.read_text(encoding="utf-8")
                )
                for item in value.get("campaigns", []):
                    key = (
                        str(item.get("campaign_type") or ""),
                        int(item.get("campaign_id") or 0),
                    )
                    counters = _empty_campaign_counters()
                    counters.update(
                        {
                            str(name): int(amount)
                            for name, amount in item.items()
                            if name not in {"campaign_type", "campaign_id"}
                            and isinstance(amount, (int, float))
                        }
                    )
                    self._durable_by_campaign[key] = counters
                self._applied_segments = {
                    str(path): int(offset)
                    for path, offset in (value.get("applied_segments") or {}).items()
                }
                self._aggregate_complete = bool(value.get("complete", True))
                self._aggregate_error = str(value.get("incomplete_reason") or "")
                self._durable_totals.clear()
                for counters in self._durable_by_campaign.values():
                    for name, amount in counters.items():
                        if name != "last_event_at":
                            self._durable_totals[name] += int(amount)
                if value.get("campaigns") and not self._applied_segments:
                    # Counters without cursors cannot be safely extended by
                    # replay (that would double count).  Rebuild from the
                    # retained journal and mark the result incomplete.
                    self._durable_by_campaign.clear()
                    self._durable_totals.clear()
                    self._aggregate_complete = False
                    self._aggregate_error = (
                        "aggregate checkpoint has counters without journal cursors"
                    )
            except (OSError, ValueError, TypeError) as exc:
                self._aggregate_complete = False
                self._aggregate_error = (
                    f"aggregate checkpoint unreadable: {type(exc).__name__}: {exc}"
                )
        try:
            paths = self.journal.segment_paths()
        except OSError:
            return
        if not aggregate_exists:
            first_generation = self.journal._generation(paths[0]) if paths else -1
            if first_generation > 0:
                self._aggregate_complete = False
                self._aggregate_error = (
                    "aggregate checkpoint missing after journal segment reclamation"
                )
        replayed = 0
        for path in paths:
            try:
                size = int(path.stat().st_size)
            except OSError:
                continue
            key = str(path)
            offset = max(0, int(self._applied_segments.get(key, 0)))
            if offset > size:
                self._aggregate_complete = False
                self._aggregate_error = (
                    f"journal segment shrank below aggregate checkpoint: {path}"
                )
                offset = size
            while offset < size:
                if replayed >= self.max_replay_events:
                    self._aggregate_complete = False
                    self._aggregate_error = (
                        "aggregate replay limit reached; counts may be incomplete"
                    )
                    return
                try:
                    events, new_offset = self.journal.read_segment_from(
                        path, offset, max_events=2048
                    )
                except (OSError, ValueError, TypeError):
                    self._aggregate_complete = False
                    self._aggregate_error = (
                        f"journal replay failed: {path}"
                    )
                    return
                if not events and new_offset == offset:
                    break
                for event in events:
                    self._apply_durable_event_locked(event)
                replayed += len(events)
                offset = new_offset
                self._applied_segments[key] = offset
        self._update_applied_segments_locked()
        self._persist_aggregate(force=True)

    # ------------------------------------------------------------------
    # Failure marker and snapshots
    # ------------------------------------------------------------------
    def _mark_failure_locked(self, reason: str) -> None:
        self._accepting = False
        self._failure_reason = str(reason)
        self._writer_errors += 1
        self._unresolved_since = self._unresolved_since or time.time()
        self._aggregate_complete = False
        self._aggregate_error = (
            self._aggregate_error or "progress persistence failure"
        )

    def _write_failure_marker(
        self, reason: str, events: int, byte_count: int
    ) -> None:
        if self._failure_path is None:
            return
        payload = {
            "schema_version": 1,
            "worker": self.name,
            "failed_at": time.time(),
            "reason": str(reason or "progress persistence failure"),
            "unresolved_events": int(events),
            "unresolved_bytes": int(byte_count),
            "journal": str(self.journal.path),
        }
        try:
            self._failure_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{self._failure_path.name}.",
                dir=str(self._failure_path.parent),
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                    json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self._failure_path)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
        except (OSError, TypeError, ValueError):
            pass

    def _load_failure_marker(self) -> None:
        if self._failure_path is None or not self._failure_path.is_file():
            return
        try:
            value = json.loads(self._failure_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        self._unresolved_events = max(
            self._unresolved_events, int(value.get("unresolved_events", 0) or 0)
        )
        self._unresolved_bytes = max(
            self._unresolved_bytes, int(value.get("unresolved_bytes", 0) or 0)
        )
        self._unresolved_since = float(value.get("failed_at", 0.0) or 0.0)
        if self._unresolved_events:
            self._failure_reason = str(
                value.get("reason") or "unresolved progress persistence failure"
            )

    def _notify_locked(self) -> None:
        self._condition.notify_all()

    def campaign_snapshot(self) -> list[dict]:
        with self._lock:
            pending = {
                key: dict(counters)
                for key, counters in self._pending_by_campaign.items()
            }
            campaigns = []
            for key, counters in sorted(self._durable_by_campaign.items()):
                payload = _campaign_payload(*key)
                payload.update({name: int(value) for name, value in counters.items()})
                payload["pending"] = pending.get(key, {})
                payload["response_observed_not_durable"] = int(
                    (pending.get(key) or {}).get("response_success", 0)
                    + (pending.get(key) or {}).get("response_error", 0)
                    + (pending.get(key) or {}).get("unknown", 0)
                )
                campaigns.append(payload)
            for key, counters in pending.items():
                if key in self._durable_by_campaign:
                    continue
                payload = _campaign_payload(*key)
                payload["pending"] = dict(counters)
                payload["response_observed_not_durable"] = int(
                    counters.get("response_success", 0)
                    + counters.get("response_error", 0)
                    + counters.get("unknown", 0)
                )
                campaigns.append(payload)
            return campaigns

    def snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            oldest_age_ms = (
                max(0.0, now - self._oldest_pending_at) * 1000.0
                if self._oldest_pending_at is not None else 0.0
            )
            return {
                "enabled": True,
                "name": self.name,
                "accepting": bool(self._accepting),
                "blocked_reason": str(self._failure_reason),
                "retry_batch_events": (
                    len(self._retry_batch) if self._retry_batch else 0
                ),
                "queue_depth": int(self._pending_items),
                "items_bytes": int(self._pending_bytes),
                "reservations_items": int(self._reserved_items),
                "reservations_bytes": int(self._reserved_bytes),
                "pending_durability": int(self._pending_items),
                "pending_durability_bytes": int(self._pending_bytes),
                "writer_batches": int(self._writer_batches),
                "writer_events_durable": int(self._writer_events_durable),
                "writer_bytes_durable": int(self._writer_bytes_durable),
                "writer_fsync_total_ms": round(self._fsync_total_ns / 1_000_000.0, 3),
                "writer_oldest_age_ms": round(oldest_age_ms, 3),
                "writer_errors": int(self._writer_errors),
                "last_error": str(self._failure_reason),
                "last_durable_at": float(self._last_durable_at or 0.0),
                "journal_used_bytes": int(self._journal_used_bytes),
                "journal_remaining_bytes": int(self._journal_remaining_bytes),
                "journal_max_bytes": int(self.journal.max_bytes),
                "aggregate_path": str(self.aggregate_path or ""),
                "aggregate_complete": bool(self._aggregate_complete),
                "aggregate_error": str(self._aggregate_error),
                "aggregate_last_success_at": float(self._aggregate_last_success_at or 0.0),
                "unresolved_events": int(self._unresolved_events),
                "unresolved_bytes": int(self._unresolved_bytes),
                "unresolved_since": float(self._unresolved_since or 0.0),
                "durable_response_success": int(
                    self._durable_totals.get("response_success", 0)
                ),
                "durable_response_error": int(
                    self._durable_totals.get("response_error", 0)
                ),
                "durable_unknown": int(self._durable_totals.get("unknown", 0)),
                "durable_socket_written_pdus": int(
                    self._durable_totals.get("socket_written_pdus", 0)
                ),
                "durable_socket_written_events": int(
                    self._durable_totals.get("socket_written_events", 0)
                ),
                "durable_accepted_tasks": int(
                    self._durable_totals.get("accepted_tasks", 0)
                ),
                "durable_tasks_taken": int(
                    self._durable_totals.get("tasks_taken", 0)
                ),
                "durable_write_failed": int(
                    self._durable_totals.get("write_failed", 0)
                ),
                "durable_expired": int(self._durable_totals.get("expired", 0)),
                "durable_cancelled": int(self._durable_totals.get("cancelled", 0)),
                "durable_events": int(self._durable_totals.get("durable_events", 0)),
            }
