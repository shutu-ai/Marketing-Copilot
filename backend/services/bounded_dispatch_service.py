"""S1 process-local admission and ready cache. DuckDB owns all business work.

No database, gateway, logging or blocking queue calls occur under the ledger
condition. Reservations remain charged through Slot handoff and completion;
result debt is retired only at an observed empty result-buffer boundary.
The fair ReadyPool is the bounded socket-admission scheduler: it charges the
resolved campaign/route group by encoded submit_sm parts before handing work to
the physical Slot.  Socket continuation/yield remains owned by the gateway.
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
import os
import queue
import sys
import threading
import time
import uuid


_mode_lock = threading.Lock()
_frozen_mode = None


def _percentile(values, fraction: float) -> float:
    """Return a bounded percentile for the low-cardinality fairness ring."""
    values = sorted(float(value) for value in values)
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(round((len(values) - 1) * fraction))))
    return round(values[index], 6)


def mode():
    with _mode_lock:
        if _frozen_mode is not None:
            return _frozen_mode
    value = os.environ.get("MC_CAMPAIGN_DISPATCH_MODE", "legacy").strip().lower()
    if value not in {"legacy", "fair"}:
        raise ValueError("MC_CAMPAIGN_DISPATCH_MODE must be legacy or fair")
    return value


def freeze_mode():
    global _frozen_mode
    with _mode_lock:
        if _frozen_mode is None:
            value = os.environ.get("MC_CAMPAIGN_DISPATCH_MODE", "legacy").strip().lower()
            if value not in {"legacy", "fair"}:
                raise ValueError("MC_CAMPAIGN_DISPATCH_MODE must be legacy or fair")
            if value == "fair" and os.environ.get("MC_ALLOW_MULTI_INSTANCE") == "1":
                raise RuntimeError("fair dispatch requires the single-process owner")
            _frozen_mode = value
        return _frozen_mode


def _positive(name, default):
    value = int(os.environ.get(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def assert_delivery_safe(task):
    """Common failures stop new fair socket effects; received results may drain.

    Called outside the Slot/campaign locks, using the existing cached pressure
    sample and immediate coordinator maintenance state, without a DB query.
    """
    reservation = task.get("_dispatch_reservation")
    if reservation is None:
        return
    from services import backpressure_service, fast_send_service
    from services import result_capacity_service as capacity
    if not capacity.ledger.safe():
        raise fast_send_service.FastSendAdmissionDenied("result byte/replay safety gate is closed")
    credit = getattr(reservation, "result_credit", None)
    if credit is None or credit.closed:
        raise fast_send_service.FastSendAdmissionDenied("missing live result reservation")
    active, _state = backpressure_service.reasons(refresh_database=False)
    reasons = [r for r in active if r in {"writes", "disk", "results", "database_maintenance"}]
    state = reservation.pool.status()
    if state["closed"] or state["failure"] or reasons:
        raise fast_send_service.FastSendAdmissionDenied("common delivery safety gate is closed")


@dataclass(frozen=True)
class Limits:
    tasks: int = 64
    parts: int = 1024
    bytes: int = 16 * 1024 * 1024
    group_tasks: int = 8
    group_parts: int = 256
    group_bytes: int = 2 * 1024 * 1024
    campaign_tasks: int = 8
    route_tasks: int = 32
    prepare_tasks: int = 8
    prepare_bytes: int = 2 * 1024 * 1024
    result_events: int = 4096
    result_bytes: int = 32 * 1024 * 1024
    max_task_bytes: int = 128 * 1024
    max_task_parts: int = 255

    def __post_init__(self):
        if any(value < 1 for value in vars(self).values()):
            raise ValueError("dispatch limits must be positive")

    @classmethod
    def from_env(cls):
        defaults = cls()
        return cls(**{name: _positive(f"MC_DISPATCH_{name.upper()}", default)
                      for name, default in vars(defaults).items()})


def cached_bytes(value, seen=None):
    """Size cached Python values once, excluding the attached ledger handle."""
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(cached_bytes(k, seen) + cached_bytes(v, seen)
                    for k, v in value.items() if k != "_dispatch_reservation")
    elif isinstance(value, (list, tuple, set, deque)):
        size += sum(cached_bytes(v, seen) for v in value)
    return size


@dataclass
class Reservation:
    pool: object
    group: tuple
    task_id: int
    generation: int
    cost: dict
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    epoch: str = ""
    claim: str = ""
    state: str = "reserved"
    task: dict | None = None
    created: float = field(default_factory=time.monotonic)
    eligible_at: float = 0.0
    preparing: bool = False
    result_credit: object = None


class ReadyPool:
    def __init__(self, limits=None):
        self.limits = limits or Limits.from_env()
        self.maxsize = self.limits.tasks
        try:
            configured_quantum = int(os.environ.get("MC_SOCKET_DRR_QUANTUM_PARTS", "20"))
        except (TypeError, ValueError):
            configured_quantum = 20
        self._drr_quantum_parts = max(1, min(4096, configured_quantum))
        self._drr_max_deficit = self._drr_quantum_parts * 20 + self.limits.max_task_parts
        self.epoch = uuid.uuid4().hex
        self._condition = threading.Condition(threading.RLock())
        self._live = {}
        self._groups = {}
        self._ring = deque()
        self._drr_deficit = {}
        self._drr_served = Counter()
        self._drr_wait_seconds = deque(maxlen=4096)
        self._drr_oldest_eligible_at = 0.0
        self._used = Counter()
        self._group_used = {}
        self._campaign_used = Counter()
        self._route_used = Counter()
        self._stats = Counter()
        self._blocked = Counter()
        self._closed = False
        self._failure = ""

    def reserve(self, task, route_id, parts):
        if int(route_id) <= 0:
            raise ValueError("resolved physical route required before reservation")
        actual = cached_bytes(task)
        # Headroom for claim/Fast schedule metadata and encoded/result references.
        charged = actual + 16384 + 512 * int(parts)
        group = (str(task["campaign_type"]), int(task["campaign_id"]), int(route_id))
        cost = dict(tasks=1, parts=int(parts), bytes=charged,
                    result_events=2 * int(parts) + 4,
                    result_bytes=(2 * int(parts) + 4) * 8192)
        with self._condition:
            reason = ""
            if self._closed or self._failure:
                reason = "closed"
            elif parts < 1 or parts > self.limits.max_task_parts or actual > self.limits.max_task_bytes:
                reason = "task_envelope"
            else:
                per_group = self._group_used.get(group, Counter())
                for unit, amount in cost.items():
                    if self._used[unit] + amount > getattr(self.limits, unit):
                        reason = f"global_{unit}"
                        break
                    if unit in {"tasks", "parts", "bytes"} and (
                            per_group[unit] + amount > getattr(self.limits, f"group_{unit}")):
                        reason = f"group_{unit}"
                        break
                if self._campaign_used[group[:2]] >= self.limits.campaign_tasks:
                    reason = "campaign_tasks"
                if self._route_used[group[2]] >= self.limits.route_tasks:
                    reason = "route_tasks"
            if reason:
                self._blocked[reason] += 1
                return None
            item = Reservation(self, group, int(task["id"]),
                               int(task.get("dispatch_generation") or 0), cost, epoch=self.epoch)
            self._live[item.id] = item
            self._used.update(cost)
            self._group_used.setdefault(group, Counter()).update(cost)
            self._campaign_used[group[:2]] += 1
            self._route_used[group[2]] += 1
            self._stats["acquired"] += 1
        from services import result_capacity_service as capacity
        if capacity.enabled():
            item.result_credit = capacity.reserve(task, int(parts))
            if item.result_credit is None:
                self.release(item)
                return None
        return item

    def publish(self, item, task):
        actual = cached_bytes(task)
        with self._condition:
            if self._closed or self._live.get(item.id) is not item or item.state != "reserved":
                raise RuntimeError("reservation cannot be published")
            if (int(task["id"]) != item.task_id
                    or int(task.get("dispatch_generation") or 0) != item.generation
                    or not task.get("claim_token") or actual > item.cost["bytes"]):
                self._stats["overflow"] += 1
                raise RuntimeError("claimed descriptor exceeded its reservation/identity")
            item.claim = str(task["claim_token"])
            if item.result_credit:
                item.result_credit.claim = item.claim
            item.task = task
            task["_dispatch_reservation"] = item
            item.state = "ready"
            self._enqueue_locked(item)

    def _enqueue_locked(self, item):
        if item.group not in self._groups:
            self._groups[item.group] = deque()
            self._ring.append(item.group)
            self._drr_deficit.setdefault(item.group, 0)
        if not item.eligible_at:
            item.eligible_at = time.monotonic()
        self._groups[item.group].append(item)
        self._condition.notify_all()

    def get(self, timeout=0.2):
        deadline = time.monotonic() + timeout
        with self._condition:
            while not self._closed:
                now = time.monotonic()
                for _ in range(len(self._ring)):
                    group = self._ring.popleft()
                    items = self._groups[group]
                    item = items[0]
                    self._ring.append(group)
                    if item.eligible_at > now:
                        self._drr_oldest_eligible_at = min(
                            (value.eligible_at for value in items if value.eligible_at > now),
                            default=0.0,
                        )
                        continue
                    # This is the actual socket-admission credit unit.  The
                    # claim scheduler has a separate DRR, but it must not be
                    # treated as proof of transport fairness: a long SMS is
                    # charged by its encoded submit_sm parts here as well.
                    self._drr_deficit[group] = min(
                        self._drr_max_deficit,
                        self._drr_deficit.get(group, 0) + self._drr_quantum_parts,
                    )
                    cost_parts = int(item.cost["parts"])
                    if cost_parts > self._drr_deficit[group]:
                        self._blocked["socket_drr_credit"] += 1
                        continue
                    if (self._used["prepare_tasks"] >= self.limits.prepare_tasks
                            or self._used["prepare_bytes"] + item.cost["bytes"] > self.limits.prepare_bytes):
                        self._blocked["prepare"] += 1
                        continue
                    items.popleft()
                    if not items:
                        self._ring.remove(group)
                        del self._groups[group]
                    item.state = "prepare"
                    item.preparing = True
                    self._drr_deficit[group] -= cost_parts
                    self._drr_served[group] += cost_parts
                    self._drr_wait_seconds.append(max(0.0, now - item.created))
                    self._drr_oldest_eligible_at = 0.0
                    self._used["prepare_tasks"] += 1
                    self._used["prepare_bytes"] += item.cost["bytes"]
                    self._stats["prepared"] += 1
                    return item.task
                remaining = deadline - now
                if remaining <= 0:
                    raise queue.Empty
                self._condition.wait(min(remaining, 0.05))
            raise queue.Empty

    def _end_prepare_locked(self, item):
        if item.preparing:
            item.preparing = False
            self._used["prepare_tasks"] -= 1
            self._used["prepare_bytes"] -= item.cost["bytes"]

    def handoff(self, item):
        with self._condition:
            self._end_prepare_locked(item)
            # An early callback can already have completed the descriptor.
            if item.state == "prepare":
                item.state = "handoff"
            self._condition.notify_all()

    def retry_ready(self, item):
        with self._condition:
            self._end_prepare_locked(item)
            if item.state == "prepare" and not self._closed:
                item.state = "ready"
                item.eligible_at = time.monotonic() + 0.05
                self._blocked["slot_busy"] += 1
                self._enqueue_locked(item)
                return True
            return False

    def begin_completion(self, item):
        with self._condition:
            if self._live.get(item.id) is not item or item.state in {"completing", "result_pending", "failed"}:
                self._stats["duplicate_completion"] += 1
                return False
            self._end_prepare_locked(item)
            item.state = "completing"
            return True

    def completed(self, item, failed=False):
        with self._condition:
            self._end_prepare_locked(item)
            if self._live.get(item.id) is item:
                item.state = "failed" if failed else "result_pending"
                if failed:
                    self._failure = "result_handoff_failed"
                self._condition.notify_all()

    def result_candidates(self):
        with self._condition:
            return {item.id for item in self._live.values() if item.state == "result_pending"}

    def retire_results(self, candidates):
        """Caller first observes Fast accumulator + durable buffer fully drained."""
        with self._condition:
            items = [item for item in self._live.values()
                     if item.id in candidates and item.state == "result_pending"]
            for item in items:
                self._release_locked(item)

    def release(self, item):
        with self._condition:
            return self._release_locked(item)

    def _release_locked(self, item):
        if self._live.get(item.id) is not item:
            self._stats["duplicate_release"] += 1
            return False
        self._end_prepare_locked(item)
        queued = self._groups.get(item.group)
        if queued and item in queued:
            queued.remove(item)
            if not queued:
                del self._groups[item.group]
                self._ring.remove(item.group)
        del self._live[item.id]
        self._used.subtract(item.cost)
        group = self._group_used[item.group]
        group.subtract(item.cost)
        if not group["tasks"]:
            del self._group_used[item.group]
        if item.group not in self._groups:
            self._drr_deficit.pop(item.group, None)
            self._drr_served.pop(item.group, None)
        for counter, key in ((self._campaign_used, item.group[:2]), (self._route_used, item.group[2])):
            counter[key] -= 1
            if not counter[key]:
                del counter[key]
        item.state = "released"
        item.task = None
        self._stats["released"] += 1
        if item.result_credit:
            from services import result_capacity_service as capacity
            capacity.release_owner(item.task_id, item.result_credit)
        self._condition.notify_all()
        return True

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def abandon(self):
        """After all owner threads stop, discard cache, never requeue DB work."""
        with self._condition:
            self._groups.clear()
            self._ring.clear()
            for item in list(self._live.values()):
                self._release_locked(item)

    def task_done(self):
        pass

    def qsize(self):
        with self._condition:
            return sum(len(items) for items in self._groups.values())

    def status(self):
        from services import result_capacity_service as capacity
        with self._condition:
            states = Counter(item.state for item in self._live.values())
            return {"mode": "fair", "epoch": self.epoch, "limits": vars(self.limits),
                    "used": dict(self._used), "states": dict(states),
                    "claim_reserved": states["reserved"],
                    "claimed_not_admitted": states["ready"] + states["prepare"],
                    "live": len(self._live), "credits": dict(self._stats),
                    "blocked": dict(self._blocked), "failure": self._failure,
                    "closed": self._closed,
                    "groups": [{"campaign_type": key[0], "campaign_id": key[1], "route_id": key[2],
                                **dict(value)} for key, value in self._group_used.items()],
                    "oldest_live_seconds": max((time.monotonic() - item.created
                                                for item in self._live.values()), default=0),
                    "result_budget": "conservative_admission_until_buffer_drain",
                    "result_pipeline_hard_bytes": (
                        "VERIFIED" if capacity.enabled() else "NOT_VERIFIED"
                    ),
                    "socket_fairness": "PARTS_DRR_ADMISSION",
                    "socket_drr": {
                        "unit": "submit_sm_parts",
                        "quantum_parts": self._drr_quantum_parts,
                        "max_deficit_parts": self._drr_max_deficit,
                        "served_parts": [
                            {"campaign_type": key[0], "campaign_id": key[1],
                             "route_id": key[2], "parts": int(value)}
                            for key, value in sorted(self._drr_served.items())
                        ],
                        "oldest_eligible_wait_seconds": max(
                            0.0, time.monotonic() - self._drr_oldest_eligible_at
                        ) if self._drr_oldest_eligible_at else 0.0,
                        "wait_samples": len(self._drr_wait_seconds),
                        "wait_p50_seconds": _percentile(self._drr_wait_seconds, 0.50),
                        "wait_p95_seconds": _percentile(self._drr_wait_seconds, 0.95),
                        "wait_max_seconds": max(self._drr_wait_seconds, default=0.0),
                    }}
