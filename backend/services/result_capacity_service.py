"""Fair result capacity: one credit from pre-claim through terminal fsync.

Only short memory operations run under the ledger lock. Bytes are charged as
64 * framed UTF-8 bytes + 4096 per event (payload object/containers, encoded
copy, WriteItem, queue/heap references and bounded encoding scratch). Actual
deep object size is checked too. Socket reservations cover the complete worst
case, not a moving queue-depth sample. Legacy does not use this ledger.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import os
import threading
import uuid


class ResultCapacityError(RuntimeError):
    pass


def enabled():
    from services import bounded_dispatch_service
    return bounded_dispatch_service.mode() == "fair"


def charge(size):
    return int(size) * 64 + 4096


MAX_EVENT = 256 * 1024
MAX_PART_EVENT = 4096
MAX_FINAL_EVENT = 128 * 1024
MAX_NODES = 16384
MAX_STRING = 64 * 1024
MAX_JSON_BYTES = 256 * 1024


@dataclass
class Credit:
    amount: int
    owner: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    epoch: str = ""
    stage: str = "incoming"
    parent: object = None
    used: int = 0
    closed: bool = False


class Ledger:
    def __init__(self, limit=None, emergency=None):
        self.limit = int(limit or os.environ.get("MC_RESULT_LIVE_BYTES", 256 * 1024 * 1024))
        self.emergency = int(emergency or os.environ.get("MC_RESULT_EMERGENCY_BYTES", 32 * 1024 * 1024))
        if min(self.limit, self.emergency) < charge(MAX_EVENT):
            raise ValueError("result budgets must each accommodate one maximum event")
        self.epoch = uuid.uuid4().hex
        self.terminal_limit = 2 * charge(MAX_EVENT)
        self.lock = threading.RLock()
        self.live = {}
        self.used = Counter()
        self.peak = Counter()
        self.stats = Counter()
        self.failure = ""
        self.recovering = False
        self.history = []  # bounded audit ring; contains no payload or identifiers

    def _audit(self):
        self.peak.update({})
        for k, v in self.used.items():
            self.peak[k] = max(self.peak[k], v)
        self.history.append(dict(self.used))
        if len(self.history) > 256:
            del self.history[:128]
        assert self.used["normal"] <= self.limit
        assert self.used["emergency"] <= self.emergency
        assert self.used["terminal"] <= self.terminal_limit

    def acquire(self, amount, owner="emergency"):
        bucket = "normal" if owner == "dispatch" else ("terminal" if owner == "terminal" else "emergency")
        with self.lock:
            ceiling = {"normal": self.limit, "emergency": self.emergency, "terminal": self.terminal_limit}[bucket]
            if self.used[bucket] + amount > ceiling:
                self.stats["denied"] += 1
                return None
            credit = Credit(amount, owner, epoch=self.epoch)
            self.live[credit.id] = credit
            self.used[bucket] += amount
            self.stats["acquired"] += 1
            self._audit()
            return credit

    def event(self, size, parent=None):
        amount = charge(size)
        if parent is None:
            return self.acquire(amount)
        with self.lock:
            if (self.live.get(parent.id) is not parent or parent.closed
                    or parent.used + amount > parent.amount):
                self.failure = "reserved result envelope exceeded"
                raise ResultCapacityError(self.failure)
            credit = Credit(amount, "event", epoch=self.epoch, parent=parent)
            self.live[credit.id] = credit
            parent.used += amount
            self.stats["acquired"] += 1
            self._audit()
            return credit

    def transfer(self, credit, stage):
        if credit is None:
            return
        with self.lock:
            if self.live.get(credit.id) is credit:
                credit.stage = stage

    def resize(self, credit, size):
        amount = charge(size)
        with self.lock:
            if self.live.get(credit.id) is not credit or amount > credit.amount:
                raise ResultCapacityError("event exceeded encoding credit")
            delta = credit.amount - amount
            credit.amount = amount
            if credit.parent:
                credit.parent.used -= delta
            else:
                bucket = "terminal" if credit.owner == "terminal" else "emergency"
                self.used[bucket] -= delta
            self._audit()

    def release(self, credit):
        if credit is None:
            return
        with self.lock:
            if self.live.get(credit.id) is not credit:
                return
            if credit.owner == "dispatch" and credit.used:
                credit.closed = True
                return
            del self.live[credit.id]
            self.stats["released"] += 1
            if credit.parent:
                credit.parent.used -= credit.amount
                if credit.parent.closed and not credit.parent.used:
                    self.release(credit.parent)
            else:
                bucket = "normal" if credit.owner == "dispatch" else ("terminal" if credit.owner == "terminal" else "emergency")
                self.used[bucket] -= credit.amount
            self._audit()

    def fail(self, reason):
        with self.lock:
            self.failure = str(reason)[:500]

    def safe(self):
        with self.lock:
            return not (self.failure or self.recovering or self.used["emergency"])

    def status(self):
        with self.lock:
            stages = Counter()
            for credit in self.live.values():
                stages[credit.stage] += credit.amount
            return {"epoch": self.epoch, "limit_bytes": self.limit,
                    "emergency_limit_bytes": self.emergency, "used_bytes": dict(self.used),
                    "peak_bytes": dict(self.peak), "stages": dict(stages),
                    "acquired": self.stats["acquired"], "released": self.stats["released"],
                    "live": len(self.live), "failure": self.failure,
                    "recovering": self.recovering, "max_event_bytes": MAX_EVENT,
                    "accounting": "64*framed_utf8+4096", "samples": list(self.history)}


ledger = Ledger()
_owners = {}
_owners_lock = threading.Lock()


def reserve(task, parts):
    # Per-part response plus one final outcome/failure; a second final covers
    # conservative failure handoff. Intents already use durable DB ownership.
    amount = parts * charge(MAX_PART_EVENT) + 2 * charge(MAX_FINAL_EVENT)
    credit = ledger.acquire(amount, "dispatch")
    if credit:
        with _owners_lock:
            _owners[int(task["id"])] = credit
    return credit


def release_owner(task_id, credit):
    with _owners_lock:
        if _owners.get(int(task_id)) is credit:
            del _owners[int(task_id)]
    ledger.release(credit)


def owner_for(payload):
    value = payload.get("task") or payload
    if "items" in value:
        values = value["items"]
        value = values[0] if len(values) == 1 else {}
    task_id = int(value.get("task_id") or value.get("id") or 0)
    with _owners_lock:
        credit = _owners.get(task_id)
        claim = str(value.get("claim_token") or "")
        return credit if credit and claim and claim == getattr(credit, "claim", "") else None


def validate(value):
    """Bound JSON traversal/encoding scratch before materializing a record."""
    stack = [(value, 0)]
    nodes = 0
    estimated = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_NODES or depth > 16:
            raise ResultCapacityError("result JSON node/depth envelope exceeded")
        if isinstance(item, str):
            if len(item) > MAX_STRING or len(item.encode("utf-8")) > MAX_STRING:
                raise ResultCapacityError("result JSON string envelope exceeded")
            estimated += len(item.encode("utf-8")) * 6 + 2
        elif isinstance(item, dict):
            if len(item) > MAX_NODES:
                raise ResultCapacityError("result JSON map envelope exceeded")
            estimated += 2 + len(item) * 2
            stack.extend((v, depth + 1) for pair in item.items() for v in pair)
        elif isinstance(item, (list, tuple)):
            if len(item) > MAX_NODES:
                raise ResultCapacityError("result JSON array envelope exceeded")
            estimated += 2 + len(item)
            stack.extend((v, depth + 1) for v in item)
        elif item is not None and not isinstance(item, (bool, int, float)):
            raise ResultCapacityError("non-JSON result payload")
        else:
            estimated += 32
        if estimated > MAX_JSON_BYTES:
            raise ResultCapacityError("result JSON encoded envelope exceeded")
