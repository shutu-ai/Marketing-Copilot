"""Independent stage credits, hysteresis and progress watchdog."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CreditReservation:
    reservation_id: int
    items: int
    bytes: int
    created_at: float


class StageCredit:
    """A bounded stage budget with high/low-water hysteresis."""

    def __init__(self, stage: str, *, limit_items: int, limit_bytes: int,
                 high_water: float = 0.9, low_water: float = 0.6,
                 max_age: float = 300.0):
        self.stage = str(stage)
        self.limit_items = max(1, int(limit_items))
        self.limit_bytes = max(1, int(limit_bytes))
        self.high_water = min(1.0, max(0.01, float(high_water)))
        self.low_water = min(self.high_water, max(0.0, float(low_water)))
        self.max_age = max(0.1, float(max_age))
        self._condition = asyncio.Condition()
        self._thread_lock = threading.Lock()
        self._next_id = 1
        self._reservations: dict[int, CreditReservation] = {}
        self.used_items = 0
        self.used_bytes = 0
        self.inflight_items = 0
        self.blocked_reason = ""
        self.progress_seq = 0
        self.last_progress_at = time.monotonic()
        self.resumed_at = 0.0
        self.resume_count = 0

    def _ratio(self, items: int, bytes_count: int) -> float:
        return max(items / self.limit_items, bytes_count / self.limit_bytes)

    def _is_high(self) -> bool:
        return self._ratio(self.used_items, self.used_bytes) >= self.high_water

    def _is_low(self) -> bool:
        return self._ratio(self.used_items, self.used_bytes) <= self.low_water

    def _available(self, items: int, bytes_count: int) -> bool:
        return (
            self.used_items + int(items) <= self.limit_items
            and self.used_bytes + int(bytes_count) <= self.limit_bytes
            and not (self.blocked_reason and not self._is_low())
            and not self._is_high()
        )

    def try_reserve(self, items: int = 1, bytes_count: int = 0) -> CreditReservation | None:
        count = max(0, int(items))
        size = max(0, int(bytes_count))
        if count > self.limit_items or size > self.limit_bytes:
            self.blocked_reason = "reservation_exceeds_stage_limit"
            return None
        with self._thread_lock:
            if not self._available(count, size):
                self.blocked_reason = "high_water" if self._is_high() else (self.blocked_reason or "capacity")
                return None
            reservation = CreditReservation(self._next_id, count, size, time.monotonic())
            self._next_id += 1
            self._reservations[reservation.reservation_id] = reservation
            self.used_items += count
            self.used_bytes += size
            self.inflight_items += count
            return reservation

    async def reserve(self, items: int = 1, bytes_count: int = 0, *, timeout: float | None = None) -> CreditReservation:
        async def wait_for_capacity():
            while True:
                reservation = self.try_reserve(items, bytes_count)
                if reservation:
                    return reservation
                async with self._condition:
                    await self._condition.wait()
        if timeout is None:
            return await wait_for_capacity()
        return await asyncio.wait_for(wait_for_capacity(), timeout=max(0.0, float(timeout)))

    def release(self, reservation: CreditReservation, *, progress: bool = True) -> None:
        with self._thread_lock:
            current = self._reservations.pop(int(reservation.reservation_id), None)
            if not current:
                return
            self.used_items = max(0, self.used_items - current.items)
            self.used_bytes = max(0, self.used_bytes - current.bytes)
            self.inflight_items = max(0, self.inflight_items - current.items)
            if progress:
                self.progress_seq += 1
                self.last_progress_at = time.monotonic()
            if self.blocked_reason and self._is_low():
                self.blocked_reason = ""
                self.resumed_at = time.time()
                self.resume_count += 1
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._notify())

    async def _notify(self) -> None:
        async with self._condition:
            self._condition.notify_all()

    def mark_progress(self, count: int = 1) -> None:
        with self._thread_lock:
            self.progress_seq += max(1, int(count))
            self.last_progress_at = time.monotonic()

    def snapshot(self) -> dict:
        with self._thread_lock:
            oldest = min((item.created_at for item in self._reservations.values()), default=0.0)
            return {
                "stage": self.stage,
                "limit_items": self.limit_items,
                "limit_bytes": self.limit_bytes,
                "used": self.used_items,
                "used_bytes": self.used_bytes,
                "reserved": len(self._reservations),
                "inflight": self.inflight_items,
                "high_water": self.high_water,
                "low_water": self.low_water,
                "oldest_age": max(0.0, time.monotonic() - oldest) if oldest else 0.0,
                "blocked_reason": self.blocked_reason,
                "progress_seq": self.progress_seq,
                "last_progress_at": self.last_progress_at,
                "resumed_at": self.resumed_at,
                "resume_count": self.resume_count,
            }


class ProgressWatchdog:
    """Detect only eligible stages that own resources but stop progressing."""

    def __init__(self, *, stall_seconds: float = 30.0):
        self.stall_seconds = max(0.1, float(stall_seconds))
        self._stages: dict[str, StageCredit] = {}

    def register(self, credit: StageCredit) -> None:
        self._stages[credit.stage] = credit

    def snapshot(self) -> dict:
        now = time.monotonic()
        stalled = []
        items = {}
        for name, credit in self._stages.items():
            state = credit.snapshot()
            eligible = state["inflight"] > 0 and not state["blocked_reason"]
            age = now - float(state["last_progress_at"])
            item = {**state, "eligible": eligible, "stalled": bool(eligible and age >= self.stall_seconds)}
            items[name] = item
            if item["stalled"]:
                stalled.append(name)
        return {"stalled": stalled, "stages": items}
