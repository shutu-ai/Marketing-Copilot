"""Bounded in-memory VNext counters; persistence belongs to ProjectionWriter."""

from __future__ import annotations

import threading
import time
from collections import Counter, deque


class VNextMetrics:
    def __init__(self, *, sample_limit: int = 4096):
        self._lock = threading.Lock()
        self._counters = Counter()
        self._samples = deque(maxlen=max(1, int(sample_limit)))
        self._started_at = time.time()

    def inc(self, key: str, value: int = 1) -> None:
        with self._lock:
            self._counters[str(key)] += int(value)

    def record(self, event: dict) -> None:
        with self._lock:
            self._samples.append(dict(event))
            for key in (
                "queued", "taken", "socket_written", "write_failed", "unknown",
                "cancelled", "expired", "accepted", "response_success", "response_error",
            ):
                if event.get(key):
                    self._counters[key] += int(event[key])

    def snapshot(self, *, include_samples: bool = True) -> dict:
        with self._lock:
            value = {
                "started_at": self._started_at,
                "counters": dict(self._counters),
                "sample_count": len(self._samples),
            }
            if include_samples:
                value["samples"] = list(self._samples)
            return value
