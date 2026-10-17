"""Optional low-overhead critical-path profiling for VNext acceptance runs.

The profiler is disabled unless a profile root is supplied.  When enabled it
keeps counters, timestamps, and bounded latency samples in process memory and
publishes one atomic JSON snapshot at most once per configured interval.  It
never writes a per-event trace and never changes durability or dispatch
semantics.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


class CriticalPathProfiler:
    """Process-local counters and bounded duration histograms."""

    def __init__(
        self,
        root: str | os.PathLike[str] | None,
        role: str,
        *,
        interval_seconds: float = 1.0,
        sample_limit: int = 4096,
    ) -> None:
        self.root = Path(root).resolve() if root else None
        self.role = "".join(
            character if character.isalnum() or character in "-_" else "_"
            for character in str(role or "process")
        ) or "process"
        self.pid = os.getpid()
        self.interval_seconds = max(0.1, float(interval_seconds))
        self.sample_limit = max(1, int(sample_limit))
        self.started_monotonic = time.monotonic()
        self._last_published = 0.0
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, int | float | str | bool | None] = {}
        self._timestamps: dict[str, float] = {}
        self._durations: dict[str, deque[int]] = {}

    @property
    def enabled(self) -> bool:
        return self.root is not None

    def inc(self, name: str, value: int = 1) -> None:
        if not self.enabled:
            return
        self._counters[str(name)] += int(value)

    def gauge(self, name: str, value: Any) -> None:
        if not self.enabled:
            return
        self._gauges[str(name)] = value

    def mark(self, name: str, at_monotonic: float | None = None) -> None:
        if not self.enabled:
            return
        self._timestamps[str(name)] = float(
            time.monotonic() if at_monotonic is None else at_monotonic
        )

    def observe_ns(self, name: str, duration_ns: int) -> None:
        if not self.enabled:
            return
        key = str(name)
        values = self._durations.setdefault(key, deque(maxlen=self.sample_limit))
        values.append(max(0, int(duration_ns)))
        self._counters[f"{key}_count"] += 1
        self._counters[f"{key}_total_ns"] += max(0, int(duration_ns))

    @staticmethod
    def _percentile(values: list[int], percentile: float) -> int:
        if not values:
            return 0
        ordered = sorted(values)
        index = (len(ordered) - 1) * percentile
        lower = int(index)
        upper = min(len(ordered) - 1, lower + 1)
        fraction = index - lower
        return int(round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction))

    def snapshot(self) -> dict[str, Any]:
        durations: dict[str, dict[str, int | float]] = {}
        for name, values in self._durations.items():
            ordered = list(values)
            durations[name] = {
                "count": len(ordered),
                "sample_count": len(ordered),
                "p50_ms": round(self._percentile(ordered, 0.50) / 1_000_000, 3),
                "p95_ms": round(self._percentile(ordered, 0.95) / 1_000_000, 3),
                "p99_ms": round(self._percentile(ordered, 0.99) / 1_000_000, 3),
                "max_ms": round(max(ordered, default=0) / 1_000_000, 3),
                "total_ns": int(sum(ordered)),
            }
        return {
            "pid": self.pid,
            "ppid": os.getppid(),
            "role": self.role,
            "enabled": self.enabled,
            "interval_seconds": self.interval_seconds,
            "started_monotonic": self.started_monotonic,
            "updated_monotonic": time.monotonic(),
            "counters": dict(sorted(self._counters.items())),
            "gauges": dict(sorted(self._gauges.items())),
            "timestamps": dict(sorted(self._timestamps.items())),
            "durations": durations,
        }

    def publish(self, *, force: bool = False) -> None:
        if not self.enabled or self.root is None:
            return
        now = time.monotonic()
        if not force and now - self._last_published < self.interval_seconds:
            return
        self._last_published = now
        directory = self.root / "profiles"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{self.role}-{self.pid}.json"
        temporary: Path | None = None
        try:
            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{path.name}.", dir=str(directory)
            )
            temporary = Path(temporary_name)
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(self.snapshot(), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
            os.replace(temporary, path)
            temporary = None
        except OSError:
            # Profiling must never affect the application path.
            pass
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass

    def publish_if_due(self) -> None:
        self.publish(force=False)
