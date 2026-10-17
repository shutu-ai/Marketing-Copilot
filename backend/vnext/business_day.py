"""Business-day and wall-clock helpers for the VNext data plane.

The sending pipeline must use one explicit wall-clock contract in every
process.  ``BusinessClock`` is deliberately small so tests can inject a
deterministic clock without changing the monotonic timers used for waits and
timeouts.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone, tzinfo
import json
import os
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "UTC"


def configured_timezone_name(value: str | None = None) -> str:
    """Return the configured business timezone name.

    ``MC_VNEXT_BUSINESS_TIMEZONE`` is preferred.  ``MC_BUSINESS_TIMEZONE`` is
    accepted for compatibility with deployment environments.  UTC is the
    safe deterministic default when neither is set; callers never infer a
    user's local timezone.
    """

    selected = value
    if selected is None:
        selected = os.environ.get("MC_VNEXT_BUSINESS_TIMEZONE")
    if selected is None:
        selected = os.environ.get("MC_BUSINESS_TIMEZONE")
    selected = str(selected or DEFAULT_TIMEZONE).strip()
    return selected or DEFAULT_TIMEZONE


def business_timezone(value: str | tzinfo | None = None) -> tuple[str, tzinfo]:
    """Resolve a timezone and return ``(stable_name, tzinfo)``."""

    if isinstance(value, tzinfo):
        return str(getattr(value, "key", "custom")), value
    name = configured_timezone_name(value)
    if name.upper() in {"UTC", "Z"}:
        return "UTC", timezone.utc
    if name.lower() in {"local", "system"}:
        local = datetime.now().astimezone().tzinfo or timezone.utc
        return "local", local
    try:
        return name, ZoneInfo(name)
    except ZoneInfoNotFoundError:
        # A bad deployment setting must be deterministic and visible rather
        # than silently making different processes choose different dates.
        return DEFAULT_TIMEZONE, timezone.utc


def _coerce_datetime(value, tz: tzinfo) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).astimezone(tz)
    if isinstance(value, datetime):
        current = value
        if current.tzinfo is None:
            current = current.replace(tzinfo=tz)
        return current.astimezone(tz)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=tz)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc).astimezone(tz)
    raise TypeError(f"unsupported clock value: {type(value).__name__}")


def normalize_business_day(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid business_day: {value!r}") from exc


def cutoff_timestamp(day: str | date, tz: tzinfo) -> float:
    """Return the first instant after the supplied business day."""

    current = date.fromisoformat(normalize_business_day(day))
    next_day = current + timedelta(days=1)
    return datetime.combine(next_day, time.min, tzinfo=tz).timestamp()


class BusinessClock:
    """Injectable wall clock used only for business-day decisions."""

    def __init__(self, wall_time: Callable[[], object] | object | None = None,
                 timezone_name: str | tzinfo | None = None):
        self.timezone_name, self.tz = business_timezone(timezone_name)
        if wall_time is not None and hasattr(wall_time, "now") and not callable(wall_time):
            self._wall_time = lambda: wall_time.now()
        else:
            self._wall_time = wall_time

    def now(self) -> datetime:
        source = self._wall_time
        value = source() if callable(source) else source
        return _coerce_datetime(value, self.tz)

    def timestamp(self) -> float:
        return self.now().timestamp()

    def business_day(self) -> str:
        return self.now().date().isoformat()

    def cutoff(self, day: str | date | None = None) -> float:
        return cutoff_timestamp(day or self.business_day(), self.tz)


class FileWallClock:
    """Read an injected wall-clock value from an atomically replaced JSON file.

    This is intentionally a small, opt-in test boundary.  Production callers
    continue to use the system wall clock; acceptance tools use the file to
    move several spawned VNext processes across a business-day boundary
    without changing the host clock.
    """

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path).resolve()

    def now(self):
        value = json.loads(self.path.read_text(encoding="utf-8"))
        selected = value.get("now", value.get("timestamp")) if isinstance(value, dict) else value
        if selected is None:
            raise ValueError(f"clock file has no now/timestamp: {self.path}")
        if isinstance(selected, str):
            return datetime.fromisoformat(selected.replace("Z", "+00:00"))
        return selected


def ensure_clock(clock=None, timezone_name: str | tzinfo | None = None) -> BusinessClock:
    if isinstance(clock, BusinessClock) and timezone_name is None:
        return clock
    if clock is None:
        return BusinessClock(timezone_name=timezone_name)
    if hasattr(clock, "now") and not callable(clock):
        return BusinessClock(lambda: clock.now(), timezone_name=timezone_name)
    return BusinessClock(clock, timezone_name=timezone_name)


__all__ = [
    "BusinessClock",
    "FileWallClock",
    "DEFAULT_TIMEZONE",
    "business_timezone",
    "configured_timezone_name",
    "cutoff_timestamp",
    "ensure_clock",
    "normalize_business_day",
]
