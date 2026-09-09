"""Error classification and bounded retry policy for realtime ingestion."""

import os
from dataclasses import dataclass


MAX_RETRIES = max(0, int(os.environ.get("MC_REALTIME_MAX_RETRIES", "4")))
RETRY_DELAYS = tuple(int(value) for value in os.environ.get("MC_REALTIME_RETRY_DELAYS", "30,120,600,1800").split(",") if value.strip())
WAITING_DELAYS = tuple(int(value) for value in os.environ.get("MC_REALTIME_WAITING_RETRY_DELAYS", "60,300,900,1800").split(",") if value.strip())


@dataclass(frozen=True)
class ErrorDecision:
    error_class: str
    retryable: bool
    delay_seconds: int
    quarantine: bool = False


def classify(error, status: str = "") -> ErrorDecision:
    message = str(error or "")
    lower = message.lower()
    if status == "waiting_for_whitelist" or "whitelist" in lower and ("building" in lower or "ready" in lower or "uploaded" in lower):
        return ErrorDecision("WAITING_DEPENDENCY", True, WAITING_DELAYS[0] if WAITING_DELAYS else 300)
    if "backpressure" in lower or "disk" in lower and ("space" in lower or "limit" in lower):
        return ErrorDecision("RESOURCE_LIMIT", True, RETRY_DELAYS[0] if RETRY_DELAYS else 30)
    if "whitelist index" in lower and any(token in lower for token in ("cannot read", "invalid", "corrupt", "failed")):
        return ErrorDecision("WHITELIST_INDEX", False, 0)
    if isinstance(error, (FileNotFoundError, PermissionError, TimeoutError)) or "temporar" in lower or "i/o" in lower:
        return ErrorDecision("TRANSIENT_IO", True, RETRY_DELAYS[0] if RETRY_DELAYS else 30)
    if any(token in lower for token in ("lock", "database is locked", "duckdb", "checkpoint")):
        return ErrorDecision("TRANSIENT_DB", True, RETRY_DELAYS[0] if RETRY_DELAYS else 30)
    if any(token in lower for token in ("campaign not found", "campaign is not enabled", "no enabled", "template", "smpp")):
        return ErrorDecision("INVALID_CAMPAIGN", False, 0)
    if any(token in lower for token in ("parsererror", "tokenizing", "cannot read file", "unsupported file", "columns")):
        return ErrorDecision("INVALID_INPUT", False, 0, quarantine=True)
    return ErrorDecision("UNKNOWN", True, RETRY_DELAYS[0] if RETRY_DELAYS else 30)


def delay_for(decision: ErrorDecision, retry_count: int) -> int:
    delays = WAITING_DELAYS if decision.error_class == "WAITING_DEPENDENCY" else RETRY_DELAYS
    if not delays:
        return 60
    return delays[min(max(0, retry_count), len(delays) - 1)]
