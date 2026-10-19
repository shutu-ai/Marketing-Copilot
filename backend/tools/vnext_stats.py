#!/usr/bin/env python3
"""Read-only VNext statistics for SSH and plain terminals.

The tool only reads bounded, already-prepared snapshots:

* ``progress/worker-*.status.json`` (worker heartbeat)
* ``progress/worker-*.progress-aggregate.json`` (durable campaign counters)
* ``projection/projection-writer-status.json``
* ``spool/manifest.json`` plus cursor/taken JSON files
* ``control-state.json`` and process metadata

It never opens DuckDB, scans journal segments, calls ``VNextRuntime.status()``
or writes anything under the runtime root.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path


SCHEMA_VERSION = 1
DEFAULT_INTERVAL = 2.0
DEFAULT_RATE_WINDOW = 5.0
MAX_CAMPAIGN_ROWS = 30


def _read_json(path: Path, warnings: list[str], label: str):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError) as exc:
        warnings.append(f"{label}: {type(exc).__name__}: {exc}")
        return None


def _pid_alive(pid: int) -> bool:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.OpenProcess.argtypes = (
                wintypes.DWORD, wintypes.BOOL, wintypes.DWORD,
            )
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return False
            try:
                code = wintypes.DWORD()
                if kernel32.GetExitCodeProcess(
                    handle, ctypes.byref(code)
                ):
                    return int(code.value) == 259  # STILL_ACTIVE
                return True
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_processes(root: Path, warnings: list[str]) -> list[dict]:
    directory = root / "diagnostics" / "processes"
    values = []
    for path in sorted(directory.glob("*.json")):
        value = _read_json(path, warnings, f"process metadata {path.name}")
        if isinstance(value, dict):
            value.setdefault("role", path.stem.rsplit("-", 1)[0])
            value["_path"] = str(path)
            values.append(value)
    return values


def _runtime_metadata(processes: list[dict]) -> dict | None:
    candidates = [
        item for item in processes
        if str(item.get("role") or "") == "runtime-parent"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: float(item.get("updated_at", 0) or 0))


def _runtime_state(processes: list[dict]) -> tuple[str, dict | None]:
    metadata = _runtime_metadata(processes)
    if metadata is None:
        return "NOT_STARTED", None
    phase = str(metadata.get("phase") or "").lower()
    alive = _pid_alive(int(metadata.get("pid", 0) or 0))
    if alive and phase not in {"stopped", "error", "exception", "control_closed"}:
        return "RUNNING", metadata
    if phase in {"stopped", "error", "exception", "control_closed"} or not alive:
        return "STOPPED", metadata
    return "RUNNING", metadata


def _read_spool(root: Path, warnings: list[str]) -> dict:
    manifest_path = root / "spool" / "manifest.json"
    manifest = _read_json(manifest_path, warnings, "spool manifest")
    if not isinstance(manifest, dict):
        return {
            "available": False,
            "pending": None,
            "total_records": None,
            "total_bytes": None,
            "business_day": None,
            "snapshot_age_seconds": None,
        }
    try:
        partition_count = int(manifest.get("virtual_partition_count", 0) or 0)
    except (TypeError, ValueError):
        partition_count = 0
    cursor_base = _read_json(
        root / "spool" / "cursors.json", warnings, "spool cursors"
    ) or {}
    taken_base = _read_json(
        root / "spool" / "taken.json", warnings, "spool taken"
    ) or {}
    current_day = str(manifest.get("current_business_day") or "")
    pending_by_partition = {}
    for partition in range(partition_count):
        key = str(partition)
        cursor_value = 0
        cursor_path = root / "spool" / "cursors" / f"partition-{partition:04d}.json"
        if cursor_path.is_file():
            item = _read_json(cursor_path, warnings, f"spool cursor {partition}")
            if isinstance(item, dict):
                cursor_value = int(item.get("cursor", 0) or 0)
        elif isinstance(cursor_base, dict):
            cursor_value = int(cursor_base.get(key, 0) or 0)
        taken: set[int] = set()
        taken_path = root / "spool" / "taken" / f"partition-{partition:04d}.json"
        if taken_path.is_file():
            item = _read_json(taken_path, warnings, f"spool taken {partition}")
            if isinstance(item, dict):
                taken = {int(value) for value in item.get("taken", [])}
        elif isinstance(taken_base, dict):
            taken = {int(value) for value in taken_base.get(key, [])}
        pending = 0
        for entry in manifest.get("segments", {}).get(key, []):
            if str(entry.get("business_day") or "") != current_day:
                continue
            records = int(entry.get("records", 0) or 0)
            first = int(entry.get("first_ordinal", 0) or 0)
            last = int(entry.get("last_ordinal", first + records - 1) or 0)
            consumed = max(0, min(last, cursor_value) - first + 1)
            taken_in_segment = sum(
                1 for ordinal in taken if first <= int(ordinal) <= last
            )
            pending += max(0, records - consumed - taken_in_segment)
        pending_by_partition[key] = pending
    try:
        updated_at = float(manifest_path.stat().st_mtime)
    except OSError:
        updated_at = None
    return {
        "available": True,
        "pending": sum(pending_by_partition.values()),
        "pending_by_partition": {
            key: value for key, value in pending_by_partition.items() if value
        },
        "total_records": int(manifest.get("total_records", 0) or 0),
        "total_bytes": int(manifest.get("total_bytes", 0) or 0),
        "business_day": current_day,
        "business_epoch": int(manifest.get("business_epoch", 1) or 1),
        "updated_at": updated_at,
        "virtual_partition_count": partition_count,
        "source": "manifest+cursor-files",
    }


def _read_worker_statuses(root: Path, warnings: list[str]) -> list[dict]:
    values = []
    directory = root / "progress"
    for path in sorted(directory.glob("worker-*.status.json")):
        value = _read_json(path, warnings, f"worker status {path.name}")
        if not isinstance(value, dict):
            continue
        value["_path"] = str(path)
        try:
            value["_age_seconds"] = max(
                0.0, time.time() - float(value.get("updated_at", 0) or 0)
            )
        except (TypeError, ValueError):
            value["_age_seconds"] = None
        values.append(value)
    values.sort(key=lambda item: int(item.get("worker_id", 0) or 0))
    return values


def _read_aggregates(root: Path, warnings: list[str]) -> list[dict]:
    values = []
    for path in sorted((root / "progress").glob("*.progress-aggregate.json")):
        value = _read_json(path, warnings, f"campaign aggregate {path.name}")
        if isinstance(value, dict):
            value["_path"] = str(path)
            values.append(value)
    return values


def _read_failures(root: Path, warnings: list[str]) -> list[dict]:
    values = []
    for path in sorted((root / "progress").glob("*.failure.json")):
        value = _read_json(path, warnings, f"progress failure {path.name}")
        if isinstance(value, dict):
            value["_path"] = str(path)
            values.append(value)
    return values


def _read_projection(root: Path, warnings: list[str]) -> dict:
    path = root / "projection" / "projection-writer-status.json"
    value = _read_json(path, warnings, "projection status")
    if not isinstance(value, dict):
        return {"available": False}
    try:
        age = max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        age = None
    value["available"] = True
    value["snapshot_age_seconds"] = age
    value["_path"] = str(path)
    return value


def _read_controls(root: Path, warnings: list[str]) -> dict[tuple[str, int], str]:
    value = _read_json(
        root / "control-state.json", warnings, "control state"
    )
    controls = {}
    if not isinstance(value, dict):
        return controls
    for item in value.get("campaigns", []):
        try:
            controls[
                (str(item["campaign_type"]), int(item["campaign_id"]))
            ] = str(item.get("state") or "")
        except (KeyError, TypeError, ValueError):
            continue
    return controls


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum_dicts(values: list[dict], keys: tuple[str, ...]) -> dict:
    result = {key: 0 for key in keys}
    for value in values:
        for key in keys:
            result[key] += _int(value.get(key, 0))
    return result


def _campaign_key(item: dict) -> tuple[str, int]:
    return (str(item.get("campaign_type") or ""), _int(item.get("campaign_id", 0)))


def _merge_campaigns(
    aggregates: list[dict],
    workers: list[dict],
) -> tuple[dict[tuple[str, int], dict], str, bool]:
    merged: dict[tuple[str, int], dict] = {}
    source = ""
    complete = True
    campaign_keys = (
        "accepted_tasks", "tasks_taken", "socket_written_events",
        "socket_written_pdus", "write_failed", "unknown", "cancelled",
        "expired", "response_success", "response_error", "durable_events",
    )
    for value in aggregates:
        source = "durable-aggregate"
        complete = complete and bool(value.get("complete", True))
        for campaign in value.get("campaigns", []):
            key = _campaign_key(campaign)
            target = merged.setdefault(
                key, {name: 0 for name in campaign_keys}
            )
            for name in campaign_keys:
                target[name] += _int(campaign.get(name, 0))
    if not merged:
        for worker in workers:
            snapshot = worker.get("snapshot") or {}
            campaigns = snapshot.get("campaign_counters") or []
            if campaigns:
                source = source or "worker-memory"
                complete = False
            for campaign in campaigns:
                key = _campaign_key(campaign)
                target = merged.setdefault(
                    key, {name: 0 for name in campaign_keys}
                )
                for name in campaign_keys:
                    target[name] += _int(campaign.get(name, 0))
    # Live pending response counts always come from the worker snapshot; they
    # are not part of the durable aggregate until fsync completes.
    for worker in workers:
        snapshot = worker.get("snapshot") or {}
        for campaign in snapshot.get("campaign_counters") or []:
            key = _campaign_key(campaign)
            target = merged.setdefault(
                key, {name: 0 for name in campaign_keys}
            )
            pending = campaign.get("pending") or {}
            target["_pending_response_success"] = (
                target.get("_pending_response_success", 0)
                + _int(pending.get("response_success", 0))
            )
            target["_pending_response_error"] = (
                target.get("_pending_response_error", 0)
                + _int(pending.get("response_error", 0))
            )
            target["_pending_unknown"] = (
                target.get("_pending_unknown", 0)
                + _int(pending.get("unknown", 0))
            )
    return merged, source or "unavailable", complete


class _RateTracker:
    def __init__(self, window_seconds: float = DEFAULT_RATE_WINDOW):
        self.window_seconds = max(0.5, float(window_seconds))
        self.samples: deque[tuple[float, float]] = deque()
        self.counter_reset = False
        self.identity = None
        self.last_reset_reason = ""

    def update(self, value: float | None, now: float, *, identity=None) -> float | None:
        if value is None:
            return None
        if self.identity is not None and identity is not None and identity != self.identity:
            self.samples.clear()
            self.counter_reset = True
            self.last_reset_reason = "counter_identity_changed"
        if identity is not None:
            self.identity = identity
        if self.samples and value < self.samples[-1][1]:
            self.samples.clear()
            self.counter_reset = True
            self.last_reset_reason = "counter_decreased"
        self.samples.append((now, value))
        while (
            len(self.samples) > 2
            and now - self.samples[0][0] > self.window_seconds
        ):
            self.samples.popleft()
        if len(self.samples) < 2:
            return None
        first_at, first_value = self.samples[0]
        last_at, last_value = self.samples[-1]
        if last_at <= first_at:
            return None
        return max(0.0, (last_value - first_value) / (last_at - first_at))


def _attach_worker_counter_identities(
    workers: list[dict], processes: list[dict], run_id: str,
) -> None:
    """Attach a stable process-generation token to each worker snapshot.

    Worker status files are deliberately small and do not need to carry PID
    metadata on every heartbeat.  The diagnostics records already contain the
    child PID and run ID, so use the newest matching worker record when it is
    available.  The status-file fallback still detects ordinary counter
    decreases when diagnostics are unavailable.
    """

    latest_by_worker: dict[int, dict] = {}
    for item in processes:
        role = str(item.get("role") or "")
        if not role.startswith("worker-") or role.startswith("worker-supervisor-"):
            continue
        try:
            worker_id = int(role.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            continue
        previous = latest_by_worker.get(worker_id)
        if previous is None or float(item.get("updated_at", 0) or 0) >= float(
            previous.get("updated_at", 0) or 0
        ):
            latest_by_worker[worker_id] = item
    for worker in workers:
        worker_id = _int(worker.get("worker_id", -1), -1)
        metadata = latest_by_worker.get(worker_id) or {}
        pid = _int(metadata.get("pid", 0), 0)
        status_path = str(worker.get("_path") or "")
        worker["_counter_identity"] = (
            f"{run_id}|worker:{worker_id}|pid:{pid}|status:{status_path}"
        )


def _transport_counter(
    workers: list[dict],
    field: str,
    *,
    fallback_fields: tuple[str, ...] = (),
) -> tuple[float | None, object | None, list[str]]:
    """Return a bounded transport counter and its process identity."""

    if not workers:
        return None, None, ["live_worker_status"]
    values: list[tuple[str, float]] = []
    reasons: list[str] = []
    for worker in workers:
        worker_id = _int(worker.get("worker_id", -1), -1)
        transports = (worker.get("snapshot") or {}).get("transports")
        if not isinstance(transports, list) or not transports:
            reasons.append(f"{field}:worker_{worker_id}_snapshot_missing")
            continue
        total = 0.0
        complete = True
        for transport in transports:
            value = None
            if field in transport:
                value = _float_or_none(transport.get(field))
            elif fallback_fields and all(name in transport for name in fallback_fields):
                value = sum(
                    _float_or_none(transport.get(name)) or 0.0
                    for name in fallback_fields
                )
            if value is None:
                complete = False
                break
            total += value
        if complete:
            values.append((str(worker.get("_counter_identity") or worker_id), total))
        else:
            reasons.append(f"{field}:worker_{worker_id}_counter_missing")
    if reasons or len(values) != len(workers):
        return None, None, reasons or [f"{field}:incomplete"]
    identity = tuple(sorted(identity for identity, _ in values))
    return sum(value for _, value in values), identity, []


def _durable_socket_counter(
    progress_values: list[dict], aggregates: list[dict], run_id: str,
) -> tuple[float | None, object | None, list[str]]:
    """Read the cumulative socket-written PDU count after successful fsync."""

    if progress_values and all("durable_socket_written_pdus" in item for item in progress_values):
        return (
            sum(_float_or_none(item.get("durable_socket_written_pdus")) or 0.0
                for item in progress_values),
            (run_id, "journal-progress"),
            [],
        )
    if aggregates:
        total = 0.0
        for aggregate in aggregates:
            for campaign in aggregate.get("campaigns") or []:
                total += _float_or_none(campaign.get("socket_written_pdus")) or 0.0
        return total, (run_id, "journal-aggregate"), []
    return None, None, ["journal_durable_socket_counter"]


def _parse_campaign_filter(values: list[str]) -> list[tuple[str | None, int]]:
    filters = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if ":" in text:
            campaign_type, campaign_id = text.rsplit(":", 1)
            if not campaign_type:
                raise argparse.ArgumentTypeError(
                    f"invalid campaign filter: {value}"
                )
            try:
                filters.append((campaign_type, int(campaign_id)))
            except ValueError as exc:
                raise argparse.ArgumentTypeError(
                    f"invalid campaign filter: {value}"
                ) from exc
        else:
            try:
                filters.append((None, int(text)))
            except ValueError as exc:
                raise argparse.ArgumentTypeError(
                    f"invalid campaign filter: {value}"
                ) from exc
    return filters


def _build_snapshot(
    root: Path,
    *,
    rate_tracker: _RateTracker | None = None,
    rate_trackers: dict[str, _RateTracker] | None = None,
    interval: float,
) -> dict:
    observed_at = time.time()
    if rate_trackers is None:
        rate_trackers = {
            "journal_durable": rate_tracker or _RateTracker(DEFAULT_RATE_WINDOW),
        }
    else:
        rate_trackers = dict(rate_trackers)
    rate_trackers.setdefault("socket_write", _RateTracker(DEFAULT_RATE_WINDOW))
    rate_trackers.setdefault("journal_durable", _RateTracker(DEFAULT_RATE_WINDOW))
    rate_trackers.setdefault("response_observed", _RateTracker(DEFAULT_RATE_WINDOW))
    warnings: list[str] = []
    incomplete: list[str] = []
    processes = _read_processes(root, warnings)
    runtime_state, runtime_meta = _runtime_state(processes)
    run_id = str(
        (runtime_meta or {}).get("run_id")
        or (runtime_meta or {}).get("_path", "").rsplit(os.sep, 1)[-1]
        or root.name
    )
    workers = _read_worker_statuses(root, warnings)
    aggregates = _read_aggregates(root, warnings)
    failures = _read_failures(root, warnings)
    projection = _read_projection(root, warnings)
    spool = _read_spool(root, warnings)
    controls = _read_controls(root, warnings)
    _attach_worker_counter_identities(workers, processes, run_id)

    status_ages = [
        float(item.get("_age_seconds") or 0.0)
        for item in workers
        if item.get("_age_seconds") is not None
    ]
    snapshot_age = max(status_ages) if status_ages else None
    stale_threshold = max(5.0, float(interval) * 3.0)
    stale = snapshot_age is not None and snapshot_age > stale_threshold
    if projection.get("available"):
        projection_age = _float_or_none(
            projection.get("snapshot_age_seconds")
        )
        if projection_age is not None and projection_age > stale_threshold:
            stale = True

    worker_snapshots = [
        item.get("snapshot") or {} for item in workers
    ]
    progress_values = [
        snapshot.get("progress") or {} for snapshot in worker_snapshots
    ]
    transport_values = [
        transport
        for snapshot in worker_snapshots
        for transport in snapshot.get("transports", [])
    ]
    route_tps_limits: dict[str, float] = {}
    for index, transport in enumerate(transport_values):
        route_id = transport.get("route_id")
        key = f"route:{route_id}" if route_id is not None else f"transport:{index}"
        if key in route_tps_limits:
            # A route_shared limiter is represented once per physical bind in
            # worker snapshots, but the configured route budget is shared.
            continue
        configured = _float_or_none(transport.get("effective_total_limit_tps"))
        if configured is None:
            tps = _float_or_none(transport.get("tps")) or 0.0
            pool_size = max(1, _int(transport.get("pool_size", 1)))
            scope = str(transport.get("tps_scope") or "route_shared").strip().lower()
            configured = tps * pool_size if scope == "per_bind" else tps
        route_tps_limits[key] = float(configured)
    counters = _sum_dicts(
        [snapshot.get("counters") or {} for snapshot in worker_snapshots],
        (
            "queued", "taken", "socket_written", "write_failed", "unknown",
            "cancelled", "expired", "accepted", "response_success",
            "response_error",
        ),
    )
    progress_totals = _sum_dicts(
        progress_values,
        (
            "queue_depth", "items_bytes", "reservations_items",
            "reservations_bytes", "pending_durability",
            "pending_durability_bytes", "writer_batches",
            "writer_events_durable", "writer_fsync_total_ms",
            "writer_errors", "unresolved_events",
            "durable_response_success", "durable_response_error",
            "durable_unknown", "durable_socket_written_pdus",
            "durable_socket_written_events", "durable_accepted_tasks",
            "durable_tasks_taken", "durable_write_failed",
            "durable_expired", "durable_cancelled", "durable_events",
        ),
    )
    transport_totals = {
        "window_inflight": sum(
            _int(item.get("window_inflight", 0)) for item in transport_values
        ),
        "window_peak": max(
            [_int(item.get("window_inflight_peak", 0)) for item in transport_values]
            or [0]
        ),
        "binds_active": sum(
            _int(item.get("bind_active", 1 if item.get("connected") else 0))
            for item in transport_values
        ),
        "binds_known": len(transport_values),
        "socket_written_parts": sum(
            _int(item.get("socket_written_parts", 0))
            for item in transport_values
        ),
        "response_observed_success": sum(
            _int(item.get("response_observed_success", item.get("response_success", 0)))
            for item in transport_values
        ),
        "response_observed_error": sum(
            _int(item.get("response_observed_error", item.get("response_error", 0)))
            for item in transport_values
        ),
        "response_observed_unknown": sum(
            _int(item.get("response_observed_unknown", item.get("unknown_parts", 0)))
            for item in transport_values
        ),
        "callback_errors": sum(
            _int(item.get("response_callback_errors", 0))
            for item in transport_values
        ),
        "unexpected_responses": sum(
            _int(item.get("unexpected_response_pdus", 0))
            for item in transport_values
        ),
        "reconnects": sum(
            _int(item.get("reconnect_count", 0)) for item in transport_values
        ),
        "response_latency_p95_ms": max(
            [
                _float_or_none(item.get("response_latency_p95_ms")) or 0.0
                for item in transport_values
            ]
            or [0.0]
        ) or None,
        "tps_limit": sum(route_tps_limits.values()) or None,
    }
    socket_counter, socket_identity, socket_rate_reasons = _transport_counter(
        workers, "socket_written_parts"
    )
    response_counter, response_identity, response_rate_reasons = _transport_counter(
        workers,
        "response_observed_total",
        fallback_fields=(
            "response_observed_success",
            "response_observed_error",
            "response_observed_unknown",
        ),
    )
    durable_counter, durable_identity, durable_rate_reasons = _durable_socket_counter(
        progress_values, aggregates, run_id
    )
    campaign_map, campaign_source, campaign_complete = _merge_campaigns(
        aggregates, workers
    )
    aggregate_written_pdus = sum(
        _int(item.get("socket_written_pdus", 0))
        for item in campaign_map.values()
    )
    aggregate_success = sum(
        _int(item.get("response_success", 0))
        for item in campaign_map.values()
    )
    aggregate_error = sum(
        _int(item.get("response_error", 0))
        for item in campaign_map.values()
    )
    aggregate_unknown = sum(
        _int(item.get("unknown", 0))
        for item in campaign_map.values()
    )
    aggregate_events = sum(
        _int(item.get("durable_events", 0))
        for item in campaign_map.values()
    )
    aggregate_taken = sum(
        _int(item.get("tasks_taken", 0))
        for item in campaign_map.values()
    )
    aggregate_accepted = sum(
        _int(item.get("accepted_tasks", 0))
        for item in campaign_map.values()
    )
    aggregate_write_failed = sum(
        _int(item.get("write_failed", 0))
        for item in campaign_map.values()
    )
    aggregate_socket_events = sum(
        _int(item.get("socket_written_events", 0))
        for item in campaign_map.values()
    )
    configured_workers = _int(
        ((runtime_meta or {}).get("last_progress") or {}).get("worker_count", 0)
    ) or len(workers)
    workers_section = {
        "configured": configured_workers,
        "known": len(workers),
        "alive": len(workers),
        "active": sum(1 for item in workers if item.get("active")),
        "inflight": sum(_int(snapshot.get("inflight", 0)) for snapshot in worker_snapshots),
        "taken": max(counters["taken"], aggregate_taken),
        "accepted": max(counters["accepted"], aggregate_accepted),
        "socket_written_events": max(
            counters["socket_written"], aggregate_socket_events
        ),
        "write_failed": max(counters["write_failed"], aggregate_write_failed),
        "checkpoint_count": sum(
            _int(snapshot.get("checkpoint_count", 0))
            for snapshot in worker_snapshots
        ),
        "control_counters": {
            name: sum(
                _int((snapshot.get("control_counters") or {}).get(name, 0))
                for snapshot in worker_snapshots
            )
            for name in ("pause", "resume", "cancel")
        },
        "status_age_seconds": snapshot_age,
    }
    # route_pool_size is the physical route-wide pool repeated in every
    # worker snapshot, not a per-worker allocation.  Summing it inflated the
    # CLI value (for example, 5 physical binds appeared as 20 with 4 workers).
    configured_binds = max(
        (_int(snapshot.get("route_pool_size", 0)) for snapshot in worker_snapshots),
        default=0,
    )
    smpp_section = {
        "binds_active": transport_totals["binds_active"],
        "binds_configured": configured_binds or None,
        "window_inflight": transport_totals["window_inflight"],
        "window_peak": transport_totals["window_peak"],
        "written_pdus": max(
            progress_totals["durable_socket_written_pdus"],
            aggregate_written_pdus,
            transport_totals["socket_written_parts"],
        ),
        "written_source": (
            "durable-aggregate"
            if aggregate_written_pdus
            or progress_totals["durable_socket_written_pdus"]
            else "worker-memory"
        ),
        "reconnects": transport_totals["reconnects"],
        "tps_limit": transport_totals["tps_limit"],
        "response_latency_p95_ms": transport_totals["response_latency_p95_ms"],
    }
    pending_success = sum(
        _int(item.get("_pending_response_success", 0))
        for item in campaign_map.values()
    )
    pending_error = sum(
        _int(item.get("_pending_response_error", 0))
        for item in campaign_map.values()
    )
    pending_unknown = sum(
        _int(item.get("_pending_unknown", 0))
        for item in campaign_map.values()
    )
    responses_section = {
        "observed_success": (
            max(progress_totals["durable_response_success"], aggregate_success)
            + pending_success
        ),
        "observed_error": (
            max(progress_totals["durable_response_error"], aggregate_error)
            + pending_error
        ),
        "observed_unknown": (
            max(progress_totals["durable_unknown"], aggregate_unknown)
            + pending_unknown
        ),
        "durable_success": max(
            progress_totals["durable_response_success"], aggregate_success
        ),
        "durable_error": max(
            progress_totals["durable_response_error"], aggregate_error
        ),
        "durable_unknown": max(
            progress_totals["durable_unknown"], aggregate_unknown
        ),
        "pending_durable": progress_totals["pending_durability"],
        "observed_not_durable": pending_success + pending_error + pending_unknown,
        "callback_errors": transport_totals["callback_errors"],
        "unexpected": transport_totals["unexpected_responses"],
        "source": "durable-aggregate+worker-pending",
    }
    journal_section = {
        "durable_events": max(
            progress_totals["durable_events"], aggregate_events
        ),
        "backlog_bytes": (
            _int(projection.get("journal_backlog_bytes", 0))
            if projection.get("available") else None
        ),
        "oldest_undurable_ms": max(
            [
                _float_or_none(item.get("writer_oldest_age_ms")) or 0.0
                for item in progress_values
            ]
            or [0.0]
        ),
        "fsync_errors": progress_totals["writer_errors"],
        "unresolved_events": (
            progress_totals["unresolved_events"]
            + sum(_int(item.get("unresolved_events", 0)) for item in failures)
        ),
        "writer_batches": progress_totals["writer_batches"],
        "fsync_total_ms": progress_totals["writer_fsync_total_ms"],
        "callback_errors": transport_totals["callback_errors"],
        "unexpected_responses": transport_totals["unexpected_responses"],
        "source": "worker-progress-store",
    }
    projection_section = {
        "available": bool(projection.get("available")),
        "applied": _int(projection.get("applied", 0)) if projection.get("available") else None,
        "lag_bytes": (
            _int(projection.get("journal_backlog_bytes", 0))
            if projection.get("available") else None
        ),
        "pending_events": (
            _int(projection.get("journal_pending_events", 0))
            if projection.get("available") else None
        ),
        "queue": (
            _int(projection.get("queue_depth", 0))
            if projection.get("available") else None
        ),
        "caught_up": (
            bool(
                projection.get("available")
                and _int(projection.get("journal_backlog_bytes", 0)) == 0
                and _int(projection.get("journal_pending_events", 0)) == 0
            )
            if projection.get("available") else None
        ),
        "snapshot_age_seconds": projection.get("snapshot_age_seconds"),
    }
    campaign_rows = []
    for key, values in sorted(campaign_map.items()):
        campaign_type, campaign_id = key
        campaign_rows.append({
            "campaign_type": campaign_type,
            "campaign_id": campaign_id,
            "state": controls.get(key, "RUNNING"),
            "accepted_tasks": values["accepted_tasks"],
            "tasks_taken": values["tasks_taken"],
            "socket_written_pdus": values["socket_written_pdus"],
            "response_success": (
                values["response_success"]
                + values.get("_pending_response_success", 0)
            ),
            "response_error": (
                values["response_error"]
                + values.get("_pending_response_error", 0)
            ),
            "unknown": values["unknown"] + values.get("_pending_unknown", 0),
            "projected": None,
            "pending_durability": (
                values.get("_pending_response_success", 0)
                + values.get("_pending_response_error", 0)
                + values.get("_pending_unknown", 0)
            ),
        })
    for key, state in sorted(controls.items()):
        if key in campaign_map:
            continue
        campaign_rows.append({
            "campaign_type": key[0],
            "campaign_id": key[1],
            "state": state,
            "accepted_tasks": None,
            "tasks_taken": None,
            "socket_written_pdus": None,
            "response_success": None,
            "response_error": None,
            "unknown": None,
            "projected": None,
            "pending_durability": None,
        })
    if not workers:
        incomplete.append("live_worker_status")
    if not projection.get("available"):
        incomplete.append("projection_status")
    if campaign_source != "durable-aggregate":
        incomplete.append("campaign_durable_aggregate")
    incomplete.append("campaign_projected")
    if spool.get("pending") is None:
        incomplete.append("spool_pending")
    incomplete.append("ingress_rejected")
    if configured_binds == 0:
        incomplete.append("bind_capacity")
    if not progress_values:
        incomplete.append("response_durable")
    if transport_totals["callback_errors"]:
        incomplete.append("response_callback_errors")
    if transport_totals["unexpected_responses"]:
        incomplete.append("unexpected_responses")
    if campaign_source == "durable-aggregate" and not campaign_complete:
        incomplete.append("campaign_counter_replay")
    admission_reason = next(
        (
            str(item.get("blocked_reason") or "")
            for item in progress_values
            if item.get("blocked_reason")
        ),
        "",
    )
    rate_unavailable_reasons: list[str] = []
    if stale:
        rate_unavailable_reasons.append("stale_snapshot")
    rate_unavailable_reasons.extend(socket_rate_reasons)
    rate_unavailable_reasons.extend(durable_rate_reasons)
    rate_unavailable_reasons.extend(response_rate_reasons)
    rate_now = time.monotonic()
    if stale:
        socket_rate = None
        durable_rate = None
        response_rate = None
    else:
        socket_rate = rate_trackers["socket_write"].update(
            socket_counter, rate_now, identity=socket_identity
        )
        durable_rate = rate_trackers["journal_durable"].update(
            durable_counter, rate_now, identity=durable_identity
        )
        response_rate = rate_trackers["response_observed"].update(
            response_counter, rate_now, identity=response_identity
        )
    for counter, value, tracker in (
        (socket_counter, socket_rate, rate_trackers["socket_write"]),
        (durable_counter, durable_rate, rate_trackers["journal_durable"]),
        (response_counter, response_rate, rate_trackers["response_observed"]),
    ):
        if (
            counter is not None
            and value is None
            and not stale
            and not tracker.last_reset_reason
        ):
            rate_unavailable_reasons.append("first_sample")
    for name, value in (
        ("socket_write_rate", socket_rate),
        ("journal_durable_rate", durable_rate),
        ("response_observed_rate", response_rate),
    ):
        if value is None:
            incomplete.append(name)
    for tracker in rate_trackers.values():
        if tracker.last_reset_reason:
            rate_unavailable_reasons.append(tracker.last_reset_reason)
    rate_unavailable_reasons = sorted(set(rate_unavailable_reasons))
    counter_reset = any(
        tracker.counter_reset for tracker in rate_trackers.values()
    )
    smpp_section.update({
        "socket_written_observed_pdus": socket_counter,
        "durable_socket_written_pdus": durable_counter,
        "socket_write_rate_pdu_per_second": socket_rate,
        "journal_durable_rate_pdu_per_second": durable_rate,
        "response_observed_rate_pdu_per_second": response_rate,
    })
    if (
        journal_section["unresolved_events"]
        or journal_section["fsync_errors"]
        or journal_section["callback_errors"]
    ):
        health = "BLOCKED"
    elif runtime_state == "RUNNING" and stale:
        health = "STALE"
    elif runtime_state != "RUNNING":
        health = "OFFLINE"
    elif admission_reason:
        health = "BACKPRESSURE"
    elif journal_section["unexpected_responses"]:
        health = "DEGRADED"
    elif not workers:
        health = "DEGRADED"
    elif not projection.get("available"):
        health = "DEGRADED"
    else:
        health = "OK"
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at": observed_at,
        "observed_at_iso": datetime.fromtimestamp(
            observed_at, tz=datetime.now().astimezone().tzinfo
        ).isoformat(timespec="seconds"),
        "snapshot_age_seconds": snapshot_age,
        "run_id": run_id,
        "source": "worker-status+durable-aggregate+projection-status",
        "stale": stale,
        "incomplete_metrics": sorted(set(incomplete)),
        "warnings": warnings,
        "runtime": {
            "state": runtime_state,
            "started": runtime_state == "RUNNING",
        },
        "ingress": {
            "accepted": spool.get("total_records"),
            "rejected": None,
            "paused_by_backpressure": bool(admission_reason),
        },
        "spool": {
            "available": spool.get("available"),
            "pending": spool.get("pending"),
            "disk_mb": (
                round(_int(spool.get("total_bytes", 0)) / (1024 * 1024), 2)
                if spool.get("total_bytes") is not None else None
            ),
            "business_day": spool.get("business_day"),
            "business_epoch": spool.get("business_epoch"),
            "source": spool.get("source", "unavailable"),
        },
        "workers": workers_section,
        "smpp": smpp_section,
        "responses": responses_section,
        "journal": journal_section,
        "projection": projection_section,
        "campaigns": campaign_rows,
        "campaign_counter_source": campaign_source,
        "health": health,
        "backpressure": admission_reason or "NONE",
        "fsync_errors": journal_section["fsync_errors"],
        "unresolved_events": journal_section["unresolved_events"],
        # Compatibility alias: the historical rate was derived from the
        # durable socket-written counter, not an observed socket write.
        "rate_pdu_per_second": durable_rate,
        "rate_window_seconds": DEFAULT_RATE_WINDOW,
        "counter_reset": counter_reset,
        "rates": {
            "socket_write_rate_pdu_per_second": socket_rate,
            "journal_durable_rate_pdu_per_second": durable_rate,
            "response_observed_rate_pdu_per_second": response_rate,
            "window_seconds": DEFAULT_RATE_WINDOW,
            "counter_reset": counter_reset,
            "unavailable_reasons": rate_unavailable_reasons,
        },
        "socket_write_rate_pdu_per_second": socket_rate,
        "journal_durable_rate_pdu_per_second": durable_rate,
        "response_observed_rate_pdu_per_second": response_rate,
        "rate_unavailable_reasons": rate_unavailable_reasons,
    }


def _fmt(value, *, digits: int | None = None, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "YES" if value else "NO"
    if isinstance(value, float):
        if digits is None:
            digits = 1
        return f"{value:.{digits}f}{suffix}"
    return f"{value}{suffix}"


def _fmt_age(seconds) -> str:
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60.0:.1f}m"
    return f"{seconds / 3600.0:.1f}h"


def _campaign_label(item: dict) -> str:
    return f"{item.get('campaign_type') or '?'}:{item.get('campaign_id')} "


def _format_human(snapshot: dict) -> str:
    lines = []
    observed = snapshot["observed_at"]
    clock = datetime.fromtimestamp(observed).strftime("%H:%M:%S")
    age = snapshot.get("snapshot_age_seconds")
    state = snapshot["runtime"]["state"]
    lines.append(
        f"VNEXT | run={snapshot['run_id']} | {clock} | "
        f"UPDATED {_fmt_age(age)} ago | state={state}"
    )
    ingress = snapshot["ingress"]
    spool = snapshot["spool"]
    lines.append(
        f"Ingress      accepted={_fmt(ingress['accepted'])}    "
        f"rejected={_fmt(ingress['rejected'])}     "
        f"paused-by-backpressure={_fmt(ingress['paused_by_backpressure'])}"
    )
    lines.append(
        f"Spool        pending={_fmt(spool['pending'])}     "
        f"disk={_fmt(spool['disk_mb'], digits=1, suffix=' MB')}       "
        f"source={spool['source']}"
    )
    workers = snapshot["workers"]
    lines.append(
        f"Workers      active={workers['active']}/{workers['configured']}      "
        f"taken={_fmt(workers['taken'])}         inflight={_fmt(workers['inflight'])}"
    )
    smpp = snapshot["smpp"]
    lines.append(
        f"SMPP         binds={_fmt(smpp['binds_active'])}/{_fmt(smpp['binds_configured'])}       "
        f"written={_fmt(smpp['written_pdus'])} PDU   "
        f"observed_total={_fmt(smpp.get('socket_written_observed_pdus'))} PDU"
    )
    lines.append(
        f"SMPP rates   socket_write={_fmt(snapshot.get('socket_write_rate_pdu_per_second'), digits=1, suffix=' PDU/s')}   "
        f"journal_durable={_fmt(snapshot.get('journal_durable_rate_pdu_per_second'), digits=1, suffix=' PDU/s')}"
    )
    responses = snapshot["responses"]
    lines.append(
        f"Responses    observed={_fmt(responses['observed_success'] + responses['observed_error'] + responses['observed_unknown'])}    "
        f"durable={_fmt(responses['durable_success'] + responses['durable_error'] + responses['durable_unknown'])}     "
        f"unknown={_fmt(responses['observed_unknown'])}  "
        f"pending_durable={_fmt(responses['pending_durable'])}  "
        f"observed_rate={_fmt(snapshot.get('response_observed_rate_pdu_per_second'), digits=1, suffix=' PDU/s')}  "
        f"callback_err={_fmt(responses['callback_errors'])}"
    )
    journal = snapshot["journal"]
    lines.append(
        f"Journal      durable={_fmt(journal['durable_events'])}     "
        f"backlog={_fmt(journal['backlog_bytes'])}      "
        f"oldest_undurable={_fmt(journal['oldest_undurable_ms'], digits=1, suffix=' ms')}"
    )
    projection = snapshot["projection"]
    lines.append(
        f"Projection   applied={_fmt(projection['applied'])}     "
        f"lag={_fmt(projection['lag_bytes'])}          "
        f"queue={_fmt(projection['queue'])}     caught_up={_fmt(projection['caught_up'])}"
    )
    lines.append("")
    lines.append(
        f"{'Campaign':<18} {'Status':<9} {'Accepted':>8} {'Written':>8} "
        f"{'RespOK':>7} {'RespErr':>7} {'Unknown':>7} {'Projected':>9}"
    )
    campaigns = snapshot["campaigns"][:MAX_CAMPAIGN_ROWS]
    for item in campaigns:
        label = _campaign_label(item)
        if len(label) > 18:
            label = label[:17] + "~"
        lines.append(
            f"{label:<18} {str(item['state']):<9} "
            f"{_fmt(item['accepted_tasks']):>8} "
            f"{_fmt(item['socket_written_pdus']):>8} "
            f"{_fmt(item['response_success']):>7} "
            f"{_fmt(item['response_error']):>7} "
            f"{_fmt(item['unknown']):>7} "
            f"{_fmt(item['projected']):>9}"
        )
    if len(snapshot["campaigns"]) > MAX_CAMPAIGN_ROWS:
        lines.append(
            f"... {len(snapshot['campaigns']) - MAX_CAMPAIGN_ROWS} more campaigns"
        )
    lines.append("")
    incomplete = snapshot.get("incomplete_metrics") or []
    lines.append(
        f"Health: {snapshot['health']} | Backpressure: {snapshot['backpressure']} | "
        f"Fsync errors: {_fmt(snapshot['fsync_errors'])} | "
        f"Last update: {snapshot.get('observed_at_iso')}"
    )
    if snapshot.get("rate_unavailable_reasons"):
        lines.append(
            "Rate notes: " + "; ".join(snapshot["rate_unavailable_reasons"][:5])
        )
    if incomplete:
        lines.append("Incomplete: " + ", ".join(incomplete))
    if snapshot.get("warnings"):
        lines.append("Warnings: " + "; ".join(snapshot["warnings"][:3]))
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only VNext runtime statistics from bounded snapshots. "
            "No Web UI, DuckDB query, or runtime mutation is performed."
        )
    )
    parser.add_argument(
        "--root", required=True,
        help="VNext runtime root (for example /opt/marketing-copilot/vnext-runtime)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--watch", action="store_true", help="refresh until Ctrl+C")
    mode.add_argument("--once", action="store_true", help="print one snapshot and exit")
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL,
        help=f"refresh interval in seconds (default {DEFAULT_INTERVAL:g})",
    )
    parser.add_argument(
        "--campaign", action="append", default=[],
        metavar="TYPE:ID",
        help="filter campaigns (repeatable; ID alone matches any type)",
    )
    parser.add_argument("--json", action="store_true", help="machine-readable JSON")
    parser.add_argument("--no-color", action="store_true", help="disable ANSI output")
    return parser


def _filter_campaigns(
    snapshot: dict, filters: list[tuple[str | None, int]]
) -> None:
    if not filters:
        return
    rows = [
        item for item in snapshot["campaigns"]
        if any(
            (campaign_type is None or item.get("campaign_type") == campaign_type)
            and _int(item.get("campaign_id", -1)) == campaign_id
            for campaign_type, campaign_id in filters
        )
    ]
    existing = {
        (item.get("campaign_type"), _int(item.get("campaign_id", -1)))
        for item in rows
    }
    for campaign_type, campaign_id in filters:
        key = (campaign_type, campaign_id)
        if campaign_type is not None and key not in existing:
            rows.append({
                "campaign_type": campaign_type,
                "campaign_id": campaign_id,
                "state": "N/A",
                "accepted_tasks": None,
                "tasks_taken": None,
                "socket_written_pdus": None,
                "response_success": None,
                "response_error": None,
                "unknown": None,
                "projected": None,
                "pending_durability": None,
            })
    snapshot["campaigns"] = rows


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    try:
        campaign_filters = _parse_campaign_filter(args.campaign)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"error: runtime root does not exist or is not a directory: {root}",
              file=sys.stderr)
        return 2
    watch = bool(args.watch)
    if not watch and not args.once:
        args.once = True
    rate_trackers = {
        "socket_write": _RateTracker(DEFAULT_RATE_WINDOW),
        "journal_durable": _RateTracker(DEFAULT_RATE_WINDOW),
        "response_observed": _RateTracker(DEFAULT_RATE_WINDOW),
    }
    color = (
        not args.json
        and not args.no_color
        and sys.stdout.isatty()
    )
    try:
        while True:
            snapshot = _build_snapshot(
                root, rate_trackers=rate_trackers, interval=args.interval
            )
            _filter_campaigns(snapshot, campaign_filters)
            if args.json:
                print(
                    json.dumps(
                        snapshot, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
            else:
                if watch and color:
                    sys.stdout.write("\x1b[2J\x1b[H")
                print(_format_human(snapshot), flush=True)
                if watch and not sys.stdout.isatty():
                    print("-" * 79, flush=True)
            if not watch:
                return 0
            time.sleep(max(0.2, float(args.interval)))
    except KeyboardInterrupt:
        if not args.json and sys.stdout.isatty():
            print()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
