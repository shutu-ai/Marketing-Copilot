"""Application-facing VNext control plane and ingress adapter.

The data plane lives in :mod:`vnext`.  This module is deliberately small: it
loads route/template control data once at ingress, starts the single VNext
runtime, and exposes read-only projection views for the API/UI.  Workers never
import this module and never open the application DuckDB.
"""

from __future__ import annotations

import os
import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from database import read_transaction
from vnext.business_day import business_timezone
from vnext.ingress import IngressRouter
from vnext.runtime import VNextRuntime
from vnext.spool import DEFAULT_MAX_SPOOL_BYTES, SpoolCapacityError
from vnext.task_index import (
    connect_reader as connect_task_index_reader,
    default_task_index_path,
    is_ready as task_index_is_ready,
)


_LOCK = threading.RLock()
_RUNTIME: VNextRuntime | None = None
_CONFIG_RESTART_REQUIRED = False
_CONFIG_CHANGE_AT = 0.0
_CONFIG_CHANGE_REASON = ""


VNEXT_METRIC_CONTRACT = {
    "sent": "socket_written",
    "sent_definition": "logical send event durably recorded after submit_sm was written to the SMPP socket",
    "sent_unit": "message_event",
    "smsc_accepted": "submit_sm_resp_success",
    "smsc_accepted_definition": "SMSC returned a successful submit_sm response; this is not handset delivery",
    "smsc_accepted_unit": "submit_sm_part",
    "delivery_receipts_available": False,
}


def pipeline_mode() -> str:
    value = str(os.environ.get("MC_PIPELINE_MODE", "vnext") or "vnext").strip().lower()
    return value if value in {"vnext", "legacy", "test"} else "vnext"


def is_active() -> bool:
    with _LOCK:
        return _RUNTIME is not None and bool(getattr(_RUNTIME, "_started", False))


def config_state() -> dict:
    """Return whether persisted SMPP settings are effective in this runtime."""

    with _LOCK:
        active = _RUNTIME is not None and bool(getattr(_RUNTIME, "_started", False))
        pending = bool(_CONFIG_RESTART_REQUIRED)
    if not active:
        return {
            "restart_required": False,
            "effective_state": "next_start",
        }
    return {
        "restart_required": pending,
        "effective_state": "pending_restart" if pending else "applied",
    }


def runtime() -> VNextRuntime:
    with _LOCK:
        if _RUNTIME is None or not getattr(_RUNTIME, "_started", False):
            raise RuntimeError("VNext runtime is not started")
        return _RUNTIME


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _env_bytes(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    # Keep the production spool fail-safe at 4 GiB or more.  Larger values
    # such as 16 GiB/32 GiB remain valid when the host has the capacity.
    return max(16 * 1024 * 1024, int(default), value)


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return bool(default)
    return value not in {"0", "false", "no", "off"}


def _configured_retention_days(value=None) -> int:
    """Read the Basic Settings retention value with the VNext bounds."""

    if value is None:
        from services.settings_service import get_setting
        value = get_setting(
            "sms_task_retention_days",
            os.environ.get("MC_VNEXT_RETENTION_DAYS", "7"),
        )
    try:
        return max(1, min(3650, int(value)))
    except (TypeError, ValueError):
        return 7


def mark_config_change(reason: str = "SMPP configuration changed") -> None:
    """Mark a structural control-plane change for a safe restart.

    Endpoint, credential, bind-pool, window and protocol settings remain
    restart-bound.  The safe rate settings use ``apply_route_rates`` instead
    and do not call this function.
    """

    global _CONFIG_RESTART_REQUIRED, _CONFIG_CHANGE_AT, _CONFIG_CHANGE_REASON
    with _LOCK:
        _CONFIG_RESTART_REQUIRED = True
        _CONFIG_CHANGE_AT = time.time()
        _CONFIG_CHANGE_REASON = str(reason or "SMPP configuration changed")


def apply_route_rates(route_configs: dict | None = None) -> dict:
    """Push persisted TPS settings to all live workers without reconnecting."""

    with _LOCK:
        value = _RUNTIME
        if value is None or not getattr(value, "_started", False):
            return {
                "ok": False,
                "reason": "runtime_inactive",
                "updated_routes": [],
            }
        routes = route_configs if route_configs is not None else _load_routes()
        return value.reload_route_rates(routes)


def apply_retention_days(value=None) -> dict:
    """Push task retention to the idle-only projection maintenance loop."""

    desired = _configured_retention_days(value)
    with _LOCK:
        runtime_value = _RUNTIME
        if runtime_value is None or not getattr(runtime_value, "_started", False):
            return {
                "ok": False,
                "reason": "runtime_inactive",
                "retention_days": desired,
            }
        applier = getattr(runtime_value, "apply_retention_days", None)
        if not callable(applier):
            return {
                "ok": False,
                "reason": "runtime_control_unavailable",
                "retention_days": desired,
            }
        result = dict(applier(desired) or {})
    result["retention_days"] = desired
    return result


def run_root() -> Path:
    configured = str(os.environ.get("MC_VNEXT_RUN_ROOT", "") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    state_dir = Path(os.environ.get("MC_STATE_DIR", str(Path(__file__).resolve().parents[1] / "data")))
    return (state_dir / "vnext").resolve()


def _load_routes() -> dict[str, dict]:
    def read(conn):
        return conn.execute(
            """SELECT id,name,host,port,system_id,password,source_addr,window_size,
                      timeout,tps,tps_scope,pool_size,bind_type,enquire_interval,
                      reconnect_max_seconds,delivery_receipt_mode,priority,
                      address_ton,address_npi,data_coding,enabled
                 FROM smpp_configs WHERE enabled=1 ORDER BY priority,id"""
        ).fetchall()

    rows = read_transaction(read, operation="vnext.routes.load")
    return {
        str(row["id"]): {
            "route_id": int(row["id"]),
            "name": str(row["name"] or ""),
            "host": str(row["host"] or ""),
            "port": int(row["port"] or 0),
            "system_id": str(row["system_id"] or ""),
            "password": str(row["password"] or ""),
            "source_addr": str(row["source_addr"] or "Marketing"),
            "window_size": int(row["window_size"] or 10),
            "timeout": float(row["timeout"] or 10),
            "tps": float(row["tps"] or 0),
            # VNext capacity is always accounted per physical bind.
            "tps_scope": "per_bind",
            "pool_size": max(1, int(row["pool_size"] or 1)),
            "bind_type": str(row["bind_type"] or "transceiver"),
            "enquire_interval": float(row["enquire_interval"] or 30),
            "reconnect_max_seconds": float(row["reconnect_max_seconds"] or 60),
            "delivery_receipt_mode": str(row["delivery_receipt_mode"] or "optional"),
            "priority": int(row["priority"] or 100),
            "address_ton": int(row["address_ton"] or 0),
            "address_npi": int(row["address_npi"] or 0),
            "data_coding": int(row["data_coding"] or 0),
        }
        for row in rows
    }


def _default_route_id(route_configs: dict[str, dict]) -> int:
    candidates = [dict(value) for value in route_configs.values()]
    if not candidates:
        return 0
    selected = min(
        candidates,
        key=lambda item: (int(item.get("priority", 100) or 100), int(item.get("route_id", 0) or 0)),
    )
    return int(selected.get("route_id") or 0)


def start() -> VNextRuntime | None:
    """Start VNext and return its runtime; explicit legacy mode is test-only."""

    global _RUNTIME, _CONFIG_RESTART_REQUIRED, _CONFIG_CHANGE_REASON, _CONFIG_CHANGE_AT
    if pipeline_mode() in {"legacy", "test"}:
        return None
    with _LOCK:
        if _RUNTIME is not None and getattr(_RUNTIME, "_started", False):
            return _RUNTIME
        routes = _load_routes()
        retention_days = _configured_retention_days()
        value = VNextRuntime(
            run_root(), routes,
            worker_count=_env_int("MC_VNEXT_WORKERS", 4, 1, 8),
            virtual_partition_count=_env_int("MC_VNEXT_PARTITIONS", 256, 16, 4096),
            max_spool_bytes=_env_bytes("MC_VNEXT_MAX_SPOOL_BYTES", DEFAULT_MAX_SPOOL_BYTES),
            projection_queue_size=_env_int("MC_VNEXT_PROJECTION_QUEUE", 8192, 128, 100000),
            max_inflight=_env_int("MC_VNEXT_MAX_INFLIGHT", 128, 1, 2048),
            prefetch_per_partition=_env_int("MC_VNEXT_PREFETCH_PER_PARTITION", 64, 1, 512),
            retention_days=retention_days,
            keep_smpp_binds=_env_bool("MC_VNEXT_KEEP_SMPP_BINDS", True),
            prewarm_smpp_binds=_env_bool("MC_VNEXT_PREWARM_SMPP_BINDS", True),
        )
        value.start()
        _RUNTIME = value
        _CONFIG_RESTART_REQUIRED = False
        _CONFIG_CHANGE_AT = 0.0
        _CONFIG_CHANGE_REASON = ""
        return value


def _runtime_business_timezone(value: object) -> str:
    """Return the effective timezone once per API response."""

    clock = getattr(value, "clock", None)
    name = getattr(clock, "timezone_name", None)
    if name:
        return str(name)
    return str(business_timezone()[0])


def stop() -> None:
    global _RUNTIME
    with _LOCK:
        value = _RUNTIME
        _RUNTIME = None
    if value is not None:
        value.stop()


def rollover_if_needed() -> dict:
    """Advance the VNext business-day epoch without touching legacy tables."""

    return runtime().rollover_if_needed()


def _template_map(campaign_type: str, campaign: dict) -> dict:
    from services import sms_task_service

    def read(conn):
        return sms_task_service._resolve_template_map(
            conn, campaign_type, int(campaign["id"]), campaign,
        )

    return read_transaction(read, operation="vnext.ingress.template")


def _excluded(msisdns: Iterable[str]) -> set[str]:
    values = list(dict.fromkeys(str(item) for item in msisdns if item))
    if not values:
        return set()

    def read(conn):
        result = set()
        for start in range(0, len(values), 1000):
            chunk = values[start:start + 1000]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT msisdn FROM exclusion_users WHERE enabled=1 AND msisdn IN ({placeholders})",
                chunk,
            ).fetchall()
            result.update(str(row["msisdn"]) for row in rows)
        return result

    return read_transaction(read, operation="vnext.ingress.exclusions")


def _source_hash(ingestion_lease: Optional[tuple]) -> str:
    if not ingestion_lease:
        return ""
    try:
        file_id = int(ingestion_lease[0])
    except (TypeError, ValueError, IndexError):
        return ""
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT file_hash FROM ingestion_files WHERE id=?", (file_id,)
        ).fetchone(),
        operation="vnext.ingress.source_hash",
    )
    return str(row["file_hash"] or "") if row else ""


def enqueue_rows(campaign_type: str, campaign: dict, rows: Iterable[dict], source_file: str = "",
                 batch_id: Optional[int] = None, validate_schedule: bool = True,
                 ingestion_lease: Optional[tuple] = None,
                 ingestion_chunk: Optional[dict] = None,
                 ingestion_chunk_progress: bool = False,
                 row_ordinal_start: int = 0) -> dict:
    """Render and durably enqueue one batch/realtime source chunk.

    This function performs only bounded control-plane reads.  The returned
    result is compatible with the existing campaign ingestion services, while
    no ``sms_tasks`` rows or SMPP calls are created here.
    """

    del ingestion_chunk_progress  # VNext completes the source lease in its caller.
    from services import sms_task_service

    if validate_schedule:
        active, reason = sms_task_service.campaign_is_active(campaign)
        if not active:
            return {"success": False, "message": reason, "created": 0}
    elif campaign.get("status") != "in_progress":
        return {"success": False, "message": "campaign is not enabled", "created": 0}

    value = runtime()
    route_id = campaign.get("smpp_config_id")
    if route_id is None or int(route_id or 0) <= 0:
        route_id = _default_route_id(value.route_configs)
    route_id = int(route_id or 0)
    if str(route_id) not in value.route_configs:
        return {"success": False, "message": "no enabled SMPP route configured", "created": 0}

    template_map = _template_map(campaign_type, campaign)
    phone_prefix = "".join(str(campaign.get("phone_prefix") or "").split())
    source_hash = _source_hash(ingestion_lease) if campaign_type == "realtime" else ""
    source_identity = os.path.basename(str(source_file or "")).strip() or "source"
    stable_source = ":".join(
        [str(campaign_type), str(int(campaign["id"])), str(batch_id or ""), source_identity, source_hash]
    )
    generation = int(campaign.get("dispatch_generation") or 1)
    materialized = list(rows)
    candidates = []
    invalid = duplicate = excluded = no_template = 0
    seen: set[str] = set()

    for raw in materialized:
        payload = {str(key): "" if item is None else str(item) for key, item in dict(raw).items()}
        from vnext.partition import normalize_msisdn
        msisdn = normalize_msisdn(payload.get("msisdn", ""), phone_prefix)
        if not msisdn:
            invalid += 1
            continue
        if msisdn in seen:
            duplicate += 1
            continue
        seen.add(msisdn)
        payload["msisdn"] = msisdn
        if not _select_template_content(template_map, payload, sms_task_service):
            no_template += 1
            continue
        candidates.append(payload)

    excluded_values = _excluded([row["msisdn"] for row in candidates])
    accepted_rows = []
    for payload in candidates:
        if payload["msisdn"] in excluded_values:
            excluded += 1
            continue
        content, source_addr = _select_template_content(template_map, payload, sms_task_service)
        if not content:
            no_template += 1
            continue
        payload["message"] = content
        payload["source_addr"] = source_addr
        accepted_rows.append((payload, source_addr))

    ingress_rows = [row for row, _source_addr in accepted_rows]
    result = {
        "success": True, "created": 0, "duplicate": duplicate,
        "excluded": excluded, "invalid": invalid, "no_template": no_template,
        "created_msisdns": [], "pipeline": "vnext",
    }
    if not ingress_rows:
        return result
    # Campaign-level fields are constant for a source chunk.  A template
    # source address is retained in the spool record when present.
    source_addr = next((item for _row, item in accepted_rows if item), "")
    try:
        ingress = value.enqueue_chunk(
            ingress_rows, campaign_type=campaign_type, campaign_id=int(campaign["id"]),
            campaign_generation=generation, route_id=route_id, phone_prefix=phone_prefix,
            source_id=stable_source, payload_ref=str(source_file or stable_source),
            row_ordinal_start=int(row_ordinal_start), flash_sms=bool(campaign.get("flash_sms")),
            source_addr=source_addr, campaign_name=str(campaign.get("name") or ""),
        )
    except SpoolCapacityError as exc:
        result.update(success=False, status="pressure", message=str(exc))
        return result
    result["created"] = int(ingress.accepted)
    result["duplicate"] += int(ingress.duplicate)
    result["created_msisdns"] = list(ingress.accepted_msisdns)
    result["rejected"] = int(ingress.rejected)
    result["rejected_reasons"] = dict(ingress.rejected_reasons)
    return result


def enqueue_canary_rows(campaign_type: str, campaign_id: int, rows: Iterable[dict], *,
                        route_id: int, source_id: str, phone_prefix: str = "",
                        payload_ref: str = "", campaign_generation: int = 1,
                        row_ordinal_start: int = 0, flash_sms: bool = False,
                        source_addr: str = "") -> dict:
    """Ingest synthetic canary rows through the real VNext ingress boundary.

    The HTTP route is disabled unless ``MC_VNEXT_CANARY_MODE=1`` and is only
    intended for loopback acceptance runs; it deliberately bypasses campaign
    template/database business reads while retaining the production spool,
    worker, SMPP and journal path.
    """

    result = runtime().enqueue_chunk(
        rows,
        campaign_type=str(campaign_type),
        campaign_id=int(campaign_id),
        campaign_generation=int(campaign_generation),
        route_id=int(route_id),
        phone_prefix=str(phone_prefix or ""),
        source_id=str(source_id),
        payload_ref=str(payload_ref or source_id),
        row_ordinal_start=int(row_ordinal_start),
        flash_sms=bool(flash_sms),
        source_addr=str(source_addr or ""),
    )
    return {"success": True, "pipeline": "vnext", **result.as_dict()}


def _select_template_content(template_map: dict, payload: dict, sms_task_service) -> tuple[str, str] | None:
    template = sms_task_service._select_template(template_map, payload.get("language", ""))
    if not template:
        return None
    content = str(template.get("content") or "")
    for key, item in payload.items():
        content = content.replace("{" + str(key) + "}", str(item))
    return content, str(template.get("source_addr") or "")


def _progress_events(business_day: str | None = None) -> list[dict]:
    """Read worker journals, never the DuckDB owned by ProjectionWriter."""

    value = runtime()
    selected_day = str(business_day or value.spool.current_business_day)
    events: dict[str, dict] = {}
    for path in sorted(value.progress_root.glob("worker-*.jsonl")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    if str(event.get("business_day") or "") != selected_day:
                        continue
                    event_id = str(event.get("event_id") or "")
                    if event_id:
                        events.setdefault(event_id, event)
        except (OSError, ValueError, TypeError):
            continue
    return list(events.values())


def _live_campaign_counter_snapshot(business_day: str | None = None) -> dict | None:
    """Read the bounded worker counter snapshot for the current business day.

    The worker status files are already maintained by ``ProgressPersistence``
    and contain durable counters by campaign.  Reading them keeps API/UI
    statistics off the journal replay path and, importantly, does not open
    DuckDB or touch the send workers.  Historical-day queries still use the
    journal fallback because the live snapshot only describes the active day.
    """

    try:
        value = runtime()
        current_day = str(getattr(value.spool, "current_business_day", ""))
        if business_day is not None and str(business_day) != current_day:
            return None
        reader = getattr(value, "campaign_counter_snapshot", None)
        if not callable(reader):
            return None
        snapshot = reader()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    return snapshot if isinstance(snapshot, dict) else None


def _counter_stats(counters: list[dict]) -> dict[str, int]:
    fields = (
        "accepted_tasks", "tasks_taken", "socket_written_events",
        "socket_written_pdus", "response_success", "response_error",
        "unknown", "write_failed", "cancelled", "expired", "durable_events",
    )
    result = {field: 0 for field in fields}
    for item in counters:
        for field in fields:
            value = item.get(field)
            if isinstance(value, (int, float)):
                result[field] += int(value)
    return result


def _campaign_stats_from_counters(
    campaign_type: str,
    campaign_id: int,
    counters: dict,
    *,
    complete: bool,
    source: str,
) -> dict:
    return {
        "pipeline": "vnext",
        "campaign_type": str(campaign_type),
        "campaign_id": int(campaign_id),
        "metric_contract": dict(VNEXT_METRIC_CONTRACT),
        "delivery_receipts_available": False,
        "sent_count": int(counters.get("socket_written_events") or 0),
        "sent_pdus": int(counters.get("socket_written_pdus") or 0),
        "smsc_accepted_pdus": int(counters.get("response_success") or 0),
        "smsc_rejected_pdus": int(counters.get("response_error") or 0),
        "unknown_pdus": int(counters.get("unknown") or 0),
        "write_failed": int(counters.get("write_failed") or 0),
        "cancelled": int(counters.get("cancelled") or 0),
        "expired": int(counters.get("expired") or 0),
        "accepted_tasks": int(counters.get("accepted_tasks") or 0),
        "tasks_taken": int(counters.get("tasks_taken") or 0),
        "event_count": int(counters.get("durable_events") or 0),
        "stats_complete": bool(complete),
        "stats_source": str(source),
    }


def _projection_campaign_totals(
    campaign_type: str,
    campaign_id: int,
    business_day: str | None = None,
) -> dict | None:
    """Read one campaign/day row from the bounded projection snapshot."""

    try:
        value = runtime()
        selected_day = str(business_day or value.spool.current_business_day)
        snapshot = value.projection.snapshot()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    for item in snapshot.get("campaign_daily_totals") or []:
        if (
            str(item.get("business_day") or "") == selected_day
            and str(item.get("campaign_type") or "") == str(campaign_type)
            and int(item.get("campaign_id") or 0) == int(campaign_id)
        ):
            return dict(item)
    return None


def campaign_daily_totals(campaign_type: str, campaign_id: int) -> list[dict]:
    """Return bounded per-day totals for one campaign from the VNext snapshot.

    The projection snapshot is already cached and aggregated by the projection
    writer.  Keeping this read-only adapter here lets detail pages consume the
    same ``socket_written`` contract as the dashboard without replaying the
    progress journal or scanning the legacy ``sms_tasks`` table.
    """

    try:
        value = runtime()
        snapshot = value.projection.snapshot()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return []

    result = []
    for item in snapshot.get("campaign_daily_totals") or []:
        if (
            str(item.get("campaign_type") or "") != str(campaign_type)
            or int(item.get("campaign_id") or 0) != int(campaign_id)
        ):
            continue
        business_day = str(item.get("business_day") or "")[:10]
        if not business_day:
            continue
        result.append(dict(item))
    result.sort(key=lambda item: str(item.get("business_day") or ""), reverse=True)
    return result


def _add_today_campaign_stats(stats: dict, daily: dict | None) -> dict:
    """Attach today/day counters without changing the cumulative contract."""

    daily = daily or {}
    values = {
        "today_sent_users": int(daily.get("socket_written") or 0),
        "today_socket_written_events": int(daily.get("socket_written") or 0),
        "today_sent_pdus": int(daily.get("socket_written_pdus") or 0),
        "today_smsc_accepted_pdus": int(daily.get("response_success") or 0),
        "today_smsc_rejected_pdus": int(daily.get("response_error") or 0),
        "today_unknown_pdus": int(daily.get("unknown") or 0),
        "today_write_failed": int(daily.get("write_failed") or 0),
        "today_accepted_tasks": int(daily.get("accepted") or 0),
        "today_tasks_taken": int(daily.get("taken") or 0),
    }
    values["today_sent_count"] = values["today_sent_users"]
    values["today_socket_written"] = values["today_sent_users"]
    values["today_event_count"] = sum(
        int(daily.get(field) or 0)
        for field in (
            "queued", "taken", "accepted", "socket_written", "write_failed",
            "unknown", "cancelled", "expired", "response_success", "response_error",
        )
    )
    stats.update(values)
    stats["sent_users"] = int(stats.get("sent_count") or 0)
    return stats


def _business_days(current_day: str, count: int = 7) -> list[str]:
    try:
        anchor = date.fromisoformat(str(current_day))
    except (TypeError, ValueError):
        anchor = date.today()
    return [
        (anchor - timedelta(days=offset)).isoformat()
        for offset in range(max(1, int(count)) - 1, -1, -1)
    ]


def _daily_totals_by_day(snapshot: dict) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for raw in snapshot.get("daily_totals") or []:
        day = str(raw.get("business_day") or "")
        if not day:
            continue
        result[day] = {
            key: int(raw.get(key) or 0)
            for key in (
                "queued", "taken", "socket_written", "socket_written_pdus",
                "write_failed", "unknown", "cancelled", "accepted", "expired",
                "response_success", "response_error",
            )
        }
    return result


def _projection_read_health(snapshot: dict) -> tuple[bool, str]:
    """Describe whether a projection snapshot is safe to present as complete.

    Older test doubles and stopped-process inspection tools may not include
    the ``ready`` field.  Treat those snapshots as available for backwards
    compatibility; live snapshots from ``ProjectionWriterProcess`` always
    include it and therefore cannot silently turn a crashed writer into a
    valid-looking all-zero result.
    """

    if "ready" not in snapshot:
        return True, ""
    if bool(snapshot.get("ready")) and bool(snapshot.get("alive", True)):
        return True, ""
    if bool(snapshot.get("native_crash")):
        return False, "projection_writer_native_crash"
    if not bool(snapshot.get("alive", True)):
        return False, "projection_writer_not_alive"
    return False, "projection_writer_not_ready"


def dashboard_stats() -> dict:
    """Return dashboard counters using the VNext projection contract."""

    value = runtime()
    projection = value.projection.snapshot()
    projection_available, projection_warning = _projection_read_health(projection)
    current_day = str(value.spool.current_business_day)
    totals = _daily_totals_by_day(projection)
    days = _business_days(current_day)
    today = totals.get(current_day, {})
    trend = []
    for day in days:
        row = totals.get(day, {})
        trend.append({
            "stats_date": day,
            # The dashboard's send count is intentionally socket_written,
            # never legacy accepted/delivered task-table state.
            "sent_count": int(row.get("socket_written", 0)),
            "socket_written_count": int(row.get("socket_written", 0)),
            "smsc_accepted_count": int(row.get("response_success", 0)),
            "unknown_count": int(row.get("unknown", 0)),
            "write_failed_count": int(row.get("write_failed", 0)),
            "conversion_count": 0,
        })
    return {
        "pipeline": "vnext",
        "metric_contract": dict(VNEXT_METRIC_CONTRACT),
        "sent_metric": "socket_written",
        "sent_unit": "message_event",
        "delivery_receipts_available": False,
        "delivery_receipt_reason": (
            "VNext records submit_sm socket and response evidence; handset DLR is not connected"
        ),
        "today_sent_count": int(today.get("socket_written", 0)),
        "today_socket_written_count": int(today.get("socket_written", 0)),
        "today_smsc_accepted_count": int(today.get("response_success", 0)),
        "today_unknown_count": int(today.get("unknown", 0)),
        "today_write_failed_count": int(today.get("write_failed", 0)),
        "daily_trend": trend,
        "projection_lag_seconds": float(projection.get("projection_lag_seconds") or 0.0),
        "stats_complete": projection_available,
        "stats_source": (
            "vnext_projection" if projection_available
            else "vnext_projection_unavailable"
        ),
        "projection_available": projection_available,
        "degraded": not projection_available,
        "degraded_reason": projection_warning,
    }


def _campaign_names_for_rows(rows: Iterable[dict]) -> dict[tuple[str, int], str]:
    """Read campaign names already materialized in the new task store.

    Task-list reads deliberately do not enrich rows from the control database.
    Ingress hints carry the name once and historical cutover imports it once;
    this keeps every filter page on the same bounded projection database.
    """

    result = {}
    for row in rows:
        campaign_type = str(row.get("campaign_type") or "")
        try:
            campaign_id = int(row.get("campaign_id") or 0)
        except (TypeError, ValueError):
            continue
        name = str(row.get("campaign_name") or "").strip()
        if name and campaign_id > 0:
            result[(campaign_type, campaign_id)] = name
    return result


def _task_item_from_event(
    row: dict,
    base: dict | None = None,
    campaign_names: dict[tuple[str, int], str] | None = None,
) -> dict:
    """Adapt one progress event to the logical-task shape consumed by the UI.

    Response events deliberately carry only transport evidence.  Their
    logical task fields are filled from the corresponding base event so the
    list never renders an empty number or message just because the selected
    row happens to be a ``submit_sm_resp`` event.
    """

    base = base or {}
    event_id = str(row.get("event_id") or "")
    normalized_msisdn = str(
        row.get("normalized_msisdn") or base.get("normalized_msisdn") or ""
    )
    language = str(row.get("language") or base.get("language") or "")
    message = str(row.get("message") or base.get("message") or "")
    campaign_type = str(row.get("campaign_type") or base.get("campaign_type") or "")
    campaign_id = int(row.get("campaign_id") or base.get("campaign_id") or 0)
    campaign_name = (
        str(row.get("campaign_name") or base.get("campaign_name") or "").strip()
        or (campaign_names or {}).get((campaign_type, campaign_id), "")
        or f"{campaign_type} #{campaign_id}"
    )
    return {
        "id": event_id,
        "event_id": event_id,
        "campaign_name": campaign_name,
        "campaign_type": campaign_type,
        "campaign_id": campaign_id,
        "smpp_config_id": int(row.get("route_id") or base.get("route_id") or 0),
        "msisdn": normalized_msisdn,
        "normalized_msisdn": normalized_msisdn,
        "language": language,
        "content": message,
        "status": str(row.get("state") or "unknown"),
        "attempts": 0,
        "error_msg": str(row.get("error") or ""),
        "updated_at": _format_event_timestamp(
            row.get("last_event_at") or row.get("created_at")
        ),
        "parts_written": int(row.get("parts_written") or 0),
        "parts_total": int(row.get("parts_total") or 0),
    }


def _format_event_timestamp(value: object) -> str:
    """Expose VNext epoch timestamps in the same string shape as legacy rows."""

    if value is None or value == "":
        return ""
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat(
            timespec="seconds"
        )
    except (TypeError, ValueError, OverflowError, OSError):
        # Keep the adapter tolerant of already-formatted timestamps from old
        # journal/projection data rather than making task listing fail.
        return str(value)


def _projection_task_page(
    page: int, page_size: int, status: str, campaign_type: str,
    smpp_config_id: int, business_day: str | None, msisdn: str = "",
) -> tuple[int, list[dict]] | None:
    """Read one page from the canonical logical-task index.

    The projection writer remains the sole writer.  This connection is
    read-only and is intentionally opened only for the API query, so the
    send workers and their journal path are not involved.  There is no
    fallback to the old task table or to journal replay: a missing projection
    store is reported as unavailable and is repaired by the offline cutover
    command.
    """

    try:
        value = runtime()
        snapshot = value.projection.snapshot()
        db_path = snapshot.get("db_path")
        if not db_path:
            return None
        # The projection child owns DuckDB exclusively.  The sidecar is the
        # normal live read path; only stopped-process tools/tests may use the
        # DuckDB fallback to inspect a projection that is not being written.
        task_index_path = snapshot.get("task_index_path") or str(
            default_task_index_path(db_path)
        )
        connection = connect_task_index_reader(task_index_path)
        using_sidecar = connection is not None
        if using_sidecar and not task_index_is_ready(connection):
            connection.close()
            return None
        if connection is None:
            if bool(snapshot.get("alive")):
                return None
            import duckdb
            connection = duckdb.connect(str(db_path), read_only=True)
        selected_day = str(business_day or "")
        # Keep the unfiltered operations page cheap by defaulting it to the
        # active business day.  A user-supplied filter is an explicit lookup
        # request and searches the retained task index across days, which is
        # what makes historical batch/phone filters deterministic.
        if not selected_day and not any((status, campaign_type, msisdn)) and int(smpp_config_id) < 0:
            selected_day = str(value.spool.current_business_day)
        conditions: list[str] = []
        params: list[object] = []
        if selected_day:
            conditions.append("e.business_day=?")
            params.append(selected_day)
        if status:
            conditions.append("e.state=?")
            params.append(str(status))
        if campaign_type:
            conditions.append("e.campaign_type=?")
            params.append(str(campaign_type))
        if int(smpp_config_id) >= 0:
            conditions.append("e.route_id=?")
            params.append(int(smpp_config_id))
        if msisdn:
            conditions.append("e.normalized_msisdn=?")
            params.append(msisdn)
        where = " AND ".join(conditions)
        offset = (max(1, int(page)) - 1) * max(1, int(page_size))
        try:
            total_cursor = connection.execute(
                f"SELECT COUNT(*) FROM vnext_task_index e WHERE {where}",
                params,
            )
            total = int(total_cursor.fetchone()[0])
            rows_cursor = connection.execute(
                f"""
                SELECT e.event_id, e.campaign_type, e.campaign_id, e.route_id,
                       e.normalized_msisdn, e.language, e.campaign_name,
                       e.message, e.state, e.parts_written, e.parts_total,
                       e.error, e.created_at, e.last_event_at, e.business_day
                  FROM vnext_task_index e
                 WHERE {where}
                 ORDER BY e.created_at DESC, e.ordinal DESC, e.event_id DESC
                 LIMIT ? OFFSET ?
                """,
                params + [max(1, int(page_size)), offset],
            )
            rows = rows_cursor.fetchall()
            if using_sidecar:
                return total, [dict(row) for row in rows]
            columns = [str(item[0]) for item in connection.description]
            return total, [dict(zip(columns, row)) for row in rows]
        finally:
            connection.close()
    except Exception:
        # Task listing is a projection read model.  Do not silently switch to
        # an old database or replay unbounded journals on a request thread.
        return None


def list_tasks(page: int = 1, page_size: int = 50, status: str = "", campaign_type: str = "",
               smpp_config_id: int = -1, cursor: Optional[int] = None,
               business_day: str | None = None, msisdn: str = "") -> dict:
    page = max(1, int(page))
    # Keep the existing cursor-based API usable by acceptance tools and older
    # clients.  VNext cursors are page tokens, while the legacy implementation
    # used a task id; VNext is selected by the router, so interpreting the
    # token here is unambiguous.  An explicit page remains authoritative.
    if page <= 1 and cursor is not None and int(cursor) > 0:
        page = int(cursor)
    page_size = max(1, min(200, int(page_size)))
    business_timezone_name = _runtime_business_timezone(runtime())
    from vnext.partition import normalize_msisdn
    normalized_msisdn = normalize_msisdn(msisdn)
    projection_page = _projection_task_page(
        page, page_size, status, campaign_type, smpp_config_id, business_day,
        normalized_msisdn,
    )
    if projection_page is not None:
        total, rows = projection_page
        campaign_names = _campaign_names_for_rows(rows)
        items = [
            _task_item_from_event(row, campaign_names=campaign_names)
            for row in rows
        ]
        has_more = page * page_size < total
        return {
            "pipeline": "vnext", "total": total, "page": page,
            "page_size": page_size, "items": items,
            "next_cursor": page + 1 if has_more else None, "has_more": has_more,
            "business_timezone": business_timezone_name,
            "stats_source": "vnext_task_index",
            "projection_available": True,
            "degraded": False,
            "degraded_reason": "",
        }

    # The new store is unavailable.  Return a bounded diagnostic result rather
    # than reintroducing the old task table or a full journal replay.
    return {
        "pipeline": "vnext", "total": 0, "page": page,
        "page_size": page_size, "items": [],
        "next_cursor": None, "has_more": False,
        "business_timezone": business_timezone_name,
        "stats_source": "vnext_task_index_unavailable",
        "projection_available": False,
        "degraded": True,
        "degraded_reason": "vnext_task_index_unavailable",
    }


def export_tasks() -> list[dict]:
    """Export the canonical VNext task index without opening the business DB."""

    try:
        value = runtime()
        snapshot = value.projection.snapshot() or {}
        db_path = snapshot.get("db_path")
        if not db_path:
            return []
        task_index_path = snapshot.get("task_index_path") or str(
            default_task_index_path(db_path)
        )
        connection = connect_task_index_reader(task_index_path)
        if connection is not None:
            if not task_index_is_ready(connection):
                connection.close()
                return []
        elif bool(snapshot.get("alive")):
            return []
        else:
            import duckdb
            connection = duckdb.connect(str(db_path), read_only=True)
        try:
            rows_cursor = connection.execute(
                """SELECT event_id,campaign_name,campaign_type,campaign_id,route_id,
                          normalized_msisdn,language,message,state,parts_written,
                          parts_total,error,created_at,last_event_at,business_day
                     FROM vnext_task_index
                    ORDER BY created_at DESC,ordinal DESC,event_id DESC"""
            )
            rows = rows_cursor.fetchall()
            if connection.__class__.__module__.startswith("sqlite3"):
                return [_task_item_from_event(dict(row)) for row in rows]
            columns = [str(item[0]) for item in connection.description]
            return [
                _task_item_from_event(dict(zip(columns, row)))
                for row in rows
            ]
        finally:
            connection.close()
    except Exception:
        return []


def task_stats(business_day: str | None = None) -> dict:
    value = runtime()
    projection = value.projection.snapshot()
    projection_available, projection_warning = _projection_read_health(projection)
    current_day = str(value.spool.current_business_day)
    selected_day = str(business_day or current_day)
    row = _daily_totals_by_day(projection).get(selected_day, {})

    socket_written = int(row.get("socket_written") or 0)
    socket_written_pdus = int(row.get("socket_written_pdus") or 0)
    response_success = int(row.get("response_success") or 0)
    response_error = int(row.get("response_error") or 0)
    unknown = int(row.get("unknown") or 0)
    write_failed = int(row.get("write_failed") or 0)
    cancelled = int(row.get("cancelled") or 0)
    expired = int(row.get("expired") or 0)

    # Keep the task-statistics cards on the same projection snapshot as the
    # dashboard. These aliases are intentionally current-business-day values
    # for the VNext endpoint, so the UI does not fall back to a different
    # worker-side source while rendering its "today" cards.
    return {
        "accepted_tasks": int(row.get("accepted") or 0),
        "tasks_taken": int(row.get("taken") or 0),
        "socket_written": socket_written,
        "socket_written_events": socket_written,
        "socket_written_pdus": socket_written_pdus,
        "sent": socket_written,
        "sent_pdus": socket_written_pdus,
        "smsc_accepted": response_success,
        "smsc_rejected": response_error,
        "unknown": unknown,
        "write_failed": write_failed,
        "cancelled": cancelled,
        "expired": expired,
        "total": int(projection.get("projection_events_applied") or 0),
        "today_socket_written": socket_written,
        "today_socket_written_events": socket_written,
        "today_socket_written_pdus": socket_written_pdus,
        "today_sent_count": socket_written,
        "today_submit_sm_resp_success": response_success,
        "today_submit_sm_resp_error": response_error,
        "today_write_failed": write_failed,
        "today_unknown": unknown,
        "today_cancelled": cancelled,
        "today_expired": expired,
        "delivery_receipts_available": False,
        "metric_contract": dict(VNEXT_METRIC_CONTRACT),
        "pipeline": "vnext",
        "stats_complete": projection_available,
        "stats_source": (
            "vnext_projection" if projection_available
            else "vnext_projection_unavailable"
        ),
        "projection_available": projection_available,
        "degraded": not projection_available,
        "degraded_reason": projection_warning,
        "projection_lag_seconds": float(projection.get("projection_lag_seconds") or 0.0),
        "business_timezone": _runtime_business_timezone(value),
    }


def campaign_stats(campaign_type: str, campaign_id: int,
                   business_day: str | None = None) -> dict:
    """Return bounded VNext counters for one campaign.

    ``socket_written`` is a logical message event.  Response counters are
    SMPP PDU events, so the API names them explicitly instead of presenting
    them as handset/user delivery counts.  The live path reads worker status
    aggregates; journal replay is retained only for historical-day queries or
    runtimes that predate the aggregate snapshot API.
    """

    live_snapshot = _live_campaign_counter_snapshot(business_day)
    if live_snapshot is not None:
        counters = next(
            (
                item for item in live_snapshot.get("campaigns") or []
                if str(item.get("campaign_type") or "") == str(campaign_type)
                and int(item.get("campaign_id") or 0) == int(campaign_id)
            ),
            {},
        )
        stats = _campaign_stats_from_counters(
            campaign_type,
            campaign_id,
            counters,
            complete=bool(live_snapshot.get("complete")),
            source=str(live_snapshot.get("source") or "worker_status_aggregate"),
        )
        return _add_today_campaign_stats(
            stats, _projection_campaign_totals(campaign_type, campaign_id, business_day)
        )

    projection_totals = _projection_campaign_totals(
        campaign_type, campaign_id, business_day
    )
    if projection_totals is not None:
        stats = _campaign_stats_from_counters(
            campaign_type,
            campaign_id,
            {
                "accepted_tasks": projection_totals.get("accepted"),
                "tasks_taken": projection_totals.get("taken"),
                "socket_written_events": projection_totals.get("socket_written"),
                "socket_written_pdus": projection_totals.get("socket_written_pdus"),
                "response_success": projection_totals.get("response_success"),
                "response_error": projection_totals.get("response_error"),
                "unknown": projection_totals.get("unknown"),
                "write_failed": projection_totals.get("write_failed"),
                "cancelled": projection_totals.get("cancelled"),
                "expired": projection_totals.get("expired"),
                "durable_events": 0,
            },
            complete=True,
            source="vnext_projection",
        )
        return _add_today_campaign_stats(stats, projection_totals)

    events = [
        item for item in _progress_events(business_day)
        if str(item.get("campaign_type") or "") == str(campaign_type)
        and int(item.get("campaign_id") or 0) == int(campaign_id)
    ]
    counters = {
        "socket_written": 0,
        "socket_written_pdus": 0,
        "submit_sm_resp_success": 0,
        "submit_sm_resp_error": 0,
        "unknown": 0,
        "write_failed": 0,
        "cancelled": 0,
        "expired": 0,
    }
    for event in events:
        state = str(event.get("state") or "unknown")
        if state in counters:
            counters[state] += 1
        if state == "socket_written":
            counters["socket_written_pdus"] += max(
                0, int(event.get("parts_written", 0) or 0)
            )
    stats = _campaign_stats_from_counters(
        campaign_type,
        campaign_id,
        {
            "socket_written_events": counters["socket_written"],
            "socket_written_pdus": counters["socket_written_pdus"],
            "response_success": counters["submit_sm_resp_success"],
            "response_error": counters["submit_sm_resp_error"],
            "unknown": counters["unknown"],
            "write_failed": counters["write_failed"],
            "cancelled": counters["cancelled"],
            "expired": counters["expired"],
            "durable_events": len(events),
        },
        complete=True,
        source="journal_replay",
    )
    return _add_today_campaign_stats(
        stats,
        {
            "socket_written": counters["socket_written"],
            "socket_written_pdus": counters["socket_written_pdus"],
            "response_success": counters["submit_sm_resp_success"],
            "response_error": counters["submit_sm_resp_error"],
            "unknown": counters["unknown"],
            "write_failed": counters["write_failed"],
        },
    )


def smpp_stats(status_value: dict | None = None) -> dict:
    """Expose live VNext bind/rate counters using the UI metric contract."""

    runtime_value = None
    if status_value is None:
        runtime_value = runtime()
        snapshot = getattr(runtime_value, "capacity_snapshot", None)
        state = dict(snapshot() if snapshot else runtime_value.status())
    else:
        state = dict(status_value)
    route_rows = state.get("routes") or []
    # The capacity snapshot intentionally does not decorate itself with
    # control-plane route definitions. The direct /api/smpp/stats path must
    # still expose configured routes without calling the heavier application
    # status() endpoint (which scans journal progress). Loading these
    # in-memory configs keeps stats read-only and out of the send path.
    if not route_rows:
        if runtime_value is None:
            runtime_value = runtime()
        route_rows = []
        for config in runtime_value.route_configs.values():
            route_rows.append({
                key: config.get(key) for key in (
                    "route_id", "name", "host", "port", "pool_size", "window_size",
                    "tps", "tps_scope", "priority", "effective_total_limit_tps",
                )
            })
    aggregates: dict[int, dict] = {}
    for route in route_rows:
        route_id = int(route.get("route_id") or 0)
        aggregates[route_id] = {
            "connected_count": 0,
            "connections": [],
            "socket_written_parts": 0,
            "socket_written_parts_1m": 0,
            "socket_written_tps_1m": 0.0,
            "response_success": 0,
            "response_success_1m": 0,
            "response_error": 0,
            "response_error_1m": 0,
            "unknown_parts": 0,
            "unknown_parts_1m": 0,
            "write_failures": 0,
        }
    for worker in state.get("worker_progress") or []:
        snapshot = worker.get("snapshot") or {}
        for transport in snapshot.get("transports") or []:
            route_id = int(transport.get("route_id") or 0)
            aggregate = aggregates.setdefault(route_id, {
                "connected_count": 0, "connections": [],
                "socket_written_parts": 0, "socket_written_parts_1m": 0,
                "socket_written_tps_1m": 0.0, "response_success": 0,
                "response_success_1m": 0, "response_error": 0,
                "response_error_1m": 0, "unknown_parts": 0,
                "unknown_parts_1m": 0, "write_failures": 0,
            })
            connected = bool(transport.get("connected"))
            aggregate["connected_count"] += int(connected)
            aggregate["socket_written_parts"] += int(transport.get("socket_written_parts") or 0)
            aggregate["socket_written_parts_1m"] += int(transport.get("socket_written_parts_1m") or 0)
            aggregate["socket_written_tps_1m"] += float(transport.get("socket_written_tps_1m") or 0.0)
            aggregate["response_success"] += int(transport.get("response_success") or 0)
            aggregate["response_success_1m"] += int(transport.get("response_success_1m") or 0)
            aggregate["response_error"] += int(transport.get("response_error") or 0)
            aggregate["response_error_1m"] += int(transport.get("response_error_1m") or 0)
            aggregate["unknown_parts"] += int(transport.get("unknown_parts") or 0)
            aggregate["unknown_parts_1m"] += int(transport.get("unknown_parts_1m") or 0)
            aggregate["write_failures"] += int(transport.get("write_failures") or 0)
            aggregate["connections"].append({
                "bind_slot": transport.get("bind_slot"),
                "connected": connected,
                "window_inflight": int(transport.get("window_inflight") or 0),
                "last_error": str(transport.get("last_error") or ""),
            })

    durable = {
        "socket_written_pdus": 0,
        "socket_written_events": 0,
        "response_success": 0,
        "response_error": 0,
        "unknown": 0,
        "write_failed": 0,
    }
    for worker in state.get("worker_progress") or []:
        progress = (worker.get("snapshot") or {}).get("progress") or {}
        for target, source in (
            ("socket_written_pdus", "durable_socket_written_pdus"),
            ("socket_written_events", "durable_socket_written_events"),
            ("response_success", "durable_response_success"),
            ("response_error", "durable_response_error"),
            ("unknown", "durable_unknown"),
            ("write_failed", "durable_write_failed"),
        ):
            durable[target] += int(progress.get(source) or 0)

    projection = state.get("projection") or {}
    days = _daily_totals_by_day(projection)
    current_day = str((state.get("daily_epoch") or {}).get("business_day") or "")
    today = days.get(current_day, {})
    items = []
    for route in route_rows:
        route_id = int(route.get("route_id") or 0)
        aggregate = aggregates.get(route_id, {})
        pool_size = max(1, int(route.get("pool_size") or 1))
        connected_count = int(aggregate.get("connected_count") or 0)
        items.append({
            "config_id": route_id,
            "name": str(route.get("name") or f"Route #{route_id}"),
            "host": route.get("host", ""),
            "port": route.get("port", 0),
            "enabled": 1,
            "priority": route.get("priority", 100),
            "tps": route.get("tps", 0),
            "pool_size": pool_size,
            "window_size": route.get("window_size", 0),
            "configured_route_tps": 0.0,
            "configured_per_bind_tps": float(route.get("tps") or 0.0),
            "effective_total_limit_tps": float(route.get("effective_total_limit_tps") or 0.0),
            "tps_scope": "per_bind",
            "tps_unit": "submit_sm_parts/s",
            "pipeline": "vnext",
            "healthy": connected_count > 0 and not any(
                item.get("last_error") for item in aggregate.get("connections", [])
            ),
            "connected_count": connected_count,
            "desired_pool_size": pool_size,
            "connections": aggregate.get("connections", []),
            "queue_size": 0,
            "socket_written_parts": int(aggregate.get("socket_written_parts") or 0),
            "socket_written_parts_1m": int(aggregate.get("socket_written_parts_1m") or 0),
            "observed_parts_tps": round(float(aggregate.get("socket_written_tps_1m") or 0.0), 3),
            "submit_sm_resp_success": int(aggregate.get("response_success") or 0),
            "submit_sm_resp_success_1m": int(aggregate.get("response_success_1m") or 0),
            "submit_sm_resp_failed": int(aggregate.get("response_error") or 0),
            "submit_sm_resp_failed_1m": int(aggregate.get("response_error_1m") or 0),
            "unknown_parts": int(aggregate.get("unknown_parts") or 0),
            "unknown_parts_1m": int(aggregate.get("unknown_parts_1m") or 0),
            "write_failures": int(aggregate.get("write_failures") or 0),
            "last_error": "; ".join(
                str(item.get("last_error") or "")
                for item in aggregate.get("connections", [])
                if item.get("last_error")
            )[:500],
        })

    summary = {
        "pipeline": "vnext",
        "queue_size": int((state.get("spool") or {}).get("pending_records") or 0),
        "socket_written_parts_1m": sum(item["socket_written_parts_1m"] for item in items),
        "socket_written_tps_1m": round(sum(item["observed_parts_tps"] for item in items), 3),
        "submit_sm_resp_success_1m": sum(item["submit_sm_resp_success_1m"] for item in items),
        "submit_sm_resp_failed_1m": sum(item["submit_sm_resp_failed_1m"] for item in items),
        "today_socket_written": int(today.get("socket_written") or 0),
        "today_smsc_accepted": int(today.get("response_success") or 0),
        "today_smsc_rejected": int(today.get("response_error") or 0),
        "today_unknown": int(today.get("unknown") or 0),
        "today_write_failed": int(today.get("write_failed") or 0),
        "durable_socket_written_pdus": int(durable["socket_written_pdus"]),
        "durable_socket_written_events": int(durable["socket_written_events"]),
        "durable_smsc_accepted_pdus": int(durable["response_success"]),
        "effective_total_limit_tps": sum(item["effective_total_limit_tps"] for item in items),
        "configured_per_bind_tps": sum(item["configured_per_bind_tps"] for item in items),
        "configured_route_tps": 0.0,
        "tps_scope": "per_bind",
        "tps_unit": "submit_sm_parts/s",
        "delivery_receipts_available": False,
        "projection_lag_seconds": float(projection.get("projection_lag_seconds") or 0.0),
    }
    return {
        "pipeline": "vnext",
        "metric_contract": dict(VNEXT_METRIC_CONTRACT),
        "summary": summary,
        "items": items,
    }


def capacity_snapshot() -> dict:
    """Return the lightweight runtime state used by capacity polling."""

    value = runtime()
    snapshot = getattr(value, "capacity_snapshot", None)
    result = dict(snapshot() if snapshot else value.status())
    result["pipeline"] = "vnext"
    result["run_root"] = str(value.root)
    result["metric_contract"] = dict(VNEXT_METRIC_CONTRACT)
    return result


def status() -> dict:
    value = runtime()
    result = value.status()
    result["pipeline"] = "vnext"
    result["run_root"] = str(value.root)
    active_by_route: dict[str, set[int]] = {}
    for worker in result.get("worker_progress") or []:
        snapshot = worker.get("snapshot") or {}
        for transport in snapshot.get("transports") or []:
            if transport.get("connected"):
                route_key = str(transport.get("route_id"))
                slot = int(transport.get("bind_slot") or 0)
                active_by_route.setdefault(route_key, set()).add(slot)
    result["routes"] = []
    for config in value.route_configs.values():
        route_key = str(config.get("route_id"))
        tps = float(config.get("tps") or 0)
        scope = "per_bind"
        result["routes"].append({
            key: config.get(key) for key in (
                "route_id", "name", "host", "port", "source_addr", "bind_type", "pool_size",
                "window_size", "tps", "tps_scope", "priority", "delivery_receipt_mode",
                "enquire_interval", "reconnect_max_seconds", "address_ton", "address_npi",
                "data_coding",
            )
        } | {
            "active_bind_count": len(active_by_route.get(route_key, set())),
            "active_bind_slots": sorted(active_by_route.get(route_key, set())),
            "configured_per_bind_tps": tps if scope == "per_bind" else None,
            "effective_total_limit_tps": (
                tps * int(config.get("pool_size") or 1)
                if scope == "per_bind" else tps
            ),
            "tps_unit": "submit_sm_parts/s",
        })
    result["config_contract"] = {
        "source": "smpp_configs",
        "effective_at": value.started_at or None,
        "restart_required": bool(_CONFIG_RESTART_REQUIRED),
        "restart_reason": _CONFIG_CHANGE_REASON,
        "restart_marked_at": _CONFIG_CHANGE_AT or None,
        "worker_count": int(value.worker_count),
        "worker_semantics": "business_data_concurrency",
        "bind_semantics": "physical_bind_per_route_pool_size",
        "window_semantics": "submit_sm_pdu_inflight_per_physical_bind",
        "tps_semantics": "submit_sm_parts_per_second",
    }
    result["metric_contract"] = dict(VNEXT_METRIC_CONTRACT)
    result["task_stats"] = task_stats()
    return result


def set_campaign_state(campaign_type: str, campaign_id: int, state: str) -> dict:
    value = runtime()
    normalized = str(state).strip().lower()
    if normalized not in {"running", "in_progress", "not_started", "paused", "cancelled", "ended", "cancel", "stopped", "pause", "resume"}:
        raise ValueError(f"unsupported VNext campaign state: {state}")
    if normalized in {"resume", "running", "in_progress"}:
        normalized = "running"
    elif normalized in {"pause", "paused", "not_started"}:
        normalized = "paused"
    else:
        normalized = "cancelled"
    value.set_campaign_state(campaign_type, int(campaign_id), normalized)
    return {"success": True, "pipeline": "vnext", "campaign_type": campaign_type,
            "campaign_id": int(campaign_id), "state": normalized}


async def wait_for_idle(timeout: float = 30.0) -> dict:
    return await runtime().wait_for_idle(timeout)
