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
from pathlib import Path
from typing import Iterable, Optional

from database import read_transaction
from vnext.ingress import IngressRouter
from vnext.runtime import VNextRuntime
from vnext.spool import SpoolCapacityError


_LOCK = threading.RLock()
_RUNTIME: VNextRuntime | None = None
_CONFIG_RESTART_REQUIRED = False
_CONFIG_CHANGE_AT = 0.0
_CONFIG_CHANGE_REASON = ""


def pipeline_mode() -> str:
    value = str(os.environ.get("MC_PIPELINE_MODE", "vnext") or "vnext").strip().lower()
    return value if value in {"vnext", "legacy", "test"} else "vnext"


def is_active() -> bool:
    with _LOCK:
        return _RUNTIME is not None and bool(getattr(_RUNTIME, "_started", False))


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
    return max(16 * 1024 * 1024, value)


def mark_config_change(reason: str = "SMPP configuration changed") -> None:
    """Mark a control-plane change that takes effect on a safe restart.

    VNext deliberately does not hot-swap sockets or route credentials while a
    PDU may be in flight.  The API/UI can therefore report the pending state
    instead of claiming that a stale worker snapshot is already effective.
    """

    global _CONFIG_RESTART_REQUIRED, _CONFIG_CHANGE_AT, _CONFIG_CHANGE_REASON
    with _LOCK:
        _CONFIG_RESTART_REQUIRED = True
        _CONFIG_CHANGE_AT = time.time()
        _CONFIG_CHANGE_REASON = str(reason or "SMPP configuration changed")


def run_root() -> Path:
    configured = str(os.environ.get("MC_VNEXT_RUN_ROOT", "") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    state_dir = Path(os.environ.get("MC_STATE_DIR", str(Path(__file__).resolve().parents[1] / "data")))
    return (state_dir / "vnext").resolve()


def _load_routes() -> dict[str, dict]:
    def read(conn):
        return conn.execute(
            """SELECT id,host,port,system_id,password,source_addr,window_size,
                      timeout,tps,tps_scope,pool_size,bind_type,enquire_interval,
                      reconnect_max_seconds,delivery_receipt_mode,priority,
                      address_ton,address_npi,data_coding,enabled
                 FROM smpp_configs WHERE enabled=1 ORDER BY priority,id"""
        ).fetchall()

    rows = read_transaction(read, operation="vnext.routes.load")
    return {
        str(row["id"]): {
            "route_id": int(row["id"]),
            "host": str(row["host"] or ""),
            "port": int(row["port"] or 0),
            "system_id": str(row["system_id"] or ""),
            "password": str(row["password"] or ""),
            "source_addr": str(row["source_addr"] or "Marketing"),
            "window_size": int(row["window_size"] or 10),
            "timeout": float(row["timeout"] or 10),
            "tps": float(row["tps"] or 0),
            "tps_scope": str(row["tps_scope"] or "route_shared"),
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
        value = VNextRuntime(
            run_root(), routes,
            worker_count=_env_int("MC_VNEXT_WORKERS", 4, 1, 8),
            virtual_partition_count=_env_int("MC_VNEXT_PARTITIONS", 256, 16, 4096),
            max_spool_bytes=_env_bytes("MC_VNEXT_MAX_SPOOL_BYTES", 512 * 1024 * 1024),
            projection_queue_size=_env_int("MC_VNEXT_PROJECTION_QUEUE", 8192, 128, 100000),
            max_inflight=_env_int("MC_VNEXT_MAX_INFLIGHT", 128, 1, 2048),
            prefetch_per_partition=_env_int("MC_VNEXT_PREFETCH_PER_PARTITION", 64, 1, 512),
        )
        value.start()
        _RUNTIME = value
        _CONFIG_RESTART_REQUIRED = False
        _CONFIG_CHANGE_AT = 0.0
        _CONFIG_CHANGE_REASON = ""
        return value


def stop() -> None:
    global _RUNTIME
    with _LOCK:
        value = _RUNTIME
        _RUNTIME = None
    if value is not None:
        value.stop()


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
            source_addr=source_addr,
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


def _progress_events() -> list[dict]:
    """Read worker journals, never the DuckDB owned by ProjectionWriter."""

    value = runtime()
    events: dict[str, dict] = {}
    for path in sorted(value.progress_root.glob("worker-*.jsonl")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    event_id = str(event.get("event_id") or "")
                    if event_id:
                        events.setdefault(event_id, event)
        except (OSError, ValueError, TypeError):
            continue
    return list(events.values())


def list_tasks(page: int = 1, page_size: int = 50, status: str = "", campaign_type: str = "",
               smpp_config_id: int = -1, cursor: Optional[int] = None) -> dict:
    del cursor
    page = max(1, int(page))
    page_size = max(1, min(200, int(page_size)))
    offset = (page - 1) * page_size
    events = [item for item in _progress_events()
              if (not status or str(item.get("state")) == str(status))
              and (not campaign_type or str(item.get("campaign_type")) == str(campaign_type))
              and (int(smpp_config_id) < 0 or int(item.get("route_id") or 0) == int(smpp_config_id))]
    events.sort(key=lambda item: (float(item.get("created_at") or 0), str(item.get("event_id"))), reverse=True)
    total = len(events)
    rows = events[offset:offset + page_size]
    items = []
    for row in rows:
        item = dict(row)
        item.update({
            "id": item["event_id"], "campaign_name": f"{item['campaign_type']} #{item['campaign_id']}",
            "smpp_config_id": item["route_id"], "content": item.pop("message", ""),
            "status": item.pop("state"), "attempts": 0, "error_msg": item.pop("error", ""),
            "updated_at": item.pop("created_at"),
        })
        items.append(item)
    has_more = offset + len(items) < total
    return {"pipeline": "vnext", "total": total, "page": page, "page_size": page_size,
            "items": items, "next_cursor": page + 1 if has_more else None, "has_more": has_more}


def task_stats() -> dict:
    events = _progress_events()
    result: dict[str, int | float | str] = {}
    for event in events:
        state = str(event.get("state") or "unknown")
        result[state] = int(result.get(state, 0)) + 1
    result["total"] = len(events)
    latest = max((float(item.get("created_at") or 0) for item in events), default=0.0)
    writer_status = runtime().projection.snapshot()
    applied_at = float(writer_status.get("last_applied_at") or 0.0)
    result["projection_lag_seconds"] = max(0.0, latest - applied_at) if applied_at else 0.0
    result["pipeline"] = "vnext"
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
        scope = str(config.get("tps_scope") or "route_shared")
        result["routes"].append({
            key: config.get(key) for key in (
                "route_id", "host", "port", "source_addr", "bind_type", "pool_size",
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
