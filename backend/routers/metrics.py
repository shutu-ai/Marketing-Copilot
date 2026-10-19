import logging
import sys
import threading
import time
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from database import is_database_read_error, write_lock_status
from auth import record_audit
from services import (
    metrics_service, backpressure_service, ingestion_job_service,
    campaign_capacity_service,
)
from services import fast_send_service
from services.smpp_gateway import gateway
from services.task_worker import worker_status

router = APIRouter(tags=['capacity'])
logger = logging.getLogger(__name__)


class CampaignCapacityPolicyRequest(BaseModel):
    weight: float = Field(1.0, ge=0.1, le=100.0)
    min_parts_tps: float = Field(0.0, ge=0.0, le=100000.0)
    max_parts_tps: Optional[float] = Field(None, gt=0.0, le=100000.0)
    max_outstanding_parts: Optional[int] = Field(None, ge=1, le=10000000)
    max_pending_tasks: Optional[int] = Field(None, ge=1, le=100000000)
    max_ingest_files: Optional[int] = Field(None, ge=1, le=1000000)
    enabled: bool = True


def _smpp_unavailable_status() -> dict:
    return {
        "state": "unknown",
        "connected": False,
        "config_id": None,
        "active_config_id": None,
        "enabled_configs": None,
        "pools": [],
        "last_error": "DATABASE_READ_UNAVAILABLE",
    }


@router.get('/metrics', response_class=PlainTextResponse)
def prometheus_metrics():
    return metrics_service.prometheus_text()


@router.get('/api/metrics/status')
def capacity_status():
    vnext_pipeline = None
    try:
        from services import vnext_service
        if vnext_service.is_active():
            vnext_pipeline = vnext_service.status()
            supervisor = vnext_pipeline.get("supervisor") or {}
            worker_rows = supervisor.get("workers") or []
            workers = {
                "configured": int(supervisor.get("worker_count") or len(worker_rows)),
                "running": sum(1 for item in worker_rows if item.get("alive")),
                "workers": worker_rows,
                "dispatch": {"mode": "vnext"},
                "pipeline": "vnext",
            }
        else:
            workers = worker_status()
    except Exception:
        workers = worker_status()
    try:
        metrics_service.observe_runtime_state(workers['running'], allow_cached=True)
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Capacity metrics refresh skipped because database read failed: %s", exc)

    try:
        ingestion = ingestion_job_service.status()
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Capacity ingestion status unavailable: %s", exc)
        ingestion = {"pending": None, "error": "DATABASE_READ_UNAVAILABLE"}

    try:
        backpressure = backpressure_service.snapshot()
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Capacity backpressure status unavailable: %s", exc)
        backpressure = {
            "task_depth": None,
            "chunk_depth": None,
            "write_depth": None,
            "write_depths": {},
            "disk": {},
            "paused": {},
            "error": "DATABASE_READ_UNAVAILABLE",
        }
    try:
        if vnext_pipeline is not None:
            smpp = vnext_service.smpp_stats(vnext_pipeline)
        else:
            smpp = gateway.status()
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Capacity SMPP status unavailable: %s", exc)
        smpp = _smpp_unavailable_status()
    return {'workers': workers, 'smpp': smpp,
            'ingestion': ingestion,
            'backpressure': backpressure,
            'fast_send': fast_send_service.status(),
            'scheduler': campaign_capacity_service.status(),
            'database': {'write_lock': write_lock_status()},
            'metrics': metrics_service.snapshot(),
            'calculated_at': time.time()}


@router.get('/api/metrics/live')
def live_status():
    """Return bounded in-process counters for acceptance observers.

    This endpoint deliberately does not refresh task aggregates, inspect the
    task table, or read an event log.  The acceptance harness uses it while a
    load is running; the stopped-service reconciliation remains the
    authoritative correctness audit.
    """
    try:
        fast_send = fast_send_service.status()
    except Exception as exc:
        fast_send = {'error': f'{type(exc).__name__}: {exc}'}
    transport_snapshot = metrics_service.campaign_transport_snapshot()
    transport = [
        {
            'campaign_type': key[0],
            'campaign_id': key[1],
            'config_id': key[2],
            **value,
        }
        for key, value in transport_snapshot.items()
    ]
    return {
        'captured_at': time.time(),
        'observer_contract': 'in_process_counters_only',
        'campaign_transport': transport,
        'fast_send': fast_send,
        'workers': worker_status(),
    }


@router.get('/api/metrics/stall-diagnostics')
def stall_diagnostics():
    """Return bounded in-process state and all Python thread stacks.

    This endpoint is intentionally diagnostic-only.  It avoids opening a new
    DuckDB connection so a stalled writer can still expose its in-memory lock
    holder and the blocked call stacks to the local acceptance watchdog.
    """
    frames = sys._current_frames()
    threads = []
    for thread in threading.enumerate():
        frame = frames.get(thread.ident)
        threads.append({
            'name': thread.name,
            'ident': thread.ident,
            'daemon': thread.daemon,
            'alive': thread.is_alive(),
            'stack': ''.join(traceback.format_stack(frame)) if frame else '',
        })
    try:
        fast_send = fast_send_service.status()
    except Exception as exc:
        fast_send = {'error': f'{type(exc).__name__}: {exc}'}
    try:
        backpressure = backpressure_service.snapshot()
    except Exception as exc:
        backpressure = {'error': f'{type(exc).__name__}: {exc}'}
    return {
        'captured_at': time.time(),
        'database': {'write_lock': write_lock_status()},
        'threads': threads,
        'workers': worker_status(),
        'fast_send': fast_send,
        'backpressure': backpressure,
    }


@router.get('/api/capacity/campaigns')
def campaign_capacity(limit: int = Query(200, ge=1, le=1000)):
    return campaign_capacity_service.snapshot(limit)


@router.get('/api/capacity/policies')
def campaign_capacity_policies():
    return {"items": campaign_capacity_service.list_policies()}


@router.put('/api/capacity/policies/{campaign_type}/{campaign_id}')
def put_campaign_capacity_policy(campaign_type: str, campaign_id: int,
                                 body: CampaignCapacityPolicyRequest,
                                 request: Request):
    values = body.model_dump() if hasattr(body, "model_dump") else body.dict()
    try:
        item = campaign_capacity_service.upsert_policy(campaign_type, campaign_id, values)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    record_audit(
        request, "capacity_policy.update", campaign_type, str(campaign_id), values
    )
    return item


@router.delete('/api/capacity/policies/{campaign_type}/{campaign_id}')
def remove_campaign_capacity_policy(campaign_type: str, campaign_id: int,
                                    request: Request):
    deleted = campaign_capacity_service.delete_policy(campaign_type, campaign_id)
    if not deleted:
        raise HTTPException(404, "capacity policy not found")
    record_audit(
        request, "capacity_policy.delete", campaign_type, str(campaign_id), {}
    )
    return {"success": True}


@router.post('/api/metrics/alerts/evaluate')
def evaluate_alerts():
    workers = worker_status()
    return {'alerts': metrics_service.evaluate_alerts(workers['running'], gateway.status())}
