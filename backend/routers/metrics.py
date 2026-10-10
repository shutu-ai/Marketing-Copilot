import logging
import time
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
        smpp = gateway.status()
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Capacity SMPP status unavailable: %s", exc)
        smpp = _smpp_unavailable_status()
    return {'workers': workers, 'smpp': smpp,
            'ingestion': ingestion,
            'backpressure': backpressure,
            'scheduler': campaign_capacity_service.status(),
            'database': {'write_lock': write_lock_status()},
            'metrics': metrics_service.snapshot(),
            'calculated_at': time.time()}


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
