"""VNext pipeline status and control endpoints."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services import vnext_service


router = APIRouter(prefix="/api/vnext", tags=["VNext Pipeline"])


class CampaignStateRequest(BaseModel):
    state: str


@router.get("/status")
def pipeline_status():
    if not vnext_service.is_active():
        return {"pipeline": vnext_service.pipeline_mode(), "started": False}
    return vnext_service.status()


@router.get("/tasks")
def pipeline_tasks(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200),
                  status: str = "", campaign_type: str = "", route_id: int = Query(-1)):
    if not vnext_service.is_active():
        return {"pipeline": vnext_service.pipeline_mode(), "total": 0, "items": []}
    return vnext_service.list_tasks(page, page_size, status, campaign_type, route_id)


@router.get("/stats")
def pipeline_stats():
    if not vnext_service.is_active():
        return {"pipeline": vnext_service.pipeline_mode(), "total": 0}
    return vnext_service.task_stats()


@router.post("/campaigns/{campaign_type}/{campaign_id}/state")
def set_campaign_state(campaign_type: str, campaign_id: int, body: CampaignStateRequest):
    if not vnext_service.is_active():
        raise HTTPException(status_code=503, detail="VNext runtime is not active")
    try:
        return vnext_service.set_campaign_state(campaign_type, campaign_id, body.state)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/campaigns/{campaign_type}/{campaign_id}/pause")
def pause_campaign(campaign_type: str, campaign_id: int):
    return set_campaign_state(campaign_type, campaign_id, CampaignStateRequest(state="paused"))


@router.post("/campaigns/{campaign_type}/{campaign_id}/resume")
def resume_campaign(campaign_type: str, campaign_id: int):
    return set_campaign_state(campaign_type, campaign_id, CampaignStateRequest(state="running"))


@router.post("/campaigns/{campaign_type}/{campaign_id}/cancel")
def cancel_campaign(campaign_type: str, campaign_id: int):
    return set_campaign_state(campaign_type, campaign_id, CampaignStateRequest(state="cancelled"))


@router.get("/idle")
async def wait_for_idle(timeout: float = Query(30.0, ge=0.1, le=300.0)):
    if not vnext_service.is_active():
        raise HTTPException(status_code=503, detail="VNext runtime is not active")
    return await vnext_service.wait_for_idle(timeout)
