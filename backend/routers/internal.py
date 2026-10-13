"""Loopback-only maintenance endpoints used by local service tools."""

import os
import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from auth import is_internal_backup_request, is_internal_maintenance_request
from services.runtime_service import backup_database

router = APIRouter(prefix="/api/internal", tags=["Internal"])


class BackupRequest(BaseModel):
    target_dir: str = ""


@router.post("/backup")
def request_backup(request: Request, body: BackupRequest):
    if not is_internal_backup_request(request):
        raise HTTPException(status_code=403, detail="loopback backup token required")
    try:
        path = backup_database(body.target_dir, resolve_database=False)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"backup failed: {exc}") from exc
    return {"success": True, "path": path}


class AcceptanceHoldRequest(BaseModel):
    seconds: float = 1.0


@router.post("/acceptance-hold")
def acceptance_hold(request: Request, body: AcceptanceHoldRequest):
    """Hold a real owner connection only in the explicit local acceptance mode."""
    if os.environ.get("MC_ACCEPTANCE_HOOKS") != "1" or not is_internal_maintenance_request(
        request, "/api/internal/acceptance-hold"
    ):
        raise HTTPException(status_code=403, detail="acceptance hook disabled")
    import database

    seconds = min(30.0, max(0.1, float(body.seconds)))
    conn = database.get_connection().set_operation("acceptance.database_owner_hold")
    try:
        conn.execute("BEGIN TRANSACTION")
        time.sleep(seconds)
    finally:
        conn.rollback()
        conn.close()
    return {"success": True, "held_seconds": seconds}
