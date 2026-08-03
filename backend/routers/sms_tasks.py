from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from database import get_connection
from services import sms_task_service

router = APIRouter(prefix="/api/sms-tasks", tags=["SMS Tasks"])


class ExclusionRequest(BaseModel):
    msisdn: str
    reason: str = ""


@router.get("")
def list_tasks(page: int = 1, page_size: int = Query(50, le=200), status: str = "", campaign_type: str = ""):
    return sms_task_service.list_tasks(page, page_size, status, campaign_type)


@router.get("/stats")
def stats():
    return sms_task_service.task_stats()


@router.get("/dlr-codes")
def dlr_codes():
    return sms_task_service.DLR_STATE_MAP


@router.post("/{task_id}/retry")
def retry(task_id: int):
    if not sms_task_service.retry_task(task_id):
        raise HTTPException(404, "Task not found")
    return {"success": True}


@router.get("/exclusions/list")
def exclusions():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM exclusion_users ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/exclusions")
def add_exclusion(body: ExclusionRequest):
    msisdn = sms_task_service.normalize_msisdn(body.msisdn)
    if not msisdn:
        raise HTTPException(400, "Invalid msisdn")
    conn = get_connection()
    existing = conn.execute("SELECT id FROM exclusion_users WHERE msisdn=?", (msisdn,)).fetchone()
    if existing:
        conn.execute("UPDATE exclusion_users SET reason=?,enabled=1 WHERE msisdn=?", (body.reason, msisdn))
    else:
        conn.execute("INSERT INTO exclusion_users (msisdn,reason,enabled) VALUES (?,?,1)", (msisdn, body.reason))
    conn.commit()
    conn.close()
    return {"success": True, "msisdn": msisdn}


@router.delete("/exclusions/{exclusion_id}")
def delete_exclusion(exclusion_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM exclusion_users WHERE id=?", (exclusion_id,))
    conn.commit()
    conn.close()
    return {"success": True}
