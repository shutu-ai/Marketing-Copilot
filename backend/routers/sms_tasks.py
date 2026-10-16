from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from database import write_transaction
from services import sms_task_service

router = APIRouter(prefix="/api/sms-tasks", tags=["SMS Tasks"])


class ExclusionRequest(BaseModel):
    msisdn: str
    reason: str = ""


@router.get("")
def list_tasks(page: int = 1, page_size: int = Query(50, le=200), status: str = "",
               campaign_type: str = "", smpp_config_id: int = Query(-1),
               cursor: Optional[int] = Query(None, ge=1)):
    from services import vnext_service
    if vnext_service.is_active():
        return vnext_service.list_tasks(page, page_size, status, campaign_type, smpp_config_id, cursor)
    return sms_task_service.list_tasks(
        page, page_size, status, campaign_type, smpp_config_id, cursor
    )


@router.get("/stats")
def stats():
    from services import vnext_service
    if vnext_service.is_active():
        return vnext_service.task_stats()
    return sms_task_service.task_stats()


@router.get("/dlr-codes")
def dlr_codes():
    return sms_task_service.DLR_STATE_MAP


@router.post("/{task_id}/retry")
def retry(task_id: int):
    from services import vnext_service
    if vnext_service.is_active():
        raise HTTPException(409, "VNext uses socket_written/unknown evidence; manual legacy retry is disabled")
    try:
        retried = sms_task_service.retry_task(task_id)
    except sms_task_service.TaskRetryRejected as exc:
        raise HTTPException(status_code=409, detail=exc.reason) from exc
    if not retried:
        raise HTTPException(404, "Task not found")
    return {"success": True}


@router.post("/{task_id}/deliver")
def confirm_delivered(task_id: int):
    from services import vnext_service
    if vnext_service.is_active():
        raise HTTPException(409, "VNext does not infer handset delivery from submit_sm")
    ok, message = sms_task_service.mark_delivered(task_id)
    if not ok:
        raise HTTPException(400, message)
    return {"success": True, "message": message}


@router.get("/exclusions/list")
def exclusions(page: int = 1, page_size: int = Query(50, le=200)):
    return sms_task_service.list_exclusions(page, page_size)


@router.post("/exclusions")
def add_exclusion(body: ExclusionRequest):
    msisdn = sms_task_service.normalize_msisdn(body.msisdn)
    if not msisdn:
        raise HTTPException(400, "Invalid msisdn")
    def write(conn):
        existing = conn.execute("SELECT id FROM exclusion_users WHERE msisdn=?", (msisdn,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE exclusion_users SET reason=?,enabled=1 WHERE msisdn=?",
                (body.reason, msisdn),
            )
        else:
            conn.execute(
                "INSERT INTO exclusion_users (msisdn,reason,enabled) VALUES (?,?,1)",
                (msisdn, body.reason),
            )

    write_transaction(write, operation="exclusion.upsert")
    return {"success": True, "msisdn": msisdn}


@router.delete("/exclusions/{exclusion_id}")
def delete_exclusion(exclusion_id: int):
    write_transaction(
        lambda conn: conn.execute("DELETE FROM exclusion_users WHERE id=?", (exclusion_id,)),
        operation="exclusion.delete",
    )
    return {"success": True}
