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
               cursor: Optional[int] = Query(None, ge=1), msisdn: str = Query("")):
    from services import vnext_service
    return vnext_service.list_tasks(
        page=page,
        page_size=page_size,
        status=status,
        campaign_type=campaign_type,
        smpp_config_id=smpp_config_id,
        cursor=cursor,
        msisdn=msisdn,
    )


@router.get("/stats")
def stats():
    from services import vnext_service
    return vnext_service.task_stats()


@router.get("/dlr-codes")
def dlr_codes():
    return sms_task_service.DLR_STATE_MAP


@router.post("/{task_id}/retry")
def retry(task_id: str):
    raise HTTPException(409, "VNext task retry is not supported; submit a new campaign batch")


@router.post("/{task_id}/deliver")
def confirm_delivered(task_id: str):
    raise HTTPException(409, "VNext does not infer handset delivery from submit_sm")


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
