import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from auth import get_current_user, record_audit
from services import update_service

router = APIRouter(prefix="/api/update", tags=["Update"], dependencies=[Depends(get_current_user)])


class UploadStart(BaseModel):
    upload_id: str = ""
    filename: str = ""
    size: int = 0
    sha256: str = ""
    chunks: int = 1


def _operator(request: Request) -> str:
    return getattr(getattr(request, "state", None), "user", {}).get("username", "system")


@router.get("/status")
def status(request: Request):
    update_service.cleanup_staging()
    update_service.record_pending_result()
    return update_service.status()


@router.get("/uploads")
def recent_uploads(limit: int = Query(5, ge=1, le=20)):
    return update_service.recent_uploads(limit)


@router.post("/upload/start")
def upload_start(data: UploadStart, request: Request):
    try:
        upload_id = data.upload_id or f"upd-{int(time.time() * 1000)}"
        meta = update_service.create_upload(upload_id, data.filename, data.size, data.sha256, data.chunks)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "update_upload_start", resource="update", resource_id=upload_id,
                 detail={"filename": meta["filename"], "size": meta["size"]})
    return meta


@router.post("/upload/chunk")
async def upload_chunk(request: Request, upload_id: str = Query(...), chunk: int = Query(...)):
    body = await request.body()
    try:
        return update_service.save_chunk(upload_id, int(chunk), body)
    except (LookupError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/verify")
def verify(upload_id: str, request: Request):
    try:
        result = update_service.precheck(upload_id)
    except (LookupError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "update_verify", resource="update", resource_id=upload_id,
                 detail={"ok": result["ok"], "version": result["version"]})
    return result


@router.post("/apply")
def apply_update(upload_id: str, request: Request, force: bool = False):
    try:
        result = update_service.apply_update(upload_id, force=force, operator=_operator(request))
    except (LookupError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    record_audit(request, "update_apply", resource="update", resource_id=upload_id, detail={"force": force})
    return result


@router.post("/rollback")
def rollback(request: Request, force: bool = False):
    try:
        result = update_service.rollback(operator=_operator(request))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    record_audit(request, "update_rollback", resource="update", detail={"force": force})
    return result


@router.post("/cancel")
def cancel(upload_id: str, request: Request):
    try:
        update_service.cancel_upload(upload_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "update_cancel", resource="update", resource_id=upload_id)
    return {"ok": True}


@router.get("/history")
def history(limit: int = Query(50, ge=1, le=200)):
    return update_service.list_history(limit)
