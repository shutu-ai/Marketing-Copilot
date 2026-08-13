import os
import tempfile
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from models.batch_campaign import BatchCampaignCreate, BatchCampaignUpdate
from services import batch_campaign_service

router = APIRouter(prefix="/api/batch-campaigns", tags=["Batch Campaign"])


@router.get("")
def list_campaigns(page: int = 1, page_size: int = Query(50, le=200)):
    return batch_campaign_service.list_campaigns(page, page_size)


@router.get("/{campaign_id}")
def get_campaign(campaign_id: int):
    campaign = batch_campaign_service.get_one(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="活动不存在")
    return campaign


@router.post("")
def create_campaign(data: BatchCampaignCreate):
    try:
        return batch_campaign_service.create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.put("/{campaign_id}")
def update_campaign(campaign_id: int, data: BatchCampaignUpdate):
    try:
        campaign = batch_campaign_service.update(campaign_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not campaign:
        raise HTTPException(status_code=404, detail="活动不存在")
    return campaign


@router.post("/{campaign_id}/generate-batches")
def generate_campaign_batches(campaign_id: int, batch_size: int = 500):
    result = batch_campaign_service.generate_batches(campaign_id, batch_size)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/{campaign_id}/batches")
def list_batches(campaign_id: int):
    return batch_campaign_service.get_batches(campaign_id)


@router.post("/batches/{batch_id}/process")
def process_batch(batch_id: int):
    return batch_campaign_service.process_batch(batch_id)


@router.post("/{campaign_id}/copy")
def copy_campaign(campaign_id: int):
    campaign = batch_campaign_service.copy(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="活动不存在")
    return campaign


@router.get("/{campaign_id}/detail")
def campaign_detail(campaign_id: int):
    detail = batch_campaign_service.get_detail(campaign_id)
    if not detail:
        raise HTTPException(status_code=404, detail="活动不存在")
    return detail


@router.get("/{campaign_id}/prospects")
def list_prospects(campaign_id: int, search: str = "", field: str = "", value: str = "", page: int = 1, page_size: int = 50):
    return batch_campaign_service.get_prospects(campaign_id, search, field, value, page, page_size)


@router.delete("/{campaign_id}")
def delete_campaign(campaign_id: int):
    if not batch_campaign_service.delete(campaign_id):
        raise HTTPException(status_code=404, detail="活动不存在")
    return {"message": "删除成功"}


@router.post("/{campaign_id}/upload")
def upload_customer_list(campaign_id: int, file: UploadFile = File(...)):
    try:
        campaign = batch_campaign_service.get_one(campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="活动不存在")
        ext = os.path.splitext(file.filename or "file.csv")[1].lower()
        if ext != ".csv":
            raise HTTPException(status_code=400, detail="仅支持 CSV 文件")
        fd, tmp_path = tempfile.mkstemp(suffix=ext)
        os.close(fd)
        try:
            content = file.file.read()
            with open(tmp_path, "wb") as f:
                f.write(content)
            result = batch_campaign_service.upload_customer_list(campaign_id, tmp_path, file.filename or "file.csv")
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
