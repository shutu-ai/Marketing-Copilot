import os

import tempfile

from fastapi import APIRouter, HTTPException, Query, UploadFile, File

from fastapi.responses import FileResponse

from models.realtime_campaign import RealtimeCampaignCreate, RealtimeCampaignUpdate

from services import realtime_campaign_service



router = APIRouter(prefix="/api/realtime-campaigns", tags=["Realtime Campaign"])





@router.get("")

def list_campaigns(page: int = 1, page_size: int = Query(50, le=200)):

    return realtime_campaign_service.list_campaigns(page, page_size)





@router.get("/{campaign_id}")

def get_campaign(campaign_id: int):

    campaign = realtime_campaign_service.get_one(campaign_id)

    if not campaign:

        raise HTTPException(status_code=404, detail="Campaign not found")

    return campaign





@router.post("")

def create_campaign(data: RealtimeCampaignCreate):

    return realtime_campaign_service.create(data)





@router.put("/{campaign_id}")

def update_campaign(campaign_id: int, data: RealtimeCampaignUpdate):

    try:

        campaign = realtime_campaign_service.update(campaign_id, data)

    except ValueError as e:

        raise HTTPException(status_code=400, detail=str(e))

    if not campaign:

        raise HTTPException(status_code=404, detail="Campaign not found")

    return campaign





@router.post("/{campaign_id}/copy")

def copy_campaign(campaign_id: int):

    campaign = realtime_campaign_service.copy(campaign_id)

    if not campaign:

        raise HTTPException(status_code=404, detail="Campaign not found")

    return campaign





@router.get("/{campaign_id}/detail")

def campaign_detail(campaign_id: int):

    detail = realtime_campaign_service.get_detail(campaign_id)

    if not detail:

        raise HTTPException(status_code=404, detail="Campaign not found")

    return detail





@router.delete("/{campaign_id}")

def delete_campaign(campaign_id: int):

    if not realtime_campaign_service.delete(campaign_id):

        raise HTTPException(status_code=404, detail="Campaign not found")

    return {"message": "Deleted"}





@router.post("/{campaign_id}/upload")

def upload_customer_list(campaign_id: int, file: UploadFile = File(...)):

    try:

        campaign = realtime_campaign_service.get_one(campaign_id)

        if not campaign:

            raise HTTPException(status_code=404, detail="Campaign not found")

        ext = os.path.splitext(file.filename or "file.csv")[1].lower()

        if ext not in (".csv", ".xlsx", ".xls"):

            raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

        fd, tmp_path = tempfile.mkstemp(suffix=ext)

        os.close(fd)

        try:

            content = file.file.read()

            with open(tmp_path, "wb") as f:

                f.write(content)

            result = realtime_campaign_service.upload_customer_list(campaign_id, tmp_path, file.filename or "file.csv")

        finally:

            try:

                if os.path.exists(tmp_path):

                    os.unlink(tmp_path)

            except Exception:

                pass

        return result

    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")





@router.post("/{campaign_id}/receive-file")

def receive_file(campaign_id: int, file: UploadFile = File(...)):

    ext = os.path.splitext(file.filename or "file.csv")[1].lower()

    if ext not in (".csv", ".xlsx", ".xls"):

        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

    fd, tmp_path = tempfile.mkstemp(suffix=ext)

    os.close(fd)

    try:

        content = file.file.read()

        with open(tmp_path, "wb") as f:

            f.write(content)

        result = realtime_campaign_service.receive_file(campaign_id, tmp_path, file.filename or "file.csv")

    finally:

        try:

            if os.path.exists(tmp_path):

                os.unlink(tmp_path)

        except Exception:

            pass

    if not result["success"]:

        raise HTTPException(status_code=400, detail=result["message"])

    return result





@router.get("/{campaign_id}/files")

def list_files(campaign_id: int, page: int = 1, page_size: int = Query(50, le=200)):

    return realtime_campaign_service.get_realtime_files(campaign_id, page, page_size)


@router.get("/{campaign_id}/sent-records")

def sent_records(campaign_id: int, page: int = 1, page_size: int = Query(50, le=200)):

    return realtime_campaign_service.get_realtime_sent_records(campaign_id, page, page_size)



@router.get("/{campaign_id}/sent-records/{filename}")

def download_sent_record(campaign_id: int, filename: str):

    campaign = realtime_campaign_service.get_one(campaign_id)

    if not campaign:

        raise HTTPException(status_code=404, detail="Campaign not found")

    safe_name = os.path.basename(filename)

    path = os.path.join(campaign.get("campaign_dir", ""), "output", safe_name)

    if not os.path.isfile(path):

        raise HTTPException(status_code=404, detail="Sent record not found")

    return FileResponse(path, filename=safe_name)





@router.get("/{campaign_id}/stats")

def stats(campaign_id: int):

    return realtime_campaign_service.get_realtime_stats(campaign_id)

