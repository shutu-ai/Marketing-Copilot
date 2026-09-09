import os
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from services import monitor_service

router = APIRouter(prefix="/api/monitor", tags=["Monitor"])


class ConfigReq(BaseModel):
    directory_path: str
    target_directories: list = []


class ConfigUpdateReq(BaseModel):
    directory_path: Optional[str] = None
    target_directories: Optional[list] = None
    enabled: Optional[int] = None


class PublishFileReq(BaseModel):
    file_id: int
    campaign_name: str = ""


class FileIdsReq(BaseModel):
    file_ids: list[int]


@router.get("/realtime")
def realtime_monitor(
    campaign_id: int = Query(None),
    source_page: int = Query(1, ge=1),
    source_page_size: int = Query(50, ge=1, le=200),
    processed_page: int = Query(1, ge=1),
    processed_page_size: int = Query(50, ge=1, le=200),
):
    return monitor_service.get_realtime_monitor(
        campaign_id,
        source_page,
        source_page_size,
        processed_page,
        processed_page_size,
    )


@router.post("/realtime/scan")
def scan_realtime_monitor(
    campaign_id: int = Query(None),
    source_page: int = Query(1, ge=1),
    source_page_size: int = Query(50, ge=1, le=200),
    processed_page: int = Query(1, ge=1),
    processed_page_size: int = Query(50, ge=1, le=200),
):
    return monitor_service.scan_realtime_watch_dir(
        campaign_id,
        source_page,
        source_page_size,
        processed_page,
        processed_page_size,
    )


@router.post("/realtime/files/{file_id}/retry")
def retry_realtime_file(file_id: int):
    result = monitor_service.retry_realtime_file(file_id)
    if result is None:
        raise HTTPException(status_code=404, detail="realtime ingestion file not found")
    if not result.get("success", True):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/realtime/files/retry")
def retry_realtime_files(body: FileIdsReq):
    return monitor_service.retry_realtime_files(body.file_ids)


@router.post("/realtime/files/archive")
def archive_realtime_files(body: FileIdsReq):
    return monitor_service.archive_realtime_files(body.file_ids)


@router.get("/configs")
def list_configs():
    return monitor_service.get_all_configs()


@router.post("/configs")
def add_config(body: ConfigReq):
    return monitor_service.add_config(body.directory_path, body.target_directories)


@router.put("/configs/{config_id}")
def update_config(config_id: int, body: ConfigUpdateReq):
    config = monitor_service.update_config(config_id, body.model_dump(exclude_unset=True))
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


@router.put("/configs/{config_id}/toggle")
def toggle_config(config_id: int, enabled: int = 1):
    config = monitor_service.toggle_config(config_id, enabled)
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


@router.delete("/configs/{config_id}")
def delete_config(config_id: int):
    if not monitor_service.delete_config(config_id):
        raise HTTPException(status_code=404, detail="配置不存在")
    return {"message": "删除成功"}


@router.post("/configs/{config_id}/scan")
def scan_directory(config_id: int):
    from database import get_connection
    conn = get_connection()
    row = conn.execute("SELECT * FROM monitor_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="配置不存在")
    config = dict(row)
    try:
        result = monitor_service.scan_directory(config_id, config["directory_path"])
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"config": config, **result}


@router.get("/files")
def list_files(status: str = Query(None), directory_path: str = Query(None)):
    return monitor_service.get_files(status, directory_path)


@router.post("/sync-source")
def sync_source():
    """Check source files; mark missing ones as 'distributed'."""
    return monitor_service.sync_source_files()


@router.put("/files/{file_id}/status")
def update_file_status(file_id: int, status: str):
    monitor_service.update_file_status(file_id, status)
    return {"message": "更新成功"}


@router.post("/files/{file_id}/distribute")
def distribute_file(file_id: int):
    result = monitor_service.distribute_file(file_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/files/publish")
def publish_file(body: PublishFileReq):
    from database import get_connection
    conn = get_connection()
    row = conn.execute("SELECT * FROM monitor_files WHERE id=?", (body.file_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="文件不存在")
    file_info = dict(row)
    file_path = os.path.join(file_info["directory_path"], file_info["filename"])
    result = monitor_service.process_file(body.file_id, file_path, body.campaign_name)
    return result
