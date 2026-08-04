from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from services import log_service, runtime_service

router = APIRouter(prefix="/api/logs", tags=["Logs"])


@router.get("/files")
def list_files():
    return {"dir": str(log_service.get_log_dir()), "items": log_service.list_log_files()}


@router.get("/entries")
def entries(file: str = "", level: str = "", keyword: str = "",
            page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=500)):
    name = file or (log_service.list_log_files() or [{}])[0].get("name", "app.log")
    try:
        return log_service.read_entries(name, level, keyword, page, page_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/level")
def level():
    return {
        "level": runtime_service.get_log_level(),
        "detail_logging": runtime_service.get_detail_logging(),
    }


@router.get("/download")
def download(file: str = ""):
    name = file or (log_service.list_log_files() or [{}])[0].get("name", "app.log")
    path = log_service.resolve_file(name)
    if path is None:
        raise HTTPException(status_code=404, detail=f"log file not found: {name}")
    return FileResponse(path, filename=path.name, media_type="text/plain; charset=utf-8")
