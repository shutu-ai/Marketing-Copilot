from fastapi import APIRouter, HTTPException
from models.db_config import DbConfigCreate, DbConfigUpdate
from services import db_config
from database import get_database_file

router = APIRouter(prefix="/api/db-config", tags=["Database Config"])


@router.get("")
def get_config():
    cfg = db_config.get_config()
    cfg["active_db_path"] = get_database_file()
    cfg["storage_mode"] = "canonical"
    return cfg


@router.post("")
def save_config(data: DbConfigCreate):
    del data
    raise HTTPException(
        status_code=409,
        detail="Canonical control database path is fixed; run the offline cutover tool to migrate it.",
    )


@router.put("")
def update_config(data: DbConfigUpdate):
    del data
    raise HTTPException(
        status_code=409,
        detail="Canonical control database path is fixed; run the offline cutover tool to migrate it.",
    )


@router.post("/test")
def test_config(data: DbConfigCreate):
    success, message = db_config.test_connection(data.model_dump())
    return {"success": success, "message": message}
