from fastapi import APIRouter, HTTPException
from models.db_config import DbConfigCreate, DbConfigUpdate
from services import db_config
from database import reconnect, init_db, set_database_file

router = APIRouter(prefix="/api/db-config", tags=["Database Config"])


@router.get("")
def get_config():
    cfg = db_config.get_config()
    return cfg


@router.post("")
def save_config(data: DbConfigCreate):
    success, message = db_config.test_connection(data.model_dump())
    if not success:
        raise HTTPException(status_code=400, detail=f"Connection test failed: {message}")
    body = data.model_dump(exclude_unset=True)
    cfg = db_config.save_config(body)
    if cfg.get("db_path"):
        reconnect(cfg["db_path"])
        set_database_file(cfg["db_path"])
    init_db()
    return cfg


@router.put("")
def update_config(data: DbConfigUpdate):
    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    if not update_data:
        return db_config.get_config()
    success, message = db_config.test_connection(update_data)
    if not success:
        raise HTTPException(status_code=400, detail=f"Connection test failed: {message}")
    cfg = db_config.save_config(update_data)
    if cfg.get("db_path"):
        reconnect(cfg["db_path"])
        set_database_file(cfg["db_path"])
    init_db()
    return cfg


@router.post("/test")
def test_config(data: DbConfigCreate):
    success, message = db_config.test_connection(data.model_dump())
    return {"success": success, "message": message}
