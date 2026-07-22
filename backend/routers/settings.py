from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services import settings_service

router = APIRouter(prefix="/api/settings", tags=["Settings"])


class SettingUpdate(BaseModel):
    value: str


@router.get("")
def list_settings():
    return settings_service.get_all()


@router.get("/{key}")
def get_setting(key: str):
    val = settings_service.get_setting(key)
    return {"key": key, "value": val}


@router.put("/{key}")
def update_setting(key: str, data: SettingUpdate):
    if not key.strip():
        raise HTTPException(status_code=400, detail="Key is required")
    return settings_service.set_setting(key, data.value)
