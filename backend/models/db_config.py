from pydantic import BaseModel
from typing import Optional


class DbConfigCreate(BaseModel):
    name: Optional[str] = None
    db_path: str = ""


class DbConfigUpdate(BaseModel):
    db_path: Optional[str] = None


class DbConfigResponse(BaseModel):
    db_path: str


class DbTestResult(BaseModel):
    success: bool
    message: str
