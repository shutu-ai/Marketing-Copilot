from pydantic import BaseModel
from typing import Optional, List


class MonitorConfigCreate(BaseModel):
    directory_path: str


class MonitorConfigResponse(BaseModel):
    id: int
    directory_path: str
    enabled: int
    created_at: str


class MonitorFileResponse(BaseModel):
    id: int
    filename: str
    directory_path: str
    file_size: int
    row_count: int
    status: str
    created_at: str
    updated_at: str
