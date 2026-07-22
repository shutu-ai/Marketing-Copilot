from pydantic import BaseModel
from typing import Optional, List


class UploadHistoryResponse(BaseModel):
    id: int
    filename: str
    total_rows: int
    success_rows: int
    failed_rows: int
    status: str
    import_mode: str
    error_detail: str
    created_at: str


class WhitelistUserResponse(BaseModel):
    id: int
    msisdn: str
    region: str
    language: str
    status: str
    upload_id: int
    created_at: str


class PreviewData(BaseModel):
    headers: List[str]
    rows: List[dict]
    total: int


class UploadResult(BaseModel):
    upload_id: int
    filename: str
    total_rows: int
    success_rows: int
    failed_rows: int
    errors: List[dict]
