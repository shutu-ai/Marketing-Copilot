from pydantic import BaseModel
from typing import Optional, List


class CampaignCreate(BaseModel):
    name: str
    description: str = ""
    marketing_type: str = "batch"
    marketing_campaign_id: int = 0
    language: str = "English"
    content: str = ""


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    enabled: Optional[int] = None
    marketing_type: Optional[str] = None
    marketing_campaign_id: Optional[int] = None


class CampaignResponse(BaseModel):
    id: int
    name: str
    description: str
    enabled: int
    marketing_type: str
    marketing_campaign_id: int
    created_at: str
    updated_at: str


class SmsTemplateCreate(BaseModel):
    campaign_id: int
    language: str
    content: str = ""
    source_addr: str = ""
    enabled: int = 1


class SmsTemplateUpdate(BaseModel):
    language: Optional[str] = None
    content: Optional[str] = None
    source_addr: Optional[str] = None
    enabled: Optional[int] = None


class SmsTemplateResponse(BaseModel):
    id: int
    campaign_id: int
    language: str
    content: str
    source_addr: str
    enabled: int
    created_at: str
    updated_at: str


class SmsTemplatePreview(BaseModel):
    content: str
    variables: dict
    rendered: str
    character_count: int
    sms_segments: int
