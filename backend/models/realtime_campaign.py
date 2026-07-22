from pydantic import BaseModel
from typing import Optional


class RealtimeCampaignCreate(BaseModel):
    name: str
    start_time: Optional[str] = ""
    end_time: Optional[str] = ""
    send_start_time: Optional[str] = ""
    send_end_time: Optional[str] = ""
    send_days: Optional[list] = None
    campaign_dir: Optional[str] = None
    sms_template_id: Optional[int] = None


class RealtimeCampaignUpdate(BaseModel):
    name: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: Optional[str] = None
    send_start_time: Optional[str] = None
    send_end_time: Optional[str] = None
    send_days: Optional[list] = None
    campaign_dir: Optional[str] = None
    sms_template_id: Optional[int] = None
