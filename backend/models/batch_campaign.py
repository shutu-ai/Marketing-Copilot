from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class BatchCampaignCreate(BaseModel):
    name: str
    start_time: Optional[str] = ""
    end_time: Optional[str] = ""
    send_start_time: Optional[str] = ""
    send_end_time: Optional[str] = ""
    send_days: Optional[list] = None
    batch_size: Optional[int] = 500
    campaign_dir: Optional[str] = None
    use_custom_directory: Optional[bool] = False
    sms_template_id: Optional[int] = None


class BatchCampaignUpdate(BaseModel):
    name: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: Optional[str] = None
    send_start_time: Optional[str] = None
    send_end_time: Optional[str] = None
    send_days: Optional[list] = None
    batch_size: Optional[int] = None
    campaign_dir: Optional[str] = None
    use_custom_directory: Optional[bool] = None
    sms_template_id: Optional[int] = None
