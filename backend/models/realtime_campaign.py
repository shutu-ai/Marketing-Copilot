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
    use_custom_directory: Optional[bool] = False
    watch_dir: Optional[str] = ""
    watch_filename_prefix: Optional[str] = "Android_4G_Switch_Somali_"
    phone_prefix: Optional[str] = ""
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
    use_custom_directory: Optional[bool] = None
    watch_dir: Optional[str] = None
    watch_filename_prefix: Optional[str] = None
    phone_prefix: Optional[str] = None
    sms_template_id: Optional[int] = None
