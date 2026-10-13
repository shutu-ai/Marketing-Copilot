from pydantic import BaseModel
from typing import Optional


class SftpConnectionTest(BaseModel):
    campaign_id: Optional[int] = None
    watch_dir: str
    sftp_host: str
    sftp_port: int = 22
    sftp_username: str
    sftp_password: str = ""


class RealtimeCampaignCreate(BaseModel):
    name: str
    use_whitelist: Optional[bool] = True
    start_time: Optional[str] = ""
    end_time: Optional[str] = ""
    send_start_time: Optional[str] = ""
    send_end_time: Optional[str] = ""
    send_days: Optional[list] = None
    campaign_dir: Optional[str] = None
    use_custom_directory: Optional[bool] = False
    watch_source: Optional[str] = "local"
    watch_dir: Optional[str] = ""
    sftp_host: Optional[str] = ""
    sftp_port: Optional[int] = 22
    sftp_username: Optional[str] = ""
    sftp_password: Optional[str] = ""
    watch_filename_prefix: Optional[str] = "Android_4G_Switch_Somali_"
    phone_prefix: Optional[str] = ""
    sms_template_id: Optional[int] = None
    smpp_config_id: Optional[int] = None
    flash_sms: Optional[bool] = False


class RealtimeCampaignUpdate(BaseModel):
    name: Optional[str] = None
    use_whitelist: Optional[bool] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: Optional[str] = None
    send_start_time: Optional[str] = None
    send_end_time: Optional[str] = None
    send_days: Optional[list] = None
    campaign_dir: Optional[str] = None
    use_custom_directory: Optional[bool] = None
    watch_source: Optional[str] = None
    watch_dir: Optional[str] = None
    sftp_host: Optional[str] = None
    sftp_port: Optional[int] = None
    sftp_username: Optional[str] = None
    sftp_password: Optional[str] = None
    watch_filename_prefix: Optional[str] = None
    phone_prefix: Optional[str] = None
    sms_template_id: Optional[int] = None
    smpp_config_id: Optional[int] = None
    flash_sms: Optional[bool] = None
