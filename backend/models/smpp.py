from pydantic import BaseModel
from typing import Optional


class SmppConfigCreate(BaseModel):
    name: str
    host: str
    port: int = 2775
    system_id: str
    password: str
    source_addr: str = ""
    bind_type: str = "transceiver"
    address_ton: int = 0
    address_npi: int = 0
    data_coding: int = 0
    pool_size: int = 1
    enquire_interval: int = 30
    timeout: int = 10
    tps: int = 20
    priority: int = 100
    reconnect_max_seconds: int = 60
    enabled: int = 1


class SmppConfigUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    system_id: Optional[str] = None
    password: Optional[str] = None
    source_addr: Optional[str] = None
    bind_type: Optional[str] = None
    address_ton: Optional[int] = None
    address_npi: Optional[int] = None
    data_coding: Optional[int] = None
    pool_size: Optional[int] = None
    enquire_interval: Optional[int] = None
    timeout: Optional[int] = None
    tps: Optional[int] = None
    priority: Optional[int] = None
    reconnect_max_seconds: Optional[int] = None
    enabled: Optional[int] = None


class SmppConfigResponse(BaseModel):
    id: int
    name: str
    host: str
    port: int
    system_id: str
    source_addr: str
    bind_type: str
    address_ton: int
    address_npi: int
    data_coding: int
    pool_size: int
    enquire_interval: int
    timeout: int
    enabled: int
    created_at: str
    updated_at: str


class SmppTestRequest(BaseModel):
    config_id: Optional[int] = None
    host: str
    port: int = 2775
    system_id: str
    password: str
    bind_type: str = "transceiver"
