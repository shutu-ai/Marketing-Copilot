from pydantic import BaseModel, Field
from typing import Literal, Optional


class SmppConfigCreate(BaseModel):
    name: str
    host: str
    port: int = 2775
    system_id: str
    password: str
    source_addr: str = ""
    bind_type: Literal['transmitter', 'receiver', 'transceiver'] = "transceiver"
    address_ton: int = 0
    address_npi: int = 0
    data_coding: int = 0
    pool_size: int = Field(default=1, ge=1, le=20)
    window_size: int = Field(default=10, ge=1, le=100)
    enquire_interval: int = Field(default=30, ge=0, le=300)
    timeout: int = Field(default=10, ge=1, le=120)
    tps: int = Field(default=20, ge=0, le=10000)
    # VNext capacity is defined per physical SMPP bind.  Keep this field in
    # the API contract for compatibility, but do not allow route-shared
    # accounting to be configured through the application.
    tps_scope: Literal['per_bind'] = 'per_bind'
    priority: int = Field(default=100, ge=1, le=10000)
    reconnect_max_seconds: int = Field(default=60, ge=0, le=300)
    delivery_receipt_mode: Literal['required', 'optional', 'disabled'] = 'optional'
    enabled: int = 1


class SmppConfigUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    system_id: Optional[str] = None
    password: Optional[str] = None
    source_addr: Optional[str] = None
    bind_type: Optional[Literal['transmitter', 'receiver', 'transceiver']] = None
    address_ton: Optional[int] = None
    address_npi: Optional[int] = None
    data_coding: Optional[int] = None
    pool_size: Optional[int] = Field(default=None, ge=1, le=20)
    window_size: Optional[int] = Field(default=None, ge=1, le=100)
    enquire_interval: Optional[int] = Field(default=None, ge=0, le=300)
    timeout: Optional[int] = Field(default=None, ge=1, le=120)
    tps: Optional[int] = Field(default=None, ge=0, le=10000)
    tps_scope: Optional[Literal['per_bind']] = None
    priority: Optional[int] = Field(default=None, ge=1, le=10000)
    reconnect_max_seconds: Optional[int] = Field(default=None, ge=0, le=300)
    delivery_receipt_mode: Optional[Literal['required', 'optional', 'disabled']] = None
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
    window_size: int
    enquire_interval: int
    timeout: int
    tps: int
    tps_scope: Literal['per_bind']
    priority: int
    reconnect_max_seconds: int
    delivery_receipt_mode: Literal['required', 'optional', 'disabled']
    enabled: int
    created_at: str
    updated_at: str
    restart_required: bool = False
    effective_state: Literal['applied', 'pending_restart', 'next_start'] = 'next_start'
    hot_applied: bool = False


class SmppTestRequest(BaseModel):
    config_id: Optional[int] = None
    host: str
    port: int = 2775
    system_id: str
    password: str
    bind_type: Literal['transmitter', 'receiver', 'transceiver'] = "transceiver"
