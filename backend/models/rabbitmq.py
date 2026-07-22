from pydantic import BaseModel
from typing import Optional, List


class RabbitMQConfigCreate(BaseModel):
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    username: str = "guest"
    password: str = "guest"
    exchange_name: str = "marketing_copilot"
    queue_prefix: str = "mc_"


class RabbitMQConfigUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    virtual_host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    exchange_name: Optional[str] = None
    queue_prefix: Optional[str] = None
    enabled: Optional[int] = None


class RabbitMQConfigResponse(BaseModel):
    id: int
    host: str
    port: int
    virtual_host: str
    username: str
    password: str
    exchange_name: str
    queue_prefix: str
    enabled: int
    created_at: str
    updated_at: str


class SubscriptionCreate(BaseModel):
    whitelist_id: int
    topic_pattern: str
    campaign_id: Optional[int] = None


class SubscriptionResponse(BaseModel):
    id: int
    whitelist_id: int
    topic_pattern: str
    campaign_id: Optional[int]
    created_at: str


class PublishTestData(BaseModel):
    routing_key: str
    message: dict
