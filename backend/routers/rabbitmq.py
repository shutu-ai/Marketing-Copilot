from fastapi import APIRouter, HTTPException, Query
from models.rabbitmq import RabbitMQConfigCreate, RabbitMQConfigUpdate, PublishTestData
from services import rabbitmq_service, sms_sender

router = APIRouter(prefix="/api/rabbitmq", tags=["RabbitMQ"])


@router.get("/config")
def get_config():
    config = rabbitmq_service.get_config()
    if not config:
        return {
            "host": "localhost", "port": 5672, "virtual_host": "/",
            "username": "guest", "password": "guest",
            "exchange_name": "marketing_copilot", "queue_prefix": "mc_",
            "enabled": 0, "id": None,
        }
    return config


@router.post("/config")
def save_config(data: RabbitMQConfigCreate):
    return rabbitmq_service.save_config(data.model_dump())


@router.put("/config")
def update_config(data: RabbitMQConfigUpdate):
    existing = rabbitmq_service.get_config()
    if not existing:
        raise HTTPException(status_code=404, detail="先创建配置")

    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    if not update_data:
        return existing

    return rabbitmq_service.save_config(update_data)


@router.post("/test")
def test_publish(data: PublishTestData):
    success = rabbitmq_service.publish(data.routing_key, data.message)
    return {"success": success, "message": "消息已发送" if success else "发送失败"}


@router.get("/status")
def get_status():
    config = rabbitmq_service.get_config()
    if not config or not config["enabled"]:
        return {"connected": False, "message": "RabbitMQ未启用"}
    return {"connected": True, "config": config["host"] + ":" + str(config["port"])}


# --- SMS Log ---
@router.get("/sms-log")
def get_sms_log(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str = Query(None),
    msisdn: str = Query(None),
):
    return sms_sender.get_sms_log(page, page_size, status, msisdn)
