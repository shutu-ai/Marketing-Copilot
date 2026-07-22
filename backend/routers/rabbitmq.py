from fastapi import APIRouter, HTTPException, Query
from models.rabbitmq import RabbitMQConfigCreate, RabbitMQConfigUpdate, SubscriptionCreate, PublishTestData
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


# --- Whitelist Subscriptions ---
@router.get("/subscriptions")
def list_subscriptions(whitelist_id: int = Query(None)):
    from database import get_connection
    conn = get_connection()
    if whitelist_id:
        rows = conn.execute(
            "SELECT * FROM whitelist_subscriptions WHERE whitelist_id=?", (whitelist_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM whitelist_subscriptions").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/subscriptions")
def create_subscription(data: SubscriptionCreate):
    from database import get_connection
    conn = get_connection()
    existing = conn.execute(
        "SELECT id FROM whitelist_subscriptions WHERE whitelist_id=? AND topic_pattern=?",
        (data.whitelist_id, data.topic_pattern)
    ).fetchone()
    if existing:
        conn.close()
        raise HTTPException(status_code=400, detail="该订阅已存在")

    cursor = conn.execute(
        "INSERT INTO whitelist_subscriptions (whitelist_id, topic_pattern, campaign_id) VALUES (?,?,?)",
        (data.whitelist_id, data.topic_pattern, data.campaign_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM whitelist_subscriptions WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    return dict(row)


@router.delete("/subscriptions/{sub_id}")
def delete_subscription(sub_id: int):
    from database import get_connection
    conn = get_connection()
    cursor = conn.execute("DELETE FROM whitelist_subscriptions WHERE id=?", (sub_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    if not deleted:
        raise HTTPException(status_code=404, detail="订阅不存在")
    return {"message": "删除成功"}


# --- SMS Log ---
@router.get("/sms-log")
def get_sms_log(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str = Query(None),
    msisdn: str = Query(None),
):
    return sms_sender.get_sms_log(page, page_size, status, msisdn)
