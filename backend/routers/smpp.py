from fastapi import APIRouter, HTTPException
from models.smpp import SmppConfigCreate, SmppConfigUpdate, SmppTestRequest
from services import smpp_service
from services.smpp_gateway import gateway

router = APIRouter(prefix="/api/smpp", tags=["SMPP"])


@router.get("/configs")
def list_configs():
    return smpp_service.get_all_configs()


@router.get("/configs/{config_id}")
def get_config(config_id: int):
    config = smpp_service.get_config(config_id)
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


@router.post("/configs")
def create_config(data: SmppConfigCreate):
    return smpp_service.create_config(data)


@router.put("/configs/{config_id}")
def update_config(config_id: int, data: SmppConfigUpdate):
    config = smpp_service.update_config(config_id, data)
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


@router.delete("/configs/{config_id}")
def delete_config(config_id: int):
    if not smpp_service.delete_config(config_id):
        raise HTTPException(status_code=404, detail="配置不存在")
    return {"message": "删除成功"}


@router.post("/test")
async def test_connection(data: SmppTestRequest):
    return await smpp_service.test_connection(data.model_dump())


@router.get("/status")
def connection_status():
    return gateway.status()


@router.get("/stats")
def config_stats():
    """Per-SMSC processing statistics merged with live connection health."""
    result = smpp_service.get_config_stats()
    pool_map = {pool["config_id"]: pool for pool in gateway.status().get("pools", [])}
    for item in result["items"]:
        pool = pool_map.get(item["config_id"]) or {}
        connections = pool.get("connections") or []
        connected_count = sum(1 for conn in connections if conn.get("connected"))
        item["healthy"] = pool.get("healthy", False)
        item["connected_count"] = connected_count
        item["desired_pool_size"] = pool.get("desired_pool_size", item["pool_size"])
        item["cooldown_seconds"] = pool.get("cooldown_seconds", 0)
        item["last_error"] = "; ".join(
            str(conn.get("last_error") or "") for conn in connections if conn.get("last_error")
        )[:500]
        item["configured_tps"] = (item.get("tps") or 0) * max(1, int(item.get("pool_size") or 1))
        item["connected_tps"] = (item.get("tps") or 0) * max(1, connected_count)
    return result
