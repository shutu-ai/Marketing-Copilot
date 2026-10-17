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
    try:
        return smpp_service.create_config(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/configs/{config_id}")
def update_config(config_id: int, data: SmppConfigUpdate):
    try:
        config = smpp_service.update_config(config_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


@router.delete("/configs/{config_id}")
def delete_config(config_id: int):
    if not smpp_service.delete_config(config_id):
        raise HTTPException(status_code=404, detail="配置不存在")
    try:
        from services import vnext_service
        restart_required = vnext_service.is_active()
    except Exception:
        restart_required = False
    return {
        "message": "删除成功",
        "restart_required": restart_required,
        "effective_state": "pending_restart" if restart_required else "next_start",
    }


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
        scope = str(item.get("tps_scope") or "route_shared")
        configured_tps = float(item.get("tps") or 0) if item.get("enabled") else 0.0
        pool_size = max(1, int(item.get("pool_size") or 1))
        item["configured_route_tps"] = configured_tps if scope == "route_shared" else 0.0
        item["configured_per_bind_tps"] = configured_tps if scope == "per_bind" else None
        live_limits = [
            float(conn.get("effective_total_limit_tps") or 0)
            for conn in connections if conn.get("effective_total_limit_tps") is not None
        ]
        item["effective_total_limit_tps"] = (
            live_limits[0] if live_limits else (
                configured_tps * pool_size if scope == "per_bind" else configured_tps
            )
        )
        item["tps_scope"] = scope
        item["tps_unit"] = "submit_sm_parts/s"
        item["observed_parts_tps"] = item.get("submit_sm_tps_1m", 0.0)
        item["configured_tps"] = item["configured_route_tps"]
        item["connected_tps"] = item["effective_total_limit_tps"] if connected_count else 0.0
    result["summary"]["configured_route_tps"] = sum(
        item.get("configured_route_tps", 0.0) for item in result["items"]
    )
    result["summary"]["effective_total_limit_tps"] = sum(
        item.get("effective_total_limit_tps", 0.0) for item in result["items"]
    )
    scopes = {str(item.get("tps_scope") or "route_shared") for item in result["items"]}
    result["summary"]["tps_scope"] = next(iter(scopes)) if len(scopes) == 1 else "mixed"
    result["summary"]["tps_unit"] = "submit_sm_parts/s"
    result["summary"]["observed_parts_tps"] = result["summary"].get("submit_sm_tps_1m", 0.0)
    return result
