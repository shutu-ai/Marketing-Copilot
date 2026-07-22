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
