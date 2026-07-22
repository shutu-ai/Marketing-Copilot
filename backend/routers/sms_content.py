from fastapi import APIRouter, HTTPException, Query
from models.sms_content import (
    CampaignCreate, CampaignUpdate, SmsTemplateCreate, SmsTemplateUpdate
)
from services import sms_content_service, batch_campaign_service, realtime_campaign_service
from pydantic import BaseModel

router = APIRouter(prefix="/api/sms-content", tags=["SMS Content"])


class TemplateValidationRequest(BaseModel):
    content: str
    available_variables: list[str] = []


class TemplateRestoreRequest(BaseModel):
    version_no: int


@router.get("/marketing-campaigns")
def list_marketing_campaigns(type: str = Query("batch", alias="type")):
    if type == "realtime":
        return realtime_campaign_service.get_all()
    return batch_campaign_service.get_all()


@router.get("/campaigns")
def list_campaigns():
    return sms_content_service.get_all_campaigns()


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: int):
    campaign = sms_content_service.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="活动不存在")
    return campaign


@router.post("/campaigns")
def create_campaign(data: CampaignCreate):
    return sms_content_service.create_campaign(data)


@router.put("/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, data: CampaignUpdate):
    campaign = sms_content_service.update_campaign(campaign_id, data)
    if not campaign:
        raise HTTPException(status_code=404, detail="活动不存在")
    return campaign


@router.delete("/campaigns/{campaign_id}")
def delete_campaign(campaign_id: int):
    if not sms_content_service.delete_campaign(campaign_id):
        raise HTTPException(status_code=404, detail="活动不存在")
    return {"message": "删除成功"}


@router.get("/templates")
def list_all_templates():
    return sms_content_service.get_all_templates_with_campaigns()


@router.get("/templates-for-select")
def list_templates_for_select():
    return sms_content_service.get_all_templates_for_select()


@router.get("/campaigns/{campaign_id}/templates")
def list_templates(campaign_id: int):
    return sms_content_service.get_templates_by_campaign(campaign_id)


@router.post("/templates")
def create_template(data: SmsTemplateCreate):
    try:
        return sms_content_service.create_template(data)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.post("/templates/validate")
def validate_template(body: TemplateValidationRequest):
    return sms_content_service.validate_template(body.content, body.available_variables)


@router.get("/templates/{template_id}/versions")
def template_versions(template_id: int):
    return sms_content_service.get_template_versions(template_id)


@router.post("/templates/{template_id}/restore")
def restore_template(template_id: int, body: TemplateRestoreRequest):
    template = sms_content_service.restore_template_version(template_id, body.version_no)
    if not template:
        raise HTTPException(404, "Template or version not found")
    return template


@router.put("/templates/{template_id}")
def update_template(template_id: int, data: SmsTemplateUpdate):
    try:
        template = sms_content_service.update_template(template_id, data)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    if not template:
        raise HTTPException(status_code=404, detail="模板不存在")
    return template


@router.delete("/templates/{template_id}")
def delete_template(template_id: int):
    if not sms_content_service.delete_template(template_id):
        raise HTTPException(status_code=404, detail="模板不存在")
    return {"message": "删除成功"}


@router.post("/preview")
def preview_template(content: str, variables: str = "{}"):
    import json
    try:
        var_dict = json.loads(variables)
    except json.JSONDecodeError:
        var_dict = {}
    return sms_content_service.preview_template(content, var_dict)
