import csv
import io
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from database import read_transaction
from services import p1_service
from services import task_archive_service
from services import vnext_service

router = APIRouter(prefix="/api/operations", tags=["Operations"])


class CampaignActionRequest(BaseModel):
    campaign_type: str
    campaign_id: int
    action: str


class BulkTaskRequest(BaseModel):
    action: str
    task_ids: list[int] = []
    campaign_type: str = ""
    campaign_id: int = 0


class ResolveRequest(BaseModel):
    resolved: bool = True


@router.post("/campaigns/action")
def campaign_action(body: CampaignActionRequest):
    try:
        return p1_service.campaign_action(body.campaign_type, body.campaign_id, body.action)
    except LookupError as exc:
        raise HTTPException(404, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/progress")
def progress(page: int = 1, page_size: int = Query(50, le=200)):
    return p1_service.list_campaign_progress(page, page_size)


@router.get("/progress/{campaign_type}/{campaign_id}")
def campaign_progress(campaign_type: str, campaign_id: int):
    try:
        return p1_service.campaign_progress(campaign_type, campaign_id)
    except (LookupError, ValueError) as exc:
        raise HTTPException(404, str(exc))


@router.post("/tasks/bulk")
def bulk_tasks(body: BulkTaskRequest):
    try:
        return p1_service.bulk_failed_tasks(body.action, body.task_ids, body.campaign_type, body.campaign_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/tasks/archive")
def failure_archive(page: int = 1, page_size: int = Query(50, le=200),
                    cursor: Optional[int] = Query(None, ge=1)):
    return p1_service.list_failure_archive(page, page_size, cursor)


@router.get("/tasks/failures")
def failed_tasks(page: int = 1, page_size: int = Query(50, le=200),
                 cursor: Optional[int] = Query(None, ge=1)):
    return p1_service.list_failed_tasks(page, page_size, cursor)


@router.get("/tasks/parquet-archives")
def parquet_archives(campaign_type: str = "", campaign_id: int = 0,
                     cursor: Optional[int] = Query(None, ge=1),
                     page_size: int = Query(50, le=200)):
    return task_archive_service.list_archives(campaign_type, campaign_id, cursor, page_size)


@router.get("/tasks/parquet-archives/items")
def parquet_archive_items(campaign_type: str = "", campaign_id: int = 0,
                          cursor: Optional[int] = Query(None, ge=1),
                          page_size: int = Query(50, le=200)):
    return task_archive_service.list_archived_tasks(
        campaign_type, campaign_id, cursor, page_size
    )


@router.get("/tasks/{task_id}/timeline")
def task_timeline(task_id: int):
    try:
        return p1_service.task_timeline(task_id)
    except LookupError as exc:
        raise HTTPException(404, str(exc))


@router.get("/exceptions")
def exceptions(resolved: Optional[bool] = None, severity: str = "", page: int = 1,
               page_size: int = Query(50, le=200)):
    return p1_service.list_events(resolved, severity, page, page_size)


@router.put("/exceptions/{event_id}/resolve")
def resolve_exception(event_id: int, body: ResolveRequest):
    if not p1_service.resolve_event(event_id, body.resolved):
        raise HTTPException(404, "event not found")
    return {"success": True, "resolved": body.resolved}


@router.get("/configuration-health")
def configuration_health(record_events: bool = False):
    return p1_service.configuration_health(record_events)


@router.get("/audits")
def audits(page: int = 1, page_size: int = Query(100, le=500), action: str = ""):
    return p1_service.list_audit_logs(page, page_size, action)


def _export_rows(report: str):
    if report == "tasks":
        return vnext_service.export_tasks()
    elif report == "files":
        query = "SELECT * FROM processed_files ORDER BY detected_at DESC"
    elif report == "failures":
        query = "SELECT * FROM failed_task_archive ORDER BY archived_at DESC"
    elif report == "audits":
        query = "SELECT * FROM audit_logs ORDER BY created_at DESC"
    else:
        raise ValueError("report must be tasks, files, failures or audits")
    return read_transaction(
        lambda conn: [dict(r) for r in conn.execute(query).fetchall()],
        operation=f"operations.export.{report}",
    )


@router.get("/export/{report}")
def export_report(report: str):
    try:
        rows = _export_rows(report)
    except vnext_service.TaskIndexUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": exc.code,
                "reason": exc.reason,
                "message": "VNext task index is unavailable; rebuild it offline before retrying",
            },
            headers={"Retry-After": "30"},
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value for key, value in row.items()})
    content = "\ufeff" + output.getvalue()
    headers = {"Content-Disposition": f'attachment; filename="marketing-copilot-{report}.csv"'}
    return StreamingResponse(iter([content]), media_type="text/csv; charset=utf-8", headers=headers)
