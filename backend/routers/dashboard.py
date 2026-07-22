from fastapi import APIRouter
from services import dashboard_service

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@router.get("/stats")
def dashboard_stats():
    return dashboard_service.get_stats()
