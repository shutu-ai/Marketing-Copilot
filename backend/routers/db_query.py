from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from services import db_query_service

router = APIRouter(prefix="/api/db-query", tags=["Database Query"])


class QueryRequest(BaseModel):
    sql: str
    params: Optional[List] = None


@router.get("/tables")
def list_tables():
    return db_query_service.get_tables()


@router.post("/execute")
def execute_query(data: QueryRequest):
    sql = data.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL cannot be empty")
    return db_query_service.execute_query(sql, data.params)
