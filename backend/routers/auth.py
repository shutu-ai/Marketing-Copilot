from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth import (
    TOKEN_TTL_SECONDS,
    client_ip,
    create_token,
    get_current_user,
    login_limited,
    record_audit,
    record_login_failure,
)
from services import user_service

router = APIRouter(prefix="/api/auth", tags=["Auth"])


class LoginRequest(BaseModel):
    username: str = ""
    password: str = ""


@router.post("/login")
def login(data: LoginRequest, request: Request):
    ip = client_ip(request)
    if login_limited(ip):
        record_audit(request, "login", detail={"result": "rate_limited", "ip": ip}, username=data.username or "unknown")
        raise HTTPException(status_code=429, detail="Too many login attempts, try again later")
    user = user_service.authenticate((data.username or "").strip().lower(), data.password or "")
    if not user:
        record_login_failure(ip)
        record_audit(request, "login", detail={"result": "failed", "ip": ip}, username=data.username or "unknown")
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_token(user["username"], role=user["role"])
    record_audit(request, "login", detail={"result": "success", "role": user["role"]}, username=user["username"])
    return {"token": token, "username": user["username"], "role": user["role"], "expires_in": TOKEN_TTL_SECONDS}


@router.post("/logout")
def logout(request: Request, user: dict = Depends(get_current_user)):
    record_audit(request, "logout", username=user["username"])
    return {"ok": True}


@router.get("/me")
def me(user: dict = Depends(get_current_user)):
    return user
