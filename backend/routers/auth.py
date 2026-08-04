import hmac

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth import (
    TOKEN_TTL_SECONDS,
    admin_credentials,
    client_ip,
    create_token,
    get_current_user,
    login_limited,
    record_audit,
    record_login_failure,
)

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
    expected_user, expected_pass = admin_credentials()
    ok_user = hmac.compare_digest(data.username or "", expected_user)
    ok_pass = hmac.compare_digest(data.password or "", expected_pass)
    if not (ok_user and ok_pass):
        record_login_failure(ip)
        record_audit(request, "login", detail={"result": "failed", "ip": ip}, username=data.username or "unknown")
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_token(expected_user)
    record_audit(request, "login", detail={"result": "success"}, username=expected_user)
    return {"token": token, "username": expected_user, "role": "admin", "expires_in": TOKEN_TTL_SECONDS}


@router.post("/logout")
def logout(request: Request, user: dict = Depends(get_current_user)):
    record_audit(request, "logout", username=user["username"])
    return {"ok": True}


@router.get("/me")
def me(user: dict = Depends(get_current_user)):
    return user
