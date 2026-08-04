"""Authentication, tokens and audit for the Marketing Copilot admin UI.

Single-account admin model: credentials come from the environment file
(``MC_ADMIN_USERNAME`` / ``MC_ADMIN_PASSWORD``). Tokens are stateless
HMAC-SHA256 signatures so the service can restart without dropping sessions,
as long as the signing secret persists (``MC_AUTH_SECRET`` or
``$MC_STATE_DIR/auth_secret``).

Set ``MC_AUTH_DISABLED=1`` only for development/automated tests; production
deployments must keep it at ``0``.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Optional

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

TOKEN_TTL_SECONDS = max(300, int(os.environ.get("MC_AUTH_TOKEN_TTL_HOURS", "12")) * 3600)
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "Huawei@123"

# Paths reachable without a token. Health/readiness are needed by probes and
# by the update runner's post-restart health check.
PUBLIC_API_PATHS = {"/api/ready", "/api/health", "/api/auth/login"}

_login_failures: dict = {}
LOGIN_MAX_FAILURES = 5
LOGIN_WINDOW_SECONDS = 15 * 60


def auth_disabled() -> bool:
    return os.environ.get("MC_AUTH_DISABLED", "0") == "1"


def admin_credentials() -> tuple:
    return (
        os.environ.get("MC_ADMIN_USERNAME", DEFAULT_USERNAME),
        os.environ.get("MC_ADMIN_PASSWORD", DEFAULT_PASSWORD),
    )


def state_dir() -> Path:
    return Path(os.environ.get("MC_STATE_DIR", os.path.join(os.path.dirname(__file__), "data")))


def get_auth_secret() -> str:
    secret = os.environ.get("MC_AUTH_SECRET", "").strip()
    if secret:
        return secret
    path = state_dir() / "auth_secret"
    if path.is_file():
        value = path.read_text(encoding="ascii").strip()
        if value:
            return value
    path.parent.mkdir(parents=True, exist_ok=True)
    value = secrets.token_hex(32)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(value, encoding="ascii")
    try:
        os.replace(str(tmp), str(path))
        os.chmod(str(path), 0o600)
    except OSError:
        pass
    return value


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def create_token(username: str, role: str = "admin", ttl_seconds: Optional[int] = None) -> str:
    ttl = ttl_seconds or TOKEN_TTL_SECONDS
    payload = {"u": username, "r": role, "iat": int(time.time()), "exp": int(time.time()) + ttl}
    body = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(get_auth_secret().encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_b64encode(signature)}"


def verify_token(token: str) -> Optional[dict]:
    try:
        body, signature = token.split(".", 1)
        expected = _b64encode(
            hmac.new(get_auth_secret().encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(expected, signature):
            return None
        payload = json.loads(_b64decode(body))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


def get_current_user(request: Request) -> dict:
    if auth_disabled():
        return {"username": "system", "role": "admin"}
    header = request.headers.get("Authorization", "")
    if not header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated", headers={"WWW-Authenticate": "Bearer"})
    payload = verify_token(header[7:].strip())
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token", headers={"WWW-Authenticate": "Bearer"})
    return {"username": payload.get("u", "unknown"), "role": payload.get("r", "admin")}


async def auth_middleware(request: Request, call_next):
    if not auth_disabled():
        path = request.url.path
        if request.method != "OPTIONS" and path.startswith("/api") and path not in PUBLIC_API_PATHS:
            try:
                request.state.user = get_current_user(request)
            except HTTPException as exc:
                from fastapi.responses import JSONResponse

                return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)
    return await call_next(request)


def client_ip(request: Request) -> str:
    return request.client.host if request.client else ""


def login_limited(ip: str) -> bool:
    now = time.time()
    failures = [t for t in _login_failures.get(ip, []) if now - t < LOGIN_WINDOW_SECONDS]
    _login_failures[ip] = failures
    return len(failures) >= LOGIN_MAX_FAILURES


def record_login_failure(ip: str) -> None:
    _login_failures.setdefault(ip, []).append(time.time())


def record_audit(
    request: Request,
    action: str,
    resource: str = "",
    resource_id: str = "",
    detail: Optional[dict] = None,
    username: Optional[str] = None,
) -> None:
    try:
        from database import get_connection

        user = username or getattr(getattr(request, "state", None), "user", {}).get("username", "system")
        conn = get_connection()
        conn.execute(
            "INSERT INTO audit_logs (username, action, resource, resource_id, detail, ip_address) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user, action, resource, resource_id, json.dumps(detail or {}, ensure_ascii=False), client_ip(request)),
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Failed to write audit log")
