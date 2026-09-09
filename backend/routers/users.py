from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth import get_current_user, record_audit
from services import user_service

router = APIRouter(prefix="/api/users", tags=["Users"], dependencies=[Depends(get_current_user)])


class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "marketing"
    display_name: str = ""
    enabled: bool = True


class UserUpdate(BaseModel):
    display_name: str = None
    role: str = None
    enabled: bool = None
    password: str = None


class ChangePassword(BaseModel):
    old_password: str
    new_password: str


def _operator(request: Request) -> str:
    return getattr(getattr(request, "state", None), "user", {}).get("username", "system")


@router.get("")
def list_users(request: Request):
    return user_service.list_users()


@router.post("")
def create_user(data: UserCreate, request: Request):
    try:
        user = user_service.create_user(data.username, data.password, data.role, data.display_name, data.enabled)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "user_create", resource="user", resource_id=str(user["id"]),
                 detail={"username": user["username"], "role": user["role"]})
    return user


@router.put("/{user_id}")
def update_user(user_id: int, data: UserUpdate, request: Request):
    try:
        user = user_service.update_user(
            user_id,
            display_name=data.display_name,
            role=data.role,
            enabled=data.enabled,
            password=data.password,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "user_update", resource="user", resource_id=str(user_id),
                 detail={"username": user["username"], "role": user["role"], "enabled": user["enabled"]})
    return user


@router.delete("/{user_id}")
def delete_user(user_id: int, request: Request):
    try:
        user_service.delete_user(user_id, operator_username=_operator(request))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "user_delete", resource="user", resource_id=str(user_id))
    return {"ok": True}


@router.put("/me/password")
def change_own_password(data: ChangePassword, request: Request):
    username = _operator(request)
    try:
        user_service.change_own_password(username, data.old_password, data.new_password)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(request, "user_change_password", resource="user", resource_id=username,
                 username=username)
    return {"ok": True}
