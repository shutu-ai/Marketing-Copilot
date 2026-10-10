"""User and role management backed by the `users` table.

The first startup seeds an admin account from the environment
(``MC_ADMIN_USERNAME`` / ``MC_ADMIN_PASSWORD``). Afterwards users and roles
are managed through the admin UI; the environment variables only seed the
initial account.
"""

import logging
import re
from datetime import datetime

from auth import ROLES, admin_credentials, hash_password, verify_password
from database import write_transaction

logger = logging.getLogger(__name__)

USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,32}$")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _public(user: dict) -> dict:
    return {k: v for k, v in user.items() if k != "password_hash"}


def bootstrap_admin() -> None:
    """Create the initial admin from env credentials when no users exist yet."""
    username, password = admin_credentials()

    def write(conn):
        count = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
        if count:
            return False
        conn.execute(
            "INSERT INTO users (username, password_hash, role, display_name, enabled) VALUES (?, ?, 'admin', ?, 1)",
            (username, hash_password(password), username),
        )
        return True

    created = write_transaction(write, operation="user.bootstrap")
    if created:
        logger.warning("bootstrapped initial admin user %r from environment", username)


def get_user_by_username(username: str):
    from database import read_transaction

    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM users WHERE username=?", (username,)
        ).fetchone(),
        operation="user.by_username",
    )
    return dict(row) if row else None


def get_role(username: str) -> str:
    user = get_user_by_username(username)
    return user["role"] if user else ""


def authenticate(username: str, password: str):
    user = get_user_by_username(username)
    if not user or not user.get("enabled"):
        return None
    if not verify_password(password, user.get("password_hash", "")):
        return None
    write_transaction(
        lambda conn: conn.execute(
            "UPDATE users SET last_login_at=?, updated_at=? WHERE id=?",
            (_now(), _now(), user["id"]),
        ),
        attempts=2,
        operation="user.login",
    )
    return _public(user)


def list_users() -> list:
    from database import read_transaction

    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM users ORDER BY id"
        ).fetchall(),
        operation="user.list",
    )
    return [_public(dict(r)) for r in rows]


def _validate(username: str, password: str, role: str, display_name: str) -> None:
    if not USERNAME_RE.match(username):
        raise ValueError("username must be 3-32 chars: letters, digits, _ . -")
    if len(password or "") < 8:
        raise ValueError("password must be at least 8 characters")
    if role not in ROLES:
        raise ValueError(f"role must be one of: {', '.join(ROLES)}")
    if len(display_name or "") > 64:
        raise ValueError("display name is too long (max 64)")


def create_user(username: str, password: str, role: str, display_name: str = "", enabled: bool = True) -> dict:
    username = (username or "").strip().lower()
    display_name = (display_name or "").strip()
    _validate(username, password, role, display_name)
    def write(conn):
        row = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
        if row:
            raise ValueError("username already exists")
        conn.execute(
            "INSERT INTO users (username, password_hash, display_name, role, enabled) VALUES (?, ?, ?, ?, ?)",
            (username, hash_password(password), display_name, role, 1 if enabled else 0),
        )
        created = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return _public(dict(created))

    return write_transaction(write, operation="user.create")


def _admin_count(conn, exclude_id: int = None) -> int:
    if exclude_id:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE role='admin' AND enabled=1 AND id<>?", (exclude_id,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS n FROM users WHERE role='admin' AND enabled=1").fetchone()
    return row["n"]


def update_user(
    user_id: int,
    display_name: str = None,
    role: str = None,
    enabled: bool = None,
    password: str = None,
) -> dict:
    def write(conn):
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise LookupError("user not found")
        user = dict(row)
        if role is not None and role not in ROLES:
            raise ValueError(f"role must be one of: {', '.join(ROLES)}")
        if password:
            if len(password) < 8:
                raise ValueError("password must be at least 8 characters")
        next_enabled = user["enabled"] if enabled is None else (1 if enabled else 0)
        next_role = user["role"] if role is None else role
        if user["role"] == "admin" and (next_enabled == 0 or next_role != "admin"):
            if _admin_count(conn, exclude_id=user_id) == 0:
                raise ValueError("cannot disable or demote the last enabled admin")
        fields, values = [], []
        if display_name is not None:
            fields.append("display_name=?"); values.append(display_name.strip())
        if role is not None:
            fields.append("role=?"); values.append(role)
        if enabled is not None:
            fields.append("enabled=?"); values.append(1 if enabled else 0)
        if password:
            fields.append("password_hash=?"); values.append(hash_password(password))
        if not fields:
            return _public(user)
        fields.append("updated_at=?")
        values.append(_now())
        values.append(user_id)
        conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", tuple(values))
        updated = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return _public(dict(updated))

    return write_transaction(write, operation="user.update")


def delete_user(user_id: int, operator_username: str = None) -> None:
    def write(conn):
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise LookupError("user not found")
        user = dict(row)
        if operator_username and user["username"] == operator_username:
            raise ValueError("cannot delete the current user")
        if user["role"] == "admin" and user["enabled"]:
            if _admin_count(conn, exclude_id=user_id) == 0:
                raise ValueError("cannot delete the last enabled admin")
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))

    write_transaction(write, operation="user.delete")


def change_own_password(username: str, old_password: str, new_password: str) -> None:
    def write(conn):
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not row:
            raise LookupError("user not found")
        user = dict(row)
        if not verify_password(old_password or "", user.get("password_hash", "")):
            raise ValueError("current password is incorrect")
        if len(new_password or "") < 8:
            raise ValueError("password must be at least 8 characters")
        conn.execute(
            "UPDATE users SET password_hash=?, updated_at=? WHERE id=?",
            (hash_password(new_password), _now(), user["id"]),
        )

    write_transaction(write, operation="user.password")
