"""Small, local-only process diagnostics for the VNext runtime.

The diagnostic files deliberately contain process/runtime facts only.  They do
not copy route credentials, message bodies, or MSISDNs.  Each process owns one
file, so a child can update its own record without introducing another IPC
lock into the runtime being diagnosed.
"""

from __future__ import annotations

import json
import faulthandler
from functools import lru_cache
import multiprocessing
import os
import platform
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


# ``faulthandler`` has one process-wide destination.  Keep one persistent
# handle per process and let multiple logical roles in that process share it;
# otherwise an API-role registration could silently replace the canary-root
# destination, or disabling one role could close the only live crash log.
_NATIVE_FAULTHANDLER_HANDLES: dict[int, dict[str, object]] = {}
_NATIVE_FAULTHANDLER_ROLES: dict[str, int] = {}


def _runtime_version() -> str:
    try:
        from importlib.metadata import version

        return str(version("duckdb"))
    except Exception:
        return "unavailable"


@lru_cache(maxsize=1)
def _native_module_versions() -> dict[str, str]:
    """Return bounded package versions without importing optional extensions."""

    try:
        from importlib.metadata import version
    except Exception:
        return {}
    values = {}
    for package in ("duckdb", "cffi", "cryptography", "numpy", "pandas", "smpplib"):
        try:
            values[package] = str(version(package))
        except Exception:
            values[package] = "unavailable"
    return values


def _start_method(value: str | None = None) -> str:
    if value:
        return str(value)
    try:
        return str(multiprocessing.get_start_method(allow_none=True) or "unset")
    except (RuntimeError, ValueError):
        return "unavailable"


def _safe_role(role: str) -> str:
    value = "".join(character if character.isalnum() or character in "-_" else "_" for character in str(role))
    return value or "process"


def enable_native_fault_capture(
    root: str | os.PathLike[str] | None,
    role: str,
) -> Path | None:
    """Enable a persistent, role-specific Python faulthandler log.

    This is deliberately independent of the database stall watchdog.  The
    file handle remains open for the process lifetime so a native crash during
    startup or shutdown can still write the last Python-visible frames.  A
    Python frame is evidence of the runtime boundary only; it is never treated
    as a C/C++ faulting frame.
    """

    if not root:
        return None
    safe_role = _safe_role(role)
    process_id = os.getpid()
    existing = _NATIVE_FAULTHANDLER_HANDLES.get(process_id)
    if existing is not None:
        roles = existing.setdefault("roles", set())
        if isinstance(roles, set):
            roles.add(safe_role)
        _NATIVE_FAULTHANDLER_ROLES[safe_role] = process_id
        return Path(str(existing["path"]))
    directory = Path(root).resolve() / "native-crash"
    path = directory / f"{safe_role}-faulthandler.log"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        handle = path.open("w", encoding="utf-8")
        faulthandler.enable(handle, all_threads=True)
        _NATIVE_FAULTHANDLER_HANDLES[process_id] = {
            "handle": handle,
            "path": str(path),
            "roles": {safe_role},
        }
        _NATIVE_FAULTHANDLER_ROLES[safe_role] = process_id
        return path
    except (OSError, ValueError):
        try:
            handle.close()
        except (NameError, AttributeError, OSError):
            pass
        return None


def disable_native_fault_capture(role: str | None = None) -> None:
    """Close a capture handle after a graceful process shutdown."""

    if role:
        safe_role = _safe_role(role)
        process_ids = [_NATIVE_FAULTHANDLER_ROLES.get(safe_role)]
    else:
        process_ids = list(_NATIVE_FAULTHANDLER_HANDLES)
    for process_id in process_ids:
        if process_id is None:
            continue
        item = _NATIVE_FAULTHANDLER_HANDLES.get(process_id)
        if item is None:
            continue
        roles = item.get("roles")
        if role and isinstance(roles, set):
            roles.discard(_safe_role(role))
            _NATIVE_FAULTHANDLER_ROLES.pop(_safe_role(role), None)
            if roles:
                continue
        elif role:
            continue
        _NATIVE_FAULTHANDLER_HANDLES.pop(process_id, None)
        if isinstance(roles, set):
            for value in roles:
                _NATIVE_FAULTHANDLER_ROLES.pop(value, None)
        handle = item.get("handle")
        try:
            faulthandler.disable()
            handle.flush()
            handle.close()
        except (AttributeError, OSError, ValueError):
            pass


def has_native_fault_capture(role: str | None = None) -> bool:
    """Return whether this process has a live capture handle for *role*."""

    if role is None:
        return bool(_NATIVE_FAULTHANDLER_HANDLES)
    process_id = _NATIVE_FAULTHANDLER_ROLES.get(_safe_role(role))
    return process_id in _NATIVE_FAULTHANDLER_HANDLES if process_id is not None else False


def write_process_metadata(
    root: str | os.PathLike[str] | None,
    role: str,
    *,
    run_id: str = "",
    start_method: str | None = None,
    phase: str = "",
    status: str = "running",
    exitcode: int | None = None,
    progress: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
    process_pid: int | None = None,
    process_ppid: int | None = None,
) -> Path | None:
    """Atomically update one bounded metadata record and return its path."""

    if not root:
        return None
    directory = Path(root).resolve() / "processes"
    observed_pid = int(process_pid if process_pid is not None else os.getpid())
    observed_ppid = int(process_ppid if process_ppid is not None else os.getppid())
    path = directory / f"{_safe_role(role)}-{observed_pid}.json"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        current: dict[str, Any] = {}
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                current = {}
        native_fault_capture = has_native_fault_capture(role)
        # A parent supervisor may update a child PID's record after the child
        # exits. Preserve the child's own capture fact instead of replacing it
        # with the parent's role map state.
        if observed_pid != os.getpid() and "native_fault_capture" in current:
            native_fault_capture = bool(current.get("native_fault_capture"))
        current.update(
            {
                "pid": observed_pid,
                "ppid": observed_ppid,
                "observer_pid": os.getpid(),
                "role": _safe_role(role),
                "run_id": str(run_id or ""),
                "python": sys.version.split()[0],
                "executable": str(Path(sys.executable).resolve()),
                "platform": platform.platform(),
                "start_method": _start_method(start_method),
                "duckdb_version": _runtime_version(),
                "native_module_versions": _native_module_versions(),
                "faulthandler_enabled": bool(getattr(__import__("faulthandler"), "is_enabled", lambda: False)()),
                "native_fault_capture": native_fault_capture,
                "thread_count": threading.active_count(),
                "phase": str(phase or current.get("phase") or ""),
                "status": str(status or "running"),
                "updated_at": time.time(),
            }
        )
        if exitcode is not None:
            current["exitcode"] = int(exitcode)
        if progress is not None:
            current["last_progress"] = dict(progress)
        if extra:
            current.update(dict(extra))
        file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(directory))
        os.close(file_descriptor)
        temporary = Path(temporary_name)
        try:
            temporary.write_text(
                json.dumps(current, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return path
    except (OSError, TypeError, ValueError):
        return None


def read_process_metadata(root: str | os.PathLike[str] | None) -> list[dict[str, Any]]:
    """Read bounded role records for status/reporting; malformed files are ignored."""

    if not root:
        return []
    directory = Path(root).resolve() / "processes"
    values: list[dict[str, Any]] = []
    try:
        paths = sorted(directory.glob("*.json"))
    except OSError:
        return values
    for path in paths[:512]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                values.append(value)
        except (OSError, ValueError, TypeError):
            continue
    return values
