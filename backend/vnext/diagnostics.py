"""Small, local-only process diagnostics for the VNext runtime.

The diagnostic files deliberately contain process/runtime facts only.  They do
not copy route credentials, message bodies, or MSISDNs.  Each process owns one
file, so a child can update its own record without introducing another IPC
lock into the runtime being diagnosed.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import platform
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


def _runtime_version() -> str:
    try:
        from importlib.metadata import version

        return str(version("duckdb"))
    except Exception:
        return "unavailable"


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
                "faulthandler_enabled": bool(getattr(__import__("faulthandler"), "is_enabled", lambda: False)()),
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
