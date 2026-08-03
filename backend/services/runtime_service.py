import atexit
import logging
import os
import shutil
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path

logger = logging.getLogger(__name__)
_lock_path = Path(os.environ.get("MC_INSTANCE_LOCK", Path(tempfile.gettempdir()) / "marketing-copilot.lock"))
_owns_lock = False


def _process_is_running(pid: int) -> bool:
    """Return whether *pid* is alive without signalling it on Windows."""
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        # Access denied still means the process exists; an invalid PID does not.
        return ctypes.get_last_error() == 5
    try:
        exit_code = wintypes.DWORD()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))) and exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def configure_logging():
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.environ.get("MC_LOG_LEVEL", "INFO").upper(), logging.INFO))
    if not any(isinstance(item, RotatingFileHandler) for item in root.handlers):
        log_path = Path(os.environ.get("MC_LOG_PATH", Path(__file__).resolve().parents[1] / "logs" / "app.log"))
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_path, maxBytes=int(os.environ.get("MC_LOG_MAX_BYTES", "10485760")),
                                      backupCount=int(os.environ.get("MC_LOG_BACKUPS", "10")), encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root.addHandler(handler)
    # Per-message detail logs (SMS body, RabbitMQ payload, etc.) are high-frequency
    # and can be disabled in production with MC_DETAIL_LOGGING=0.
    detail_logger = logging.getLogger("mc.detail")
    if os.environ.get("MC_DETAIL_LOGGING", "0") == "1":
        detail_logger.setLevel(logging.INFO)
    else:
        detail_logger.setLevel(logging.WARNING)
    detail_logger.propagate = True


def acquire_instance_lock():
    global _owns_lock
    if os.environ.get("MC_ALLOW_MULTI_INSTANCE") == "1":
        return
    _lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(_lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            pid = int(_lock_path.read_text(encoding="ascii").strip())
        except (ValueError, OSError):
            pid = 0
        if _process_is_running(pid):
            raise RuntimeError(f"another Marketing Copilot instance is running (pid={pid})")
        _lock_path.unlink(missing_ok=True)
        try:
            fd = os.open(str(_lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError("another Marketing Copilot instance acquired the lock") from exc
    with os.fdopen(fd, "w", encoding="ascii") as stream:
        stream.write(str(os.getpid()))
    _owns_lock = True


def release_instance_lock():
    global _owns_lock
    if _owns_lock:
        try: _lock_path.unlink(missing_ok=True)
        finally: _owns_lock = False


def backup_database(target_dir: str = "") -> str:
    import database
    source = Path(database.get_database_file())
    if str(source) == ":memory:" or not source.exists():
        raise RuntimeError("database file is not available for backup")
    destination_dir = Path(target_dir or os.environ.get("MC_BACKUP_DIR", source.parent / "backups"))
    destination_dir.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    target = destination_dir / f"{source.stem}-{datetime.now().strftime('%Y%m%d-%H%M%S')}{source.suffix}"
    conn = database.get_connection()
    conn.execute("CHECKPOINT")
    conn.close()
    with database._lock:
        shutil.copy2(source, target)
    keep = int(os.environ.get("MC_BACKUP_KEEP", "14"))
    backups = sorted(destination_dir.glob(f"{source.stem}-*{source.suffix}"), reverse=True)
    for old in backups[keep:]: old.unlink(missing_ok=True)
    return str(target)


atexit.register(release_instance_lock)
