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
LOG_LEVEL_NAMES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
DETAIL_LOGGER_NAME = "mc.detail"


def get_log_path() -> Path:
    """Absolute path of the application log file (RotatingFileHandler target)."""
    return Path(os.environ.get("MC_LOG_PATH", Path(__file__).resolve().parents[1] / "logs" / "app.log"))


def normalize_log_level(level: str) -> str:
    """Validate and normalize a log level name; raise ValueError when invalid."""
    normalized = (level or "").strip().upper()
    if normalized not in LOG_LEVEL_NAMES:
        raise ValueError(f"invalid log level: {level!r}, expected one of {', '.join(LOG_LEVEL_NAMES)}")
    return normalized


def get_log_level() -> str:
    """Current effective level of the root logger as a name (e.g. 'INFO')."""
    numeric = logging.getLogger().getEffectiveLevel()
    name = logging.getLevelName(numeric)
    return name if isinstance(name, str) and not name.startswith("Level ") else str(numeric)


def set_log_level(level: str) -> str:
    """Apply a validated log level to the root logger at runtime."""
    normalized = normalize_log_level(level)
    logging.getLogger().setLevel(getattr(logging, normalized))
    return normalized


def normalize_detail_logging(value) -> str:
    """Normalize an MC_DETAIL_LOGGING-style value to '1' or '0'."""
    if isinstance(value, bool):
        return "1" if value else "0"
    normalized = str(value or "").strip().lower()
    return "1" if normalized in ("1", "true", "yes", "on") else "0"


def get_detail_logging() -> str:
    """Current effective state of per-message detail logging ('1' or '0')."""
    detail_logger = logging.getLogger(DETAIL_LOGGER_NAME)
    return "1" if detail_logger.isEnabledFor(logging.INFO) else "0"


def set_detail_logging(value) -> str:
    """Enable or disable per-message detail logs at runtime."""
    normalized = normalize_detail_logging(value)
    detail_logger = logging.getLogger(DETAIL_LOGGER_NAME)
    detail_logger.setLevel(logging.INFO if normalized == "1" else logging.WARNING)
    detail_logger.propagate = True
    return normalized


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
        log_path = get_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_path, maxBytes=int(os.environ.get("MC_LOG_MAX_BYTES", "10485760")),
                                      backupCount=int(os.environ.get("MC_LOG_BACKUPS", "10")), encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root.addHandler(handler)
    # Per-message detail logs (SMS body, RabbitMQ payload, etc.) are high-frequency
    # and can be disabled in production with MC_DETAIL_LOGGING=0.
    set_detail_logging(os.environ.get("MC_DETAIL_LOGGING", "0"))


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


def backup_database(target_dir: str = "", *, resolve_database: bool = True) -> str:
    import database
    # The runtime path is immutable for the process and is selected by the
    # canonical MC_CONTROL_DB_PATH environment variable.  ``resolve_database``
    # is retained as a harmless CLI/API compatibility argument; it no longer
    # probes or switches to another database file.
    if resolve_database:
        try:
            from services import db_config as db_config_service
            db_config_service.record_canonical_path(database.get_database_file())
        except Exception:
            # A backup must not depend on the retired path-config sidecar.
            pass
    source = Path(database.get_database_file())
    if str(source) == ":memory:" or not source.exists():
        raise RuntimeError("database file is not available for backup")
    destination_dir = Path(target_dir or os.environ.get("MC_BACKUP_DIR", source.parent / "backups"))
    destination_dir.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    target = destination_dir / f"{source.stem}-{datetime.now().strftime('%Y%m%d-%H%M%S')}{source.suffix}"
    # Establish a real maintenance boundary from CHECKPOINT through the file
    # copy.  This drains tracked readers/writers before opening the raw
    # checkpoint connection and blocks new admissions until the copy is done.
    # The raw DuckDB connection is closed before copying because Windows
    # otherwise keeps an exclusive file handle open after CHECKPOINT.
    with database.maintenance_exclusive("backup.database"):
        config = database.prepare_duckdb_connection()
        conn = database.duckdb.connect(
            str(source), config=config
        )
        try:
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
        # The long-lived process owner also holds the database file on
        # Windows.  It is safe to close it only after the maintenance boundary
        # has drained all tracked transactions; the next DB request lazily
        # recreates it through database.get_connection().
        database.close_root_connection()
        shutil.copy2(source, target)
    keep = int(os.environ.get("MC_BACKUP_KEEP", "14"))
    backups = sorted(destination_dir.glob(f"{source.stem}-*{source.suffix}"), reverse=True)
    for old in backups[keep:]: old.unlink(missing_ok=True)
    return str(target)


atexit.register(release_instance_lock)
