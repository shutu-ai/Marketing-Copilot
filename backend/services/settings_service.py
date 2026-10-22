import logging
import re

from database import get_connection, read_transaction, write_transaction


logger = logging.getLogger(__name__)

RETENTION_SETTING_DEFAULTS = {
    # VNext ages its task index and progress projection with the same
    # seven-day default.  Keep the legacy fallback aligned with that value.
    "sms_task_retention_days": "7",
    "realtime_file_retention_days": "1",
    "realtime_output_retention_days": "30",
    "realtime_match_chunk_size": "5000",
    "realtime_whitelist_build_chunk_size": "10000",
    "realtime_whitelist_build_memory_limit": "4GB",
    "realtime_whitelist_build_threads": "1",
    "realtime_index_build_timeout": "7200",
    "realtime_whitelist_upload_timeout": "3600",
    "realtime_whitelist_retention_days": "7",
}

_INTEGER_SETTING_RANGES = {
    "sms_task_retention_days": (1, 3650),
    "realtime_file_retention_days": (1, None),
    "realtime_output_retention_days": (1, None),
    "realtime_match_chunk_size": (500, 100000),
    "realtime_whitelist_build_chunk_size": (1000, 100000),
    "realtime_whitelist_build_threads": (1, 16),
    "realtime_index_build_timeout": (0, 86400),
    "realtime_whitelist_upload_timeout": (1, 86400),
    "realtime_whitelist_retention_days": (1, 3650),
}
_MEMORY_LIMIT_RE = re.compile(
    r"^(?:\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)|"
    r"\d+(?:\.\d+)?\s*%)$",
    re.IGNORECASE,
)


def _normalize_setting_value(key: str, value: str) -> str:
    """Validate values that are exposed by the Basic Settings page.

    Runtime consumers already have bounded fallbacks, but silently storing a
    malformed value makes a successful save misleading.  Validation happens
    at the control-plane boundary and does not add work to the VNext send
    path.
    """

    text = str(value if value is not None else "").strip()
    if key in {"batch_base_dir", "realtime_base_dir"}:
        # An empty path intentionally keeps the runtime fallback behavior.
        return text
    if key == "realtime_whitelist_build_memory_limit":
        if not _MEMORY_LIMIT_RE.fullmatch(text):
            raise ValueError(
                "realtime_whitelist_build_memory_limit must be a size such as 4GB or a percentage"
            )
        return text
    if key in _INTEGER_SETTING_RANGES:
        try:
            number = int(text)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be an integer") from exc
        minimum, maximum = _INTEGER_SETTING_RANGES[key]
        if number < minimum or (maximum is not None and number > maximum):
            bound = f"{minimum}-{maximum}" if maximum is not None else f">={minimum}"
            raise ValueError(f"{key} must be in range {bound}")
        return str(number)
    return text


def get_setting(key: str, default: str = "") -> str:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT value FROM app_settings WHERE key=?", (key,)
        ).fetchone(),
        operation="settings.get",
    )
    return row["value"] if row else default


def set_setting(key: str, value: str) -> dict:
    key = str(key or "").strip()
    value = _normalize_setting_value(key, value)
    if key == "log_level":
        from services.runtime_service import normalize_log_level, set_log_level
        value = normalize_log_level(value)
    if key == "detail_logging":
        from services.runtime_service import normalize_detail_logging, set_detail_logging
        value = normalize_detail_logging(value)
    def write(conn):
        conn.execute(
            "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?",
            (key, value, value),
        )

    write_transaction(write, operation="setting.update")
    if key == "log_level":
        set_log_level(value)
    elif key == "detail_logging":
        set_detail_logging(value)
    elif key == "sms_task_retention_days":
        # The VNext projection child owns DuckDB and already performs bounded,
        # idle-only aging.  Send the new value through that control channel so
        # changing retention does not restart workers or touch the send path.
        try:
            from services import vnext_service
            vnext_service.apply_retention_days(value)
        except Exception as exc:
            # The setting remains durable and is picked up on the next VNext
            # start/maintenance pass if the live runtime is unavailable.
            logger.debug("Could not apply VNext retention immediately: %s", exc)
    return {"key": key, "value": value}


def get_all() -> list:
    rows = read_transaction(
        lambda conn: conn.execute("SELECT * FROM app_settings ORDER BY key").fetchall(),
        operation="settings.list",
    )
    return [dict(r) for r in rows]
