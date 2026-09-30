from database import get_connection

RETENTION_SETTING_DEFAULTS = {
    "sms_task_retention_days": "3",
    "realtime_file_retention_days": "1",
    "realtime_output_retention_days": "30",
    "realtime_match_chunk_size": "5000",
    "realtime_whitelist_build_chunk_size": "10000",
    "realtime_whitelist_build_memory_limit": "4GB",
    "realtime_whitelist_build_threads": "1",
    "realtime_index_build_timeout": "7200",
    "realtime_whitelist_retention_days": "7",
}


def get_setting(key: str, default: str = "") -> str:
    conn = get_connection()
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> dict:
    if key == "log_level":
        from services.runtime_service import normalize_log_level, set_log_level
        value = normalize_log_level(value)
    if key == "detail_logging":
        from services.runtime_service import normalize_detail_logging, set_detail_logging
        value = normalize_detail_logging(value)
    conn = get_connection()
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?",
        (key, value, value)
    )
    conn.commit()
    conn.close()
    if key == "log_level":
        set_log_level(value)
    elif key == "detail_logging":
        set_detail_logging(value)
    return {"key": key, "value": value}


def get_all() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM app_settings ORDER BY key").fetchall()
    conn.close()
    return [dict(r) for r in rows]
