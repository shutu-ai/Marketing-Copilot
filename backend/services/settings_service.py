from database import get_connection


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
