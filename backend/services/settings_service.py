from database import get_connection


def get_setting(key: str, default: str = "") -> str:
    conn = get_connection()
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> dict:
    conn = get_connection()
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?",
        (key, value, value)
    )
    conn.commit()
    conn.close()
    return {"key": key, "value": value}


def get_all() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM app_settings ORDER BY key").fetchall()
    conn.close()
    return [dict(r) for r in rows]
