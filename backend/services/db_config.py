import json
import os
import logging

import duckdb

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db_config.json")

DEFAULT_CONFIG = {
    "db_path": "",
}


def _read_file() -> dict:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read db_config.json: {e}")
    return {}


def _write_file(data: dict):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_config() -> dict:
    data = _read_file()
    return data or dict(DEFAULT_CONFIG)


def save_config(data: dict) -> dict:
    _write_file(data)
    logger.info(f"Database config saved to {CONFIG_PATH}")
    return data


def test_connection(data: dict) -> tuple[bool, str]:
    db_path = data.get("db_path", "")
    if not db_path:
        return False, "Database path is empty"
    try:
        conn = duckdb.connect(db_path)
        conn.execute("SELECT 1")
        conn.close()
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)
