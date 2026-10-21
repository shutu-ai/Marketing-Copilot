import json
import os
import logging

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get(
    "MC_DB_CONFIG_FILE",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "db_config.json"),
)

DEFAULT_CONFIG = {
    "db_path": "",
}

# Tables whose row counts tell whether the database actually holds business data.
# app_settings is deliberately excluded: init_db always seeds default settings, so
# it cannot tell a fresh-but-unused database apart from an empty or wrong one.
BUSINESS_TABLES = [
    "campaigns",
    "realtime_campaigns",
    "batch_campaigns",
    "smpp_configs",
    "sms_tasks",
]


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
    from database import get_database_file
    data.update({"db_path": get_database_file(), "storage_mode": "canonical"})
    return data


def get_effective_db_path(seed: str) -> str:
    """Return the process-selected canonical path without opening a file.

    Kept as a no-op shim for stopped-service test harnesses. Production
    startup no longer calls this resolver or consults a persisted path.
    """

    del seed
    from database import get_database_file
    return get_database_file()


def canonical_path() -> str:
    from database import get_database_file
    return get_database_file()


def record_canonical_path(path: str = "") -> dict:
    value = str(path or canonical_path())
    data = {"db_path": value, "storage_mode": "canonical"}
    _write_file(data)
    return data


def save_config(data: dict) -> dict:
    del data
    result = record_canonical_path()
    logger.info("Canonical database path recorded in %s", CONFIG_PATH)
    return result


def test_connection(data: dict) -> tuple[bool, str]:
    requested = str(data.get("db_path") or "").strip()
    active = canonical_path()
    if requested and os.path.abspath(requested) != os.path.abspath(active):
        return False, "The canonical control database path cannot be changed at runtime"
    if not os.path.isfile(active):
        return False, f"Canonical control database does not exist: {active}"
    return True, "Canonical control database is configured"
