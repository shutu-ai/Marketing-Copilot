import json
import os
import logging
from typing import Optional

import duckdb

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
    return data or dict(DEFAULT_CONFIG)


def is_db_empty(path: str) -> Optional[bool]:
    """Return True when the DuckDB file holds no business data, False when it does,
    or None when the file is missing / cannot be opened."""
    if not path or not os.path.exists(path) or os.path.isdir(path):
        return None
    try:
        conn = duckdb.connect(path, read_only=True)
    except Exception as exc:
        logger.warning("Cannot open database for emptiness check %s: %s", path, exc)
        return None
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        known = [table for table in BUSINESS_TABLES if table in tables]
        if not known:
            return True
        total = 0
        for table in known:
            try:
                total += conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            except Exception:
                pass
        return total == 0
    finally:
        conn.close()


def get_effective_db_path(seed: str) -> str:
    """Resolve the database path to use, seeding it from *seed* on first use.

    Guard rules:
    - No persisted config (fresh deployment): persist *seed* and use it.
    - Persisted path missing: log an error and fall back to *seed* for this run
      (the persisted config is kept so the operator can fix it in the UI).
    - Persisted database is empty while *seed* contains business data: log an
      error, correct the persisted config, and use *seed* so a restart can never
      silently serve an empty database.
    """
    cfg = get_config()
    path = (cfg.get("db_path") or "").strip()
    if not path:
        path = seed
        cfg["db_path"] = path
        save_config(cfg)
        logger.info("Seeded database path %s into %s", path, CONFIG_PATH)
        return path

    if not os.path.exists(path):
        logger.error(
            "Persisted database path does not exist (%s); falling back to %s for this run. "
            "Fix the path in the Database Config page or remove %s.",
            path, seed, CONFIG_PATH,
        )
        return seed

    persisted_empty = is_db_empty(path)
    seed_empty = is_db_empty(seed)
    if persisted_empty is True and seed_empty is False:
        logger.error(
            "Persisted database %s is EMPTY while %s contains business data; "
            "falling back to %s and updating the persisted config to prevent "
            "serving an empty database after a restart.",
            path, seed, seed,
        )
        cfg["db_path"] = seed
        save_config(cfg)
        return seed

    logger.info("Using persisted database path %s", path)
    return path


def save_config(data: dict) -> dict:
    _write_file(data)
    logger.info(f"Database config saved to {CONFIG_PATH}")
    return data


def test_connection(data: dict) -> tuple[bool, str]:
    db_path = data.get("db_path", "")
    if not db_path:
        return False, "Database path is empty"
    if not os.path.exists(db_path):
        return False, f"Database file does not exist: {db_path}"
    if os.path.isdir(db_path):
        return False, "Database path points to a directory"
    try:
        # read-only + no auto-create: a typo must never silently create an empty DB
        conn = duckdb.connect(db_path, read_only=True)
        conn.execute("SELECT 1")
        conn.close()
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)
