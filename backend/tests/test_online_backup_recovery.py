"""Deterministic online-copy and independent recovery regression."""

from __future__ import annotations

import json

import duckdb

import database
from services import db_config, runtime_service


def test_backup_copy_is_readable_after_owner_checkpoint(tmp_path, monkeypatch):
    db_path = tmp_path / "owner.duckdb"
    config_path = tmp_path / "db_config.json"
    backup_dir = tmp_path / "backups"
    monkeypatch.setattr(db_config, "CONFIG_PATH", str(config_path))
    database.set_database_file(str(db_path))

    conn = database.get_connection()
    try:
        conn.execute("CREATE TABLE backup_marker(event_id VARCHAR, value INTEGER)")
        conn.execute("INSERT INTO backup_marker VALUES ('online-001', 42)")
    finally:
        conn.close()

    backup_path = runtime_service.backup_database(str(backup_dir))
    assert config_path.exists()
    assert json.loads(config_path.read_text(encoding="utf-8"))["db_path"] == str(db_path)

    independent = duckdb.connect(backup_path, read_only=True)
    try:
        row = independent.execute(
            "SELECT event_id, value FROM backup_marker"
        ).fetchone()
        assert row == ("online-001", 42)
        try:
            assert str(independent.execute("PRAGMA integrity_check").fetchone()[0]).lower() == "ok"
        except duckdb.Error:
            # Older DuckDB builds may not expose integrity_check; the real
            # business-row read above remains the required recovery check.
            pass
    finally:
        independent.close()
