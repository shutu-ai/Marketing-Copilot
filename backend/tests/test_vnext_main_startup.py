from __future__ import annotations

import importlib

from database import get_database_file, set_database_file
from services import db_config


def test_main_startup_owns_vnext_runtime_without_legacy_workers(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "app.duckdb"))
    monkeypatch.setenv("MC_DB_CONFIG_FILE", str(tmp_path / "db-config.json"))
    monkeypatch.setenv("MC_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("MC_VNEXT_RUN_ROOT", str(tmp_path / "vnext"))
    monkeypatch.setenv("MC_VNEXT_WORKERS", "1")
    monkeypatch.setenv("MC_PIPELINE_MODE", "vnext")
    monkeypatch.setenv("MC_AUTH_DISABLED", "1")

    original_db = get_database_file()
    isolated_db = str(tmp_path / "app.duckdb")
    set_database_file(isolated_db)
    monkeypatch.setattr(db_config, "get_effective_db_path", lambda seed: seed)
    main = importlib.import_module("main")
    main.startup()
    try:
        status = main.vnext_router.pipeline_status()
        tasks = main.sms_tasks_router.list_tasks(
            page=1, page_size=20, status="", campaign_type="", smpp_config_id=-1, cursor=None,
        )
        assert status["pipeline"] == "vnext"
        assert status["supervisor"]["worker_count"] == 1
        assert tasks["pipeline"] == "vnext"
    finally:
        main.shutdown()
        set_database_file(original_db)
