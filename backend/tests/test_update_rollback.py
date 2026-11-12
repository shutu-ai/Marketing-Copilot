from __future__ import annotations

import json
import importlib.util
import os
from types import SimpleNamespace
from pathlib import Path

import pytest

from services import runtime_service, update_service


def _pretend_linux(monkeypatch):
    monkeypatch.setattr(
        update_service,
        "os",
        SimpleNamespace(
            name="posix",
            environ=os.environ,
            path=os.path,
            replace=os.replace,
        ),
    )


def _load_update_runner():
    path = Path(__file__).parents[1] / "tools" / "apply_update.py"
    spec = importlib.util.spec_from_file_location("test_apply_update_runner", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _make_backup(root, name, source_version, target_version, created_at):
    backup = root / name
    (backup / "backend").mkdir(parents=True)
    (backup / "frontend").mkdir()
    (backup / update_service.BACKUP_MANIFEST_NAME).write_text(
        json.dumps(
            {
                "kind": update_service.BACKUP_KIND,
                "source_version": source_version,
                "target_version": target_version,
                "created_at": created_at,
            }
        ),
        encoding="utf-8",
    )
    return backup


def test_rollback_selects_direct_predecessor_not_manual_backup(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_UPDATE_DIR", str(tmp_path / "updates"))
    backups = tmp_path / "updates" / "backups"
    backups.mkdir(parents=True)
    (backups / "rollback-20261012170000-7986e9d").mkdir()
    expected = _make_backup(
        backups,
        "20261111233956",
        "20261111-b2609f62",
        "20261111-b78df055",
        "2026-11-11 23:39:56",
    )
    _make_backup(
        backups,
        "20261111172036",
        "20261111-424a9e3f",
        "20261111-b2609f62",
        "2026-11-11 17:20:36",
    )

    _pretend_linux(monkeypatch)
    monkeypatch.setattr(
        update_service,
        "version_info",
        lambda: {"version": "20261111-b78df055", "git_sha": "b78df055"},
    )
    monkeypatch.setattr(update_service, "load_state", lambda: {})
    monkeypatch.setattr(update_service, "drain_business", lambda: {})
    monkeypatch.setattr(runtime_service, "backup_database", lambda: "db-backup")
    monkeypatch.setattr(update_service, "_spawn_runner", lambda: None)
    captured = {}
    monkeypatch.setattr(update_service, "save_state", lambda state: captured.update(state))

    result = update_service.rollback(operator="test")

    assert result["state"]["backup_dir"] == str(expected)
    assert captured["version"] == "20261111-b2609f62"
    assert captured["previous_version"] == "20261111-b78df055"
    assert captured["rollback_from_version"] == "20261111-b78df055"


def test_rollback_refuses_unverified_legacy_backup(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_UPDATE_DIR", str(tmp_path / "updates"))
    legacy = tmp_path / "updates" / "backups" / "rollback-20261012170000-7986e9d"
    (legacy / "backend").mkdir(parents=True)
    (legacy / "frontend").mkdir()
    _pretend_linux(monkeypatch)
    monkeypatch.setattr(
        update_service,
        "version_info",
        lambda: {"version": "20261111-b78df055", "git_sha": "b78df055"},
    )
    monkeypatch.setattr(update_service, "load_state", lambda: {})

    with pytest.raises(LookupError, match="no verified rollback backup"):
        update_service.rollback()


def test_cleanup_only_prunes_generated_backups_and_protects_active_one(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_UPDATE_DIR", str(tmp_path / "updates"))
    monkeypatch.setenv("MC_UPDATE_KEEP_BACKUPS", "1")
    backups = tmp_path / "updates" / "backups"
    backups.mkdir(parents=True)
    active = _make_backup(
        backups,
        "20261111172036",
        "20261111-424a9e3f",
        "20261111-b2609f62",
        "2026-11-11 17:20:36",
    )
    newest = _make_backup(
        backups,
        "20261111233956",
        "20261111-b2609f62",
        "20261111-b78df055",
        "2026-11-11 23:39:56",
    )
    manual = backups / "rollback-20261012170000-7986e9d"
    manual.mkdir()
    monkeypatch.setattr(
        update_service,
        "load_state",
        lambda: {"status": "swapping", "backup_dir": str(active)},
    )

    update_service.cleanup_staging()

    assert active.is_dir()
    assert newest.is_dir()
    assert manual.is_dir()


def test_runner_rejects_backup_for_another_current_version(tmp_path):
    runner = _load_update_runner()
    backup = _make_backup(
        tmp_path,
        "20261111233956",
        "20261111-b2609f62",
        "20261111-b78df055",
        "2026-11-11 23:39:56",
    )

    with pytest.raises(RuntimeError, match="target version mismatch"):
        runner.validate_backup_for_state(
            backup,
            {
                "action": "rollback",
                "version": "20261111-b2609f62",
                "rollback_from_version": "20261111-other",
            },
            rollback=True,
        )
