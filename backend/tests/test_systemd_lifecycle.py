import json
import sys
from argparse import Namespace
from pathlib import Path

from tools import install_systemd_service
from tools import recover_projection


def _args() -> Namespace:
    return Namespace(
        service_name="marketing-copilot.service",
        description="Marketing Copilot backend",
        user="marketing-copilot",
        group="marketing-copilot",
        env_file="/opt/marketing-copilot/marketing-copilot.env",
        exec_start="",
        host="",
        port="",
    )


def test_systemd_stops_app_children_and_isolates_update_runner():
    args = _args()
    root = Path("/opt/marketing-copilot")

    app_unit = install_systemd_service.build_unit(args, root)
    runner_unit = install_systemd_service.build_update_unit(args, root)
    recovery_unit = install_systemd_service.build_recovery_unit(args, root)

    assert "KillMode=control-group" in app_unit
    assert "TimeoutStopSec=90" in app_unit
    assert "MC_UPDATE_RUNNER_UNIT=marketing-copilot-update@apply.service" in app_unit
    assert "KillMode=process" not in app_unit
    assert "Type=oneshot" in runner_unit
    assert f"ExecStart={install_systemd_service.find_python(root)} -m tools.apply_update" in runner_unit
    assert "MC_SERVICE_UNIT=marketing-copilot.service" in runner_unit
    assert "Type=oneshot" in recovery_unit
    assert "tools.recover_projection" in recovery_unit
    assert "KillMode=control-group" in recovery_unit


def test_projection_recovery_can_promote_validated_output(monkeypatch, tmp_path):
    source_root = tmp_path / "vnext"
    source_projection = source_root / "projection"
    source_projection.mkdir(parents=True)
    (source_projection / "vnext-projection.duckdb").write_text("old", encoding="utf-8")

    output_root = tmp_path / ".vnext-recovery-ready"
    output_projection = output_root / "projection"
    output_projection.mkdir(parents=True)
    (output_projection / "vnext-projection.duckdb").write_text("new", encoding="utf-8")
    (output_root / "recovery-state.json").write_text(
        json.dumps({"ok": True, "phase": "validated"}), encoding="utf-8"
    )

    monkeypatch.setattr(recover_projection, "_service_is_active", lambda unit: False)
    monkeypatch.setattr(
        recover_projection,
        "_validate_projection",
        lambda path: {"vnext_progress_events": 1, "vnext_projection": 1, "vnext_task_index": 1},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recover_projection",
            "--root", str(source_root),
            "--output-root", str(output_root),
            "--promote-existing",
        ],
    )

    assert recover_projection.main() == 0
    assert (source_projection / "vnext-projection.duckdb").read_text(encoding="utf-8") == "new"
    assert json.loads((output_root / "recovery-state.json").read_text(encoding="utf-8"))["phase"] == "promoted"
    assert any(path.name.startswith(".vnext-projection-backup-") for path in tmp_path.iterdir())
