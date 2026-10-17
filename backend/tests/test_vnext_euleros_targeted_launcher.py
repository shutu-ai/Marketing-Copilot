from __future__ import annotations

import shutil
import subprocess
import os
from argparse import Namespace
from pathlib import Path

import pytest

from tools.vnext_runtime_canary_acceptance import _validate_gate_contract


def _launcher_path() -> Path:
    package_root = Path(__file__).resolve().parents[2]
    candidates = (
        package_root / "deploy" / "vnext-runtime-canary-test-runner.sh",
        package_root / "bin" / "run-vnext-runtime-canary.sh",
    )
    return next(path for path in candidates if path.is_file())


def _args(**overrides) -> Namespace:
    values = {
        "gates": "r0,r1,r3,r4,r5,r6",
        "authorize_p5500": False,
        "stability_seconds": 0.0,
        "b2_seconds": 42.0,
    }
    values.update(overrides)
    return Namespace(**values)


def test_r2_requires_positive_authorization_and_is_not_silently_removed():
    with pytest.raises(ValueError, match="authorize-p5500"):
        _validate_gate_contract(_args(gates="r2"))
    assert _validate_gate_contract(_args(gates="r2", authorize_p5500=True)) == ["r2"]


def test_stability_gate_must_be_explicit():
    with pytest.raises(ValueError, match="explicit r7"):
        _validate_gate_contract(_args(stability_seconds=40.0))
    assert _validate_gate_contract(_args(gates="r7", stability_seconds=40.0)) == ["r7"]


def test_b2_is_an_explicit_300_message_boundary_diagnostic():
    assert _validate_gate_contract(_args(gates="b2")) == ["b2"]
    with pytest.raises(ValueError, match="only requested gate"):
        _validate_gate_contract(_args(gates="b2,r0"))
    with pytest.raises(ValueError, match="between 40 and 45"):
        _validate_gate_contract(_args(gates="b2", b2_seconds=46.0))


def test_launcher_contract_contains_all_frozen_canary_options_without_server_contact():
    source = _launcher_path().read_text(encoding="utf-8")
    for option in (
        "--authorize-p5500",
        "--route-tps",
        "--response-delay-ms",
        "--workers",
        "--partitions",
        "--projection-queue",
        "--max-inflight",
        "--prefetch",
        "--watchdog",
        "--b2-seconds",
    ):
        assert option in source
    assert 'requested_gates' in source
    assert '"executed_gates"' in Path(
        Path(__file__).resolve().parents[1] / "tools" / "vnext_runtime_canary_acceptance.py"
    ).read_text(encoding="utf-8")
    assert 'COMMAND+=(--authorize-p5500)' in source
    assert 'AUTHORIZE_P5500' in source


def test_launcher_has_valid_bash_syntax_when_bash_is_available():
    if os.name == "nt":
        pytest.skip("Windows bash wrapper cannot consume a native Windows path; validate in WSL")
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is not installed on this development host")
    completed = subprocess.run(
        [bash, "-n", str(_launcher_path())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
