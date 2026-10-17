"""Run the isolated T7-4..T7-9 recovery scenarios without pytest.

The repository normally runs these functions through pytest.  This small
loopback runner exists for the offline EulerOS/AlmaLinux validation image,
where the available application wheelhouse intentionally does not contain
pytest.  It uses the same test functions and records a machine-readable
result; a failed function is never converted into a PASS.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback
import types

BACKEND_DIR = Path(__file__).resolve().parents[1]
TEST_PATH = BACKEND_DIR / "tests" / "test_vnext_t7_recovery_matrix.py"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


class _ExpectedRaises:
    def __init__(self, expected):
        self.expected = expected
        self.seen = False

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, _traceback):
        if exception_type is None:
            raise AssertionError(f"expected {self.expected.__name__} to be raised")
        if not issubclass(exception_type, self.expected):
            return False
        self.seen = True
        return True


def _load_matrix_module():
    shim = types.ModuleType("pytest")
    shim.raises = _ExpectedRaises
    sys.modules.setdefault("pytest", shim)
    spec = importlib.util.spec_from_file_location("vnext_t7_recovery_matrix_tests", TEST_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {TEST_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _df_type(path: Path) -> str:
    try:
        output = subprocess.check_output(["df", "-T", str(path)], text=True, stderr=subprocess.STDOUT)
        rows = output.splitlines()
        return rows[1].split()[1] if len(rows) > 1 and len(rows[1].split()) > 1 else "unknown"
    except (OSError, subprocess.SubprocessError, IndexError):
        return "unknown"


def run(args: argparse.Namespace) -> dict:
    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    module = _load_matrix_module()
    scenarios = [
        ("T7-4", module.test_t7_4_feeder_pause_then_resume_has_no_duplicate_source_events),
        ("T7-5", module.test_t7_5_route_a_failure_keeps_route_b_progressing),
        ("T7-6", module.test_t7_6_full_projection_hint_queue_keeps_journal_authoritative),
        ("T7-7", module.test_t7_7_journal_hard_limit_stops_admission_and_recovers_after_gc),
        ("T7-8", module.test_t7_8_spool_capacity_releases_on_day_reclaim_and_new_day_admits),
        ("T7-9", module.test_t7_9_cross_day_cursor_and_writer_restart_do_not_redispatch_or_double_count),
    ]
    results = []
    for name, scenario in scenarios:
        started = time.monotonic()
        case_root = run_root / name.lower()
        try:
            scenario(case_root)
        except BaseException as exc:  # preserve the concrete failure evidence
            results.append({
                "id": name,
                "status": "FAIL",
                "duration_seconds": round(time.monotonic() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=12),
                "run_root": str(case_root),
            })
        else:
            results.append({
                "id": name,
                "status": "PASS",
                "duration_seconds": round(time.monotonic() - started, 3),
                "run_root": str(case_root),
            })
    return {
        "schema": "vnext-t7-recovery-matrix-v1",
        "status": "PASS" if all(item["status"] == "PASS" for item in results) else "FAIL",
        "python": sys.version,
        "platform": platform.platform(),
        "duckdb_version": __import__("duckdb").__version__,
        "run_root": str(run_root),
        "filesystem_type": _df_type(run_root),
        "scenarios": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = run(args)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "status": result["status"],
        "filesystem_type": result["filesystem_type"],
        "scenarios": [(item["id"], item["status"]) for item in result["scenarios"]],
        "output": str(output),
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
