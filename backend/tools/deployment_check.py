"""Preflight checks for the supported Linux Python/systemd deployment."""
import argparse
import os
import platform
import sys
from pathlib import Path


def writable_parent(path: str) -> bool:
    target = Path(path).expanduser().resolve()
    parent = target if target.is_dir() else target.parent
    return parent.exists() and os.access(parent, os.W_OK)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-linux", action="store_true")
    args = parser.parse_args()
    failures = []
    if args.require_linux and platform.system() != "Linux": failures.append("operating system is not Linux")
    if sys.version_info < (3, 11) or sys.version_info >= (3, 13): failures.append("Python 3.11 or 3.12 is required")
    paths = {
        "database": os.environ.get("DUCKDB_PATH", ""),
        "log": os.environ.get("MC_LOG_PATH", ""),
        "instance lock": os.environ.get("MC_INSTANCE_LOCK", ""),
        "backup": os.environ.get("MC_BACKUP_DIR", ""),
    }
    for label, value in paths.items():
        if not value or not writable_parent(value): failures.append(f"{label} path is missing or not writable: {value}")
    frontend = Path(os.environ.get("MC_FRONTEND_DIR", ""))
    if not (frontend / "index.html").is_file(): failures.append(f"frontend build is missing: {frontend / 'index.html'}")
    try:
        from database import init_db, get_connection
        init_db(); conn = get_connection(); conn.execute("SELECT 1"); conn.close()
    except Exception as exc: failures.append(f"database check failed: {exc}")
    if failures:
        for failure in failures: print(f"FAIL: {failure}")
        raise SystemExit(1)
    print("LINUX PYTHON DEPLOYMENT CHECK PASS")


if __name__ == "__main__": main()
