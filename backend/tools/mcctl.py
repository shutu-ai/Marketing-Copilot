#!/usr/bin/env python3
"""Local maintenance CLI for Marketing Copilot.

This command talks to the local update service instead of the HTTP API. It is
usable when the web process is down, provided the application environment and
update backup directory are available.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import sys
from pathlib import Path


def project_root() -> Path:
    path = Path(__file__).resolve()
    if path.parent.name == "tools" and path.parent.parent.name == "backend":
        return path.parents[2]
    if path.parent.name == "bin":
        return path.parent.parent
    return path.parent


def _load_env_file() -> None:
    """Load deployment settings without requiring shell-specific syntax."""
    root = project_root()
    env_path = Path(os.environ.get("MC_ENV_FILE", str(root / "marketing-copilot.env")))
    if not env_path.is_file():
        return
    assignment = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")
    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = assignment.match(raw_line)
        if not match:
            continue
        key, value = match.groups()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        value = value.replace("${APP_HOME}", str(root)).replace("$APP_HOME", str(root))
        os.environ.setdefault(key, value)


def _service():
    _load_env_file()
    backend = str(project_root() / "backend")
    if backend not in sys.path:
        sys.path.insert(0, backend)
    from services import update_service

    return update_service


def _json_default(value):
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _print_json(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=_json_default))


def _running_instance_pid():
    lock_path = Path(os.environ.get("MC_INSTANCE_LOCK", "/tmp/marketing-copilot.lock"))
    try:
        pid = int(lock_path.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return None
    return pid


def _version(args) -> int:
    update_service = _service()
    release = update_service.version_info()
    if args.json:
        _print_json(release)
        return 0
    print(f"{release.get('name', 'Marketing Copilot')}: {release.get('version', '-')}")
    for key in ("build_time", "builder", "git_sha", "python_runtime", "python_version"):
        if release.get(key):
            print(f"{key}: {release[key]}")
    return 0


def _status(args) -> int:
    update_service = _service()
    update_service.ensure_update_dir()
    current = update_service.version_info()
    state = update_service.load_state()
    backups_root = update_service.update_dir() / "backups"
    instance_pid = _running_instance_pid()
    if instance_pid:
        active_business = {
            "available": False,
            "reason": "database is locked by the running application process",
            "owner_pid": instance_pid,
        }
    else:
        try:
            active_business = update_service.active_business()
        except Exception as exc:
            active_business = {"error": f"database unavailable: {exc}"}
    result = {
        "version": current.get("version", update_service.DEFAULT_VERSION),
        "release": current,
        "mode": update_service.update_mode(),
        "state": state,
        "has_backups": any(backups_root.iterdir()) if backups_root.is_dir() else False,
        "update_dir": str(update_service.update_dir()),
        "active_business": active_business,
    }
    _print_json(result)
    return 0


def _history(args) -> int:
    update_service = _service()
    rows = update_service.list_history(args.limit)
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        print("No update history found.")
        return 0
    for row in rows:
        print(
            f"{row.get('created_at', '-'):<19} "
            f"{row.get('action', '-'):<8} "
            f"{row.get('status', '-'):<8} "
            f"{row.get('version', '-'):<24} "
            f"{row.get('run_id', '-')}"
        )
    return 0


def _backups(args) -> int:
    update_service = _service()
    root = update_service.ensure_update_dir() / "backups"
    items = []
    if root.is_dir():
        for path in sorted((item for item in root.iterdir() if item.is_dir()), reverse=True):
            release = {}
            release_path = path / "backend" / "release.json"
            try:
                release = json.loads(release_path.read_text(encoding="utf-8"))
            except Exception:
                pass
            items.append({
                "backup_ts": path.name,
                "version": release.get("version", "unknown"),
                "build_time": release.get("build_time", ""),
                "path": str(path),
            })
    if args.json:
        _print_json(items)
        return 0
    if not items:
        print("No application backups found.")
        return 0
    print("backup_ts        version                    path")
    for item in items:
        print(f"{item['backup_ts']:<16} {item['version']:<25} {item['path']}")
    return 0


def _rollback(args) -> int:
    if not args.yes:
        print("Rollback is destructive to the current application tree.", file=sys.stderr)
        print("Re-run with --yes after checking `mcctl backups` and `mcctl status`.", file=sys.stderr)
        return 2
    update_service = _service()
    operator = args.operator or getpass.getuser() or "local-cli"
    result = update_service.rollback(operator=operator)
    _print_json(result)
    print("Rollback runner started; use `mcctl status` to monitor completion.", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcctl",
        description="Local Marketing Copilot version and update recovery commands",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    version = subparsers.add_parser("version", help="show the running release metadata")
    version.add_argument("--json", action="store_true", help="print JSON")
    version.set_defaults(handler=_version)

    status = subparsers.add_parser("status", help="show update state and business activity")
    status.set_defaults(handler=_status)

    history = subparsers.add_parser("history", help="show update history")
    history.add_argument("--limit", type=int, default=50)
    history.add_argument("--json", action="store_true", help="print JSON")
    history.set_defaults(handler=_history)

    backups = subparsers.add_parser("backups", help="list application backups available for rollback")
    backups.add_argument("--json", action="store_true", help="print JSON")
    backups.set_defaults(handler=_backups)

    rollback = subparsers.add_parser("rollback", help="restore the newest application backup")
    rollback.add_argument("--yes", action="store_true", help="confirm the rollback")
    rollback.add_argument("--operator", default="", help="operator name recorded in update state")
    rollback.set_defaults(handler=_rollback)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except Exception as exc:
        print(f"mcctl: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
