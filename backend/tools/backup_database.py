"""Create a consistent DuckDB backup through the single-owner protocol.

When the application service owns the database, this command delegates to its
loopback-only maintenance endpoint instead of opening a competing DuckDB
process. It retains direct mode for stopped-service maintenance backups.
"""

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from auth import internal_backup_token
from services.runtime_service import _process_is_running, backup_database


def _request_online_backup(target_dir: str) -> str:
    port = os.environ.get("MC_PORT", "19090")
    url = os.environ.get(
        "MC_BACKUP_REQUEST_URL",
        f"http://127.0.0.1:{port}/api/internal/backup",
    )
    body = json.dumps({"target_dir": target_dir or ""}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-MC-Backup-Token": internal_backup_token(),
        },
        method="POST",
    )
    timeout = max(1.0, float(os.environ.get("MC_BACKUP_REQUEST_TIMEOUT_SECONDS", "30")))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"online backup request failed HTTP {exc.code}: {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"online backup request could not reach the running owner at {url}: {exc}"
        ) from exc
    path = str(payload.get("path") or "")
    if not path:
        raise RuntimeError(f"online backup response did not contain a path: {payload}")
    return path


def _online_owner_pid() -> int | None:
    lock_value = os.environ.get("MC_INSTANCE_LOCK", "").strip()
    if not lock_value:
        return None
    lock_path = Path(lock_value).expanduser()
    if not lock_path.exists():
        return None
    try:
        pid = int(lock_path.read_text(encoding="ascii").strip())
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"instance lock exists but is unreadable: {lock_path}"
        ) from exc
    if pid > 0 and pid != os.getpid() and _process_is_running(pid):
        return pid
    return None


parser = argparse.ArgumentParser()
parser.add_argument("--target-dir", default="")
args = parser.parse_args()

owner_pid = _online_owner_pid()
if owner_pid is not None:
    print(_request_online_backup(args.target_dir))
else:
    print(backup_database(args.target_dir))
