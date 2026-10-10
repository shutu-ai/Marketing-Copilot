"""Online update management: staging uploads, verification, apply and rollback.

Deployment model
----------------
The application is installed as a self-contained directory tree
(``backend/`` + ``frontend/`` + ``venv/``, plus ``offline/`` on manual
deployments). Updates replace the *contents* of the application directories
in place after the old process has stopped. This avoids touching service
definitions, keeps the web process able to perform the swap with its own
permissions, and gives fast rollback from backups kept under the update
directory.

The web process performs every reversible preparation step (database backup,
task drain, code backup), then spawns the detached runner
``backend/tools/apply_update.py`` which stops the service, swaps directories,
restarts, health-checks and rolls back on failure.
"""

import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_VERSION = "1.2.0"
STATE_FILE_NAME = "apply.state"
RUNNING_STATES = ("prepared", "stopping", "swapping", "starting", "health_checking")


# ---------------------------------------------------------------------------
# Paths and version metadata
# ---------------------------------------------------------------------------

def app_root() -> Path:
    return Path(__file__).resolve().parents[1]


def project_root() -> Path:
    return app_root().parent


def state_dir() -> Path:
    return Path(os.environ.get("MC_STATE_DIR", str(app_root() / "data")))


def update_dir() -> Path:
    env = os.environ.get("MC_UPDATE_DIR")
    if env:
        return Path(env)
    return state_dir() / "updates"


def ensure_update_dir() -> Path:
    root = update_dir()
    for sub in ("", "uploads", "backups", "logs"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    return root


def upload_dir(upload_id: str) -> Path:
    return ensure_update_dir() / "uploads" / _safe_id(upload_id)


def _safe_id(value: str) -> str:
    cleaned = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in "-_.")
    if not cleaned:
        raise ValueError("invalid identifier")
    return cleaned


def version_info() -> dict:
    release = app_root() / "release.json"
    if release.is_file():
        try:
            data = json.loads(release.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("version"):
                return data
        except Exception:
            logger.warning("release.json is invalid: %s", release)
    return {
        "name": "Marketing Copilot",
        "version": DEFAULT_VERSION,
        "build_time": "",
        "builder": "",
        "git_sha": "",
    }


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def update_mode() -> str:
    mode = os.environ.get("MC_UPDATE_MODE", "").strip().lower()
    if mode in ("systemd", "manual"):
        return mode
    if os.name == "nt":
        return "manual"
    if os.path.exists("/etc/systemd/system/marketing-copilot.service") or os.path.exists("/run/systemd/system"):
        return "systemd"
    return "manual"


# ---------------------------------------------------------------------------
# Staged uploads (chunked)
# ---------------------------------------------------------------------------

def _meta_path(upload_id: str) -> Path:
    return upload_dir(upload_id) / "meta.json"


def load_meta(upload_id: str) -> dict:
    path = _meta_path(upload_id)
    if not path.is_file():
        raise LookupError(f"upload {upload_id} does not exist")
    return json.loads(path.read_text(encoding="utf-8"))


def save_meta(upload_id: str, meta: dict) -> None:
    path = _meta_path(upload_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(path))


def create_upload(upload_id: str, filename: str, size: int, sha256: str, chunks: int) -> dict:
    ensure_update_dir()
    safe_name = Path(str(filename or "update.tar.gz")).name
    if not safe_name.endswith((".tar.gz", ".tgz", ".tar")):
        raise ValueError("update package must be a tar.gz archive")
    max_size = int(os.environ.get("MC_UPDATE_MAX_TOTAL_BYTES", str(2 * 1024 ** 3)))
    if size and size > max_size:
        raise ValueError(f"package too large: {size} bytes > {max_size}")
    ud = upload_dir(upload_id)
    ud.mkdir(parents=True, exist_ok=True)
    meta = {
        "upload_id": upload_id,
        "filename": safe_name,
        "size": int(size or 0),
        "sha256": (sha256 or "").strip().lower(),
        "chunks": max(1, int(chunks or 1)),
        "received": 0,
        "complete": False,
        "verified": False,
        "created_at": now_str(),
        "precheck": None,
    }
    save_meta(upload_id, meta)
    return meta


def save_chunk(upload_id: str, chunk_index: int, data: bytes) -> dict:
    ud = upload_dir(upload_id)
    meta = load_meta(upload_id)
    if meta.get("complete") or meta.get("verified"):
        raise ValueError("upload already finished; start a new upload")
    if chunk_index < 0 or chunk_index >= meta["chunks"]:
        raise ValueError(f"chunk index {chunk_index} out of range")
    max_chunk = int(os.environ.get("MC_UPDATE_MAX_CHUNK_BYTES", str(64 * 1024 * 1024)))
    if len(data) > max_chunk:
        raise ValueError(f"chunk too large: {len(data)} bytes > {max_chunk}")
    part = ud / f"part-{chunk_index:05d}"
    with open(part, "wb") as handle:
        handle.write(data)
    meta["received"] = len(list(ud.glob("part-*")))
    save_meta(upload_id, meta)
    if meta["received"] >= meta["chunks"]:
        _assemble(upload_id, meta)
    return {
        "upload_id": upload_id,
        "received": meta["received"],
        "chunks": meta["chunks"],
        "complete": bool(meta.get("complete")),
    }


def _assemble(upload_id: str, meta: dict) -> None:
    ud = upload_dir(upload_id)
    target = ud / meta["filename"]
    with open(target, "wb") as out:
        for index in range(meta["chunks"]):
            part = ud / f"part-{index:05d}"
            if not part.is_file():
                raise ValueError(f"missing chunk {index}")
            with open(part, "rb") as handle:
                shutil.copyfileobj(handle, out)
    if meta["size"] and target.stat().st_size != meta["size"]:
        raise ValueError(f"size mismatch: expected {meta['size']}, got {target.stat().st_size}")
    if meta["sha256"]:
        actual = _sha256_file(target)
        if actual != meta["sha256"]:
            raise ValueError(f"SHA-256 mismatch: expected {meta['sha256']}, got {actual}")
    meta["complete"] = True
    save_meta(upload_id, meta)


def cancel_upload(upload_id: str) -> None:
    ud = upload_dir(upload_id)
    if not ud.exists():
        return
    meta_path = ud / "meta.json"
    verified = False
    if meta_path.is_file():
        try:
            verified = bool(json.loads(meta_path.read_text(encoding="utf-8")).get("verified"))
        except Exception:
            pass
    if verified:
        raise ValueError("verified uploads cannot be cancelled; apply or keep it")
    shutil.rmtree(ud, ignore_errors=True)


# ---------------------------------------------------------------------------
# Verification / preflight
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_extract(archive: Path, dest: Path) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    count = 0
    total_bytes = 0
    max_total = int(os.environ.get("MC_UPDATE_MAX_TOTAL_BYTES", str(2 * 1024 ** 3)))
    with tarfile.open(archive, "r:*") as tar:
        for member in tar.getmembers():
            name = member.name.replace("\\", "/")
            parts = [p for p in name.split("/") if p not in ("", ".")]
            if name.startswith("/") or ".." in parts:
                raise ValueError(f"unsafe archive path: {member.name}")
            target = (dest / name).resolve()
            if target != root and not str(target).startswith(str(root) + os.sep):
                raise ValueError(f"archive path escapes target: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"unsupported archive entry: {member.name} (type {member.type})")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ValueError(f"unsupported archive entry: {member.name} (type {member.type})")
            total_bytes += member.size
            if total_bytes > max_total:
                raise ValueError("extracted package exceeds size limit")
            source = tar.extractfile(member)
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, "wb") as out:
                shutil.copyfileobj(source, out)
            count += 1
    return count


def _verify_manifest(root: Path) -> list:
    manifest = root / "checksums.sha256"
    bad = []
    for line in manifest.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("  ")
        if len(parts) < 2:
            parts = line.split(" ", 1)
        if len(parts) < 2:
            continue
        expected = parts[0].strip().lower()
        # `sha256sum -b` (binary mode) prefixes paths with `*`; strip it.
        rel = parts[1].strip().lstrip("*")
        path = (root / rel).resolve()
        if not str(path).startswith(str(root.resolve()) + os.sep) or not path.is_file():
            bad.append(f"{rel}: missing")
            continue
        if _sha256_file(path) != expected:
            bad.append(f"{rel}: checksum mismatch")
    return bad


def precheck(upload_id: str) -> dict:
    ud = upload_dir(upload_id)
    meta = load_meta(upload_id)
    if not meta.get("complete"):
        raise ValueError("upload is not complete")
    archive = ud / meta["filename"]
    payload = ud / "payload"
    if payload.exists():
        shutil.rmtree(payload)
    checks, errors = [], []
    count = 0
    try:
        count = _safe_extract(archive, payload)
        checks.append({"name": "archive", "ok": True, "detail": f"{count} entries extracted"})
    except Exception as exc:
        errors.append(f"archive: {exc}")
        checks.append({"name": "archive", "ok": False, "detail": str(exc)})

    # Built packages wrap the payload in a single top-level directory
    # (e.g. marketing-copilot-sles12sp5-system-python-<ts>/backend/...).
    # Normalize it so the required-path checks work for both layouts.
    content = payload
    if not errors and not (payload / "backend").is_dir():
        children = [p for p in payload.iterdir() if p.is_dir()]
        candidates = [c for c in children if (c / "backend").is_dir()]
        if len(candidates) == 1:
            content = candidates[0]
        elif len(children) == 1:
            content = children[0]

    manifest = content / "checksums.sha256"
    if not errors and manifest.is_file():
        bad = _verify_manifest(content)
        if bad:
            errors.append(f"checksums.sha256: {len(bad)} problem(s), first: {bad[:3]}")
            checks.append({"name": "checksums.sha256", "ok": False, "detail": f"{len(bad)} problem(s)"})
        else:
            checks.append({"name": "checksums.sha256", "ok": True, "detail": "all files verified"})
    elif not errors:
        errors.append("checksums.sha256 is missing from the package")

    for rel, label in (
        ("backend/main.py", "backend entry"),
        ("backend/requirements.txt", "Python requirements"),
        ("frontend/dist/index.html", "frontend build"),
    ):
        if (content / rel).is_file():
            checks.append({"name": label, "ok": True, "detail": rel})
        else:
            errors.append(f"missing required file: {rel}")

    wheels = content / "offline" / "wheels"
    has_wheels = wheels.is_dir() and any(wheels.iterdir())
    checks.append(
        {
            "name": "offline wheels",
            "ok": has_wheels,
            "detail": "dependency update will be applied" if has_wheels else "absent; dependencies stay unchanged",
        }
    )

    new_version = "unknown"
    release = content / "backend" / "release.json"
    if release.is_file():
        try:
            release_data = json.loads(release.read_text(encoding="utf-8"))
            new_version = str(release_data.get("version") or "unknown")
        except Exception as exc:
            errors.append(f"backend/release.json: {exc}")
    else:
        errors.append("backend/release.json is missing; rebuild the package with the updated builder")

    syntax_failures = []
    py_count = 0
    for py in sorted((content / "backend").rglob("*.py")):
        try:
            compile(py.read_bytes(), str(py), "exec")
            py_count += 1
        except Exception as exc:
            syntax_failures.append(f"{py.name}: {exc}")
    if syntax_failures:
        errors.append(f"backend syntax errors: {syntax_failures[:3]}")
        checks.append({"name": "backend syntax", "ok": False, "detail": f"{len(syntax_failures)} file(s) failed"})
    else:
        checks.append({"name": "backend syntax", "ok": True, "detail": f"{py_count} Python files compiled"})

    result = {
        "ok": not errors,
        "version": new_version,
        "checks": checks,
        "errors": errors,
        "verified_at": now_str(),
        "payload_files": count,
    }
    meta["verified"] = bool(result["ok"])
    meta["precheck"] = result
    meta["payload_root"] = str(content)
    save_meta(upload_id, meta)
    return result


# ---------------------------------------------------------------------------
# Apply / rollback state machine
# ---------------------------------------------------------------------------

def load_state() -> dict:
    path = ensure_update_dir() / STATE_FILE_NAME
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    path = ensure_update_dir() / STATE_FILE_NAME
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(path))


def active_business() -> dict:
    from database import get_connection

    conn = get_connection()
    row = conn.execute(
        """SELECT
             (SELECT COUNT(*) FROM batch_campaigns WHERE status='in_progress') AS batch_in_progress,
             (SELECT COUNT(*) FROM realtime_campaigns WHERE status='in_progress') AS realtime_in_progress,
             (SELECT COUNT(*) FROM sms_tasks WHERE status='sending') AS sending_tasks,
             (SELECT COUNT(*) FROM sms_tasks WHERE status IN ('queued','retrying')
                AND TRY_CAST(next_attempt_at AS TIMESTAMP WITH TIME ZONE) <= CURRENT_TIMESTAMP) AS pending_tasks"""
    ).fetchone()
    conn.close()
    return dict(row)


def drain_business() -> dict:
    """Requeue in-flight tasks and stop workers/scheduler before an update."""
    from database import get_connection

    conn = get_connection()
    conn.execute(
        """UPDATE sms_tasks
           SET status='retrying', error_msg='update maintenance; requeued',
               next_attempt_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
           WHERE status='sending'"""
    )
    conn.commit()
    conn.close()
    from services.activity_scheduler import stop_scheduler
    from services.task_worker import stop_worker

    stop_scheduler()
    stop_worker()
    time.sleep(1.0)
    return active_business()


def _copy_tree(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    if source.exists():
        shutil.copytree(
            source,
            destination,
            ignore=shutil.ignore_patterns(
                "__pycache__", "*.pyc", "logs", "*.duckdb", "*.duckdb.wal",
                "db_config.json", "uploads", "monitor_dir", "data", "venv",
            ),
        )


def apply_update(upload_id: str, force: bool = False, operator: str = "system") -> dict:
    if os.name == "nt":
        raise RuntimeError("online updates are only supported on the Linux target; update the dev machine manually")
    state = load_state()
    if state.get("status") in RUNNING_STATES:
        raise RuntimeError("another update or rollback is already running")
    meta = load_meta(upload_id)
    if not meta.get("verified") or not (meta.get("precheck") or {}).get("ok"):
        raise ValueError("package has not passed verification")
    current = version_info()
    new_version = (meta.get("precheck") or {}).get("version", "unknown")
    if current.get("version") == new_version and not force:
        raise ValueError("the package version equals the running version; pass force=true to reapply")
    activity = active_business()
    if (activity["batch_in_progress"] or activity["realtime_in_progress"]) and not force:
        raise RuntimeError("active campaigns are running; apply during a maintenance window or pass force=true")

    from services.runtime_service import backup_database

    db_backup = backup_database()
    drained = drain_business()

    ensure_update_dir()
    backup_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_root = ensure_update_dir() / "backups" / backup_ts
    payload = Path(meta.get("payload_root") or upload_dir(upload_id) / "payload")
    trees = [
        ("backend", project_root() / "backend", payload / "backend"),
        ("frontend", project_root() / "frontend", payload / "frontend"),
    ]
    if (project_root() / "offline").exists():
        trees.append(("offline", project_root() / "offline", payload / "offline"))
    if (project_root() / "bin").exists():
        trees.append(("bin", project_root() / "bin", payload / "bin"))
    for label, source, _new in trees:
        _copy_tree(source, backup_root / label)

    state_data = {
        "action": "apply",
        "run_id": f"apply-{backup_ts}",
        "upload_id": upload_id,
        "mode": update_mode(),
        "status": "prepared",
        "payload_dir": str(payload),
        "backup_dir": str(backup_root),
        "backup_ts": backup_ts,
        "db_backup": db_backup,
        "operator": operator,
        "version": new_version,
        "previous_version": current.get("version", DEFAULT_VERSION),
        "started_at": now_str(),
    }
    save_state(state_data)
    try:
        _spawn_runner()
    except Exception:
        save_state(
            {
                **state_data,
                "status": "failed",
                "result": {"ok": False, "status": "failed", "message": "failed to start update runner", "rolled_back": False},
                "finished_at": now_str(),
            }
        )
        from services.activity_scheduler import start_scheduler
        from services.task_worker import start_worker

        start_worker()
        start_scheduler()
        raise
    return {"state": state_data, "db_backup": db_backup, "drained": drained}


def rollback(operator: str = "system") -> dict:
    if os.name == "nt":
        raise RuntimeError("online updates are only supported on the Linux target; update the dev machine manually")
    state = load_state()
    if state.get("status") in RUNNING_STATES:
        raise RuntimeError("another update or rollback is already running")
    backup_root = _latest_backup_dir()
    if not backup_root:
        raise LookupError("no backup found; nothing to roll back")

    from services.runtime_service import backup_database

    db_backup = backup_database()
    drained = drain_business()
    state_data = {
        "action": "rollback",
        "run_id": f"rollback-{backup_root.name}",
        "mode": update_mode(),
        "status": "prepared",
        "backup_dir": str(backup_root),
        "backup_ts": backup_root.name,
        "db_backup": db_backup,
        "operator": operator,
        "version": "rollback",
        "started_at": now_str(),
    }
    save_state(state_data)
    try:
        _spawn_runner()
    except Exception:
        save_state(
            {
                **state_data,
                "status": "failed",
                "result": {"ok": False, "status": "failed", "message": "failed to start update runner", "rolled_back": False},
                "finished_at": now_str(),
            }
        )
        from services.activity_scheduler import start_scheduler
        from services.task_worker import start_worker

        start_worker()
        start_scheduler()
        raise
    return {"state": state_data, "db_backup": db_backup, "drained": drained}


def _spawn_runner() -> None:
    script = app_root() / "tools" / "apply_update.py"
    if not script.is_file():
        raise RuntimeError(f"update runner missing: {script}")
    log_path = ensure_update_dir() / "logs" / f"runner-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
    env = os.environ.copy()
    env["MC_UPDATE_RUNNER_LOG"] = str(log_path)
    try:
        with open(log_path, "wb") as handle:
            subprocess.Popen(
                [sys.executable, str(script)],
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=str(app_root()),
            )
    except OSError as exc:
        raise RuntimeError(f"failed to start update runner: {exc}") from exc
    logger.info("update runner spawned, log=%s", log_path)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def recover_if_runner_dead() -> None:
    """If the detached runner died before stopping the service, restore workers."""
    state = load_state()
    if not state or state.get("status") not in RUNNING_STATES:
        return
    pid = state.get("runner_pid")
    # The runner writes its own PID right after starting; without a PID the
    # apply endpoint is still handling spawn failures itself, so wait.
    if not pid:
        return
    if _pid_alive(int(pid)):
        return
    save_state(
        {
            **state,
            "status": "failed",
            "result": {
                "ok": False,
                "status": "failed",
                "message": "update runner exited unexpectedly; service restored",
                "rolled_back": False,
            },
            "finished_at": now_str(),
        }
    )
    try:
        from services.activity_scheduler import start_scheduler
        from services.task_worker import start_worker

        start_worker()
        start_scheduler()
    except Exception:
        logger.exception("failed to restart workers after runner death")


def _latest_backup_dir() -> Path:
    root = ensure_update_dir() / "backups"
    if not root.is_dir():
        return None
    backups = sorted(root.iterdir(), key=lambda p: p.name)
    return backups[-1] if backups else None


def cleanup_staging(max_age_hours: int = 24) -> None:
    keep_backups = max(1, int(os.environ.get("MC_UPDATE_KEEP_BACKUPS", "3")))
    keep_uploads = max(1, int(os.environ.get("MC_UPDATE_KEEP_UPLOADS", "3")))
    root = ensure_update_dir()
    now = time.time()
    state = load_state()
    active_upload = state.get("upload_id") if state.get("status") in RUNNING_STATES else None
    uploads_root = root / "uploads"
    if uploads_root.is_dir():
        completed = []
        for ud in uploads_root.glob("*"):
            if not ud.is_dir():
                continue
            meta_path = ud / "meta.json"
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
            except Exception:
                meta = {}
            if meta.get("verified") or meta.get("complete"):
                try:
                    completed.append((ud.stat().st_mtime, ud, ud.name))
                except OSError:
                    pass
                continue
            try:
                if now - ud.stat().st_mtime > max_age_hours * 3600:
                    shutil.rmtree(ud, ignore_errors=True)
            except OSError:
                pass
        completed.sort(key=lambda item: item[0])
        for _mtime, ud, uid in completed[:-keep_uploads]:
            if uid == active_upload:
                continue
            shutil.rmtree(ud, ignore_errors=True)
    backups_root = root / "backups"
    if backups_root.is_dir():
        backups = sorted(backups_root.iterdir(), key=lambda p: p.name)
        for old in backups[:-keep_backups]:
            shutil.rmtree(old, ignore_errors=True)


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

HISTORY_RETENTION_DAYS = 365


def prune_history() -> None:
    """Remove update-history rows older than the one-year retention window."""
    try:
        from database import get_connection

        conn = get_connection()
        conn.execute(
            f"""DELETE FROM update_history
               WHERE try_cast(created_at AS TIMESTAMP)
                     < CURRENT_TIMESTAMP - INTERVAL '{HISTORY_RETENTION_DAYS} days'"""
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("failed to prune update history")


def record_pending_result() -> None:
    """Persist the last runner result into ``update_history`` exactly once."""
    state = load_state()
    result = state.get("result") or {}
    if not result:
        return
    run_id = state.get("run_id") or f"{state.get('action', 'update')}-{state.get('backup_ts', '')}"
    try:
        from database import get_connection

        conn = get_connection()
        conn.execute(
            """INSERT INTO update_history
               (run_id, action, version, previous_version, status, operator, detail)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id) DO UPDATE SET
                 version=excluded.version,
                 previous_version=excluded.previous_version,
                 status=excluded.status,
                 detail=excluded.detail""",
            (
                run_id,
                state.get("action", ""),
                result.get("version") or state.get("version", ""),
                state.get("previous_version", ""),
                result.get("status", "failed"),
                state.get("operator", "system"),
                json.dumps(
                    {"message": result.get("message", ""), "db_backup": state.get("db_backup", "")},
                    ensure_ascii=False,
                ),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("failed to record update history")


def list_history(limit: int = 50) -> list:
    try:
        prune_history()
        from database import get_connection

        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM update_history ORDER BY created_at DESC, id DESC LIMIT ?", (int(limit),)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def recent_uploads(limit: int = 5) -> list:
    """Return recent staged uploads (newest first), including verified prechecks."""
    root = ensure_update_dir() / "uploads"
    if not root.is_dir():
        return []
    items = []
    for ud in root.glob("*"):
        if not ud.is_dir():
            continue
        meta_path = ud / "meta.json"
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
        except Exception:
            continue
        precheck = meta.get("precheck") or {}
        items.append(
            {
                "upload_id": ud.name,
                "filename": meta.get("filename", ""),
                "size": meta.get("size", 0),
                "complete": bool(meta.get("complete")),
                "verified": bool(meta.get("verified")),
                "version": precheck.get("version", ""),
                "precheck": precheck,
                "created_at": meta.get("created_at", ""),
            }
        )
    items.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return items[:limit]


def status() -> dict:
    ensure_update_dir()
    cleanup_staging()
    record_pending_result()
    prune_history()
    recover_if_runner_dead()
    current = version_info()
    state = load_state()
    return {
        "version": current.get("version", DEFAULT_VERSION),
        "release": current,
        "mode": update_mode(),
        "state": state,
        "has_backups": _latest_backup_dir() is not None,
        "active_business": active_business(),
    }
