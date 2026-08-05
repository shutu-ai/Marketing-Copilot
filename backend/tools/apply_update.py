#!/usr/bin/env python3
"""Detached runner for online updates and rollbacks.

Spawned by the web process after an apply/rollback request. It stops the old
service, replaces the application directories in place, starts the new
version, health-checks it, and rolls back on failure. Stdlib only: this
script must keep working while the backend tree is being replaced.

State is read/written through ``apply.state`` under the update directory.
The systemd unit should use ``KillMode=process`` so ``systemctl stop`` does
not signal this detached runner together with the main process.
"""

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

SERVICE_UNIT = os.environ.get("MC_SERVICE_UNIT", "marketing-copilot.service")
HEALTH_URL = os.environ.get("MC_PROBE_URL", "http://127.0.0.1:8000/api/ready")
HEALTH_TRIES = int(os.environ.get("MC_UPDATE_HEALTH_TRIES", "30"))
HEALTH_INTERVAL = float(os.environ.get("MC_UPDATE_HEALTH_INTERVAL", "2.0"))
STOP_GRACE = float(os.environ.get("MC_UPDATE_STOP_GRACE", "30"))

# Entries under the application directories that carry runtime configuration or
# business data. They are preserved across updates (never cleared) and are not
# copied back from the package (build scripts exclude them anyway).
PROTECTED_ENTRIES = {
    "db_config.json", "uploads", "monitor_dir", "data", "logs", "venv",
}


def log(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [apply_update] {message}", flush=True)


def app_root() -> Path:
    return Path(__file__).resolve().parents[1]


def project_root() -> Path:
    return app_root().parent


def state_file() -> Path:
    env = os.environ.get("MC_UPDATE_STATE_FILE")
    if env:
        return Path(env)
    base = os.environ.get("MC_UPDATE_DIR", "")
    if base:
        return Path(base) / "apply.state"
    state_dir = os.environ.get("MC_STATE_DIR", str(app_root() / "data"))
    return Path(state_dir) / "updates" / "apply.state"


def load_state() -> dict:
    path = state_file()
    if not path.is_file():
        raise RuntimeError(f"state file missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    path = state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(path))


def run(cmd: list, timeout: int = 120) -> tuple:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return proc.returncode, ((proc.stdout or "") + (proc.stderr or "")).strip()
    except FileNotFoundError:
        return 127, "command not found"
    except subprocess.TimeoutExpired:
        return 124, "timeout"


# ---------------------------------------------------------------------------
# Stop / start
# ---------------------------------------------------------------------------

def lock_file() -> Path:
    env = os.environ.get("MC_INSTANCE_LOCK", "")
    return Path(env) if env else Path("/tmp/marketing-copilot.lock")


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def read_lock_pid() -> int:
    try:
        return int(lock_file().read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return 0


def wait_lock_released(timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not lock_file().exists():
            return True
        pid = read_lock_pid()
        if pid and not pid_alive(pid):
            try:
                lock_file().unlink(missing_ok=True)
            except OSError:
                pass
            return True
        time.sleep(0.5)
    return False


def stop_service(state: dict) -> str:
    """Stop the old process. Returns 'systemctl' or 'pid' to record how."""
    mode = state.get("mode", "manual")
    if mode == "systemd":
        code, out = run(["systemctl", "stop", SERVICE_UNIT])
        if code == 0:
            log("stopped via systemctl")
            wait_lock_released(15)
            return "systemctl"
        log(f"systemctl stop failed ({code}): {out[:200]}; falling back to pid stop")
    pid = read_lock_pid()
    if pid:
        log(f"sending SIGTERM to pid {pid}")
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            log("no permission to signal pid; trying pkill")
            run(["pkill", "-TERM", "-f", "uvicorn main:app"])
    else:
        log("no lock pid; trying pkill")
        run(["pkill", "-TERM", "-f", "uvicorn main:app"])
    if not wait_lock_released(STOP_GRACE):
        log("grace period expired; sending SIGKILL")
        pid = read_lock_pid()
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        wait_lock_released(10)
    return "pid"


def start_direct(state: dict) -> None:
    env = os.environ.copy()
    run_script = project_root() / "run.sh"
    log_path = os.environ.get("MC_UPDATE_RUNNER_LOG", "")
    console = Path(log_path).with_suffix(".console.log") if log_path else project_root() / "logs" / "update-console.log"
    console.parent.mkdir(parents=True, exist_ok=True)
    with open(console, "ab") as handle:
        if run_script.is_file():
            os.chmod(run_script, 0o755)
            log(f"starting via run.sh: {run_script}")
            subprocess.Popen(
                [str(run_script)],
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=str(project_root()),
            )
        else:
            venv_python = project_root() / "venv" / "bin" / "python"
            log(f"starting uvicorn directly: {venv_python}")
            subprocess.Popen(
                [
                    str(venv_python),
                    "-m",
                    "uvicorn",
                    "main:app",
                    "--host",
                    os.environ.get("MC_HOST", "0.0.0.0"),
                    "--port",
                    os.environ.get("MC_PORT", "8000"),
                    "--workers",
                    "1",
                ],
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=str(app_root()),
            )
    log(f"direct start issued; console log {console}")


def bring_up(state: dict, stopped_via: str) -> None:
    mode = state.get("mode", "manual")
    if mode == "manual":
        start_direct(state)
        return
    if stopped_via == "systemctl":
        code, out = run(["systemctl", "start", SERVICE_UNIT])
        if code == 0:
            log("started via systemctl")
            return
        log(f"systemctl start failed ({code}): {out[:200]}; starting uvicorn directly")
        start_direct(state)
        return
    log("unit is still active; relying on systemd Restart=always to start the new version")


def health_ok() -> bool:
    for attempt in range(1, HEALTH_TRIES + 1):
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=5) as resp:
                if resp.status == 200:
                    log(f"health check passed on attempt {attempt}")
                    return True
        except Exception:
            pass
        time.sleep(HEALTH_INTERVAL)
    return False


# ---------------------------------------------------------------------------
# Swapping
# ---------------------------------------------------------------------------

def _is_protected(name: str) -> bool:
    if name in PROTECTED_ENTRIES:
        return True
    lowered = name.lower()
    return lowered.endswith(
        (".duckdb", ".duckdb.wal", ".wal", ".db", ".log", ".pid", ".lock")
    )


def replace_tree(new_tree: Path, target: Path) -> None:
    log(f"replacing {target} with {new_tree}")
    target.mkdir(parents=True, exist_ok=True)
    for entry in target.iterdir():
        if _is_protected(entry.name):
            log(f"preserving {entry.name} during swap")
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            entry.unlink(missing_ok=True)
    shutil.copytree(
        new_tree,
        target,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "logs", "*.duckdb", "*.duckdb.wal",
            "db_config.json", "uploads", "monitor_dir", "data", "venv",
        ),
    )


def install_dependencies(state: dict) -> None:
    payload = Path(state.get("payload_dir", ""))
    wheels = payload / "offline" / "wheels"
    requirements = payload / "offline" / "requirements.txt"
    if not wheels.is_dir() or not any(wheels.iterdir()):
        log("no wheels in package; skipping dependency install")
        return
    reqs_text = requirements.read_text(encoding="utf-8", errors="replace") if requirements.is_file() else ""
    digest = hashlib.sha256(reqs_text.encode("utf-8")).hexdigest()
    marker = state_file().parent / "last_requirements.sha256"
    if marker.is_file() and marker.read_text(encoding="ascii").strip() == digest:
        log("requirements unchanged; skipping dependency install")
        return
    venv_python = project_root() / "venv" / "bin" / "python"
    if not venv_python.is_file():
        log(f"venv python missing: {venv_python}; skipping dependency install")
        return
    cmd = [
        str(venv_python), "-m", "pip", "install", "--no-index",
        "--find-links", str(wheels), "-r", str(requirements),
    ]
    log("installing dependencies from offline wheels")
    code, out = run(cmd, timeout=1800)
    if code != 0:
        raise RuntimeError(f"pip install failed ({code}): {out[-500:]}")
    marker.write_text(digest, encoding="ascii")
    log("dependencies installed")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

def swap_into_place(state: dict) -> None:
    action = state.get("action", "apply")
    if action == "apply":
        payload = Path(state.get("payload_dir", ""))
        for label, source, target in (
            ("backend", payload / "backend", project_root() / "backend"),
            ("frontend", payload / "frontend", project_root() / "frontend"),
        ):
            replace_tree(source, target)
        install_dependencies(state)
        log("new version swapped in")
    else:
        backup = Path(state.get("backup_dir", ""))
        for label in ("backend", "frontend", "offline"):
            source = backup / label
            target = project_root() / label
            if source.is_dir():
                replace_tree(source, target)
                log(f"restored {label} from backup")


def main() -> int:
    # Never let systemctl stop (KillMode=control-group on older units) kill us.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
    state = load_state()
    log(f"runner started: action={state.get('action')} mode={state.get('mode')} pid={os.getpid()}")
    save_state({**load_state(), "status": "stopping", "runner_pid": os.getpid()})
    try:
        stopped_via = stop_service(state)
        save_state({**load_state(), "status": "swapping", "stopped_via": stopped_via})
        swap_into_place(state)
        save_state({**load_state(), "status": "starting"})
        bring_up(state, stopped_via)
        save_state({**load_state(), "status": "health_checking"})
        if health_ok():
            result = {
                "ok": True,
                "status": "success",
                "version": state.get("version", ""),
                "message": "service is healthy after update",
            }
            save_state(
                {
                    **load_state(),
                    "status": "success",
                    "result": result,
                    "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            log("update finished successfully")
            return 0
        raise RuntimeError("health check failed")
    except Exception as exc:
        log(f"update failed: {exc}")
        rolled_back = False
        try:
            backup = Path(state.get("backup_dir", ""))
            if backup.is_dir():
                log("attempting automatic rollback")
                stop_again = stop_service(load_state())
                for label in ("backend", "frontend", "offline"):
                    source = backup / label
                    target = project_root() / label
                    if source.is_dir():
                        replace_tree(source, target)
                bring_up(load_state(), stop_again)
                rolled_back = True
                log("rollback completed")
        except Exception as rollback_exc:
            log(f"rollback failed: {rollback_exc}")
        result = {
            "ok": False,
            "status": "failed",
            "version": state.get("version", ""),
            "message": f"{exc}",
            "rolled_back": rolled_back,
        }
        save_state(
            {
                **load_state(),
                "status": "failed",
                "result": result,
                "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
