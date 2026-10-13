"""WSL-native DuckDB ownership, HTTP concurrency and online-backup acceptance.

The real application is started as one Uvicorn process.  All concurrent
business writes go through HTTP client subprocesses; no client opens the
application database.  The process-local lock probe uses a separate isolated
database so it can inject bounded 0.5/3/6/12 second holds without competing
with the real service.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = TOOLS_DIR.parent
REPO_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


class BlockedEnvironment(RuntimeError):
    pass


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None}
    ordered = sorted(float(value) for value in values)

    def value_at(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(len(ordered) - 1, lower + 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {key: round(value_at(fraction), 3) for key, fraction in (
        ("p50", 0.50), ("p95", 0.95), ("p99", 0.99),
    )}


def _process_snapshot(pid: int) -> dict:
    result = {"pid": pid, "sampled_at": _utc_now()}
    if platform.system() != "Linux":
        return result
    status = Path(f"/proc/{pid}/status")
    stat = Path(f"/proc/{pid}/stat")
    try:
        if status.exists():
            for line in status.read_text(encoding="utf-8").splitlines():
                if line.startswith("VmRSS:"):
                    result["rss_bytes"] = int(line.split()[1]) * 1024
                    break
                if line.startswith("Threads:"):
                    result["threads"] = int(line.split()[1])
        if stat.exists():
            fields = stat.read_text(encoding="utf-8").split()
            result["cpu_ticks"] = int(fields[13]) + int(fields[14])
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _http_request(base_url: str, method: str, path: str, payload=None, timeout: float = 10.0):
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url + path, data=body, headers=headers, method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            try:
                value = json.loads(raw.decode("utf-8")) if raw else None
            except json.JSONDecodeError:
                value = raw.decode("utf-8", errors="replace")
            return response.status, value
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            value = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            value = raw.decode("utf-8", errors="replace")
        return exc.code, value


class LockAcceptance:
    def __init__(self, args: argparse.Namespace):
        if platform.system() == "Linux" and str(REPO_ROOT).startswith("/mnt/"):
            raise BlockedEnvironment(
                f"refusing DrvFs repository path {REPO_ROOT}; run from a WSL ext4 copy"
            )
        self.args = args
        parent = Path(args.root).expanduser() if args.root else REPO_ROOT / ".lock-e2e-runs"
        parent.mkdir(parents=True, exist_ok=True)
        self.root = parent / f"marketing-copilot-{args.profile}-{uuid.uuid4().hex[:8]}"
        self.root.mkdir()
        self.db_path = self.root / "marketing_copilot.duckdb"
        self.config_path = self.root / "db_config.json"
        self.state_dir = self.root / "state"
        self.update_dir = self.root / "updates"
        self.backup_dir = self.root / "backups"
        self.online_backup_dir = self.root / "online-backups"
        self.log_path = self.root / "marketing-copilot.log"
        self.api_output = self.root / "api.stdout.log"
        self.second_output = self.root / "second-uvicorn.stdout.log"
        self.api_port = _free_port()
        self.second_port = _free_port()
        self.python = Path(args.python or sys.executable).absolute()
        self.process: subprocess.Popen | None = None
        self.resource_samples: list[dict] = []
        self.base_url = f"http://127.0.0.1:{self.api_port}"

    def env(self, *, port: int | None = None) -> dict[str, str]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.update_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        frontend = self.root / "no-dist"
        frontend.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update({
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str(BACKEND_DIR),
            "DUCKDB_PATH": str(self.db_path),
            "MC_DB_CONFIG_FILE": str(self.config_path),
            "MC_STATE_DIR": str(self.state_dir),
            "MC_UPDATE_DIR": str(self.update_dir),
            "MC_BACKUP_DIR": str(self.backup_dir),
            "MC_FRONTEND_DIR": str(frontend),
            "MC_INSTANCE_LOCK": str(self.root / "instance.lock"),
            "MC_LOG_PATH": str(self.log_path),
            "MC_SYSTEM_MODE": "production",
            "MC_AUTH_DISABLED": "1",
            "MC_AUTH_SECRET": "local-lock-acceptance-secret",
            "MC_BACKUP_REQUEST_TOKEN": "local-lock-acceptance-token",
            "MC_ACCEPTANCE_HOOKS": "1",
            "MC_LOG_LEVEL": "INFO",
            "MC_DETAIL_LOGGING": "0",
            "MC_TASK_WORKERS": "1",
            "MC_REALTIME_INGEST_WORKERS": "1",
            "MC_REALTIME_COPY_WORKERS": "1",
            "MC_TASK_ARCHIVE_ENABLED": "false",
            "MC_DUCKDB_WRITE_LOCK_TIMEOUT_SECONDS": "1",
        })
        if port is not None:
            env["MC_PORT"] = str(port)
        else:
            env["MC_PORT"] = str(self.api_port)
        return env

    def start(self) -> None:
        if not self.python.exists():
            raise BlockedEnvironment(f"Python executable does not exist: {self.python}")
        handle = self.api_output.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(self.python), "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
             "--port", str(self.api_port), "--log-level", "warning"],
            cwd=str(BACKEND_DIR), env=self.env(port=self.api_port),
            stdout=handle, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + self.args.start_timeout
        last = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                detail = self.api_output.read_text(encoding="utf-8", errors="replace")
                raise RuntimeError(f"API exited during startup ({self.process.returncode}): {detail[-4000:]}")
            try:
                status, body = _http_request(self.base_url, "GET", "/api/ready", timeout=1)
                if 200 <= status < 300:
                    return
                last = (status, body)
            except Exception as exc:
                last = str(exc)
            time.sleep(0.2)
        raise TimeoutError(f"API did not become ready: {last}")

    def stop(self) -> None:
        if not self.process:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.process = None

    def sample(self) -> None:
        if self.process and self.process.poll() is None:
            self.resource_samples.append(_process_snapshot(self.process.pid))

    def start_second_owner(self) -> dict:
        handle = self.second_output.open("w", encoding="utf-8")
        second = subprocess.Popen(
            [str(self.python), "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
             "--port", str(self.second_port), "--log-level", "warning"],
            cwd=str(BACKEND_DIR), env=self.env(port=self.second_port),
            stdout=handle, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 8
        while second.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)
        if second.poll() is None:
            second.terminate()
            second.wait(timeout=10)
        output = self.second_output.read_text(encoding="utf-8", errors="replace")
        return {
            "returncode": second.returncode,
            "rejected": second.returncode not in (None, 0)
            and ("another Marketing Copilot instance" in output
                 or "instance" in output.lower() or "lock" in output.lower()),
            "output_tail": output[-4000:],
        }

    def direct_contender(self) -> dict:
        hold_code = r'''
import json, sys, urllib.request
request = urllib.request.Request(
    sys.argv[1] + "/api/internal/acceptance-hold",
    data=json.dumps({"seconds": 3}).encode(),
    headers={"Content-Type": "application/json", "X-MC-Backup-Token": sys.argv[2]},
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    response.read()
'''
        holder = subprocess.Popen(
            [str(self.python), "-c", hold_code, self.base_url,
             "local-lock-acceptance-token"],
            cwd=str(BACKEND_DIR), env=self.env(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        time.sleep(0.4)
        code = "import sys, duckdb; duckdb.connect(sys.argv[1])"
        contender = subprocess.run(
            [str(self.python), "-c", code, str(self.db_path)],
            capture_output=True, text=True, timeout=15, env=self.env(),
        )
        holder_stdout, holder_stderr = holder.communicate(timeout=10)
        diagnostics = f"stdout={contender.stdout}\nstderr={contender.stderr}"
        return {
            "returncode": contender.returncode,
            "rejected": contender.returncode != 0 and (
                "lock" in diagnostics.lower() or "conflict" in diagnostics.lower()
            ),
            "diagnostics": diagnostics[-4000:],
            "owner_hold_returncode": holder.returncode,
            "owner_hold_stderr": holder_stderr[-1000:],
        }

    def http_clients(self) -> dict:
        soak_duration = 0.0
        if self.args.profile == "smoke":
            client_count, writes_per_client = self.args.clients or 4, self.args.writes or 20
        elif self.args.profile == "stress":
            client_count, writes_per_client = self.args.clients or 8, self.args.writes or 125
        else:
            client_count, writes_per_client = self.args.clients or 4, self.args.writes or 25
            soak_duration = float(self.args.duration_seconds or 30)
        client_code = r'''
import json, sys, time, urllib.request, urllib.error
base, count, prefix, duration = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
deadline = time.perf_counter() + duration if duration > 0 else None
index = 0
while (count < 0 or index < count) and (deadline is None or time.perf_counter() < deadline):
    key = f"acceptance_{prefix}_{index:06d}"
    request = urllib.request.Request(
        base + "/api/settings/" + key,
        data=json.dumps({"value": str(index)}).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="PUT",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
            status = response.status
            error = ""
    except Exception as exc:
        status = 0
        error = f"{type(exc).__name__}: {exc}"
    print(json.dumps({"index": index, "status": status, "latency_ms": (time.perf_counter() - started) * 1000, "error": error}), flush=True)
    index += 1
'''
        processes = []
        started = time.perf_counter()
        for client in range(client_count):
            processes.append(subprocess.Popen(
                [str(self.python), "-c", client_code, self.base_url,
                 str(-1 if soak_duration else writes_per_client), f"c{client}",
                 str(soak_duration)],
                cwd=str(BACKEND_DIR), env=self.env(), stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
            ))
        rows = []
        process_errors = []
        for process in processes:
            stdout, stderr = process.communicate(
                timeout=max(60, int(soak_duration) + 30, client_count * writes_per_client // 2 + 30)
            )
            if process.returncode != 0:
                process_errors.append({"returncode": process.returncode, "stderr": stderr[-2000:]})
            for line in stdout.splitlines():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        self.sample()
        successful = [row for row in rows if 200 <= int(row.get("status", 0)) < 300]
        latencies = [float(row["latency_ms"]) for row in successful]
        return {
            "client_processes": client_count,
            "writes_per_client": writes_per_client,
            "requested": None if soak_duration else client_count * writes_per_client,
            "responses": len(rows),
            "successful": len(successful),
            "failed": len(rows) - len(successful),
            "process_errors": process_errors,
            "latency_ms": _percentiles(latencies),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "throughput_per_second": round(len(successful) / max(0.001, time.perf_counter() - started), 3),
            "duration_seconds": soak_duration or None,
            "pass": (
                (len(successful) == client_count * writes_per_client if not soak_duration else len(successful) > 0)
                and not process_errors
            ),
        }

    def lock_holds(self) -> dict:
        import database

        probe_db = self.root / "gate-probe.duckdb"
        database.set_database_file(str(probe_db))
        setup = database.get_connection()
        setup.execute("CREATE TABLE IF NOT EXISTS gate_probe(phase VARCHAR, value INTEGER)")
        setup.close()
        durations = [0.5, 3.0, 6.0, 12.0]
        results = []
        for duration in durations:
            owner = database.get_connection().set_operation(f"acceptance.injected_hold.{duration:g}s")
            owner.execute("BEGIN TRANSACTION")
            owner.execute("INSERT INTO gate_probe VALUES (?, ?)", (f"hold-{duration:g}", 1))
            observed = {}

            def contender():
                conn = database.get_connection().set_operation("acceptance.contender")
                conn.write_lock_timeout = 0.25
                started = time.perf_counter()
                try:
                    conn.execute("INSERT INTO gate_probe VALUES ('contender', 1)")
                    observed["result"] = "unexpected_success"
                except Exception as exc:
                    observed["result"] = "timeout" if isinstance(exc, TimeoutError) else type(exc).__name__
                    observed["error"] = str(exc)
                finally:
                    observed["elapsed_seconds"] = time.perf_counter() - started
                    conn.close()

            thread = threading.Thread(target=contender, name="acceptance-contender")
            started = time.perf_counter()
            thread.start()
            time.sleep(duration)
            owner.rollback()
            owner.close()
            thread.join(timeout=5)
            database.write_transaction(
                lambda conn: conn.execute("INSERT INTO gate_probe VALUES ('progress', ?)", (int(duration * 10),)),
                operation="acceptance.progress_after_hold",
            )
            results.append({
                "hold_seconds": duration,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "contender": observed,
                "released": not database.write_lock_status()["held"],
                "progress_after_release": True,
            })
        verify = database.get_connection()
        count = int(verify.execute("SELECT COUNT(*) AS count FROM gate_probe").fetchone()["count"])
        verify.close()
        return {
            "durations": results,
            "rows_after_recovery": count,
            "expected_rows": len(durations),
            "pass": all(
                item["released"] and item["progress_after_release"]
                and item["contender"].get("result") == "timeout"
                for item in results
            ) and count == len(durations),
        }

    def online_backup(self, expected_writes: int) -> dict:
        self.online_backup_dir.mkdir(parents=True, exist_ok=True)
        completed = subprocess.run(
            [str(self.python), str(TOOLS_DIR / "backup_database.py"),
             "--target-dir", str(self.online_backup_dir)],
            cwd=str(BACKEND_DIR), env=self.env(), capture_output=True, text=True, timeout=60,
        )
        path_text = completed.stdout.strip().splitlines()[-1] if completed.stdout.strip() else ""
        backup_path = Path(path_text) if path_text else None
        ready_status, ready_body = _http_request(self.base_url, "GET", "/api/ready")
        result = {
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
            "path": str(backup_path) if backup_path else None,
            "service_ready_after_backup": ready_status == 200,
            "service_ready_body": ready_body,
            "readable_after_service_stop": False,
            "marker_rows": 0,
        }
        if completed.returncode != 0 or not backup_path or not backup_path.exists():
            return result | {"pass": False}
        self.stop()
        try:
            import duckdb

            conn = duckdb.connect(str(backup_path), read_only=True)
            try:
                marker = conn.execute(
                    "SELECT COUNT(*) FROM app_settings WHERE key LIKE 'acceptance_%'"
                ).fetchone()[0]
                conn.execute("SELECT COUNT(*) FROM app_settings").fetchone()
                result["marker_rows"] = int(marker)
                result["readable_after_service_stop"] = True
            finally:
                conn.close()
        except Exception as exc:
            result["recovery_error"] = f"{type(exc).__name__}: {exc}"
        result["pass"] = bool(
            result["service_ready_after_backup"]
            and result["readable_after_service_stop"]
            and result["marker_rows"] == expected_writes
        )
        return result

    def run(self) -> dict:
        report = {
            "profile": self.args.profile,
            "started_at": _utc_now(),
            "git_commit": self.git_commit(),
            "root": str(self.root),
            "status": "FAIL",
            "live_env_status": "LIVE_ENV_UNVERIFIED",
        }
        try:
            self.start()
            self.sample()
            report["second_owner"] = self.start_second_owner()
            report["direct_duckdb_contender"] = self.direct_contender()
            report["http_concurrency"] = self.http_clients()
            report["lock_holds"] = self.lock_holds()
            expected = int(report["http_concurrency"]["successful"])
            report["online_backup"] = self.online_backup(expected)
            report["resource"] = {
                "samples": self.resource_samples,
                "max_rss_bytes": max((int(item.get("rss_bytes", 0)) for item in self.resource_samples), default=0),
                "max_threads": max((int(item.get("threads", 0)) for item in self.resource_samples), default=0),
            }
            report["status"] = "PASS" if all((
                report["second_owner"].get("rejected"),
                report["direct_duckdb_contender"].get("rejected"),
                report["http_concurrency"].get("pass"),
                report["lock_holds"].get("pass"),
                report["online_backup"].get("pass"),
            )) else "FAIL"
        except BlockedEnvironment as exc:
            report["status"] = "BLOCKED_RESOURCE"
            report["failure"] = str(exc)
        except Exception as exc:
            report["status"] = "FAIL"
            report["failure"] = f"{type(exc).__name__}: {exc}"
            report["traceback"] = traceback.format_exc()
        finally:
            self.stop()
            report["finished_at"] = _utc_now()
            report["artifacts_dir"] = str(self.root)
        return report

    @staticmethod
    def git_commit() -> str:
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True,
            ).strip()
        except Exception:
            return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "stress", "soak"), default="smoke")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--root", default="")
    parser.add_argument("--clients", type=int, default=0)
    parser.add_argument("--writes", type=int, default=0)
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--start-timeout", type=float, default=60)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    runner = LockAcceptance(args)
    report = runner.run()
    output = Path(args.output).expanduser() if args.output else runner.root / "acceptance.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"ACCEPTANCE_JSON={output}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
