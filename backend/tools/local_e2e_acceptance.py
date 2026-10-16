"""Local E2E, fault-injection and bounded stress acceptance runner.

This runner starts the real Marketing Copilot API (including DuckDB,
activity scheduler, realtime ingestion worker, SMS worker and SMPP gateway)
and a loopback-only SMPP simulator.  Only the external producer is simulated:
input files are published through the local watch-directory contract.

The runner intentionally uses HTTP APIs for normal setup and read-only DuckDB
queries for reconciliation.  The explicit ``sender-only`` gate is the one
exception: it stops the API before seeding pre-generated tasks into the
isolated database, then restarts the real service for the sender proof.
"""

from __future__ import annotations

import argparse
import csv
import faulthandler
import hashlib
import json
import os
import platform
import shutil
import signal
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
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parents[1]
DEFAULT_SCENARIOS = TOOLS_DIR / "local_e2e_scenarios.json"
sys.path.insert(0, str(TOOLS_DIR))
from local_e2e_data import make_msisdns, partition_rows, write_event_file, write_whitelist


class BlockedEnv(RuntimeError):
    pass


class HarnessConfigFail(RuntimeError):
    """The declared acceptance configuration did not become effective."""


def _load_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise BlockedEnv("duckdb is unavailable in the selected Python environment") from exc
    return duckdb


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _duration_ms(start: Any, end: Any) -> float | None:
    first, last = _parse_time(start), _parse_time(end)
    if not first or not last:
        return None
    return max(0.0, (last - first).total_seconds() * 1000.0)


def _percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None}
    ordered = sorted(float(value) for value in values)

    def percentile(p: float) -> float:
        if len(ordered) == 1:
            return round(ordered[0], 3)
        rank = (len(ordered) - 1) * p
        lower, upper = int(rank), min(len(ordered) - 1, int(rank) + 1)
        weight = rank - lower
        return round(ordered[lower] + (ordered[upper] - ordered[lower]) * weight, 3)

    return {"p50": percentile(0.50), "p95": percentile(0.95), "p99": percentile(0.99)}


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    events = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                events.append(value)
    return events


def _safe_process_snapshot(pid: int, role: str) -> dict:
    result = {"pid": pid, "role": role, "sampled_at": _utc_now()}
    if pid <= 0:
        return result
    try:
        if platform.system().lower() == "windows":
            command = (
                "$p=Get-Process -Id " + str(pid) + " -ErrorAction Stop; "
                "[pscustomobject]@{rss_bytes=$p.WorkingSet64;cpu_seconds=$p.CPU} "
                "| ConvertTo-Json -Compress"
            )
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                capture_output=True, text=True, timeout=2, check=True,
            )
            data = json.loads(completed.stdout.strip())
            result.update({
                "rss_bytes": int(data.get("rss_bytes") or 0),
                "cpu_seconds": float(data.get("cpu_seconds") or 0.0),
            })
        else:
            stat_path = Path(f"/proc/{pid}/stat")
            status_path = Path(f"/proc/{pid}/status")
            if stat_path.exists():
                fields = stat_path.read_text(encoding="utf-8").split()
                result["cpu_ticks"] = int(fields[13]) + int(fields[14])
            if status_path.exists():
                for line in status_path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("VmRSS:"):
                        result["rss_bytes"] = int(line.split()[1]) * 1024
                        break
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


class AcceptanceRunner:
    def __init__(self, args: argparse.Namespace, spec: dict):
        self.args = args
        self.spec = dict(spec)
        self.scenario = args.scenario
        self.gate = str(getattr(args, "gate", "full") or "full").strip().lower()
        if self.gate not in {"full", "ingest-only", "sender-only"}:
            raise ValueError("gate must be full, ingest-only, or sender-only")
        # Some managed Windows images expose a temp directory whose child
        # ACLs are stricter than its parent.  Keep the run isolated but place
        # it under the writable repository root by default.  WSL callers may
        # opt into an ext4-backed root (for example /tmp) so a DrvFS fsync
        # stall is not mistaken for an application lock or result-fold bug.
        configured_run_root = str(
            getattr(args, "run_root", "")
            or os.environ.get("MC_LOCAL_E2E_RUN_ROOT", "")
        ).strip()
        run_parent = (
            Path(configured_run_root).expanduser().resolve()
            if configured_run_root else REPO_ROOT / ".local-e2e-runs"
        )
        run_parent.mkdir(parents=True, exist_ok=True)
        self.root = run_parent / f"marketing-copilot-{self.scenario}-{self.gate}-{uuid.uuid4().hex[:8]}"
        self.root.mkdir(parents=True, exist_ok=False)
        self.db_path = self.root / "marketing_copilot.duckdb"
        self.watch_dir = self.root / "watch"
        self.campaign_dir = self.root / "campaign"
        self.whitelist_path = self.root / "whitelist.csv"
        self.smsc_event_log = self.root / "smsc.events.jsonl"
        self.smsc_metrics = self.root / "smsc.metrics.json"
        self.api_log = self.root / "api.log"
        self.smsc_log = self.root / "smsc.log"
        self.app_log = self.root / "marketing-copilot.log"
        self.api_port = _find_free_port()
        self.smsc_port = _find_free_port()
        self.processes: dict[str, subprocess.Popen] = {}
        self.log_handles = []
        self.file_manifests: list[dict] = []
        self.resource_samples: list[dict] = []
        self.backlog_samples: list[int] = []
        self.failures: list[str] = []
        self.smpp_config_id = None
        self.realtime_campaign_id = None
        self.template_id = None
        self.content_campaign_id = None
        self.whitelist_msisdns: list[str] = []
        self.rows: list[dict] = []
        self.started_at = time.time()
        self.last_snapshot: dict = {}
        self.last_snapshot_at = 0.0
        self.last_watchdog_metrics: dict = {}
        self.final_metrics: dict = {}
        self.final_metrics_path: Path | None = None
        self.last_runtime_snapshot_at = 0.0
        self.api_call_stats: dict[str, dict[str, float | int]] = {}
        self.config_audit: dict[str, Any] = {"status": "NOT_RUN"}
        self.diagnostics_dir = self.root / "diagnostics"
        self.watchdog_stop = threading.Event()
        self.watchdog_thread: threading.Thread | None = None
        self.watchdog_samples: list[dict] = []
        self.watchdog_events: list[dict] = []
        self.watchdog_last_progress: dict | None = None
        self.watchdog_last_progress_at = time.monotonic()
        self.watchdog_stall_30_emitted = False
        self.watchdog_stall_60_emitted = False
        self.task_worker_disabled = self.gate in {"ingest-only", "sender-only"}
        self.sender_seed: dict[str, Any] = {}
        # Keep the venv launcher path intact.  On Linux a venv's python is
        # commonly a symlink to the system interpreter; resolving it here
        # would make child API/SMSC processes lose the venv dependencies.
        self.python_executable = Path(args.python or sys.executable).absolute()
        self.delivery_receipt_mode = str(
            args.delivery_receipt_mode or self.spec.get("delivery_receipt_mode") or "disabled"
        )
        self.send_mode = str(
            getattr(args, "send_mode", None)
            or self.spec.get("send_mode")
            or os.environ.get("MC_SMS_SEND_MODE", "reliable")
        ).strip().lower()
        if self.send_mode not in {"reliable", "fast"}:
            raise ValueError("send mode must be reliable or fast")
        self.dispatch_mode = str(
            getattr(args, "dispatch_mode", None)
            or os.environ.get("MC_CAMPAIGN_DISPATCH_MODE", "legacy")
        ).strip().lower()
        if self.dispatch_mode not in {"legacy", "fair"}:
            raise ValueError("dispatch mode must be legacy or fair")
        self.db_concurrency = str(
            getattr(args, "db_concurrency", None)
            or os.environ.get("MC_DUCKDB_WRITE_CONCURRENCY", "serialized")
        ).strip().lower()
        if self.db_concurrency not in {"serialized", "occ"}:
            raise ValueError("database concurrency must be serialized or occ")
        self.pool_size = max(1, int(getattr(args, "pool_size", 1) or 1))
        self.flash_sms = bool(getattr(args, "flash_sms", False))
        self.no_live_observer = bool(getattr(args, "no_live_observer", False))

        events = int(args.events if args.events is not None else self.spec.get("events", 5))
        files = int(args.files if args.files is not None else self.spec.get("files", 1))
        self.event_count = max(1, events)
        self.file_count = max(1, min(files, self.event_count))
        self.expected_tasks = int(
            args.expected_tasks
            if args.expected_tasks is not None
            else self.spec.get("expected_tasks", self.event_count)
        )
        self.expected_status_counts = {
            str(key): int(value)
            for key, value in (self.spec.get("expected_status_counts") or {}).items()
        }
        self.expected_submit_events = int(
            self.spec.get("expected_submit_events", self.expected_tasks)
        )
        if args.events is not None and self.scenario == "quick":
            self.expected_tasks = max(1, self.event_count - 2)
        self.timeout_seconds = int(
            args.timeout_seconds or self.spec.get("timeout_seconds", 120)
        )
        self.faults = dict(self.spec.get("faults") or {})
        self.legacy_smsc_tps = args.smsc_tps is not None
        injected_fault_keys = (
            "response_delay", "reject_ratio", "drop_response_ratio",
            "drop_receipt_ratio", "disconnect_after", "disconnect_before_response",
            "partial_submit_after",
            "drop_receipts",
        )
        inferred_fault_mode = any(
            bool(self.faults.get(key)) for key in injected_fault_keys
        ) or self.spec.get("delivery_receipt_mode") in {"optional", "required"}
        self.simulator_mode = str(
            getattr(args, "simulator_mode", None)
            or self.spec.get("simulator_mode")
            or ("fault" if inferred_fault_mode else "fast")
        ).strip().lower()
        if self.simulator_mode not in {"fast", "fault"}:
            raise ValueError("simulator mode must be fast or fault")
        self.simulator_audit = str(
            getattr(args, "simulator_audit", None)
            or self.spec.get("simulator_audit")
            or ("fast" if self.simulator_mode == "fast" else "full")
        ).strip().lower()
        if self.simulator_audit not in {"fast", "full", "off"}:
            raise ValueError("simulator audit must be fast, full, or off")
        if args.smsc_tps is not None:
            self.legacy_smsc_tps = True
        if args.smsc_response_delay is not None:
            self.faults["response_delay"] = float(args.smsc_response_delay)
        if args.smsc_reject_ratio is not None:
            self.faults["reject_ratio"] = float(args.smsc_reject_ratio)
        if args.smsc_drop_response_ratio is not None:
            self.faults["drop_response_ratio"] = float(args.smsc_drop_response_ratio)
        if args.smsc_drop_receipt_ratio is not None:
            self.faults["drop_receipt_ratio"] = float(args.smsc_drop_receipt_ratio)
        if args.smsc_receipt_delay is not None:
            self.faults["dlr_delay"] = float(args.smsc_receipt_delay)
        if args.smsc_disconnect_after is not None:
            self.faults["disconnect_after"] = int(args.smsc_disconnect_after)
        if args.smsc_disconnect_before_response is not None:
            self.faults["disconnect_before_response"] = int(
                args.smsc_disconnect_before_response
            )
        if self.simulator_mode == "fast" and any(
            bool(self.faults.get(key)) for key in injected_fault_keys
        ):
            raise ValueError(
                "fast simulator mode cannot combine with fault injection; "
                "use --simulator-mode fault"
            )
        if self.simulator_mode == "fast" and self.delivery_receipt_mode != "disabled":
            raise ValueError("fast simulator mode requires delivery_receipt_mode=disabled")
        if self.legacy_smsc_tps:
            legacy_tps = float(args.smsc_tps)
            self.simulator_tps = max(0.0, legacy_tps)
            self.route_tps = max(1.0, legacy_tps)
            self.tps_scope = "legacy_both"
        else:
            simulator_tps = (
                getattr(args, "simulator_tps", None)
                if getattr(args, "simulator_tps", None) is not None
                else self.spec.get(
                    "simulator_tps",
                    self.faults.get("tps", 0.0) if self.simulator_mode == "fault" else 0.0,
                )
            )
            route_tps = (
                getattr(args, "route_tps", None)
                if getattr(args, "route_tps", None) is not None
                else self.spec.get("route_tps", self.faults.get("tps", 200))
            )
            self.simulator_tps = max(0.0, float(simulator_tps if simulator_tps is not None else 0.0))
            self.route_tps = max(1.0, float(route_tps if route_tps is not None else 200.0))
            self.tps_scope = "separate"
        self.simulator_response_delay = max(
            0.0,
            float(
                getattr(args, "simulator_response_delay", None)
                if getattr(args, "simulator_response_delay", None) is not None
                else self.faults.get("response_delay", 0.0)
            ),
        )
        self.simulator_max_inflight = max(
            1, int(getattr(args, "simulator_max_inflight", 256) or 256)
        )
        self.duration_seconds = int(
            args.duration_seconds or self.spec.get("duration_seconds", 0) or 0
        )
        if self.scenario == "stability" and not self.duration_seconds:
            self.duration_seconds = 1800

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    def _environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update({
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str(TOOLS_DIR.parent),
            "DUCKDB_PATH": str(self.db_path),
            "MC_DB_CONFIG_FILE": str(self.root / "db_config.json"),
            "MC_STATE_DIR": str(self.root / "state"),
            "MC_UPDATE_DIR": str(self.root / "updates"),
            "MC_BACKUP_DIR": str(self.root / "backups"),
            "MC_FRONTEND_DIR": str(self.root / "no-dist"),
            "MC_INSTANCE_LOCK": str(self.root / "instance.lock"),
            "MC_LOG_PATH": str(self.root / "marketing-copilot.log"),
            "MC_STALL_DIAGNOSTICS_DIR": str(self.diagnostics_dir),
            "MC_STALL_DIAGNOSTICS_THRESHOLD_SECONDS": "30",
            "MC_SYSTEM_MODE": "production",
            "MC_AUTH_DISABLED": "1",
            "MC_LOG_LEVEL": "INFO",
            "MC_DETAIL_LOGGING": "0",
            "MC_REALTIME_REQUIRE_READY": "1",
            "MC_REALTIME_WATCH_STABILITY_SECONDS": "0.5",
            "MC_REALTIME_WATCH_RESCAN_SECONDS": "3",
            "MC_REALTIME_WATCH_PENDING_RECHECK_SECONDS": "0.2",
            "MC_REALTIME_COPY_WORKERS": str(min(8, max(1, self.args.workers))),
            "MC_REALTIME_INGEST_WORKERS": str(min(16, max(1, self.args.ingest_workers))),
            "MC_TASK_WORKERS": str(min(32, max(1, self.args.task_workers))),
            "MC_DISABLE_TASK_WORKER": "1" if self.task_worker_disabled else "0",
            "MC_CLAIM_BATCH": str(min(500, max(1, self.args.claim_batch))),
            "MC_SMPP_RESPONSE_ERROR_LOG_INTERVAL": "0",
            "MC_SMPP_WINDOW_SIZE": str(max(1, self.args.smpp_window)),
            "MC_SMPP_SEND_QUEUE_CAPACITY": str(max(1, self.args.smpp_send_queue_capacity)),
            "MC_TASK_ARCHIVE_ENABLED": "false",
            "MC_SMS_SEND_MODE": self.send_mode,
            "MC_CAMPAIGN_DISPATCH_MODE": self.dispatch_mode,
            "MC_DUCKDB_WRITE_CONCURRENCY": self.db_concurrency,
        })
        return env

    def _spawn(self, role: str, command: list[str], cwd: Path | None = None) -> None:
        log_path = self.api_log if role == "api" else self.smsc_log
        handle = log_path.open("w", encoding="utf-8")
        self.log_handles.append(handle)
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        self.processes[role] = subprocess.Popen(
            command,
            cwd=str(cwd or REPO_ROOT),
            env=self._environment(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )

    def _fault_args(self) -> list[str]:
        args = [
            "--host", "127.0.0.1", "--port", str(self.smsc_port),
            "--system-id", "test", "--password", "test",
            "--receipt-delay", str(float(self.faults.get("dlr_delay", 0.1))),
            "--simulator-mode", self.simulator_mode,
            "--simulator-response-delay", str(self.simulator_response_delay),
            "--simulator-tps", str(self.simulator_tps),
            "--reject-ratio", str(float(self.faults.get("reject_ratio", 0.0))),
            "--drop-response-ratio", str(float(self.faults.get("drop_response_ratio", 0.0))),
            "--drop-receipt-ratio", str(float(self.faults.get("drop_receipt_ratio", 0.0))),
            "--disconnect-after", str(int(self.faults.get("disconnect_after", 0))),
            "--disconnect-before-response",
            str(int(self.faults.get("disconnect_before_response", 0))),
            "--partial-submit-after", str(int(self.faults.get("partial_submit_after", 0))),
            "--partial-submit-status", str(self.faults.get("partial_submit_status", "0xb")),
            "--seed", str(int(self.faults.get("seed", 1))),
            "--simulator-audit", self.simulator_audit,
            "--audit-queue-size", str(int(getattr(self.args, "simulator_audit_queue_size", 8192) or 8192)),
            "--audit-batch-size", str(int(getattr(self.args, "simulator_audit_batch_size", 128) or 128)),
            "--audit-flush-interval", str(float(getattr(self.args, "simulator_audit_flush_interval", 0.05) or 0.05)),
            "--max-inflight-per-connection", str(self.simulator_max_inflight),
            "--event-log", str(self.smsc_event_log),
            "--metrics-output", str(self.smsc_metrics),
        ]
        if self.faults.get("drop_receipts"):
            args.append("--drop-receipts")
        return args

    def start(self) -> None:
        if self.python_executable.name.lower().startswith("python") is False:
            raise BlockedEnv(f"selected executable is not Python: {self.python_executable}")
        if not self.python_executable.exists():
            raise BlockedEnv(f"Python executable does not exist: {self.python_executable}")
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.campaign_dir.mkdir(parents=True, exist_ok=True)
        (self.root / "run-manifest.json").write_text(
            json.dumps(
                {
                    "scenario": self.scenario,
                    "gate": self.gate,
                    "simulator_mode": self.simulator_mode,
                    "simulator_tps": self.simulator_tps,
                    "simulator_response_delay": self.simulator_response_delay,
                    "simulator_audit": self.simulator_audit,
                    "route_tps": self.route_tps,
                    "tps_scope": self.tps_scope,
                    "delivery_receipt_mode": self.delivery_receipt_mode,
                    "send_mode": self.send_mode,
                    "dispatch_mode": self.dispatch_mode,
                    "db_concurrency": self.db_concurrency,
                    "faults": self.faults,
                    "pool_size": self.pool_size,
                    "window_size": max(1, self.args.smpp_window),
                    "legacy_smsc_tps": self.legacy_smsc_tps,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self._spawn(
            "smsc",
            [str(self.python_executable), str(TOOLS_DIR / "smsc_simulator.py"), *self._fault_args()],
            cwd=REPO_ROOT,
        )
        self._wait_for_port(self.smsc_port, self.timeout_seconds, "SMSC simulator")
        self._spawn(
            "api",
            [str(self.python_executable), "-m", "uvicorn", "main:app",
             "--host", "127.0.0.1", "--port", str(self.api_port), "--log-level", "warning"],
            cwd=TOOLS_DIR.parent,
        )
        self._wait_http("/api/ready", self.timeout_seconds, "Marketing Copilot API ready")

    def _wait_for_port(self, port: int, timeout: float, label: str) -> None:
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            process = self.processes.get("smsc")
            if process and process.poll() is not None:
                raise RuntimeError(f"{label} exited with code {process.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    return
            except OSError as exc:
                last_error = str(exc)
                time.sleep(0.1)
        raise TimeoutError(f"Timed out waiting for {label}: {last_error}")

    def _http(self, method: str, path: str, payload: Any = None,
              *, body: bytes | None = None, headers: dict[str, str] | None = None,
              timeout: float = 10) -> tuple[int, Any]:
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        data = body
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path, data=data, headers=request_headers, method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                if not raw:
                    return response.status, None
                try:
                    return response.status, json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    return response.status, raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                body_value = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                body_value = raw.decode("utf-8", errors="replace")
            return exc.code, body_value

    def _api_json(self, method: str, path: str, payload: Any = None) -> Any:
        endpoint = path.split("?", 1)[0]
        key = f"{method.upper()} {endpoint}"
        started = time.perf_counter()
        status = 0
        body = None
        try:
            status, body = self._http(method, path, payload)
            if status < 200 or status >= 300:
                raise RuntimeError(f"API {method} {path} -> HTTP {status}: {body}")
            return body
        finally:
            elapsed = max(0.0, time.perf_counter() - started)
            item = self.api_call_stats.setdefault(key, {
                "calls": 0, "total_seconds": 0.0, "max_seconds": 0.0,
                "response_bytes_estimate": 0,
            })
            item["calls"] = int(item["calls"]) + 1
            item["total_seconds"] = float(item["total_seconds"]) + elapsed
            item["max_seconds"] = max(float(item["max_seconds"]), elapsed)
            if body is not None:
                try:
                    item["response_bytes_estimate"] = int(item["response_bytes_estimate"]) + len(
                        json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
                    )
                except (TypeError, ValueError):
                    pass

    def _assert_smpp_effective_config(
        self,
        declared: dict[str, Any],
        *,
        require_bind: bool = True,
        event_log: Path | None = None,
    ) -> dict:
        """Prove CLI values reached DB, gateway status and physical binds."""
        config_id = int(declared["config_id"])
        db_config = self._api_json("GET", f"/api/smpp/configs/{config_id}") or {}
        status = self._api_json("GET", "/api/smpp/status") or {}
        pool = next(
            (item for item in (status.get("pools") or [])
             if int(item.get("config_id") or 0) == config_id),
            None,
        ) or {}
        connections = list(pool.get("connections") or [])
        bind_count = sum(1 for item in connections if item.get("connected"))
        expected_pool = max(1, int(declared["pool_size"]))
        expected_window = max(1, int(declared["window_size"]))
        expected_route_tps = float(declared.get("route_tps", declared["tps"]))
        db_effective = {
            "id": int(db_config.get("id") or 0),
            "pool_size": int(db_config.get("pool_size") or 0),
            "window_size": int(db_config.get("window_size") or 0),
            "tps": float(db_config.get("tps") or 0),
            "updated_at": db_config.get("updated_at"),
            "host": db_config.get("host"),
            "port": int(db_config.get("port") or 0),
        }
        gateway_effective = {
            "desired_pool_size": int(pool.get("desired_pool_size") or 0),
            "connection_count": len(connections),
            "connected_count": bind_count,
            "connections": [
                {
                    "slot": item.get("slot"),
                    "connected": bool(item.get("connected")),
                    "window_size": int(item.get("window_size") or 0),
                    "configured_route_tps": float(item.get("configured_route_tps") or 0),
                    "effective_total_limit_tps": float(item.get("effective_total_limit_tps") or 0),
                    "tps_scope": item.get("tps_scope"),
                    "window_unit": item.get("window_unit"),
                }
                for item in connections
            ],
            "active_config_id": status.get("active_config_id"),
        }
        bind_event_path = event_log or self.smsc_event_log
        bind_events = [
            item for item in _read_jsonl(bind_event_path)
            if item.get("event_type") == "bind" and item.get("success")
        ]
        mismatches = []
        if db_effective["id"] != config_id:
            mismatches.append(f"db.id={db_effective['id']} expected={config_id}")
        if db_effective["pool_size"] != expected_pool:
            mismatches.append(f"db.pool_size={db_effective['pool_size']} expected={expected_pool}")
        if db_effective["window_size"] != expected_window:
            mismatches.append(f"db.window_size={db_effective['window_size']} expected={expected_window}")
        if db_effective["tps"] != expected_route_tps:
            mismatches.append(f"db.tps={db_effective['tps']} expected={expected_route_tps}")
        if gateway_effective["desired_pool_size"] != expected_pool:
            mismatches.append(
                f"gateway.desired_pool_size={gateway_effective['desired_pool_size']} expected={expected_pool}"
            )
        if require_bind and gateway_effective["connected_count"] != expected_pool:
            mismatches.append(
                f"gateway.connected_count={gateway_effective['connected_count']} expected={expected_pool}"
            )
        if require_bind and len(bind_events) < expected_pool:
            mismatches.append(f"simulator.bind_success={len(bind_events)} expected_at_least={expected_pool}")
        for item in gateway_effective["connections"]:
            if item["window_size"] != expected_window:
                mismatches.append(
                    f"gateway.slot{item['slot']}.window_size={item['window_size']} expected={expected_window}"
                )
            if item["configured_route_tps"] != expected_route_tps:
                mismatches.append(
                    f"gateway.slot{item['slot']}.configured_route_tps={item['configured_route_tps']} expected={expected_route_tps}"
                )
            if item["effective_total_limit_tps"] > expected_route_tps + 1e-9:
                mismatches.append(
                    f"gateway.slot{item['slot']}.effective_total_limit_tps={item['effective_total_limit_tps']} exceeds shared route={expected_route_tps}"
                )
        self.config_audit = {
            "status": "PASS" if not mismatches else "HARNESS_CONFIG_FAIL",
            "declared": dict(declared),
            "db_effective": db_effective,
            "gateway_effective": gateway_effective,
            "simulator_effective": {
                "bind_success_events": len(bind_events),
                "event_log": str(bind_event_path),
            },
            "route_tps_scope": "route_shared_submit_sm_parts_per_second",
            "mismatches": mismatches,
        }
        if mismatches:
            raise HarnessConfigFail("; ".join(mismatches))
        return self.config_audit

    def _live_transport_totals(self, timeout: float = 2) -> dict[str, int]:
        payload = self._api_json("GET", "/api/metrics/live") or {}
        totals = {
            "scheduled_parts": 0,
            "socket_parts": 0,
            "response_success": 0,
            "response_failure": 0,
        }
        for row in payload.get("campaign_transport") or []:
            totals["scheduled_parts"] += int(float(row.get("scheduled_parts_total") or 0))
            totals["socket_parts"] += int(float(row.get("socket_parts_total") or 0))
            totals["response_success"] += int(float(row.get("submit_sm_resp_success_total") or 0))
            totals["response_failure"] += int(float(row.get("submit_sm_resp_failure_total") or 0))
        totals["backlog_parts"] = max(0, totals["scheduled_parts"] - totals["socket_parts"])
        return totals

    def _wait_http(self, path: str, timeout: float, label: str) -> Any:
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            process = self.processes.get("api")
            if process and process.poll() is not None:
                raise RuntimeError(f"API exited with code {process.returncode}")
            try:
                status, body = self._http("GET", path)
            except urllib.error.URLError as exc:
                last = {"status": 0, "body": str(exc)}
                time.sleep(0.25)
                continue
            last = {"status": status, "body": body}
            if 200 <= status < 300:
                return body
            time.sleep(0.25)
        raise TimeoutError(f"Timed out waiting for {label}: {last}")

    def _wait_until(self, predicate, timeout: float, label: str, interval: float = 0.5):
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            self._sample_runtime()
            try:
                value = predicate()
                if value:
                    return value
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(interval)
        raise TimeoutError(f"Timed out waiting for {label}; last_error={last_error}")

    def _sample_runtime(self) -> None:
        for role, process in self.processes.items():
            returncode = process.poll()
            if role == "api" and returncode is not None:
                if not any(
                    item.get("event") == "process_exit" and item.get("role") == role
                    for item in self.watchdog_events
                ):
                    self._capture_process_exit(role, returncode)
                raise RuntimeError(f"API exited with code {returncode} (0x{returncode & 0xffffffff:08X})")
            if returncode is None:
                self.resource_samples.append(_safe_process_snapshot(process.pid, role))
        # Do not open DuckDB from this controller while the application is
        # running.  The application already exposes the same counters through
        # its read APIs, and a second Windows process can otherwise hold a
        # transient DuckDB lock against the real workers.
        now = time.monotonic()
        if not self.no_live_observer and now - self.last_runtime_snapshot_at >= 1.0:
            snapshot = self._live_snapshot(timeout=2)
            self.last_runtime_snapshot_at = now
            if snapshot:
                self.last_snapshot = snapshot
                self.last_snapshot_at = now
                self.backlog_samples.append(int(snapshot.get("backlog", 0)))
        if self.args.max_memory_mb:
            limit = int(self.args.max_memory_mb) * 1024 * 1024
            if any(int(item.get("rss_bytes", 0) or 0) > limit for item in self.resource_samples[-4:]):
                raise RuntimeError(f"resource threshold exceeded: rss>{self.args.max_memory_mb}MB")

    def _db_query(self, sql: str, params: tuple = ()) -> list[dict]:
        if not self.db_path.exists():
            return []
        duckdb = _load_duckdb()
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            cursor = conn.execute(sql, params)
            columns = [item[0] for item in cursor.description or []]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()

    def _db_snapshot(self) -> dict:
        try:
            files = self._db_query(
                "SELECT status,COUNT(*) AS count FROM ingestion_files WHERE campaign_id=? GROUP BY status",
                (self.realtime_campaign_id or -1,),
            )
            tasks = self._db_query(
                "SELECT status,COUNT(*) AS count FROM sms_tasks WHERE campaign_type='realtime' AND campaign_id=? GROUP BY status",
                (self.realtime_campaign_id or -1,),
            )
            file_counts = {str(row["status"]): int(row["count"]) for row in files}
            task_counts = {str(row["status"]): int(row["count"]) for row in tasks}
            return {
                "files": file_counts,
                "tasks": task_counts,
                "task_total": sum(task_counts.values()),
                "task_terminal": sum(task_counts.get(key, 0) for key in ("delivered", "accepted", "failed", "partial", "unknown", "expired", "cancelled")),
                "backlog": sum(task_counts.get(key, 0) for key in ("queued", "sending", "retrying")),
            }
        except Exception:
            return {}

    def _live_snapshot(self, timeout: float = 10) -> dict:
        if not self.realtime_campaign_id:
            return {}
        try:
            status, stats = self._http(
                "GET", f"/api/realtime-campaigns/{self.realtime_campaign_id}/stats",
                timeout=timeout,
            )
            if status < 200 or status >= 300 or not isinstance(stats, dict):
                return {}
            task_keys = (
                "queued", "sending", "submitted", "accepted", "delivered",
                "retrying", "failed", "partial", "unknown", "expired", "cancelled",
            )
            task_counts = {
                key: int(stats.get(f"today_{key}_users", 0) or 0)
                for key in task_keys
            }
            expected_status = self._expected_terminal_status()
            if expected_status in {"failed", "unknown", "partial"}:
                # The realtime campaign summary predates these terminal
                # outcomes.  Use the normal task API for the bounded fault
                # scenarios instead of opening DuckDB from this observer.
                listed_status, listed = self._http(
                    "GET",
                    f"/api/sms-tasks?campaign_type=realtime&status={expected_status}&page_size=200",
                    timeout=timeout,
                )
                if listed_status < 200 or listed_status >= 300 or not isinstance(listed, dict):
                    listed = {}
                task_counts[expected_status] = max(
                    task_counts.get(expected_status, 0),
                    len(listed.get("items") or []),
                )
            task_total = int(stats.get("today_task_created_users", 0) or 0)
            return {
                "files": {"completed": int(stats.get("today_file_count", 0) or 0)},
                "tasks": task_counts,
                "task_total": task_total,
                "task_terminal": sum(task_counts.get(key, 0) for key in ("delivered", "accepted", "failed", "partial", "unknown", "expired", "cancelled")),
                "backlog": sum(task_counts.get(key, 0) for key in ("queued", "sending", "retrying")),
            }
        except Exception:
            return {}

    def _watchdog_metrics(self) -> dict:
        if self.no_live_observer:
            return {"disabled": True}
        try:
            status, body = self._http("GET", "/api/metrics/live", timeout=2)
            if status < 200 or status >= 300:
                return {"error": f"HTTP {status}", "body": body}
            return body if isinstance(body, dict) else {"body": body}
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def _capture_final_metrics(self) -> None:
        """Capture one full metrics snapshot after business completion.

        Live polling uses only bounded in-process counters.  This single
        post-completion snapshot preserves DB/result/claim timing evidence
        without turning the observer into a repeated workload participant.
        """
        try:
            body = self._api_json("GET", "/api/metrics/status")
            if not isinstance(body, dict):
                return
            self.final_metrics = body
            path = self.root / "final-metrics.json"
            path.write_text(
                json.dumps(body, ensure_ascii=False, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
            self.final_metrics_path = path
        except Exception as exc:
            self.failures.append(f"final metrics snapshot failed: {type(exc).__name__}: {exc}")

    def _observability_report(self) -> dict:
        return {
            "live": self.last_watchdog_metrics,
            "final_metrics_path": str(self.final_metrics_path) if self.final_metrics_path else None,
            "final_metrics_captured": bool(self.final_metrics),
        }

    def _watchdog_db_snapshot(self) -> dict:
        """Describe the deferred database part of a live watchdog snapshot.

        DuckDB permits only compatible connection modes across processes.  A
        second process opening the live database, even read-only, can hold the
        file lock against the application's read-write connection.  The
        watchdog therefore uses the live API/metrics/thread snapshots while
        the service is running; ``_reconcile`` performs the authoritative
        database aggregate after both child processes have stopped.
        """
        return {
            "status": "deferred_until_shutdown",
            "reason": "opening a second live DuckDB connection can create a false file-lock stall",
            "campaign_id": self.realtime_campaign_id,
            "authoritative_source": "post_shutdown_reconciliation",
        }

    def _watchdog_tail(self, path: Path, limit: int = 120) -> list[str]:
        text = self._read_log(path)
        return text.splitlines()[-limit:]

    def _watchdog_snapshot(self, full: bool = False) -> dict:
        live = {} if self.no_live_observer else self._live_snapshot(timeout=2)
        metrics = self._watchdog_metrics()
        if metrics and not metrics.get("error"):
            self.last_watchdog_metrics = metrics
        transport = {
            "scheduled_parts": 0,
            "socket_parts": 0,
            "response_success": 0,
            "response_failure": 0,
        }
        for row in metrics.get("campaign_transport") or []:
            transport["scheduled_parts"] += int(float(row.get("scheduled_parts_total") or 0))
            transport["socket_parts"] += int(float(row.get("socket_parts_total") or 0))
            transport["response_success"] += int(float(row.get("submit_sm_resp_success_total") or 0))
            transport["response_failure"] += int(float(row.get("submit_sm_resp_failure_total") or 0))
        task_counts = live.get("tasks") or {}
        progress = {
            "generated": int(live.get("task_total", 0) or 0),
            "scheduled_parts": transport["scheduled_parts"],
            "socket_submit": transport["socket_parts"],
            "response_observed": transport["response_success"] + transport["response_failure"],
            "task_accepted": int(task_counts.get("accepted", 0) or 0),
        }
        process_states = {
            role: {
                "pid": process.pid,
                "running": process.poll() is None,
                "returncode": process.poll(),
            }
            for role, process in self.processes.items()
        }
        snapshot = {
            "captured_at": _utc_now(),
            "progress": progress,
            "live_snapshot": live,
            "metrics": metrics,
            "process_states": process_states,
            "processes": [
                _safe_process_snapshot(process.pid, role)
                for role, process in self.processes.items()
                if process.poll() is None
            ],
        }
        if full:
            snapshot.update({
                "database": self._watchdog_db_snapshot(),
                "logs": {
                    "app": self._watchdog_tail(self.app_log),
                    "api": self._watchdog_tail(self.api_log),
                    "smsc": self._watchdog_tail(self.smsc_log),
                },
            })
            try:
                if self.no_live_observer:
                    raise RuntimeError("live observer disabled")
                status, body = self._http(
                    "GET", "/api/metrics/stall-diagnostics", timeout=2
                )
                snapshot["api_stacks"] = {
                    "status": status,
                    "body": body,
                }
            except Exception as exc:
                snapshot["api_stacks"] = {
                    "error": f"{type(exc).__name__}: {exc}"
                }
            stack_path = self.diagnostics_dir / (
                f"controller-threads-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.txt"
            )
            try:
                with stack_path.open("w", encoding="utf-8") as handle:
                    faulthandler.dump_traceback(file=handle, all_threads=True)
                snapshot["controller_stack_file"] = str(stack_path)
            except OSError as exc:
                snapshot["controller_stack_error"] = str(exc)
        return snapshot

    def _watchdog_capture(self, threshold: int, snapshot: dict) -> None:
        self.diagnostics_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.diagnostics_dir / f"stall-{threshold}s-{stamp}.json"
        payload = {
            "threshold_seconds": threshold,
            "no_progress_seconds": round(max(0.0, time.monotonic() - self.watchdog_last_progress_at), 3),
            "last_progress": self.watchdog_last_progress,
            "snapshot": snapshot,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        self.watchdog_events.append({
            "threshold_seconds": threshold,
            "captured_at": payload["snapshot"].get("captured_at"),
            "path": str(path),
            "full": threshold >= 60,
        })

    def _capture_process_exit(self, role: str, returncode: int) -> None:
        self.diagnostics_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.diagnostics_dir / f"process-exit-{role}-{stamp}.json"
        try:
            snapshot = self._watchdog_snapshot(full=True)
            path.write_text(json.dumps({
                "event": "process_exit",
                "role": role,
                "returncode": returncode,
                "returncode_hex": f"0x{returncode & 0xffffffff:08X}",
                "captured_at": _utc_now(),
                "snapshot": snapshot,
            }, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
            self.watchdog_events.append({
                "event": "process_exit",
                "role": role,
                "returncode": returncode,
                "returncode_hex": f"0x{returncode & 0xffffffff:08X}",
                "captured_at": snapshot.get("captured_at"),
                "path": str(path),
                "full": True,
            })
        except Exception as exc:
            self.watchdog_events.append({
                "event": "process_exit_diagnostic_failed",
                "role": role,
                "returncode": returncode,
                "error": f"{type(exc).__name__}: {exc}",
            })

    def _watchdog_loop(self) -> None:
        while not self.watchdog_stop.wait(5):
            try:
                snapshot = self._watchdog_snapshot(full=False)
                progress = snapshot["progress"]
                now = time.monotonic()
                if self.watchdog_last_progress is None or progress != self.watchdog_last_progress:
                    self.watchdog_last_progress = dict(progress)
                    self.watchdog_last_progress_at = now
                    self.watchdog_stall_30_emitted = False
                    self.watchdog_stall_60_emitted = False
                no_progress = now - self.watchdog_last_progress_at
                sample = {
                    "captured_at": snapshot.get("captured_at"),
                    "progress": progress,
                    "no_progress_seconds": round(no_progress, 3),
                    "live_snapshot": snapshot.get("live_snapshot", {}),
                    "metrics": snapshot.get("metrics", {}),
                    "process_states": snapshot.get("process_states", {}),
                }
                self.watchdog_samples.append(sample)
                if no_progress >= 30 and not self.watchdog_stall_30_emitted:
                    self._watchdog_capture(30, snapshot)
                    self.watchdog_stall_30_emitted = True
                if no_progress >= 60 and not self.watchdog_stall_60_emitted:
                    self._watchdog_capture(60, self._watchdog_snapshot(full=True))
                    self.watchdog_stall_60_emitted = True
            except Exception as exc:
                self.watchdog_samples.append({
                    "captured_at": _utc_now(),
                    "error": f"{type(exc).__name__}: {exc}",
                })

    def _start_watchdog(self) -> None:
        if self.watchdog_thread and self.watchdog_thread.is_alive():
            return
        self.watchdog_stop.clear()
        self.watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="local-e2e-no-progress-watchdog",
            daemon=True,
        )
        self.watchdog_thread.start()

    def _stop_watchdog(self) -> None:
        self.watchdog_stop.set()
        thread = self.watchdog_thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        self.watchdog_thread = None

    def _watchdog_report(self) -> dict:
        return {
            "status": "STALL_CAPTURED" if self.watchdog_events else "NO_STALL_CAPTURED",
            "progress_last": self.watchdog_last_progress,
            "samples": len(self.watchdog_samples),
            "process_states_last": (
                self.watchdog_samples[-1].get("process_states", {})
                if self.watchdog_samples else {}
            ),
            "events": list(self.watchdog_events),
            "metrics_last": self.last_watchdog_metrics,
            "diagnostics_dir": str(self.diagnostics_dir) if self.watchdog_events else None,
        }

    def _upload_multipart(self, path: str, field: str, filename: str, content: bytes) -> Any:
        boundary = f"----MarketingCopilotE2E{uuid.uuid4().hex}"
        body = b"--" + boundary.encode() + b"\r\n"
        body += f'Content-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'.encode()
        body += b"Content-Type: text/csv\r\n\r\n" + content + b"\r\n"
        body += b"--" + boundary.encode() + b"--\r\n"
        status, value = self._http(
            "POST", path, body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        if status < 200 or status >= 300:
            raise RuntimeError(f"upload {path} -> HTTP {status}: {value}")
        return value

    def _wait_for_smpp(self) -> None:
        self._wait_until(
            lambda: bool((self._api_json("GET", "/api/health").get("smpp") or {}).get("connected")),
            self.timeout_seconds, "SMPP bind connected",
        )

    def _provision(self) -> None:
        declared_smpp = {
            "name": "local-e2e-loopback",
            "host": "127.0.0.1",
            "port": self.smsc_port,
            "system_id": "test",
            "password": "test",
            "source_addr": "E2E",
            "bind_type": "transceiver",
            "pool_size": self.pool_size,
            "window_size": max(1, self.args.smpp_window),
            "enquire_interval": 5,
            "timeout": 2 if self.faults.get("drop_response_ratio") else 10,
            # This is the Marketing Copilot route-shared pacing contract.
            # The simulator's artificial pacing is passed to its own process
            # and is intentionally not reused here.
            "tps": self.route_tps,
            "priority": 1,
            "reconnect_max_seconds": 2,
            "delivery_receipt_mode": self.delivery_receipt_mode,
            "enabled": 1,
        }
        smpp = self._api_json("POST", "/api/smpp/configs", declared_smpp)
        self.smpp_config_id = int(smpp["id"])
        declared_smpp["config_id"] = self.smpp_config_id
        self._declared_smpp_config = declared_smpp

        self._api_json("POST", "/api/realtime-campaigns", {
            "name": f"local-e2e-{self.scenario}",
            "use_whitelist": True,
            "campaign_dir": str(self.campaign_dir),
            "use_custom_directory": True,
            "watch_source": "local",
            "watch_dir": str(self.watch_dir),
            "watch_filename_prefix": "E2E_",
            "phone_prefix": "251",
            "smpp_config_id": self.smpp_config_id,
            "flash_sms": self.flash_sms,
        })
        campaigns = self._api_json("GET", "/api/realtime-campaigns")
        self.realtime_campaign_id = int(campaigns["items"][0]["id"])

        content_campaign = self._api_json("POST", "/api/sms-content/campaigns", {
            "name": f"local-e2e-template-{self.scenario}",
            "marketing_type": "realtime",
            "marketing_campaign_id": self.realtime_campaign_id,
            "language": "English",
        })
        self.content_campaign_id = int(content_campaign["id"])
        message = str(self.spec.get("message") or "Marketing Copilot local E2E event={event_id} msisdn={msisdn}")
        message *= max(1, int(self.spec.get("repeat_message", 1)))
        template = self._api_json("POST", "/api/sms-content/templates", {
            "campaign_id": self.content_campaign_id,
            "language": "English",
            "content": message,
            "source_addr": "E2E",
        })
        self.template_id = int(template["id"])

        self._build_input_rows()
        write_whitelist(self.whitelist_path, self.whitelist_msisdns)
        self._upload_multipart(
            f"/api/realtime-campaigns/{self.realtime_campaign_id}/upload",
            "file", self.whitelist_path.name,
            self.whitelist_path.read_bytes(),
        )
        self._wait_until(
            lambda: (self._api_json("GET", f"/api/realtime-campaigns/{self.realtime_campaign_id}/detail")
                     .get("whitelist_index", {}).get("status") == "ready"),
            self.timeout_seconds, "whitelist index ready",
        )
        self._api_json("PUT", f"/api/realtime-campaigns/{self.realtime_campaign_id}", {
            "sms_template_id": self.template_id,
            "smpp_config_id": self.smpp_config_id,
            "status": "in_progress",
        })
        self._wait_until(
            lambda: self._api_json("GET", f"/api/realtime-campaigns/{self.realtime_campaign_id}").get("status") == "in_progress",
            self.timeout_seconds, "realtime campaign active",
        )
        if not self.task_worker_disabled:
            self._wait_for_smpp()
        self._assert_smpp_effective_config(
            declared_smpp, require_bind=not self.task_worker_disabled,
        )

    def _build_input_rows(self) -> None:
        if self.scenario == "quick":
            valid = make_msisdns(3)
            self.whitelist_msisdns = valid
            self.rows = [
                {"msisdn": valid[0], "language": "English", "event_id": "quick-001"},
                {"msisdn": valid[1], "language": "English", "event_id": "quick-002"},
                {"msisdn": valid[2], "language": "English", "event_id": "quick-003"},
                {"msisdn": valid[2], "language": "English", "event_id": "quick-duplicate"},
                {"msisdn": "2511999999", "language": "English", "event_id": "quick-excluded"},
            ]
            return
        self.whitelist_msisdns = make_msisdns(self.event_count)
        self.rows = [
            {"msisdn": msisdn, "language": "English", "event_id": f"{self.scenario}-{index:07d}"}
            for index, msisdn in enumerate(self.whitelist_msisdns, 1)
        ]

    def _publish_inputs(self) -> None:
        partitions = partition_rows(self.rows, self.file_count)
        for index, rows in enumerate(partitions, 1):
            self.file_manifests.append(write_event_file(
                self.watch_dir, "E2E_", index, rows,
                ready=True,
                chunk_size=self.args.producer_chunk_size,
                chunk_delay=self.args.producer_chunk_delay,
            ))

    def _stop_api_process(self) -> None:
        process = self.processes.get("api")
        if not process or process.poll() is not None:
            return
        # The sender-only gate opens DuckDB itself immediately after stopping
        # the API. A hard Windows TerminateProcess can leave the native owner
        # in the middle of a WAL checkpoint and surface as a misleading
        # UnicodeDecodeError/lock failure in the next connector. The API is
        # spawned as a process group, so request the same graceful signal used
        # for the simulator first; only the bounded timeout path is forced.
        graceful_sent = False
        if platform.system().lower() == "windows" and hasattr(signal, "CTRL_BREAK_EVENT"):
            try:
                process.send_signal(signal.CTRL_BREAK_EVENT)
                graceful_sent = True
            except Exception:
                graceful_sent = False
        if not graceful_sent:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def _seed_sender_only_tasks(self) -> None:
        """Insert a bounded pre-generated task set while the API is stopped."""
        duckdb = _load_duckdb()
        if not self.db_path.exists():
            raise RuntimeError("sender-only seed database does not exist")
        message = str(
            self.spec.get("message")
            or "Marketing Copilot sender-only event={event_id} msisdn={msisdn}"
        )
        message *= max(1, int(self.spec.get("repeat_message", 1)))
        conn = duckdb.connect(str(self.db_path))
        try:
            campaign_cursor = conn.execute(
                """SELECT name,dispatch_generation,flash_sms,smpp_config_id,sms_template_id
                   FROM realtime_campaigns WHERE id=?""",
                (self.realtime_campaign_id,),
            )
            campaign_row = campaign_cursor.fetchone()
            campaign = (
                dict(zip([item[0] for item in campaign_cursor.description], campaign_row))
                if campaign_row else None
            )
            if campaign is None:
                raise RuntimeError("sender-only campaign was not found")
            # The application expires prior-day tasks using DuckDB's
            # CURRENT_DATE. Seed with that same database-owner clock instead
            # of the runner's UTC date; otherwise a near-midnight timezone
            # boundary turns a valid sender-only workload into a safe but
            # misleading "previous day task cancelled" result after restart.
            task_date = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
            task_date = getattr(task_date, "isoformat", lambda: str(task_date))()
            campaign_name = str(campaign["name"] or "")
            dispatch_generation = int(campaign["dispatch_generation"] or 0)
            task_rows = []
            for row in self.rows:
                content = message
                for key, value in row.items():
                    content = content.replace("{" + str(key) + "}", str(value))
                task_rows.append((
                    "realtime", int(self.realtime_campaign_id), None, campaign_name,
                    str(row["msisdn"]), str(row.get("language") or "English"),
                    int(campaign["sms_template_id"] or self.template_id or 0),
                    int(campaign["smpp_config_id"] or self.smpp_config_id or 0),
                    bool(campaign["flash_sms"]), content, "E2E",
                    json.dumps(row, ensure_ascii=False), "sender-only-preloaded",
                    task_date,
                    f"sender-only:{self.realtime_campaign_id}:{row['msisdn']}",
                    dispatch_generation,
                ))
            conn.execute("BEGIN TRANSACTION")
            conn.executemany(
                """INSERT INTO sms_tasks
                   (campaign_type,campaign_id,batch_id,campaign_name,msisdn,language,
                    sms_template_id,smpp_config_id,flash_sms,content,source_addr,payload,
                    source_file,task_date,dedupe_key,dispatch_generation)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                task_rows,
            )
            task_ids = [int(row[0]) for row in conn.execute(
                """SELECT id FROM sms_tasks
                   WHERE campaign_type='realtime' AND campaign_id=?
                     AND source_file='sender-only-preloaded'
                   ORDER BY id""",
                (self.realtime_campaign_id,),
            ).fetchall()]
            if task_ids:
                conn.execute(
                    """INSERT INTO task_event_batches
                       (campaign_type,campaign_id,batch_id,event_type,from_status,to_status,
                        event_count,first_task_id,last_task_id,detail)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        "realtime", int(self.realtime_campaign_id), None, "created", "",
                        "queued", len(task_ids), min(task_ids), max(task_ids),
                        json.dumps({"gate": "sender-only", "pre_generated": True}),
                    ),
                )
            conn.commit()
            self.sender_seed = {
                "source": "isolated DuckDB seed while API stopped",
                "tasks_inserted": len(task_ids),
                "task_ids": {"first": min(task_ids) if task_ids else None,
                             "last": max(task_ids) if task_ids else None},
                "source_file": "sender-only-preloaded",
            }
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _wait_for_ingestion_only(self) -> None:
        if self.no_live_observer:
            raise RuntimeError("ingest-only gate requires the live observer for its completion check")

        def done():
            snapshot = self._live_snapshot(timeout=2)
            if snapshot:
                self.last_snapshot = snapshot
                self.last_snapshot_at = time.monotonic()
            transport = self._live_transport_totals()
            return bool(
                snapshot.get("files", {}).get("completed", 0) >= len(self.file_manifests)
                and snapshot.get("task_total", 0) >= self.expected_tasks
                and transport["socket_parts"] == 0
            )

        self._wait_until(
            done, self.timeout_seconds,
            "ingestion-only file/task completion", interval=1.0,
        )

    def _expected_terminal_status(self) -> str:
        return str(self.spec.get("expected_terminal_status") or "accepted")

    def _wait_for_business(self) -> None:
        expected_status = self._expected_terminal_status()

        def done():
            if self.no_live_observer:
                events = [item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"]
                return len(events) >= self.expected_submit_events
            transport = self._live_transport_totals()
            now = time.monotonic()
            if self.last_snapshot and now - self.last_snapshot_at < 0.8:
                snapshot = self.last_snapshot
            else:
                snapshot = self._live_snapshot(timeout=2)
                if snapshot:
                    self.last_snapshot = snapshot
                    self.last_snapshot_at = now
            self.last_snapshot = snapshot
            files_complete = snapshot.get("files", {}).get("completed", 0) >= len(self.file_manifests)
            tasks_ready = snapshot.get("task_total", 0) >= self.expected_tasks
            terminal = snapshot.get("tasks", {}).get(expected_status, 0) >= self.expected_tasks
            if self.expected_status_counts:
                terminal = all(
                    snapshot.get("tasks", {}).get(status, 0) >= count
                    for status, count in self.expected_status_counts.items()
                )
                # Unknown is intentionally durable but may be omitted from a
                # live aggregate endpoint until the result journal flushes.
                # A zero backlog plus the complete task total is sufficient to
                # proceed to the authoritative stopped-service reconciliation.
                terminal = terminal or (
                    snapshot.get("task_total", 0) >= self.expected_tasks
                    and snapshot.get("backlog", 0) == 0
                )
            if expected_status == "partial":
                sms_complete = transport["socket_parts"] >= 1
            else:
                sms_complete = transport["socket_parts"] >= self.expected_submit_events
            if files_complete and tasks_ready and terminal and sms_complete:
                return True
            return False

        self._wait_until(
            done, self.timeout_seconds,
            f"business completion status={expected_status}",
            interval=1.0,
        )

    def _restart_api(self) -> None:
        self._stop_api_process()
        self._spawn(
            "api",
            [str(self.python_executable), "-m", "uvicorn", "main:app",
             "--host", "127.0.0.1", "--port", str(self.api_port), "--log-level", "warning"],
            cwd=TOOLS_DIR.parent,
        )
        self._wait_http("/api/ready", self.timeout_seconds, "API ready after restart")

    def _hold_stability(self) -> None:
        if self.duration_seconds <= 0:
            return
        deadline = time.monotonic() + self.duration_seconds
        while time.monotonic() < deadline:
            self._sample_runtime()
            time.sleep(min(1.0, max(0.05, deadline - time.monotonic())))

    def _read_log(self, path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def _simulator_observation(self, submit_events: list[dict]) -> dict:
        metrics = {}
        try:
            if self.smsc_metrics.is_file():
                metrics = json.loads(self.smsc_metrics.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            metrics = {}
        timestamps = [_parse_time(item.get("time")) for item in submit_events]
        timestamps = [item for item in timestamps if item]
        elapsed = (
            max(timestamps) - min(timestamps)
        ).total_seconds() if len(timestamps) > 1 else 0.0
        stats = dict(metrics.get("stats") or {})
        received = int(stats.get("received") or len(submit_events))
        response_sent = int(stats.get("response_sent") or 0)
        return {
            "mode": self.simulator_mode,
            "simulator_tps": self.simulator_tps,
            "response_delay": self.simulator_response_delay,
            "dlr_enabled": bool(metrics.get("dlr_enabled", self.simulator_mode == "fault")),
            "tps_scope": "simulator",
            "received": received,
            "response_sent": response_sent,
            "submit_sm_per_second": round(received / max(0.001, elapsed), 3) if received else 0.0,
            "response_per_second": round(response_sent / max(0.001, elapsed), 3) if response_sent else 0.0,
            "metrics": metrics,
            "audit_status": metrics.get("audit_status", "NOT_RECORDED"),
            "metrics_path": str(self.smsc_metrics),
            "event_log_path": str(self.smsc_event_log),
        }

    def _reconcile(self) -> dict:
        expected_status = self._expected_terminal_status()
        submit_events = [item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"]
        dlr_events = [item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "dlr"]
        simulator_observation = self._simulator_observation(submit_events)
        tasks = self._db_query(
            """SELECT t.id,t.msisdn,t.status,t.attempts,t.source_file,t.created_at,t.submitted_at,t.delivered_at,
                      t.smsc_message_ids,COUNT(m.smsc_message_id) AS mapping_count
                 FROM sms_tasks t LEFT JOIN sms_message_bindings_v2 m ON m.task_id=t.id
                WHERE t.campaign_type='realtime' AND t.campaign_id=?
                GROUP BY ALL ORDER BY t.id""",
            (self.realtime_campaign_id or -1,),
        )
        mappings = self._db_query(
            """SELECT m.smsc_message_id,m.task_id,t.msisdn
                 FROM sms_message_bindings_v2 m JOIN sms_tasks t ON t.id=m.task_id
                WHERE t.campaign_type='realtime' AND t.campaign_id=?""",
            (self.realtime_campaign_id or -1,),
        )
        mapping_by_mid = {str(item["smsc_message_id"]): item for item in mappings}
        # ``0`` is the SMPP ESME_ROK success code; do not use ``or 1`` here,
        # because that would turn the legitimate zero into a rejection.
        accepted_events = [item for item in submit_events if int(item.get("response_status", 1)) == 0]
        accepted_ids = [str(item.get("message_id")) for item in accepted_events if item.get("message_id")]
        unbound_accepts = [mid for mid in accepted_ids if mid not in mapping_by_mid]
        has_unknown_expected = expected_status == "unknown" or self.expected_status_counts.get("unknown", 0) > 0
        unknown_submit_ids = unbound_accepts if has_unknown_expected else []
        task_by_msisdn = {str(item.get("msisdn")): item for item in tasks}
        # Fast audit intentionally omits raw destination addresses.  When
        # delivery receipts are disabled there may also be no durable
        # message-id binding, so reconcile the redacted destination digest
        # against authoritative task MSISDNs instead of treating a redacted
        # address as an empty string.  A multipart task legitimately emits
        # several submit_sm records with the same destination, so this is a
        # membership check rather than a one-record-per-task decrement.
        expected_digests = {
            hashlib.sha256(str(item.get("msisdn", "")).encode("utf-8")).hexdigest()[:16]
            for item in tasks
        }
        misdelivered = []
        for item in accepted_events:
            message_id = str(item.get("message_id") or "")
            mapping = mapping_by_mid.get(message_id)
            raw_destination = item.get("destination_addr")
            expected_msisdn = (
                mapping.get("msisdn") if mapping else
                task_by_msisdn.get(str(raw_destination), {}).get("msisdn")
            )
            digest = str(item.get("destination_digest") or "")
            valid = True
            if digest:
                if expected_msisdn is not None:
                    valid = digest == hashlib.sha256(
                        str(expected_msisdn).encode("utf-8")
                    ).hexdigest()[:16]
                elif digest in expected_digests:
                    # No raw address/message binding is present, but every
                    # accepted PDU is still proven to target an authoritative
                    # task recipient; multipart parts may repeat it.
                    valid = True
                else:
                    valid = False
            elif mapping:
                valid = str(raw_destination) == str(mapping.get("msisdn"))
            else:
                valid = str(raw_destination) in task_by_msisdn
            if not valid:
                misdelivered.append({
                    "message_id": item.get("message_id"),
                    "destination_addr": raw_destination,
                    "destination_digest": digest or None,
                    "task_msisdn": expected_msisdn,
                })
        attempts = self._db_query(
            """SELECT task_id,COUNT(*) AS count FROM sms_send_attempts
                WHERE task_id IN (SELECT id FROM sms_tasks WHERE campaign_type='realtime' AND campaign_id=?)
                GROUP BY task_id HAVING COUNT(*)>1""",
            (self.realtime_campaign_id or -1,),
        )
        statuses = Counter(str(item["status"]) for item in tasks)
        file_rows = self._db_query(
            "SELECT * FROM ingestion_files WHERE campaign_type='realtime' AND campaign_id=? ORDER BY id",
            (self.realtime_campaign_id or -1,),
        )
        file_breakdown = [
            {
                key: row.get(key)
                for key in (
                    "id", "filename", "status", "phase", "total_rows",
                    "processed_rows", "created_tasks", "duplicate_rows",
                    "excluded_rows", "invalid_rows", "no_template_rows",
                    "last_progress_at", "last_error", "owner_id", "lease_token",
                )
                if key in row
            }
            for row in file_rows
        ]
        if self.gate == "ingest-only":
            file_statuses = Counter(str(row.get("status") or "") for row in file_rows)
            created = sum(int(row.get("created_tasks") or 0) for row in file_rows)
            excluded = sum(int(row.get("excluded_rows") or 0) for row in file_rows)
            invalid = sum(int(row.get("invalid_rows") or 0) for row in file_rows)
            no_template = sum(int(row.get("no_template_rows") or 0) for row in file_rows)
            db_status = self._db_query(
                "SELECT key,value FROM app_settings WHERE key IN ('system_mode','detail_logging')"
            )
            smpp_rows = self._db_query(
                "SELECT id,host,port FROM smpp_configs WHERE id=?", (self.smpp_config_id or -1,)
            )
            system_mode = {str(item["key"]): str(item["value"]) for item in db_status}.get("system_mode")
            loopback = bool(smpp_rows and smpp_rows[0].get("host") in {"127.0.0.1", "localhost", "::1"})
            queued = statuses.get("queued", 0)
            ingest_ok = (
                len(file_rows) == len(self.file_manifests)
                and file_statuses.get("completed", 0) == len(self.file_manifests)
                and len(tasks) == self.expected_tasks
                and created == self.expected_tasks
                and queued == self.expected_tasks
                and not submit_events
                and not attempts
            )
            return {
                "status": "PASS" if ingest_ok and simulator_observation["audit_status"] == "VERIFIED" else "FAIL",
                "gate": self.gate,
                "expected_terminal_status": "queued",
                "delivery_receipt_mode": self.delivery_receipt_mode,
                "input_files": len(self.file_manifests),
                "input_rows": len(self.rows),
                "valid_events_expected": self.expected_tasks,
                "generated_tasks": len(tasks),
                "task_statuses": dict(statuses),
                "smsc_submit_sm": len(submit_events),
                "smsc_submit_sm_success": len(accepted_events),
                "smsc_submit_sm_rejected": len(submit_events) - len(accepted_events),
                "simulator": simulator_observation,
                "duplicate_send_tasks": [int(item["task_id"]) for item in attempts],
                "lost_submit_mappings": [],
                "misdelivered": [],
                "unknown_retry_violation": False,
                "dlr_violation": bool(dlr_events),
                "stage_counts": {
                    "input_rows": len(self.rows),
                    "filtered_excluded": excluded,
                    "filtered_invalid": invalid,
                    "filtered_no_template": no_template,
                    "tasks_created": created,
                    "tasks_queued": queued,
                    "socket_submit_sm": len(submit_events),
                    "submit_sm_resp_observed": len(accepted_events),
                    "tasks_accepted": statuses.get("accepted", 0),
                },
                "files": file_breakdown,
                "file_statuses": dict(file_statuses),
                "pre_generated_tasks": {},
                "observability": self._observability_report(),
                "throughput": {
                    "events_per_second": None,
                    "submit_sm_per_second": None,
                    "simulator_submit_sm_per_second": simulator_observation["submit_sm_per_second"],
                    "simulator_response_per_second": simulator_observation["response_per_second"],
                    "queue_backlog_max": max(self.backlog_samples or [0]),
                    "queue_backlog_final": queued,
                },
                "environment": {
                    "system_mode": system_mode,
                    "smpp_host": smpp_rows[0].get("host") if smpp_rows else None,
                    "smpp_port": smpp_rows[0].get("port") if smpp_rows else None,
                    "smpp_loopback_only": loopback,
                    "pool_size": self.pool_size,
                    "window_size": max(1, self.args.smpp_window),
                    "route_tps": self.route_tps,
                    "simulator_tps": self.simulator_tps,
                    "tps_scope": self.tps_scope,
                    "simulator_mode": self.simulator_mode,
                    "simulator_audit": self.simulator_audit,
                    "delivery_receipt_mode": self.delivery_receipt_mode,
                    "send_mode": self.send_mode,
                    "dispatch_mode": self.dispatch_mode,
                    "dispatch_mode_effective": self.dispatch_mode,
                    "db_concurrency": self.db_concurrency,
                    "live_env_status": "LIVE_ENV_UNVERIFIED",
                },
                "diagnostics": {
                    "last_snapshot": self.last_snapshot,
                    "watchdog": self._watchdog_report(),
                    "artifacts_dir": str(self.root),
                    "api_log": str(self.api_log),
                    "app_log": str(self.app_log),
                    "smsc_log": str(self.smsc_log),
                    "smsc_event_log": str(self.smsc_event_log),
                    "smsc_metrics": str(self.smsc_metrics),
                    "run_manifest": str(self.root / "run-manifest.json"),
                },
            }
        event_rows = self._db_query(
            "SELECT task_id,event_type,created_at FROM task_events WHERE task_id IN (SELECT id FROM sms_tasks WHERE campaign_type='realtime' AND campaign_id=?) ORDER BY id",
            (self.realtime_campaign_id or -1,),
        )
        task_events = defaultdict(list)
        for row in event_rows:
            task_events[int(row["task_id"])].append(row)

        file_to_db = [_duration_ms(item.get("created_at"), item.get("processing_started_at") or item.get("updated_at")) for item in file_rows]
        file_to_db = [value for value in file_to_db if value is not None]
        ingest_to_task = []
        task_to_resp = []
        file_created = {str(item.get("filename")): item.get("created_at") for item in file_rows}
        submit_by_msisdn = {
            str(item.get("destination_addr")): item
            for item in submit_events
            if item.get("destination_addr")
        }
        for task in tasks:
            if task.get("source_file") in file_created:
                value = _duration_ms(file_created[task["source_file"]], task.get("created_at"))
                if value is not None:
                    ingest_to_task.append(value)
            submitted_event = next((item for item in task_events[int(task["id"])] if item.get("event_type") == "submitted"), None)
            if submitted_event:
                value = _duration_ms(task.get("created_at"), submitted_event.get("created_at"))
                if value is not None:
                    task_to_resp.append(value)
            else:
                # The simulator records the submit_sm/submit_sm_resp result
                # at the protocol boundary.  Older task-event ledgers do not
                # emit a separate ``submitted`` event, so correlate by the
                # destination MSISDN for the acceptance latency metric.
                submit_event = submit_by_msisdn.get(str(task.get("msisdn")))
                if submit_event and not submit_event.get("response_dropped"):
                    value = _duration_ms(task.get("created_at"), submit_event.get("time"))
                    if value is not None:
                        task_to_resp.append(value)

        db_status = self._db_query(
            "SELECT key,value FROM app_settings WHERE key IN ('system_mode','detail_logging')"
        )
        smpp_rows = self._db_query(
            "SELECT id,host,port FROM smpp_configs WHERE id=?", (self.smpp_config_id or -1,)
        )
        system_mode = {str(item["key"]): str(item["value"]) for item in db_status}.get("system_mode")
        loopback = bool(smpp_rows and smpp_rows[0].get("host") in {"127.0.0.1", "localhost", "::1"})
        duplicate_tasks = [int(item["task_id"]) for item in attempts]
        unknown_retry_violation = (
            has_unknown_expected
            and (any(int(item.get("attempts") or 0) != 1 for item in tasks) or len(submit_events) != self.expected_submit_events)
        )
        dlr_violation = bool(dlr_events) and self.delivery_receipt_mode == "disabled"
        simulator_audit_ok = simulator_observation["audit_status"] == "VERIFIED"
        expected_ok = (
            len(tasks) == self.expected_tasks
            and (
                all(statuses.get(status, 0) == count
                    for status, count in self.expected_status_counts.items())
                if self.expected_status_counts
                else statuses.get(expected_status, 0) == self.expected_tasks
            )
            and (
                not unbound_accepts
                or has_unknown_expected
                or self.delivery_receipt_mode == "disabled"
            )
            and not misdelivered
            and not duplicate_tasks
            and not unknown_retry_violation
            and not dlr_violation
            and simulator_audit_ok
        )
        if self.expected_status_counts:
            expected_ok = expected_ok and all(
                statuses.get(status, 0) == count
                for status, count in self.expected_status_counts.items()
            )
        elif expected_status == "delivered":
            expected_ok = expected_ok and statuses.get("delivered", 0) == self.expected_tasks
        elif expected_status == "failed":
            expected_ok = expected_ok and not accepted_events
        elif expected_status == "partial":
            expected_ok = expected_ok and statuses.get("partial", 0) == self.expected_tasks and len(accepted_events) >= 1

        sim_times = [_parse_time(item.get("time")) for item in submit_events]
        sim_times = [item for item in sim_times if item]
        elapsed = (max(sim_times) - min(sim_times)).total_seconds() if len(sim_times) > 1 else max(0.001, time.time() - self.started_at)
        resources = [item for item in self.resource_samples if item.get("rss_bytes") is not None]
        max_rss = max((int(item.get("rss_bytes") or 0) for item in resources), default=0)
        max_cpu = max((float(item.get("cpu_seconds") or 0.0) for item in resources), default=0.0)
        final_backlog = sum(statuses.get(key, 0) for key in ("queued", "sending", "retrying"))
        data_coding_counts = Counter(str(item.get("data_coding")) for item in submit_events)
        esm_class_counts = Counter(str(item.get("esm_class")) for item in submit_events)
        multipart_counts = Counter(
            str((item.get("concat") or {}).get("total", 1)) for item in submit_events
        )
        return {
            "status": "PASS" if expected_ok else "FAIL",
            "gate": self.gate,
            "expected_terminal_status": expected_status,
            "delivery_receipt_mode": self.delivery_receipt_mode,
            "input_files": len(self.file_manifests),
            "input_rows": len(self.rows),
            "valid_events_expected": self.expected_tasks,
            "generated_tasks": len(tasks),
            "task_statuses": dict(statuses),
            "files": file_breakdown,
            "file_statuses": dict(Counter(str(row.get("status") or "") for row in file_rows)),
            "smsc_submit_sm": len(submit_events),
            "smsc_submit_sm_success": len(accepted_events),
            "smsc_submit_sm_rejected": len(submit_events) - len(accepted_events),
            "simulator": simulator_observation,
            "data_coding_counts": dict(data_coding_counts),
            "esm_class_counts": dict(esm_class_counts),
            "multipart_total_counts": dict(multipart_counts),
            "smsc_dlr": len(dlr_events),
            "dlr_violation": dlr_violation,
            "duplicate_send_tasks": duplicate_tasks,
            "lost_submit_mappings": (
                [] if has_unknown_expected or self.delivery_receipt_mode == "disabled"
                else unbound_accepts
            ),
            "unknown_submit_ids": unknown_submit_ids,
            "misdelivered": misdelivered,
            "unknown_retry_violation": unknown_retry_violation,
            "stage_counts": {
                "input_rows": len(self.rows),
                "filtered_excluded": sum(int(row.get("excluded_rows") or 0) for row in file_rows),
                "filtered_invalid": sum(int(row.get("invalid_rows") or 0) for row in file_rows),
                "filtered_no_template": sum(int(row.get("no_template_rows") or 0) for row in file_rows),
                "tasks_created": len(tasks),
                "tasks_queued": statuses.get("queued", 0),
                "tasks_sending": statuses.get("sending", 0),
                "tasks_accepted": statuses.get("accepted", 0),
                "tasks_unknown": statuses.get("unknown", 0),
                "tasks_partial": statuses.get("partial", 0),
                "socket_submit_sm": len(submit_events),
                "submit_sm_resp_observed": len(submit_events) - sum(
                    1 for item in submit_events if item.get("response_dropped", False)
                ),
                "submit_sm_resp_success": len(accepted_events),
            },
            "pre_generated_tasks": dict(self.sender_seed),
            "observability": self._observability_report(),
            "latency_ms": {
                "file_discovery_to_ingestion_start": _percentiles(file_to_db),
                "ingestion_to_task_creation": _percentiles(ingest_to_task),
                "task_creation_to_submit_sm_resp": _percentiles(task_to_resp),
            },
            "throughput": {
                "events_per_second": round(self.expected_tasks / max(0.001, elapsed), 3),
                "submit_sm_per_second": round(len(submit_events) / max(0.001, elapsed), 3),
                "simulator_submit_sm_per_second": simulator_observation["submit_sm_per_second"],
                "simulator_response_per_second": simulator_observation["response_per_second"],
                "queue_backlog_max": max(self.backlog_samples or [0]),
                "queue_backlog_final": final_backlog,
            },
            "resource": {
                "max_rss_bytes": max_rss,
                "max_cpu_seconds_sampled": round(max_cpu, 3),
                "sample_count": len(self.resource_samples),
            },
            "recovery": {
                "db_file_exists": self.db_path.exists(),
                "api_log_scheduler_started": "Activity scheduler started" in self._read_log(self.app_log),
                "api_log_ingestion_started": "Realtime ingestion worker started" in self._read_log(self.app_log),
                "smsc_bind_seen": any(item.get("event_type") == "bind" and item.get("success") for item in _read_jsonl(self.smsc_event_log)),
                "simulator_audit": simulator_audit_ok,
            },
            "environment": {
                "system_mode": system_mode,
                "smpp_host": smpp_rows[0].get("host") if smpp_rows else None,
                "smpp_port": smpp_rows[0].get("port") if smpp_rows else None,
                "smpp_loopback_only": loopback,
                "pool_size": self.pool_size,
                "window_size": max(1, self.args.smpp_window),
                "route_tps": self.route_tps,
                "simulator_tps": self.simulator_tps,
                "tps_scope": self.tps_scope,
                "simulator_mode": self.simulator_mode,
                "simulator_audit": self.simulator_audit,
                "delivery_receipt_mode": self.delivery_receipt_mode,
                "send_mode": self.send_mode,
                "dispatch_mode": self.dispatch_mode,
                "dispatch_mode_effective": self.dispatch_mode,
                "db_concurrency": self.db_concurrency,
                "flash_sms": self.flash_sms,
                "live_env_status": "LIVE_ENV_UNVERIFIED",
            },
            "diagnostics": {
                "last_snapshot": self.last_snapshot,
                "watchdog": self._watchdog_report(),
                "artifacts_dir": str(self.root),
                "api_log": str(self.api_log),
                "app_log": str(self.app_log),
                "smsc_log": str(self.smsc_log),
                "smsc_event_log": str(self.smsc_event_log),
                "smsc_metrics": str(self.smsc_metrics),
                "final_metrics": str(self.final_metrics_path) if self.final_metrics_path else None,
                "run_manifest": str(self.root / "run-manifest.json"),
            },
        }

    def run(self) -> dict:
        report = {
            "scenario": self.scenario,
            "gate": self.gate,
            "started_at": _utc_now(),
            "git_commit": self._git_commit(),
            "command": " ".join([str(item) for item in sys.argv]),
            "status": "BLOCKED_ENV",
            "local_e2e_status": "BLOCKED_ENV",
            "live_env_status": "LIVE_ENV_UNVERIFIED",
            "config_audit": self.config_audit,
            "observer": {
                "api_calls": self.api_call_stats,
                "sampling_policy": "bounded_in_process_live_snapshot; final task/event audit after shutdown",
            },
        }
        try:
            _load_duckdb()
            self.start()
            self._start_watchdog()
            self._provision()
            if self.gate == "sender-only":
                # Provisioning intentionally ran with the task worker disabled.
                # Stop the API before the direct seed so the gate cannot race
                # a live DuckDB writer, then restart the real sender path.
                self._stop_api_process()
                self._seed_sender_only_tasks()
                self.task_worker_disabled = False
                self._restart_api()
                self._wait_for_smpp()
                self._assert_smpp_effective_config(
                    self._declared_smpp_config, require_bind=True,
                )
                self._wait_for_business()
            else:
                self._publish_inputs()
                if self.gate == "ingest-only":
                    self._wait_for_ingestion_only()
                else:
                    self._wait_for_business()
            if self.scenario == "restart":
                before = len([item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"])
                self._restart_api()
                time.sleep(2)
                after = len([item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"])
                if before != after:
                    self.failures.append(f"restart emitted duplicate submit_sm: before={before} after={after}")
            if self.scenario == "stability" or self.duration_seconds:
                self._hold_stability()
            # Stop the real writers before opening a second read-only DuckDB
            # connection for the final ledger.  This keeps the audit query
            # independent from the worker's last buffered result flush.
            # Capture one final in-process gateway snapshot while the API is
            # still alive; a short quick run can finish before the periodic
            # watchdog tick and otherwise lose actual socket-fairness data.
            try:
                self._watchdog_snapshot(full=False)
            except Exception as exc:
                self.failures.append(f"final live metrics snapshot failed: {type(exc).__name__}: {exc}")
            self._capture_final_metrics()
            self._stop_watchdog()
            self.shutdown()
            report["reconciliation"] = self._reconcile()
            report["status"] = report["reconciliation"]["status"]
            report["local_e2e_status"] = "LOCAL_E2E_PASS" if report["status"] == "PASS" and not self.failures else "LOCAL_E2E_FAIL"
            if self.failures:
                report["reconciliation"]["runner_failures"] = list(self.failures)
        except HarnessConfigFail as exc:
            report["status"] = "HARNESS_CONFIG_FAIL"
            report["local_e2e_status"] = "HARNESS_CONFIG_FAIL"
            report["failure_details"] = [str(exc)]
            try:
                self._stop_watchdog()
                self.shutdown()
                report["reconciliation"] = self._reconcile()
            except Exception as reconcile_exc:
                report["failure_details"].append(f"reconcile failed: {reconcile_exc}")
        except BlockedEnv as exc:
            report["status"] = "BLOCKED_ENV"
            report["local_e2e_status"] = "BLOCKED_ENV"
            report["failure_details"] = [str(exc)]
        except Exception as exc:
            report["status"] = "FAIL"
            report["local_e2e_status"] = "LOCAL_E2E_FAIL"
            report["failure_details"] = [f"{type(exc).__name__}: {exc}", traceback.format_exc()]
            try:
                # A failed wait may leave the real workers alive.  Stop them
                # before opening the audit connection, otherwise DuckDB's
                # single-writer lock can turn the diagnostic query into a
                # second failure and hide the business evidence.
                self._stop_watchdog()
                self.shutdown()
                report["reconciliation"] = self._reconcile()
            except Exception as reconcile_exc:
                report["failure_details"].append(f"reconcile failed: {reconcile_exc}")
        finally:
            report["config_audit"] = self.config_audit
            report["observer"] = {
                "api_calls": self.api_call_stats,
                "sampling_policy": "bounded_in_process_live_snapshot; final task/event audit after shutdown",
            }
            report["finished_at"] = _utc_now()
            report["ports"] = {"api": self.api_port, "smsc": self.smsc_port}
            self._stop_watchdog()
            report["diagnostics"] = self._watchdog_report()
            report["artifacts_retained"] = report.get("status") != "PASS" or bool(self.args.keep_artifacts)
            self.shutdown()
            if report["artifacts_retained"]:
                report.setdefault("diagnostics", {})["artifacts_dir"] = str(self.root)
            else:
                shutil.rmtree(self.root, ignore_errors=True)
        return report

    def _git_commit(self) -> str:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT),
                capture_output=True, text=True, timeout=5, check=True,
            )
            return completed.stdout.strip()
        except Exception:
            return "unknown"

    def shutdown(self) -> None:
        for role in ("api", "smsc"):
            process = self.processes.get(role)
            if not process or process.poll() is not None:
                continue
            try:
                if role == "smsc" and platform.system().lower() == "windows" and hasattr(signal, "CTRL_BREAK_EVENT"):
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    process.send_signal(signal.SIGTERM)
                process.wait(timeout=10)
            except Exception:
                try:
                    process.kill()
                    process.wait(timeout=5)
                except Exception:
                    pass
        for handle in self.log_handles:
            try:
                handle.close()
            except OSError:
                pass


def _scenario_spec(path: Path, name: str) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if name == "stability" and name not in data:
        return {
            "description": "Happy path followed by a bounded steady-state hold.",
            "files": 10, "events": 1000, "expected_tasks": 1000,
            "timeout_seconds": 600, "duration_seconds": 1800, "faults": {"tps": 100},
        }
    if name not in data:
        raise ValueError(f"unknown scenario {name}; available: {', '.join(sorted(data))}")
    return data[name]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="quick")
    parser.add_argument("--scenario-file", default=str(DEFAULT_SCENARIOS))
    parser.add_argument(
        "--gate", choices=("full", "ingest-only", "sender-only"), default="full",
        help="acceptance phase: full E2E, ingestion-only, or pre-generated sender-only",
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--events", type=int)
    parser.add_argument("--files", type=int)
    parser.add_argument("--expected-tasks", type=int)
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--smsc-tps", type=float)
    parser.add_argument("--smsc-response-delay", type=float)
    parser.add_argument("--simulator-mode", choices=("fast", "fault"), default=None)
    parser.add_argument("--simulator-tps", type=float)
    parser.add_argument("--simulator-response-delay", type=float)
    parser.add_argument("--simulator-audit", choices=("fast", "full", "off"), default=None)
    parser.add_argument("--simulator-max-inflight", type=int, default=256)
    parser.add_argument("--simulator-audit-queue-size", type=int, default=8192)
    parser.add_argument("--simulator-audit-batch-size", type=int, default=128)
    parser.add_argument("--simulator-audit-flush-interval", type=float, default=0.05)
    parser.add_argument("--route-tps", type=float)
    parser.add_argument("--smsc-reject-ratio", type=float)
    parser.add_argument("--smsc-drop-response-ratio", type=float)
    parser.add_argument("--smsc-drop-receipt-ratio", type=float)
    parser.add_argument("--smsc-receipt-delay", type=float)
    parser.add_argument("--smsc-disconnect-after", type=int)
    parser.add_argument("--smsc-disconnect-before-response", type=int)
    parser.add_argument("--delivery-receipt-mode", choices=("disabled", "optional", "required"))
    parser.add_argument(
        "--send-mode", choices=("reliable", "fast"), default=None,
        help="send pipeline for the isolated run; default is reliable",
    )
    parser.add_argument(
        "--dispatch-mode", choices=("legacy", "fair"), default=None,
        help="campaign dispatch mode for the isolated API; explicit fair is required for S2",
    )
    parser.add_argument(
        "--db-concurrency", choices=("serialized", "occ"), default=None,
        help="DuckDB write concurrency for the isolated API",
    )
    parser.add_argument("--flash-sms", action="store_true", help="create the loopback campaign with Flash SMS enabled")
    parser.add_argument("--task-workers", type=int, default=2)
    parser.add_argument("--ingest-workers", type=int, default=2)
    parser.add_argument("--workers", type=int, default=2, help="local watch-copy workers")
    parser.add_argument("--claim-batch", type=int, default=20)
    parser.add_argument("--smpp-window", type=int, default=10)
    parser.add_argument(
        "--pool-size", type=int, default=1,
        help="number of loopback SMPP binds in the isolated acceptance run",
    )
    parser.add_argument("--smpp-send-queue-capacity", type=int, default=100)
    parser.add_argument("--producer-chunk-size", type=int, default=0)
    parser.add_argument("--producer-chunk-delay", type=float, default=0.0)
    parser.add_argument("--max-memory-mb", type=int, default=0)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument(
        "--no-live-observer", action="store_true",
        help="diagnostic isolation mode: do not poll API stats/metrics during the send wait",
    )
    parser.add_argument(
        "--run-root", default="",
        help="directory for isolated run artifacts; WSL can use an ext4 path such as /tmp",
    )
    parser.add_argument("--output", default=str(REPO_ROOT / "Docs" / "local-e2e-acceptance-results.json"))
    args = parser.parse_args()
    try:
        if args.smsc_tps is not None and (
            args.simulator_tps is not None or args.route_tps is not None
        ):
            parser.error(
                "--smsc-tps is legacy and cannot be combined with "
                "--simulator-tps/--route-tps; pass both new scopes explicitly"
            )
        if args.smsc_response_delay is not None and args.simulator_response_delay is not None:
            parser.error(
                "--smsc-response-delay is legacy and cannot be combined with "
                "--simulator-response-delay"
            )
        spec = _scenario_spec(Path(args.scenario_file), args.scenario)
        report = AcceptanceRunner(args, spec).run()
    except Exception as exc:
        report = {
            "scenario": args.scenario,
            "gate": getattr(args, "gate", "full"),
            "status": "BLOCKED_ENV" if isinstance(exc, (BlockedEnv, FileNotFoundError)) else "FAIL",
            "local_e2e_status": "BLOCKED_ENV" if isinstance(exc, (BlockedEnv, FileNotFoundError)) else "LOCAL_E2E_FAIL",
            "live_env_status": "LIVE_ENV_UNVERIFIED",
            "failure_details": [f"{type(exc).__name__}: {exc}", traceback.format_exc()],
            "command": " ".join([str(item) for item in sys.argv]),
        }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
