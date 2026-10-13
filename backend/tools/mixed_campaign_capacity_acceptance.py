"""Bounded WSL acceptance for mixed campaign fairness and capacity.

This is an orchestration tool, not a second business implementation.  It
starts the real API, scheduler, realtime workers, task workers and SMPP
gateway in an isolated ext4 copy, then drives the public HTTP and local-watch
contracts.  DuckDB is opened read-only only after the service is stopped.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import platform
import random
import shutil
import socket
import subprocess
import sys
import time
import traceback
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

TOOLS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = TOOLS_DIR.parent
REPO_ROOT = BACKEND_DIR.parent
DEFAULT_SCENARIOS = TOOLS_DIR / "mixed_campaign_scenarios.json"
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from local_e2e_acceptance import (
    AcceptanceRunner,
    BlockedEnv,
    _duration_ms,
    _find_free_port,
    _parse_time,
    _read_jsonl,
    _safe_process_snapshot,
    _utc_now,
    _percentiles,
)
from local_e2e_data import make_msisdns, write_event_file
TERMINAL_STATUSES = {"accepted", "delivered", "failed", "partial", "unknown", "expired", "cancelled"}


class ResourceBlocked(BlockedEnv):
    """Raised when a run must stop at a declared resource boundary."""


def _iso_from_epoch(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()


def _event_time(event: dict) -> datetime | None:
    return _parse_time(event.get("time"))


def _metric_value(snapshot: dict, name: str, labels: dict | None = None) -> float:
    expected = {str(k): str(v) for k, v in (labels or {}).items()}
    values = []
    for item in snapshot.get("counters", []) + snapshot.get("gauges", []):
        if item.get("name") != name:
            continue
        actual = {str(k): str(v) for k, v in (item.get("labels") or {}).items()}
        if all(actual.get(key) == value for key, value in expected.items()):
            values.append(float(item.get("value") or 0))
    return sum(values)


def _metric_summary(snapshot: dict) -> dict:
    prefixes = (
        "marketing_copilot_database_",
        "marketing_copilot_scheduler_",
        "marketing_copilot_task_claim",
        "marketing_copilot_tasks_",
        "marketing_copilot_campaign_",
        "marketing_copilot_fair_",
        "marketing_copilot_sms_write_",
        "marketing_copilot_batch_quantum",
        "realtime_",
        "marketing_copilot_smpp_",
    )
    result = {"counters": [], "gauges": [], "rates": []}
    for family in result:
        for item in snapshot.get(family, []):
            if str(item.get("name", "")).startswith(prefixes):
                result[family].append(item)
    return result


def _metric_latency_summary(snapshot: dict, base_name: str) -> dict:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "sum": 0.0})
    buckets: dict[float | None, float] = defaultdict(float)
    for item in snapshot.get("counters", []):
        name = str(item.get("name") or "")
        labels = item.get("labels") or {}
        if name == f"{base_name}_bucket":
            bucket_label = labels.get("le")
            try:
                boundary = None if bucket_label == "+Inf" else float(bucket_label)
            except (TypeError, ValueError):
                boundary = None
            buckets[boundary] += float(item.get("value") or 0)
            continue
        if name == f"{base_name}_count":
            key = json.dumps({k: v for k, v in labels.items() if k != "le"}, sort_keys=True)
            grouped[key]["count"] += float(item.get("value") or 0)
        elif name == f"{base_name}_sum":
            key = json.dumps({k: v for k, v in labels.items() if k != "le"}, sort_keys=True)
            grouped[key]["sum"] += float(item.get("value") or 0)
    count = sum(item["count"] for item in grouped.values())
    total = sum(item["sum"] for item in grouped.values())

    def histogram_quantile(quantile: float) -> float | None:
        if not buckets or count <= 0:
            return None
        target = count * quantile
        cumulative = 0.0
        for boundary, bucket_count in sorted(
            buckets.items(), key=lambda item: float("inf") if item[0] is None else item[0]
        ):
            cumulative = max(cumulative, bucket_count)
            if cumulative >= target:
                return None if boundary is None else round(boundary, 6)
        return None

    return {
        "count": int(count),
        "sum_seconds": round(total, 6),
        "mean_seconds": round(total / count, 6) if count else None,
        "p50_seconds": histogram_quantile(0.50),
        "p95_seconds": histogram_quantile(0.95),
        "p99_seconds": histogram_quantile(0.99),
        "quantiles_note": "bucket boundaries are reported when the runtime metric is a histogram; otherwise business timeline quantiles are reported separately",
    }


def build_activity_plan(spec: dict, seed: int) -> list[dict]:
    """Return a reproducible mixed activity creation/activation plan."""
    total = max(2, int(spec.get("activity_count", 4)))
    realtime_count = max(1, min(total - 1, int(spec.get("realtime_count", total // 2))))
    large_count = max(1, min(total, int(spec.get("large_activity_count", 2))))
    order = list(range(total))
    random.Random(int(seed)).shuffle(order)
    return [
        {
            "logical_index": logical_index,
            "campaign_type": "realtime" if logical_index < realtime_count else "batch",
            "large": logical_index < large_count,
            "late": logical_index >= large_count,
            "route_id": logical_index % max(1, int(spec.get("smsc_routes", 1))),
        }
        for logical_index in order
    ]


class MixedCampaignRunner(AcceptanceRunner):
    def __init__(self, args: argparse.Namespace, spec: dict, seed: int):
        super().__init__(args, spec)
        self.spec = dict(spec)
        self.seed = int(seed)
        self.profile = str(args.profile)
        self.smsc_ports: list[int] = []
        self.smsc_event_logs: list[Path] = []
        self.smsc_log_paths: list[Path] = []
        self.smpp_config_ids: list[int] = []
        self.activity_meta: dict[tuple[str, int], dict] = {}
        self.rows_by_activity: dict[tuple[str, int], list[dict]] = {}
        self.activity_history: dict[tuple[str, int], list[dict]] = defaultdict(list)
        self.input_files: list[dict] = []
        self.metrics_samples: list[dict] = []
        self.last_task_rows: list[dict] = []
        self.expected_total = 0
        self.started_monotonic = time.monotonic()
        self.stop_reason = "not_started"
        self.injected = False
        self.lock_regression: dict = {"status": "NOT_RUN"}
        self.creation_order: list[int] = []
        self.sftp_status = self._sftp_environment_status()
        self.pre_stop_snapshot: dict = {}
        self.stop_transition: dict = {}
        self.timing: dict[str, float] = {}
        self._check_ext4_and_resources()

    def _check_ext4_and_resources(self) -> None:
        if platform.system().lower() == "linux" and str(REPO_ROOT).startswith("/mnt/"):
            raise BlockedEnv(f"refusing DrvFs repository path: {REPO_ROOT}")
        free_bytes = shutil.disk_usage(REPO_ROOT).free
        minimum = int(self.spec.get("min_free_gb", 1) * 1024 ** 3)
        if free_bytes < minimum:
            raise ResourceBlocked(
                f"free disk below guard: {free_bytes / 1024 ** 3:.2f} GiB < {minimum / 1024 ** 3:.2f} GiB"
            )

    @staticmethod
    def _sftp_environment_status() -> str:
        if platform.system().lower() != "linux":
            return "BLOCKED_ENV"
        if shutil.which("sshd") is None or importlib.util.find_spec("paramiko") is None:
            return "BLOCKED_ENV"
        return "NOT_RUN_REMOTE"

    def _environment(self) -> dict[str, str]:
        env = super()._environment()
        env.update({
            "MC_REALTIME_DISCOVERY_QUANTUM": str(self.spec.get("discovery_quantum", 1)),
            "MC_REALTIME_COPY_QUEUE_SIZE": str(self.spec.get("copy_queue_size", 2)),
            "MC_REALTIME_COPY_WORKERS": str(self.spec.get("copy_workers", 1)),
            "MC_REALTIME_WATCH_RESCAN_SECONDS": "3",
            "MC_BATCH_INGEST_QUANTUM": "1",
            "MC_BATCH_QUANTUM_SECONDS": "0.25",
            "MC_FAIR_ACTIVE_CAMPAIGNS": str(self.spec.get("fair_active_campaigns", 2)),
            "MC_FAIR_CLAIM_PER_CAMPAIGN": str(self.spec.get("fair_claim_per_campaign", 3)),
            "MC_FAIR_DRR_QUANTUM_PARTS": "4",
            "MC_SMPP_INTERNAL_TPS_MULTIPLIER": "1",
            "MC_DUCKDB_WRITE_LOCK_TIMEOUT_SECONDS": "3",
            "MC_TASK_WORKERS": str(self.spec.get("task_workers", 2)),
            "MC_REALTIME_INGEST_WORKERS": str(self.spec.get("ingest_workers", 2)),
            "MC_CLAIM_BATCH": str(self.spec.get("claim_batch", 8)),
            "MC_TASK_ARCHIVE_ENABLED": "false",
        })
        return env

    def _spawn_role(self, role: str, command: list[str], cwd: Path, log_path: Path) -> None:
        handle = log_path.open("w", encoding="utf-8")
        self.log_handles.append(handle)
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        self.processes[role] = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=self._environment(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )

    def start(self) -> None:
        started = time.monotonic()
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.campaign_dir.mkdir(parents=True, exist_ok=True)
        route_count = max(1, int(self.spec.get("smsc_routes", 1)))
        tps = float(self.spec.get("smsc_tps", 1000) or 0)
        response_delay = float(self.spec.get("smsc_response_delay", 0) or 0)
        for route in range(route_count):
            port = _find_free_port()
            event_log = self.root / f"smsc-route-{route + 1}.events.jsonl"
            log_path = self.root / f"smsc-route-{route + 1}.log"
            command = [
                str(self.python_executable), str(TOOLS_DIR / "smsc_simulator.py"),
                "--host", "127.0.0.1", "--port", str(port),
                "--system-id", "test", "--password", "test",
                "--receipt-delay", "0.1", "--submit-response-delay", str(response_delay),
                "--tps", str(tps), "--reject-ratio", "0", "--drop-response-ratio", "0",
                "--drop-receipt-ratio", "0", "--disconnect-after", "0",
                "--partial-submit-after", "0", "--seed", str(self.seed + route),
                "--event-log", str(event_log),
            ]
            self._spawn_role(f"smsc-{route + 1}", command, REPO_ROOT, log_path)
            self.smsc_ports.append(port)
            self.smsc_event_logs.append(event_log)
            self.smsc_log_paths.append(log_path)
        for route, port in enumerate(self.smsc_ports, 1):
            self._wait_for_port(port, self.timeout_seconds, f"SMSC route {route}")
        self.api_port = _find_free_port()
        self._spawn_role(
            "api",
            [str(self.python_executable), "-m", "uvicorn", "main:app",
             "--host", "127.0.0.1", "--port", str(self.api_port), "--log-level", "warning"],
            BACKEND_DIR,
            self.api_log,
        )
        self._wait_http("/api/ready", self.timeout_seconds, "Marketing Copilot API ready")
        self.timing["process_startup_seconds"] = round(time.monotonic() - started, 3)

    def _wait_for_port(self, port: int, timeout: float, label: str) -> None:
        """Wait for a route-specific simulator and surface early exits."""
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            for role, process in self.processes.items():
                if role.startswith("smsc") and process.poll() is not None:
                    raise RuntimeError(f"{label} exited with code {process.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    return
            except OSError as exc:
                last_error = str(exc)
                time.sleep(0.1)
        raise TimeoutError(f"Timed out waiting for {label}: {last_error}")

    def shutdown(self) -> None:
        started = time.monotonic()
        first_shutdown = not self.stop_transition
        if first_shutdown:
            self.stop_transition = {"requested_at": _utc_now()}
        for role, process in list(self.processes.items()):
            if process.poll() is not None:
                continue
            try:
                process.terminate()
                process.wait(timeout=12)
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
        if "shutdown_seconds" not in self.timing:
            self.timing["shutdown_seconds"] = round(time.monotonic() - started, 3)
        if first_shutdown:
            self.stop_transition.update({
                "completed_at": _utc_now(),
                "elapsed_seconds": self.timing["shutdown_seconds"],
            })

    def run(self) -> dict:
        """Use the shared lifecycle, while preserving resource classification."""
        report = super().run()
        details = " ".join(str(item) for item in report.get("failure_details", []))
        resource_markers = ("free disk", "RSS exceeded", "filesystem crossed the free-space guard")
        if report.get("status") == "BLOCKED_ENV" and any(marker in details for marker in resource_markers):
            report["status"] = "BLOCKED_RESOURCE"
            report["local_e2e_status"] = "BLOCKED_RESOURCE"
            report["failure_details"] = ["resource boundary: " + details]
        return report

    def _csv_bytes(self, rows: list[dict]) -> bytes:
        from io import StringIO

        stream = StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=["msisdn", "language", "event_id"])
        writer.writeheader()
        writer.writerows(rows)
        return stream.getvalue().encode("utf-8")

    def _make_content(self, marketing_type: str, campaign_id: int, label: str, route_id: int) -> int:
        campaign = self._api_json("POST", "/api/sms-content/campaigns", {
            "name": f"mixed-content-{label}-{self.seed}",
            "marketing_type": marketing_type,
            "marketing_campaign_id": campaign_id,
            "language": "English",
        })
        template = self._api_json("POST", "/api/sms-content/templates", {
            "campaign_id": int(campaign["id"]),
            "language": "English",
            "content": f"Mixed {label} {{event_id}} {{msisdn}} route={route_id}",
            "source_addr": f"MIX{route_id}",
        })
        return int(template["id"])

    def _rows(self, label: str, count: int, start: int) -> list[dict]:
        return [
            {"msisdn": msisdn, "language": "English", "event_id": f"mixed-{label}-{index:06d}"}
            for index, msisdn in enumerate(make_msisdns(count, start=start), 1)
        ]

    def _provision_activity(self, index: int, campaign_type: str, route_id: int, count: int) -> None:
        label = f"{campaign_type[:2]}-{index:02d}"
        campaign_dir = self.root / f"campaign-{label}"
        campaign_dir.mkdir(parents=True, exist_ok=True)
        route_config_id = self.smpp_config_ids[route_id]
        if campaign_type == "realtime":
            watch_dir = self.root / f"watch-{label}"
            watch_dir.mkdir(parents=True, exist_ok=True)
            response = self._api_json("POST", "/api/realtime-campaigns", {
                "name": f"mixed-realtime-{label}-{self.seed}",
                "use_whitelist": False,
                "campaign_dir": str(campaign_dir),
                "use_custom_directory": True,
                "watch_source": "local",
                "watch_dir": str(watch_dir),
                "watch_filename_prefix": f"MIX_{label}_",
                "phone_prefix": "251",
                "smpp_config_id": route_config_id,
            })
            campaign_id = int(response["id"])
            template_id = self._make_content(campaign_type, campaign_id, label, route_id + 1)
        else:
            response = self._api_json("POST", "/api/batch-campaigns", {
                "name": f"mixed-batch-{label}-{self.seed}",
                "batch_size": 10 if count < 100 else 20,
                "campaign_dir": str(campaign_dir),
                "use_custom_directory": True,
                "phone_prefix": "251",
                "smpp_config_id": route_config_id,
            })
            campaign_id = int(response["id"])
            source_name = f"mixed-{label}.csv"
            rows = self._rows(label, count, 100000 + index * 10000)
            upload_started = time.time()
            self._upload_multipart(
                f"/api/batch-campaigns/{campaign_id}/upload",
                "file", source_name, self._csv_bytes(rows),
            )
            template_id = self._make_content(campaign_type, campaign_id, label, route_id + 1)
            self.rows_by_activity[(campaign_type, campaign_id)] = rows
            self.activity_meta[(campaign_type, campaign_id)] = {
                "label": label,
                "campaign_type": campaign_type,
                "campaign_id": campaign_id,
                "route_id": route_id,
                "route_config_id": route_config_id,
                "template_id": template_id,
                "expected_rows": count,
                "source_name": source_name,
                "campaign_dir": str(campaign_dir),
                "source_uploaded_at": _iso_from_epoch(upload_started),
                "activated_at": None,
                "large": index < int(self.spec.get("large_activity_count", 2)),
                "late": index >= int(self.spec.get("large_activity_count", 2)),
            }
            return
        key = (campaign_type, campaign_id)
        self.rows_by_activity[key] = self._rows(label, count, 100000 + index * 10000)
        self.activity_meta[key] = {
            "label": label,
            "campaign_type": campaign_type,
            "campaign_id": campaign_id,
            "route_id": route_id,
            "route_config_id": route_config_id,
            "template_id": template_id,
            "expected_rows": count,
            "watch_dir": str(self.root / f"watch-{label}"),
            "prefix": f"MIX_{label}_",
            "campaign_dir": str(campaign_dir),
            "activated_at": None,
            "large": index < int(self.spec.get("large_activity_count", 2)),
            "late": index >= int(self.spec.get("large_activity_count", 2)),
        }

    def _run_lock_regression(self) -> dict:
        if not self.spec.get("run_lock_regression", True):
            return {"status": "NOT_RUN"}
        output = self.root / "lock-regression.json"
        root = self.root / "lock-regression"
        completed = subprocess.run(
            [str(self.python_executable), str(TOOLS_DIR / "local_lock_e2e_acceptance.py"),
             "--profile", "smoke", "--python", str(self.python_executable),
             "--root", str(root), "--output", str(output)],
            cwd=str(BACKEND_DIR), env=self._environment(),
            capture_output=True, text=True, timeout=120,
        )
        parsed = {}
        if output.exists():
            try:
                parsed = json.loads(output.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                parsed = {}
        return {
            "status": "PASS" if completed.returncode == 0 and parsed.get("status") == "PASS" else "FAIL",
            "returncode": completed.returncode,
            "result": parsed,
            "stdout_tail": completed.stdout[-2000:],
            "stderr_tail": completed.stderr[-2000:],
        }

    def _provision(self) -> None:
        self.lock_regression = self._run_lock_regression()
        for route in range(max(1, int(self.spec.get("smsc_routes", 1)))):
            smpp = self._api_json("POST", "/api/smpp/configs", {
                "name": f"mixed-loopback-route-{route + 1}",
                "host": "127.0.0.1",
                "port": self.smsc_ports[route],
                "system_id": "test",
                "password": "test",
                "source_addr": f"MIX{route + 1}",
                "bind_type": "transceiver",
                "pool_size": 1,
                "window_size": 10,
                "enquire_interval": 5,
                "timeout": 10,
                "tps": max(1, int(float(self.spec.get("smsc_tps", 1000) or 1000))),
                "priority": route + 1,
                "reconnect_max_seconds": 2,
                "delivery_receipt_mode": "disabled",
                "enabled": 1,
            })
            self.smpp_config_ids.append(int(smpp["id"]))
        plan = build_activity_plan(self.spec, self.seed)
        self.creation_order = [int(item["logical_index"]) for item in plan]
        for item in plan:
            index = int(item["logical_index"])
            campaign_type = str(item["campaign_type"])
            count = int(self.spec.get("large_rows", 40) if item["large"] else self.spec.get("small_rows", 4))
            self._provision_activity(index, campaign_type, int(item["route_id"]), count)
        self.expected_total = sum(int(meta["expected_rows"]) for meta in self.activity_meta.values())
        self.realtime_campaign_id = next(
            (campaign_id for (kind, campaign_id) in self.activity_meta if kind == "realtime"), None
        )
        self._wait_until(
            lambda: bool((self._api_json("GET", "/api/health").get("smpp") or {}).get("connected")),
            self.timeout_seconds, "at least one SMPP bind connected",
        )

    def _activate(self, key: tuple[str, int]) -> None:
        meta = self.activity_meta[key]
        path = f"/api/{meta['campaign_type']}-campaigns/{meta['campaign_id']}"
        payload = {"sms_template_id": meta["template_id"], "smpp_config_id": meta["route_config_id"]}
        if meta["campaign_type"] == "batch":
            self._api_json("PUT", path, payload)
        self._api_json("PUT", path, {**payload, "status": "in_progress"})
        meta["activated_at"] = _utc_now()
        self._wait_until(
            lambda: self._api_json("GET", path).get("status") == "in_progress",
            self.timeout_seconds, f"{meta['label']} active",
        )

    def _publish_realtime_file(self, key: tuple[str, int], file_index: int, rows: list[dict]) -> dict:
        meta = self.activity_meta[key]
        published = write_event_file(
            meta["watch_dir"], meta["prefix"], file_index, rows, ready=True,
        )
        published["activity"] = {"campaign_type": key[0], "campaign_id": key[1]}
        published["published_at_iso"] = _iso_from_epoch(published.get("producer_published_at"))
        self.input_files.append(published)
        return published

    def _publish_inputs(self) -> None:
        shuffled = list(self.activity_meta.items())
        random.Random(self.seed + 1).shuffle(shuffled)
        ordered = sorted(shuffled, key=lambda item: (not item[1]["large"], item[1]["label"]))
        large = [(key, meta) for key, meta in ordered if meta["large"]]
        late = [(key, meta) for key, meta in ordered if not meta["large"]]
        random.Random(self.seed + 2).shuffle(large)
        random.Random(self.seed + 3).shuffle(late)
        for key, meta in large:
            self._activate(key)
            if meta["campaign_type"] == "realtime":
                self._publish_realtime_file(key, 1, self.rows_by_activity[key])
        time.sleep(float(self.spec.get("late_activation_delay_seconds", 0.5)))
        for key, meta in late:
            self._activate(key)
            if meta["campaign_type"] == "realtime":
                self._publish_realtime_file(key, 1, self.rows_by_activity[key])

    def _environment_task_rows(self) -> list[dict]:
        rows: list[dict] = []
        cursor = None
        while True:
            path = "/api/sms-tasks?page_size=200"
            if cursor:
                path += f"&cursor={int(cursor)}"
            payload = self._api_json("GET", path)
            items = payload.get("items") or []
            rows.extend(items)
            if not payload.get("has_more") or not items or not payload.get("next_cursor"):
                return rows
            cursor = payload["next_cursor"]

    def _event_records(self) -> list[dict]:
        events: list[dict] = []
        for path in self.smsc_event_logs:
            events.extend(_read_jsonl(path))
        return events

    def _activity_counts(self, rows: list[dict]) -> dict[tuple[str, int], dict[str, int]]:
        result = defaultdict(Counter)
        for row in rows:
            result[(str(row.get("campaign_type")), int(row.get("campaign_id") or 0))][str(row.get("status"))] += 1
        return {key: dict(value) for key, value in result.items()}

    def _sample_runtime(self) -> None:
        for role, process in self.processes.items():
            if process.poll() is None:
                self.resource_samples.append(_safe_process_snapshot(process.pid, role))
        try:
            rows = self._environment_task_rows()
            self.last_task_rows = rows
            events = self._event_records()
            submits = [item for item in events if item.get("event_type") == "submit_sm"]
            counts = self._activity_counts(rows)
            now = time.monotonic()
            for key in self.activity_meta:
                activity_rows = counts.get(key, {})
                self.activity_history[key].append({
                    "monotonic": now,
                    "sampled_at": _utc_now(),
                    "task_total": sum(activity_rows.values()),
                    "accepted": activity_rows.get("accepted", 0),
                    "terminal": sum(activity_rows.get(status, 0) for status in TERMINAL_STATUSES),
                    "submit_sm": sum(1 for event in submits if self._event_key(event) == key),
                })
            self.last_snapshot = {
                "task_total": len(rows),
                "tasks": dict(Counter(str(row.get("status")) for row in rows)),
                "submit_sm": len(submits),
                "backlog": sum(1 for row in rows if row.get("status") in {"queued", "sending", "retrying"}),
            }
            self.backlog_samples.append(int(self.last_snapshot["backlog"]))
            if len(self.metrics_samples) == 0 or now - self.metrics_samples[-1]["monotonic"] >= 1.0:
                status = self._api_json("GET", "/api/metrics/status")
                self.metrics_samples.append({"monotonic": now, "sampled_at": _utc_now(), "status": status})
        except Exception:
            pass
        if self.args.max_memory_mb:
            limit = int(self.args.max_memory_mb) * 1024 * 1024
            recent = self.resource_samples[-max(1, len(self.processes)):]
            if any(int(item.get("rss_bytes", 0) or 0) > limit for item in recent):
                raise ResourceBlocked(f"RSS exceeded {self.args.max_memory_mb} MiB")
        free_bytes = shutil.disk_usage(self.root).free
        if free_bytes < int(self.spec.get("min_free_gb", 1) * 1024 ** 3):
            raise ResourceBlocked("run artifact filesystem crossed the free-space guard")

    def _key_for_msisdn(self, msisdn: str) -> tuple[str, int] | None:
        value = str(msisdn or "")
        for key, rows in self.rows_by_activity.items():
            if any(str(row.get("msisdn")) == value for row in rows):
                return key
        return None

    def _event_key(self, event: dict) -> tuple[str, int] | None:  # type: ignore[override]
        return self._key_for_msisdn(event.get("destination_addr"))

    def _inject_extra_inputs(self) -> None:
        if self.injected or not self.spec.get("extra_realtime_files"):
            return
        self.injected = True
        extra_count = int(self.spec.get("extra_realtime_files", 1))
        extra_rows = int(self.spec.get("extra_rows", 4))
        for key, meta in self.activity_meta.items():
            if meta["campaign_type"] != "realtime":
                continue
            for file_index in range(2, 2 + extra_count):
                start = 500000 + int(meta["campaign_id"]) * 10000 + file_index * 100
                rows = self._rows(f"{meta['label']}-extra-{file_index}", extra_rows, start)
                self.rows_by_activity[key].extend(rows)
                meta["expected_rows"] += len(rows)
                self.expected_total += len(rows)
                self._publish_realtime_file(key, file_index, rows)

    def _wait_for_business(self) -> None:
        business_started = time.monotonic()
        self._business_started_monotonic = business_started
        deadline = time.monotonic() + float(self.spec.get("time_budget_seconds", self.timeout_seconds))
        injected_at = time.monotonic() + float(self.spec.get("inject_after_seconds", 1.0))
        while time.monotonic() < deadline:
            if not self.injected and time.monotonic() >= injected_at:
                self._inject_extra_inputs()
            self._sample_runtime()
            events = self._event_records()
            submits = [item for item in events if item.get("event_type") == "submit_sm"]
            counts = self._activity_counts(self.last_task_rows)
            complete = all(
                counts.get(key, {}).get("accepted", 0) >= int(meta["expected_rows"])
                and sum(1 for event in submits if self._event_key(event) == key) >= int(meta["expected_rows"])
                for key, meta in self.activity_meta.items()
            )
            if complete:
                self.stop_reason = "completed"
                self._capture_pre_stop_snapshot()
                return
            if int(self.spec.get("max_events", 0) or 0) and len(submits) >= int(self.spec["max_events"]):
                self.stop_reason = "max_events"
                return
            time.sleep(0.25)
        self.stop_reason = "time_budget"
        self._capture_pre_stop_snapshot()

    def _capture_pre_stop_snapshot(self) -> None:
        """Capture live counters immediately before process shutdown."""
        try:
            self._sample_runtime()
        except Exception:
            pass
        latest_metrics = self.metrics_samples[-1].get("status", {}).get("metrics", {}) if self.metrics_samples else {}
        self.pre_stop_snapshot = {
            "captured_at": _utc_now(),
            "task_snapshot": dict(self.last_snapshot),
            "metrics": _metric_summary(latest_metrics),
            "resource": dict(self.resource_samples[-1]) if self.resource_samples else {},
        }
        business_started = getattr(self, "_business_started_monotonic", None)
        if business_started is not None:
            self.timing.setdefault("business_wait_seconds", round(time.monotonic() - business_started, 3))

    def _db_activity_rows(self, key: tuple[str, int]) -> list[dict]:
        return self._db_query(
            "SELECT * FROM ingestion_files WHERE campaign_type=? AND campaign_id=? ORDER BY id",
            key,
        )

    def _max_no_progress(self, key: tuple[str, int]) -> float | None:
        samples = self.activity_history.get(key) or []
        if len(samples) < 2:
            return None
        maximum = 0.0
        last_progress = None
        for sample in samples:
            progress = int(sample.get("task_total", 0)) + int(sample.get("submit_sm", 0))
            if progress > (last_progress[1] if last_progress else -1):
                if last_progress and int(sample.get("task_total", 0)) > 0:
                    maximum = max(maximum, float(sample["monotonic"] - last_progress[0]))
                last_progress = (float(sample["monotonic"]), progress)
        if last_progress:
            maximum = max(maximum, max(0.0, time.monotonic() - last_progress[0]))
        return round(maximum, 3)

    def _timeline(self, key: tuple[str, int], tasks: list[dict], events: list[dict]) -> dict:
        meta = self.activity_meta[key]
        files = self._db_activity_rows(key)
        task_events = self._db_query(
            """SELECT event_type,created_at FROM task_events
                WHERE task_id IN (SELECT id FROM sms_tasks WHERE campaign_type=? AND campaign_id=?)
                ORDER BY id""",
            key,
        )
        first_event_by_type: dict[str, str] = {}
        for row in task_events:
            first_event_by_type.setdefault(str(row.get("event_type")), str(row.get("created_at")))
        fairness_state = self._db_query(
            """SELECT campaign_type,campaign_id,route_id,eligible_since,
                      last_file_claim_at,last_task_claim_at,last_submit_at,
                      last_dispatched_at,backlog,consecutive_skip_rounds,last_skip_reason
                 FROM campaign_fairness_state
                WHERE campaign_type=? AND campaign_id=?
                ORDER BY route_id""",
            key,
        )
        manifest_rows = self._db_query(
            "SELECT created_at,published_at FROM batch_campaign_manifests WHERE campaign_id=? ORDER BY id",
            (key[1],),
        ) if key[0] == "batch" else []
        own_events = [event for event in events if self._event_key(event) == key and event.get("event_type") == "submit_sm"]
        first_submit = min((_event_time(event) for event in own_events if _event_time(event)), default=None)
        task_created = min((_parse_time(row.get("created_at")) for row in tasks if _parse_time(row.get("created_at"))), default=None)
        file_created = min((_parse_time(row.get("created_at")) for row in files if _parse_time(row.get("created_at"))), default=None)
        file_started = min((_parse_time(row.get("processing_started_at")) for row in files if _parse_time(row.get("processing_started_at"))), default=None)
        producer = min((_parse_time(item.get("published_at_iso")) for item in self.input_files if item.get("activity") == {"campaign_type": key[0], "campaign_id": key[1]} and _parse_time(item.get("published_at_iso"))), default=None)
        task_to_submit = [_duration_ms(row.get("created_at"), event.get("time")) for row in tasks for event in own_events if str(row.get("msisdn")) == str(event.get("destination_addr"))]
        task_to_submit = [value for value in task_to_submit if value is not None]
        fairness_eligible = min(
            (_parse_time(row.get("eligible_since")) for row in fairness_state if _parse_time(row.get("eligible_since"))),
            default=None,
        )
        manifest_created = min(
            (_parse_time(row.get("created_at")) for row in manifest_rows if _parse_time(row.get("created_at"))),
            default=None,
        )
        first_batch_quantum = manifest_created or task_created
        return {
            "campaign_type": key[0],
            "campaign_id": key[1],
            "label": meta["label"],
            "route_id": meta["route_id"],
            "large": meta["large"],
            "late": meta["late"],
            "expected_rows": meta["expected_rows"],
            "producer_first_published_at": producer.isoformat() if producer else meta.get("source_uploaded_at"),
            "first_file_created_at": file_created.isoformat() if file_created else None,
            "first_file_claim_at": file_started.isoformat() if file_started else None,
            "first_task_created_at": task_created.isoformat() if task_created else None,
            "first_task_claim_at": first_event_by_type.get("claimed"),
            "first_batch_quantum_at": first_batch_quantum.isoformat() if key[0] == "batch" and first_batch_quantum else None,
            "first_submit_sm_at": first_submit.isoformat() if first_submit else None,
            "eligible_since": fairness_eligible.isoformat() if fairness_eligible else meta.get("activated_at"),
            "became_ineligible_at": None,
            "max_consecutive_skipped_rounds": max(
                (int(row.get("consecutive_skip_rounds") or 0) for row in fairness_state),
                default=0,
            ),
            "fairness_state": fairness_state,
            "last_progress_at": max(
                [str(row.get("updated_at")) for row in tasks + files if row.get("updated_at")],
                default=None,
            ),
            "first_service_wait_ms": _duration_ms(meta.get("activated_at"), first_submit.isoformat() if first_submit else None),
            "file_to_claim_ms": _duration_ms(producer.isoformat() if producer else None, file_started.isoformat() if file_started else None),
            "claim_to_task_ms": _duration_ms(file_started.isoformat() if file_started else None, task_created.isoformat() if task_created else None),
            "task_to_submit_sm_resp": _percentiles(task_to_submit),
            "max_no_progress_seconds": self._max_no_progress(key),
            "backlog_curve": self.activity_history.get(key, []),
            "actual_submit_sm": len(own_events),
            "measurement_notes": [
                "became_ineligible_at is null because the current durable fairness state stores eligibility and last-observed state, not a historical ineligibility transition log",
            ],
        }

    def _retry_diagnostics(self, tasks: list[dict]) -> dict:
        """Build a bounded, evidence-based retry/unknown report from the ledger."""
        events = self._db_query(
            """SELECT task_id,event_type,from_status,to_status,message,detail,created_at
                 FROM task_events ORDER BY id"""
        )
        attempts = self._db_query(
            """SELECT task_id,attempt_token,planned_parts,accepted_parts,failed_parts,
                       state,created_at,updated_at
                 FROM sms_send_attempts ORDER BY created_at"""
        )
        parts = self._db_query(
            """SELECT attempt_token,part_no,state,response_status,smsc_message_id,
                       sent_at,responded_at,updated_at
                 FROM sms_send_parts ORDER BY attempt_token,part_no"""
        )
        task_by_id = {int(row["id"]): row for row in tasks}
        events_by_task: dict[int, list[dict]] = defaultdict(list)
        invalid_event_rows = []
        known_event_types = {
            "created", "claimed", "submit_authorized", "send_failure",
            "submitted", "delivery_receipt", "manual_retry", "manual_delivered",
            "recovered_unknown",
        }
        for row in events:
            task_id = int(row.get("task_id") or 0)
            detail = row.get("detail") or "{}"
            try:
                detail_value = json.loads(detail) if isinstance(detail, str) else detail
                if not isinstance(detail_value, dict):
                    raise ValueError("event detail is not an object")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                detail_value = {}
                invalid_event_rows.append({"task_id": task_id, "reason": str(exc)[:160]})
            item = dict(row)
            item["detail_object"] = detail_value
            events_by_task[task_id].append(item)
            if (
                str(row.get("event_type") or "") not in known_event_types
                or len(str(row.get("event_type") or "")) > 64
                or len(str(row.get("from_status") or "")) > 32
                or len(str(row.get("to_status") or "")) > 32
            ):
                invalid_event_rows.append({
                    "task_id": task_id,
                    "event_type": str(row.get("event_type") or "")[:160],
                    "reason": "non-canonical event field",
                })

        attempts_by_task: dict[int, list[dict]] = defaultdict(list)
        for row in attempts:
            attempts_by_task[int(row.get("task_id") or 0)].append(row)
        parts_by_attempt: dict[str, list[dict]] = defaultdict(list)
        for row in parts:
            parts_by_attempt[str(row.get("attempt_token") or "")].append(row)

        def attempts_bucket(value) -> str:
            value = int(value or 0)
            return "0" if value == 0 else "1" if value == 1 else "2" if value == 2 else "3+"

        def reason_for(row: dict, task_events: list[dict]) -> str:
            text = str(row.get("error_msg") or "").lower()
            failure_class = str(row.get("failure_class") or "")
            if "queue full" in text:
                return "smpp_send_queue_full"
            if "window exhausted" in text:
                return "smpp_window_exhausted"
            if "lock timeout" in text or "write lock" in text:
                return "duckdb_write_lock_timeout"
            if failure_class == "delivery_unknown" or int(row.get("last_smpp_status") or 0) == -1:
                return "delivery_unknown"
            if failure_class == "partial_submit":
                return "partial_submit"
            for event in reversed(task_events):
                detail = event.get("detail_object") or {}
                if event.get("event_type") == "send_failure":
                    message = str(event.get("message") or "").lower()
                    if "queue full" in message:
                        return "smpp_send_queue_full"
                    if detail.get("failure_class"):
                        return str(detail["failure_class"])
            return failure_class or ("error" if text else "none")

        breakdown = Counter()
        event_breakdown = Counter()
        unknown_partial_retry_violations = []
        partial_fence_violations = []
        for row in tasks:
            task_id = int(row["id"])
            task_events = events_by_task.get(task_id, [])
            failure_class = str(row.get("failure_class") or "") or "none"
            reason = reason_for(row, task_events)
            route_id = int(row.get("transport_smpp_config_id") or row.get("smpp_config_id") or 0)
            key = (
                str(row.get("status") or ""), failure_class, reason,
                str(row.get("campaign_type") or ""), int(row.get("campaign_id") or 0),
                route_id, attempts_bucket(row.get("attempts")),
                (
                    "retry_scheduled"
                    if row.get("next_attempt_at") and (
                        str(row.get("status") or "") in {"queued", "retrying", "sending"}
                        or failure_class != "none"
                    )
                    else "no_retry_time"
                ),
            )
            breakdown[key] += 1
            for event in task_events:
                if event.get("event_type") != "send_failure":
                    continue
                detail = event.get("detail_object") or {}
                event_class = str(detail.get("failure_class") or failure_class)
                event_reason = "smpp_send_queue_full" if "queue full" in str(event.get("message") or "").lower() else event_class
                event_breakdown[(event_class, event_reason, route_id, str(row.get("campaign_type") or ""), int(row.get("campaign_id") or 0))] += 1
            if failure_class in {"delivery_unknown", "partial_submit"} and str(row.get("status")) in {"queued", "retrying", "sending"}:
                unknown_partial_retry_violations.append({"task_id": task_id, "status": row.get("status"), "failure_class": failure_class})
            task_attempts = attempts_by_task.get(task_id, [])
            accepted_parts = sum(int(item.get("accepted_parts") or 0) for item in task_attempts)
            if accepted_parts and str(row.get("status")) in {"queued", "retrying"}:
                partial_fence_violations.append({"task_id": task_id, "accepted_parts": accepted_parts, "status": row.get("status")})

        def counter_rows(counter, fields):
            return [dict(zip(fields, key), count=value) for key, value in sorted(counter.items(), key=lambda item: (str(item[0]), item[1]))]

        current_statuses = Counter(str(row.get("status") or "") for row in tasks)
        return {
            "current_statuses": dict(sorted(current_statuses.items())),
            "failure_breakdown": counter_rows(
                breakdown,
                ("status", "failure_class", "error_reason", "campaign_type", "campaign_id", "config_id", "attempts_bucket", "retry_schedule"),
            ),
            "failure_event_breakdown": counter_rows(
                event_breakdown,
                ("failure_class", "error_reason", "config_id", "campaign_type", "campaign_id"),
            ),
            "attempt_state_counts": dict(Counter(str(row.get("state") or "") for row in attempts)),
            "part_state_counts": dict(Counter(str(row.get("state") or "") for row in parts)),
            "durable_event_counts": dict(Counter(str(row.get("event_type") or "") for row in events)),
            "event_integrity": {
                "rows": len(events),
                "invalid_rows": len(invalid_event_rows),
                "invalid_examples": invalid_event_rows[:20],
                "canonical_event_types": sorted(known_event_types),
            },
            "unknown_partial_retry_violations": unknown_partial_retry_violations[:20],
            "partial_fence_violations": partial_fence_violations[:20],
            "safety_status": "PASS" if not unknown_partial_retry_violations and not partial_fence_violations else "FAIL",
        }

    def _reconcile(self) -> dict:
        reconcile_started = time.monotonic()
        events = self._event_records()
        submit_events = [event for event in events if event.get("event_type") == "submit_sm"]
        accepted_events = [event for event in submit_events if int(event.get("response_status", 1)) == 0]
        route_progress = []
        for route_index, path in enumerate(self.smsc_event_logs, 1):
            route_events = _read_jsonl(path)
            route_submits = [item for item in route_events if item.get("event_type") == "submit_sm"]
            route_progress.append({
                "route_index": route_index,
                "configured_port": self.smsc_ports[route_index - 1] if route_index <= len(self.smsc_ports) else None,
                "submit_sm": len(route_submits),
                "submit_sm_success": sum(1 for item in route_submits if int(item.get("response_status", 1)) == 0),
                "status_codes": dict(Counter(str(item.get("response_status")) for item in route_submits)),
            })
        tasks = self._db_query(
            """SELECT id,campaign_type,campaign_id,msisdn,status,attempts,max_attempts,
                       next_attempt_at,created_at,updated_at,submitted_at,delivered_at,
                       smsc_message_ids,error_msg,last_smpp_status,failure_class,
                       smpp_config_id,transport_smpp_config_id
                  FROM sms_tasks ORDER BY id"""
        )
        tasks_by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
        for task in tasks:
            tasks_by_key[(str(task["campaign_type"]), int(task["campaign_id"]))].append(task)
        events_by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
        for event in accepted_events:
            key = self._event_key(event)
            if key:
                events_by_key[key].append(event)
        timelines = []
        retry_claimed_tasks: list[int] = []
        missing_msisdns: list[str] = []
        unexpected_msisdns: list[str] = []
        for key, meta in sorted(self.activity_meta.items()):
            own_tasks = tasks_by_key.get(key, [])
            expected = {str(row["msisdn"]) for row in self.rows_by_activity[key]}
            actual = {str(row["msisdn"]) for row in own_tasks}
            missing_msisdns.extend(sorted(expected - actual))
            unexpected_msisdns.extend(sorted(actual - expected))
            retry_claimed_tasks.extend(int(row["id"]) for row in own_tasks if int(row.get("attempts") or 0) > 1)
            timelines.append(self._timeline(key, own_tasks, events))
        mapped_events = sum(len(value) for value in events_by_key.values())
        submit_by_msisdn = Counter(str(event.get("destination_addr")) for event in accepted_events)
        duplicate_submit_msisdns = sorted(
            msisdn for msisdn, count in submit_by_msisdn.items() if count > 1
        )
        status_counts = Counter(str(row.get("status")) for row in tasks)
        full = all(
            len(tasks_by_key.get(key, [])) == int(meta["expected_rows"])
            and sum(
                1 for row in tasks_by_key.get(key, [])
                if str(row.get("status")) in {"accepted", "delivered"}
            ) == int(meta["expected_rows"])
            and len(events_by_key.get(key, [])) == int(meta["expected_rows"])
            for key, meta in self.activity_meta.items()
        )
        all_served = all(item["actual_submit_sm"] > 0 for item in timelines)
        fairness_grace = timedelta(seconds=float(self.spec.get("fairness_late_grace_seconds", 1.0) or 0.0))
        late_served_early = all(
            any(
                item["actual_submit_sm"] > 0 and (
                    not large["last_progress_at"]
                    or not item["first_submit_sm_at"]
                    or _parse_time(item["first_submit_sm_at"]) <= _parse_time(large["last_progress_at"]) + fairness_grace
                )
                for large in timelines if large["large"]
            )
            for item in timelines if item["late"]
        )
        no_starvation = all_served and late_served_early and not duplicate_submit_msisdns
        capacity_profile = bool(self.spec.get("capacity_expected")) or self.profile in {"capacity", "scale10k", "soak", "rca1k", "rca3k"}
        capacity_limited = (
            capacity_profile
            and self.stop_reason in {"time_budget", "max_events"}
            and no_starvation
            and not unexpected_msisdns
        )
        correctness_pass = full and not missing_msisdns and not unexpected_msisdns and not duplicate_submit_msisdns
        fairness_pass = no_starvation
        if capacity_profile:
            final_status = "PASS" if correctness_pass else "CAPACITY_LIMITED" if capacity_limited else "FAIL"
        else:
            final_status = "PASS" if correctness_pass else "FAIL"
        latest_metrics = self.metrics_samples[-1]["status"].get("metrics", {}) if self.metrics_samples else {}
        metric_snapshot = _metric_summary(latest_metrics)
        retry_diagnostics = self._retry_diagnostics(tasks)
        self.timing["reconcile_seconds"] = round(time.monotonic() - reconcile_started, 3)
        route_configs = self._db_query(
            """SELECT id,name,host,port,tps,window_size,pool_size,enabled,
                       delivery_receipt_mode FROM smpp_configs ORDER BY id"""
        )
        bottlenecks = {
            "database_write_transaction_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_database_write_transaction_seconds"),
            "database_write_lock_wait_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_database_write_lock_wait_seconds"),
            "database_write_lock_hold_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_database_write_lock_hold_seconds"),
            "database_statement_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_database_statement_seconds"),
            "task_claim_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_task_claim_seconds"),
            "task_send_async_call_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_task_send_async_call_seconds"),
            "realtime_file_processing_seconds": _metric_latency_summary(latest_metrics, "realtime_file_processing_seconds"),
            "sms_write_batch_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_sms_write_batch_seconds"),
            "smpp_send_queue_wait_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_send_queue_wait_seconds"),
            "smpp_window_wait_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_window_wait_seconds"),
            "smpp_send_lock_wait_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_send_lock_wait_seconds"),
            "smpp_throttle_wait_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_throttle_wait_seconds"),
            "smpp_submit_sm_send_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_submit_sm_send_seconds"),
            "smpp_submit_sm_resp_latency_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_submit_sm_resp_latency_seconds"),
            "smpp_submit_parts_loop_seconds": _metric_latency_summary(latest_metrics, "marketing_copilot_smpp_submit_parts_loop_seconds"),
        }
        return {
            "status": final_status,
            "stop_reason": self.stop_reason,
            "acceptance_states": {
                "BATCH_INGEST_FAIRNESS": "PASS" if all(item["actual_submit_sm"] > 0 for item in timelines if item["campaign_type"] == "batch") else "FAIL",
                "REALTIME_INTAKE_FAIRNESS": "PASS" if all(item["actual_submit_sm"] > 0 for item in timelines if item["campaign_type"] == "realtime") else "FAIL",
                "SMS_DISPATCH_FAIRNESS": "PASS" if fairness_pass else "FAIL",
                "CROSS_TYPE_E2E": "PASS" if correctness_pass else "FAIL",
                "DUCKDB_LOCK_REGRESSION": self.lock_regression.get("status", "NOT_RUN"),
                "SFTP": self.sftp_status,
            },
            "expected_tasks": self.expected_total,
            "generated_tasks": len(tasks),
            "task_statuses": dict(status_counts),
            "smsc_submit_sm": len(submit_events),
            "smsc_submit_sm_success": len(accepted_events),
            "mapped_submit_sm": mapped_events,
            "duplicate_send_tasks": duplicate_submit_msisdns,
            "retry_claimed_tasks": retry_claimed_tasks,
            "retry_diagnostics": retry_diagnostics,
            "missing_msisdns": missing_msisdns,
            "unexpected_msisdns": unexpected_msisdns,
            "per_campaign_timeline": timelines,
            "bottleneck_evidence": bottlenecks,
            "route_progress": route_progress,
            "route_configs": route_configs,
            "runtime_metrics": metric_snapshot,
            "data_reconciliation": {
                "input_files": len(self.input_files),
                "tasks": len(tasks),
                "accepted_submit_events": len(accepted_events),
                "duplicate_send_tasks": len(duplicate_submit_msisdns),
                "retry_claimed_tasks": len(retry_claimed_tasks),
                "missing_msisdns": len(missing_msisdns),
                "unexpected_msisdns": len(unexpected_msisdns),
            },
            "capacity": {
                "classification": "PASS" if correctness_pass else "CAPACITY_LIMITED" if capacity_limited else "FAIL",
                "time_budget_seconds": self.spec.get("time_budget_seconds"),
                "max_events": self.spec.get("max_events"),
                "elapsed_seconds": round(time.monotonic() - self.started_monotonic, 3),
                "resource_guard": {
                    "max_rss_bytes": max((int(item.get("rss_bytes", 0) or 0) for item in self.resource_samples), default=0),
                    "max_cpu_ticks": max((int(item.get("cpu_ticks", 0) or 0) for item in self.resource_samples), default=0),
                    "free_bytes_final": shutil.disk_usage(self.root).free,
                },
            },
            "environment": {
                "platform": platform.platform(),
                "python": str(self.python_executable),
                "ext4_copy_root": str(REPO_ROOT),
                "duckdb_path": str(self.db_path),
                "smsc_ports": self.smsc_ports,
                "smsc_routes": len(self.smsc_ports),
                "sftp_status": self.sftp_status,
                "sles_status": "NOT_RUN",
                "live_env_status": "LIVE_ENV_UNVERIFIED",
            },
            "control_parameters": {
                "smsc_tps": self.spec.get("smsc_tps"),
                "smsc_response_delay": self.spec.get("smsc_response_delay"),
                "task_workers": self.spec.get("task_workers"),
                "claim_batch": self.spec.get("claim_batch"),
                "smpp_window": self.args.smpp_window,
                "smpp_send_queue_capacity": self.args.smpp_send_queue_capacity,
                "smsc_routes": self.spec.get("smsc_routes"),
            },
            "diagnostics": {
                "artifacts_dir": str(self.root),
                "api_log": str(self.api_log),
                "smsc_logs": [str(path) for path in self.smsc_log_paths],
                "smsc_event_logs": [str(path) for path in self.smsc_event_logs],
                "metrics_samples": len(self.metrics_samples),
                "creation_order": self.creation_order,
                "last_snapshot": self.last_snapshot,
                "lifecycle": {
                    "pre_stop": self.pre_stop_snapshot,
                    "stop_transition": self.stop_transition,
                    "post_stop": {
                        "captured_at": _utc_now(),
                        "task_statuses": dict(status_counts),
                        "generated_tasks": len(tasks),
                        "submit_sm": len(submit_events),
                    },
                    "timing": dict(self.timing),
                },
            },
        }


def _load_profile(path: Path, profile: str) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    profiles = data.get("profiles") or {}
    if profile not in profiles:
        raise ValueError(f"unknown profile {profile}; available={','.join(sorted(profiles))}")
    return dict(profiles[profile])


def _args_for_runner(args: argparse.Namespace, spec: dict) -> argparse.Namespace:
    return argparse.Namespace(
        scenario=f"mixed-{args.profile}", python=args.python, events=None, files=1,
        expected_tasks=None, expected_status_counts={}, expected_submit_events=0,
        timeout_seconds=int(args.time_budget_seconds or spec.get("time_budget_seconds", 180)),
        duration_seconds=0, smsc_tps=None, smsc_response_delay=None,
        smsc_reject_ratio=None, smsc_drop_response_ratio=None,
        smsc_drop_receipt_ratio=None, smsc_receipt_delay=None, smsc_disconnect_after=None,
        delivery_receipt_mode="disabled", task_workers=int(spec.get("task_workers", 2)),
        ingest_workers=int(spec.get("ingest_workers", 2)), workers=int(spec.get("copy_workers", 1)),
        claim_batch=int(spec.get("claim_batch", 8)),
        smpp_window=int(spec.get("smpp_window", 10)),
        smpp_send_queue_capacity=int(spec.get("smpp_send_queue_capacity", 32)),
        producer_chunk_size=0, producer_chunk_delay=0.0,
        max_memory_mb=int(args.max_memory_mb or 0), keep_artifacts=bool(args.keep_artifacts),
        profile=args.profile,
    )


def _final_status(report: dict, profile: str) -> dict:
    reconciliation = report.get("reconciliation") or {}
    capacity = reconciliation.get("capacity", {}).get("classification", "NOT_RUN")
    fairness = reconciliation.get("acceptance_states", {}).get("SMS_DISPATCH_FAIRNESS", "FAIL")
    local = report.get("status", "FAIL")
    report.update({
        "CODE_ACCEPTANCE": "NOT_RUN",
        "WSL_MIXED_FAIRNESS": "PASS" if fairness == "PASS" else "FAIL",
        "LOCAL_MIXED_E2E": "PASS" if local == "PASS" else "CAPACITY_LIMITED" if local == "CAPACITY_LIMITED" else local,
        "DUCKDB_LOCK_REGRESSION": reconciliation.get("acceptance_states", {}).get("DUCKDB_LOCK_REGRESSION", "NOT_RUN"),
        "CAPACITY_ACCEPTANCE": capacity,
        "LIVE_ENV": "UNVERIFIED",
        "profile": profile,
    })
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "fairness", "capacity", "soak", "scale10k", "rca1k", "rca3k"), default="smoke")
    parser.add_argument("--scenario-file", default=str(DEFAULT_SCENARIOS))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--seed", type=int, default=20261013)
    parser.add_argument("--time-budget-seconds", type=int, default=0)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--smsc-tps", type=float, default=None)
    parser.add_argument("--smsc-response-delay", type=float, default=None)
    parser.add_argument("--task-workers", type=int, default=0)
    parser.add_argument("--claim-batch", type=int, default=0)
    parser.add_argument("--smpp-window", type=int, default=0)
    parser.add_argument("--smpp-send-queue-capacity", type=int, default=0)
    parser.add_argument("--max-memory-mb", type=int, default=0)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        spec = _load_profile(Path(args.scenario_file), args.profile)
        if args.time_budget_seconds:
            spec["time_budget_seconds"] = args.time_budget_seconds
        if args.max_events:
            spec["max_events"] = args.max_events
        if args.smsc_tps is not None:
            spec["smsc_tps"] = args.smsc_tps
        if args.smsc_response_delay is not None:
            spec["smsc_response_delay"] = args.smsc_response_delay
        if args.task_workers:
            spec["task_workers"] = args.task_workers
        if args.claim_batch:
            spec["claim_batch"] = args.claim_batch
        if args.smpp_window:
            spec["smpp_window"] = args.smpp_window
        if args.smpp_send_queue_capacity:
            spec["smpp_send_queue_capacity"] = args.smpp_send_queue_capacity
        runner_args = _args_for_runner(args, spec)
        runner = MixedCampaignRunner(runner_args, spec, args.seed)
        report = _final_status(runner.run(), args.profile)
        report["scenario_file"] = str(Path(args.scenario_file).absolute())
        report["seed"] = args.seed
        report["command"] = " ".join(sys.argv)
    except (BlockedEnv, ResourceBlocked) as exc:
        report = {
            "profile": args.profile,
            "status": "BLOCKED_RESOURCE" if isinstance(exc, ResourceBlocked) else "BLOCKED_ENV",
            "LOCAL_MIXED_E2E": "BLOCKED_RESOURCE" if isinstance(exc, ResourceBlocked) else "BLOCKED_ENV",
            "WSL_MIXED_FAIRNESS": "BLOCKED_ENV",
            "CAPACITY_ACCEPTANCE": "BLOCKED_RESOURCE" if isinstance(exc, ResourceBlocked) else "BLOCKED_ENV",
            "LIVE_ENV": "UNVERIFIED",
            "failure": str(exc),
            "command": " ".join(sys.argv),
        }
    except Exception as exc:
        report = {
            "profile": args.profile,
            "status": "FAIL",
            "LOCAL_MIXED_E2E": "FAIL",
            "WSL_MIXED_FAIRNESS": "FAIL",
            "CAPACITY_ACCEPTANCE": "FAIL",
            "LIVE_ENV": "UNVERIFIED",
            "failure": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
            "command": " ".join(sys.argv),
        }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"MIXED_ACCEPTANCE_JSON={output}")
    return 0 if report.get("status") in {"PASS", "CAPACITY_LIMITED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
