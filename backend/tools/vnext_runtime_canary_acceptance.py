"""Runtime canary acceptance through the real FastAPI/main lifecycle.

The runner is intentionally loopback-only.  It seeds a disposable DuckDB
route, starts the real ``main.app`` with Uvicorn, injects deterministic
synthetic rows through the gated canary API, and reconciles API progress,
worker journals, the loopback SMPP boundary, and the projection database.
It is separate from ``vnext_loopback_acceptance.py``: that historical runner
prefills the spool before workers start, while this tool tests live ingress.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import multiprocessing
import os
from collections import Counter
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

try:
    import psutil
except ImportError:  # standalone application runtime intentionally excludes test-only psutil
    psutil = None

TOOLS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = Path(os.environ.get("MC_APP_BACKEND_DIR", str(TOOLS_DIR.parent))).resolve()
REPO_ROOT = BACKEND_DIR.parent
CANARY_HTTP_TIMEOUT_SECONDS = 300.0
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from vnext.diagnostics import (
    disable_native_fault_capture,
    enable_native_fault_capture,
    has_native_fault_capture,
    read_process_metadata,
    write_process_metadata,
)
from vnext.ingress import IngressRouter
from vnext.projection_writer import ProgressJournal
from vnext_loopback_acceptance import LoopbackSMSC


_ALLOWED_GATES = ("r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "b2")


def _parse_requested_gates(raw: str) -> list[str]:
    values = [item.strip().lower() for item in str(raw or "").split(",") if item.strip()]
    if not values:
        raise ValueError("at least one gate must be requested")
    unknown = [item for item in values if item not in _ALLOWED_GATES]
    if unknown:
        raise ValueError(f"unsupported gate(s): {','.join(unknown)}")
    duplicates = sorted({item for item in values if values.count(item) > 1})
    if duplicates:
        raise ValueError(f"duplicate gate(s): {','.join(duplicates)}")
    return values


def _validate_gate_contract(args: argparse.Namespace) -> list[str]:
    values = _parse_requested_gates(getattr(args, "gates", ""))
    if "r2" in values and not bool(getattr(args, "authorize_p5500", False)):
        raise ValueError("R2 requires --authorize-p5500")
    if "b2" in values and values != ["b2"]:
        raise ValueError("B2 must run as the only requested gate")
    stability_seconds = float(getattr(args, "stability_seconds", 0.0) or 0.0)
    if stability_seconds < 0:
        raise ValueError("stability-seconds must be non-negative")
    if stability_seconds > 0 and "r7" not in values:
        raise ValueError("stability-seconds > 0 requires explicit r7 in --gates")
    b2_seconds = float(getattr(args, "b2_seconds", 42.0) or 0.0)
    if not 40.0 <= b2_seconds <= 45.0:
        raise ValueError("b2-seconds must be between 40 and 45")
    return values


def _validate_runtime_arguments(args: argparse.Namespace) -> None:
    route_tps = float(getattr(args, "route_tps", 0.0))
    response_delay_ms = float(getattr(args, "response_delay_ms", 0.0))
    workers = int(getattr(args, "workers", 0))
    partitions = int(getattr(args, "partitions", 0))
    projection_queue = int(getattr(args, "projection_queue", 0))
    max_inflight = int(getattr(args, "max_inflight", 0))
    prefetch = int(getattr(args, "prefetch", 0))
    if route_tps <= 0:
        raise ValueError("route-tps must be greater than zero")
    if response_delay_ms < 0:
        raise ValueError("response-delay-ms must be non-negative")
    if not 1 <= workers <= 4:
        raise ValueError("workers must be between 1 and 4")
    if not 16 <= partitions <= 256:
        raise ValueError("partitions must be between 16 and 256")
    if projection_queue < 128:
        raise ValueError("projection-queue must be at least 128")
    if not 1 <= max_inflight <= 256:
        raise ValueError("max-inflight must be between 1 and 256")
    if not 1 <= prefetch <= 128:
        raise ValueError("prefetch must be between 1 and 128")
    if getattr(args, "watchdog", "enabled") not in {"enabled", "disabled"}:
        raise ValueError("watchdog must be enabled or disabled")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _disk_bytes(root: Path) -> int:
    total = 0
    if not root.exists():
        return 0
    for path in root.rglob("*"):
        if path.is_file():
            try:
                total += int(path.stat().st_size)
            except OSError:
                pass
    return total


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _LoopbackThread:
    def __init__(self, *, response_delay: float = 0.0):
        import asyncio

        self.loop = asyncio.new_event_loop()
        self.smsc = LoopbackSMSC(response_delay=response_delay)
        self.thread = threading.Thread(target=self._run, name="mc-canary-loopback", daemon=True)
        self.port = 0

    def _run(self) -> None:
        asyncio = __import__("asyncio")
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def start(self) -> int:
        import asyncio

        self.thread.start()
        future = asyncio.run_coroutine_threadsafe(self.smsc.start(), self.loop)
        self.port = int(future.result(timeout=10))
        return self.port

    def stop(self) -> None:
        import asyncio

        if self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self.smsc.stop(), self.loop)
            future.result(timeout=10)
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread.is_alive():
            self.thread.join(timeout=10)
        self.loop.close()


class _UvicornThread:
    def __init__(self, app, port: int):
        import uvicorn

        self.server = uvicorn.Server(uvicorn.Config(
            app,
            host="127.0.0.1",
            port=int(port),
            lifespan="on",
            log_level="warning",
            access_log=False,
        ))
        self.thread = threading.Thread(target=self._run, name="mc-canary-api", daemon=True)
        self.error: str = ""

    @property
    def port(self) -> int:
        return int(self.server.config.port)

    def _run(self) -> None:
        try:
            self.server.run()
        except BaseException as exc:  # pragma: no cover - startup failures are reported by the runner
            self.error = f"{type(exc).__name__}: {exc}"

    def start(self) -> None:
        self.thread.start()
        deadline = time.monotonic() + 30.0
        while not self.server.started and self.thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        if not self.server.started:
            raise RuntimeError(self.error or "Uvicorn did not report started")

    def stop(self) -> None:
        self.server.should_exit = True
        if self.thread.is_alive():
            self.thread.join(timeout=45)
        if self.thread.is_alive():
            raise RuntimeError("Uvicorn shutdown exceeded 45 seconds")


class _Sampler:
    def __init__(self, runner: "CanaryRunner", interval: float = 1.0):
        self.runner = runner
        self.interval = max(1.0, float(interval))
        self.samples: list[dict] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="mc-canary-sampler", daemon=True)
        self._last_proc_cpu_ticks: dict[int, int] = {}
        self._last_proc_cpu_at = time.monotonic()

    def _linux_process_snapshot(self) -> dict:
        """Aggregate the canary process tree without a test-only psutil wheel."""

        proc_root = Path("/proc")
        if not proc_root.is_dir():
            return {"pids": [os.getpid()], "rss_bytes": 0, "cpu_percent": 0.0,
                    "fd_count": 0, "thread_count": 0}
        parents: dict[int, int] = {}
        stats: dict[int, dict] = {}
        for path in proc_root.glob("[0-9]*"):
            try:
                pid = int(path.name)
                text = (path / "stat").read_text(encoding="ascii")
                marker = text.rfind(")")
                fields = text[marker + 2:].split()
                parents[pid] = int(fields[1])
                ticks = int(fields[11]) + int(fields[12])
                rss = 0
                threads = 0
                for line in (path / "status").read_text(encoding="ascii").splitlines():
                    if line.startswith("VmRSS:"):
                        rss = int(line.split()[1]) * 1024
                    elif line.startswith("Threads:"):
                        threads = int(line.split()[1])
                try:
                    fd_count = len(list((path / "fd").iterdir()))
                except OSError:
                    fd_count = 0
                stats[pid] = {"ticks": ticks, "rss": rss, "fds": fd_count, "threads": threads}
            except (OSError, ValueError, IndexError):
                continue
        root_pid = os.getpid()
        pids = [root_pid]
        index = 0
        while index < len(pids):
            parent = pids[index]
            pids.extend(pid for pid, ppid in parents.items() if ppid == parent and pid not in pids)
            index += 1
        pids = [pid for pid in pids if pid in stats]
        now = time.monotonic()
        elapsed = max(0.001, now - self._last_proc_cpu_at)
        ticks = sum(int(stats[pid]["ticks"]) for pid in pids)
        previous = sum(int(self._last_proc_cpu_ticks.get(pid, ticks)) for pid in pids)
        clock = max(1, int(os.sysconf("SC_CLK_TCK")))
        cpu_percent = max(0.0, (ticks - previous) / clock / elapsed * 100.0 / max(1, os.cpu_count() or 1))
        self._last_proc_cpu_ticks = {pid: int(stats[pid]["ticks"]) for pid in pids}
        self._last_proc_cpu_at = now
        return {
            "pids": pids,
            "rss_bytes": sum(int(stats[pid]["rss"]) for pid in pids),
            "cpu_percent": round(cpu_percent, 3),
            "fd_count": sum(int(stats[pid]["fds"]) for pid in pids),
            "thread_count": sum(int(stats[pid]["threads"]) for pid in pids),
        }

    def _process_snapshot(self) -> dict:
        if psutil is None and Path("/proc").is_dir():
            return self._linux_process_snapshot()
        if psutil is None:
            rss = 0
            try:
                for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
                    if line.startswith("VmRSS:"):
                        rss = int(line.split()[1]) * 1024
                        break
            except (OSError, ValueError, IndexError):
                pass
            try:
                fd_count = len(list(Path("/proc/self/fd").iterdir()))
            except OSError:
                fd_count = 0
            return {"pids": [os.getpid()], "rss_bytes": rss, "cpu_percent": 0.0, "fd_count": fd_count}
        processes = []
        try:
            root = psutil.Process(os.getpid())
            processes = [root, *root.children(recursive=True)]
        except (psutil.Error, OSError):
            processes = []
        rss = cpu = fd_count = 0
        pids = []
        for process in processes:
            try:
                rss += int(process.memory_info().rss)
                cpu += float(process.cpu_percent(None))
                pids.append(int(process.pid))
                try:
                    fd_count += int(process.num_fds())
                except (AttributeError, psutil.Error):
                    pass
            except (psutil.Error, OSError):
                continue
        return {"pids": pids, "rss_bytes": rss, "cpu_percent": round(cpu, 3), "fd_count": fd_count}

    def _sample(self) -> dict:
        sample = {
            "ts": time.time(),
            "process": self._process_snapshot(),
            "run_root_bytes": _disk_bytes(self.runner.run_root),
            "spool_bytes": _disk_bytes(self.runner.run_root / "spool"),
            "journal_bytes": _disk_bytes(self.runner.run_root / "progress"),
        }
        try:
            status = self.runner.api("GET", "/api/vnext/status")
            projection = status.get("projection") or {}
            sample.update({
                "pending_records": int((status.get("spool") or {}).get("pending_records", 0)),
                "writer_alive": bool(projection.get("alive", False)),
                "writer_queue_depth": projection.get("queue_depth"),
                "writer_lag_seconds": float(projection.get("projection_lag_seconds", 0.0) or 0.0),
                "writer_restart_count": int(projection.get("restart_count", 0) or 0),
                "route_credits": self.runner.route_credit_state(),
                "start_method": status.get("start_method"),
                "runtime_processes": status.get("processes") or [],
            })
        except Exception as exc:
            sample["api_error"] = f"{type(exc).__name__}: {exc}"
        return sample

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.samples.append(self._sample())
            self.stop_event.wait(self.interval)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> list[dict]:
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(timeout=5)
        return list(self.samples)


class CanaryRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_root = Path(args.run_root).resolve()
        self.run_root.mkdir(parents=True, exist_ok=True)
        self.api_port = _free_port()
        self.smsc = _LoopbackThread(response_delay=max(0.0, float(args.response_delay_ms) / 1000.0))
        self.api_server: _UvicornThread | None = None
        self.main = None
        self.route_id = 1
        self.route_port = 0
        self._received_baseline = 0
        self.expected_destinations: dict[int, set[str]] = {}
        self.events: list[dict] = []
        self.started_at = time.time()
        self._progress_offsets: dict[str, int] = {}
        self._progress_events_by_id: dict[str, dict] = {}
        self._received_counts: Counter[str] = Counter()
        self._received_observer_index = 0
        self._received_baselines: dict[int, Counter[str]] = {}
        self._phase_marker_path = self.run_root / "diagnostics" / "phase-markers.jsonl"
        self._phase_sequence = 0
        self.native_fault_path: Path | None = None
        self.api_native_fault_path: Path | None = None
        _validate_runtime_arguments(args)
        self.requested_gates = _validate_gate_contract(args)
        self.executed_gates: list[str] = []
        self._first_campaign_completed = False
        # The runner is loopback-only; do not let an inherited corporate proxy
        # intercept its local API or SMPP observations.
        self._url_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _mark_phase(self, phase: str, *, progress: dict | None = None, role: str = "canary-root") -> None:
        """Persist bounded stage markers without message or MSISDN content."""

        self._phase_sequence += 1
        item = {
            "sequence": int(self._phase_sequence),
            "phase": str(phase),
            "pid": os.getpid(),
            "monotonic_seconds": time.monotonic(),
            "captured_at": time.time(),
            "progress": dict(progress or {}),
        }
        try:
            self._phase_marker_path.parent.mkdir(parents=True, exist_ok=True)
            with self._phase_marker_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            write_process_metadata(
                self.run_root / "diagnostics",
                role,
                run_id=self.run_root.name,
                phase=str(phase),
                status="running",
                progress=dict(progress or {}),
                extra={
                    "last_phase_marker": str(phase),
                    "phase_marker_path": str(self._phase_marker_path),
                    "native_fault_log": str(
                        self.api_native_fault_path if role == "api-main" else self.native_fault_path
                    ) if (self.api_native_fault_path if role == "api-main" else self.native_fault_path) else "",
                },
            )
        except OSError:
            pass

    def _record_resource_tracker(self) -> None:
        try:
            tracker = getattr(multiprocessing, "resource_tracker", None)
            tracker_pid = getattr(getattr(tracker, "_resource_tracker", None), "_pid", None)
            if not tracker_pid:
                return
            write_process_metadata(
                self.run_root / "diagnostics",
                "resource-tracker",
                run_id=self.run_root.name,
                start_method="spawn",
                phase="observed",
                status="running",
                process_pid=int(tracker_pid),
                process_ppid=os.getpid(),
                extra={
                    "observed_only": True,
                    "native_fault_capture": "UNAVAILABLE",
                    "observed_by_pid": os.getpid(),
                },
            )
        except (AttributeError, TypeError, ValueError, OSError):
            pass

    def setup_environment(self) -> None:
        env = {
            "DUCKDB_PATH": str(self.run_root / "app.duckdb"),
            "MC_DB_CONFIG_FILE": str(self.run_root / "db-config.json"),
            "MC_STATE_DIR": str(self.run_root / "state"),
            "MC_VNEXT_RUN_ROOT": str(self.run_root / "vnext"),
            "MC_VNEXT_WORKERS": str(max(1, min(4, int(self.args.workers)))),
            "MC_VNEXT_PARTITIONS": str(max(16, min(256, int(self.args.partitions)))),
            "MC_VNEXT_PROJECTION_QUEUE": str(max(128, int(self.args.projection_queue))),
            "MC_VNEXT_MAX_INFLIGHT": str(max(1, min(256, int(self.args.max_inflight)))),
            "MC_VNEXT_PREFETCH_PER_PARTITION": str(max(1, min(128, int(self.args.prefetch)))),
            "MC_PIPELINE_MODE": "vnext",
            "MC_VNEXT_CANARY_MODE": "1",
            "MC_AUTH_DISABLED": "1",
            "MC_ADMIN_USERNAME": "canary",
            "MC_ADMIN_PASSWORD": "not-a-production-secret",
            "MC_FRONTEND_DIR": str(self.run_root / "empty-frontend"),
            "MC_STALL_DIAGNOSTICS_DIR": str(self.run_root / "stall-diagnostics"),
            "MC_STALL_WATCHDOG_ENABLED": "1" if getattr(self.args, "watchdog", "enabled") == "enabled" else "0",
            "PYTHONFAULTHANDLER": "1",
        }
        os.environ.update(env)
        self._mark_phase("environment_ready", progress={"watchdog": self.args.watchdog})

    def seed_database(self) -> None:
        self._mark_phase("seed_begin")
        self.route_port = self.smsc.start()
        from database import close_root_connection, get_connection, init_db, set_database_file

        db_path = str(self.run_root / "app.duckdb")
        set_database_file(db_path)
        init_db()
        conn = get_connection()
        conn.execute(
            """INSERT INTO smpp_configs(
                name,host,port,system_id,password,source_addr,window_size,timeout,
                tps,priority,enabled
            ) VALUES (?,?,?,?,?,?,?,?,?,?,1)""",
            (
                "canary-loopback", "127.0.0.1", self.route_port, "canary", "canary",
                "CANARY", 32, 10, max(0.0, float(self.args.route_tps)), 100,
            ),
        )
        conn.commit()
        self.route_id = int(conn.execute("SELECT id FROM smpp_configs ORDER BY id DESC LIMIT 1").fetchone()[0])
        conn.close()
        close_root_connection()
        self._mark_phase("seed_complete", progress={"route_id": int(self.route_id)})

    def start_api(self) -> None:
        self.api_native_fault_path = enable_native_fault_capture(
            self.run_root / "diagnostics", "api-main"
        )
        self._mark_phase("api_start", role="api-main")
        if self.main is None:
            self.main = importlib.import_module("main")
        from services import vnext_service
        self.vnext_service = vnext_service
        self.api_server = _UvicornThread(self.main.app, self.api_port)
        self.api_server.start()
        self.wait_for(lambda value: value.get("started") is True, "/api/vnext/status", timeout=30)
        self._record_resource_tracker()
        self._mark_phase("api_started", role="api-main", progress={"port": int(self.api_port)})

    def stop_api(self) -> None:
        if self.api_server is not None:
            self.api_server.stop()
            self.api_server = None
            self._mark_phase("api_stopped", role="api-main")

    def api(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"http://127.0.0.1:{self.api_port}{path}"
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with self._url_opener.open(request, timeout=CANARY_HTTP_TIMEOUT_SECONDS) as response:
                value = json.loads(response.read().decode("utf-8"))
                return value if isinstance(value, dict) else {"value": value}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {path}: {detail[:500]}") from exc

    def wait_for(self, predicate, path: str, *, timeout: float = 300.0) -> dict:
        deadline = time.monotonic() + max(0.1, float(timeout))
        last = {}
        while time.monotonic() < deadline:
            last = self.api("GET", path)
            if predicate(last):
                return last
            time.sleep(0.1)
        raise TimeoutError(f"timeout waiting for {path}: {json.dumps(last, ensure_ascii=False)[:1000]}")

    @staticmethod
    def rows(campaign_id: int, count: int, *, start: int = 0) -> list[dict]:
        return [
            {"msisdn": f"090{int(campaign_id):04d}{index:07d}",
             "message": f"canary campaign={campaign_id} row={index}"}
            for index in range(int(start), int(start) + int(count))
        ]

    @staticmethod
    def destinations(rows: list[dict], phone_prefix: str = "") -> set[str]:
        values = set()
        for row in rows:
            raw = str(row["msisdn"])
            if raw.startswith("+"):
                raw = raw[1:]
            if raw.startswith("0") and phone_prefix:
                raw = str(phone_prefix) + raw[1:]
            values.add(raw)
        return values

    def ingest(self, campaign_id: int, rows: list[dict], *, source_id: str,
               row_ordinal_start: int = 0, phone_prefix: str = "") -> dict:
        result = self.api(
            "POST",
            f"/api/vnext/canary/batch/{int(campaign_id)}/ingest",
            {
                "rows": rows,
                "route_id": self.route_id,
                "source_id": source_id,
                "payload_ref": f"canary:{source_id}",
                "phone_prefix": phone_prefix,
                "row_ordinal_start": int(row_ordinal_start),
                "source_addr": "CANARY",
            },
        )
        self.expected_destinations.setdefault(int(campaign_id), set()).update(
            self.destinations(rows, phone_prefix)
        )
        return result

    @staticmethod
    def response_summary(response: dict) -> dict:
        """Keep gate evidence bounded and exclude synthetic MSISDN values."""
        accepted_msisdns = response.get("accepted_msisdns")
        return {
            "accepted": int(response.get("accepted", 0) or 0),
            "accepted_msisdns_count": len(accepted_msisdns) if isinstance(accepted_msisdns, list) else 0,
            "duplicate": int(response.get("duplicate", 0) or 0),
            "rejected": int(response.get("rejected", 0) or 0),
            "rejected_reasons": response.get("rejected_reasons") or {},
            "by_campaign": response.get("by_campaign") or {},
            "by_partition": response.get("by_partition") or {},
            "success": bool(response.get("success", False)),
        }

    def wait_idle(self, timeout: float = 300.0) -> dict:
        self._mark_phase("wait_idle_begin", progress={"timeout_seconds": float(timeout)})
        result = self.api("GET", f"/api/vnext/idle?timeout={max(0.1, float(timeout))}")
        self._mark_phase(
            "wait_idle_complete",
            progress={"pending_records": int((result.get("spool") or {}).get("pending_records", -1))},
        )
        return result

    def progress_events(self) -> list[dict]:
        progress_root = self.run_root / "vnext" / "progress"
        for path in sorted(progress_root.glob("worker-*.jsonl")):
            try:
                key = str(path)
                offset = int(self._progress_offsets.get(key, 0))
                journal = ProgressJournal(path)
                while True:
                    batch, next_offset = journal.read_from(offset, max_events=4096)
                    if not batch and next_offset == offset:
                        break
                    for event in batch:
                        event_id = str(event.get("event_id") or "")
                        if event_id:
                            self._progress_events_by_id.setdefault(event_id, event)
                    offset = int(next_offset)
                    if len(batch) < 4096:
                        break
                self._progress_offsets[key] = offset
            except (OSError, ValueError, TypeError):
                continue
        return list(self._progress_events_by_id.values())

    def campaign_summary(self, campaign_id: int) -> dict:
        events = [item for item in self.progress_events() if int(item.get("campaign_id", -1)) == int(campaign_id)]
        states: dict[str, int] = {}
        for event in events:
            state = str(event.get("state") or "unknown")
            states[state] = states.get(state, 0) + 1
        event_ids = sorted(str(item["event_id"]) for item in events)
        return {
            "events": len(events),
            "states": states,
            "event_id_sample": event_ids[:3] + (["..."] if len(event_ids) > 6 else []) + event_ids[-3:],
        }

    def loopback_summary(self, campaign_id: int) -> dict:
        received_values = self.smsc.smsc.received
        if self._received_observer_index > len(received_values):
            self._received_observer_index = 0
            self._received_counts.clear()
            self._received_baselines.clear()
        for item in received_values[self._received_observer_index:]:
            self._received_counts[str(item["destination"])] += 1
        self._received_observer_index = len(received_values)
        expected = self.expected_destinations.get(int(campaign_id), set())
        baseline = self._received_baselines.get(int(campaign_id), Counter())
        current_counts = Counter({
            value: max(0, int(count) - int(baseline.get(value, 0)))
            for value, count in self._received_counts.items()
            if int(count) > int(baseline.get(value, 0))
        })
        actual_keys = expected.intersection(current_counts)
        duplicates = sorted(value for value in actual_keys if current_counts[value] > 1)
        missing = expected.difference(current_counts)
        unexpected = sorted(
            value for value, count in current_counts.items()
            if count and value not in expected
        )
        return {
            "expected": len(expected),
            "received": sum(current_counts[value] for value in actual_keys),
            "duplicates": duplicates,
            "missing": sorted(missing)[:20],
            "missing_count": len(missing),
            "unexpected": unexpected[:20],
            "unexpected_count": len(unexpected),
        }

    def route_credit_state(self) -> dict:
        root = self.run_root / "vnext" / "route-credits"
        result = {}
        for path in sorted(root.glob("route-*.json")):
            try:
                result[path.name] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
        return result

    def writer_crash(self) -> dict:
        runtime = self.vnext_service.runtime()
        process = runtime.projection.process
        old_pid = getattr(process, "pid", None)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        return {"old_pid": old_pid, "alive_after_terminate": bool(process.is_alive())}

    def worker_crash(self) -> dict:
        runtime = self.vnext_service.runtime()
        supervisor = runtime.supervisor
        if supervisor is None:
            raise RuntimeError("worker supervisor is not active")
        status = supervisor.status()
        worker = next(item for item in status["workers"] if item.get("alive"))
        handle = supervisor._handles[int(worker["worker_id"])]
        old_pid = getattr(handle.process, "pid", None)
        handle.process.terminate()
        handle.process.join(timeout=5)
        self.wait_for(
            lambda value: int(value.get("supervisor", {}).get("automatic_restarts", 0)) >= 1
            and all(item.get("alive") for item in value.get("supervisor", {}).get("workers", [])),
            "/api/vnext/status",
            timeout=30,
        )
        return {"worker_id": worker["worker_id"], "old_pid": old_pid}

    def query_projection(self) -> dict:
        db_path = self.run_root / "vnext" / "projection" / "vnext-projection.duckdb"
        if not db_path.exists():
            return {"exists": False}
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            event_count = int(conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0])
            totals = conn.execute(
                "SELECT COALESCE(SUM(taken),0), COALESCE(SUM(socket_written),0), COALESCE(SUM(cancelled),0) FROM vnext_projection"
            ).fetchone()
            return {
                "exists": True,
                "unique_events": event_count,
                "taken": int(totals[0]),
                "socket_written": int(totals[1]),
                "cancelled": int(totals[2]),
            }
        finally:
            conn.close()

    def _diagnostic_processes(self) -> list[dict]:
        """Collect canary and VNext child records without exposing payload data."""

        values: list[dict] = []
        seen: set[tuple[str, int]] = set()
        for root in (
            self.run_root / "diagnostics",
            self.run_root / "vnext" / "diagnostics",
        ):
            for item in read_process_metadata(root):
                key = (str(item.get("role") or ""), int(item.get("pid") or 0))
                if key in seen:
                    continue
                seen.add(key)
                values.append(item)
        return values

    def _native_fault_logs(self) -> list[str]:
        paths: list[str] = []
        for root in (
            self.run_root / "diagnostics" / "native-crash",
            self.run_root / "vnext" / "diagnostics" / "native-crash",
        ):
            try:
                paths.extend(str(path) for path in sorted(root.glob("*-faulthandler.log")))
            except OSError:
                continue
        return paths

    def gate_live(self, name: str, campaign_id: int, batches: list[tuple[list[dict], str, int]], *,
                  expected: int, timeout: float = 300.0, retry_check: bool = False) -> dict:
        started = time.monotonic()
        gate_progress = {
            "gate": name,
            "campaign_id": int(campaign_id),
            "expected": int(expected),
        }
        self._mark_phase("gate_start", progress=gate_progress)
        if str(name).lower().startswith("r2"):
            self._mark_phase("r2_campaign_begin", progress=gate_progress)
        self._received_baselines.setdefault(int(campaign_id), Counter(self._received_counts))
        accepted = duplicate = 0
        responses = []
        for rows, source_id, ordinal in batches:
            item = self.ingest(campaign_id, rows, source_id=source_id, row_ordinal_start=ordinal, phone_prefix="251")
            responses.append(self.response_summary(item))
            accepted += int(item.get("accepted", 0))
            duplicate += int(item.get("duplicate", 0))
        self._mark_phase(
            "batch_enqueued",
            progress={
                "gate": name,
                "campaign_id": int(campaign_id),
                "expected": int(expected),
                "accepted": int(accepted),
                "batch_count": len(batches),
            },
        )
        self._mark_phase("ingress_complete", progress={"gate": name, "campaign_id": int(campaign_id), "accepted": int(accepted)})
        if retry_check and batches:
            rows, source_id, ordinal = batches[0]
            retry = self.ingest(campaign_id, rows, source_id=source_id, row_ordinal_start=ordinal, phone_prefix="251")
            responses.append(self.response_summary(retry))
            duplicate += int(retry.get("duplicate", 0))
        status = self.wait_idle(timeout)
        self._mark_phase("idle_boundary_reached", progress={"gate": name, "campaign_id": int(campaign_id)})
        boundary_deadline = time.monotonic() + max(1.0, min(float(timeout), 30.0))
        while time.monotonic() < boundary_deadline:
            boundary = self.loopback_summary(campaign_id)
            if boundary["received"] >= expected:
                break
            time.sleep(0.05)
        self._mark_phase(
            "projection_reconcile_begin",
            progress={"gate": name, "campaign_id": int(campaign_id)},
        )
        summary = self.campaign_summary(campaign_id)
        loopback = self.loopback_summary(campaign_id)
        self._mark_phase(
            "projection_reconcile_end",
            progress={
                "gate": name,
                "campaign_id": int(campaign_id),
                "socket_written": int(summary["states"].get("socket_written", 0)),
                "loopback_received": int(loopback["received"]),
            },
        )
        if not self._first_campaign_completed:
            self._first_campaign_completed = True
            self._mark_phase(
                "first_campaign_completed",
                progress={
                    "gate": name,
                    "campaign_id": int(campaign_id),
                    "status": "PASS" if (
                        summary["states"].get("socket_written", 0) == expected
                        and loopback["received"] == expected
                    ) else "FAIL",
                },
            )
        self._mark_phase("reconciliation_complete", progress={"gate": name, "campaign_id": int(campaign_id), "received": int(loopback["received"])})
        passed = (
            accepted == expected
            and summary["states"].get("socket_written", 0) == expected
            and loopback["received"] == expected
            and not loopback["duplicates"]
            and loopback["missing_count"] == 0
            and loopback["unexpected_count"] == 0
            and int((status.get("spool") or {}).get("pending_records", 0)) == 0
        )
        return {
            "gate": name,
            "status": "PASS" if passed else "FAIL",
            "duration_seconds": round(time.monotonic() - started, 3),
            "campaign_id": campaign_id,
            "expected": expected,
            "accepted": accepted,
            "duplicate": duplicate,
            "responses": responses,
            "summary": summary,
            "loopback": loopback,
            "runtime": {
                "pending_records": int((status.get("spool") or {}).get("pending_records", 0)),
                "projection": status.get("projection", {}),
                "supervisor": status.get("supervisor", {}),
            },
        }

    def gate_r0(self) -> dict:
        time.sleep(max(5.0, float(self.args.idle_seconds)))
        rows = self.rows(1000, 100)
        return self.gate_live("R0", 1000, [(rows, "r0-wave-1", 0)], expected=100)

    def gate_r1(self) -> dict:
        batches = []
        total = 0
        for campaign_id in range(1101, 1105):
            first = self.rows(campaign_id, 125)
            second = self.rows(campaign_id, 125, start=125)
            batches.extend([(first, f"r1-{campaign_id}-a", 0), (second, f"r1-{campaign_id}-b", 125)])
            total += 250
        result = {"gate": "R1", "status": "PASS", "campaigns": []}
        for campaign_id in range(1101, 1105):
            item = self.gate_live(
                "R1-campaign",
                campaign_id,
                [(self.rows(campaign_id, 125), f"r1-{campaign_id}-a", 0),
                 (self.rows(campaign_id, 125, start=125), f"r1-{campaign_id}-b", 125)],
                expected=250,
                retry_check=True,
            )
            result.setdefault("campaigns", []).append(item)
        result.update({
            "gate": "R1",
            "status": "PASS" if all(item["status"] == "PASS" for item in result["campaigns"]) else "FAIL",
            "expected": total,
            "accepted": sum(item["accepted"] for item in result["campaigns"]),
            "duplicates": sum(item["duplicate"] for item in result["campaigns"]),
        })
        return result

    def gate_r2(self) -> dict:
        plan = ((1201, 2500), (1202, 2500), (1203, 250), (1204, 250))
        result = {"gate": "R2", "status": "PASS", "authorized": True, "campaigns": []}
        for campaign_id, count in plan:
            batches = []
            for start in range(0, count, 250):
                size = min(250, count - start)
                batches.append((self.rows(campaign_id, size, start=start), f"r2-{campaign_id}-{start}", start))
            result["campaigns"].append(self.gate_live("R2-campaign", campaign_id, batches, expected=count, timeout=300))
        result["status"] = "PASS" if all(item["status"] == "PASS" for item in result["campaigns"]) else "FAIL"
        result["expected"] = 5500
        result["accepted"] = sum(item["accepted"] for item in result["campaigns"])
        return result

    def gate_r3(self) -> dict:
        rows = self.rows(1300, 1000)
        return self.gate_live("R3", 1300, [(rows[:250], "r3-a", 0), (rows[250:500], "r3-b", 250),
                                            (rows[500:750], "r3-c", 500), (rows[750:], "r3-d", 750)], expected=1000, timeout=300)

    def gate_r4(self) -> dict:
        crash = self.writer_crash()
        rows = self.rows(1400, 400)
        result = self.gate_live("R4", 1400, [(rows, "r4-burst", 0)], expected=400, timeout=300)
        result["writer_crash"] = crash
        result["writer_restart_count"] = int(result["runtime"]["projection"].get("restart_count", 0) or 0)
        result["status"] = "PASS" if result["status"] == "PASS" and result["writer_restart_count"] >= 1 else "FAIL"
        return result

    def gate_r5(self) -> dict:
        started = time.monotonic()
        a = self.rows(1501, 100)
        b = self.rows(1502, 100)
        self.ingest(1501, a, source_id="r5-a", phone_prefix="251")
        self.ingest(1502, b, source_id="r5-b", phone_prefix="251")
        time.sleep(0.1)
        self.api("POST", "/api/vnext/campaigns/batch/1501/pause")
        paused_snapshot = self.campaign_summary(1501)
        time.sleep(0.5)
        paused_after = self.campaign_summary(1501)
        self.api("POST", "/api/vnext/campaigns/batch/1501/resume")
        self.wait_idle(300)

        persistent_id = 1503
        self.api("POST", f"/api/vnext/campaigns/batch/{persistent_id}/pause")
        persistent_rows = self.rows(persistent_id, 20)
        self.ingest(persistent_id, persistent_rows, source_id="r5-persist", phone_prefix="251")
        time.sleep(0.5)
        self.stop_api()
        self.start_api()
        status_after_restart = self.api("GET", "/api/vnext/status")
        persisted = any(item.get("campaign_id") == persistent_id and item.get("state") == "paused"
                        for item in status_after_restart.get("controls", []))
        self.api("POST", f"/api/vnext/campaigns/batch/{persistent_id}/resume")
        self.wait_idle(300)

        cancelled_id = 1504
        self.api("POST", f"/api/vnext/campaigns/batch/{cancelled_id}/cancel")
        cancelled_rows = self.rows(cancelled_id, 20)
        self.ingest(cancelled_id, cancelled_rows, source_id="r5-cancel", phone_prefix="251")
        self.wait_idle(300)
        summary_a = self.campaign_summary(1501)
        summary_b = self.campaign_summary(1502)
        summary_p = self.campaign_summary(persistent_id)
        summary_c = self.campaign_summary(cancelled_id)
        result = {
            "gate": "R5",
            "status": "PASS" if (
                summary_b["states"].get("socket_written", 0) == 100
                and summary_p["states"].get("socket_written", 0) == 20
                and summary_c["states"].get("cancelled", 0) == 20
                and persisted
            ) else "FAIL",
            "duration_seconds": round(time.monotonic() - started, 3),
            "pause": {"before": paused_snapshot, "after": paused_after, "control_persisted": persisted},
            "campaign_a": summary_a,
            "campaign_b": summary_b,
            "persistent_campaign": summary_p,
            "cancelled_campaign": summary_c,
        }
        return result

    def gate_r6(self) -> dict:
        started = time.monotonic()
        crash = self.worker_crash()
        rows = self.rows(1601, 200)
        result = self.gate_live("R6", 1601, [(rows, "r6-after-restart", 0)], expected=200)
        result["worker_crash"] = crash
        result["status"] = "PASS" if result["status"] == "PASS" else "FAIL"
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        return result

    def gate_r7(self, seconds: float) -> dict:
        started = time.monotonic()
        sampler = _Sampler(self, interval=1.0)
        sampler.start()
        sent = 0
        index = 0
        target = 4000 if float(seconds) >= 300.0 else 0
        deadline = time.monotonic() + max(1.0, float(seconds))
        while time.monotonic() < deadline or (target and sent < target):
            count = 100 if index % 6 == 0 else 20
            if target:
                count = min(count, max(0, target - sent))
                if count <= 0:
                    break
            campaign_id = 1700 + index
            rows = self.rows(campaign_id, count)
            item = self.ingest(campaign_id, rows, source_id=f"r7-{index}", phone_prefix="251")
            sent += int(item.get("accepted", 0))
            index += 1
            time.sleep(5.0)
        self.wait_idle(300)
        samples = sampler.stop()
        result = {
            "gate": "R7",
            "status": "PASS" if (
                samples
                and (not target or sent >= target)
                and all(int(item.get("pending_records", 0)) < 10000 for item in samples)
            ) else "FAIL",
            "duration_seconds": round(time.monotonic() - started, 3),
            "sent": sent,
            "target": target,
            "samples": samples,
        }
        if samples:
            result["peak_rss_bytes"] = max(item["process"]["rss_bytes"] for item in samples)
            result["peak_fd_count"] = max(item["process"]["fd_count"] for item in samples)
            result["peak_run_root_bytes"] = max(item["run_root_bytes"] for item in samples)
            result["max_writer_lag_seconds"] = max(float(item.get("writer_lag_seconds", 0.0) or 0.0) for item in samples)
        return result

    def gate_b2(self) -> dict:
        """Cross the historical elapsed-time boundary with a small real-main load.

        B2 is deliberately separate from R7: it sends exactly 300 synthetic
        messages once, then keeps the real API/status path active until a
        bounded 40--45 second elapsed-time boundary.  This preserves the
        diagnostic question without recreating the historical P5500 volume.
        """

        started = time.monotonic()
        boundary_seconds = float(self.args.b2_seconds)
        expected = 300
        campaign_id = 1800
        sampler = _Sampler(self, interval=1.0)
        sampler.start()
        self._mark_phase(
            "b2_boundary_begin",
            progress={
                "campaign_id": campaign_id,
                "expected": expected,
                "health_seconds": boundary_seconds,
            },
        )
        batches = [
            (self.rows(campaign_id, 100, start=0), "b2-wave-1", 0),
            (self.rows(campaign_id, 100, start=100), "b2-wave-2", 100),
            (self.rows(campaign_id, 100, start=200), "b2-wave-3", 200),
        ]
        result = self.gate_live("B2", campaign_id, batches, expected=expected, timeout=300)
        health_requests = 0
        health_errors: list[str] = []
        last_health = {}
        health_started = time.monotonic()
        deadline = started + boundary_seconds
        self._mark_phase(
            "b2_health_begin",
            progress={"campaign_id": campaign_id, "expected": expected},
        )
        if result.get("status") == "PASS":
            while time.monotonic() < deadline:
                try:
                    status = self.api("GET", "/api/vnext/status")
                    projection = status.get("projection") or {}
                    spool = status.get("spool") or {}
                    last_health = {
                        "started": bool(status.get("started")),
                        "pending_records": int(spool.get("pending_records", 0) or 0),
                        "projection_alive": bool(projection.get("alive", False)),
                        "writer_lag_seconds": float(projection.get("projection_lag_seconds", 0.0) or 0.0),
                    }
                    health_requests += 1
                except Exception as exc:
                    health_errors.append(type(exc).__name__)
                    break
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(1.0, remaining))
        samples = sampler.stop()
        elapsed = time.monotonic() - started
        health_elapsed = max(0.0, time.monotonic() - health_started)
        self._mark_phase(
            "b2_health_end",
            progress={
                "campaign_id": campaign_id,
                "health_requests": health_requests,
                "health_errors": len(health_errors),
            },
        )
        result.update({
            "gate": "B2",
            "boundary_seconds": boundary_seconds,
            "expected_synthetic_messages": expected,
            "health_activity_seconds": round(health_elapsed, 3),
            "health_requests": health_requests,
            "health_errors": health_errors,
            "last_health": last_health,
            "samples": samples,
            "duration_seconds": round(elapsed, 3),
        })
        if samples:
            result["peak_rss_bytes"] = max(item["process"]["rss_bytes"] for item in samples)
            result["peak_fd_count"] = max(item["process"]["fd_count"] for item in samples)
            result["peak_run_root_bytes"] = max(item["run_root_bytes"] for item in samples)
            result["max_writer_lag_seconds"] = max(float(item.get("writer_lag_seconds", 0.0) or 0.0) for item in samples)
        result["status"] = "PASS" if (
            result.get("status") == "PASS"
            and 40.0 <= elapsed <= 45.5
            and health_requests > 0
            and not health_errors
            and last_health.get("started") is True
            and last_health.get("projection_alive") is True
            and int(last_health.get("pending_records", 1)) == 0
            and samples
        ) else "FAIL"
        self._mark_phase(
            "b2_boundary_complete",
            progress={
                "campaign_id": campaign_id,
                "expected": expected,
                "health_requests": health_requests,
                "status": result["status"],
            },
        )
        return result

    def run(self) -> dict:
        self._mark_phase(
            "run_start",
            progress={"requested_gates": list(self.requested_gates)},
        )
        self._mark_phase(
            "native_modules_ready",
            progress={
                "faulthandler_enabled": bool(__import__("faulthandler").is_enabled()),
                "native_fault_capture": has_native_fault_capture("canary-root"),
            },
        )
        gates = self._gates()
        results: dict[str, dict] = {}
        try:
            self.setup_environment()
            self.seed_database()
            self.start_api()
            for name in gates:
                try:
                    if name.lower() == "r7":
                        result = self.gate_r7(self.args.stability_seconds)
                    else:
                        result = getattr(self, f"gate_{name.lower()}")()
                except Exception as exc:
                    result = {"gate": name.upper(), "status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}
                results[name.upper()] = result
                self.executed_gates.append(name)
                if result.get("status") != "PASS":
                    break
        finally:
            self._mark_phase("shutdown_begin", progress={"executed_gates": list(self.executed_gates)})
            try:
                self.stop_api()
                self.smsc.stop()
            finally:
                self._mark_phase("shutdown_end", progress={"executed_gates": list(self.executed_gates)})

        events = self.progress_events()
        by_event = {str(item.get("event_id")): item for item in events if item.get("event_id")}
        projection = self.query_projection()
        source_files = [
            self.run_root / "vnext" / "control-state.json",
            self.run_root / "vnext" / "projection" / "journal-offsets.json",
            self.run_root / "vnext" / "projection" / "projection-writer-status.json",
        ]
        hashes = {str(path.relative_to(self.run_root)): _sha256(path) for path in source_files if path.exists()}
        gate_contract_matches = self.executed_gates == self.requested_gates
        status = "PASS" if (
            results
            and gate_contract_matches
            and all(item.get("status") == "PASS" for item in results.values())
        ) else "FAIL"
        try:
            filesystem = subprocess.check_output(["df", "-T", "."], text=True, stderr=subprocess.STDOUT).strip()
        except (OSError, subprocess.CalledProcessError):
            filesystem = "UNAVAILABLE"
        try:
            freeze = subprocess.check_output(
                [sys.executable, "-m", "pip", "freeze"], text=True, stderr=subprocess.STDOUT,
            ).splitlines()
        except (OSError, subprocess.CalledProcessError):
            freeze = []
        return {
            "tool": "vnext_runtime_canary_acceptance",
            "status": status,
            "captured_at": time.time(),
            "duration_seconds": round(time.time() - self.started_at, 3),
            "run_root": str(self.run_root),
            "config": {
                "workers": self.args.workers,
                "partitions": self.args.partitions,
                "route_tps": self.args.route_tps,
                "projection_queue": self.args.projection_queue,
                "max_inflight": self.args.max_inflight,
                "prefetch": self.args.prefetch,
                "watchdog": getattr(self.args, "watchdog", "enabled"),
                "real_smsc": False,
                "loopback_host": "127.0.0.1",
                "b2_seconds": float(getattr(self.args, "b2_seconds", 42.0)),
                "b2_expected_synthetic_messages": 300,
            },
            "preflight": {
                "python": sys.version,
                "executable": sys.executable,
                "filesystem_df_T": filesystem,
                "dependency_freeze": sorted(freeze),
            },
            "requested_gates": [item.upper() for item in self.requested_gates],
            "executed_gates": [item.upper() for item in self.executed_gates],
            "gate_contract": {
                "requested_gates": [item.upper() for item in self.requested_gates],
                "executed_gates": [item.upper() for item in self.executed_gates],
                "matches": bool(gate_contract_matches),
                "authorize_p5500": bool(self.args.authorize_p5500),
            },
            "gates": results,
            "loopback": {
                "binds": self.smsc.smsc.binds,
                "submit_sm_received": len(self.smsc.smsc.received),
                "submit_sm_resp_success": self.smsc.smsc.response_success,
                "errors": self.smsc.smsc.errors,
            },
            "progress": {
                "unique_events": len(by_event),
                "states": {state: sum(1 for item in by_event.values() if item.get("state") == state)
                           for state in sorted({str(item.get("state")) for item in by_event.values()})},
                "taken_without_socket_written": sum(
                    1 for item in by_event.values()
                    if int(item.get("taken", 0)) and not int(item.get("socket_written", 0))
                ),
            },
            "projection_database": projection,
            "artifacts": {
                "sha256": hashes,
                "disk_bytes": _disk_bytes(self.run_root),
            },
            "diagnostics": {
                "native_fault_log": str(self.native_fault_path) if self.native_fault_path else "",
                "native_fault_logs": self._native_fault_logs(),
                "phase_markers": str(self._phase_marker_path),
                "processes": self._diagnostic_processes(),
            },
            "historical_status": "VNEXT_PIPELINE_READY_FOR_CANARY",
            "legacy_status": "INTEGRATED_NO_GO",
        }

    def _gates(self) -> list[str]:
        return list(self.requested_gates)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", default="")
    parser.add_argument("--gates", default="r0,r1,r3,r4,r5,r6")
    parser.add_argument("--authorize-p5500", action="store_true",
                        help="explicitly authorize the R2/P5500 canary gate")
    parser.add_argument("--stability-seconds", type=float, default=0.0)
    parser.add_argument("--b2-seconds", type=float, default=42.0)
    parser.add_argument("--idle-seconds", type=float, default=5.0)
    parser.add_argument("--route-tps", type=float, default=80.0)
    parser.add_argument("--response-delay-ms", type=float, default=2.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=64)
    parser.add_argument("--projection-queue", type=int, default=128)
    parser.add_argument("--max-inflight", type=int, default=32)
    parser.add_argument("--prefetch", type=int, default=16)
    parser.add_argument("--watchdog", choices=("enabled", "disabled"), default="enabled")
    args = parser.parse_args()
    run_root = Path(args.run_root).resolve()
    os.environ["PYTHONFAULTHANDLER"] = "1"
    native_fault_path = enable_native_fault_capture(run_root / "diagnostics", "canary-root")
    runner: CanaryRunner | None = None
    requested_gates: list[str] = []
    try:
        requested_gates = _validate_gate_contract(args)
    except (TypeError, ValueError):
        requested_gates = []
    try:
        runner = CanaryRunner(args)
        runner.native_fault_path = native_fault_path
        result = runner.run()
    except BaseException as exc:
        result = {
            "tool": "vnext_runtime_canary_acceptance",
            "status": "ERROR",
            "captured_at": time.time(),
            "run_root": str(run_root),
            "exception": f"{type(exc).__name__}: {exc}",
            "requested_gates": [item.upper() for item in requested_gates],
            "executed_gates": [item.upper() for item in (runner.executed_gates if runner else [])],
            "gate_contract": {
                "requested_gates": [item.upper() for item in requested_gates],
                "executed_gates": [item.upper() for item in (runner.executed_gates if runner else [])],
                "matches": False,
                "authorize_p5500": bool(args.authorize_p5500),
            },
        }
    output = Path(args.output).resolve() if args.output else run_root / "canary-result.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps({
            "status": result["status"],
            "run_root": result["run_root"],
            "requested_gates": result.get("requested_gates", []),
            "executed_gates": result.get("executed_gates", []),
            "gate_contract_matches": bool((result.get("gate_contract") or {}).get("matches", False)),
            "gates": {key: value.get("status") for key, value in result.get("gates", {}).items()},
            "projection": result.get("projection_database", {}),
        }, ensure_ascii=False, sort_keys=True))
    finally:
        disable_native_fault_capture("api-main")
        disable_native_fault_capture("canary-root")
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
