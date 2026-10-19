"""Bounded, loopback-only streaming acceptance harness for VNext.

This is a test-run artifact.  It reuses the frozen VNext ingress, spool,
worker, SMPP transport, Journal, and single ProjectionWriter implementation;
the only extra behavior here is a bounded multi-campaign feeder and metrics
collection for T3/T4.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import faulthandler
import json
import multiprocessing
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback


RESOURCE_PROFILE_RSS_KIB = {
    # LOW_MEM_2G retains the historical harness-inclusive guard for fault
    # injection.  Production profiles guard application-only RSS so the
    # synthetic feeder's bounded bookkeeping is reported separately.
    "LOW_MEM_2G": 2 * 1024 * 1024,
    "PROD_8G": 6 * 1024 * 1024,
    "PROD_16G": 12 * 1024 * 1024,
}

SUPPORTED_WORKER_COUNTS = (8, 12, 16, 24)
MAX_WORKERS = max(SUPPORTED_WORKER_COUNTS)
WORKER_JOIN_TIMEOUT_SECONDS = 120.0
LIFECYCLE_HEARTBEAT_SECONDS = 1.0
LIFECYCLE_STALL_DIAGNOSTIC_SECONDS = 15.0


def _validate_worker_count(value: object) -> int:
    """Validate the bounded worker matrix without silently changing it."""

    try:
        workers = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--workers must be one of 8, 12, 16, or 24"
        ) from exc
    if workers not in SUPPORTED_WORKER_COUNTS:
        choices = ", ".join(str(item) for item in SUPPORTED_WORKER_COUNTS)
        raise ValueError(f"--workers must be one of {choices}; received {workers}")
    return workers


def _parse_worker_count(value: object) -> int:
    """argparse adapter for the explicit worker matrix."""

    try:
        return _validate_worker_count(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _worker_bind_target(pool_size: int, worker_count: int) -> int:
    """Return each worker's local target while RouteCredit keeps the pool shared."""

    workers = max(1, int(worker_count))
    return max(1, (int(pool_size) + workers - 1) // workers)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _pid_start_time(pid: int | None = None) -> int | float | None:
    """Return a process start marker suitable for same-run PID correlation."""

    observed = int(pid or os.getpid())
    proc_stat = Path(f"/proc/{observed}/stat")
    try:
        text = proc_stat.read_text(encoding="ascii")
        closing = text.rfind(")")
        fields = text[closing + 2:].split()
        if len(fields) > 19:
            return int(fields[19])
    except (OSError, UnicodeError, ValueError, IndexError):
        pass
    try:
        import psutil  # type: ignore

        return float(psutil.Process(observed).create_time())
    except (ImportError, OSError, ValueError):
        return None


def _atomic_json_write(path: Path, value: dict) -> None:
    """Write bounded diagnostics without adding fsync to the hot path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _append_jsonl(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        handle.write("\n")
        handle.flush()


class _WorkerLifecycle:
    """Low-overhead worker heartbeat and bounded stall evidence.

    This is intentionally local to the acceptance harness.  It records phase
    transitions and a once-per-second heartbeat without changing spool,
    journal, bind, or response semantics.  A watchdog thread can still dump
    Python frames when the event loop is stuck in an await or executor call.
    """

    def __init__(self, worker_id: int, partitions: tuple[int, ...], root: str):
        self.worker_id = int(worker_id)
        self.partitions = tuple(int(item) for item in partitions)
        self.root = Path(root).resolve() if root else None
        self.path = self.root / f"worker-{self.worker_id}.json" if self.root else None
        self.stack_path = self.root / f"worker-{self.worker_id}-stacks.log" if self.root else None
        self.asyncio_path = self.root / f"worker-{self.worker_id}-asyncio.jsonl" if self.root else None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._loop = None
        self._stop_event = None
        self._feeder_done = None
        self._fault_handle = None
        self._watchdog: threading.Thread | None = None
        self._last_stall_dump = 0.0
        self._last_snapshot_progress: tuple | None = None
        self._state = {
            "schema_version": 1,
            "worker_id": self.worker_id,
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "pid_start_time": _pid_start_time(),
            "phase": "BOOT",
            "phase_entered_monotonic": time.monotonic(),
            "last_progress_monotonic": time.monotonic(),
            "loop_pass_count": 0,
            "stop_event_is_set": False,
            "feeder_done_is_set": False,
            "partitions": list(self.partitions),
            "local_processed": {},
            "local_inflight": 0,
            "global_pending": {"scope": "all_workers", "records": None},
            "active_bind_leases": 0,
            "response_pending": 0,
            "journal_durable_count": 0,
            "updated_utc": _utc_timestamp(),
            "updated_monotonic": time.monotonic(),
            "last_progress_detail": "BOOT",
        }
        if self.root:
            self.root.mkdir(parents=True, exist_ok=True)
            try:
                handle = self.stack_path.open("w", encoding="utf-8")
                faulthandler.enable(handle, all_threads=True)
                self._fault_handle = handle
                self._state["faulthandler_path"] = str(self.stack_path)
                self._state["faulthandler_enabled"] = bool(faulthandler.is_enabled())
            except (OSError, ValueError):
                self._state["faulthandler_path"] = ""
                self._state["faulthandler_enabled"] = False
            self._write()
            self._watchdog = threading.Thread(
                target=self._watchdog_loop,
                name=f"mc-vnext-lifecycle-{self.worker_id}",
                daemon=True,
            )
            self._watchdog.start()

    def attach_loop(self, loop) -> None:
        self._loop = loop

    def attach_events(self, *, stop_event, feeder_done) -> None:
        self._stop_event = stop_event
        self._feeder_done = feeder_done

    def _write(self) -> None:
        if self.path is None:
            return
        with self._lock:
            value = dict(self._state)
        try:
            _atomic_json_write(self.path, value)
        except (OSError, TypeError, ValueError):
            pass

    def update(self, phase: str | None = None, *, progress: bool = True, detail: str = "", **fields) -> None:
        now = time.monotonic()
        with self._lock:
            if phase and phase != self._state.get("phase"):
                self._state["phase"] = str(phase)
                self._state["phase_entered_monotonic"] = now
            if progress:
                self._state["last_progress_monotonic"] = now
                self._last_stall_dump = 0.0
            if detail:
                self._state["last_progress_detail"] = str(detail)
            self._state.update(fields)
            self._state["stop_event_is_set"] = bool(self._state.get("stop_event_is_set", False))
            self._state["updated_utc"] = _utc_timestamp()
            self._state["updated_monotonic"] = now
        if phase or progress or fields:
            self._write()

    def set_events(self, *, stop_event, feeder_done) -> None:
        self.update(
            progress=False,
            stop_event_is_set=bool(stop_event.is_set()),
            feeder_done_is_set=bool(feeder_done.is_set()),
        )

    def record_worker_snapshot(self, worker, *, global_pending: int | None = None) -> None:
        try:
            snapshot = worker.snapshot()
            counters = dict(snapshot.get("counters") or {})
            transports = list(snapshot.get("transports") or [])
            active_binds = sum(1 for item in transports if item.get("bind_slot") is not None)
            response_pending = sum(int(item.get("window_inflight", 0) or 0) for item in transports)
            snapshot_progress = (
                int(counters.get("task_taken", 0) or 0),
                int(counters.get("socket_written", 0) or 0),
                int(counters.get("response_success", 0) or 0),
                int(counters.get("write_failed", 0) or 0),
                int(snapshot.get("inflight", 0) or 0),
                response_pending,
            )
            changed = snapshot_progress != self._last_snapshot_progress
            self._last_snapshot_progress = snapshot_progress
            self.update(
                progress=changed,
                local_processed={
                    "task_taken": int(counters.get("task_taken", 0) or 0),
                    "socket_written": int(counters.get("socket_written", 0) or 0),
                    "response_success": int(counters.get("response_success", 0) or 0),
                    "write_failed": int(counters.get("write_failed", 0) or 0),
                },
                local_inflight=int(snapshot.get("inflight", 0) or 0),
                active_bind_leases=active_binds,
                response_pending=response_pending,
                journal_durable_count=int(counters.get("journal_events_durable", 0) or 0),
                **({"global_pending": {"scope": "all_workers", "records": int(global_pending)}}
                   if global_pending is not None else {}),
            )
        except Exception as exc:  # pragma: no cover - diagnostic path
            self.update(progress=False, detail=f"snapshot_error:{type(exc).__name__}:{exc}")

    def _capture_asyncio_tasks(self) -> None:
        if self.asyncio_path is None:
            return
        tasks = []
        try:
            loop = asyncio.get_running_loop()
            for task in sorted(asyncio.all_tasks(loop), key=lambda item: str(item.get_name())):
                frames = []
                for frame in task.get_stack(limit=12):
                    frames.append({
                        "file": str(frame.f_code.co_filename),
                        "line": int(frame.f_lineno),
                        "function": str(frame.f_code.co_name),
                    })
                tasks.append({
                    "name": str(task.get_name()),
                    "done": bool(task.done()),
                    "cancelled": bool(task.cancelled()),
                    "coro": repr(task.get_coro()),
                    "stack": frames,
                })
            _append_jsonl(self.asyncio_path, {
                "captured_utc": _utc_timestamp(),
                "captured_monotonic": time.monotonic(),
                "worker_id": self.worker_id,
                "tasks": tasks,
            })
        except (OSError, RuntimeError, TypeError, ValueError):
            pass

    def _dump_stall(self, phase: str, stalled_seconds: float) -> None:
        now = time.monotonic()
        if now - self._last_stall_dump < LIFECYCLE_STALL_DIAGNOSTIC_SECONDS:
            return
        self._last_stall_dump = now
        if self.stack_path is not None:
            try:
                with self.stack_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        f"\n--- lifecycle stall phase={phase} seconds={stalled_seconds:.3f} "
                        f"utc={_utc_timestamp()} ---\n"
                    )
                    faulthandler.dump_traceback(file=handle, all_threads=True)
                    frames = sys._current_frames()
                    for thread in threading.enumerate():
                        frame = frames.get(thread.ident)
                        handle.write(f"THREAD name={thread.name!r} ident={thread.ident!r} daemon={thread.daemon!r}\n")
                        if frame is not None:
                            handle.write("".join(traceback.format_stack(frame)))
            except (OSError, ValueError):
                pass
        loop = self._loop
        if loop is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(self._capture_asyncio_tasks)
            except RuntimeError:
                pass
        self.update(
            progress=False,
            detail=f"stall_diagnostic:{phase}:{stalled_seconds:.3f}s",
            last_stall_diagnostic_utc=_utc_timestamp(),
            last_stall_diagnostic_monotonic=now,
        )

    def _watchdog_loop(self) -> None:
        while not self._stop.wait(LIFECYCLE_HEARTBEAT_SECONDS):
            with self._lock:
                phase = str(self._state.get("phase") or "")
                last_progress = float(self._state.get("last_progress_monotonic") or 0.0)
                stalled = max(0.0, time.monotonic() - last_progress)
                if self._stop_event is not None:
                    self._state["stop_event_is_set"] = bool(self._stop_event.is_set())
                if self._feeder_done is not None:
                    self._state["feeder_done_is_set"] = bool(self._feeder_done.is_set())
                self._state["updated_utc"] = _utc_timestamp()
                self._state["updated_monotonic"] = time.monotonic()
            self._write()
            if phase and stalled >= LIFECYCLE_STALL_DIAGNOSTIC_SECONDS:
                self._dump_stall(phase, stalled)

    def close(self, *, phase: str = "PROCESS_EXIT") -> None:
        self.update(phase, progress=True)
        self._stop.set()
        if self._watchdog and self._watchdog is not threading.current_thread():
            self._watchdog.join(timeout=2.0)
        if self._fault_handle is not None:
            try:
                faulthandler.disable()
                self._fault_handle.flush()
                self._fault_handle.close()
            except (OSError, ValueError):
                pass
            self._fault_handle = None
        self._write()


def _partition_assignment_evidence(partition_map, worker_count: int) -> dict:
    """Summarize the immutable 256-partition assignment for run evidence."""

    worker_count = int(worker_count)
    assignments = partition_map.to_dict().get("assignments") or {}
    worker_partitions = [
        tuple(partition_map.partitions_for_worker(worker_id))
        for worker_id in range(worker_count)
    ]
    flattened = [partition_id for partitions in worker_partitions for partition_id in partitions]
    expected = set(range(int(partition_map.virtual_partition_count)))
    observed = set(flattened)
    owner_distribution = {
        str(worker_id): len(worker_partitions[worker_id])
        for worker_id in range(worker_count)
    }
    assignment_owner_distribution = {str(worker_id): 0 for worker_id in range(worker_count)}
    for assignment in assignments.values():
        owner = str(int(assignment.get("worker_id", -1)))
        assignment_owner_distribution[owner] = assignment_owner_distribution.get(owner, 0) + 1
    duplicate_count = len(flattened) - len(observed)
    return {
        "virtual_partition_count": int(partition_map.virtual_partition_count),
        "worker_count": worker_count,
        "owner_distribution": owner_distribution,
        "assignment_owner_distribution": assignment_owner_distribution,
        "assigned_partition_count": len(flattened),
        "unique_partition_count": len(observed),
        "missing_partition_count": len(expected - observed),
        "duplicate_partition_count": duplicate_count,
        "assignment_count": len(assignments),
        "valid": (
            len(assignments) == int(partition_map.virtual_partition_count)
            and observed == expected
            and duplicate_count == 0
            and owner_distribution == assignment_owner_distribution
        ),
        "worker_partitions": {
            str(worker_id): list(partitions)
            for worker_id, partitions in enumerate(worker_partitions)
        },
    }


def _install_source(source_root: str) -> None:
    root = Path(source_root).resolve()
    backend = root / "backend"
    if str(backend) not in os.sys.path:
        os.sys.path.insert(0, str(backend))


def _parse_counts(value: str) -> list[int]:
    counts = [int(item.strip()) for item in str(value).split(",") if item.strip()]
    if len(counts) < 4 or any(item <= 0 for item in counts):
        raise ValueError("--counts requires at least four positive integers")
    return counts


def _parse_ids(value: str) -> set[int]:
    if not value.strip():
        return set()
    return {int(item.strip()) for item in value.split(",") if item.strip()}


def _directory_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _journal_bytes(journal_paths: list[Path]) -> int:
    total = 0
    for path in journal_paths:
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def _parse_process_rows(output: str) -> dict[int, dict]:
    """Parse the numeric four-column ``ps`` form used on SLES and WSL.

    ``ps`` implementations differ in whitespace and header behavior.  The
    parser intentionally accepts extra trailing columns while requiring the
    first four fields to be numeric.  The raw output is retained by the
    sampler diagnostic writer, so malformed lines remain auditable.
    """

    rows: dict[int, dict] = {}
    for line in str(output or "").splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
            cpu, rss = float(parts[2]), int(parts[3])
        except (TypeError, ValueError):
            continue
        if pid <= 0 or ppid < 0 or rss < 0:
            continue
        rows[pid] = {
            "ppid": ppid,
            "cpu_percent": cpu,
            "rss_kib": rss,
            "state": "?",
        }
    return rows


def _select_process_tree(rows: dict[int, dict], roots: list[int]) -> set[int]:
    selected = {int(pid) for pid in roots if int(pid) > 0 and int(pid) in rows}
    changed = True
    while changed:
        changed = False
        for pid, row in rows.items():
            if pid not in selected and int(row.get("ppid", -1)) in selected:
                selected.add(pid)
                changed = True
    return selected


def _read_proc_rows() -> tuple[dict[int, dict], dict]:
    """Read Linux process rows without depending on a ps dialect or psutil."""

    proc_root = Path("/proc")
    if not proc_root.is_dir():
        raise RuntimeError("/proc is unavailable")
    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    rows: dict[int, dict] = {}
    status_checked = 0
    status_mismatches = 0
    read_errors = 0
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            stat_text = (entry / "stat").read_text(encoding="ascii")
            closing = stat_text.rfind(")")
            if closing < 0:
                raise ValueError("invalid /proc stat comm field")
            fields = stat_text[closing + 2:].split()
            # fields[0] is state, fields[1] ppid, fields[11]/[12] are utime/stime.
            if len(fields) < 13:
                raise ValueError("incomplete /proc stat")
            state = str(fields[0])
            ppid = int(fields[1])
            cpu_ticks = int(fields[11]) + int(fields[12])
            statm = (entry / "statm").read_text(encoding="ascii").split()
            if len(statm) < 2:
                raise ValueError("incomplete /proc statm")
            rss_kib = int(statm[1]) * page_size // 1024
            status_rss_kib = None
            try:
                status_text = (entry / "status").read_text(encoding="ascii")
                match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", status_text, re.MULTILINE)
                if match:
                    status_rss_kib = int(match.group(1))
                    status_checked += 1
                    if status_rss_kib != rss_kib:
                        status_mismatches += 1
            except (OSError, UnicodeError):
                pass
            rows[pid] = {
                "ppid": ppid,
                "cpu_ticks": cpu_ticks,
                "rss_kib": rss_kib,
                "status_rss_kib": status_rss_kib,
                "state": state,
            }
        except (OSError, UnicodeError, TypeError, ValueError):
            # A process can exit between /proc directory enumeration and the
            # individual reads.  It is a normal sampling race, not a zero RSS.
            read_errors += 1
            continue
    if not rows:
        raise RuntimeError("/proc contained no readable process rows")
    return rows, {
        "source": "proc",
        "status_rss_checked": status_checked,
        "status_rss_mismatches": status_mismatches,
        "read_errors": read_errors,
    }


def _read_proc_details(pids: list[int]) -> list[dict]:
    """Capture bounded OS wait evidence for a suspected stuck process tree."""

    details = []
    for raw_pid in sorted({int(pid) for pid in pids if int(pid) > 0}):
        root = Path(f"/proc/{raw_pid}")
        item = {"pid": raw_pid}
        try:
            stat_text = (root / "stat").read_text(encoding="ascii")
            closing = stat_text.rfind(")")
            fields = stat_text[closing + 2:].split()
            item.update({
                "state": str(fields[0]),
                "ppid": int(fields[1]),
                "cpu_ticks": int(fields[11]) + int(fields[12]),
                "pid_start_time": int(fields[19]),
            })
        except (OSError, UnicodeError, ValueError, IndexError):
            item["stat_error"] = True
        try:
            item["wchan"] = (root / "wchan").read_text(encoding="ascii").strip()
        except (OSError, UnicodeError):
            item["wchan"] = "NOT_CAPTURED"
        try:
            status = (root / "status").read_text(encoding="ascii")
            for line in status.splitlines():
                if line.startswith(("Name:", "State:", "Threads:", "VmRSS:")):
                    key, _, value = line.partition(":")
                    item[key.lower()] = value.strip()
        except (OSError, UnicodeError):
            item["status_error"] = True
        try:
            item["fd_count"] = len(list((root / "fd").iterdir()))
        except OSError:
            item["fd_count"] = None
        threads = []
        try:
            thread_root = root / "task"
            for thread_path in sorted(thread_root.iterdir(), key=lambda value: value.name)[:256]:
                thread_item = {"tid": int(thread_path.name)}
                try:
                    thread_stat = (thread_path / "stat").read_text(encoding="ascii")
                    closing = thread_stat.rfind(")")
                    fields = thread_stat[closing + 2:].split()
                    thread_item.update({
                        "state": str(fields[0]),
                        "cpu_ticks": int(fields[11]) + int(fields[12]),
                    })
                except (OSError, UnicodeError, ValueError, IndexError):
                    thread_item["stat_error"] = True
                try:
                    thread_item["wchan"] = (thread_path / "wchan").read_text(encoding="ascii").strip()
                except (OSError, UnicodeError):
                    thread_item["wchan"] = "NOT_CAPTURED"
                threads.append(thread_item)
        except OSError:
            pass
        item["threads"] = threads
        details.append(item)
    return details


def _bind_lease_evidence(root: Path) -> list[dict]:
    """Read bind lease metadata while redacting the lease token."""

    values = []
    if not root.exists():
        return values
    for path in sorted(root.glob("route-*-bind-*.lease")):
        value = {"path": str(path), "exists": True}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            value.update({
                "route_id": str(payload.get("route_id", "")),
                "slot": int(payload.get("slot", 0) or 0),
                "pid": int(payload.get("pid", 0) or 0),
                "created_at": float(payload.get("created_at", 0.0) or 0.0),
                "age_seconds": round(max(0.0, time.time() - float(payload.get("created_at", time.time()))), 3),
            })
        except (OSError, ValueError, TypeError):
            value["read_error"] = True
        values.append(value)
    return values


def _parent_lifecycle_event(path: Path | None, event: str, **fields) -> None:
    if path is None:
        return
    payload = {
        "schema_version": 1,
        "event": str(event),
        "utc": _utc_timestamp(),
        "monotonic": time.monotonic(),
        "pid": os.getpid(),
        "ppid": os.getppid(),
    }
    payload.update(fields)
    try:
        _append_jsonl(path, payload)
    except (OSError, TypeError, ValueError):
        pass


def _capture_join_forensics(
    *, run_root: Path, lifecycle_root: Path, credits_root: Path,
    processes: list[multiprocessing.Process], reason: str,
) -> Path | None:
    """Persist same-run child state, OS wait data, and bind leases."""

    pids = [int(process.pid or 0) for process in processes if process.is_alive()]
    states = []
    for process in processes:
        path = lifecycle_root / f"worker-{processes.index(process)}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            value = {"path": str(path), "state_missing": True}
        value.update({
            "worker_id": processes.index(process),
            "parent_pid": os.getpid(),
            "exitcode_at_capture": process.exitcode,
            "alive_at_capture": bool(process.is_alive()),
        })
        states.append(value)
    evidence = {
        "schema_version": 1,
        "captured_utc": _utc_timestamp(),
        "captured_monotonic": time.monotonic(),
        "reason": str(reason),
        "run_root": str(run_root),
        "processes": states,
        "proc_details": _read_proc_details(pids),
        "bind_leases": _bind_lease_evidence(credits_root),
    }
    path = lifecycle_root / "join-timeout-forensics.json"
    try:
        _atomic_json_write(path, evidence)
        return path
    except (OSError, TypeError, ValueError):
        return None


def _read_ps_rows(pids: list[int] | None = None) -> tuple[dict[int, dict], dict]:
    command = ["ps", "-o", "pid=,ppid=,pcpu=,rss="]
    if pids:
        command.extend(["-p", ",".join(str(pid) for pid in sorted(set(pids)) if int(pid) > 0)])
    try:
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        return {}, {
            "source": "ps",
            "command": command,
            "returncode": 127,
            "stderr": f"{type(exc).__name__}: {exc}",
            "stdout": "",
            "error": f"ps unavailable: {type(exc).__name__}: {exc}",
        }
    rows = _parse_process_rows(completed.stdout)
    error = None
    if completed.returncode != 0:
        error = f"ps exited {completed.returncode}"
    elif not rows:
        error = "ps returned no parseable rows"
    return rows, {
        "source": "ps",
        "command": command,
        "returncode": int(completed.returncode),
        "stderr": str(completed.stderr or "")[-2048:],
        "stdout": str(completed.stdout or ""),
        "error": error,
    }


def _read_psutil_rows(roots: list[int] | None = None) -> tuple[dict[int, dict], dict]:
    try:
        import psutil  # type: ignore
    except ImportError as exc:
        raise RuntimeError("psutil is unavailable") from exc
    rows: dict[int, dict] = {}
    errors = 0
    processes = []
    if roots:
        seen: set[int] = set()
        for root in roots:
            try:
                process = psutil.Process(int(root))
                candidates = [process, *process.children(recursive=True)]
            except (psutil.Error, OSError, ValueError):
                continue
            for candidate in candidates:
                if int(candidate.pid) not in seen:
                    seen.add(int(candidate.pid))
                    processes.append(candidate)
    else:
        processes = list(psutil.process_iter(["pid", "ppid", "cpu_percent", "memory_info"]))
    for process in processes:
        try:
            info = process.as_dict(attrs=["pid", "ppid", "cpu_percent", "memory_info"])
            memory = info.get("memory_info")
            rows[int(info["pid"])] = {
                "ppid": int(info.get("ppid") or 0),
                "cpu_percent": float(info.get("cpu_percent") or 0.0),
                "rss_kib": int(getattr(memory, "rss", 0) or 0) // 1024,
                "state": "?",
            }
        except (OSError, psutil.Error, TypeError, ValueError, KeyError):
            errors += 1
    if not rows:
        raise RuntimeError("psutil contained no readable process rows")
    return rows, {"source": "psutil", "read_errors": errors}


def _append_sampler_diagnostic(path: Path | None, value: dict) -> None:
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError:
        # A diagnostic write must never change the acceptance path.
        pass


def _process_tree_metrics(
    roots: list[int], *, sampler_state: dict | None = None, label: str = "",
    diagnostic_path: Path | None = None,
) -> dict:
    """Collect valid process-tree metrics or explicitly report NOT_CAPTURED.

    Linux ``/proc`` is primary because it works on SLES and WSL without adding
    a dependency.  ``ps`` is retained as a targeted cross-check and fallback;
    optional psutil is used only on platforms without ``/proc`` and without a
    usable ps command.  No failure is converted into a numeric zero.
    """

    roots = [int(pid) for pid in roots if int(pid) > 0]
    state = sampler_state if sampler_state is not None else {}
    sampler_label = str(label or "default")
    primary_rows: dict[int, dict] = {}
    primary_diag: dict = {}
    source = ""
    errors: list[str] = []
    try:
        primary_rows, primary_diag = _read_proc_rows()
        source = "proc"
    except Exception as exc:
        errors.append(f"proc: {type(exc).__name__}: {exc}")
        try:
            primary_rows, primary_diag = _read_psutil_rows(roots)
            source = "psutil"
        except Exception as psutil_exc:
            errors.append(f"psutil: {type(psutil_exc).__name__}: {psutil_exc}")
            primary_rows, primary_diag = _read_ps_rows(None)
            if primary_rows:
                source = "ps"
            else:
                errors.append(f"ps: {primary_diag.get('error') or 'no rows'}")

    selected = _select_process_tree(primary_rows, roots)
    selected_rows = {
        pid: row for pid, row in primary_rows.items()
        if pid in selected and str(row.get("state", "")).upper() not in {"Z", "ZOMBIE", "DEAD"}
    }
    ps_rows: dict[int, dict] = {}
    ps_diag: dict = {}
    if source == "proc":
        if selected:
            ps_rows, ps_diag = _read_ps_rows(sorted(selected))
        else:
            ps_diag = {
                "source": "ps",
                "error": "no selected process ids for targeted cross-check",
            }
        if ps_diag.get("error"):
            ps_diag["cross_check_error"] = ps_diag["error"]
        for pid, row in selected_rows.items():
            ps_row = ps_rows.get(pid)
            if ps_row is not None:
                row["ps_rss_kib"] = int(ps_row.get("rss_kib", 0) or 0)
                row["ps_cpu_percent"] = float(ps_row.get("cpu_percent", 0.0) or 0.0)
        primary_diag = {**primary_diag, "ps_cross_check": ps_diag}

    sampled_at = time.time()
    now_monotonic = time.monotonic()
    cpu_total = 0.0
    cpu_valid = False
    rss_total = 0
    cpu_state = state.setdefault(sampler_label, {})
    for pid, row in selected_rows.items():
        rss_total += int(row.get("rss_kib", 0) or 0)
        cpu_value = row.get("cpu_percent")
        if cpu_value is None and row.get("cpu_ticks") is not None:
            previous = cpu_state.get(pid)
            current_ticks = int(row.get("cpu_ticks") or 0)
            cpu_state[pid] = (current_ticks, now_monotonic)
            if previous:
                previous_ticks, previous_at = previous
                elapsed = now_monotonic - float(previous_at)
                if elapsed > 0:
                    clock_ticks = float(os.sysconf("SC_CLK_TCK"))
                    cpu_value = ((current_ticks - int(previous_ticks)) / clock_ticks) / elapsed * 100.0
            if cpu_value is None:
                cpu_value = 0.0
        if cpu_value is not None:
            cpu_total += float(cpu_value)
            cpu_valid = True

    sampler_ok = bool(selected_rows)
    sampler_error = None if sampler_ok else "; ".join(errors) or "no live process rows for roots"
    diagnostic = {
        "sampled_at_epoch": sampled_at,
        "label": sampler_label,
        "roots": roots,
        "selected_pids": sorted(selected_rows),
        "source": source or "none",
        "sampler_ok": sampler_ok,
        "sampler_error": sampler_error,
        "primary": primary_diag,
    }
    _append_sampler_diagnostic(diagnostic_path, diagnostic)
    return {
        "sampler_ok": sampler_ok,
        "sampler_error": sampler_error,
        "sampled_pid_count": len(selected_rows),
        "sampled_pids": sorted(selected_rows),
        "sampling_source": source or "none",
        "sampled_at_epoch": sampled_at,
        "cpu_percent": round(cpu_total, 3) if cpu_valid else None,
        "rss_kib": int(rss_total) if sampler_ok else None,
        "diagnostic_path": str(diagnostic_path) if diagnostic_path else None,
        "diagnostic": diagnostic,
    }


def _load_durable_offsets(projection) -> tuple[dict[str, int] | None, str | None]:
    path = getattr(projection, "offsets_path", None)
    if path is None:
        return None, "projection offsets path unavailable"
    path = Path(path)
    try:
        if not path.exists():
            return None, "durable offsets file does not exist"
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            return None, "durable offsets file is not an object"
        return {str(key): max(0, int(item)) for key, item in value.items()}, None
    except (OSError, TypeError, ValueError) as exc:
        return None, f"durable offsets unreadable: {type(exc).__name__}: {exc}"


def _projection_completion_evidence(
    projection, journal_paths: list[Path], *, max_status_age_seconds: float = 2.0,
) -> dict:
    """Return durable completion evidence, including stale-status diagnostics.

    The offset file is fsynced after the ProjectionWriter DB transaction.  It
    is therefore a stronger completion signal than a potentially old status
    JSON.  The status remains useful for queue/backlog diagnostics and is
    reported with its age rather than silently trusted.
    """

    try:
        status = projection.snapshot()
    except Exception as exc:
        status = {}
        status_error = f"snapshot: {type(exc).__name__}: {exc}"
    else:
        status_error = str(status.get("snapshot_error") or "") or None
    status_path = getattr(projection, "status_path", None)
    status_mtime_epoch = None
    status_age_ms = None
    if status_path is not None:
        try:
            status_mtime_epoch = float(Path(status_path).stat().st_mtime)
            status_age_ms = round(max(0.0, time.time() - status_mtime_epoch) * 1000.0, 3)
        except OSError:
            status_error = status_error or "status file is unavailable"

    offsets, offsets_error = _load_durable_offsets(projection)
    durable = None
    if offsets is not None:
        try:
            from vnext.projection_writer import ProgressJournal

            total_bytes = committed_bytes = backlog_bytes = 0
            backlog_events = 0
            for journal_path in journal_paths:
                storage = ProgressJournal(journal_path).storage_snapshot(offsets)
                total_bytes += int(storage.get("total_bytes", 0) or 0)
                committed_bytes += int(storage.get("committed_bytes", 0) or 0)
                backlog_bytes += int(storage.get("backlog_bytes", 0) or 0)
                backlog_events += int(storage.get("backlog_events_estimate", 0) or 0)
            durable = {
                "total_bytes": total_bytes,
                "committed_bytes": committed_bytes,
                "backlog_bytes": backlog_bytes,
                "backlog_events_estimate": backlog_events,
                "offsets_file": str(Path(getattr(projection, "offsets_path"))),
            }
        except Exception as exc:
            offsets_error = f"durable journal check failed: {type(exc).__name__}: {exc}"

    status_committed = status.get("journal_committed_offset_bytes")
    try:
        status_committed = int(status_committed) if status_committed is not None else None
    except (TypeError, ValueError):
        status_committed = None
    status_backlog = int(status.get("journal_backlog_bytes", 0) or 0)
    status_pending = int(status.get("journal_pending_events", 0) or 0)
    status_queue = status.get("queue_depth")
    try:
        status_queue = int(status_queue) if status_queue is not None else None
    except (TypeError, ValueError):
        status_queue = None
    live_journal_bytes = _journal_bytes(journal_paths)
    status_complete = (
        status_committed is not None
        and live_journal_bytes <= status_committed
        and status_backlog <= 0
        and int(status.get("journal_backlog_events_estimate", 0) or 0) <= 0
        and status_pending <= 0
        and (status_queue is None or status_queue <= 0)
    )
    durable_complete = bool(durable is not None and durable["backlog_bytes"] <= 0)
    status_fresh = bool(
        (status_age_ms is not None and status_age_ms <= max_status_age_seconds * 1000.0)
        or (status_path is None and status_error is None)
    )
    complete = durable_complete or (durable is None and status_complete and status_fresh)
    if complete:
        state = "COMPLETED"
    elif status_error or offsets_error or not status_fresh:
        state = "STALE_STATUS"
    else:
        state = "INCOMPLETE"
    durable_offset_mtime_epoch = None
    offset_path = getattr(projection, "offsets_path", None)
    if offset_path is not None:
        try:
            durable_offset_mtime_epoch = float(Path(offset_path).stat().st_mtime)
        except OSError:
            durable_offset_mtime_epoch = None
    return {
        "state": state,
        "complete": complete,
        "status_age_ms": status_age_ms,
        "status_fresh": status_fresh,
        "status_error": status_error,
        "offsets_error": offsets_error,
        "status_committed_offset_bytes": status_committed,
        "status_backlog_bytes": status_backlog,
        "status_pending_events": status_pending,
        "status_queue_depth": status_queue,
        "live_journal_bytes": live_journal_bytes,
        "durable": durable,
        "durable_offset_mtime_epoch": durable_offset_mtime_epoch,
    }


def _projection_caught_up(projection, journal_paths: list[Path]) -> bool:
    """Require durable journal EOF before stopping, even if status is stale."""

    return bool(_projection_completion_evidence(projection, journal_paths).get("complete"))


def _destination_campaign(destination: str) -> int | None:
    # The feeder uses 25190{campaign:02d}{index:07d} synthetic destinations.
    try:
        if destination.startswith("25190") and len(destination) >= 7:
            return int(destination[5:7])
    except ValueError:
        pass
    return None


class CountingLoopbackSMSC:
    """Minimal loopback SMPP server with bounded event memory."""

    def __init__(self, smpp, *, max_destination_keys: int = 100_000,
                 response_delay_seconds: float = 0.0):
        self._smpp = smpp
        self.server = None
        self.total_received = 0
        self.responses = 0
        self.response_success = 0
        self.binds = 0
        self.active_binds = 0
        self.max_active_binds = 0
        self.errors: list[str] = []
        self.destination_counts: Counter[str] = Counter()
        self.campaign_pdus: Counter[int] = Counter()
        self.max_destination_keys = max(1, int(max_destination_keys))
        self.destination_tracking_truncated = False
        self.response_delay_seconds = max(0.0, float(response_delay_seconds))
        self.started_monotonic: float | None = None
        self.first_received_monotonic: float | None = None
        self.last_received_monotonic: float | None = None
        self.first_response_monotonic: float | None = None
        self.last_response_monotonic: float | None = None
        self._connection_stats: dict[int, dict] = {}

    async def _handle(self, reader, writer):
        bound = False
        connection_id = None
        try:
            while True:
                command, _status, sequence, body = await self._smpp.read_pdu(reader)
                if command in {
                    self._smpp.BIND_RECEIVER,
                    self._smpp.BIND_TRANSMITTER,
                    self._smpp.BIND_TRANSCEIVER,
                }:
                    if not bound:
                        bound = True
                        self.binds += 1
                        connection_id = self.binds
                        self._connection_stats[connection_id] = {
                            "connection_id": connection_id,
                            "received": 0,
                            "responses": 0,
                            "bound_at_monotonic": time.monotonic(),
                            "first_received_monotonic": None,
                            "last_received_monotonic": None,
                            "first_response_monotonic": None,
                            "last_response_monotonic": None,
                            "closed_at_monotonic": None,
                        }
                        self.active_binds += 1
                        self.max_active_binds = max(self.max_active_binds, self.active_binds)
                    response = self._smpp.cstring("VNEXT-STREAM-SMSC")
                    writer.write(
                        self._smpp.HEADER.pack(
                            16 + len(response), command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                        + response
                    )
                    await writer.drain()
                elif command == self._smpp.SUBMIT_SM:
                    destination = self._smpp.parse_destination(body)
                    self.total_received += 1
                    received_at = time.monotonic()
                    if self.first_received_monotonic is None:
                        self.first_received_monotonic = received_at
                    self.last_received_monotonic = received_at
                    if connection_id is not None:
                        stats = self._connection_stats[connection_id]
                        stats["received"] += 1
                        stats["first_received_monotonic"] = (
                            stats["first_received_monotonic"] or received_at
                        )
                        stats["last_received_monotonic"] = received_at
                    if (
                        destination in self.destination_counts
                        or len(self.destination_counts) < self.max_destination_keys
                    ):
                        self.destination_counts[destination] += 1
                    else:
                        self.destination_tracking_truncated = True
                    campaign = _destination_campaign(destination)
                    if campaign is not None:
                        self.campaign_pdus[campaign] += 1
                    if self.response_delay_seconds:
                        await asyncio.sleep(self.response_delay_seconds)
                    response = self._smpp.cstring(f"VNEXT-{self.total_received:012d}")
                    writer.write(
                        self._smpp.HEADER.pack(
                            16 + len(response), command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                        + response
                    )
                    await writer.drain()
                    self.responses += 1
                    self.response_success += 1
                    responded_at = time.monotonic()
                    if self.first_response_monotonic is None:
                        self.first_response_monotonic = responded_at
                    self.last_response_monotonic = responded_at
                    if connection_id is not None:
                        stats = self._connection_stats[connection_id]
                        stats["responses"] += 1
                        stats["first_response_monotonic"] = (
                            stats["first_response_monotonic"] or responded_at
                        )
                        stats["last_response_monotonic"] = responded_at
                elif command == self._smpp.ENQUIRE_LINK:
                    writer.write(
                        self._smpp.HEADER.pack(
                            16, command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                    )
                    await writer.drain()
                elif command == self._smpp.UNBIND:
                    writer.write(
                        self._smpp.HEADER.pack(
                            16, command | self._smpp.RESPONSE_MASK, 0, sequence
                        )
                    )
                    await writer.drain()
                    break
                else:
                    self.errors.append(f"unsupported command=0x{command:08x}")
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except Exception as exc:  # pragma: no cover - forensic path
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            if bound:
                self.active_binds = max(0, self.active_binds - 1)
                if connection_id is not None:
                    self._connection_stats[connection_id]["closed_at_monotonic"] = time.monotonic()
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def start(self) -> int:
        self.started_monotonic = time.monotonic()
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return int(self.server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    @staticmethod
    def _rate(count: int, start: float | None, end: float | None) -> float | None:
        if start is None or end is None or end <= start:
            return None
        return round(float(count) / max(0.001, end - start), 3)

    def snapshot(self) -> dict:
        return {
            "received": int(self.total_received),
            "responses": int(self.responses),
            "response_success": int(self.response_success),
            "received_pdu_per_second": self._rate(
                self.total_received, self.first_received_monotonic, self.last_received_monotonic
            ),
            "response_pdu_per_second": self._rate(
                self.responses, self.first_response_monotonic, self.last_response_monotonic
            ),
            "response_delay_seconds": self.response_delay_seconds,
            "binds": int(self.binds),
            "active_binds": int(self.active_binds),
            "max_active_binds": int(self.max_active_binds),
            "per_bind": [
                {
                    **stats,
                    "active_seconds": (
                        round(
                            max(0.0, float(stats["closed_at_monotonic"] or time.monotonic())
                                  - float(stats["bound_at_monotonic"])),
                            6,
                        )
                        if stats.get("bound_at_monotonic") is not None else None
                    ),
                    "received_pdu_per_second": self._rate(
                        int(stats["received"]),
                        stats.get("first_received_monotonic"),
                        stats.get("last_received_monotonic"),
                    ),
                    "response_pdu_per_second": self._rate(
                        int(stats["responses"]),
                        stats.get("first_response_monotonic"),
                        stats.get("last_response_monotonic"),
                    ),
                }
                for stats in sorted(self._connection_stats.values(), key=lambda item: item["connection_id"])
            ],
        }


async def _drain_transport_responses(worker, *, timeout: float = 60.0) -> None:
    """Wait for every written PDU's response before releasing bind leases.

    ``ShardWorker.run_until_idle`` can finish as soon as socket writes have
    been queued, while the SMPP reader task is still consuming responses.
    Closing the transport at that point clears its pending response window and
    makes a high-rate loopback run look short on ``submit_sm_resp`` events.
    The streaming harness must drain that transport-level window before it
    releases a physical bind slot for another worker.
    """

    result = await worker.drain_transports(timeout=timeout)
    if not result.get("drained"):
        raise TimeoutError(
            f"SMPP response drain exceeded {timeout:.1f}s; "
            f"pending={int(result.get('pending', 0) or 0)}"
        )


def _release_completed_worker_caches(spool, worker) -> None:
    """Release reader-only caches after a completed worker pass.

    Ingress owns the authoritative spool index.  A worker only needs records
    for its current bounded pass; retaining every decoded record and every
    completed destination lock in each spawned worker multiplies memory by
    the worker count during a long steady run.
    """

    release = getattr(worker, "release_reader_caches", None)
    if release:
        release()
        return
    event_index = getattr(spool, "_event_index", None)
    if isinstance(event_index, dict):
        event_index.clear()
    number_locks = getattr(worker, "_number_locks", None)
    if isinstance(number_locks, dict):
        number_locks.clear()


def _worker_process(
    source_root: str,
    worker_id: int,
    partitions: tuple[int, ...],
    spool_root: str,
    credits_root: str,
    route_config: dict,
    journal_path: str,
    result_path: str,
    worker_count: int,
    feeder_done,
    stop_event,
    profile_root: str = "",
    profile_interval_seconds: float = 1.0,
    lifecycle_root: str = "",
    progress_mode: str = "decoupled",
) -> None:
    lifecycle = _WorkerLifecycle(worker_id, tuple(partitions), lifecycle_root)
    lifecycle.attach_events(stop_event=stop_event, feeder_done=feeder_done)
    worker = None
    profiler = None
    progress_store = None
    result = None
    try:
        lifecycle.update(
            "BOOT",
            detail="worker_process_started",
            stop_event_is_set=bool(stop_event.is_set()),
            feeder_done_is_set=bool(feeder_done.is_set()),
        )
        _install_source(source_root)
        from vnext.critical_path_profiler import CriticalPathProfiler
        from vnext.projection_writer import ProgressJournal
        from vnext.route_credits import RouteCreditBroker
        from vnext.spool import TaskSpool
        from vnext.worker import ShardWorker

        lifecycle.update("SPOOL_INIT", detail="imports_ready")
        spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=512 * 1024 * 1024)
        credits = RouteCreditBroker(credits_root)
        profiler = CriticalPathProfiler(
            profile_root,
            f"worker-{int(worker_id)}",
            interval_seconds=profile_interval_seconds,
        )
        journal = ProgressJournal(journal_path, profiler=profiler)
        progress_store = None
        if progress_mode == "decoupled":
            try:
                from vnext.progress import ProgressPersistence

                progress_store = ProgressPersistence(
                    journal,
                    name=f"worker-{int(worker_id)}",
                    max_pending_items=131_072,
                    max_pending_bytes=64 * 1024 * 1024,
                    batch_size=256,
                    flush_interval_seconds=0.05,
                    aggregate_path=(
                        Path(journal_path).with_name(
                            f"{Path(journal_path).stem}.progress-aggregate.json"
                        )
                    ),
                    profiler=profiler,
                )
                progress_store.start()
            except (ImportError, TypeError, AttributeError):
                # The harness can still run against an older source checkout.
                progress_store = None
        worker_route_config = dict(route_config)
        worker_route_config["_worker_bind_target"] = _worker_bind_target(
            route_config.get("pool_size", 1), worker_count
        )
        try:
            worker = ShardWorker(
                worker_id,
                tuple(partitions),
                spool,
                {str(route_config["route_id"]): worker_route_config},
                credits=credits,
                progress_sink=journal.append if progress_store is None else None,
                progress_store=progress_store,
                max_inflight=128,
                prefetch_per_partition=64,
                profiler=profiler,
            )
        except TypeError as exc:
            # Old baseline source: no progress_store keyword.
            if "progress_store" not in str(exc):
                raise
            if progress_store is not None:
                progress_store.close(timeout=5.0)
                progress_store = None
            worker = ShardWorker(
                worker_id,
                tuple(partitions),
                spool,
                {str(route_config["route_id"]): worker_route_config},
                credits=credits,
                progress_sink=journal.append,
                max_inflight=128,
                prefetch_per_partition=64,
                profiler=profiler,
            )
        lifecycle.update("SPOOL_INIT", detail="worker_ready", local_inflight=0)
        last_transport_snapshots: list[dict] = []

        async def loop() -> dict:
            nonlocal last_transport_snapshots
            lifecycle.attach_loop(asyncio.get_running_loop())
            loop_pass_count = 0
            stop_requested = False
            while True:
                loop_pass_count += 1
                if stop_event.is_set() and not stop_requested:
                    # The parent sets this only after producer, SMPP response,
                    # Journal EOF, and Projection catch-up are complete.  Wire
                    # the external lifecycle signal into the worker's own
                    # cancellation boundary so a stale lightweight spool
                    # observation cannot make run_until_idle spin forever.
                    worker.stop()
                    stop_requested = True
                    lifecycle.update("STOP_EVENT_OBSERVED", progress=False, detail="worker_stop_requested")
                lifecycle.update(
                    "RUN_UNTIL_IDLE_ENTER",
                    progress=False,
                    detail=f"pass={loop_pass_count}",
                    loop_pass_count=loop_pass_count,
                    stop_event_is_set=bool(stop_event.is_set()),
                    feeder_done_is_set=bool(feeder_done.is_set()),
                )
                lifecycle.record_worker_snapshot(worker)
                # Each pass may become idle while another worker still owns
                # the bounded physical bind pool.  Release the leases at the
                # pass boundary so a worker with newly admitted partitions
                # cannot be starved behind an idle worker.
                await worker.run_until_idle(close_transports=False)
                lifecycle.update("RUN_UNTIL_IDLE_EXIT", progress=False, detail=f"pass={loop_pass_count}")
                lifecycle.record_worker_snapshot(worker)
                lifecycle.update("DRAIN_RESPONSES_ENTER", progress=False)
                await _drain_transport_responses(worker)
                lifecycle.update("DRAIN_RESPONSES_EXIT", progress=False)
                lifecycle.record_worker_snapshot(worker)
                last_transport_snapshots = [
                    dict(item) for item in worker.snapshot().get("transports", [])
                ]
                lifecycle.update("CLOSE_TRANSPORT_ENTER", progress=False)
                await worker.close_transports()
                lifecycle.update("CLOSE_TRANSPORT_EXIT", progress=False)
                if progress_store is not None:
                    progress_drain = await worker.drain_progress(timeout=30.0)
                    lifecycle.update(
                        "PROGRESS_DRAIN_EXIT",
                        progress=False,
                        pending_events=int(
                            progress_drain.get("pending_events", 0) or 0
                        ),
                    )
                _release_completed_worker_caches(spool, worker)
                lifecycle.update("SPOOL_SNAPSHOT_ENTER", progress=False)
                # A final shutdown check must reload all cursor/taken files.
                # The lightweight path is deliberately retained for the hot
                # feeder loop, but it is not authoritative at the lifecycle
                # boundary when another process may have just atomically
                # replaced a cursor file.
                status = spool.status() if stop_event.is_set() else spool.lightweight_snapshot()
                lifecycle.update(
                    "SPOOL_SNAPSHOT_EXIT",
                    progress=False,
                    pending_records=int(status.get("pending_records", 0) or 0),
                    global_pending={
                        "scope": "all_workers",
                        "records": int(status.get("pending_records", 0) or 0),
                    },
                    pending_by_partition={
                        str(key): int(value)
                        for key, value in (status.get("pending_by_partition") or {}).items()
                        if int(value or 0) != 0
                    },
                )
                lifecycle.set_events(stop_event=stop_event, feeder_done=feeder_done)
                if stop_event.is_set():
                    lifecycle.update("STOP_EVENT_OBSERVED", progress=False, detail="parent_stop_event_set")
                if stop_event.is_set() and int(status.get("pending_records", 0) or 0) == 0:
                    break
                if stop_requested and int(status.get("pending_records", 0) or 0) > 0:
                    lifecycle.update(
                        "SHUTDOWN_BLOCKED",
                        progress=False,
                        detail="durable spool still has pending records after stop",
                    )
                await asyncio.sleep(0.02)
            lifecycle.update("FINAL_SNAPSHOT", detail="all_worker_work_and_responses_drained")
            result = worker.snapshot()
            result["transports"] = last_transport_snapshots
            if progress_store is not None:
                result["progress_final"] = progress_store.snapshot()
                await worker.close_progress(timeout=30.0)
            lifecycle.record_worker_snapshot(worker)
            return result

        result = asyncio.run(loop())
        lifecycle.update("ASYNCIO_RUN_RETURNED")
    except Exception as exc:  # pragma: no cover - exercised by integration runs
        lifecycle.update(
            "WORKER_EXCEPTION",
            detail=f"{type(exc).__name__}:{exc}",
            worker_exception=f"{type(exc).__name__}: {exc}",
        )
        result = {
            "worker_id": int(worker_id),
            "partitions": list(partitions),
            "process_pid": os.getpid(),
            "process_role": "stream-worker",
            "worker_exception": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if progress_store is not None:
            try:
                progress_store.close(timeout=30.0)
            except Exception:
                pass
        if profiler is not None:
            profiler.publish(force=True)
            if result is not None and profiler.enabled:
                result["critical_path"] = profiler.snapshot()
        if result is not None:
            result["process_pid"] = os.getpid()
            result["process_role"] = "stream-worker"
            try:
                lifecycle.update("RESULT_FILE_WRITING", detail="atomic_result_write_start")
                Path(result_path).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
                lifecycle.update("RESULT_FILE_WRITTEN", detail="result_file_write_complete")
            except (OSError, TypeError, ValueError) as exc:
                lifecycle.update(
                    "RESULT_FILE_WRITE_FAILED",
                    detail=f"{type(exc).__name__}:{exc}",
                    result_file_error=f"{type(exc).__name__}: {exc}",
                )
        lifecycle.close()


def _sample(
    *, run_root: Path, process_roots: list[int], application_roots: list[int] | None = None,
    process_roles: dict | None = None, spool, projection, smsc, started: float,
    profile_root: Path | None = None, sampler_state: dict | None = None,
) -> dict:
    application_roots = list(application_roots or process_roots)
    diagnostic_path = run_root / "resource-sampler-diagnostics.jsonl"
    harness_metrics = _process_tree_metrics(
        process_roots,
        sampler_state=sampler_state,
        label="harness",
        diagnostic_path=diagnostic_path,
    )
    application_metrics = _process_tree_metrics(
        application_roots,
        sampler_state=sampler_state,
        label="application",
        diagnostic_path=diagnostic_path,
    )
    sampler_ok = bool(harness_metrics["sampler_ok"] and application_metrics["sampler_ok"])
    sampler_errors = [
        str(item["sampler_error"])
        for item in (harness_metrics, application_metrics)
        if item.get("sampler_error")
    ]
    try:
        projection_status = projection.snapshot()
    except Exception as exc:  # pragma: no cover - diagnostic path
        projection_status = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    spool_status = spool.lightweight_snapshot()
    return {
        "at_seconds": round(time.monotonic() - started, 3),
        # Backward-compatible aliases retain the historical harness-inclusive
        # values; the explicit fields are used for production profile gates.
        "cpu_percent_sum": harness_metrics["cpu_percent"],
        "rss_kib_sum": harness_metrics["rss_kib"],
        "process_count": harness_metrics["sampled_pid_count"],
        "harness_cpu_percent": harness_metrics["cpu_percent"],
        "harness_rss_kib": harness_metrics["rss_kib"],
        "harness_process_count": harness_metrics["sampled_pid_count"],
        "application_cpu_percent": application_metrics["cpu_percent"],
        "application_rss_kib": application_metrics["rss_kib"],
        "application_process_count": application_metrics["sampled_pid_count"],
        "sampler_ok": sampler_ok,
        "sampler_error": "; ".join(sampler_errors) or None,
        "sampled_pid_count": harness_metrics["sampled_pid_count"],
        "sampling_source": (
            harness_metrics["sampling_source"]
            if harness_metrics["sampling_source"] == application_metrics["sampling_source"]
            else f"{harness_metrics['sampling_source']}+{application_metrics['sampling_source']}"
        ),
        "harness_sampler": harness_metrics,
        "application_sampler": application_metrics,
        "process_roles": process_roles or {},
        "run_bytes": _directory_bytes(run_root),
        "disk_free_bytes": shutil.disk_usage(run_root).free,
        "spool_bytes": int(spool_status.get("bytes", 0) or 0),
        "spool_pending_records": int(spool_status.get("pending_records", 0) or 0),
        "journal_bytes": sum(
            path.stat().st_size
            for path in run_root.rglob("worker-*.jsonl")
            if path.is_file()
        ),
        "projection_applied": int(projection_status.get("applied", 0) or 0),
        "projection_lag_seconds": float(projection_status.get("projection_lag_seconds", 0.0) or 0.0),
        "projection_queue_depth": int(projection_status.get("queue_depth", 0) or 0),
        "projection_journal_committed_offset_bytes": int(
            projection_status.get("journal_committed_offset_bytes", 0) or 0
        ),
        "projection_journal_backlog_bytes": int(
            projection_status.get("journal_backlog_bytes", 0) or 0
        ),
        "projection_journal_backlog_events_estimate": int(
            projection_status.get("journal_backlog_events_estimate", 0) or 0
        ),
        "smpp_received": int(getattr(smsc, "total_received", 0)),
        "smpp_response_success": smsc.response_success,
    }


def _read_critical_path_profiles(profile_root: Path | None) -> list[dict]:
    if profile_root is None:
        return []
    values = []
    for path in sorted((profile_root / "profiles").glob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        if isinstance(value, dict):
            values.append(value)
    return values


def _aggregate_critical_path_profiles(profile_root: Path | None) -> dict:
    profiles = _read_critical_path_profiles(profile_root)
    counters: Counter[str] = Counter()
    gauges: dict[str, object] = {}
    timestamps: dict[str, float] = {}
    duration_values: dict[str, list[dict]] = {}
    for profile in profiles:
        counters.update({
            str(key): int(value or 0)
            for key, value in (profile.get("counters") or {}).items()
            if isinstance(value, (int, float))
        })
        gauges.update(profile.get("gauges") or {})
        for key, value in (profile.get("timestamps") or {}).items():
            try:
                timestamps[str(key)] = max(timestamps.get(str(key), 0.0), float(value))
            except (TypeError, ValueError):
                continue
        for name, value in (profile.get("durations") or {}).items():
            if isinstance(value, dict):
                duration_values.setdefault(str(name), []).append(value)
    durations = {}
    for name, values in duration_values.items():
        durations[name] = {
            "count": int(counters.get(f"{name}_count", 0)),
            "p50_ms_max": max(float(item.get("p50_ms", 0.0) or 0.0) for item in values),
            "p95_ms_max": max(float(item.get("p95_ms", 0.0) or 0.0) for item in values),
            "p99_ms_max": max(float(item.get("p99_ms", 0.0) or 0.0) for item in values),
            "max_ms_max": max(float(item.get("max_ms", 0.0) or 0.0) for item in values),
            "total_ns": int(counters.get(f"{name}_total_ns", 0)),
        }
    return {
        "enabled": bool(profiles),
        "profile_count": len(profiles),
        "roles": [str(item.get("role") or "") for item in profiles],
        "counters": dict(sorted(counters.items())),
        "gauges": dict(sorted(gauges.items())),
        "timestamps": dict(sorted(timestamps.items())),
        "durations": dict(sorted(durations.items())),
        "profiles": profiles,
    }


def _profile_sample_row(sample: dict) -> dict:
    critical = sample.get("critical_path") or {}
    counters = critical.get("counters") or {}
    durations = critical.get("durations") or {}

    def duration_p95(name: str):
        value = (durations.get(name) or {}).get("p95_ms_max")
        return round(float(value), 3) if value is not None else ""

    return {
        "at_seconds": sample.get("at_seconds", 0.0),
        "socket_written_pdu": counters.get("socket_written_pdu", 0),
        "response_pdus_received": counters.get("response_pdus_received", 0),
        "journal_events_durable": counters.get("journal_events_durable", 0),
        "projection_committed_events": sample.get("projection_applied", 0),
        "journal_backlog_bytes": sample.get("projection_journal_backlog_bytes", 0),
        "journal_backlog_events_estimate": sample.get("projection_journal_backlog_events_estimate", 0),
        "response_callback_p95_ms": duration_p95("response_callback"),
        "journal_append_p95_ms": duration_p95("journal_append"),
        "journal_fsync_p95_ms": duration_p95("journal_fsync"),
        "tps_credit_wait_p95_ms": duration_p95("tps_credit_wait"),
        "response_window_wait_p95_ms": duration_p95("response_window_wait"),
        "socket_write_drain_p95_ms": duration_p95("socket_write_drain"),
        "projection_status_write_p95_ms": duration_p95("process_status_write"),
    }


def _write_critical_path_samples(run_root: Path, samples: list[dict]) -> str:
    path = run_root / "critical-path-samples.csv"
    fields = list(_profile_sample_row(samples[0]).keys()) if samples else ["at_seconds"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for sample in samples:
            writer.writerow(_profile_sample_row(sample))
    return str(path)


def _safe_rate(count: int | float, seconds: float | None) -> float | None:
    if seconds is None or float(seconds) <= 0:
        return None
    return round(float(count) / float(seconds), 3)


def _throughput_observability(
    *, route_config: dict, feed_stats: Counter, smsc: CountingLoopbackSMSC,
    worker_results: list[dict], critical_path: dict,
) -> dict:
    """Build route/bind/worker/loopback evidence without changing dispatch."""

    workers = []
    binds = []
    credit_totals = Counter()
    for worker in worker_results:
        counters = worker.get("counters") or {}
        credit = worker.get("route_credits") or {}
        for key in (
            "reservations", "credit_refills", "file_credit_refills", "credit_wait_count",
            "credit_granted_units", "credit_consumed_units", "credit_remaining_units",
            "credit_wasted_units", "credit_wait_seconds", "credit_refill_cost_seconds",
            "bind_acquires",
        ):
            if key in {"credit_wait_seconds", "credit_refill_cost_seconds"}:
                credit_totals[key] += float(credit.get(key, 0.0) or 0.0)
            else:
                credit_totals[key] += int(credit.get(key, 0) or 0)
        worker_socket_written = int(
            counters.get("socket_written_pdu", counters.get("socket_written", 0)) or 0
        )
        worker_responses = int(
            counters.get("response_pdus_received", counters.get("response_success", 0)) or 0
        )
        worker_row = {
            "worker_id": worker.get("worker_id"),
            "task_taken": int(counters.get("task_taken", counters.get("taken", 0)) or 0),
            "socket_written_pdu": worker_socket_written,
            "response_pdus_received": worker_responses,
            "journal_events_durable": int(counters.get("journal_events_durable", 0) or 0),
            "route_credits": credit,
            "transports": [],
        }
        for transport in worker.get("transports") or []:
            bind = dict(transport)
            bind["socket_pdu_per_bind_active_second"] = _safe_rate(
                bind.get("socket_written_parts", 0), bind.get("bind_active_seconds")
            )
            bind["response_pdu_per_bind_active_second"] = _safe_rate(
                bind.get("response_success", 0), bind.get("bind_active_seconds")
            )
            bind["worker_id"] = worker.get("worker_id")
            binds.append(bind)
            worker_row["transports"].append(bind)
        workers.append(worker_row)
    durations = critical_path.get("durations") or {}
    counters = critical_path.get("counters") or {}
    return {
        "configured": {
            "route_id": route_config.get("route_id"),
            "configured_tps_scope": route_config.get("tps_scope"),
            "configured_tps_per_bind": route_config.get("tps")
            if route_config.get("tps_scope") == "per_bind" else None,
            "configured_tps_route_shared": route_config.get("tps")
            if route_config.get("tps_scope") == "route_shared" else None,
            "configured_tps_total_ceiling": (
                float(route_config.get("tps", 0.0)) * int(route_config.get("pool_size", 1))
                if route_config.get("tps_scope") == "per_bind"
                else float(route_config.get("tps", 0.0))
            ),
            "pool_size": int(route_config.get("pool_size", 1)),
            "window_size": int(route_config.get("window_size", 0)),
        },
        "producer": {
            "accepted_tasks": int(sum(
                value for key, value in feed_stats.items() if str(key).endswith("_accepted")
            )),
            "batches": int(feed_stats.get("batches", 0) or 0),
            "backpressure_waits": int(feed_stats.get("backpressure_waits", 0) or 0),
        },
        "credits": dict(credit_totals),
        "binds": binds,
        "workers": workers,
        "window": {
            "configured_size": int(route_config.get("window_size", 0)),
            "wait_count": int(counters.get("response_window_wait_count", 0) or 0),
            "wait_p50_ms": (durations.get("response_window_wait") or {}).get("p50_ms_max"),
            "wait_p95_ms": (durations.get("response_window_wait") or {}).get("p95_ms_max"),
            "wait_p99_ms": (durations.get("response_window_wait") or {}).get("p99_ms_max"),
        },
        "loopback": smsc.snapshot(),
        "writer": {
            "socket_written_pdu": int(counters.get("socket_written_pdu", 0) or 0),
            "response_pdus_received": int(counters.get("response_pdus_received", 0) or 0),
            "journal_events_durable": int(counters.get("journal_events_durable", 0) or 0),
            "projection_commits": int(counters.get("projection_commits", 0) or 0),
            "tps_credit_wait_p95_ms": (durations.get("tps_credit_wait") or {}).get("p95_ms_max"),
            "response_callback_p95_ms": (durations.get("response_callback") or {}).get("p95_ms_max"),
        },
    }


async def run(args: argparse.Namespace) -> dict:
    args.workers = _validate_worker_count(args.workers)
    _install_source(args.source_root)
    from vnext.ingress import IngressRouter
    from vnext.partition import PartitionMap
    from vnext.projection_writer import ProgressJournal, ProjectionWriterProcess
    from vnext.route_credits import RouteCreditBroker  # noqa: F401 - import contract check
    from vnext.smpp_transport import (
        BIND_RECEIVER,
        BIND_TRANSMITTER,
        BIND_TRANSCEIVER,
        ENQUIRE_LINK,
        HEADER,
        RESPONSE_MASK,
        SUBMIT_SM,
        UNBIND,
        cstring,
        read_pdu,
    )
    from vnext.spool import TaskSpool
    smpp = type(
        "SmppSymbols",
        (),
        {
            "BIND_RECEIVER": BIND_RECEIVER,
            "BIND_TRANSMITTER": BIND_TRANSMITTER,
            "BIND_TRANSCEIVER": BIND_TRANSCEIVER,
            "ENQUIRE_LINK": ENQUIRE_LINK,
            "HEADER": HEADER,
            "RESPONSE_MASK": RESPONSE_MASK,
            "SUBMIT_SM": SUBMIT_SM,
            "UNBIND": UNBIND,
            "cstring": staticmethod(cstring),
            "read_pdu": staticmethod(read_pdu),
            "parse_destination": staticmethod(_parse_destination),
        },
    )

    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    lifecycle_root = run_root / "lifecycle"
    lifecycle_root.mkdir(parents=True, exist_ok=True)
    parent_lifecycle_path = lifecycle_root / "parent.jsonl"
    _parent_lifecycle_event(
        parent_lifecycle_path,
        "PARENT_START",
        source_root=str(Path(args.source_root).resolve()),
        requested_workers=int(args.workers),
        worker_join_timeout_seconds=WORKER_JOIN_TIMEOUT_SECONDS,
    )
    profile_root = Path(args.profile_root).resolve() if args.profile_root else None
    if profile_root:
        profile_root.mkdir(parents=True, exist_ok=True)
    spool_root = run_root / "spool"
    credits_root = run_root / "route-credits"
    spool = TaskSpool(spool_root, virtual_partition_count=256, max_bytes=args.spool_max_bytes)
    ingress = IngressRouter(spool)
    counts = _parse_counts(args.counts)
    campaign_ids = list(range(1, len(counts) + 1))
    multipart_campaigns = _parse_ids(args.multipart_campaigns)
    total_tasks = sum(counts)

    # Reuse the exact loopback protocol server already used by the frozen
    # P10/P100/P1K/P5500 acceptance tool; this harness only adds streaming.
    smsc = CountingLoopbackSMSC(
        smpp, response_delay_seconds=max(0.0, float(args.response_delay_ms)) / 1000.0
    )
    port = await smsc.start()
    partition_map = PartitionMap.create(run_root / "partition-map.json", 256, args.workers)
    partition_evidence = _partition_assignment_evidence(partition_map, args.workers)
    if not partition_evidence["valid"]:
        raise RuntimeError(f"invalid partition assignment: {partition_evidence}")
    worker_partitions = [
        tuple(partition_map.partitions_for_worker(worker_id))
        for worker_id in range(args.workers)
    ]
    route_config = {
        "route_id": 1,
        "host": "127.0.0.1",
        "port": port,
        "system_id": "test",
        "password": "test",
        "source_addr": "VNEXT",
        "pool_size": args.pool_size,
        "window_size": args.window_size,
        "tps": args.route_tps,
        "tps_scope": args.tps_scope,
        "timeout": 10,
    }
    context = multiprocessing.get_context("spawn")
    feeder_done = context.Event()
    stop_event = context.Event()
    journal_paths = [run_root / "progress" / f"worker-{worker_id}.jsonl" for worker_id in range(args.workers)]
    projection_kwargs = {
        "batch_size": args.projection_batch_size,
        "journal_paths": journal_paths,
        "staging_mode": args.projection_staging_mode,
    }
    if profile_root:
        projection_kwargs.update({
            "profile_root": str(profile_root),
            "profile_interval_seconds": args.profile_interval,
        })
    try:
        projection = ProjectionWriterProcess(
            run_root / "projection",
            **projection_kwargs,
            status_interval_seconds=args.projection_status_interval,
        )
    except TypeError as exc:
        # The frozen f4ab3373 baseline predates the bounded status interval.
        # Keep the harness identical while allowing that source tree to run
        # with its historical per-loop status behavior.
        if "status_interval_seconds" not in str(exc):
            raise
        projection = ProjectionWriterProcess(run_root / "projection", **projection_kwargs)
    projection.start()
    processes = []
    workers_alive_peak = 0
    for worker_id in range(args.workers):
        journal_path = journal_paths[worker_id]
        result_path = run_root / "workers" / f"worker-{worker_id}.json"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.parent.mkdir(parents=True, exist_ok=True)
        process = context.Process(
            target=_worker_process,
            args=(
                str(Path(args.source_root).resolve()),
                worker_id,
                worker_partitions[worker_id],
                str(spool_root),
                str(credits_root),
                route_config,
                str(journal_path),
                str(result_path),
                args.workers,
                feeder_done,
                stop_event,
                str(args.profile_root) if args.profile_root else "",
                args.profile_interval,
                str(lifecycle_root),
                args.progress_mode,
            ),
            name=f"mc-vnext-stream-worker-{worker_id}",
            daemon=True,
        )
        process.start()
        processes.append(process)
        workers_alive_peak = max(
            workers_alive_peak,
            sum(1 for item in processes if item.is_alive()),
        )

    started = time.monotonic()
    samples: list[dict] = []
    feed_stats = Counter()
    offsets = {campaign_id: 0 for campaign_id in campaign_ids}
    remaining = {campaign_id: count for campaign_id, count in zip(campaign_ids, counts)}
    steady_deadline = (
        time.monotonic() + max(1.0, float(args.steady_seconds))
        if args.steady_seconds > 0 else None
    )
    feed_stopped_at = None
    smpp_drained_at = None
    projection_caught_up_at = None
    projection_drain_deadline = None
    completion_detection_lag_ms = None
    completion_status_age_ms = None
    producer_done_recorded = False
    all_pdus_responded_recorded = False
    journal_eof_recorded = False
    projection_caught_up_recorded = False
    join_forensics_path = None
    completion_evidence = {
        "state": "INCOMPLETE",
        "complete": False,
        "status_age_ms": None,
        "durable": None,
    }
    sampler_state: dict = {}

    async def feed() -> None:
        while (
            (steady_deadline is not None and time.monotonic() < steady_deadline)
            or (steady_deadline is None and any(value > 0 for value in remaining.values()))
        ):
            progress = False
            while int(spool.lightweight_snapshot().get("pending_records", 0) or 0) >= args.max_pending_records:
                feed_stats["backpressure_waits"] += 1
                await asyncio.sleep(0.5)
                if steady_deadline is not None and time.monotonic() >= steady_deadline:
                    break
            for campaign_id in campaign_ids:
                if steady_deadline is None and remaining[campaign_id] <= 0:
                    continue
                if steady_deadline is not None and time.monotonic() >= steady_deadline:
                    break
                progress = True
                batch_count = (
                    args.batch_size if steady_deadline is not None
                    else min(args.batch_size, remaining[campaign_id])
                )
                offset = offsets[campaign_id]
                long_message = campaign_id in multipart_campaigns
                rows = []
                for index in range(offset, offset + batch_count):
                    message = f"VNext stream campaign={campaign_id} task={index}"
                    if long_message:
                        message += " " + ("MULTIPART-" * 24)
                    rows.append({
                        "msisdn": f"090{campaign_id:02d}{index:07d}",
                        "message": message,
                    })
                result = ingress.enqueue_chunk(
                    rows,
                    campaign_type="batch",
                    campaign_id=campaign_id,
                    campaign_generation=1,
                    route_id=1,
                    phone_prefix="251",
                    source_id=f"stream-{campaign_id}-{offset}",
                    payload_ref=f"synthetic-stream:{campaign_id}:{offset}",
                    row_ordinal_start=offset,
                )
                if result.accepted != batch_count:
                    raise RuntimeError(
                        f"ingress accepted {result.accepted}/{batch_count} for campaign {campaign_id}: {result.as_dict()}"
                    )
                offsets[campaign_id] += batch_count
                if steady_deadline is None:
                    remaining[campaign_id] -= batch_count
                feed_stats["batches"] += 1
                feed_stats[f"campaign_{campaign_id}_accepted"] += result.accepted
                await asyncio.sleep(args.feed_interval)
            if not progress:
                break
        feeder_done.set()

    feed_task = asyncio.create_task(feed())
    guard_reason = ""
    try:
        next_sample = 0.0
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= next_sample:
                workers_alive = sum(1 for process in processes if process.is_alive())
                workers_alive_peak = max(workers_alive_peak, workers_alive)
                sample = _sample(
                    run_root=run_root,
                    process_roots=[os.getpid()]
                    + [process.pid for process in processes if process.pid]
                    + [int(getattr(projection, "pid", 0) or 0)],
                    application_roots=[process.pid for process in processes if process.pid]
                    + [int(getattr(projection, "pid", 0) or 0)],
                    process_roles={
                        "harness": [os.getpid()],
                        "workers": [process.pid for process in processes if process.pid],
                        "projection_writer": [int(getattr(projection, "pid", 0) or 0)],
                    },
                    spool=spool,
                    projection=projection,
                    smsc=smsc,
                    started=started,
                    profile_root=profile_root,
                    sampler_state=sampler_state,
                )
                sample["workers_alive"] = workers_alive
                sample["critical_path"] = _aggregate_critical_path_profiles(profile_root)
                samples.append(sample)
                next_sample += max(0.1, args.sample_interval)
                if not sample["sampler_ok"]:
                    guard_reason = "resource_sampler_error"
                    break
                if sample["run_bytes"] > args.max_run_bytes:
                    guard_reason = "run_disk_guard_exceeded"
                    break
                guard_rss_kib = (
                    sample["application_rss_kib"]
                    if args.rss_guard_scope == "application"
                    else sample["harness_rss_kib"]
                )
                if guard_rss_kib > args.max_rss_kib:
                    guard_reason = "rss_guard_exceeded"
                    break
            if feed_task.done():
                await feed_task
                if feed_stopped_at is None:
                    feed_stopped_at = time.monotonic()
                    if not producer_done_recorded:
                        _parent_lifecycle_event(
                            parent_lifecycle_path,
                            "PRODUCER_DONE",
                            feeder_done=bool(feeder_done.is_set()),
                            generated_tasks=sum(int(value) for value in feed_stats.values()
                                                if isinstance(value, int)),
                        )
                        producer_done_recorded = True
                    projection_drain_deadline = (
                        feed_stopped_at + max(1.0, float(args.projection_drain_timeout))
                    )
                status = spool.lightweight_snapshot()
                if int(status.get("pending_records", 0) or 0) == 0:
                    if (
                        smpp_drained_at is None
                        and smsc.total_received > 0
                        and smsc.response_success == smsc.total_received
                    ):
                        smpp_drained_at = time.monotonic()
                        if not all_pdus_responded_recorded:
                            _parent_lifecycle_event(
                                parent_lifecycle_path,
                                "ALL_PDUS_RESPONDED",
                                smpp_received=int(smsc.total_received),
                                response_success=int(smsc.response_success),
                            )
                            all_pdus_responded_recorded = True
                    completion_evidence = _projection_completion_evidence(
                        projection, journal_paths,
                        max_status_age_seconds=max(2.0, args.sample_interval * 3.0),
                    )
                    if completion_evidence.get("durable") and not journal_eof_recorded:
                        _parent_lifecycle_event(
                            parent_lifecycle_path,
                            "JOURNAL_EOF",
                            journal_paths_count=len(journal_paths),
                            completion_evidence=dict(completion_evidence),
                        )
                        journal_eof_recorded = True
                    if completion_evidence.get("complete"):
                        if projection_caught_up_at is None:
                            projection_caught_up_at = time.monotonic()
                            completion_status_age_ms = completion_evidence.get("status_age_ms")
                            durable_mtime = completion_evidence.get("durable_offset_mtime_epoch")
                            if durable_mtime is not None:
                                completion_detection_lag_ms = round(
                                    max(0.0, time.time() - float(durable_mtime)) * 1000.0,
                                    3,
                                )
                            if not projection_caught_up_recorded:
                                _parent_lifecycle_event(
                                    parent_lifecycle_path,
                                    "PROJECTION_CAUGHT_UP",
                                    completion_evidence=dict(completion_evidence),
                                )
                                projection_caught_up_recorded = True
                        elif time.monotonic() - projection_caught_up_at >= 0.5:
                            break
                    else:
                        projection_caught_up_at = None
                else:
                    projection_caught_up_at = None
                if (
                    projection_drain_deadline is not None
                    and time.monotonic() >= projection_drain_deadline
                ):
                    guard_reason = "projection_drain_timeout"
                    break
            await asyncio.sleep(0.1)
    finally:
        if not feed_task.done():
            feed_task.cancel()
            feeder_done.set()
        stop_event.set()
        _parent_lifecycle_event(
            parent_lifecycle_path,
            "STOP_SIGNAL_SET",
            guard_reason=str(guard_reason),
            feeder_done=bool(feeder_done.is_set()),
            pending_records=int(spool.lightweight_snapshot().get("pending_records", 0) or 0),
        )
        join_deadline = time.monotonic() + WORKER_JOIN_TIMEOUT_SECONDS
        join_started = {int(process.pid or 0): time.monotonic() for process in processes}
        for index, process in enumerate(processes):
            _parent_lifecycle_event(
                parent_lifecycle_path,
                "JOIN_START",
                worker_id=index,
                pid=int(process.pid or 0),
                pid_start_time=_pid_start_time(process.pid),
            )
        next_join_sample = 0.0
        while any(process.is_alive() for process in processes) and time.monotonic() < join_deadline:
            now = time.monotonic()
            if now >= next_join_sample:
                alive = [process for process in processes if process.is_alive()]
                _parent_lifecycle_event(
                    parent_lifecycle_path,
                    "JOIN_HEARTBEAT",
                    alive_worker_ids=[index for index, process in enumerate(processes) if process.is_alive()],
                    elapsed_seconds=round(now - min(join_started.values(), default=now), 3),
                )
                next_join_sample = now + LIFECYCLE_HEARTBEAT_SECONDS
            for process in processes:
                process.join(timeout=0)
            await asyncio.sleep(0.2)
        for index, process in enumerate(processes):
            process.join(timeout=0)
            _parent_lifecycle_event(
                parent_lifecycle_path,
                "JOIN_END",
                worker_id=index,
                pid=int(process.pid or 0),
                exitcode=process.exitcode,
                alive=bool(process.is_alive()),
                elapsed_seconds=round(max(0.0, time.monotonic() - join_started.get(int(process.pid or 0), time.monotonic())), 3),
            )
        for process in processes:
            if process.is_alive():
                join_forensics_path = _capture_join_forensics(
                    run_root=run_root,
                    lifecycle_root=lifecycle_root,
                    credits_root=credits_root,
                    processes=processes,
                    reason="worker_join_deadline_exceeded",
                )
                _parent_lifecycle_event(
                    parent_lifecycle_path,
                    "TERMINATE_REQUESTED",
                    worker_id=processes.index(process),
                    pid=int(process.pid or 0),
                    pid_start_time=_pid_start_time(process.pid),
                )
                process.terminate()
                await asyncio.to_thread(process.join, 5)
        workers_alive_peak = max(
            workers_alive_peak,
            sum(1 for process in processes if process.is_alive()),
        )
        await smsc.stop()
        projection.stop(timeout=300)
        _parent_lifecycle_event(
            parent_lifecycle_path,
            "PARENT_CLEANUP_COMPLETE",
            worker_exitcodes=[process.exitcode for process in processes],
            alive_worker_ids=[index for index, process in enumerate(processes) if process.is_alive()],
            join_forensics_path=str(join_forensics_path) if join_forensics_path else "",
        )

    progress = []
    for journal_path in journal_paths:
        progress.extend(ProgressJournal(journal_path).read())
    task_events = [item for item in progress if not str(item.get("state", "")).startswith("submit_sm_resp_")]
    socket_events = [item for item in task_events if item.get("state") == "socket_written"]
    write_failed_events = [item for item in task_events if item.get("state") == "write_failed"]
    response_events = [item for item in progress if ":resp:" in str(item.get("event_id", ""))]
    response_success_events = [
        item for item in response_events if item.get("state") == "submit_sm_resp_success"
    ]
    response_unknown_events = [item for item in response_events if item.get("state") == "unknown"]
    campaign_tasks = Counter(int(item.get("campaign_id") or 0) for item in task_events)
    campaign_socket_tasks = Counter(int(item.get("campaign_id") or 0) for item in socket_events)
    campaign_write_failed = Counter(int(item.get("campaign_id") or 0) for item in write_failed_events)
    campaign_pdus = Counter()
    for item in socket_events:
        campaign_pdus[int(item.get("campaign_id") or 0)] += int(
            item.get("parts_written") or item.get("parts_total") or 1
        )
    received_campaign_pdus = Counter(getattr(smsc, "campaign_pdus", Counter()))
    smpp_total = int(getattr(smsc, "total_received", 0))
    destination_counts = Counter(getattr(smsc, "destination_counts", Counter()))
    duplicate_destination_pdu_count = sum(value - 1 for value in destination_counts.values() if value > 1)
    spool_status = spool.status()
    projection_status = projection.snapshot()
    journal_bytes = sum(
        path.stat().st_size
        for path in run_root.rglob("worker-*.jsonl")
        if path.is_file()
    )
    worker_results = []
    worker_result_paths = []
    for process_index in range(args.workers):
        result_path = run_root / "workers" / f"worker-{process_index}.json"
        if result_path.is_file():
            worker_result_paths.append(result_path)
            worker_results.append(json.loads(result_path.read_text(encoding="utf-8")))
    worker_result_files_count = len(worker_result_paths)
    workers_exited_cleanly_count = sum(
        1 for process in processes if process.exitcode == 0
    )
    worker_processes = [
        {
            "worker_id": index,
            "pid": int(process.pid or 0),
            "exitcode": process.exitcode,
            "alive": bool(process.is_alive()),
            "result_file": str(run_root / "workers" / f"worker-{index}.json"),
            "result_file_exists": (run_root / "workers" / f"worker-{index}.json").is_file(),
            "journal_file": str(journal_paths[index]),
            "journal_file_exists": journal_paths[index].is_file(),
            "partition_count": len(worker_partitions[index]),
        }
        for index, process in enumerate(processes)
    ]
    expected_campaigns = {
        str(campaign_id): {
            "tasks": int(feed_stats[f"campaign_{campaign_id}_accepted"]),
            "socket_written_tasks": int(campaign_socket_tasks[campaign_id]),
            "write_failed_tasks": int(campaign_write_failed[campaign_id]),
            "pdu_written": int(campaign_pdus[campaign_id]),
            "smpp_pdu_received": int(received_campaign_pdus[campaign_id]),
        }
        for campaign_id, count in zip(campaign_ids, counts)
    }
    generated_tasks = sum(
        int(feed_stats[f"campaign_{campaign_id}_accepted"])
        for campaign_id in campaign_ids
    )
    finite_complete = args.steady_seconds <= 0 and generated_tasks == total_tasks
    steady_complete = (
        args.steady_seconds > 0
        and (time.monotonic() - started) >= float(args.steady_seconds)
        and generated_tasks > 0
    )
    resource_sampler_valid = bool(samples) and all(
        bool(item.get("sampler_ok")) for item in samples
    )
    resource_gate = "PASS" if resource_sampler_valid else "NOT_VERIFIED"
    if guard_reason == "resource_sampler_error":
        completion_state = "SAMPLER_ERROR"
    elif projection_caught_up_at is not None:
        completion_state = "COMPLETED"
    elif completion_evidence.get("state") == "STALE_STATUS":
        completion_state = "STALE_STATUS"
    elif guard_reason == "projection_drain_timeout":
        completion_state = "TIMEOUT"
    else:
        completion_state = str(completion_evidence.get("state") or "INCOMPLETE")
    failure_reason = guard_reason or (smsc.errors[0] if smsc.errors else "")
    if not failure_reason and write_failed_events:
        failure_reason = "task_write_failed"
    if not failure_reason and len(response_events) != smpp_total:
        failure_reason = "response_event_count_mismatch"
    passed = (
        not guard_reason
        and resource_gate == "PASS"
        and not smsc.errors
        and (finite_complete or steady_complete)
        and len(task_events) == generated_tasks
        and len(socket_events) == generated_tasks
        and smsc.response_success == smpp_total
        and smpp_total == sum(campaign_pdus.values())
        and len(response_events) == smpp_total
        and len(response_success_events) == smsc.response_success
        and not response_unknown_events
        and not write_failed_events
        and smsc.max_active_binds <= args.pool_size
        and not spool_status.get("pending_records")
        and int(projection_status.get("applied", 0) or 0) == len(progress)
        and projection_caught_up_at is not None
        and partition_evidence["valid"]
        and len(processes) == args.workers
        and worker_result_files_count == args.workers
        and workers_exited_cleanly_count == args.workers
        and all(item["socket_written_tasks"] == item["tasks"] for item in expected_campaigns.values())
    )
    samples_sorted = sorted(samples, key=lambda item: float(item.get("at_seconds", 0.0)))
    critical_path = _aggregate_critical_path_profiles(profile_root)
    throughput_observability = _throughput_observability(
        route_config=route_config,
        feed_stats=feed_stats,
        smsc=smsc,
        worker_results=worker_results,
        critical_path=critical_path,
    )
    critical_path_samples_path = (
        _write_critical_path_samples(run_root, samples) if profile_root else None
    )

    def _peak(key: str) -> int | None:
        values = [item.get(key) for item in samples_sorted if item.get(key) is not None]
        return max((int(value) for value in values), default=None)

    def _slope_last_180(key: str) -> float | None:
        valid_samples = [item for item in samples_sorted if item.get(key) is not None]
        if not valid_samples:
            return None
        end = valid_samples[-1]
        start = next(
            (item for item in reversed(samples_sorted)
             if item.get(key) is not None
             and float(end.get("at_seconds", 0.0)) - float(item.get("at_seconds", 0.0)) >= 180.0),
            valid_samples[0],
        )
        seconds = float(end.get("at_seconds", 0.0)) - float(start.get("at_seconds", 0.0))
        if seconds <= 0:
            return None
        return round((float(end[key]) - float(start[key])) / seconds, 3)

    def _counter_value(sample: dict, name: str) -> int:
        return int(((sample.get("critical_path") or {}).get("counters") or {}).get(name, 0) or 0)

    def _rate_summary(value_getter) -> dict:
        rates = []
        previous = None
        for sample in samples_sorted:
            if previous is not None:
                seconds = float(sample.get("at_seconds", 0.0)) - float(previous.get("at_seconds", 0.0))
                if seconds > 0:
                    rates.append(
                        max(0.0, (float(value_getter(sample)) - float(value_getter(previous))) / seconds)
                    )
            previous = sample
        if not rates:
            return {"sample_count": 0, "p50": None, "p95": None, "max": None}
        ordered = sorted(rates)

        def percentile(percentile: float) -> float:
            index = (len(ordered) - 1) * percentile
            lower = int(index)
            upper = min(len(ordered) - 1, lower + 1)
            fraction = index - lower
            return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)

        return {
            "sample_count": len(rates),
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "max": round(max(ordered), 3),
        }

    timeline_timestamps = critical_path.get("timestamps") or {}

    def _timeline_value(name: str):
        value = timeline_timestamps.get(name)
        return round(float(value), 6) if value is not None else None

    def _after_feed_stop(name: str):
        value = timeline_timestamps.get(name)
        if value is None or feed_stopped_at is None:
            return None
        return round(max(0.0, float(value) - float(feed_stopped_at)), 3)

    producer_active_seconds = (
        round(max(0.0, feed_stopped_at - started), 3) if feed_stopped_at is not None else None
    )
    smpp_drain_seconds = (
        round(max(0.0, smpp_drained_at - feed_stopped_at), 3)
        if feed_stopped_at is not None and smpp_drained_at is not None else None
    )
    projection_drain_seconds = (
        round(max(0.0, projection_caught_up_at - feed_stopped_at), 3)
        if feed_stopped_at is not None and projection_caught_up_at is not None else None
    )
    last_timestamps = {
        "feed_start_monotonic": round(started, 6),
        "feed_stop_monotonic": round(feed_stopped_at, 6) if feed_stopped_at is not None else None,
        "last_task_taken_monotonic": _timeline_value("last_task_taken_monotonic"),
        "last_socket_write_monotonic": _timeline_value("last_socket_write_monotonic"),
        "last_smpp_response_received_monotonic": _timeline_value("last_smpp_response_received_monotonic"),
        "last_response_journal_durable_monotonic": _timeline_value("last_response_journal_durable_monotonic"),
        "last_projection_db_commit_monotonic": _timeline_value("last_projection_db_commit_monotonic"),
        "last_projection_offset_durable_monotonic": _timeline_value("last_projection_offset_durable_monotonic"),
    }
    return {
        "status": "PASS" if passed else "FAIL",
        "failure_reason": failure_reason,
        "resource_gate": resource_gate,
        "completion_state": completion_state,
        "completion_detection_lag_ms": completion_detection_lag_ms,
        "status_age_ms": completion_status_age_ms,
        "completion_evidence": completion_evidence,
        "source_root": str(Path(args.source_root).resolve()),
        "run_root": str(run_root),
        "started_epoch": time.time() - (time.monotonic() - started),
        "duration_seconds": round(time.monotonic() - started, 3),
        "workers": args.workers,
        "requested_workers": args.workers,
        "effective_workers": len(processes),
        "worker_processes_started_count": len(processes),
        "worker_result_files_count": worker_result_files_count,
        "workers_alive_peak": workers_alive_peak,
        "workers_exited_cleanly_count": workers_exited_cleanly_count,
        "worker_join_timeout_seconds": WORKER_JOIN_TIMEOUT_SECONDS,
        "lifecycle_diagnostics": {
            "enabled": True,
            "root": str(lifecycle_root),
            "parent_events": str(parent_lifecycle_path),
            "worker_state_files": [str(lifecycle_root / f"worker-{index}.json") for index in range(args.workers)],
            "faulthandler_files": [str(lifecycle_root / f"worker-{index}-stacks.log") for index in range(args.workers)],
            "asyncio_task_files": [str(lifecycle_root / f"worker-{index}-asyncio.jsonl") for index in range(args.workers)],
            "join_forensics": str(join_forensics_path) if join_forensics_path else "",
        },
        "worker_processes": worker_processes,
        "partition_owner_distribution": partition_evidence["owner_distribution"],
        "partition_assignment_evidence": partition_evidence,
        "journal_paths_count": len(journal_paths),
        "journal_files_count": sum(1 for path in journal_paths if path.is_file()),
        "pool_size": args.pool_size,
        "route_tps": args.route_tps,
        "tps_scope": args.tps_scope,
        "configured_effective_total_tps": args.route_tps * args.pool_size if args.tps_scope == "per_bind" else args.route_tps,
        "window_size": args.window_size,
        "projection_batch_size": args.projection_batch_size,
        "projection_staging_mode": args.projection_staging_mode,
        "projection_status_interval_seconds": args.projection_status_interval,
        "resource_profile": args.resource_profile,
        "rss_guard_scope": args.rss_guard_scope,
        "response_delay_ms": float(args.response_delay_ms),
        "counts": counts,
        "total_tasks": generated_tasks,
        "accepted_message_count": generated_tasks,
        "socket_written_task_count": len(socket_events),
        "write_failed_task_count": len(write_failed_events),
        "socket_written_pdu_count": sum(campaign_pdus.values()),
        "journal_event_count": len(progress),
        "journal_expected_event_count": len(progress),
        "response_event_count": len(response_events),
        "durable_response_event_count": len(response_events),
        "response_success_event_count": len(response_success_events),
        "response_unknown_event_count": len(response_unknown_events),
        "events_per_task": round(len(progress) / generated_tasks, 6) if generated_tasks else 0.0,
        "events_per_pdu": round(len(progress) / sum(campaign_pdus.values()), 6) if sum(campaign_pdus.values()) else 0.0,
        "submit_sm_resp_success": smsc.response_success,
        "projection_events_applied": int(projection_status.get("applied", 0) or 0),
        "duckdb_unique_event_rows": int(
            (projection_status.get("metrics") or {}).get("events_new", 0) or 0
        ),
        "projection_lag_seconds": float(projection_status.get("projection_lag_seconds", 0.0) or 0.0),
        "projection_caught_up_before_stop": projection_caught_up_at is not None,
        "projection_drain_timeout_seconds": float(args.projection_drain_timeout),
        "journal_bytes": journal_bytes,
        "producer_active_seconds": producer_active_seconds,
        "smpp_drain_seconds": smpp_drain_seconds,
        "projection_drain_seconds": projection_drain_seconds,
        "drain_seconds": projection_drain_seconds,
        "critical_path_timeline": {
            **last_timestamps,
            "feed_active_seconds": producer_active_seconds,
            "pending_send_drain_seconds": _after_feed_stop("last_socket_write_monotonic"),
            "smpp_response_drain_seconds": _after_feed_stop("last_smpp_response_received_monotonic"),
            "response_journal_drain_seconds": _after_feed_stop("last_response_journal_durable_monotonic"),
            "projection_commit_drain_seconds": _after_feed_stop("last_projection_db_commit_monotonic"),
            "offset_durable_drain_seconds": _after_feed_stop("last_projection_offset_durable_monotonic"),
            "journal_backlog_bytes_timeline": [
                {
                    "at_seconds": item.get("at_seconds"),
                    "bytes": item.get("projection_journal_backlog_bytes"),
                }
                for item in samples_sorted
            ],
            "journal_backlog_events_estimate_timeline": [
                {
                    "at_seconds": item.get("at_seconds"),
                    "events": item.get("projection_journal_backlog_events_estimate"),
                }
                for item in samples_sorted
            ],
        },
        "critical_path_rates": {
            "socket_written_pdu_per_second": _rate_summary(
                lambda item: _counter_value(item, "socket_written_pdu")
            ),
            "response_received_pdu_per_second": _rate_summary(
                lambda item: _counter_value(item, "response_pdus_received")
            ),
            "journal_durable_events_per_second": _rate_summary(
                lambda item: _counter_value(item, "journal_events_durable")
            ),
            "projection_committed_events_per_second": _rate_summary(
                lambda item: item.get("projection_applied", 0)
            ),
        },
        "critical_path": critical_path,
        "critical_path_samples_csv": critical_path_samples_path,
        "steady_seconds_requested": float(args.steady_seconds),
        "producer": {
            "batches": int(feed_stats["batches"]),
            "backpressure_waits": int(feed_stats["backpressure_waits"]),
            "counts_by_campaign": {
                str(campaign_id): int(feed_stats[f"campaign_{campaign_id}_accepted"])
                for campaign_id in campaign_ids
            },
        },
        "spool": spool_status,
        "smpp": {
            "binds": smsc.binds,
            "active_binds": smsc.active_binds,
            "max_active_binds": smsc.max_active_binds,
            "submit_sm_pdu_received": smpp_total,
            "submit_sm_resp_success": smsc.response_success,
            "submit_sm_resp_error": int(
                len([item for item in response_events if item.get("state") == "submit_sm_resp_error"])
            ),
            "duplicate_destination_pdu_count": duplicate_destination_pdu_count,
            "destination_tracking_truncated": bool(smsc.destination_tracking_truncated),
            "errors": smsc.errors,
            "observability": smsc.snapshot(),
        },
        "throughput_observability": throughput_observability,
        "campaigns": expected_campaigns,
        "projection": projection_status,
        "worker_results": worker_results,
        "resource_samples": samples,
        "resource_peaks": {
            "application_rss_kib": _peak("application_rss_kib"),
            "harness_rss_kib": _peak("harness_rss_kib"),
            "application_rss_slope_last_180_kib_per_second": _slope_last_180(
                "application_rss_kib"
            ),
            "harness_rss_slope_last_180_kib_per_second": _slope_last_180(
                "harness_rss_kib"
            ),
        },
        "guard": {
            "max_run_bytes": args.max_run_bytes,
            "max_rss_kib": args.max_rss_kib,
            "max_pending_records": args.max_pending_records,
            "sample_interval_seconds": args.sample_interval,
            "resource_profile": args.resource_profile,
            "rss_guard_scope": args.rss_guard_scope,
            "application_rss_limit_kib": args.max_rss_kib
                if args.rss_guard_scope == "application" else None,
            "harness_rss_limit_kib": args.max_rss_kib
                if args.rss_guard_scope == "harness" else None,
            "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "nice": os.getpriority(os.PRIO_PROCESS, 0) if hasattr(os, "getpriority") else None,
        },
        "execution_mode": "bounded_streaming_feeder_concurrent_worker_journal_projection",
        "test_harness_scope": "synthetic_loopback_only_not_production_api",
    }


def _parse_destination(body: bytes) -> str:
    offset = 0
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated service_type")
    offset = end + 1 + 2
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated source_addr")
    offset = end + 1 + 2
    end = body.find(b"\x00", offset)
    if end < 0:
        raise ValueError("unterminated destination_addr")
    return body[offset:end].decode("ascii", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--counts", default="20000,20000,5000,5000")
    parser.add_argument("--multipart-campaigns", default="",
                        help="campaign ids for the separate multipart experiment")
    parser.add_argument(
        "--workers",
        type=_parse_worker_count,
        default=8,
        help="worker matrix entry: 8, 12, 16, or 24 (default: 8)",
    )
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--route-tps", type=float, default=200.0)
    parser.add_argument("--tps-scope", choices=("route_shared", "per_bind"), default="per_bind")
    parser.add_argument("--window-size", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--projection-batch-size", type=int, default=512)
    parser.add_argument("--projection-staging-mode", choices=("vectorized", "executemany"), default="vectorized")
    parser.add_argument("--projection-status-interval", type=float, default=1.0,
                        help="status/diagnostic write interval in seconds")
    parser.add_argument("--instrumentation", choices=("off", "on"), default="off",
                        help="enable bounded critical-path profiles and per-second CSV")
    parser.add_argument(
        "--progress-mode", choices=("decoupled", "legacy"), default="decoupled",
        help="worker progress journal path; legacy is only for explicit A/B",
    )
    parser.add_argument("--profile-root", default="",
                        help="profile output root; defaults to <run-root>/critical-path when enabled")
    parser.add_argument("--profile-interval", type=float, default=1.0,
                        help="maximum profile snapshot frequency in seconds")
    parser.add_argument("--response-delay-ms", type=float, default=0.0,
                        help="delay each loopback submit_sm_resp to exercise response draining")
    parser.add_argument("--feed-interval", type=float, default=0.05)
    parser.add_argument("--max-pending-records", type=int, default=20000)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--steady-seconds", type=float, default=0.0,
                        help="keep the bounded producer active for this duration before drain")
    parser.add_argument("--projection-drain-timeout", type=float, default=600.0,
                        help="maximum seconds to wait for journal EOF catch-up after feed stop")
    parser.add_argument("--max-run-bytes", type=int, default=5 * 1024 * 1024 * 1024)
    parser.add_argument("--resource-profile", choices=tuple(RESOURCE_PROFILE_RSS_KIB),
                        default="LOW_MEM_2G")
    parser.add_argument("--rss-guard-scope", choices=("application", "harness"), default="")
    parser.add_argument("--max-rss-kib", type=int, default=None)
    parser.add_argument("--spool-max-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    args.workers = _validate_worker_count(args.workers)
    args.pool_size = max(1, min(20, args.pool_size))
    args.window_size = max(1, min(1000, args.window_size))
    args.projection_batch_size = max(1, min(4096, args.projection_batch_size))
    args.projection_status_interval = min(60.0, max(0.1, float(args.projection_status_interval)))
    args.profile_interval = min(60.0, max(0.1, float(args.profile_interval)))
    if args.instrumentation == "on" and not args.profile_root:
        args.profile_root = str(Path(args.run_root).resolve() / "critical-path")
    elif args.profile_root:
        args.profile_root = str(Path(args.profile_root).resolve())
    args.projection_drain_timeout = max(1.0, min(3600.0, args.projection_drain_timeout))
    args.resource_profile = str(args.resource_profile).upper()
    if args.max_rss_kib is None:
        args.max_rss_kib = RESOURCE_PROFILE_RSS_KIB[args.resource_profile]
    if not args.rss_guard_scope:
        args.rss_guard_scope = "harness" if args.resource_profile == "LOW_MEM_2G" else "application"
    result = asyncio.run(run(args))
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "status": result["status"],
        "duration_seconds": result["duration_seconds"],
        "total_tasks": result["total_tasks"],
        "accepted_message_count": result["accepted_message_count"],
        "socket_written_task_count": result["socket_written_task_count"],
        "socket_written_pdu_count": result["socket_written_pdu_count"],
        "journal_event_count": result["journal_event_count"],
        "response_event_count": result["response_event_count"],
        "response_unknown_event_count": result["response_unknown_event_count"],
        "submit_sm_resp_success": result["submit_sm_resp_success"],
        "projection_events_applied": result["projection_events_applied"],
        "max_active_binds": result["smpp"]["max_active_binds"],
        "requested_workers": result["requested_workers"],
        "effective_workers": result["effective_workers"],
        "worker_result_files_count": result["worker_result_files_count"],
        "workers_alive_peak": result["workers_alive_peak"],
        "workers_exited_cleanly_count": result["workers_exited_cleanly_count"],
        "partition_owner_distribution": result["partition_owner_distribution"],
        "failure_reason": result["failure_reason"],
        "resource_gate": result["resource_gate"],
        "completion_state": result["completion_state"],
        "completion_detection_lag_ms": result["completion_detection_lag_ms"],
        "status_age_ms": result["status_age_ms"],
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
