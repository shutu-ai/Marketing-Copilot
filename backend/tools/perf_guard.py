"""Bounded process-tree guard for offline performance acceptance runs.

This is a test harness utility, not a production service manager.  It starts
one already-reviewed local runner command, records the process tree and
low-overhead Linux resource samples under a Run root, and stops only that
process tree when the declared timeout, aggregate RSS, or Run disk budget is
exceeded.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any


class GuardError(RuntimeError):
    """Raised when the guard cannot prove its process/resource boundary."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inside(root: pathlib.Path, candidate: pathlib.Path) -> pathlib.Path:
    root = root.resolve()
    candidate = candidate.expanduser().resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise GuardError(f"BLOCKED_ENV: path escapes run root: {candidate}") from exc
    return candidate


def _parse_cpu_list(value: str) -> set[int]:
    cpus: set[int] = set()
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        if "-" in item:
            start, end = item.split("-", 1)
            first, last = int(start), int(end)
            if first > last:
                raise GuardError(f"invalid CPU range: {item}")
            cpus.update(range(first, last + 1))
        else:
            cpus.add(int(item))
    if len(cpus) > 4:
        raise GuardError("BLOCKED_RESOURCE: perf guard allows at most four CPUs")
    return cpus


def _proc_stat(pid: int) -> dict[str, Any] | None:
    base = pathlib.Path("/proc") / str(pid)
    try:
        stat = (base / "stat").read_text(encoding="utf-8")
        right = stat.rfind(")")
        fields = stat[right + 2 :].split()
        status_lines = (base / "status").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError, OSError, UnicodeError):
        return None
    if len(fields) < 22:
        return None
    status: dict[str, str] = {}
    for line in status_lines:
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    try:
        ppid = int(fields[1])
        start_ticks = int(fields[19])
        cpu_ticks = int(fields[11]) + int(fields[12])
    except (IndexError, ValueError):
        return None
    rss_kb = 0
    vm_rss = status.get("VmRSS", "0").split()
    if vm_rss:
        try:
            rss_kb = int(vm_rss[0])
        except ValueError:
            pass
    try:
        exe = os.readlink(str(base / "exe"))
    except (FileNotFoundError, PermissionError, OSError):
        exe = ""
    try:
        cmdline = (base / "cmdline").read_bytes().replace(b"\x00", b" ").decode(
            "utf-8", errors="replace"
        ).strip()
    except (FileNotFoundError, PermissionError, OSError):
        cmdline = ""
    return {
        "pid": pid,
        "ppid": ppid,
        "starttime_ticks": start_ticks,
        "cpu_ticks": cpu_ticks,
        "rss_bytes": rss_kb * 1024,
        "threads": int(status.get("Threads", "0") or 0),
        "state": fields[0] if fields else "",
        "exe": exe,
        "cmdline": cmdline,
    }


def _children_by_parent() -> dict[int, list[int]]:
    children: dict[int, list[int]] = {}
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit():
            continue
        info = _proc_stat(int(item.name))
        if info is not None:
            children.setdefault(int(info["ppid"]), []).append(int(info["pid"]))
    return children


def _tree(root_pid: int) -> list[dict[str, Any]]:
    children = _children_by_parent()
    pids = [root_pid]
    seen = {root_pid}
    index = 0
    while index < len(pids):
        for child in children.get(pids[index], []):
            if child not in seen:
                seen.add(child)
                pids.append(child)
        index += 1
    result = []
    for pid in pids:
        info = _proc_stat(pid)
        if info is not None:
            result.append(info)
    return result


def _set_child_limits(cpu_set: set[int], nice_increment: int) -> None:
    if os.name != "posix":
        raise GuardError("BLOCKED_ENV: perf guard requires a POSIX /proc target")
    if not pathlib.Path("/proc").is_dir():
        raise GuardError("BLOCKED_ENV: /proc is unavailable")
    if cpu_set:
        available = set(os.sched_getaffinity(0))
        if not cpu_set.issubset(available):
            raise GuardError(
                f"BLOCKED_RESOURCE: requested CPUs {sorted(cpu_set)} are not all available"
            )
        os.sched_setaffinity(0, cpu_set)
    if nice_increment:
        os.nice(int(nice_increment))


def _signal_owned(proc: subprocess.Popen, root_pid: int, sig: int) -> list[int]:
    sent: list[int] = []
    for info in sorted(_tree(root_pid), key=lambda item: int(item["pid"]), reverse=True):
        pid = int(info["pid"])
        try:
            os.kill(pid, sig)
            sent.append(pid)
        except (ProcessLookupError, PermissionError, OSError):
            continue
    try:
        proc.send_signal(sig)
    except (ProcessLookupError, OSError):
        pass
    return sent


def _write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    if sys.platform != "linux" or not pathlib.Path("/proc").is_dir():
        raise GuardError("BLOCKED_ENV: perf guard requires Linux /proc")
    if not args.command:
        raise GuardError("a command is required after --")
    run_root = pathlib.Path(args.run_root).expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    def run_path(value: str, default_name: str) -> pathlib.Path:
        candidate = pathlib.Path(value) if value else pathlib.Path(default_name)
        if not candidate.is_absolute():
            candidate = run_root / candidate
        return _inside(run_root, candidate)

    output = run_path(args.output, "perf-guard.json")
    samples_path = run_path(args.samples, "resource-samples.jsonl")
    stdout_path = run_path(args.stdout, "command.stdout.log")
    cpu_set = _parse_cpu_list(args.cpu_list)
    if args.timeout_seconds <= 0 or args.sample_seconds <= 0:
        raise GuardError("timeout and sample interval must be positive")
    rss_limit = int(args.rss_limit_mb) * 1024 * 1024
    disk_limit = int(args.disk_limit_gb) * 1024 ** 3
    before_disk = shutil.disk_usage(run_root)
    start = time.monotonic()
    started_at = _utc_now()
    command = [str(item) for item in args.command]
    if args.ionice_class is not None and shutil.which("ionice"):
        command = ["ionice", "-c", str(args.ionice_class), *command]
    env = os.environ.copy()
    safe_env = {
        key: value
        for key, value in env.items()
        if key in {
            "MC_DUCKDB_OFFLINE_ONLY", "MC_DUCKDB_EXTENSION_DIR", "TMPDIR", "HOME",
            "XDG_CACHE_HOME", "PYTHONPYCACHEPREFIX", "DUCKDB_HOME", "MC_PYTHON_BIN",
        }
    }
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    samples: list[dict[str, Any]] = []
    ledger: dict[int, dict[str, Any]] = {}
    reason = "completed"
    guard_status = "RUNNING"
    proc: subprocess.Popen | None = None
    with stdout_path.open("w", encoding="utf-8") as stdout_handle:
        proc = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
            preexec_fn=lambda: _set_child_limits(cpu_set, args.nice_increment),
            text=False,
        )
        root_pid = int(proc.pid)
        first = _proc_stat(root_pid)
        if first is None:
            raise GuardError("BLOCKED_ENV: child disappeared before PID ledger capture")
        ledger[root_pid] = {
            "role": "guarded-command-root",
            "pid": root_pid,
            "ppid": first["ppid"],
            "starttime_ticks": first["starttime_ticks"],
            "exe": first["exe"],
            "cmdline": first["cmdline"],
        }
        with samples_path.open("w", encoding="utf-8") as sample_handle:
            while True:
                tree = _tree(root_pid)
                for item in tree:
                    pid = int(item["pid"])
                    ledger.setdefault(pid, {
                        "role": "guarded-descendant",
                        "pid": pid,
                        "ppid": item["ppid"],
                        "starttime_ticks": item["starttime_ticks"],
                        "exe": item["exe"],
                        "cmdline": item["cmdline"],
                    })
                rss = sum(int(item.get("rss_bytes") or 0) for item in tree)
                usage = shutil.disk_usage(run_root)
                disk_delta = max(0, (before_disk.total - before_disk.free) - (usage.total - usage.free))
                sample = {
                    "sampled_at": _utc_now(),
                    "elapsed_seconds": round(time.monotonic() - start, 3),
                    "root_pid": root_pid,
                    "process_count": len(tree),
                    "rss_bytes_aggregate": rss,
                    "disk_delta_bytes": disk_delta,
                    "processes": tree,
                }
                samples.append(sample)
                sample_handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
                sample_handle.flush()
                if rss_limit > 0 and rss > rss_limit:
                    reason = f"aggregate RSS exceeded {args.rss_limit_mb} MiB"
                    guard_status = "BLOCKED_RESOURCE"
                    break
                if disk_limit > 0 and disk_delta > disk_limit:
                    reason = f"Run disk delta exceeded {args.disk_limit_gb} GiB"
                    guard_status = "BLOCKED_RESOURCE"
                    break
                if time.monotonic() - start > args.timeout_seconds:
                    reason = f"timeout exceeded {args.timeout_seconds} seconds"
                    guard_status = "TIMEOUT"
                    break
                if proc.poll() is not None:
                    guard_status = "EXITED"
                    break
                time.sleep(args.sample_seconds)
        if guard_status in {"BLOCKED_RESOURCE", "TIMEOUT"} and proc.poll() is None:
            _signal_owned(proc, root_pid, signal.SIGTERM)
            try:
                proc.wait(timeout=args.termination_grace_seconds)
            except subprocess.TimeoutExpired:
                _signal_owned(proc, root_pid, signal.SIGKILL)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    reason += "; process did not exit after SIGKILL"
        exit_code = proc.poll()
    finished = time.monotonic()
    for pid, item in ledger.items():
        current = _proc_stat(pid)
        item["end_seen"] = current is None
        if current is not None:
            item["end_state"] = current["state"]
            item["end_rss_bytes"] = current["rss_bytes"]
    if guard_status == "EXITED":
        if exit_code == 0:
            status = "PASS"
        else:
            status = "FAIL"
            reason = f"guarded command exited with {exit_code}"
    else:
        status = guard_status
    report = {
        "status": status,
        "reason": reason,
        "exit_code": exit_code,
        "started_at": started_at,
        "duration_seconds": round(finished - start, 3),
        "run_root": str(run_root),
        "command": command,
        "env_paths": safe_env,
        "limits": {
            "timeout_seconds": args.timeout_seconds,
            "aggregate_rss_mb": args.rss_limit_mb,
            "run_disk_delta_gb": args.disk_limit_gb,
            "cpu_list": sorted(cpu_set),
            "nice_increment": args.nice_increment,
            "ionice_class": args.ionice_class,
        },
        "stdout": str(stdout_path),
        "resource_samples": str(samples_path),
        "sample_count": len(samples),
        "max_aggregate_rss_bytes": max(
            (int(item.get("rss_bytes_aggregate") or 0) for item in samples), default=0
        ),
        "max_disk_delta_bytes": max(
            (int(item.get("disk_delta_bytes") or 0) for item in samples), default=0
        ),
        "pid_ledger": list(ledger.values()),
        "termination": {
            "guard_owned_only": True,
            "no_pattern_kill": True,
            "all_recorded_pids_gone": all(item.get("end_seen") for item in ledger.values()),
        },
    }
    _write_json(output, report)
    print(f"PERF_GUARD_STATUS={status}")
    print(f"PERF_GUARD_OUTPUT={output}")
    return 0 if status == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", default="")
    parser.add_argument("--samples", default="")
    parser.add_argument("--stdout", default="")
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--sample-seconds", type=float, default=2)
    parser.add_argument("--rss-limit-mb", type=int, default=2048)
    parser.add_argument("--disk-limit-gb", type=int, default=5)
    parser.add_argument("--cpu-list", default="")
    parser.add_argument("--nice-increment", type=int, default=10)
    parser.add_argument("--ionice-class", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--termination-grace-seconds", type=float, default=15)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    try:
        return run(args)
    except GuardError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
