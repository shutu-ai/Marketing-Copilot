"""Independent SMPP load generator for the local SMSC simulator.

This tool deliberately speaks the loopback SMPP socket directly.  It does not
start Marketing Copilot, does not use an application route limit, and reports
client-observed response latency separately from the simulator's own counters.
It is intended for SIM-1..SIM-6 evidence and keeps the simulator process
optional so an existing fault profile can also be measured.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time
import signal
import uuid
from typing import Any

from smsc_simulator import (
    BIND_TRANSCEIVER,
    HEADER,
    RESPONSE_MASK,
    SUBMIT_SM,
    UNBIND,
    cstring,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_bind(sequence: int, system_id: str, password: str) -> bytes:
    body = b"".join([
        cstring(system_id), cstring(password), cstring(""),
        bytes([0x34, 0, 0]), cstring(""),
    ])
    return HEADER.pack(16 + len(body), BIND_TRANSCEIVER, 0, sequence) + body


def build_submit(
    sequence: int,
    source: str,
    destination: str,
    message: str,
    *,
    data_coding: int = 0,
    flash: bool = False,
    multipart_parts: int = 1,
    part: int = 1,
    reference: int = 1,
) -> bytes:
    if data_coding == 8:
        encoded = message.encode("utf-16-be")
    else:
        encoded = message.encode("ascii", errors="replace")
    esm_class = 0x20 if flash else 0
    if multipart_parts > 1:
        udh = bytes([5, 0, 3, reference & 0xFF, multipart_parts & 0xFF, part & 0xFF])
        encoded = udh + encoded
        esm_class |= 0x40
    if len(encoded) > 255:
        raise ValueError("short_message exceeds SMPP single-PDU limit")
    body = b"".join([
        cstring(""), bytes([5, 0]), cstring(source),
        bytes([1, 1]), cstring(destination),
        bytes([esm_class, 0, 0]), cstring(""), cstring(""),
        bytes([0, 0, data_coding, 0, len(encoded)]), encoded,
    ])
    return HEADER.pack(16 + len(body), SUBMIT_SM, 0, sequence) + body


async def read_pdu(reader: asyncio.StreamReader) -> tuple[int, int, int, bytes]:
    header = await reader.readexactly(16)
    length, command_id, status, sequence = HEADER.unpack(header)
    if length < 16 or length > 1024 * 1024:
        raise ValueError(f"invalid PDU length {length}")
    return command_id, status, sequence, await reader.readexactly(length - 16)


class ClientConnection:
    def __init__(self, args: argparse.Namespace, connection_id: int):
        self.args = args
        self.connection_id = connection_id
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.next_sequence = 2
        self.pending: dict[int, float] = {}
        self.rtts_ms: deque[float] = deque(maxlen=500_000)
        self.sent = 0
        self.responses = 0
        self.success = 0
        self.failed = 0
        self.unknown_responses = 0
        self.errors: list[str] = []
        self.wake = asyncio.Event()
        self.reader_task: asyncio.Task | None = None

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self.args.host, self.args.port
        )
        self.writer.write(build_bind(1, self.args.system_id, self.args.password))
        await self.writer.drain()
        command_id, status, sequence, _ = await asyncio.wait_for(
            read_pdu(self.reader), timeout=self.args.timeout
        )
        if command_id != (BIND_TRANSCEIVER | RESPONSE_MASK) or sequence != 1 or status != 0:
            raise RuntimeError(
                f"bind failed connection={self.connection_id} command=0x{command_id:08x} "
                f"sequence={sequence} status=0x{status:08x}"
            )

    async def _read_responses(self) -> None:
        assert self.reader is not None
        try:
            while True:
                command_id, status, sequence, _ = await read_pdu(self.reader)
                if command_id != (SUBMIT_SM | RESPONSE_MASK):
                    continue
                started = self.pending.pop(sequence, None)
                if started is None:
                    self.unknown_responses += 1
                else:
                    self.rtts_ms.append((time.perf_counter() - started) * 1000.0)
                self.responses += 1
                if status == 0:
                    self.success += 1
                else:
                    self.failed += 1
                self.wake.set()
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            if self.pending:
                self.errors.append(f"response reader ended with {len(self.pending)} pending: {exc}")
            self.wake.set()

    def _message_for(self, ordinal: int) -> tuple[str, int, int, int]:
        data_coding = 8 if self.args.unicode else 0
        message = self.args.message
        if self.args.multipart_parts > 1:
            part = (ordinal % self.args.multipart_parts) + 1
            reference = (ordinal // self.args.multipart_parts) % 256
            return message, data_coding, part, reference
        return message, data_coding, 1, ordinal % 256

    async def run(self, target_count: int | None, deadline: float | None) -> None:
        assert self.writer is not None
        self.reader_task = asyncio.create_task(
            self._read_responses(), name=f"smsc-benchmark-reader-{self.connection_id}"
        )
        try:
            while (
                (target_count is None or self.sent < target_count)
                and (deadline is None or time.monotonic() < deadline)
            ):
                while (
                    len(self.pending) < self.args.window
                    and (target_count is None or self.sent < target_count)
                    and (deadline is None or time.monotonic() < deadline)
                ):
                    ordinal = self.sent
                    message, data_coding, part, reference = self._message_for(ordinal)
                    sequence = self.next_sequence
                    self.next_sequence = (self.next_sequence + 1) & 0x7FFFFFFF or 2
                    packet = build_submit(
                        sequence,
                        self.args.source,
                        f"{self.args.destination_prefix}{self.connection_id:02d}{ordinal:08d}",
                        message,
                        data_coding=data_coding,
                        flash=self.args.flash,
                        multipart_parts=self.args.multipart_parts,
                        part=part,
                        reference=reference,
                    )
                    self.pending[sequence] = time.perf_counter()
                    self.writer.write(packet)
                    self.sent += 1
                await self.writer.drain()
                if self.pending:
                    self.wake.clear()
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.wake.wait(), timeout=0.25)
            # Drain responses already in flight without extending the measured
            # send phase indefinitely when a fault profile intentionally drops.
            drain_deadline = time.monotonic() + max(0.0, self.args.drain_timeout)
            while self.pending and time.monotonic() < drain_deadline:
                self.wake.clear()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), timeout=0.25)
        except Exception as exc:
            self.errors.append(f"connection run failed: {type(exc).__name__}: {exc}")
        finally:
            if self.reader_task:
                self.reader_task.cancel()
                await asyncio.gather(self.reader_task, return_exceptions=True)
                self.reader_task = None
            with suppress(Exception):
                self.writer.write(HEADER.pack(16, UNBIND, 0, self.next_sequence))
                await self.writer.drain()
            self.writer.close()
            with suppress(Exception):
                await self.writer.wait_closed()


def percentiles(values: list[float] | deque[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None}
    ordered = sorted(values)

    def at(percentile: float) -> float:
        index = (len(ordered) - 1) * percentile
        lower = int(index)
        upper = min(len(ordered) - 1, lower + 1)
        fraction = index - lower
        return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)

    return {"p50": at(0.50), "p95": at(0.95), "p99": at(0.99)}


def resource_sample() -> dict[str, float | int]:
    result: dict[str, float | int] = {"sampled_at": time.time(), "rss_bytes": 0, "cpu_seconds": time.process_time()}
    try:
        import resource
        result["rss_bytes"] = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if platform.system().lower() != "windows":
            result["rss_bytes"] *= 1024
    except (ImportError, OSError):
        pass
    return result


async def event_loop_lag(stop: asyncio.Event, samples: deque[float]) -> None:
    interval = 0.1
    expected = time.perf_counter() + interval
    while not stop.is_set():
        await asyncio.sleep(interval)
        now = time.perf_counter()
        samples.append(max(0.0, (now - expected) * 1000.0))
        expected = now + interval


async def sample_resources(stop: asyncio.Event, samples: list[dict]) -> None:
    while not stop.is_set():
        samples.append(resource_sample())
        await asyncio.sleep(1.0)


async def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    loop_lag: deque[float] = deque(maxlen=20_000)
    resources: list[dict] = []
    stop_sampling = asyncio.Event()
    lag_task = asyncio.create_task(event_loop_lag(stop_sampling, loop_lag), name="smsc-loop-lag")
    resource_task = asyncio.create_task(sample_resources(stop_sampling, resources), name="smsc-resources")
    connections = [ClientConnection(args, index) for index in range(args.pool_size)]
    try:
        await asyncio.gather(*(connection.connect() for connection in connections))
        deadline = time.monotonic() + args.duration if args.duration > 0 else None
        # A duration is a steady-state benchmark contract.  It deliberately
        # overrides the count default so ``--duration 30`` cannot silently
        # finish after the CLI's 10,000-message default in under a second.
        if args.duration > 0:
            targets = [None] * args.pool_size
        elif args.count > 0:
            base, extra = divmod(args.count, args.pool_size)
            targets = [base + (1 if index < extra else 0) for index in range(args.pool_size)]
        else:
            targets = [None] * args.pool_size
        await asyncio.gather(*(
            connection.run(targets[index], deadline)
            for index, connection in enumerate(connections)
        ))
    finally:
        stop_sampling.set()
        lag_task.cancel()
        resource_task.cancel()
        await asyncio.gather(lag_task, resource_task, return_exceptions=True)
    elapsed = max(0.001, time.perf_counter() - started)
    rtts = deque()
    errors: list[str] = []
    sent = responses = success = failed = unknown = 0
    pending = 0
    for connection in connections:
        rtts.extend(connection.rtts_ms)
        errors.extend(connection.errors)
        sent += connection.sent
        responses += connection.responses
        success += connection.success
        failed += connection.failed
        unknown += connection.unknown_responses
        pending += len(connection.pending)
    max_rss = max((int(item.get("rss_bytes") or 0) for item in resources), default=0)
    return {
        "status": "PASS" if sent and not errors and pending == 0 and responses == sent else "FAIL",
        "started_at": utc_now(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "host": args.host,
            "port": args.port,
            "loopback_only": args.host in {"127.0.0.1", "localhost", "::1"},
        },
        "configuration": {
            "pool_size": args.pool_size,
            "window_size": args.window,
            "count_requested": args.count,
            "duration_seconds": args.duration,
            "drain_timeout_seconds": args.drain_timeout,
            "simulator_mode": args.simulator_mode,
            "simulator_tps": args.simulator_tps,
            "route_tps": None,
            "dlr_enabled": False,
            "unicode": args.unicode,
            "flash": args.flash,
            "multipart_parts": args.multipart_parts,
        },
        "counters": {
            "client_submit_sm": sent,
            "client_submit_sm_resp": responses,
            "client_submit_sm_resp_success": success,
            "client_submit_sm_resp_failed": failed,
            "client_unknown_responses": unknown,
            "client_pending_at_close": pending,
        },
        "throughput": {
            "client_submit_sm_per_second": round(sent / elapsed, 3),
            "client_response_per_second": round(responses / elapsed, 3),
            "elapsed_seconds": round(elapsed, 3),
        },
        "latency_ms": {
            "client_socket_to_response_observed": percentiles(rtts),
            "sample_count": len(rtts),
        },
        "resources": {
            "max_rss_bytes": max_rss,
            "samples": len(resources),
            "event_loop_lag_ms": percentiles(loop_lag),
        },
        "errors": errors,
    }


class SpawnedSimulator:
    def __init__(self, args: argparse.Namespace, root: Path):
        self.args = args
        self.root = root
        self.event_log = root / "smsc.events.jsonl"
        self.metrics = root / "smsc.metrics.json"
        self.log = root / "smsc.log"
        self.process: subprocess.Popen | None = None
        self.handle = None

    def start(self) -> None:
        command = [
            sys.executable, str(Path(__file__).resolve().with_name("smsc_simulator.py")),
            "--host", self.args.host, "--port", str(self.args.port),
            "--system-id", self.args.system_id, "--password", self.args.password,
            "--simulator-mode", self.args.simulator_mode,
            "--simulator-tps", str(self.args.simulator_tps),
            "--simulator-response-delay", str(self.args.simulator_response_delay),
            "--simulator-audit", self.args.audit_mode,
            "--event-log", str(self.event_log),
            "--metrics-output", str(self.metrics),
        ]
        self.root.mkdir(parents=True, exist_ok=True)
        self.handle = self.log.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            command,
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=self.handle,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"simulator exited with code {self.process.returncode}")
            try:
                with socket.create_connection((self.args.host, self.args.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError("timed out waiting for SMSC simulator")

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            try:
                if platform.system().lower() == "windows":
                    self.process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self.process.terminate()
                self.process.wait(timeout=10)
            except (subprocess.TimeoutExpired, OSError, ValueError):
                with suppress(Exception):
                    self.process.kill()
                    self.process.wait(timeout=5)
        if self.handle:
            self.handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--system-id", default="test")
    parser.add_argument("--password", default="test")
    parser.add_argument("--pool-size", type=int, default=1)
    parser.add_argument("--window", type=int, default=50)
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--duration", type=float, default=0.0,
                        help="steady-state duration; when set, count is ignored")
    parser.add_argument("--drain-timeout", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--message", default="Marketing Copilot simulator benchmark")
    parser.add_argument("--source", default="E2E")
    parser.add_argument("--destination-prefix", default="251900")
    parser.add_argument("--unicode", action="store_true")
    parser.add_argument("--flash", action="store_true")
    parser.add_argument("--multipart-parts", type=int, default=1)
    parser.add_argument("--simulator-mode", choices=("fast", "fault"), default="fast")
    parser.add_argument("--simulator-tps", type=float, default=0.0)
    parser.add_argument("--simulator-response-delay", type=float, default=0.0)
    parser.add_argument("--audit-mode", choices=("fast", "full", "off"), default="fast")
    parser.add_argument("--no-spawn", action="store_true",
                        help="connect to an already running simulator")
    parser.add_argument("--run-root", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args()
    args.pool_size = max(1, int(args.pool_size))
    args.window = max(1, int(args.window))
    args.count = max(0, int(args.count))
    args.multipart_parts = max(1, int(args.multipart_parts))
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("the benchmark is loopback-only")
    if args.count == 0 and args.duration <= 0:
        parser.error("set --count or --duration")
    if args.port == 0:
        args.port = free_port()

    temp_root = None
    if args.run_root:
        root = Path(args.run_root).expanduser().resolve() / f"smsc-benchmark-{uuid.uuid4().hex[:8]}"
    else:
        temp_root = Path(tempfile.mkdtemp(prefix="smsc-benchmark-"))
        root = temp_root
    spawned = None
    try:
        if not args.no_spawn:
            spawned = SpawnedSimulator(args, root)
            spawned.start()
        result = asyncio.run(run_benchmark(args))
        result["simulator"] = {}
        if spawned:
            spawned.stop()
            try:
                result["simulator"] = json.loads(spawned.metrics.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                result["simulator"] = {"audit_status": "NOT_RECORDED", "error": str(exc)}
            result["artifacts"] = {
                "root": str(root),
                "event_log": str(spawned.event_log),
                "metrics": str(spawned.metrics),
                "log": str(spawned.log),
            }
        output = Path(args.output) if args.output else root / "benchmark.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("status") == "PASS" else 2
    finally:
        if spawned:
            spawned.stop()
        if temp_root and not args.keep_artifacts:
            with suppress(Exception):
                import shutil
                shutil.rmtree(temp_root)


if __name__ == "__main__":
    raise SystemExit(main())
