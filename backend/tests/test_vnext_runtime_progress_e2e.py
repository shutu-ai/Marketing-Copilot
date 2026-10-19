from __future__ import annotations

import json
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

from vnext.projection_writer import ProgressJournal
from vnext.runtime import VNextRuntime
from vnext.smpp_transport import (
    BIND_TRANSCEIVER,
    ENQUIRE_LINK,
    RESPONSE_MASK,
    SUBMIT_SM,
)


class _LoopbackServer:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(16)
        self.socket.settimeout(0.2)
        self.port = self.socket.getsockname()[1]
        self.received = 0
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._accept_loop, daemon=True)

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = connection.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def _recv_pdu(self, connection: socket.socket):
        header = self._recv_exact(connection, 16)
        length, command, status, sequence = struct.unpack("!IIII", header)
        body = self._recv_exact(connection, max(0, length - 16))
        return command, status, sequence, body

    def _handle(self, connection: socket.socket):
        try:
            command, _status, sequence, _body = self._recv_pdu(connection)
            if command != BIND_TRANSCEIVER:
                return
            connection.sendall(
                struct.pack("!IIII", 22, BIND_TRANSCEIVER | RESPONSE_MASK, 0, sequence)
                + b"LOCAL\x00"
            )
            while not self.stop_event.is_set():
                command, _status, sequence, _body = self._recv_pdu(connection)
                if command == SUBMIT_SM:
                    self.received += 1
                    connection.sendall(
                        struct.pack("!IIII", 19, SUBMIT_SM | RESPONSE_MASK, 0, sequence)
                        + b"id\x00"
                    )
                elif command == ENQUIRE_LINK:
                    connection.sendall(
                        struct.pack(
                            "!IIII", 16, ENQUIRE_LINK | RESPONSE_MASK, 0, sequence
                        )
                    )
                else:
                    return
        except (ConnectionError, OSError):
            return
        finally:
            try:
                connection.close()
            except OSError:
                pass

    def _accept_loop(self):
        while not self.stop_event.is_set():
            try:
                connection, _address = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(
                target=self._handle, args=(connection,), daemon=True
            ).start()

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        try:
            self.socket.close()
        except OSError:
            pass
        self.thread.join(timeout=5)


def test_production_runtime_durably_records_two_campaigns(tmp_path):
    server = _LoopbackServer().start()
    runtime = VNextRuntime(
        tmp_path / "runtime",
        {
            1: {
                "route_id": 1,
                "host": "127.0.0.1",
                "port": server.port,
                "system_id": "test",
                "password": "test",
                "source_addr": "VNEXT",
                "bind_type": "transceiver",
                "pool_size": 1,
                "window_size": 50,
                "tps": 0,
                "tps_scope": "per_bind",
                "timeout": 5,
            }
        },
        worker_count=1,
        virtual_partition_count=8,
        max_inflight=8,
        prefetch_per_partition=8,
        projection_queue_size=1024,
        projection_batch_size=64,
        status_interval_seconds=0.25,
        progress_aggregate_interval_seconds=0.2,
        prewarm_smpp_binds=True,
    )
    try:
        runtime.start()
        for campaign_id in (101, 102):
            result = runtime.enqueue_chunk(
                [
                    {
                        "msisdn": f"2519000{campaign_id}{index:03d}",
                        "message": f"campaign-{campaign_id}-{index}",
                    }
                    for index in range(4)
                ],
                campaign_type="batch",
                campaign_id=campaign_id,
                route_id=1,
                source_id=f"source-{campaign_id}",
            )
            assert result.accepted == 4
        deadline = time.monotonic() + 20.0
        status = runtime.status()
        while time.monotonic() < deadline:
            status = runtime.status()
            workers = status.get("worker_progress") or []
            idle = bool(workers) and status["spool"]["pending_records"] == 0 and all(
                not bool(item.get("active"))
                and int((item.get("snapshot") or {}).get("inflight", 0)) == 0
                and all(
                    int(transport.get("window_inflight", 0)) == 0
                    for transport in (item.get("snapshot") or {}).get("transports", [])
                )
                for item in workers
            )
            if idle:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(
                {
                    "spool": status["spool"],
                    "workers": [
                        {
                            "active": item.get("active"),
                            "snapshot": item.get("snapshot"),
                        }
                        for item in (status.get("worker_progress") or [])
                    ],
                    "projection": status.get("projection"),
                }
            )
        assert status["spool"]["pending_records"] == 0, status["spool"]
        assert server.received == 8

        journals = sorted((tmp_path / "runtime" / "progress").glob("worker-*.jsonl"))
        assert journals
        events = []
        for path in journals:
            events.extend(ProgressJournal(path).read())
        socket_events = [
            event for event in events if event.get("state") == "socket_written"
        ]
        response_events = [
            event for event in events if str(event.get("state", "")).startswith("submit_sm_resp_")
        ]
        assert len(socket_events) == 8
        assert len({event["event_id"] for event in socket_events}) == 8
        assert len(response_events) == 8
        assert {event["campaign_id"] for event in events} == {101, 102}

        aggregates = sorted(
            (tmp_path / "runtime" / "progress").glob("*.progress-aggregate.json")
        )
        assert len(aggregates) == 1
        aggregate_deadline = time.monotonic() + 10.0
        totals = {101: 0, 102: 0}
        while time.monotonic() < aggregate_deadline:
            totals = {101: 0, 102: 0}
            for path in aggregates:
                value = json.loads(path.read_text(encoding="utf-8"))
                assert value["complete"] is True
                for campaign in value["campaigns"]:
                    totals[int(campaign["campaign_id"])] += int(
                        campaign.get("response_success", 0)
                    )
            if totals == {101: 4, 102: 4}:
                break
            time.sleep(0.05)
        assert totals == {101: 4, 102: 4}

        projection_path = (
            tmp_path / "runtime" / "projection" / "projection-writer-status.json"
        )
        projection_deadline = time.monotonic() + 10.0
        projection_status = {}
        while time.monotonic() < projection_deadline:
            projection_status = json.loads(
                projection_path.read_text(encoding="utf-8")
            )
            if (
                int(projection_status.get("applied", 0) or 0) >= 8
                and int(projection_status.get("journal_backlog_bytes", 0) or 0) == 0
                and int(projection_status.get("journal_pending_events", 0) or 0) == 0
            ):
                break
            time.sleep(0.05)
        assert projection_status["journal_backlog_bytes"] == 0
        assert projection_status["journal_pending_events"] == 0
        assert projection_status["applied"] >= 8

        transport_statuses = [
            transport
            for item in (status.get("worker_progress") or [])
            for transport in (item.get("snapshot") or {}).get("transports", [])
        ]
        assert transport_statuses
        assert all(transport["connected"] for transport in transport_statuses)

        worker_statuses = sorted(
            (tmp_path / "runtime" / "progress").glob("worker-*.status.json")
        )
        assert len(worker_statuses) == 1
        for path in worker_statuses:
            value = json.loads(path.read_text(encoding="utf-8"))
            progress = value["snapshot"]["progress"]
            assert progress["pending_durability"] == 0
            assert progress["writer_errors"] == 0
            assert progress["accepting"] is True
            assert value["snapshot"]["healthy"] is True
        cli = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parents[1] / "tools" / "vnext_stats.py"),
                "--root", str(runtime.root),
                "--once", "--json",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
        assert cli.returncode == 0, cli.stderr
        cli_snapshot = json.loads(cli.stdout)
        assert cli_snapshot["runtime"]["state"] == "RUNNING"
        assert cli_snapshot["smpp"]["written_pdus"] >= 4
        assert cli_snapshot["campaign_counter_source"] == "durable-aggregate"
        assert cli_snapshot["health"] in {"OK", "STALE"}
    finally:
        runtime.stop()
        server.stop()
