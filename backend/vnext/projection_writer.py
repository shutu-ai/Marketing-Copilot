"""Dedicated batched progress projection writer."""

from __future__ import annotations

import json
import multiprocessing
import os
import queue
import time
import contextlib
from collections import deque
from pathlib import Path


class ProjectionWriterBusy(RuntimeError):
    pass


class ProjectionBackpressure(RuntimeError):
    pass


class ProgressJournal:
    """Compact append-only progress events, independent of DuckDB."""

    def __init__(self, path: str | os.PathLike[str], *, max_event_bytes: int = 64 * 1024):
        self.path = Path(path).resolve()
        self.max_event_bytes = max(1024, int(max_event_bytes))
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: dict) -> None:
        encoded = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(encoded) > self.max_event_bytes:
            raise ValueError("progress event exceeds bounded size")
        with self.path.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

    def append_many(self, events: list[dict]) -> None:
        if not events:
            return
        encoded = []
        for event in events:
            line = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            if len(line) > self.max_event_bytes:
                raise ValueError("progress event exceeds bounded size")
            encoded.append(line)
        with self.path.open("ab") as handle:
            for line in encoded:
                handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def read(self) -> list[dict]:
        if not self.path.exists():
            return []
        result = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    result.append(json.loads(line))
        return result


class ProjectionWriter:
    """A bounded writer object; in production it is owned by one process."""

    def __init__(self, db_path: str | os.PathLike[str], *, queue_size: int = 8192,
                 batch_size: int = 128, retry_seconds: float = 0.05):
        self.db_path = str(Path(db_path).resolve())
        self.queue_size = max(1, int(queue_size))
        self.batch_size = max(1, int(batch_size))
        self.retry_seconds = max(0.001, float(retry_seconds))
        self._queue: deque[dict] = deque(maxlen=self.queue_size)
        self._conn = None
        self._paused_until = 0.0
        self._applied = 0
        self._duplicates = 0
        self._last_event_at = 0.0
        self._last_applied_at = 0.0

    def start(self) -> None:
        import duckdb
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(self.db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS vnext_progress_events (
                event_id VARCHAR PRIMARY KEY,
                partition_id INTEGER NOT NULL,
                ordinal BIGINT NOT NULL,
                campaign_type VARCHAR NOT NULL,
                campaign_id BIGINT NOT NULL,
                route_id BIGINT NOT NULL,
                normalized_msisdn VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                parts_written INTEGER NOT NULL,
                parts_total INTEGER NOT NULL,
                message VARCHAR NOT NULL DEFAULT '',
                error VARCHAR NOT NULL DEFAULT '',
                created_at DOUBLE NOT NULL
            )
        """)
        self._conn.execute(
            "ALTER TABLE vnext_progress_events ADD COLUMN IF NOT EXISTS message VARCHAR DEFAULT ''"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS vnext_projection (
                campaign_type VARCHAR NOT NULL,
                campaign_id BIGINT NOT NULL,
                partition_id INTEGER NOT NULL,
                route_id BIGINT NOT NULL,
                queued BIGINT NOT NULL DEFAULT 0,
                taken BIGINT NOT NULL DEFAULT 0,
                socket_written BIGINT NOT NULL DEFAULT 0,
                write_failed BIGINT NOT NULL DEFAULT 0,
                unknown BIGINT NOT NULL DEFAULT 0,
                cancelled BIGINT NOT NULL DEFAULT 0,
                response_success BIGINT NOT NULL DEFAULT 0,
                response_error BIGINT NOT NULL DEFAULT 0,
                last_ordinal BIGINT NOT NULL DEFAULT 0,
                updated_at DOUBLE NOT NULL,
                PRIMARY KEY(campaign_type,campaign_id,partition_id,route_id)
            )
        """)
        self._conn.execute(
            "ALTER TABLE vnext_projection ADD COLUMN IF NOT EXISTS cancelled BIGINT DEFAULT 0"
        )

    def stop(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def submit(self, event: dict) -> None:
        if len(self._queue) >= self.queue_size:
            raise ProjectionBackpressure("projection queue capacity exhausted")
        self._queue.append(dict(event))
        self._last_event_at = max(self._last_event_at, float(event.get("created_at") or time.time()))

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, time.monotonic() + max(0.0, float(seconds)))

    def _apply(self, event: dict) -> bool:
        if self._conn is None:
            raise RuntimeError("projection writer is not started")
        event_id = str(event["event_id"])
        inserted = self._conn.execute(
            """INSERT INTO vnext_progress_events(
                event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                normalized_msisdn,state,parts_written,parts_total,message,error,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(event_id) DO NOTHING RETURNING event_id""",
            (
                event_id, int(event["partition_id"]), int(event["ordinal"]),
                str(event["campaign_type"]), int(event["campaign_id"]), int(event["route_id"]),
                str(event["normalized_msisdn"]), str(event["state"]),
                int(event.get("parts_written", 0)), int(event.get("parts_total", 0)),
                str(event.get("message", "")),
                str(event.get("error", "")), float(event.get("created_at") or time.time()),
            ),
        ).fetchone()
        if not inserted:
            self._duplicates += 1
            return False
        state = str(event.get("state", ""))
        values = {
            "queued": int(event.get("queued", 0)),
            "taken": int(event.get("taken", 0)),
            "socket_written": int(event.get("socket_written", state == "socket_written")),
            "write_failed": int(event.get("write_failed", state == "write_failed")),
            "unknown": int(event.get("unknown", state == "unknown")),
            "cancelled": int(event.get("cancelled", state == "cancelled")),
            "response_success": int(event.get("response_success", 0)),
            "response_error": int(event.get("response_error", 0)),
        }
        self._conn.execute(
            """INSERT INTO vnext_projection(
                campaign_type,campaign_id,partition_id,route_id,queued,taken,
                socket_written,write_failed,unknown,cancelled,response_success,response_error,
                last_ordinal,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(campaign_type,campaign_id,partition_id,route_id) DO UPDATE SET
                queued=vnext_projection.queued+excluded.queued,
                taken=vnext_projection.taken+excluded.taken,
                socket_written=vnext_projection.socket_written+excluded.socket_written,
                write_failed=vnext_projection.write_failed+excluded.write_failed,
                unknown=vnext_projection.unknown+excluded.unknown,
                cancelled=vnext_projection.cancelled+excluded.cancelled,
                response_success=vnext_projection.response_success+excluded.response_success,
                response_error=vnext_projection.response_error+excluded.response_error,
                last_ordinal=GREATEST(vnext_projection.last_ordinal,excluded.last_ordinal),
                updated_at=excluded.updated_at""",
            (
                str(event["campaign_type"]), int(event["campaign_id"]), int(event["partition_id"]),
                int(event["route_id"]), values["queued"], values["taken"],
                values["socket_written"], values["write_failed"], values["unknown"],
                values["cancelled"],
                values["response_success"], values["response_error"], int(event["ordinal"]),
                float(event.get("created_at") or time.time()),
            ),
        )
        self._applied += 1
        self._last_applied_at = time.time()
        return True

    def flush(self, *, max_events: int | None = None) -> int:
        if time.monotonic() < self._paused_until:
            raise ProjectionWriterBusy("projection writer is paused")
        if self._conn is None:
            self.start()
        limit = self.batch_size if max_events is None else max(0, int(max_events))
        batch = []
        while self._queue and len(batch) < limit:
            batch.append(self._queue.popleft())
        if not batch:
            return 0
        try:
            self._conn.execute("BEGIN TRANSACTION")
            for event in batch:
                self._apply(event)
            self._conn.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                self._conn.execute("ROLLBACK")
            for event in reversed(batch):
                self._queue.appendleft(event)
            raise
        return len(batch)

    def replay(self, journal: ProgressJournal) -> int:
        events = journal.read()
        for event in events:
            self.submit(event)
        total = 0
        while self._queue:
            total += self.flush()
        return total

    def snapshot(self) -> dict:
        return {
            "db_path": self.db_path,
            "queue_depth": len(self._queue),
            "queue_capacity": self.queue_size,
            "applied": self._applied,
            "duplicates": self._duplicates,
            "projection_lag_seconds": max(0.0, self._last_event_at - self._last_applied_at) if self._last_applied_at else 0.0,
            "last_applied_at": self._last_applied_at,
        }


def _process_main(db_path: str, input_queue, stop_event, status_path: str, batch_size: int) -> None:
    writer = ProjectionWriter(db_path, batch_size=batch_size)
    writer.start()
    try:
        while True:
            try:
                event = input_queue.get(timeout=0.05)
            except queue.Empty:
                # ``stop_event`` is only a shutdown hint.  The parent places a
                # sentinel after all events, and the sentinel is the durable
                # FIFO boundary that tells this process the IPC queue has
                # drained.  Breaking on a transient empty read after the hint
                # races the multiprocessing feeder thread and silently drops
                # the tail of a batch.
                continue
            if event is None:
                break
            writer.submit(event)
            if len(writer._queue) >= writer.batch_size:
                writer.flush()
            Path(status_path).write_text(json.dumps(writer.snapshot(), sort_keys=True), encoding="utf-8")
        while writer._queue:
            writer.flush()
        Path(status_path).write_text(json.dumps(writer.snapshot(), sort_keys=True), encoding="utf-8")
    finally:
        writer.stop()


class ProjectionWriterProcess:
    """Multiprocess owner wrapper used by V4/runtime integration."""

    def __init__(self, root: str | os.PathLike[str], *, db_name: str = "vnext-projection.duckdb",
                 queue_size: int = 8192, batch_size: int = 128):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / db_name
        self.status_path = self.root / "projection-writer-status.json"
        context = multiprocessing.get_context()
        self.queue = context.Queue(maxsize=max(1, int(queue_size)))
        self.stop_event = context.Event()
        self.process = context.Process(
            target=_process_main,
            args=(str(self.db_path), self.queue, self.stop_event, str(self.status_path), max(1, int(batch_size))),
            name="mc-vnext-projection-writer",
            daemon=True,
        )

    def start(self) -> None:
        self.process.start()

    def submit(self, event: dict, *, timeout: float = 0.0) -> None:
        try:
            self.queue.put(dict(event), timeout=max(0.0, float(timeout)))
        except queue.Full as exc:
            raise ProjectionBackpressure("projection writer IPC queue is full") from exc

    def stop(self, timeout: float = 30.0) -> None:
        self.stop_event.set()
        deadline = time.monotonic() + max(0.1, float(timeout))
        while True:
            try:
                self.queue.put(None, timeout=0.05)
                break
            except queue.Full:
                if time.monotonic() >= deadline:
                    break
        self.process.join(timeout=max(0.0, float(timeout)))
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=2.0)

    def snapshot(self) -> dict:
        if not self.status_path.exists():
            return {"queue_depth": None, "alive": self.process.is_alive()}
        try:
            value = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            value = {}
        return {**value, "alive": self.process.is_alive()}
