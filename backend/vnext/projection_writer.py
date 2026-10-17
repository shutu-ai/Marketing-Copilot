"""Dedicated batched progress projection writer."""

from __future__ import annotations

import json
import multiprocessing
import os
import queue
import threading
import time
import contextlib
from collections import deque
from pathlib import Path

from .diagnostics import enable_native_fault_capture, write_process_metadata


class ProjectionWriterBusy(RuntimeError):
    pass


class ProjectionBackpressure(RuntimeError):
    pass


class ProgressJournal:
    """Compact append-only progress events, independent of DuckDB."""

    def __init__(self, path: str | os.PathLike[str], *, max_event_bytes: int = 64 * 1024,
                 max_bytes: int = 512 * 1024 * 1024):
        self.path = Path(path).resolve()
        self.max_event_bytes = max(1024, int(max_event_bytes))
        self.max_bytes = max(self.max_event_bytes, int(max_bytes))
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def size(self) -> int:
        try:
            return int(self.path.stat().st_size)
        except FileNotFoundError:
            return 0

    def can_append(self, event: dict) -> bool:
        encoded = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        return len(encoded) <= self.max_event_bytes and self.size() + len(encoded) <= self.max_bytes

    def append(self, event: dict) -> None:
        encoded = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(encoded) > self.max_event_bytes:
            raise ValueError("progress event exceeds bounded size")
        if self.size() + len(encoded) > self.max_bytes:
            raise ProjectionBackpressure("progress journal capacity exhausted")
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
        if self.size() + sum(len(line) for line in encoded) > self.max_bytes:
            raise ProjectionBackpressure("progress journal capacity exhausted")
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

    def read_from(self, offset: int = 0, *, max_events: int = 256) -> tuple[list[dict], int]:
        """Read complete journal lines after *offset* without consuming a partial tail."""

        if not self.path.exists():
            return [], max(0, int(offset))
        safe_offset = max(0, int(offset))
        events: list[dict] = []
        next_offset = safe_offset
        limit = max(1, int(max_events))
        with self.path.open("rb") as handle:
            size = int(self.path.stat().st_size)
            if safe_offset > size:
                safe_offset = 0
                next_offset = 0
            handle.seek(safe_offset)
            while len(events) < limit:
                line = handle.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    break
                next_offset = handle.tell()
                if line.strip():
                    events.append(json.loads(line.decode("utf-8")))
        return events, next_offset


class ProjectionWriter:
    """A bounded writer object; in production it is owned by one process."""

    def __init__(self, db_path: str | os.PathLike[str], *, queue_size: int = 8192,
                 batch_size: int = 512, retry_seconds: float = 0.05,
                 flush_interval_seconds: float = 0.1):
        self.db_path = str(Path(db_path).resolve())
        self.queue_size = max(1, int(queue_size))
        self.batch_size = max(1, int(batch_size))
        self.retry_seconds = max(0.001, float(retry_seconds))
        self.flush_interval_seconds = min(5.0, max(0.01, float(flush_interval_seconds)))
        self._queue: deque[dict] = deque(maxlen=self.queue_size)
        self._conn = None
        self._paused_until = 0.0
        self._applied = 0
        self._duplicates = 0
        self._last_event_at = 0.0
        self._last_applied_at = 0.0
        self._last_flush_monotonic = time.monotonic()
        self._sql_statements = 0
        self._last_batch_new = 0
        self._last_batch_duplicates = 0

    def _table_columns(self, table: str) -> set[str]:
        try:
            rows = self._conn.execute(f"DESCRIBE {table}").fetchall()
        except Exception:
            return set()
        return {str(row[0]) for row in rows}

    def start(self) -> None:
        import duckdb
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(self.db_path)
        legacy_projection = False
        projection_columns = self._table_columns("vnext_projection")
        if projection_columns and "business_day" not in projection_columns:
            # Preserve an old projection as an explicitly unattributed
            # compatibility source, then create the day-isolated table.
            self._conn.execute("ALTER TABLE vnext_projection RENAME TO vnext_projection_legacy")
            legacy_projection = True
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
                created_at DOUBLE NOT NULL,
                business_day VARCHAR NOT NULL DEFAULT '',
                expires_at DOUBLE NOT NULL DEFAULT 0
            )
        """)
        self._conn.execute(
            "ALTER TABLE vnext_progress_events ADD COLUMN IF NOT EXISTS message VARCHAR DEFAULT ''"
        )
        self._conn.execute(
            "ALTER TABLE vnext_progress_events ADD COLUMN IF NOT EXISTS business_day VARCHAR DEFAULT ''"
        )
        self._conn.execute(
            "ALTER TABLE vnext_progress_events ADD COLUMN IF NOT EXISTS expires_at DOUBLE DEFAULT 0"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS vnext_projection (
                business_day VARCHAR NOT NULL DEFAULT '',
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
                accepted BIGINT NOT NULL DEFAULT 0,
                expired BIGINT NOT NULL DEFAULT 0,
                response_success BIGINT NOT NULL DEFAULT 0,
                response_error BIGINT NOT NULL DEFAULT 0,
                last_ordinal BIGINT NOT NULL DEFAULT 0,
                updated_at DOUBLE NOT NULL,
                PRIMARY KEY(business_day,campaign_type,campaign_id,partition_id,route_id)
            )
        """)
        self._conn.execute(
            "ALTER TABLE vnext_projection ADD COLUMN IF NOT EXISTS cancelled BIGINT DEFAULT 0"
        )
        self._conn.execute(
            "ALTER TABLE vnext_projection ADD COLUMN IF NOT EXISTS accepted BIGINT DEFAULT 0"
        )
        self._conn.execute(
            "ALTER TABLE vnext_projection ADD COLUMN IF NOT EXISTS expired BIGINT DEFAULT 0"
        )
        if legacy_projection:
            legacy_columns = self._table_columns("vnext_projection_legacy")
            if legacy_columns:
                self._conn.execute(
                    """INSERT INTO vnext_projection(
                        business_day,campaign_type,campaign_id,partition_id,route_id,
                        queued,taken,socket_written,write_failed,unknown,cancelled,
                        accepted,expired,response_success,response_error,last_ordinal,updated_at
                    ) SELECT '',campaign_type,campaign_id,partition_id,route_id,
                        queued,taken,socket_written,write_failed,unknown,cancelled,
                        0,0,response_success,response_error,last_ordinal,updated_at
                    FROM vnext_projection_legacy"""
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

    @staticmethod
    def _event_row(event: dict) -> tuple:
        state = str(event.get("state", ""))
        return (
            str(event["event_id"]), int(event["partition_id"]), int(event["ordinal"]),
            str(event["campaign_type"]), int(event["campaign_id"]), int(event["route_id"]),
            str(event["normalized_msisdn"]), state,
            int(event.get("parts_written", 0)), int(event.get("parts_total", 0)),
            str(event.get("message", "")), str(event.get("error", "")),
            float(event.get("created_at") or time.time()),
            str(event.get("business_day") or ""), float(event.get("expires_at") or 0.0),
            int(event.get("queued", 0)), int(event.get("taken", 0)),
            int(event.get("accepted", 0)),
            int(event.get("socket_written", state == "socket_written")),
            int(event.get("write_failed", state == "write_failed")),
            int(event.get("unknown", state == "unknown")),
            int(event.get("cancelled", state == "cancelled")),
            int(event.get("expired", state == "expired")),
            int(event.get("response_success", state == "submit_sm_resp_success")),
            int(event.get("response_error", state == "submit_sm_resp_error")),
        )

    def _apply_batch(self, batch: list[dict]) -> int:
        """Apply one bounded batch with set-based event and aggregate DML."""

        if self._conn is None:
            raise RuntimeError("projection writer is not started")
        unique: dict[str, dict] = {}
        for event in batch:
            unique.setdefault(str(event["event_id"]), event)
        duplicate_in_batch = len(batch) - len(unique)
        self._sql_statements += 7 + int(bool(unique))
        self._conn.execute("DROP TABLE IF EXISTS _vnext_projection_batch")
        self._conn.execute("DROP TABLE IF EXISTS _vnext_projection_new")
        self._conn.execute("""
            CREATE TEMPORARY TABLE _vnext_projection_batch (
                event_id VARCHAR, partition_id INTEGER, ordinal BIGINT,
                campaign_type VARCHAR, campaign_id BIGINT, route_id BIGINT,
                normalized_msisdn VARCHAR, state VARCHAR, parts_written INTEGER,
                parts_total INTEGER, message VARCHAR, error VARCHAR, created_at DOUBLE,
                business_day VARCHAR, expires_at DOUBLE, queued BIGINT, taken BIGINT,
                accepted BIGINT, socket_written BIGINT, write_failed BIGINT,
                unknown BIGINT, cancelled BIGINT, expired BIGINT,
                response_success BIGINT, response_error BIGINT
            )
        """)
        rows = [self._event_row(event) for event in unique.values()]
        if rows:
            self._conn.executemany(
                "INSERT INTO _vnext_projection_batch VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
        self._conn.execute("""
            CREATE TEMPORARY TABLE _vnext_projection_new AS
            SELECT b.*
            FROM _vnext_projection_batch b
            LEFT JOIN vnext_progress_events p ON p.event_id=b.event_id
            WHERE p.event_id IS NULL
        """)
        new_count = int(self._conn.execute(
            "SELECT COUNT(*) FROM _vnext_projection_new"
        ).fetchone()[0])
        self._conn.execute("""
            INSERT INTO vnext_progress_events(
                event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                normalized_msisdn,state,parts_written,parts_total,message,error,created_at,
                business_day,expires_at
            ) SELECT event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                normalized_msisdn,state,parts_written,parts_total,message,error,created_at,
                business_day,expires_at
            FROM _vnext_projection_new
        """)
        self._conn.execute("""
            INSERT INTO vnext_projection(
                business_day,campaign_type,campaign_id,partition_id,route_id,
                queued,taken,socket_written,write_failed,unknown,cancelled,
                accepted,expired,response_success,response_error,last_ordinal,updated_at
            ) SELECT business_day,campaign_type,campaign_id,partition_id,route_id,
                SUM(queued),SUM(taken),SUM(socket_written),SUM(write_failed),SUM(unknown),
                SUM(cancelled),SUM(accepted),SUM(expired),SUM(response_success),SUM(response_error),
                MAX(ordinal),MAX(created_at)
            FROM _vnext_projection_new
            GROUP BY business_day,campaign_type,campaign_id,partition_id,route_id
            ON CONFLICT(business_day,campaign_type,campaign_id,partition_id,route_id)
            DO UPDATE SET
                queued=vnext_projection.queued+excluded.queued,
                taken=vnext_projection.taken+excluded.taken,
                socket_written=vnext_projection.socket_written+excluded.socket_written,
                write_failed=vnext_projection.write_failed+excluded.write_failed,
                unknown=vnext_projection.unknown+excluded.unknown,
                cancelled=vnext_projection.cancelled+excluded.cancelled,
                accepted=vnext_projection.accepted+excluded.accepted,
                expired=vnext_projection.expired+excluded.expired,
                response_success=vnext_projection.response_success+excluded.response_success,
                response_error=vnext_projection.response_error+excluded.response_error,
                last_ordinal=GREATEST(vnext_projection.last_ordinal,excluded.last_ordinal),
                updated_at=excluded.updated_at
        """)
        self._duplicates += duplicate_in_batch + (len(unique) - new_count)
        self._applied += new_count
        self._last_batch_new = new_count
        self._last_batch_duplicates = duplicate_in_batch + (len(unique) - new_count)
        if batch:
            self._last_applied_at = time.time()
        return new_count

    def _apply(self, event: dict) -> bool:
        """Compatibility wrapper for callers that apply one event directly."""

        if self._conn is None:
            raise RuntimeError("projection writer is not started")
        self._conn.execute("BEGIN TRANSACTION")
        try:
            inserted = self._apply_batch([event])
            self._conn.execute("COMMIT")
            return bool(inserted)
        except Exception:
            with contextlib.suppress(Exception):
                self._conn.execute("ROLLBACK")
            raise

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
            self._apply_batch(batch)
            self._conn.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                self._conn.execute("ROLLBACK")
            for event in reversed(batch):
                self._queue.appendleft(event)
            raise
        self._last_flush_monotonic = time.monotonic()
        return len(batch)

    def replay(self, journal: ProgressJournal) -> int:
        events = journal.read()
        for event in events:
            self.submit(event)
        total = 0
        while self._queue:
            total += self.flush()
        return total

    def daily_summary(self, business_day: str | None = None) -> list[dict]:
        """Read only the requested business-day aggregate from the writer DB."""

        if self._conn is None:
            self.start()
        if business_day is None:
            query = "SELECT * FROM vnext_projection ORDER BY campaign_type,campaign_id,partition_id,route_id"
            rows = self._conn.execute(query).fetchall()
        else:
            query = (
                "SELECT * FROM vnext_projection WHERE business_day=? "
                "ORDER BY campaign_type,campaign_id,partition_id,route_id"
            )
            rows = self._conn.execute(query, (str(business_day),)).fetchall()
        columns = [str(item[0]) for item in self._conn.description]
        return [dict(zip(columns, row)) for row in rows]

    def snapshot(self) -> dict:
        return {
            "db_path": self.db_path,
            "queue_depth": len(self._queue),
            "queue_capacity": self.queue_size,
            "applied": self._applied,
            "duplicates": self._duplicates,
            "last_batch_new": self._last_batch_new,
            "last_batch_duplicates": self._last_batch_duplicates,
            "sql_statements": self._sql_statements,
            "projection_storage": "one_writer_one_duckdb",
            "flush_interval_seconds": self.flush_interval_seconds,
            "flush_due": bool(self._queue and time.monotonic() - self._last_flush_monotonic >= self.flush_interval_seconds),
            "projection_lag_seconds": max(0.0, self._last_event_at - self._last_applied_at) if self._last_applied_at else 0.0,
            "last_applied_at": self._last_applied_at,
        }


def _load_offsets(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return {str(key): max(0, int(item)) for key, item in value.items()}


def _persist_offsets(path: Path, offsets: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(offsets, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)


def _write_process_status(path: Path, value: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        pass


def _process_main(
    db_path: str,
    input_queue,
    stop_event,
    status_path: str,
    batch_size: int,
    journal_paths: tuple[str, ...] = (),
    offsets_path: str = "",
    flush_interval_seconds: float = 0.1,
    diagnostic_root: str = "",
    run_id: str = "",
    start_method: str = "spawn",
) -> None:
    status_file = Path(status_path)
    role = "projection-writer"
    native_fault_path = enable_native_fault_capture(diagnostic_root, role)

    def diagnostic(**kwargs) -> None:
        try:
            write_process_metadata(
                diagnostic_root,
                role,
                run_id=run_id,
                start_method=start_method,
                **kwargs,
            )
        except Exception:
            pass

    diagnostic(
        phase="starting",
        status="starting",
        extra={"native_fault_log": str(native_fault_path) if native_fault_path else ""},
    )
    writer = ProjectionWriter(
        db_path,
        batch_size=batch_size,
        flush_interval_seconds=flush_interval_seconds,
    )
    journals = tuple(ProgressJournal(path) for path in journal_paths)
    offsets_file = Path(offsets_path) if offsets_path else None
    offsets = _load_offsets(offsets_file) if offsets_file else {}
    pending_events: list[dict] = []
    pending_offsets: dict[str, int] = {}
    graceful_stop = False
    journal_cursor = 0

    def status() -> dict:
        value = writer.snapshot()
        value.update({
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "start_method": start_method,
            "journal_paths": [str(item.path) for item in journals],
            "journal_offsets": dict(offsets),
            "journal_pending_events": len(pending_events),
            "journal_replay": bool(journals),
        })
        return value

    try:
        writer.start()
        diagnostic(phase="running", status="running", progress={"applied": 0, "queue_depth": 0})
        if journals:
            while True:
                if pending_events:
                    try:
                        flushed = writer.flush(max_events=len(pending_events))
                    except Exception as exc:
                        diagnostic(
                            phase="flush_retry",
                            status="degraded",
                            extra={"last_error": f"{type(exc).__name__}: {exc}"},
                        )
                        time.sleep(writer.retry_seconds)
                        _write_process_status(status_file, status())
                        continue
                    if flushed != len(pending_events):
                        raise RuntimeError("projection writer flushed an incomplete journal batch")
                    if offsets_file:
                        offsets.update(pending_offsets)
                        _persist_offsets(offsets_file, offsets)
                    pending_events = []
                    pending_offsets = {}
                    diagnostic(
                        phase="journal_flush",
                        status="running",
                        progress={"applied": writer.snapshot().get("applied", 0), "queue_depth": 0},
                    )
                    _write_process_status(status_file, status())
                    continue

                try:
                    while True:
                        hint = input_queue.get_nowait()
                        if hint is None:
                            graceful_stop = True
                except queue.Empty:
                    pass
                except (EOFError, OSError):
                    graceful_stop = True

                budget = writer.batch_size
                batch: list[dict] = []
                candidates: dict[str, int] = {}
                if journals:
                    # A bounded quantum per journal prevents a hot worker or
                    # large campaign from consuming the whole batch budget.
                    # The cursor rotates even when the current journal is
                    # empty, so a newly active small campaign gets a prompt
                    # turn without another persistent queue.
                    count = len(journals)
                    quantum = (
                        writer.batch_size if count == 1 else
                        max(1, min(64, (writer.batch_size + count - 1) // count))
                    )
                    for offset in range(count):
                        if budget <= 0:
                            break
                        index = (journal_cursor + offset) % count
                        journal = journals[index]
                        key = str(journal.path)
                        events, new_offset = journal.read_from(
                            offsets.get(key, 0), max_events=min(quantum, budget)
                        )
                        if events:
                            batch.extend(events)
                            candidates[key] = new_offset
                            budget -= len(events)
                    journal_cursor = (journal_cursor + 1) % count
                if batch:
                    for event in batch:
                        writer.submit(event)
                    pending_events = batch
                    pending_offsets = candidates
                    continue
                if graceful_stop or (stop_event.is_set() and not pending_events):
                    break
                _write_process_status(status_file, status())
                time.sleep(min(0.05, writer.flush_interval_seconds))
        else:
            while True:
                try:
                    event = input_queue.get(timeout=min(0.05, writer.flush_interval_seconds))
                except queue.Empty:
                    if writer._queue and time.monotonic() - writer._last_flush_monotonic >= writer.flush_interval_seconds:
                        try:
                            writer.flush()
                        except ProjectionWriterBusy:
                            pass
                        except Exception as exc:
                            diagnostic(
                                phase="flush_retry",
                                status="degraded",
                                extra={"last_error": f"{type(exc).__name__}: {exc}"},
                            )
                            time.sleep(writer.retry_seconds)
                    if stop_event.is_set() and not writer._queue:
                        break
                    _write_process_status(status_file, status())
                    continue
                except (EOFError, OSError):
                    break
                if event is None:
                    break
                writer.submit(event)
                if (
                    len(writer._queue) >= writer.batch_size
                    or time.monotonic() - writer._last_flush_monotonic >= writer.flush_interval_seconds
                ):
                    try:
                        writer.flush()
                    except ProjectionWriterBusy:
                        pass
                    except Exception as exc:
                        diagnostic(
                            phase="flush_retry",
                            status="degraded",
                            extra={"last_error": f"{type(exc).__name__}: {exc}"},
                        )
                        time.sleep(writer.retry_seconds)
                diagnostic(
                    phase="event", status="running",
                    progress={"applied": writer.snapshot().get("applied", 0), "queue_depth": len(writer._queue)},
                )
                _write_process_status(status_file, status())
            while writer._queue:
                try:
                    writer.flush()
                except ProjectionWriterBusy:
                    time.sleep(writer.retry_seconds)
                except Exception as exc:
                    diagnostic(
                        phase="flush_retry",
                        status="degraded",
                        extra={"last_error": f"{type(exc).__name__}: {exc}"},
                    )
                    time.sleep(writer.retry_seconds)
        _write_process_status(status_file, status())
        diagnostic(
            phase="stopped", status="stopped",
            progress={"applied": writer.snapshot().get("applied", 0), "queue_depth": len(writer._queue)},
        )
    except BaseException as exc:
        diagnostic(
            phase="exception", status="error",
            extra={"last_error": f"{type(exc).__name__}: {exc}"},
        )
        raise
    finally:
        writer.stop()


class ProjectionWriterProcess:
    """Multiprocess owner wrapper used by V4/runtime integration."""

    def __init__(self, root: str | os.PathLike[str], *, db_name: str = "vnext-projection.duckdb",
                 queue_size: int = 8192, batch_size: int = 512,
                 journal_paths: list[str | os.PathLike[str]] | tuple[str | os.PathLike[str], ...] = (),
                 flush_interval_seconds: float = 0.1, start_method: str = "spawn",
                 mp_context=None, diagnostics_root: str | os.PathLike[str] | None = None,
                 run_id: str = ""):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / db_name
        self.status_path = self.root / "projection-writer-status.json"
        self.queue_size = max(1, int(queue_size))
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_seconds = min(5.0, max(0.01, float(flush_interval_seconds)))
        self._mp_context = mp_context or multiprocessing.get_context(start_method or "spawn")
        self.start_method = self._mp_context.get_start_method()
        self.journal_paths = tuple(str(Path(path).resolve()) for path in journal_paths)
        self.offsets_path = self.root / "journal-offsets.json"
        self.diagnostics_root = Path(diagnostics_root).resolve() if diagnostics_root else self.root / "diagnostics"
        self.run_id = str(run_id or self.root.name)
        self._lock = threading.RLock()
        self._started = False
        self._stopping = False
        self.restart_count = 0
        self.queue = None
        self.stop_event = None
        self.process = None
        self._create_process()

    def _create_process(self) -> None:
        context = self._mp_context
        self.queue = context.Queue(maxsize=self.queue_size)
        self.stop_event = context.Event()
        self.process = context.Process(
            target=_process_main,
            args=(
                str(self.db_path), self.queue, self.stop_event, str(self.status_path),
                self.batch_size, self.journal_paths, str(self.offsets_path),
                self.flush_interval_seconds, str(self.diagnostics_root), self.run_id,
                self.start_method,
            ),
            name="mc-vnext-projection-writer",
            daemon=True,
        )

    def start(self) -> None:
        with self._lock:
            if self.process is not None and self.process.is_alive():
                return
            if self._started:
                self._create_process()
            self._stopping = False
            self.process.start()
            self._started = True
            write_process_metadata(
                self.diagnostics_root,
                "projection-parent",
                run_id=self.run_id,
                start_method=self.start_method,
                phase="child_started",
                status="running",
                progress={"child_pid": getattr(self.process, "pid", None)},
            )

    def restart(self) -> bool:
        with self._lock:
            if self._stopping or (self.process is not None and self.process.is_alive()):
                return False
            self._create_process()
            self.process.start()
            self._started = True
            self.restart_count += 1
            write_process_metadata(
                self.diagnostics_root,
                "projection-parent",
                run_id=self.run_id,
                start_method=self.start_method,
                phase="child_restarted",
                status="running",
                progress={"child_pid": getattr(self.process, "pid", None), "restart_count": self.restart_count},
            )
            return True

    def submit(self, event: dict, *, timeout: float = 0.0) -> None:
        if self.queue is None:
            raise ProjectionBackpressure("projection writer process is not started")
        try:
            self.queue.put(dict(event), timeout=max(0.0, float(timeout)))
        except queue.Full as exc:
            raise ProjectionBackpressure("projection writer IPC queue is full") from exc

    def stop(self, timeout: float = 30.0) -> None:
        with self._lock:
            if not self._started or self.process is None:
                return
            self._stopping = True
            process = self.process
            stop_event = self.stop_event
            input_queue = self.queue
        stop_event.set()
        deadline = time.monotonic() + max(0.1, float(timeout))
        while True:
            try:
                input_queue.put(None, timeout=0.05)
                break
            except queue.Full:
                if time.monotonic() >= deadline:
                    break
        process.join(timeout=max(0.0, float(timeout)))
        if process.is_alive():
            process.terminate()
            process.join(timeout=2.0)
        write_process_metadata(
            self.diagnostics_root,
            "projection-parent",
            run_id=self.run_id,
            start_method=self.start_method,
            phase="child_stopped",
            status="stopped",
            exitcode=getattr(process, "exitcode", None),
        )

    def snapshot(self) -> dict:
        if not self.status_path.exists():
            return {
                "queue_depth": None,
                "alive": bool(self.process and self.process.is_alive()),
                "journal_replay": bool(self.journal_paths),
                "pid": getattr(self.process, "pid", None),
                "exitcode": getattr(self.process, "exitcode", None),
                "start_method": self.start_method,
                "restart_count": self.restart_count,
            }
        try:
            value = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            value = {}
        return {
            **value,
            "alive": bool(self.process and self.process.is_alive()),
            "journal_replay": bool(self.journal_paths),
            "pid": getattr(self.process, "pid", None),
            "exitcode": getattr(self.process, "exitcode", None),
            "start_method": self.start_method,
            "restart_count": self.restart_count,
        }
