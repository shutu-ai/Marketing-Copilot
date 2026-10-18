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

from .critical_path_profiler import CriticalPathProfiler
from .diagnostics import enable_native_fault_capture, write_process_metadata


class ProjectionWriterBusy(RuntimeError):
    pass


class ProjectionBackpressure(RuntimeError):
    pass


class ProgressJournal:
    """Durable append-only progress events with bounded generation segments.

    ``path`` remains the stable logical journal name.  Once the active file
    reaches ``segment_bytes`` new records go to ``<stem>.gNNNNNNNN<suffix>``.
    The writer acknowledges a segment by its full path and byte offset; only
    an older segment whose durable cursor reaches EOF can be reclaimed.  This
    keeps the original single-worker writer ownership while allowing a hard
    capacity stop to recover without resetting an integer offset onto a new
    file.
    """

    def __init__(self, path: str | os.PathLike[str], *, max_event_bytes: int = 64 * 1024,
                 max_bytes: int = 512 * 1024 * 1024,
                 segment_bytes: int | None = None,
                 profiler: CriticalPathProfiler | None = None):
        self.path = Path(path).resolve()
        self.max_event_bytes = max(1024, int(max_event_bytes))
        self.max_bytes = max(self.max_event_bytes, int(max_bytes))
        requested_segment_bytes = int(segment_bytes or min(self.max_bytes, 64 * 1024 * 1024))
        self.segment_bytes = max(self.max_event_bytes, min(self.max_bytes, requested_segment_bytes))
        self.profiler = profiler
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._read_metrics = {
            "journal_read_ns": 0,
            "json_decode_ns": 0,
            "journal_read_calls": 0,
            "journal_events_read": 0,
            "journal_bytes_read": 0,
            "last_read_offset": 0,
            "last_read_size": 0,
        }

    def _generation_path(self, generation: int) -> Path:
        return self.path.with_name(
            f"{self.path.stem}.g{int(generation):08d}{self.path.suffix}"
        )

    @staticmethod
    def _generation(path: Path) -> int:
        name = path.name
        marker = ".g"
        start = name.rfind(marker)
        if start < 0 or not name.endswith(path.suffix):
            return -1
        value = name[start + len(marker):-len(path.suffix)] if path.suffix else name[start + len(marker):]
        try:
            return int(value)
        except ValueError:
            return -1

    def segment_paths(self) -> list[Path]:
        """Return existing segments in append order, retaining the base path."""

        started_ns = time.perf_counter_ns()
        try:
            paths = [self.path] if self.path.exists() else []
            pattern = f"{self.path.stem}.g*{self.path.suffix}"
            generated = [
                candidate for candidate in self.path.parent.glob(pattern)
                if self._generation(candidate) >= 0
            ]
            paths.extend(sorted(generated, key=self._generation))
            return paths or [self.path]
        finally:
            if self.profiler:
                self.profiler.inc("journal_segment_scan_count")
                self.profiler.observe_ns(
                    "journal_segment_scan", time.perf_counter_ns() - started_ns
                )

    def active_path(self) -> Path:
        return self.segment_paths()[-1]

    def size(self) -> int:
        total = 0
        for path in self.segment_paths():
            try:
                total += int(path.stat().st_size)
            except FileNotFoundError:
                continue
        return total

    @staticmethod
    def _encode(event: dict) -> bytes:
        return (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    def can_append(self, event: dict) -> bool:
        encoded = self._encode(event)
        return len(encoded) <= self.max_event_bytes and self.size() + len(encoded) <= self.max_bytes

    def _append_encoded(self, encoded: list[bytes]) -> None:
        append_started_ns = time.perf_counter_ns()
        if not encoded:
            return
        if any(len(line) > self.max_event_bytes for line in encoded):
            raise ValueError("progress event exceeds bounded size")
        if self.size() + sum(len(line) for line in encoded) > self.max_bytes:
            raise ProjectionBackpressure("progress journal capacity exhausted")
        if self.profiler:
            self.profiler.inc("journal_append_event_count", len(encoded))
            self.profiler.inc("journal_append_bytes", sum(len(line) for line in encoded))
        handles: dict[Path, object] = {}
        try:
            active = self.active_path()
            for line in encoded:
                try:
                    active_size = int(active.stat().st_size)
                except FileNotFoundError:
                    active_size = 0
                if self.profiler:
                    self.profiler.inc("journal_active_stat_count")
                if active_size and active_size + len(line) > self.segment_bytes:
                    generations = [self._generation(path) for path in self.segment_paths()]
                    active = self._generation_path(max(generations or [-1]) + 1)
                handle = handles.get(active)
                if handle is None:
                    active.parent.mkdir(parents=True, exist_ok=True)
                    open_started_ns = time.perf_counter_ns()
                    handle = active.open("ab")
                    if self.profiler:
                        self.profiler.observe_ns(
                            "journal_open", time.perf_counter_ns() - open_started_ns
                        )
                    handles[active] = handle
                write_started_ns = time.perf_counter_ns()
                handle.write(line)
                if self.profiler:
                    self.profiler.observe_ns(
                        "journal_write", time.perf_counter_ns() - write_started_ns
                    )
            for handle in handles.values():
                handle.flush()
                fsync_started_ns = time.perf_counter_ns()
                os.fsync(handle.fileno())
                if self.profiler:
                    self.profiler.observe_ns(
                        "journal_fsync", time.perf_counter_ns() - fsync_started_ns
                    )
        finally:
            for handle in handles.values():
                handle.close()
            if self.profiler:
                self.profiler.observe_ns(
                    "journal_append", time.perf_counter_ns() - append_started_ns
                )
                self.profiler.publish_if_due()

    def append(self, event: dict) -> None:
        started_ns = time.perf_counter_ns()
        encoded = self._encode(event)
        if self.profiler:
            self.profiler.observe_ns("journal_json_encode", time.perf_counter_ns() - started_ns)
        self._append_encoded([encoded])
        if self.profiler:
            self.profiler.inc("journal_events_durable")
            self.profiler.mark("last_journal_durable_monotonic")
            if ":resp:" in str(event.get("event_id") or ""):
                self.profiler.mark("last_response_journal_durable_monotonic")
            self.profiler.publish_if_due()

    def append_many(self, events: list[dict]) -> None:
        started_ns = time.perf_counter_ns()
        encoded = [self._encode(event) for event in events]
        if self.profiler:
            self.profiler.observe_ns("journal_json_encode", time.perf_counter_ns() - started_ns)
        self._append_encoded(encoded)
        if self.profiler:
            self.profiler.inc("journal_events_durable", len(events))
            self.profiler.mark("last_journal_durable_monotonic")
            for event in events:
                if ":resp:" in str(event.get("event_id") or ""):
                    self.profiler.mark("last_response_journal_durable_monotonic")
            self.profiler.publish_if_due()

    def read(self) -> list[dict]:
        result: list[dict] = []
        for path in self.segment_paths():
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        result.append(json.loads(line))
        return result

    def _read_segment_from(self, path: Path, offset: int = 0, *, max_events: int = 256) -> tuple[list[dict], int]:
        started_ns = time.perf_counter_ns()
        if not path.exists():
            return [], max(0, int(offset))
        safe_offset = max(0, int(offset))
        events: list[dict] = []
        next_offset = safe_offset
        limit = max(1, int(max_events))
        bytes_read = 0
        decode_ns = 0
        with path.open("rb") as handle:
            size = int(path.stat().st_size)
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
                bytes_read += len(line)
                if line.strip():
                    decode_started_ns = time.perf_counter_ns()
                    events.append(json.loads(line.decode("utf-8")))
                    decode_ns += time.perf_counter_ns() - decode_started_ns
        self._read_metrics["journal_read_ns"] += time.perf_counter_ns() - started_ns
        self._read_metrics["json_decode_ns"] += decode_ns
        self._read_metrics["journal_read_calls"] += 1
        self._read_metrics["journal_events_read"] += len(events)
        self._read_metrics["journal_bytes_read"] += bytes_read
        self._read_metrics["last_read_offset"] = safe_offset
        self._read_metrics["last_read_size"] = size
        return events, next_offset

    def read_segment_from(self, path: str | os.PathLike[str], offset: int = 0, *, max_events: int = 256) -> tuple[list[dict], int]:
        """Read a generation-specific file using its own durable byte cursor."""

        return self._read_segment_from(Path(path).resolve(), offset, max_events=max_events)

    def read_from(self, offset: int = 0, *, max_events: int = 256) -> tuple[list[dict], int]:
        """Backward-compatible read of the stable base segment."""

        return self._read_segment_from(self.path, offset, max_events=max_events)

    def read_metrics(self) -> dict:
        return dict(self._read_metrics)

    def storage_snapshot(self, offsets: dict[str, int] | None = None) -> dict:
        offsets = offsets or {}
        paths = self.segment_paths()
        total_bytes = 0
        committed_bytes = 0
        reclaimable_bytes = 0
        for index, path in enumerate(paths):
            try:
                size = int(path.stat().st_size)
            except FileNotFoundError:
                continue
            total_bytes += size
            committed = min(size, max(0, int(offsets.get(str(path), 0))))
            committed_bytes += committed
            if index < len(paths) - 1 and committed >= size and size > 0:
                reclaimable_bytes += size
        backlog_bytes = max(0, total_bytes - committed_bytes)
        capacity_remaining_bytes = max(0, self.max_bytes - total_bytes)
        average_event_bytes = max(1, int(self._read_metrics["journal_bytes_read"] / max(1, self._read_metrics["journal_events_read"])))
        return {
            "total_bytes": total_bytes,
            "committed_bytes": committed_bytes,
            "backlog_bytes": backlog_bytes,
            "backlog_events_estimate": int(backlog_bytes / average_event_bytes),
            "reclaimable_bytes": reclaimable_bytes,
            "capacity_remaining_bytes": capacity_remaining_bytes,
            "max_bytes": self.max_bytes,
            "segment_bytes": self.segment_bytes,
            "segment_count": len([path for path in paths if path.exists()]),
            "active_segment": str(paths[-1]),
            # This is deliberately conservative: once the remaining hard
            # capacity cannot hold the configured maximum event, admission
            # must be treated as blocked even if a smaller event could fit.
            "blocked": capacity_remaining_bytes < self.max_event_bytes,
        }

    def reclaim(self, committed_offsets: dict[str, int]) -> list[str]:
        """Remove only fully acknowledged, inactive segments."""

        removed: list[str] = []
        paths = self.segment_paths()
        for path in paths[:-1]:
            try:
                size = int(path.stat().st_size)
            except FileNotFoundError:
                continue
            if size <= 0 or int(committed_offsets.get(str(path), 0)) < size:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            removed.append(str(path))
        return removed


class ProjectionWriter:
    """A bounded writer object; in production it is owned by one process."""

    def __init__(self, db_path: str | os.PathLike[str], *, queue_size: int = 8192,
                 batch_size: int = 512, retry_seconds: float = 0.05,
                 flush_interval_seconds: float = 0.1,
                 staging_mode: str = "vectorized", reuse_staging: bool = True,
                 profiler: CriticalPathProfiler | None = None):
        self.db_path = str(Path(db_path).resolve())
        self.queue_size = max(1, int(queue_size))
        self.batch_size = max(1, int(batch_size))
        self.retry_seconds = max(0.001, float(retry_seconds))
        self.flush_interval_seconds = min(5.0, max(0.01, float(flush_interval_seconds)))
        self.staging_mode = str(staging_mode or "vectorized").strip().lower()
        if self.staging_mode not in {"executemany", "vectorized"}:
            raise ValueError("staging_mode must be executemany or vectorized")
        self.reuse_staging = bool(reuse_staging)
        self.profiler = profiler
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
        self._metrics = {
            "journal_read_ns": 0,
            "json_decode_ns": 0,
            "rows_prepare_ns": 0,
            "staging_prepare_rows_ns": 0,
            "staging_clear_table_ns": 0,
            "staging_transpose_ns": 0,
            "staging_bind_execute_ns": 0,
            "staging_relation_create_ns": 0,
            "staging_relation_insert_ns": 0,
            "staging_load_ns": 0,
            "staging_total_ns": 0,
            "staging_batch_count": 0,
            "staging_event_count": 0,
            "new_event_dedupe_ns": 0,
            "events_insert_ns": 0,
            "aggregate_upsert_ns": 0,
            "db_commit_ns": 0,
            "offset_persist_ns": 0,
            "batch_end_to_end_ns": 0,
            "batches_total": 0,
            "events_read": 0,
            "events_new": 0,
            "events_replayed": 0,
            "sql_calls": 0,
            "executemany_calls": 0,
            "vectorized_staging_calls": 0,
            "journal_total_bytes": 0,
            "journal_committed_offset_bytes": 0,
            "journal_backlog_bytes": 0,
            "journal_backlog_events_estimate": 0,
            "projection_events_applied": 0,
        }
        self._batch_sizes: deque[int] = deque(maxlen=4096)
        self._staging_batch_durations_ns: deque[int] = deque(maxlen=4096)
        self._staging_event_durations_ns: deque[int] = deque(maxlen=4096)
        self._metrics_last_published = 0.0
        self._published_metrics: dict = {}
        self._batch_table_created = False

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
        self._batch_table_created = False
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
            str(event.get("normalized_msisdn") or ""), state,
            int(event.get("parts_written", 0)), int(event.get("parts_total", 0)),
            str(event.get("message") or ""), str(event.get("error") or ""),
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

    def _clear_staging(self, execute, batch_table_sql: str) -> None:
        """Clear/reuse the staging tables without changing transaction scope.

        The method is intentionally small so isolated benchmark adapters can
        vary only the cleanup order while retaining the production DML path.
        """

        if self.reuse_staging:
            if not self._batch_table_created:
                execute("DROP TABLE IF EXISTS _vnext_projection_batch")
                execute(batch_table_sql)
                self._batch_table_created = True
            execute("DELETE FROM _vnext_projection_batch")
            execute("DROP TABLE IF EXISTS _vnext_projection_new")
        else:
            execute("DROP TABLE IF EXISTS _vnext_projection_batch")
            execute("DROP TABLE IF EXISTS _vnext_projection_new")
            execute(batch_table_sql)

    def _stage_rows(self, rows: list[tuple], execute, executemany) -> None:
        """Insert prepared rows into the staging table using the configured A0 path."""

        if not rows:
            return
        if self.staging_mode == "vectorized":
            if any(len(row) != 25 for row in rows):
                raise ValueError("projection staging columns must have equal lengths")
            transpose_started_ns = time.perf_counter_ns()
            columns = [list(column) for column in zip(*rows)]
            transpose_elapsed_ns = time.perf_counter_ns() - transpose_started_ns
            self._metrics["staging_transpose_ns"] += transpose_elapsed_ns
            if self.profiler:
                self.profiler.observe_ns("staging_transpose", transpose_elapsed_ns)

            types = (
                "VARCHAR", "INTEGER", "BIGINT", "VARCHAR", "BIGINT", "BIGINT",
                "VARCHAR", "VARCHAR", "INTEGER", "INTEGER", "VARCHAR", "VARCHAR",
                "DOUBLE", "VARCHAR", "DOUBLE", "BIGINT", "BIGINT", "BIGINT",
                "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            )
            bind_started_ns = time.perf_counter_ns()
            select_columns = ", ".join(
                f"(u.value)[{index}]::{column_type}"
                for index, column_type in enumerate(types, start=1)
            )
            parameters = ", ".join(f"?::{column_type}[]" for column_type in types)
            self._metrics["vectorized_staging_calls"] += 1
            execute(
                "INSERT INTO _vnext_projection_batch SELECT "
                f"{select_columns} FROM UNNEST(list_zip({parameters})) AS u(value)",
                columns,
            )
            bind_elapsed_ns = time.perf_counter_ns() - bind_started_ns
        else:
            bind_started_ns = time.perf_counter_ns()
            executemany(
                "INSERT INTO _vnext_projection_batch VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            bind_elapsed_ns = time.perf_counter_ns() - bind_started_ns
        self._metrics["staging_bind_execute_ns"] += bind_elapsed_ns
        if self.profiler:
            self.profiler.observe_ns("staging_bind_execute", bind_elapsed_ns)

    def _record_staging_batch(self, elapsed_ns: int, event_count: int) -> None:
        elapsed_ns = max(0, int(elapsed_ns))
        event_count = max(0, int(event_count))
        self._metrics["staging_load_ns"] += elapsed_ns
        self._metrics["staging_total_ns"] += elapsed_ns
        self._metrics["staging_batch_count"] += 1
        self._metrics["staging_event_count"] += event_count
        self._staging_batch_durations_ns.append(elapsed_ns)
        if event_count:
            per_event_ns = elapsed_ns // event_count
            self._staging_event_durations_ns.extend(
                [per_event_ns] * min(event_count, self._staging_event_durations_ns.maxlen)
            )
        if self.profiler:
            self.profiler.observe_ns("staging_total", elapsed_ns)
            self.profiler.inc("staging_batch_count")
            self.profiler.inc("staging_event_count", event_count)

    def _apply_batch(self, batch: list[dict]) -> int:
        """Apply one bounded batch with set-based event and aggregate DML."""

        if self._conn is None:
            raise RuntimeError("projection writer is not started")
        batch_started_ns = time.perf_counter_ns()
        unique: dict[str, dict] = {}
        for event in batch:
            unique.setdefault(str(event["event_id"]), event)
        duplicate_in_batch = len(batch) - len(unique)
        sql_calls = 0

        def execute(statement: str, parameters=None):
            nonlocal sql_calls
            sql_calls += 1
            self._sql_statements += 1
            self._metrics["sql_calls"] += 1
            if parameters is None:
                return self._conn.execute(statement)
            return self._conn.execute(statement, parameters)

        def executemany(statement: str, parameters):
            nonlocal sql_calls
            sql_calls += 1
            self._sql_statements += 1
            self._metrics["sql_calls"] += 1
            self._metrics["executemany_calls"] += 1
            return self._conn.executemany(statement, parameters)

        batch_table_sql = """
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
        """
        staging_started_ns = time.perf_counter_ns()
        clear_started_ns = time.perf_counter_ns()
        self._clear_staging(execute, batch_table_sql)
        clear_elapsed_ns = time.perf_counter_ns() - clear_started_ns
        self._metrics["staging_clear_table_ns"] += clear_elapsed_ns
        if self.profiler:
            self.profiler.observe_ns("staging_clear_table", clear_elapsed_ns)

        rows_prepare_started_ns = time.perf_counter_ns()
        rows = [self._event_row(event) for event in unique.values()]
        rows_prepare_elapsed_ns = time.perf_counter_ns() - rows_prepare_started_ns
        self._metrics["rows_prepare_ns"] += rows_prepare_elapsed_ns
        self._metrics["staging_prepare_rows_ns"] += rows_prepare_elapsed_ns
        if self.profiler:
            self.profiler.observe_ns("staging_prepare_rows", rows_prepare_elapsed_ns)
        self._stage_rows(rows, execute, executemany)
        self._record_staging_batch(time.perf_counter_ns() - staging_started_ns, len(rows))

        dedupe_started_ns = time.perf_counter_ns()
        execute("""
            CREATE TEMPORARY TABLE _vnext_projection_new AS
            SELECT b.*
            FROM _vnext_projection_batch b
            LEFT JOIN vnext_progress_events p ON p.event_id=b.event_id
            WHERE p.event_id IS NULL
        """)
        new_count = int(execute(
            "SELECT COUNT(*) FROM _vnext_projection_new"
        ).fetchone()[0])
        self._metrics["new_event_dedupe_ns"] += time.perf_counter_ns() - dedupe_started_ns

        events_insert_started_ns = time.perf_counter_ns()
        execute("""
            INSERT INTO vnext_progress_events(
                event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                normalized_msisdn,state,parts_written,parts_total,message,error,created_at,
                business_day,expires_at
            ) SELECT event_id,partition_id,ordinal,campaign_type,campaign_id,route_id,
                normalized_msisdn,state,parts_written,parts_total,message,error,created_at,
                business_day,expires_at
            FROM _vnext_projection_new
        """)
        self._metrics["events_insert_ns"] += time.perf_counter_ns() - events_insert_started_ns

        aggregate_started_ns = time.perf_counter_ns()
        execute("""
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
        self._metrics["aggregate_upsert_ns"] += time.perf_counter_ns() - aggregate_started_ns
        self._duplicates += duplicate_in_batch + (len(unique) - new_count)
        self._applied += new_count
        self._last_batch_new = new_count
        self._last_batch_duplicates = duplicate_in_batch + (len(unique) - new_count)
        self._metrics["batches_total"] += 1
        self._metrics["events_read"] += len(batch)
        self._metrics["events_new"] += new_count
        self._metrics["events_replayed"] += duplicate_in_batch + (len(unique) - new_count)
        self._metrics["projection_events_applied"] = self._applied
        self._batch_sizes.append(len(batch))
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
        batch_started_ns = time.perf_counter_ns()
        try:
            self._conn.execute("BEGIN TRANSACTION")
            self._apply_batch(batch)
            commit_started_ns = time.perf_counter_ns()
            self._conn.execute("COMMIT")
            self._metrics["db_commit_ns"] += time.perf_counter_ns() - commit_started_ns
            if self.profiler:
                self.profiler.mark("last_projection_db_commit_monotonic")
                self.profiler.inc("projection_commits")
            self._metrics["batch_end_to_end_ns"] += time.perf_counter_ns() - batch_started_ns
            if self.profiler:
                self.profiler.publish_if_due()
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

    @staticmethod
    def _percentile(values: deque[int], percentile: float) -> int:
        if not values:
            return 0
        ordered = sorted(values)
        index = (len(ordered) - 1) * percentile
        lower = int(index)
        upper = min(len(ordered) - 1, lower + 1)
        fraction = index - lower
        return int(round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction))

    def _publish_metrics(self, *, force: bool = False) -> dict:
        now = time.monotonic()
        if not force and self._published_metrics and now - self._metrics_last_published < 1.0:
            return dict(self._published_metrics)
        values = dict(self._metrics)
        values.update({
            "batch_size_p50": self._percentile(self._batch_sizes, 0.50),
            "batch_size_p95": self._percentile(self._batch_sizes, 0.95),
            "staging_batch_ms_p50": round(
                self._percentile(self._staging_batch_durations_ns, 0.50) / 1_000_000, 3
            ),
            "staging_batch_ms_p95": round(
                self._percentile(self._staging_batch_durations_ns, 0.95) / 1_000_000, 3
            ),
            "staging_event_us_p50": round(
                self._percentile(self._staging_event_durations_ns, 0.50) / 1_000, 3
            ),
            "staging_event_us_p95": round(
                self._percentile(self._staging_event_durations_ns, 0.95) / 1_000, 3
            ),
            "staging_mode": self.staging_mode,
            "reuse_staging": self.reuse_staging,
            "applied_scope": "process_local",
        })
        self._published_metrics = values
        self._metrics_last_published = now
        return dict(values)

    def snapshot(self, *, force_metrics: bool = False) -> dict:
        metrics = self._publish_metrics(force=force_metrics)
        return {
            "db_path": self.db_path,
            "queue_depth": len(self._queue),
            "queue_capacity": self.queue_size,
            "batch_size": self.batch_size,
            "applied": self._applied,
            "duplicates": self._duplicates,
            "last_batch_new": self._last_batch_new,
            "last_batch_duplicates": self._last_batch_duplicates,
            "sql_statements": self._sql_statements,
            "staging_mode": self.staging_mode,
            "reuse_staging": self.reuse_staging,
            "projection_storage": "one_writer_one_duckdb",
            "flush_interval_seconds": self.flush_interval_seconds,
            "flush_due": bool(self._queue and time.monotonic() - self._last_flush_monotonic >= self.flush_interval_seconds),
            "projection_lag_seconds": max(0.0, self._last_event_at - self._last_applied_at) if self._last_applied_at else 0.0,
            "last_applied_at": self._last_applied_at,
            "projection_events_applied": self._applied,
            "metrics": metrics,
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
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(offsets, sort_keys=True, separators=(",", ":")))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    if os.name == "posix":
        directory_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)


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
    staging_mode: str = "vectorized",
    reuse_staging: bool = True,
    profile_root: str = "",
    profile_interval_seconds: float = 1.0,
) -> None:
    status_file = Path(status_path)
    role = "projection-writer"
    native_fault_path = enable_native_fault_capture(diagnostic_root, role)
    profiler = CriticalPathProfiler(
        profile_root,
        role,
        interval_seconds=float(profile_interval_seconds or 1.0),
    )

    def diagnostic(**kwargs) -> None:
        started_ns = time.perf_counter_ns()
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
        finally:
            profiler.observe_ns("diagnostic_write", time.perf_counter_ns() - started_ns)
            profiler.publish_if_due()

    diagnostic(
        phase="starting",
        status="starting",
        extra={"native_fault_log": str(native_fault_path) if native_fault_path else ""},
    )
    writer = ProjectionWriter(
        db_path,
        batch_size=batch_size,
        flush_interval_seconds=flush_interval_seconds,
        staging_mode=staging_mode,
        reuse_staging=reuse_staging,
        profiler=profiler,
    )
    journals = tuple(ProgressJournal(path, profiler=profiler) for path in journal_paths)
    offsets_file = Path(offsets_path) if offsets_path else None
    offsets = _load_offsets(offsets_file) if offsets_file else {}
    pending_events: list[dict] = []
    pending_offsets: dict[str, int] = {}
    graceful_stop = False
    journal_cursor = 0

    def status(*, force_metrics: bool = False) -> dict:
        value = writer.snapshot(force_metrics=force_metrics)
        journal_total_bytes = 0
        journal_committed_bytes = 0
        journal_backlog_bytes = 0
        journal_reclaimable_bytes = 0
        journal_backlog_events_estimate = 0
        journal_paths_status: list[str] = []
        journal_read_ns = 0
        json_decode_ns = 0
        journal_read_calls = 0
        journal_events_read = 0
        journal_bytes_read = 0
        for journal in journals:
            storage = journal.storage_snapshot(offsets)
            journal_total_bytes += int(storage["total_bytes"])
            journal_committed_bytes += int(storage["committed_bytes"])
            journal_backlog_bytes += int(storage["backlog_bytes"])
            journal_reclaimable_bytes += int(storage["reclaimable_bytes"])
            journal_backlog_events_estimate += int(storage["backlog_events_estimate"])
            journal_paths_status.extend(str(path) for path in journal.segment_paths() if path.exists())
            read_metrics = journal.read_metrics()
            journal_read_ns += int(read_metrics["journal_read_ns"])
            json_decode_ns += int(read_metrics["json_decode_ns"])
            journal_read_calls += int(read_metrics["journal_read_calls"])
            journal_events_read += int(read_metrics["journal_events_read"])
            journal_bytes_read += int(read_metrics["journal_bytes_read"])
        value.setdefault("metrics", {}).update({
            "journal_read_ns": journal_read_ns,
            "json_decode_ns": json_decode_ns,
            "journal_read_calls": journal_read_calls,
            "journal_events_read": journal_events_read,
            "journal_bytes_read": journal_bytes_read,
            "journal_total_bytes": journal_total_bytes,
            "journal_committed_offset_bytes": journal_committed_bytes,
            "journal_committed_offsets": journal_committed_bytes,
            "journal_backlog_bytes": journal_backlog_bytes,
            "journal_backlog_events_estimate": journal_backlog_events_estimate,
        })
        value.update({
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "start_method": start_method,
            "journal_paths": journal_paths_status or [str(item.path) for item in journals],
            "journal_offsets": dict(offsets),
            "journal_pending_events": len(pending_events),
            "journal_replay": bool(journals),
            "journal_total_bytes": journal_total_bytes,
            "journal_committed_offset_bytes": journal_committed_bytes,
            "journal_backlog_bytes": journal_backlog_bytes,
            "journal_reclaimable_bytes": journal_reclaimable_bytes,
            "journal_backlog_events_estimate": journal_backlog_events_estimate,
            "journal_read_calls": journal_read_calls,
            "journal_events_read": journal_events_read,
            "journal_bytes_read": journal_bytes_read,
        })
        if profiler.enabled:
            value["critical_path"] = profiler.snapshot()
        return value

    def write_status(*, force_metrics: bool = False) -> None:
        started_ns = time.perf_counter_ns()
        _write_process_status(status_file, status(force_metrics=force_metrics))
        profiler.inc("process_status_write_count")
        profiler.observe_ns("process_status_write", time.perf_counter_ns() - started_ns)
        profiler.publish_if_due()

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
                        write_status()
                        continue
                    if flushed != len(pending_events):
                        raise RuntimeError("projection writer flushed an incomplete journal batch")
                    if offsets_file:
                        offsets.update(pending_offsets)
                        offset_started_ns = time.perf_counter_ns()
                        _persist_offsets(offsets_file, offsets)
                        offset_elapsed_ns = time.perf_counter_ns() - offset_started_ns
                        writer._metrics["offset_persist_ns"] += offset_elapsed_ns
                        profiler.inc("offset_persist_count")
                        profiler.observe_ns("offset_persist", offset_elapsed_ns)
                        profiler.mark("last_projection_offset_durable_monotonic")
                        removed_segments: list[str] = []
                        for journal in journals:
                            removed_segments.extend(journal.reclaim(offsets))
                        if removed_segments:
                            for removed in removed_segments:
                                offsets.pop(removed, None)
                            offset_started_ns = time.perf_counter_ns()
                            _persist_offsets(offsets_file, offsets)
                            offset_elapsed_ns = time.perf_counter_ns() - offset_started_ns
                            writer._metrics["offset_persist_ns"] += offset_elapsed_ns
                            profiler.inc("offset_persist_count")
                            profiler.observe_ns("offset_persist", offset_elapsed_ns)
                            profiler.mark("last_projection_offset_durable_monotonic")
                    pending_events = []
                    pending_offsets = {}
                    diagnostic(
                        phase="journal_flush",
                        status="running",
                        progress={"applied": writer.snapshot().get("applied", 0), "queue_depth": 0},
                    )
                    write_status()
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
                        # Keep each journal bounded and fair, while avoiding
                        # the old 64-event ceiling turning a 512-event
                        # writer batch into repeated tiny DuckDB commits.
                        max(64, min(256, (writer.batch_size + count - 1) // count))
                    )
                    for offset in range(count):
                        if budget <= 0:
                            break
                        index = (journal_cursor + offset) % count
                        journal = journals[index]
                        remaining_for_journal = min(quantum, budget)
                        for segment_path in journal.segment_paths():
                            if remaining_for_journal <= 0 or not segment_path.exists():
                                break
                            key = str(segment_path)
                            current_offset = offsets.get(key, 0)
                            try:
                                segment_size = int(segment_path.stat().st_size)
                            except OSError:
                                continue
                            events, new_offset = journal.read_segment_from(
                                segment_path, current_offset,
                                max_events=remaining_for_journal,
                            )
                            if events:
                                batch.extend(events)
                                candidates[key] = new_offset
                                budget -= len(events)
                                remaining_for_journal -= len(events)
                            if not events and current_offset < segment_size:
                                # A non-newline tail is not safe to skip; the
                                # next loop will retry the same byte boundary.
                                break
                    journal_cursor = (journal_cursor + 1) % count
                if batch:
                    for event in batch:
                        writer.submit(event)
                    pending_events = batch
                    pending_offsets = candidates
                    continue
                if graceful_stop or (stop_event.is_set() and not pending_events):
                    break
                write_status()
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
                    write_status()
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
                write_status()
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
        write_status(force_metrics=True)
        diagnostic(
            phase="stopped", status="stopped",
            progress={"applied": writer.snapshot(force_metrics=True).get("applied", 0), "queue_depth": len(writer._queue)},
        )
    except BaseException as exc:
        diagnostic(
            phase="exception", status="error",
            extra={"last_error": f"{type(exc).__name__}: {exc}"},
        )
        raise
    finally:
        writer.stop()
        profiler.publish(force=True)


class ProjectionWriterProcess:
    """Multiprocess owner wrapper used by V4/runtime integration."""

    def __init__(self, root: str | os.PathLike[str], *, db_name: str = "vnext-projection.duckdb",
                 queue_size: int = 8192, batch_size: int = 512,
                 journal_paths: list[str | os.PathLike[str]] | tuple[str | os.PathLike[str], ...] = (),
                 flush_interval_seconds: float = 0.1, start_method: str = "spawn",
                 mp_context=None, diagnostics_root: str | os.PathLike[str] | None = None,
                 run_id: str = "", staging_mode: str = "vectorized",
                  reuse_staging: bool = True,
                  profile_root: str | os.PathLike[str] | None = None,
                  profile_interval_seconds: float = 1.0):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / db_name
        self.status_path = self.root / "projection-writer-status.json"
        self.queue_size = max(1, int(queue_size))
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_seconds = min(5.0, max(0.01, float(flush_interval_seconds)))
        self.staging_mode = str(staging_mode or "vectorized").strip().lower()
        if self.staging_mode not in {"executemany", "vectorized"}:
            raise ValueError("staging_mode must be executemany or vectorized")
        self.reuse_staging = bool(reuse_staging)
        self.profile_root = Path(profile_root).resolve() if profile_root else None
        self.profile_interval_seconds = max(0.1, float(profile_interval_seconds))
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
                self.start_method, self.staging_mode, self.reuse_staging,
                str(self.profile_root) if self.profile_root else "",
                self.profile_interval_seconds,
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
                "staging_mode": self.staging_mode,
                "reuse_staging": self.reuse_staging,
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
            "staging_mode": self.staging_mode,
            "reuse_staging": self.reuse_staging,
            "restart_count": self.restart_count,
        }
