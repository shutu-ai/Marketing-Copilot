"""Single-transaction live projection; DuckDB is outside this write path."""
from __future__ import annotations

import time
from pathlib import Path

from .projection_writer import ProjectionWriter, ProjectionWriterBusy
from .task_index import connect_writer, mark_ready, upsert_rows


COUNTERS = (
    "queued", "taken", "accepted", "socket_written", "write_failed", "unknown",
    "cancelled", "expired", "response_success", "response_error",
)
KEYS = ("business_day", "campaign_type", "campaign_id", "partition_id", "route_id")
WAL_LIMIT_BYTES = 256 * 1024 * 1024


class SQLiteProjectionWriter(ProjectionWriter):
    """Reuse lifecycle/retention/status, with one DB and no temporary-table DML."""

    def __init__(self, db_path, **kwargs):
        kwargs["task_index_path"] = db_path
        super().__init__(db_path, **kwargs)
        self.pending_offsets: dict[str, int] = {}

    def _start_connection(self):
        self._conn = connect_writer(self.db_path)
        # This database owns consumption positions, not just a repairable cache.
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")
        self._conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS projection_offsets (
                path TEXT PRIMARY KEY, position INTEGER NOT NULL CHECK(position>=0)
            );
            CREATE TABLE IF NOT EXISTS projection_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vnext_progress_events (
                event_id TEXT PRIMARY KEY, partition_id INTEGER NOT NULL,
                ordinal INTEGER NOT NULL, campaign_type TEXT NOT NULL,
                campaign_id INTEGER NOT NULL, route_id INTEGER NOT NULL,
                normalized_msisdn TEXT NOT NULL, language TEXT NOT NULL,
                state TEXT NOT NULL, parts_written INTEGER NOT NULL,
                parts_total INTEGER NOT NULL, message TEXT NOT NULL,
                error TEXT NOT NULL, created_at REAL NOT NULL,
                business_day TEXT NOT NULL, expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_vnext_progress_day_order
                ON vnext_progress_events(business_day,created_at,ordinal,event_id);
            CREATE TABLE IF NOT EXISTS vnext_projection (
                business_day TEXT NOT NULL, campaign_type TEXT NOT NULL,
                campaign_id INTEGER NOT NULL, partition_id INTEGER NOT NULL,
                route_id INTEGER NOT NULL,
                {','.join(name+' INTEGER NOT NULL DEFAULT 0' for name in COUNTERS)},
                socket_written_pdus INTEGER NOT NULL DEFAULT 0,
                last_ordinal INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL,
                PRIMARY KEY({','.join(KEYS)})
            );
        """)
        mark_ready(self._conn)
        self._task_index_ready = True

    def committed_offsets(self):
        return dict(self._conn.execute("SELECT path,position FROM projection_offsets"))

    def _before_commit(self):
        self._conn.executemany(
            "INSERT INTO projection_offsets VALUES (?,?) ON CONFLICT(path) "
            "DO UPDATE SET position=MAX(position,excluded.position)",
            list(self.pending_offsets.items()),
        )

    def flush(self, *, max_events=None):
        if self._conn is None:
            self.start()
        wal = Path(self.db_path + "-wal")
        if wal.exists() and wal.stat().st_size >= WAL_LIMIT_BYTES:
            # A pinned reader must cause backpressure, never unbounded WAL growth.
            busy, log, done = self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            if busy or done < log:
                raise ProjectionWriterBusy("projection WAL pinned by a reader; retry after reader closes")
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return super().flush(max_events=max_events)

    def _apply_batch(self, batch):
        upsert_rows(self._conn, self._latest_task_index_rows(batch))
        groups = {}
        duplicates = new_count = 0
        for event in batch:
            if str(event.get("event_kind") or "") == "task_index":
                continue
            row = self._event_row(event)
            cursor = self._conn.execute(
                "INSERT INTO vnext_progress_events VALUES (" + ",".join("?" * 16) + ") "
                "ON CONFLICT(event_id) DO NOTHING", row[:16],
            )
            if not cursor.rowcount:
                duplicates += 1
                continue
            new_count += 1
            key = (row[14], row[3], row[4], row[1], row[5])
            values = list(row[16:]) + [row[9] if row[8] == "socket_written" else 0, row[2], row[13]]
            if key not in groups:
                groups[key] = values
            else:
                previous = groups[key]
                groups[key] = [a + b for a, b in zip(previous[:11], values[:11])] + [
                    max(previous[11], values[11]), max(previous[12], values[12]),
                ]
        columns = (*KEYS, *COUNTERS, "socket_written_pdus", "last_ordinal", "updated_at")
        increments = (*COUNTERS, "socket_written_pdus")
        updates = [f"{c}={c}+excluded.{c}" for c in increments]
        updates += ["last_ordinal=MAX(last_ordinal,excluded.last_ordinal)",
                    "updated_at=MAX(updated_at,excluded.updated_at)"]
        self._conn.executemany(
            f"INSERT INTO vnext_projection({','.join(columns)}) VALUES "
            f"({','.join('?' for _ in columns)}) ON CONFLICT({','.join(KEYS)}) "
            f"DO UPDATE SET {','.join(updates)}",
            [(*key, *values) for key, values in groups.items()],
        )
        self._applied += new_count
        self._duplicates += duplicates
        self._last_batch_new, self._last_batch_duplicates = new_count, duplicates
        self._last_applied_at = time.time()
        self._metrics["projection_events_applied"] = self._applied
        for key, value in {"batches_total": 1, "events_read": len(batch),
                           "events_new": new_count, "events_replayed": duplicates}.items():
            self._metrics[key] += value
        self._batch_sizes.append(len(batch))
        return new_count

    def _checkpoint_if_due(self):
        # SQLite auto-checkpoints incrementally; pinned readers are bounded in flush.
        pass

    def snapshot(self, **kwargs):
        value = super().snapshot(**kwargs)
        value.update(projection_storage="one_writer_one_sqlite", checkpoint_mode="sqlite_wal")
        return value
