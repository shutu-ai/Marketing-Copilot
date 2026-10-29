"""Concurrent read sidecar for the VNext logical-task projection.

DuckDB remains the durable projection/event store and has one process owner.
The operations page is a read-heavy workload, so it uses this small SQLite
WAL sidecar.  The projection owner is the only writer; API requests open
short-lived read-only SQLite connections and never contend with the DuckDB
writer lock.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


TASK_INDEX_TABLE = "vnext_task_index"
TASK_INDEX_COLUMNS = (
    "event_id",
    "partition_id",
    "ordinal",
    "campaign_type",
    "campaign_id",
    "route_id",
    "normalized_msisdn",
    "language",
    "campaign_name",
    "state",
    "parts_written",
    "parts_total",
    "message",
    "error",
    "created_at",
    "business_day",
    "expires_at",
    "last_event_at",
)

TASK_INDEX_SQLITE_INDEXES = (
    ("idx_vnext_task_day_order", "business_day, created_at DESC, ordinal DESC, event_id DESC"),
    ("idx_vnext_task_day_type_order", "business_day, campaign_type, created_at DESC, ordinal DESC, event_id DESC"),
    ("idx_vnext_task_day_state_order", "business_day, state, created_at DESC, ordinal DESC, event_id DESC"),
    ("idx_vnext_task_day_msisdn_order", "business_day, normalized_msisdn, created_at DESC, ordinal DESC, event_id DESC"),
    ("idx_vnext_task_campaign_order", "campaign_type, campaign_id, created_at DESC, ordinal DESC, event_id DESC"),
)


def default_task_index_path(projection_db_path: str | Path) -> Path:
    """Return the stable sidecar path for a projection database."""

    projection = Path(projection_db_path).resolve()
    if projection.name == "vnext-projection.sqlite3":
        return projection
    return projection.with_name("vnext-task-index.sqlite3")


def _configure(connection: sqlite3.Connection, *, writer: bool) -> None:
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")
    if writer:
        # WAL keeps API readers independent of the projection process's short
        # SQLite upsert transactions.  NORMAL is durable enough here because
        # the authoritative event and progress journals remain on disk.
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
    else:
        connection.execute("PRAGMA query_only=ON")


def connect_writer(path: str | Path) -> sqlite3.Connection:
    """Open/create the sidecar writer and its bounded query indexes."""

    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(target), timeout=5.0)
    _configure(connection, writer=True)
    connection.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {TASK_INDEX_TABLE} (
            event_id TEXT PRIMARY KEY,
            partition_id INTEGER NOT NULL DEFAULT 0,
            ordinal INTEGER NOT NULL DEFAULT 0,
            campaign_type TEXT NOT NULL DEFAULT '',
            campaign_id INTEGER NOT NULL DEFAULT 0,
            route_id INTEGER NOT NULL DEFAULT 0,
            normalized_msisdn TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT '',
            campaign_name TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT 'queued',
            parts_written INTEGER NOT NULL DEFAULT 0,
            parts_total INTEGER NOT NULL DEFAULT 0,
            message TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL DEFAULT 0,
            business_day TEXT NOT NULL DEFAULT '',
            expires_at REAL NOT NULL DEFAULT 0,
            last_event_at REAL NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_vnext_task_day_order
            ON {TASK_INDEX_TABLE}(business_day, created_at DESC, ordinal DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS idx_vnext_task_day_type_order
            ON {TASK_INDEX_TABLE}(business_day, campaign_type, created_at DESC, ordinal DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS idx_vnext_task_day_state_order
            ON {TASK_INDEX_TABLE}(business_day, state, created_at DESC, ordinal DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS idx_vnext_task_day_msisdn_order
            ON {TASK_INDEX_TABLE}(business_day, normalized_msisdn, created_at DESC, ordinal DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS idx_vnext_task_campaign_order
            ON {TASK_INDEX_TABLE}(campaign_type, campaign_id, created_at DESC, ordinal DESC, event_id DESC);
        CREATE TABLE IF NOT EXISTS task_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
        );
        """
    )
    connection.commit()
    return connection


def connect_reader(path: str | Path) -> sqlite3.Connection | None:
    """Open a read-only sidecar connection, or return ``None`` if absent."""

    target = Path(path).resolve()
    if not target.is_file():
        return None
    uri = f"file:{target.as_posix()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=1.0)
        _configure(connection, writer=False)
        return connection
    except sqlite3.Error:
        return None


def is_ready(connection: sqlite3.Connection) -> bool:
    try:
        row = connection.execute(
            "SELECT value FROM task_index_meta WHERE key='ready'"
        ).fetchone()
    except sqlite3.Error:
        return False
    return bool(row and str(row[0]) == "1")


def mark_ready(connection: sqlite3.Connection, ready: bool = True) -> None:
    connection.execute(
        """INSERT INTO task_index_meta(key,value) VALUES('ready',?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
        ("1" if ready else "0",),
    )
    connection.commit()


def upsert_rows(connection: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    """Apply latest-state rows in one short WAL transaction."""

    values = list(rows)
    if not values:
        return 0
    placeholders = ",".join("?" for _ in TASK_INDEX_COLUMNS)
    connection.executemany(
        f"""INSERT INTO {TASK_INDEX_TABLE}({','.join(TASK_INDEX_COLUMNS)})
            VALUES ({placeholders})
            ON CONFLICT(event_id) DO UPDATE SET
                partition_id=excluded.partition_id,
                ordinal=excluded.ordinal,
                campaign_type=excluded.campaign_type,
                campaign_id=excluded.campaign_id,
                route_id=excluded.route_id,
                normalized_msisdn=CASE WHEN excluded.normalized_msisdn <> ''
                    THEN excluded.normalized_msisdn ELSE {TASK_INDEX_TABLE}.normalized_msisdn END,
                language=CASE WHEN excluded.language <> ''
                    THEN excluded.language ELSE {TASK_INDEX_TABLE}.language END,
                campaign_name=CASE WHEN excluded.campaign_name <> ''
                    THEN excluded.campaign_name ELSE {TASK_INDEX_TABLE}.campaign_name END,
                state=excluded.state,
                parts_written=excluded.parts_written,
                parts_total=MAX(excluded.parts_total,{TASK_INDEX_TABLE}.parts_total),
                message=CASE WHEN excluded.message <> ''
                    THEN excluded.message ELSE {TASK_INDEX_TABLE}.message END,
                error=excluded.error,
                created_at=MIN({TASK_INDEX_TABLE}.created_at,excluded.created_at),
                business_day=CASE WHEN excluded.business_day <> ''
                    THEN excluded.business_day ELSE {TASK_INDEX_TABLE}.business_day END,
                expires_at=MAX(excluded.expires_at,{TASK_INDEX_TABLE}.expires_at),
                last_event_at=excluded.last_event_at
             WHERE excluded.last_event_at >= {TASK_INDEX_TABLE}.last_event_at""",
        values,
    )
    return len(values)


def delete_rows(connection: sqlite3.Connection, event_ids: Iterable[str]) -> int:
    """Delete a bounded set of logical-task rows in one WAL transaction.

    The projection database is authoritative.  The sidecar only needs the
    same committed task ids removed after DuckDB aging succeeds.  Chunking the
    ``IN`` predicates keeps this compatible with SQLite builds that retain the
    older 999-parameter limit.
    """

    values = list(dict.fromkeys(str(value) for value in event_ids if value))
    if not values:
        return 0
    deleted = 0
    for start in range(0, len(values), 900):
        chunk = values[start:start + 900]
        placeholders = ",".join("?" for _ in chunk)
        cursor = connection.execute(
            f"DELETE FROM {TASK_INDEX_TABLE} WHERE event_id IN ({placeholders})",
            chunk,
        )
        deleted += max(0, int(cursor.rowcount or 0))
    connection.commit()
    return deleted


def rebuild_from_duckdb(
    projection_db_path: str | Path,
    sidecar_path: str | Path | None = None,
    *,
    batch_size: int = 50_000,
) -> dict:
    """Rebuild the sidecar offline from the canonical DuckDB task index.

    The caller must stop the projection writer first.  This is deliberately
    an operator/migration operation, never an application-startup fallback.
    """

    import duckdb

    projection = Path(projection_db_path).resolve()
    sidecar = Path(sidecar_path).resolve() if sidecar_path else default_task_index_path(projection)
    source = duckdb.connect(str(projection), read_only=True)
    target = connect_writer(sidecar)
    started = __import__("time").perf_counter()
    rows_seen = 0
    try:
        # The canonical DuckDB task index has one row per event_id.  Rebuild
        # the offline sidecar as a bulk load: maintaining five SQLite indexes
        # and evaluating the upsert conflict path for every historical row is
        # needlessly expensive during the one-time cutover.
        for index_name, _columns in TASK_INDEX_SQLITE_INDEXES:
            target.execute(f"DROP INDEX IF EXISTS {index_name}")
        target.execute(f"DELETE FROM {TASK_INDEX_TABLE}")
        target.execute("DELETE FROM task_index_meta")
        target.commit()
        source_result = source.execute(
            f"SELECT {','.join(TASK_INDEX_COLUMNS)} FROM {TASK_INDEX_TABLE}"
        )
        size = max(1000, int(batch_size))
        placeholders = ",".join("?" for _ in TASK_INDEX_COLUMNS)
        insert_sql = (
            f"INSERT INTO {TASK_INDEX_TABLE}({','.join(TASK_INDEX_COLUMNS)}) "
            f"VALUES ({placeholders})"
        )
        while True:
            rows = source_result.fetchmany(size)
            if not rows:
                break
            target.execute("BEGIN")
            target.executemany(insert_sql, [tuple(row) for row in rows])
            target.execute("COMMIT")
            rows_seen += len(rows)
        for index_name, columns in TASK_INDEX_SQLITE_INDEXES:
            target.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {TASK_INDEX_TABLE}({columns})"
            )
        mark_ready(target, True)
        return {
            "path": str(sidecar),
            "rows": rows_seen,
            "ready": True,
            "elapsed_seconds": round(__import__("time").perf_counter() - started, 3),
        }
    finally:
        target.close()
        source.close()

