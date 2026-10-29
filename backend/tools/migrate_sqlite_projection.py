"""Offline, verified DuckDB -> unified SQLite projection migration.

Run with the service stopped: python -m tools.migrate_sqlite_projection --root ...
The source database, sidecar, journals and offsets are never replaced.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from decimal import Decimal
from pathlib import Path

from vnext.projection_writer import _ProjectionOwnerLock
from vnext.sqlite_projection import SQLiteProjectionWriter
from vnext.task_index import TASK_INDEX_COLUMNS


def _digest_rows(rows, digest):
    for row in rows:
        normalized = [str(Decimal(str(v)).normalize()) if isinstance(v, (int, float)) else v for v in row]
        digest.update(json.dumps(normalized, ensure_ascii=True, separators=(",", ":")).encode())
        digest.update(b"\n")


def migrate(root: Path, batch_size: int = 5000) -> dict:
    import duckdb

    root = root.resolve()
    source_path = root / "vnext-projection.duckdb"
    target_path = root / "vnext-projection.sqlite3"
    staging_path = root / "vnext-projection.migrating.sqlite3"
    source_lock = _ProjectionOwnerLock(str(source_path) + ".owner.lock")
    target_lock = _ProjectionOwnerLock(str(target_path) + ".owner.lock")
    source_lock.acquire(0)
    try:
        target_lock.acquire(0)
        try:
            if target_path.exists() or staging_path.exists():
                raise RuntimeError("target/staging exists; inspect it before retrying migration")
            offsets = json.loads((root / "journal-offsets.json").read_text())
            if not isinstance(offsets, dict) or any(
                not isinstance(k, str) or type(v) is not int or v < 0 for k, v in offsets.items()
            ):
                raise ValueError("invalid journal offsets")
            source = duckdb.connect(str(source_path), read_only=True)
            writer = SQLiteProjectionWriter(staging_path)
            try:
                writer.start()
                counts = {}
                for table, order in [("vnext_progress_events", "event_id"),
                                     ("vnext_task_index", "event_id"),
                                     ("vnext_projection", "business_day,campaign_type,campaign_id,partition_id,route_id")]:
                    columns = (list(TASK_INDEX_COLUMNS) if table == "vnext_task_index" else
                               [r[1] for r in writer._conn.execute(f"PRAGMA table_info({table})")])
                    names = ",".join(columns)
                    result = source.execute(f"SELECT {names} FROM {table} ORDER BY {order}")
                    digest = hashlib.sha256()
                    count = 0
                    while rows := result.fetchmany(max(1, batch_size)):
                        writer._conn.execute("BEGIN IMMEDIATE")
                        writer._conn.executemany(
                            f"INSERT INTO {table}({names}) VALUES ({','.join('?' for _ in columns)})", rows,
                        )
                        writer._conn.commit()
                        _digest_rows(rows, digest)
                        count += len(rows)
                    check = hashlib.sha256()
                    cursor = writer._conn.execute(f"SELECT {names} FROM {table} ORDER BY {order}")
                    checked = 0
                    while rows := cursor.fetchmany(max(1, batch_size)):
                        _digest_rows(rows, check)
                        checked += len(rows)
                    if checked != count or digest.digest() != check.digest():
                        raise RuntimeError(f"migration verification failed: {table}")
                    counts[table] = count
                writer.pending_offsets = offsets
                writer._conn.execute("BEGIN IMMEDIATE")
                writer._before_commit()
                writer._conn.commit()
                if writer.committed_offsets() != offsets:
                    raise RuntimeError("offset verification failed")
                if writer._conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("SQLite integrity check failed")
                writer._conn.execute(
                    "INSERT INTO projection_meta VALUES ('migration_complete','1')"
                )
                writer._conn.commit()
                busy, _, _ = writer._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if busy:
                    raise RuntimeError("migration checkpoint busy")
            finally:
                writer.stop()
                source.close()
            staging_path.rename(target_path)
            if os.name == "posix":
                directory = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            return {"target": str(target_path), "verified_rows": counts, "offsets": len(offsets)}
        finally:
            target_lock.release()
    finally:
        source_lock.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(migrate(args.root), ensure_ascii=False))


if __name__ == "__main__":
    main()
