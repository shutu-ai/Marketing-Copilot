from __future__ import annotations

import sqlite3
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import pytest

from vnext.sqlite_projection import SQLiteProjectionWriter
from vnext.task_index import connect_reader, is_ready


def event(i, **changes):
    result = dict(event_id=str(i), partition_id=1, ordinal=i, campaign_type="batch",
                  campaign_id=2, route_id=3, state="socket_written", parts_written=2,
                  parts_total=2, business_day=datetime.now(timezone.utc).date().isoformat(),
                  created_at=1000 + i, taken=1, normalized_msisdn="123", message="多语言 ሰላም")
    result.update(changes)
    return result


def test_single_transaction_replay_and_reader(tmp_path):
    path = tmp_path / "vnext-projection.sqlite3"
    writer = SQLiteProjectionWriter(path)
    writer.start()
    try:
        for i in range(20):
            writer.submit(event(i))
        writer.pending_offsets = {"journal.1": 42}
        writer.flush()
        reader = connect_reader(path)
        try:
            assert is_ready(reader)
            reader.execute("BEGIN")
            assert reader.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0] == 20
            for i in range(20):
                writer.submit(event(i))
            writer.submit(event(20))
            writer.pending_offsets = {"journal.1": 99}
            writer.flush()
            assert reader.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0] == 20
        finally:
            reader.close()
        assert writer.daily_summary()[0]["socket_written"] == 21
        assert writer.daily_summary()[0]["socket_written_pdus"] == 42
        assert writer.committed_offsets() == {"journal.1": 99}
        assert writer.snapshot()["daily_totals"][0]["taken"] == 21
    finally:
        writer.stop()
    writer = SQLiteProjectionWriter(path)
    writer.start()
    try:
        assert writer.committed_offsets() == {"journal.1": 99}
        assert writer.daily_summary()[0]["socket_written"] == 21
        writer.age_expired(reference_day="2100-01-01")
        assert writer.daily_summary() == []
    finally:
        writer.stop()


def test_failed_commit_rolls_back_index_counters_and_offsets(tmp_path):
    writer = SQLiteProjectionWriter(tmp_path / "atomic.sqlite3")
    writer.start()
    conn = writer._conn

    class FailCommit:
        def execute(self, sql, *args):
            if sql == "COMMIT":
                raise sqlite3.OperationalError("injected failure")
            return conn.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(conn, name)

    try:
        writer._conn = FailCommit()
        writer.submit(event(1))
        writer.pending_offsets = {"segment": 123}
        with pytest.raises(sqlite3.OperationalError):
            writer.flush()
        assert writer.committed_offsets() == {}
        assert conn.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0] == 0
        assert writer._applied == 0
        writer._conn = conn
        writer.flush()
        assert writer.committed_offsets() == {"segment": 123}
        assert writer.daily_summary()[0]["socket_written"] == 1
    finally:
        writer._conn = conn
        writer.stop()


def test_offline_migration_preserves_history_and_offsets(tmp_path):
    import json
    from tools.migrate_sqlite_projection import migrate
    from vnext.projection_writer import ProjectionWriter

    old = ProjectionWriter(tmp_path / "vnext-projection.duckdb")
    old.start()
    try:
        for i in range(3):
            old.submit(event(i))
        old.flush()
        (tmp_path / "journal-offsets.json").write_text(json.dumps({"old.segment": 456}))
        with pytest.raises(Exception, match="owner|owned"):
            migrate(tmp_path)
    finally:
        old.stop()
    assert migrate(tmp_path, batch_size=2)["verified_rows"]["vnext_progress_events"] == 3
    new = SQLiteProjectionWriter(tmp_path / "vnext-projection.sqlite3")
    new.start()
    try:
        assert new.committed_offsets() == {"old.segment": 456}
        new.submit(event(1))
        new.submit(event(3))
        new.flush()
        assert new.daily_summary()[0]["socket_written"] == 4
    finally:
        new.stop()


@pytest.mark.parametrize("after_commit", [False, True])
def test_process_death_at_commit_boundary(tmp_path, after_commit):
    path = tmp_path / "crash.sqlite3"
    payload = json.dumps(event(1))
    script = '''
import json, os, sys
from vnext.sqlite_projection import SQLiteProjectionWriter
w = SQLiteProjectionWriter(sys.argv[1]); w.start(); c = w._conn
class Crash:
    def execute(self, sql, *args):
        if sql == "COMMIT":
            if sys.argv[3] == "True": c.execute(sql, *args)
            os._exit(77)
        return c.execute(sql, *args)
    def __getattr__(self, key): return getattr(c, key)
w._conn = Crash(); w.submit(json.loads(sys.argv[2]))
w.pending_offsets = {"segment":123}; w.flush()
'''
    env = dict(os.environ, PYTHONPATH=str(__import__("pathlib").Path(__file__).resolve().parents[1]))
    result = subprocess.run([sys.executable, "-c", script, str(path), payload, str(after_commit)], env=env)
    assert result.returncode == 77
    writer = SQLiteProjectionWriter(path)
    writer.start()
    try:
        assert writer.committed_offsets() == ({"segment": 123} if after_commit else {})
        writer.submit(json.loads(payload))
        writer.pending_offsets = {"segment": 123}
        writer.flush()
        assert writer.daily_summary()[0]["socket_written"] == 1
        assert writer._conn.execute("SELECT COUNT(*) FROM vnext_task_index").fetchone()[0] == 1
    finally:
        writer.stop()


def test_sqlite_process_consumes_journal_with_atomic_offsets(tmp_path, monkeypatch):
    from vnext.projection_writer import ProjectionWriterProcess, ProgressJournal
    monkeypatch.setenv("MC_VNEXT_PROJECTION_ENGINE", "sqlite")
    journal = ProgressJournal(tmp_path / "progress.jsonl")
    journal.append_many([event(i) for i in range(10)])
    process = ProjectionWriterProcess(tmp_path / "projection", journal_paths=[journal.path])
    process.start()
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            snapshot = process.snapshot()
            if snapshot.get("applied", 0) >= 10:
                break
            time.sleep(.05)
        assert snapshot["applied"] == 10
        assert snapshot["projection_storage"] == "one_writer_one_sqlite"
        assert snapshot["ready"]
    finally:
        process.stop()
    conn = sqlite3.connect(process.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM projection_offsets").fetchone()[0] == 1
        assert conn.execute("SELECT SUM(socket_written) FROM vnext_projection").fetchone()[0] == 10
    finally:
        conn.close()


def test_write_collision_retries_without_losing_batch(tmp_path):
    path = tmp_path / "collision.sqlite3"
    writer = SQLiteProjectionWriter(path)
    writer.start()
    writer._conn.execute("PRAGMA busy_timeout=20")
    other = sqlite3.connect(path)
    try:
        other.execute("BEGIN IMMEDIATE")
        writer.submit(event(1))
        writer.pending_offsets = {"segment": 99}
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            writer.flush()
        assert len(writer._queue) == 1
        assert writer.committed_offsets() == {}
        other.rollback()
        assert writer.flush() == 1
        assert writer.daily_summary()[0]["socket_written"] == 1
    finally:
        other.close()
        writer.stop()


def test_pinned_reader_applies_backpressure_then_recovers(tmp_path, monkeypatch):
    from vnext import sqlite_projection
    from vnext.projection_writer import ProjectionWriterBusy
    path = tmp_path / "pinned.sqlite3"
    writer = SQLiteProjectionWriter(path)
    writer.start()
    reader = sqlite3.connect(path)
    try:
        writer.submit(event(1)); writer.flush()
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM vnext_task_index").fetchall()
        writer.submit(event(2)); writer.flush()
        monkeypatch.setattr(sqlite_projection, "WAL_LIMIT_BYTES", 1)
        writer.submit(event(3))
        with pytest.raises(ProjectionWriterBusy, match="pinned"):
            writer.flush()
        assert len(writer._queue) == 1
        reader.rollback()
        writer.flush()
        assert writer.daily_summary()[0]["socket_written"] == 3
    finally:
        reader.close()
        writer.stop()


def test_empty_sqlite_file_cannot_bypass_migration(tmp_path, monkeypatch):
    from vnext.projection_writer import ProjectionWriterProcess
    (tmp_path / "vnext-projection.duckdb").touch()
    (tmp_path / "vnext-projection.sqlite3").touch()
    monkeypatch.setenv("MC_VNEXT_PROJECTION_ENGINE", "sqlite")
    with pytest.raises(RuntimeError, match="verified offline"):
        ProjectionWriterProcess(tmp_path)
