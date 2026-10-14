"""Correctness harness for the opt-in DuckDB OCC coordinator.

The process mode is selected before importing ``database`` in each child so a
test cannot accidentally switch a live application between storage modes.
The scenarios deliberately use barriers: a serialized writer gate would
deadlock at the barrier, while independent DuckDB transactions must overlap.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import database


def _run_occ(tmp_path, source: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["MC_DUCKDB_WRITE_CONCURRENCY"] = "occ"
    env["PYTHONPATH"] = str(Path(database.__file__).resolve().parent)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source), str(tmp_path / "occ.duckdb")],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
    )


def test_occ_non_conflicting_transactions_overlap(tmp_path):
    result = _run_occ(
        tmp_path,
        """
        import sys, threading
        import database

        database.set_database_file(sys.argv[1])
        setup = database.get_connection()
        setup.execute("CREATE TABLE task_rows(id INTEGER PRIMARY KEY, value INTEGER)")
        setup.executemany("INSERT INTO task_rows VALUES (?, ?)", [(1, 0), (2, 0)])
        setup.close()

        barrier = threading.Barrier(2, timeout=4)
        entered = []
        errors = []

        def worker(row_id):
            def tx(conn):
                conn.execute("UPDATE task_rows SET value=value+1 WHERE id=?", (row_id,))
                entered.append(row_id)
                barrier.wait()
            try:
                database.write_transaction(tx, attempts=1, operation=f"test.row.{row_id}")
            except BaseException as exc:
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker, args=(row_id,)) for row_id in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(8)
        assert all(not thread.is_alive() for thread in threads), threads
        assert not errors, errors
        assert sorted(entered) == [1, 2]
        verify = database.get_connection()
        try:
            rows = verify.execute("SELECT id,value FROM task_rows ORDER BY id").fetchall()
            assert [(row["id"], row["value"]) for row in rows] == [(1, 1), (2, 1)]
        finally:
            verify.close()
        assert database.write_concurrency_mode() == "occ"
        assert database.write_lock_status()["active_transactions"] == 0
        """,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_occ_conflict_retries_the_complete_transaction(tmp_path):
    result = _run_occ(
        tmp_path,
        """
        import sys, threading
        import database

        database.set_database_file(sys.argv[1])
        setup = database.get_connection()
        setup.execute("CREATE TABLE counter(id INTEGER PRIMARY KEY, value INTEGER)")
        setup.execute("INSERT INTO counter VALUES (1, 0)")
        setup.close()

        barrier = threading.Barrier(2, timeout=4)
        attempts = {"a": 0, "b": 0}
        errors = []

        def worker(name):
            def tx(conn):
                attempts[name] += 1
                if attempts[name] == 1:
                    barrier.wait()
                conn.execute("UPDATE counter SET value=value+1 WHERE id=1")
            try:
                database.write_transaction(
                    tx, attempts=3, operation="test.same-row", base_delay=0.01,
                    retry_jitter=0,
                )
            except BaseException as exc:
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(8)
        assert all(not thread.is_alive() for thread in threads), threads
        assert not errors, errors
        assert attempts["a"] + attempts["b"] >= 3, attempts
        verify = database.get_connection()
        try:
            assert verify.execute("SELECT value FROM counter WHERE id=1").fetchone()["value"] == 2
        finally:
            verify.close()
        """,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_maintenance_exclusive_drains_and_blocks_new_admission(tmp_path):
    result = _run_occ(
        tmp_path,
        """
        import sys, threading, time
        import database

        database.set_database_file(sys.argv[1])
        setup = database.get_connection()
        setup.execute("CREATE TABLE maintenance_marker(value INTEGER)")
        setup.close()

        owner = database.get_connection().set_operation("test.long-reader")
        owner.execute("BEGIN TRANSACTION")
        owner.execute("INSERT INTO maintenance_marker VALUES (1)")

        entered = threading.Event()
        failures = []

        def maintenance():
            with database.maintenance_exclusive("test.backup", timeout=4):
                entered.set()
                time.sleep(0.15)

        maint = threading.Thread(target=maintenance)
        maint.start()
        time.sleep(0.15)
        assert not entered.is_set()
        assert database.write_lock_status()["maintenance_requested"] is True
        assert database.write_lock_status()["active_transactions"] == 1

        owner.rollback()
        owner.close()
        assert entered.wait(4)
        maint.join(4)
        assert not maint.is_alive()
        assert database.write_lock_status()["active_transactions"] == 0
        assert database.write_lock_status()["maintenance_active"] is False

        with database.maintenance_exclusive("test.block-new", timeout=2):
            def contender():
                conn = database.get_connection().set_operation("test.blocked")
                conn.write_lock_timeout = 0.1
                try:
                    conn.execute("SELECT 1")
                except BaseException as exc:
                    failures.append(exc)
                finally:
                    conn.close()
            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(2)
            assert not thread.is_alive()
        assert failures and isinstance(failures[0], TimeoutError), failures
        """,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_external_owner_lock_is_not_an_occ_retry():
    error = RuntimeError("could not set lock on file: database is locked")
    assert database.is_external_owner_error(error)
    assert not database.is_transient_write_error(error)
