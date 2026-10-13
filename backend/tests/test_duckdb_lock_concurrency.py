"""Focused regressions for the single-owner DuckDB and process writer gates."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import database
from services import metrics_service


@pytest.mark.skipif(platform.system() != "Linux", reason="cross-process DuckDB locking is accepted in WSL Linux")
def test_second_process_cannot_open_active_duckdb_rw(tmp_path):
    db_path = tmp_path / "active.duckdb"
    holder_code = textwrap.dedent(
        """
        import sys, time, duckdb
        path = sys.argv[1]
        conn = duckdb.connect(path)
        conn.execute("CREATE TABLE IF NOT EXISTS holder_marker(value INTEGER)")
        conn.execute("INSERT INTO holder_marker VALUES (1)")
        print("READY", flush=True)
        time.sleep(8)
        conn.close()
        """
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, str(db_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "READY"
        contender = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, duckdb; duckdb.connect(sys.argv[1])",
                str(db_path),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        diagnostics = f"stdout={contender.stdout}\nstderr={contender.stderr}"
        assert contender.returncode != 0, diagnostics
        assert "lock" in diagnostics.lower() or "conflict" in diagnostics.lower(), diagnostics
    finally:
        holder.terminate()
        holder.wait(timeout=10)


def test_write_gate_timeout_reports_owner_and_releases_after_close(tmp_path):
    database.set_database_file(str(tmp_path / "gate.duckdb"))
    setup = database.get_connection()
    setup.execute("CREATE TABLE gate_marker(value INTEGER)")
    setup.close()

    owner = database.get_connection().set_operation("test.injected_hold")
    owner.execute("BEGIN TRANSACTION")
    owner.execute("INSERT INTO gate_marker VALUES (1)")

    observed = {}

    def contender():
        conn = database.get_connection().set_operation("test.contender")
        conn.write_lock_timeout = 0.1
        try:
            conn.execute("INSERT INTO gate_marker VALUES (2)")
        except Exception as exc:  # the assertion below checks the stable type/message
            observed["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=contender, name="duckdb-contender")
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert isinstance(observed.get("error"), TimeoutError)
    message = str(observed["error"])
    assert "test.injected_hold" in message
    assert "holder_thread" in message
    assert database.write_lock_status()["held"] is True

    owner.rollback()
    owner.close()
    assert database.write_lock_status()["held"] is False

    database.write_transaction(
        lambda conn: conn.execute("INSERT INTO gate_marker VALUES (3)"),
        operation="test.after_release",
    )
    verify = database.get_connection()
    try:
        assert verify.execute("SELECT COUNT(*) AS count FROM gate_marker").fetchone()["count"] == 1
    finally:
        verify.close()

    metric_names = {item["name"] for item in metrics_service.snapshot()["counters"]}
    assert "marketing_copilot_database_write_lock_wait_seconds_count" in metric_names
    assert "marketing_copilot_database_write_lock_hold_seconds_count" in metric_names
    assert "marketing_copilot_database_write_transaction_seconds_count" in metric_names
    assert "marketing_copilot_database_write_lock_timeouts_total" in metric_names
