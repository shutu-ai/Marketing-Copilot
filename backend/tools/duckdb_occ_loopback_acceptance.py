"""Small local loopback probe for the DuckDB admission/OCC coordinator.

This harness exercises only an isolated temporary DuckDB file.  It does not
connect to an SMSC, touch application runtime data, or leave a result file in
the repository.  The pytest suite remains the authoritative regression suite;
this command is intended for a repeatable operator smoke check.

Examples (from the repository root)::

    python backend/tools/duckdb_occ_loopback_acceptance.py --mode occ
    python backend/tools/duckdb_occ_loopback_acceptance.py --mode serialized
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("occ", "serialized"), default="occ")
    parser.add_argument("--timeout", type=float, default=8.0)
    return parser.parse_args()


def main() -> int:
    args = _args()
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "backend"))
    with tempfile.TemporaryDirectory(prefix="mc-duckdb-occ-loopback-") as temp_dir:
        os.environ["DUCKDB_PATH"] = str(Path(temp_dir) / "loopback.duckdb")
        os.environ["MC_DUCKDB_WRITE_CONCURRENCY"] = args.mode
        import database

        database.write_transaction(
            lambda conn: conn.execute(
                "CREATE TABLE coordinator_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
            ),
            operation="loopback.bootstrap",
        )
        database.write_transaction(
            lambda conn: conn.executemany(
                "INSERT INTO coordinator_probe VALUES (?, 0)", [(1,), (2,)]
            ),
            operation="loopback.seed",
        )

        entered = []
        entered_lock = threading.Lock()
        overlap = threading.Barrier(2) if args.mode == "occ" else None
        errors: list[str] = []

        def update_row(row_id: int) -> None:
            try:
                def update(conn):
                    with entered_lock:
                        entered.append(row_id)
                    if overlap is not None:
                        overlap.wait(timeout=args.timeout)
                    conn.execute(
                        "UPDATE coordinator_probe SET value=value+1 WHERE id=?",
                        (row_id,),
                    )

                database.write_transaction(
                    update,
                    attempts=3,
                    operation=f"loopback.row.{row_id}",
                )
            except Exception as exc:  # pragma: no cover - CLI failure detail
                errors.append(f"row {row_id}: {exc}")

        threads = [threading.Thread(target=update_row, args=(row_id,)) for row_id in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(args.timeout + 2)
        if any(thread.is_alive() for thread in threads):
            errors.append("non-conflicting transaction probe did not finish")

        values = database.read_transaction(
            lambda conn: conn.execute(
                "SELECT id,value FROM coordinator_probe ORDER BY id"
            ).fetchall(),
            operation="loopback.verify_rows",
        )
        row_values = {int(row["id"]): int(row["value"]) for row in values}
        if row_values != {1: 1, 2: 1}:
            errors.append(f"unexpected row values: {row_values}")
        if args.mode == "occ" and len(set(entered)) != 2:
            errors.append("OCC callbacks did not overlap at admission")

        holder = database.get_connection().set_operation("loopback.holder")
        holder.execute("BEGIN TRANSACTION")
        holder.execute("UPDATE coordinator_probe SET value=value+1 WHERE id=1")
        maintenance_entered = threading.Event()
        maintenance_done = threading.Event()

        def maintenance() -> None:
            try:
                with database.maintenance_exclusive("loopback.maintenance", timeout=args.timeout):
                    maintenance_entered.set()
                    time.sleep(0.05)
            finally:
                maintenance_done.set()

        maintenance_thread = threading.Thread(target=maintenance)
        maintenance_thread.start()
        requested = False
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status = database.coordinator.status()
            if status["maintenance_requested"]:
                requested = True
                break
            time.sleep(0.005)

        reader_done = threading.Event()

        def blocked_reader() -> None:
            try:
                database.read_transaction(
                    lambda conn: conn.execute("SELECT 1").fetchone(),
                    operation="loopback.blocked_reader",
                )
            finally:
                reader_done.set()

        reader_thread = threading.Thread(target=blocked_reader)
        reader_thread.start()
        time.sleep(0.05)
        blocked = requested and not reader_done.is_set()
        holder.commit()
        holder.close()
        maintenance_thread.join(args.timeout + 2)
        reader_thread.join(args.timeout + 2)
        if not maintenance_entered.is_set() or not maintenance_done.is_set():
            errors.append("maintenance barrier did not drain and complete")
        if not blocked:
            errors.append("new admission was not blocked while maintenance was requested")
        if database.coordinator.status()["active_transactions"] != 0:
            errors.append(f"active admissions leaked: {database.coordinator.status()}")

        result = {
            "harness": "duckdb_occ_loopback_acceptance",
            "mode": args.mode,
            "temporary_database": True,
            "smsc": "not_used",
            "non_conflicting_rows": row_values,
            "occ_callbacks_entered": len(set(entered)),
            "maintenance_requested_before_release": requested,
            "new_admission_blocked": blocked,
            "active_transactions_after": database.coordinator.status()["active_transactions"],
            "errors": errors,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
