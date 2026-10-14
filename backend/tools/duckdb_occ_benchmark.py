"""Compare the local serialized and OCC admission paths on isolated DuckDB.

The benchmark deliberately uses independent rows and a configurable short
critical section. It measures coordinator behavior, not SMSC throughput. The
database is created under a temporary directory and is deleted automatically.
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("occ", "serialized"), required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--operations", type=int, default=40)
    parser.add_argument("--hold-ms", type=float, default=2.0)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "backend"))
    os.environ["MC_DUCKDB_WRITE_CONCURRENCY"] = args.mode

    with tempfile.TemporaryDirectory(prefix="mc-duckdb-occ-benchmark-") as temp_dir:
        os.environ["DUCKDB_PATH"] = str(Path(temp_dir) / "benchmark.duckdb")
        import database

        workers = max(1, int(args.workers))
        operations = max(1, int(args.operations))
        hold_seconds = max(0.0, float(args.hold_ms)) / 1000.0
        database.write_transaction(
            lambda conn: conn.execute(
                "CREATE TABLE benchmark_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
            ),
            operation="benchmark.bootstrap",
        )
        database.write_transaction(
            lambda conn: conn.executemany(
                "INSERT INTO benchmark_rows VALUES (?, 0)",
                [(index,) for index in range(workers)],
            ),
            operation="benchmark.seed",
        )

        errors: list[str] = []
        entered: set[int] = set()
        entered_lock = threading.Lock()

        def worker(worker_id: int) -> None:
            try:
                for _ in range(operations):
                    def update(conn):
                        with entered_lock:
                            entered.add(worker_id)
                        if hold_seconds:
                            time.sleep(hold_seconds)
                        conn.execute(
                            "UPDATE benchmark_rows SET value=value+1 WHERE id=?",
                            (worker_id,),
                        )

                    database.write_transaction(
                        update,
                        attempts=3,
                        operation=f"benchmark.worker.{worker_id}",
                    )
            except Exception as exc:  # pragma: no cover - CLI failure detail
                errors.append(f"worker {worker_id}: {exc}")

        threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(workers)]
        started = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = max(0.000001, time.perf_counter() - started)
        values = database.read_transaction(
            lambda conn: conn.execute(
                "SELECT SUM(value) total FROM benchmark_rows"
            ).fetchone()["total"],
            operation="benchmark.verify",
        )
        expected = workers * operations
        result = {
            "benchmark": "duckdb_occ_vs_serialized",
            "mode": args.mode,
            "workers": workers,
            "operations_per_worker": operations,
            "hold_ms": float(args.hold_ms),
            "total_operations": expected,
            "elapsed_seconds": round(elapsed, 6),
            "operations_per_second": round(expected / elapsed, 3),
            "callbacks_entered": len(entered),
            "sum_value": int(values or 0),
            "expected_sum": expected,
            "temporary_database": True,
            "smsc": "not_used",
            "errors": errors,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if not errors and int(values or 0) == expected else 1


if __name__ == "__main__":
    raise SystemExit(main())
