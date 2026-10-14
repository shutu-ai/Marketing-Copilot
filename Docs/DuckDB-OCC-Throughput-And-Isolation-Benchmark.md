# DuckDB OCC Throughput and Isolation Benchmark

Status: local loopback evidence only; not a production canary result.

## Scope and controls

The benchmark uses `backend/tools/duckdb_occ_benchmark.py`. Each invocation
creates a fresh temporary DuckDB file, uses four worker threads, updates
independent rows, holds each short callback for 1 ms, verifies the final sum,
and deletes the temporary directory. It does not use runtime data, a real
SMSC, or a second process.

Commands:

```powershell
.\.p0-test-venv\Scripts\python.exe backend/tools/duckdb_occ_benchmark.py --mode serialized --workers 4 --operations 20 --hold-ms 1
.\.p0-test-venv\Scripts\python.exe backend/tools/duckdb_occ_benchmark.py --mode occ --workers 4 --operations 20 --hold-ms 1
```

## Current local result

These are current worktree runs on Windows, Python 3.13, DuckDB 1.2.0. They
are functional comparison evidence, not Linux/ext4 throughput certification.

| Mode | Operations | Elapsed | Operations/s | Sum / expected | Errors |
|---|---:|---:|---:|---:|---:|
| serialized | 80 | 0.260572 s | 307.017 | 80 / 80 | 0 |
| occ | 80 | 0.078694 s | 1016.596 | 80 / 80 | 0 |

The measured ratio in this small artificial workload is approximately 3.31x.
It should not be extrapolated to SMS throughput: the callback is deliberately
sleep-based, the rows are independent, and the database is local temporary
storage. Its purpose is to show that OCC removes the in-process writer gate
from non-conflicting DML while preserving correctness.

## Loopback isolation result

`backend/tools/duckdb_occ_loopback_acceptance.py` was run in both modes. Both
runs reported:

- `errors=[]`;
- row values `{1: 1, 2: 1}` after concurrent independent updates;
- `maintenance_requested_before_release=true`;
- `new_admission_blocked=true`;
- `active_transactions_after=0`;
- `smsc=not_used` and `temporary_database=true`.

The OCC run entered both independent callbacks before either completed. The
serialized run remained correct and exercised the compatibility path.

## Application regression evidence

Current targeted runs recorded during this work:

| Gate | Command family | Result |
|---|---|---|
| OCC coordinator | `test_duckdb_occ_coordinator.py` | 4 passed |
| lock/backup/realtime regression | existing lock/backup/progress selection | 21 passed, 1 skipped |
| realtime ingestion/scheduling | three realtime test modules | 46 passed |
| hot path and ingestion | phase 1/2/3, flash SMS, realtime progress | 55 passed, 2 skipped |
| complete backend suite | `pytest backend/tests -q` | 187 passed, 3 skipped, 4 warnings |
| frontend production build | `npm run build` from `frontend/` | passed; dependency/chunk warnings |

Final-gate reruns must be performed after any further code change and their
outputs must replace this table if they differ.

## Not yet proven

The following are intentionally not marked pass:

- equal-window Linux ext4 benchmark with a recorded kernel/filesystem/runtime;
- G3 single-campaign 1k/10k stress and G4 mixed-campaign capacity/fairness in
  the same current OCC build;
- G5 controlled failure-injection and recovery run;
- SLES 12 SP5 offline package/runtime acceptance;
- real SMSC delivery, DLR, disconnect, duplicate, and stop-fence canary.

Until those gates are current, the evidence supports local correctness and
comparative behavior only. It does not support `PRODUCTION_READY` or a global
OCC cutover.
