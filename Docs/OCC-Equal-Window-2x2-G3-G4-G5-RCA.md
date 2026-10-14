# OCC Equal-Window 2x2 G3/G4/G5 RCA

## Decision

The current local result is `MORE_EVIDENCE_NEEDED`; the production decision is `NO_GO`.
The loopback simulator is not a real-SMSC or SLES12SP5 qualification. No raw runtime
artifact is copied into the repository; this document references the ext4 evidence
directory only.

Current source under review: `c9890a76040f92dabd50c4b50754d44716d6daca`.

## Reproduction envelope

The failing arm used AlmaLinux-8 WSL on ext4 `/tmp`, CPython 3.11, DuckDB 1.2.0,
four loopback SMPP binds, window 50, route TPS 200, eight task workers, two
realtime ingestion workers, two watch-copy workers, and queue capacity 2000. The
input was `stress-1k` with ten files and 100 rows per file. `delivery_receipt_mode`
was disabled and the live environment was not used.

## Observed sequence

1. Watch-copy registration and task creation initially progress; individual files
   can reach 100 tasks and 100 accepted submissions.
2. Under `MC_DUCKDB_WRITE_CONCURRENCY=occ` plus `--send-mode fast`, concurrent
   ingestion-state transactions report DuckDB write-write conflicts.
3. A later progress/registration attempt cannot find the expected
   `processed_files` row. The worker classifies this as an UNKNOWN or transient DB
   failure and schedules a retry.
4. The final ledger does not close: processed-file rows are missing or, in earlier
   probes, the unique-key mirror contained concatenated filename/status values.
5. Because the authoritative G3 closure was not reached, no duplicate-send or
   zero-backlog claim is made for the current arm.

Primary current evidence:

- `/tmp/mc-occ-2x2-giYoXB/runs/marketing-copilot-stress-1k-full-5f52f748`
- `/tmp/mc-occ-2x2-giYoXB/results/g3-occ-fast-lock-1.stdout.log`

The later worktree probe directories (for example `b07c18d3`) were produced
after uncommitted experimental edits and are not counted as current acceptance
evidence. The historical `g3-*final-*` JSON files were produced before the
current source revision and are also excluded.

## Causal chain

The code path has three different kinds of writes:

| Path | Role | Failure relevance |
|---|---|---|
| `ingestion_files` and `ingestion_chunks` | authoritative lease/checkpoint state | OCC conflict can abort a whole logical checkpoint transaction |
| `sms_tasks` and SMPP result tables | task/send ledger | fast mode adds high-frequency concurrent writes |
| `processed_files` | non-authoritative audit/UI mirror with a unique business key | current DuckDB 1.2.0 probes show missing/corrupted mirror rows after contention |

The earlier fixes removed advisory `campaign_fairness_state` writes from task and
file claims (`6339b40`, `1511d4c`), made realtime processed registration
idempotent (`fa3e9c9`), and fenced processed-file mirror writes (`c9890a7`).
Those changes address confirmed hot-row and mirror races, but the current equal-
window OCC/fast run still fails. Therefore the RCA does not claim that the
remaining conflict is solved.

## Why the historical PASS records do not close the gate

Three serialized/reliable, three OCC/reliable, and three serialized/fast 1K
records exist under `/tmp/mc-occ-2x2-giYoXB/results/`, but they were generated at
the earlier `1511d4c` source revision. They are useful regression history only.
The current revision has not completed three independent runs for each of the
four arms, and current OCC/fast has an observed failure. Reusing those records as
current evidence would make the acceptance report internally inconsistent.

## Remediation required before another go/no-go review

1. Reproduce the failure with current source while tracing transaction operation,
   table, row key, and retry outcome; keep the trace outside the repository.
2. Treat `processed_files` as a mirror: do not let a mirror update abort the
   authoritative ingestion lease/chunk/task transaction, and reconcile the mirror
   from authoritative state after a retry/restart.
3. Validate the fix against DuckDB 1.2.0 and the target supported DuckDB version;
   do not infer safety from a single low-concurrency smoke run.
4. Rerun G3 as 4 arms × 3 independent processes at the exact equal-window settings.
5. Only after G3 is clean, run G4 isolation and the G5 fault matrix plus an
   effective 600-second soak. Then execute the SLES12SP5 and real-SMSC canary
   gates separately with approval and rollback evidence.

## Non-claims

This RCA makes no production-readiness claim, no SLES12SP5 claim, no real-SMSC
claim, and no claim that the current OCC/fast path is safe for automatic retry of
unknown submissions.
