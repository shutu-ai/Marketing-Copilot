# Workflow Isolation and Stop-Fence RCA

Status: remediation implemented; current production decision is
`MORE_EVIDENCE_NEEDED`.

## Problem statement

The application historically treated one Python writer lock as the database
concurrency policy. That made unrelated campaign work wait behind the slowest
writer, obscured the distinction between DuckDB OCC conflicts and an external
file owner, and made maintenance/backup behavior depend on callers remembering
the same lock discipline. Realtime ingestion added a second failure mode:
filesystem discovery and parsing could be adjacent to database work, so a
slow or changing file could amplify database contention.

The desired behavior is isolation by workflow and campaign while preserving
the safety of a single DuckDB owner and an exclusive maintenance boundary.

## Contributing factors

1. A process-wide mutex serialized unrelated DML, so historical low-contention
   results could not demonstrate true workload overlap.
2. Legacy connections used manual commit/close patterns with no uniform
   whole-transaction OCC retry.
3. File I/O, CSV/XLSX parsing, SFTP download, and retention cleanup could be
   interleaved with database lifetime in some paths.
4. A database-file owner error was easy to misclassify as transient write
   contention, which risks retry storms and hides a second process.
5. Daily window closure and ingestion leases need a durable stop fence; a queue
   depth metric alone cannot prevent a task from crossing a campaign generation
   or day boundary.

## Remediation

### Database boundary

`TransactionCoordinator` now counts active connection admissions and exposes a
maintenance barrier. In OCC mode ordinary DML does not acquire `_write_lock`.
`write_transaction()` begins, executes, commits, and closes one complete logical
unit; on an identified OCC conflict it retries the callback from a fresh
connection with a bounded exponential delay and jitter.

External owner/I/O/WAL errors are not OCC retries. They are surfaced to the
caller and feed fail-closed backpressure/maintenance behavior.

### Stop fence

Campaign status changes increment `dispatch_generation` and reconcile queued
work in the same transaction. Claims check status, schedule, current day, and
generation before returning a task. Realtime ingestion leases and chunk
completion are asserted inside the same durable task/chunk transaction. Daily
finalization cancels unstarted realtime work and expires queued input records
after the window closes. A worker may finish only the work it already fenced;
new claims are rejected.

### Workflow isolation

- scheduler turns rotate across campaign IDs;
- campaign/route capacity is enforced before claim;
- realtime inbox limits and copy queues are bounded per campaign;
- backpressure distinguishes per-workload pressure from global database
  maintenance/disk failure;
- submission/receipt result writes are batched and idempotent;
- filesystem publication happens atomically before the short database
  registration transaction, and file deletion is outside database admission.

## Verification performed

The current worktree has targeted OCC coordinator tests, realtime ingestion and
scheduling regressions, hot-path regression tests, and temporary-database
serialized/OCC loopback probes. Results and commands are recorded in
[DuckDB-OCC-Throughput-And-Isolation-Benchmark.md](DuckDB-OCC-Throughput-And-Isolation-Benchmark.md)
and `duckdb-occ-global-lock-refactor-results.json`.

## Remaining evidence / no-go conditions

This RCA does not close the incident as production-ready. A current equal-window
Linux ext4 run, G3/G4/G5 workload evidence, SLES offline validation, and real
SMSC stop-fence/duplicate safety are still required. Historical fast-send or
WSL JSON files are reference-only and cannot be used as post-refactor proof.

## Operator conclusion

The safe interim posture is:

```text
MC_DUCKDB_WRITE_CONCURRENCY=serialized
single owner process
maintenance_exclusive for backup/schema/update
```

The OCC path may be used for local validation or an explicitly approved canary,
with immediate rollback to serialized on any admission leak, unexplained file
owner error, duplicate/late task, or stop-fence violation.
