# DuckDB OCC Concurrency Architecture Decision

Status: implemented in the current worktree; canary decision remains `MORE_EVIDENCE_NEEDED`.

Baseline: `45a258ea56632762d7f732b3c7f3b1b78875800b` (`origin/main`, fetched before the refactor)

## Decision

Keep `serialized` as the reliable process default and add an explicit
`MC_DUCKDB_WRITE_CONCURRENCY=occ` opt-in. Both modes use one in-process owner,
one resolved DuckDB file, and the same service-level transaction APIs.

The OCC mode removes the historical Python writer mutex from ordinary DML. A
condition-variable coordinator still tracks every connection-level admission so
maintenance can form a real quiescent boundary. The compatibility mutex remains
available only for `serialized` normal writes and for rollback diagnostics.

This is deliberately a mode switch, not a runtime toggle. The selected mode is
read at import time and freezes after the first coordinator use; changing it
requires a process restart. `configure_write_concurrency()` rejects changes
after that point.

## Invariants

1. Application writes are made through `write_transaction(callback, ...)` where
   the callback contains the complete logical write unit and never calls
   `commit()` or `close()`.
2. OCC retries are bounded to at most five attempts, use a fresh connection for
   every attempt, apply exponential backoff plus bounded jitter, and replay the
   whole callback.
3. Only DuckDB write-write transaction conflicts are OCC-retryable. File-owner,
   I/O, WAL replay, and external lock errors are not disguised as OCC conflicts;
   they fail closed and are surfaced to the caller.
4. Backup, checkpoint, schema, and other maintenance actions use
   `maintenance_exclusive()`: stop new admissions, drain active admissions, run
   the action, and release the barrier. The backup path executes `CHECKPOINT`
   while the barrier is held and closes the raw checkpoint connection before
   copying the file.
5. File copying, CSV/XLSX parsing, SFTP activity, and retention unlink operations
   are outside application write transactions. Database publication is a short
   transaction after the filesystem operation succeeds.
6. The service is a single owner. The instance lock remains mandatory. A second
   process or a manually opened DuckDB file is an external-owner condition, not
   a workload-isolation mechanism.
7. Result writes remain idempotent: task dedupe keys, `ON CONFLICT` result/event
   inserts, claim tokens, attempt tokens, and the ingestion lease fence are
   replay-safe.

## Coordinator model

| Operation | `serialized` | `occ` |
|---|---|---|
| Ordinary read | tracked admission | tracked admission |
| Ordinary DML | tracked admission + compatibility Python writer gate | tracked admission only; DuckDB OCC resolves conflicts |
| `write_transaction` retry | bounded transient retry | bounded OCC-conflict retry from a fresh connection |
| Backup/checkpoint/schema | maintenance barrier plus writer gate | maintenance barrier |
| External database owner | fail closed | fail closed |

The coordinator exposes non-sensitive diagnostics through `write_lock_status()`
and metrics for admission wait, transaction wait/hold, maintenance wait, commit
time, OCC conflict/retry/failure, and bounded backoff. In OCC, the legacy
`held` gauge means an active maintenance barrier; it does not claim that normal
DML owns a global mutex.

## Workload isolation

The refactor keeps the existing business isolation controls and makes their
database boundaries explicit:

- campaign and route capacity are enforced before task claim;
- round-robin campaign selection prevents one campaign from consuming every
  scheduler turn;
- realtime ingestion has per-campaign inbox limits, bounded discovery/copy
  queues, and recovery admission/backpressure;
- task claims keep the short claim lock and campaign-generation fence;
- the result writer batches idempotent submission/receipt outcomes;
- global maintenance and disk/journal failure are fail-closed rather than
  converted into an unbounded per-campaign retry storm.

## Why the default is not switched yet

Local Windows tests and the temporary-database loopback harness pass for both
modes. They do not prove Linux ext4 behavior, SLES 12 SP5 packaging/runtime
compatibility, multi-process ownership behavior, or real SMSC delivery safety.
Those are explicit canary gates and are recorded as missing evidence in the
results JSON. Until they are current and equal-window, the production default
must remain `serialized`.

## Rollback

Set `MC_DUCKDB_WRITE_CONCURRENCY=serialized`, stop the owner cleanly, verify the
instance lock is released, and start exactly one owner. Do not switch mode in a
running process and do not copy a live database without the maintenance
barrier. See the [rollback runbook](DuckDB-OCC-Rollback-Runbook.md).
