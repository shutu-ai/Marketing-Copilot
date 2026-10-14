# DuckDB Global Writer Lock Callsite Audit

Audit date: 2026-10-14

Baseline revision: `45a258ea56632762d7f732b3c7f3b1b78875800b`

Repository rule: the audit was started with CodeGraph because this repository
contains `.codegraph/`; `rg` was then used to verify the on-disk residuals and
line locations. The baseline was fetched before editing. No commit or push was
performed for this worktree.

## Executive result

The normal OCC DML path no longer takes the historical Python `_write_lock`.
`TransactionCoordinator` tracks admissions, `write_transaction()` owns fresh
connection retry, and `maintenance_exclusive()` is the only global admission
barrier. The default remains `serialized` until the Linux/ext4 and canary gates
are complete.

The important realtime, ingestion, scheduler, batch, task, whitelist, monitor,
metrics, update, and backup call paths were migrated to `read_transaction()` or
`write_transaction()`. The remaining direct application connection in
`sms_task_service.enqueue_rows()` is an intentional temporary-table staging
phase: it performs SQL normalization and CPU/template preparation, closes that
connection before durable writes, and sends the final task/event/chunk unit
through `write_transaction()` from a fresh connection. The only remaining
explicit `commit()` in realtime whitelist code belongs to the separate temporary
whitelist-index DuckDB file, not the application database.

## Callsite inventory

| Area | Baseline pattern | Current boundary | Status / risk |
|---|---|---|---|
| `backend/database.py` | `get_connection`, `_write_lock`, manual commit | coordinator + wrapped admission; explicit helper APIs | Implemented; compatibility gate retained for serialized mode |
| `realtime_campaign_service.py` | direct reads, daily finalization, file cleanup, manual commits | short reads, complete daily write transaction, file I/O outside DB, retention snapshot/finalize split | Migrated |
| `ingestion_job_service.py` | direct job/file/chunk transactions | complete write callbacks with bounded retry and operation labels | Migrated |
| `realtime_ingestion_state.py` | local retry/connection ownership | delegates to coordinator; lease assertions remain inside write callback | Migrated |
| `realtime_ingestion_worker.py` | direct reads and processed-file updates | read/write helpers; no database held during file parsing | Migrated |
| `realtime_whitelist_service.py` | direct version/binding reads and cleanup commit | read snapshots, write binding/cleanup transactions; index build DB remains separate | Migrated |
| `activity_scheduler.py` | direct batch selection reads | short read transactions | Migrated |
| `batch_campaign_service.py` | direct probes, batch processing commit, daily reset | read snapshots; status/stats/chunk unit uses complete write transaction | Migrated |
| `sms_task_service.py` | direct external task, recovery, retention, stats; long enqueue connection | write helpers for durable paths; staging connection closed before final durable transaction | Migrated with documented staging exception |
| `sms_write_buffer.py` | result batch transaction | idempotent event/result batch in `write_transaction()` | Preserved and verified |
| `monitor_service.py` | direct monitor reads and redistribution commit | filesystem distribution outside DB; read snapshots and short status writes | Migrated |
| `metrics_service.py` | direct alert open/resolve commits | alert open/resolve write callbacks; backlog read snapshot | Migrated |
| `update_service.py` | direct drain/history commit | read/write helpers; backup uses maintenance barrier | Migrated |
| `runtime_service.py` | backup under writer-gate assumption | `maintenance_exclusive` → raw `CHECKPOINT` → close → copy | Migrated |
| `backend/routers/internal.py` | owner-hold acceptance endpoint | intentionally holds a connection for the lock/maintenance acceptance test | Test-only, not a production workflow |
| `send_store_sharding_poc.py` | separate shard connections/manual commits | POC implementation, outside the selected application writer path | Deferred and explicitly out of scope |

## Direct-connection exceptions

The following are not silent production gaps:

1. `sms_task_service.enqueue_rows()` uses one connection for a temporary
   `ingest_task_stage` table and read-heavy normalization. That connection is
   closed before persistent task insertion. This avoids holding a persistent
   writer admission while parsing/template expansion runs. The durable phase is
   retried as one complete unit.
2. `realtime_whitelist_service.build_version()` commits to a newly-created
   per-campaign whitelist index DuckDB file. It is not the configured
   application database and is published atomically only after the build closes.
3. Legacy API callers that still call `get_connection()` are protected by
   `WrappedConnection` admission tracking. They are not allowed to bypass the
   process coordinator, but a future migration should replace long manual
   multi-statement transactions with `write_transaction()`.

## Static verification commands

From the repository root:

```powershell
codegraph explore "DuckDB OCC global write lock removal workload isolation task requirements relevant backend symbols and files"
rg -n "get_connection|\.commit\(|\.rollback\(" backend/services
rg -n "maintenance_exclusive|write_transaction|read_transaction|TransactionCoordinator" backend/database.py backend/services
```

The audit is not a claim that every historical or POC callsite disappeared.
It records the boundary and retry risk for each callsite class so a future
review cannot treat historical concurrency evidence as post-refactor evidence.
