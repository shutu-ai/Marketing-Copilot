# DuckDB OCC Operations Runbook

## Preconditions

- one owner process only; instance lock is healthy;
- database path is on local supported storage, not a shared/network mount;
- disk and WAL/journal headroom are healthy;
- the current release has current G3/G4/G5, SLES, and SMSC evidence;
- no backup or schema operation is running.

## Observe

Check the metrics endpoint and database diagnostics for:

- `marketing_copilot_database_transaction_active`;
- `marketing_copilot_database_transaction_oldest_age_seconds`;
- `marketing_copilot_database_maintenance_requested` and
  `...maintenance_active`;
- `marketing_copilot_database_occ_conflicts_total`;
- `marketing_copilot_database_occ_failures_total`;
- `marketing_copilot_database_transaction_admission_timeouts_total`;
- per-campaign inbox/backpressure and per-route claim capacity.

An active maintenance barrier is expected to block new admissions. An active
transaction age that grows without drain, repeated OCC failures, external file
owner errors, or cross-campaign backlog growth is a stop signal.

## Enable for an approved canary

Set the mode before starting the owner and restart the service:

```text
MC_DUCKDB_WRITE_CONCURRENCY=occ
```

Do not change the variable in a running process. Run the temporary loopback
probe first:

```powershell
python backend/tools/duckdb_occ_loopback_acceptance.py --mode occ
```

Then run the equal-window workload and SMSC simulator gates from the current
release plan. Record the exact revision, environment, filesystem, duration,
campaign seeds, route limits, and all failure injections.

## Backup/schema/update

Use the application backup/update entry point. It must acquire
`maintenance_exclusive`, drain admissions, issue `CHECKPOINT`, close the raw
checkpoint connection, and copy the database. Never copy the live DuckDB file
from an ad-hoc shell command. Never perform schema work while task workers are
active outside the barrier.

## Stop criteria

Stop the canary and roll back if any of these occurs:

- OCC failure count does not converge under the configured workload;
- an external owner/I/O error is retried as if it were an OCC conflict;
- active admissions remain non-zero after workers stop;
- a maintenance barrier cannot drain within its deadline;
- a task is duplicated, sent after a campaign/day stop fence, or loses its
  durable result evidence;
- one campaign or route starves another beyond the accepted fairness bound;
- disk/WAL pressure is not fail-closed.

## Evidence handoff

Write only sanitized JSON/Markdown evidence to `Docs/`. Do not include runtime
databases, uploads, logs, backups, secrets, phone numbers, or SMS content.
