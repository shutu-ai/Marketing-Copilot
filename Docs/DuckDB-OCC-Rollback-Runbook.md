# DuckDB OCC Rollback Runbook

Use this procedure when an OCC canary shows a stop-fence, duplicate, fairness,
admission, or storage-owner problem.

1. Stop new intake and campaign control actions through the application.
2. Stop the scheduler and task workers using the normal service controls; do
   not kill the process while it owns an uncheckpointed write unless the
   incident commander accepts that risk.
3. Confirm the owner is stopped and the instance lock is released. Ensure no
   second process has opened the database.
4. Set `MC_DUCKDB_WRITE_CONCURRENCY=serialized` in the service environment.
5. Start exactly one owner process. The mode is immutable after first use, so a
   restart is mandatory.
6. Run the serialized loopback probe and the database/backup regression tests.
7. Inspect queued, retrying, sending, unknown, and delivery-result states. Do
   not manually requeue work with submission evidence; use the existing result
   recovery rules.
8. Preserve sanitized diagnostics and record the exact revision and reason for
   rollback. Do not delete the database, WAL, backups, uploads, or logs as part
   of this runbook.

Rollback success means the single owner is healthy, active transaction count
returns to zero after a controlled stop, maintenance can complete, and no
late/duplicate task or lost result is observed. OCC remains disabled until the
failed gate is reproduced, fixed, and revalidated on the target filesystem.
