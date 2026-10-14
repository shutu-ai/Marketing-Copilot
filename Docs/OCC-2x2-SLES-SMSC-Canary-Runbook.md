# OCC 2x2 SLES12SP5 / Real-SMSC Canary Runbook

## Gate status

This runbook is prepared but not executed. Current status is
`SLES12SP5=NOT_RUN`, `REAL_SMSC=NOT_RUN`, and `production_decision=NO_GO`.

## Preconditions and approval

Obtain written approval for a non-production canary, a named rollback owner, a
maintenance window, SMSC credentials/route limits, and a test MSISDN allowlist.
Confirm that the target package contains only source, frontend `dist`, offline
wheels, release metadata, systemd configuration, and checksums. Do not include
runtime databases, logs, uploads, backups, packet captures, or secrets.

Record before starting:

- package checksum and exact source SHA;
- SLES12SP5 host/kernel/glibc and standalone Python runtime version;
- `MC_UPDATE_MODE` (default `systemd`), service/unit name, bind address, and
  database path;
- SMSC host/port, bind account, route TPS/window, DLR mode, and test numbers;
- backup path, restore test result, and rollback command.

## SLES12SP5 preflight

1. Install the offline runtime/package on an isolated non-production host.
2. Verify `backend/release.json`, `frontend/dist/index.html`,
   `offline/wheels/`, `run.sh`, and `checksums.sha256`.
3. Verify the package contains no database, runtime log, upload, backup, cache,
   or secret file.
4. Start with the standard systemd mode and run health/readiness checks.
5. Confirm the service can stop, start, restart, checkpoint, and close a backup
   without leaving a writer or stale lease.

## Real-SMSC canary sequence

Run only after the local loopback G3/G4/G5 gates are PASS.

1. Configure the approved test route with the exact window/TPS limits; record the
   provider response codes and DLR contract.
2. Send a small allowlisted batch using serialized/reliable mode. Reconcile
   input, task, submit, accepted, DLR, and final ledger counts.
3. Repeat the bounded canary in OCC/reliable, serialized/fast, and OCC/fast only
   if each arm has current local PASS evidence.
4. Exercise a controlled reject, response drop, disconnect, delayed DLR, and
   restart. Unknown/partial submissions must never be auto-retried without an
   explicit reconciliation decision.
5. Take a maintenance backup, verify it, and perform a restore/checkpoint close
   test before declaring the canary complete.

## Stop and rollback triggers

Stop immediately on duplicate send, lost task-to-SMSC mapping, an unknown
submission being automatically retried, a submit after a stop fence, route/TPS
violation, unbounded backlog, database lock/WAL failure, or missing recovery
checkpoint. Disable the campaign/route, preserve the external evidence bundle,
stop the service using the approved unit command, and restore the last verified
backup or package. Do not delete evidence before incident review.

## Evidence to attach to the gate review

Attach only reviewed summaries to Git. Keep raw logs and database/backup files in
the controlled evidence store. The review bundle must contain the source SHA,
package/checksum manifest, exact commands, per-arm counts and percentiles,
resource/OCC metrics, fault timeline, rollback result, SMSC response/DLR
correlation, and operator approval. A missing live or SLES gate remains
`NOT_RUN`, never `PASS` by inference.
