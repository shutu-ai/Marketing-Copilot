# OCC Cross-Campaign Isolation Report

## Status

`NOT_RUN` for the current source revision
`c9890a76040f92dabd50c4b50754d44716d6daca`.

No cross-campaign result is promoted from the historical `Docs/` JSON files or
from a run that used a different source revision. There is therefore no claim
that campaign B/C progress is isolated from campaign A backlog under OCC/fast.

## Required G4 experiment

The next approved run must start four fresh processes/DBs as applicable and use
the same source/data/SMSC loopback setup as G3:

| Workload | Campaign A | Campaign B | Campaign C |
|---|---:|---:|---:|
| Pending backlog | 50,000 | 1,000 | 1,000 |
| Ingestion | active | active | active |
| Sender | observe only | loopback submit | loopback submit |
| Controls | pause/cancel/resume/backup | pause/cancel/resume | pause/cancel/resume |

The run must exercise ingestion, claim, SMPP submit, checkpoint, API control,
backup, and recovery paths while A remains backlogged. It must be repeated three
times for each covered concurrency/send arm or explicitly recorded as partial.

## Measurements

Capture per campaign and globally:

- task created, submitted, accepted, rejected, unknown, retrying, and backlog;
- task-to-submit p50/p95/p99 and file-to-task latency;
- OCC conflicts, transaction retries, admission waits, queue depth, CPU/RSS;
- stop-fence violations and submits after pause/cancel;
- duplicate/lost task-to-SMSC mappings and API control latency;
- separate DB, SMPP, journal, and checkpoint bottleneck attribution.

## Pass criteria

G4 is PASS only when B and C continue to make bounded progress while A is
backlogged, no campaign consumes another campaign's task or file namespace, a
stop fence prevents late submissions within the agreed budget, and all final
counts reconcile without duplicate or lost mappings. A single successful smoke
run is insufficient.

## Evidence handling

Raw run directories, DuckDB databases, protocol JSONL, logs, and backup files
remain in the external test root. The repository should receive only the signed
or reviewed summary and the reproducible command/configuration, never the raw
runtime collection.
