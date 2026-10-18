# Marketing Copilot VNext — Bulk Staging Adapter A/B

Task: `MC-VNEXT-STAGING-BULK-INGEST-OPTIMIZATION-20261018`
Decision: `STAGING_CANDIDATE=NO_CHANGE_WITH_EVIDENCE`.

## Adapter contract

The benchmark uses a separate `BenchmarkProjectionWriter` subclass only for experiments. It reuses the production writer's downstream dedupe, event insert, aggregate `ON CONFLICT`, transaction, and commit path. Each arm owns a fresh DuckDB file and receives the same generated 25-column rows.

| Arm | Implementation | Status |
|---|---|---|
| A0 | Current `LIST + list_zip + UNNEST`, staging-table reuse | PASS; control |
| A1 | pandas DataFrame with nullable/string/integer/float dtypes, connection-local relation, `INSERT ... SELECT` | PASS; tested |
| A2 | PyArrow Table/relation, `INSERT ... SELECT` | SKIPPED; PyArrow absent from the offline environment and wheelhouses |
| A3 | NDJSON write + flush + `fsync` + DuckDB `read_json_auto` + delete, all included in elapsed time | PASS; smoke/11K screened |
| A4 | Same A0 input and SQL, only the reuse cleanup order reversed | PASS; control isolation |

A1/A3/A4 all produced the same digest as A0 in the evaluated runs. The staged table schema remained 25 columns with the original VARCHAR/INTEGER/BIGINT/DOUBLE contract. Duplicate event IDs were not removed outside SQL; the existing staging join and `vnext_progress_events.event_id` primary key remained authoritative.

## R2a quick screen

Workload: 11,000 events, four campaigns, two business days, two events per SMS, 10% requested duplicates, randomized arm order, CPU affinity `0-3`, `nice=10`, DuckDB `1.2.0`.

| batch | A0 wall / staging (s) | A1 wall / staging (s) | A4 wall / staging (s) | A2 |
|---:|---:|---:|---:|---|
| 256 | 1.566303 / 0.554312 | 1.927340 / 0.819115 | 1.730472 / 0.615770 | SKIPPED |
| 512 | 1.232341 / 0.518325 | 1.133166 / 0.476498 | 1.201240 / 0.517114 | SKIPPED |
| 1024 | 0.794978 / 0.388043 | 0.633012 / 0.260987 | 0.824525 / 0.419141 | SKIPPED |
| 2048 | 0.693803 / 0.396670 | 0.470495 / 0.188889 | 0.676180 / 0.393664 | SKIPPED |

The apparent A1 improvement grows with batch size, but it is not a stable default-512 win and does not approach the requested strong staging improvement threshold. A3 screening at 11K was correct, with wall times 1.582698/1.134843/0.866014 s for batches 256/512/1024; its file create/write/fsync and relation-read costs were material and it was not a production candidate.

## R2b paired expansion

The only plausible candidate, A1, was compared with A0 for three rounds at 100,000 events. Every arm had one digest across its three rounds, and A0/A1 digests matched. Unique event rows were 90,002 and aggregate rows were 8.

| batch | A0 median wall (s) | A1 median wall (s) | wall change | A0 median staging (s) | A1 median staging (s) | staging change |
|---:|---:|---:|---:|---:|---:|---:|
| 512 | 11.686108 | 11.491560 | -1.7% | 4.421086 | 4.238013 | -4.1% |
| 1024 | 8.125283 | 7.000101 | -13.9% | 3.753418 | 2.556360 | -31.9% |

The batch-1024 result is directionally useful, but the production comparison is batch 512 and the gain is within the range where host scheduling and DuckDB variability matter. No candidate reached the required evidence level for a production mode switch. No 500K/30%-duplicate expansion was started because the candidate had already failed the selection gate.

## Memory, dependency, and correctness

- The 11K sequential benchmark process high-water samples were approximately 125–142 MiB; the 100K sequential cases were approximately 173–187 MiB. These are benchmark-process measurements, not an 8 GiB/16 GiB capacity claim.
- pandas `2.3.2` is already present in the SLES12SP5-compatible offline wheelhouse and was usable with CPython 3.11/DuckDB 1.2.0.
- PyArrow was not installed and no matching wheel was found in the three inspected offline archives. It was explicitly recorded as `SKIPPED`, not installed online.
- A0/A1/A3/A4 had identical event/projection digests, 25-column staging schemas, complete duplicate accounting, and no loss of response rows in the adapter benchmark.

## Decision

`STAGING_AB_BENEFIT=NO_CHANGE`. The current product implementation remains the safe default. The only source changes are internal timing counters, benchmark adapters, and tests; no `ProjectionWriterProcess` production configuration or upstream Worker/SMPP/Journal behavior was changed.

Raw JSON results:

- `/tmp/mc-vnext-staging-20261018.MSdK4g/r2a-final.json`
- `/tmp/mc-vnext-staging-20261018.MSdK4g/r2a-a3-final.json`
- `/tmp/mc-vnext-staging-20261018.MSdK4g/r2b-final.json`
