# Marketing Copilot VNext — Bulk Staging Adapter A/B

Task: `MC-VNEXT-STAGING-BULK-INGEST-OPTIMIZATION-20261018`
Decision: `STAGING_CANDIDATE=NO_CHANGE_WITH_EVIDENCE`; A2 remains benchmark-only pending R3/R4 gates.

## Adapter contract

The benchmark uses a separate `BenchmarkProjectionWriter` subclass only for experiments. It reuses the production writer's downstream dedupe, event insert, aggregate `ON CONFLICT`, transaction, and commit path. Each arm owns a fresh DuckDB file and receives the same generated 25-column rows.

| Arm | Implementation | Status |
|---|---|---|
| A0 | Current `LIST + list_zip + UNNEST`, staging-table reuse | PASS; control |
| A1 | pandas DataFrame with nullable/string/integer/float dtypes, connection-local relation, `INSERT ... SELECT` | PASS; tested |
| A2 | PyArrow Table/relation, `INSERT ... SELECT` | PASS; isolated native-ext4 run with PyArrow 12.0.1 and NumPy 1.26.4 |
| A3 | NDJSON write + flush + `fsync` + DuckDB `read_json_auto` + delete, all included in elapsed time | PASS; smoke/11K screened |
| A4 | Same A0 input and SQL, only the reuse cleanup order reversed | PASS; control isolation |

A1/A3/A4 all produced the same digest as A0 in the evaluated runs. The staged table schema remained 25 columns with the original VARCHAR/INTEGER/BIGINT/DOUBLE contract. Duplicate event IDs were not removed outside SQL; the existing staging join and `vnext_progress_events.event_id` primary key remained authoritative.

## R2a quick screen

Workload: 11,000 events, four campaigns, two business days, two events per SMS, 10% requested duplicates, randomized arm order, CPU affinity `0-3`, `nice=10`, DuckDB `1.2.0`. The final valid run used CPython 3.11.13, pandas 2.3.2, NumPy 1.26.4, and PyArrow 12.0.1 on native ext4.

| batch | A0 wall / staging (s) | A1 wall / staging (s) | A2 wall / staging (s) | A3 wall / staging (s) | A4 wall / staging (s) |
|---:|---:|---:|---:|---:|
| 256 | 1.972362 / 0.684315 | 1.935207 / 0.828515 | 2.047142 / 0.597249 | 2.024936 / 0.706491 | 2.068402 / 0.727129 |
| 512 | 1.484356 / 0.631804 | 1.066215 / 0.457120 | 0.917792 / 0.266722 | 1.088738 / 0.441579 | 1.282835 / 0.538362 |
| 1024 | 0.818254 / 0.409380 | 0.676868 / 0.279994 | 0.546901 / 0.155799 | 0.820537 / 0.361404 | 0.815359 / 0.409214 |
| 2048 | 0.648154 / 0.368485 | 0.491681 / 0.198829 | 0.443493 / 0.118245 | 0.584848 / 0.286366 | 0.701571 / 0.395941 |

A2 was the fastest arm from batch 512 onward in this screen, while all five arms preserved the same digest and 25-column schema. A3 file create/write/fsync and relation-read costs remained material and it was not a production candidate.

## R2b paired expansion

The two strongest arms, A1 and A2, were compared with A0 for three rounds at 100,000 events. Every arm had one digest across its three rounds, and all A0/A1/A2 digests matched. Unique event rows were 90,002 and aggregate rows were 8.

| batch | A0 median wall (s) | A1 median wall (s) | A2 median wall (s) | A1 wall change | A2 wall change | A0 median staging (s) | A1 median staging (s) | A2 median staging (s) | A1 staging change | A2 staging change |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 512 | 12.191240 | 11.765172 | 9.587405 | -3.5% | -21.4% | 4.647704 | 4.392319 | 2.382979 | -5.5% | -48.7% |
| 1024 | 8.600536 | 7.174149 | 5.849494 | -16.6% | -32.0% | 3.962810 | 2.648162 | 1.435465 | -33.2% | -63.8% |

The 100K result makes A2 the strongest experimental arm, but the default production comparison is batch 512 and R2 is not a production authorization. A1 had one 21.5-second batch-1024 outlier; medians are reported. The A2 run used a temporary test-only dependency environment and did not change the application dependency contract.

## R2c 500K stress screen

One paired 500,000-event / 30%-duplicate screen was run for A0 and A2 on the same native-ext4 guard. The digest matched, with 333,334 unique event rows and 8 aggregate rows.

| batch | A0 wall / staging (s) | A2 wall / staging (s) | wall change | staging change |
|---:|---:|---:|---:|---:|
| 512 | 72.029992 / 19.729794 | 69.588518 / 13.708721 | -3.4% | -30.5% |
| 1024 | 50.036760 / 17.323214 | 40.902258 / 8.089067 | -18.3% | -53.3% |

The 500K default-batch result does not establish a strong end-to-end production win, despite the meaningful staging reduction. A2 is retained as the best benchmark arm, but it is not promoted into R3/R4 without target-environment dependency approval and candidate-specific end-to-end gates.

## Memory, dependency, and correctness

- The 11K sequential benchmark process high-water samples were approximately 125–142 MiB; the 100K sequential cases were approximately 173–187 MiB. These are benchmark-process measurements, not an 8 GiB/16 GiB capacity claim.
- pandas `2.3.2` is already present in the SLES12SP5-compatible offline wheelhouse and was usable with CPython 3.11/DuckDB 1.2.0.
- PyArrow `12.0.1` was downloaded only into a temporary native-ext4 test venv; its `manylinux_2_17_x86_64.manylinux2014_x86_64` wheel loaded with NumPy `1.26.4`. No production dependency or deployment package was changed.
- A0/A1/A2/A3/A4 had identical event/projection digests, 25-column staging schemas, complete duplicate accounting, and no loss of response rows in the adapter benchmark.

## Decision

`STAGING_AB_BENEFIT=NO_CHANGE`. A2 is the best isolated benchmark arm, but the 500K default-batch result is not a strong enough end-to-end basis for R3/R4 promotion. The current product implementation remains the safe default. No `ProjectionWriterProcess` production configuration or upstream Worker/SMPP/Journal behavior was changed.

Raw JSON results:

- `/tmp/mc-vnext-staging-20261018-pyarrow-r2a-np1/r2a-final.json`
- `/tmp/mc-vnext-staging-20261018-pyarrow-r2b/r2b-final.json`
- `/tmp/mc-vnext-staging-20261018-pyarrow-r2c/r2c-final.json`
