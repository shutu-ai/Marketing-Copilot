# Marketing Copilot VNext — Staging Internal Cost RCA

Task: `MC-VNEXT-STAGING-BULK-INGEST-OPTIMIZATION-20261018`
Date: 2026-10-18
Decision: `STAGING_INTERNAL_RCA=PASS`; no production ingest candidate promoted.

## Freeze and environment

- `HEAD`: `c8505017e8526a21a41aabb6e9bcdaaece2f6216`
- `origin/main`: `75aa2ddc58ede3b140d7538f02edf1b8f417832e`
- The working tree was already dirty before this task. Existing user changes were preserved; no commit or push was performed.
- Relevant final file hashes:
  - `backend/vnext/projection_writer.py`: `e204693343a99a36cdc04097a839b418cea505dd`
  - `backend/tools/vnext_single_writer_benchmark.py`: `361b42ee89d24ff97255ab217d40df5faada604b`
  - `backend/tests/test_vnext_projection_writer.py`: `9db67ab6cf4e59b868d1ee02a2c3dd33660ca60a`
  - `backend/tests/test_vnext_staging_benchmark.py`: `d9d70fa96c87f0d5ecd91c25b423a1fdba7aa3f0`
- Formal local runs used `/tmp/mc-vnext-staging-20261018.MSdK4g` on WSL `AlmaLinux-8`, native `/dev/sdd` ext4. `df -T .` recorded `ext4` with approximately 879 GiB available.
- Python `3.11.13`; DuckDB `1.2.0`; pandas `2.3.2`; NumPy `2.2.6`; PyArrow was not installed and was not present in the inspected offline wheelhouses.
- Benchmark guard: CPU affinity `0-3`, `nice=10`, requested `ionice -c 3 -n 7` (the system reported the idle-class data as ignored). These are local reproducibility controls, not EulerOS production sizing.

## Instrumentation added

`ProjectionWriter._apply_batch()` now keeps the existing transaction and SQL order, but records aggregate counters for:

1. `staging_prepare_rows_ns` — `_event_row()` conversion;
2. `staging_clear_table_ns` — staging/new-table cleanup and first table creation;
3. `staging_transpose_ns` — the A0 `zip(*rows)` column construction;
4. `staging_bind_execute_ns` — A0 parameter construction and DuckDB `UNNEST` execution;
5. `staging_relation_create_ns` / `staging_relation_insert_ns` — candidate relation/file creation and insertion;
6. `staging_total_ns` and the compatibility `staging_load_ns`.

Batch durations and event-normalized durations are retained in bounded deques and published as P50/P95 milliseconds/microseconds. The new counters are process-local and are not written per event. `staging_total_ns == staging_load_ns` was asserted in regression tests and in every evaluated benchmark case.

## A0 internal sweep

Workload: 11,000 generated events, 10% duplicate request, four campaigns, two business days, two events per SMS, 25-column rows. The A0 digest was `4f4d316bf81280560d3878ce5d4d01460afd0a49ee31a99cae4d1edb5464f6f6`; each case had 25 staging columns and 9,902 unique event rows.

| batch | flush wall (s) | batch end-to-end (s) | staging total (s) | staging / batch E2E | batches | prepared rows |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1.566303 | 1.498211 | 0.554312 | 37.0% | 43 | 9,918 |
| 512 | 1.232341 | 1.156841 | 0.518325 | 44.8% | 22 | 9,910 |
| 1024 | 0.794978 | 0.729917 | 0.388043 | 53.2% | 11 | 9,906 |
| 2048 | 0.693803 | 0.627417 | 0.396670 | 63.2% | 6 | 9,904 |

The local ext4 run does not reproduce the previous EulerOS 98.1% proportion; that difference is an environment/workload fact, not a reason to discard the server evidence. The internal split is still clear: A0 `staging_bind_execute_ns` accounted for 83.3–90.1% of staging time, cleanup for 2.4–11.3%, row preparation for 3.8–6.2%, and transpose for approximately 0.9–1.3%.

At 100,000 events, A0 batch 512 had 196 batches, 4.353638 s staging, and 10.853411 s batch end-to-end; batch 1024 had 98 batches, 3.753418 s staging, and 7.480219 s batch end-to-end. This confirms that the cost is not only Python `zip(*rows)`; the parameter-bound DuckDB input path remains the dominant measured staging component in this environment.

## Profiler disturbance

The final 100K A0 comparison used the same workload and CPU guard:

- profiler OFF: 12.011486 s;
- profiler ON: 12.096638 s;
- measured wall difference: `+0.709%`;
- event digest: identical;
- profile snapshot contained `staging_prepare_rows`, `staging_clear_table`, `staging_transpose`, `staging_bind_execute`, and `staging_total` durations.

This is consistent with the earlier low-disturbance server evidence and is acceptable for diagnostic runs. Profiling remains disabled unless a profile root is explicitly supplied.

## RCA conclusion

The current `LIST + list_zip + UNNEST` input path is the measurable A0 staging hotspot. Cleanup and Python row preparation are secondary. A replacement must therefore reduce the actual relation/parameter-bound input cost while preserving the same 25 values, event-id idempotency, set-based DML, transaction, and offset boundaries. The local evidence did not justify changing the production path or adding a new runtime dependency.

Raw evidence was kept outside the repository under `/tmp/mc-vnext-staging-20261018.MSdK4g/`; only summarized results and paths are delivered.
