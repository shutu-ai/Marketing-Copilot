# DuckDB claim/retry/SMPP capacity RCA and acceptance

Date: 2026-10-13
Environment: isolated WSL2 AlmaLinux-8 ext4 copy at `/tmp/mc-mixed-20261013`
Python: `/opt/marketing-copilot/venv/bin/python`
Live environment: not touched and not verified.

## FINAL_STATUS

`CODE_ACCEPTANCE: PASS` · `DUCKDB_LOCK_REGRESSION: PASS` · `RETRY_UNKNOWN_SAFETY: PASS` · `MIXED_CAMPAIGN_FAIRNESS: PASS` · `CAPACITY_ACCEPTANCE: CAPACITY_LIMITED` · `SFTP: BLOCKED_ENV` · `LIVE_ENV: UNVERIFIED`.

`F3: NOT_RUN` and `F4: NOT_RUN` are intentional. The current task defines F3 as a weighted multipart/fair-disabled comparison and F4 as a lease/quarantine fault matrix; the drop-response and partial-submit runs below are R1 fault coverage and are not substitutes for those optional matrices.

## BASELINE_AND_WORKTREE

- `BASELINE_SHA/HEAD`: `0c6af330edcee04ef6ae3d17047791a4b221189b`.
- `origin/main`: same SHA.
- The worktree was already dirty before this RCA. Existing user artifacts, task files, `frontend/package-lock.json`, `cache/`, `.local-e2e-runs/` and `.rollback-bb7-parent/` were preserved; no reset, checkout, deletion, commit or push was performed.
- Current WSL fingerprint: AlmaLinux 8.10, WSL2 kernel 6.18.33.2, Python 3.11.13, DuckDB 1.2.0, 22 CPUs, 15.7 GiB memory, ext4 `/tmp` with about 941 GiB free.
- Isolated run root: `/tmp/mc-mixed-20261013`; all loopback SMSC ports and DuckDB paths were per-run. The existing `/opt/marketing-copilot` service on port 8000 was not restarted or modified.
- The supplied live parameter values are present in `deploy/marketing-copilot.env.example` as project/systemd defaults (including workers, claim batch, write buffering, SMPP window/TPS multiplier, auth and update paths); the password is not copied into evidence. The SLES12 SP5 manual archive intentionally uses its manual-deployment template and preserves an existing target `marketing-copilot.env`.

## Conclusion

`ROOT_CAUSE_STATUS: CONFIRMED`.

The confirmed capacity defect was fail-fast insertion into the bounded SMPP async-send queue. Under task/route pressure, `put_nowait()` converted transient local queue pressure into `SmppWindowExhaustedError`, status `0x45`, and ordinary retry churn. The minimal fix makes the producer wait for bounded queue capacity and records the wait; it still fails explicitly after the bounded wait expires. This preserves the existing unknown/partial-submit fencing rules.

The previous runtime metrics were also point observations, so they could not support p50/p95/p99 bottleneck analysis. The changed metric paths now use bounded histograms for DB lock/transaction/statement, task claim/send, write buffer, SMPP queue/window/lock/throttle/send/response and ingestion timings.

The same-profile pre-fix run reproduced the historical event-field symptom: 4 invalid event rows and two concatenated event-type fields, alongside 866 queue-full retry failures. The post-fix run under the same profile had 0 invalid rows and 0 concatenated event fields. The queue/retry flood is confirmed as the trigger condition; the internal serialization mechanism is not independently isolated as a second root cause.

## ROOT_CAUSE_MATRIX

| Hypothesis | Evidence | Decision | Change |
|---|---|---|---|
| SMPP async queue overflow caused retry churn | Pre-fix 1.48K artifact had 600 `retrying` tasks and queue-full/0x45 failures; same-shaped post-fix controls expose non-zero queue wait and no queue-full safety violation | Confirmed | `smpp_gateway.send_async()` now waits bounded time for queue capacity |
| DB lock contention was the sole throughput bottleneck | Post-fix 3K/10K lock-wait p95 is 0.5s, while queue-wait p95 is 1.0s and p99 reaches 2.0s; isolated claim SQL scan/window is fast at the sampled scale | Not sole root cause | Instrumented; no unproven claim SQL rewrite |
| Retry flood corrupted durable event fields | Same-profile pre-fix: 866 queue-full failures, 4 invalid rows and concatenated event types; post-fix: 0 invalid rows | Trigger condition confirmed; separate serialization mechanism inconclusive | Queue backpressure removes the reproduced flood; durable event fencing retained |
| Missing latency quantiles hid the hot path | Before artifacts had count/sum only; fresh artifacts provide bounded histogram p50/p95/p99 | Confirmed observability gap | Added bounded histograms across DB/task/SMPP/write-buffer paths |

## Test contract

SMPP capacity and smoke tests do **not** require delivery receipts. They validate `submit_sm_resp`, durable task state, retry safety, duplicate fencing and route progress. All mixed/capacity and non-DLR fault runs used `delivery_receipt_mode=disabled` and observed zero DLR events; the dedicated R1 DLR-delay case intentionally enabled delivery receipts and observed five DLRs. The standalone `smsc_smoke_test.py` now has the same default; `--wait-for-receipt` is opt-in.

## Changes

- `backend/services/smpp_gateway.py`: bounded backpressure for the async-send queue; queue-wait histogram.
- `backend/services/metrics_service.py`, `backend/database.py`, `backend/services/task_worker.py`, `backend/services/sms_write_buffer.py`, `backend/services/realtime_ingestion_worker.py`, `backend/services/batch_campaign_service.py`: bounded latency histograms.
- `backend/tools/mixed_campaign_capacity_acceptance.py`: pre-stop/stop/post-stop lifecycle evidence, machine-readable retry/failure/event-integrity diagnostics, route progress and histogram bottleneck evidence. Fairness uses an explicit 1-second dispatch completion grace.
- `backend/tools/mixed_campaign_scenarios.json`: 1K and 3K RCA profiles.
- `backend/tools/smsc_smoke_test.py`: submit response is sufficient by default; receipt waiting is opt-in.
- `backend/tests/test_capacity_bottleneck_regression.py`: queue backpressure and unknown/partial retry safety regression tests.

## G1 controls

All controls used queue capacity 32 and SMPP window 10. Each result contains `pre_stop`, `stop_transition`, `post_stop`, startup/business-wait/shutdown/reconcile timings, task status snapshots, backlog/resource samples and route progress.

| Control | Parameters | Tasks | Successful submit_sm_resp | Post-stop status | Result |
|---|---|---:|---:|---|---|
| High TPS | 1000 TPS, response 0s, workers 2, claim 8 | 1040 | 241 | accepted 241, queued 741, retrying 6, sending 50, unknown 2 | capacity limited; safety PASS |
| Low TPS | 5 TPS, response 0.03s, workers 2, claim 8 | 1040 | 251 | accepted 251, queued 712, retrying 5, sending 53, unknown 19 | capacity limited; safety PASS |
| Fixed SMPP / workers 2 | 25 TPS, response 0.03s, workers 2, claim 8 | 1040 | 398 | accepted 398, queued 570, retrying 13, sending 54, unknown 5 | capacity limited; safety PASS |
| Fixed SMPP / workers 4 | 25 TPS, response 0.03s, workers 4, claim 12 | 1040 | 343 | accepted 343, queued 623, retrying 64, sending 8, unknown 2 | capacity limited; safety PASS |

Same-profile pre-fix comparison (only `smpp_gateway.py` restored to HEAD, all other diagnostic code and parameters held constant): 1040 generated, 174 successful responses, `accepted=174, retrying=866`, 866 `smpp_send_queue_full` failures, 4 invalid/concatenated event rows. The post-fix row above produced 343 successful responses, 0 invalid event rows and bounded queue wait instead of fail-fast queue-full errors.

No control had missing MSISDNs, unexpected MSISDNs, duplicate sends, partial-fence violations or unknown/partial retry violations. The workers-4 run demonstrated the new backpressure directly: `smpp_send_queue_wait_seconds` count 372, mean 0.604s, p95 boundary 2s; `task_send_async_call_seconds` had the same shape.

The fourth single-variable control, run after the final instrumentation change, was fixed SMPP at 25 TPS/0.03s with workers 2 and claim batch 8: 1040 generated, 398 successful submit responses, post-stop `accepted=398, unknown=5, queued=570, retrying=13, sending=54`, no duplicates/missing and retry safety PASS. The workers-4 comparison at the same SMPP settings achieved 343 successful responses in its bounded run; both are capacity-limited, so this is a diagnostic comparison rather than an SLA claim.

## RETRY_STATE_ANALYSIS

Fresh mixed runs group current status by `failure_class`, `error_reason`, route/config, campaign, attempts bucket and retry schedule, and separately count durable `submit_authorized`/`send_failure` events, `sms_send_attempts` states and `sms_send_parts` states. Across the high/low/worker controls and 3K/10K runs: duplicate sends `0`, missing MSISDNs `0`, invalid event rows `0`, unknown/partial retry violations `0`, partial-fence violations `0`, safety `PASS`. Unknown tasks from drop-response and disconnect stayed terminal unknown; partial-submit stayed terminal partial.

## DB_LOCK_AND_CLAIM_PROFILE

The current histogram evidence is bounded and operation-level. Representative 10K post-fix values: DB transaction p50/p95/p99 `0.5/0.5/0.5s`, lock wait `0.1/0.5/0.5s`, statement `0.1/0.1/0.1s`, task claim `0.5/0.5/1.0s`. The 3K run had lock-wait p95/p99 `0.5/0.5s`.

An isolated read-only copy of the stopped 3K database was profiled with DuckDB `EXPLAIN` and `EXPLAIN ANALYZE` (`Docs/wsl-claim-sql-profile.json` in the evidence set). The claim plan is `sms_tasks` sequential scan → policy hash join → partitioned `ROW_NUMBER` window → fair-row filter → `TOP_N`; it scanned 7184 rows, produced 12 candidates, and the sampled window operator was about 25ms. This is evidence for a bounded SQL hotspot, not proof that claim SQL is the primary capacity limit. The pre-fix artifacts had no histogram quantiles, so a direct p95 before/after claim comparison is unavailable and is explicitly not invented.

Before/after operational comparison: the pre-fix 10K artifact reported transaction sum/count `643.898/1631s`, lock-wait `501.912/1846s`, task-claim `154.781/372s`, and 707 successful submits; the post-fix 10K run reported transaction `503.614/2918s`, lock-wait `336.611/3106s`, task-send/queue-wait `644.718/2015s`, and 1959 successful submits. These are cumulative concurrent sums, not wall-clock time; the evidence points to the SMPP queue/retry path as the confirmed changed bottleneck, while DB contention remains a monitored contributor.

## SMPP_ROUTE_CAPACITY

The mixed reports record configured TPS, response delay, window, queue capacity, route count, submit/response totals, status codes, route progress, queue wait, window wait, send-lock wait, throttle wait, socket send and response latency. The 3K two-route run used 25 TPS per route and produced 971/975 successful responses; the 10K run produced 1034/925. All capacity runs used `delivery_receipt_mode=disabled`; the dedicated DLR-delay R1 test alone used `required`.

## PER_CAMPAIGN_PROGRESS

Each mixed JSON retains per-campaign first publish/file/task/submit times, first service wait, claim/task timing, longest no-progress interval, fairness state and backlog curve. Normal and low-TPS fairness each ran seeds 20261013/14/15: all six runs were PASS with 152/152 accepted, no duplicates/missing, and no retry-safety violations. The fairness gate uses the explicit 1-second dispatch completion grace documented in the scenario profile; it does not claim unlimited-duration starvation proof.

## SCENARIO_MATRIX_AND_RECONCILIATION

| Gate | Current evidence | Status |
|---|---|---|
| R0 | Fresh Windows full backend suite: 127 passed, 3 skipped, 0 failed | PASS |
| R1 reject/drop/partial/DLR-delay/disconnect/restart | Six current loopback results; no duplicate submit, no lost mapping, unknown/partial fenced | PASS |
| R2 owner/backup/0.5/3/6/12s lock holds | Current isolated lock regression | PASS |
| R3 three seeds, normal and low TPS | Six current fairness results, all 152/152 | PASS |
| R4 1K and ~3K | Current bounded controls, full reconciliation, capacity-limited classification | CAPACITY_LIMITED |
| R5 10K staged | Current full 10600-task reconciliation, no duplicate/missing, backlog remains at budget | CAPACITY_LIMITED |
| R6 100K | Not started per stopping rule | NOT_RUN |
| F3 weighted multipart/fair-disabled | Not executed | NOT_RUN |
| F4 lease/quarantine matrix | Not executed | NOT_RUN |

## Fairness and capacity

- Normal mixed fairness: PASS, 152/152 tasks accepted, two routes 76/76, no duplicates/missing, lock regression PASS, retry safety PASS. Receipt confirmation was not required.
- 3K staged capacity: 3040 generated, 1946 successful submit responses, routes 971/975, no duplicates/missing, safety PASS; `CAPACITY_LIMITED` within a 120-second budget.
- 10K staged capacity: 10600 generated, 1959 successful submit responses, routes 1034/925, no duplicates/missing, safety PASS; `CAPACITY_LIMITED` within a 150-second budget.

The capacity-limited classification means the bounded time budget ended with queued work. It is not a data-loss or duplicate-submit classification. In the 3K run queue wait p95/p99 histogram boundaries were 1s/1s; in the 10K run they were 1s/2s. DB lock-wait p95 was 0.5s in both runs.

## Retry and unknown/partial safety

- R1 drop-response: PASS. 3/3 submit attempts were accepted by the simulator but their responses were dropped; all 3 tasks became `unknown`, no ordinary retry, no DLR required.
- R1 partial-submit: PASS. One multipart task accepted its first part and rejected five later parts; final state `partial`, no duplicate send and no partial-fence violation.
- Fresh mixed reports had zero invalid event rows. Unknown/partial retry violation lists and partial-fence violation lists were empty.

## DuckDB lock and operations

Lock regression PASS: second API owner rejected, direct DuckDB contender rejected while the owner held the file, 80/80 concurrent HTTP writes succeeded, 0.5/3/6/12-second injected lock holds recovered after release, and the online backup was readable with service readiness restored.

The SFTP step was not run because no SFTP target/credentials were provided (`SFTP: BLOCKED_ENV`). Live production status remains `UNVERIFIED`; no production restart or deployment was performed.

## REMAINING_RISKS

- SFTP target, SLES target host and production SMPP were not accessed; live status remains unverified.
- F3 weighted multipart/fair-disabled and F4 lease/quarantine matrices remain `NOT_RUN`.
- R4/R5 are capacity-limited under declared budgets; accepted throughput is not a production SLA.
- Historical concatenated event fields reproduced only under the pre-fix queue-flood profile; the separate low-level serialization mechanism remains unisolated and needs a dedicated reproduction before any additional repair is justified.

## COMMANDS_AND_ARTIFACTS

- Fresh regression: `.p0-test-venv\Scripts\python.exe -m pytest -q backend/tests` → exit 0, `127 passed, 3 skipped`.
- Frontend: `npm run build` in `frontend` → exit 0, non-fatal Rolldown/chunk warnings only.
- Mixed controls: `backend/tools/mixed_campaign_capacity_acceptance.py` with profiles `rca1k`, `rca3k`, `fairness`, `scale10k`; all raw JSON and stdout are under `/tmp/mc-mixed-20261013/Docs`.
- R1 faults: `backend/tools/local_e2e_acceptance.py` scenarios `reject`, `drop-response`, `partial-submit`, `dlr-delay`, `disconnect`, `restart`; raw JSON is under the same WSL Docs directory.
- Lock: `backend/tools/local_lock_e2e_acceptance.py --profile smoke` → `rca-lock.json`.
- Isolated SQL: `claim-sql-profile.json` generated from a copied stopped 3K DuckDB, read-only, no production DB access.
- Same-profile before-fix: `/tmp/mc-mixed-20261013/Docs/rca-prefixed-25tps-workers4-before.json`; only the tracked HEAD `smpp_gateway.py` was restored in a temporary backend copy.
- SLES package: `release/marketing-copilot-sles12sp5-system-python-20261013-rca-receipt-disabled.tar.gz`, internal checksums PASS.

## Verification

Windows regression suite: **127 passed, 3 skipped, 0 failed**.
Standalone SMPP smoke with a 30-second-delayed receipt: **PASS** after `submit_sm_resp`; it returned without waiting for the receipt.
Receipt-default smoke-test source syntax and the WSL acceptance sources were compiled/parsed successfully.
Release packaging is performed separately with the repository SLES12 SP5 build script; release artifacts remain local and are not Git changes.

Local SLES12 SP5 package: `release/marketing-copilot-sles12sp5-system-python-20261013-rca-receipt-disabled.tar.gz`
Size: 71,835,474 bytes
SHA-256: `e834e9d39f1231313b2f008306f893256dd99616de5e92e798bf92565d02a547`
`release.json` and every entry in `checksums.sha256` verified successfully; forbidden runtime/data artifacts were absent.

Final status fields:

```text
ROOT_CAUSE_STATUS: CONFIRMED
ROOT_CAUSES: [bounded SMPP async-send queue fail-fast overflow/retry churn, retry-flood event-field corruption trigger, missing bounded latency histograms]
CHANGES: [bounded SMPP queue backpressure, bounded latency histograms, retry/event diagnostics, receipt-free default SMPP smoke]
REGRESSION: 127 passed, 3 skipped, 0 failed
CODE_ACCEPTANCE: PASS
DUCKDB_LOCK_REGRESSION: PASS
WSL_LOCK_REGRESSION: PASS
RETRY_UNKNOWN_SAFETY: PASS
MIXED_FAIRNESS: PASS
CAPACITY_ACCEPTANCE: CAPACITY_LIMITED
CAPACITY_1K: CAPACITY_LIMITED
CAPACITY_3K: CAPACITY_LIMITED
CAPACITY_10K: CAPACITY_LIMITED
F3: NOT_RUN
F4: NOT_RUN
SFTP: BLOCKED_ENV
LIVE_ENV: UNVERIFIED
PUBLISH: NOT_RUN (do not commit/push)
REPORT: Docs/WSL-DuckDB-Claim-Retry-SMPP-Capacity-RCA-And-Acceptance.md
JSON: Docs/wsl-claim-retry-smpp-capacity-results.json
```
