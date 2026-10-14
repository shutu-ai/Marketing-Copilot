# Fast Send 1K Stall RCA

Date: 2026-10-14
Decision: `NO_GO` for production Fast Send/canary; production default remains `reliable`.

## Executive finding

The work separated three different failure classes instead of treating every 1K timeout as one deadlock:

1. A real Fast-mode Claim × Pause lock-order inversion was fixed. Fast claim now commits the database claim before publishing its in-process campaign fence. The deterministic regression test proves that a pause can acquire the fence and finish while claim observation is deliberately blocked.
2. A real whitelist-upload lifetime race was fixed. The uploaded source is now copied to a durable campaign-owned file before the `building` version row is published, so the scheduler cannot race a router cleanup of a temporary upload path.
3. The remaining `/mnt/d` WSL failure is an environment/storage durability failure, not a long-held application database lock. The app log recorded `SMS result journal fsync timed out`; the 60-second snapshot showed the journal acknowledgement path waiting while the application writer lock was only about 0.01 seconds old. The same equal-window workloads passed 3/3 per mode on WSL ext4 (`/tmp`).

The result is a local ext4 convergence pass with a cross-environment limitation. It is not evidence for a live SMSC or SLES production canary.

## Evidence-first timeline

| Probe | Configuration | Result | Interpretation |
|---|---|---|---|
| Windows native | Fast/Reliable local loopback, including low-concurrency/no-observer probes | `BLOCKED_ENV`, API process exited `0xC0000374` in `duckdb.cp313-win_amd64.pyd` | Native Windows DuckDB runtime is not a valid business-pass environment for this acceptance run. |
| WSL `/mnt/d` Fast | 1K, 10 files, pool 4, window 50, TPS 200, 8 task workers, 2 ingest workers | `FAIL`: 1000 socket submits/responses, 980 accepted, 20 `sending` at 180 seconds | Journal fsync timed out; final journal contained all checkpoint items but the DB lacked three corresponding event rows. Retained as a storage-path failure, not silently converted to PASS. |
| WSL `/tmp` Fast | Same parameters, three independent runs | `PASS` 3/3, 1000/1000 accepted, no duplicate/lost/misdelivery/unknown retry | Equal-window Fast convergence is reproducible on ext4. |
| WSL `/tmp` Reliable | Same parameters, three independent runs | `PASS` 3/3, 1000/1000 accepted, no duplicate/lost/misdelivery/unknown retry | Reliable is the control and remains the default. |

All loopback runs use the local simulator only; `live_env_status` is `LIVE_ENV_UNVERIFIED`.

## Root-cause analysis

### Claim × Pause deadlock

Before the fix, the Fast claim callback called `observe_claimed_task()` while the claim transaction still held the DuckDB writer gate. Campaign control acquired the Fast campaign barrier first and then entered its database transaction. The opposing order was:

```text
claim:   DB writer gate -> campaign fence
control: campaign fence -> DB writer gate
```

That cycle was possible under a pause/cancel race. The implementation now returns from the complete `write_transaction()` first and only then publishes the local Fast fence. SQL `dispatch_generation`, claim tokens, and attempt tokens remain authoritative; the local fence is only a queue accelerator.

### Whitelist source lifetime race

The upload route previously created the version row and then copied from the request temporary path. The scheduler could observe the new `building` row after the router had removed that temporary path. The fix materializes a unique file inside the campaign source directory before `create_version()` and cleans it up only on copy failure.

### `/mnt/d` journal fsync stall

The failed WSL run reached all 1000 socket submissions and responses. The application log then recorded a result-journal acknowledgement timeout. The watchdog captured:

- 30-second and 60-second no-progress snapshots;
- application and controller thread stacks;
- process state and logs;
- Fast checkpoint, write-buffer, backpressure, SMPP pool/window, worker, and writer-gate metrics;
- deferred live DuckDB metadata, because opening a second live DuckDB connection would itself create a false file-lock stall.

The authoritative post-shutdown reconciliation found 980 accepted and 20 sending. The journal contained 1000 Fast checkpoint payload items and all terminal markers, but three corresponding `sms_result_events` rows were absent. This is retained as evidence of an unsafe `/mnt/d` fsync/commit environment for this probe. Moving only the isolated run root to WSL ext4 removed the failure without changing production queue sizes, worker counts, or backpressure policy.

## Changes made

- Moved Fast local-fence publication after the committed claim transaction.
- Added a deterministic Claim × Pause regression test.
- Materialized whitelist upload sources before publishing a build row and added a race-prevention test.
- Added opt-in 30-second watchdog and 60-second full snapshots, process-exit capture, bounded API thread-stack diagnostics, and `faulthandler` application stacks.
- Added `--run-root` to the local acceptance harness so WSL ext4 and DrvFS behavior can be compared explicitly.
- Added isolated `ingest-only` and pre-generated-task `sender-only` acceptance gates, including an opt-in task-worker disable switch for the ingest phase and structured reconciliation/observability output.
- Added a fenced-out callback warning without weakening claim/attempt fences.

No production default was changed, no backpressure guard was disabled, and no queue/worker expansion was used as a fix.

## Residual risks and decision

The following remain open and prevent a production Fast cutover:

- no real SMSC or SLES12SP5 canary was run;
- dedicated G1 ingest-only and G2 sender-only gates now pass on WSL ext4 at commit `3442ca9` (G2 repeated with pool=4 and pool=1);
- the 50K multi-campaign isolation track passed offline, but G5 long soak/fault/restart acceptance was not completed; the new 600-second stability run was stopped before completion;
- Windows native and WSL DrvFS storage behavior remain blocked/unsafe for this evidence path.

Use `reliable` in production. Fast remains explicit opt-in and `NO_GO` for production performance until the remaining G5 gates, target SLES validation, and live-SMSC evidence are complete.
