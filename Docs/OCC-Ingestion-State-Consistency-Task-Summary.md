# OCC ingestion state consistency task summary

## Scope

- Task: `MC-OCC-INGESTION-CONSISTENCY-CLOSURE`
- Baseline: `2bec392493d480508be016289858de83e92b5cc4`
- Implementation source SHA before this summary document: `28e0a54dea2ce317e4fcb4172d2993a32308f45f`
- Current conclusion: code changes are submitted locally, but the requested G3 closure is not proven; production decision remains `NO_GO`.

## Completed implementation

The local implementation commits address the following related code paths:

1. Separate authoritative ingestion state from the `processed_files` display/projection mirror, so a missing or stale mirror row does not roll back durable file, chunk, or task state.
2. Make projection writes idempotent and repairable, and add `backend/tools/reconcile_processed_files.py` for bounded inspection/repair of the mirror.
3. Retry complete OCC transactions for ingestion progress, task checkpoints, file recovery, and related realtime state updates instead of replaying only a partial write.
4. Fence stale workers and leases so delayed progress, retry, completion, and task-creation callbacks cannot regress a completed file or create duplicate work.
5. Normalize replayed completed chunk states and bound the OCC replay budget.
6. Keep DuckDB OCC connection execution at one thread by default, with `MC_DUCKDB_THREADS` available for an explicit deployment override.
7. Add regression coverage for missing projections, delayed projection writes, same-key upserts, stale leases, replayed chunks, recovery, checkpoint/task atomicity, and concurrent ingestion ownership.

The implementation range contains these related files only:

```text
backend/database.py
backend/services/batch_campaign_service.py
backend/services/ingestion_job_service.py
backend/services/monitor_service.py
backend/services/realtime_campaign_service.py
backend/services/realtime_ingestion_state.py
backend/services/realtime_ingestion_worker.py
backend/services/realtime_whitelist_service.py
backend/services/sms_task_service.py
backend/tests/test_occ_ingestion_consistency.py
backend/tests/test_realtime_progress_batches.py
backend/tools/reconcile_processed_files.py
```

## Local commit chain

```text
aa1a7fb0 fix realtime ingestion authority and projection consistency
e92e18b2 increase OCC replay budget for ingestion state
efbd74df retry realtime task checkpoints under OCC
730eda42 repair OCC-replayed ingestion chunk states
53b0aff4 stabilize OCC replay backoff under concurrent sends
e7f3196d restore validated OCC replay jitter default
a5d511ea isolate DuckDB OCC connection execution threads
28e0a54d bound OCC replay and normalize completed chunks
```

## Verification status

- `git diff --check origin/main..HEAD`: `PASS`.
- The latest Windows targeted-test attempt was blocked during pytest fixture setup by `PermissionError: [WinError 5]` while pytest scanned its temporary-directory root. It reported `3 passed, 23 errors`; the errors were environment setup errors, not business assertion failures. The test result is therefore not accepted as a current gate.
- Earlier targeted and full-suite runs were recorded during implementation, but the full suite was not rerun after the final replay-budget and completed-chunk normalization commits.
- Intermediate WSL/ext4 OCC+Fast runs reproduced stalls and/or incomplete ingestion state, so no current-source G3 pass is claimed. The final OCC+Fast acceptance run was stopped before producing a result.

| Gate | Status |
| --- | --- |
| P0 reproduction | `LOCAL_REPRODUCED` |
| P1 authority/projection separation | `IMPLEMENTED` |
| P2 fault-injection closure | `NOT_VERIFIED` |
| G3 OCC+Fast 1K x3 | `NOT_VERIFIED` |
| G3 equal-window 2x2 | `NOT_RUN` on the final source |
| G4/G5 | `NOT_RUN` |
| SLES12SP5 | `NOT_RUN` |
| REAL_SMSC | `NOT_RUN` |
| Production decision | `NO_GO` |

## Deliberately excluded from the commit

Runtime databases, WAL/journal files, logs, generated CSV/JSON evidence, caches, virtual environments, backup directories, the source task instruction file, the unrelated `frontend/package-lock.json` modification, and other pre-existing user worktree changes remain uncommitted. They are not source code or durable documentation and must not be included in the push.
