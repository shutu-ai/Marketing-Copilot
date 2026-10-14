# Fast Send Lock-Order Review

Date: 2026-10-14
Scope: Fast claim, campaign control, database writer gate, socket admission, callback/checkpoint path.

## Reviewed order

| Path | Required order | Why |
|---|---|---|
| Fast claim | claim DB transaction → commit/release writer gate → publish local campaign fence | A claim must be durable before the local queue accelerator sees it. No campaign barrier is acquired inside the DB transaction. |
| Pause/cancel/resume | campaign control barrier → DB transaction with `dispatch_generation` CAS → publish new local state | Control serializes with Fast queue admission before taking the writer gate. The SQL generation fence is authoritative after restart. |
| Socket admission | claim/attempt DB fence → SMPP window/queue admission → socket write | `record_send_intent()` authorizes the external write and binds the attempt token before the first socket write. |
| SMPP callback | socket response → in-memory part outcome → durable journal/write-buffer event → fenced task/attempt fold | A late callback cannot overwrite a newer claim or attempt. Fast folds successful tasks through one idempotent checkpoint event. |
| Recovery | durable journal replay → DB event-id fence → task/attempt reconciliation | A result is not considered safely replayed merely because it entered an in-memory queue. |

## Deadlock removed

The old nested order was:

```text
Fast claim: DB writer gate -> campaign fence
Campaign control: campaign fence -> DB writer gate
```

The claim path now publishes `observe_claimed_task()` only after `write_transaction()` returns. The two paths no longer hold the opposite resources while acquiring one another:

```text
claim:   DB transaction commits, then local fence publication
control: local fence, then DB transaction
```

This keeps the useful serialization boundary while eliminating the Claim × Pause cycle. The change does not remove the generation, claim-token, or attempt-token SQL predicates.

## Verification

- `test_fast_claim_publishes_local_fence_after_db_commit`: blocks local observation, starts pause, and asserts control completes before observation is released.
- Fast checkpoint, stop-fence, callback-fence, recovery, and DuckDB reliability tests pass in the full backend suite.
- WSL ext4 equal-window 1K runs pass 3/3 in both Fast and Reliable with pool 4, window 50, route TPS 200, 8 task workers, 2 ingestion workers, and a 2000 send queue.
- A separate WSL `/mnt/d` run captured a result-journal fsync timeout; it is documented as an environment/storage limitation and is not used to claim a lock-order regression.

## Non-goals

This review does not authorize Fast production cutover, alter the production default, weaken backpressure, increase the shared sender queue, or claim exactly-once SMS delivery against a real SMSC.
