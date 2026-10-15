# Marketing Copilot S2.1 Fast Dispatch Stall RCA

## Decision

**S2.1_READY_FOR_S2.2**

This closure is limited to S2.1 Fast dispatch/result-path behavior. `S2_READY` remains `NO_GO`; no S3 or S4 work was started.

## Baseline and scope

- Baseline: `origin/main` and `HEAD` at `f37857337f78bfba3ba6665a0e7c1de6fa1cd117`.
- Scenario: `stress-1k`, 10 input files × 100 rows, 1000 expected tasks, full gate.
- Dispatch: `fair`; database: `serialized`; send mode: `fast`; DLR: disabled.
- Fixed acceptance parameters: pool 4, SMPP window 50, SMSC TPS 200, task workers 8, ingest workers 2, database workers 2, business timeout 300 seconds.
- The campaign timeout, worker counts, TPS, dispatch limits, and queue capacities were not increased.
- Runs used the AlmaLinux-8 WSL ext4 isolated directories and the loopback SMSC only.

## Root cause

The Fair Fast success path bypassed the existing Fast checkpoint accumulator. Each successful task called `sms_write_buffer.submit("fast_checkpoint", {"items": [payload]})` directly. That caused one Journal event, one WriteItem, and normally one serialized `sms_result_batch` transaction per successful task.

The Fair `ReadyPool` intentionally held each result reservation until the durable result buffer drained. Under serialized DuckDB, the per-task result transaction became the limiting stage: the socket returned successful responses, but claims accumulated behind result completion and the 300-second business window expired with queued tasks.

Evidence from the unmodified current-origin baseline:

- `539/1000` accepted, `460` queued, `539` successful `submit_sm_resp`, zero SMSC rejects and zero unknown tasks.
- `538` Fast checkpoint events were enqueued and `538` serialized `sms_result_batch` transactions were observed for that partial run.
- The retained S2 evidence on the prior implementation showed the same shape at the reported `900/1000`: successful socket responses, no rejects/unknowns, and a residual queue.

Therefore the stall was not an SMSC rejection or retry storm. It was result-path transaction amplification combined with Fair result-credit retention.

## Secondary accounting defect found during closure

Reusing the accumulator exposed a second defect. A multi-task Fast checkpoint has no single result-credit parent, so `owner_for()` returned no parent and the aggregate Journal event was charged to the emergency bucket. `ledger.safe()` treats emergency usage as a socket safety failure; longer aggregation then closed new Fair admissions and produced deferred/cancelled attempts.

This was reproduced before the final fix as `result byte/replay safety gate is closed`, with duplicate-send violations. Those experimental variants were discarded and are not part of the accepted source.

## Minimal fix

1. Fair Fast successes now enter the existing bounded checkpoint accumulator instead of directly submitting one event per task.
2. Fair accumulation requires the task's live dispatch result reservation and is bounded by the existing ReadyPool task limit; it cannot become an unmetered success queue.
3. Fair uses a 300 ms checkpoint debounce so the serialized path forms small batches without holding a claim across the existing stale-claim protection window. This is an internal result batching interval, not the business timeout.
4. Multi-task `fast_checkpoint` Journal events are charged to the existing normal dispatch result budget. Single-task and invalid/unowned events retain the emergency fail-closed path. Resizing such a dispatch-owned credit now debits the normal bucket correctly.

No Worker, TPS, timeout, socket window, dispatch limit, or queue-capacity increase was made.

## Regression coverage

- Fair Fast successes are verified to accumulate three tasks and emit one checkpoint event: `backend/tests/test_fast_send.py`.
- Aggregate Fast checkpoint credits are verified to use the normal dispatch budget and leave the emergency bucket safe: `backend/tests/test_s11_result_capacity.py`.
- The focused Fast/result-capacity suite passed `21` tests.

## Evidence locations

The three final JSON reports and retained loopback artifacts remain in the WSL ext4 paths `/tmp/mc-s21-final-20261015-r5`, `/tmp/mc-s21-final-20261015-r6`, and `/tmp/mc-s21-final-20261015-r7`. Runtime databases, logs, messages, and SMSC artifacts were not copied into the repository.
