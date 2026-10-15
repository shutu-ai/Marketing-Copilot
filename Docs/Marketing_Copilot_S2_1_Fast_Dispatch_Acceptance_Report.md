# Marketing Copilot S2.1 Fast Dispatch Acceptance Report

## Verdict

**S2.1_READY_FOR_S2.2**

S2.1 passed three independent 1K loopback acceptance runs on the repaired source. `S2_READY` remains `NO_GO`; S2.2 is the next permitted scope. S3/S4 were not entered.

## Controlled parameters

All three runs used the same configuration:

```text
scenario=stress-1k  files=10  rows=1000  gate=full
dispatch=fair  db=serialized  send=fast  dlr=disabled
pool=4  smpp_window=50  smsc_tps=200
task_workers=8  ingest_workers=2  database_workers=2
business_timeout=300s  SMSC=loopback
```

No timeout, Worker, TPS, socket-window, dispatch-limit, or queue-capacity increase was used.

## Independent runs

| Run | App uptime (s) | Tasks | `submit_sm` | Successful responses | Rejects | Unknown | Duplicates | Post-run DB task/attempt state |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| R5 | 273.127 | 1000 accepted | 1000 | 1000 | 0 | 0 | 0 | `accepted=1000` / `accepted=1000` |
| R6 | 255.468 | 1000 accepted | 1000 | 1000 | 0 | 0 | 0 | `accepted=1000` / `accepted=1000` |
| R7 | 255.573 | 1000 accepted | 1000 | 1000 | 0 | 0 | 0 | `accepted=1000` / `accepted=1000` |

The harness reports `PASS` for all three runs. The uptime values are below the original 300-second business window; wall-clock report duration also includes setup and controlled teardown. The post-run read-only DuckDB check confirms every task and send attempt is accepted.

## Result-path evidence

The repaired Fair accumulator reduced the result transaction amplification without changing the external rate controls:

| Run | Fast checkpoint batches observed | Items represented in checkpoint metrics | Serialized result transactions |
|---|---:|---:|---:|
| R5 | 701 | 985 at the report snapshot | 700 |
| R6 | 667 | 1000 | 667 |
| R7 | 666 | 997 at the report snapshot | 666 |

The report snapshot is taken before harness teardown; the post-run database verification above is the terminal acceptance check. All three runs reported no Fast fail-closed error, no unknown retry violation, no lost mapping, and no misdelivery.

## Validation

- Focused regression: `21 passed` for `test_fast_send.py` and `test_s11_result_capacity.py` in AlmaLinux-8 WSL ext4.
- Frontend: `npm run build` passed after one sandbox-permission retry; only existing dependency annotation and chunk-size warnings were emitted.
- Full backend suite was attempted. It reached `41 passed` but hit `612` environment errors from the pre-existing read-only `/tmp/mc-live-hotpath-20261013` test/temporary environment. The unrelated environment was not modified.

## Artifacts and safety

All acceptance artifacts remain outside the repository under the WSL ext4 `/tmp/mc-s21-final-20261015-r5..r7` directories. No runtime database, log, upload, message body, backup, or secret was added to deployment/repository artifacts.

## Gate result

`S2.1_READY_FOR_S2.2`
