# Fast Send 1K Convergence Acceptance

Date: 2026-10-14
Decision: local WSL-ext4 equal-window convergence `PASS`; production Fast cutover `NO_GO`.

## Fixed comparison window

Both modes used the same isolated harness settings:

```text
1,000 valid events; 10 input files; pool=4; SMPP window=50;
route TPS=200; send queue=2000; task workers=8; ingest workers=2;
watch-copy workers=2; DLR=disabled; local loopback SMSC.
```

The three Fast runs and three Reliable runs used WSL ext4-backed run roots (`/tmp`).

| Mode | Run 1 | Run 2 | Run 3 | Accepted | SMSC submits | Duplicate/lost/misdelivered | Watchdog |
|---|---:|---:|---:|---:|---:|---|---|
| Fast | `PASS` | `PASS` | `PASS` | 1000/1000 each | 1000/1000 each | none | no stall each |
| Reliable | `PASS` | `PASS` | `PASS` | 1000/1000 each | 1000/1000 each | none | no stall each |

Observed loopback throughput was approximately 37.947–39.479 events/s for Fast and 27.028–27.620 events/s for Reliable. These are simulator measurements, not a live-route capacity promise.

## Gate status

| Gate | Status | Evidence/limitation |
|---|---|---|
| G0 full regression/build/compile | `PASS_WITH_LIMITATIONS` | 183 backend tests passed, 3 skipped; frontend build passed with existing Rolldown/chunk warnings; Python compile and diff checks pass. |
| G1 ingest-only 1K | `NOT_RUN` | The current harness has no dedicated sender-disabled ingest-only mode. Full runs did verify 10 files, 1000 input rows, 1000 generated tasks, and file completion, but that is not substituted for G1. |
| G2 sender-only pre-generated 1K | `NOT_RUN` | No dedicated pre-generated-task sender gate was run. Full runs verify the combined sender path only. |
| G3 Fast 1K ×3 | `PASS_LOCAL_EXT4` | Three independent 1000/1000 local loopback runs; no duplicates, lost mappings, misdelivery, unknown retry, or watchdog stall. |
| G3 Reliable 1K ×3 | `PASS_LOCAL_EXT4` | Three independent 1000/1000 local loopback runs with the same fixed window. |
| G4 equal-window A/B | `PASS_LOCAL_EXT4` | Fast and Reliable each passed 3/3 under the exact shared settings above. |
| G5 stop/fault/restart/soak | `PARTIAL` | Deterministic lock-order and existing fault/stop-fence unit evidence passed; no long soak or complete new G5 matrix was run. |

## Environment boundaries

- Windows native probes are `BLOCKED_ENV`: the API process exited `0xC0000374` in the CPython 3.13 DuckDB extension, including low-concurrency/no-live-observer attempts.
- A WSL `/mnt/d` Fast 1K run reached 1000 socket submissions/responses but stalled at 980 accepted after `SMS result journal fsync timed out`. Its watchdog artifacts are retained locally and are intentionally not committed.
- WSL `/tmp` ext4 runs passed. This isolates the convergence claim to a Linux/ext4-like filesystem; it does not validate SLES12SP5 packaging, a real SMSC, or production data.

## Release decision

Keep `MC_SMS_SEND_MODE=reliable` as the production default. Fast remains an explicit experimental opt-in. Do not auto-cut over or call the local simulator throughput a production SLA. Before a canary, complete G1/G2/G5, validate the actual SLES12SP5 runtime and deployment package, and run the live SMSC acceptance matrix.
