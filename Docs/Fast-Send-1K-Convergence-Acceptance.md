# Fast Send 1K Convergence Acceptance

Date: 2026-10-14
Decision: current-commit G3/G4 equal-window verification `NOT_RUN`; historical pre-fix exploratory runs are retained for comparison only; production Fast cutover `NO_GO`.

## Fixed comparison window

Both modes used the same isolated harness settings:

```text
1,000 valid events; 10 input files; pool=4; SMPP window=50;
route TPS=200; send queue=2000; task workers=8; ingest workers=2;
watch-copy workers=2; DLR=disabled; local loopback SMSC.
```

The three Fast runs and three Reliable runs below used WSL ext4-backed run roots (`/tmp`), but their reports identify commit `c29eefc` and predate the `65060c4` fix. They are historical exploratory evidence, not current-commit G3/G4 acceptance.

| Mode | Run 1 | Run 2 | Run 3 | Accepted | SMSC submits | Duplicate/lost/misdelivered | Watchdog |
|---|---:|---:|---:|---:|---:|---|---|
| Fast (historical `c29eefc`) | `HISTORICAL_PASS` | `HISTORICAL_PASS` | `HISTORICAL_PASS` | 1000/1000 each | 1000/1000 each | none observed | no stall captured |
| Reliable (historical `c29eefc`) | `HISTORICAL_PASS` | `HISTORICAL_PASS` | `HISTORICAL_PASS` | 1000/1000 each | 1000/1000 each | none observed | no stall captured |

Observed loopback throughput was approximately 37.947–39.479 events/s for Fast and 27.028–27.620 events/s for Reliable. These are simulator measurements, not a live-route capacity promise.

## Gate status

| Gate | Status | Evidence/limitation |
|---|---|---|
| G0 full regression/build/compile | `PASS_WITH_LIMITATIONS` | 183 backend tests passed, 3 skipped; frontend build passed with existing Rolldown/chunk warnings; Python compile and diff checks pass. |
| G1 ingest-only 1K | `PASS_LOCAL_EXT4` | Dedicated `ingest-only` gate at commit `3442ca9`: 10/10 files completed, 1000 input rows generated 1000 tasks, all 1000 remained queued, and socket submits/responses were 0. The report captured the watchdog and observability sections with no stall. |
| G2 sender-only pre-generated 1K | `PASS_LOCAL_EXT4` | Dedicated `sender-only` gate at commit `3442ca9`, repeated with pool=4 and pool=1: 1000 pre-generated tasks, 1000 socket submits, 1000 responses, and 1000 accepted in each run; no duplicate, unknown, or retry violation. |
| G3 Fast 1K ×3 | `NOT_RUN_CURRENT_COMMIT` | The retained 3/3 report set is tagged `c29eefc` and predates the fix; it is not evidence for the current `3c8189a` delivery. |
| G3 Reliable 1K ×3 | `NOT_RUN_CURRENT_COMMIT` | The retained 3/3 report set is tagged `c29eefc` and predates the fix; a paired current-commit baseline is required. |
| G4 equal-window A/B | `NOT_RUN_CURRENT_COMMIT` | Current-commit Fast/Reliable A/B has not been rerun after the code and gate changes; historical results are retained but excluded from PASS. |
| G5 stop/fault/restart/soak | `PARTIAL_STOPPED` | Deterministic lock-order and existing fault/stop-fence evidence passed. The 50K multi-campaign isolation track passed offline with no socket writes because SMSC was unavailable. The new 600-second stability run was stopped before completion, so no long-soak pass is claimed. |

## Environment boundaries

- Windows native probes are `BLOCKED_ENV`: the API process exited `0xC0000374` in the CPython 3.13 DuckDB extension, including low-concurrency/no-live-observer attempts.
- A WSL `/mnt/d` Fast 1K run reached 1000 socket submissions/responses but stalled at 980 accepted after `SMS result journal fsync timed out`. Its watchdog artifacts are retained locally and are intentionally not committed.
- Historical WSL `/tmp` ext4 runs passed at `c29eefc`, but current-commit G3/G4 remains unverified. This isolates only the historical result to a Linux/ext4-like filesystem; it does not validate SLES12SP5 packaging, a real SMSC, or production data.
- The dedicated G1/G2 reports were generated under `/tmp/mc-e2e-gates-final` against commit `3442ca92937f6ddf83c0d5ac639eac743dfce503`; the reports include `workers`, `smpp`, `ingestion`, `backpressure`, `fast_send`, `scheduler`, `database`, and `metrics` observability sections.

## Release decision

Keep `MC_SMS_SEND_MODE=reliable` as the production default. Fast remains an explicit experimental opt-in. Do not auto-cut over or call the local simulator throughput a production SLA. Before a canary, rerun current-commit G3/G4, complete the remaining G5 soak/fault matrix, validate the actual SLES12SP5 runtime and deployment package, and run the live SMSC acceptance matrix.

## Final verification summary

- `DONE`: RCA and Claim/Pause lock-order fix; G0 regression/build/compile checks; dedicated G1 ingest-only; dedicated G2 sender-only with pool=1 and pool=4; historical equal-window evidence retained with its pre-fix SHA; existing local fault-path evidence.
- `BLOCKED_ENV`: Windows-native DuckDB probe and WSL `/mnt/d` durability path; no real SMSC or SLES12SP5 canary was attempted.
- `FAIL`: the historical WSL `/mnt/d` Fast run remains 980 accepted / 20 sending after journal fsync timeout and is not overwritten by the ext4 result.
- `NOT_RUN`: current-commit G3/G4 1K rerun, 3K/10K/50K equal-window expansion, live B/C socket G5 matrix, and the SLES12SP5/real-SMSC canary.
- `PARTIAL_STOPPED`: the requested 600-second local stability run was stopped before completion; its missing final report is recorded as `MISSING_ARTIFACT` and is not a pass.
