# Marketing Copilot VNext E2E Critical-Path Profiling and RCA Report

## Closure identity

- Task: `MC-VNEXT-E2E-CRITICAL-PATH-PROFILING-CLOSURE-20261017`
- Baseline: `origin/main=24708c8eb183d689d4f1e8e3df7f63a0ed81c222`
- Current source: working tree, `WT_FILESET_SHA256=9fba337f0672c0b68fb0f6daea4f64c81abeace465d317d17eddcdb8976105fa`
- Product code change: low-disturbance, optional critical-path instrumentation and acceptance-harness evidence only; no resend, route-credit, single-writer, journal durability, or offset semantics were changed.
- Test server Run root: `/home/paas/marketing-copilot-test/runs/vnext-e2e-critical-20261017-codex01`
- Test server: `paas-core`, EulerOS 2.0 SP15, x86_64, 64 CPUs, 375 GiB RAM, ext4 Run filesystem, Python 3.11.6, DuckDB 1.2.0, psutil 6.1.1.

## R0/R1 evidence

The profiler is disabled when no profile root is supplied. When enabled it writes bounded per-process JSON snapshots at a controlled interval and a bounded per-second CSV summary. It does not fsync or write a file per event. The following independent timestamps are emitted where a source exists: task taken, socket write, SMPP response received, response journal durable, projection DB commit, and projection offset durable. Missing sources remain `null` and are reported as `not_measured` rather than inferred.

Instrumentation coverage includes:

- producer/spool acceptance and task-taken progress;
- SMPP TPS-credit wait, response-window wait, socket write/drain, response receipt, callback wall time;
- journal JSON encode, open, write, fsync, active-segment stat, segment scan, durable-event count;
- projection read/decode, staging, dedupe, aggregate upsert, DB commit, offset persist, status/diagnostic writes and backlog timeline.

The OFF arm intentionally has no critical-path event timestamps. This is a valid uninstrumented control, not a zero-latency measurement.

## R2 matrix and observed stages

All server runs used loopback SMPP only, isolated Run directories, CPU affinity `0-3`, and `PROD_8G` application RSS guard (6 GiB). The configured route rate is not reported as achieved throughput.

| Case | Result | Workload | Observed result |
|---|---|---:|---|
| B0/A0 pure writer | PASS | 11,000 events, batch 256/512/1024/2048 | `all_correct=true`; 183.335–185.436 events/s; all four cases matched the expected projection |
| B1 P5500 OFF/ON | PASS | 3 paired runs, 5,500 tasks, 11,000 events | Every arm had 5,500 PDU and responses, 11,000 projection events; ON median 112.384 s vs OFF 111.650 s, +0.657% |
| B2 delay/window | PASS | 1,000 tasks per arm | Four combinations all completed; total wall 18.880–21.131 s, projection drain 16.352–18.066 s |
| B3 50K | PASS | 4 campaigns, 50,000 tasks, 100,000 events | 50,000 PDU/responses and 100,000 projection events; 1,041.312 s; application RSS 759,100 KiB |
| B4 multipart | PASS | 5,500 tasks, campaign 1 long message | 8,250 PDU/responses and 13,750 projection events; 144.667 s |
| B5 steady precheck | PASS | 20 s steady request, 6,450 tasks | 6,450 PDU/responses and 12,900 projection events; 104.848 s |

### B1 paired instrumentation overhead

| Metric | OFF median | ON median | Difference |
|---|---:|---:|---:|
| End-to-end duration | 111.650 s | 112.384 s | +0.657% |
| Application peak RSS | 291,624 KiB | 295,852 KiB | +4,228 KiB |
| Harness peak RSS | 446,524 KiB | 464,928 KiB | +18,404 KiB |
| SMPP drain | 8.481 s | 10.076 s | workload variance; no monotonic regression |
| Projection drain | 102.337 s | 103.036 s | workload variance; no monotonic regression |

### B2 sensitivity

| Response delay | Window | Wall | Response latency P95 | Window wait P95 | Projection drain |
|---:|---:|---:|---:|---:|---:|
| 0 ms | 10 | 20.017 s | 650.350 ms | 912.529 ms | 16.960 s |
| 0 ms | 50 | 21.131 s | 866.995 ms | 398.634 ms | 18.066 s |
| 5 ms | 10 | 18.989 s | 671.291 ms | 834.438 ms | 16.352 s |
| 5 ms | 50 | 18.880 s | 1,127.906 ms | 575.676 ms | 16.499 s |

The window changes response-side wait distribution, but total wall time remains dominated by projection drain in this harness.

## RCA conclusion

### Primary limiting stage: ProjectionWriter staging load

The large-run evidence is decisive. B3 projection metrics report:

- `batch_end_to_end_ns=1,025,359,614,039` (1,025.360 s);
- `staging_load_ns=1,005,957,123,196` (1,005.957 s, approximately 98.1% of batch end-to-end time);
- 201 vectorized staging calls for 100,000 events;
- `aggregate_upsert_ns=7.706 s`, `events_insert_ns=1.338 s`, `db_commit_ns=3.336 s`, `offset_persist_ns=1.531 s`.

This is why B3 spent 947.194 s in projection drain after the producer and SMPP phases had mostly completed. The bottleneck is the current DuckDB staging-load path, not network socket write, callback execution, or database commit itself.

### Secondary limiting stage: SMPP response window and TPS credit waits

Across the three instrumented P5500 runs, median P95 values were:

- response-window wait: 1,643.032 ms;
- SMPP response latency: 1,228.032 ms;
- TPS-credit wait: 678.150 ms;
- socket write/drain: 21.486 ms;
- response callback wall: 6.972 ms;
- journal fsync: 6.672 ms.

Observed phase rates derived from measured phase walls were approximately 729.6 socket-written PDU/s, 544.4 response PDU/s, 1,088.2 durable journal events/s, and 106.9 projection events/s. These are measured workload rates; they are not `route_tps * pool_size` claims. Per-second profile maxima are burst rates and are not used as capacity baselines.

At feed stop, the median deltas were 9.395 s to the last task-taken/send point, 10.103 s to the last SMPP response, 10.108 s to response-journal durability, 102.913 s to the last projection DB commit, and 102.914 s to offset durability. The journal backlog peaked around 4.31 MB and drained to zero; the long tail is projection catch-up.

## Correctness and safety interpretation

All observed runs had zero response-unknown events, complete response-journal counts, complete projection counts, and no automatic resend. The full native suite covers replay, offset durability, generation recovery, GC/reclaim, cutoff, queue pressure, route failure, pause/resume and writer ownership contracts. No previous segment-path-cache or low-status experiment was reintroduced.

## Evidence locations

- Server raw runs: `/home/paas/marketing-copilot-test/runs/vnext-e2e-critical-20261017-codex01/results/`
- Local compressed result evidence: `.tmp/e2e-critical-server-evidence-20261017/matrix/`
- Local paired profile evidence: `.tmp/e2e-critical-server-evidence-20261017/r2-p5500-on-01/` through `r2-p5500-on-03/`
- Native full-suite workspace: `/tmp/mc-e2e-critical-native-20261017-01` on WSL `AlmaLinux-8`, ext4 `/dev/sdd`
