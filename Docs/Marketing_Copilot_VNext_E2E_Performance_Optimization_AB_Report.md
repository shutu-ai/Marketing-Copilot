# Marketing Copilot VNext E2E Performance Optimization A/B Report

## Scope and baseline

- Baseline SHA: `24708c8eb183d689d4f1e8e3df7f63a0ed81c222`
- Working-tree file-set SHA: `9fba337f0672c0b68fb0f6daea4f64c81abeace465d317d17eddcdb8976105fa`
- A/B target: verify instrumentation overhead and evaluate one evidence-backed ProjectionWriter staging candidate.
- No production resend, queue, offset, journal, writer-ownership, or route-credit behavior was changed.

## Instrumentation OFF/ON paired comparison

Three OFF/ON pairs used the same server, CPU affinity, workload and loopback simulator: 8 workers, 4 binds, 5,500 tasks, `route_tps=200`, `tps_scope=per_bind`, window 50, batch 250, projection batch 512.

| Pair | OFF wall | ON wall | OFF app RSS | ON app RSS | Correctness |
|---:|---:|---:|---:|---:|---|
| 01 | 111.594 s | 111.668 s | 291,624 KiB | 296,608 KiB | PASS/PASS |
| 02 | 111.650 s | 112.643 s | 290,692 KiB | 295,852 KiB | PASS/PASS |
| 03 | 111.700 s | 112.384 s | 293,208 KiB | 295,216 KiB | PASS/PASS |
| Median | 111.650 s | 112.384 s | 291,624 KiB | 295,852 KiB | all counts complete |

Instrumentation overhead is +0.657% on median end-to-end wall, +4,228 KiB application RSS, and +18,404 KiB harness RSS. This meets the low-disturbance requirement for profiling and is small relative to the observed projection drain.

## R3 candidate evaluation

R2 identified the vectorized staging load as the primary stage. The one minimal candidate was the already-supported `projection-staging-mode=executemany` path, evaluated independently with three 1,000-task instrumented runs. This was an experiment only; it was not promoted.

| Arm | Run 01 | Run 02 | Run 03 | Median |
|---|---:|---:|---:|---:|
| Vectorized control (`r2-b2-delay0-window50`) | 21.131 s | — | — | 21.131 s |
| Executemany candidate (`r3-executemany-*`) | 23.030 s | 24.036 s | 24.023 s | 24.023 s |

Candidate result: `NO_CHANGE`. Executemany is approximately 13.7% slower on median wall and provides no material backlog reduction. It does not satisfy the required +15% improvement or equivalent drain reduction. No production configuration was changed, and no candidate source was merged.

The historical failed segment-path-cache/status experiment remains excluded; it is not part of this A/B and is not recommended for reintroduction.

## R2 sensitivity evidence

The response/window matrix confirms that a smaller window increases window wait, while the larger window can increase response latency under this loopback scheduling pattern. Neither setting removes the projection drain tail:

| Delay / window | Wall | Window wait P95 | Projection drain |
|---|---:|---:|---:|
| 0 ms / 10 | 20.017 s | 912.529 ms | 16.960 s |
| 0 ms / 50 | 21.131 s | 398.634 ms | 18.066 s |
| 5 ms / 10 | 18.989 s | 834.438 ms | 16.352 s |
| 5 ms / 50 | 18.880 s | 575.676 ms | 16.499 s |

## A/B decision

- Instrumentation: ACCEPTED; overhead is bounded and measured.
- Throughput optimization candidate: REJECTED / NO_CHANGE.
- Current limiting stage: ProjectionWriter vectorized staging load, followed by SMPP response-window/TPS waits during feed.
- Required next optimization: a new independent staging design with correctness proof and three paired medians; do not tune only from the route TPS setting.
