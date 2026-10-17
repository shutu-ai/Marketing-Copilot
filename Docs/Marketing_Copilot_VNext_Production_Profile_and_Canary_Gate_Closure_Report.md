# Marketing Copilot VNext Production Profile and Canary Gate Closure Report

## Final card

| Field | Closure value |
|---|---|
| BASELINE_SHA | `24708c8eb183d689d4f1e8e3df7f63a0ed81c222` |
| CURRENT_SOURCE_SHA | `HEAD + WT_FILESET_SHA256=9fba337f0672c0b68fb0f6daea4f64c81abeace465d317d17eddcdb8976105fa` |
| PRODUCT_CODE_CHANGE | Optional critical-path instrumentation and bounded evidence output; no algorithmic throughput candidate promoted |
| PRIMARY_LIMITING_STAGE | ProjectionWriter vectorized staging load; B3 staging load ≈1,005.957 s of 1,025.360 s batch end-to-end |
| SECONDARY_LIMITING_STAGE | SMPP response-window and TPS-credit waits during feed/drain |
| SOCKET_RATE | Median measured phase rate ≈729.6 written PDU/s; not configured-rate multiplication |
| RESPONSE_RATE | Median measured phase rate ≈544.4 response PDU/s |
| JOURNAL_DURABLE_RATE | Median measured phase rate ≈1,088.2 durable events/s |
| PROJECTION_COMMITTED_RATE | Median measured phase rate ≈106.9 events/s |
| RESPONSE_CALLBACK_P95 | 6.972 ms median across instrumented P5500 runs |
| JOURNAL_FSYNC_P95 | 6.672 ms median across instrumented P5500 runs |
| WINDOW_WAIT_P95 | 1,643.032 ms median across instrumented P5500 runs |
| FEED_STOP_DELTA_TASK_TAKEN | 9.395 s median |
| FEED_STOP_DELTA_SMPP_RESPONSE | 10.103 s median |
| FEED_STOP_DELTA_RESPONSE_JOURNAL | 10.108 s median |
| FEED_STOP_DELTA_PROJECTION_COMMIT | 102.913 s median |
| FEED_STOP_DELTA_OFFSET_DURABLE | 102.914 s median |
| BACKLOG | Peak journal backlog ≈4.31 MB, drains to zero; projection tail is the long pole |
| RSS/PSS/CGROUP | B3 app RSS 759,100 KiB; harness RSS 1,177,100 KiB; cgroup memory.max unavailable on server; PSS not exposed by harness |
| PROFILE | Server actual: 64 CPUs / 375 GiB RAM; workload guard `PROD_8G` app RSS 6 GiB; not an 8 GiB or 16 GiB hardware host |
| T6_TARGETED | PASS: Windows targeted suite 10 passed; native current-source smoke 80/80 under PROD_8G and LOW_MEM_2G |
| T7_NATIVE_FULL | PASS: native ext4 backend suite 424 passed, 2 skipped, 4 warnings in 248.68 s |
| EULEROS_MATRIX | PASS for loopback B1/B3/B4/B5 runs; all counts complete and no unknown responses |
| EULEROS_600S_800PDU | NOT_VERIFIED: no 600-second / 800-PDU steady gate was run in this closure |
| PROD_8G_SIZING | PASS as application-RSS guard evidence on the actual server; hardware-equivalent sizing not claimed |
| PROD_16G_SIZING | NOT_VERIFIED: no 16 GiB cgroup/host was available |
| LOW_MEM_2G | PASS for smoke guard only; stress acceptance not claimed |
| R3_CANDIDATE | NO_CHANGE; executemany median 24.023 s vs vectorized 21.131 s |
| CANARY | NO-GO pending a new staging optimization and explicit 600 s / 800 PDU and 16 GiB profile gates |
| COMMIT_PUSH | NOT_REQUESTED; changes remain uncommitted and unpushed |

## Frozen server profile

The authorized test server was frozen before testing as `paas-core`, EulerOS 2.0 SP15 x86_64, kernel `6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`, 64 CPUs, approximately 375 GiB RAM and 8 GiB swap. The Run filesystem is ext4 (`/dev/sda2`); `/tmp` is also ext4. The cgroup memory limit files were unavailable, so RSS guards are harness-level application limits rather than cgroup enforcement.

The server venv used CPython 3.11.6, DuckDB 1.2.0 and psutil 6.1.1. The source package was uploaded from the working tree with archive SHA-256 `277DF2A83B06BF511BFF0E1DE151D9AABD31CA19515F8CB8A33DBD0D2DB8C6D6`. No server credentials, production database, real SMSC or real recipient data were used.

## Gate evidence

1. B0 pure writer: all four batch-size cases correct.
2. B1 three paired instrumentation runs: all 5,500 tasks, 5,500 PDU/responses and 11,000 projection events complete in every arm.
3. B3 50K: 50,000 tasks, 50,000 PDU/responses, 100,000 journal/projection events; PASS under the 6 GiB application guard.
4. B4 multipart and B5 steady precheck: PASS with complete projection and response counts.
5. Native ext4 full suite: `424 passed, 2 skipped, 4 warnings`.
6. Current-source native smoke: PROD_8G and LOW_MEM_2G both 80/80 PASS.

## Release decision

The closure is technically reproducible and the correctness gates are green, but production canary is `NO-GO`. The evidence closes the RCA, not the staging bottleneck. A follow-up optimization must be independently sourced, preserve one ProjectionWriter/one DuckDB/offset durability/at-most-once semantics, demonstrate at least the required paired improvement, and then repeat the missing 600-second and 16 GiB profile gates before canary approval.
