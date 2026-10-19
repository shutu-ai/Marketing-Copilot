# Marketing-Copilot VNext：Worker Clean Exit 与 Scaling Decision

## 最终决策

```text
FINAL_DECISION=WORKER_LIFECYCLE_FIXED_NO_SCALING_BENEFIT
RECOMMENDED_WORKERS=8
CANARY_READY=NO
```

生命周期问题已修复：8/12/16/24 Worker 的 1K smoke 各两轮全部通过；随后按要求完成了 `8→12→16→24` 与 `24→16→12→8` 两轮 5K。5K 中更多 Worker 没有带来稳定的发送阶段收益或 E2E wall throughput 收益，反而增加 RSS、Harness 开销和 drain 时间，因此不把 12/16/24 列为生产扩容候选。生产默认值没有改变，也没有 Canary。

## 冻结配置与门禁

```text
SLES 12 SP5 / native ext4 / CPython 3.11.4 / DuckDB 1.2.0
synthetic SMSC 127.0.0.1
pool_size=4
route_tps=200, tps_scope=per_bind, configured ceiling=800 TPS
window_size=50
producer batch=250
projection batch=512, staging=vectorized, status interval=0.1s
instrumentation=on, profile interval=1.0s
resource_profile=PROD_8G, rss_guard_scope=application
```

所有正式 run 在 `/home/paas/mc-vnext-worker-scaling-20261018-01` 的 `/dev/sda1` native `ext4` 上以独立 run-root 串行执行。运行前可用空间约 `3.6–3.7 GB`，5K 完成后约 `3.5 GB`；未产生仓库内运行数据。

## 1K hard gate：两轮全部通过

每一轮都满足：`requested=effective=spawned=result_files=clean_exit`；1000 accepted、1000 socket-written PDU、1000 submit_sm_resp_success；Journal/Projection `2000/2000`；unknown、duplicate、write_failed 为 0；`max_active_binds=4`；projection caught up；无遗留 lease 或测试进程。

| 顺序 | Run | Workers | Duration (s) | Worker result / clean exit | Journal / Projection | Stop→PROCESS_EXIT phase P50/P95/Max (s) |
|---|---|---:|---:|---:|---:|---:|
| A1 | `lifecycle-w8-a` | 8 | 14.430 | 8 / 8 | 2000 / 2000 | 0.303 / 0.357 / 0.358 |
| A2 | `lifecycle-12-r2` | 12 | 17.013 | 12 / 12 | 2000 / 2000 | 0.278 / 0.341 / 0.361 |
| A3 | `lifecycle-w16-a` | 16 | 18.832 | 16 / 16 | 2000 / 2000 | 0.317 / 0.375 / 0.379 |
| A4 | `lifecycle-w24-a` | 24 | 20.383 | 24 / 24 | 2000 / 2000 | 0.285 / 0.338 / 0.389 |
| B1 | `lifecycle-w24-b` | 24 | 20.259 | 24 / 24 | 2000 / 2000 | 0.300 / 0.417 / 0.516 |
| B2 | `lifecycle-w16-b` | 16 | 18.868 | 16 / 16 | 2000 / 2000 | 0.285 / 0.341 / 0.344 |
| B3 | `lifecycle-w12-b` | 12 | 17.621 | 12 / 12 | 2000 / 2000 | 0.286 / 0.329 / 0.329 |
| B4 | `lifecycle-w8-b` | 8 | 14.612 | 8 / 8 | 2000 / 2000 | 0.282 / 0.304 / 0.363 |

`lifecycle-12-r2` is the post-RCA 12-worker run and is the A-round 12-worker evidence; it uses the same 1K workload and frozen configuration. The phase distribution is `PROCESS_EXIT` for every worker in every successful run.

## 5K AB/BA：正确性全部通过，扩容收益不成立

5K 的每个 run 满足 `accepted/socket_written/loopback_received/submit_sm_resp_success=5000/5000/5000/5000`、Journal/Projection `10000/10000`、`max_active_binds=4`、backlog 0、projection lag 0、worker result/clean exit 全部一致。

速率口径：

- `stage_sampled`：`critical-path-samples.csv` 中第一个到最后一个非零 `socket_written_pdu` 样本的 5000 PDU/秒；这是采样 stage span，不把采样间隔伪装成精确 socket 首尾时间。
- `feed_window`：5000 PDU / producer active window；用于说明固定 4×200 TPS 约束下的 feed 观察值。
- `e2e`：5000 PDU / result `duration_seconds`，包含 response、Journal、Projection 和 clean-exit drain。
- `post_feed_response`：5000 PDU / post-feed SMPP response drain time，仅作 drain 对照，不作为生产吞吐 SLA。

| 顺序 | Run | Workers | Duration (s) | Stage sampled PDU/s | Feed-window PDU/s | E2E PDU/s | Post-feed response PDU/s | App RSS / Harness RSS (KiB) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A1 | `lifecycle-5k-w8-a` | 8 | 46.502 | 147.52 | 170.14 | 107.52 | 362.50 | 308072 / 585580 |
| A2 | `lifecycle-5k-w12-a` | 12 | 45.895 | 149.85 | 172.22 | 108.94 | 371.86 | 462464 / 732152 |
| A3 | `lifecycle-5k-w16-a` | 16 | 50.480 | 133.02 | 172.62 | 99.05 | 281.69 | 615216 / 896312 |
| A4 | `lifecycle-5k-w24-a` | 24 | 54.867 | 127.56 | 172.92 | 91.13 | 229.39 | 915080 / 1191656 |
| B1 | `lifecycle-5k-w24-b` | 24 | 55.661 | 116.23 | 173.79 | 89.83 | 215.38 | 917532 / 1183632 |
| B2 | `lifecycle-5k-w16-b` | 16 | 50.933 | 130.15 | 173.47 | 98.17 | 266.88 | 617156 / 892840 |
| B3 | `lifecycle-5k-w12-b` | 12 | 45.998 | 149.48 | 172.11 | 108.70 | 369.14 | 461608 / 738692 |
| B4 | `lifecycle-5k-w8-b` | 8 | 44.189 | 158.61 | 163.77 | 113.15 | 470.59 | 307828 / 579908 |

两轮中位数摘要：

| Workers | Stage sampled median PDU/s | Feed-window median PDU/s | E2E median PDU/s | App RSS 约值 |
|---:|---:|---:|---:|---:|
| 8 | 153.07 | 166.96 | 110.34 | 0.29 GiB |
| 12 | 149.67 | 172.17 | 108.82 | 0.43 GiB |
| 16 | 131.59 | 173.05 | 98.61 | 0.57 GiB |
| 24 | 121.90 | 173.36 | 90.48 | 0.85 GiB |

因此 24 相对 8 的两轮中位数变化为：stage sampled 约 `-20.4%`，feed-window 约 `+3.8%`，E2E PDU/s 约 `-18.0%`；E2E wall duration 也从约 `45.3s` 增加到约 `55.3s`。这没有达到任务建议的“发送阶段至少约 +15% 且 E2E wall throughput 至少约 +10%”双门槛。16 的资源消耗明显增加而 E2E 反而下降；12 的 E2E 仅接近 8，未形成稳定收益。

## 生命周期与资源判定

- 1K 全部 worker final phase 为 `PROCESS_EXIT`，父进程看到的 exitcode 全为 `0`。
- 5K 全部 worker 结果文件/clean exit 数量一致；stop→`PROCESS_EXIT` phase 的 P95 约 `0.80–1.70s`，最大约 `1.77s`，远低于 120s diagnostic ceiling。
- 所有成功 run 的 `max_active_binds=4`；没有遗留 `route-*-bind-*.lease`。
- 5K 最大 application RSS 约 `917532 KiB`，最大 Harness RSS 约 `1191656 KiB`；所有 resource gate 为 PASS，但 Worker 增加的 RSS 与 drain 成本没有换来 stage/E2E 收益。
- `50K`、600 秒/800 PDU/s 后续容量验收和生产 Canary 未执行；它们不是本任务成功定义的必要项，且不应在没有扩容收益时继续放大资源消耗。

## 正确性、语义与测试

本次修复没有修改生产 `backend/vnext`，只修改 Harness 生命周期观测/停机边界和测试。发送限速、4 Bind、window 50、Journal fsync、Spool checkpoint、unknown 不重发、Projection 单 owner、T6/T7 恢复语义均保持原状。5K 全部 run 的 duplicate、unknown、write_failed 均为 0；没有 redispatch evidence。

```text
targeted lifecycle/worker/transport tests: 35 passed
backend/tests: 447 passed, 5 skipped, 4 warnings
```

详细根因和失败现场见 [Worker_Shutdown_Lifecycle_RCA.md](Marketing_Copilot_VNext_Worker_Shutdown_Lifecycle_RCA.md)。机器可读证据见 [Worker_Lifecycle_Closure_Evidence.json](Marketing_Copilot_VNext_Worker_Lifecycle_Closure_Evidence.json)。

