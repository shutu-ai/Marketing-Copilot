# Marketing Copilot VNext T3/T4/T6/T7 证据闭环报告

## 结论

本轮在 EulerOS 测试机原生 ext4 上完成了 harness 修复、T3 多 Campaign 流式验收、T4 600 秒持续负载、T6 服务级跨业务日以及当前故障工具覆盖的 T7 子场景。结论为：

```text
EULEROS_VNEXT_FINAL_CANARY_READY=NO
```

T3 在修正 transport response drain 后三次独立 50K 正式运行通过；T6 服务级跨日通过；T7 已执行的三个子场景通过，但完整故障矩阵仍未覆盖。T4 在可持续 ProjectionWriter 速率下通过 600 秒 steady + drain，但在约 800 PDU/s 的高压运行中，单 ProjectionWriter 的 journal catch-up 明显落后，不能把高压 drain 判为通过。因此本轮仍是 NO_GO，不构成真实 SMSC、SLES12SP5 或生产部署许可。

## 基线、范围与环境

| 项目 | 值 |
|---|---|
| Task | `MC-VNEXT-EULEROS-T3-T4-T6-T7-CLOSURE-20261017` |
| Git HEAD / origin/main | `ee2591839a86806397284497d30eefe1f282054f` |
| 产品源代码基线 | `71ca9e25f40d9c57932ab8c02a6b69877a099eba` |
| 测试机 | EulerOS 2.0 SP15，`paas-core`，非 root |
| Python / DuckDB | CPython 3.11.6 / DuckDB 1.2.0 |
| 文件系统 | `/dev/sda2`，`ext4`，所有正式 Run 的 source、venv、spool、journal、WAL/DB、临时目录在该文件系统 |
| Run 根目录 | `/home/paas/marketing-copilot-test/runs/vnext-t3-t4-t6-t7-closure-20261017-codex02` |
| loopback | 仅绑定 `127.0.0.1`，未连接真实 SMSC、生产 API 或生产数据库 |
| 依赖 | `pip freeze` 仅含离线 DuckDB wheel：`duckdb==1.2.0`，wheel SHA-256 为 `55f2b0...1c19f` |
| 磁盘 | R0 初始约 26% 使用；正式 Run 后复核约 33%，仍有约 13.1 GB 可用 |

R0 原始证据在远端 Run 的 `evidence/` 下，包括 `source-identity.json`、`env-check.json`、`effective-config.json`、`run-config.json`、`pre-run-storage.json`、`df-T.txt`、`pip-freeze.txt` 和 `t4-timeseries.csv`。

正式 T3/T4 Run 实际使用了 all 64 CPUs、`nice=0`、`ionice=none: prio 0`；没有将任务描述中建议的 `CPU 0-3 / nice +10 / ionice class=3` 伪写成正式配置。另有独立 `resource-guard-smoke.json` 证明 `taskset 0-3 + nice 10 + ionice class 3` 的 wrapper 可执行，但该约束未用于正式 T3/T4 数值结论。

## R1 Harness 修复与可信度

本轮实际采用的 harness 版本：

| 文件 | SHA-256 |
|---|---|
| `backend/tools/vnext_loopback_acceptance.py` | `cb9d1b6d97cb7770d2b3758d080aee425013eda93b949dc9257623f07930a0b9` |
| `backend/tools/vnext_streaming_acceptance.py` | `dd46fb4421842d170e622ae626f23cbed553f17f0cb6b1814e45325b5d67540f0` |
| `backend/tools/vnext_daily_fault_acceptance.py` | `690e9b61e05cf82b0fc84ca69ca524ea063631558028798d1f4cbc5837d9d822` |
| `backend/vnext/business_day.py` | `0437479252f3c8555fe2926e129aa289fb4fa99c7c4a82ae6ab80946c9655675` |
| `backend/vnext/runtime.py` | `4dfa384a70ceb17070d4db07032dbe785f1a15313ed98d920746c77e37462d2c` |

修复点：

- feeder 使用有界 chunk、增量 offset、`lightweight_snapshot()` 和 `max_pending_records`；热循环不再调用全量 `TaskSpool.status()`。
- loopback 支持显式四 Campaign plan，T3 以 `(campaign_type, campaign_id, event_id, part_index)` 语义核对，不以 destination 全局唯一替代业务身份。
- 普通同日 loopback 不再硬编码 T6 跨日计数为零，而是输出 `executed=false / not_exercised` 和 `null`。
- worker pass 结束前等待 SMPP response window 清零，再释放 bind；修复了高负载下 socket write 已完成但响应被 transport close 清掉的问题。
- worker pass 边界释放已完成的 reader-side spool index 和 destination lock cache，避免 8 个 worker 各自长期保留完整 decoded spool index。
- 每个 worker 输出 bounded snapshot；每秒采集 feeder、spool、SMPP、projection、CPU/RSS、磁盘和 journal 样本。
- `FileWallClock` 和 `MC_VNEXT_TEST_CLOCK_FILE` 是显式、opt-in 的测试时钟 seam；默认生产路径仍使用系统 wall clock，未修改操作系统时间。

因此 `HARNESS_TRUSTWORTHY=PASS`。早期 `t3-multi-50k-01` 的缺响应是修复前 harness 关闭 response window 的可复现 RCA，不从结果中删除，见下表。

## T3：四 Campaign、50K 流式供给

正式参数为 8 workers、4 physical binds、`tps_scope=per_bind`、每 bind 200 submit_sm parts/s、window 50、bounded producer、1 秒采样。Campaign plan 为 `20,000 + 20,000 + 5,000 + 5,000`，全部是单 PDU 短信；multipart 使用独立实验。

| Run | 状态 | tasks | socket PDU | SMPP response | projection | drain(s) | max bind | 说明 |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `t3-multi-50k-01` | FAIL | 50,000 | 50,000 | 48,867 | 50,000 | 254.430 | 4 | 修复前 response window 被过早关闭 |
| `t3-multi-50k-02` | PASS | 50,000 | 50,000 | 50,000 | 50,000 | 252.357 | 4 | 正式独立 Run |
| `t3-multi-50k-03` | PASS | 50,000 | 50,000 | 50,000 | 50,000 | 250.567 | 4 | 正式独立 Run |
| `t3-multi-50k-04` | PASS | 50,000 | 50,000 | 50,000 | 50,000 | 256.006 | 4 | 正式独立 Run |

三次修复后正式 Run 的 per-second loopback received rate：P50 约 768–775 PDU/s，P95 约 822–829 PDU/s，峰值约 852–874 PDU/s；这是 4 bind × 200 TPS 配置下的采样表现，不是新的产品 SLA。每个 Campaign 的 task、socket task、PDU、SMPP receive 均分别闭合，duplicate destination PDU 为 0，`pending_records=0`，projection lag 终点为 0。最大 aggregate RSS 约 0.95 GiB，最大 pending 约 16.4K，单 Run 目录约 55 MB。

独立 mixed multipart Run `multipart-mixed-7500`：5,500 tasks，Campaign 2/4 使用 multipart，socket PDU 与 response 各 11,000，projection events 5,500，`duplicate_destination_pdu_count=0`，`status=PASS`。这验证了 SMS task 数和 PDU part 数没有被错误地当作同一个计数。

因此：`T3_50K_MULTI_STREAMING=PASS`。

## T4：600 秒 steady 与独立 drain

T4 的两个高价值结果需要分开解释。T4-02 以约 800 task/PDU/s 持续供给，证明了 active producer、worker、bind、SMPP loopback 能保持重叠，但单 ProjectionWriter 的 journal replay 追不上该 offered rate；T4-03 将 offered rate 控制在已观测的 ProjectionWriter 可持续区间，完整通过 600 秒和 drain。两者都保留，不能用低速 Run 掩盖高压 drain 风险。

| Run | active 近似(s) | 总时长(s) | tasks / PDU / response | projection | drain(s) | max RSS | 结果 |
|---|---:|---:|---:|---:|---:|---:|---|
| `t4-steady-600s-01` | 未完成 | 546.160 | 181,000 / 181,000 / 181,000 | 89,047 | 0 | 2,109,300 KiB | FAIL：`rss_guard_exceeded`；修复前 reader cache RCA |
| `t4-steady-600s-02` | 600.292 | 971.363 | 492,000 / 492,000 / 492,000 | 155,166 | 371.071 | 1,396,876 KiB | FAIL：高压 projection drain 未闭合 |
| `t4-steady-600s-03` | 600.313 | 604.737 | 45,050 / 45,050 / 45,050 | 45,050 | 4.424 | 704,260 KiB | PASS |

T4-01 在约 206 秒采样点越过 2 GiB aggregate RSS guard；之后的 bounded stop 只清理本 Run PID，没有删除证据。缓存释放修复后，T4-02 的 RSS 峰值约 1.33 GiB，运行目录峰值约 0.50 GiB，说明该问题不是继续线性复制完整 spool index。T4-02 的 492K tasks 中，SMPP loopback receive/response 均完整；但在 active 结束后 371.071 秒，ProjectionWriter 只应用 155,166 events，最终受到 300 秒 bounded stop timeout 保护而退出，不能把 `projection_lag_seconds=0` 的末尾快照误读为“没有 journal backlog”。

T4-03 的 offered rate 约 P50 75.6、P95 86.9、峰值 96.6 PDU/s，producer/worker 同时运行 600 秒，停止供给后 4.424 秒排空，45,050 task、PDU、response 和 projection 全闭合，RSS/Run bytes 保持有界。

Gate 结论：

- `T4_600S_STEADY=PASS`：T4-03 提供完整 600 秒 active producer 证据。
- `T4_DRAIN=PASS`：在可持续 ProjectionWriter 区间的 T4-03。
- 高压 `T4_DRAIN=FAIL`：T4-02 是真实的容量/排空限制，不能隐藏。

## T6：服务级跨业务日、重启与 reclaim

`t6-daily-boundary-r2` 使用真实 `VNextRuntime`、spawned workers、单 ProjectionWriter 和注入 BusinessClock，未修改 OS clock。UTC 场景从 `2026-10-17 23:59:59` 跨到 `2026-10-18 00:00:00`，old campaign 在 cutoff 前暂停并保留 8 条，rollover 后 stop/restart，再恢复并写入 new campaign 8 条。

| 直接测量项 | 结果 |
|---|---:|
| `old_day_socket_after_cutoff_measured` | 0 |
| `old_day_redispatch_after_restart_measured` | 0 |
| `old_day_new_pdu_after_cutoff` | 0 |
| `old_day_socket_received` | 0 |
| `new_day_socket_received` | 8 |
| `new_day_stats_contamination` | 0 |
| old-day reclaim | 8 records / 3,400 bytes |
| business day | `2026-10-17 -> 2026-10-18` |

所有 `evidence_flags` 均为 true，projection rows 只出现 new-day campaign。`T6_BUSINESS_DAY_E2E=PASS`。

## T7：故障注入与覆盖边界

当前 `vnext_fault_injection.py` 已执行并保留：

| 子场景 | 结果 | 关键证据 |
|---|---|---|
| worker crash/restart | PASS | crash exit 17；5 条 crash journal + 15 条恢复；20 条唯一；pending 0 |
| ProjectionWriter pause/resume | PASS | pause blocked=true；20 条 applied；duplicates 0；queue 0 |
| campaign control pause/resume | PASS | pause 期间 0 writes；resume 后 5 writes |

以下必测场景在本轮没有对应的可复核执行器，统一标 `NOT_VERIFIED`，不扩展为 T7 PASS：feeder pause 30–60s 增量恢复、single-route unavailable with healthy-route isolation、IPC hint queue full、journal high-water/hard-limit、spool capacity exhaustion/recovery、跨日 Worker/Writer fault combination。故 `T7_STAGE_RECOVERY=NOT_VERIFIED`。

## Gate 矩阵

| Gate | 结论 | 依据 |
|---|---|---|
| `SOURCE_AND_ENV_IDENTITY` | PASS（配置差异单列） | source/harness hash、EulerOS/ext4、Python、DuckDB、freeze、Run root 完整 |
| `HARNESS_TRUSTWORTHY` | PASS | bounded feeder、1s samples、响应 drain、worker results、mixed multipart、真实失败 RCA |
| `T3_50K_MULTI_STREAMING` | PASS | 三次独立修复后 50K 全链路闭合 |
| `T4_600S_STEADY` | PASS | T4-03 active 600.313s，无 crash/guard |
| `T4_DRAIN` | PASS（可持续区间）；FAIL（高压区间） | T4-03 与 T4-02 分别提供两类证据 |
| `T6_BUSINESS_DAY_E2E` | PASS | 服务级 rollover/restart，old-day wire/re-dispatch 都是实测 0 |
| `T7_STAGE_RECOVERY` | NOT_VERIFIED | 已执行 3/9 类场景，完整矩阵缺项 |
| `SINGLE_WRITER_PROJECTION` | FAIL（高压 drain） | T4-02 单 Writer journal catch-up 不足；T3/T4-03 无重复 |
| `NATIVE_STABILITY` | PASS（已执行范围） | 无 native crash；T4-01 是 guard fail，T4-02 是 bounded timeout，不伪装成成功 |
| `PERFORMANCE_NON_REGRESSION` | NOT_VERIFIED | 没有同护栏、同负载、同 harness 的旧版长稳态 A/B |
| `CANARY_READY` | NO | 强制 Gate 未全部 PASS |

## 风险与下一步

1. 首要阻断项是 ProjectionWriter 在约 800 PDU/s offered rate 下的 journal replay/catch-up；需要在保持单 Writer、单 DuckDB、fsync 和 at-most-once 语义不变的前提下，补充批量大小、DuckDB SQL/commit、journal read 和 admission/backpressure 的可比较 A/B，再决定最小产品优化。
2. 需要补齐 T7 缺失的 feeder、route isolation、IPC queue、journal hard-limit、spool capacity 和跨日组合执行器，并按 `not_sent/unknown/partial/expired/written/response` 分类 crash window。
3. 若发布门禁要求 CPU/nice/ionice 护栏，需在相同 wrapper 下重跑正式 T3/T4；本轮只把 wrapper smoke 作为能力证据。
4. 仍未连接真实 SMSC、生产 API、生产数据库；EulerOS 结果不等同于 SLES12SP5 离线包验收。

本地完整回归：`409 passed, 5 skipped, 4 warnings`；新增/修改脚本 `py_compile` 通过。原始机器证据和所有失败样本均保留在远端 Run；本报告不包含 DB/WAL/Spool/Journal/venv 或秘密。
