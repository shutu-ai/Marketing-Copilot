# Marketing Copilot VNext 单 Writer Dedupe/Aggregate 优化 A/B 报告

任务：`MC-VNEXT-PRODUCTION-SIZING-SINGLE-WRITER-THROUGHPUT-CLOSURE-20261017`

结论：`NO_CHANGE`。现有 `f4ab3373` 的 A3（vectorized + staging reuse）路径保留。新一轮没有发现满足“同一环境连续三次 median 提升至少 15%，且无正确性/内存回归”的新 SQL 优化；所有 A/B digest 一致，但大样本增益只有噪声范围。临时 production hot-path 实验在 streaming T3 出现 18 次 response-window timeout，已回滚，不作为候选发布。

## 1. 不变量与候选定义

所有 case 保持一个 `ProjectionWriterProcess`、一个 DuckDB、相同 25 列 event payload、相同 event-id 去重、同一事务内 event insert + aggregate upsert + offset durability。四种实现为：

| Label | Dedupe/insert path | Staging table | 目的 |
|---|---|---|---|
| A0 | `executemany` | 每批 create/drop | 最保守基线 |
| A1 | `executemany` | reuse | 分离 staging 生命周期成本 |
| A2 | vectorized `LIST + list_zip + UNNEST` | 每批 create/drop | 分离 SQL 参数化成本 |
| A3 | vectorized | reuse | 当前生产候选/基线 |

本轮只对 benchmark/harness 做测量扩展：campaign、response event、business day、state count、daily summary 和 app-only/harness-inclusive RSS 均进入结果；没有把临时 production Writer 优化留在最终源代码中。

## 2. 测试矩阵

- 文件系统：`/dev/sdd ext4`，源、venv、数据库和临时 Run 同一 native ext4。
- Python：`3.11.13`；DuckDB：`1.2.0`。
- 4K sweep：10% deterministic duplicate，batch `256/512/1024/2048`，A0/A3，所有 digest 对等。
- 100K/300K/500K：4 campaigns、2 events/SMS、2 business days，batch `1024`，A3 baseline/candidate；duplicate `0%/10%/30%`。
- candidate 与 baseline 使用同一 harness、同一数据生成器和同一 ext4 envelope；production candidate 只在临时 source tree 运行。

## 3. 4K A0/A3 sweep

下表为 events/s；candidate 相对 baseline 的变化不是 production SQL 变更的证明，只用于确认测量链路和结果 digest。

| batch | baseline A3 | candidate A3 | delta | digest/correctness |
|---:|---:|---:|---:|---|
| 256 | 6,679.348 | 6,102.615 | -8.63% | equal / PASS |
| 512 | 10,719.841 | 10,692.937 | -0.25% | equal / PASS |
| 1024 | 13,945.533 | 14,772.956 | +5.93% | equal / PASS |
| 2048 | 16,377.101 | 16,869.286 | +3.01% | equal / PASS |

A0/A3 的 digest 在四个 batch 上均相同。4K 的单次波动不足以支持合入新 SQL。

## 4. 大样本 A/B

### 4.1 100K、10% duplicate：连续三次

| Run | baseline eps | candidate eps | baseline wall | candidate wall | delta eps | unique rows |
|---|---:|---:|---:|---:|---:|---:|
| r1 | 12,306.034 | 12,281.510 | 8.126095s | 8.142321s | -0.199% | 90,002 |
| r2 | 11,773.551 | 11,438.729 | 8.493614s | 8.742230s | -2.844% | 90,002 |
| r3 | 11,602.790 | 11,920.401 | 8.618617s | 8.388979s | +2.737% | 90,002 |
| median | 11,773.551 | 11,920.401 | — | — | **+1.247%** | 90,002 |

三次 digest 均相等，state counts 也相等：`socket_written=45,001`、`submit_sm_resp_success=45,001`。median 增益远小于 +15% 门槛。

### 4.2 规模与重复率扩展

| events | duplicate | baseline eps / wall | candidate eps / wall | delta | unique rows | digest |
|---:|---:|---:|---:|---:|---:|---|
| 100,000 | 0% | 11,227.931 / 8.906361s | 10,904.895 / 9.170193s | -2.876% | 100,000 | equal |
| 300,000 | 10% | 10,365.834 / 28.941230s | 10,437.100 / 28.743616s | +0.688% | 270,002 | equal |
| 500,000 | 30% | 11,393.527 / 43.884569s | 11,390.947 / 43.894506s | -0.023% | 333,334 | equal |

`result_sha256` 对每一组 baseline/candidate 相等；没有 duplicate、response event、跨 campaign 或跨 business day 的 correctness regression。

### 4.3 Baseline A3 stage timing

| events | rows prepare | staging | dedupe | events insert | aggregate | DB commit | batch E2E |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 0.217s | 3.750s | 1.403s | 0.536s | 0.634s | 1.067s | 7.496s |
| 300,000 | 0.667s | 11.692s | 7.660s | 1.722s | 1.981s | 3.592s | 26.984s |
| 500,000 | 0.883s | 15.637s | 13.562s | 2.496s | 3.313s | 5.048s | 40.609s |

Stage 数据支持后续沿 staging、dedupe、commit、aggregate 做定点分析；它不支持只凭 SQL calls 或单个 microbench 数字宣称 800 PDU/s。

## 5. 临时 production candidate 的 A/B 边界

临时 source tree 曾加入 segment path cache 和低频 status/diagnostic 写入，以观察 production 高压阶段是否可减少额外 I/O。结果：

- baseline T3：`PASS`，50,000 tasks/PDU/response，100,000 Journal/projection events，app RSS peak `716,884 KiB`，projection drain `208.577s`。
- temporary candidate T3：`FAIL`，50,000 accepted 但 49,982 PDU/response，99,982 Journal/projection events，18 个 `write_failed`，错误为 response window 在 timeout 内未释放；projection drain `287.083s`。
- 因为 response durability/窗口释放优先于实验收益，candidate 已回滚；最终 production `projection_writer.py`、`runtime.py` 与 baseline 一致。

这项实验不是 A0–A3 SQL candidate 的通过结果，而是“不得合入”的 production hot-path 证据。

## 6. 决策

| Gate | 结果 | 说明 |
|---|---|---|
| A0–A3 correctness | `PASS` | digest、unique rows、projection、state/daily summary 一致 |
| 连续三次 median +15% | `FAIL` | 100K/10% median 仅 +1.247% |
| 100/300/500K scalability | `PASS`（测量） | 0/10/30% duplicate 均无 digest regression |
| memory regression | `NO_REGRESSION`（纯 Writer） | 两侧同一 local ext4；production candidate 另有 response-window failure |
| 新 SQL merge | `NO_CHANGE` | 不满足合入门槛 |
| production canary | `NO_GO` | target profile、800 PDU/s 和授权服务器未验证 |

最终保留 `f4ab3373` 的 A3 路径。任何后续优化需继续提供连续三次 paired median、完整 digest、应用 RSS/斜率、Journal backlog 和独立 drain；不得以 harness-inclusive RSS 替代 application-only RSS。

## 7. 原始证据路径

主要 Run：

- 4K：`/tmp/mc-production-sizing-20261017-f4ab-baseline/run-4k-ab/result.json` 与 `/tmp/mc-production-sizing-20261017-candidate/run-4k-ab/result.json`
- 100K/10% r1–r3：各 baseline/candidate `run-100k-10-r1`、`run-100k-10-r2`、`run-100k-10-r3`
- 100K/0%：`run-100k-0`
- 300K/10%：`run-300k-10`
- 500K/30%：`run-500k-30`
- streaming T3：两套根目录下的 `run-t3-50k/result.json`

这些路径位于受控 `/tmp` native ext4；报告不把数据库、Journal 或临时 Run 纳入版本控制。

## 8. End card

```text
BASELINE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22
CANDIDATE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22+working-tree measurement-only
RESOURCE_PROFILE=UNCONFIRMED
REAL_DEPLOYMENT_RAM_TOTAL_GIB=NOT_VERIFIED
REAL_DEPLOYMENT_VNEXT_BUDGET_GIB=PROD_8G 6 GiB initial / PROD_16G 12 GiB initial (unconfirmed)
CPU_QUOTA_OR_AFFINITY=local taskset 0-3; target unconfirmed
NICE_AND_IONICE=local nice 0/default
FS_AND_DUCKDB_VERSION=ext4 /dev/sdd; DuckDB 1.2.0
SOURCE_DIFF_SUMMARY=benchmark/harness measurement changes; production Writer/runtime unchanged
RSS_APPLICATION_PEAK_GIB=baseline T3 0.684; temporary candidate 0.696
RSS_HARNESS_PEAK_GIB=baseline 1.163; temporary candidate 1.161
RSS_SLOPE_LAST_180S=negative for both local T3 runs
SOCKET_PDU_RATE_600S=NOT_VERIFIED; local baseline producer phase about 218 PDU/s
JOURNAL_EVENT_RATE_600S=NOT_VERIFIED
PROJECTION_COMMITTED_EVENT_RATE_600S=NOT_VERIFIED
JOURNAL_BACKLOG_BYTES_END=0 baseline local T3; target profile NOT_VERIFIED
JOURNAL_BACKLOG_TREND_LAST_180S=negative/zero baseline; target NOT_VERIFIED
SMPP_DRAIN_SECONDS=baseline 207.144; temporary candidate 285.531
PROJECTION_DRAIN_SECONDS=baseline 208.577; temporary candidate 287.083
T3_50K_MULTI=baseline PASS; temporary candidate FAIL and rolled back
T4_600S_800_PDU=NOT_VERIFIED
T6_BUSINESS_DAY=benchmark 2-day coverage; production gate NOT_VERIFIED
T7_9_OF_9=NOT_VERIFIED; local current matrix T7-4..T7-9 only
CORRECTNESS_DIGEST_MATCH=YES for A/B 100/300/500K at 0/10/30% duplicate
PERFORMANCE_AB_COMPARABLE=YES; no qualifying +15% improvement
PRODUCT_BOTTLENECK_RCA=staging, dedupe, commit and aggregate; hot-path experiment hit response-window timeout
SINGLE_WRITER_CAPACITY_ESTIMATE=NOT_VERIFIED
CANDIDATE_READY=NOT_VERIFIED
RELEASE_DEPLOYMENT_AUTHORIZED=NO
UNVERIFIED_OR_BLOCKED_REASONS=actual target RAM/cgroup/CPU/I/O and authorized target-server high-pressure run absent
```
