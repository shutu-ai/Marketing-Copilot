# Marketing Copilot VNext 生产 Profile 与最终 Canary Gate 报告

任务：`MC-VNEXT-PRODUCTION-SIZING-SINGLE-WRITER-THROUGHPUT-CLOSURE-20261017`

最终决定：`NO_GO`。本地 correctness 和代码级恢复测试通过，但实际生产 profile 未冻结，约 800 PDU/s 的 steady/drain 门禁未在本任务授权的目标环境完成，不能授权部署或 canary。

## 1. 最终 gate 摘要

| Gate | 结果 | 证据/阻断 |
|---|---|---|
| R0 source/environment freeze | `PASS`（local）/ `BLOCKED_ENV`（target） | HEAD/tree、依赖、ext4、DuckDB 已冻结；实际 8/16 GiB、cgroup、CPU/I/O 未提供 |
| R1 current baseline | `PASS`（local measurement） | 4K、100/300/500K Writer cases；T3 baseline 完整闭合 |
| R2 Dedupe/Aggregate A/B | `NO_CHANGE` | digest 全相等；100K/10% 三次 median 仅 +1.247%，未达 +15% |
| R3 status/segment/offset | `PASS`（code/tests） | 不关闭 event fsync/offset durability；production candidate 未留存 |
| R4 resource profiles | `NOT_VERIFIED` | `PROD_8G`/`PROD_16G` 只有模型预算，未在实际目标运行 |
| R5 local stability | `PASS`（native backend tests） | `418 passed, 2 skipped, 6 warnings`；Windows targeted `13 passed` |
| R5 high-pressure T3 | `PASS` baseline / `FAIL` temporary candidate | baseline 50K 闭合；实验 candidate 18 write failures，已回滚 |
| R6 target 600s/800 PDU/s | `NOT_VERIFIED` | 无本任务新的授权目标服务器 run；本地 T3 实际 producer 约 218 PDU/s |
| T6 business-day | `PARTIAL` | benchmark 覆盖 2 business days；生产长稳 gate 未验证 |
| T7 9/9 | `NOT_VERIFIED` | 当前本地证据为 T7-4…T7-9 6/6，不签署 T7-1…T7-9 |
| Canary ready | `NO_GO` | R0/R4/R6/target authorization 阻断 |

## 2. Freeze card

```text
HEAD=f4ab33735e657f1734db13d90e223c38047a0f22
HEAD_TREE=82284ae4c1f6c7f8fec911594019582735b78b24
BRANCH=main
PRODUCTION_SOURCE=f4ab3373; no production Writer/runtime diff after rollback
LOCAL_PLATFORM=AlmaLinux-8 WSL2 x86_64
LOCAL_FS=/dev/sdd ext4, same filesystem for source/venv/DB/Journal/Spool
PYTHON=3.11.13
DUCKDB=1.2.0
LOCAL_AFFINITY=0-3
LOCAL_NICE_IONICE=nice 0 / ionice default
```

R0 JSON 与矩阵：

- `Docs/Marketing_Copilot_VNext_Production_Sizing_R0_Sizing.json`
- `Docs/Marketing_Copilot_VNext_Production_Sizing_R0_Environment.json`
- `Docs/Marketing_Copilot_VNext_Production_Sizing_Comparison_Matrix.md`

## 3. Profile 与 gate 语义

| Profile | 总 RAM | application-only 初始预算 | 结论 |
|---|---:|---:|---|
| `PROD_8G` | 8 GiB | 6 GiB | 模型存在，实际部署未确认 |
| `PROD_16G` | 16 GiB | 12 GiB | 模型存在，实际部署未确认 |
| `LOW_MEM_2G` | 2 GiB | stress guard | 仅用于受控压力/降级，不是生产 profile |

RSS guard 必须明确 `application-only` 或 `harness-inclusive`。本地 baseline T3 application-only peak 为 `716,884 KiB`，harness-inclusive peak 为 `1,219,948 KiB`；二者都不能替代实际部署 cgroup 的 peak。

## 4. 本地验证结果

### 4.1 Full tests

最终 source tree 在 native WSL ext4 执行：

```text
418 passed, 2 skipped, 6 warnings in 238.50s
```

Windows 隔离可写 pytest temp root 的 targeted 结果：`13 passed in 10.26s`。frontend 未被本轮修改，因此没有重跑 frontend build。源代码的关键 SHA 和依赖 freeze 在 R0 environment JSON 中记录。

### 4.2 Writer A/B

100K/10% duplicate 的 baseline/candidate 连续三次 digest 全相等，median throughput 为 `11,773.551` vs `11,920.401 events/s`，delta `+1.247%`；100K/0%、300K/10%、500K/30% 也均 digest equal，delta 分别为 `-2.876%`、`+0.688%`、`-0.023%`。因此不合入新的 dedupe/aggregate SQL。

### 4.3 Streaming T3

本地 baseline：

- 50,000 tasks / socket PDU / submit_sm_resp 全部闭合。
- 100,000 Journal events / 100,000 projection rows。
- duration `446.393s`，producer `229.181s`，SMPP drain `207.144s`，projection drain `208.577s`。
- max app RSS `716,884 KiB`，max harness RSS `1,219,948 KiB`，最终 backlog `0`。
- 生产阶段约 `218.2 PDU/s`，不是 800 PDU/s 证明。

临时 production candidate：

- 50,000 tasks accepted，但只有 49,982 PDU/response 和 99,982 durable events。
- 18 个 `write_failed`，原因均为 response window 未在 timeout 内释放。
- duration `601.522s`，SMPP drain `285.531s`，projection drain `287.083s`。
- candidate 已回滚，不进入最终源代码和 canary。

## 5. 目标环境与历史证据边界

本任务没有重新连接测试服务器；此前服务器信息和历史报告不自动构成本轮授权。历史 EulerOS evidence 仅用于风险对照：

- T3 50K：`projection_drain_timeout`，drain `742.003s`，最终不是 600 秒 gate PASS。
- T4：2 GiB guard 在 `331.293s` 达到 `2,099,128 KiB`，超过 `2,097,152 KiB`，steady 600 秒未完成。
- T7-4…T7-9：历史 6/6 PASS；T7-1…T7-3 未在本轮联合验证，因此不能宣称 9/9。

这些结果说明受限资源下 projection/Journaling catch-up 和 RSS 是真实风险，但不回答“8 GiB/16 GiB 目标 profile 是否通过”。

## 6. Release / Canary 阻断条件

在以下条件完成前，保持 `NO_GO`：

1. 提供并冻结实际机器总 RAM、container/cgroup limit、CPU quota/affinity、nice/ionice、磁盘/挂载和 DuckDB 版本。
2. 在授权目标环境用同一 source/config 重跑 T3 50K、T4 600s steady + bounded drain，并分别报告 application-only 与 harness-inclusive RSS。
3. 以真实 measured PDU/s、Journal events/s、committed projection events/s 和 backlog slope 判定，不以 route quota 参数代替实际供给。
4. 补齐 T7-1…T7-3 与 T7-4…T7-9 的同一版本联合证据，或明确将 gate 改名为 6/6 局部 recovery gate。
5. 通过两侧 digest、response event count、committed offset、Journal EOF 和 DB aggregate 对账后，才可讨论 canary。

## 7. Final end card

```text
BASELINE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22
CANDIDATE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22+working-tree measurement-only
RESOURCE_PROFILE=UNCONFIRMED
REAL_DEPLOYMENT_RAM_TOTAL_GIB=NOT_VERIFIED
REAL_DEPLOYMENT_VNEXT_BUDGET_GIB=PROD_8G 6 GiB initial / PROD_16G 12 GiB initial (unconfirmed)
CPU_QUOTA_OR_AFFINITY=local taskset 0-3; target unconfirmed
NICE_AND_IONICE=local nice 0/default
FS_AND_DUCKDB_VERSION=ext4 /dev/sdd; DuckDB 1.2.0
SOURCE_DIFF_SUMMARY=benchmark/harness only; production Writer/runtime rollback/no diff
RSS_APPLICATION_PEAK_GIB=baseline T3 0.684; temporary candidate 0.696
RSS_HARNESS_PEAK_GIB=baseline 1.163; temporary candidate 1.161
RSS_SLOPE_LAST_180S=negative for both local T3 runs
SOCKET_PDU_RATE_600S=NOT_VERIFIED; local baseline producer phase about 218 PDU/s
JOURNAL_EVENT_RATE_600S=NOT_VERIFIED
PROJECTION_COMMITTED_EVENT_RATE_600S=NOT_VERIFIED
JOURNAL_BACKLOG_BYTES_END=0 after local baseline T3 projection catchup; target profile NOT_VERIFIED
JOURNAL_BACKLOG_TREND_LAST_180S=negative/zero baseline; target NOT_VERIFIED
SMPP_DRAIN_SECONDS=baseline 207.144; temporary candidate 285.531
PROJECTION_DRAIN_SECONDS=baseline 208.577; temporary candidate 287.083
T3_50K_MULTI=local baseline PASS; temporary candidate FAIL, rolled back
T4_600S_800_PDU=NOT_VERIFIED
T6_BUSINESS_DAY=benchmark 2 business days; production gate NOT_VERIFIED
T7_9_OF_9=NOT_VERIFIED; local current matrix T7-4..T7-9 6/6
CORRECTNESS_DIGEST_MATCH=YES for pure Writer A/B at 0/10/30% duplicate and 100/300/500K
PERFORMANCE_AB_COMPARABLE=YES; no qualifying +15% improvement
PRODUCT_BOTTLENECK_RCA=staging then dedupe/commit/aggregate; candidate response-window failure
SINGLE_WRITER_CAPACITY_ESTIMATE=NOT_VERIFIED
CANDIDATE_READY=NOT_VERIFIED
RELEASE_DEPLOYMENT_AUTHORIZED=NO
UNVERIFIED_OR_BLOCKED_REASONS=target RAM/cgroup/CPU/I/O, 800 PDU/s gate and current-task server authorization absent
```

最终交付状态：本地报告、R0 sizing/environment JSON、comparison matrix 已写入 `Docs/`；未提交或推送，未触碰用户已有的其他 dirty/untracked 文件。
