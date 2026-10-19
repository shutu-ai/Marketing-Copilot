# Marketing Copilot VNext — Projection Staging Bulk Ingest Optimization & E2E Performance Closure

**Codex 任务 ID：** `MC-VNEXT-STAGING-BULK-INGEST-OPTIMIZATION-20261018`  
**目标仓库：** `https://github.com/shutu-ai/Marketing-Copilot`  
**冻结基线：** `origin/main` / `75aa2ddc58ede3b140d7538f02edf1b8f417832e`（执行时先再次核对）  
**优先级：** 性能优先、正确性不退步、最小代码改动  
**当前状态：** `CANARY=NO-GO`，本任务不是生产部署授权

## 0. 本次任务目的与严格边界

最新 EulerOS 实测显示：B3 四 Campaign、50,000 tasks、50,000 PDU/response、100,000 Journal/Projection events **全部正确对账**，但总 wall time **1,041.312 s**，独立 Projection drain **947.194 s**。`ProjectionWriter` B3 的 `staging_load_ns=1,005.957 s`，占 `batch_end_to_end_ns=1,025.360 s` 的 **约 98.1%**，一共 201 次 vectorized staging，平均每次约 5 s。这是本轮必须解决的单点性能瓶颈。

**唯一核心问题：** 当前 25 列 `LIST + list_zip + UNNEST` 参数化输入方式，或 staging 周边步骤，为什么在真实 EulerOS / DuckDB 1.2.0 进程中异常缓慢？可否在不改变业务正确性的情况下，换成显著更快的批量数据输入方式？

- 保留 **1 个 `ProjectionWriterProcess`、1 个 DuckDB 文件、1 个 DB owner**，不按 Campaign 拆库/拆 Writer。
- 不改变 Worker / SMPP 路由、TPS Credit、生产批次调度、Spool、Journal append 的逐事件持久化、offset 的可恢复边界、at-most-once、old-day cutoff、不自动补发 unknown/partial 等业务合同。
- 不关掉 `fsync`，不降低统计准确性，不绕过 `event_id` 去重，不把缺少 response events 的旧口径当作 PASS。
- 不把 2 GiB guard 当作 8–16 GiB 目标环境的生产容量标准；不以调高 guard、延长 drain 超时或仅减少日志来伪造吞吐改善。
- **不直接连接真实 SMSC、生产数据库、生产 API，不执行部署。** 当前任务**不自动授权**连接 EulerOS；先完成本地工作，只有得到单独授权并具备 VPN 后才允许进行远端 synthetic loopback 测试。
- 不修改其他脏文件。除非用户明确要求，否则**不 commit、不 push**。不要提交 Run、数据库、WAL、Journal、Spool、私密日志、凭据或依赖缓存。
- Repo 存在 `.codegraph/` 时，按 `AGENTS.md` 首先使用 CodeGraph。保持已有测试、报告及配置兼容。

## 1. 已知事实：避免重复低价值优化

来源：

- `Docs/Marketing_Copilot_VNext_E2E_Critical_Path_Profiling_and_RCA_Report.md`
- `Docs/Marketing_Copilot_VNext_E2E_Performance_Optimization_AB_Report.md`
- `Docs/Marketing_Copilot_VNext_Production_Profile_and_Canary_Gate_Closure_Report.md`
- `Docs/Marketing_Copilot_VNext_E2E_Performance_Evidence_Summary.json`

事实表：

| 项目 | 基线事实 | 本次解释 |
|---|---:|---|
| B3 tasks / PDU / response | 50,000 / 50,000 / 50,000 | 发送和响应已闭环 |
| B3 Journal / Projection | 100,000 / 100,000 events | 每条单段短信≈2 个持久事件 |
| B3 total / Projection drain | 1,041.312 / 947.194 s | 长尾由 Projection 引起 |
| B3 staging / batch E2E | 1,005.957 / 1,025.360 s | staging 约 98.1%，但需拆开其内部步骤 |
| B3 aggregate / DB commit | 7.706 / 3.336 s | 当前不是首要优化点 |
| B0 EulerOS pure Writer | 约 183–185 events/s | 与目标差距大，需要验证相同数据和环境 |
| 当前 `executemany` 替代实验 | 结果更慢，未推广 | **不得再把单纯回退 executemany 当成方案** |
| Profiling ON/OFF | 约 +0.657% wall | 可以复用低扰动 Profiler |
| 当前 server Profile | EulerOS 64 CPU、约 375 GiB RAM，A/B 用 `taskset 0-3`、应用 RSS 6 GiB guard | **不是**真实 8/16 GiB 机器的硬件验收 |
| 600s / ~800 PDU/s | NOT_VERIFIED | 不可宣称已达到 |

需核查的源文件：

- `backend/vnext/projection_writer.py`：`ProjectionWriter._event_row()`、`_apply_batch()`、`flush()`、Writer 进程 Journal 回放及 offset 持久化。
- `backend/vnext/critical_path_profiler.py`：复用已有低频、低扰动 profiler。
- `backend/tools/vnext_single_writer_benchmark.py`：既有 microbenchmark；必须升级为**同生产阶段行为的 staging 专项 A/B**。
- `backend/tools/vnext_streaming_acceptance.py`：端到端 B1/B3/B4/B5/steady/drain，按 task、PDU、response、Journal events 独立对账。
- `backend/tests/test_vnext_projection_writer.py`、`backend/tests/test_vnext_critical_path_profiler.py`：回归与新测试。

## 2. R0 — 源码身份、环境及基线冻结

1. 记录本地 `HEAD`、`origin/main`、Git tree、相关文件 hash、dirty/untracked，禁止覆盖用户已有修改。若当前 HEAD 已超出 `75aa2ddc`，先审阅差异，以当前正确代码为对照；报告中明确新旧身份。
2. 记录 Python 3.11、DuckDB 1.2.0、`pandas` / `pyarrow` 是否已有离线 wheel、实际版本及 DuckDB 1.2.0 API 兼容性。**不要求强行安装额外依赖**。
3. 在 native ext4 新的隔离 Run 内测量；保存 `df -T`、CPU affinity、nice、ionice、内存、cgroup（若可读）、进程资源与历史受限环境差异。
4. 为公平 A/B，冻结同一组数据生成器、字段类型、分片比例、duplicate 率、4 Campaign、两业务日；将 baseline 及候选随机或交错排序运行，隔离 DB/临时目录。
5. 做一次未更改产品代码的 baseline；不仅记录 `staging_load_ns`，也记录真实 **阶段 wall、事件吞吐、DB 行数/digest、资源峰值、Journal 末尾 backlog**。

**R0 Gate：** 身份、数据、版本、测试资源可复现；缺少目标 EulerOS 授权不妨碍本地实验，但正式目标 Gate 记 `BLOCKED_ENV`，不虚报。

## 3. R1 — 先把 staging 的 98.1% 拆开

原 `_apply_batch()` 的 `staging_started_ns` 位于重复 staging 表操作之前，因此 **staging_load 不等于单个 `UNNEST` SQL 的时间**。以可关闭的低扰动计数器新增如下分段（使用 `perf_counter_ns`，聚合计数，不逐事件落盘）：

1. `staging_prepare_rows_ns`：25 列 `_event_row` 编码、字符串/数字映射。
2. `staging_clear_table_ns`：`DELETE FROM _vnext_projection_batch`、`DROP TABLE IF EXISTS _vnext_projection_new` 等。
3. `staging_transpose_ns`：`zip(*rows)` 与 25 个 Python 列表的构建。
4. `staging_bind_execute_ns`：参数转换 + `LIST/list_zip/UNNEST` 执行；进一步拆成参数构造与 DuckDB execute 如果能做到且不明显干扰。
5. `staging_relation_create_ns` / `staging_relation_insert_ns`：供 Pandas/Arrow 等候选测试；注意真实表重用成本。
6. `staging_total_ns` 与现有 `staging_load_ns` 差异校验；计算每 batch ms、每 event μs，P50/P95、累计与事件数量。

要点：

- 计时不允许改变事务边界、现有 SQL 语义或插入顺序。
- 用**每一步的绝对耗时及占比**判定瓶颈，不要用“list_zip 可能慢”代替实测。
- 记录 Python CPU/进程 RSS、DuckDB 1.2.0 的执行表现，必要时使用仅在诊断 Run 开启的 DuckDB profiling；避免增加正常运行开销。
- 对同一批次 256、512、1024、2048 events 做至少一个快速 sweep；观察是否有固定每批成本或随 batch size 非线性上升。
- 对 EulerOS **201 次 staging / 100K events** 的结果进行复核，分析 5 s/批来自 SQL 绑定、数据库执行、表维护，还是环境调度。禁止只用 WSL 的 11K events/s 数据推翻服务器数据。

**R1 Gate：** `STAGING_INTERNAL_RCA=MEASURED`，每个耗时统计有口径，Profiler ON/OFF 干扰可接受；否则暂不改生产 ingest 实现。

## 4. R2 — 独立批量导入候选实验（不要先修改生产路径）

实现一个**最小、独立的 staging adapter**，只负责将同一 `list[dict]` / 25 列行写入 `_vnext_projection_batch`，在单 Writer DB connection 和**现有相同事务内**执行，不额外开启 DB owner。初始候选仅在 benchmark 中启用，生产默认保持不变。

| Arm | 实验设计 | 注意事项 |
|---|---|---|
| `A0` | 当前 `LIST + list_zip + UNNEST`，staging reuse | 真正基线，保持原参数 / 类型 |
| `A1` | Pandas DataFrame 形成批量关系、DuckDB `INSERT ... SELECT` | 检查 25 列 dtype、nullable、Arrow/NumPy 转换开销；只有已有兼容依赖才尝试 |
| `A2` | PyArrow Table/RecordBatch，经 DuckDB relation 注册/`INSERT ... SELECT` | 优先检验列式输入；依赖不可用时明确 SKIPPED，不在线乱装 |
| `A3` | JSONL/NDJSON 单批读取作为备选 | 只允许在可隔离 native ext4 临时文件中实验，**必须把文件写入/fsync/读取/删除计入总成本**，不能拿纯扫描时间对比 A0 |
| `A4` | 保留同当前数据输入，仅调整 staging 表清理/复用顺序 | 用以隔离 metadata/DDL/DELETE 成本，不改变关键事务语义 |

要求：

- DuckDB API 及 DuckDB 1.2.0 语义先做小 smoke；不保证每项都支持，**失败或安装负担高的方案可有证据地跳过**。
- `event_id`、`business_day`、`campaign_id`、`partition_id`、`route_id`、`ordinal`、`parts_*`、`state`、`message/error`、计数等 **25 列原值/类型/精度** 一致。
- 非 ASCII 中文、Emoji、空字符串、NULL/None（在现有规范下）、超长但合法 message、负值/边界、重复 event、同号码不同 Campaign、两业务日、多分片 response 都纳入样本。
- Python↔DuckDB 的类型转换与临时 relation 注册必须在当前连接/事务内；不能以丢数据、预聚合、省略 event rows 换速度。
- 保留 `vnext_progress_events.event_id` 幂等及聚合 `ON CONFLICT` 语义。不得先跳过去重后再在 SQL 外部对账。
- 所有 A/B 用**相同数据、预热条件、真实 DB 文件、相同落库语义**，记录完整 wall 与 staging breakdown；不将纯生成列、纯 SQL 耗时误称为生产 events/s。

### 分阶段筛选以避免大量无效测试

**R2a 快速筛选：** 11K events × batch 256/512/1024。先跑 A0、A1、A2、A4；A3 仅当内存关系方案未见收益且环境支持时运行。任何 correctness digest 不同直接淘汰。

**R2b 放大验证：** 只让表现最好的 **至多 2 个**候选参加 100K events、4 Campaign、2 events/SMS、10% duplicates 与 500K/30% duplicates 的对照；每个候选与 A0 进行三轮同环境 paired A/B（至少 100K 核心负载三轮，500K 可先一轮筛查）。

**进入端到端的门槛：** 不能只要求微基准 +15%；优先选定有**明显 staging 改善（建议 ≥2×）**、整体投影端到端明显改善、正确性 100% 对账、没有显著 RSS/依赖恶化的方案。若无候选明显胜出，停止优化并报告 `NO_CHANGE`；不要浪费时间跑一遍 600 秒高压测试。

**R2 Gate：** `STAGING_CANDIDATE=SELECTED` 或 `NO_CHANGE_WITH_EVIDENCE`，附支持/缺失依赖、DuckDB 版本及性能/内存成本。

## 5. R3 — 最小生产接入（仅限候选胜出后）

- 在 `ProjectionWriter` 增加**最小化可切换 ingest mode**，只替换 `_apply_batch()` 中的 staging 输入分支；下游 dedupe、event insert、aggregate、commit、offset、GC 和 Worker 合同一律不动。
- 保留经过验证的旧 A0 为安全回退选项。配置暴露尽可能少，只保留必要的 `staging_mode` 和与运行时兼容的默认值；不要另造插件框架。
- 如果 Arrow/Pandas 是候选，评估离线打包及 SLES12SP5 `manylinux2014_x86_64` 的 wheel 可用性；如果依赖无法可靠部署，不应作为生产默认，即使微基准更快。
- 任何一轮出现 `write_failed`、response 丢失、unknown 异常或 Projection 内容/计数不等，立即撤销 candidate 的生产推广，保留独立实验代码及 RCA。

**R3 Gate：** `PRODUCT_CANDIDATE_CORRECTNESS=PASS` 才允许进入 R4；否则 `ROLLED_BACK`。

## 6. R4 — 端到端验证：先 50K，再 600 秒

### R4a：同一环境 50K paired E2E

- 配置对照：8 Workers、4 Bind、per-bind 200 `submit_sm` PDU/s、window 50、4 Campaign `(20K,20K,5K,5K)`、每条 1 PDU，Projection batch 512 为默认对照；在同一 ext4、同一 CPU/IO 护栏下比较 A0 与胜出候选。
- 至少 **3 组 paired 50K**；每组完整统计独立阶段：Producer accepted → Worker taken → Socket PDU written → loopback received → `submit_sm_resp` → durable response Journal → DuckDB commit → offset durable → final drain。
- 对账：50K tasks / 50K PDU / 50K responses / 100K 事件（全部是正常单段成功场景）；每 Campaign exact identity、no duplicate destination PDU、no unexpected unknown、Journal committed EOF、DB unique event rows 与 aggregate 一致。
- 对比 `staging_load_ns`、Writer applied events/s、Journal backlog 绝对值与斜率、Projection drain、总 wall、RSS application-only / harness-inclusive、CPU；Profiling ON/OFF 策略一致。
- 吞吐改善应来自实际 staging / Projection 的加速，不得只把发送提早结束而让 backlog 增长。

### R4b：Multipart + 恢复正确性

- 维持 multipart 独立 case：按实际 parts/PDU 计算 response events，绝不假定 task=PDU。
- 进行 Writer crash/restart、DB COMMIT 后 offset 未持久化的幂等 replay、Journal segment GC、响应晚到/unknown、硬限额、跨午夜 old-day cutoff；保留既有 T6/T7 语义。
- 完整 backend pytest 及项目既有可执行定向验证，失败不得以性能收益掩盖。

### R4c：600s / ~800 PDU/s（只在 R4a/B 完成后）

- 在**明确获得目标服务器授权**后使用 EulerOS 原生 ext4 + synthetic 127.0.0.1 loopback；无需且不允许真实 SMSC。
- 匹配实际部署 CPU/IO 条件；不要把 `taskset 0-3 + nice 10 + ionice -c3` 的受限服务器结果直接视为真实 8/16 GiB 生产规格结论。若保留 4 核，则该 gate 应明确是 4 核容量。
- `PROD_8G` 初始 **application RSS ≤6 GiB**；`PROD_16G` 初始 **≤12 GiB**，需要与实测总 RAM/cgroup/共享服务预留一起确认；2 GiB 仅用于压力降级 smoke。
- 持续 600 秒真实、不断供给的约 800 PDU/s 负载，记录**实际**平均/P50/P95 PDU/s；生产配置 `4 × 200 TPS` 仅是总上限，不是达成数据。可将 760 PDU/s（95% 理论目标）作为候选的最低实测近似门槛，报告必须同时列真实数字，不得宣称“实测 800”除非达到。
- 对正常单段短信，约 800 PDU/s 对应**至少约 1,600 持久事件/s**（发送与响应）；按真实 Journal 事件量计算目标，不以 message 数代替事件数。
- 独立测量 600s active 期间 Projection 可持续处理率、backlog trend、最大 backlog、应用 RSS 增长率与最后停止供给后的 complete drain。理想目标为 **≤60 秒** 排空；若达不到，要明确容量边界和原因，不能自动记 PASS。
- 只有 Journal committed offsets 追到真实 EOF、`projection_events_applied`/DB rows/aggregate 完整一致、响应数量闭合，才可判断 drain 完成。`projection_lag_seconds=0` 或 `spool pending=0` 不足以签署。
- 16 GiB profile 没有实际环境时，可单独保留 `PROD_16G=NOT_VERIFIED`；**不要把 16 GiB 尚未验证误写成已通过，也不要自动否决已独立证明通过的 8 GiB 规格**。

## 7. 回退、停止和失败归因规则

立即停止候选推广并回退生产默认：

1. 任意 event digest、DB aggregate、response、跨天或 replay 结果不一致。
2. 提升 staging 却导致端到端无改善，或变慢超过测量噪声。
3. worker/socket 响应窗口出现候选引起的异常超时、unknown、漏记、重复；不把真实故障隐藏在长 drain 中。
4. 引入的 pandas/pyarrow/native wheel 无法在部署目标可靠安装、运行，或内存明显超出生产 guard。
5. 需要引入多个 DuckDB writer、改变 Journal durability、修改上游 at-most-once 语义才能“提升”。此类方案必须另立架构提案，**不属于本任务**。

允许诚实输出：`PERF_NO_CHANGE`、`BLOCKED_ENV`、`CORRECTNESS_FAIL`、`NO_GO`。不得为完成任务而循环增加复杂度。

## 8. 最终交付（保存到 Docs/；不提交原始 Run 文件）

- `Docs/Marketing_Copilot_VNext_Staging_Internal_Cost_RCA_Report.md`
  - 98.1% staging 的内部拆解；各路径 SQL/转换/临时表耗时；环境差异和数据口径。
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_Adapter_AB_Report.md`
  - A0–A4 支持状态、batch/数据规模、三轮 paired wall、events/s、digest、RSS、依赖与胜出判定。
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_E2E_and_Canary_Gate_Report.md`
  - baseline/candidate T3、multipart、T6/T7、600s steady/drain 与 profile 状态。
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_Evidence_Summary.json`
  - freeze identity、source SHA、workload config、timing counters、memory/CPU、passed/failed/not-verified、raw evidence **路径**与核验摘要；不得包含用户号码、密钥或运行时原始数据。
- 必要的最小代码与测试变更；若候选不胜出或正确性失败，保留基线产品实现并仅保留有价值的 benchmark/profiling 改动。

要求报告写明以下 Gate：

```text
SOURCE_AND_ENV_IDENTITY=PASS|FAIL|BLOCKED_ENV
STAGING_INTERNAL_RCA=PASS|NOT_VERIFIED
A0_A4_CORRECTNESS=PASS|FAIL|NOT_VERIFIED
STAGING_AB_BENEFIT=PASS|NO_CHANGE|NOT_VERIFIED
PRODUCT_CANDIDATE_CORRECTNESS=PASS|ROLLED_BACK|NOT_APPLICABLE
T3_50K_PAIRED=PASS|FAIL|NOT_VERIFIED
MULTIPART_AND_RECOVERY=PASS|FAIL|NOT_VERIFIED
EULEROS_600S_800PDU=PASS|FAIL|NOT_VERIFIED|BLOCKED_ENV
PROD_8G_CAPACITY=PASS|FAIL|NOT_VERIFIED
PROD_16G_CAPACITY=PASS|FAIL|NOT_VERIFIED
SINGLE_WRITER_CAPACITY_ESTIMATE=MEASURED|NOT_VERIFIED
CANARY_READY=YES|NO
```

`CANARY_READY=YES` 只能在当前候选源码已验证关键正确性、符合被声明的**实际部署规格**、完成目标吞吐 + steady + drain 等强制 gate 后签署。本任务不授予部署权限。

## 9. Codex 执行策略（效率优先）

1. **先读证据和源代码，再实测 staging 内部耗时，最后写候选。** 不要提前扩散修改至 SMPP/Spool。
2. **先小样本筛选，最多两个候选放大。** 发现明显不如 A0 或 digest 错误就停止该候选。
3. 对外部条件缺失不循环等待、不擅自连接：明确 `BLOCKED_ENV`，尽可能完成本地 A/B 与报告。
4. **只提交用户明确授权的文件。** 本任务完成时默认在工作区留下报告和验证结果，不执行 `git commit` / `git push`。
5. 最终汇报用最简结论：**方案有没有真正变快？快了多少？数据是否完全正确？有没有增加复杂度和依赖？当前是否真的能满足目标规格？**

---

**验收哲学：** 优先高效、可靠、可运行。允许 at-most-once 场景中已定义的未发送/unknown，绝不能以重复发送或跨日补发换吞吐；**不要为了额外安全抽象牺牲大量性能，也不要用微基准成绩代替实际持续运行能力。**
