# Marketing Copilot VNext — A0/A2 × Batch Sizing, EulerOS Discrepancy & E2E Throughput Closure

**Codex 任务 ID**：`MC-VNEXT-A0-A2-BATCH-EULEROS-E2E-CLOSURE-20261018`  
**类型**：有条件实施的性能优化与验收任务；不是架构重构/生产部署指令  
**基线**：`origin/main = cd8e63537d264a623d347735f38c946caada4f75`（执行时先核对；如有后续提交，以当时 HEAD 为准并记录差异）  
**当前发布结论**：`CANARY_READY=NO` / `NO_GO`；本任务不得提前更改为 GO。  
**重点**：先验证简单的 A0 扩大批次能否解决问题，再评估 PyArrow A2 是否有超出额外依赖和复杂度的真实收益；必须解释 WSL 与 EulerOS 的巨大性能差异。

---

## 0. 执行原则：先理解后改动

在当前仓库执行。优先读取 `AGENTS.md`，遵守其中 CodeGraph、环境、保留 dirty/untracked 等规则。检查 `git status --short`、`HEAD`、`origin/main` 和既有 `Docs/` 证据。

**不变约束**：

1. 保留 **一个 `ProjectionWriterProcess`、一个 DuckDB 文件、一个 DB owner**。不要分 Campaign Writer、分库、扩展 RBAC、审批流或进行大范围重构。
2. 保留 `ProgressJournal` durable append / fsync、Journal segment 与 offset 安全回收、DuckDB 同事务 `event_id` 幂等去重和汇总、重启重放正确性。
3. **不能为了性能而自动重发可能已写入 socket 的 PDU**；`unknown` 语义不变。重复发送风险高于少量意外未发送。跨业务日/午夜后旧任务必须继续禁止分发。
4. 保留 SMPP `submit_sm_resp` reader/drain 正确性；不能用减少持久化事件、跳过完整 Projection、改掉 timeout 或禁用 fsync 来制造虚假吞吐提升。
5. 不改变生产默认 ingest：开始时 `vectorized LIST + list_zip + UNNEST (A0)`，默认 batch 512。**只有在候选正确性和真实端到端 A/B 符合条件后才可考虑修改默认值，且必须单独说明。**
6. 不把 benchmark 的独立 `BenchmarkProjectionWriter` 当作生产已支持的 A2；当前 A2 存在于 `backend/tools/vnext_single_writer_benchmark.py`，不是生产 `ProjectionWriter` 的内建模式。
7. 不连接真实 SMSC、生产 API/DB、真实收件人；仅使用 synthetic loopback。未获得当前任务明确的目标服务器授权及 VPN/访问前，不连接 EulerOS；把相关门禁标记为 `BLOCKED_ENV`，完成本地工作并提交可复现命令与缺口，**不得声称服务器实测通过**。
8. 不执行部署；不擅自 `git commit` / `git push`。保留原有 dirty、untracked、`AGENTS.md`、前端锁文件和运行数据，不删除/覆盖现有 Run；产物不得包含凭据、号码列表、数据库、日志或缓存。

## 1. 两批测试的合并事实（不要重复发现已知信息）

资料：

- [`9c46b5c3` 第一批源码/报告](https://github.com/shutu-ai/Marketing-Copilot/commit/9c46b5c396029134cec1f57a46829007b4a0e5bf)：`STAGING_INTERNAL_RCA=PASS`，A0 `staging_bind_execute` 占 staging 约 83.3%–90.1%；A1/Pandas 于 batch 512 的 100K 三轮整体改善约 1.7%，A2 因缺 PyArrow 未执行；生产默认未切换。
- [`cd8e6353` 第二批补齐 A2 证据](https://github.com/shutu-ai/Marketing-Copilot/commit/cd8e63537d264a623d347735f38c946caada4f75)：临时 WSL native ext4 环境 `pyarrow==12.0.1`、`numpy==1.26.4`、`duckdb==1.2.0`，A2 11K/100K/500K 完整 digest/schema 等价，依赖未加入生产。
- [`75aa2ddc` 的 EulerOS E2E RCA](https://github.com/shutu-ai/Marketing-Copilot/commit/75aa2ddc58ede3b140d7538f02edf1b8f417832e)：B3 50K SMS / 100K events，全链路约 1041.312s；staging load ≈1005.957s / 1025.360s 批处理耗时，约 **98.1%**；201 次向量化 staging；Projection drain 947.194s；结果最终正确但不满足高吞吐目标。

### 第二批真实结果：A0 vs A2（数值为 elapsed wall，不是 send throughput）

| workload | batch | A0 total wall | A2 total wall | A2 total wall 改善 | A2 staging 改善 | 统计强度 |
|---|---:|---:|---:|---:|---:|---|
| 100K events, ~10% duplicates | 512 | 12.191240s | 9.587405s | **21.4%** | 48.7% | 三轮中位数 |
| 100K events, ~10% duplicates | 1024 | 8.600536s | 5.849494s | **32.0%** | 63.8% | 三轮中位数 |
| 500K events, 30% duplicates | 512 | 72.029992s | 69.588518s | **3.4%** | 30.5% | 仅一轮 |
| 500K events, 30% duplicates | 1024 | 50.036760s | 40.902258s | **18.3%** | 53.3% | 仅一轮 |

**重要推论，需要正式实验确认**：

- `A0/1024` 在相同 100K/500K 配置下已明显优于 `A0/512`；**优先验证“不新增依赖，只扩大 batch”的低复杂度方案**。
- `A2/512` 的收益随着 500K/30% duplicate 负载明显缩水；`A2/1024` 仍有改进，但 500K 仅一轮，不能作为真实生产结论。
- 100K 与 500K 数据负载、重复率不同，不能把增益变化直接归因为“规模导致 Arrow 退化”；必须拆分 `events / duplicates / batch fill / DB growth / stage` 影响。
- WSL 上 100K 的 staging 是秒级，而 EulerOS B3 100K 的 staging 约 1006s，两者 **不是同源码同配置同数据集的一对 A/B**，不可直接写成 200 倍性能回退；应进行同源、同配置、同负载验证。
- 过去 P5500 的 socket-written、response、Journal durable 速率显著快于 Projection，但即使 A2 微基准显著加速，也仍未证实 `800 PDU/s × 600s` 的完整链路能力。

参考文件：

- `Docs/Marketing_Copilot_VNext_Staging_Internal_Cost_RCA_Report.md`
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_Adapter_AB_Report.md`
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_E2E_and_Canary_Gate_Report.md`
- `Docs/Marketing_Copilot_VNext_Bulk_Staging_Evidence_Summary.json`
- `Docs/Marketing_Copilot_VNext_E2E_Critical_Path_Profiling_and_RCA_Report.md`
- `backend/vnext/projection_writer.py` (`_clear_staging`, `_stage_rows`, `_apply_batch`, `_process_main`, `ProjectionWriterProcess`)
- `backend/tools/vnext_single_writer_benchmark.py` (`BenchmarkProjectionWriter`, `_stage_pyarrow`, A0/A2)
- `backend/tools/vnext_streaming_acceptance.py`；`backend/vnext/runtime.py`。

---

## 2. R0 — 冻结环境、指标口径和测试设计（必做）

1. 冻结 HEAD、tree、工作区既有改动、Python/DuckDB/NumPy/Pandas/PyArrow 版本、 wheel 来源、Linux 内核、ext4、mount、CPU affinity、CPU quota、`nice/ionice`、物理内存/cgroup 限额和应用 RSS 预算。
2. 如源代码已更新，不套用历史 SHA 的 perf 数字。隔离复制正式源码和 venv，Run root 唯一，原始证据不写入 git。
3. 记录并排除 WSL 与 EulerOS 的 Python 3.11 patch、NumPy ABI、PyArrow wheel、DuckDB 编译/设置差异、磁盘 I/O、宿主机负载。CPU affinity `0–3`/nice10 作为对照资源 envelope，不等于真实部署资源。
4. **实验臂固定四个**（所有 cohort 同版本、同负载、同顺序对照/随机轮换）：
   - `C0 = A0 / batch 512`：现有生产默认对照。
   - `C1 = A0 / batch 1024`：首选零新依赖候选。
   - `C2 = A2 / batch 512`：Arrow，仅实验候选。
   - `C3 = A2 / batch 1024`：Arrow + 批次优化。
   - 仅当以上数据仍不能解释 batch 填充或内存变化时，再加 2048 exploratory，不自动扩大矩阵。
5. **严格区分四个数量**：task、socket PDU、SMPP response、Journal/Projection events；单段成功通常约 2 events/PDU，multipart 不允许简单按 2 倍计算。记录实际 `event_count / committed_rate / backlog_slope`，不得以 `route_tps × pool_size` 冒充实测速率。
6. 拆分 `producer_active`、`SMPP drain`、`projection drain`；drain 时钟起点和完成条件必须固定一致，并用 durable Journal EOF/offset、DB event rows、aggregate 对账。不要把 socket/drain 与 DB drain 的重叠时间重复相加。
7. `PROD_8G` 模型：总 RAM 8 GiB、初始 application-only RSS guard 6 GiB；`PROD_16G`：16/12 GiB；`LOW_MEM_2G` 仅 stress。真实硬件/cgroup 未验证时标 `MODEL_ONLY`/`NOT_VERIFIED`；375 GiB 测试服务器应用 RSS 低于 6 GiB 不能宣称“真实 8 GiB 机器 PASS”。

**交付**：`Docs/Marketing_Copilot_VNext_A0_A2_Batch_R0_Freeze.md` 和机器可读 R0 JSON。

## 3. R1 — 先解释 WSL/EulerOS 差距，再评估 A2（优先级 P0）

### 3.1 同源数据与最小基准

- 首先在**已授权、可用**的 EulerOS/测试服务器（如未授权就只在本地完成并 `BLOCKED_ENV`）使用**同一提交、同一批生成的可复现 event seed**，运行 C0/C1/C2/C3 的 11K/100K 纯 Writer 基准。
- 记录每一臂 Python/RSS、actual flush batch P50/P95、batch total/staging P50/P95、`staging_prepare_rows`、`clear_table`、`transpose`、`bind_execute`、`relation_create/insert`、dedupe/aggregate/commit/offset 和每 1000 events 成本。
- A0 仍保留当前 `LIST + list_zip + UNNEST`，不得擅自删除原有路径、去重索引或任何持久性边界。
- 先固定 CPU affinity/nice/ionice；如果差异仍巨大，**有限度**补一个相同源码的受限 4-CPU vs 合理生产 CPU 配额/正常 I/O 优先级对照，而不是盲目使用 64 核。记录 `user/sys CPU`、I/O wait、上下文切换、DuckDB PRAGMA/settings、系统负载及实际 Run 文件系统。
- 每次 A/B 数据表、Warm-up 策略和缓存策略一致；控制测试顺序偏差（至少轮换各臂顺序）。微基准必须注明“没有生产 Journal fsync/offset/Worker lifecycle”范围限制。

### 3.2 明确止损策略

- 如果 EulerOS staging 耗时重现 **~5 秒/批** 而 WSL 是毫秒级：优先查平台/绑定执行异常，**暂不调生产默认或多 Writer**。
- 如果 A0/1024 在同环境已经达到吞吐目标且无内存/延迟回归：优先选择 C1；除非 C3 有清楚、稳定、端到端收益，否则不引入 PyArrow 生产依赖。
- 如果 A2 只在 WSL 赢、在 EulerOS 不赢：记录 `NO_CHANGE_TARGET_ENV`，不推广。

**交付**：`Docs/Marketing_Copilot_VNext_EulerOS_vs_WSL_Staging_Discrepancy_RCA.md`；原始 metrics 以 JSON 路径、数据集 hash、source SHA 和表格给出。

## 4. R2 — 500K/30% 与负载维度归因（优先级 P0）

**不再仅复述上一轮 500K 单次结果**，本轮用 R1 最优的 2–4 臂开展：

1. 100K/10% duplicate：确认三轮配对可复现。
2. **500K/30% duplicate：至少三轮 paired**，包括 C0/C1 和 A2 候选（若依赖/授权允许）；比较各轮与 median，不以单轮 3.4% / 18.3% 直接签署。
3. 若 500K/30% 增益比 100K/10% 明显下降，补充 **500K/0% 或 10% duplicate** 与 **100K/30% duplicate** 中最少必要的两个对照，区分数据库增长与去重率影响。除非结果冲突，不开启更大无意义测试矩阵。
4. 为每个臂比较 full wall、staging wall、`batch_end_to_end`、dedupe、aggregate、commit、RSS/PSS(如可用)、DB/WAL/Journal bytes、batch occupancy（配置 vs 实际 P50/P95）。
5. 记录 `events_applied`、`events_replayed`、unique event rows、aggregate rows、跨 business day 与 campaign digest，字段/nullable/Unicode/长文本/多段响应一致；**验证 warm-up 和 benchmark 数据生成没有不公平地影响 30% duplicate 测试**。
6. 三轮 median 是默认统计方式，报告每轮原始数值、极端值和机器同期负载；一旦有计数缺失/不一致，立即停掉该 candidate，不用后续平均吞掉错误。

### R2 准入判据

- 实验正确性 100% 等价：25 列 type/null、event-ID dedupe、response count、aggregate/day/route totals 均一致。
- C1/C3 比 C0 的单 Writer **总 wall median 至少改善 15%** 可进入端到端；若性能接近则优先 C1/保持 A0 默认以降低部署复杂度。
- 同时显示 `A2/1024` 相对 `A0/1024` 的**边际价值**。如果 Arrow 的边际收益微弱或者内存/兼容成本明显增加，**不因相对 A0/512 的大数字而推广 Arrow**。
- 不把“单 Writer microbench ≥1600 events/s”当作最终 E2E PASS；只证明候选值得进入后续。

## 5. R3 — 最小化接入受控候选（条件成立才执行）

根据 R1/R2 结果选择至多一个候选：

- **选 C1**：仅允许通过现有 `projection_batch_size` 可配置值选择 1024；避免改数据模型/增加依赖。需要在 `ProjectionWriterProcess`、`VNextRuntime`、接受测试 harness 中实际传递配置并验证生效；即便通过本地测试，生产默认值仍暂留 512，待 R4/R5 决策。
- **选 C3/C2**：将现有 `_stage_pyarrow()` 的实现整理为最小的生产 `ProjectionWriter` staging adapter（不 import benchmark 模块），只新增明确的 `arrow` 实验模式配置；**默认仍是 A0，必须显式 opt-in**。不要大范围改写 `_apply_batch` 下游 SQL。
- Arrow 需在 Writer 启动前验证 `pyarrow`/`numpy`/`duckdb` 兼容与 wheel 可用性；如显式 Arrow 模式缺依赖，应在**发送开始前**清晰拒绝启动，而非运行一半后静默退回 A0 或损坏批次。对测试/部署包需验证 CPython 3.11 / manylinux2014 / glibc 2.17 与既有依赖；不得直接全局强制降级 NumPy，先进行兼容矩阵。只有经本轮测试和项目依赖流程确认后才可加入生产包。
- 每批 Arrow relation 必须与 Writer 连接/事务绑定：确保注册、SQL insert、unregister 的 finally 释放；异常时整个 DuckDB 事务 rollback，offset 不前移，不能 GC 未确认的 Journal；内存不得随批次无限增长；重启后 replay event-id 仍幂等。
- 不添加独立数据库、第二 writer、IPC 服务、线程式批处理队列或长期保留所有 25 列数据缓存；保持可回退单一模式开关。
- 测试矩阵必须覆盖 `null`、Unicode/emoji、超长文本、整数/浮点类型、event-ID 重复、跨 day、multipart、DuckDB rollback、Worker/Writer crash、Journal segment GC、SMPP response unknown 和午夜旧任务禁止重新发送。

若 R1/R2 未选出明确候选，**跳过 R3，输出 `NO_CHANGE_WITH_EVIDENCE` 和下一步条件，不要为了交付而硬做生产改动**。

## 6. R4 — 端到端完整链路比较（条件式、受控）

**只在 R3 有合格候选时执行 candidate-specific 回归**；本地 native ext4 先完成，不需等待远端：

1. 400 tasks 单段 loopback；80 tasks multipart；T7-4～T7-9；至少补一次 T7-1～T7-3/完整 9/9 与必要的 T6 跨天边界验证；Windows targeted 和 native ext4 `backend/tests` 全量。
2. 做不依赖生产 API/DB 的 **T3 50K 四 Campaign**（20K+20K+5K+5K，按任务既有设置），固定 8 workers/4 binds/window50/route credit 条件；baseline 与 candidate 同一机器同一时段相同资源 envelope 分别运行，唯一 Run root；最好 >=3 paired 的缩略 E2E 后再做至少一组 50K 正式 pair。
3. 不要求执行所有四臂的昂贵 T3：优先比较 **生产 C0** 与 **R2 的最佳候选**，必要时加入 C1 判断 Arrow 的真实边际收益。测试失败时不要继续浪费服务器长跑时间。
4. 报告每 Campaign accepted、taken、socket_written task、PDU、response success/error/unknown、Journal event、Projection row、offset EOF/GC、duplicate/missing/unexpected。禁止通过丢弃响应或调整到期策略增加速率。
5. 计算真实 feed-phase PDU/s、response PDU/s、Journal durable events/s、DB committed events/s，分开 producer、SMPP drain、Projection drain。特别报告 **有效 batch P50/P95 与每秒 backlog**，避免 benchmark 1024 批次在实际 Writer 中频繁变成 64–256 而虚假预期。
6. 内存分别记录 application-only 与 harness-inclusive RSS，Writer 子进程峰值、稳定段/末 180s RSS 趋势。按 8 GiB 模型 6 GiB app RSS guard，16 GiB 模型 12 GiB app RSS guard；仅在对应资源可明确限制/确认时签署 profile PASS。

### R4 E2E 推广条件

- 绝对条件：所有完整响应、Durable Journal、Projection 和业务汇总对账 PASS；未知响应不自动重发；重启与跨日语义 PASS；新依赖兼容，进程和文件句柄无持续增长。
- 相对条件：在可比 envelope 下，50K **Projection drain/整体 wall 有实测明显改善**，默认优先采用 >=15% 的三轮 paired median 或由 50K 证据证明同等强度的改善；一两个偶然更快的 Run 不得签署。
- 若 C1 已达到目标且 C3 无充足增益，选择 C1；若 C3 显著优于 C1 且部署成本可控，则保留 C3 进入生产候选，但 **仍不直接设为默认**。
- 如果重现 E2E write_failed、window timeout 或 missing responses，candidate 为 `NO_GO`，立即回退，不以微基准遮盖缺陷。

## 7. R5 — EulerOS 600 秒与部署容量门禁（授权且前置条件满足才跑）

条件：R4 PASS + 当前任务有目标服务器明确授权 + synthetic loopback + 对真实资源规格可做有效模拟。否则 `BLOCKED_ENV/NOT_VERIFIED`，不准虚构结果。

1. 用通过的最佳候选和冻结 A0/C1 可比基线执行 `600s` 稳态，配置 4 binds × 200 per-bind TPS 作为**目标额度**，最终以实测是否约 **800 PDU/s** 判定；单段正常完成期望约 **1600 Journal events/s**，multipart 按真实 PDU/事件数计算。
2. 持续记录实际 PDU/s、response/s、Journal append/s、Projection committed/s、backlog B/s 与 events/s、RSS/CPU/I/O、journal retained/GC/reclaim、限流与 backpressure、每秒成功响应。
3. **必须确认实际供给达标且 backlog 长期稳定/可收敛**，不能只靠长 drain 最终追平。停止供给后可在既定合理窗口内排空；报告 `drain_seconds` 和独立阶段时间。如果吞吐不足，优先报告真实可持续容量，不强行把 guard 放大到 PASS。
4. `PROD_8G` 应用 RSS 6 GiB、`PROD_16G` 12 GiB 的模型分别验证。测试服务器有 >300 GiB 物理内存并不等价于 8/16 GiB 主机；如无实际 cgroup 或受控限额，分别标注“RSS guard pass / hardware-equivalent NOT_VERIFIED”，不得把高内存测试机器标成 16 GiB profile PASS。
5. 历史 EulerOS B3 的 `1005.957s staging/100K events` 是对照信号；**同 SHA、同 envelope 的新旧 staging 数据**才可签署“解决 98.1% 根因”。最终不再存在巨大的 unexplained platform gap 才能标为 `EULEROS_DISCREPANCY_CLOSED`。
6. 即使 600 秒未过，也完整记录失败点、可持续 PDU/s、rss/backlog 增长和最小复现；不因目标没达到继续引入复杂架构或第二 Writer。

## 8. 源码检查清单与指标

- `backend/vnext/projection_writer.py`: `_clear_staging` / `_stage_rows` / `_apply_batch` / `_record_staging_batch` / `flush` / `_process_main` / `ProjectionWriterProcess`；注意事务 rollback、offset commit、replay consistency。
- `backend/tools/vnext_single_writer_benchmark.py`: `BenchmarkProjectionWriter` / `_stage_pyarrow` / `_run_case`。基准类不能偷偷改变生产执行分支。
- `backend/tools/vnext_streaming_acceptance.py`: 配置参数接线，RSS 范围，producer/drain phase 边界、response 对账与 `projection_drain_timeout`，`T3/T4` 结果 JSON。
- `backend/vnext/runtime.py`: `projection_batch_size` 目前可配置、受 1–4096 范围限制；验证测试通过 runtime 与生产实际路径，不能只测独立 Writer。
- `backend/vnext/smpp_transport.py`、`worker.py`、`spool.py`: 不修改既有防重发/午夜/响应语义。若发现真正缺陷，仅做与正确性直接相关的定点修复并加测试。

**必须有的聚合指标**：

```text
baseline_sha / candidate_sha / repo_diff / dependency_hash / dataset_seed/hash
runtime_python / duckdb_version / pyarrow_version / numpy_version
cpu_affinity / cpu_quota / nice / ionice / filesystem / disk_wait / cgroup
arm / staging_mode / batch_size_configured / batch_size_p50_p95_actual
100K_10pct_events_per_second / 500K_30pct_events_per_second / wall_median
staging_prepare / clear / transpose / bind / relation_create / relation_insert
staging_p50_p95 / batch_total_p50_p95 / dedupe / aggregate / commit / offset
response_count / journal_event_count / DB_event_rows / daily_digest / replay_duplicates
socket_pdu_rate_steady / response_rate_steady / projection_committed_rate_steady
producer_seconds / smpp_drain_seconds / projection_drain_seconds
journal_backlog_bytes_curve / backlog_slope_last180 / final_EOF_offsets
rss_application_peak / rss_writer_peak / rss_harness_peak / rss_slope_last180
throughput_gate / recovery_gate / compatibility_gate / 8G_profile / 16G_profile / canary
```

## 9. 阶段验收和停止条件（必须执行，避免无效重测）

| Gate | PASS 定义 | 未通过操作 |
|---|---|---|
| `R0_SOURCE_ENV` | 当前源码/数据集/依赖/资源可追溯 | STOP, ENV_GAP |
| `R1_PLATFORM_DIFFERENCE` | EulerOS 与 WSL 同源/同负载对比，慢段可解释 | STOP 大改动，仅输出 RCA |
| `R2_PAIRED_CORRECTNESS` | C0/C1/C2/C3 digest/schema/unique/daily 等价 | REJECT 对应臂 |
| `R2_SUSTAINED_BENEFIT` | 500K 三轮和 batch/dup 维度证明候选收益 | NO_CHANGE，跳过产品改动 |
| `R3_PRODUCT_OPT_IN` | 最小接入、默认不变、回退明确、依赖可用 | 不推广、保留基线 |
| `R4_E2E_INTEGRITY` | 50K/多段/跨日/T7 响应与 DB 端到端对账 | ROLLBACK，NO_GO |
| `R4_E2E_BENEFIT` | 同资源可复现总耗时和 drain 改善 | NO_CHANGE |
| `R5_600S_800PDU` | 600s 实际平均约 800 PDU/s，事件负载处理/积压收敛，drain 有界 | NO_GO，报告实测容量 |
| `PROD_8G_PROFILE` | 受控 8 GiB 目标规格或有效限额 + RSS/稳定性 | NOT_VERIFIED |
| `PROD_16G_PROFILE` | 仅当 16 GiB 是拟发布目标且有受控规格时签署 | NOT_VERIFIED（不自动否决已通过的 8G 独立候选） |
| `CANARY_READY` | 必须满足真实目标 Profile、正确性、吞吐、恢复全部必要门禁 | NO_GO |

若 R1/R2 提前失败，按阶段停止：**不要为了填满任务表而跑未必有意义的 50K、600s 或构建复杂 Arrow 产品分支。**

## 10. 必须交付的文件与最终报告

在 `Docs/` 中新增（不要覆盖前两批报告，不删历史 NO_GO 证据）：

1. `Marketing_Copilot_VNext_A0_A2_Batch_R0_Freeze.md`
2. `Marketing_Copilot_VNext_EulerOS_vs_WSL_Staging_Discrepancy_RCA.md`
3. `Marketing_Copilot_VNext_A0_A2_Batch_100K_500K_Paired_AB_Report.md`
4. `Marketing_Copilot_VNext_A0_A2_50K_E2E_and_Recovery_Report.md`（未进入 R4 时标 NOT_APPLICABLE + 原因）
5. `Marketing_Copilot_VNext_A0_A2_Production_Profile_Canary_Decision.md`
6. `Marketing_Copilot_VNext_A0_A2_Closure_Evidence_Summary.json`（schema、source、环境、arms、每轮数据、gate 均机器可读）

报告需包含：每个 Run 的原始 JSON/日志路径和 SHA、实测/缺失区分、配置额度与实测区分、清晰的 `GO / NO_GO / NO_CHANGE_WITH_EVIDENCE / BLOCKED_ENV`。不要只放截图或只有结论。

**最终汇报示例**：

```text
BASELINE_HEAD=<sha>
CANDIDATE_HEAD=<sha_or_none>
SOURCE_DIFF=<paths>
DEPLOY_ENV=<actual_or_not_verified>
WINNING_ARM=C0|C1|C2|C3|NONE
A0_512 / A0_1024 / A2_512 / A2_1024=<paired medians>
EULEROS_STAGING_98PCT_ROOT_CAUSE=<established_or_not_verified>
500K_30PCT_PAIRED=<pass_fail_unverified>
ARROW_DEPENDENCY_COMPATIBILITY=<pass_fail_not_verified>
T3_50K_PAIRED=<pass_fail_not_applicable>
T6_T7_RECOVERY=<pass_fail_not_applicable>
T4_600S_ACTUAL_PDU_RATE=<number_or_not_verified>
PROJECTION_EVENTS_RATE=<number_or_not_verified>
APP_RSS_PEAK=<number_or_not_verified>
JOURNAL_BACKLOG_END_AND_SLOPE=<numbers_or_not_verified>
PROD_8G_PROFILE=<pass_fail_not_verified>
PROD_16G_PROFILE=<pass_fail_not_verified>
DEFAULT_INGEST_MODE_CHANGED=NO (unless explicit later approval)
CANARY_READY=NO|YES_WITH_ALL_GATES_AND_APPROVAL
FINAL_DECISION=<reasoned_decision>
COMMIT_PUSH=NOT_REQUESTED
```

## 11. 优先执行顺序（给 Codex 的指令）

1. **先读仓库当前代码 + 两批 A/B 原始报告**，冻结 R0。不要再设计第五种 adapter。
2. **先证明 C1（A0/1024）是否能以最低复杂度解决性能问题**，再看 C3（A2/1024）是否有额外真实收益。
3. **优先缩小 EulerOS 与 WSL 差距**，无当前服务器授权则完成本地证据并明确停止点；不要重复声称 EulerOS 已 PASS。
4. 合格候选先只在实验配置和 synthetic E2E 下 opt-in，完整对账后再考虑 600s/800 PDU/s。
5. 严格保留业务发送可靠性约束；如果没有可证实、可复制的收益，就保留 `A0/512` 默认并以 `NO_CHANGE_WITH_EVIDENCE` 结束。

**成功定义不是“引入 PyArrow”，而是用最小复杂度实证消除 EulerOS Projection backlog，稳定完成目标吞吐与发送结果对账。**
