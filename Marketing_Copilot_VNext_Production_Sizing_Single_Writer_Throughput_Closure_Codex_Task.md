# Marketing Copilot VNext — 生产资源规格校准、单 Writer 吞吐收敛与最终性能验收（Codex Task）

> Task ID: `MC-VNEXT-PRODUCTION-SIZING-SINGLE-WRITER-THROUGHPUT-CLOSURE-20261017`  
> Target repository: `https://github.com/shutu-ai/Marketing-Copilot`  
> Frozen starting HEAD: `f4ab33735e657f1734db13d90e223c38047a0f22` (`origin/main`, 2026-10-17 核对)  
> Comparison reference: `65e3d6f16810cbe1ab46940c23dd00f715f3cd4f`（仅用于代码差异、历史背景；其响应事件链路与当前不等价，**不得直接用于等价吞吐签署**）  
> Decision: **保持单 `ProjectionWriterProcess` + 单 DuckDB；不做 Campaign 分库/多 Writer，不重构消息发送架构**。  
> Current release decision: `NO_GO`。本任务执行完成前不得改写为 READY。  
> Execution scope: 本地分析、最小代码优化、离线 synthetic loopback、经用户**单独明确授权后**在 EulerOS 专用测试服务器验证；不接真实 SMSC/生产 API/生产 DB。

## 0. 任务目标（不要把性能测试变成安全工程）

将当前 VNext 在**真实预期部署内存 8–16 GB** 条件下重新定标，证实/解决单 Writer 的 sustained catch-up 问题，并把此前 2 GiB RSS guard 引发的 `NO_GO` 与真实性能瓶颈分开判断。优先提升实际吞吐、正确性、长任务稳定性和端到端可恢复性，不引入额外复杂控制框架。

完成后要能够定量回答：

1. 8 GB 级和 16 GB 级部署中，各给多少应用内存、CPU 配额、磁盘 I/O，是否与实际计划吻合？
2. 在**每单段 SMS 至少 1 条 task 事件 + 1 条 submit_sm_resp 事件**的完整统计合同下，Writer 可持续处理多少 **Journal events/s**？当 `800 submit_sm PDU/s` 时，单段无异常的理论最低需求约为 **1,600 events/s**（混合长短信按真实 `events/PDU`、`events/task` 计算）。
3. 是否能在 600 秒连续供给后 **Journal backlog 收敛至 0**，并在明确的 drain 时间窗内完成；如果不行，最耗时的是 DuckDB 去重、aggregate/upsert、offset fsync、staging，还是 CPU/I/O/RSS 限制？
4. 在当前版本上能否完成全部 T7 9/9 联合回归、午夜截止和 at-most-once 检查？
5. 是否有实测收益足以支持单 Writer 的局部优化；是否还需要考虑后续独立 Writer + DuckDB **实验**（本任务不实施）？

## 1. 已知事实与源码审计基线（禁止重写历史结论）

### 1.1 已核对的报告

- `Docs/Marketing_Copilot_VNext_Single_Writer_Performance_RCA_and_AB_Report.md`
- `Docs/Marketing_Copilot_VNext_Production_Response_Journal_T7_Closure_Report.md`
- `Docs/Marketing_Copilot_VNext_T3_T4_T6_T7_Evidence_Closure_Report.md`
- `Docs/Marketing_Copilot_VNext_Harness_and_Bottleneck_Audit.md`

`f4ab3373` 是 **13 个文件的代码、测试、工具、文档组合提交**，不是只提交文档。执行前核对当前 `git rev-parse HEAD`、`git status --short` 和远端状态；若 HEAD 已更新，记录差异，不得静默套用过期行号或覆盖其他人改动。

### 1.2 不能忽略的真实数据

| 项目 | 已有证据 | 本任务如何使用 |
|---|---|---|
| A3 向量化微基准 | 4,096 events、batch 512 约 290.763 events/s；相对 A0 约 +55.5% | 说明向量化有收益，但 **4K 不能代表 100K–1M 增长型数据库** |
| Native ext4 当前版本 T4 | 600 秒实际仅约 **164.2 PDU/s**；197,500 events 最终完整；drain **274.014s** | 证明正确性与低速运行；**不是 800 TPS 的性能通过** |
| EulerOS T3 50K | task/PDU/response 全闭合，100,000 Journal events；projection drain **742.003s**，最终 guard FAIL | 正确性可保留，但收敛时限未满足 |
| EulerOS T4 | 331.293s 时 RSS 2,099,128 KiB，guard 2,097,152 KiB；178,000 PDU response 完整，142,505/356,000 events 已投影 | RSS guard FAIL + Writer backlog；未完成 600 秒，不可当稳定吞吐 |
| EulerOS T7-4…T7-9 | 6/6 PASS | 保留证据；当前 SHA 上 T7-1…T7-3 需要联合验收 |
| EulerOS 的运行限制 | `taskset 0-3` + `nice +10` + `ionice -c3` + 2 GiB RSS；宿主有其它负载 | **不是** 8–16 GB 预期部署的自然资源条件，不应以它直接定型生产容量 |

**禁止将 2 GiB 超限解读为 8 GB/16 GB 机器内存不足；也禁止把放宽 RSS guard 误当作消除了 Writer backlog。**

### 1.3 需要重点检验的当前源码热路径

- `backend/vnext/projection_writer.py`：
  - `ProjectionWriter._apply_batch`：25 列 `list_zip/UNNEST` staging、每批 `DELETE FROM _vnext_projection_batch`、新事件临时表 `LEFT JOIN vnext_progress_events` 去重、`INSERT ... SELECT`、按 `(business_day,campaign_type,campaign_id,partition_id,route_id)` 的汇总 `ON CONFLICT DO UPDATE`。
  - `ProjectionWriter.flush`：单事务 `BEGIN/COMMIT`；`batch_size` 当前常用 512。
  - `_process_main`：多 Worker Journal 轮询读取；每批 offset 持久化；`status()`、`storage_snapshot()`、段枚举、diagnostics/status 写文件及 Journal GC 的周期成本。
  - `ProgressJournal._append_encoded`：每个事件写入时可能涉及 `segment_paths()` glob、`stat`、flush/fsync；单事件 `append` 一次 sync；这是候选成本，**不得无证据取消 durable sync**。
  - `_persist_offsets`：临时文件 + flush/fsync + replace + POSIX 目录 fsync；**持久化协议不可被破坏**。
- `backend/vnext/runtime.py`：Worker `run_until_idle(close_transports=False) → drain_transports → close` 已接入；单 Writer watchdog 保留；`status()` 比 `_idle_snapshot()` 重，需要避免以完整状态扫描做热采样。
- `backend/vnext/worker.py`：task event 与 response event 都要落 durable Journal；`release_reader_caches` 应只在无 inflight 的 pass 结束后执行。
- `backend/vnext/spool.py`：跨天 cutoff、durable dispatch-taken fence、旧日回收、reader index；不可为提升吞吐而重新派发旧任务或已写出的不确定 PDU。
- `backend/tools/vnext_streaming_acceptance.py`：当前 T3/T4 harness；测 `accepted task`、`socket PDU`、`SMPP response`、`Journal event`、`DuckDB new event`、committed EOF，**不能以 spool idle 或 `projection_lag_seconds=0` 代替 Writer 完成**。

## 2. 不可违反的业务与架构约束

1. **时效性 > 完成率**：新业务日开始，昨日未发送的任务禁止 socket write；旧日 task 不能在重启、Bind 等待、TPS 等待、多段发送部分写出后重新派发。
2. **At-most-once**：允许极端崩溃窗口内漏发/unknown/partial，但已写出的/不确定的 PDU 绝不自动重发；不得通过测试重试或大范围恢复重写此语义。
3. **Journal / Projection 正确性**：`event_id` 幂等；只有 DB durable commit 后可前移逻辑消费确认；已确认的 offset 必须 durable 后才可 GC 对应 inactive segment；无法确认的记录必须可安全 replay、不得双计数。
4. 保持**一个 Writer 进程 / 一个 DuckDB 所有者 / 一个数据库文件**。不得新增 per-Campaign Writer、拆数据库、引入分布式锁/队列/RBAC/审批平台。
5. 不以关闭 `fsync`、牺牲 SQL 去重、只记 task 不记 response、降低真实负载、超售 TPS 或扩张内存至无限大来制造 PASS。
6. 不升级 DuckDB 1.2.0 或改操作系统/生产数据作为取巧；候选 SQL/API 必须在当前 DuckDB 实际版本验证。
7. 遵守 `AGENTS.md`；有 `.codegraph/` 时先用 CodeGraph；保留 dirty/untracked；**未经用户明确要求不得 commit/push**。不自动连接远端、要求 VPN 或安装额外在线依赖。

## 3. R0 — 冻结身份、确认实际资源规格与验收模式（必须先做）

- 保存 `HEAD`、tree SHA、`git status`、依赖版本、`uname -a`、DuckDB 版本、文件系统类型、挂载选项、目标部署规格以及产品与 harness 文件 SHA。
- 先明确「8 GB / 16 GB」指**整机 RAM**还是**专门分配给 VNext 的 cgroup/容器限制**，并记录部署时共享的其它服务；优先从真实部署参数/用户资料确认，不能自动推断。
- 定义两套正式资源模型（初始建议，最终按确认的部署规格调整）：

| Profile | 参考部署 | 建议应用进程树 RSS 上限 | 备注 |
|---|---|---|---|
| `PROD_8G` | 8 GiB 机器总 RAM | **6 GiB** | 留约 2 GiB 给 OS、Page Cache、其它服务；若实情不同重新定标 |
| `PROD_16G` | 16 GiB 机器总 RAM | **12 GiB** | 留约 4 GiB 缓冲；不得未经确认就分配全部 RAM |
| `LOW_MEM_2G` | 低内存故障注入 | **2 GiB** | **非生产性能门禁**；仅验证有界退化、清理与恢复 |

- 明确 RSS 是全体**应用进程树**还是包含 Python synthetic loopback/harness；至少分别报告 `application_only` 和 `harness_inclusive` 两套指标，不能混用。补充 `VmHWM`、PSS（若可用）、cgroup `memory.current`/`memory.peak`、OS available memory/page cache，用于解释不同口径。
- CPU/I/O **以计划部署能力为准**：若拟配 4 vCPU 用 4 vCPU；若拟配 8 vCPU 用 8 vCPU；未明确时生成分档（如 4/8 核）的独立 A/B，不可直接把 64 CPU 宿主全量结果等同实际生产。对生产主门禁不要保留与实际部署无关的 `nice +10`、`ionice -c3`；4 核/低优先级另留压力子场景。
- 一个 A/B 对比组必须保持同机、同环境、同 affinity、同 nice/ionice、同依赖、同数据分布、同短信响应/事件语义和相同后台负载窗口；如宿主干扰显著，报告 `INCONCLUSIVE`。
- 每个正式 run 使用独立 ext4 目录，磁盘剩余空间不足时先停测，不能删除历史证据；不要在 `/mnt/d`、NTFS 或 WSL 模拟目录上伪造 EulerOS/ext4 基线。

**R0 输出**：`sizing.json` + `environment.json` + 对照矩阵。真实 EulerOS 运行须在用户再次授权后进行；未授权则执行本地可行阶段并保持 `BLOCKED_ENV`。

## 4. R1 — 先确定瓶颈归因：阶段耗时 + 事件增量 + RSS 分解

对当前 `f4ab3373` **原样运行**小、中、大三类对照。不得边改边测而丢掉冻结基线：

- **小**：P5500（完整 response / multipart）。
- **中**：50K，4 Campaign，20K+20K+5K+5K，正常单段短信，约 100K Journal events。
- **大**：使用当前 harness 可承受的增长数据集，至少 100K/300K/500K **events**，分桶记录随 DuckDB `vnext_progress_events` 变大的 P50/P95 batch cost；区分新数据写入和 10%/30% replay/duplicate。

最低每秒采样项：

1. `tasks/s`、`socket PDU/s`、`response PDU/s`、`durable Journal events/s`、`Projection newly_applied events/s`；明确 event 倍率。
2. `Journal total bytes / committed bytes / backlog bytes / segment_count`；以 committed EOF 作为真正 drain 判据。
3. `batch_size P50/P95`、每秒 batches、`journal_read_ns`、`json_decode_ns`、`rows_prepare_ns`、`staging_load_ns`、`new_event_dedupe_ns`、`events_insert_ns`、`aggregate_upsert_ns`、`db_commit_ns`、`offset_persist_ns`、batch end-to-end。
4. 以 process PID/role 区分 feeder、loopback SMSC、8 Worker、Writer、父进程的 RSS/PSS/HWM、CPU time、线程数；并记录磁盘读写字节和采样窗口 I/O 变化。
5. 按时间区分 producer ACTIVE、SMPP DRAIN、Projection CATCH-UP 三个阶段及其持续时间。

**特别审计低频指标本身的开销**：`ProjectionWriterProcess.status()` 当前每次可能扫描全部 Journal segment，`_process_main` 每批调用诊断/status；`ProgressJournal.append()` 可能每事件 glob/stat/fsync。建立“采样启用 vs 限频/关闭”的同数据 A/B。允许降低诊断写文件频率，但不能降低 Journal/offset 的持久化合同。

**R1 门禁**：归因只能定位到有数值支持的操作层；如果数据库去重/聚合占比最大，则进入 R2；如果 stage 采样/文件枚举占比最大，先优化采样/枚举；如果 CPU/I/O 限制是主因，先调整实际规格。禁止预设单 Writer 已达到极限。

## 5. R2 — 单 Writer SQL 优化：优先去重与 Aggregate，而不是重复做 staging

当前 `f4ab3373` 已支持 vectorized staging；上一轮小规模 A3 比 A0 快，但持续负载 `new_event_dedupe` 与 `aggregate_upsert` 累计耗时更突出。**首先做候选实现的基准实验；证明收益后再以最小改动落地**。

### 5.1 候选实验（保留 A0 原行为方便 fallback）

- Dedupe A：当前 `TEMP new AS SELECT batch LEFT JOIN vnext_progress_events WHERE p.event_id IS NULL`。
- Dedupe B：尝试 DuckDB 1.2.0 可支持的替代 anti-join 计划、bulk `INSERT ... ON CONFLICT DO NOTHING` / `RETURNING` 或 staged unique-id strategy；**必须从实际插入成功的 event 集合产生 aggregate**，不能因为重复 replay 再次累加。
- Aggregate A：当前 `GROUP BY` + `ON CONFLICT DO UPDATE`。
- Aggregate B：先在 staging 生成合并的 per `(day,campaign,partition,route)` aggregate，再以单批 set-based 更新；逐项对照 SQL elapsed 与 DuckDB query plan，避免为“SQL 更漂亮”增加成本。
- Staging：保留 `vectorized+reuse` 作为起始候选；只在完整 end-to-end A/B 有明显收益时更换。避免未经证明引入 Arrow/Pandas/第三方额外依赖。
- Batch：在 256/512/1024/2048 范围尝试，观察低延迟 vs 持续吞吐 vs RSS，不能只报告最佳孤立值。不要无界扩大 batch。

### 5.2 数据集与正确性

- 以真实 `event_id`、task/response 各种状态、multipart、多 Campaign、多日、跨 partition、相同号码不同活动、跨 session replay 生成至少 100K / 300K / 500K /（资源允许则）1M events。
- 所有候选输出同一个 deterministic digest：`vnext_progress_events` 的明细 + `vnext_projection` 聚合 + daily summary；比较 **event rows、按状态 counts、日界归属、重复/未知/部分写出**。
- 对 0%、10%、30% duplicate 及“Writer 在 COMMIT 之后、offset 落盘之前崩溃”的回放必须保持精确一致。
- 因响应本来有 `:resp:` event-id，task 数、PDU 数、response 数和 Journal events 数应单独检验。

### 5.3 是否实施判定

- 只有同环境端到端或增长型大样本吞吐改善明显（建议连续 3 次中位数不低于 +15%，无正确性/内存退化），才选择新 SQL 路径；阈值不足则保留旧方案并记录 `NO_CHANGE`。
- 禁止仅凭 4096-event microbench 或单次最优结果判定生产 throughput 通过。

## 6. R3 — Offset/Journal 持久化和状态写入频率优化（如证据支持）

当前处理顺序：

```text
Journal durable append
   -> Writer batch DB transaction COMMIT
   -> offsets.json temp + fsync + replace + directory fsync
   -> reclaim ONLY fully acknowledged INACTIVE Journal segments
```

不得更改为“先删文件，后持久化 offset”。

### 候选（要求 A/B）

1. **状态/诊断限频**：将 `_write_process_status`、`storage_snapshot`、diagnostic metadata 从“每批可能触发多次”调整为固定低频（例如每 1 秒或更低），错误、shutdown 和 critical transition 则立即输出。确保查询可见性不过度滞后。
2. **Segment index 缓存**：减少 `segment_paths()` / `glob` / `stat` / `active_path` 的反复枚举开销；文件轮转与外部进程重启后必须重新发现 segment。优先局部缓存，不要写第二套持久索引系统。
3. **offset checkpoint 合批**：在 DB COMMIT 后允许对 *durable offset* 按固定时间/字节窗口合批刷盘，**这意味着 crash 后可重放更多已提交事件，但 DB `event_id` 去重必须保证不重复聚合**；任何 segment 仍只可按已 durable ack 的 offsets 回收。出现 crash/restart、停止和午夜切换时要有明确 checkpoint fence。
4. **Journal fsync**：保留每个 event durability baseline；只有另外设计出可明确证明相同用户可接受 durability 语义的批量 append 协议、并独立审批，才可作为后续可选研究。**本任务不得直接关闭或延后必需的 event fsync**。

重点测试 crash-at-window：DB COMMIT 后/offset 持久化前、offset 持久化后/GC 前、GC 后重启、Writer killed during staging/commit、response arrive while hard-limit hit。所有 replay 的 counts 不得重复；未持久化事件不得假装完成。

## 7. R4 — 内存 RCA：将 2 GiB guard 从正式性能门禁移到低资源测试

**不要仅通过把 `--max-rss-kib` 数值改成 6/12 GiB 便宣布内存问题解决。**

- 复现旧的 2 GiB/4 CPU/低优先级组合，只作为 `LOW_MEM_2G`；目标是有界退化、退出可控、无死锁、退出/重启不导致双发送；其资源触发本身不应否决正常 8–16 GB profile。
- 在 `PROD_8G`、`PROD_16G` 使用确认的资源参数，以同样负载运行，采集每个角色与聚合 RSS/PSS/HWM，`writer staging arrays`、`dedupe temporary relations`、`worker index`、`feeder expected-map`、`SMSC received/destination bookkeeping` 的可能增长。
- synthetic harness 的 `received[]`、`destination_counts`、samples 和最终 `progress` 全量读取可能与产品内存共同增长；必须分别报告 harness-only / application-only RSS。防止错误地把测试器增长归罪于产品。
- 必须区分随任务量稳定线性累积的可回收缓存、GC 后回落、操作系统 page cache 与不可回收泄漏。提供至少 600 秒时序、斜率、峰值和停止后的回落。
- 对 8 GB profile 使用 **6 GiB 应用聚合 RSS** 初始主门禁；16 GB profile **12 GiB**；如部署同机存在其它服务，按实际留量下调。报告还需检查整机内存余量、swap/OOM 和磁盘空间。
- 不引入无界 event list/batch、无限缓存或依赖无限 journal 来取得短时高吞吐。

**R4 门禁**：在声明的目标部署规格下 RSS 不触发合理护栏，且没有持续不收敛的增长；如增长来源只是测试 harness，局部修正 harness，不重构产品。

## 8. R5 — 完整端到端回归与可持续吞吐容量曲线

### 8.1 测试先后顺序

1. 本地 `pytest backend/tests -q`（至少不低于现有覆盖结果；记录实际 passed/skipped）、`py_compile`、前端若本轮有相关修改则 `npm run build`。
2. 纯 Writer A/B：同 DuckDB 1.2.0、同 ext4、同 CPU/RAM 配额；100K/300K/500K events，分离 warm-up 与正式测量，**至少 3 次独立 run**。
3. 四 Campaign 50K + mixed multipart smoke，包含 delayed SMPP response、Journal durable response event 对账。
4. 若外部目标机获得独立授权，先做阶梯负载：100 / 250 / 400 / 600 / 800 PDU/s（在实际 deployment profile 下）；每级先运行短稳态 120 秒，观察 backlog growth/衰减和 RSS。若某级显著失稳，不强行进入更高级长稳态。
5. 每个有望满足指标的 profile 做 **600 秒连续 offered load**，保持 producer 与 Worker、SMPP、Writer 真正并发；停止 feed 后分别记录 socket/response drain 与 Projection drain，且在定义的上限内 complete。
6. T7 1–9 当前 SHA 联合回归；跨午夜旧日任务零派发、replay 幂等、Journal hard-limit/segment GC、安全 stop/restart，保持 at-most-once。

### 8.2 每个场景必须机器对账

- `accepted_task_count`
- `socket_written_task_count`
- `socket_written_pdu_count`
- `loopback_received_pdu_count`
- `submit_sm_resp_success / error / unknown`
- `task_event_count`
- `durable_response_event_count`
- `journal_expected_event_count`
- `duckdb_unique_event_rows` / `projection_new_events_applied` / `aggregate_digest`
- `journal_eof_bytes / durable_committed_offset_bytes / uncommitted_backlog_bytes`
- per-Campaign counts, duplicate wire counts, old-day socket-write after cutoff / redispatch after restart。

**注意**：`projection_events_applied` 是进程局部计数，Writer restart 后不能把它当全生命周期唯一 DB 行数；被 GC 的 segment 不能用“不再存在的路径”错误判断 `caught_up`。要从当前 segment EOF、durable offsets、DB 行数和必要的 GC ledger 综合判定。不要用 `projection_lag_seconds=0` 单独签署。

### 8.3 正式性能目标与失败结论

- **目标负载**：`8 Worker / 4 Bind / per_bind=200 PDU/s / window=50`，单段短信近似 800 PDU/s 对应至少约 1600 Journal events/s；多段/未知状态按实际倍率修正。
- **主目标**：实际 offer 能接近配置上限、连续 600 秒无崩溃/资源越界，Projected committed rate 不长期低于 Journal durable production rate（最近 180 秒 backlog 无持续正斜率），停止 feed 后 **目标 ≤60 秒** 完成全链路追平。若特定部署业务 SLA 不同，记录签署后的真实值；不能静默延长 drain 上限或减小 offered rate 宣称 800 PASS。
- **资源目标**：在 `PROD_8G` RSS ≤6 GiB、`PROD_16G` RSS ≤12 GiB 的初始建议上限以内（根据确认的机器共享资源调整）；稳态 RSS 趋稳，cgroup 无 OOM，磁盘与 Journal segment 容量有界。
- **正确性目标**：所有 expected PDU/response/Journal/DB 与 Campaign/day aggregate 对账；duplicate socket=0；不允许旧日重发；异常/unknown/partial 如实记录、不重试；Binder 数量不超过 4。
- **对照目标**：相同配置、相同完整响应事件语义的 baseline vs candidate 至少三次，输出 P50/P95、峰值、资源曲线、drain 时间；如果 old 版本缺少 response persistence，必须先建等价对照或判定 `NON_COMPARABLE`，绝不硬比。

### 8.4 GO/NO_GO 的语义

- `GO` 只代表当前授权的 synthetic loopback + 已确认硬件资源 profile 的候选门禁；**不代表真实 SMSC 或生产部署授权**。
- 如果某个 profile 下无法保持高压 backlog 稳定收敛，则 `PERFORMANCE_TARGET=FAIL`，同时报告可持续容量区间，不能将低速 600 秒 PASS 冒充高压 PASS。
- 如果未获目标机授权、VPN 不可用或尚未确认部署规格，标记 `BLOCKED_ENV/NOT_VERIFIED`；不要自动切换 VPN、请求 root、使用真实 SMSC 或执行部署。
- 若完成单 Writer 关键路径优化仍远低于业务需求，才输出 **“多 Writer + 独立 DuckDB 值得进行单独 A/B 探索”** 的证据型建议；本任务内不实施。

## 9. 最小改动要求与建议输出

建议在既有文件中定点修改，避免引入新架构：

- `backend/vnext/projection_writer.py` — stages/SQL/checkpoint/status sampling 的局部优化，保持 DB schema 与 durable semantics。
- `backend/tools/vnext_single_writer_benchmark.py` — 扩展增长型数据、真实 response 事件、batch size、digest/A-B。
- `backend/tools/vnext_streaming_acceptance.py` — 稳态分层、RSS 进程角色划分、真正的 Journal EOF 对账、单独 drain timing。
- `backend/tests/test_vnext_projection_writer.py` — replay/idempotency/crash windows/GC/多日多 Campaign。
- `backend/tests/test_vnext_streaming_acceptance.py`、`backend/tests/test_vnext_t7_recovery_matrix.py` — 资源护栏口径、状态门禁、9/9 检验。

允许新增**少量**专用、可测试的小工具或测试脚本，但不新建全套性能管理服务。修改前评估影响面，证明测试和生产沿用同一核心 Writer 实现。

## 10. 报告与证据合同

至少生成（建议文件名）：

1. `Docs/Marketing_Copilot_VNext_Production_Sizing_and_Memory_RCA_Report.md`：部署规格、RSS 分解、2G vs 8/16G 结论、CPU/I/O 对照。
2. `Docs/Marketing_Copilot_VNext_Single_Writer_Dedupe_Aggregate_Optimization_AB_Report.md`：对每个 SQL/Batch/offset/status 候选的效果、回退决策、500K 级正确性 digest。
3. `Docs/Marketing_Copilot_VNext_Production_Profile_Final_Canary_Gate_Report.md`：T3/T4/T6/T7、600s steady/drain、真实事件倍率、GO/NO_GO、残余风险。

每份报告包含 `HEAD`、tree、文件 hash、配置、依赖、时间、filesystem、Run id、具体命令、退出码、资源护栏、所有 PASS/FAIL/NOT_VERIFIED 与原始机器 JSON/CSV/日志索引。所有 FAIL 和超时原样保存，不从最终样本中删除。

证据清单仅提交脱敏的 summary JSON/CSV、必要的测试日志、报告；**不得把密码、真实号码、生产数据、DuckDB、WAL、Spool、Journal、venv、cache、秘密文件加入 Git**。原始 run 只保留在受控测试目录。

## 11. Codex 执行顺序与结束方式

```text
S0 校验 HEAD、AGENTS.md、工作区与环境身份
S1 明确 8/16GB 资源规格，保留 2GiB stress profile
S2 冻结 current-SHA Writer + harness 的原始基线
S3 低扰动逐阶段 timing / 角色级 RSS / 100K-500K 增长负载 RCA
S4 依 RCA 选择最小 SQL/offset/status/segment 优化；先 A/B 后 merge
S5 SQL digest/replay/journal GC / midnight at-most-once 全量回归
S6 同 profile 四 Campaign 50K + multipart/delayed response
S7 获目标机独立授权后：阶梯负载 → 600s → independent drain
S8 当前 SHA 的 T7-1..T7-9 联合验收 + 完整 Gate 报告
S9 对当前结果明确 PASS/FAIL/NO_GO/NOT_VERIFIED，不自动 commit/push
```

### 最终交付卡片（报告和终端摘要都必须填写）

```text
BASELINE_HEAD=
CANDIDATE_HEAD=
RESOURCE_PROFILE=PROD_8G|PROD_16G|LOW_MEM_2G|UNCONFIRMED
REAL_DEPLOYMENT_RAM_TOTAL_GIB=
REAL_DEPLOYMENT_VNEXT_BUDGET_GIB=
CPU_QUOTA_OR_AFFINITY=
NICE_AND_IONICE=
FS_AND_DUCKDB_VERSION=
SOURCE_DIFF_SUMMARY=
RSS_APPLICATION_PEAK_GIB=
RSS_HARNESS_PEAK_GIB=
RSS_SLOPE_LAST_180S=
SOCKET_PDU_RATE_600S=
JOURNAL_EVENT_RATE_600S=
PROJECTION_COMMITTED_EVENT_RATE_600S=
JOURNAL_BACKLOG_BYTES_END=
JOURNAL_BACKLOG_TREND_LAST_180S=
SMPP_DRAIN_SECONDS=
PROJECTION_DRAIN_SECONDS=
T3_50K_MULTI=
T4_600S_800_PDU=
T6_BUSINESS_DAY=
T7_9_OF_9=
CORRECTNESS_DIGEST_MATCH=
PERFORMANCE_AB_COMPARABLE=
PRODUCT_BOTTLENECK_RCA=
SINGLE_WRITER_CAPACITY_ESTIMATE=
CANDIDATE_READY=YES|NO|NOT_VERIFIED
RELEASE_DEPLOYMENT_AUTHORIZED=NO
UNVERIFIED_OR_BLOCKED_REASONS=
```

**任务完成标准**：本地完成正确性与 RCA、资源规格校准，且在授权测试环境给出可信的 8/16 GB deployment-profile 负载结论；对于不能完成的真实环境 Gate，保留 `NOT_VERIFIED`，不伪装成达标。若单 Writer 已达标，结束架构优化；若未达标，保留有量化依据的下一步决策，不提前实现多 Writer。
