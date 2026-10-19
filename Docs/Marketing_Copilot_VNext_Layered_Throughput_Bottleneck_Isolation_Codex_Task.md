# Marketing-Copilot VNext — 分层吞吐瓶颈隔离与最小优化闭环（Codex 任务）

> **Task ID:** `MC-VNEXT-LAYERED-THROUGHPUT-ISOLATION-20261018`  
> **基线 `origin/main`:** `3a9a1a4e6029fa965b4b7fff4ca4c49fddd54b58`（执行时先验证；若远端有新提交，记录实际 SHA，不静默覆盖）  
> **目标：** 定位 4 Bind × 200 TPS 条件下 Marketing-Copilot VNext 完整 E2E 的吞吐折损出现在哪一层，并仅在根因可复现时做最小修复。  
> **范围：** 性能诊断 + 必要的低开销测试工具；**不以源码重构、调大 TPS/Window 或跑 600 秒作为任务完成条件**。

## 0. 读前结论及工程原则

本任务基于刚提交的三份报告：

- `Docs/Marketing_Copilot_VNext_528_to_169_Source_AB_RCA.md`
- `Docs/Marketing_Copilot_VNext_SMPP_Loopback_Bind_Lifecycle_Isolation.md`
- `Docs/Marketing_Copilot_VNext_Throughput_RCA_Evidence.json`

此前证据：

| 观察 | 结果 | 正确解释 |
|---|---:|---|
| 历史 **50K** 快轮 E2E | 528.2 PDU/s | 与后续运行的源码、Worker、status interval、host 状态并非完全一致，仅作参考 |
| 历史 **50K** 慢轮 E2E | 约 162–169 PDU/s | 不能据此宣布稳定源码回归 |
| 最新严格 **5K** A 中位数 | 116.66 PDU/s | 两对实验出现胜负反转，不足以确认旧源码更快 |
| 最新严格 **5K** B 中位数 | 100.58 PDU/s | 与 A 的差异未形成稳定因果 |
| 独立 L0 SMPP loopback | 14,239.716 PDU/s | 4 连接、window=50、**没有应用 Credit/持久化**，仅证明基础 socket/模拟器有较高余量 |
| 完整 E2E staging bind execute P95 | 约 1,965–2,136 ms | **统计语义/进程汇总口径必须先审计**，不能与此前 8–16ms 直接认定为同类回归 |
| 完整 5K Bind lifecycle | 每轮 43–48 次，最多 4 个同时活动 | 连接可能频繁建立/关闭；无连接泄漏或错误证据，需量化时间占比 |

**明确停止重复猜测：** 现阶段不再围绕网络故障、PyArrow、大 Batch 或旧新版源码做无目标长跑，也不把配置上限 800 当作当前已具备的稳定容量。

工程约束：

1. 阅读根目录 `AGENTS.md`；若存在 `.codegraph/`，先使用 `codegraph explore` 定位代码。当前需重点审计 `backend/vnext/route_credits.py`、`smpp_transport.py`、`worker.py`、`projection_writer.py`、`critical_path_profiler.py`，及 `backend/tools/vnext_streaming_acceptance.py`、`benchmark_smsc_simulator.py`。
2. 保留不相关的 dirty/untracked 文件；**不自行 commit/push、部署或修改生产实例**。运行结果、数据库、日志、压测数据、凭据不提交 Git。所有测试仅授权 SLES 12 SP5 服务器本地 127.0.0.1 synthetic loopback，严禁真实号码/真实 SMSC/生产库。
3. 默认生产参数始终保留：**单 ProjectionWriter、单 DuckDB owner、A0/vectorized、projection batch 512、Journal 逐事件 durability、dedup、at-most-once、跨日不重发**。任何对比“去掉 Journal/数据库/限速”只允许在**隔离的诊断工具**中发生，不能作为真实生产代码路径或上线建议。
4. 性能测试以 SLES 服务器为准；Windows/AlmaLinux WSL 仅负责测试工具开发、正确性/回归。远端采用独立于 SSH 会话的 runner；清楚记录 pid、exit code、UTC 起止时间、源码和依赖 hash。
5. 尽量复用已存在的工具与数据。**先检查当前代码是否已经实现某项功能，不重复造工具**。优先静态分析、5K 小样本、单变量 A/B；没有清晰收益不运行 50K 或 600 秒。
6. 如果服务器磁盘空间、CPU 调度环境或权限不足，及时停止并明确 `BLOCKED_ENV`；不得为了达标修改全局系统配置或占用其他业务资源。

---

## R0 — 源码与统计口径审计（优先，暂不新增性能机制）

### R0.1 先确认分层实验可实际构造

梳理现存工具、以下关键链路和边界，给出代码符号、配置、进程/线程以及每段的具体计时起止点：

```text
Synthetic feeder → Ingress/Spool → Worker scheduling/checkpoint
                                     ↓
                         RouteCreditBroker.acquire
                                     ↓
                         SMPP window.acquire
                                     ↓
                        SMPP send / response reader
                                     ↓
                         response callback / Journal fsync
                                     ↓
                       ProjectionWriter journal read
                                     ↓
                       DuckDB stage / aggregate / commit
                                     ↓
                       durable offsets / reclaim / status
```

尤其核对 `ShardWorker.run_until_idle(close_transports=True)` 的默认语义：Harness 是否每次 bounded worker pass 都关闭 Transport？已存在的 `drain_transports()` 能否与下一轮 pass 共用物理 Bind？**先测每次 close/rebind 的真实时长和次数，不先把默认值改为 False。**

### R0.2 核验 staging 的“2 秒 P95”到底是什么

- 检查 `ProjectionWriter._stage_rows()` 内 `staging_bind_execute` 的 `perf_counter_ns` 包围范围及 `DuckDB.execute(UNNEST(list_zip(...)))` 的耗时；区分 SQL 执行、OS 调度挂起、锁/磁盘等待、CPU 占用。
- 检查 `CriticalPathProfiler` 的 `deque(maxlen=4096)` 与 `_aggregate_profile_snapshots()`：当前的 `p95_ms_max` 可能是**各进程各自 P95 的最大值**，绝非全部 batch 的 pooled/global P95；样本上限也会影响样本代表性。确认每一个报告值的原始角色、样本数、batch size、合并方式，避免误读。
- 对比 Writer 独立 benchmark、E2E 的 `staging_total`、`batch_end_to_end`、`commit`、`batch_count`，明确**wall-clock 与累计 per-worker 时间不能相加**。不能把单次两秒与历史 8ms 直接推导为稳定退化。
- 若旧结果仅存聚合 P95、无法恢复原始样本，标注 `NOT_COMPARABLE`，不要凭推测修 SQL。

### R0.3 冻结实验条件

在**相同 SLES server/native ext4** 上固定：

| 配置 | 固定值 |
|---|---|
| 当前产品基线 | `3a9a1a4e` 指向的产品后端（其上次产品修改为 `3e73ab80`） |
| Python/DuckDB | 采用当前匹配 SLES 的冻结环境，记录版本和 hash |
| workers / bind pool | 8 / 4（在后续隔离层也明确各层真实并发数） |
| credit | 200 PDU/s **per-bind**，上限理论合计 800 |
| SMPP window | 50 |
| campaign / workload | 4 campaigns、single part；5K = 1,250 × 4；固定 seed/消息大小/内容分布 |
| producer batch / projection batch | 250 / 512（相关的层） |
| status interval / instrumentation | 0.1s / ON（相同所有可比层；诊断开销可单独 OFF/ON 成对） |
| response delay | 0，simulation-local 127.0.0.1 |
| A/B 执行计划 | 所有正式层同一次 SLES 宿主、同版本依赖、同磁盘类型、同结果计时口径 |

收集：`uname`, `os-release`, `findmnt/df -T`, `df -h`, `loadavg`, 可读取的 `/proc/stat`, CPU affinity、nice、cgroup 可见限制、iostat（已可用则使用，不能安装时不强制）、CPU steal/iowait、服务器磁盘空间和周围其他业务负载迹象。实验尽可能在相对安静的窗口；如无法隔离主机，也应保证紧邻 AB/BA 交叉运行，不把 loadavg 变化假定为根因。

**R0 输出：** `R0_SOURCE_AND_METRIC_SEMANTICS=VERIFIED|GAP`、准确分层连接图、已有工具复用表、测试空间和限额安全检查。

---

## R1 — 构造最小、可比较的分层链路（核心任务）

**采用复用原组件的独立诊断 harness，严禁在生产路径加“跳过安全/持久化”的开关。** 需要时只增加 1 个小型测试脚本与定向测试；不要建立一个新服务框架。各层必须使用一致的 synthetic workload 和吞吐计时口径。

| 层 | 保留的真正组件 | 仅在该层测试中移除/替代的组件 | 观察重点 |
|---|---|---|---|
| **L0** 已有基线 | `benchmark_smsc_simulator.py` + SMPP simulator/socket | 应用限速、Worker/Spool、Journal、Projection | Socket/SMSC 上限；直接复用 14,240 的历史证据，**默认不重跑** |
| **L1** | `SmppTransport` + 真实 `RouteCreditBroker` + 真实 Window + simulator | Worker/Spool、Journal、Projection；response sink 为轻量内存确认（明确 synthetic-only） | credit 等待、window 等待、Transport/response reader、Bind 的基本表现 |
| **L2** | L1 + 真实 Worker + Ingress/Spool、真实 dispatch checkpoint、Worker 分组调度 | 持久 ProgressJournal、Projection（sink 仅诊断记录，不能用来代表生产可靠性） | Worker 调度、cursor/checkpoint、重复 Bind 造成的增量 |
| **L3** | L2 + **真实 ProgressJournal append/fsync** + durable response callback | Projection DB 和其 offsets/reclaim；诊断工具保留 Journal，不删除原始数据 | Journal fsync、回调同步等待、response reader 背压、持久响应成本 |
| **L4** | 当前完整 `vnext_streaming_acceptance.py` 生产同语义端到端链路 | 无 | Projection staging/commit、DB 读写、offset、最终 drain，以及完整正确性 |

实现要求：

- L1/L2/L3 使用**同一个生产类的实际方法**，而不是复制一个不同的 SMPP Client；只有 L0 用独立 socket 实现，作为历史基础能力参考。若现有类的依赖组合无法安全在诊断工具中独立运行，优先使用最小 **composition/test fixture**，不得直接改生产函数以绕开写入语义。
- L1 必须记录 `per_bind_credit_consumed/granted/wasted/refill_cost`、实际 TPS、window inflight peak、响应延迟、window wait、每 Bind socket/resp，确认不突破 4×200。不要改成关闭 credit 后宣称 800 达标。
- L2 要保留真实 Worker 的 cursor/checkpoint、`task_taken` 和 at-most-once 行为；没有 Journal 的该层**只作诊断，不能宣称 crash durability PASS**。防重复派发、业务日截止的正常语义仍应按照原 Worker 路径运行。
- L3 durable Journal 不可 silent-drop event；响应计数与已落盘事件一致。其 Projection 暂停/缺席不应触发生产业务的 replay 或重发；只能运行于隔离 run root。
- L4 必须确认 5K 单段：accepted/socket/PDU/resp 各 5,000；Journal/Projection 各 10,000；unknown/duplicate/write_failed 各 0；Journal backlog=0，durable offsets 到 EOF。历史 T6 和现有 T7 六项 PASS 不等于本轮新增层的故障验证。
- 记录每层的 `client_sent`, `simulator_received`, `responses_received`, `durable_response_event`, `db_committed_events`，缺失项用 `N/A` 不得写 0；阶段间所计对象不同必须标注。不用生产故障/恢复测试去验仅供诊断的无 Journal L1/L2。
- 不做每 PDU 的同步日志、JSON/fsync profiling、全量 trace。只用 bounded counters/histograms、低频 CSV/JSON，ON/OFF 配对检查观测开销。

### R1.1 计时口径（所有结果必须统一）

```text
stage_active_pdu_rate = successful socket_written PDU / (last_socket_write - first_socket_write)
response_phase_pdu_rate = successful response PDU / (last_response - first_socket_write)
e2e_wall_pdu_rate = successful socket_written PDU / (test_end - test_start)
projection_committed_events_rate = committed progress events / relevant observation interval
```

保留这些指标，但**主要分层定位比较 `stage_active_pdu_rate` 与 `response_phase_pdu_rate`**；阶段没有 producer/Spool 的时候不强行比较 `full E2E wall`。所有层使用同一套吞吐计时实现，输出每层 `start/stop`, first/last PDU/response, read/write timestamps 和实际样本数量。5K 短测试启动/结束固定开销较大，需同时报告 warm phase 和 complete wall。

---

## R2 — SLES 短跑单变量隔离 A/B

1. 先运行 L1 `1K` smoke、L2 `1K`、L3 `1K`、L4 `1K`，任何计数/错误/unknown 异常则停止、修诊断工具而不是调生产参数。
2. 然后每层用相同 `5K`，**先一轮 L1→L2→L3→L4 预筛**。只要某一步由高吞吐骤降至很低吞吐，围绕该边界做 **至少 3 组紧邻 AB/BA 交叉复测**，而不是机械地对所有层进行 50K 长跑。
3. 若某层明显退化，优先做**单变量**的附加诊断：
   - **L1→L2**：Worker pass close/rebind 是否主导？记录每 Bind handshake/lease wait/active time、close/reopen 费用、pass 数量，衡量 `run_until_idle(close_transports=True)` 被循环调用的频率。只在隔离诊断环境中做一次“维持 Transport 直到当前测试全部发送/响应排空后再关闭”的对照；确保先 drain response、Bind ≤4、没有 duplicate，并且**不直接推广生产**。
   - **L2→L3**：Journal append/fsync、response callback 持久化是否导致 reader 串行等待和 window 阻塞？分离 callback elapsed、fsync actual time、response receive→durable lag；回调耗时变长才考虑异步持久化设计，**不得跳过必须持久的响应事件**。
   - **L3→L4**：DuckDB staging/aggregate/commit、Writer 单 owner、offset persist、status 刷新是否产生持续 backlog；配套采集 `batch actual p50/p95`、`staging_bind_execute`, `staging_total`, commit、lock/host iowait、durable offset tail。**仅当 staging 明确是产能瓶颈** 才重新评估微批、批大小或 SQL。
   - **L0→L1**：Credit Broker pacing + Window 的上限：默认 ceiling=800，任何短时 burst 必须与 lease/滑窗策略相容；记录 grant/consume、refill wall cost、per-bind 帧发送间隔和 `window_inflight`。只有稳定低于配置且无外部等待/慢响应时才怀疑实现效率。
4. 如果同一个层相邻轮次波动大，先检查共享 host CPU/iowait/调度、run-root 磁盘、仪表开销（ON/OFF），不要勉强选择“胜出层”。
5. **指标定义明确**：`stage_drop_ratio = downstream_stage_rate / upstream_stage_rate` 只是筛查线索。找单一 bottleneck 时必须结合 Window/credit/Bind/response/Journal/backlog 的时间证据确认，不得把两层实现的线程数/工作语义不同造成的自然差异称为根因。
6. 保留冻结的 8 workers、4 binds、50 Window、每 Bind 200 TPS，不用“关闭 TPS/widen Window”作正式优劣判定；可做额外 synthetic-only 上限测试，但需单独标明不同配置与意义。

### 进入长跑的限制

- 未找到可稳定复现的明显吞吐损失：`ROOT_CAUSE_INCONCLUSIVE` + `NO_CHANGE_WITH_EVIDENCE`；**停止**，无需凑齐 50K/600s。
- 找到唯一主要瓶颈且最小候选验证通过：在**有合理资源预算**时才做 10K（必要时单次 50K）同服务器 paired A/B。只有其完成正确性、稳定显著 E2E 收益且资源不过界，才讨论后续 600 秒/800 PDU/s 的正式任务。
- 如果是 host/harness 限制而不是业务代码：应停在 `HARNESS_OR_ENVIRONMENT_LIMITED`，不实施产品性能修复。

---

## R3 — 若根因确认，实施且仅实施局部修复

**这不是强制代码修改阶段。** 只在 R2 形成可重复因果证据时执行：

- 优先能避免重复 Bind 建立/减少无谓 pass 生命周期损耗的**最小改动**，但要证明 Window response 全部排空、关闭重启后无重复发送、租约仍公平、上限仍然是 4。不能创建长期未受控的 Bind；不能让跨业务日 `socket_written` 任务被重发。
- 若根因是响应回调同步 fsync 延迟，只允许先做**局部可控的诊断与成本界定**；任何真正改变 durable event 接收时序的产品修复必须保持响应真实落盘语义，并有故障注入支持，否则不实施。
- 若根因是 DuckDB staging，必须先处理统计语义、batch size 和 OS 调度/IO 差异；不准直接引入 PyArrow、大架构重构、多 Writer 或全局弱化 durability。
- 源码改动必须附定向单测：4 Bind Slot 限制、TPS 计数、response/unknown、断连/超时、跨日旧任务不重派发、Worker stop/drain、Journal durability/Projection offset。运行现有 T6、T7-4..T7-9（诚实报 6/6），无需补造不存在的 T7-1..3。
- 对 1K / 5K 做正确性和性能回归；至少 3 次邻接配对或等价稳定样本，报告中位数/范围、资源、数据对账，显示实际收益。不能用单次快轮直接切默认配置。

---

## R4 — 交付、决策与 Codex 输出

### 交付物（**最多三份**，避免再写过多相互重复报告）

1. `Docs/Marketing_Copilot_VNext_Layered_SMPP_Throughput_RCA.md`  
   R0 静态审计、准确的 staging P95 语义、`run_until_idle`/Bind 生命周期证据、分层测试定义、L0-L4 速率和差异因果分析。
2. `Docs/Marketing_Copilot_VNext_Layered_Performance_AB_and_Decision.md`  
   真实 SLES A/B、主机资源控制、是否采用单点最小修复、恢复/正确性、是否具备后续容量验收的条件；明确 `NO_CHANGE_WITH_EVIDENCE` 是合法结果。
3. `Docs/Marketing_Copilot_VNext_Layered_Throughput_Evidence.json`  
   source/依赖/hash/run-root、每层配置、计时口径、原始证据路径及 SHA256、p50/p95 样本数、分层吞吐和事件计数、可复现命令、gate 输出。

开发新增文件以最小隔离 Harness/测试为主，**不得为了交付报告数量人为引入生产复杂度**。原始运行文件保留到隔离服务器，不进入 Git。

### Exit gate

| Gate | PASS 标准 | 未通过处理 |
|---|---|---|
| G0 Environment | 代表性的 SLES、本机 loopback、资源和版本冻结，工作区未破坏 | `BLOCKED_ENV` |
| G1 Instrument semantics | SQL staging P95 / profiler / active & wall 的测量口径已确认 | `METRIC_SEMANTICS_GAP` |
| G2 Layer integrity | L1/L2/L3 有正确非丢失的 synthetic 计数；L4 5K 完整 Journal/Projection 对账 | FAIL 则停止 |
| G3 Attribution | 有至少 3 组能说明同一个阶段损失的交叉配对、足够资源证据 | 不充分则 `INCONCLUSIVE` |
| G4 No regression | 如有改动，T6/T7及专项测试通过、无 duplicate、无错误改变 | 回滚候选 |
| G5 Cost-effective improvement | 有足够证据说明产品路径端到端受益，而不是仅 isolated layer | 不获益则不改默认 |
| G6 Capacity | 600 秒/800 PDU/s 不是本任务强制门禁 | 未跑写 `NOT_RUN`，不可宣称 CANARY GO |

最终在终端和报告末尾提供如下机器可读摘要（**缺失一律 `NOT_MEASURED`，不得杜撰**）：

```text
BASELINE_HEAD=
SERVER_OS_FILESYSTEM=
SERVER_AUTHORIZED=
RUNNER_DETACHED_AND_CLEANED=
SERVER_HOST_LOAD_AND_IOWAIT=
APP_RSS_SAMPLE_VALID=
INSTRUMENTATION_COMPARABILITY=
STAGING_BIND_P95_ORIGIN_AND_AGGREGATION=
STAGING_2S_ANOMALY=CONFIRMED|METRIC_ARTIFACT|INCONCLUSIVE
BIND_LIFECYCLE_5K_AND_TIME_SHARE=
L0_EXISTING_REFERENCE_PDU_PER_SEC=14239.716
L1_CREDIT_TRANSPORT_STAGE_RATE=
L2_WORKER_SPOOL_STAGE_RATE=
L3_DURABLE_JOURNAL_STAGE_RATE=
L4_FULL_E2E_STAGE_RATE=
L4_FULL_E2E_WALL_RATE=
PRIMARY_DROP_BOUNDARY=NONE|L0_TO_L1|L1_TO_L2|L2_TO_L3|L3_TO_L4|INCONCLUSIVE
CAUSE_CONFIDENCE=
CREDIT_AND_WINDOW_CONFORMANCE=
APP_ROUTE_TPS_DEFAULT_CHANGED=NO
PROJECTION_DEFAULT_CHANGED=NO
DUPLICATE_PDU=
UNKNOWN_RESPONSE=
L4_JOURNAL_PROJECTION_RECONCILED=
T6_CROSS_DAY=
T7_EXISTING_6_OF_6=
PRODUCT_CODE_CHANGED=YES|NO
CANDIDATE_PAIRED_E2E_DELTA=
50K_RUN=NOT_RUN|PASS|FAIL
600S_800PDU=NOT_RUN
CANARY_READY=NO
FINAL_DECISION=ROOT_CAUSE_CONFIRMED|HARNESS_OR_ENVIRONMENT_LIMITED|ROOT_CAUSE_INCONCLUSIVE|NO_CHANGE_WITH_EVIDENCE|MINIMAL_FIX_VALIDATED|BLOCKED_ENV
COMMIT_PUSH=NOT_REQUESTED
```

**任务完成的判断不是“跑出 800 TPS”，而是以最少实验将高吞吐 L0 与低吞吐 E2E 之间的损失分配到可观察的应用阶段；若没有证据，就停止无效优化并真实报告不确定性。**
