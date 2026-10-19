# Marketing-Copilot VNext — 环节独立与背压暂停/恢复不变的定向性能优化（Codex 任务）

> Task ID: `MC-VNEXT-STAGE-INDEPENDENCE-BACKPRESSURE-SAFE-PERF-20261019`
>
> 类型：架构不变量保护 + 性能根因定位 + 最小优化 + 受控验收
>
> 代码仓库：`https://github.com/shutu-ai/Marketing-Copilot`
>
> 起始已验证远端基线：`origin/main = 86b1bee4a5fcbc005f43b4382c83dfe0781db14d`。**执行前重新核实 HEAD**；如后续已有提交，记录差异，不得悄悄用旧代码覆盖新实现。
>
> 目标平台：SLES 12 SP5，CPython 3.11、DuckDB 1.2.0、native ext4。正式性能证据必须在授权测试服务器、隔离 run-root 上获取。
>
> 运行约束：只用 synthetic Loopback SMSC（127.0.0.1）；**不连接真实运营商 SMSC，不真实发短信、不修改生产数据或部署生产服务**。

## 0. 最终目标与不可妥协的原则

在**保持现有 VNext 管线架构、阶段之间独立、既有背压及自动暂停/恢复、人工暂停/继续、durable checkpoint、at-most-once、T6/T7 恢复逻辑不变**的前提下，优化 8 Worker 下的 **Worker/TaskSpool 内部无效开销**和/或 **ProjectionWriter 的已证实长尾开销**，提高真实 E2E 吞吐。

### 0.1 成功不等于“速度提高”

只要以下任一不变量回归，**即使吞吐翻倍也必须判定 `REJECTED_INVARIANT_REGRESSION`，不得推广补丁**：

1. **模块独立性**：Ingress、持久化 TaskSpool、ShardWorker/SMPP、durable ProgressJournal、独立 ProjectionWriter/DuckDB 的职责、所有权与故障隔离保持原状。不得直接让上游同步等待下游数据库事务以实现伪“优化”；不得合并成一个超级 Worker/单进程直通管线。
2. **有界背压**：每个已实现的容量/信用/水位上限、阻断原因及背压返回/等待语义保持原样。下游堵塞时上游仅能在其自己的已允许缓冲预算内继续推进，到达边界后必须停；不得把有界队列改成无界缓存。
3. **自动暂停/恢复**：下游释放容量后，其上游依既有策略自动恢复，不依赖重启、手动修改 Cursor、清空 Journal/Spool、重新发起 Campaign 或后台“强制 kick”。`StageCredit` 的 high/low-water hysteresis 和 wakeup 不得绕过。
4. **人工暂停/继续及取消**：已暂停 Campaign 不越过原有 socket/write fence；恢复后不重复投递已 taken 的任务；取消及业务日过期语义不变。**人工 pause/resume 与容量背压 auto block/resume 必须分别验收**。
5. **durable 先后关系**：保留 Spool `dispatch_taken`/checkpoint 在 socket write 之前，Journal append/flush/fsync、Projection 单 Writer 的数据库事务及 committed offset 先后关系；不得将 durable ack 提前到真实持久化之前。
6. **at-most-once**：连接超时、未知 SMSC 响应、Crash Recovery 时不能主动重发已越过 dispatch fence 的 PDU；允许保守丢失风险优先于重复发送，不能通过放松恢复语义“提高吞吐”。
7. **容量与限流**：固定 **8 Worker、4 physical Bind、每 Bind 200 TPS、`tps_scope=per_bind`、Window=50**，总配置 ceiling=800 PDU/s；不改变默认 Projection batch=512/vectorized、status interval（正式测试使用现有冻结值）、资源硬限制与磁盘预算。
8. **独立故障/恢复**：Projection 暂停/滞后不能使已确认的 Spool/Journal 数据消失；恢复后从 durable offset 推进；Ingress/Worker 不得未经可用容量确认而越过已经生效的背压边界。
9. **公平、隔离与可回滚**：不破坏多 Campaign 公平调度、256 virtual partitions 的稳定映射、单 Writer 所有权、跨日业务日及恢复约束；任何补丁能够单独回滚。

**注意**：这是对**当前代码实际已实现的合同**做保持与强化验证，不得臆测未接线的控制策略已在生产启用。若发现某一级仅有 `StageCredit` 类、可选 `ingress_credit`、`journal_admission` 接口而调用入口未实际使用，须明确记为 `EXISTING_WIRING_GAP`，不能为满足本次性能任务私自引入跨阶段架构改造；先报告，除非与本次确认的性能/语义 bug 直接相关且有最小安全修复依据。

## 1. 已知证据和本轮不再重复追踪的方向

- 最新 Worker 生命周期提交：`86b1bee4`。修复仅在测试 Harness 将外部 `stop_event` 映射到 `ShardWorker.stop()`、停机使用完整 `spool.status()`，并增加 heartbeat/forensics 和全局 join deadline；生产 `backend/vnext` 未修改。
- 8/12/16/24 Workers 的 1K 各 2 轮、5K 反序矩阵全部正确性 PASS。24 vs 8：`stage_sampled=-20.4%`，`E2E=-18.0%`。**已证实堆 Worker 不是本场景可行优化**。本轮固定 8 Workers，不再测试 12/16/24/32。
- 既有分层：L0 Loopback≈14239.716；L1 `SMPP+Credit+Window`≈793.045；L2 单 Worker bounded-pass≈260.860；L3 增加逐事件 durable Journal≈197.140 PDU/s。L1→L2 是可复现边界，但不是“仅连接重建”造成的；keep-transport 对照无收益。
- 既有 L4 测试曾出现 `staging_bind_execute` 单进程 P95≈2055ms（23 batches），Projection/offset drain≈31.62s。**这不是全局 pooled P95，且来自另一轮 workload**，不得直接宣称当前 HEAD 的 Projection 已回归或必然是首要瓶颈。
- 最新 8 Worker 5K（两轮中位数）：stage **sampled**≈153.07 PDU/s，E2E≈110.34 PDU/s。这与旧 L4 **first/last socket** stage≈384.39 的定义/环境不同，不能直接做性能回归结论。
- 现阶段 `CANARY_READY=NO`，600 秒/800 PDU/s 尚未验收。本轮不以达到 800 PDU/s 或上线为成功前提。

### 1.1 主要阅读范围

按仓库 `AGENTS.md` 执行。若有 `.codegraph/`，先用 CodeGraph 查符号及调用路径；保护其他 dirty/untracked 改动。

- `backend/vnext/pressure.py`：`StageCredit` high/low-water hysteresis、reservation/release/notify、`ProgressWatchdog`。
- `backend/vnext/ingress.py`：`IngressRouter`、`ingress_credit`（可能为可选）、`enqueue_chunk()`、Spool admission。
- `backend/vnext/spool.py`：`TaskSpool.iter_from()`、`refresh()`、`checkpoint()`、`lightweight_snapshot()`、`status()`、capacity、cursor/taken persistence。
- `backend/vnext/worker.py`：`ShardWorker._seed_buffers()`、`run_until_idle()`、pause/resume、`journal_admission`（可能为可选）、dispatch fence、response drain。
- `backend/vnext/smpp_transport.py`、`route_credits.py`：固定 Bind/TPS/Window，**只做约束审查，原则上不改**。
- `backend/vnext/projection_writer.py`：ProgressJournal segmentation/容量、单独的 ProjectionWriter、`staging_bind_execute`、SQL/commit/offset/reclaim、ProjectionBackpressure。
- `backend/tools/vnext_streaming_acceptance.py`：真实 8-Worker E2E 测试 Harness、producer backpressure、完整 reconciliation、资源采样与分阶段时间线。
- `backend/tests/`：与 pressure、spool、worker、projection、recovery、campaign pause/cancel、T6/T7 有关的既有测试。
- `Docs/Marketing_Copilot_VNext_Layered_SMPP_Throughput_RCA.md`、`Docs/Marketing_Copilot_VNext_Worker_Clean_Exit_and_Scaling_Decision.md` 等现有报告。

## 2. R0 — 冻结条件与“实际连接图”审计（先执行，不改产品代码）

1. 记录 `git rev-parse HEAD`、`origin/main`、`git status --short`、实际运行代码与 Harness SHA256；不要动不相关文件，不做自动 commit/push。
2. 确认授权 SLES 测试机、native ext4、CPython 3.11、DuckDB 1.2.0、CPU/内存/磁盘/host load；优先复用已有测试软件包和离线依赖。不假设旧测试目录存在；通过服务器实际 `df -hT` 与可写权限确认。
3. 形成一张**真实调用链表**，每个阶段写明：生产入口、容量/水位阈值、谁触发 block、阻断后阻止哪个动作、谁解除阻断、是否持久化、停止/恢复时序、已有测试及缺失项。
4. 分清 **类具备能力** 与 **调用入口实际接通**：`StageCredit`、`IngressRouter(ingress_credit=...)`、`Worker(journal_admission=...)`、`ProjectionBackpressure`、Harness `max_pending_records` 是不同层级的机制；逐一追踪实际接线，不能合并成一句“全链路背压已覆盖”。
5. 为后续优化做独立性对照：指出哪些 Stage 独立运行/独立进程，哪些已允许暂时停滞，哪些队列必须 bounded，既有 resume watermark 何时成立。
6. 若已存在明显背压/暂停/恢复语义缺陷，建立独立问题记录：`PRE_EXISTING_CONTRACT_GAP`。本轮性能补丁不可偷偷改变合同，更不能用“提高性能”掩盖该缺陷。

**产出**：`Docs/Marketing_Copilot_VNext_Stage_Independence_and_Backpressure_Contract.md`，含证据/缺口标注。此文件即使最终不修改代码也须交付。

## 3. R1 — 冻结 8 Workers 当前性能基线（先找实测瓶颈）

统一参数：

```text
--workers 8
--pool-size 4
--route-tps 200 --tps-scope per_bind --window-size 50
--counts 1250,1250,1250,1250       # 5K, 4 campaigns
--batch-size 250
--projection-batch-size 512
--projection-staging-mode vectorized
--projection-status-interval 0.1    # 保持与本轮既有 5K 对照一致
--instrumentation on --profile-interval 1.0
--response-delay-ms 0
--resource-profile PROD_8G --rss-guard-scope application
```

执行要求：

- 测试在同一台 SLES 的**独立 native-ext4 run-root** 串行运行；保留统一 PYTHONPATH/环境/参数源 SHA，使用 detached runner 防止 SSH 断线影响进程；确认无遗留 Bind/进程。
- 用最新提交后的固定 8-Worker Harness 与真实 VNext 应用模块进行 1K 正确性 smoke，然后 5K baseline。**优先复用最近 8-Worker 5K 的可信原始证据**；若 SHA/测试口径/运行条件可比，避免无意义重复。
- 确认 E2E 入口、feeder、first/last **precise socket-write**（若可直接捕获）、response、Journal durable、Projection committed offset、完全清洁退出的时间边界。`stage_sampled`、`stage_exact`、`feed_window` 和 `e2e_wall` 必须分列，不得互相替换。
- 采样 Worker：任务排队时长、`_seed_buffers`/`iter_from`/`refresh`/`checkpoint`/Status 的调用次数与累计时间，idle/pass 次数、spool reads、Bind active 利用、TPS credit wait、Window wait、per-worker 任务分布；采集应为**低开销 bounded 指标**，不可每条短信同步刷新的深度日志。
- 采样 Projection：Journal consumed/committed events、backlog bytes/time、staging SQL batch count、per-batch actual duration、DuckDB transaction commit、Offset durability、drain 时间；保留 batch 原始样本或有限量 histogram，并标注 pooled 与 process-local 口径。
- 记录 CPU / iowait / filesystem util、application RSS、harness RSS、disk growth、resource guard 状态；不因“shared host”噪声做单轮定性。

**分析输出**：列出按 E2E wall 贡献排序的耗时路径，每项附数据来源和是否 **on critical path**。禁止把并发任务的等待时间简单累加成 wall 秒数。

## 4. R2 — 背压与暂停/恢复合同测试（优化前先建立硬门槛）

优先使用**可控小规模测试、隔离参数/故障注入**触发状态，不通过无界压力或大量数据填满服务器磁盘。保留实际生产语义与组件，允许 Harness 降低**测试专用**容量阈值以迅速触发边界；不得改生产默认阈值。

### BP-1 — Ingress/Spool 容量背压

- 测试专用小容量或 producer 高速短发，制造 `StageCredit` high-water / Spool max_bytes / `max_pending_records` 的**已实际接通的**对应触发点。
- 验证达到既定阈值后，生产者发生明确 block/wait/reject（按当前合同）而不是继续无界入队或静默 drop；未 accepted 的任务不得冒充 accepted；其他可正常工作的模块继续独立处理既有任务。
- 消费释放容量后按照 existing low-water/通知机制恢复，**无需人工干预**；最终入队 accepted 与 Spool record 一致。

### BP-2 — Journal / Projection 下游背压与恢复

- 以受控方式暂停/放慢 ProjectionWriter 或限制测试专用 Journal 容量，观察 Journal 增长、容量保护和 Producer/Worker 按**实际存在的传播机制**最终停在自己的预算边界；不得无限积压。
- 恢复 Projection 后，验证按 durable journal segment/offset 继续消费，并正确 reclaim 已完整提交且允许回收的段；未提交段不可提前删除。
- 最终 Projection committed events、unique rows、Journal offset EOF 一致；背压解除后恢复持续推进，无重复、无乱序确认、无须清空/重启。
- **避免人为“通过”**：如果该运行路径原来没有从 Journal 背压传播到 Worker 的 admission wiring，客观记录既有合同范围与 `EXISTING_WIRING_GAP`；不得默认为已支持，亦不得单纯为了本任务而广泛重构。

### BP-3 — 人工 Campaign 暂停/继续/取消（独立于 BP-1/2）

- 给至少两个 Campaign：暂停其中一个，另一个继续。确认暂停项在受控 race/fence 下不产生新的非法 socket write，其他 Campaign 不被全局锁误阻断。
- 恢复被暂停 Campaign，确认剩余合法任务按原 durable cursor 推进、无重复；取消和业务日过期分别遵循既有语义。
- 同时发生人工暂停及容量背压时，两者的 gate 独立生效；容量恢复不应自动取消人工暂停，人工 resume 亦不能绕开仍生效的容量背压。

### BP-4 — 崩溃/重启与 at-most-once

- 分别在 Spool admission/dispatch checkpoint 后、Journal durable 后、Projection commit/offset 边界注入受控失败（只在 synthetic test 环境）。
- 验证恢复使用原始 durable cursor/Journals/offset；已送往 socket 或结果 unknown 的任务不被擅自重新发送；尚未 take 的合法任务可按既有策略继续。
- 断点恢复后追平 backlog，数据正确、状态可解释、没有隐藏的人工“修复”步骤。

### BP-5 — 独立环节、限额与暂停状态的观测

- `StageCredit` block reason/wake/resume、Spool pending/size、Journal capacity/segment/offset、Projection backlog/queue、Worker paused/cancelled、RouteCredit/Bind slots 均能与状态变化对应。
- 验证 `max_active_binds <= 4`、per-bind 200 TPS 节流、Window=50 与 TaskSpool/Journal/Projection 硬限制，且全部测试没有残留进程、lease、测试设备/数据库。
- 为故障注入用例增加回归测试，不能用只检查函数返回值的 mock 代替关键持久化/恢复验证；现有 T6/T7 相关测试需保留通过。

如现有 StageCredit/Producer 的测试环境难以直接触发某一级，应分层建立真实组件的薄 Harness，记录为何与正式运行路径对应；不可修改生产代码来造一个假想合同。

**产出**：每项 `PASS / FAIL_PRE_EXISTING / FAIL_NEW / NOT_APPLICABLE_WITH_REASON / BLOCKED_ENV`，并给出 actual thresholds、stop/resume timestamps、accepted/pending/committed counts、SHA、证据路径。

## 5. R3 — 仅对已确认热点实施最小、可回滚优化

### 候选 A：8 Workers 的 TaskSpool / 调度热点

只有 R1 证实真实关键路径开销，才允许择一或少数**互相独立**的候选：

- 减少没有新 durable generation 时的重复 `iter_from()` / segment CRC 重扫、重复全局快照、重复 per-partition cursor 文件 stat/read 或无效 polling；保持缓存失效条件明确，不能跨 business day 或重启信任过时数据。
- 有真实调用次数/时延证据才调整读者缓存、分区选择或小范围任务批处理。不能跳过 `_eligible()` pause/cancel/business-day 检查；不能把 `TaskSpool.checkpoint()` 延后至 socket write 之后。
- 禁止将多个分区/收件人的进度持久化合并成不安全的统一 Cursor；禁止为减少 fsync 将 taken/checkpoint 改成纯内存确认。
- 不增加 Worker、Bind、Window、不把 per-bind credit 换成未验证的全局逻辑、不重写连接池。

### 候选 B：单 ProjectionWriter / DuckDB 尾部

只有 R1 在本轮同配置证明其位于 critical path，才允许优化：

- 先定位 vectorized staging 单批约 2s 的**当前** SQL/事务/磁盘时间来源；区分 `staging_bind_execute`、commit、offset persistence、Journal reading 和 status scheduling。
- 优先消除可证明的多余 staging scan、重复 binding/SQL 执行、无效对象创建或重复数据复制，保持相同 event IR、DuckDB 单 Writer、同等 transaction/offset durability。
- 不通过增大默认 batch、取消 unique/event 去重、跳过 fsync、先提交 offset 后 DB commit、加第二 DuckDB writer 或无限扩大 queue 换取吞吐。
- 若候选 `executemany` 在既有 A/B 无优化收益，不重复推广；有新对比假设才允许复测。

### 变更预算与退出条件

- 每个候选须在改动前写明：`root_cause_evidence`、`critical_path_share`、`source_lines`、`minimal_patch`、`revert_plan` 和不可变条件。
- 优先 **不触碰** `route_credits.py`、`smpp_transport.py`、`pressure.py` 对外协议、PartitionMap 所有权、Journal schema。
- 若需要改变上下游 API、数据结构或暂停/恢复状态机，**停止在 RCA 阶段**，不能借性能任务做架构重构。
- 最多以一个清晰、独立、可回滚的生产补丁为初始候选；若无正收益/证据不足，明确 `NO_CHANGE_WITH_EVIDENCE`。

## 6. R4 — 正式 A/B、正确性和背压回归

1. 在**同平台、同依赖、同 workload、同 flags、同 instrumentation**下将未修改 baseline 与候选用交叉顺序执行 5K，例如 `A→B` 再 `B→A`，运行相互隔离，避免共享 state/offset/lease；如共享服务器负载干扰明显，优先缩小并重新配对，不是盲目增加次数。
2. 完整记录 `E2E wall PDU/s`、精确 `stage_active PDU/s`（若可得）、sampled stage、feed time、SMPP response/Journaling/Projection drain、Peak backlog、Peak RSS、CPU/iowait；所有量分口径报告。
3. 每轮必须检查：`accepted=socket_written=loopback_received=responses=5000`（单 part 情况下）；Journal durable=10000、Projection unique/committed=10000、offset EOF；duplicate/unknown/write_failed=0；8/8 Worker 结果/clean exit；max Bind<=4；无残留资源。
4. 候选必须重新执行 **BP-1 至 BP-5**（或对受影响边界全量重跑并对其他边界保留明确基线回归）；一个 hard invariant FAIL 即拒绝。
5. 只有 `E2E` 中位数提升达到**至少 10% 且大于测试噪声**，或经过清晰证据证明在稳定负载下能显著减少核心瓶颈，而其他关键阶段无显著回归时，才建议进入进一步确认。对照下 stage 无收益而 E2E 受益时必须如实说明原因。
6. 若 5K 证据均 PASS、disk/RSS 预算足够、用户授权且候选存在可靠收益，**可条件性做一次 50K** 用于验证收益不随规模消失和 backlog 不持续增长。否则 `50K=NOT_RUN_BY_GATE`；**不自动执行 600s/800 PDU 稳态，也不做生产 Canary**。
7. 既有后端 pytest 全量必须运行（已知基准曾为 `447 passed, 5 skipped`，不将历史次数当作当前一定相同）；重要的背压/人工暂停/恢复/重启/性能小回归测试须单独可复跑。
8. 任何临时的测试专用容量、故障注入或观测开关，不得改变生产默认路径；安全清理仅限本轮明确创建的 run-root。

### 6.1 关键失败/退出决策

| 情况 | 最终决策 |
|---|---|
| 背压、人工暂停、恢复、durability、at-most-once 任一回归 | `REJECTED_INVARIANT_REGRESSION` |
| 修复正确但 E2E/关键路径未见可靠收益 | `NO_CHANGE_WITH_EVIDENCE`；不推广候选 |
| 只有测量脚本变快，产品关键路径未证实改善 | `HARNESS_ONLY_GAIN` |
| 组件合同原本未接线或已有功能缺口 | `PRE_EXISTING_CONTRACT_GAP`；单独记录，不伪装优化成果 |
| 5K 全面 PASS，收益稳定，语义不变 | `CANDIDATE_VALIDATED_5K`，仍非生产 Canary |
| 条件性 50K 也验证持续正确与收益 | `CANDIDATE_VALIDATED_50K`，仍需后续容量/Canary 门禁 |
| 缺真实机器、依赖、权限或测试路径 | `BLOCKED_ENV`，不得写 PASS |

## 7. 机器证据与报告（尽量精简但可复现）

本任务在 `Docs/` 下形成以下输出，不新增大量零散总结：

1. `Marketing_Copilot_VNext_Stage_Independence_and_Backpressure_Contract.md`：实际组件连接图、背压状态机与门禁表、生产/测试 wiring 差异和既有 gap。
2. `Marketing_Copilot_VNext_8W_Spool_Projection_Targeted_Performance_RCA_and_AB.md`：冻结参数、baseline、critical path、候选 patch diff、A/B 指标、完整错误/资源/背压判定、`NO_CHANGE` 或 candidate 决策。
3. `Marketing_Copilot_VNext_Backpressure_Safe_Performance_Evidence.json`：机器可读摘要；正式 run JSON/SHA 指针与源代码 commit、环境版本、参数、两轮结果、BP1–BP5、最终结论。

JSON 至少包含：

```json
{
  "task_id": "MC-VNEXT-STAGE-INDEPENDENCE-BACKPRESSURE-SAFE-PERF-20261019",
  "source_head": "ACTUAL_SHA",
  "target_server_os": "SLES12SP5",
  "filesystem": "native_ext4_or_NOT_VERIFIED",
  "workers": 8,
  "physical_bind_limit": 4,
  "tps_per_bind": 200,
  "window": 50,
  "production_defaults_changed": false,
  "stage_independence_preserved": "PASS_OR_FAIL_OR_NOT_VERIFIED",
  "backpressure_wiring_audited": "PASS_WITH_SCOPE_OR_GAP",
  "auto_backpressure_pause_resume": "PASS_OR_FAIL_OR_NOT_VERIFIED",
  "manual_pause_resume_cancel": "PASS_OR_FAIL_OR_NOT_VERIFIED",
  "durable_restart_at_most_once": "PASS_OR_FAIL_OR_NOT_VERIFIED",
  "journal_projection_offsets_reconciled": "PASS_OR_FAIL_OR_NOT_VERIFIED",
  "spool_workload_and_projection_critical_path": "EVIDENCE_OR_NOT_VERIFIED",
  "paired_five_k": "PASS_OR_NOT_RUN_OR_FAIL",
  "fifty_k": "NOT_RUN_BY_GATE_OR_PASS_OR_FAIL",
  "six_hundred_seconds_800pdu": "NOT_RUN",
  "canary_ready": "NO",
  "product_code_changed": "YES_OR_NO",
  "decision": "NO_CHANGE_WITH_EVIDENCE_OR_CANDIDATE_VALIDATED_5K_OR_CANDIDATE_VALIDATED_50K_OR_REJECTED_INVARIANT_REGRESSION_OR_PRE_EXISTING_CONTRACT_GAP_OR_BLOCKED_ENV",
  "commit_push": "NOT_REQUESTED"
}
```

JSON 值不可用 `PASS` 代替未执行；每个结论须能回溯到 run ID、结果 hash、关键日志/CSV、代码差异/测试名。报告中明确保留本轮是否生产代码修改；最终产物要求 `python -m json.tool`、`git diff --check`、依赖与敏感信息扫描通过。

## 8. Codex 操作与交付要求

- 从当前工作区开始，**保护全部无关 dirty/untracked 修改**；禁止 `git reset --hard`、删除既有用户数据、清空历史运行目录或覆盖生产配置。
- 遵循 `AGENTS.md`；必要时使用 CodeGraph，先确认改动影响面。
- 有明确证据时可改**必要的** `backend/vnext` 产品热点代码，但只能是小补丁，且必须有 BP-1~BP-5 不变量回归；不要为了性能新增复杂框架、跨阶段中央协调器、审批/RBAC 或多级安全网。
- 不连接真实 SMSC，不实际发送短信，不修改真实用户 Campaign/DB；服务器授权范围以现有任务为准。测试中断需清理本轮进程/lease，不扩大磁盘消耗。
- 本轮**不 commit，不 push，不部署**，除非我在任务完成后另行明确要求。
- Codex 最终答复请使用下列结构：

```text
SOURCE_HEAD=...
PRIMARY_BOTTLENECK=...
ROOT_CAUSE_CONFIDENCE=...
STAGE_INDEPENDENCE=PASS/FAIL/NOT_VERIFIED
AUTO_BACKPRESSURE_PAUSE_RESUME=PASS/FAIL/NOT_VERIFIED
MANUAL_PAUSE_RESUME=PASS/FAIL/NOT_VERIFIED
CRASH_RECOVERY_AT_MOST_ONCE=PASS/FAIL/NOT_VERIFIED
SMPP_Bind_TPS_Window=4/200/50_UNCHANGED
BASELINE_5K_E2E=...
CANDIDATE_5K_E2E=...
PAIRED_E2E_DELTA_PERCENT=...
PROJECTION_DRAIN_DELTA=...
FIVE_K_RECONCILIATION=...
FULL_BACKEND_TESTS=...
PRODUCT_CODE_CHANGED=YES/NO
50K=PASS/FAIL/NOT_RUN_BY_GATE
600S_800PDU=NOT_RUN
CANARY_READY=NO
FINAL_DECISION=...
REPORTS=...
COMMIT_PUSH=NOT_REQUESTED
```

> **最终原则：保留原有独立流水线及背压/暂停/恢复的能力和时序，只消除各环节内部被证明确属多余的 CPU/I/O/锁/调度开销。不得拿正确性、durability 或架构隔离换吞吐。**
