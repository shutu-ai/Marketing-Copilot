# Marketing Copilot — S1 有界多 Campaign 派发与共同安全合同

**Task ID**: `MC-SCHED-S1-BOUNDED-DISPATCH-20261015`  
**阶段定位**: S1 实施；承接已关闭的 S0 + S0.1，下一阶段为 S2 Route/Slot 实际发送公平。  
**Codex 推荐模型**: `gpt-6.1-sol` / `high`。遇到无法解释的竞态、跨阶段恢复漏洞或架构性死锁时，停止扩大修改范围，单独使用 `xhigh` 进行审查。

## 0. 前置阅读与源代码基线

开始前完整阅读并核对实际代码（不以文档代替源码）：

1. `AGENTS.md`。
2. `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`，尤其 §4、§5、§6 DEC01–DEC08、§7 S1、§8 T04/T06/T07/T11/T13。
3. `Docs/Marketing_Copilot_S0_Correctness_Baseline_Closure_Report.md`。
4. `Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md`。
5. 当前代码中的 `backend/database.py`、`services/task_worker.py`、`services/sms_task_service.py`、`services/campaign_capacity_service.py`、`services/backpressure_service.py`、`services/smpp_gateway.py`、`services/fast_send_service.py`、`services/sms_write_buffer.py`、`services/result_journal_service.py`、以及 Batch/Realtime/可选 RabbitMQ 相关发送入口与测试。

核对当前工作区 `HEAD`、`origin/main`、差异及未跟踪资料。预期上游 Git SHA：`91a8cc299c5c327091e03fd82d0723ab3e48b79e`；如代码已继续演进，必须以真实 HEAD 为准，记录差异及影响，**不得盲目 reset/checkout 覆盖用户修改**。

最新已确认门禁：`S0.1_READY`、`P0_CORRECTNESS_READY=YES`。`G3=NO_GO/NOT_VERIFIED`、生产仍 `NO_GO`。S0.1 已通过独立 OCC+Fast 1K ×3，但这不等于四组合 G3 通过。S0.1 报告记录的是提交前测试快照，后续推送提交为 `91a8cc29`；后续验收必须以**最终源码快照与 manifest**重新取证。

## 1. 本轮目标（只做 S1）

在**单服务进程、单 DuckDB native database owner、独立 cursor 事务、共享 File/SMS Worker 池**的既有架构中，实现：

- **Claim 之前先有容量**：任务留在 DuckDB 权威持久队列；没有容量时不能先大量 Claim 再阻塞进程内 FIFO。
- **按活动 × 已解析 Route 分组的有界 ready/prepare**：`(campaign_type, campaign_id, resolved_route_id)`；调度缓存是有界的、可丢失的派生视图，绝不是新的业务权威存储。
- **局部背压与公共安全**：A 活动 ready 饱和时不得阻止有配额且可执行的 B/C 被认领、准备；DB、Journal、磁盘、维护等公共故障仍必须全局 fail-closed。
- **资源守恒及异常恢复**：所有权和 credit 生命周期清晰；Claim commit/publish 中途失败、取消、超时、停机、异常、迟到回调均不得丢任务、重复发送或双重释放容量。
- **兼容及灰度**：保留当前正常运行的 legacy 行为，新增 S1 fair dispatch 以配置显式开启；在 S2 端到端 Slot 公平验证前不得宣传“socket 完全隔离”。

S1 不是 Slot / socket 公平重构，也不是文件 Worker 扩容，更不是迁移数据库/引入外部 MQ。

## 2. 分步骤实施要求

### S1-A：冻结接口、审查旧入口、确立事务与锁合同

1. 画出当前：候选 → DB Claim → 内存 `_task_queue` → SMS prepare → `send_async` → Slot FIFO → socket → callback → Journal → DuckDB 的**实际**生命周期，并标注 Reliable/Fast 差异及直接同步发送、RabbitMQ（如启用）路径。
2. 找出所有绕过常规 dispatcher 的发送入口，逐个选择：接入公共容量/停止/结果安全合同，或保持原受控路径并**禁止将其算入 fair 支持范围**。若生产可能启用而未适配，S1 全入口门禁不得 PASS。
3. 明确 claim token、attempt token、generation、route revision、namespace、Journal event ID 以及 task/part 身份不可混用的界限。
4. 冻结锁顺序：调度内存锁内禁止 DB I/O、网络、fsync、阻塞 `put`、sleep；DB 事务内不反向获取 campaign/调度锁；网络/Slot condition 锁内不等待 DB/Journal。新增并发测试覆盖关键交错。
5. 冻结 S0.1 数据库 owner：**不允许退回每事务 `duckdb.connect()`/`close()`**，禁止新建第二个主库 RW Owner 或使用多进程向主库并发写入。

### S1-B：显式 feature flag 与分层 credit 账本

1. 新增进程启动时冻结的 `MC_CAMPAIGN_DISPATCH_MODE=legacy|fair`；**默认 `legacy`**。它不同于原有 `MC_FAIR_TASK_SCHEDULER_ENABLED`，不能将两者偷换。无效值 fail-closed 或明确拒绝启动，不静默改变发送语义。
2. 实现清晰、可测试、线程安全的 credit/reservation 接口。至少在 S1 实际使用路径覆盖：
   - `claim_ready`: task 数、预估 parts 和实际缓存 bytes（含 claimed、ready、prepare、hand-off 的同一任务引用）；
   - `prepare_active`: 同时进行的准备/编码工位及 bytes；
   - `campaign/route ready`: 每组与全局有界容量，避免单组占满共享池；
   - `result_pending` 的**安全容量规划与准入约束**：分析最坏分片/结果事件及现有 Journal 交接路径，不能仅以 `queue.put` 代表 durable fsync。不能安全落地的额外硬保障不得伪称已实现，要作为阻断/后续明确列出。
3. 唯一 reservation ID + owner epoch + 任务/claim/generation + 状态转移 + 幂等 release。所有权转移不是重复计费。证明 `acquired - released = live`、per-group 总账与 global 一致、`used <= hard limit`。
4. 暂用固定有限预算、简单公平/既有 DRR，禁止没有测量依据的自适应 PID、所谓保证最低 TPS。容量单位必须显式展示（task/part/byte）；后续 socket 真实 parts 与 PDU 窗口的硬预算属于 S2。
5. 不得因队列已满制造重复 DB Claim、快速 retry 风暴、attempt budget 消耗，或使用无限 Python list/heap 转移阻塞。

### S1-C：按 Campaign × resolved Route 的有界 ready/prepare 派发

1. 实现小而清晰的进程内 bounded ready pool/调度器；不为每个 Campaign 分配独立线程、进程、DB 或 SMPP 连接。
2. 在 Claim **之前**取得足量可追踪 reservation；保持单 Claim Dispatcher，避免多个 DB worker 无序争抢。Claim 事务短、有界，可在事务外完成调度选择；提交失败释放预留且不保留伪“已领取”缓存。
3. 承接既有 DRR 候选轮转；超过候选扫描窗口的新 Campaign 不能永久不可见。容量满的 A 留在 DB 或局部暂停，不能长时间占用 shared worker；B/C 应能继续被认领和准备（若共同底层资源健康且有 credit）。
4. Route 由真正生效配置/修订解析：`route=0` 不能作为物理容量池绕过额度；fallback/revision 更改时仅允许**可证明尚未写 socket**的工作描述符迁移，保留已发/未知 attempt 的证据。实际 socket 级 route/Slot arbiter 留待 S2。
5. SMS worker 准备完成后不能无界等 Slot FIFO、长网络等待占满所有通用 Worker。若受当前 `send_async` / Slot 入口限制，只能实现部分解耦，需明确可验证的 S1 边界，**不可以用扩大队列掩盖阻塞**。
6. 停机、pause/cancel、route 故障、prepare 抛错、queue 满、DB OCC 冲突、Claim 后 publish 前进程死亡都须保留原任务证据并安全回收 credit。恢复逻辑依靠 durable claim/intent/recovery provenance，而非内存状态或“没有成功响应”推测未发。

### S1-D：共同安全合同与所有入口兼容

1. 复用并继续通过 S0 的 R02（精确 chunk 枚举）、R03（Reliable/Fast 统一停止栅栏）及 R17（unknown/partial、receipt/attempt/part 关联）修复。**不得重新开放任何自动整条重发旁路**。
2. 保持已可能写出 socket 的 attempt/parts/Journal 证据，不承诺外部 exactly-once，不以 DLR disabled 的 accepted 冒充 delivered。
3. 在公平模式启用范围内核对 Batch/Realtime、Reliable/Fast、同步/异步 gateway、direct/API 和可选 RabbitMQ；没有证据的入口逐项列 `NOT_VERIFIED`，不伪造“全入口 PASS”。
4. 支持全局 DB maintenance、磁盘不足、Journal fsync/quarantine 不健康时停止新发送副作用，而已收到的回执可继续安全排空；单 Campaign 饱和不应变成全局停止。
5. 保持 API、schema、任务语义与旧数据兼容。原则上**不做数据库 DDL/Journal 格式改变**；如证明不可避免，先输出兼容与回滚 ADR，停止超范围实施，不要悄悄迁移。

### S1-E：可观测性与最小可执行验收

新增/补齐低基数指标或结构化诊断，至少包含：

- 当前 dispatch mode，按活动/route 的 eligible backlog、ready tasks/parts/bytes、oldest eligible age；
- `claim_reserved`、`claimed_not_admitted`、prepare active、容量阻断原因、credit acquired/released/live、overflow/double-release/expired；
- Claim/ready/prepare 等待及异常；Journal/DB/Slot 阻断原因；
- socket 层实际进展与活动级零进展持续时间，以便在 S2 区分“派发层通过”与“Slot HOL 未解决”。

指标不得以手机号、task-id、消息正文或 message-id 为高基数标签。不要因记录指标而引入 DB 热路径或长锁。

## 3. 测试与门禁（不得以测试文件存在代替执行）

必须在独立目录和 loopback/mock SMSC 上运行，保留命令、退出码、最终 SHA/源码 manifest、配置、原始日志及审计。延续 S0.1 的 WSL AlmaLinux 8 / ext4 / CPython 3.11 / DuckDB 1.2.0 经过验证的测试路径；Windows ACL 无法使用时采用干净 basetemp，不删除旧证据。

**一、保持正确性：**

- G0 完整后端回归 + frontend build，使用最终源码，不能仅引用旧的 `290 passed`。
- S0/S0.1 定向 OCC owner/变量宽度、full transaction retry、chunk/file lease、R03 stop（包含 part 间停止）、R17 DLR/unknown/partial。
- T01 ingest-only（1000 task, socket=0）、sender-only pool1/4、multipart 等可行回归；按变更影响重新运行 OCC+Fast 1K smoke 和守恒审计。**不把它叫 G3 四组合 PASS**。

**二、S1 新行为确定性测试：**

1. A 的 ready 额度耗尽，B/C 仍可 claim/prepare；同 route 与不同 route；活动超过候选窗口仍有机会被调度。
2. task/part/byte/global/per-group 额度耗尽后不超配、不 Claim 后等待无界；释放、取消、重复 callback、异常与 worker stop 不泄漏、不双释放。
3. DB Claim commit/publish 中间注入失败/主进程退出，重启按 durable 状态处理；对已可能发出的任务不得用“未在内存里”作为重发依据。
4. Reliable/Fast、Batch/Realtime、同步发送及 RabbitMQ 实际可用入口逐项核对，`legacy` 与 `fair` 明确隔离；flag 默认 legacy，旧配置和停止栅栏完全保留。
5. Journal/DB/disk 公共故障时 fail-closed；活动独有拥塞时不错误地全局阻塞。
6. 明确标注哪些断言只是在 dispatch/prepare 层成立；只要旧 Slot FIFO 仍可能 head-of-line blocking，**端到端实际 socket 公平 = NOT_VERIFIED**。

**三、失效判据：**

任何重复实际发送、unknown/partial 自动整条重发、权威 task 漏失、非法状态、Journal 结果丢失、claim 无证据重排、credit 账失衡、死锁或 hang、native SIGSEGV，均阻断 S1 READY。测试因环境或未运行缺失不得伪装 PASS。测试上限与计数以实际项目方案与冻结参数为准，不编造性能提升。

## 4. 范围禁止项

- 不实施 S2 Slot/route socket 级公平、逐 PDU window、Slot 拉取式调度及硬 outstanding parts（只允许定义兼容接口/诊断）；
- 不实施 S3 大文件 chunk quantum、Batch/SFTP 工作池扩容、SMS/File Worker 大规模扩容；
- 不实施 S4 长稳 600 秒、Journal 全量重构/压缩格式变更、完整重启协议重写；
- 不引入 Kafka/Redis/新服务进程/多 DuckDB 主写进程；
- 不升级 DuckDB、不修改真实 SMSC/生产配置，不接入真实手机号码；
- 不为“通过验收”增大超时、sleep、队列容量或重试上限来掩盖问题；
- 不重置、不修改用户已有 DB、WAL、Journal、上传文件、日志和既有未提交资料；
- 尊重 `AGENTS.md`：**除非用户明确授权，不自动提交或推送 Git**。

## 5. 交付物

1. S1 源码变更及确定性测试；每个主要文件说明负责的边界。
2. `Docs/Marketing_Copilot_S1_Bounded_Dispatch_Implementation_and_Acceptance.md`，至少记录：
   - 最终源码 SHA / tested manifest 与环境；
   - legacy / fair 实际配置、各功能是否生效；
   - 任务/credit/route/Journaling 的状态机与不变量；
   - A/B/C 隔离、Claim commit/publish fault、双模式 stop、DLR/unknown 等测试证据；
   - 所有测试命令、原始日志路径、exit code、守恒审计；
   - 未完成的入口/风险和 S2 的明确依赖；
   - 回滚过程（默认 legacy，不能重置未确认任务）；
   - `S1_READY` 或 `S1_NO_GO` 的证据性判定。
3. 清楚回答两句：
   - **在 Claim/ready/prepare 层，单 Campaign 拥塞是否会传播到其他可用 Campaign？**
   - **在实际 SMPP socket 层是否已经隔离？** 若尚未实施 S2，应答 `NOT_VERIFIED`，不得以派发层测试冒充 socket 公平通过。

**通过门槛**：S0.1 修复无回归；S1 增量门禁与 credit 守恒全部通过；legacy 回退有效；S1 入口清单没有未受控发送绕行；文档不把 G3/生产门禁升级为 PASS。若受 S2 物理 Slot FIFO 制约，明确 S1 的实际已交付边界与剩余限制；若存在不安全的绕行、结果容量缺口或重启重发风险，标 `S1_NO_GO`。

完成后只输出一页摘要：修改内容、真实门禁结果、S1 verdict、遗留风险、下一阶段是否可开始 S2。
