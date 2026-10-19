# Marketing Copilot — High-Throughput Multi-Campaign Scheduling & Worker Scalability Architecture Refactor

> **Codex 执行任务 / 工程实施计划**  
> 项目：`shutu-ai/Marketing-Copilot`  
> 基线：`origin/main @ a0b330059052610241ed33efa3c7f359116bbdbc`（2026-10-15 核验）  
> 任务编号：`MC-SCHED-ISOLATION-SCALABILITY-V1`  
> 当前结论：`NO_GO / NOT_VERIFIED`；禁止把已实现等同于已验收。  
> 目标环境：单进程 DuckDB Owner、Linux/ext4 本地测试；最终兼容 SLES 12 SP5。  
> 工作原则：**先关闭正确性风险，再建立端到端公平调度，最后证明扩展能力；每阶段独立验收。**

---

## 0. 启动要求：先读代码，禁止依据旧任务覆盖新实现

你是该仓库的实施 Agent。按照下列顺序执行：

1. `git status --short`、`git rev-parse HEAD`、`git rev-parse origin/main`；在存在用户改动时先记录，**不要 reset/clean/stash/删除、覆盖或提交无关文件**。源码基线必须至少包含 `a0b33005`；如 `origin/main` 已更新，核对差异后用新的确切 SHA 重新建立实施/验收证据。
2. 先读以下文件（以当前源码为准）：
   - `Docs/OCC-Ingestion-State-Consistency-Task-Summary.md`
   - `Docs/DuckDB-OCC-Concurrency-Architecture-Decision.md`
   - `Docs/OCC-Equal-Window-2x2-G3-G4-G5-RCA.md`
   - `Docs/Multi-Campaign-Send-Isolation-Acceptance.md`
   - `backend/database.py`
   - `backend/services/{realtime_ingestion_state,realtime_ingestion_worker,ingestion_job_service,realtime_campaign_service,sms_task_service,task_worker,smpp_gateway,fast_send_service,sms_write_buffer,campaign_capacity_service,backpressure_service,activity_scheduler}.py`
   - `backend/tools/{local_e2e_acceptance,multi_campaign_isolation_acceptance,reconcile_processed_files,duckdb_occ_benchmark}.py`（如果存在）
   - 相关测试 `backend/tests/`。
3. 首先列出：**哪些功能已经存在、哪些只在离线单测中证明、哪些在最新源码上仍为 `NOT_VERIFIED`、哪些会被本任务替换**。用静态代码调用关系和实际运行证据核对，而不是凭文件名推断。
4. 本任务是**替代原单独 OCC 一致性专项的综合重构计划**；不是要求重新设计已经交付的权威状态/镜像分离功能。
5. 源码阅读或编辑优先使用仓库既有 CodeGraph/`rg`/AST 工具；严禁修改用户已有 `frontend/package-lock.json`，禁止提交真实 MSISDN、短信正文、密码、数据库、WAL、Journal、协议原始日志、缓存、venv、上传文件及临时压测产物。

## 1. 已核实的真实现状（不要重复开发）

### 1.1 已实现，需保留

- `a0b33005` 的总结确认，之前的 `28e0a54d` 等提交已经：
  - 将权威 `ingestion_files` / `ingestion_chunks` / `sms_tasks` 与非权威 `processed_files` 展示投影分离；
  - 实现投影幂等、状态版本防回退与 `reconcile_processed_files.py`；
  - 对文件租约、Checkpoint、任务生成进行完整事务重试与过期 Worker fence；
  - 默认设置单个 DuckDB 连接的执行线程数为 1，可由 `MC_DUCKDB_THREADS` 显式调整；
  - 增加针对并发、恢复、投影等回归测试。
- DuckDB 已有 `serialized`（默认）与 `MC_DUCKDB_WRITE_CONCURRENCY=occ`（opt-in）两模式；`maintenance_exclusive()` 协调备份/Checkpoint，单 DB Owner 约束不可取消。
- SMS 任务领取已有短 `_TASK_CLAIM_LOCK`、批量 Claim、按活动/路由的候选轮转与加权 DRR、Claim Token / Attempt Token / Generation Fence。
- 多活动的暂停/取消已有任务资格检查与 SMPP Slot 的旧 Generation 无效化。
- Fast Send 保留明确 opt-in，Reliable 仍为默认；`submit_sm_resp==0` 只代表 SMSC 接受，**只有成功 DLR 才能认定 delivered**。
- Result Journal / Write Buffer 有幂等提交与保守 Unknown/Partial 恢复。

### 1.2 已确认的架构瓶颈

- `task_worker.py` 中**单个 Claim Dispatcher → 全局 `_task_queue`（混合不同活动） → 共享 SMS Workers**。
- `smpp_gateway.py::_SmppConnection` **每个 SMPP Slot 各有一个 `queue.Queue` 与一条异步 Sender 线程**；不同活动仍混合 FIFO 排队，出队时再判断失效 Generation。某个活动占满前序队列时，其他活动仍可能 HOL（队头）阻塞。
- `realtime_ingestion_worker.py` 文件 Claim 采用一把本地 `_claim_lock`，虽然文件解析并行，Claim 可能成为扩展瓶颈。
- `campaign_capacity_service.py` 已有按活动 DRR、路由 TPS/weight；`max_outstanding_parts` 在现版本诊断中仍列为 **advisory**，不能把它写成已强制执行的配额。
- `backpressure_service.py` 有全局任务深度/写入/Journal/磁盘/维护保护；需要把**局部活动软拥塞**与**必须全局 fail-closed 的安全风险**明确分开。
- `MC_REALTIME_INGEST_WORKERS` 目前上限 16，`MC_TASK_WORKERS` 目前上限 32；简单加线程不会自动消除以上共享瓶颈。

### 1.3 尚未通过，必须保留风险状态

- `Docs/OCC-Ingestion-State-Consistency-Task-Summary.md` 明确：最近 Windows 目标测试因 pytest 临时目录 `WinError 5` 导致 `3 passed, 23 errors`；这是环境级阻断而非业务通过证据；最终源码全套测试**没有重新执行并通过**。
- 先前 WSL/ext4 OCC+Fast 曾出现文件摄取状态不收敛；最终版本的验收运行中止且**没有成功报告**。最新源码 G3 `NOT_VERIFIED`，G4/G5 `NOT_RUN`，SLES/真实 SMSC `NOT_RUN`。
- **特别风险审计**：`ingestion_job_service.is_completed_chunk_status()` 现在允许包含 `"completed"` 的非标准状态字符串，例如 `processingcompleted`、`completedcompleted`；这是针对既往异常现象的兼容处理，**不得据此自动推论底层并发数据正确**。必须捕获原始异常、有效状态集合、源事务及任务守恒证据；防止宽松判定将损坏或非终态当成已完成。

---

## 2. 整体目标与非目标

### 最终系统目标

**活动 A 高积压/慢文件/暂停/取消/局部故障时，活动 B、C 在其所需资源仍健康、有路由可用额度的条件下持续获得有界发送进展。增加文件 Worker 与 SMS Worker 时，直到外部/计算/磁盘/路由真正饱和前吞吐可有效提升；到达饱和点后平稳退化，不形成争用风暴。**

必须满足：

1. 活动级有界**逻辑队列**与独立预算，**不是每活动独立数据库、独立 OS 进程、独立 SMPP 连接或独立永久线程**。
2. 文件摄取、任务 Claim、SMPP 发出、结果写入均采用**按阶段的容量准入**与公平调度，不允许一个环节预取填满下一个共享环节。
3. 不牺牲去重、Claim Token、Attempt Token、租约、Generation Fence、日窗口、异步 Result Journal 及 Unknown/Partial 无自动重发语义。
4. 保留 `serialized + reliable` 稳定默认与独立的 opt-in/回滚控制；**不能在一个进程内运行时切换 OCC 模式**。
5. 单 DuckDB RW Owner；若文件解析需要跨进程 CPU 扩展，子进程只处理不接触主库的纯计算，结果交单 Owner 事务化写入。不要为“多 Worker”无依据引入多 DB Writer 进程。
6. 偏向最小可维护实现；禁止为了架构美观而引入 RabbitMQ/Kafka、Redis、全局事件总线、跨库分片或新的分布式一致性协议。

### 不承诺

- 同一个已满载 SMSC 路由上，B/C 不可能保持**不受 A 消耗影响的绝对 TPS**；承诺的是**公平、有界等待且可借用空闲份额**。
- 全局数据库维护、磁盘不足、Journal 不健康时可以阻塞所有发送；这是有意的安全边界，不能为了“互不影响”绕过。
- 当前设计与单元测试 PASS 不代表生产/SLES/真实 SMSC 已验证。

---

## 3. 建议目标架构（允许同等或更简洁实现，必须证明等价）

```text
           Watch / SFTP / File Registration
                         │
         Per-Campaign File Ready / Lease Index (durable DB)
                         │  fair admission; per-campaign budgets
         Shared File-Worker Pool (1/2/4/8/16)
             │ parsing / whitelist / templating
             ▼
  Short fenced OCC DB transaction (chunk + dedupe + sms_tasks)
             │            ┌── repairable processed_files projection
             ▼            └── (never blocks authority)
  Durable SMS Tasks + Campaign/Route Eligible Index
             │
  Fair Claim/Admission Controller (weighted DRR / route credits)
             │ claim ONLY when bounded downstream space reserved
  Per-Campaign × Route bounded logical ready queues
             │  cancel/resume generation fence; capacity reclaim
             ▼
  Shared SMS Dispatch Workers (1/2/4/8/16/32)
             │ short pre-wire authorization, claim/attempt fence
             ▼
   Per-Route Fair SMPP Slot Arbiter + Route TPS / Window credits
             ├──── Slot 1
             ├──── Slot 2
             ├──── Slot 3
             └──── Slot 4 ...
             │   short socket write (no waiting for SMSC ACK under lock)
             ▼
  SMPP Responses / DLR -> Durable Journal -> Idempotent Result Batch
```

**架构关键点**：公平应作用于**真正的发送准入/Socket Capacity**，不能止步于数据库 Claim。尽量由空闲 Slot/Route **主动拉取符合配额的下一个活动任务**；避免先将大量 A 任务放到 Slot FIFO 再指望出队时丢弃。可以保留极小且有界的 Slot 本地缓冲，但要证明不会破坏等待上界。内存逻辑队列只是 durable DB 任务的缓存/索引，不是任务权威存储。

---

## 4. 分阶段实施；禁止一次性推倒重写

### Phase 0 — Latest Source Correctness Readiness（强制停止门禁）

**目标**：复用已实现的 OCC 摄取一致性修复，验证最新源码的基本可靠性，而不是重新做旧任务。

1. 明确冻结 SHA、Python/DuckDB 版本、OS、文件系统、运行模式；所有 `run_id` 标记确切源码 SHA、环境、scenario、CLI、时间和原始结果路径。源码快照如不含 `.git`，写 manifest + 散列，禁止凭 `git_commit=unknown` 宣称同版本。
2. 在 Linux/ext4 隔离副本上运行新版本完整后端测试、Frontend build、OCC/Serialized loopback 与最新相关目标回归。Windows pytest 临时目录故障只标 `BLOCKED_ENV`，不得当作 PASS/FAIL 业务证据。
3. 针对 `ingestion_chunks.status` 的合法状态、原始异常字符串与版本戳执行**数据守恒、显式状态机检查**。任何含 `completed` 的拼接字符串必须有独立的权威 Chunk 完成/任务幂等提交证据才能纠正；无法判定时 fail-closed/quarantine，不能把异常文本直接记为成功。
4. 故障注入：同文件竞争 Claim、过期 Lease 回调、两 Worker 同步 Checkpoint、状态投影缺失/延迟、强杀重启、DB OCC 冲突及恢复。验证不会重复 task insert 或重新提交 unknown SMS。
5. 在 `occ + fast` 下跑当前源码**至少一次完整独立 1K 业务 E2E**，完整 10 文件 × 100 行输入守恒，Task/SMSC/Result 全部收敛，post-shutdown DB 对账；失败就输出 RCA 与证据，**不得进入大规模重构验收**。修补必须局部且有新的回归测试。
6. 只推进到 `P0_CORRECTNESS_READY`（不是 G3 PASS），保留生产 `NO_GO`。这一步可以与 Phase 1 的静态设计审计并行，但 **Phase 1 生产逻辑切换前必须通过**。

**P0 PASS 判据**：无业务数据损坏；无非法状态被无证据自动视为完成；行数/文件数/Chunk 数/SMS Task 去重守恒；无 Unknown/Partial 自动重发；Owner/Journaling 安全；完整进程退出后可复核。

### Phase 1 — Queue, Credits, and Lock Ownership Design（设计门禁）

产出 `Docs/High-Throughput-Scheduling-Architecture-Decision.md`，包含：

- 资源图：file claim、file worker、whitelist、task claim、task queue、slot queue、route window、SMSC TPS、Journal writer、DuckDB maintenance；标记何种资源**按活动隔离**、按**路由**隔离、或必须**全局安全共享**。
- 每个逻辑队列 ID：`(campaign_type, campaign_id, route_id)`；支持有界容量、活动/路由权重、可借用空闲份额，明确生成/失效/清空/重启重建规则。
- **容量许可**：`file processing`、`claimed-but-not-admitted`、`ready`、`socket-in-flight-parts`、`result-pending` 的预算计量/释放时机；区分消息条数与 SMPP parts；不允许失败路径泄漏 credit。
- 锁顺序表：campaign fence、task claim lock、scheduler lock、route/slot lock、DuckDB transaction coordinator、Journal lock；**不允许在 DB transaction 内等待 socket、网络、CPU 大解析或长时队列 put；不允许持调度锁进行 DB 调用/阻塞队列 put**。
- 最小生命周期状态机（Durable DB 为准）：`queued → claimed/sending → socket_authorized → accepted/rejected/unknown/partial`；任何预取失效/队列剔除必须通过 token/generation 严格对账。发送前失败且**确定未 socket write**时可安全复位；已发送或结果不确定时绝不自动重发。
- 为每个阶段提供 flag 和 rollback 计划；`serialized`/`occ` 与 `reliable`/`fast` 均为进程级实验配置；可新加独立 `MC_CAMPAIGN_DISPATCH_MODE=legacy|fair`（建议默认 legacy、明确 opt-in）。若另有更简洁命名，可调整但不得默认切换。

**设计复核**：明确「单队列拥堵」与「同一 SMSC 路由物理容量不足」的区别；不要将任意停顿归因于 DuckDB。

### Phase 2 — Campaign-Aware Bounded Dispatch（解除全局队列 HOL）

实施选择：优先集中在 `backend/services/task_worker.py`、`campaign_capacity_service.py` 与必要的 `sms_task_service.py` API，**避免创建每活动常驻 Worker 线程**。

1. 将单一混合 FIFO 的调度语义替换为有界**活动×路由逻辑队列/就绪池**，使用现有加权 DRR/轮转或经证明等效的调度算法。活动数量可变化，空活动自动回收；总内存使用要有**全局硬上限**与**活动软上限**。
2. Claim 采用**下游容量优先**：先获得目的 route/ready capacity，再 batch claim；在数据库事务提交后发布令牌绑定的 task；若出队失败，释放 credit 并通过可靠恢复/重试状态机处理；任何 `sending` 不允许在无后续持久化证据的内存队列中无限停留。
3. A 的逻辑队列满时**跳过 A**，B/C 仍能被领取和送往可用路由；单活动不允许耗尽全部 Worker / Slot admission credits。公平要以**已实际 socket admitted parts**和任务等待时间衡量，而非仅以 Claim 数量。
4. Pause/Cancel/End：立即冻结 A 新准入、使已入队的旧 Generation 无效化、在本活动队列内有界清除**尚未 socket write**的任务；释放预算；DB 处理严格遵守 Claim/Attempt fence。`resume` 只能按新 generation 恢复，不能复用失效缓存。
5. 队列中任何任务持有可追溯的 `(task_id, claim_token, attempt_token?, dispatch_generation, campaign, route)` 及 deadline；多份引用也不能触发两次 Socket Admission。
6. 当公平调度关闭时沿用已存在的 legacy 行为并标记日志；生产/历史兼容不能被无意改变。允许有界选择器缓存，但要有准确失效策略。

**P2 测试**：A=50K pending、B/C=1K；暂停 A / 取消 A / A 的预取队列满 / A 运行慢任务；证明 B/C 始终能从数据库到任务派发持续取得进展，不出现 A 无界占位；暂停前 claim 而尚未发出的任务不得越过停止栅栏。

### Phase 3 — Fair SMPP Slot Arbitration（解除 Slot FIFO HOL）

涉及 `smpp_gateway.py` 与 Phase 2 的调度接口：

1. 每 route 下的多个 SMPP Slot **共享活动感知就绪选择**；某个 Slot 有 window/route capacity 时，选择下一个具备发送资格的活动任务。不能把旧 Slot FIFO 当成实现了活动公平；如果保留 Slot 队列，必须非常小、有活动预算且对取消快速剔除。
2. Route TPS 仍按 `route/config_id` **共享**，禁止每 Slot 独立放大现网限速；Window 仍按 Slot、按短信 parts 正确计数。SMS multi-part 可以占用多个 part credits，但不能绕过任务/活动安全配额。
3. 接入 `max_outstanding_parts` 真正硬约束（目前版本属 advisory），并提供 per-campaign send credit、per-route credit、per-slot window 用量。配置为空时给出可解释默认值；避免一个大活动占满所有 Token 与窗口。
4. SMSC 响应慢/掉线时只影响对应 route/Slot；不同正常 route 的任务应继续；**同一路由全断**会导致该路由全部活动受影响，这是物理边界，不声称零影响。
5. 保持 socket I/O 前的 `record_send_intent()` / Fast `prepare_task()`、Generation Fence、Attempt Token 与 Journal 关联；绝不把 `send_async` 入队成功当作 SMSC accepted，更不得当作 delivered。
6. 对“队列满、Window 不足、route TPS 等待”实现明确分类与计数；已知**未写 socket**可以安全 defer/requeue（经 Token fence），已经或可能写 socket 则转 unknown/partial 严格对账；不能把 queue full 转化为重复提交风暴。

**P3 负载**：同 route A 多 parts 且慢、B/C 单 parts；不同 route A 掉线、B/C 健康；4 binds、Window=50、Route TPS=200，验证真实 `submit_sm` fairness、SMSC response 与映射守恒。单连接中已有发出的慢任务会占物理窗口；验收采用**有界等待**而非假定零延迟。

### Phase 4 — File Worker / SMS Worker Scalability（扩展能力）

1. 文件处理 Worker：支持 `MC_REALTIME_INGEST_WORKERS=1,2,4,8,16`；确保文件 Claim 临界区只覆盖短事务/轻量轮转，避免持 `_claim_lock` 读取并扫描全部活动或慢 SQL；可考虑原有单 Claim Dispatcher + 限额派发，或按租约 CAS 原子领取，**以压测数据决定**，不盲目改为多线程 DB 同时争抢。
2. 阶段拆分：`watch/SFTP discovery`、`file register`、`file claim`、`parse/whitelist/template`、`task enqueue/checkpoint`，使用有限 Worker/批量 DB 入库；长文件不能一直占有全局注册/数据库写锁。慢活动具有明确 in-flight file 上限与任务配额。
3. SMS Worker：支持 `MC_TASK_WORKERS=1,2,4,8,16,32`，确保增加 Worker 不导致 Claim TPS、DB OCC 冲突、Result Journal fsync 相互放大。根据测量调整 batch claim、result checkpoint 与内存 credit，**以系统/路由有效容量限制最大活跃工作数**。
4. 可实现**有界自适应调度**：输入信号为队列年龄、Slot window 利用率、实际 parts TPS、OCC conflict/retry、Journal lag、CPU/RSS、文件处理速率；输出仅为小范围 batch/budget/admission rate 调节；默认固定配置优先，未经证据不做复杂自适应 PID。
5. DuckDB 在单 Owner 进程内运行；`MC_DUCKDB_THREADS=1` 默认不变。如使用可选 CPU 子进程，必须不直接打开主 DB，限制 IPC 队列并证明终止/重启恢复，属于可选优化而非强制大改。
6. 确保 Worker `stop/shutdown/restart` 无阻塞 `queue.put` 泄漏，Slot 死亡不会将未发送任务永久留在 `sending`；所有队列、Semaphore、Token 都有“成功、拒绝、超时、异常、取消、重启”释放/恢复测试。

**P4 输出**：Worker 数—吞吐—CPU—冲突—等待时间对比矩阵；给出每个阶段达到平稳饱和点的证据。**吞吐提升不要求线性，但不接受新增 Worker 后造成不可控退化、死锁、无限重试或业务数据不一致。**

### Phase 5 — Backpressure, Fault Isolation, Observability（可观测与可靠性）

- 区分：
  - **per-campaign 软背压**：A 配额满 / A 文件过多 / A 发送准备慢，冻结 A 局部新准入。
  - **per-route 背压**：某 Route TPS/window/断连，影响该 route 活动，不阻塞其他健康 route。
  - **global fail-closed**：DuckDB maintenance、磁盘/WAL 风险、Result Journal 不健康、核心持久化不可用，允许全局暂停。
- 指标按 bounded label（campaign id、route id、stage、outcome）跟踪；避免高基数手机号、task-id、message-id 标签。至少增加/整理：
  - `file_claim_wait`、`file_active_by_campaign`、`file_stage_latency_p95_p99`；
  - `sms_eligible_backlog_by_campaign_route`、`sms_ready_queue_depth`、`sms_claimed_not_socket_admitted`、`oldest_ready_wait`；
  - `route_tps_reserved/actual`、`slot_window_inflight`、`fairness_wait_rounds`、`campaign_socket_parts_total`；
  - `database_occ_conflicts/retries/failures_by_operation`、`db_txn_wait_p95_p99`；
  - `journal_pending_durable`、`journal_fsync_lag`、`result_checkpoint_lag`、`unknown/partial counts`；
  - `queue_credit_leak`、`late_submit_rejected`、`duplicate_socket_submit_detected`。
- 给出一键独立 watchdog：在 **30 s 无业务进展**或异常资源峰值时，保存线程栈、各阶段队列/credits、DB coordinator、SMPP route/slot、Journal、输入文件—Chunk—Task—SMSC 状态守恒计数；**在服务运行中不直接用第二进程读写主 DuckDB**，最终 DB 对账在 Owner 退出后执行。
- 使用显式保护：堆积/欠账超限时拒绝新增工作而不丢弃权威任务；日志与诊断脱敏；保留控制 API 可用性。

---

## 5. 综合验收门禁（仅对当次冻结源码有效）

### G0 — 基线/静态质量

- 本次 Git SHA、完整启动配置、环境、文件系统、源码变更列表、`git diff --check`、Python 编译/后端全套测试、Frontend build 均有原始日志和机器可读结果。
- 不允许将旧提交测试复制成当前 `PASS`；修复后所有受影响门禁必须重新执行。

### G1 — Authority / Lease / Projection 正确性

- 独立 1K ingest-only：10 文件×100 有效行、10/10 文件完成、1000 唯一任务、socket=0；文件/Chunk/任务数、去重和镜像一致；镜像删除后可重建、不触发发送；状态枚举及版本无损坏。
- sender-only 1K（pool=1、pool=4）：1000 个持久预生成任务均最终按预期完成，0 重发、0 遗漏提交映射；未知响应保守处理。

### G3 — 同窗口 2×2 四组合 1K 收敛（先于扩容压测）

| DB | SMS | 运行次数 | 必须证明 |
|---|---|---:|---|
| serialized | reliable | 3 个新进程 | 1000/1000 守恒 |
| occ | reliable | 3 个新进程 | 1000/1000 守恒 |
| serialized | fast | 3 个新进程 | 1000/1000 守恒 |
| occ | fast | 3 个新进程 | 1000/1000 守恒 |

统一参数：`pool_size=4`、`smpp_window=50`、`route_tps=200`、`task_workers=8`、`ingest_workers=2`、`watch_copy_workers=2`、`send_queue_capacity=2000`（legacy 对照；新设计须记录等效下游**总**预算）、`DLR=disabled`、本地 Loopback SMSC、独立 ext4 DB/Journal/Watch，每轮 10 文件×100 输入。必须明确是否为单-part SMS；**`1000 task == 1000 submit_sm` 仅在每任务一 part 且无确定拒绝/重试时成立**，否则按 part 层级对账。

- 四组合均 3/3 完整收敛，0 非法状态、0 丢失映射、0 漏任务、0 不安全 Unknown 自动重发、0 活动停止后新发。
- 若任一 FAIL，必须停止进入 G4/G5/G6，先 RCA/最小修复/复测；不可增加超时、改小样本或放宽守恒来获得 PASS。

### G4 — 跨活动/跨 Route 公平隔离

1. A=50K pending，B/C=1K 各；A 持续大量输入、A 暂停/取消、A 的逻辑队列饱和；B/C 在健康 route 的情况下完整发送并持续进展。
2. A 与 B/C **同一路由**和**不同路由**分别测试；在同一路由饱和时检查公平份额而不是绝对 TPS 无损；不同健康路由必须不受 A route 断线阻塞。
3. 活动停止栅栏、Queue Invalidations、分片公平、Claim Token/Attempt Token/Journal 对账。B/C **健康且有可用准入 credit 时连续 30 s 零 socket progress** 即须抓取阻塞证据并记作 FAIL 或明确受控资源限制；不能以“仍有 queued”冒充发送进展。
4. 至少 3 轮独立重现或明确 `PARTIAL/NOT_RUN`。从可用容量、active campaigns 与 B/C 分片数推导可接受最大等待预算，并在结果中记录预算依据。

### G5 — Fault/Restart/Backup/Soak

- 独立覆盖：SMSC 明确拒绝、提交响应丢失、部分分片成功、连接掉线、延迟 DLR、A pause/cancel 与正在 Socket Admission 并发、数据库 OCC 冲突/等待、Journal fsync 延迟、Worker 异常退出、应用强杀重启、在线维护备份、异常投影恢复。
- `submit_sm_resp=0` 是 SMSC accepted，不是 delivered；仅匹配有效 DLR 才标 delivered。
- `unknown/partial` 无自动重发，重启不会多发、重复记录/映射不能覆盖新 Attempt，已入队但未发 Socket 的任务按可证明的事实恢复。
- 完成**有效的 600 秒**稳定性验证，期间 A/B/C 有持续提交/状态进展；不可将“进程空转 600 秒”作为长稳通过。若停止中途无产物，标记 `PARTIAL_STOPPED / MISSING_ARTIFACT`。

### G6 — Worker 扩展与平稳饱和（新增）

| 维度 | 参数 | 数据与断言 |
|---|---|---|
| 文件 Worker | `1/2/4/8/16` | 文件/s、有效 task/s、租约冲突、解析 P95/99、CPU/RSS、平均 DB 时间 |
| SMS Worker | `1/2/4/8/16/32` | 短信/s 与 `submit_sm` parts/s 分开、实际 route/slot 利用率、queue p95/99 |
| 混合 | files=`8/16` × SMS=`8/16/32` | 多活动公平、B/C 最大连续零进度区间、OCC 冲突及 Journal 延迟 |
| 故障 | A 压满 / 单 Slot 断连 / 镜像重建 | 对其他活动/健康 Route 的影响，恢复时间，0 业务违规 |

规则：每个数据点相同输入/路由/环境至少三次独立试验；随机化模式顺序或配对交替，记录 P50/P95/P99 与方差。资源未饱和前期望扩容有**可测的有效收益**，达到瓶颈后吞吐平台稳定；如果升 Worker 反而持续显著退化，按证据判为 `SCALABILITY_REGRESSION`，允许调度器抑制无效并发。禁止宣称 2×/3× 一定提升。

### G7 — 系统环境准入（不得代替 G3–G6）

- WSL/ext4 是本地 Linux E2E 证据，不等于 SLES 12 SP5。
- SLES 12 SP5 离线包/依赖/系统服务/关闭恢复/备份验证独立完成后才标 `SLES_PASS`。
- 真实 SMSC 仅经明确授权的测试路由与 allowlist canary；SMSC 缺失则 `REAL_SMSC=NOT_RUN`。禁止发真实手机号进行自动压力测试。
- 生产继续 `MC_DUCKDB_WRITE_CONCURRENCY=serialized` 和 `MC_SMS_SEND_MODE=reliable`；新公平调度单独 opt-in，直到所有 gate 当前版本 PASS 且用户同意才允许建议灰度；**不得自动切换**。

---

## 6. 必要的自动化测试清单

优先添加独立、可复现测试模块（命名可按现有命名习惯）：

- `test_campaign_fair_dispatch.py`：A 队列满而 B/C 公平出队，route credit 借用、权重、活跃活动集合变动；
- `test_smpp_slot_head_of_line.py`：A 慢 multipart / A paused / Slot 阻塞，B/C 获得可用 Slot；
- `test_dispatch_credit_accounting.py`：enqueue/claim/cancel/timeout/exception/restart 不泄漏、不重复释放 permit；
- `test_worker_scalability_safety.py`：Worker 数变化不重复 claim/重入，无限等待可中断；
- `test_occ_ingestion_consistency.py` **扩展而不重写**：检查 source/chunk/task 的真实完成证据，不允许仅靠拼接 `completed` 判断完成；
- `test_campaign_pause_cancel_socket_fence.py`：停止后已 Claim、已入队、已 prepare、未 socket 的细分竞态；
- `test_journal_replay_and_fairness.py`：重启结果重放与活动公平不会重复 Socket 发送；
- `test_cross_route_failure_isolation.py`：route A 断线，route B 有效持续发送。

每个测试必须断言**实际业务结果**而非仅仅“线程还活着/方法调用次数”；Test Doubles 无法证明真实 SMPP Socket 连续性，关键部分必须走真实本地 SMPP simulator。

---

## 7. 工程实施与代码质量约束

- 保持既有 API、前端读写和运维兼容；如新增诊断 API，仅本地/受控访问，绝不暴露 MSISDN/短信正文。
- 所有多线程共享状态有明确单一所有者、锁/条件变量和 shutdown 清理。**不要用无限 Blocking Queue put、永久 Retry、sleep under lock、同一 Task 多线程并发 Socket Write**。
- OCC 冲突重试必须完整事务、Fresh Connection、Jitter/Deadline、有界次数；不要将磁盘 I/O / file owner 锁错误当作 OCC 冲突重试。
- 受控负载下压力上升必须表现为延时/背压，不是进程内存无界增长。
- 手工 SQL 更新必须用 generation/lease/claim/attempt fence；各类重试/结果落库需有幂等键和证据来源。
- 关键 lock-order 画出 acquisition/release 顺序，逐对验证无死锁（特别是 `campaign barrier ↔ DB transaction ↔ scheduler queue ↔ SMPP slot`）。
- 如果发现 DuckDB 1.2.0 的可复现非标准字符串或损坏：**先保存最小复现及独立权威证据**，不要仅通过字符串包含/放宽状态校验把异常变为 PASS；必要时提出可验证的数据库版本兼容方案与升级矩阵，但未经核实不要直接升级生产。
- 每个 Phase 完成后写阶段性验收记录和 SHA；若因测试环境缺失，明确 `BLOCKED_ENV / NOT_RUN` 而不是 `PASS`；最后只能基于完整证据决定 `PASS / FAIL / MORE_EVIDENCE_NEEDED`。

---

## 8. Codex 最终交付物（均为 Git 可审阅的轻量文本）

建议在 `Docs/` 生成：

1. `High-Throughput-Scheduling-Architecture-Decision.md`（含现状和目标流程图、协议/锁/额度、兼容方案）；
2. `High-Throughput-Queue-Worker-Callsite-Audit.md`（按源文件/函数记录已迁移与例外）；
3. `High-Throughput-Worker-Scalability-Benchmark.md`（G6 曲线及瓶颈解释）；
4. `High-Throughput-Cross-Campaign-Isolation-Report.md`（G4 per-campaign/route 真实 socket 证据）；
5. `High-Throughput-G3-G4-G5-G6-Acceptance.md`（门禁、已运行/未运行/失败、source SHA）；
6. `High-Throughput-G3-G4-G5-G6-Results.json`（机器可读）；
7. `High-Throughput-Failure-RCA-And-Recovery.md`（失败注入与恢复因果链）；
8. `High-Throughput-Operations-And-Rollback-Runbook.md`（功能 flag、调参、止损、恢复）；
9. 当前项目下有意义的代码和自动化回归测试（不要求为每阶段机械新建重复代码）。

**JSON 建议固定字段**：`source_sha`、`evidence_sha`、`environment`、`modes`、`config`、`workload`、`per_campaign`、`per_route`、`per_slot`、`stage_invariants`、`worker_scaling`、`gates`、`production_decision`、`historical_evidence_excluded=true`、`missing_artifacts`、`failure_classification`。每个指标附 `unit` 和消息/parts 粒度，不得混淆 accepted、delivered、submitted。

只提交本任务新代码/测试/报告；保留并跳过所有无关工作树文件（特别是 `frontend/package-lock.json`），不删除现有日志、数据库、缓存或临时验收产物；raw artifacts 留在外部目录。代码提交采用按阶段可追溯的 commit。最终完成本地相关测试及证据一致性复核后，再推送相关提交至 `origin/main`（不 force push），同步校验本地/远端 SHA；如果出现用户未提交的冲突变更，**停止覆盖操作并明确报告**。

### 最终回复模板

```text
BASELINE_SHA / FINAL_SHA:
PHASES: P0 / P1 / P2 / P3 / P4 / P5
GATES: G0 / G1 / G3 / G4 / G5 / G6 / G7
FILES_CHANGED:
BACKEND_TESTS / FRONTEND_BUILD:
G3_2x2 (each arm current runs):
G4 B/C PROGRESS + A BACKLOG:
G5 FAULT/RESTART/600s:
G6 FILE/SMS WORKER SCALE CURVES:
DUPLICATE/UNKNOWN/PARTIAL/STOP FENCE INVARIANTS:
SLES / REAL SMSC:
PRODUCTION_DECISION: PASS | FAIL | MORE_EVIDENCE_NEEDED
BLOCKERS AND NEXT ACTION:
COMMIT(S) / ORIGIN-MAIN SYNC:
```

**完工定义**：不是“增加了公平调度代码”或“Worker 能配 32 个”，而是**当前版本真实运行下 A 拥塞时 B/C 有公平且有界的 socket 提交进展、任务和结果完全对账、Worker 扩容不造成争用风暴；并且现网安全门禁没有被放宽**。
