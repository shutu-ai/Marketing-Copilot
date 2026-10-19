# Marketing Copilot VNext — MSISDN 分片 / 多进程异步高吞吐流水线：Codex 实施任务

> 任务编号：`MC-VNEXT-SHARD-ASYNC-REPLACE-20261016`  
> 任务类型：**真实代码实施 + 架构替换 + 自动化测试 + 离线交付**（不仅写方案）  
> 仓库：`https://github.com/shutu-ai/Marketing-Copilot`  
> 审查冻结基线：`5650b8fc6bcdd620858258db3937cd5c5ad59d14`（执行前检查 GitHub / 本地 HEAD；若变化，比较 diff 并在报告中说明）  
> 原始集成状态：`INTEGRATED_NO_GO`，不得因本任务开始而更改历史验收结论。

## 0. 总任务与关键决策（必须遵守）

**在现有项目上实现一条新的、单一的高效率主链路**：

`Input (Batch / Realtime / SFTP) → Chunk Parse → Normalized MSISDN → Stable Virtual Shard → Durable Task Spool → Shard Worker Process → Async SMPP Socket → Batched Projection → Dedicated DB Writer → DuckDB`。

**最优先目标**：流转简洁、每个阶段独立背压与快速恢复、不同 Campaign/Route 的公平推进、DuckDB 冲突不阻塞正常业务、可随 Worker 数量伸缩、压测中可测量高吞吐。

明确选择：

1. **仅实现一条 VNext 热路径**。不要长期并行维护旧 `reliable/fast`、`legacy/fair`、`serialized/occ` 的多重分支组合；旧实现留在冻结 Git tag/历史版本以便回滚，新路径不调用旧 `task_worker`、旧 `record_send_intent`、旧 per-response DuckDB 更新或旧固定 3 秒 Batch 主调度。
2. **手机号（最终完整标准化号码）决定逻辑分片**，而不是 Campaign ID。初始 `256` 个虚拟分片、`4` 个进程，配置可变，不将取模直接绑定 PID。
3. **一个号码始终归属同一虚拟分片**，同一分片同时只能有一个有效 Owner；同号码默认按任务序号保持 Socket 提交顺序。不同号码/活动可以并发；**跨 Campaign 不做全局号码去重**。
4. **发送完成口径为 `socket_written`**（真实 Socket write 成功返回），不是 `SMSC accepted`/`delivered`。`submit_sm_resp` 在协议层依然必须异步读取，释放 Window/PDU credit、统计响应错误；不要求业务等待它，不逐条落 DuckDB；DLR 默认关闭。
5. **Best-effort / at-most-once-after-dispatch**：默认不对 Socket 写出后或写出结果不确定的任务自动重发；按策略记录计数。允许少量崩溃窗口漏发，不能把 `socket_written` 宣称为远端已接收。明确 `queued / taken / socket_written / write_failed / response_success / response_error / unknown` 的语义。
6. **保留极少但必要的可靠性机制**：任务接收后持久化、分片接管防双 Owner、容量硬上限、协议窗口/TPS、不对不明结果自动重发、暂停/取消后阻止*新* Socket 提交（已在途 PDU 允许自然完成）。不复刻旧逐短信 Attempt/Part/Result 状态机。
7. **DuckDB 不参与每条短信的发送热路径**。所有 Worker 禁止直接打开共享生产 DuckDB；后台专用 Writer 以批量事务做配置、统计和业务投影。写锁竞争可延迟投影，但不能占住解析/SMPP Worker；投影积压达到预留硬容量后才逐级背压。
8. **简单优先**：本版不引入 Kafka、RabbitMQ、Redis、第二套线上数据库、每个处理阶段一个独立日志、每个 Campaign 一个进程等重型设计；复用现有 Python、DuckDB、SMPP 库、离线部署链与当前 UI/API 必需业务功能。
9. 不增加无关账号/RBAC/审批框架，也不因为追求防护而让发送核心流程再次同步等待数据库。
10. 所有真实短信发送仅在用户显式授权、具有真实 SMSC 凭据和发布门禁后才允许；本任务测试必须默认 Loopback + 合成号码，不触碰生产业务。

**重要约束**：允许主动删除或改写旧的内部类/模块/字段和旧前端内部接口，不要求一对一兼容；但外部业务关键功能（Campaign 创建、启动、暂停、停止，模板，文件导入，任务统计，SMPP 配置）必须有清晰的新实现或明确迁移表，不可静默丢功能。避免迁移期间两条主流程同时发送同一任务。

## 1. 已知源码与根因线索（仅作实施入口，必须以当前 HEAD 复核）

- `backend/main.py`：启动顺序涉及旧 Journal、Scheduler、Task Worker；VNext 替换生命周期必须只有一个发送所有权。
- `backend/services/activity_scheduler.py`：固定 `while not _stop.wait(3)`；单 `_batch_worker`、`queue.Queue(maxsize=1)`，每轮 `_BATCH_INGEST_QUANTUM`；在 `batch_size=10` 的四活动压测中容易形成吞吐上限。
- `backend/services/batch_campaign_service.py`：`generate_batches`/`process_pending_batches`、`pending_campaign_ids`、全局 source/generation lock；改为增量 Chunk→Router，不等待整批 Manifest 完成才分发处理。
- `backend/services/realtime_ingestion_worker.py` 与 `realtime_campaign_service.py`：现有文件识别/解析/Lease/Chunk 机制可复用，但 Worker 不得继续与短信 DB 热写/发送同步耦合。
- `backend/services/sms_task_service.py`：`enqueue_rows` 及 `claim_next_tasks`、`record_send_intent` 的旧 DB 事务热点；VNext 改用简洁的 Spool ID、任务游标和分片执行状态。
- `backend/services/task_worker.py`、`bounded_dispatch_service.py`、`campaign_capacity_service.py`：旧 Candidate→Claim→DRR→ReadyPool 可参考，但新路径只保留必要的 Campaign/Route 公平配额，不做旧 Claim 对接器。
- `backend/services/smpp_gateway.py`：借鉴已有真实 SMPP 编码、Window、Sequence、Multipart、Throttle 和响应接收；**把成功响应的 `record_part_result`/逐条 DB Journal 从新模式剥离**。
- `backend/services/sms_write_buffer.py`、`result_journal_service.py`：可复用成熟的批量持久化/重放思路，但 VNext 不得在新发送路径直接串联旧完整结果事件与单任务 terminal marker。
- `backend/services/backpressure_service.py`：已有全局滞回背压；VNext 改为每阶段×每分片的局部压力 + 共享资源硬保护，不允许一个慢 Campaign 占满所有资源。
- `backend/services/send_store_sharding_poc.py`：旧 PoC 按 `campaign_type:campaign_id` 哈希，不是 MSISDN；不要直接把 PoC 当生产实现，也不要在旧生产库上运行。
- `Docs/Marketing_Copilot_EulerOS_SLES_Proxy_Load_and_Dual_VPN_Performance_Acceptance_Report.md`：当前 WSL/EulerOS 5500 都失败，E4 为 `generated=3500/5500`，E4 EulerOS `submit_sm_resp_success=2787`；仍为 `INTEGRATED_NO_GO`。VNext 新 `socket_written` 验收需与旧 `accepted` 门禁明确区分。

Codex 先读 `AGENTS.md`、现有测试/部署文档，再输出最小变更映射表：**复用什么、替换什么、删除什么**；禁止为了兼容反向堆叠两个完整状态机。

## 2. VNext 最简运行结构

推荐 **3 种进程角色**，而不是按每一步无限拆进程：

```text
                       ┌───────────────────────┐
Batch / Realtime / SFTP ──► API + Ingress Router │
                       │ 校验/解析/Chunk/Hash  │
                       └──────────┬────────────┘
                                  │ 按虚拟分片单写者追加
                        ┌─────────▼──────────┐
                        │ Durable Task Spool │
                        │ 256 logical shards │
                        └─────────┬──────────┘
                                  │ 4 workers 各拥有约 64 分片
            ┌─────────────────────┼─────────────────────┐
            ▼                     ▼                     ▼
    Shard Worker #0       Shard Worker #1       Shard Worker #2/#3
    有界 Campaign/Route    有界 Campaign/Route    ...
    公平队列               公平队列
    Async SMPP             Async SMPP
            │                     │                     │
            └─────────────────────┼─────────────────────┘
                                  ▼
                       Batched Progress / Stats
                                  │ 单独汇总，允许滞后
                                  ▼
                       Dedicated DB Writer
                                  │
                                  ▼
                          DuckDB projections

Supervisor / Control Bus：分片 owner epoch、pause/stop、Route TPS credits、背压/观测。
```

- Ingress Router 对一个虚拟分片的 Inbox Spool 保持**单写者**；多个文件源通过有界内部队列汇入，不允许多进程竞争同一个 append 文件。
- 每个 Worker 单独维护自己的消费游标/发送进度文件；不要多进程写同一文件段。数据库 Writer 独立消费聚合结果日志/检查点。
- Worker 进程不能在 `socket.write`、网络连接、解析文件过程中持有全局进程锁，也不能在一个活动等待 Socket Window 时阻塞另一个活动。
- Route 可以被多个 Worker 使用，但同一个物理 SMSC/Route 的 TPS 配额是**全局共享的**，绝不能随 Worker/pool 数增加而线性翻倍。
- 新流程的 `control plane` 与数据平面分离：Campaign 配置允许由 DB Writer 持久化、控制事件传到 Worker；Worker 在本地内存判断状态，不需要每短信查 DuckDB。

## 3. 手机号 Hash、Partition Map 和伸缩

- 使用最终标准化的国家码+号码作为稳定 Key。先核对项目 `phone_prefix`、前导零和 `normalize_msisdn()` 的历史语义，给出统一转换规则，避免两个 Campaign 同一真实号码得到不同 Key。
- `partition_id = uint64_be(SHA256(normalized_msisdn)[0:8]) % virtual_partition_count`。**禁止内置 Python `hash()`**。
- `virtual_partition_count` 初始 256，创建该 Deployment 后不可在线随意修改。分片至 Worker 的映射采用版本化 `partition-map.json`，包含 `partition_id -> worker_id`、`map_revision`、`owner_epoch`。
- 同一个号码的任务按持久化序号排序；在异步发送中，同号码只允许一个 Socket 写入序列在进行，是否等待响应：**不等待**。不同号码可并发执行，重的号码不阻塞其他号码。
- 扩容/减容只移动空闲或安全冻结的虚拟分片：停止旧 owner 新准入→排空/安全标记在途→提交新 `owner_epoch`→新 Worker 接管。不能通过同时启动旧/新 Worker 实现“迁移”。
- 一个热号/热 Campaign 的队列堆积不得耗尽整个 Worker 的预算；引入 `(partition, campaign, route)` 有界就绪配额和公平轮转，默认不用复杂多层 DRR 嵌套。
- 首次版本**不做自动跨机器重平衡**，仅支持单机进程数伸缩（例如 2/4/8）；水平跨主机不是本任务目标。

## 4. Spool 数据合同：保留必要的持久化，避免过度日志

只实现两个持久化数据边界：

**A. Task Spool（权威待处理任务）**

- 最小字段：`schema_version, event_id, partition_id, ordinal, campaign_type, campaign_id, campaign_generation, route_id, normalized_msisdn, payload_ref/message, part_count_hint, source_id, created_at, crc`。
- 单写者、append-only 分段文件、原子 manifest/cursor 更新；每段受字节和记录数限制，group fsync，索引/游标按批次 checkpoint。**文件成功 group fsync 后才将任务视为已接收**。
- 原文件/Chunk 在 Spool 确认写入后即可释放对应解析 Worker。源数据仍按现有文件去重机制防止重复装入。
- 通过 `dispatch_taken` 进度游标（批量持久确认）防止进程重启后自动重新发送写出结果不确定的任务；这个策略是 **at-most-once-after-dispatch**，可在崩溃窗口产生漏发。明确统计“已取出但未确认 socket_written”。
- 从任务接收→消费的窗口确保幂等 event_id：`campaign + stable source identity + row ordinal + generation`，而不是仅用号码去重。
- 完成并确认 Worker 进度及 DB 投影消费游标后才允许删除旧分段；容量不足停止该分片的新输入。

**B. Compact Progress / Statistics（非业务阻塞投影通道）**

- Worker 批量写简洁的 `taken, socket_written, response_ok, response_error, rejected, unknown, throughput` 计数与 Watermark；不逐条写完整 SMSC 成功结果，不保存成功响应正文或逐条 `message_id` 到 DuckDB。
- 按 Campaign/Route/Partition/时间窗口聚合，使用 `event_id` 或单调区间序号防重复投影；Write Gateway / DB Writer 故障时可重放。仅必要的异常详情保留受控有限样本。
- 如果统计投影暂时落后，继续发送直到投影/Spool 的硬预算耗尽；展示 UI 必须显示 `projection_lag_seconds` 和统计口径，不能将落后数据伪装为实时或 SMSC accepted。

严禁：每个阶段各建一份全量 WAL；普通未同步的临时文件视为成功接收；无限增长的内存列表/重试堆；文件内容/手机号写入调试日志。

## 5. Stage 独立背压与低压恢复

对所有有界队列实现同一个轻量 `StageCredit` 契约，但每个阶段独立拥有状态：

```text
stage + partition + campaign/(route optional)
  limit_items / limit_bytes / max_age
  used / reserved / inflight
  high_water / low_water / resumed_at
  blocked_reason / progress_seq / last_progress_at
```

- 高水位立刻停止该**局部上游**新工作准入；低水位自动恢复（容量释放采用 Condition/Event 唤醒，不能等主调度 3s 定时轮询）。
- 阶段局部拥塞不得持有公共 Worker、全局锁或数据库事务：例如 Batch-A 解析慢或 Route-A 断开，Batch-B/Route-B 正常推进。
- 使用滞回，避免高/低水位抖动；初始水位采用合理低负载试验值，**任务中禁止把示例数字当成正式生产阈值**；按 item+byte 双预算，考虑最老等待时长。
- 磁盘、进程内存、真实 Route 总配额是共享硬上限：耗尽时允许相应全局暂停。这不是局部隔离失效。
- 控制面和结果投影预留应急容量。慢 Writer 不得占用解析/SMPP 线程，但写入日志/磁盘预算耗尽时必须减速或停止相关新输入。
- 实现 `STALLED` watchdog：只检测**有资格执行且拥有资源却长期无进度**，不能把合法 Route TPS 等待或用户暂停误报为故障。
- 一个 Worker 故障，Supervisor 只隔离/重启其分片，不重启健康 Worker；恢复后自动按持久游标推进，不靠人工调用恢复脚本。

## 6. SMPP：轻量异步发送，不同步等待响应写库

- 保留现有真实 `smpplib` 编码和 TCP/SMPP protocol 契约：Bind、Keepalive、`submit_sm`、Sequence、PDU Window、Multipart、连接断开/重建。
- 成功定义分层：
  - `enqueued_durable`: Spool 持久接收。
  - `dispatch_taken`: Worker 取得工作并完成最低限度的防重复进度持久化。
  - `socket_written`: OS Socket write 返回成功（**新主业务完成口径**）。
  - `submit_sm_resp_success/error`: **可选后台诊断计数**，不阻塞该 Task 的业务完成，也不用于重试正常发送。
- 即使成功响应不落库，异步 Reader 必须持续消费 `submit_sm_resp`，维护 Sequence→PDU credit，避免 Window 永久耗尽；超时或断线按连接代际清理 credit，不能把超时当成已经获得 SMSC ACK。
- `registered_delivery=0` 默认关闭 DLR；保留失败/断连计数与小型异常采样环形缓冲即可。
- 发送前不进行 `sms_task_service.record_send_intent()` 的逐 Task DuckDB transaction，也不逐条 `record_part_result()`。**不能在旧模块里简单注释掉几句后留下不兼容的 Window/回调状态**；为新发送引擎建立窄接口及独立测试。
- 支持每 Route 多 Slot/Socket，但**Route 总 TPS 使用单机进程间轻量 Credit Broker**（优先有界、短期租约的 token grants，避免每 PDU 同步 IPC）。Broker 不读取/写入 DuckDB 热表；Broker 不可用时暂停所涉 Route，不影响其他 Route。
- 数据面默认不记录完整响应明细、不自动重试 `socket_written`/写出不明任务。只有**能够证明没有执行 Socket 写入**的操作可按有限策略重新排队。
- Route-A 掉线时，Route-B 的任务仍可以发，即使它们归属同一个 Worker；不能在一个 Worker 中用阻塞式 `connect`/`throttle` 停住整个事件循环。
- 对 Campaign pause/cancel：通过 Supervisor 发布带 generation 的控制事件，Worker 本地完成暂停栅栏后确认；API 对外可以报告 `pending`，仅在控制日志和相关 Owner 都确认停止新发送后报告 `stopped`。已经写入 Socket 的数据不会撤回。不需要每条短信前再查 DuckDB。

## 7. DB Writer：从发送热链移走数据库冲突

- 单独 `ProjectionWriter` 进程拥有 DuckDB 的应用写连接。**其他 Worker/Router 不直接操作主 DuckDB**；API 所需的 DuckDB 读写都必须通过 Writer 提供的 IPC/查询接口完成，不允许 API 进程另开跨进程 DuckDB 读写连接或出现第二个无协调写 Owner。
- Writer 使用固定有限批次/短事务、批量 upsert，操作按 `control > essential progress > analytics` 优先级公平交错；不能无限偏向一个 Campaign。
- `DB write lock`/临时 WAL 冲突时：只阻塞 Writer，并基于独立统计通道重试/退避；发送与解析进程在各自 Spool 尚有容量时继续。
- 接受 UI 统计最终一致，但不允许“控制命令已成功”却只存在于非持久内存中。暂停/停止的强制生效依赖控制日志与 Worker 回执，而不是投影 Writer 是否已刷新报表。
- 该版不要同时引入 Send Store 1/4 shard DuckDB 生产迁移；先证明“单 Writer + Worker 无 DB 热访问”的收益，再决定是否进一步分库。
- 划定 Owner/IPC 的启动、终止、重启次序；对统计投影重放应具备单调 watermark，重复应用不累计两次。

## 8. 迁移与最小兼容：一次切换，不双轨维护

- 在独立 `vnext` 开发分支实施，先冻结当前主分支为可回滚基线；`main` 不提前切换或自动操作生产服务。
- 新启动入口只启动 `IngressRouter / WorkerSupervisor / ProjectionWriter` 三类角色，不再启动旧 Scheduler + TaskWorker + Journal 发送组合。
- 允许保留必要 UI/API 功能名称，但其后端要指向 VNext 数据合同；对于含义已变化的指标，新增清晰语义（例如 `socket_written`），**不把它映射成旧 `accepted/delivered`**。
- 一次性迁移基础业务配置、模板、活动定义与必要的未处理输入源；**不自动迁移旧库中的 `sending/unknown/partial` 为可重新发送任务**。
- 对存在旧的活跃任务或未消耗 Journal 的系统，迁移前停止输入、冻结旧 Sender、审计并让用户做一次性处置；不能双引擎并行消费同一个任务。
- 完成 VNext 目标门禁后，删除 VNext 启动路径中旧的模式环境变量、旧兼容条件分支、已废弃内部事件和不再调用的旧辅助模块（保持历史 Git tag 供回滚）。不追求保留所有旧单元测试；保留可迁移的业务不变量与新契约测试。
- README、运维手册、部署脚本、前端展示同步改为一个主流程。记录 Breaking Changes 与迁移影响，不维护永久 adapter。

## 9. 分阶段实际编码任务（每阶段要交可运行代码，不能只写研究报告）

### V0 — 冻结与小规模基准（不得修改主业务）

- 记录最新 `main` SHA、现有 Git 工作区 dirty 文件（不得擅自覆盖）。
- 跑当前相关 G0、P100/P1K、失败 5500 的复用/元数据检查；不要在共享服务器重复全量压测。
- 冻结 `VNext Contract v1`、旧字段到新字段映射、部署架构、文件/进程所有权图。
- 输出 `Docs/Marketing_Copilot_VNext_Architecture_and_Migration.md`，仅作为 V1–V5 的实施约束。

### V1 — 分片器 + Spool + Supervisor（首个可执行垂直切片）

- `backend/vnext/partition.py`：完整号码标准化、稳定 256-shard、versioned map、rebalance fencing。
- `backend/vnext/spool.py`：Append/CRC/segment/Checkpoint/Recovery/容量边界与 crash injection。
- `backend/vnext/supervisor.py`：分片单 Owner、IPC、进程 restart、退出清理、控制日志、Worker lease epoch。
- 单进程→4 进程验证：相同号码稳定落入相同分片；重启后 Map 不变；无双 Owner；队列不会无限增长。
- 源文件 Chunk 到 Spool 的单向流转 + 合成消息 Smoke。

### V2 — Worker 异步 SMPP 核心（不接旧 Claim/Journal）

- `backend/vnext/worker.py`、`backend/vnext/smpp_transport.py`、`backend/vnext/route_credits.py`。
- Worker 内异步 Socket + 非阻塞 Route 等待；SMPP response reader 持续释放窗口。
- 成功业务口径 `socket_written`，失败单独统计；无需每 Task DB 事务。
- 先通过 10 / 100 / 1000 Loopback 成功写出、Window/MultiPart/Route TPS、单 Route 断连不阻断其他 Route。

### V3 — Ingest/Batch/Realtime/SFTP → 分片流水线

- 复用解析与模板逻辑，但旧 `generate_batches` 的 `3s × 1 batch` 路径不能成为 VNext 主链路。
- 单源重输入不重复写入；保证每源/每 Campaign 公平准入；不同活动通过各自工作额度并行处理。
- 文件/网络慢、模板缺失、一个活动队列满时，健康活动继续。
- **关键场景**：同一 4 Campaign：2×2500 + 2×250，`batch_size=10`，全部能持续进入 Spool，不会固定在 3500。

### V4 — 投影 Writer、背压闭环、自动恢复

- `backend/vnext/projection_writer.py`、`backend/vnext/pressure.py`、`backend/vnext/metrics.py`。
- Writer 暂停 30–60 秒再恢复：Socket 发送在缓冲未满时继续；恢复后投影水位自动追平。
- 逐 Partition/Campaign/Route 暴露 queued、taken、socket_written、inflight、response_ok/error、spool_bytes、oldest_age、pause_reason、resume_count、projection_lag、rate_credits。
- Watchdog 区分合法阻塞与无进度异常，支持 crash-safe 接管和 graceful drain。

### V5 — 主链切换、清理与正式交付

- 只保留一条 VNext 发送主链；旧模式不再从 `backend/main.py` 启动。
- 修订 API 和 Vue UI 的任务/吞吐统计，明确新业务发送语义。
- 移除没有生产消费者的旧分支和双轨配置；删除代码必须有静态引用检查与功能清单审核。
- 更新 Linux/EulerOS/原 SLES 离线包的安装、启动、健康检查、进程资源限制和卸载流程；原生 SLES 未实测的兼容性不得伪称已验证。
- 提交可复跑源码/测试/文档，不提交 DB、Spool、运行日志、凭据或真实号码。

每阶段必须附：变更文件、已删除的兼容路径、代码测试、已知限制、后续最小依赖。不要以“计划完成”替代实现完成。

## 10. 验收矩阵：新语义独立于旧 accepted 测试

| Gate | 目标 | 不通过条件 |
| --- | --- | --- |
| G0 | 新增单元测试、源代码编译、前端构建和保留的业务行为通过 | 持续失败/未经解释地删掉业务功能 |
| P10/P100 | 10/100 合成任务 Spool→Socket 写出完整，进程正常退出 | 意外缺失/不明重试/Socket 未发 |
| P1K | 1000 合成任务全部 `socket_written`，统计与 Simulator socket 审计一致 | 只凭 APP Counter 宣布成功 |
| P5500 | 固定 2×2500+2×250、`batch_size=10`，300s 内完成所有预期 Socket Write（真实 TCP loopback） | 仍为 3500 生成、某 Campaign 饥饿、延长业务时限伪通过 |
| Stage Isolation | Batch-A 解析暂停时 Batch-B/Realtime 继续；Route-A 断连时 Route-B 继续 | 任一活动合法堵塞长期占用所有 Worker |
| Pressure | 高水位停止本分片输入，低水位事件驱动快速恢复，内存/磁盘有界 | 无界缓存、需要手动重启恢复 |
| DB Lock | 让 Writer 遭遇模拟写锁冲突，Worker 在 Spool 容量内继续发送，Writer 恢复后补齐投影 | 单一 DB 锁挡住所有 SMPP Socket/I/O |
| Restart | 进程崩溃→相同 Partition 新 Owner 接管；不将写出不明 Task 盲目重发 | 双 Owner、无限重放、数据状态不明未报告 |
| Route PDU | 多 Worker/Pool 共享 Route TPS，不超发；Window credit 正确，Multipart 正确 | TPS 乘 Worker 数、Window 泄漏 |
| Control | Campaign Pause/Cancel 后新 Socket Write 停止、允许已在途完成 | API 报停止成功后继续新发送 |
| Safety | 全部测试 synthetic + loopback，服务器 Run 有界、无生产影响 | 连接真 SMSC/越目录/后台遗留进程 |

**验收口径约束：** 新的 `socket_written=5500` 不等于旧 `accepted=5500`，须并行报告真实 simulator `submit_sm_received` 和 `submit_sm_resp_success`，不可涂改历史 `INTEGRATED_NO_GO` 或旧 G4/G6。若 VNext 新 Gate PASS，可声明 `VNEXT_PIPELINE_READY_FOR_CANARY`，不得无条件声称旧 `INTEGRATED_READY_FOR_S5`。

加上有成本上限的压力扰动 A/B：`N=1,2,4,8` Worker、相同 Route TPS、相同任务/资源；报告 Queue 等待、Spool fsync、Socket 写出、投影延迟和 CPU/RSS；不要假设进程数翻倍就能翻倍吞吐。

## 11. 双 VPN 与 EulerOS 共享主机执行边界

- 阶段 A：可用 GitHub/外部下载的 VPN，只在开发机完成代码、依赖归档、离线 Wheelhouse、构建与静态测试。
- 手动切换至测试服务器 VPN 后，阶段 B 在 **实际 EulerOS 2.0 SP15** 运行；业务同意将其当作本轮 SLES 代理性能目标，但报告仍如实写 EulerOS，不伪称原生 SLES。
- 独立运行目录只允许 `/home/paas/marketing-copilot-test/runs/<unique-run>/`，独立正式 Python 3.11.5、独立 venv、离线 `pip --no-index`、Run 内 DuckDB/Spool/Logs/Temp，Loopback SMSC。
- 沿用资源策略：初次 `CPU affinity ≤4 logical CPUs`、`nice +10`、`ionice idle`、进程 RSS 预警聚合 2GiB、Run 磁盘预警 5GiB（以现场批准的严格约束为准）；采用真正停止条件，不能仅把 warning 记录后继续无限运行。
- 先 10→100→1000；通过资源复核和用户同意的压测窗口后才执行 5500。禁止自动执行 52K/102K/G4/G6、真正 SMSC、外部生产 API、全局 pip、systemd、root、系统网络设置。
- 服务器不通过 GitHub/PyPI/DuckDB 扩展仓库联网；Python 安装包及扩展需离线验证 SHA256。Codex **不自动切换 VPN**，交付跨网络的打包/验签/恢复步骤。
- 阶段 C 手动切回 GitHub VPN，导入并哈希核对脱敏证据，形成 WSL/EulerOS 对照；是否 commit/push 必须遵守当前用户授权和仓库约束，不能自动提交无关 dirty 文件。

## 12. 任务交付物与严格结束条件

交付的源码模块（可按实现微调命名，但不要做双轨重复实现）：

```text
backend/vnext/
  partition.py
  spool.py
  supervisor.py
  ingress.py
  worker.py
  smpp_transport.py
  route_credits.py
  pressure.py
  projection_writer.py
  metrics.py
backend/tests/test_vnext_*.py
backend/tools/vnext_loopback_acceptance.py
backend/tools/vnext_fault_injection.py
Docs/Marketing_Copilot_VNext_Architecture_and_Migration.md
Docs/Marketing_Copilot_VNext_Implementation_and_Acceptance_Report.md
```

同时实际修改 `backend/main.py`、相关服务路由、Campaign 接口、前端任务/统计页面、部署脚本；清理无效旧内部运行路径。

**结束时必须清楚给出**：

1. 当前提交 SHA、实际完成的 V0–V5 阶段；没有实现的阶段不得标 PASS。
2. 新旧业务字段对应/删除清单、真正被去除的旧兼容分支清单。
3. WSL/测试服务器各 Gate 实测数据与失败证据；不得推断不存在的测试结果。
4. 5500-tasks：生成、Spool、taken、socket_written、SMSC 接收四活动分项完整计数，时间和吞吐。
5. Worker 数、每 Worker 分片和 Route 全局 TPS；所有资源预算、CPU/RSS、Spool 最大深度、DB projection lag。
6. 崩溃、DB Writer 堵塞、Route 断连、暂停/停止的恢复表现和已知漏发语义。
7. 旧 `INTEGRATED_NO_GO` 保留；根据新 Gate 单独判定 `VNEXT_NO_GO` 或 `VNEXT_PIPELINE_READY_FOR_CANARY`。
8. Git dirty 文件、生成证据/临时文件/数据库不提交；提交前仅展示本任务文件清单。

**禁止仅生成架构 PPT/方案或重复调参后宣布完成；本任务必须交付真实可运行的新流水线实现。**
