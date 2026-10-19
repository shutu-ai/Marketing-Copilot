# Marketing Copilot — S1.1 Result Pipeline Hard-Bytes Safety Closure

**任务 ID**：`MC-SCHED-S1.1-RESULT-PIPELINE-BOUNDED-BYTES`  
**推荐 Codex 模型**：`gpt-6.1-sol`，reasoning effort `high`（如遇 Journal 格式迁移/复杂恢复协议设计再人工升为 `xhigh`）  
**远端基线**：`origin/main` 提交 `9aa6b27a29f07bccc64a19069ecf0fea8e1c8892`（执行前实际核对，若已更新则先记录并分析差异）  
**本阶段目标**：关闭 S1 唯一明确阻断项 `result_pipeline_hard_bytes=NOT_VERIFIED`，以真实容量和故障证据争取 `S1_READY`。  
**严格边界**：**不要进入 S2 Route/Slot socket fairness、逐 PDU window、S3 Worker/文件扩容或 S4 大规模长期稳定性重构**。

## 0. 先阅读与核验

阅读并以当前源码为准，不能只从旧设计推测：

1. `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`（§5.3、§5.5、§7 的 S1 和 §8）。
2. `Docs/Marketing_Copilot_S1_Bounded_Dispatch_Implementation_and_Acceptance.md`；明确原结论 `S1_NO_GO`。
3. `Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md`；保留原生 DuckDB owner/cursor 修复。
4. `backend/services/result_journal_service.py`、`sms_write_buffer.py`、`bounded_dispatch_service.py`、`task_worker.py`、`backpressure_service.py`、`sms_task_service.py`、`fast_send_service.py`、`smpp_gateway.py` 及其全部调用点和现有测试。
5. `AGENTS.md`，若存在 `.codegraph/` 先用 CodeGraph 定位代码。

先列出事件流与所有缓冲区、复制/引用关系：

`actual submit_sm / response / DLR → callback → result admission → Journal append queue → fsync → DB write queue / _pending / retry heap → DB event → terminal marker → compaction/replay`

检查 `wait=False` 也会同步等待 journal fsync 的语义；检查 terminal marker、Fast checkpoint 批量事件、迟到/重复 DLR、API/direct/RabbitMQ fair 拒绝路径。把每一个容量节点的**当前硬限制、单位、所有权、释放点、恢复方式**写进报告；区分「持久文件长度」「尚未解决的 durable debt」「进程内驻留 bytes」「发送前的结果预留」四种不同概念，**不能拿任意一个指标代替其他三个**。

## 1. 必须解决的安全问题

当前 S1 使用任务预留估算 `result_events/result_bytes`，但不能直接证明完整 pipeline 的最坏内存 bytes：

- `result_journal_service._queue` 有 item 数量上限，而 `_AppendRequest.records` 与 payload 大小尚未硬限制；writer batching 可能叠加放大驻留内存。
- `load_state()` 使用 `events/order/terminal` 字典与列表累积全量历史；`_maybe_compact()` 又物化 pending/quarantined 清单。
- `sms_write_buffer` 的四个队列之外，`_pending` 与 `_retry_heap` 独立持有 `WriteItem`，容量可能随持续故障、DLR 或重启回放增长；队列满后入 heap 不是有界背压。
- 持久 append 在写缓冲入队之前发生；若 fsync 成功后内存准入失败，必须**保留可回放的事件且不伪称 DB 已提交**。
- `ReadyPool` 估算的预留不能证明 actual serialized event size；应核对 Fast batched event、多分片、错误文字、message id、回执等最坏实际编码大小。
- 终态 marker、写入失败重试、恢复及 compact 也产生额外字节；不能让「释放 capacity 所需的 terminal」被自己的容量限制死锁。

要求提供一个封闭的、有证据支持的上限协议，而不只是新增 gauge。具体配置数值需根据有效负载编码和内存目标计算、测试后冻结；不得捏造生产内存/SLA。

## 2. 目标合同：有界结果通道和保守 fail-closed

### 2.1 明确定义上限与计量

为 fair 模式构建统一的结果预算合同；保留默认 `legacy` 行为，既有 event_id、Journal checksum 和 DB 幂等语义不得回退：

1. 按**实际序列化后的 UTF-8 byte size**（包含 framing/checksum/记录开销）为 journal append request 计费，不能仅依赖 `sys.getsizeof` 或 8 KB 固定估值；对固定开销、Python 对象开销及编码临时峰值给出保守可验证的预算或采用有界逐条编码/流式处理。
2. 分清 `incoming/queued`、`in-writer-batch`、`write-pending`、`retry`、`replay`、`compaction`、`terminal` 的所有权：同一事件不能漏算或无理由重复算；跨阶段转移必须原子可审计。
3. 任何单事件/批次实际编码超过最大可接受包络时，**不得执行新的外部 socket 副作用**；对已经收到的响应或 DLR，保留证据、停止继续发送，并采用安全的有界持久/拒收处理，不得静默截断证据或将 unknown 变成 retry。
4. 先于 **fair 模式 socket admission** 保证最坏必需结果记录、multipart parts、终态/必要协议元数据的容量。为迟到 DLR、重复输入和不可预测的外部回执指定独立有界 emergency 路径及饱和策略，明确真实网络背压/连接关闭的能力边界；不要承诺无限外部输入不会导致压力。
5. 高水位和故障时停止新的发送/Claim，允许结果排空和 terminal 写入；DB/Journal/disk/maintenance 不健康时维持 global fail-closed，不把公共安全故障伪装成单 Campaign 可恢复问题。
6. 预算总账必须覆盖**进程内正在排队/持有的 payload bytes、未完成 event 引用、必要 retry/replay 工作集**；不要求整个 Python 进程 RSS 的数学绝对上限，但必须可测 RSS 峰值并证明受控增长，报告哪些额外内存不在预算内。
7. Disk journal 文件可能持久增长：以 disk budget/高水位、可验证的 compact 策略和 durable debt 水位抑制新发送。不能把物理磁盘无限增长称作有界内存成功。

### 2.2 写入、ack 与 crash-safe 恢复

- 新 event 入队之前取得必要 byte credit；每次写入/转移/释放有独立 ID 与 owner/epoch。`acquired - released == live`，多次 callback/timeout/cancel/restart 不能 double release。
- `fsync` 之前不得告诉调用方已 durable；`fsync` 之后即使 DB writer 阻塞也不得丢事件；Journal 已 committed 的 event_id 不能在 DB 中缺失对应事实。
- terminal marker 的保留额度**不和普通事件竞争到相互死锁**；quarantine 永不冒充成功，`flush()` 在 unresolved/quarantine 存在时不得返回纯成功。
- 重启必须先完成必需 replay/reconciliation 屏障再允许新 fair 发送；读取 Journal、解析、比对 terminal、DB pending 和 compact 均需要**受控工作集**。不能仅仅把 `load_state()` 包一层 size check，超过上限就丢掉历史事实。
- 如现有 append-only 单文件格式无法支持真正的有界 replay/compact，优先设计**兼容的分段/分页/外存索引**最小改造；明确新旧格式 reader、历史 journal 恢复、原子 rename/fsync、checksum、重复 event 去重和回滚限制。只有充分证明必要时才改格式，杜绝顺手大规模重写。
- 不将未确认 submit 的 attempt 恢复为 queued/retrying；partial/unknown 和原来的 DLR/stop fence 不能退化。**No exactly-once SMSC claim**。
- 退出/异常路径：旧 worker epoch 未退出不能假装容量已归还，所有未结 durable event 必须可追溯。`legacy ↔ fair` 只能在完全关闭旧运行时后按兼容规则切换，严禁静默删除 Journal。

### 2.3 并发/锁约束

- 不允许在 ledger/Queue/Slot/campaign gate 锁内执行 `fsync`、DuckDB SQL、网络 I/O、长扫描、阻塞 `put` 或重试 `sleep`。
- 必须避免 writer 为了终态 ack 等待被 pending 自己占用的额度；独立终态保留或不阻塞的流式清算方案需用确定性测试证明。
- 保持 S0.1 单一 native DuckDB owner + 每事务 cursor 设计，不再引入每事务 `duckdb.connect/close`；不要额外开 DuckDB RW 进程。
- 回调/Listener 不得在共享锁内长时间等待结果 IO；如果本阶段修改回调交接的同步持久化时序，必须精确披露新的崩溃窗口，并用 durable intent → unknown 保守回收。

## 3. 推荐实现顺序（可以内部小步提交，但不要超范围）

**A. 先做失败复现与预算方案**：新增可重复的 payload/队列/DB 停顿/Journal fsync/重启 fixture。展示旧版 `queue count bounded` 但真实 resident bytes 或 retry/replay 工作集不受限；记录执行数据。输出不变量、上限单位、byte 估算/真实编码比对、锁顺序和所有事件生命周期的设计。需要大规模格式改变则先在报告中做 ADR，再实施最小兼容版本。

**B. 实施统一硬字节门禁**：在 Journal append、内存传递、DB pending/retry 及 fair 结果预留之间建立原子 byte credit/ownership。强制单事件上限、批量上限、总在途上限、退出归还，含 Fast checkpoint、DLR、terminal emergency reserve。应用安全背压到 fair socket 准入，明确 legacy 不共享的路径及回退行为。

**C. 有界 replay/compact**：消除全量 `load_state`/compaction 对任意历史 Journal 的无界内存依赖；保留原始、迟到和 quarantine 证据；保证含大量 committed、少量 pending、重叠 terminal 的事件重建与 atomic compaction 正确。对历史格式不兼容须显式标记 `BLOCKED`，不能强行宣称 PASS。

**D. 故障闭环与复测**：进行内存/bytes ledger 审计、持久 event 守恒、队列满/超大事件/fsync failure/DB outage/crash recovery 测试。最后在冻结源码上跑完整后端测试、S1 专项、Reliable/Fast loopback 回归、必要的 OCC+Fast 1K、前端 build。测试输出与代码 SHA 对应。

## 4. 测试与门禁（不可删减）

**确定性负向测试**：

- `N01` Journal queue item 数量未达到上限但 bytes 已满，新的 fair socket 必须被阻断；旧 result 仍可安全 drain。
- `N02` 变长 UTF-8 / 非 ASCII / 长错误描述 / 长 DLR / 多分片 / Fast batch：actual serialized bytes 不得超出计费包络，oversize 必须明确拒绝或持久隔离。
- `N03` DB 长时间不可用或高冲突，`_pending/_retry_heap/queue` 聚合的内存驻留不得随事件数无限增长；仍存 event 可 restart replay，不会只因容量不足自动丢弃或自动重发。
- `N04` Journal `fsync` 慢、失败、超时、append 之后入内存之前 crash：不会对发送结果虚假确认，不会删除 durable 事实，fair 新发送全局关闭。
- `N05` terminal marker 在最大 event debt 下仍能前进；重复 terminal、double callback、重试及 shutdown 不 double-release 或死锁。
- `N06` 大量 committed 历史 + pending + quarantined + duplicate events/terminal 的 Journal 进行 replay/compaction；工作集上界可测，重启后 exactly-one DB event effect，pending/quarantine 完整保留。
- `N07` stale worker epoch、Claim/publish crash、pause/cancel、partial/unknown、旧 attempt 迟到 DLR：不安全的 retries=0，关键状态单调。
- `N08` 两 Campaign、不同 route 及共同 Journal 故障：局部 ready/credit 耗尽只阻塞本组；共同持久层不健康必须全局阻止新 socket。
- `N09` 兼容既有 legacy Journal 的恢复与切换；如果涉及格式迁移，验证跨版本读回、未完成记录及 rollback 场景。

**门禁**：

1. 冻结源码、配置、运行环境、依赖版本、输入内容与 hash；保留测试失败原始日志。
2. 全量后端 `pytest backend/tests -q` 最终代码 PASS（跳过项需逐条解释），相关 S0/S0.1/S1 回归 PASS，前端 `npm run build` PASS。
3. fair 的 loopback E2E 要验证真正的 `submit_sm`、task/attempt/part/Journal/DB 守恒，不能只以 `3/3 quick` 证明 hard bytes。追加至少一组真实 byte-pressure + 恢复 E2E。
4. 重要旧行为 smoke：legacy 默认、fair opt-in、reliable + fast、OCC + serialized 的相关安全合同与 S0.1 数据库 owner 回归。合理复用已有 1K harness；凡修改公共 result pipeline，应重跑最终源码 OCC+Fast 1K 至少 3 个独立进程并保存每次外部 socket/Journal 对账，以防复发。
5. `used_bytes <= limit_bytes` 在所有可达状态，记录 queued/in_writer/pending/retry/replay/terminal 等的原始时序与最大值；每项计量规则明确，溢出必能定位且不会被吞掉。
6. 人工故障注入必须结束在明确 `PASS/FAIL/BLOCKED_ENV/NOT_RUN`，遇 FAIL 不能靠放大队列、增加 timeout、降低并发或忽略异常来得出 PASS。
7. `G3` 四组合 × 3、真实 SMSC、SLES12SP5、完整 socket fairness 和生产切换仍然是独立门禁，**不得从 S1.1 直接宣称这些已 PASS**。

## 5. 交付物与判定

在 `Docs/` 新增：

`Marketing_Copilot_S1_1_Result_Pipeline_Hard_Bytes_Closure_and_Acceptance.md`

包含：

- 最终 SHA / 工作树状态 / frozen manifest / 环境与版本；
- bytes 所有权图和配置项，编码样本尺寸、容量上限/峰值和工作集上界；
- 旧实现可复现故障及 RCA；修改清单、兼容/回滚决策与可追踪代码位置；
- N01–N09 分别对应日志/测试/审计产物；运行命令、次数、exit code、失败证据、守恒结果；
- 新增诊断状态 `result_pipeline_hard_bytes=VERIFIED | NOT_VERIFIED`，`VERIFIED` 只可由满足硬限协议的测试/代码证据支持；
- 未覆盖入口、未验证条件、剩余限制（包括不可控 DLR 突发及磁盘绝对容量）；
- 是否满足 `S1_READY` 的依据，不得与 `G3` 或生产 PASS 混淆。

**最终判定（二选一）**：

- `S1_READY`：本阶段硬 byte 管理、可恢复 replay/compact、全部关键故障证据和回归门禁均有有效证明；但 `socket_fairness=NOT_VERIFIED`，只能开始 S2 设计实施。
- `S1_NO_GO`：任何硬上限、恢复/兼容、结果守恒或故障测试不能证明；原样保留证据、阻断 S2，不可将 `NOT_VERIFIED` 改名成 PASS。

## 6. 工作区与安全约束

- **不能清理、reset、覆盖**用户现存未跟踪文件、`frontend/package-lock.json` 改动、真实 DB/WAL/Journal/上传/日志/备份/密钥。
- 用全新 WSL AlmaLinux-8 ext4 测试目录、隔离 DB + Journal + synthetic loopback SMSC；不得发送真实号码、连接真实 SMSC、修改生产环境。
- 保持同一 DuckDB RW owner；尽量无新 DDL、无新第三方服务；如必须改 Journal 格式先证明兼容性，不得破坏历史事件的读取。
- 只提交相关源码、测试和新验收报告；**仓库 `AGENTS.md` 禁止未授权提交与推送，本任务默认不自动 commit/push**。若本次用户明确指示提交，则先做精确文件清单、diff 审查后再提交。
- 在结束时，用简明中文汇报：最终 S1 判定、实际已证明的硬 bytes、是否仍有潜在无界缓冲、关键测试与后续 S2 是否解锁。
