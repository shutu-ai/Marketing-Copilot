# Marketing-Copilot VNext — SMPP Response / Journal 解耦与轻量实时统计 CLI（立即实施）

> **Codex 执行任务 | 优先级 P0 | 目标：支持近期 1–2 个真实 Campaign 的受控上线**
>
> **任务 ID**：MC-VNEXT-P0-RESPONSE-JOURNAL-DECOUPLING-CLI-20261019  
> **起始基线**：`origin/main` / `91257109f16158948d4e74f8b54ec13eab257fde`（开始前重新核对 HEAD，避免覆盖新提交）  
> **源仓库**：`https://github.com/shutu-ai/Marketing-Copilot`  
> **执行端**：Codex，在用户现有工作区实施；正式 Linux 验证使用 **SLES12SP5 原生 ext4**。  
> **工作方式**：本任务要求**实际修改实现、运行测试并给出交付文件**，不是再次只写 RCA 或选择 `NO_CHANGE_WITH_EVIDENCE`。根因不明时可调整实现路径，但不得放弃下面的语义与验收。

## 0. 业务要求（高于实现方式）

1. **立即实现** SMPP Reader 与同步 Journal `append/fsync` 解耦，减轻后续 Response/Projection 对新发送的非必要阻塞；同时检查 `socket_written` 进度写入是否也在发送热路径上产生同步 I/O。不能只把 Reader 的 callback 换成 `create_task()` 就宣称解耦成功。
2. **立即实现一个简单、无需 Web 服务的 CLI 实时统计工具**，在 SSH/终端中直观看见系统、Bind、Worker、Journal、Projection 及 1–2 个 Campaign 的状态。CLI 无须依赖 Vue、FastAPI、额外付费服务、复杂监控栈、额外数据库或非必要依赖。
3. **优先可用、稳定、吞吐、低复杂度**。保持原有独立流水线、背压暂停和自动恢复、人工暂停/继续/取消、at-most-once、业务日隔离、持久化和重启恢复。不要引入中央分布式协调平台、RBAC、审批流、重型事件总线或无必要的新服务。
4. **默认保持** `workers=8`、`pool_size=4`、`tps_scope=per_bind`、每 Bind `200 TPS`、`window=50`，单一 ProjectionWriter/单 DuckDB owner。不为吞吐擅改限速、协议窗口、默认 Journal 持久化或重新发送策略。
5. 只在 `127.0.0.1` synthetic loopback / 用户批准的独立测试环境运行自动发包测试；**不得主动触达真实 SMSC、真实号码、生产 Campaign 或执行正式部署**。本任务交付有限业务上线就绪建议，不代替用户的上线授权。

## 1. 当前源码事实与怀疑路径（先核对，不机械照抄）

- `backend/vnext/smpp_transport.py::AsyncSmppTransport._read_responses()`：解析 `submit_sm_resp` → 匹配 `_pending` → 释放 SMPP Window → **同步 `await response_callback(event)`** → 下一次 `read_pdu()`。Callback 慢会形成 Reader 队头阻塞，尽管 Window 在 callback 之前已 release。
- `backend/vnext/worker.py::_response_event()` → `_emit()` → `progress_sink(event)`；生产 `backend/vnext/runtime.py::emit()` 执行同步 `journal.append(event)`，然后投递非权威的 Projection IPC hint。SLES 文件系统上的 JSON、open/write/fsync/close 位于 Worker 的 asyncio 事件循环调用链。
- `send_task()` 的发送完成路径也会执行 `_emit(socket_written)`；**必须覆盖发送事件和响应事件的全部写盘路径**。不能在 Reader 解耦后仍让单个同步 fsync 堵住同一 Worker 的事件循环。
- `backend/vnext/projection_writer.py::ProgressJournal.append_many()` 已有批量、按 segment fsync 接口；但其批量使用前须审视事务边界、顺序、错误传播、容量检查、reclaim、文件索引与 crash window。
- 生产 `VNextRuntime.status()` 会调用 `TaskSpool.status()` 全量核对，并枚举 Journal 与 process metadata：**不适合 CLI 每 1–2 秒全量扫描**。`VNextMetrics`、`CriticalPathProfiler`、Worker Status、`ProjectionWriterProcess.snapshot()` 已有部分汇总数据可复用。
- 上一轮报告的 5K：E2E `45.857s / 109.035 PDU/s`；Producer active `29.534s`；Response drain `12.903s`；`response_window_wait` P95 `2371.399ms`；Projection staging total `2.089s`；`projection-status-interval` 现有 wiring gap。**这些是线索，不是解耦已被证明能提升多少 TPS 的保证**。
- 最新背压报告已确认 `StageCredit`、Spool hard limit、`journal_admission`、Projection backlog/offset 及人工控制的组件合同；Streaming harness 不等同于完整 Runtime，因此最终测试必须覆盖**真实生产 Runtime 的代码路径**。

## 2. P0A：Reader / ProgressJournal 真正解耦 — 必须实际实现

### 2.1 推荐的最小结构（Codex 可基于实际代码选更简洁等价方案）

```text
ShardWorker / SMPP Send path
     |    发送前：已有 checkpoint + Journal admission
     |    新增：预留有界的进度/响应持久化容量（计数+字节）
     v
AsyncSmppTransport (send + independent read_pdu loop)
     |  response received -> match sequence -> release SMPP Window
     |  fast non-await enqueue to RESERVED bounded progress channel
     |  return immediately to read next SMPP PDU
     v
Dedicated progress persistence task / worker-thread actor (one ordered owner per worker)
     |  bounded batches -> ProgressJournal.append_many() -> flush/fsync
     |  committed acknowledgements and per-Campaign counters
     |  release persistence credit only AFTER successful durability
     v
Existing Journal segments / ProjectionWriter / offsets (unchanged source of truth)
```

**绝对不要**在每条响应上 `create_task(journal.append(...))` 并无限制地散列后台任务，也不要把 `asyncio.Queue(maxsize=...)` 当成完整的故障处理方案。该队列内的事件在进程崩溃时仍可能丢失；必须说明和处理这个事实。

### 2.2 发送前容量预留与双重额度

- 分离两个额度：**SMPP protocol Window**（收到对应 Response 后释放）和 **progress durability credit**（对应事件真实 fsync 成功后释放）。两种额度不能混用。
- 在取得不可逆 `dispatch_taken` / 发往 Socket 前，确认本次任务在最坏情况下需要的进度事件（单条/多段 SMS、写入结果、每段响应/unknown）的**条目数与字节上界**能够容纳于有界内存队列及 Journal 的当前可用预算。多段不能按照单 PDU 简化。
- 做好预留与取消：尚未发送、业务日失效、Pause、连接前失败，应退回不需要的预留；一旦可能 Socket Write，绝不可因额度等待导致偷偷重发。不得将 Journal hard limit 降级成“记录失败但继续发送”。
- 响应到达时只做最少工作：取 `_pending`、解除 Window、对**预留的位置**做同步 O(1) 入队；不能 await fsync、DuckDB、全量 Spool/Journal 扫描或无界锁。达到异常容量/不变量时 fail closed，标记健康异常并停止**新**发送；已在途响应需要安全排空，不能死锁。
- 允许正常情况下 Writer fsync 延迟与 Reader 读取并发；如果 Journal 长时间无法写盘/磁盘满，**允许并必须背压新发送**，避免无界内存、假成功、覆盖事件。不能承诺任何下游故障情况下仍维持配置 TPS。
- 推荐**每 Worker 单持久化消费者 + 有界批次**，使用现有 `ProgressJournal.append_many`；同步磁盘工作须从 asyncio 事件循环移走（如专用 writer thread / `asyncio.to_thread` 的受控单消费者），避免 Worker loop 也被 fsync 卡住。不要每条事件创建线程。
- 保留 Journal 事件 ID / Segment / Offset / UTF-8 JSON / T6、T7 恢复协议，不更改消费者的持久化事实。即便 Response 在 `socket_written` 进度记录之前到达，也必须按现有合法状态语义处理；若必要，显式定义/验证 writer 排序，不能使 Projection 汇总错乱。

### 2.3 关键可靠性语义（不能声称超越物理可能的保证）

- **Checkpoint before socket**：在任何可能的 Socket Write 前 durable checkpoint 不变。网络状态未知时**永不自动重发已越过该边界的 PDU**；可以接受不可避免的“少发/未知”，绝不能引入重复发送。
- **Reader 看到响应不等于 Response 已持久化**：CLI 必须分列 `response_observed` 与 `response_durable`。内存中观察到但尚未 fsync 的成功，进程崩溃后不能当作永久成功。
- 对 **`checkpoint`/`socket_written` durable 而响应未 durable** 的任务，重启时只能根据原协议标识为 `UNKNOWN` / `UNRESOLVED` 或等待可用的 DLR/对账证据；**绝不据此重发**。若外部要求“每一条已解析 Response 在任何瞬时崩溃后仍保证保存”，必须先写入耐久日志再回报成功——这和完全非阻塞是不同约束；不得作不实的零丢失承诺。
- Writer append/fsync 出错、取消、任务异常、线程死亡时：上报显式 degraded/blocked，停止新 dispatch，现存的已观察响应保持在有界内存中尽可能重试/停机排空；停机未能 durable 则报告未解决计数，绝不能假装 Journal caught up。
- `drain_transports()`、`close_transports()`、正常 shutdown、人工 pause/cancel、WorkerSupervisor restart 必须等待**协议在途 + 进度持久化债务**两个不同的 drain 边界（可分开计时）。窗口为空不代表结果 durable。要保证关闭 Reader 前处理应答、关闭 writer 前最后批次持久化；超时后保留未知与可复核证据。
- Reader disconnect、unexpected response、重复/乱序 sequence、window 释放一次且仅一次、多段交错与部分已写、`enquire_link`、`unbind`、SMSC 低速响应/短连接及 reconnect，均应保持协议处理正确。不因 DLR（最终送达报告）缺失而把 `submit_sm_resp_success` 叫作“送达成功”。
- `StageCredit` high/low water、Spool hard limit、Journal `can_append()` admission、Projection backlog/reclaim、Campaign 手动控制彼此独立；恢复时不丢/不覆盖既有 Campaign pause。

### 2.4 需要有界、低开销的真实统计信号

新实现至少公开以下**聚合计数/瞬时 gauge**，按 Worker、Route/Bind 与 Campaign（如可从 context 拿到）区分，避免每次统计序列化整条 SMS：

- `socket_written_pdu`, `response_observed_success/error/unknown`, `response_durable_success/error/unknown`, `response_observed_not_durable`；
- `window_inflight`, `window_peak`, `bind_active`, `bind_acquires`, `reconnect_count`, `response_latency_p50/p95`（若启用轻量 bounded histogram）；
- `progress_queue_depth/items_bytes`, `progress_reservations`, `progress_pending_durability`, `progress_writer_batches`, `progress_writer_fsync_total_ms`, `progress_writer_oldest_age_ms`, `progress_writer_errors`, `admission_blocked_reason`；
- `worker_inflight`, `spool_pending`, `journal_backlog`, `projection_applied`, `projection_lag`, `checkpoint`/`pause`/`resume` counters。

不把每条号码、短信正文、Secret 或大量逐事件 trace 写到统计文件；日志/计数写入频率与短信量解耦。

## 3. P0B：无需 Web UI 的轻量 CLI — 必须实际实现

### 3.1 实用命令行合同

建议新增 `backend/tools/vnext_stats.py`（复用已有 `backend/tools/__init__.py`；不依赖 FastAPI 或 Vue）；Codex 可选择更适合仓库的入口，但应提供等价命令和 `--help`：

```bash
# 在仓库根目录：默认每 2 秒刷新，按 Ctrl+C 退出
python3 backend/tools/vnext_stats.py --root /path/to/vnext-runtime --watch

# 单次打印，方便 SSH/排障
python3 backend/tools/vnext_stats.py --root /path/to/vnext-runtime --once

# 只看两个活动（例子；实际 ID 从运行数据获取）
python3 backend/tools/vnext_stats.py --root /path/to/vnext-runtime --watch --campaign batch:101 --campaign batch:102

# 机器可读输出，不清屏
python3 backend/tools/vnext_stats.py --root /path/to/vnext-runtime --json --once

# 自定义刷新频率
python3 backend/tools/vnext_stats.py --root /path/to/vnext-runtime --watch --interval 2
```

- 不依赖 curses / rich / colorama 等额外依赖。优先使用 Python 标准库；有 TTY 可 ANSI 清屏和表格，无 TTY 自动保持纯文本；`--no-color`，`--json` 便于脚本读取。输出应适配 80–120 列普通 SSH 终端。
- 单次快照带 `observed_at`、`snapshot_age_seconds`、`run_id`、`source`、`stale`、`incomplete_metrics` 标志。观测不到的字段显示 `N/A`，不得用 `0` 掩盖丢失/停服。秒级 PDU/s 使用同一计数器差分，进程重启/rollover 不能出现负速率或错误倍增。
- 支持 `Ctrl+C` 退出、路径不存在错误、Runtime 未启动（离线可查部分 durable counters）、新旧格式兼容与 Windows PowerShell/Python 3.11。

### 3.2 终端显示示例（仅示意，禁止用这些数字当测试结果）

```text
VNEXT | run=pilot-001 | 08:31:42 | UPDATED 0.9s ago | state=RUNNING
Ingress      accepted=...    rejected=...     paused-by-backpressure=NO
Spool        pending=...     disk=... MB       source=lightweight-snapshot
Workers      active=8/8      taken=...         inflight=...
SMPP         binds=4/4       written=... PDU   rate(5s)=... PDU/s
Responses    observed=...    durable=...     unknown=...  pending_durable=...
Journal      durable=...     backlog=...      oldest_undurable=... ms
Projection   applied=...     lag=...          queue=...     caught_up=...
Campaign     Status    Accepted  SocketWritten  RespOK  RespErr  Unknown  Projected
batch:101    RUNNING   ...       ...            ...     ...      ...      ...
batch:102    PAUSED    ...       ...            ...     ...      ...      ...
Health: OK | Backpressure: NONE | Fsync errors: 0 | Last update: ...
```

**定义清楚**：`socket_written`≠`submit_sm_resp_success`≠`delivered`；Campaign 多段短信分开显示任务数与 PDU 数，不做错误加总。

### 3.3 数据采集方式（防监控拖慢发送的硬限制）

- CLI **只能读取独立低频、已经准备好的聚合快照**，不得每 2 秒运行完整 `VNextRuntime.status()`、`TaskSpool.status()`、扫描所有 Spool segment、重新扫描全部 Journal、或在发送线程中做 DuckDB 查询。
- 优先复用现有 `worker-*.status.json`、`projection-writer-status.json`、`CriticalPathProfiler` 等**原有低频状态**；新加的跨 Worker 聚合在独立只读 collector/CLI 进程中进行，只有在确有必要时才做**增量式**、按稳定 offset 读取 Journal，严格限制每轮处理的最大事件数和 I/O 时间。
- Campaign 统计必须有明确可信来源：可以由 Worker 内存计数低频快照、或独立只读 Journal 增量消费累计（按 `event_id` / Campaign / generation 去重）。不得把缺失 Campaign 分项统计伪装成已验证；若 Journal segments 已被合法 reclaim，应补齐准确性策略（持久增量聚合 checkpoint、读取 Projection 的只读汇总，或显式 `N/A`/`incomplete`），不能盲目从旧 offset 恢复后丢计数。
- Snapshot 原子写入，固定间隔推荐 1–2s，可选择 5s；失败时统计显示 `STALE`，**绝不能反向阻塞/终止业务流水线**。总体增量开销应可测量，资源使用有界。
- 对两活动并行同时运行、一个暂停另一个继续、背压后恢复、多个 Worker 重启、Journal segment rollover、Projection backlog 的计数正确性必须有测试。
- 只读权限、默认本地路径即可，不引入完整鉴权体系。不要在统计记录中输出 MSISDN、短信内容或 SMSC credentials。

## 4. 实施流程（避免再次陷入无限 RCA）

### Phase A — 代码定位与小范围设计冻结

- 按 `AGENTS.md` 工作。若仓库含 `.codegraph/`，先 `codegraph explore`。记下真实源文件、测试与 CLI 入口，`git status` 保护用户既有 dirty/untracked。
- 枚举所有 `progress_sink`、Response Callback 和 Journal append 调用点，明确**真实生产 Runtime**与 Loopback harness 的差异；确认 consumer、Close/Drain、异常传播和 event order。
- 做一张 1 页以内的 invariants/状态图：`reserved → possible_socket_write → response_observed → response_durable → projected`，定义不一致与崩溃恢复的语义。
- 开发可以直接开始；不要先写几十页论文/重复 50K 基线。

### Phase B — 解耦实现

- 实现有界的 per-worker `ProgressPersistence`（名称可调整）与容量预留协议，真正把同步 Journal I/O 移出 Reader/Worker event-loop 关键路径。尽量复用现有 `ProgressJournal.append_many()`，不要并发写同一 Journal 文件。
- 安全地接入 `AsyncSmppTransport`、`ShardWorker`、`VNextRuntime`、相关 harness，并保持 legacy 调用方的兼容性（无 sink 时的行为明确定义）。
- 确保 `socket_written`、`submit_sm_resp_*`、`unknown`、取消/过期等终态的容量与输出一致，恢复场景不会自动重发。
- Writer 异常/磁盘满/长时间 stall 时 fail-closed，不能静默继续发送；pressure 恢复自动唤醒，不覆盖 Campaign 用户暂停状态。

### Phase C — CLI 实现

- 使用稳定聚合快照和独立读通路，实现 `--root/--watch/--once/--interval/--campaign/--json/--no-color`；支持无需部署 Web UI。
- 确认统计来源、刷新时间、累计与速率口径、Campaign 多段计数与未知状态；提供清楚的 README/示例命令。
- CLI 只是观察者：停止、断线、Ctrl+C、读取损坏 JSON 均不能干预正在运行的 Sender。

### Phase D — 定向与集成测试（每一步结果先可复现）

**必须覆盖**：

1. **高响应压力**：synthetic SMSC 批量响应 + 人为 `journal.append_many` fsync 延迟（在测试专用依赖/fixture 注入）；证明 Reader 持续读 PDU，不受单次 fsync 阻塞。记录 `reader_loop_gap`, `response_observed_rate`, `response_durable_rate`, `socket_written_rate`, `durability_queue_depth`。
2. **正常背压**：progress queue 高低水位与最小容量、multipart 预留、Journal hard budget 耗尽、Writer stall；无无界增长、不会错误释放 credit、不会异常死锁。下游恢复后自动继续。
3. **持久化失败**：write/fsync 抛错、Journal 空间不足、停机时 pending debt，验证 blocked reason、无 fake success，已写 PDU 不重发。对 observer-success/not-durable 的 crash gap 显示 unresolved，不得写成已持久化。
4. **协议完整**：duplicate/unknown/late/out-of-order `submit_sm_resp`、socket failure、disconnect、`enquire_link`、window=50、4 Bind cap、TPS 200/bind、多段拆分、部分成功/失败。
5. **人工 Campaign**：A paused / B continues；resume 后 A 继续；cancel、业务日 rollover 与 Crash/Restart at-most-once，不能出现未授权 Socket Write。
6. **真实生产 Runtime**：不仅是 standalone harness；从 `VNextRuntime` 入口进行 1–2 Campaign synthetic E2E，包括 status、Spool/Journal/Projection 正确性、WorkerSupervisor restart、clean exit 和最终 durable drain。
7. **CLI**：TTY/非 TTY，JSON schema，双 Campaign，Stale 快照、计数重置、run-id 变化、journal segment rotation/reclaim、并发 CLI watcher 不改变发送结果。

### Phase E — SLES native ext4 A/B 与是否推进试运行

- 在 SLES12SP5 native ext4 上，保持相同原始源数据、8 Workers、4 Bind × 200 TPS、Window 50、同一 loopback SMSC、同一负载、同等观测条件。运行旧代码对照与新代码，先 **1K smoke**，再至少**两轮交错 5K A/B**；一轮 `old→new`，一轮 `new→old`，各自独立 run root，不并发。
- 对每轮同时统计：**准确发送阶段 PDU/s**（Socket 第一个到最后一个 PDU 的 monotonic 时间窗）、wall E2E PDU/s、Response Queue tail/drain、Journal durable drain、Projection caught-up、max pending memory/bytes、RSS/CPU、bind/Window 利用率、进程和 lease 残留、两个 Campaign 对账。
- 测量 **CLI OFF / ON** 的相同流量配对差异，目标统计开销 **≤3%**；若超过必须减少采样与 I/O 或明确标记不通过。数字仅作工程目标，不能通过关掉必要的 durable 写入取巧。
- 基线报告 `45.857s / 109.035 PDU/s` 可以作为历史背景，但**不能替代同环境配对**。改善不足时也必须给出已实施的结构与正确性证明，诚实说明吞吐收益。
- 1K/5K 任何错误即停止扩大。50K、600s、生产部署**不作为本轮必做**；只在明确正收益、正确性门槛满足并得到后续授权时安排。

## 5. 不可妥协的验收门槛

**Invariant:**

- 原流水线 `Ingress → durable TaskSpool → ShardWorker/SMPP → durable ProgressJournal → single-owner Projection/DuckDB` 各自独立；不把 Projection commit 纳入 Sender 成功前提。
- 原 `checkpoint-before-socket`, `unknown-never-resend`, `business-day`, `Campaign pause/resume/cancel`, `StageCredit` high/low water，Journal hard-budget + reclaim, Projection offset commit order 全部保留。
- `response_observed`、`response_durable`、`projection_applied` 三种状态分离；统计不报假成功，Crash Gap 不报为已 durable。
- 只允许增加**预留的有界队列**和最少必要的聚合数据；不会形成无限进程/线程/任务、内存增长或持久化“假确认”。
- 对真实 Loopback 在途窗口与真实 Journal 失败均有有界退出/可恢复错误；无未观察到的重发。
- CLI 不扫描大表、不执行生产 SQL、不因 UI/统计改变业务 Backpressure；watch 查询频率再高也不应随短信量成倍增加发送负担。
- 测试要求至少保持现有 `447 passed, 5 skipped` 的已知回归水平（允许有明确原因的测试增减，不能掩盖退化）；新增专项测试须通过。

## 6. 输出文件与任务关闭

新增/更新文件路径以实际仓库约定为准，建议最多三个 Markdown/JSON 交付物：

```text
Docs/Marketing_Copilot_VNext_Response_Journal_Decoupling_Implementation.md
Docs/Marketing_Copilot_VNext_Pilot_CLI_Statistics_Guide.md
Docs/Marketing_Copilot_VNext_Response_Decoupling_CLI_Acceptance.json
```

实现代码应该包括对应的持久化 actor / Transport/Worker/Runtime 接线、CLI、单元与集成测试；不增加无意义的重复报告。

机器摘要必须包含：

```text
SOURCE_HEAD=
PRODUCT_CHANGES=
RESPONSE_READER_JOURNAL_DECOUPLED=PASS|FAIL
SOCKET_WRITTEN_JOURNAL_IO_OFF_EVENT_LOOP=PASS|FAIL
PROGRESS_QUEUE_BOUNDED=PASS|FAIL
PREDISPATCH_RESERVATION=PASS|FAIL
JOURNAL_DURABILITY_AND_CRASH_GAP_HANDLED=PASS|FAIL
READER_AND_DURABLE_DRAIN=PASS|FAIL
STAGE_INDEPENDENCE_AND_AUTO_BACKPRESSURE=PASS|FAIL
CAMPAIGN_PAUSE_RESUME_CANCEL=PASS|FAIL
AT_MOST_ONCE_NO_REDISPATCH=PASS|FAIL
CLI_NO_WEB_DEPENDENCY=PASS|FAIL
CLI_CAMPAIGN_STATS=PASS|PARTIAL|FAIL
CLI_OVERHEAD_PAIRED_PERCENT=
SLES_1K=
SLES_5K_PAIRED=
STAGE_THROUGHPUT_OLD_NEW=
E2E_THROUGHPUT_OLD_NEW=
JOURNAL_AND_PROJECTION_RECONCILIATION=
BACKEND_TESTS=
50K=NOT_RUN_UNLESS_AUTHORIZED
600S=NOT_RUN
CANARY_READY=NO_UNTIL_LIVE_SMSC_PILOT_APPROVED
FINAL_DECISION=IMPLEMENTED_VALIDATED|IMPLEMENTED_BUT_PERF_NO_GAIN|CORRECTNESS_BLOCKED
```

如发现某项无法在可接受的复杂度下实现，在结论中如实写明“限制与未闭环”，不能假设绕过 durability 能得到同等可靠性。**本任务要实际交付可运行实现，不以「需后续研究」替代代码。**

## 7. Git 与部署纪律

- 遵守 `AGENTS.md`：保护已有 dirty/untracked，**用户未明确要求时不 commit、不 push**；不包含日志、数据库、短信正文、真实号码、SMSC 凭证、运行数据。
- SLES 自动测试仅 Loopback；**不部署生产、不调用真实 SMSC**。在完成后提供一个可直接操作的“1–2 个有限活动试运行说明”，包含开始、CLI 观察、人工暂停/恢复、正常退出与故障恢复命令；实际开通仍由用户批准。
- 在已有工作区进行最小必要修改；提交最后一个报告前先核对 `git diff`、测试结果、源树 SHA、CLI `--help` 和文件列表。
