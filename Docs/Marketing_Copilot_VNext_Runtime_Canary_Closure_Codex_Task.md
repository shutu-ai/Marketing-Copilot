# Marketing Copilot VNext — Runtime Canary Closure / Codex 实施与验收任务

> **任务类型**：针对当前 VNext 主运行时的 P0 收口与受控验收，**要求实际修复源码、补充自动化测试、生成报告与离线交付物**，不只是编写设计文档。  
> **GitHub 仓库**：`https://github.com/shutu-ai/Marketing-Copilot`  
> **冻结基线**：`4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`（开始时先验证 HEAD / origin/main，并记录漂移）  
> **历史状态**：旧系统 `INTEGRATED_NO_GO` **保持不变**。VNext 当前仅为 `VNEXT_PIPELINE_READY_FOR_CANARY`（合成号码 + TCP Loopback 测试范围）。  
> **目标环境**：开发机 Windows / WSL AlmaLinux-8 ext4；服务器 EulerOS 2.0 SP15（经用户授权，本轮作为 SLES12SP5 性能代理，报告如实标注 EulerOS）。  
> **网络事实**：访问外部下载、GitHub 与连接测试服务器需要用户**手动切换 VPN**；不得要求同时连接，不得自动切换 VPN。  
> **交付原则**：**只有一条 VNext 高吞吐主路径**，按需保留有限的显式 legacy/test 回退入口；不叠加第二套持久化协议、队列中间件或重型兼容层。

---

## 一、背景、已经确认的结果与本次范围

基线 `4de1e0d1` 已引入 `backend/vnext/` 的 MSISDN Hash + 256 虚拟分片、4 Worker、TaskSpool、异步 SMPP、ProgressJournal、独立 ProjectionWriter 和分阶段背压。V0–V5 已报告完成；后端 `376 passed, 4 skipped`、前端构建 PASS。在 EulerOS 独立 Loopback 任务测试中，P10 / P100 / P1K / P5500 全部 PASS；P5500 为 `generated=5500`、`socket_written=5500`、`submit_sm_received=5500`、`submit_sm_resp_success=5500`、`projection.applied=5500`、`pending=0`、`17.416 s`（包括启动与收尾），约 `315.8/s`，`route_tps=0`。这些是**已经提交的历史证据**，不是本任务新跑出的结果。

这一次不继续拆大架构，只关闭独立 Runner 和正式服务之间的行为差距：

1. **在线输入可见性**：长驻 Worker 在启动后能立即发现新写入、特别是新建 Segment 的 Spool 任务，无须重启；同时确保多进程 manifest/cursor/owner 数据无过期读或覆盖。
2. **在线 Writer 的最终投影**：Worker 与 Writer 同时运行；Writer 停机、队列满、DB 暂时不可写、Writer 重启后，已持久化 ProgressJournal 自动重放并幂等追平，而不是依赖独立测试 Runner 事后导入日志。
3. **活动暂停 / 取消 / 恢复实时生效**：在大批量任务正在发送时执行操作，也会让后续新任务及时遵守控制指令；服务重启不得清除持久化暂停状态。
4. **持续运行验证**：以真实 `main`/API 启动和真实 `VNextRuntime.start()` 入口，在启动后多波次输入、持续限速/慢响应、资源采样、局部压力与自动恢复下验收。
5. **保留高吞吐设计**：发送完成语义为 `socket_written`，`submit_sm_resp` 后台处理以释放 SMPP Window/提供诊断；不回退到逐短信 DuckDB Claim/Intent、逐响应落库或历史多套发送状态机。

**不在本任务范围**：真实 SMSC、真实号码、生产发布、原生 SLES12SP5 实机放行、52K/102K、完整旧 G4/G6、生产数据库迁移、全盘重写、引入 Redis/Kafka/RabbitMQ、新增账号审批系统。

## 二、执行前必须遵守的仓库与现场规则

1. 先读仓库 `AGENTS.md`。若有 `.codegraph/`，先用 CodeGraph 定位符号与调用关系。建立只读 `git status --short`、`git rev-parse HEAD`、`git rev-parse origin/main` 和基线记录。**不重置、不清理、不覆盖既有 dirty 文件**（如 `AGENTS.md`、`frontend/package-lock.json`）；如有同文件冲突，精确合并并记录。
2. 不自行 `git commit` / `git push`；未经明确授权，只生成代码、测试、文档和交付包。不要把数据库、Spool、日志、实际号码、密码、运行时证据目录、缓存提交到 GitHub。
3. 外部依赖优先现有离线 wheelhouse。禁止因 VPN 不通循环下载，也不得要求测试服务器访问 GitHub、PyPI 或 DuckDB 扩展仓库。
4. 所有发送测试仅使用**合成号码**和绑定 `127.0.0.1` 的 SMPP Simulator；拒绝实际公网 SMSC。默认不记录号码全文、短信正文或连接凭据。
5. 共享 EulerOS 服务器仅限 `/home/paas/marketing-copilot-test/runs/<run-id>/`（独立 venv / cache / spool / DB / logs / tmp / ports）；非 root，不碰系统服务、生产数据、宿主机配置。沿用用户批准的资源隔离：**最多 4 个允许的逻辑 CPU**、`nice -n 10`、`ionice -c3`，RSS 2 GiB 与测试目录 5 GiB 作为预警/停机保护门槛；基线异常或业务主机资源明显承压应 fail-closed 停止。本任务不得自动执行 P5500，须由前置规模、监控及现场授权决定。
6. 对外部署采用已有 CPython 3.11.5 standalone + 离线依赖、手动部署脚本，不升级全局 Python/DuckDB、不改主机防火墙/系统网络。

## 三、源码级待核查问题（不是跳过复现直接拍脑袋修补）

### P0-A — 长驻 Worker 看不到在线新增 Spool

- `backend/vnext/runtime.py::_runtime_worker_entry` 在启动时构造一次 `TaskSpool`，然后循环 `ShardWorker.run_until_idle()`。
- `backend/vnext/spool.py::TaskSpool.__init__` 缓存 `_manifest` / `_event_index`；`iter_from()` 使用已加载的 Manifest Segment 列表。
- `backend/vnext/worker.py::_seed_buffers` 每轮从当前 Spool 实例建立迭代器；如果新 Segment 只被另一个进程写入，则有跨进程过期读风险。
- 现有 `backend/tools/vnext_loopback_acceptance.py` 先填好所有任务，再启动 Worker 进程。**该 PASS 无法覆盖在线持续入队行为。**

要求先用最小真实多进程测试确定：`runtime.start()` → 无任务空闲 → `enqueue_chunk()` → 不重启、不中断 Worker → 自动完成；再追加第二、第三波，必须全部消费。重点覆盖此前分片空 Manifest、追加到旧 Segment、创建新 Segment、Worker idle 后再次入队、多个 Campaign 同时输入。

修复建议（允许提出更简洁的等价方案）：生产者作为唯一 Manifest/Spool Append Owner；Worker 采用**版本增量发现**或原子通知 + 有界兜底轮询，发现新 Segment/新增 Ordinal；跨进程只能由所属 Worker 写其 Partition 的 checkpoint，不得因不同进程内存缓存而覆写生产者 manifest。避免每 100ms 全盘重载/重建 256 分片索引，也不能把 CPU 和 I/O 的压力转移成新的热点。必要时使用共享版本文件 + wake-up hint，通知丢失时也能通过兜底检测恢复。

### P0-B — ProjectionWriter 在线自动恢复与背压隔离

- `backend/vnext/runtime.py::emit` 先 `ProgressJournal.append` 后 `projection_queue.put_nowait`；队列满会抛 `ProjectionBackpressure`，可能中断 Worker 热路径。
- `backend/vnext/projection_writer.py::_process_main` 读取 IPC 队列、批量写 DuckDB；`ProgressJournal.read/replay` 存在，但**正式启动链未证明包含自动 Journal catch-up**。
- `backend/tools/vnext_loopback_acceptance.py` 在 Worker 结束后收集 Journal，再离线启动 Writer 导入；不能用它证明并发运行时 Worker/Writer 故障恢复。

**优先采用最简单的一条可靠事实流**：每 Worker 的持久化 ProgressJournal 是已产生进度的权威来源，Writer 异步按 offset 增量读取并批量投影；IPC 可以仅作唤醒/加速提示，**不得作为唯一事实来源，也不得因队列满使已成功持久化的业务结果丢失或阻断整个 Worker**。如保留 IPC，为每种故障提供统一自动补偿，无双主写入协议或第二套复杂状态机。

具体实现约束：
- 追加记录的原子性、完整行处理（尾部半行等）、日志分段/增长上限、损坏处理策略、Writer offset/checkpoint、提交与 offset 顺序需要明确；**数据库提交成功但 offset 未推进可重复读取，必须使用幂等 Event ID 处理**。
- Writer 重启先从可追踪进度补齐，在线发送不中断；成功投影只允许发生一次，DB 投影计数不得因为 replay 翻倍。
- Writer 暂停/DB 锁冲突不应让已具备 Spool/发送信用的 Worker 因 IPC Queue Full 卡住；Journal/磁盘达到硬上限时才按明确规则停止相应生产者，并持续监控和自动恢复。
- 不能把整个 Journal 读入内存再长期轮询；采用有界流式 tailer 或等效实现。
- 正确区分 `socket_written`（本地 Socket Write）与 SMSC `accepted`；结果统计允许异步，SMPP 的 `submit_sm_resp` 读取/Window 释放不能删除。

### P0-C — 活动控制持久化和运行中实时生效

- `backend/vnext/runtime.py::start` 目前写空 `control-state.json`；必须确认重启是否清除原有 Pause/Cancel。
- `backend/vnext/runtime.py::_runtime_worker_entry` 在每次 `run_until_idle()` 之前读取控制文件；而 `worker.run_until_idle()` 可能连续处理一个很大的 Backlog，导致暂停指令长时间不生效。
- `backend/vnext/worker.py::_eligible` 影响新调度，真正 Socket 写前还需界定正在 in-flight 的行为。

要求：保留持久活动状态与单调递增的控制版本/代际（可复用当前文件，不要再新建复杂控制数据库）；新命令通过原子文件更新或明确的 IPC 分发尽快通知各 Worker。在派发前或进入外部 Socket 阶段前检查已更新的可用状态，不能仅等到本轮大批次全部结束。Worker 重启、服务整体重启均继承状态；暂停与取消的区别、`resume` 后何时恢复、已经进入 Socket 的在途任务是否允许完成，需要固定为可测试合同。

控制语义建议：Pause/Cancel 阻止**确认生效之后的新任务**被送入 Socket；已进入 Socket 的 PDU 允许自然完成或按明确窗口上界排空，不伪称可以撤销已经写出 Socket 的数据。控制生效延迟应有时间线与单独指标（从命令写入到各 Worker 确认），不能依赖固定三秒级调度。消除重启后的状态复位。

### P1-D — 监控/背压的真实性和低压恢复

- 验证 `backend/vnext/pressure.py` 的高低水位、`backend/vnext/route_credits.py` 的跨进程 Route 配额和 `backend/vnext/supervisor.py` 的分片 Owner 约束。
- 指标至少包括：每 Stage/Partition/Campaign Ready/queued/inflight、最老积压 age、spool bytes、writer lag、Journal retained bytes、Worker 上次真正进度、每 Route 发送与 Window/timeout、控制状态、恢复次数、RSS peak。
- 写入堵塞应局限于 Writer/投影层；只有相关持久化资源达到高水位或容量/磁盘硬上限，才允许把背压逐级反馈给上游。容量跌破低水位后，采用事件通知迅速恢复，允许有界兜底 poll；不能靠调高无限队列、无限重试或大规模日志堆积实现“假解耦”。
- 若测试发现 `TaskSpool.status()` 每次全量读取全部 Segment 造成 O(total-data) 开销，应改为增量计数或缓存快照；不得将状态 API/监控扫描变成新的吞吐瓶颈。

### P1-E — 保持简化，不扩大兼容

- VNext 主流程继续保持 `Ingress -> Durable Spool -> ShardWorker -> Async SMPP -> ProgressJournal -> ProjectionWriter`。
- 不复活 `MC_SMS_SEND_MODE` 两套主引擎、不增加每短信 DuckDB Claim/Send Intent 事务；后台 SMSC Response 只处理必要的 Sequence/Window/连接生命周期及轻量统计。
- 保留现有路由、模板、批量和实时活动的对外 API/业务功能，不为了移除内部历史实现而破坏尚在使用的功能。
- 新加抽象必须能取代旧的重复机制，不能产生两套互相独立的 Spool/Journal/Queue 权威来源。

## 四、分阶段实施与强制验收门禁

### C0 — 冻结源码与建立失败复现（必须先做）

1. 查阅本任务第 3 节源码、`backend/main.py`、`backend/services/vnext_service.py`、`backend/routers/vnext.py`、`backend/tests/test_vnext_*.py`，绘制不超过一页的生产启动与在线事件流图。
2. 在纯隔离测试目录新增能稳定暴露 P0-A / P0-B / P0-C 的测试：先证明现有源码至少一个在线场景失败或给出有证据的“不复现”结论，不能凭主观猜测直接修改生产逻辑。
3. 记录事件消费者和持久数据的唯一 Owner、Crash Boundary、Replay Source、幂等键和等待点。
4. 交付 C0 RCA：实际故障/可构造风险/未验证项分栏。不要把性能故障和业务语义/安全事故混为一谈。

**门禁**：有可重复的最小复现或清楚的证明性检查；无真实 SMSC；原始工作区未被破坏。

### C1 — 在线 Spool/Worker Live Ingress 修复

测试程序必须调用**真实** `VNextRuntime.start()` 和 `runtime.enqueue_chunk()`，至少包括：

- 空闲已启动，Wave-1=100 条（两 Campaign）→ 自动完成；Wave-2=1,000 条（四 Campaign）→ 自动完成；Wave-3=持续多波次小 Batch/Realtime 输入 → 自动完成。
- 在同一 Partition 的已有 Segment 后面追加；触发新 Segment；新增大量文件/活动后无需 Worker 重启；中间有空闲期。
- 两个 Worker/多个 Partition、同号路由稳定、单号码顺序或明确约定的顺序语义、去重/idempotency、同一 Partition 不出现双 Owner。
- Worker 在输入过程中受控 crash/restart，之后继续看到新输入；已 `dispatch_taken` 但发送结果不确定的记录不盲目自动重发。**报告必须独立统计 taken_without_socket_written，不准用 `pending=0` 代替成功率。**

**门禁**：在线新输入稳定可见，无长时间 idle starvation，新增和恢复记录可对账；不得通过 `kill` + 全量重启模拟“发现新任务”。

### C2 — Writer 在线自动重放、投影与容量恢复

建立一条持续运行的 Integration E2E：Worker 和 `ProjectionWriterProcess` **并行运行**，运行中间按顺序注入：

1. Writer 正常 → 在线发送 100 条 → 投影追平。
2. 在发送前/中暂停 Writer 或注入仅测试 DB 的写锁冲突；继续发送另一批（不超过允许的 Journal 容量），验证 Worker 不等待 DuckDB 提交；记录 Writer backlog 与发送吞吐。
3. Writer 恢复 → 自动追平，要求 `applied == durable unique progress events`，重复应用为 0 或仅内部去重次数增加，**聚合业务计数不能重复**。
4. 队列满、IPC 通知丢失、Writer crash/restart、服务停止再启动后，仍从 Journal 自动补偿。不得人工调用测试 Runner 的事后 `journal.read()` 来假装生产恢复。
5. Journal 写失败、内容 CRC/完整性异常、磁盘软/硬水位：明确 fail-closed 范围，不能静默丢失；异常解除后恢复或明确给出需人工修复的错误。

**门禁**：正常 Writer 下无可观测长期 lag，有限时间 DB 不可写时既有发送能够继续（在真实持久化额度允许范围内），恢复后自动 drain 且无重复/丢失统计。保证内存使用有硬界。

### C3 — Pause/Cancel/Resume 实时控制与重启

通过正式控制 API 或与 API 完全相同的控制调用路径，验证：

- 单个大 Campaign 正在持续 Dispatch 时 Pause；明确记录命令发起、持久化、各 Worker 观察/确认、最后一个允许写入 Socket 的时间，不允许下一批仍大量发出。
- Pause-A 不得阻止同 Route 上仍具 Credit 的 Campaign-B；Cancel-A 阻止新的 Dispatch，已有在途 PDU 按声明的有界规则处理。
- Resume-A 后不需要 API 或 Worker 重启即可继续；在 Pause 期间停止整个服务、再启动，Pause 状态仍有效。
- Worker crash/restart + 新 owner epoch、Route reconnect、同号并发输入下控制仍生效；不会出现已 Cancel 活动重新自动激活。
- 控制状态原子持久化，错误格式/意外缺失不可静默当作全部 Running。

**门禁**：控制边界可证明且可测，活动之间不发生无关的全局暂停。

### C4 — 正式服务在线端到端与受控性能验证

**必须区分两个测试工具**：

- 原 `backend/tools/vnext_loopback_acceptance.py` 的 prefill-then-worker 路径保留作高吞吐回归，对照历史 P5500，不把它当成生产启动路径验证。
- 新增或扩展 `backend/tools/vnext_runtime_canary_acceptance.py`：使用 `backend/main.py`/FastAPI 启动服务、实际 `VNextRuntime.start()`、通过业务 API/实际文件摄取持续送入合成数据、4 Worker 长驻、实时 Writer 并行处理，真实 TCP Loopback SMPP，自动生成小体积证据及时间线。能在 Windows/WSL/EulerOS 复用。

门禁矩阵（每门独立全新 Run Root；**所有 Case 都应在 Worker/Writer 已启动后注入数据**）：

| ID | 工作负载/故障 | 必须满足的结果 |
| --- | --- | --- |
| R0 | 启动空闲 5–10 秒，后续注入 100 | 不重启 Worker 自动 `socket_written=100`，loopback 收到 100，投影最终追平 |
| R1 | 1,000，分多波次 + 多 Campaign + 同号覆盖 | 总数/去重口径明确，实际接收与去重期望一致，活动无无故饥饿 |
| R2 | 5,500，4 Campaign（2500/2500/250/250），在线逐波输入 | `generated=5500`, `socket_written=5500`, `loopback.received=5500`, `projection.applied=5500`, `pending=0`，无 missing/duplicate，严格 300s，须授权后执行 |
| R3 | Route TPS=80 PDU/s，P1K + 慢响应/有限窗口 | 实际 PDU 额度不超发、Window 不泄漏、独立 Route 不被堵住，完成时间与实际配额一致；不可把 `route_tps=0` 当作限速通过 |
| R4 | Writer 停顿/恢复，Queue 满、Journal 补偿 | Socket 主线在额度内持续；Writer 自动追平；全程独立采样 backlog/lag |
| R5 | Campaign A 暂停/取消/恢复，B 正常 | A 控制及时，B 可继续；重启后 A 状态保留 |
| R6 | Worker/Writer crash/restart 与分片重新接管 | 不出现双 Owner，事件最终投影可去重，未知与已 taken 未写 Socket 独立报告 |
| R7 | 长时间运行 ≥10 分钟，持续低/中负载及间歇突发 | RSS、CPU、fd、Queue/Journal/Spool age/bytes 无无界增长，背压高压暂停、低压自动恢复 |

说明：R2 的指标口径为 VNext `socket_written`，**不等于**旧系统 `accepted`；R2 真实 TCP Loopback 可另外记录 `submit_sm_resp`，但不要求逐条成功响应触发 DB 写入。合成测试不验证真实运营商投递率。

采样要求：运行中每 ≤1s 记录 Parent/Worker/Writer/Simulator 的 `RSS`、CPU、磁盘 bytes、fd、Queue depth、投影 lag、发送 PDU/秒、Route Credit 与 Window 使用量，输出峰值/时间序列，不能用结束时 `free -m` 代替峰值。采样器不得长期占用大量 CPU 或保存号码正文。若环境性能波动大，保留每次 Run 的开始/结束和配置；原 EulerOS 17.416s、315.8/s 只是历史非在线 Runner 对照，**不硬套为 R2 的新验收 SLA**。

### C5 — 双 VPN、EulerOS 受限验收与收口报告

必须按**可断点恢复**阶段执行，不允许假设同一网络同时访问 GitHub 与 EulerOS：

**阶段 A / GitHub 或下载 VPN：**
- 完成 C0–C4 的开发机实现、本地测试与修复。Windows 全量 pytest 和前端 build；WSL AlmaLinux ext4 做正式 Linux 基线。
- 按仓库 `AGENTS.md` 使用既有 CPython 3.11.5 standalone / 离线 wheelhouse。准备独立应用测试包、测试工具、资源限制启动脚本、校验清单和 SHA256，不携带 runtime 数据或真实凭据。
- 将报告写明 `STAGE_A_READY_FOR_TRANSFER` 或未通过原因；**到此停下，由用户手动切换 VPN**。

**阶段 B / 测试服务器 VPN：**
- 用外部拷贝/离线包方式落到独立 Run Root，不使用服务器 git clone/pip 在线下载。
- Read-only 主机预检 CPU、ext4、剩余磁盘和当前业务负载。通过后执行 R0→R1→R3→R4 的小型门禁和资源采样；R2/持续 R7 需要现场资源检查和用户许可。默认不启用真实 SMSC。
- 每一轮单独进程组和端口。结束清理本轮测试子进程及监听端口（不强杀不属于测试的进程），不删除原有服务器证据。
- 将有限 JSON、MD 摘要、SHA256、资源序列记录到 Run Root 并导回开发机。禁止敏感日志。

**阶段 C / 切回 GitHub VPN：**
- 离线核验服务器证据 SHA256；更新源码内新的自动化门禁、报告、部署文档；只提交**任务相关**源文件（未经用户明确要求不得提交或 push）。
- 生成主报告 `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`，注明 HEAD、dirty 保护、完整代码 diff、全部门禁、资源峰值、失败与复测、是否能进入真实测试 SMSC 的**后续评审**，不虚报已经通过。

## 五、结果状态与停止条件

必须输出两个不同状态：

- `VNEXT_RUNTIME_CANARY_CLOSURE=PASS | FAIL | BLOCKED_ENV | NOT_RUN`：本任务真实长驻服务是否完成所有必需门禁。
- `VNEXT_PIPELINE_READY_FOR_CANARY`：继承历史合成/Loopback 结果，不因本次局部 PASS 自动扩大为生产就绪。
- 旧 `INTEGRATED_NO_GO` 原样保留，不以 `socket_written` 抹掉旧 `accepted` G4/G6 的失败。

出现以下问题必须停止扩大规模、保留证据：无法识别在线新增任务、Writer 重放丢事件或重复统计、无法从 Pause 恢复、双 Owner、反复发送同一不明结果任务、Spool/Journal 坏数据被静默跳过、磁盘/内存逼近资源限制、SMPP 测试器连接外部地址、测试进程无法清理、源数据/测试目录越界。

**真正完成的条件**：C0–C3 的 P0 修复及测试全部通过；真实 `main + API + VNextRuntime` 在线入口的 R0/R1/R2/R3/R4/R5/R6 通过；R7 具备合格资源采样和背压证据；WSL 和 EulerOS 受限 Run 可追溯；没有为了性能牺牲短信协议 Window 与持久化 Spool 可靠边界，也没有重建双轨复杂架构。

如果阶段 A 已能通过但阶段 B 因 VPN/主机环境阻断，应如实输出 `BLOCKED_ENV`，仍交付已完成代码和本地证据，不伪造远端通过。

## 六、Codex 交付内容

- 任务相关源码修改与自动化测试（优先复用 `backend/vnext/`，只修真实路径）。
- 在线增量入队、Writer 崩溃与 Journal 补偿、控制命令时间线、Route 限速、高低水位、RSS 采样的可运行验收工具。
- `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`：根因、修改、前后对照、证据索引、未知项、结果状态。
- 离线可运行测试包/脚本、SHA256 清单、WSl/ext4/服务器独立报告；**运行产物放 `.acceptance-data` 或 `test-artifacts` 的本地未跟踪目录，不把日志、DB、Spool 直接纳入 Git 提交**。
- 最后简要列出已通过测试、失败原因、文件变更、尚需手动 VPN/现场授权的步骤；不得仅说“全部完成”却无实际运行证据。

## 七、Codex 启动指令（可直接复制）

```text
请执行 Marketing_Copilot_VNext_Runtime_Canary_Closure_Codex_Task.md。

以 GitHub main 4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e 为冻结参考，
先阅读 AGENTS.md 和仓库已有 VNext 代码，保护现有 dirty 文件。

先构造真实运行时失败复现，再修复：
1) 长驻 Worker 在线发现新增 Spool Task；
2) Writer/IPC 受阻后自动从持久 ProgressJournal 追平投影；
3) Pause/Cancel/Resume 在正在发送的大批次中及时生效且重启状态不丢。

继续做带实际 main/API 启动的持续输入测试、有限 Route TPS、
慢响应、Worker/Writer crash、独立背压与自动低压恢复、RSS 峰值采样。
保留 4 Campaign / 5500 规模和 300s 门禁；必须与旧 accepted 合同分开报告。

不得重新引入 legacy/reliable/fast 双主链，不增加每短信 DuckDB 同步事务，
不引入 Kafka/Redis/RabbitMQ，不触碰真实短信服务。

先完成开发机/WSL 测试与离线交付，然后暂停，等待我手动切换 VPN
再执行受限 EulerOS 隔离服务器验收。最终回传 SHA256 证据并出具报告。

请实际修改和运行测试，未经我明确授权不要 git commit / push。
```
