# Marketing Copilot — S2/S3/S4 一体化架构收敛 + 高性能 SMSC 模拟器 + 统一验收任务

- **任务编号**：MC-INTEGRATED-ARCH-FAST-SMSC-CLOSURE-20261016
- **推荐执行模型**：GPT-6.1 Sol High；首轮系统级因果/并发架构审计建议切换 Sol XHigh。普通测试与证据整理可人工切换 Luna Medium。
- **起点**：GitHub `shutu-ai/Marketing-Copilot`，`origin/main` 最新已知提交 `fd07c5b2c8c2b03dd71f3a3fa3754c83ec93e34f`；执行时核对真实 `HEAD` 和 `origin/main`，以实际源码而不是文档记录为准。
- **背景结论**：S0/S0.1 正确性已闭环；S1/S1.1 有界派发及结果硬字节容量 `S1_READY`；S2 已实现浅 handoff 和逐 PDU Window 原型；S2.1 Fair Fast checkpoint/credit 放大问题已修复，`S2.1_READY_FOR_S2.2`。**完整 S2/G3/G4/S3/S4/生产均未通过，不得误报。**
- **范围**：整合 SMSC Simulator 高性能测试能力、原 S2 余项、S3 Worker/文件扩容、S4 恢复/长稳工作，采用**一个任务、一个总体架构决策、一套端到端观测与守恒模型、连续实施、里程碑检查**。不是取消正确性门禁，也不是在每个中间里程碑 NO_GO 后机械停止整个工程。
- **不包含**：外部付费 SMS API 或第三方性能测试服务、S5 目标机部署、真实 SMSC 发送、真实号码、真实生产数据库、自动生产模式切换，以及未经批准的重大存储/MQ 选型变更。

## 0. 执行原则——避免再次陷入“局部修复—新问题—再补丁”

1. **先看完整系统，再改代码**：阅读 `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`、S0.1、S1/S1.1、S2、S2.1 的 RCA/验收报告，并核对当前源码、测试与历史未验证边界。若存在 `.codegraph/`，遵循 `AGENTS.md` 先使用 CodeGraph。
2. **不重做已验证的安全机制**：保留单 DuckDB native owner/cursor + OCC 事务重试、Journal/Write Buffer 字节容量、result/dispatch credit、Fast accumulator、attempt/part 的安全状态机、已有 DLR 回归与 Reliable/Fast 停止栅栏、默认 `legacy` 回滚路径。只有实测或确定性证据揭示缺陷时才调整。性能测试禁用 DLR，不因此删除历史 DLR 安全逻辑。
3. **同一根因一次处理全路径**：先建立 Claim → ReadyPool → Prepare → Route/Slot → Socket/PDU → Response → Journal/DB → Credit release 的全链路容量/等待模型；不得单纯增加新的队列、全局锁或 Ledger 掩盖根因。
4. **单 Owner**：保留单服务进程与唯一 DuckDB RW Owner；共享、有限的 File/SMS Worker 池。不得为了“扩容”而引入多个 DuckDB 写进程或每个 Campaign 独立数据库/进程。
5. **全局安全与局部隔离明确区分**：Campaign/Route 局部拥塞不应停止其他有资格的健康活动；DB/Journal/磁盘共同失效可以全局 fail-closed，同时尽力排空已收结果。
6. **失败先分类，后处理**：把 `BUSINESS_CORRECTNESS_FAIL`、`PERFORMANCE_REGRESSION`、`FAIRNESS_FAIL`、`ENV_BLOCKED`、`TEST_HARNESS_FAIL`、`NOT_EXECUTED` 分开。pytest 临时目录 ACL/只读错误绝不能记作业务失败，也不能记作测试 PASS。
7. **允许连续修复与重试**：中间里程碑发生可诊断的性能或公平性 FAIL 时，在本一体化任务范围内做 RCA、局部修复、重测；不用为每次失败新建 S2.x 工程。只有出现不安全重复外发、丢失权威业务事实、损坏 Journal/DB 或无法解释的跨代际重复，才停止相关发送测试、保存证据并优先关闭正确性缺陷。
8. **禁止通过调大参数作弊**：不能仅靠增加业务 timeout、TPS、Worker、队列容量、隐藏 unknown/partial、降低负载、延长等待预算、放宽对账条件把失败改成通过。合理的容量配置可优化，但需公平同参数 A/B 比较并明确其影响。
9. **不动无关工作区**：保留原有 `frontend/package-lock.json` 与所有未跟踪用户资料、运行 DB、WAL、Journal、备份、密钥、日志；不得 `git reset --hard`、`git clean -fd`、删除旧 `/tmp` 历史证据。遵循 `AGENTS.md`：未经新的明确授权**不自动 commit/push**。
10. **安全运行**：仅在新建 WSL AlmaLinux-8 ext4 隔离工作副本中使用 synthetic/loopback SMSC、合成号码与独立 DB/Journal/Watch/端口；不触碰真实 SMSC、真实任务数据。
11. **严格区分模拟器与业务限速**：性能场景的模拟器默认不限速、零人工响应延迟、DLR disabled；Marketing Copilot 的 Route TPS 仍保留并被单独验证。不得把“模拟器 TPS=200”与“业务 Route TPS=200”混为同一个约束，更不能把业务限速也关闭来获得好看的数字。
12. **两类测试不可混用**：正常性能/公平测试使用 fast-response 模拟器；故障恢复测试显式启用 slow/reject/drop/disconnect profile。性能指标必须同时给出模拟器独立能力和客户端实际 socket/response/全链路数据。

## 0A. 前置里程碑 M0：SMSC Simulator 高性能 Fast-Response 模式（优先实现）

> **本节是对原一体化任务的正式合并，不是附加建议。** 若不先排除模拟器自身的处理瓶颈，则后续声称“Marketing Copilot 只有 4 TPS / Worker 扩容没有收益”的结论不可信。M0 完成后应立即继续整个 S2–S4 任务，不另立项目。

### 0A.1 已核实的现状和需要打破的耦合

先检查以下真实文件及当前源码差异：

- `backend/tools/smsc_simulator.py`：Python `asyncio` loopback SMPP 3.4；`Session.run()` 对每个连接顺序 `await handle_submit()`；当前每个 `submit_sm` 还会同步 `print` 格式化的消息 JSON，并通过 `record_event()` 同步追加 JSONL；`handle_submit()` 调用全局 TPS 等待和可配置响应 `sleep`。这些行为**可能**成为测试端瓶颈，应先测量而不是直接假定。
- `backend/tools/local_e2e_acceptance.py`：当前 `--smsc-tps` 覆盖 `faults.tps`，这个参数同时传给模拟器 `--tps`，又写入 Marketing Copilot 的 SMPP route 配置 `tps`。这使“模拟器独立最大 TPS”和“应用 Route 限速”无法独立实验；必须拆开。
- `backend/tools/local_e2e_scenarios.json`：`stress-1k` 原场景 `faults.tps=50`；历史 G3/S2.1 通常通过 `--smsc-tps 200` 覆盖。不能把场景名称误解为无速率限制，也不能据此认为模拟器已被验证具备 200 TPS 实际承载能力。
- 最新 S2.1 三次 1K 的约 255–273 秒是**端到端进程 uptime**（含摄取、调度、写结果），不是纯 SMSC 响应时延或独立模拟器容量。不得从 1000/255 算出约 4 TPS 后直接归因于模拟器。

### 0A.2 模拟器运行模式与默认语义

增加明确的 Simulator Mode，建议 `fast` / `fault` 两种模式（按现有代码命名风格实现；保留现有 CLI 兼容）。

**`fast`：正常吞吐、公平性及扩容基线的默认选择。**

1. 保持真实 TCP/loopback、SMPP bind/sequence/message-id、`submit_sm` PDU 接收解析和合法性检查，真实发送 `submit_sm_resp`；**绝不能用直接回调/模拟成功计数替代 Socket 协议**。
2. 收到合法 PDU 后尽快返回 `submit_sm_resp`，人工响应延迟默认 `0`；不对每个 PDU 做多余的 `asyncio.sleep()`。注意响应仍受真实 socket/事件循环调度，不能承诺“0ms 实际 RTT”。
3. 默认 `simulator_tps=0` 表示**模拟器本身不人为限速**。Marketing Copilot 的 `route_tps` 仍为显式配置（例如 200 parts/s）并保持 route-shared 计费，不能乘以 SMPP pool size。
4. **DLR 完全关闭**：无 `deliver_sm` 生成、无 DLR 延时任务、无送达统计或等待；性能运行显式设置应用 `delivery_receipt_mode=disabled`。在 fast 模式 `registered_delivery` 被请求时可按协议回应 `submit_sm_resp`，但此模式仍不得生成 `deliver_sm`；测试报告必须显式显示 `dlr_generated=0`。
5. 默认禁止每个 PDU 的同步 `print`、pretty JSON 输出以及消息正文/手机号日志；日志只记录启动配置、错误、有界周期聚合摘要、最终 counters。默认敏感内容不得进入 stdout/stderr。
6. 协议 responder 不得同步等待磁盘 JSONL 完成后才回复；使用有界、低开销的审计事件缓冲和批量异步写入（或经证明不影响关键路径的等价方案）。**不得丢审计证据来伪造 PASS**：缓冲溢出、后台写盘失败、计数缺口必须显式标记实验 `TEST_HARNESS_FAIL` / `SIMULATOR_AUDIT_INCOMPLETE` 并保留诊断，不能静默 drop / 仅采样后仍声称逐 PDU 独立对账。
7. 每个连接可以同时拥有多个在途 submit PDU；将串行 `await handle_submit()` 中阻塞后续读取的操作移出关键链路，使用**有界**并发 submit handler / responder、一个受控的 socket writer/写锁和有限待处理 PDU 容量。不得为每个 PDU 无限创建 task；响应 sequence 必须与请求匹配。可允许标准 SMPP 允许的响应顺序变化，并用测试验证。需要在压力下有明确的 backpressure、shutdown drain 和错误报告。
8. 保留对长短信 UDH、Unicode/Flash、绑定、解绑定、enquire_link、pdu 格式、message-id 与 `submit_sm_resp` 的兼容。回应 `success` 不等于运营商送达；业务 `accepted` 语义不许改成 `delivered`。
9. 关闭不必要 DLR 创建与日志后，保持准确的 `received`, `response_sent`, `response_success`, `response_failed`, `inflight`, `max_inflight`, `error`, `bind_count` 统计及相应 P50/P95/P99 response latency；最好区分 `server_parse_to_response_write` 和 `client_socket_to_resp_observed`，不以时间戳不同时钟直接相减。

**`fault`：异常/协议正确性专项模式。**

- 保留现有可配置 `--tps`、submit-response-delay、拒绝、丢响应、断连、多分片 partial、迟到 response 等故障注入机制，以及必要时的专门 DLR 协议回归。
- DLR 不参与**常规吞吐基准或多 Campaign 公平验收**；用户本轮要求忽略 DLR，对其不新增大规模模拟、吞吐 SLA 或重型完整送达测试。已有重要 DLR 安全状态机/单元回归仍不能删除或掩盖。
- 故障注入模式不能悄悄应用于 fast 正常性能用例；场景必须在运行 manifest 中记录 `simulator_mode=fast|fault`、inject 配置、实际生效值。

### 0A.3 将应用限速与模拟器限速完全解耦

建议在 `local_e2e_acceptance.py` 增加显式参数（如有更符合仓库风格的名称可调整，但语义要固定）：

| 新参数 / 语义 | 性能基线值 | 作用范围 |
|---|---|---|
| `--simulator-mode fast` | `fast` | 只影响 SMSC 模拟器进程运行模式 |
| `--simulator-tps 0` | `0` = 不限速 | **只影响模拟器**的人工响应/接收节流 |
| `--simulator-response-delay 0` | `0` 秒 | **只影响模拟器**的故障注入延迟 |
| `--route-tps 200` | `200` parts/s | **只影响 Marketing Copilot** 的 SMPP route-shared pacing |
| `--delivery-receipt-mode disabled` | `disabled` | 性能测试应用侧不请求 DLR，模拟器也不生成 DLR |
| `--simulator-audit fast` | `fast` | 启用轻量但可完整核对的事件审计，非必需严格按此名字 |

兼容规则：保留旧 `--smsc-tps` 并**明确记录其 legacy 含义**（历史上同时控制两端）；新任务/G3/G4/G6 必须分别显式传 `--simulator-tps` 和 `--route-tps`。若新旧参数同时出现，优先 fail-fast 报配置冲突，而不是静默任选值。`0` 不能被 `or 200` 兜底表达式误处理为 200。`tps_scope=simulator` 与 `tps_scope=route` 的实际值必须同时写入报告和 SMSC 启动日志。

模拟器模式选择不得修改生产 `smpp_gateway.py` 的 TPS/window/route 逻辑，也不得让生产 API 指向虚拟响应器。要通过 E2E harness 参数和独立模拟器进程的显式配置实现。

### 0A.4 先测试模拟器本身，再对 Marketing Copilot 归因

提供一个最小独立 SMPP load generator（可复用已有 harness 里的协议客户端或新增 `backend/tools/benchmark_smsc_simulator.py`，不依赖外部付费 API），完成：

1. **SIM-1 独立能力基线**：仅启动 loopback SMSC，不启动 Marketing Copilot；使用 pool=1/4、window=1/2/50、响应 0、DLR off、simulator_tps=0，逐步提升发送压力。记录实际稳定 `submit_sm`/s、response/s、P50/P95/P99 RTT、CPU/RSS、event loop lag、审计 backlog、每 bind 活跃窗口与真实 submit/response 总数；至少一次稳定持续 30–60 秒，环境参数记录完整。
2. **SIM-2 速率控制对照**：simulator_tps=50/200 只影响模拟器，route_tps=50/200 只影响应用，运行 manifest 核对没有跨参数污染；独立客户端施压时模拟器被设 50 应呈限速特征，fast unlimited 应不被该人工限速控制。不要把 Python 调度精度下的速率等同于毫秒级严格上界。
3. **SIM-3 结果一致性**：多 bind + 多 PDU in flight，序列号一对一映射，没有重复/丢响应；验收必须能从模拟器协议边界独立确定 submit/response 计数，且和客户端日志核对一致。若开启故障注入，统计必须能明确区分**故意丢响应**和审计故障。
4. **SIM-4 编码与兼容**：ASCII/UCS2/Flash/multipart，bind/unbind、enquire_link、断连以及旧 fault mode 关键行为不退化；fast 模式不产生 DLR。
5. **SIM-5 日志成本 A/B**：同硬件同负载比较旧 debug/sync-JSONL 与 fast 模式，确认取消日志阻塞是否真实提高 simulator TPS/响应时间；不可仅以代码注释代替数字。
6. **SIM-6 模拟器不会限制被测吞吐**：当测试业务 Route 目标为 `R` parts/s 时，模拟器独立可持续能力应有充分余量，**建议至少 `2 × R`**（例如 R=200，模拟器独立稳定 400+ part/s）；若达不到则先优化模拟器或将性能结果标记 `SIMULATOR_LIMITED/NOT_VERIFIED`，**不得据此说应用必然慢**。该 2 倍只是基准可信度门槛，不是强加给 Marketing Copilot 的业务性能 SLA；如果最终确定不同余量阈值，须写明依据与结果。

### 0A.5 标准性能测试分层：三套数据不可混为同一个 TPS

- **A / Simulator-only**：量独立模拟器能力与 SMPP 响应延迟，不接 Marketing Copilot；验证测试工具足以胜任。
- **B / Sender-only**：预生成任务，不跑文件摄取；开启 Fair + 真实 socket + fast simulator。记录任务从 eligible→socket write→response→Journal/DB commit 的瓶颈，区分 `client_submit_sm/s`、`client_submit_sm_resp/s`、`db_accepted_tasks/s`，并核对 1K/多分片。
- **C / Full E2E**：完整 watch/file ingestion→Claim→ReadyPool→Slot→SMSC→Journal/DB；记录从第一有效输入到终态的端到端 elapsed、稳态业务 task/s、实际 PDU/s 与结果处理速率。与 B/A 同硬件、同连接数和路由限速对照；**任何一组失速时先定位慢在哪个阶段，再优化生产代码**。

如模拟器独立能力未被确认，C 的数据仍可用于业务正确性/守恒检查，但不可拿来证明生产应用最大 TPS 或 Worker 扩展收益。

### 0A.6 M0 完成条件

- `SIM-1` 至 `SIM-5` 均有原始可复核结果；`SIM-6` 给出模拟器容量余量判定。
- Fast 模拟器默认 0 人为延迟、0 人为 TPS 限制、DLR disabled、无 per-message sync console log；轻量审计仍完整。
- Harness 已将 route TPS / simulator TPS 分离，所有标准测试显式配置并输出有效值；向后兼容旧参数。
- 旧故障测试用例仍可运行，且 fast mode / fault mode 互不串扰。
- 如模拟器能力未达到基准余量，继续在本总任务内修复模拟器；不得通过放宽应用业务正确性约束变通。

---

## 1. 第一个里程碑：系统级审计、观测与收敛设计（可以立即实施）

### 1.1 建立一张唯一的“资源所有权与等待图”

对每类资源明确：容量单位（task/part/byte/slot/事务）、谁 reserve、谁持有、谁转移、谁 release、异常与重启后的责任、是否有锁等待：

- File admission、chunk/file lease、Batch/SFTP/copy I/O；
- Claim/Ready/Prepare/Route/Slot handoff；
- per-route TPS、per-connection PDU Window、multipart continuation；
- Fast checkpoint accumulator、Result Parent/Child Credit、Journal normal/emergency/terminal、Write Buffer replay/retry/debt；
- 单 DuckDB Owner 的 serialized 与 OCC事务/写入争用。

识别重复占用、提前 Claim、credit 迟放、全局等待链、锁反序、无界队列、吞吐放大和单 Campaign 对其他 Campaign 的传播路径。评估是否可简化现有机制，**仅在有充分证据时合并/删去冗余层**，不得破坏 S0–S1.1 安全保证。

### 1.2 补齐端到端性能分解

建立最小侵入的阶段观测与可复核 trace（不得使用手机号、正文、message-id 作指标 labels）：

`eligible → candidate → reserved → claimed → prepared → slot_admitted → socket_pdu_written → response → durable_fsync → db_committed → terminal → credit_released`

至少记录每阶段：throughput（task/s 与 part/s 分开）、队列长度/驻留 bytes、等待时间 P50/P95/P99、inflight、credit used/limit、最老 eligible age、每 Campaign/Route 最近一次**真实 socket write**、DB 事务数/冲突/持锁耗时、checkpoint 聚合大小、Journal fsync、CPU/RSS、进展停滞根因。保持低开销，不得通过无界 per-task 内存追踪制造新问题。

对 S2.1 的 1K 运行耗时约 255–273 秒，仅将其视作**全流程 uptime**而非纯 socket 吞吐；分析阶段分布和瓶颈，不直接把 SMSC 设置 `200 TPS` 当作端到端必达吞吐。必须先按 M0 区分 Simulator-only / Sender-only / Full E2E，分清模拟器收包、模拟器发回包、客户端收到响应、DB accepted 四类时间；模拟器能力不足时，不得把被测应用的吞吐归因为数据库/调度问题。

### 1.3 收敛决策文档

输出 `Docs/Marketing_Copilot_Integrated_Architecture_Convergence_Plan.md`：当前端到端资源/等待图、根因候选与证据等级、必须改/暂不改的模块、统一容量账、目标路径、锁协议、可回滚界面、测试矩阵与风险。**不要另起一套新架构**；复用 Phase1 的单 Owner、分层隔离和优先复用现有代码决策。

完成计划后，在同一个总任务内继续实施，不需用户为每个原阶段重新写独立任务。

## 2. 第二个里程碑：完成 S2 真实 socket 公平与正确性

### 2.1 Route/Slot 公平

- 对 `(campaign_type, campaign_id, resolved_route_id)` 在真正 socket admission 实施可验证的 parts-based DRR（不是只在 Claim 或 Slot 选择时累计任务数）。
- 空闲健康 Slot 拉取/准入 eligible 活动；Slot 本地最多一个浅交接描述符或同等级严格有界预算，不能恢复深 FIFO head-of-line 阻塞。
- 同 Route 的 A 持续大量 backlog 时，B/C 有资格且下游有 credit 必须持续获得真实 socket 写入机会；不同 Route 的 A 故障不能阻止健康 Route 的 B/C。
- 固定 route revision/connection generation、路由/账号 TPS 合同；`route=0` 只能候选，不能绕开物理容量。Config revision/fallback 变更不得自动重发已写出/不明 attempt。
- 严格控制 per-Campaign/Route outstanding parts 与全局额度，保持 work-conserving，但不凭配置名字承诺硬保底 SLA。

### 2.2 逐 PDU Window、Multipart 让出

- 逐 PDU reserve/bind `(connection_generation, sequence)`；正常 response/timeout/异常/断连各路径恰好一次释放；旧代迟到响应不得释放新代 permits。
- **超时不能视作 SMSC 未处理**。对 uncertain write 先停止旧代写入、隔离并确认断开，再结清该代本地许可，保留 durable unknown/partial；不可仅超时就重新取得可能仍在外部生效的发送资格。
- 支持 `parts > window`，尤其 window=1/2 且多分片，不能一次拿全量 permits、不能死锁。
- 一个长 multipart 仍在同一 Slot/connection generation，但必须有有界 continuation/yield 或以可靠观测证实其最长非抢占时间符合公平预算；不得为提升公平而跨连接乱发同一 multipart。
- 在 Pause/Cancel、每日窗口结束、部分发送、中途断线与重连时继续遵守 S0/R03/R17 栅栏与保守状态机。

### 2.3 Fast、Reliable 与 Result Pipeline 共同合同

- Fast checkpoint 应继续以**有界聚合**接入结果容量账，避免每任务独立事务放大；聚合 credit 计入 normal parent，不得误计 emergency 并触发误停。
- 保证 Journal、Write Buffer、Fast accumulator、Dispatch parent credit 的容量与释放单调一致，无 double-release、无孤儿 durable debt、无 checkpoint/terminal 假确认。
- 如果结果积压形成全局瓶颈，先区分真实 DB/磁盘故障、模拟器响应瓶颈与仅因过度保守释放策略导致的自阻塞；保留安全前提下优化周期/批量和释放点。
- 保留可选 RabbitMQ/同步直发在 Fair 下当前明确拒绝的行为。若要求宣称全入口 Fair 支持，需适配并补独立门禁；否则在报告中明确列为不支持，绝不能静默绕开 credit。

## 3. 第三个里程碑：S3 Worker/文件扩容与源头公平

与 S2 调度改造在同一工程周期内协调实施、profiling 与复测，不需另起项目。所有扩容吞吐曲线必须基于已测得充足容量余量的 Fast-Response SMSC；若模拟器或 Route 限额已饱和，必须单独标出平台期的客观原因：

- Realtime file claim 锁内只做最小原子认领；解析放锁外；按活动轮转、活动级在途文件/bytes 配额、全局 bounded 工作池。
- 长 CSV 在**已提交的安全 chunk checkpoint**处让出，让 Worker=1 时 B 也有进展；不允许在带引号/跨行 CSV 的任意 byte offset 错误恢复。保留 file hash、去重、lease/token、权威 chunk/task 原子性。
- Batch source/generation 锁不得跨长 I/O、解析或全局背压等待；Batch API 与 scheduler 路径都要覆盖。
- SFTP/copy 慢源的活动间同步 I/O 阻塞需局部隔离（有限 I/O 池、超时/取消、活动配额）；不可用无限子线程。
- XLSX 当前整表加载有 RSS 风险：先加明确文件 bytes/内存准入与有界并行，不因没有实测就强制引入复杂 subprocess；需要时仅把**纯解析**移至 child，父进程独占 DB。
- SMS worker scaling 与文件 worker scaling 以实测收益/瓶颈为准，不承诺线性加速。不要将 DB/Journal/socket 已饱和时增加 Worker 误报为扩展成功。
- 测试 worker 点位遵循 Phase1：file=1/2/4/8/16，SMS=1/2/4/8/16/32，适当组合；每个关键配置至少三次独立测量，记录原始数据、环境、等待与 CPU/RSS，不依赖虚构性能数字。

## 4. 第四个里程碑：S4 恢复、背压和长稳闭环

- 完整启动 replay barrier：唯一 Owner → 校验 Journal 格式/CRC → 幂等 replay/DB 对账 → attempt/part/claim 归类 → 恢复文件 lease 与活动 generation → 新准入；异常或未决 quarantine 时 fail-closed。
- 崩溃点矩阵：claim commit/publish 前后、intent 前后、socket write 及 response 之间、Journal fsync 与 DB commit 之间、DB commit 与 terminal 之间；验证重复 event 幂等、旧 epoch fenced、可证明未写的任务才能按合同重排，unknown/partial 不自动整条重发。
- Journal replay/index/compact、Write Buffer pending/retry、terminal/emergency 和 result parent/child 的字节上限，故障期间内存不会无界增长；持久化不健康停止**新发送**但允许安全的已收结果排空。
- 严格 bounded shutdown/restart：不能把尚存活的旧 sender/worker 从 map 删除后就允许新 epoch 并发写；pause/cancel 不全局 flush 其他 Campaign。
- 多活动持续负载：**有效持续 600 秒**的可观测业务进展，而不是空转；长时间无 socket/DB 进展应捕获线程栈、Stage waiting 和 credit 水位。
- 单个 Campaign、Slot、Route 故障应与健康活动隔离；DB、Journal、磁盘共同故障明确作为全局安全阻断，不以“绝对互不影响”作不可能承诺。

## 5. 统一验证矩阵：实施可以连续，但最终证据不可拼接旧 SHA

### 5.1 环境卫生先行

- 发现历史 pytest 报告有 `41 passed + 612 environment errors`，系既有只读临时路径问题；在**全新隔离 WSL ext4 目录**构建/运行测试，不触碰旧只读目录，不污染旧结果。`pytest -p no:cacheprovider --basetemp=<fresh/new/path>`；必要时显式设置 `TMPDIR/TMP/TEMP` 到全新可写路径。
- 若确有同源代码/同 fixture 引发的业务错误，分类记录。环境修复不等于业务修复，不能偷换。
- 始终保留测试源码 SHA + 测试文件 manifest + 环境/依赖版本 + flags + scenario hash + 原始日志 + 独立 DB/Journal/端口路径。

### 5.2 门禁顺序（重构工作跨里程碑连续进行；最终整体验收仍有前置依赖）

0. **G-SIM（M0）**：Fast-Response SMSC 模拟器先完成 SIM-1–SIM-6；必须证明该模拟器能够快速响应、多 bind 并发、DLR off、无每条同步日志，并对测试 Route TPS 有足够余量；若不能证明，则后续 G3/G4/G6 中**性能相关**结论 `SIMULATOR_LIMITED/NOT_VERIFIED`，业务正确性实验可继续但不能“测出应用只有约 4 TPS”后直接改架构。
1. **G0**：最终源码全套后端 `pytest backend/tests -q` 完整退出码 0，frontend build 成功；所有重要 OCC/owner、DLR、stop、Journal/credit、PDU Window 回归通过。若仅环境障碍标 `ENV_BLOCKED`，立即建立新隔离环境重试，不声称 PASS。
2. **G1**：ingest-only 10×100 → 1000 unique task/10 file+chunk authoritative complete/socket=0；sender-only pool1/4 分别对账；ASCII/UCS2/Flash/Multipart、window1/2/50、fast/reliable 的必要门禁。Sender-only 使用 Fast-Response Simulator、DLR disabled，并记录送达模拟器的真实 PDU TPS、模拟器响应 RTT、DB accepted TPS；单独保存 fault-mode 协议回归，不得混入正常吞吐曲线。
3. **G3**：**最终相同源码 SHA**、Fair 显式开启、四组合 `serialized/reliable`, `occ/reliable`, `serialized/fast`, `occ/fast`，各 3 个全新进程 × 10 files × 100 = 1K；pool4、window50、**业务 route TPS200、模拟器 TPS unlimited(0)、submit response delay 0、simulator fast mode、DLR disabled**、task8、ingest2、copy2，保持原门禁时间口径；每轮独立 DB、Journal、Watch、ports、日志。对账 task key 集合、actual socket/PDU、attempt/parts、response、Journal event、文件/chunk、unknown/quarantine；额外保存 simulator-only 基线与模拟器协议边界的 response/s。此次模拟器条件改变，旧配置的 S2/S2.1 性能结果仅作历史参考，不能拼进新 G3 证据。**任何失败先在同一一体化任务内 RCA + 修复 + 用最终 SHA 重跑受影响及必要全矩阵，而非自动进入无限独立 S2.x。**
4. **G4**：真实 loopback socket 多 Campaign A=50K pending、B/C 各1K，同/异 route，pool1/4、window1/2/50、A pause/cancel、long multipart/continuation。**常规公平/吞吐子场景一律 fast simulator + DLR off + 不限速模拟器；慢 ACK、Slot 断线等隔离故障使用独立 fault-mode 子场景**，不能混合两者的吞吐数字。记录 socket 层 per-Campaign parts served、wait P50/P95/最长停顿、eligible age、route/slot credit；健康且 eligible 有 credit 的 B/C 不得因为 A 的可隔离拥塞长期饥饿。按设计推导/freeze 实际等待预算，不能单凭所有线程存活或 fake socket 判通过。
5. **G5**：fault-mode 的慢响应/断连/拒绝/丢响应注入 + restart/replay/barrier + Journal/DB outage/credit 守恒/unknown 保守处理。**DLR 不参与性能测量，本轮不扩展新的 DLR 压测或送达 SLA**；保留必要的既有 DLR 状态机单元/集成回归，确保不会将 unknown/partial 自动整条重发。
6. **G6**：Worker 扩容矩阵与真实有效 600 秒多 Campaign 长稳；使用 Fast-Response Simulator 且 DLR off、模拟器不限速、Route TPS 明示，测吞吐、RSS、CPU、DB/Journal/PDU 限制因素、P50/P95 与最差等待；不能仅以 Worker 数被接受作为成功。
7. **Legacy 对照**：默认 legacy 的基础回归与关键兼容路径；Fair 必须是显式 opt-in。可选直发/RabbitMQ 入口在 Fair 下不支持时应明确拒绝且经测试验证。

**不要求在缺少真实 SMSC/SLES12SP5 的环境中伪造 G7 PASS**。G7/S5 与生产切换独立留到用户明确授权。

### 5.3 安全/对账不变量（全程强制）

- 不丢任务、不重复 durable task、每 file/chunk 权威状态/计数可对账；展示投影不是权威。
- 每 task/claim/attempt/part/connection generation/submit_sm/resp/Journal/DB event 可关联；重复回调/迟到响应不能越代际写新状态。
- `unknown`、`partial` 和可能已经写入 socket 的 attempt **绝不自动整条重发**；不能声称无 SMSC 幂等合同的 exactly-once。
- `acquired - released = live`，每单位硬使用量不超过配置上限；各 campaign/route 子账与总账不矛盾；不以未持久化 queue put 当结果已经落盘。
- pause/cancel/end/每日窗口栅栏成功返回后，无新的 submit_sm；已出 socket 的请求结果不可撤销或伪造为未发。
- 任意未验证门禁应如实说明原因和影响；不得因“整体正在推进”而将其改为 PASS。

## 6. 交付成果与验收结论

只生成**一份主报告**：

`Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md`

必须包含：

1. 冻结的起点/最终 SHA（或未提交工作区 manifest）、改动列表与所有用户工作区的保护记录。
2. 统一端到端架构与资源/锁/credit 所有权图；现有复杂度简化与保留的理由；另外包含 SMSC 模拟器串行/日志/限速瓶颈的真实证据和高性能改造设计。
3. 测得的瓶颈、根因及修复前后相同配置比较；必须有 **Simulator-only / Sender-only / Full E2E** 三组分层测试，记录 simulator 能力余量和有效 route/simulator 速率配置，将模拟器处理率、socket TPS 和全流程 throughput 明确区分。
4. 每个最终门禁 G-SIM/G0/G1/G3/G4/G5/G6 的真实 PASS/FAIL/NOT_RUN/ENV_BLOCKED、原始证据路径；不拼接不同源码版本结果冒充当前通过。
5. fairness/isolation 的真实 socket 事件证据、Worker 扩容曲线、600 秒长稳与 failure-recovery 对账；注明性能场景 `DLR disabled`，模拟器 `simulator_tps=0`（无限制）并非应用 Route TPS=0。
6. Remaining risk、legacy rollback、是否需要独立 S5 的明确结论，以及 fast/fault simulator 模式兼容与故障注入没有被永久移除的证据。

结论只能为：

- `INTEGRATED_READY_FOR_S5`：G-SIM 与所有本任务适用的最终安全/公平/扩容/恢复门禁真实通过；仍**不等于生产 READY**。
- `INTEGRATED_NO_GO`：存在实质安全、正确性、公平、性能或恢复阻断。
- `INTEGRATED_BLOCKED_ENV`：仅外部环境或不可获得的测试依赖阻断，并列出已完成与未执行的精确范围；不得冒充功能 PASS。

本一体化任务允许**中间阶段调整实现和继续推进**，但最终判定绝不降低验收要求。请执行者在同一主报告持续更新，而不是每发现新问题就生成新的 S2.x/S3.x 任务书。最后汇报架构是否收敛、实际测试结果和残余风险。


## 7. 交给 Codex 的执行入口与分工（同一个任务，不生成新 S2.x）

本任务已经包含 M0 SMSC 模拟器性能基线、系统级审计、S2/S3/S4 实施与全部 G-SIM/G0/G1/G3/G4/G5/G6 验收。执行顺序建议：

```text
M0  Simulator fast/fault 分离 + 限速参数解耦 + 独立性能基线
  ↓
M1  端到端瓶颈剖析与架构收敛计划
  ↓
M2  S2 Socket Fairness / PDU Window / multipart 完整收敛
  ↕   （与 M3 交叉实施并复测，但安全缺陷优先）
M3  S3 File/SMS Worker 源头公平和扩容
  ↓
M4  S4 恢复、故障、安全和有效 600 秒长稳
  ↓
Final G-SIM/G0/G1/G3/G4/G5/G6 + legacy → INTEGRATED_* 判定
```

交付过程持续更新 `Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md`；不要每个小问题另起一个新的任务 MD。遇到会话上下文过长，先在此主报告中写入当前代码 SHA/manifest、已验证门禁、未解决阻断、下一步精确入口，然后在新 Codex 会话继续读取**同一任务**。

若需要手动换模型：高风险锁顺序、状态/恢复协议和架构因果分析使用 **GPT-6.1 Sol XHigh**；实现、常规 RCA 与集成测试使用 **GPT-6.1 Sol High**。模型切换由使用者完成，不要把切换当作业务流程依赖。

**最终提示**：现在的首要任务是让 SMSC 模拟器足够快、并把独立 simulator TPS 与业务 route TPS 分开，再依据完整阶段剖析去优化系统。绝不为了提升报告数字而绕过真实 SMPP socket、数据库、Journal、Credit 或发送安全栅栏。
