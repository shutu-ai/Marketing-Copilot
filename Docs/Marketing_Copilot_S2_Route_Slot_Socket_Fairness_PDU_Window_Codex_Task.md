# Marketing Copilot — S2 Route/Slot Socket Fairness & PDU Window Refactor

**任务 ID**：`MC-SCHED-S2-SOCKET-FAIRNESS-20261015`  
**建议 Codex 模型**：`gpt-6.1-sol` / `xhigh`（复杂调度、PDU Window、竞态和最终审查）；普通测试执行或低风险修复可以人工切换至 `high` 或经济模型。  
**代码基线**：`319878bfbc9fcd2f7a083be15d22e9a45a78825e`，实施前核对本地 HEAD、`origin/main` 和工作区状态。  
**目标门禁**：`S2_READY` / `S2_NO_GO`；**当前状态**：`S1_READY`，`socket_fairness=NOT_VERIFIED`，完整 G3、生产切换仍 `NO_GO / NOT_VERIFIED`。

## 0. 执行原则

先阅读仓库 `AGENTS.md`，并遵守已有用户文件保护、CodeGraph（如存在）、WSL/AlmaLinux-8、CPython 3.11 及测试要求。阅读以下正式文档，优先以本轮实际源码为准：

- `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`：§5.1–5.6、§7 S2、§8 T03–T07/T11/T13 及对账口径。
- `Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md`。
- `Docs/Marketing_Copilot_S1_Bounded_Dispatch_Implementation_and_Acceptance.md`。
- `Docs/Marketing_Copilot_S1_1_Result_Pipeline_Hard_Bytes_Closure_and_Acceptance.md`。
- 相关 G3/G4 RCA、现有测试和 `backend/tools/local_e2e_acceptance.py`；不能拿旧 SHA 的 PASS 作为当前门禁结果。

**约束**：S2 仅修改 Fair 模式的 Route/Slot 发送公平与 PDU 级容量控制、必要的协议/诊断/测试；不开始 S3 文件/Batch/SFTP/Worker 扩容、不开始 S4 replay/长稳重构、不部署、不连接真实 SMSC/手机号，不自动切生产。默认 `MC_CAMPAIGN_DISPATCH_MODE=legacy`、旧入口和已稳定的 S0/S1.1 安全功能必须保留；不能用加大队列、超时、重试、线程数或牺牲业务正确性来掩盖公平缺陷。维护**单服务进程、单 DuckDB native owner/cursor + OCC、一个共享文件池与 SMS Worker 池**，不引入外部 MQ 或第二主库 RW Owner。禁止 reset/clean/删除或覆盖用户现有未提交内容、运行数据库、Journal、备份、凭据和日志。测试仅操作新建隔离目录、模拟 SMSC 和合成数据。未经用户另行授权不要 commit/push；完成后报告修改文件、测试和门禁。

## 1. 基线事实与 S2 核心缺口（先核实，勿盲从行号）

在 `319878bf` 上，`backend/services/bounded_dispatch_service.py` 已实现 Claim 前 reservation 和按 `(campaign_type, campaign_id, resolved_route_id)` 分组的 ready/prepare；`backend/services/task_worker.py` 在 Fair 模式可非阻塞交接至 SMPP。`backend/services/result_capacity_service.py` / `result_journal_service.py` / `sms_write_buffer.py` 已完成 hard-bytes、durable/terminal、bounded replay/compact，并在 S1.1 报告中验证。

但 `backend/services/smpp_gateway.py` 的 `_SmppConnection._send_queue` 仍为 **Slot 混合 FIFO**；`try_send_async()` 仅是非阻塞 `put_nowait`，未构成 socket 级公平。`_send_windowed_async()` 仍在整条 task 入口获取 `_window` permit，`_take_async_completion_locked()` / reaper / disconnect 按 task 释放；`inflight` 则可能按 sequence/parts 描述。长 multipart、慢回包、坏 Slot 和连接恢复可导致 head-of-line 阻塞、窗口计量不一致。`_throttle` 已有 route 共享 TPS，但单靠 Claim DRR/现有 Slot 轮转不能保障最终 socket 公平。

先输出一个简短源码路径及锁/所有权图，确认真正的 `send_async`、Fair `try_send_async`、stop gate、send intent、part response、timeout/connection-generation 和 Journal handoff 边界；特别核实旧代码的 premature timeout release / late response 竞态。把发现写进最终报告，而非先行发明新的发送协议。

## 2. A：冻结基线、设计最小可实施合同

1. 记录准确 HEAD、dirty files、实际源 manifest SHA、依赖/环境、完整配置和旧已知测试结果；保留 `frontend/package-lock.json` 与现存临时资料。
2. 用真实 loopback SMPP 及可控 fake clock/barrier 做 **当前失败对照**：Campaign A 连续积压、B/C 在同 route 同/不同 Slot 可用时受 FIFO 阻塞；长 multipart + 窗口小于 parts；单 Slot 慢响应/断线。若现有 harness 不支持，先开发最小隔离 harness。失败基线应以实际 socket `submit_sm` 时序为 oracle，不只用 Claim/prepare 或 mock 调用次数。
3. 确定每个逻辑身份：`campaign_type/id`、`resolved config_id + revision`、`owner_epoch`、`claim_token`、`attempt_token`、`connection_generation`、`task_id/part_no/sequence`、result credit ID。route=0 仅为预选占位，不能拿物理 socket credit。
4. 制定锁顺序与资源不变量，落实 Phase1 §5.6：scheduler mutex 内禁止 DB/网络/fsync/阻塞等待；campaign gate 内不等 TPS/window/response；Slot condition 不调用持久化回调；socket write 只有短锁。发现无法安全迁移的 legacy 兼容冲突时立即记录阻断而不暗中降级。

## 3. B：Route Fair Slot Arbiter 与浅 handoff

1. Fair 模式新增或复用轻量 **per-route arbiter**，在健康、有容量的 Slot 主动向活动分组取下一可发送工作项；每组 `oldest-due FIFO`，组间 DRR/weighted fair 按**实际编码 PDU parts** 服务成本计费，维护有界 deficit/burst、ready descriptors 和 eligible-at。保留 `work-conserving` 借用空闲份额，新的 B/C 活动到达后可获得公平机会，禁止 A 长期霸占已预约未来时隙。统一考虑一个 Campaign 横跨多个 routes 的活动总账限制。
2. 每 Slot 只允许一个未启动 handoff 描述符或等效很浅的硬预算；**Fair 不再将多个活动塞入旧 Slot 深 FIFO**。普通 SMS task worker 不可等 route/window/TPS/Socket 网络等待，连接建立/重试属于独立 I/O 路径。Fair 的 ReadyPool、result parent/child credit、Claim 恢复/失败、stop/取消拥有清晰的单次 acquire→transfer→release 状态机。
3. 按 `config_id + revision` 固定物理 route，route 总 TPS 沿用现有共享上限，**不能随着 pool_size 倍增 TPS**；route 不健康仅暂停对应 route，其他健康 route 可持续服务。Slot down/重连/动态配置变更按 revision 和 generation 隔离；**只有可证明未写出的 item 才允许迁移 route**，已发或未知 item 不自动跨 route 重提。
4. 所有计划的发送入口经过相同 admission；对 legacy、Fair、RabbitMQ、同步直发逐项列清单。既有 Fair 显式拒绝的入口可以继续拒绝，但报告不得宣称该入口 Fair 兼容。生产依赖未支持入口时必须阻断 Fair 上线，绝不偷偷绕过 arbiter。
5. 公平评估必须是 **socket write 事实**：按 `(campaign, route, slot)` 统计 actual PDU、最老 eligible age、无实际进展时长、blocked reason；候选/Claim/prepare 分配不是最终份额证据。

## 4. C：PDU Window、multipart 与 timeout 安全

1. Fair 将每个活跃 Slot/connection 的 SMPP window 明确定义为：**已实际 socket 写出但尚未收到匹配 submit_sm_resp 的 PDU 个数**，每条 PDU 写入前单独获取 1 permit，记账按 `(connection_generation, sequence, attempt, part)`；不按 task/整条 multipart 获取与释放，也不让 `inflight task` 计数冒充 `inflight PDU`。与旧模式的不同单位须显式显示。
2. 在 socket write 失败、明确没写出、成功应答、重复应答、异常/late response、部分发送和连接关闭等全部路径实施**恰好一次本地 permit 结算**。对“写入是否成功不明确”的 I/O 异常按可能已发处理，保留 durable attempt/part 与 unknown/partial 证据；permit 可在已证实本代 socket 关闭/隔离后清算，但**timeout 本身不是 SMSC 已停止处理的证据**。
3. 对未返回 response 的 connection generation：先停止该代新写入→明确隔离并关闭旧连接→本地释放该代窗口账→建立新代；迟到旧代 response 只能归属旧 sequence/attempt/part，不能释放新代 permit 或污染新 attempt。任何情形不能通过单纯 reaper 计时恢复尚在写入的旧代 credit。
4. 支持 `parts > window_size`；不能一次性拿整条 multipart 的 permit 或一直等待 ACK 而冻结 Sender。保留同 Slot/同 connection generation 的 UDH、编码、源地址与 Flash DCS。若单条 multipart 的串行 parts 循环造成不可接受的非抢占时间，应做受控 continuation/yield，防止其他活动饥饿；不可为掩盖问题而将同一 multipart 随意分散至多个 socket。
5. 使用实际 `_make_task_parts` 计算 parts；preclaim 估算和真正编码不一致时，必须进行补充 credit 或安全延期，不能超出 global/route/campaign/slot/result hard budgets 后继续写。统一 `max_outstanding_parts`、window、TPS 的单位与准入诊断。
6. 保留 S0 的两模式逐 PDU pause/cancel/window-end gate，成功停止返回后 `submit_sm` 无新写入；清理**本活动**未发 ready/Slot descriptor，不 disconnect 或 flush 其他 Campaign。ACK/DLR/Journal 事实对已发 parts 继续合法进入。

## 5. D：结果、安全、回退和可观测性不变量

- 保留 S1.1 `result_pipeline_hard_bytes=VERIFIED`，socket 之前拿到正常/紧急/terminal 足额结果容量；任何结果 fsync、DB、disk、maintenance 或 quarantine 不健康时停止新 Fair socket，允许在预算内排空现有结果。不能以失败发送/强制清 queue 来释放 debt，也不能悄悄转为 retrying。
- 稳定 `sms_tasks`、Fast batches、attempt/part 状态机、result Journal event_id、OCC transaction fencing；不改变旧 DLR 的真实交付语义。partial/unknown 永不自动整条重发；晚到的真实证据可以单调修复状态，但不得跨 attempt 改写发送资格。
- Fair 只通过显式 opt-in：`MC_CAMPAIGN_DISPATCH_MODE=fair`；默认 legacy 和 serialized+reliable 不变。若有必要新增细粒度 Fair feature flag，必须冻结于进程启动且报告具体生效配置；不允许运行中无迁移协议切换。legacy→fair/回滚时必须停 Owner 并安全恢复 existing sending，不因内存 ready 丢失触发自动重发。
- 记录有限 label/cardinality 的观测：eligible groups；actual socket writes task/parts；按 route/campaign 公平等待；每 Slot PDU window used/limit、connection generation（日志诊断用，不作为高基数 metrics label）；route TPS 实际值；旧代 timeout/release/duplicate/late response；credit acquired/released/live；Journal/replay/DB/任务对账；30 秒 eligible 且可服务但零 socket 进展要保存可查诊断，不能用 worker heartbeat 冒充业务进展。
- 失败/停止/取消/断线/重启：没有永久挂死、credit 双释放、slot permit 穿代，也不删除有歧义 durable evidence。若无法证明零重复/守恒，宁可 fail-closed 并输出 `S2_NO_GO`。

## 6. E：测试与门禁（每次修改后保留当前 SHA 对应证据）

按先确定性测试，再完整 G0/G1/G3，最后 G4 的顺序。使用独立 WSL `AlmaLinux-8` ext4 文件系统、Python 3.11、DuckDB 1.2.0、loopback-only SMPP；每轮新的 DB/Journal/API 端口/Watch/artifacts，外部真实服务不连接。记录 `SHA + source manifest + env/config/scenario hash + run IDs`，且原始失败产物不能被重跑覆盖。

### E1. 必须的确定性单元/集成回归

- A 长期积压、B/C 后加入；同 route 两 Slot、单 Slot、异 route；单 Slot 慢 ACK/断线，所有健康路径在可用容量下能持续获得真实 socket write。
- A 的 heavy multipart 对 B/C 小消息公平，包含 `parts > window`、window=1/2/50、单任务不能无限独占；实际 parts 计量、route TPS 总量（与 pool_size 无关）。
- ACK 早于注册、重复、乱序、超时后迟到、错误 status、未知 sequence、write 后失联、old generation ACK、reconnect、发送失败和 partial；hard window <= configured permit，恰好一次 release，无跨代 credit 污染。
- pause/cancel/end/day window：Claim 后、intent 后、throttle 中、slot admission 前、part1 后 part2 前；停止成功返回后该活动没有新的 submit_sm；B/C 不被 A 停止误阻断。
- 同时验证 Durable claim/attempt/part/Journal/terminal、DLR negative/unknown/partial、重复/晚到回调、result hard-bytes、失败恢复、RabbitMQ/direct 安全入口、legacy 不回归。
- Fair route configuration revision/fallback：未发 item 能安全 defer/re-resolve，可能已发 item 保留原路由证据；禁用/断线 route 不影响另一健康 route。

### E2. G0/G1 基线

- 最终源码的全量 backend `pytest backend/tests -q`、OCC/S0/S1/S1.1 定向回归和前端 `npm run build` 全部真实执行；若 Windows pytest `basetemp` ACL 故障，在新建 WSL ext4 basetemp 上重跑，不把 fixture setup error 算 PASS。
- `G1/T01 ingest-only`：10×100，1000 唯一业务 task，0 socket，10/10 file/chunk completed；`sender-only` pool1/4 + multipart 保持 per-part、attempt、Journal 完整审计；Fair/legacy 均验证。

### E3. G3 是进入 G4 的硬门禁

**四种组合**：`serialized/reliable`、`occ/reliable`、`serialized/fast`、`occ/fast`，分别 **3 次新的独立进程和数据目录**，合计 12 次；不以之前 3× OCC+Fast 或 S1.1 结果顶替。每组 10×100 tasks、合适的固定单 part 模板、pool4、window50、route TPS200、SMS task workers8、ingest2、copy2、同一公平实现和可比资源预算；公平用 `MC_CAMPAIGN_DISPATCH_MODE=fair` 显式启用并在证据里验证真实生效。`stress-1k` 场景可能默认 TPS50，必须显式覆盖 200。此处窗口是新 Fair 模式 **PDU** 单位，不能直接对比 legacy task window 的性能结论。

每轮：exit0、1000/1000 唯一 keys、无非法 chunk/task 状态、file/chunk 10/10 completed、submit_sm 按实际 parts 与 attempts 对账、Journal event↔DB event 无差异、无重复自动发送、无新的拒绝超额窗口、unknown/partial/迟到结果没有违法重发；压测超时、少一个结果文件、只成功一轮均不构成 PASS。legacy 需要独立回归，但 legacy 的 PASS 不代替新 Fair G3。

**任一 G3 FAIL 必须保留 run 并先做 RCA/最小修复/重新冻结 SHA，再从受影响门禁重新验证；不得继续宣称 G4/G5/G6 通过。** 若无法在可用资源下完成，则标 `G3_NOT_VERIFIED`/`BLOCKED_ENV`，不能编造 PASS。

### E4. G4 实际 socket 公平和隔离

仅在本次最终源码 G3 满足上述所有组合 × 3 后：

- A=持续大 backlog（目标 50K），B/C 各 1K；先同 route，再异 route；配置 pool1/4、window小/大、singlepart/multipart；A ready 满、慢反馈/单 Slot 故障、A pause/cancel，跨 route 健康 Slot 继续进展。
- 使用**真实 loopback socket 上记录的 submit_sm 时序**，持续记录 B/C oldest eligible age、P50/P95 等等待和最长零进展区间；30 秒 eligible + 健康 + credit 可用但无 PDU write 触发诊断/FAIL/RCA，不能因为全 route 停止服务而假称公平失败，也不能用“受控限制”掩盖已冻结的公平预算违规。
- 每个 Campaign 最终业务 key、attempt、part、实际 socket、response、Journal、DB 的守恒独立审计；真实失败/partial/unknown 归类清楚；队列 occupancy 和 release 账一致。
- 如全量 50K 长压受环境或时间限制，保留已完成的确定性与小型压力证据，标记对应 G4 项 `NOT_VERIFIED`，不得提升 `S2_READY`，不扩展到 S3/S4。

### E5. 最终状态判定

`S2_READY` 仅在：当前最终源码 G0/G1/G3 四组合 3/3、规定的 G4 loopback socket 公平与隔离测试、PDU hard window/timeout 代际/结果硬字节/stop fence、legacy 回归和全部守恒审计均通过且证据可复核时成立。否则 `S2_NO_GO`，写明未闭的 P0/P1/环境阻断、最小复现、确切剩余工作；不得把“核心功能实现”称为生产 READY。

本阶段**不授予** S3 扩容、S4 600 秒长稳、SLES12SP5、真实 SMSC、生产切换验收；这些保持独立 `NOT_VERIFIED/NO_GO`。如果 Fair 被禁用不等于可忽略既有生产风险；持续保留已有安全修复。

## 7. 交付物

1. 必要源码和确定性测试：明确新增模块与变更范围，无无关大规模重构。
2. `Docs/Marketing_Copilot_S2_Route_Slot_Socket_Fairness_Implementation_and_Acceptance.md`：架构决策、关键所有权/锁/credit 合同、变更列表、G0/G1/G3/G4 每轮结果表、source manifest、重要 raw artifact 位置、回滚流程、`S2_READY` 或 `S2_NO_GO`。
3. 审计诊断需可区分 `dispatch_fairness`、`socket_fairness`、`pdu_window_hard_limit`、`result_pipeline_hard_bytes`，**证据充分时**才能设置为 VERIFIED。不要简单修改诊断字面值造 PASS。
4. 最终汇报只摘要：实际 SHA/文件、已证不变量、失败和未执行门禁、是否进入 S3 的结论。若发现 S2 方案无法与现有代码安全兼容，暂停相关发送路径变更，提交明确设计阻断及方案，而不是继续扩大修改范围。

**执行顺序**：冻结/复现 → arbiter/浅 handoff → PDU window/代际释放 → stop/result/兼容 → 定向/G0/G1 → G3×12 → G4 → 独立复核与报告。优先稳态正确性，公平与吞吐只能建立在无丢失/无重复/可恢复的实际 socket 事实之上。
