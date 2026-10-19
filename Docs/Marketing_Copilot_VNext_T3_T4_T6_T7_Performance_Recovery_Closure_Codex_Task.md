# Marketing Copilot VNext — T3/T4/T6/T7 Evidence Closure & Bottleneck Attribution

**Codex 执行任务 | 2026-10-17 | P0 验收收敛 + 仅证据驱动的小范围修复**

- **Task ID**: `MC-VNEXT-EULEROS-T3-T4-T6-T7-CLOSURE-20261017`
- **仓库**: `https://github.com/shutu-ai/Marketing-Copilot`
- **当前 main**: `ee2591839a86806397284497d30eefe1f282054f`（仅添加验收报告）
- **产品源码基线**: `71ca9e25f40d9c57932ab8c02a6b69877a099eba`
- **旧性能对照基线**: `439a1842c4f1ed9144fd7605695435fca76c472c`
- **报告**: `Docs/Marketing_Copilot_VNext_EulerOS_Concurrent_Final_Acceptance_Report.md`
- **目标**: 修复测试工具可信度，完成 T3（50K+ 多 Campaign 流式）、T4（600s 持续负载）、T6（真实服务级跨日）、T7（阶段故障与恢复），测量并归因约 630–660 PDU/s 的实际瓶颈，形成可信的 GO/NO_GO 结论。
- **优先级**: 业务时效性与禁止跨日重发 > 发送正确性/可恢复性 > 实际吞吐与稳定性 > 最小复杂度。

> **总原则**：不要启动新一轮架构重构；保持 **一个 ProjectionWriterProcess / 一个 VNext DuckDB Writer Owner**；不得新增“每 Campaign 一个 Writer”或多 DuckDB。优先修复验收工具与观测缺口；只有取得可复核的产品侧瓶颈证据才允许实施局部产品优化。与本任务无关的文件、配置和历史证据不动。

## 0. 已验证事实（不要篡改或扩大解释）

基于 GitHub 的 `ee259183` 验收报告：

| 事项 | 现有事实 | 当前结论 |
|---|---|---|
| EulerOS/ext4 目标环境 | EulerOS 2.0 SP15 / CPython 3.11.5 / DuckDB 1.2.0 / ext4 / 非 root `paas` | 可复用；SLES12SP5 不等于 EulerOS |
| 后端回归 | `409 passed, 2 skipped`；focused 12 passed | PASS（报告记录） |
| P5500 并发发送 + Projection | `5500/5500`，8.699s，632.26 PDU/s，4 Binds，零重复 | PASS |
| 单 Campaign one-shot 50K | 77.935s，641.56 PDU/s | PASS，但不是 T3 |
| 真正四 Campaign streaming 50K | 约 20K watermark 附近停止进展；feeder 可能重复全量扫描；exit 143 | `TEST_HARNESS_ERROR/INCONCLUSIVE`，**不是产品故障证据** |
| 480K one-shot | 681.091s 完成 450341，剩余 29659，约 661.1 PDU/s，无 native crash | 该候选 FAIL（未排空），**不是 600s streaming 证明** |
| 旧/新 P1K A/B | 4.731333s -> 2.361667s，3/3 PASS | 小负载描述性约 2.00x；不外推至 50K/长稳 |
| T6 | 注入时钟单测通过，但服务级组合不足 | NOT_VERIFIED |
| T7 | Worker crash、Writer pause、control pause 的局部实测通过 | 完整矩阵 NOT_VERIFIED |
| 最终 | `EULEROS_VNEXT_FINAL_CANARY_READY=NO` | **NO_GO**，直到新证据支持升级 |

**重要**：800 PDU/s 是 8 Worker、4 Bind、`tps_scope=per_bind`、每 Bind 200 TPS 的**配置上限**，不是已经达成的实测吞吐或必须突破的硬性产品 SLA。现有 630–660 PDU/s 不能提前定性为 DuckDB、Journal `fsync`、SMPP、Spool、线程调度或硬件的瓶颈。

## 1. 代码级定位清单（开始时先重新核对 HEAD）

遵守 `AGENTS.md`：先确认是否存在 `.codegraph/`，存在则先用 CodeGraph 定位；之后再查源码、测试和原始报告。重点：

- `backend/tools/vnext_loopback_acceptance.py`
  - 非 5500 的 `_campaign_plan()` 默认生成单 Campaign；one-shot 50K 不能声称多活动。
  - 测试末尾从各 worker `ProgressJournal.read()` 全量读取，`TaskSpool.status()` 会遍历历史数据；这些只能用于收尾对账，不能放在高频 feeder 控制循环。
  - 目前 `old_business_day_socket_writes_after_cutoff`、`old_business_day_re_dispatch_after_restart`、`day_stats_cross_contamination` 是**常量零占位**；必须区分 `not_exercised` 与真实 `0`，不得作为 T6 PASS。
  - 使用 `expected[destination]` 与重复号码对账时要考虑跨 Campaign 同号码合法、不同 Campaign、多段 PDU。身份键应以 `campaign_type + campaign_id + business_day + event_id`（及 `part_index`）为主，不能仅由号码断言全局唯一。
- `backend/vnext/ingress.py`、`spool.py`、`worker.py`、`smpp_transport.py`：审计 admission -> durable append -> prefetch -> bind/TPS/window wait -> cutoff fence -> per-part Socket -> Progress Journal 的完整路径。
- `backend/vnext/business_day.py`、`runtime.py`、`services/vnext_service.py`、`services/activity_scheduler.py`：BusinessClock、rollover、服务级调度、重启行为、closed-day reclaim，注意不修改操作系统时间。
- `backend/vnext/pressure.py`：已有 StageCredit 丢唤醒修复；必须验证实际生产链接线与快速恢复，而不是再造压力框架。
- `backend/vnext/projection_writer.py`：单 Writer；按 Campaign/day/partition/route 的 set-based 聚合；Journal offset、幂等重放、`executemany` staging、每 event `fsync`；确认是否有持续积压而不是只看终态 lag=0。
- `backend/tools/vnext_fault_injection.py`、`backend/tests/test_vnext_daily_expiry_projection.py`：在现有故障工具上按必要扩展，不复制第二套正式运行时。

仅将以上作为源码审计假设；若 HEAD 已有变更，记录差异，切勿凭任务描述覆盖新实现。

## 2. R0 — 冻结证据和安全预检（先决门禁）

1. 记录本地 HEAD、tree SHA、`origin/main`、dirty/untracked、测试服务器实际源码 SHA256、运行环境、`python -m pip freeze`、`df -T`、剩余容量、可用 CPU/RAM；产品源码和验收工具必须有各自清晰的版本/哈希。
2. 复用**已有获得授权的 EulerOS 测试服务器**（此前测试位于 `paas@10.202.245.70`，先检查当前连通性和权限）；仅在独立 Run 根目录内执行，端口仅绑定 `127.0.0.1`，使用合成号码与 SMPP 模拟器；**绝不连接真实 SMSC/生产 API/生产库或改系统服务**。
3. CPU `0-3`、`nice +10`、`ionice class=3`、聚合 RSS 2 GiB 等以当前服务器可执行、报告曾使用的护栏为起点；不能偷偷放松后声称 A/B 同条件。测试进程设置明确 wall-clock timeout，停止时采集状态、仅清理属于本 Run 的 PID。
4. 上轮 Run 约 6.3 GiB、ext4 总占用约 82%。分别计算 **测试数据占用**、venv/runtime、日志和整个 Run 使用量；执行前确认磁盘有足够余量，设置磁盘保护阈值。不能随意删除历史 Run 或证据；空间不足则给出 BLOCKED_ENV 而不是删除用户数据。
5. 遵守“不 commit/push”、“不 reset/clean/stash”、“不改原始用户 DuckDB/日志/未跟踪文件”；代码改动只在授权工作区/隔离测试源码内。
6. 验收应优先在 EulerOS native ext4 完成；WSL 缺依赖不构成要求强行在线安装的理由，`/mnt/d` 不是 ext4 性能证据。

**R0 输出**：`source-identity.json`、`env-check.json`、`effective-config.json`（脱敏）、`pre-run-storage.json`、运行/进程所有权清单。

## 3. R1 — 修复测试 Harness / 流式 Feeder（P0：先让证据可信）

修复在上轮 20K watermark 附近停止前进的真实原因：

- **有界流式供给**：固定 chunk、增量生产游标、有限缓存；不一次创建全部任务对象，不在每个 chunk 后全量扫描 256 个 partition/所有 Segment，不重新遍历整个 Journal/Spool 来计算即时指标。
- **生产者和被测系统分开计量**：producer generated/accepted/rejected/blocked/retry、spool pending、worker taken、Socket PDU、SMSC received/response、journal offset/bytes、projection applied、平均和瞬时速率以及时间戳。Feeder 自己的限速、backpressure wait、blocked_reason、CPU/内存、扫描耗时也要记录。
- **稳定增量恢复**：遇到 spool/journal 暂时背压时，按可见条件等待后重试（不使用固定死循环）；不得在未确认是否已经 durable enqueue 的情况下盲目重复提交，也不得通过测试特权绕过日切/业务合同。
- **高负载测试数据**：四 Campaign 至少包含 batch 和 realtime；保留如 `2500+2500+250+250` 的短负载回归。50K 建议四活动 `20K+20K+5K+5K`；支持单段和混合多段短信；输出短信任务数、PDU part 数、成功响应数的**独立**计数，不把 `SMS count == PDU count` 当作所有测试的断言。
- **真实归属与去重**：按 `(business_day, campaign_type, campaign_id, event_id, part_index)` 或协议实测可靠的同等主键进行对账；同号码在不同活动并不天然是重复短信。必须能分别报告重复 event、重复 wire/PDU 和合法 multipart。
- **T6 指标诚信**：普通同日测试的 `old_day_socket_after_cutoff` 等字段改为 `null`/`not_exercised`（同时提供 evidence flag）；仅在确实注入了跨天并采集了 wire 与 dispatch 的场景才报告真实数值零。不要继续硬编码 `0` 作为 PASS。
- **负载工具监控本身不能成为负载瓶颈**：实时采样走 O(1)/受限增量计数，昂贵全量扫描仅在结束与独立对账阶段执行；测试工具 CPU、RSS、磁盘 I/O 与被测代码分开记录。
- 分别验证 40 条四 Campaign（含 multipart）、P5500 与单 Campaign 50K 的既有行为没有退化；复现并消除旧约 20K 停滞。工具修复后冻结 harness 源码 SHA，再开展正式测试。不得拿已经修改过的 harness 结果去和不等价旧 harness 直接宣称性能提升。

**R1 Gate**：`HARNESS_TRUSTWORTHY = PASS` 必须有机器 JSON、全程 heartbeat、水位进展、退出码、各阶段计数及独立最终对账；若问题是测试工具则明确 `HARNESS_FIXED`，若来自产品则进入 R5 的可重复 RCA。不得只凭“exit=0”断言。

## 4. R2 — T3 真正多 Campaign 50K+ 持续供给

- 在同一个测试期间启动 Producer + 8 Worker + 4 physical Binds + SMSC loopback + Durable Journal + 单 Projection Writer（不能等 Worker 结束才启动 Writer）。
- `tps_scope=per_bind`、每 Bind `200 submit_sm_parts/s`、window 50（另覆盖至少一组小 Window）；并记录实时 max_active_binds 和每 Bind 的实际 PDU rate。
- 至少 50K **业务短信任务**、至少四个活动，持续有界分块供给，活动类型包括 batch 与 realtime。混合 multipart 用独立实验，不把 multipart 的额外 PDU 误报成丢失/超发。
- 对所有活动分别验证 `generated -> accepted -> terminal/expired -> written -> loopback received -> response -> projected`。对于同日正常且未人为故障的测试，最终 pending=0、合法预期事件全对账、无 unexpected/duplicate wire、无挂起子进程；不得仅以总体累计掩盖单活动饿死。
- 每 1s 或适当低成本周期生成 stage snapshot；检查在活跃负载下某小活动是否始终有进展，记录最大无进展时长、最老等待任务年龄、最长 Projection lag。
- 若某步骤 30–60s 无进展：先保存 dump/snapshot、PID 和 stack/faulthandler 证据（能获取则获取），再做有界停止；不要直接 kill 丢失现场。证明是 tester 停滞还是产品停滞。
- 三次一致配置正式运行，分别保留结果。若个别失败不要平均掩盖。

**R2 Gate**：`T3_MULTI_CAMPAIGN_50K_STREAMING = PASS / FAIL / NOT_VERIFIED`，必须有真实四活动与期间持续供给证据；one-shot 50K 不得代替。

## 5. R3 — T4 600 秒连续稳态与分离排空

**重点纠正旧测试方法**：不得将“one-shot 480K 在约 681s 尚余 29659”直接称为死锁，也不得要求 480K 在 600s 内全部清空而又不给启动/收尾余量。

1. 连续**至少 600s wall-clock**保持 active producer，动态维持适度 backlog；采集不少于 10s 一个持续采样点（建议 1s 原始、10s 汇总）。生产者与发送者必须真正重叠，符合“continuous feeder”，而非一次性预加载。
2. **稳态阶段**：采样 offered PDU/s、accepted task/s、submitted PDU/s、SMSC received PDU/s、submit_sm_resp/s、Projection applied events/s、每 Campaign 进度、pending backlog、Journal lag/bytes、Spool bytes、Worker inflight、Bind/Window 使用率、route credit wait、CPU/RSS/I/O、进程重启/SIGSEGV/错误。
3. **排空阶段**：到 600s 后停止新供给，保持 VNext Worker/Writer 运行直到队列排空或明示的独立 drain timeout；记录 drain 秒数及停止时待处理数量。对比“稳态验收”和“排空验收”两种不同指标。
4. **期限边界**：T4 应在当前业务日足够早开始，预留 600s + drain，防止自然午夜切换把旧日任务过期误判为发送瓶颈。另由 T6 负责跨日业务逻辑。
5. **合法积压解释**：健康 steady state 未必要求 `pending=0`；如果 producer 供给速率高于发送能力，backlog 增长属于负载条件，应被量化。要检验积压**有界**、响应正常、无无进展死锁、并且停供后能追赶；不能用“全程零 backlog”作为高吞吐验收的虚假硬门槛。
6. 容量“800”属于上限非保证值；将吞吐限制精确报告为 **实际持续 PDU/s 与链路性能 headroom**，并给出分阶段证据。绝不为追求 800 破坏原定 TPS/Bind 上限或短信正确性。
7. 至少一次完整 600s，在环境允许的情况下重复一次；如果发生资源上限或测试工具故障，保留完整失败样本并标记 FAIL/BLOCKED/NOT_VERIFIED，而非移除失败运行。

**R3 Gate**：`T4_600S_STEADY = PASS / FAIL / NOT_VERIFIED`、`T4_DRAIN = PASS / FAIL / NOT_VERIFIED` 分别判定；附时间序列 JSON/CSV 和曲线或统计，不能只给测试结束后 lag=0。

## 6. R4 — T6 服务级真实跨业务日 / 硬截止 / Spool 清理

**业务合同不变**：业务发送窗口结束后允许当天已有任务继续排空；**新业务日**开始，前一天**未写出的**任务立即永久过期且绝不再派发。时效性优先，允许过期漏发，不允许重发；已经入 Socket 的 PDU 或其迟到响应属于前日，不应重新发送、污染当日统计。生产环境不改 OS clock。

- 通过**可注入 BusinessClock**与正式 `VNextRuntime`/服务控制入口运行进程级组合，不单靠孤立类单测；涵盖 `23:59:59 -> 00:00:00`、服务级 scheduled rollover、跨两日/多日跳跃、时区配置。用配置的 `MC_VNEXT_BUSINESS_TIMEZONE` 并显式验证当前用的是 UTC 还是业务指定时区；不得默默 fallback 到错误时区。必要时为测试时钟提供跨进程同步信号，但避免生产中引入复杂时钟服务。
- 记录真实 `event_id`、`business_day`、`expires_at`、各阶段时间戳、socket write 时间（或单调+墙钟关联）、SMSC recv、cutoff 和 restart，**直接计算**违规旧日写入/旧日重派数量；不可来自常量零。
- 强制验证：pending Spool、Worker prefetch、Bind slot 等待、TPS credit 等待、window 等待、已 dispatch-taken 但尚未 Socket 的竞态、multipart 前段已写后截止、Worker crash/restart-at-cutoff、Projection 迟到 resp、closed segment 回收前后恢复、新一天与旧一天同时存在统计。
- 只允许旧日记录结算或过期；旧日重新派发 = **硬 FAIL**。在合法部分发送/unknown 事件出现时不能将它们自动重投。确认 Worker fencing 不会因 await 挂起跨日而绕过检查。
- 检验 Journal 的历史留存/回收：是否会在多天后触发硬上限；只可以在 Projection offset/durable reconciliation 完成并且不会破坏重放时回收/轮转，不能直接 truncate 有未确认内容的活跃 Journal。这里先测试归因，只有真实必要再小修。
- 验证跨日清理在 Worker 仍有活跃文件引用时不会删除其所需 segment，清理后下一日正常使用容量，记录 `before/after` bytes 与 active references。

**R4 Gate**：`T6_BUSINESS_DAY_E2E = PASS` 需 **真实、非占位** 的 `old_day_new_pdu_after_cutoff=0`、`old_day_redispatch_after_restart=0`、`new_day_stats_contamination=0`；所有测试场景均有 executed flag、计数和可复核证据；缺某场景则 NOT_VERIFIED。

## 7. R5 — T7 故障注入、阶段隔离与自动恢复

复用现有 `vnext_fault_injection.py`、worker supervisor、Projection watchdog，优先在测试工具外部施加故障，不改产品流程。至少覆盖：

| 场景 | 必须核对的效果 |
|---|---|
| Feeder 暂停 30–60s，再恢复 | 已入 Spool 的任务继续处理；恢复后增量生产而不是全量重发 |
| 单 Route 不可达（双 Route 同时负载） | 受影响 Route 有界积压，健康 Route 持续前进；恢复后正常发送 |
| 单 Worker crash/restart | 其他 Worker 不被拖死；相同 Partition 不存在两个 owner；按 at-most-once 口径审计潜在漏发窗口 |
| Projection Writer pause、强制 kill/restart | Journal 保存且 Writer offset 回放；已提交事件不重复累计；活动可追赶 |
| IPC hint queue full | Worker 不因提示队列满崩溃；Journal 才是 durable source |
| Journal high-water / hard-limit | dispatch 前 admission 停止，不能出现 Socket 已发却无 durable 记录的普通路径；解除阻塞后恢复，无无限忙等待 |
| Spool 容量耗尽 / 恢复 | 停止新 ingress，有容量后快速继续；已接收任务及其他健康活动尽可能前进 |
| 跨天时 Worker/Writer 故障 | 当日恢复但**绝不派发前日旧任务**，昨日状态不污染今日 |

**故障正确性规则**：at-most-once 的 crash 窗口可能漏发，必须按 `not_sent/unknown/partial/expired/written/response` 明确归属，不将允许的潜在漏发伪装成成功或强制自动重试；不允许重复 wire 或跨日补发。阻塞必须有 bounded timeout、状态与 owner 归属。必要时区分“超时重试同一连接/配额等待”与“重新发送同一短信”。

**R5 Gate**：`T7_STAGE_RECOVERY = PASS / FAIL / NOT_VERIFIED`，输出每场景的子进程 exit、恢复耗时、阻塞期间其他健康 Campaign/Route 进度、队列最大深度、最后收敛统计；核心缺失不得标记整体 PASS。

## 8. R6 — 吞吐瓶颈归因、必要的轻量产品修复与 A/B

在测试工具已可信、T3/T4 至少取得可用真实采样之后，才对性能作判断。产品侧只允许有证据、有局部收益、可回退的优化，不为架构优雅改造数据链路。

**至少量化以下各阶段的 per-message/PDU 平均、P50/P95、峰值与累计等待**：

1. feeder/ingress enqueue 与 Spool append/`fsync`、容量水位及锁占用；
2. Worker 取队列、调度与 prefetch、inflight、RouteBindLease acquisition；
3. RouteCredit wait（route_shared/per_bind 分开）、window wait、socket write、SMSC response latency；
4. Journal `.append()` 字节数、`fsync` 次数与耗时、`can_append()` 的边界；
5. Projection Journal tail、set-based batch size / `executemany()` staging、SQL elapsed/commits/s、offset 落盘、实时 lag；
6. 系统级 CPU user/sys、RSS、ext4 IOPS/latency、磁盘剩余量、context switches（能采样则采样；不可得标 NOT_AVAILABLE）。

- 仅使用**低扰动**累计 counters + 周期性采样，不新增逐 PDU 磁盘日志/JSON/fsync；仪表仪器本身的成本须与关闭采样状态做 A/B，避免观测拖慢测量对象。
- 优先排查：单段与 multipart PDU 比例、发送通道是否足够活跃、Journal 每事件 fsync、Projection staging 插入效率，以及 harness 自身供给不足。上述是待证假设，禁止先写结论。
- 若瓶颈在测试工具，只改 harness；若瓶颈在产品且有重复证据，可做最小必要修复并回归：例如优化预取/批量/无效全量扫描、减轻锁争用。但**不得未经耐久性证明删除 Journal fsync，不改变 SMSC 配额/at-most-once 语义，不增加 Writer 或数据库数量**。
- **A/B 可比性**：固定测试服务器、资源护栏、代码 SHA、harness SHA、任务分布、source chunks、SMSC simulator、业务时区、读写方式；三轮以上 50K 对照有条件时可跑，同样 600s 负载优先；旧 `439a1842` 缺少新业务日合同，不可将某些“功能缺失造成的更快”直接归因新旧优化。记录负载协议差异与统计边界。
- 报告中同时保留 `P1K old/new 2.003x` 的历史结果及其小负载限制；不要假称已达到 800 实测。

**R6 Gate**：`BOTTLENECK_ATTRIBUTED = YES / NO`（无足够证据则 NO）；`PERFORMANCE_NON_REGRESSION = PASS / FAIL / NOT_VERIFIED`。即使无法达到 800，只要持续工作正确稳定、成本和限制清晰，提供量化的剩余差距，不以更高 TPS 为由自动否决产品正确性。

## 9. 最终核对与门禁定义

**必须分开报告以下事实**：

- `generated_messages` vs `accepted_messages` vs `expired_messages` vs `socket_written_messages` vs `submit_sm_pdu_count` vs `submit_sm_resp_success/error` vs `projection_events_applied`；multipart 的计数不能错误要求全部相等。
- 每个 Campaign/day/route 单独对账、全局聚合对账、event 幂等、SMSC 接收身份、重复 wire、unknown/partial、pending、Worker/Projection restart。
- `end_of_run_projection_lag=0` 不代表运行中无积压；报告应同时输出 `max_projection_lag_seconds`、`p95_projection_lag_seconds`、`max_journal_backlog_bytes`、`drain_seconds`。
- `old_day_* = 0` 仅在切日真实执行时有意义；其他测试为 `NOT_EXERCISED`，不能作为跨天 PASS 证据。
- 如果活动在本日时间窗结束后继续清空当日任务是合法行为；午夜开始后的新增旧日 Socket/PDU 是硬违规；正在进行的旧 PDU 响应可继续旧日归档。
- 发布判断必须基于本轮所有强制门禁真实证据；缺数据写 NOT_VERIFIED；发现异常写 FAIL；测试环境确实不可用写 BLOCKED_ENV。不可把旧 Commit 的测试结论移植给新构建。

### 正式 Gate 矩阵

| Gate | PASS 要求 |
|---|---|
| `SOURCE_AND_ENV_IDENTITY` | 产品与 harness SHA、EulerOS/ext4、依赖、护栏、运行目录记录完整 |
| `HARNESS_TRUSTWORTHY` | 增量 feeder、稳定出数、真实身份对账、无固定假零、独立监控开销 |
| `T3_50K_MULTI_STREAMING` | 真正四 Campaign、持续给数、闭环与公平性对账 |
| `T4_600S_STEADY` | 连续供给至少 600s、活跃链路、没有无法解释的停滞/异常、时序数据完整 |
| `T4_DRAIN` | 停供后在独立窗口正确收敛，未完成量解释清楚 |
| `T6_DAILY_EXPIRY_AND_RECLAIM` | 服务级跨天全部关键竞态场景真实执行，旧日违规写入/重新派发为 0 |
| `T7_STAGE_RECOVERY` | 全部核心故障场景实际执行并恢复，有健康活动不受牵连证据 |
| `SINGLE_WRITER_PROJECTION` | 仅一个 Owner，持续 lag 可收敛，重复回放不重复统计 |
| `NATIVE_STABILITY` | 600s 无未解释 native crash、进程泄漏或无法恢复的退出；保留 crash 证据 |
| `PERFORMANCE_NON_REGRESSION` | 可比的长负载对照与资源变化有充分证据，不降低正确性 |
| `CANARY_READY` | 必须全部强制 Gate PASS 后才可 `YES`；仍与真实 SMSC/生产部署许可分开 |

**不把 `REAL_SMSC`、`SLES12SP5`、`PRODUCTION_DEPLOYMENT` 这些本轮不在范围内的项目伪装成 PASS。** 即便 EulerOS Canary Ready，也不是直接允许生产部署。

## 10. 最终交付物

在 `Docs/` 新增：

1. `Marketing_Copilot_VNext_T3_T4_T6_T7_Evidence_Closure_Report.md` — 中文结论、测试环境、发现、失败 RCA、Gate 决策、残留风险与下一步。
2. `Marketing_Copilot_VNext_Harness_and_Bottleneck_Audit.md` — feeder 问题 RCA、完整源码链路映射、各阶段 timing & resource 证据以及采用/拒绝的优化理由。

在 `backend/tools/` 优先扩展现有工具，确有必要可增加**少量单用途** `vnext_streaming_acceptance.py`、`vnext_daily_fault_acceptance.py`、`vnext_stage_metrics.py`，不可搭建全新的第二运行时；在 `backend/tests/` 补充日期边界、消息/PDU、多 Campaign 分配、stop/resume、replay 的针对性自动化测试。

机器证据存于**独立、隔离的测试 Run**：

- `evidence/source-identity.json`, `env-check.json`, `run-config.json`
- `evidence/harness-smoke.json`, `t3-multi-50k-{01,02,03}.json`
- `evidence/t4-steady.json`, `t4-timeseries.csv`, `t4-drain.json`
- `evidence/t6-daily-boundary.json`, `t6-spool-reclaim.json`
- `evidence/t7-fault-matrix.json`, `stage-bottleneck.json`, `ab-comparison.json`
- `evidence/post-run-audit.json`, `evidence-manifest.json`, `final-gates.json`

电子证据可打包脱敏 CSV/JSON、日志、配置、source SHAs 与工具版本；**不**把真实生产数据、秘密、测试 DB/WAL/Spool/Journal/venv 放入 Git 或报告。运行现场只有在确认安全、权限允许并明确属于本次 Run 时才能做有界清理；旧 Run 不处理。

## 11. Codex 执行顺序与交付纪律

1. `R0` 冻结身份，审计 `ee259183` HEAD / `71ca9e25` 产品树和原始证据；不开始大负载直到 disk/env 预检通过。
2. `R1` 修复测试 feeder、身份计数和固定零占位；完成小规模复现与 A/B；**必要时先单独验收 harness**。
3. `R2` EulerOS 正式 50K+ 多 Campaign streaming；保留全部失败样本。
4. `R3` 真正 600s continuous feeder + 独立 drain；测真实持续速率与瓶颈。
5. `R4` 服务级注入时钟跨日、重启、partial、多日/时区及 Spool 回收。
6. `R5` 实际故障注入与阶段恢复；不将测试工具故障冒充产品故障。
7. `R6` 根因定位，只在有数据时小范围修复并重新跑受影响 Gate；冻结最终证据和结论。
8. 后端全量 pytest、关键 VNext 子集、前端构建（如涉及）、测试工具 smoke、EulerOS 目标原生验收；版本/日期/Run SHA 与产出文档一一对应。
9. 最后输出：实际修改的文件、关键负载数据、通过/失败/未验证矩阵、目前 `CANARY_READY`、剩余阻断事项、是否需要进一步产品改动。**未经用户明确指令，不执行 commit/push**。

### 不在本轮实施

- 不变更 VNext 整体架构、不重构 Worker/Bind/TPS 配置合同。
- 不增设每 Campaign 一个 Projection Writer、多个 DuckDB 文件或通用分布式调度层。
- 不为追求“exactly once”增加需要复杂协调的发送重试；允许明确的 at-most-once 漏发窗口，但不允许无意重复发送。
- 不进行真实 SMSC/生产环境部署，不修改用户数据库、操作系统时间、系统服务及既有日志/历史运行数据。
- 不添加高频逐 PDU 额外同步 I/O 来“观察”性能。

---

## 直接交给 Codex 的短提示词

```text
执行 Docs/Marketing_Copilot_VNext_T3_T4_T6_T7_Performance_Recovery_Closure_Codex_Task.md（如文档放在别处，请按实际路径读取）。
以 origin/main=ee259183 为报告基线，产品源码=71ca9e25；先确认现有 HEAD、用户工作区与测试服务器权限/磁盘。
只做 T3/T4/T6/T7 验收收敛和必要的证据驱动修复：先修验收 feeder 的 20K 卡住问题、禁止硬编码旧日违规计数为零；之后跑 EulerOS 真实多 Campaign 50K streaming、600s 连续负载+独立 drain、正式服务级跨天不复发测试、阶段故障恢复；采集发送/PDU/Journal/Projection 真实持续时序、耗时及资源，归因 630–660 PDU/s。
必须保留单 Projection Writer、单 DuckDB；只有测出产品瓶颈才实施最小局部修复，不做架构重构、不增加多 Writer、不降低消息正确性和截止保证。
报告给出真实 PASS/FAIL/NOT_VERIFIED/BLOCKED_ENV，不复用旧版本 PASS；保存原始证据与 dirty/untracked，未经授权不要 commit/push、不要连接真实 SMSC 或执行生产部署。
```
