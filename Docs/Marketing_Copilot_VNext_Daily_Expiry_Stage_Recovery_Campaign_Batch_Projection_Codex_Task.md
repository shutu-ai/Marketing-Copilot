# Marketing Copilot VNext — Daily Expiry, Stage Isolation & Campaign-Batched Single Projection Writer

**Codex 完整开发与验证任务**  
**Task ID**：`MC-VNEXT-DAILY-EXPIRY-STAGE-RECOVERY-SINGLE-WRITER-20261017`  
**日期**：2026-10-17  
**项目**：`https://github.com/shutu-ai/Marketing-Copilot`  
**已核查的远端代码基线**：`origin/main = 439a1842c4f1ed9144fd7605695435fca76c472c`  
**任务目标**：用最少的架构复杂度，一次性完成：① 旧业务日短信永久过期、不跨日派发和低成本 Spool 回收；② 各阶段有界堵塞、快速恢复、不无谓相互拖死；③ **保留单 Projection Writer + 单 DuckDB**，改为按 Campaign 分组的 Set-based 批量写入并证明真实收益。  
**本轮禁止实施**：每营销活动一个 Writer / DuckDB、多个进程同时写一个 DuckDB、外部 MQ/Redis、全新调度平台、为了保留历史任务而构建复杂重试机制。

---

## 0. 用户的业务决策（高于历史旧设计）

1. **业务时效性第一**。一条营销短信的业务日到期后，如果尚未提交到 SMSC，就宁可废弃，也绝不能因 Worker/Spool 恢复、文件补扫、任务重新生成或隔日调度而变成「前一天残留任务的再次派发」。正常创建的**新业务日新任务**须拥有全新的业务日身份，可以按该日正常营销规则执行；不得把昨日残留复活成今日任务。
2. **发送窗口结束与业务日结束是两回事**。当天正常发送窗口结束后，可以排空已经接纳到 Spool 的当日任务；到新业务日切换点，强制停止昨日新的 PDU Socket 写入。旧日已写入 Socket 的 PDU 仍可能在次日取得晚到响应，仅归属旧业务日对账，不得重新发送或污染新日统计。
3. **统计每天独立**。日统计必须按业务日归属，而不能在晚到事件并发时简单 `TRUNCATE` 全局表或重置共享累计计数导致混账。昨日未发送数可归类 `expired`；不要求保留昨日逐条未发送短信及原始 Spool。只保留最小的关闭/对账摘要与必要的已经写出的发送证据，达到条件后可整体删除旧日运行数据。
4. **吞吐优先于过度防御与过度分层**。局部堵塞不立即拖死其他健康部分；超出有界持久化容量则要正确背压；恢复后无需人工救火。不得因为“设计更完美”引入高频锁、数据库热路径读写、频繁 fsync、轮询风暴或低效的逐短信事务。
5. **Projection 暂时只有一个 Writer、一个 DuckDB 文件**。Writer 可按 `(business_day, campaign_type, campaign_id, partition_id, route_id)` 分组批量落库。未来「一个活动一个 Writer + 独立 DB 文件」仅保留为**未实施、待 A/B 证明**的可选方向。
6. 运行环境：主验证优先使用可访问的专用测试服务器 EulerOS 2.0 SP15 / ext4 / CPython 3.11.5 / DuckDB 1.2.0，严格使用本地 synthetic SMPP simulator；不能碰真实 SMSC、生产数据、生产服务。SLES12SP5 是发布兼容性目标，不以 EulerOS 测试冒充 SLES 实测。

## 1. GitHub 源码审计所得——当前必须解决的实质问题

以下是 `439a1842` 的**代码观察**，而非声称已经实测复现的运行时故障。Codex 开始后必须检查 HEAD 是否变化、逐项确认对应行的现状；若已由新代码修好，记录验证后跳过，不做重复重构。

| # | 文件/位置 | 已确认的代码现状 | 风险和应对 |
|---|---|---|---|
| A1 | `backend/services/activity_scheduler.py` `_process_batches`/`_run` | 继续运行 `batch_campaign_service.reset_daily_batches()`；VNext active 时跳过旧的 overdue cancel / realtime finalize | Legacy 每日任务重置与 VNext Spool 失效没有统一闭环；旧日 Spool 可能跨日保留/继续消费。必须有 VNext 专属的业务日截止协议，不能只改 legacy `sms_tasks`。 |
| A2 | `backend/vnext/spool.py` `SpoolRecord`、`TaskSpool` | 记录有 `created_at`、`campaign_generation`，无显式 `business_day`/过期截止；Spool 单根 append-only、总字节 hard cap；未发现安全的已完成业务日整目录回收实现 | 业务日必须可持久判定；跨日重启不可复活；旧日目录/manifest/cursor/index 的回收应与活跃 Worker 隔离，避免长时间累积达到 512MiB 上限。 |
| A3 | `backend/vnext/worker.py` `_process()`、`backend/vnext/smpp_transport.py` `send_task()` | Worker 成功取得 Bind 后执行 `spool.checkpoint()` 再发送；TPS、Window 等待以及 `writer.write()` 之间可能隔日；在发送前未见完整业务日截止判断 | **双重截止门禁**：Worker 领取/提交边界 + 每 PDU 实际 Socket 写入紧邻前，特别是预取、限速等待、断线重连和多段短信。到期只终止旧任务，不能自动重试；保留现有 at-most-once 策略。 |
| A4 | `backend/vnext/ingress.py` `IngressRouter.enqueue_chunk()`、`backend/services/vnext_service.py` `enqueue_rows()` | 当次输入写入同一个 Spool；事件身份基于 campaign/source/ordinal/generation，未绑定明确业务日 | 新日任务去重 namespace 与昨日隔离；昨日 source/lease 重试即使晚到也不得重新生成昨日发送任务；新日合法活动仍可自行产生新任务。 |
| A5 | `backend/vnext/pressure.py` `StageCredit.reserve()` | `try_reserve` 返回空之后才进入 `asyncio.Condition.wait()`，潜在 release/notify 先于 wait 的竞态；`runtime.py` 创建 `IngressRouter(self.spool)` 未注入 `ingress_credit` | 不能只因存在类就宣布各阶段背压生效。先以可控竞争测试证明是否有丢唤醒，再修最小竞态并按需接线；不应强制每个阶段都新建 Credit/Watchdog。 |
| A6 | `backend/vnext/runtime.py` `emit` | Worker 先 `ProgressJournal.append(event)`，再对 Projection IPC `put_nowait`；满时忽略 Hint；Projection 由 Journal 主导回放 | **短期 Projection 堵塞已解耦**；但 Journal 无法无限增长，必须验证磁盘上限/长期堵塞的有界准入与恢复，不应将 IPC 满误判成数据丢失。 |
| A7 | `backend/vnext/projection_writer.py` `_apply`/`flush` | Writer 已有外层单事务批次，但批内每个事件逐条执行一次 `INSERT ... ON CONFLICT`，再一次统计 `UPSERT`；默认 batch 512 | 改用 Set-based 合并写入、按 Campaign 汇总，保留 `event_id` 去重/恢复幂等；关注重复 event、同批重复、回放 offset 原子顺序。 |
| A8 | `backend/vnext/projection_writer.py` `_process_main` journal 主循环 | 每轮按 journal 路径固定顺序消费，前面的高流量 Worker 可能耗尽整个 512-event budget | 应实现**轻量级有界轮转读取/公平分组**，避免一个大 Campaign/Worker 使小活动长期无法投影；不引入活动级进程。 |
| A9 | `backend/vnext/projection_writer.py` `ProgressJournal.append()` | 每一条事件独立 open/flush/fsync；`append_many()` 已存在 | 可作为性能候选点，但不能无依据地取消 durable fence；优先先优化 Set-based 写入；如需改 Journal 分组持久化，须证明故障语义等价与端到端收益。 |
| A10 | `backend/tools/vnext_loopback_acceptance.py` `run_gate` | 先运行/等待 Worker，**随后**才启动 Projection 处理 Worker Journals | 旧 P5500 能证明两阶段均完成，但**无法证明发送与 Projection 同时运行时的资源竞争、背压与持续吞吐**。必须增加并行实时 E2E 验收。 |
| A11 | `backend/tests/test_vnext_spool.py`、`test_vnext_projection_writer.py`、`test_vnext_ingress_pressure.py` | 已有单批次幂等、pause/replay、StageCredit 基本单协程测试 | 缺少业务日边界、晚到响应、午夜等待竞态、跨天 Crash/Restart、持续积压、真实同时发送+落库的独立覆盖。 |

重要原则：**绝不**为了旧日清理去删除仍打开、仍可能被迟到响应写入的目录；**绝不**以提高吞吐为理由移除此前的持久化与 at-most-once 防线。审计来源见第 10 节的固定 GitHub 引用。

## 2. 本轮范围 / 非范围与实施顺序

按照阶段 `D0 → D1 → D2 → D3 → D4` **一次执行完整闭环**。阶段只是内部自检点，不因完成某一阶段就宣布总任务 PASS。

| 阶段 | 实施范围 | 优先级 |
|---|---|---|
| D0 | 冻结源码、实测基线、确认业务时间定义和实际瓶颈 | P0 |
| D1 | 截止门禁、每日 Spool Epoch、过期与回收、日统计隔离 | P0 |
| D2 | 现有背压最小补线、竞态修复、阶段局部恢复 | P0 |
| D3 | 单 Writer 按活动 Set-based 批量事务、Journal 公平回放、真实 A/B | P0 |
| D4 | 正确性/故障/性能/长稳服务器验收、汇总报告 | P0 |

**本轮不做**：上线/真实 SMSC、多个 Projection 写进程、每 Campaign 一个 DB、SMPP 参数的第二轮重构、额外的安全审批/RBAC、把所有旧结果迁移成新数据模型、全表频繁扫描与逐短信日志增强。必要的数据结构/极少量显式配置可以新增，但须先复用现有字段与机制。

## 3. D0 — 冻结基线并建立可复核 A/B

1. 从 `origin/main` 开始，记录 `HEAD`、`origin/main`、tree SHA、Git dirty/untracked 列表。代码已在 `439a1842` 审查；任何偏移须先读取 diff 更新本文件的源码假设。**不得 reset、clean、stash、覆盖其他人的修改**。如存在 `.codegraph/` 按 `AGENTS.md` 先用 CodeGraph 查依赖。
2. 检查 `backend/main.py`、`activity_scheduler`、`batch_campaign_service.reset_daily_batches`、`realtime_campaign_service` 和 VNext 接入点的实际调度顺序。明确哪个时间是文件/活动窗口结束、哪个是**业务日结束**。对于跨午夜发送窗口（例如 23:00–01:00），本轮用户强约束仍以业务日切换终止旧日任务；如旧配置含跨午夜语义，报告清晰列出行为变更，禁止静默允许昨日 Spool 过午夜。
3. 时区：优先复用现有明确的业务/部署时区。若目前只依赖服务器本地时间，固定一个清楚可测试的业务时区来源、记录有效时区，避免不同进程主机时区不一致；不要凭用户本人所在地推断营销活动所在时区。使用注入的 Clock/可控 wall time 测试跨日，等待时间/超时仍用 monotonic。
4. 建立干净隔离运行目录（ext4），测试数据只用合成号码与本地 loopback SMSC；冻结基线：当前 `439a1842` 正常 P5500、Worker/Bind/TPS/Window、Projection 512-batch 和 Journal 端到端对账。历史 EulerOS `P5500=5500/5500, 40.769s (4 Worker, pool=4, route_shared 200)` 是参考，不是新测试成绩，更不是 800-TPS 端到端证明。
5. 给出可运行的 *old / new* 相同工作负载脚本/配置和三轮对照；绝不能拿 `RouteCreditBroker` acquire/s 代替实际 `submit_sm`/projection/s。

### D0 输出

`Docs/Marketing_Copilot_VNext_Daily_Stage_Projection_Baseline_and_Code_Audit.md`：源码问题分类（已确认/测试待证/不适用），基准命令、实际生效配置、时间语义、风险和不需要修改的现有防线。

## 4. D1 — 业务日强制过期、回收和统计隔离

### 4.1 业务日身份与硬截止（核心 P0）

- 明确定义 `business_day`（在业务时区的日期）及 `expires_at` / 当日新 Socket 写入截止。无论窗口内先创建多少任务，都不允许昨天的任务在今日获得新的 Socket 写入。`business_day` 必须随 durable task 存储，并贯穿 worker progress、Projection 统计及恢复路径。优先最小扩展 `SpoolRecord` 的版本/可选字段和 CRC 序列化；对已有历史 V1 Spool 的缺省字段必须**安全失败/过期**而不能把无归属的旧任务派发。兼顾向后读取需要，采用显式兼容而不是推断已发送状态。
- 业务日唯一性：`event_id` 或 namespace 必须避免昨天源重扫当作今天补发；昨日旧 ingestion lease/文件/任务恢复一律失效、不得穿过新日 admission。合法的今日新活动可以自己生成新日的发送对象，不能因旧日的 event ID 冲突被误去重。
- 在业务输入入口和 worker 已预取/选择环节设置低开销日期检查；**在 socket `writer.write(submit_sm)` 紧邻前再次执行截止判断**（不能只在领取任务或取得 TPS Credit 之前判断）；包含 Connect/Bind 等待、Window Semaphore、Route Credit、每一段 multipart PDU 的边界。到期后未发部分立刻终止，原先已写出的部分保持 `partial/unknown` 适当终态，禁止自动重发。过期状态不许升级为 `socket_written` 或发送成功。
- 发送前已有 `dispatch_taken` durable checkpoint 的 at-most-once 约束必须保留；「过期标记」不得错误地将未确定的 socket 结果重新变成可派发。发送已进入 OS socket 的数据不保证 SMSC 午夜前处理；确保**不再主动写入昨日 PDU**，并对迟到响应继续记在昨日。
- 午夜进程不在线、崩溃、重启、时钟切换、SFTP 文件迟到、重试队列、重新生成 batch 的各入口统一拒绝昨日残余。不要仅靠 00:00 的一次性 scheduler tick 清理；即使 tick 延迟，socket 最终边界也不能泄漏。防止等待协程“永远不被唤醒”而跨天仍持有旧任务。

### 4.2 每日 Epoch 和 Spool 容量回收

- 优先比较**按业务日隔离 root/epoch 并整体回收**与原地逐 Segment 清理两种方案，选复杂度最低、回收代价最低、不会重扫海量历史数据的方案。倾向按日 namespace：`run_root/days/YYYY-MM-DD/...`，但必须实测 VNext Runtime/Worker/Projection 现有进程生命周期是否允许低成本切换。不可为了每日切换引入全天发送停机或重启整个 Web/API 服务。
- 切换协议建议为：① **durable 关闭旧业务日准入/发送**并提升当前 epoch（使所有旧 Worker 的 PDU 前置门禁立即失败）；② 旧日未派发统一 `expired` 并写**紧凑关闭收据**（旧任务数、已发送数、已过期数、已知 unknown/partial 数等）；③ 活跃旧 PDU 用有界 grace time 收尾/标记 Unknown，不阻塞新日准入；④ 确认旧日路径无文件描述符/进程/offset 引用后，原子重命名隔离、删除可回收数据，必要结果摘要保留；⑤ 下一日运行使用新 epoch，不允许旧 Spool 通过 `refresh()` / `recover_index()` 再被 Worker 扫描。
- 清理不得使 `event_id` 幂等记录、Projection offset、进程租约、昨日晚到响应丢失归属；应先对账后清理必要的旧日证据。**不要求保存旧日未发短信的逐条明文**；最小摘要即可，但不能在未确定已发/未发情况下把已发状态当作可重发。
- 文件系统满：不要依赖日切换才能解除容量背压。当前日有界 Spool 达上限需拒绝新增、保持已有可发送任务消费；若有完成区段在线回收的必要，必须提供仅对已提交/已安全关闭区段的回收策略（不做每条消息磁盘搬迁）；若每日 root 方案已经足够，避免新增在线 GC。
- 尤其审查 Worker restart 与新的 daily epoch：旧 Worker 进程因 watchdog 在午夜恢复时，不能重新绑定昨天的 Spool 并执行旧任务。采用父子 epoch fencing 或等价轻量方案。

### 4.3 按日统计不混账

- 现有 `vnext_progress_events` / `vnext_projection` 若按 campaign/partition/route 聚合，应增加最小业务日维度和读 API 当前日过滤，不能只清空当天计数同时让昨日迟到事件写入今日统计。
- `queued`、`taken`、`socket_written`、`write_failed`、`unknown`、`cancelled`、`expired` 与响应计数各自定义清晰，**Socket 写出≠SMSC 成功响应≠最终投递**。新增 `expired` 无需逐旧任务 SQL 写入：可用关闭时可审计的增量/摘要实现；但聚合数须准确且可重复计算、不双算。
- 处理旧 DB/Spool 文件的升级和旧未闭环记录：安全迁移/拒绝/过期，不能直接 destructive reset 用户已有 DB。优先 `init_db()` 可重复迁移及向后兼容读；只处理必要 VNext projection 数据，不改原始业务库的既有数据。
- 用可控 Clock 跑跨午夜、跨时区、跨月末、跨年末，以及合法新一天重新从源生成任务的测试。明确“昨天任务不能再发”与“今天同一手机号的新营销行为”的区别。

### D1 验收不可妥协

`old_business_day_socket_writes_after_cutoff = 0`；`old_business_day_re_dispatch_after_restart = 0`；`new_day_campaign_progress > 0`；`yesterday_late_response_count_in_today = 0`；旧日安全回收后恢复空间可实际复用；任何未证实案例保持 FAIL，不以删除 evidence 逃避对账。

## 5. D2 — 轻量阶段隔离与快速恢复

### 5.1 先测试后改代码

- 使用现有 `StageCredit`、`IngressRouter`、Spool hard-cap、Worker/Supervisor、RouteBindLease、RouteCreditBroker、Journal/Projection，不允许重复发明第三套信号量或控制平面。
- 证明各边界有**正确的背压方向**：Ingest 慢不影响已入 Spool 的发送；Spool 满停止新增但不阻止旧任务被发送；某 SMPP Route 断线不会拖停其他健康 Route；一个 Worker 崩溃时健康 Worker 继续；Projection 30/60 秒停止后，Worker 仍能在 Journal 安全容量内继续，恢复后 lag 下降；Journal/磁盘容量逼近限制时禁止危险的新发送准入（不能发生已写出但无法持久化结果的无保护路径）。
- **核实 `StageCredit.reserve()` 可能丢唤醒**：构造 release 发生在 `try_reserve` 返回 False、`Condition.wait` 真正开始前的确定性 race，多轮复现/定向调度；修复为正确 condition predicate / 事件通知机制或删除实际不需要的 StageCredit 逻辑。修复不得引入忙轮询或每 PDU 文件操作。
- 对持续压力只保留有限的**可观测水位**，不要让暂停的一个 Campaign 无限占用整个缓冲。恢复自动发生，不能要求人工修改 DB/删除 `.lock` 文件/重新启动整个应用。跨日切换优先级高于恢复旧任务；隔日过期不再继续排空。
- 监测指标只需：每阶段 accepted/processed、pending items/bytes、oldest age、blocked reason、resume count、Journal bytes、Projection lag、Worker process restarts、PDU socket TPS。低频采样；不在短信热路径做全量 Spool index refresh 或统计 SQL。

### 5.2 故障注入清单

| 注入 | 必须证明 |
|---|---|
| 禁止新文件摄取 / 停解析线程 | 现有 Spool 的当日待发短信正常继续；恢复后摄取恢复；没有源重复消费 |
| Spool 恰好达到容量上限 | 新增有界拒绝/挂起，发送侧继续，释放/日回收后恢复；无热循环 |
| 单 Route 不回应/断线/慢响应 | 其他 Route 的 SMPP Socket 写出有进展，坏 Route 无超限/重发 |
| 单 Worker SIGKILL/异常退出 | Supervisor 有界恢复，其他 Worker 不停；未写出但已 dispatch_taken 的可能遗漏应记录、**绝不补发** |
| Projection Writer 暂停 30–60 秒、随后 restart | Worker Journal 保留，落库可追平；同一事件仅累加一次 |
| Projection IPC 满 / Writer 长时间落后 | Hint 失败不导致任务丢失；Journal 有安全容量水位；超限时安全准入收敛，不无限膨胀 |
| 恰在跨日中断/重启 + 残留预取与限速睡眠 | 旧日 Socket 新写入为 0，新日可正常处理，旧日不重新派发 |

如果已有代码在某个场景满足要求，记录证据，不以“全模块改造”为目标。**性能对照为门禁**；新增锁/等待路径若造成持续吞吐明显下降，优先简化或回滚该改动。

## 6. D3 — 单 Writer 按活动 Set-based 批量 Projection

### 6.1 严格架构边界

- 仍然**一个 `ProjectionWriterProcess`，一个被唯一 Writer 拥有的 DuckDB 文件**；不在业务 Worker、Ingress、API 热路径创建写连接。不按短信任务或活动创建 DuckDB 写入进程。
- 继续复用现有 `worker-{id}.jsonl` durable progress、offset watermark、单事务和 crash replay。活动批次是 Writer **内存内组织与 SQL 聚合维度**，不是要求物理分隔 Journal 或按 Campaign 各开事务。
- 本轮不实现独立 Campaign 数据库；可以在文档保留扩展说明 `DEFERRED: campaign_writer + independent_duckdb_per_campaign`，不得添加占位代码/抽象工厂。

### 6.2 Set-based SQL 具体要求

1. 为当前 Writer 的每一批（默认 512，可基准校准）创建有界批内 staging，并**去重同批重复 `event_id`**。按照 `business_day, campaign_type, campaign_id, partition_id, route_id` 分组形成增量；不要把每条 SMS 依次跑 2 条 SQL。
2. **以真正新插入的事件集**作为统计增量来源：已有 `event_id` 的重复回放必须既不新增明细也不重复累计聚合；同批事件重复亦只记一次。不能先汇总输入后 `ON CONFLICT DO NOTHING`，否则聚合会双计。
3. 原子边界为：`BEGIN` → staging / insert previously unseen events → 按活动 set-based UPSERT aggregate → `COMMIT`。成功后再推进 Journal offsets；若在 Commit 后、offset 写入前崩溃，重放必须以 `event_id` 幂等而不双算。若任何 SQL 失败，事务回滚、批次原样重试、不跳过 offset。
4. 选择 DuckDB 1.2.0 + CPython 3.11 **真实支持**的参数化批量方法（例如有界 VALUES/临时 staging/set based INSERT），避免为此强制引入 pandas/Arrow/大型依赖。验证批次上限、参数个数、内存与 SQL 语句大小；不得拼接用户可控 SQL。
5. 现有聚合 `vnext_projection` 要求按活动和日期隔离；保留当前独立 state counters 的语义，不把 `socket_written` 计成 `submit_sm_resp_success`。查询兼容旧 API/UI；低流量活动无需等满 batch 仍在 flush interval 内呈现。
6. **公平消费 Journal**：现有按 Worker journal 固定顺序取满 budget 可能让某个 Worker 连续垄断。先做定量复现；若成立采用小粒度 round-robin journal 读取和活动级有界分组，在每次批次/短窗口内给小 Campaign 可见进度，不引入另一层持久化队列。
7. 现有 `ProgressJournal.append` 每事件 fsync 是待测热路径，**不是默认删除项**。只有先证明 Writer/Journal 是性能瓶颈，且新的 batch durable fence 和崩溃审计语义足够可靠时，才做简单批量 flush。不要把“少 fsync”直接等同于“安全完成”。

### 6.3 数据正确性回归和 A/B

- 100% 可重复：单活动、多实时/批量活动、同事件在同批重复、跨批重复、Writer 重启重复、事务中间抛异常、offset 已落后 Commit、跨日迟到事件、同手机号跨 Campaign 正常分别计数、低流量及时 flush。
- 对照旧逐事件 `_apply()` 版本和新 Set-based：相同 `source SHA`、DuckDB 版本、硬件、输入事件序列、batch/window 设置至少三次；记录 projection events/s、事务数/s、每批 SQL 次数、P50/P95 flush latency、RSS、CPU user/sys、I/O、journal backlog/lag。**必须是功能等价**之后才比较性能。
- 如果 set-based 方案没有收益甚至严重退化，找出瓶颈并采用更简单的兼容批量方式，允许在报告中说明哪个候选被放弃；不要以完成代码行数作为验收标准。

## 7. D4 — EulerOS 验收矩阵：必须包含并行运行发送与投影

### 7.1 验收工具升级重点

当前 `backend/tools/vnext_loopback_acceptance.py` 在 Worker 完成后才启动 Projection——它验证的是**先发送、后回放**。本轮必须新建或扩展一个真实 `VNextRuntime` 的并行运行工具，让文件摄取、Worker Socket 写入、Journal、Projection Writer **在同一个时间窗口同时运行**。必须输出每秒时序指标并独立对账；不能把串行 P5500 换个名字冒充真正端到端并发验收。

保持旧 P10/P100/P1K/P5500 gate 作为兼容回归；新增以下测试，严禁连接真实 SMSC：

| 组别 | 配置与规模 | 必须提供的证据 |
|---|---|---|
| T1 功能最小集 | P10、P100、P1K，连续实时 + 批量 Campaign | 每活动 accepted/socket_written/response/projection/expired/unknown 的明确独立计数 |
| T2 历史 P5500 | 4 workers / 4 binds / route_shared=200 / window=50 / 4 Campaign (`2500+2500+250+250`) | 5500 完整对账，和 2026-10-17 EulerOS 历史 `40.769s` 口径可比较，且不得混淆串行与并行总耗时 |
| T3 实际高吞吐 | **8 Workers / 4 Binds / `tps_scope=per_bind` / `tps=200` / 实际 window**，多 Campaign ≥5,500，并保持足够长的稳态负载 | 物理活跃 Bind ≤4，逐 Bind ≤授权限制，全 route 配置上限 800 PDU/s；真实 socket/resp/projection 速率；不能拿 credit benchmark 当 800 SMS TPS 已证实 |
| T4 大容量公平 | ≥50K 合成任务（至少 2 实时 + 2 批量），并发发送与落库 | 大活动不使小活动长期饿死，Journal 不无限积压；每活动最终对账、内存/文件系统有界 |
| T5 长稳 | ≥600 秒真实持续工作负载，不仅仅空转 | PDU 速率、Projection 追赶能力、RSS、CPU、I/O、未增长失控的 backlog、无 SIGSEGV |
| T6 恢复 | 第 5 节全部故障注入，至少包含 Writer 暂停/重启 + Worker 崩溃 + Route 失败 | 其他健康环节持续进展、恢复后没有卡死、无重复写出 |
| T7 跨天 | Fake Clock 模拟 23:59:59→00:00:00，含重启/限速等待/多段/迟到响应，以及低量真实时钟验证 | **旧日新 PDU 写入=0**、旧日重新派发=0、今日进度>0、昨日统计不污染今日、旧 Spool 回收空间可复用 |
| T8 压力/恢复交叉 | 长时 Projection backlog 时跨天、同时进行新一天 Campaign | 昨日追尾处理不得拖死今日，无过期任务复活，单 Writer 对历史/今日进度公平，有界资源 |

可用固定真实服务器账号之前为 `paas@10.202.245.70`，**先确认网络授权、非生产隔离和资源配额**；如果测试服务器无法访问/当前未授权，不能宣称远端门禁 PASS，可交付代码与本地可复现测试，并标记 `BLOCKED_ENV`。不为跑完任务而修改测试服务器既有服务、防火墙、路由/配置、生产数据；不得输出凭据。

### 7.2 通过/失败判据

必须至少给出：

- `OLD_DAY_NEW_SUBMIT_AFTER_CUTOFF=0`、`OLD_DAY_REDISPATCH_AFTER_RESTART=0`、`DAY_STATS_CROSS_CONTAMINATION=0`。
- 每 Campaign 核对 `generated/accepted/expired/taken/socket_written/submit_sm_resp_success/submit_sm_resp_error/unknown/projection`，并明确不同计数可合法不相等的原因；`duplicate_wire=0`。不是所有 expired 都是发送丢失；不能为满额而自动补发昨日任务。
- `projection_applied` 与 Journal 唯一事件的对账成立；offset 回放幂等，DB commit 恢复不双计。当前日数据 query 不包括昨日晚到结果。
- `worker_crash_isolated`、`projection_pause_catchup`、`route_failure_isolation`、`ingress_backpressure_resume`、`spool_cleanup_reclaims_space` 均实测 PASS；失败或证据不足时必须分别标出。
- no `SIGSEGV(-11)`、no stuck Worker/Writer、no runaway CPU/RSS/disk、no unbounded busy loop；若出现 native crash，保留 fault evidence，不用后续 PASS 覆盖。
- **性能门禁**：相同负载/环境新旧吞吐对照，默认持续实际 PDU/s 与 Projection events/s 均不应有 >5% 的稳定中位回退；若存在需分析确证原因与收益，并明确 NO_GO 而不能降低压力或掩盖。单 Writer Set-based 预期减少逐事件 SQL 次数，但“SQL 次数下降”不能替代端到端吞吐/lag 的实际改善。
- `tps_scope` 必须按 `439a1842` 配置契约保留：`route_shared` 是配置的 route 总额度，`per_bind` 是每实际 Bind 的额度；Worker 数量不是 SMPP Bind 个数，更不能静默乘法放大 SMSC 配额。
- 真实 SMSC、生产部署、原生 SLES12SP5 尚未测试时，必须显式 `NOT_VERIFIED`；最终不得声称生产 READY。

### 7.3 运行卫生和证据

- 所有临时 DB、Spool、WAL、Journals、二进制测试包及进程诊断都存入**全新独立的 Run 目录**。不触碰 `backend/marketing_copilot.duckdb` 原件、不篡改历史 `test-artifacts`、不清理既有 dirty/untracked。
- 日切换/故障注入采用可控 Clock 和隔离合成流量，**不调整测试服务器系统时钟**。
- 按工程现状跑后端全量回归（此前 EulerOS `407 passed, 2 skipped`）、新专属单测、frontend `npm run build`、旧 P5500，以及真正同时运行的并行 E2E；如源代码修改发布行为，检查是否需要重新打包正式 release，不能复用旧 SHA `c588cd2f` 的二进制声称包含本轮变更。
- 证据最少包括 machine JSON、简明 Markdown 报告、源码和 harness SHA、有效配置快照（脱敏）、硬件和时间窗口、压力采样、进程退出码、离线 ZIP/tar SHA256、failures/缺口。没有运行的测试须标 `NOT_RUN`；不可编造时间和 TPS。

## 8. 建议的代码变更边界（实际按源码审计结果确定）

**很可能改动**：

- `backend/vnext/spool.py`：业务日身份、严格过期、安全根/segment 回收，旧格式兼容。
- `backend/vnext/ingress.py`：当日业务身份/过期准入。
- `backend/vnext/worker.py` / `backend/vnext/smpp_transport.py`：跨日/断线/预取/Window/TPS/Multi-part 的最终写前截止门禁。
- `backend/vnext/runtime.py`：daily epoch owner fencing、切日、Worker/Projection 有界收尾、状态/metrics、恢复路径。
- `backend/services/vnext_service.py`、`backend/services/activity_scheduler.py`、必要的 `batch_campaign_service.py` / `realtime_campaign_service.py`：业务日期语义、旧数据源阻断、新业务日状态与活动统计 API，不触碰 Legacy 发送路径的其他行为。
- `backend/vnext/pressure.py`：如果丢唤醒可复现，修原语并最小接线。
- `backend/vnext/projection_writer.py`：单 Writer 分批分组 Set-based SQL、Journal 公平回放、跨日归属、幂等 offset。
- `backend/tools/vnext_loopback_acceptance.py` / `backend/tools/vnext_fault_injection.py` 或新增小型工具：并行发送/投影及跨日/故障验收。
- `backend/tests/test_vnext_*.py` 新增专属日期、积压、Writer 幂等、Stage 恢复、Worker 崩溃测试。

**可明确不动**：RouteBindLease 的 pool 上限、`RouteCreditBroker` 的 8-PDU durable lease、配置 `tps_scope`、核心 msisdn stable partition、旧 Legacy 数据库和非相关前端，除非证据证明必须修改。新增配置尽量 0–2 个，优先现有日界/队列长度/批次设置。避免“写一个可配置开关就多一条不受控路径”。

## 9. 交付物与最终报告合同

在 `Docs/` 新建：

1. `Marketing_Copilot_VNext_Daily_Stage_Projection_Baseline_and_Code_Audit.md`：代码审计证据、原始基准、风险/不做决定；
2. `Marketing_Copilot_VNext_Daily_Expiry_Stage_Recovery_Single_Writer_Closure_Report.md`：设计、精确改动、测试指令、性能 3x A/B、EulerOS 证据、风险/限制、最终门禁。

测试脚本及关键机器可读结果按仓库约定保留在**隔离证据目录**。代码报告中逐条附 `commit/path/line` 或可复现命令、配置和度量。不能只写“全部 PASS”，必须让第三方能够复算发件/响应/结果聚合/过期与回收。

最终状态枚举：

- `DAILY_EXPIRY_GATE = PASS | FAIL | NOT_VERIFIED`
- `SPOOL_RECLAIM_GATE = PASS | FAIL | NOT_VERIFIED`
- `STAGE_ISOLATION_RECOVERY_GATE = PASS | FAIL | NOT_VERIFIED`
- `SINGLE_WRITER_SET_BASED_GATE = PASS | FAIL | NOT_VERIFIED`
- `CAMPAIGN_FAIRNESS_GATE = PASS | FAIL | NOT_VERIFIED`
- `EULEROS_CONCURRENT_E2E_GATE = PASS | FAIL | BLOCKED_ENV`
- `PERFORMANCE_NON_REGRESSION = PASS | FAIL | NOT_VERIFIED`
- `NATIVE_STABILITY_GATE = PASS | FAIL | NOT_VERIFIED`
- `OVERALL = VNEXT_DAILY_STAGE_PROJECTION_READY | VNEXT_DAILY_STAGE_PROJECTION_NO_GO`
- `CAMPAIGN_PER_WRITER_DUCKDB = DEFERRED_NOT_IMPLEMENTED`。

只有所有本轮必需 gate 有真实 PASS 才给 `READY`。不因完成本地静态单测、旧串行 P5500 或简单 credit benchmark 而提升端到端门禁。真实 SMSC、生产部署独立审批，始终不能由本任务授予。

### Git/工作区纪律

遵守仓库 `AGENTS.md`：保留原有本地 dirty/tracked/untracked 修改、数据库、测试材料、离线包；本任务**不执行 git commit/push**，除非用户另行明确要求。只修改与本任务有关的源码、测试及 Docs；不得把证据归档、二进制、`.env`、客户号码、凭据加入 Git。

## 10. 固定审查引用（`439a1842`）

- [每日 Scheduler：activity_scheduler.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/services/activity_scheduler.py)
- [Legacy 日重置：batch_campaign_service.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/services/batch_campaign_service.py)
- [业务时间校验：sms_task_service.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/services/sms_task_service.py)
- [VNext 控制面：vnext_service.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/services/vnext_service.py)
- [Ingress：ingress.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/ingress.py)
- [Spool：spool.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/spool.py)
- [Worker：worker.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/worker.py)
- [SMPP：smpp_transport.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/smpp_transport.py)
- [Stage Credit：pressure.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/pressure.py)
- [VNext Runtime：runtime.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/runtime.py)
- [Projection Writer：projection_writer.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/vnext/projection_writer.py)
- [P5500 串行回放现状：vnext_loopback_acceptance.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/tools/vnext_loopback_acceptance.py)
- [故障注入：vnext_fault_injection.py](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/backend/tools/vnext_fault_injection.py)
- [配置闭环与服务器 P5500 基线](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/Docs/Marketing_Copilot_VNext_Config_Performance_Closure_Report.md)
- [仓库 AGENTS.md](https://github.com/shutu-ai/Marketing-Copilot/blob/439a1842/AGENTS.md)

---

## 11. 可直接复制给 Codex 的任务入口

> 阅读并完整执行 `Docs/Marketing_Copilot_VNext_Daily_Expiry_Stage_Recovery_Campaign_Batch_Projection_Codex_Task.md`。优先确保前一业务日未发送任务不在下一天恢复/重派发；每天 Spool 低成本回收；阶段堵塞和恢复简单可靠、不降低发送吞吐；Projection 维持一个 Writer/一个 DuckDB，以 Campaign 分组的 Set-based 批量 SQL 和幂等 offset 回放提升效率。先核查 `439a1842` 当前代码，按 D0–D4 完整实施、测试及出具报告。必须新增发送与 Projection **同时运行**的 EulerOS 端到端压力测试，不得以旧的先发送后回放 P5500 代替。跨天=硬性禁止旧日新 PDU 写出；业务时效优先，可丢弃昨日未发任务。每活动一个 Writer/独立 DuckDB **本轮不实施**。不接真实 SMSC、不做生产部署、不 commit/push，保护本地 dirty/untracked 和历史证据；任何未测试项如实标记。
