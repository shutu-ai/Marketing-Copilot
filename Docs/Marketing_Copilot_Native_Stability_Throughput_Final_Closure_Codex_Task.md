# Marketing Copilot — Native Stability & Throughput Final Closure

> **统一 Codex 执行任务 / 一体化收口后续工作（不是新的 S2、S3、S4 重构）**  
> Task ID: `MC-INTEGRATED-NATIVE-THROUGHPUT-FINAL-CLOSURE-20261016`  
> 基线：`shutu-ai/Marketing-Copilot`，`origin/main` 提交 `95efa42491af5bb0aa0b08560bf293fd26a92553`（执行前核实）  
> 主报告：`Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md`，尤其第 8 节  
> **初始结论：`INTEGRATED_NO_GO`。不得默认转为 READY，不得进入 S5 / 真实 SMSC / 生产切换。**

## 0. 最重要的任务目标

不要再启动一轮大规模架构重构，也不要将每个新发现单独切成 S2.x 子项目。

在**一个连续执行、单一进度报告、统一最终验收**的任务中，解决：

1. **验收可信度**：G4 CLI 参数是否真正落到 SMPP 实际配置，压测采样/审计是否反向拖慢或压垮应用，52K 规模和 deadline/Route TPS 之间是否存在数学上不可满足的验收条件。
2. **Native 稳定性**：高并发 G4 在 WSL AlmaLinux-8 / ext4 / Python 3.11 / DuckDB 1.2.0 出现过真实 API `SIGSEGV (-11)`；需要证据驱动收敛、最小修复，不允许用低 worker PASS 掩盖。
3. **端到端实际吞吐**：在 simulator-only 已达 ~3,283 PDU/s、route cap=200 parts/s 时，应用 sender-only 仅 ~4.1–4.2 tasks/s，pool1≈pool4。调查 DB/result/checkpoint/claim 写入放大、等待链和有效 batch size；仅实施有可量化证据的最小必要改动。
4. **真实公平性、容量与长稳**：同一最终源码/同一可验证参数下，补充 G4/G6 高并发、长短信、pause/cancel、慢 ACK、断连、长稳、吞吐和完整账本证据。
5. **保留 S0–S2/S1.1 关键正确性**：OCC/serialized、Fast/Reliable、Journal fsync、result hard bytes、PDU generation、attempt/part、unknown/partial、停止栅栏、recovery、legacy 回退不得退化。

执行策略：先证明测试可信、再查明 crash、再优化热路径、最后同版验收。可以分内部里程碑，但**不得每完成一个里程碑就宣布工程 READY 或另起总计划**。

## 1. 必须核查的现场与事实（不得直接推测原因）

- 当前报告：ext4 G0 `344 passed, 2 skipped`；G3 4 组合 ×3 为 `12/12 PASS`；G5 既有 fault/recovery PASS。**这不能外推到 G4 高并发稳定性。**
- G4 八项严格矩阵：小 Campaign B/C 每项各 1,000/1,000，但大 Campaign/总 52K 未完成；存在另外一次高并发 API `SIGSEGV (-11)`，以及 `stall-60s` 诊断。全部保留原始 FAIL。
- G6：约 631 秒有效运行，生成 11,270、accepted 5,283，严格容量目标 FAIL；不能将「有业务进展」误判为完成 52K，也不能用 FAIL 代替吞吐根因。
- G1 Sender-only：pool1 ~4.173/s、pool4 ~4.110/s；模拟器 ext4 ~3,282.644/s、server response P99 ~0.988ms；socket 写入和 Journal fsync 均不是已知的最大耗时项。报告记录 `sms_result_batch` lock/tx ~0.10–0.12s、Fast checkpoint ~0.085–0.105s、Claim tx ~0.046–0.048s（**需明确这些是均值、分位还是采样；不能将单项耗时简单相加当作每条成本**）。
- `backend/tools/multi_campaign_fair_acceptance.py` 暴露 `--pool-size`、`--smpp-window`、`--route-tps`，但在 `_provision()` 中目前写死 `pool_size=1`、`window_size=10`、`tps=1000`。**先核实运行中是否有后续覆盖，再判定 G4 原矩阵真实参数是否一致**；不要无证据声称所有旧数据无效。
- 同一个 runner 的 `_sample_runtime()` 周期性调用 `_environment_task_rows()` 分页读取全部任务；`_wait_for_business()` 还反复从头读取 SMSC event JSONL、分页取任务。52K 高压下测试观察者可能本身造成额外 DB/API/磁盘负载。需测量，不能只凭代码推断实际影响比例。
- 同 runner 有四活动：realtime-A、batch-A、realtime-B、batch-B；实际预期总量由 `2 * large_rows + 2 * small_rows` 决定。核对报告里的“A=50K，B/C=1K，52K”到底指哪两个大活动/哪两个小活动；严禁错误映射验收规模。
- Runner 默认 `--timeout-seconds=180`，route TPS 默认 200；若该 Route 的 52,000 条均为单 PDU 且必须全部发送，单 route 理论下限为 260 秒，不含任何 DB/文件/控制开销。两 route 按各自实际配额计算；**先以冻结的实际配置、part 总数、deadline、可用 route、SMSC/DB 处理上限做可满足性审查**。

源码开始前先阅读仓库 `AGENTS.md`、一体化主报告第 8 节、`Docs/Marketing_Copilot_Integrated_Architecture_Convergence_Plan.md`，以及 S0.1、S1.1、S2.1 的现有 RCA。若有 `.codegraph/`，按 `AGENTS.md` 先用 CodeGraph 了解调用关系。

## 2. 工作区和环境硬约束

- 先 `git status --short`、`git rev-parse HEAD`、`git rev-parse origin/main`，记录 tracked/untracked、diff、依赖及版本。如果远端在 `95efa424` 后发生新提交，先检查差异、调整基线，绝不覆盖更新。
- 用户既有 `frontend/package-lock.json` 修改、未跟踪文档、证据、数据库、缓存、临时文件、WAL/Journal、日志/备份**不得 reset/clean/删除/自动提交**。
- 仓库 `AGENTS.md`：**未经用户明确授权，不执行 commit 或 push**。最终只报告建议提交清单和状态。
- 所有测试运行在全新隔离 WSL `AlmaLinux-8`、Linux ext4（不是 `/mnt/d` 主数据路径），CPython 3.11.13 / DuckDB 1.2.0 与冻结依赖。原始 WSL/Windows 证据分别保存、不可拼接不同源码或配置成一个 PASS。
- SMSC 只用 `127.0.0.1` Fast/fault 模拟器、合成号码及隔离数据库，不连接真实 SMSC、生产客户数据、SFTP 生产目录。
- 默认 `MC_CAMPAIGN_DISPATCH_MODE=legacy`、生产推荐 reliable、serialized；OCC 和 Fair 仅在明确隔离验收下显式打开，未通过前不更改生产默认。
- 为原始 native 崩溃保留所有可用证据。Core、完整 thread dump、号码/短信正文等敏感/大体积运行证据只在隔离目录，**不进 Git**，报告只引用哈希、摘要、路径。

## 3. 阶段 A — 验收工具和业务目标审计（先做，否则后面结果不可信）

### A1. 配置落地必须有确定性证明

核对 `multi_campaign_fair_acceptance.py`、`local_e2e_acceptance.py`、`smpp_gateway.py`、配置 API 和 DB：

- CLI `pool_size/window_size/route_tps/simulator_tps/response_delay/audit/dispatch/send_mode/db_concurrency/task_workers/ingest_workers` → process env/API create/update → DB 配置 → gateway effective status → 实际 bind 数 → simulator events，每层记录 declared/effective。
- 修复仅在有证据的参数丢失/硬编码处；**不要静默修正后沿用旧轮次 PASS**。测试启动时在发送前 assert 预期配置、所需活动/route/bind 数及 revision，错配直接 `HARNESS_CONFIG_FAIL`，保留旧结果。
- same-route 和 two-route 实际配置不能只靠 label 判断：确保两个有效物理 config_id/route 的确存在；route shared TPS 不随 pool_size 倍增；可能共享一个 SMSC 账号时单独标记配额假设。
- 按活动精确核对输入、合法行、generated、claimed、accepted、socket PDU、attempt/part、realtime/batch、route。给出 `2*large_rows+2*small_rows` 与目标 52K 的明确对应。
- 增加快速单元/集成断言：至少 `pool=1/4`、`window=1/2/50`、route=200、simulator=0、same/two-route 各一组；验证**实际生效值**而不是只检查传入 args。

### A2. 降低观测者效应但不降低审计强度

- 对运行期间周期性 `/api/sms-tasks?page_size=200` 全量扫描、每轮 `_read_jsonl(...)` 全量扫描做 profiling（调用次数、平均/总耗时、DB 读事务、扫描文件 bytes、CPU/RSS）。对 10K/52K 工作集至少一个有/无高频采样的公平对照。
- 改为有界统计 API（优先已有聚合/状态快照）、增量日志游标或受控稀疏采样；采样必须是 read-only，且不会让共享 DuckDB Owner 长时间持锁。**最终仍需完整逐条离线审计**：task keys、submit sequences、attempt/part、Journal terminal、simulator audit digest 全保留；不允许以关 audit、丢事件换 TPS。
- 采样和验收结算要分开：运行中轻量 counters；停机后的完整、只读、流式对账；按运行 epoch、文件 hash 和 source/target manifest 校验。
- 若硬门禁要求 52K 全部完成，必须核对该门禁在冻结 `route_tps × duration × number_of_independent_routes` 下的**理论可达性**，区分以下四项：`fairness_while_backlogged`、`sustained_throughput`、`strict_total_completion`、`native_stability`。
- **不可擅自放宽原有 52K / 600 秒硬性标准**：如果存在物理/数学矛盾，保留原门禁 FAIL，另输出 `SPEC_INFEASIBLE_NEEDS_DECISION` 及最小配置/指标修订建议，待用户批准后才改变正式合同。允许延长**诊断性运行**以定位瓶颈，但必须标为非门禁实验，不能用它伪称 180 秒门禁通过。

阶段 A 交付：`validation_contract.json`（冻结每个 gate 的任务数、parts 数、时间窗、route/pool/window、budget、真实参数）及 harness A/B 证据。若仍不可信，不进入大规模业务重构，继续修测试工具。

## 4. 阶段 B — 高并发 SIGSEGV 根因收敛（最高优先级）

不将 G3 12/12 PASS 或低 worker 的 G4 子测试当作高并发稳定性证明。先保护复现现场，形成事实/假设分离表。

### B1. 最小可复现与负载解耦

在同一隔离环境、同一版本、同一数据量逐步做：

1. 原始 G4 高并发配置（worker、pool/window、DB 模式、route 数、数据量与现有失败场景一致）重新运行，并在每次运行前冻结源码与 harness SHA。
2. `simulator-only`、`sender-only`、`ingest-only`、`mixed Full E2E` 分别测；区分本地 TCP/SMPP/DB native 内存生命周期与文件解析路径。
3. `serialized vs OCC`、`reliable vs fast`、`worker=1/2/4/8/16`（可行范围内）、少量到 52K 渐进负载；每个关键对照至少 2–3 个独立进程，首个 crash 保存现场而不是立即做重试掩盖。
4. 禁用**非业务相关**高频观察者扫描的诊断对照，观察 crash 是否消失；仍保持正常安全/Journal/audit 链路完整。
5. 若内存持续涨、文件句柄泄漏、DuckDB cursor/native owner 生命周期与线程关闭存在关联，记录每线程 connection/cursor 身份、事务、线程活动和锁等待，但不记录 PII。

### B2. 崩溃证据

- 启用 `PYTHONFAULTHANDLER=1`；Linux 环境允许时启用 core dump / `coredumpctl` / `gdb`（不可用则记录限制，不假造 faulting frame），收集进程 exit -11、Python thread stacks、native stack、模块版本、OOM/dmesg、RSS、FD 数、WAL/DB/JOURNAL 状态、最后安全 watermark。
- 对 `SIGSEGV`、`SIGABRT`、OOM、死锁、watchdog timeout 分开归类。`SIGSEGV` 不可仅凭同时存在 OCC 冲突就宣称 DuckDB C++ 确定根因。
- 特别审计 `backend/database.py` process-level native owner + `.cursor()` 的竞争使用、备份时 `close_root_connection()` 与在途事务、连接路径切换、worker stop/start epoch、长寿命 callbacks。修复不能让 backup 改动重新引入 S0.1 的 connect/close 高频 native 生命周期缺陷。
- 有必要时隔离比较受支持的 DuckDB 版本组合，但每种依赖版本作为**另一份独立实验**；不得把升级依赖的通过直接归入冻结的 DuckDB 1.2.0 门禁。不得未经批准升级生产依赖或新增多 DB writer。

### B3. 修复原则

- 尽量修正确定的连接/资源所有权、锁序、生命周期与 bounded buffer bug；不大改已有 Claim/Journaling/Reliable/Fast 状态机。
- 禁止 `try/except: pass` 隐藏 native 风险、改为无界重试、扩大队列/timeout、强行降低默认 Worker 作为唯一方案、跳过某类 task、清空 unknown/partial、自动重发可能写出的 PDU。
- 修复后必须重跑原始能复现 crash 的同参数场景及新故障注入测试；若无法复现原 crash 且无 native 原因证据，只能标 `RISK_NOT_CLOSED`，不能凭几次运行成功断言 RCA 已证实。

## 5. 阶段 C — 吞吐热点最小优化（不牺牲正确性）

### C1. 建立完整的独立三臂测量

- `simulator-only`：无限速 Fast，DLR 关闭，记录 protocol audit、RTT、CPU、event loop backlog、max inflight。
- `sender-only`：预生成 1K 或更大（不包含文件摄取），pool1/4、window1/2/50；DB 模式和 Fast/Reliable分开；记录任务/s、parts/s 与每阶段等待和操作数。
- `full E2E`：将 discovery/ingestion/claim/socket/checkpoint/terminal 分别累计每秒处理数，记录 queue/backlog/eligible age；不能将 1,000 accepted 的最终成功当作达到 Route TPS。
- 所有测量使用同一冻结源码、同一 CPU/RSS/存储约束、同一模拟器配置；每点至少 3 个独立样本并保留分布，避免只取最好的值。

### C2. 需要测的具体热点

记录事件/s、真实 batch items、DB transaction/s、SQL statement/s、锁等待 P50/P95/P99、DuckDB execute/commit、Journal fsync、Fast checkpoint size/flush lag、Result bytes/debt、ReadyPool live/blocked、Claim batch、cursor/connection 数。

对现有可能的写入放大做第一性审计：

- Fast checkpoint 聚合成功后是否仍只有 1–2 task/event？是否由于 300 ms debounce、credit 释放、ReadyPool capacity、DB transaction 互相形成反馈？
- `sms_write_buffer` 是否把一个批次拆成大量短事务？Set-based 写是否真实合并（含无 DLR 的 accepted 与 secondary 状态更新）？是否有不必要重复 ORM/SQL/投影/统计写入？
- Claim 是否与结果写入在单 DB coordinator 中形成交替阻塞，造成 `pool4≈pool1`？是否仍反复全文候选扫描、频繁小事务？
- 仅在证据证明其为热点后进行**有界**批量化、相同 event-id 的 batch commit、无多余小事务、控制面采样减负等最小改动；**不得修改 durable identity、fencing 或业务语义以换性能**。
- 验收至少包括 `before/after` 同条件性能、实际事务合并率、不会增加 worst-case result bytes、stop/cancel 最坏响应边界；没有明确收益则撤销优化，不叠加新 Ledger/Queue/Worker 机制。

### C3. 吞吐目标的解释

`route_tps=200` 是可用协议路由的配置上限，不自动表示应用必须保证 200 TPS，更不是模拟器产出承诺。最终报告需展示端到端**可持续速率与瓶颈归因**，不能用「配置 200」冒充测试达到 200。

若项目原需求含最低业务 TPS/SLA，读取已批准的具体目标；如果仓库没有冻结的最低值，报告实测及容量曲线，并明确标记 `THROUGHPUT_TARGET_UNSPECIFIED`，不能自行设定有利的 PASS 值。原 G4/G6 容量门禁在未获批准前仍沿用原口径。

## 6. 阶段 D — 统一 G4 / G6 / 正确性最终收口

**只对这一轮实际冻结的最终源码 + 测试 harness SHA 宣告 PASS。** 故障根因/优化修复后重新测试受影响门禁，不依赖历史中间版本的通过结果。

### D1. 基础 G0 / G1 / G3 / G5

- G0：AlmaLinux-8/ext4 全 backend pytest exit=0；Windows `WinError 32` backup 定向回归和 set-based 数量差异分别记录；前端 `npm run build`。
- G1：ingest-only 1K，sender-only pool1/4，multipart window1/2/50，ASCII/UCS2/Flash、Journal/result byte/counters，严格 file/chunk/task/attempt/part 守恒。
- G3：serialized/reliable、serialized/fast、occ/reliable、occ/fast 各 3 个独立新进程（12/12），1K，每个完成提交/响应/Journal/DB/audit，0 unsafe resend / duplicate / unmapped result。若出现任一 native 崩溃，立即保留证据并报告 FAIL，不从其它轮次拼 PASS。
- G5：drop submit_sm_resp、明确 reject、partial、late DLR、disconnect、fsync/DB outage、quarantine、replay barrier、unknown/partial 防整条自动重发、pause/cancel linearization。保留本地模拟器 `fast` 与 `fault` 两种明确模式。
- G-SIM：模拟器 capability（速率、响应、计数/audit 完整）与“无法找到精确旧版 debug/synchronous JSONL”分开。**不能因为已证明新的 Fast 模拟器快，就伪造旧版 A/B**；如正式门禁把旧版 A/B 规定为必需且不可取回，就保持 `HISTORICAL_AB_UNVERIFIED`，提交已记录的替代对照建议，不自行宣布全面 PASS。

### D2. G4：公平、容量、实际 socket 隔离

冻结每组 `route structure`（same/two route）、pool1/4、window1/2/50、B/C 与 A 数量、Route TPS、实际 bind/route revision、deadline、有效输入和 socket parts。

必须具备四种可独立审计的输出：

1. `FAIRNESS`: 大活动持续积压下，小活动 B/C 以及其他活跃组有真实 socket 写入/提交进展，跨活动没有可用资源下的无限饥饿；记录 P50/P95/max eligible→socket 和最大零进展窗口；同路由 DRR 按 parts，不以 task 数冒充公平。
2. `CAPACITY`: 固定大规模 52K 业务**完整容量测试**以及**实际完成数和每秒处理曲线**；时间窗必须单独列明（不允许改变原硬门禁就宣布通过）。若硬要求 deadline 低于单/双 route 的最低服务时间，原门禁继续 FAIL 并报合同冲突。
3. `CONTROL`: pause/cancel 后停止新 PDU、已发部分保留 evidence、其他 Campaign 继续、route revision/断连后不盲目重发。
4. `NATIVE`: 全程无 SIGSEGV、heap corruption、fatal error、无不明丢任务、无信用账本泄漏；否则即使 B/C 各 1000 通过，也仍 FAIL。

对 `A=50K/B/C=1K` 与四活动 runner `2L+2S` 的映射必须在报告最前面清晰写出；无真实采样数据不能将一个报告中的小组覆盖率推广到全部活动。

测试不可依赖每 0.5 秒读取全部 52K DB 任务或整份 JSONL 的 polling，允许低频观测加完成后严格逐条审计，不允许丢失 correctness evidence。

### D3. G6：至少 600 秒有效高负载长稳

- 在有持续输入、可观察 durable task/part/checkpoint/terminal 进展的高负载下运行至少 600 秒；不能仅靠程序存活、空转、初始 B/C 很快完成即称 PASS。
- 记录 per-campaign 当前 backlog 及积压增长率、生成/接受/发送每秒速率、最差 eligible age、30/60s 零进展证据、CPU/RSS/FD、DuckDB tx、Journal bytes/replay/credit、窗口占用、SMSC audit。
- 区分「600 秒内稳定服务且容量有限」和「在 600 秒内完成全部 52K」两个不同条件；若**原 G6 合同同时要求容量完成**，必须两个都达成才可从现有 FAIL 改为 PASS。若物理上不可能，出具合同可行性报告，但不能绕过旧门禁。
- 运行后继续只读对账：生成/Claim/Ready/Socket/响应/attempt/part/Journal/DB；无 invalid chunk state、duplicate event、missing message mapping、自动 unknown 重发。遇到原生崩溃立刻 FAIL 并保留 core/栈，不删去重跑。

## 7. 必须新增或强化的确定性回归

优先复用现有 `backend/tests`，只在必要时添加小而精确的测试。以下场景必须有相应断言：

- `CONFIG-1`：矩阵 CLI pool/window/route 参数实际落到 DB + gateway status + bind/socket 行为；错配启动即阻断。
- `CONFIG-2`：same-route/two-route 存在真实不同物理 route；总 TPS 不乘 pool；四活动输入各自数量正确。
- `OBS-1`：高压下采样每轮 DB 查询数量/字节有界；最终 audit 精确无缺口；禁用高频采样仅用于对照而非正式关 audit。
- `FAIR-1`：A 长积压+两个 B/C，socket writes 持续、等待可测；window1/2+heavy multipart continuation/yield 不饿死其他活动。
- `NATIVE-1`：多线程 cursor/close + backup + stop/restart/route revision 并发，不触碰正在使用的 native owner；负载超时有线程现场。
- `BATCH-1`：Fast/Reliable 成功路径 batch 实际 item 数、DB transactions 数、Journal event_id/terminal、result credit/bounded bytes，异常情况下守恒；符合 Windows/WSL 现有 set-based fence。
- `SAFE-1`：DB/Journal fsync failure、late response、partial/unknown、duplicate callback、cancel fence、重启 recovered 事件不触发不安全自动重发。

所有测试按真实工作区版本/依赖冻结；不要模拟“通过”的指标替代真实 SMPP socket 对账。

## 8. 交付文件与执行规范

**新建唯一工作报告**：`Docs/Marketing_Copilot_Native_Stability_Throughput_Final_Closure_Report.md`。建议包含以下章节：

1. 最新版本/源码/测试 harness/依赖 manifest 与保留的原始失败；
2. G4 参数有效性（declared vs effective）、52K 活动映射和理论下限审计；
3. 压测观察者效应 profiling 与优化前后成本；
4. SIGSEGV reproduction / native frame（若有）/ verified cause /修复与剩余风险；
5. DB/result/claim 的 throughput 分解、batch size、事务数及 before/after；
6. G0、G1、G3、G4、G5、G6、G-SIM 每个 gate 的 `PASS/FAIL/NOT_RUN/ENV_BLOCKED/SPEC_INFEASIBLE_NEEDS_DECISION`，不得只写总 PASS；
7. 真实 socket/test suite 的 source SHA、运行路径、原始输出文件及散列、审计守恒结果；
8. 进程/资源/锁/credit 不变量与 legacy 兼容、回退；
9. 最终 `INTEGRATED_READY_FOR_S5` **或** `INTEGRATED_NO_GO`，附最少剩余阻断。

可以同步更新原总报告 `Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md` 的末尾新增章节（追加，不覆盖或重写历史失败）。不得涂改以前的故障数据或用新版补充结果静默改写旧结果。

### 内部连续里程碑（不拆成子任务）

- **Checkpoint A**：验收配置与观测者效应的审计证据/工具修复完毕；同一任务内继续。
- **Checkpoint B**：高并发 native 问题定位、修复并跑过直接复现；如未收敛继续调查，保留 `NO_GO`。
- **Checkpoint C**：热路径性能 A/B 后有证据地最小优化，资源账不破坏。
- **Checkpoint D**：同 SHA 各门禁统一验收与报告，分类结束；若硬性 52K 期限从数学上不可行，作为需用户决策的规格问题明示，绝不暗改。

如果运行上下文超长，可保存 checkpoint/report 并在新 Codex 会话继续同一个任务，**不需要生成新的 S2.2/S3.1 大型重构计划**。

### 结果输出契约

最终向用户报告：

- 修复文件和证据概况；
- 哪些失败由测试 harness 导致、哪些是系统真实性能/原生稳定性问题；
- 每种模式的真实应用 tasks/s、parts/s、simulator/s、batch/transaction 统计及资源曲线；
- G4/G6 严格旧标准是否达成；若不能，准确说明是硬性不可行、崩溃、业务瓶颈还是剩余证据缺失；
- `INTEGRATED_READY_FOR_S5 / INTEGRATED_NO_GO` 以及独立 S5 仍包含真实 SMSC/SLES 环境验收；
- `git status --short`；并明确**未擅自 commit/push，未修改用户无关资料**。

## 9. 不可妥协的禁止项

- 不直接调大超时、Route TPS、Worker、队列容量来冒充达到原有固定门禁。诊断性运行可使用不同参数，但证据不得被并入原条件的 PASS。
- 不改变 Gate 判定、任务总数/part 数、time_budget，除非用户明确批准修订合同；将“规格本身不可能”与“代码不达标”分别报告。
- 不为了吞吐跳过 Journal fsync、DB audit、原始 attempt 身份、防重复/防未知重发、result hard bytes、PDU window 或 stop/cancel 栅栏。
- 不把 `SIGSEGV` 隐藏成 env blocked 后继续宣布 READY；即使根因难以复现，也必须在报告中保留原生高并发风险。
- 不新增第二 DuckDB writer、无界线程池、深 Slot FIFO 或重复结果队列来掩盖瓶颈。
- 不删除失败证据，不删除/重置用户文件，不自动提交推送，不连接真实生产资源。

**开始执行顺序：核实最新 HEAD/工作区 → 读现有报告与源码 → A 测试配置可信度 → B native crash → C DB/result 性能 → D 全量门禁与报告。**
