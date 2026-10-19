# Marketing Copilot — G4 合同澄清、双 Route 修复与吞吐 / WAL 根因收口 Codex 任务

**任务 ID**：`MC-G4-CONTRACT-ROUTE-THROUGHPUT-WAL-CLOSURE-20261016`  
**性质**：基于现有架构的最小修复、性能归因、严格验证；**不是**架构重构任务  
**已知最新提交**：`f5b72b8cc25e78bb5e7759b0f1ac358833187874`（以执行时实际 `origin/main` 为准）  
**目标仓库**：`shutu-ai/Marketing-Copilot`  
**报告路径**：`Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Report.md`  
**验收合同**：`Docs/validation_contract.json`（默认不得修改既有合同内容）  
**最终状态**：只能按实测证据报告 `INTEGRATED_READY_FOR_S5` 或 `INTEGRATED_NO_GO`；合同待决、门禁未通过或不可确认时必须为 `INTEGRATED_NO_GO`。

## 0. 执行命令

在仓库根目录启动 Codex，提供如下指令：

> 执行 `Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Codex_Task.md`。从最新 `origin/main` 开始，遵循本任务工作区保护、冻结合同、阶段门禁与统一报告要求。先澄清 G4 合同矛盾（只形成待用户决策建议，不擅自修改），再最小修复并验收双 Route bind；随后隔离开发机/WSL/观察器开销、归因 5500-task 超时和 OCC WAL 冲突；仅在证据充分时进行局部修复。不得重构调度架构、不得降低验收标准，保留全部失败证据并如实给出最终 NO-GO / READY 判定。

## 1. 必须阅读的冻结基线与源文件

先核对（勿将报告中的历史 `HEAD` 误认为当前 `HEAD`）：

1. `Docs/Marketing_Copilot_Native_Stability_Throughput_Final_Closure_Report.md`
2. `Docs/validation_contract.json`
3. `Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md`
4. `Docs/Marketing_Copilot_Integrated_Architecture_Convergence_Plan.md`
5. `backend/services/smpp_gateway.py`、`task_worker.py`、`sms_task_service.py`
6. `backend/services/sms_write_buffer.py`、`result_journal_service.py`、`fast_send_service.py`
7. `backend/services/realtime_ingestion_worker.py`、`realtime_campaign_service.py`、`activity_scheduler.py`
8. `backend/tools/multi_campaign_fair_acceptance.py`、`mixed_campaign_capacity_acceptance.py`、`local_e2e_acceptance.py`
9. 项目已有相关 tests、`AGENTS.md`（仅遵守指令，不修改）。

在报告首页记录：执行起点 `git rev-parse HEAD`、`git rev-parse origin/main`、`git status --porcelain`、测试环境、依赖版本和被测代码 / runner / 配置的 SHA256。若 `origin/main` 在执行期间更新，先审查差异，避免用不一致的源码拼凑 PASS。

### 已确认的历史事实（不可用新 PASS 覆盖旧失败）

- 截至 `f5b72b8c`：G0 后端 `348 passed, 2 skipped`，前端 build PASS。
- 精确历史 OCC + Fast 1K SIGSEGV 参数复跑 PASS，**不等于**所有高并发 native 风险消失。
- 四活动 5500 tasks 的 OCC、serialized 诊断都在 300 秒内超时；OCC 存在 WAL write-write conflict。
- 双 Route：第二路由没有成功 bind，配置真实性 FAIL。
- G4 合同同时存在命名 A/B/C=52,000 与四活动 runner=102,000 的口径歧义；单 Route 200 PDU/s、单 PDU 假设下最短分别为 260 s 与 510 s，均不可能在 180 s 完成。
- G6 有效约 631 秒，仅 5,283 accepted，严格容量目标 FAIL。
- 300/500/1000 ms Fast checkpoint A/B：DB 写入量下降不代表端到端吞吐上升；默认仍为 300 ms。
- 旧的 SIGSEGV、超时、WAL conflict、双 Route、观察器过载、G6 失败证据均需保留。

## 2. 不可违反的范围和安全约束

- **禁止**大规模架构重构，不新增调度层，不开发或迁移到 File-Centric Pipeline；“每目录线程/进程全流程发送、完成后统一结果文件入库”目前只是讨论方案，不是本任务授权内容。
- 保留现有 `file/chunk lease`、dedupe、checkpoint、claim token / generation、发送前 durable authorization、Journal fsync、Result capacity / credits、event-id 幂等、unknown/partial 保守策略、pause/cancel fence、DLR 语义、route-shared TPS、PDU window、multipart 保护和 fail-closed。
- 禁止为了性能跳过授权、压缩安全证据、放松限速、增加无限队列、把 uncertain 任务自动重发、把 `accepted` 误记为 `delivered`。
- **禁止**静默更改 G4/G6 原容量、限时、正确性阈值；对数学矛盾必须写入决策表，等待用户明确决定后再形成新的合同版本。
- 禁止更改生产 SMSC、真实号码、生产数据库，不进行 SLES12SP5 生产部署或 S5 切换；所有端到端测试限 loopback / 合成数据与隔离数据库。
- **禁止**清理、重置、覆盖历史失败 evidence、WAL、Journal、用户未提交改动。不得执行 `git reset --hard`、`git clean` 等破坏操作。
- 不主动调用仅用于系统架构设计文档的 `solution-architect` Skill 或其他不相关 Skill；以本任务指定的代码级诊断工作流程为准。
- 先测量后改代码；若没有可靠收益/根因证据，则只保留 instrumentation、tests、报告，不做猜测性调参。

## 3. Phase A — G4 合同审计与决策准备（先做，不得私自改合同）

### A1. 源码、合同、runner 三方逐项核对

核对并输出一张可机读表：

| 项目 | 当前合同 / runner 实际值 | 需明确的问题 |
|---|---|---|
| A/B/C 命名活动 | A=50K、B=1K、C=1K → 52K | 是否为目标全量？ |
| 当前四活动 runner | realtime-A=50K、batch-A=50K、realtime-B=1K、batch-B=1K → 102K | 是否设计成额外复制的两类活动？ |
| 相同 Route | route-shared=200 parts/s | 200 是每物理路由，不是每连接 |
| 双 Route | 每物理 Route 独立上限 200 parts/s | 实际流量如何分配？两个 Route 是否都应 bind？ |
| deadline | 当前 180 s | 是公平性观察期限，还是全量容量完成期限？ |
| G6 | 600 s 有效长稳及原严格容量目标 | 600 s 是否要求全量完成？详细原判定必须保留 |

**数学下限（不要据此推导实测 TPS）**：对于单 Route，`T_min >= total_PDU / 200`；若全部单 PDU，52K 至少 260 s、102K 至少 510 s。实际 multipart、DB、摄取、停顿只会增加所需时间。双 Route 应按实际分配的 `max(PDU_route_i / TPS_route_i)` 计算，不能在没有双 Route 实际 bind 和分流证据时假定总 TPS=400。

### A2. 提交合同决策记录，不代替用户决策

在统一报告建立 `CONTRACT_DECISION_REQUIRED` 节，提供**可选修订方案及取舍**，例如：

- 区分“短时公平性观察窗口（如现有 180 s）”与“全量容量完成期限”（期限须独立确定）；
- 以 52K A/B/C 或 102K 四活动选择**一种**正式容量映射，并说明覆盖范围；
- 区分同 Route / 双 Route 的 TPS scope 和可达的数学下限。

只提出建议和量化影响。**在用户确认之前**：保持原 `validation_contract.json` 不变，正式 `G4_CAPACITY` 状态只能为 `SPEC_INFEASIBLE_NEEDS_DECISION`，不能将任一缩小或延长的测试称为正式 PASS。已可判定的软件正确性子门禁可以独立继续验证。

## 4. Phase B — 双 Route bind / 配置真实性最小修复（优先代码修复）

### B1. 根因确认

重点审查 `backend/services/smpp_gateway.py` 中 `SmppGateway.connect()` 对 enabled configs 的循环，当前在第一条配置成功后返回；检查实际调用方、`_connection_for`、池初始化、定向路由、主备 failover、连接状态与 `/api/smpp/status` 展示之间的关系。

明确区分两种模式：

1. **主备/回退配置**：保持原“优先首个可用 Route，故障后尝试备 Route”的兼容语义，不能粗暴强制所有配置每次都 bind。
2. **并行双 Route 场景**：当存在按 Route 定向的真实待发任务/显式并行测试要求时，所需两个 Route 必须各自完成有效连接、SMPP bind 与就绪判定，不能将 TCP connected 当作 bind PASS。

### B2. 最小修改与覆盖

- 以最小明确的连接准入/初始化逻辑解决“并行配置只激活首条”的问题；不新建一套 Gateway 或独立调度架构。
- 保证 route affinity：任务所关联的 Route 必须按配置发送；目标 Route 不可用时遵循已有明确的故障/回退策略，不能无授权改投其他 Route。
- 保证多 Route 池容量、per-Route shared TPS、每连接 PDU window、bind generation、disconnect/reconnect、pause/cancel fence、credit 守恒。
- 若某并行测试需要 Route 2 而该路由未 bind，必须在测试开始前 `HARNESS_CONFIG_FAIL` / fail-closed，不能以 Route 1 的成功率掩盖。

### B3. 验收

- 主备兼容：单 Route、首 Route 正常、首 Route 失败后的既有 failover 行为均回归。
- 双 Route：`pool=1/window=1` 与 `pool=4/window=50` 至少覆盖，记录每 Route `declared -> DB persisted -> Gateway effective -> TCP connected -> SMPP bind -> submit/response`。
- 双 Route 真实 PDU audit、数目、配置 ID、任务归属一致；验证每个 Route 的 PDU TPS 不随 pool 倍增。
- 第二 Route 不可用的注入测试不产生 misdelivery、重复发送、unknown 自动重发、假 PASS。

## 5. Phase C — 5500-task 超时：区分软件热路径与开发环境资源瓶颈

先建立**无代码更改的可复核 profile 基线**，再只为定位问题增加低开销 instrumentation。

### C1. 固定负载与变量

- 以原 1K 成功案例为控制组、原四活动 5500-task 为关键诊断组，固定 source SHA、Python/DuckDB、DB schema、文件数据、route TPS、pool/window、mode、观察器与后台进程状态。
- 5500-task 使用原 `large_rows=2500, small_rows=250` 四活动映射，总 5500；区分 OCC 和 serialized；各组至少 2 次，资源允许再做第 3 次。不得把重复跑结果混合为一次 PASS。
- 每组使用独立 ext4 运行目录、独立 DB/WAL/Journal/模拟器端口、冻结配置清单；不得混用 `/mnt/c` / `/mnt/d` 与 ext4 结果。
- 不修改正式 gate 的 180/300/600 s 阈值来制造 PASS；额外诊断可在**独立标识**的 profile 中延长观测，但必须写明 `DIAGNOSTIC_ONLY`。

### C2. 逐阶段测量（不得用循环采集本身拖慢被测程序）

记录：

1. **文件路径**：发现文件、复制/稳定性、Claim/Lease、CSV/XLSX 分块读取、白名单匹配、任务生成和持久 checkpoint 的数量、耗时、等待。
2. **发送路径**：Fair 候选扫描、Claim transaction、send intent/authorization 事务、PDU socket write、SMSC response、slot/window/Route TPS 等待、按活动公平服务时间。
3. **结果路径**：Fast checkpoint 聚合次数与真实 items/batch，WriteBuffer enqueue/drain、Journal append/fsync、`sms_result_batch` transaction、SQL 执行与锁等待/持有、terminal ack、WAL checkpoint/replay。
4. **队列守恒**：generated / eligible / claimed / authorized / written PDU / responded / accepted / failed / unknown / partial / DB committed / journal terminal；每阶段 backlog 曲线和 age P50/P95/max。
5. **系统资源**：WSL 和 Windows 宿主 CPU（总量及**最忙单核**）、进程/线程 CPU、RSS/可用内存、Swap、磁盘利用率与 `await`/fsync/读写速率、WSL 限额、测试和后台进程 CPU 竞争、模拟器 CPU 与 I/O。

只记录非敏感计数及摘要，不输出真实手机号、短信正文或凭据。采样成本 A/B：默认每 1 秒有限指标；全量任务、attempt、part、event 对账仅在停机后做。若 `pidstat` / `iostat` 不可用，优先用 `/proc` 与现有系统能力，说明缺口；不得将新安装工具或宿主机配置变更作为隐含依赖。

### C3. 必做的分离实验

- Simulator-only（与业务应用分开）以证明 SMSC 不是限制源。
- Ingest-only（不得发送短信）测文件解析/白名单/任务写入能力。
- Sender-only（预生成任务）测 Claim/授权/发送/结果热路径。
- Result-only / DB isolated benchmark 测 batch size、锁等待、WAL/checkpoint 开销，但结果不得伪装成端到端 TPS。
- 完整 E2E 对照，拆分每阶段占比、确认是否真正存在并行重叠；**不要简单将并发阶段耗时相加当作总时长**。
- 尽可能以同机受控方式进行 CPU 亲和/并发后台负载对照（例如允许情况下的 `taskset`），但不能强制修改用户 `.wslconfig`、操作系统或生产环境；资源实验必须与正式性能门禁结果分列。

### C4. 归因结论必须满足证据规则

将每个候选瓶颈分类为：

- `APP_DB_TRANSACTION_OR_LOCK`
- `APP_SCHEDULING_OR_SYNC_WAIT`
- `INGEST_OR_WHITELIST`
- `SMPP_OR_ROUTE_LIMIT`
- `HARNESS_OR_OBSERVER`
- `HOST_OR_WSL_RESOURCE_BOUND`
- `UNKNOWN`（证据不足时使用）。

任何“主要原因”必须有至少一种对照实验或可量化时间/资源守恒支持。CPU 整机利用率低不能排除单核瓶颈；磁盘空闲不等于 fsync/锁开销不存在。明确分离：软限制、硬件限制、两者共同作用，以及暂未确认的贡献。

## 6. Phase D — OCC WAL write-write conflict 与 Native 稳定性

### D1. 不预设根因

- 追踪 OCC 模式的所有数据库 owner、连接、cursor、WAL、事务提交/回滚、replay、maintenance，以及 5500-task 冲突键 `"realtime, 1, 1"` 的具体访问来源。
- 检查多个写路径是否同时修改同一 logical key，是否存在跨线程重用不安全连接、事务嵌套、写冲突后的错误重试、异常恢复次序问题。
- 强制区分 `OCC conflict`、`WAL replay error`、`lock timeout`、`SIGSEGV`，**不可**将它们当作同一已证实的 DuckDB native 根因。
- 保留原始 SIGSEGV 失败及其“无 core/native faulting frame”的限制；复跑未 SIGSEGV 只说明特定复现条件未再出现。

### D2. 最小变更策略

- 优先修复确定性线程/连接所有权错误、事务 fencing / 原子性、重复写同一冲突 key、错误的 rollback/retry 处理。
- 只有在能验证全事务重试幂等与无重复外部发送的前提下，才能更改 OCC conflict retry。
- 不得通过无限重试、捕获吞掉 WAL 错误、改写 unknown/partial、增加锁超时或降低审计等级掩盖问题。
- serialized 默认/回滚能力不得破坏；OCC 仍为显式试验，不得因单轮 PASS 自动切换生产默认。

### D3. Native 诊断

若再发生 SIGSEGV/SIGABRT：保存退出码、stderr/faulthandler、内核日志（可用时）、core 与 gdb 栈（可用时）、二进制版本、内存限制和精确复现参数；若无法取得 native frame，必须如实写 `UNAVAILABLE`，不能猜测具体崩溃栈。

## 7. Phase E — 基于证据的局部优化（仅在前述工作完成后）

仅允许有明确证据支持的最小变更，例如：

- 减少高频无效 DB 查询或重复事务；
- 对 **Result/Checkpoint** 做已验证安全的有界聚合；
- 修正不必要的同步等待与监控开销；
- 在不改变发送前授权语义的前提下优化事务内容或批量操作。

尤其注意：`sms_task_service.record_send_intent()` 的发送前 durable authorization 不能用“仅发送结束后落盘”替换。已有 Result Journal/WriteBuffer 不得重新设计成另一套结果文件采集架构。

每个性能改动都必须：保留开关或明确可逆方案；运行同 SHA 前后 A/B（受控环境）；报告 throughput、P95/P99、CPU、内存、DB lock/tx、Journal、结果延迟和无重复/无丢失对账；若收益不确定或有安全回归则回滚该改动并保留原失败记录。`MC_FAST_SEND_FAIR_CHECKPOINT_MS=300` 保持默认，500/1000 ms 仅为显式实验，未证实端到端收益前不得提高默认值。

## 8. Phase F — 验收门禁与停止规则

### F1. 验收顺序

1. G0：全部后端 tests + frontend build；若新增/修改目标模块测试，定向测试先 PASS。
2. `G4_CONFIG`：same-route + two-route 的真实 effective/persisted/bind/TPS/响应审计。
3. `G4_OBSERVER`：live bounded 采样与停止后全量 audit，开销 A/B。
4. 1K 控制组：原精确 OCC+Fast + serialized / reliable 必要回归，result/claim/attempt/part/credit 无差错。
5. 5500-task：serialized 对照、OCC 冲突复现与关闭，测试超时严格保持 FAIL，额外 profile 只作诊断。
6. G4 严格容量：**仅在用户批准统一合同后**按照新正式版本执行；未经批准不得用缩小任务量、改延时、改时间窗凑 PASS。
7. G5：故障恢复覆盖至少 drop response、disconnect、partial/multipart、restart/replay、Journal/DB outage、pause/cancel，保证 unknown 不自动重发。
8. G6：完整原 600 s 有效长稳和原容量要求；历史 FAIL 永久保留。合同未决时只能诊断，不得宣称正式 PASS。
9. 若触及 G3 关键链路，必须在最终同一冻结 SHA 上复跑受影响的 G3 组合；不以历史 SHA 的 12/12 冒充新源码 12/12。

### F2. 自动 Fail 条件

任何 `SIGSEGV` / `SIGABRT`、WAL 数据一致性损坏、unknown 自动重发、misdelivery、重复 submit、部分消息状态丢失、dispatch/part credits 不守恒、错误 Route bind 假 PASS、审计缺口、不可恢复积压、违背限速、越权发送，均直接 FAIL 并保留证据。禁止“部分通过”替代严格容量通过。

### F3. 状态规则

- `PASS`：最终源码及合同一致、场景实跑完成、证据可复核；
- `FAIL`：实跑触发失败条件或未达到严格指标；
- `BLOCKED_SPEC` / `SPEC_INFEASIBLE_NEEDS_DECISION`：合同不自洽或待用户批准；
- `NOT_RUN` / `ENV_BLOCKED`：证据不足或环境不允许；必须说明影响范围；
- **全局 READY**：只有全部进入 S5 的必需门禁在统一正式合同下 PASS 才能使用 `INTEGRATED_READY_FOR_S5`；否则 `INTEGRATED_NO_GO`。

## 9. 交付物、统一报告与提交

### 9.1 必须交付

- `Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Report.md`：**唯一主报告**，正文必须包含：
  1. Base SHA、git 状态、变更文件 SHA、依赖/硬件/WSL/ext4 环境。
  2. 合同 52K/102K 和 180/600 s 的数学下限、选项、待决记录，明确是否获得用户批准。
  3. 双 Route 问题的精确根因、最小修复 diff、主备/并行路由的验收对照。
  4. 1K、5500-task 详细运行条件、事件守恒、各阶段吞吐和等待、资源曲线，区分开发机瓶颈与软件瓶颈。
  5. OCC WAL 冲突根因/未证实项、serialized 对照、Native 复现记录。
  6. 每项性能改动的成对 A/B 与“保留/回滚”决策。
  7. G0/G1/G3/G4_CONFIG/G4_OBSERVER/G4_FAIRNESS/G4_CAPACITY/G4_NATIVE/G5/G6/G-SIM 状态矩阵。
  8. 所有历史与新增 FAIL artifact 的**引用索引**；不得改写或删旧失败。
  9. 未闭环项目、用户待决项、最终 `INTEGRATED_READY_FOR_S5` / `INTEGRATED_NO_GO`。
- 若需新增 machine-readable 诊断结果，将其放在隔离的 `evidence` 运行目录，并在报告中给出路径、摘要和对应 SHA；不要提交大体积运行产物、DB/WAL/Journal、缓存或敏感数据。
- 必要的局部生产代码、定向回归测试以及 runner 参数真实性测试。避免无关文档和架构方案文件泛滥。

### 9.2 Git 工作流程

1. 开始前 `git fetch origin`，以最新 `origin/main` 为已知可复核基线；有 dirty worktree 时保护用户修改，不覆盖、不 stash 丢失、不 reset。
2. 每次测试记录代码 manifest、命令行、环境和关键参数；全部测试基于一致代码，不能跨 SHA 拼装正式 PASS。
3. 只提交本任务变更：生产最小修复、必要 tests/runner、唯一主报告；`validation_contract.json` 除非用户**明确批准决策并授权更新**，否则保持不变。
4. 不更改 `AGENTS.md`、无关用户文件和 `frontend/package-lock.json`，不提交 `.tmp/`、`cache/`、`memory/`、DB/WAL/Journal、日志及历史运行 evidence。
5. 提交前检查 diff 与测试。具备 Git 授权且没有远端冲突时，可提交并推送 `origin/main`；即使最终 `INTEGRATED_NO_GO`，也可提交真实的根因修复、测试及报告。若无法安全推送，应说明阻断并保留工作区，不 force push。
6. 最终消息必须写出 Commit SHA、push 状态、变更文件数、正式 gate 结果、未解决的合同决策和最终全局状态。

## 10. 成功标准（不是强行做到 READY）

本任务的**工程完成**指：G4 数学/映射矛盾被清晰记录并形成待决方案；双 Route 真 bind 问题得到最小修复且协议对账；5500-task 超时的主要限制有定量证据区分软件/开发环境；OCC WAL 冲突完成可复核根因分析与必要安全修复；所有门禁在同一源码/合同下如实判定；所有失败证据保留。

**注意**：如果用户尚未决策 52K/102K 或 180 s 完成窗口，或者 G4/G6/Native 仍 FAIL，则最终必须保持 `INTEGRATED_NO_GO`。这仍可视为本 Codex 诊断/修复任务按范围完成，但绝不等于 S5 已具备上线条件。
