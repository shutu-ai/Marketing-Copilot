# Marketing Copilot — S0.1 OCC + Fast SIGSEGV 与摄取状态异常专项闭环

**任务 ID**：`MC-SCHED-S0.1-OCC-FAST-CRASH-CLOSURE-20261015`  
**建议 Codex 模型**：`gpt-6.1-sol` / `xhigh`  
**上游基线提交**：`a856dc05b5b3272a7196a86a71b4ad9ff01ea922`  
**本阶段目标**：关闭 S0 遗留 R01 阻断，取得真实、可核验的 `P0_CORRECTNESS_READY` 证据；**不得实施 S1–S4 架构重构**。

## 一、先阅读与确认

1. `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`（S0 与 G1/G3 验收、R01/R02/R03/R17）
2. `Docs/Marketing_Copilot_S0_Correctness_Baseline_Closure_Report.md`
3. `Docs/OCC-Ingestion-State-Consistency-Task-Summary.md`
4. 当前源码、`backend/tests/test_s0_correctness.py`、`backend/tests/test_occ_ingestion_consistency.py`、`backend/tools/local_e2e_acceptance.py` 及关联结果审计逻辑。

启动前记录当前 HEAD、工作区状态、测试环境、依赖（尤其 DuckDB 1.2.0）、测试源码/参数 manifest hash；核实报告对应提交与当前代码是否一致。保留已有用户修改、未跟踪资料与验收证据，不得重置或清理真实数据库。

**现有事实，不得改写为 PASS**：

- S0：`S0_NO_GO`，`P0_CORRECTNESS_READY = NO`。
- R02/R03/R17 确定性回归已 PASS，但不证明 R01 已关闭。
- R01 假确认修复已落地；OCC+Fast 1K 曾发生 API `SIGSEGV`，之后同参数重跑成功。
- 失败快照：预期 1,000 tasks，实际 600 tasks；500 socket success；301 accepted、199 sending、100 queued；出现 `completedcompletedcompletedcompletedcompletedcompleted` 非法 chunk 状态。
- G0：288 passed / 2 skipped；OCC 定向：161 passed（均为上次证据）。G3 与生产仍 `NO_GO / NOT_VERIFIED`。
- 崩溃记录位于历史报告所指向的隔离运行目录；如原始证据不可取得，明确标注，不可按报告文字反推 native 根因。

## 二、执行范围：先找到根因，再做最小修复

### A. 建立可追溯的失败证据链

检查失败运行的 API 退出快照、stderr/stdout、Python traceback、native 崩溃诊断、OCC conflict/retry、DuckDB 连接和事务生命周期、Journal 与 chunk/task 状态。按单条事件时间线对齐：

`file lease/claim → chunk processing → task checkpoint → OCC rollback/retry → Journal/result commit → process exit`。

至少回答：

- `SIGSEGV` 是谁发出的信号/如何被观察到？是否有 native core 或 C/C++ 栈？其 `0xFFFFFFF5` 状态值如何记录/解码？
- 进程崩溃前最后一个**已证实**的调用点是什么？OCC 冲突重试耗尽与 SIGSEGV 是因果还是时间邻近？
- 非法 `completed...` 最初出现在哪里：内存、SQL 参数、DB row、replay，还是审计解码？是否与崩溃共享写入路径？
- 哪些事务已 commit、哪些 rollback、哪些结果仅在 Journal、哪些 socket 已实际写出？
- 是否涉及跨线程 DuckDB connection/cursor 共享、连接关闭与仍运行查询竞争、事务包装器误用、native extension/版本问题或工作区环境；这些首先列为**待证假设**，不能直接宣布根因。

如证据不足，增加可受控启用的诊断：`faulthandler`/线程栈、native core dump 或 gdb（环境允许时）、进程退出结构化元数据、事务 operation/id/owner/thread、异常类别、chunk 状态原始 bytes/hash 与变更事件。诊断不得泄露号码、正文、凭据；避免在热锁中同步大量写日志影响复现。

### B. 设计最小且可重复的复现场景

从现有 10 files × 100 / OCC+Fast / loopback-only 流程出发，抽出最小可验证复现。保证每次为**新 API 进程、新 DuckDB、新 Journal、新端口、新输出目录**，数据库放 WSL `AlmaLinux-8` 的 ext4 内，不允许复用旧 DB/WAL/日志。

以变量隔离方式交叉检查：

- `occ` 对比 `serialized`（同一 workload）
- `fast` 对比 `reliable`（同一 workload）
- 单文件/多文件、ingest workers 1/2、task workers 的受控比较
- Journal/result 提交的可控 OCC conflict/fault injection
- 非法 chunk 状态写入、重试、恢复的确定性回归

**先进行短、小、可定位的诊断运行，避免一开始就做大量 1K 重跑。** 对 crash 和非法状态优先通过 barrier、故障注入、可验证事件顺序形成确定性测试。若 native crash 无法可靠重现，保存全部重复尝试结果与概率范围、core 缺口及待证分支，**不得因偶发 PASS 认定已修复**。

### C. 最小正确性修复

只在证据明确后，针对实际根因修改关联的最少代码。修复必须满足：

1. OCC 失败：完整事务重试/有界 fail-closed，禁止部分 commit、错误吞没、Journal 假确认。
2. chunk 状态：只能做明确、合法且受 lease/version fence 保护的转换；异常字符串保留原始证据并 quarantine/fail-closed；禁止通过字符串包含判断完成。
3. native 崩溃：定位连接所有权/释放、事务与线程使用或依赖/环境根因，提供有证据的修复与独立对照；**禁止仅调大重试次数、降低并发、加 sleep 或延长 timeout 伪装关闭**。
4. 所有已发/可能已发 SMS 保留 attempt/part/Journal 证据；`unknown/partial` 绝不因修复而自动整条重发。
5. 保留 R03 stop gate 与 R17 DLR 保证，不能引入新的 late submit、重复 socket、跨 attempt 污染。

若经诊断表明 DuckDB 1.2.0 或 WSL/运行时存在不可安全规避的 native 缺陷，先提交版本/环境 A-B 矩阵与风险报告；库升级需验证 schema、Journal、回退和现有契约，不得无证据直接升级替代根因分析。

## 三、验证与明确验收口径

**先锁定最终源码快照**（SHA + manifest + 环境参数），然后测试；最终修复后的任何代码更改都必须重跑受影响验证。允许在专门的隔离测试环境使用模拟 SMSC，不向真实号码发送。

1. **确定性 R01 测试**：catalog error、OCC conflict、事务回滚、Journal 未假确认、重复事件幂等；新增 crash/非法状态针对性测试。需能展示修复前失败、修复后通过，或者提供明确不可复现原因。
2. **R02/R03/R17 防回归**：重跑非法 chunk/lease、Reliable/Fast stop/multipart、DLR/unknown/partial 相关测试，必须全部通过。
3. **G0**：当前最终源码全套后端测试 + Frontend build；错误与 skip 如实记录。测试系统/fixture ACL 问题按隔离 basetemp 修复，不能算业务 PASS。
4. **G1/T01**：独立 ingest-only 的 10×100：恰好 1,000 唯一任务，文件/chunk 权威完成、socket=0；sender-only pool=1/4 与 multipart 关键路径通过。
5. **OCC+Fast 1K**：最终源码在固定一致参数下至少 **3 次相互独立的新进程端到端运行**，每次完整 1,000 task/实际 PDU/attempt/Journal/file/chunk 对账，并且所有进程正常退出，且无 SIGSEGV、非法状态、假确认、未知不安全重发；同时保留最初失败证据。这里的 3 次属于**S0.1 稳定性回归要求**，**不代表 G3 四组合×3 已通过**，也不单独构成 native 根因闭环。
6. **对账**：输入业务 key 集合与 task 集合双向比对；task→attempt→part→实际 socket→SMSC 响应→Journal→DB 事件关联；无不可解释差集。DLR disabled 不能冒充真实送达。
7. **门禁原则**：若 native 根因仍未明确、非法状态仍可在真实压力下发生、存在 SIGSEGV 或任一守恒失败，必须输出 `S0.1_NO_GO`，继续阻断 S1、G3 和生产切换。即使多次运行 PASS，也不能覆盖尚未解释的 native crash。

现有 Phase 1 文档 G3 要求的 serialized/reliable、occ/reliable、serialized/fast、occ/fast 各 3 次，以及 S1–S4 的公平/高负载验收，**不在本任务内冒充完成**。

## 四、交付物

仅新增/修改与专项修复有关的源码、诊断测试/隔离验收工具，以及：

`Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md`

报告必须包含：

- 精确源码版本、依赖/OS/参数/manifest、原始证据路径与复现命令
- 故障时间线及「事实 / 推断 / 未证」分类
- SIGSEGV、OCC 冲突、非法状态三者的因果结论或未解决分支
- 修复前后差异、触及模块、并发和状态不变量
- 所有测试的单次原始结果、退出码与审计摘要（不可仅报合计 PASS）
- 测试缺口、回退边界与剩余风险
- 最终判定：`S0.1_READY / P0_CORRECTNESS_READY=YES` **仅在全部本任务门禁及根因闭环成立时**；否则 `S0.1_NO_GO / P0_CORRECTNESS_READY=NO`
- G3 和生产各自仍应单独标注 `NO_GO / NOT_VERIFIED`，不能随 S0.1 自动升级。

## 五、工作区和提交约束

- 不改业务 schema、不新增 ready pool/credit/Slot fairness、PDU window 重构或 Worker 扩容；这些属于 S1–S4。
- 不读写/删除生产 DB、真实 WAL/Journal、联系人或发送凭据；不得触发真实 SMSC。
- 诊断与验收只使用隔离目录，保留失败 artifacts；不删除用户已有文件和 `frontend/package-lock.json` 既有修改。
- 提交/推送前确认 `git status`、`git diff --check`、新增文件清单，严禁提交运行数据库、WAL、Journal、日志、缓存、密钥、敏感样本。
- 如提交和推送，明确记录最终 HEAD、origin/main SHA，且仅包括本次 S0.1 相关文件。**如需要绕过已有用户工作区修改，请只分批精确 stage 本次文件。**

## 六、结束时只汇报

1. SIGSEGV 与非法状态的**已证实**根因（或具体未证原因）
2. 修改文件及最小修复点
3. 独立 OCC+Fast 1K 运行结果和完整对账
4. `S0.1_READY` 或 `S0.1_NO_GO`，及 `P0_CORRECTNESS_READY` 是否成立
5. 新报告路径与提交 SHA（如有）

**执行优先级**：证据与最小复现 > 修复 > 确定性测试 > 独立端到端重跑 > 门禁结论。严禁为赶进度降低验收标准。
