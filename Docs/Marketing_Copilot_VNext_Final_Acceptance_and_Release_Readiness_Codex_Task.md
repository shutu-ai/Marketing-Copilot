# Marketing Copilot VNext — Runtime Canary 最终验收收口与发布准备（Codex 任务）

> Task ID: `MC-VNEXT-FINAL-CANARY-ACCEPTANCE-RELEASE-READINESS-20261017`  
> 类型：**证据复核、验收状态决策、受控发布准备**。本任务不是新架构开发、Native 崩溃猜测式修复或新一轮压测。  
> 冻结 GitHub 基线：`01409126a5c315ee91696c135db52a5c5ece2287`  
> 项目：`shutu-ai/Marketing-Copilot`  
> 语言：报告和解释使用中文，技术字段、状态常量及原始证据保留原样。

## 0. Codex 直接执行指令

在当前 Marketing-Copilot 工作区执行本任务。先读取 `AGENTS.md`，确认 `git rev-parse HEAD` / `origin/main` / `git status`；工作区可能存在无关 dirty/untracked 文件，**不得 reset、clean、覆盖或混入提交**。若存在 `.codegraph/`，按仓库指引先使用 CodeGraph 理解引用。

**优先只读复核本地已有证据，禁止自动连接服务器开展新的业务测试；不再执行 R2/P5500 或 R7。** 不启动真实 SMSC、生产系统或真实号码，不执行正式部署、切换路由或生产发布；也不修改 `/opt`、系统 Python、全局 pip、systemd、VPN 或系统 core 策略。证据不足要明确标为 `EVIDENCE_GAP`，不得凭报告值补造机器记录。未经用户明确批准，**不执行 `git commit`、`git push`**。

最终提交一份**可复核的当前 VNext Loopback 验收决定**和一份**下一步受控发布准备检查表**；历史失败与尚未证明的原生根因必须保留。非阻断问题不再引出新的项目阶段或不必要的重构。

## 1. 已知事实与不得误判的边界

### 1.1 当前新增正向证据

- 最新 R2/P5500 使用 EulerOS 2.0 SP15 x86_64、非 root `paas`、原生 ext4、standalone CPython 3.11.5、Loopback SMSC。
- 本轮门禁 `--gates r2 --authorize-p5500`，4 Campaign：`2500 + 2500 + 250 + 250 = 5500`。
- Route TPS `80`、response delay `2 ms`、4 Workers、64 partitions、projection queue `128`、max inflight `32`、prefetch `16`、Watchdog enabled。
- 最近一次有证据的 R2 结果：`R2_RESULT=PASS`，`accepted=5500/5500`，Projection `unique/taken/socket_written=5500/5500/5500`，Perf Guard `PASS`、`exit=0`、`85.542 s`、RSS `585,560,064 B`、磁盘增量 `0 B`、已记录 PID 均退出、无本轮 `SIGSEGV`。
- 测试工具曾把 `urllib` timeout 固定为 `30 s`，造成一次 R2 长轮询 `TimeoutError`；提交 `a3ea4ab617c39bd3d0c5740c2396ef047f8ebc61` 将客户端 timeout 改为 `300 s` 并增加单测。注意这是**测试工具修复**，不是声称历史所有 Native 故障根因均为超时。
- 最新追加报告已在提交 `01409126a5c315ee91696c135db52a5c5ece2287` 发布，**只新增报告文件**。
- 独立 EulerOS B2 `300/300`、`42 s` 已 PASS；此前 EulerOS R7 约 10 分钟 `4000/4000` 已 PASS；不要重复相同测试。

### 1.2 历史必须保留的负向事实

- 历史 D7/R2/P5500 确实多次出现 guarded command `SIGSEGV (-11)`，包括一次明确记录的 `34.361 s` 崩溃；没有 Native core / faulting frame。
- `RCA_LEVEL=RCA_STRONGLY_INDICATED`；不能因为这次 R2 PASS 升级成 `RCA_PROVED`，也不能改写历史 D7 的失败事实。
- 旧集成架构历史 `INTEGRATED_NO_GO` 与 VNext 的当前合成 Loopback 验收是两个不同范围；不能用一方结果改写另一方。
- **Loopback PASS 不是实际 SMSC、生产号码、原生 SLES12SP5 机器、长期生产运行或自动正式上线授权。**
- `socket_written` 是 VNext 主链业务发送阶段完成；`submit_sm_resp_success` 属异步协议响应证据；DLR / 终端送达 / 用户收到短信不是本次验收内容，禁止混淆。

## 2. 冻结材料（按此顺序读取）

1. `AGENTS.md` 与最新 git commit / diff / worktree 状态。
2. `Docs/Marketing_Copilot_VNext_EulerOS_R2_300s_Followup_Acceptance_Report.md`。
3. `Docs/Marketing_Copilot_VNext_EulerOS_Targeted_Native_Crash_Verification_Report.md` 和对应 Evidence Index。
4. `Docs/Marketing_Copilot_VNext_Static_Audit_and_Native_Crash_RCA_Report.md` 与 `Docs/Marketing_Copilot_VNext_Crash_Diagnostics_and_AB_Evidence.md`。
5. `Docs/Marketing_Copilot_VNext_Native_SIGSEGV_RCA_and_Runtime_Stabilization_Report.md`、`Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`、`Docs/Marketing_Copilot_VNext_SIGSEGV_Evidence_Index.md`。
6. 源码 `backend/tools/vnext_runtime_canary_acceptance.py`，`backend/services/vnext_service.py`，`backend/routers/vnext.py`，`backend/vnext/{runtime,spool,worker,projection_writer,smpp_transport}.py`，`deploy/{build-sles12sp5-system-python-manual,build-vnext-runtime-canary-test-bundle,vnext-runtime-canary-test-runner}.sh`。
7. 本地归档优先查找（不要猜测其存在）：
   - `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/r2-evidence-sanitized-300s.tar.gz`
   - `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/marketing-copilot-vnext-runtime-canary-test-20261017-vnext-euleros-targeted-r2-300s.tar.gz`
   - `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`
   - `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`
   - 以及对应 `.sha256`、manifest、收据、原始 `canary.json`（如果精简包确实包含）。

> GitHub 仓库不包含这些运行归档，不能以 GitHub 上的 Markdown 报告取代本地字节级证据检查；若某份归档不存在，保持报告引用、标为 `NOT_LOCALLY_AVAILABLE`，不得声称已完成本地复核。

## 3. 阶段 E0 — 提交与包身份冻结（必须）

- `git rev-parse HEAD` / `git rev-parse origin/main`；比较是否为 `01409126...`。如果工作区新提交继续前进，只记实际 SHA，审计相关变化，不强制 reset。
- 确认 `a3ea4ab6` 只改测试工具 timeout 与单测，`01409126` 只新增验收报告；记录相应 blob SHA 或 git diff 摘要。
- 对提交前已有 dirty/untracked 做只读清单，保留用户文件。
- 审查此次服务器真正运行的三包身份，报告中的预期 SHA256：

| 对象 | 预期 SHA256（按实际归档复核） |
|---|---|
| 正式应用包 | `bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44` |
| standalone CPython 3.11.5 | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` |
| 300s 测试包 | `f1d59bf5e6817c8915a754eca53d73a4cb8c8cbce175f666a8a191d35d5f6e9a` |
| 300s sanitized R2 evidence | `39e918deb75f4b5659d20976c6f17816e0299389c3fb36168e7c8bec52521013` |

- 检查归档成员的绝对路径、`..` 路径逃逸、符号/硬链接、内部 manifest/hash、是否混入数据库/WAL/Spool/Journal/logs/credentials/测试工具。正式应用包与测试包严格分离。
- 特别核对 `release.json` 的来源：既有正式包记录 `8f14e82d-dirty`，本次实际运行包不能未经审计宣称“精确由当前 clean main 构建”。区分：`TESTED_PACKAGE_IDENTITY_VERIFIED`、`SOURCE_PROVENANCE_EXACT` 与 `BUILD_REPRODUCIBLE` 三种不同事实。若确有版本来源差异，列为发布阻断事项，但不否定已经完成的测试运行本身。
- 只读归档校验使用隔离临时目录和安全解包，**不自动删除原件、历史 run 或任何业务目录**。

**E0 输出：** 表格列出文件路径、实际哈希、manifest 结果、源码身份、证据来源及 `VERIFIED / MISMATCH / UNAVAILABLE`。

## 4. 阶段 E1 — R2/P5500 全链路、全 Campaign 严格对账（核心）

从 R2 当前真实机器证据（如有 `canary.json` / JSON guard / phase / projection summary），按如下字段输出四 Campaign 的表：

| Campaign ID | expected | ingress accepted | socket_written | Loopback received | submit_sm_resp_success（若分 Campaign 可得） | Projection unique/taken/written | pending | duplicate/missing/unexpected | 证据路径 |
|---|---:|---:|---:|---:|---:|---|---:|---|---|
| 1201 | 2500 | | | | | | | | |
| 1202 | 2500 | | | | | | | | |
| 1203 | 250 | | | | | | | | |
| 1204 | 250 | | | | | | | | |
| TOTAL | 5500 | | | | | | | | |

审计准则：

- **检查执行配置真实性：** requested gates = executed gates = `r2`；`--authorize-p5500`、Route TPS 80、response 2 ms、4 workers、64 partitions、projection queue 128、inflight 32、prefetch 16；不能只引用命令行，还应比对 result config、SMSC/route 有效配置、guard 记录（存在时）。
- **整体验收：** `R2.status=PASS`、四 campaign 逐项 PASS、expected=accepted=5500、Projection `unique=taken=socket_written=5500`、pending=0、Guard exit=0、无 SIGSEGV、无残留进程。
- **Loopback/响应验收：** 从 R2 `loopback.submit_sm_received`、`submit_sm_resp_success` 和每 Campaign `loopback.expected/received/duplicates/missing_count/unexpected_count` 核验；若报告没写但 JSON 有字段，从 JSON 补入最终报告；若证据完全缺失，填 `EVIDENCE_GAP`，不得凭 Projection 数量推断响应成功。注意长短信多 PDU 时 task 与 PDU 数可能不相同；此场景若全部是单 PDU 才可做数值一一相等比较，需依据请求与 SMPP 配置说明。
- 检查协议层 `bind`、response error、未知 sequence、send result unknown/write_failed、`taken_without_socket_written`。缺字段标注 `NOT_RECORDED`，不可写成零。
- 明确这次 R2 是**按 Campaign 顺序提交、每 Campaign 批量 250 条，并等待 Idle**的受控合同，不能写成所有 5500 条同一时刻竞争的并行 4 Campaign 压测。
- 区分吞吐：`5500 / 85.542 ≈ 64.3 tasks/s` 仅是包括 setup/shutdown 的 guard 有效观测比例；不能作为独立 SMSC TPS SLA，也不能推导真实 SMSC 容量。原始 Loopback 80 TPS 是限速设定，不保证端到端必达 80 tasks/s。
- 若有多个候选 result JSON，按 Run ID、时间、包哈希和 guard PID 识别本次授权重测，避免把第一次 30 秒超时的失败数据合并进新一次 PASS。

**E1 通过条件：** 所有关键数量由机器证据支持，无法支撑的字段保留 gap；不得用推断值填 0 或 PASS。

## 5. 阶段 E2 — 归档、安全与历史失败的并行证据链

构建短小时间线，各自独立：

1. **历史失败**：EulerOS D7/R2/P5500 多次 `SIGSEGV(-11)`，特别是 34.361 s D7，Native faulting frame 不存在。历史 verdict 不改。
2. **当前代码稳定化**：`spawn` 统一、Journal durable replay、Worker control、Watchdog false-stall、Idle O(total) 扫描改为 lightweight；F02 A/B 1000 次查询约 102× 成本降低（不是 SMS TPS ×102）。
3. **本轮首次新 R2**：首个 Campaign 2500 全部发送与投影，但验收工具 urllib `30 s` 触发 `TimeoutError`，guard exit=2；**不是 SIGSEGV**，不记为业务丢失或原生崩溃。
4. **本轮授权修复后重测**：urllib `300 s`、全新独立 Run、R2 5500 PASS、guard 85.542 s / exit 0、无崩溃。
5. **R7 历史 PASS**：EulerOS 10 分钟 4000/4000、资源有界且完成；不要把 4000 长稳和 5500 容量假装来自同一个连续测试或相同提交。

保留：

```text
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
HISTORICAL_D7_R2_NATIVE_FAILURE=PRESERVED
LATEST_EULEROS_R2_P5500=PASS    # 仅在 E1 数据核验满足时
LATEST_EULEROS_R7=PASS          # 指向准确的历史版本、run 和证据
```

不能写“修复 300 秒超时证明 DuckDB/C++ SIGSEGV 根因已解决”；无法证明 Native 根因时，也不能把当前新证据 PASS 错写为历史 FAIL 的覆盖或反向否定。

## 6. 阶段 E3 — Canary 状态重评与范围边界（必须形成决定）

不得一开始就预设总状态 PASS。先判断 E0/E1/E2 是否通过。采用分层而不是一个模糊的 `READY`：

**Layer A：当前合成 Loopback 的 VNext Runtime Canary**

- 若原始机器证据、4 Campaign、Guard、清理和完整 Loopback/Projection 均可核验：`CURRENT_VNEXT_LOOPBACK_CANARY=PASS_VERIFIED`。
- 若证据只支持报告中的聚合数但 Loopback 响应明细缺失：`CURRENT_VNEXT_LOOPBACK_CANARY=PASS_REPORTED_EVIDENCE_GAP`（明确缺哪些字段）；不得悄然提高等级。
- 若哈希错误、合同不符、数量不对账或 Guard 不通过：`CURRENT_VNEXT_LOOPBACK_CANARY=NOT_VERIFIED/FAIL`，给出具体原因。

**Layer B：Native 历史根因与稳定性剩余风险**

- 历史 `SIGSEGV(-11)` 继续保留；`RCA_LEVEL=RCA_STRONGLY_INDICATED` 与 `NATIVE_FAULTING_FRAME=UNAVAILABLE` 原样保留。
- 最新测试是强正向恢复证据，不代表对所有并发时序、重复运行或底层 C++ 函数级 RCA 的证明。

**Layer C：发布和生产 readiness**

- 可用状态：`RELEASE_CANDIDATE_DOCUMENTED`、`LOOPBACK_CANARY_VERIFIED`、`PRODUCTION_APPROVAL=NOT_GRANTED`。状态字符串若项目已有标准字段，优先兼容现有标准，不要无必要创造另一套状态机。
- 旧工程的 `INTEGRATED_NO_GO` 属不同冻结架构、历史判定，保持不变；不能用它阻止新的独立 VNext 合成门禁正常记录，也不能因为 VNext PASS 将其改写为 PASS。
- 对既有 `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`，在**新报告**中明确解释：这是截至历史 D7 的旧冻结状态；新一轮有更近期 PASS，但应如何按门禁定义更新**当前**状态，要由证据与原合同一致性决定。只更新新/current verdict，不覆写旧报告历史字段。

输出一个简洁的 decision matrix：Scope、source baseline、evidence run、evidence grade、current verdict、unresolved risk、deployment authorization。

## 7. 阶段 E4 — 真正发布前必须完成的最小准备清单

只评审、准备，不部署。优先列出阻断项目（P0）和非阻断建议（P1）；不得把安全官僚化或无关功能加入门禁：

- **源码/包一致性**：最新 clean source SHA、正式应用包 `release.json`、wheelhouse、standalone Python/SLES ABI 与本次受测包建立 hash traceability；如正式包为 `8f14e82d-dirty`，制定未来从 clean baseline 重建并校验的步骤。测试包不得混入正式包。
- **环境有效性**：EulerOS 是 SLES12SP5 兼容代理，不等同于实机 SLES 验收；内核、glibc、wheel、`_ctypes`、ssl、pip check、离线启动均需明确已验证范围。无需无证据的系统升级。
- **真实 SMSC 差异**：SMSC 地址、认证、Bind Transceiver、窗口、TPS、重连、submit_sm_resp、超时、编码/长短信/网络策略等在实际现场尚未经验证；未来执行前需单独授权并使用允许的号码/路由。不要自动连接真实 SMSC。
- **部署生命周期**：默认 `MC_PIPELINE_MODE=vnext` 及可控 legacy 回退定义、状态/监控、任务暂停与取消、Spool/Journal/Projection 数据持久性、Worker/Writer 崩溃恢复、正确停机和双 owner fence。明确当实时链路报错、SMSC 抖动或磁盘不足时的停止规则。
- **资源与存储**：正式环境磁盘容量评估、预留空间/清理策略、只在项目 Run Root 留痕；延续 max 4 CPU / 2 GiB RSS / 5 GiB 测试数据限制作为**共享测试服务器条件**，不误写为生产可承诺的 SLA。
- **发布批准边界**：只建议进入“受控现场评审/小流量真实 SMSC 专项”准备，不得自动声明生产上线通过；实机/真实 SMSC/数据保护是否需要额外批准由用户决定。

输出按 `Must-have before real SMSC` 和 `Not required for current Loopback acceptance` 两栏组织；避免再次提出 52K/102K、严格吞吐 SLA、原生 Beeline 等与本任务无关的老门禁。

## 8. 阶段 E5 — 交付物与验收标准

在 `Docs/` 下创建或更新**当前独立的新报告**，不要覆盖历史报告的结论：

1. `Docs/Marketing_Copilot_VNext_Runtime_Canary_Final_Acceptance_and_Release_Readiness_Report.md`  
   结论前置；冻结源码与包；R2/R7/Lifecycle 验收矩阵；各 Campaign 和 SMPP/Projection 对账；历史 Native 故障保留；当前分层状态；生产 readiness 检查表；未执行项。
2. `Docs/Marketing_Copilot_VNext_Final_Acceptance_Evidence_Reconciliation.md`  
   给出本地每个证据文件、哈希、内部 manifest、来自机器 JSON 的 JSON path/字段名、数值和证据强度；缺字段如实列出。
3. 如方便机器使用，生成一个简洁的 JSON receipt 到隔离证据目录（**不纳入 Git 运行数据**）：当前 source/package SHA、各 gate counters、archive hash verdict、EVIDENCE_GAP 列表、当前分层状态。

验收检查：

- [ ] 当前 `main` SHA / working tree 审计完成，无关内容未触碰。
- [ ] R2 精确 4 Campaign 对账，Loopback 收件和 `submit_sm_resp_success` 有据可查，missing/duplicate/unexpected 单独记录。
- [ ] guard exit、85.542 s、RSS、磁盘、进程清理有机器证据，未把报告代替原始机器数据。
- [ ] 合同确认为单 Campaign 依次执行的 5500 测试，没有虚构四 Campaign 同时竞争。
- [ ] R7 / 历史 D7 按准确版本和时间单独保存，`SIGSEGV` 历史不改写。
- [ ] 当前合成 Loopback verdict 与 Native RCA 与生产授权三个范围彼此独立。
- [ ] 发布前阻断条件具体、可操作，不新增大架构或无关测试。
- [ ] `git diff --check` PASS；只提交任务相关文档（及必要的极小证据解析工具，若确需）。
- [ ] **本任务未执行新的 R2/P5500、R7、实际 SMSC、正式部署、生产变更；未经授权不 commit/push。**

### 若证据不足怎么办

如果本地没有 300 秒精简证据归档，只允许：复核 GitHub 报告、记录 `NOT_LOCALLY_AVAILABLE`、给出缺失的精确文件名与 SHA。**不要为了凑结论自动连接服务器或重新跑 5500 条测试**。若用户以后单独授权只读回传，可在那个任务中补上字节级核验，而非本任务自动进行。

## 9. 最终 Codex 回复格式

回答应包含：实际基线 SHA、是否完整读取 R2 evidence、R2 Loopback/Projection 全字段对账结论、历史 SIGSEGV 状态、**分层的当前验收结论**、生产发布是否授权、文档路径、`git diff --check`、是否 commit/push。简明说明遗留 `EVIDENCE_GAP`；不宣称未做的服务器测试或生产验证。

---

**工作指导语：** 本轮的目标是“用现有有效证据，把 VNext 受控 Loopback 工程验收从反复压测转成明确可追溯的版本化放行判断”。尊重最新 5500 PASS，也尊重历史 SIGSEGV；没有新的函数级 Native 证据就不写 `RCA_PROVED`。避免在已经证明有效的分片 + Spool + Async SMPP + Journal + 独立 ProjectionWriter 主链上做额外重构。
