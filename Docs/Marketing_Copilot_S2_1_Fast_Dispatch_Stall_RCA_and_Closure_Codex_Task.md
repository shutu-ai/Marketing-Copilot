# Marketing Copilot — S2.1 Fast Dispatch Stall RCA & Correctness Closure

**任务 ID：** `MC-SCHED-S2.1-FAST-STALL-CLOSURE-20261015`  
**阶段性质：** S2 专项阻断修复；不是另一次 S2 全量重构  
**推荐 Codex 模型：** `gpt-6.1-sol`，`model_reasoning_effort="xhigh"`（复杂并发 RCA 与核心修复）；常规测试执行可手动切至 High/经济模型  
**远端基线：** `f37857337f78bfba3ba6665a0e7c1de6fa1cd117`（开始前仍须核对当前本地与远端 HEAD，若已前进则检查增量差异，禁止盲目 reset）

## 0. 背景和最终目标

首先完整阅读：

1. `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`
2. `Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md`
3. `Docs/Marketing_Copilot_S1_Bounded_Dispatch_Implementation_and_Acceptance.md`
4. `Docs/Marketing_Copilot_S1_1_Result_Pipeline_Hard_Bytes_Closure_and_Acceptance.md`
5. `Docs/Marketing_Copilot_S2_Route_Slot_Socket_Fairness_Implementation_and_Acceptance.md`
6. 仓库的 `AGENTS.md` 及对应源代码、测试和验收 harness。

**已知失败：** S2 fair + `serialized/fast`、10 files × 100 tasks、pool=4、window=50、route TPS=200、task workers=8、ingest=2、copy=2、DLR disabled，在 300 秒门禁下仅 **900/1000 accepted**、900 socket success、**100 queued**，unknown/partial=0；因此 **S2_NO_GO**。历史 `serialized/reliable` 3/3 通过，但最终源码存在后续审计字段修改，不能直接当最终 SHA 的 G3 通过。

**本任务目标：** 对 `serialized/fast` 的最后 100 queued 进行可审计的根因收敛和最小正确性修复，确保不破坏 S0–S1.1 已闭合的安全合同，为后续 **S2.2 完成四组合 G3 与 G4** 提供稳定候选源码。不得将本专项通过自动标记为 `S2_READY`。

## 1. 阶段 A — 冻结证据并重建故障链

- 在 Windows 工作树检查 `git status --short`、`git rev-parse HEAD`、`git rev-parse origin/main`、未跟踪与脏文件；尤其保护 `frontend/package-lock.json`、既有运行数据和用户材料。只在隔离 WSL AlmaLinux-8 的 **ext4** 目录创建全新源码快照及 DB/Journal/端口/Watch/日志，禁用对 loopback 的代理，不连接真实 SMSC/号码。
- 优先查看现有 `.s2-evidence-20261015/g3/serialized-fast-r1.json`、相应 `smsc.events.jsonl` 和（若仍存在）`/tmp/mc-s2-g3-final/*` 的原始证据。核对 SHA、manifest、运行参数，不存在的证据标记 `MISSING_ARTIFACT`，不能编造。
- 将失败现象拆成两段：`1000 created` 到 `900 socket` 的 **admission/dispatch** 差集，以及 `900 socket` 到 `900 accepted` 的 **result** 对账。定位最后 100 个 `queued` 的实际 `campaign_type/id`、route、`next_attempt_at`、eligibility、age、reason（使用合成 ID，不暴露真实号码/正文）。
- 对相同最终源码和原始参数执行至少一次受控 `serialized/fast` loopback 复现。使用独立进程、独立数据和严格 300s 截止；若 30s 没有有效 socket/task 进展，**立即采样诊断但不直接判 PASS/延长截止**。采集线程栈、最新业务进展和各阶段资源账本。必要时做严格受控的单变量对照（例如 fair reliable/fast、pool1/4），**不得借对照结果修改正式门禁参数**。

### 必须采集、逐层核对的状态

1. `sms_tasks`：queued、retrying、sending、accepted 各数目、task_date/活动窗口、generation 与真实 due 条件；是否有 100 个任务始终符合 Claim 资格。
2. `fair_dispatch_candidates` / `fair_scheduler_state`：窗口位置、group 轮转、实际候选 ID、zero-candidate 原因、SQL/事务等待时长，是否出现窗口饥饿或热点 DB 事务。
3. `ReadyPool`：per-group/global 任务数/parts/bytes、reserved、ready、prepare、handoff、result_pending、released、oldest age、blocked reason、`acquired-released=live`，是否有 credit 泄漏或归还滞后。
4. `result_capacity_service` + Journal/Write Buffer：normal/emergency/terminal 的实占字节与所有权、pending/replay/quarantine/rejected、Fast checkpoint pending、`result_pipeline_hard_bytes=VERIFIED` 是否持续成立。
5. `task_worker` 与 Fast：实际 claim 次数、claimed/published 差集、Fast batch/claim token/attempt、checkpoint flush 节奏、Fast accumulator 是否导致结果债务无法释放。
6. `smpp_gateway` Route/Slot：route revision、shallow handoff、sender 活性、per-PDU used/unbound/reserved、每 Slot 实际 `submit_sm`、Slot 有无可用 credit、`_fair_served` 与 group 选择是否饥饿。
7. Backpressure/fail-closed：是否全局停止新 admission，准确理由是结果风险、磁盘、DB 维护、写缓冲、还是无资源；全局安全关闭不得因追求 1000/1000 被绕过。
8. 线程锁与 epoch：核对 campaign gate → Slot send lock、PDU ledger、ReadyPool condition、DB owner/cursor、journal writer 的等待链；不能仅凭 100 queued 猜测“发送速度慢”。

请输出从持久输入 → eligible → candidate → reserved → claimed → ready → handoff → socket → response → Journal/DB terminal 的**逐级计数守恒表**和时间线。对不能直接取得的观测，增加有限、脱敏、可关闭的诊断；禁止引入每任务高基数时序指标或无界日志。

## 2. 阶段 B — 根因收敛及最小修复

在有实际证据后选择最小修复，优先核对下面的候选机制（**均为假设，不是既定根因**）：

- Fast 模式 checkpoint/结果债务与 S1.1 parent/child result credit 释放的时机不一致，导致新的 Claim 长期无法 admission；
- Fair candidate keyset 游标、分组轮转、`next_attempt_at` 或活动资格过滤，使最后一个 group/批次被跳过；
- `ReadyPool` reservation 完成/重复 callback/stop/空转路径 credit 未释放，出现永久满；
- Gateway Route/Slot idle/handoff/DRR 账没有真正归还资格，任务虽 eligible 却长期无法取得 Slot；
- Fast batch 的 payload/claim、checkpoint 提交与 Write Buffer/Journaling 的状态误判；
- 单 DuckDB native owner 或锁等待引发数据读取/claim 长时间无进展；
- scheduler/filter/claim 其他路径（以最终证据为准）。

修复要求：

- **不允许**靠提高 300 秒 timeout、并发 Worker、SMPP TPS、queue/window、临时扩大 credits 或自动重发 unknown/partial 来掩盖根因。
- 保持 S0.1 单 native DuckDB owner + cursor 生命周期、完整 OCC 事务重试、R02/R03/R17 停止及 DLR 安全栅栏、S1/S1.1 的字节硬限制与 fail-closed。
- 不修改外部消息幂等语义，不让已写/不确定 submit_sm 再次整条发送；Fast 与 Reliable 均保证 attempt/part/Journal 证据、generation fence。
- 不增加第二数据库 RW owner，不引入新的消息队列/第三方服务，不进行 S3/S4 文件 Worker 扩容和长期恢复重构。
- 如果证明是业务的**合理限流而不是缺陷**，必须给出资源预算、进度、等待界和与冻结 300s 门禁冲突的客观说明；不能单纯将门禁改为 PASS。
- 以小规模可复现测试先锁定 bug，再做精确代码更改；运行修复前失败、修复后成功的 deterministic regression，严格核对修复前后行为。

## 3. 阶段 C — 当前最终源码专项验证

**先冻结修复后的源快照（完整 SHA + tracked/untracked 检查 + manifest），所有正式 PASS 都来自这一份最终源码。任何功能或诊断代码后改均需重跑受影响门禁。**

### 最低验收

- 新增针对上述根因的确定性、可重复、不会依赖等待运气的回归测试。测试需在旧失败条件下失败，在最小修复后通过。
- `serialized/fast`、fair、正式同窗口 `stress-1k`，**至少三次独立新进程**，每次新 DB/Journal/Watch/port/artifacts，均要求原来 300s 门禁内完整 1000 tasks/attempt/part/socket/response/Journaling 守恒；不允许 900/1000 被误判 PASS。
- 用冻结的完全相同输入与硬件设置做 `serialized/reliable` 最低一次回归，必要时补充 `occ/fast` smoke；作为交叉影响检查，**不把这些专项运行冒充 G3 的四组合×3 正式门禁**。
- S2 PDU Window 与 handoff 确定性回归、S0/S0.1/S1/S1.1 关联回归、后端全量 `pytest`、前端 `npm run build`。历史 `342 passed / 2 skipped` 是对照，不是必须凑出来的数字；以当前实际用例数量/失败情况为准。
- 完成每次 run 的独立 DB 审计：1000 expected key ↔ task key、每个 task/attempt/part 与实际 socket submit/response、文件/chunk、Journal event/DB transaction、unknown/partial、不安全重发、credit acquired/released/live、native SIGSEGV、timeout/no progress。
- 保存所有失败运行、日志、线程栈、source manifest、环境和原始审计；禁止只保留 PASS 摘要或删除已有失败证据。无需连接真实 SMSC。

### 失败时的处理

- 任一 `serialized/fast` 重跑 FAIL、永久 queued、资源泄漏、重复 socket、错误终态、结果丢失、fail-open 或硬字节回归 ⇒ `S2.1_NO_GO`。记录新的具体 RCA、证据和阻断项，不允许升级成 `S2_READY`。
- 可复现环境故障与业务失败分类；单个偶发用例重跑 PASS 不自动覆盖先前 FAIL，按 root cause/最终源码重新判门禁。
- 本阶段只要求使 serialized/fast 的 G3 阻断项可稳定关闭；完整四组合 × 3、G1 matrix、heavy multipart continuation/yield、G4 socket fairness/watchdog 仍是后续 **S2.2** 的工作。即使专项全部通过，本任务最高结论也只能是 `S2.1_READY_FOR_S2.2`。

## 4. 阶段 D — 报告和交付

请生成：

`Docs/Marketing_Copilot_S2_1_Fast_Dispatch_Stall_RCA_and_Closure_Report.md`

包含以下内容：

1. 最终 SHA、环境、配置、源 manifest、旧失败证据与本次可复现结果；
2. 1000 → 900 的逐层守恒差集与时间线，定位是否完全处于 Claim 前或跨多个阶段；
3. 明确的 **root cause**、可证事实 / 推断 / 未证边界；修复前后最小对照；
4. 修改文件与关键函数、为何属于最小修复、兼容/回滚风险；
5. 单元、专项 3× 1K、Regression、全量测试和 Frontend build 的原始日志/审计路径；
6. `result_pipeline_hard_bytes`、`pdu_window_hard_limit`、`dispatch_fairness`、`socket_fairness` 的实际状态；
7. 仍未执行的 G3 四组合×3、G4 multipart、真实 SMSC、SLES12SP5、S3/S4 等完整列表；
8. 最终判定：**`S2.1_READY_FOR_S2.2`** 或 **`S2.1_NO_GO`**，同时明确 **`S2_READY` 仍为 NO**（除非另行完成完整 S2 硬门禁，不得偷换）。

最终仅简要向用户报告：RCA、修复摘要、3× 1K 结果、回归、剩余阻断项、下一阶段建议。

## 5. 约束与 Git

- 默认 `MC_CAMPAIGN_DISPATCH_MODE=legacy` 不变；Fair 继续 opt-in。
- 使用 `AGENTS.md` 规定的 CodeGraph 优先策略（若有 `.codegraph/`）；禁止复制/上传真实手机号、凭据及消息正文。
- 保持既有脏工作区和用户未跟踪资料原样，不 `git reset --hard`、不 `git clean`、不清理旧 Journal/DB/失败证据。
- 只在隔离副本运行测试；不要启动生产服务器或实际发送。
- 不需自动提交/推送 Git，除非用户另行明确授权；允许将修复代码及报告保留在工作树。若确认得到授权后提交，仅包含本阶段源码、测试和报告。
- 不把 S2.1 的修复扩大成与本阻断无关的全系统重构。
