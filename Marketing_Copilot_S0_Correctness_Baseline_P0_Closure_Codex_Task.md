# Marketing Copilot — S0 正确性基线与 P0 风险关闭（Codex Task）

**任务 ID**：`MC-SCHED-S0-CORRECTNESS-20261015`  
**推荐 Codex 模型**：`gpt-6.1-sol`，`model_reasoning_effort="high"`  
**仓库**：`https://github.com/shutu-ai/Marketing-Copilot`  
**设计提交基线**：`e50ac1528ff2e6efb26b472806ed5e8cf3afba11`  
**任务定位**：这是 Phase 1 设计报告中 **S0** 的实施任务，不是 S1/S2/S3 大规模队列重构任务。

## 0. 必须先阅读并确认的材料

1. `Docs/Marketing_Copilot_Phase1_Architecture_Assessment.md`：重点阅读 §1、§2、§4、§5.5、§5.6、§7 S0、§8 T01/T02/T03/T06/T11。
2. `Docs/OCC-Ingestion-State-Consistency-Task-Summary.md`：不得继承任何旧版 G3 PASS；当前 **G3 = NO_GO / NOT_VERIFIED**。
3. `Docs/OCC-Equal-Window-2x2-G3-G4-G5-RCA.md` 及相关验收记录（如存在）。
4. 现有测试：`backend/tests/test_occ_ingestion_consistency.py`、`test_realtime_progress_batches.py`、`test_fast_send.py`、`test_multi_campaign_queue_isolation.py`、`test_sms_result_recovery.py`、`test_startup_recovery.py`，以及工具 `backend/tools/local_e2e_acceptance.py`。

实际代码与工作树高于文档推断。首先记录 `git status --short`、`git rev-parse HEAD`、依赖版本、Python/OS、启用的发送模式和并发配置。不得覆盖已有未提交文件。若 HEAD 已在设计提交之后有变化，以实际 HEAD 为冻结测试基线，列出差异，不退回旧提交、不强制 reset。

## 1. 目标与边界

**目标**：用当前源码复现并关闭 Phase 1 的四个 P0（R01、R02、R03、R17），以最小必要修改建立可复验正确性基线；完成独立 ingest-only、sender-only、以及至少一次当前源码 OCC+Fast 10 文件×100 有效任务的端到端守恒对账。

**不做**：S1 的活动×路由有界 ready pool / credits；S2 的 Slot 公平调度和逐 PDU window 大改；S3 的文件/短信 Worker 全面扩容；S4 的完整恢复/600 秒长稳；架构外的功能开发。不要为了消除卡顿直接无限增加队列、timeout、线程或重试次数。

持续保留：单服务进程、单 DuckDB RW Owner；既有 API 和任务状态兼容；历史数据、Journal、attempt、part 和原始日志不可清空；没有证据不得承诺 exactly-once。**不使用真实 SMSC、不向真实手机号发送消息。**

## 2. 先复现、后修复：四类 P0

### R01 — OCC+Fast 不收敛与 G3 缺口

- 冻结当前源码、依赖、场景和测试环境；执行当前测试，区分环境错误与业务失败。
- 在**全新隔离的测试目录及数据库/Journal**下，重现历史 OCC+Fast 停顿、进度不收敛、非法状态或缺失记录。
- 记录每个失败的最小触发条件、操作/线程栈、活动/文件/chunk/task/attempt/part 权威状态、DB/OCC/Journal 水位。不能将“没有产物的中止运行”判为通过。
- 只修复已复现的直接正确性缺陷；优先缩小改动范围，修复后对相同最小场景和相关测试复测。

### R02 — 非法 chunk 状态与权威摄取一致性

- 检查 completed 文本的宽松判断及 prepare/complete 路径不一致；构造确定性非法值注入。
- 区分底层并发导致的真实非法值与手工注入，保留原始值、来源及可复现步骤，不擅自归因 DuckDB。
- 未能证明已完成业务提交时 fail-closed 或 quarantine；不能因为状态字符串包含 `completed` 就跳过关键 token/权威检查。
- 断言同一个业务键仅有一个 durable task，chunk 与 task 同事务，旧 lease/旧 token 不可回退终态或重复累加统计；`processed_files` 仅为可修复投影，不能影响权威摄取与重复发送。

### R03 — Reliable 模式停止/暂停竞态

- 用可控 barrier 注入：authorize 后、throttle 等待中、socket lock 前、multipart 两个 part 之间，与 pause/cancel/end/window 结束竞争。
- 与已有 Fast socket gate 对照，按实际锁顺序做**最小安全修复**：已返回成功的停止/暂停请求之后，不应有新的 `submit_sm` 写入；已发/不明请求须保存 attempt/part 证据，不假装被撤销。
- 检查时间窗口/活动 generation/claim fence 的时点；不得在持有 DB 锁或 campaign gate 时无限等待节流、响应或外部网络；不得影响其他 Campaign 的健康发送。
- 未能完成全模式统一安全合同则记录阻断，不能将 R03 标记 CLOSED。

### R17 — DLR 重新开启不安全重发路径

- 针对 `sms_task_service.py` receipt 路径构造**确定性**用例：task `unknown`/`partial` 受到 UNKNOWN、UNDELIV、EXPIRED 等负 DLR 后，不得变为可以整体重新 Claim 的 `retrying`；ENROUTE/正 DLR 不得抹掉未解决 part 的 partial/unknown 证据。
- 关联 receipt 到正确 namespace、attempt、part/message binding；迟到/重复 receipt 不得覆盖新 attempt 或双计。
- 只在可以证明**整条尚未对外写出**或确证全拒绝且适合重试时，才允许受控重试；不把负 DLR 当作“未发出”证明。
- 补覆盖旧 attempt / duplicate DLR / mixed multipart / partial / unknown 的矩阵测试；不要通过粗暴禁用全部合法重试来掩盖问题。

## 3. 测试计划与验收门禁

### G0 — 当前源码标准回归

- 后端全套 `pytest backend/tests -q`；前端 `npm run build`（从 `frontend` 目录）。保存实际命令、SHA、结果、失败列表。
- Windows pytest 临时目录若有 ACL 问题，使用**新建隔离 basetemp**并禁用 cacheprovider 复跑；不删除或修改已有临时目录。可转 WSL `AlmaLinux-8` 的 ext4 独立工作副本、CPython 3.11 进行核实。
- Fixture 初始化失败应标 `BLOCKED_ENV`，不能混入 `PASS`；代码修复后以**最终源码**重新运行相关门禁。

### T01 — 独立摄取（ingest-only）

- 新建 10 文件×100 有效行的固定 manifest，包含重复行/源 hash、旧 lease、镜像延迟/删除等边界。
- 验证预期 1000 个**唯一 durable task**，权威 file/chunk 完成，零真实 socket 发送。以业务键集合差集、状态转移和 checkpoint 计数对账，不只比较合计数。

### T02 — 状态、事务和 P0 注入

- serialized 与 OCC 两种配置：并发 file worker/checkpoint、旧 token、非法状态、OCC 事务冲突、R03 的 stop fence、R17 的负 DLR/partial/unknown。
- 明确外部 I/O/owner 错误不能伪装成 OCC 可重试错误；完整事务重试有限；task/attempt/part/event 不被重复创建或不安全回退。

### Sender-only / 发送证据

- 使用**隔离的 loopback/mock SMSC**和预生成任务，验证至少 pool=1 与 pool=4；保存真实模拟 socket `submit_sm`、回执、attempt/part、Journal 证据。
- 对单 part 与 multipart 分开统计：1000 task 不意味着任意场景有 1000 个 PDU；DLR disabled 不等于交付成功。

### OCC+Fast 1K 端到端（S0 最低完成门槛）

- `stress-1k`，10×100，明确 `--expected-tasks 1000`；`--gate full --send-mode fast`；OCC 模式，`MC_DUCKDB_THREADS=1`，pool=4，SMPP window=50，SMSC TPS=200，task workers=8，ingest workers=2，copy workers=2，slot queue=2000，DLR disabled。根据工具当前 CLI 名称核对并设置所有选项，不能假定 scenario 自带 TPS=200。
- 每次新进程、隔离 DB/Journal/Watch 与全新结果目录；持续至明确定义的终止条件。保存 `result.json`、事件/提交对账、线程栈（停顿时）、配置快照及 SHA。只允许模拟环境发送。
- 成功要求 1000 个预期唯一业务 task 完整可解释；无非法状态、未归类丢失、不安全整条重发或停止后新发送；若使用 multipart 须按实际 PDU/part 对账。
- **注意**：一次 OCC+Fast 成功仅可支持 `S0` 最低基线，**不能**升级为 G3 PASS。正式 G3 为 serialized/occ × reliable/fast 四组合、每组合 3 个全新独立进程，属于后续 S2 闸门，不能用旧版本或单场景数据拼接。

### 结论规则

只有 G0 当前回归、T01、T02、R03/R17 确定性回归和 S0 最低 1K 闭环均完成且通过，才能写 `P0_CORRECTNESS_READY`。

任何未解决 P0/业务守恒失败：`S0_NO_GO`。仅环境/依赖阻断：`S0_BLOCKED_ENV`，附证据、已通过子项和未运行项。无论 S0 结果如何，除非另经独立门禁核验，`G3 = NO_GO / NOT_VERIFIED`、生产 `NO_GO` 不得改变。

## 4. 工作方式与安全规则

1. 在独立测试副本运行高负载和故障注入，Linux 用 ext4；**不对工作中的 DuckDB 打开第二 RW Owner**，运行中通过已有 API/metrics 观察，停止 Owner 后再只读对账。
2. 先写/运行重现测试，再进行最小修复；一类 P0 一次局部改动，不扩大为 S1–S4 重构。修复一个 P0 后执行针对性回归，阶段完成后重新跑全套相关门禁。
3. 每个结论标明 `源码证据`、`动态复现`、`历史报告`、`未验证`；不得虚构测试运行、性能吞吐、SLES 或真实 SMSC 验收。
4. 不清理/重置真实任务、DB、WAL、Journal、日志、缓存及用户未提交的改动；不改前端 lockfile、密钥、生产连接配置；输出证据单独保存在非提交目录。
5. 不要盲目 `git add .`、强推、覆写历史；只纳入此次相关代码、测试和总结文档。默认先生成本地 commit，**未经用户明确授权不要推送**。
6. 如果证明现版本 DuckDB 无法保证权威数据守恒，停止其它大规模行为改造，提交具体失败证据与存储版本/方案复核建议。

## 5. 交付物

创建 `Docs/Marketing_Copilot_S0_Correctness_Baseline_Closure_Report.md`，包含：

- 冻结源码 SHA / 环境 / 依赖 / 运行命令 / 隔离测试位置（脱敏）
- R01、R02、R03、R17 各自**复现→根因证据→修复→回归**状态与源码位置
- G0、T01、T02、sender-only、OCC+Fast 1K 的独立执行记录、结果文件路径和精确守恒表
- 数据完整性、停止后 socket、unknown/partial/DLR 重发、租约 fencing 的关键断言
- 修改文件清单、兼容影响和潜在回滚方式
- 仍缺失的 G3×4×3、G4–G7、SLES12SP5 和真实 SMSC 证据
- 最终结论只能是 `P0_CORRECTNESS_READY`、`S0_NO_GO` 或 `S0_BLOCKED_ENV`

**Codex 结束时汇报**：最终 commit SHA（若产生）、四个 P0 的状态、测试门禁摘要、是否允许进入 S1，以及仍存在的证据缺口。禁止以阶段设计 READY 替代运行 PASS。
