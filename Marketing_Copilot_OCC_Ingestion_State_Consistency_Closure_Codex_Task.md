# Codex 工程任务：Marketing Copilot OCC 摄取状态一致性修复与 G3 收敛关闭

> **任务编号**：MC-OCC-INGESTION-CONSISTENCY-CLOSURE  
> **优先级**：P0（正确性优先，性能次之）  
> **基线**：`origin/main` = `2bec392493d480508be016289858de83e92b5cc4`（2026-10-15 核对）  
> **本轮目标**：找出并修复 `MC_DUCKDB_WRITE_CONCURRENCY=occ` + `MC_SMS_SEND_MODE=fast` 时，`processed_files` 投影状态缺失 / OCC 冲突导致摄取事务回滚、文件错误重试、SMS 任务流水线不能闭合的问题。  
> **验收策略**：先可证明的单元/故障注入，再 WSL/ext4 上的 **OCC+Fast 1K 连续 3 次**；通过后才恢复全量 G3 2×2 四组合对比。**不在本任务内宣称 G4/G5/SLES/真实 SMSC 已通过。**

---

## 0. 背景与已确认事实（必须保留证据级别）

仓库：`https://github.com/shutu-ai/Marketing-Copilot`。

- `2bec392` 是五份验收文档的提交；待修复代码基线是其祖先 `c9890a76040f92dabd50c4b50754d44716d6daca`。必须记录实际执行代码的完整 SHA，绝不可把文档 SHA、基线 SHA、工作树探针混为一个结果。
- 新版本地后端回归已报告 `189 passed, 3 skipped`；**这不是 G3 OCC+Fast 收敛通过**。
- 当前有效 G3 证据：**OCC+Fast FAIL**，其余 serialized+reliable、occ+reliable、serialized+fast 的旧 `1511d4c` PASS 均为 `HISTORICAL`，不能算当前提交通过。
- 失败环境：AlmaLinux 8 WSL、`/tmp` ext4、CPython 3.11、DuckDB 1.2.0、模拟 SMSC、pool=4、window=50、route TPS=200、task workers=8、ingestion workers=2、watch-copy workers=2、queue=2000、DLR=disabled，10 文件 × 100 行。
- 失败迹象：`ingestion_files` / `ingestion_chunks` OCC 冲突、`processed_files` 缺失或状态异常，导致摄取状态更新和完成收敛失败，并触发文件层 `UNKNOWN`/重试。这里的 **文件 UNKNOWN 不等于 SMPP 已发送但未确认的 SMS UNKNOWN**，不得混淆。
- 未证实真实 SMSC 重复发送。现有报告中的 `lost_mapping` 主要指摄取文件镜像状态，而不是已证实丢失了 SMSC message-id 绑定。必须分别独立统计。

现有报告：

- `Docs/OCC-Equal-Window-2x2-G3-G4-G5-RCA.md`
- `Docs/OCC-Equal-Window-2x2-G3-G4-G5-Acceptance.md`
- `Docs/OCC-2x2-Concurrency-Performance-Results.json`
- `Docs/DuckDB-OCC-Concurrency-Architecture-Decision.md`
- `Docs/Workflow-Isolation-Stop-Fence-RCA.md`

**不要为了“通过”测试而：延长无进度超时、放宽任务总数、忽略异常、删除约束/唯一索引、关闭 OCC、改用全局 RLock、允许 unknown SMS 自动重发，或直接跳过镜像一致性检查。**

## 1. 最重要的设计约束

1. **单一权威状态**：以 `ingestion_files`、`ingestion_chunks` 的 lease/token/processing/terminal 状态和 `sms_tasks` 的去重/任务事实为核心真相；`processed_files` 应成为可校验、可补建的展示/审计投影。先核对其各字段的实际消费者，再决定去耦方式，不能只改注释。
2. **事务原子性不能降低**：摄取 lease、幂等 task 创建、chunk checkpoint / completion 必须维持已有的同事务约束；该事务 OCC 冲突时只能从新连接重跑完整逻辑事务，不能拆成可能丢业务记录的多次非原子写入。
3. **镜像失败不能重做真实业务**：`processed_files` 缺失、投影冲突、投影写入失败不能回滚已经提交的权威 checkpoint，更不能使已创建任务或已提交 SMS 再执行一次。反之，权威事务未提交，不得宣称镜像“已完成”。
4. **可恢复不是仅内存队列**：如果采用异步 projection，必须能从权威表/持久 outbox、version 或其他持久证据恢复。崩溃后不应留下永久缺失或错误 terminal 的镜像。
5. **避免新热点**：禁止把 `processed_files` 的短锁推广为全业务互斥锁；不要在 DuckDB 事务或受保护的短临界区内计算 SHA256、解析文件、做 SFTP、执行长时间日志/HTTP/I/O。
6. **单一 DuckDB owner**：保留当前 `TransactionCoordinator`、`maintenance_exclusive`、OCC 完整事务重试、备份独占屏障和实例锁。不迁移 Sharded Send Store，不新增第二业务数据库。
7. **SMS 安全底线不变**：`dispatch_generation`、Claim/Attempt Token、SMPP socket admission fence、Result Journal Event-ID 幂等、unknown/partial 不自动重发；Fast 仍显式 opt-in，默认 serialized+reliable。
8. **文件身份稳定**：镜像唯一键 `(campaign_type,campaign_id,filename)`、文件 ID、lease、代际、替换/重传、同名不同内容及隔日复用，需要明确判别。不得让旧版本投影覆盖新版本或让缺失镜像改变文件去重结果。

## 2. Phase 0：证据冻结、调用点审计与最小复现（先完成，不得直接重构）

### 2.1 冻结

- `git fetch --prune`（仅在连接允许时）；保存完整 HEAD/parent、`git status --porcelain`、Python/DuckDB/OS/FS 信息；不得清理、重置或提交用户其他未跟踪改动（包括 `package-lock.json`）。
- 确认 `Docs/OCC-2x2-Concurrency-Performance-Results.json` 的历史/current 标签，不得修改旧失败 raw evidence 或复用其他 SHA 的 PASS。
- 检索 `.codegraph/` 相关图（若已安装）及 `rg`，建立写入和读取 `processed_files` 的完整调用清单：
  - `realtime_ingestion_state.register_processed()`、`finish_processed()`、`Progress._write()`、`transition()`、`recover_expired_files()`、`processed_transaction()`；
  - `realtime_ingestion_worker._processed_id()`、`_discard_expired_queued_files()`、`_discard_inactive_campaign_files()`、错误分支 `_mark_retry()` / `_quarantine()`；
  - `ingestion_job_service` 的 claim/lease/chunk/recovery；
  - `realtime_campaign_service` 的 enqueue、完成、统计/UI 读取；
  - `operations_service`、API/历史审计/监控和其他所有 direct SQL 写入。
- 区分哪些消费者**必须**读取 authoritative state，哪些只是展示镜像。记录每处调用点是否影响幂等、输入文件清理、任务创建和人工可见状态。

### 2.2 故障精确定位

- 在隔离 WSL/ext4 测试 DB 中，使用 DuckDB 1.2.0 同版本复现：相同文件注册并发、progress vs terminal update、旧 lease / 新 lease 交叉、批量结果写与摄取同时运行、`ON CONFLICT DO NOTHING` 后再读、同 unique key 的更新。
- 记录每个冲突的 **operation 名、事务起止时间、表名、文件 ID、唯一键（脱敏）、lease generation、attempt number、SQLSTATE/异常类型、完整回滚/重试最终结果**。不要记录真实 MSISDN 或正文。
- 核查“丢失或损坏镜像”究竟是：事务未提交、错误映射、错误版本覆盖、遗漏写路径、DuckDB unique-index 问题，还是观测器读了不同快照。不要仅凭异常文字断言 DuckDB 引擎损坏；若怀疑引擎问题，单独提供最小 SQL/线程复现。
- 输出 `SOURCE_CONFIRMED` / `LOCAL_REPRODUCED` / `HYPOTHESIS` 分级证据和明确根因链。**若没有复现原因，不要靠隐藏异常来标记修复。**

## 3. Phase 1：将权威写事务与镜像投影安全解耦

### 3.1 方案选择（Codex 必须先给出 ADR）

优先评估 **权威事务提交 + 持久投影工作队列（transactional outbox）/ 幂等 reconciliation** 的方案。具体结构以调用点审计结果确定，避免不必要的大型通用事件平台。

推荐业务顺序：

```text
File/Chunk claim (durable lease)
    -> parse/match outside transaction
    -> durable sms_tasks + chunk state + ingestion_files + [minimal projection version/outbox]
       COMMIT via DuckDB OCC; on conflict retry complete transaction
    -> projection worker UPSERT processed_files (separate short transaction)
    -> periodically or on restart repair missing/stale projection
```

- 如果 `processed_files` 存有无法从权威表重新导出的 `file_hash`、原始文件路径、文件大小、展示字段等，则应在 **权威记录或事务内的极简 outbox** 持久化重建所需的最小事实。不得假定可从删除/移动后的原文件重新计算。
- 投影事件最少包含持久文件 ID + 稳定身份、可排序版本/代际、状态或规范化变化标识、生成时间；不得依赖可丢失的进程内引用。
- 统一确定 `processed_files` 是按文件**当前版本**投影还是按文件**历史事件**留痕；保留现有 API/UI 兼容性、历史统计语义及必要的审计轨迹。
- 投影失败通过**幂等重放、最大重试次数/退避、失败可见、后续 repair 扫描**处理；可延迟，但不能静默丢失。为队列/lag/无法修复的坏行设健康指标。
- 必须有版本防倒退规则：晚到 `processing` 不得覆盖同文件已 durable `completed`；旧 lease 不得覆盖新 lease；补建不能错误覆盖同名新文件版本。投影更新应当基于权威最新快照或有序版本 CAS，而不是照搬事件顺序。
- 新 outbox 的生命周期、清理策略、schema migration / 升级回滚、离线重建方案必须写清；如用扫描重建而不用 outbox，需要证明字段完整、重建成本可控、崩溃不丢资料。
- 使用 bounded worker/quantum，避免投影任务无限积压挤占 SMPP Result Writer；多活动之间应该公平轮转。投影退化不能占满所有 DB OCC 重试预算。

### 3.2 修改现有热点路径

- `Progress._write()`：权威 `ingestion_files` 的合法 progress 应能独立提交，不因镜像行缺失而抛出 `LookupError` 回滚。投影修复不得恢复旧 lease 的业务写权限。
- `transition()`：`completed` 判定仍需检查 chunk 全部完成；在权威事务内更新 `ingestion_files`、必要文件登记和持久 projection 信号，镜像由投影层维护。禁止先投影 completed 再尝试权威事务。
- `register_processed()`：必须解决现有入口对 `processed_id` 的强依赖。缺镜像可触发修复，不得把 UI 记录缺失误判为整个文件已失去 lease 或必须重新发短信。文件级唯一性与 idempotency 依赖权威 key 或经证明稳定的业务主键。
- `realtime_ingestion_worker` 中直接更新 `processed_files` 的失效文件/停用活动路径：纳入同一投影流程，不能绕过版本栅栏。真正权威的 terminal/expire 操作仍由对应文件/活动事务控制。
- `recover_expired_files()` / restart：先按权威 lease 状态恢复，再 reconciliation 投影。缺失镜像不能导致无条件 requeue 已完成文件。
- 对于 `processed_files` 与 `ingestion_files` 状态相矛盾的旧行，提供**非破坏性** backfill 工具：只修复明确可判定的字段；不可判定的归类 `MANUAL_REVIEW`，不得篡改 SMS 发送事实。
- 根据实测决定是否撤销或收窄 `_processed_files_guard`。若仍保留，证明它只保护单表短操作且没有与 OCC DB/活动 Fence 产生循环锁顺序；不允许为求简单重新加回全局 DB RLock。

### 3.3 幂等与结果守恒

- 文件/Chunk 业务提交需确保：相同 chunk 或同一次提交重试不会重复计 `created_tasks`；不产生重复 task；lease 丢失后的旧 worker 无法写入。
- `processed_files` 行缺失或被人为破坏后，系统自动补齐：唯一键无重复、字段来源清晰、修复可重复运行、重启后仍正确。
- SMSC 已接受与 DLR 已送达不得混用；`unknown`/`partial` 不自动重发；projection 的 replay **绝不能**调用 SMS send/任务重新生成。

## 4. Phase 2：确定性的单元、并发和强杀测试

必须新增或扩展自动化测试，至少覆盖：

1. `processed_files` 在 valid lease 处理过程中突然缺失：权威任务创建/chunk 完成不受镜像缺失影响，投影最终补齐；
2. 同 key 两个并发注册/投影 upsert，OCC conflict 发生后完整重试，最终一条镜像且数据正确；
3. `processing` 投影延迟到 `completed` 之后：旧状态不得覆盖 completed；
4. `completed` 与旧 lease 的 retry/expire/recover 竞争：旧 worker 不能把文件降级或再建任务；
5. 同名文件新版本/不同 hash、跨日和多 campaign：不串数据，不误认成同一个 SMS 提交；
6. file progress/terminal、task enqueue、SMS result writer 并发：验证 OCC 冲突可恢复，镜像异常不导致权威状态回滚；
7. 事务提交后、outbox 投影前强杀重启：状态与投影可重放，且没有重复 SMS socket writes；
8. 投影 worker 重复消费同一个事件、故障后重投：exactly-once **业务效果**，不是依赖 exactly-once 消息投递；
9. 镜像 DB 写入 失败、队列满、维护备份屏障：正常结果不伪成功；错误可观测，恢复后最终收敛；
10. unknown/partial 结果与 drop-response、reject、DLR delay、restart 回归：不得发生自动重发或状态晋级错误；
11. 证明 Serialized 与 OCC 两种模式都通过核心状态机；Fast/ Reliable 路径不因投影策略改变发送授权。

禁止测试只 mock 掉 DuckDB、OCC 或重试机制。至少一组需要在真实 DuckDB 1.2.0/WSL ext4 中有多线程屏障、实冲突、真实磁盘提交和重启恢复证据。

## 5. Phase 3：G3 重新收敛（不得复用旧 SHA 产物）

### 5.1 执行环境

- 优先 **WSL 本地 ext4 `/tmp` 或 `$HOME`**，不在 `/mnt/c` 或 `/mnt/d` DrvFS 上运行数据库、Journal、WAL 与压测目录。
- 用 CPython 3.11 / DuckDB 1.2.0 与现有可运行虚拟环境；使用单 owner 进程、loopback SMSC。Windows CPython 3.13 DuckDB 原生扩展环境不作为业务 PASS。
- 记录 `git rev-parse HEAD`、完整 source SHA、worktree 状态；若打包测试未包含 `.git`，用构建时不可变 `source_manifest.json` + 文件 hashes 标记，且必须实际验证与提交的源树一致，不能仅写入声称值。
- 不接入真实 SMSC、不使用现网数据库/号码/凭据；所有 raw 测试数据位于隔离工作目录，**不能纳入 Git**。

### 5.2 先修复 OCC+Fast 独立 1K ×3

配置完全固定：

```text
MC_DUCKDB_WRITE_CONCURRENCY=occ
MC_SMS_SEND_MODE=fast
Scenario=stress-1k; 10 files x 100 rows = 1000 unique eligible events
Pool=4; SMPP window=50; route TPS=200
Task workers=8; ingestion workers=2; watch-copy workers=2
SMPP send queue=2000; DLR=disabled; SMSC=loopback
```

在当前支持这些参数的 `backend/tools/local_e2e_acceptance.py` 上执行（先用 `--help` 确认选项；以下为模板，不要假定当前 venv 路径一定存在）：

```bash
MC_DUCKDB_WRITE_CONCURRENCY=occ \
python backend/tools/local_e2e_acceptance.py \
  --scenario stress-1k --send-mode fast \
  --pool-size 4 --smpp-window 50 --smpp-send-queue-capacity 2000 \
  --smsc-tps 200 --task-workers 8 --ingest-workers 2 --workers 2 \
  --delivery-receipt-mode disabled --keep-artifacts \
  --output /tmp/mc-occ-ingestion-evidence/occ-fast-1k-run1.json
```

- 重复 **3 个独立进程和独立 DB/Journal/输入目录**；不能连续复用同一个运行库。
- `input_rows=1000` → 可解释的过滤/去重 → `tasks_created=1000` → 1000 `submit_sm` → 1000 成功响应 → `accepted=1000`；最终 queued/sending/retrying 不遗留（测试模拟器均成功的前提）。
- 10 文件逐个 `rows==100`、chunk 对账、`ingestion_files=completed`、`processed_files` 投影终态一致、无缺失/重影/状态倒退；出错要列出 stage/key/event，并保留 raw 证据。
- `sms_message_bindings`、part journal、attempt/task/status 与 SMSC 事件独立核对；`duplicate_send=0`、无 `unknown_sms_auto_retry`、无 late-submit-after-stop、无 lost mapping；不得用“最终 accepted”取代 submit 数量审计。
- **卡住自动取证**：连续 30 秒无业务进展写入轻量快照，60 秒完整 thread dump、入队深度、DB transaction active/conflicts/retries、projector lag、file/chunk/task/attempt/journal 分阶段数量；超时即 `FAIL` 而非无限延长。

### 5.3 然后重跑 G3 四组合

仅在上面 OCC+Fast ×3 完整 PASS 后，执行同提交、同环境、同数据和限速参数的四组合（每组 3 次）：

| | Reliable | Fast |
|---|---|---|
| Serialized | 3 次 | 3 次 |
| OCC | 3 次 | 3 次 |

指标：业务短信/s、submit_sm part TPS、p50/p95/p99、输入到任务延迟、任务到 submit_sm_resp 延迟、OCC 冲突/重试/失败数、Journal fsync/checkpoint lag、峰值 RSS/CPU、各阶段队列曲线。分别提供正确性与性能结论；不得使用历史 `1511d4c` 结果计算本次提升。

若某轮新修改产生新 SHA，必须重新判定受影响门禁，不能把之前的 PASS 提升到新版本。性能若未优于 Serialized 也需如实报告；正确性 PASS 不等于性能 GO。

## 6. Phase 4：G4/G5 与生产门禁范围控制

- 本任务优先关闭 OCC 摄取镜像故障与 G3，不允许为赶进度直接跑上万条长稳压测掩盖数据损坏。
- **G3 PASS 后**可执行现有 G4 小规模隔离 smoke：A 大积压，B/C 正常推进，校验投影维护不会让某一个活动阻塞其他活动。正式 G4（A=50K，B/C 各 1K）与完整 G5（故障注入、强杀恢复、有效 600 秒 soak）仍应按资源、场地和批准范围执行，并保留状态 `NOT_RUN` 直到真正通过。
- 任何时候发现重复 SMS 提交、unknown 自动重发、权威账目丢失、维护屏障泄漏、不可恢复的投影错账，立即停止扩压并 RCA；不自动进行 SLES/真实 SMSC canary。
- SLES12SP5、真实 SMSC、真实运营商 DLR 仍为独立门禁；没有环境/授权必须写 `BLOCKED_ENV` 或 `NOT_RUN`，不得说 PASS。

## 7. 交付内容（必须）

建议新增/更新：

1. `Docs/OCC-Ingestion-State-Consistency-RCA.md`：真实根因、逐调用点权威/投影关系、故障前后事务时序、锁顺序图、SQL/代码位置及可复现方式；
2. `Docs/OCC-Ingestion-Projection-Architecture-Decision.md`：权威数据模型、outbox/重建选择、schema/版本/lease、投影幂等、迁移/回滚与运维语义；
3. `Docs/OCC-Ingestion-Consistency-Acceptance.md`：故障注入、并发回归、ext4 结果、每轮可追溯 SHA、有效/历史对照与遗留问题；
4. `Docs/occ-ingestion-state-consistency-results.json`：机器可读证据索引和严格门禁；
5. 如需要：`backend/tools/reconcile_processed_files.py` 或同等可控修复入口，支持只读 `--check`、单活动/文件范围 `--repair`，防止误修改权威状态；
6. 最小必要的 backend 代码、数据库迁移、测试与指标；不要引入不必要组件。

机器可读结果至少包含：

```json
{
  "source_sha": "ACTUAL_TESTED_SHA",
  "baseline_sha": "2bec392493d480508be016289858de83e92b5cc4",
  "mode": "occ+fast",
  "runtime": {"python": "...", "duckdb": "...", "filesystem": "ext4"},
  "gates": {
    "P0_reproduction": "PASS|FAIL|NOT_RUN",
    "P1_authority_projection_separation": "PASS|FAIL|NOT_RUN",
    "P2_fault_injection": "PASS|FAIL|NOT_RUN",
    "G3_occ_fast_1k_x3": "PASS|FAIL|NOT_RUN",
    "G3_equal_window_2x2": "PASS|FAIL|NOT_RUN",
    "G4": "NOT_RUN",
    "G5": "NOT_RUN",
    "SLES12SP5": "NOT_RUN",
    "REAL_SMSC": "NOT_RUN"
  },
  "runs": [],
  "duplicate_sms_submit": "NOT_VERIFIED",
  "unknown_sms_auto_retry": "NOT_VERIFIED",
  "processed_mirror_reconciliation": "NOT_VERIFIED",
  "production_decision": "NO_GO"
}
```

状态示例是 schema 提示，不允许填充虚构 PASS；最终状态必须由脚本实测生成。`MORE_EVIDENCE_NEEDED`、`NO_GO`、`BLOCKED_ENV` 应按实际适用场景准确记录。

## 8. 验收不可妥协条款

- **业务真相不会因投影失败倒退**，也不会因投影缺失触发 SMS 重新发送；
- 每个 task / chunk / file 的 unique identity 与 lease/CAS 均保持正确；
- 在 DuckDB OCC conflict、强杀/重启、延迟投影以及旧 worker 回调下镜像最终一致；
- 没有跨活动的全局 Python 写锁回归；
- 1K 10 文件全链路在当前源树 OCC+Fast **3/3 完整通过**后，才允许宣称 G3 该分支 PASS；
- 同版本四组合每臂 3 次都通过，才能宣称整体 G3 PASS；
- 当前 50K 多活动隔离、完整 G5、SLES、真实 SMSC 未执行，不能借助本任务完成状态来自动提升门禁。

## 9. Codex 执行规则

1. 先输出短审计结论与实施方案，再直接开展实现和测试；不要用“请确认”打断常规安全开发流程。
2. 无法获得 WSL/ext4/所需资源时，完成代码、故障注入与可执行命令，并将环境门禁标为 `BLOCKED_ENV`，**不要伪造或代替执行数据**。
3. 最小改动修复真实根因，保持兼容既有 DB 和 API、性能路径；必要的 schema migration 必须幂等、可恢复、向后兼容。
4. 仅暂存本任务相关文件，保留日志、DB、WAL、缓存、临时目录、raw 产物和用户其他工作区文件；不运行 `git reset --hard`、不强推、不删除既有证据。
5. 必须至少运行后端相关测试及完整 suite（如环境允许）、前端 build、Python compile/check、异常和重启回归；写明每项实际命令及结论。
6. 如仓库工作流允许，提交并推送到 `origin/main`；若无权限/网络问题，报告阻断，不谎称成功。
7. 最终报告按 **DONE / FAILED / BLOCKED_ENV / NOT_RUN / UNVERIFIED** 分类，列出最终 SHA、真实修复路径、验收数字和下一步工作，不允许用“测试绿了”代替正确性证据。

**执行优先顺序：先封闭摄取状态一致性，再判定 OCC+Fast 1K，最后才恢复 2×2 G3 性能比较。不要在故障未闭合时跑 G4/G5。**
