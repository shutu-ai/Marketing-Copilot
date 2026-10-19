# Marketing-Copilot VNext — Linux 吞吐验证、CLI 双速率与试运行就绪 Codex 任务

**任务 ID**：`MC-VNEXT-P1-LINUX-THROUGHPUT-CLI-PILOT-20261019`  
**目标仓库**：`https://github.com/shutu-ai/Marketing-Copilot`  
**已交付基线**：`122f47d42b5fbcc2ca871d887286ac93ce665e4d`  
**优先级**：P0（先完成必要验证，尽快支持 1–2 个真实 Campaign 的受控试运行）  
**任务属性**：性能验证 + 极小范围可观测性完善；**不是再做架构重构**

---

## 0. 给 Codex 的总指令

你是 Marketing-Copilot VNext 项目的资深 Python/SMPP 性能工程师。以实际仓库代码为准，先阅读 `AGENTS.md`、现有任务/报告与源码，**不能只根据本任务书假设 CLI 参数、配置项、服务路径已存在**。如果本文与现行源码不一致，应标注差异，采用源码实际接口，不得靠猜测创造测量结果。

**最高目标**：确认此次 Response/Journal 解耦在目标 Linux 系统上是否真正提高或至少不降低持续 Socket 发送吞吐，且不会因 CLI/聚合检查点增加明显开销，然后交付可操作的 1–2 Campaign 试运行手册与客观的 GO / NO-GO 结论。

执行原则：

1. **性能与实际业务可用性优先**，不要借机扩大安全架构、账号体系、抽象层、审批流、数据库和 UI 重构。
2. **绝不重复发送优先于尽量不漏发**：继续保持 checkpoint-before-socket、unknown-never-resend、at-most-once；允许把无法确认的发送显式列为 unknown/unresolved，不得自动补发。
3. 保留 SMPP 与 Journal/Projection 分层、有限容量、背压、Campaign pause/resume/cancel、Crash recovery。**不要通过无界队列、忽略写盘异常或跳过 checkpoint 换 TPS**。
4. 保持 SMPP **4 Bind / TPS 200 / Window 50** 的既有基准配置（解释配置作用域，不得自行调整争取更好分数）。
5. 只修改完成本任务所需的 CLI、低频状态快照和有证据证明必须修复的缺陷；未经性能证据**禁止修改 SMPP 发送协议、批处理架构、并发模型、Journal 持久化语义**。
6. **未经新的明确授权：严禁连接真实 SMSC、提交真实号码、执行真实营销发送、部署/重启生产服务、修改生产数据库或线上配置**。性能测试只使用明确授权的测试主机与 `127.0.0.1`/隔离 synthetic loopback SMSC。生成试运行操作手册不构成上线授权。
7. 不要为了验证而在服务器装重型工具或更改系统全局软件/安全策略。测试需保留可复现命令，禁止伪造、填零、推算不存在的数据。
8. 不修改既存无关 dirty/untracked 文件；**不自动 commit、push 或打 tag**。若任务结束时用户另行授权提交，才按实际变更提交。

---

## 1. 已知基线与本次范围

上一轮提交已经实现：

- `backend/vnext/progress.py`：每 Worker 一个有序、有限容量的 `ProgressPersistence` writer thread；Reader 非阻塞提交事件，Journal `append_many + fsync` 离开 Worker asyncio 事件循环。
- `backend/vnext/smpp_transport.py`：Reader 完成 `_pending` 匹配、协议 Window release 后投递响应事件。
- `backend/vnext/worker.py`：发送前按 `1 + encoded_parts` 预留事件/字节额度，容量不足则在 checkpoint/socket 之前停止新 dispatch。
- `backend/vnext/runtime.py`：Worker 低频状态/聚合快照与进程协调。
- `backend/tools/vnext_stats.py`：独立、只读、标准库 CLI；不调用 Web UI、FastAPI、DuckDB，也不扫描 Journal 内容。
- 持久化 Campaign aggregate checkpoint，支持重启、segment rollover/reclaim，无法保证准确时输出 `incomplete`。
- Windows / Python 3.13 回归：**469 passed、5 skipped**。目标 Linux native ext4 的 1K/5K、CLI paired overhead、600s、真实 SMSC 尚未验证。

**本次任务包含三类工作**：

A. **小范围 CLI 指标修正**：明确区分 Socket 实际写出速率与 Journal durable 速率。  
B. **目标 Linux 性能与正确性验证**：1K smoke、5K old/new paired、CLI OFF/ON paired、故障/恢复基础检查和 600s steady。  
C. **1–2 Campaign 试运行准备**：生成可直接执行的操作清单、阈值建议和回退/暂停方案；不替用户执行生产操作。

**非目标**：全面重写 stats CLI、建设 Web Dashboard、实现逐 Campaign Projection SQL、50K 极限压测、真实 SMSC 联调、生产发布、无界可靠投递、恰好一次送达保证。50K 与全量上线另议。

## 2. Phase 0 — 基线核查（先做，禁止跳过）

1. `git status --short`、`git rev-parse HEAD`、`git log -3 --oneline`；记录 HEAD 与 `origin/main` 是否一致。对用户已有 dirty/untracked 仅记录不清理。
2. 核对基线提交 `122f47d4` 与当前 HEAD 的关系：若 HEAD 已向前推进，记录增量；若关键实现发生变化，以实际代码重新评估。
3. 阅读：
   - `Docs/Marketing_Copilot_VNext_Response_Journal_Decoupling_Implementation.md`
   - `Docs/Marketing_Copilot_VNext_Pilot_CLI_Statistics_Guide.md`
   - `Docs/Marketing_Copilot_VNext_Response_Decoupling_CLI_Acceptance.json`
   - 上述五个核心源码文件、`backend/tools/vnext_streaming_acceptance.py` 及其参数/帮助。
4. 检查性能测试环境：目标 OS 版本、CPU、内存、Python、原生文件系统（重点 ext4）、磁盘类型、进程限制、可用测试账号/路径；确认是否真的有授权的 Linux 测试环境。
5. 找出旧版同步持久化路径与新版解耦路径可比的运行方式。**优先同一测试 harness 的 legacy/decoupled 模式**；若需要切换旧代码，固定 old/new commit SHA 和差异。禁止把不同机器/不同测试负载的结果当成配对比较。
6. 冻结配置快照（脱敏）：Bind 数、TPS scope、Window、Worker 数、partition 数、队列大小、batch_size、flush_interval、Journal hard budget、Projection 参数；两组实验不得任意改变。

**Phase 0 产出**：测试矩阵、环境与源码 SHA、可运行命令；若没有测试主机权限，标记 `LINUX_ENV=BLOCKED_ENV`，继续完成本地 CLI/测试/文档部分，不得把 Windows 数据冒充 Linux 实测。

## 3. Phase 1 — CLI 双速率，最小变更

### 3.1 问题

现有 CLI 的 `rate(5s)` 使用 durable `socket_written` 计数差分。Journal fsync 慢时，此值可能暂时下降，**不等于当时实际 Socket 写出 TPS**，容易将持久化滞后误判为发送降速。

### 3.2 要求

不增加发送热路径的磁盘 IO、锁竞争或重型统计。优先使用 `AsyncSmppTransport.snapshot()` / Worker 心跳里已存在的 **process-local 实际 `socket_written_parts`** 计数，通过现有约 1 s 的快照供 CLI 读取；不要在每 PDU 的 Reader/Socket 写入路径执行文件操作。基于当前 API 可在既有快照中增加少量计数，不创建新服务。

在 CLI 文本与 `--json` 中分开提供：

| 指标（建议名，可按现有 schema 命名） | 数据定义 | 解释 |
| --- | --- | --- |
| `socket_write_rate_pdu_per_second` | Worker/Transport 观察到 Socket 已执行写出路径的 PDU 累计数，低频快照的滚动差分 | **发送侧实际观测速率**；不等于 SMSC 接受成功/送达；`writer.drain()` 完成也不等于对端确认 |
| `journal_durable_rate_pdu_per_second` | Journal fsync 成功的 `socket_written` PDU 累计数差分 | **持久化进展速率**；允许短时滞后/突发 |
| `response_observed_rate_pdu_per_second`（可选） | SMPP Reader 观察到的 `submit_sm_resp` PDU 差分 | 识别响应/Window 瓶颈 |

实现约束：

- 保留原有输出字段或明确兼容性映射，不默默改变旧 JSON 字段语义。
- 区分 **PDU** 与 **SMS task**：长短信分段要按真实 PDU 数计数。
- 按 Run ID、进程代际、Worker 实例识别计数器重置；不能将新进程的低累计数与旧进程做差。缺失/stale/不完整数据展示 `N/A` 与原因，而不是伪造 0 或负速率。
- CLI 原本的 Campaign 聚合继续以 durable checkpoint 为准；逐 Campaign `Projected=N/A` 必须持续显示 `incomplete_metrics`，不得假造已投影数。
- 默认 2 s 观察频率，窗口可保持约 5 s；只读现有 bounded snapshot，不读全量 Journal/Spool，不查 DuckDB。
- 多 Worker 重启、不同心跳刷新时间、CLI 多进程同时观看时，不能重复计数或明显误报。

### 3.3 测试

补充最小单元/CLI 测试：Socket 计数先增长但 durable 尚未增长；随后 durable 追平；multipart；Worker 重启；缺失快照；stale；counter reset；`--once --json`/`--watch --json`；确认 CLI 不写运行目录。

**不要**为这些字段引入 Prometheus、数据库迁移、后台服务或第三方 TUI 库。

## 4. Phase 2 — Linux native ext4 的 1K / 5K 性能验证

在**已授权的目标 Linux 测试环境**执行。优先采用实际生产同类 OS（已知目标文档为 **SLES12SP5 native ext4**）；如果可用服务器是 EulerOS 或其他 Linux，需明确标记 `OS_MATCH=SAME/DIFFERENT`，不能把其结果写成 SLES 已验证。

### 4.1 通用规则

- 所有流量只能打到隔离 synthetic/loopback SMSC；统一短信长度/分段比例、响应延迟模型、消息数、Campaign 数、并发、TPS/Window/Bind、磁盘位置、资源限制。
- **配对测量**：优先交错 `old → new → new → old` 或等价的平衡顺序；每轮前把前一轮数据与运行进程清理到可比较状态，保留证据；至少 2 个有效配对，噪声明显时至少 3 对。
- 每轮完整记录：`socket_written_pdus`、平均/稳态 Socket PDU/s、durable PDU/s、响应成功/错误/unknown、Window wait、TPS credit wait、Journal backlog/最大未落盘年龄、Projection lag/追平时间、CPU、RSS 峰值、磁盘增长、writer fsync 次数与耗时、实际总时长。
- 历史基线 `4 Bind / TPS 200 / Window 50` 保持不变。**如果 Window × 响应延迟限制了吞吐，应报告协议上限，不把该瓶颈错误归因于 Journal。**
- 验证无新增重复发送：按 Campaign 和 event_id / PDU 关联 reconcile `accepted → checkpoint → socket_written → submit_sm_resp → durable → projection`；分别解释 task 与 PDU 的多对一关系。
- 只在有真实分母/时间窗口时报告 TPS；不要用 durable 事件数代替 Socket TPS，也不要把投影完成时间混为发送完成时间。

### 4.2 测试矩阵

| ID | 场景 | 目的 | 最低输出 |
| --- | --- | --- | --- |
| L1 | 1K PDU smoke；解耦模式 | 验证 Linux 路径与对账 | 完成率、异常、最终 backlog=0 |
| L2 | 5K PDU old/new 配对，CLI OFF | 验证新版发送吞吐是否改善/回退 | 每轮 Socket TPS、瓶颈、配对差异 |
| L3 | 5K PDU CLI OFF/ON 配对（均为新版） | 量化只读 CLI 的开销 | Socket TPS 与 CPU/RSS 配对差异 |
| L4 | Writer 人工延迟 600 ms（隔离测试） | 证明 Reader/Window 不直接等待 fsync | Reader 继续收响应、Window 正常释放、队列涨落证据 |
| L5 | 临时 Journal 容量不足，再由 Projection 回收 | 验证受控背压与自动恢复 | checkpoint 前暂停、恢复、无重复/假 durable |
| L6 | 双 Campaign：A pause/resume/cancel、B 继续 | 验证隔离性与统计准确性 | A/B 分别对账、CLI 状态变化 |

> L4/L5 使用注入或隔离故障，不允许向生产磁盘故意写满空间，不允许对生产服务制造延迟。

**统计口径**：old/new 以实际发送侧稳态 PDU/s 为主指标；给出 `delta % = (new / old - 1) × 100%`，保留每轮原始值与分布，不只报最漂亮的一轮。CLI 负担按 `(CLI_ON / CLI_OFF - 1) × 100%`。如负载受配置 TPS 限速或窗口延迟支配而缺乏判别力，应明确 `INCONCLUSIVE_PROTOCOL_LIMITED` 并设计补充的等配置、低响应延迟试验，不静默扩大 TPS/Window。

### 4.3 初步判定建议

- `CLI_OVERHEAD_PAIRED_PERCENT`：目标 Socket TPS 下降不超过 **3%**，并检查是否在重复试验噪声范围内；超出时调查 CLI 文件读频率、心跳生成和 aggregate checkpoint 而不是直接上复杂方案。
- `NEW_VS_OLD_SOCKET_TPS`：目标 **不低于旧版 95%**（容许测量波动），优先要求明显改善；若下降 5% 以上，应先定位原因、做最小修复并重跑受影响项。
- 不允许任何一次出现**已确认的重复 Socket 发送、虚假 durable 成功、Journal/Spool 错账**；出现即停止进一步升级为 Pilot-ready。
- `at-most-once` 允许 crash gap / unknown，前提是有明确计数与可人工对账证据，且绝不自动重发。

这些阈值是本次工程验证的**建议门槛，不是已测事实或对外 SLA**；如果测试噪声较大，结论应为 `INCONCLUSIVE`，不能强行 PASS。

## 5. Phase 3 — 600 s 稳态及资源检查

仅在 Phase 2 的基础正确性通过、测试环境资源充分且已授权后进行；不触达真实 SMSC：

1. 使用与实际部署接近的资源 profile（若已有 **16 GiB** profile，沿用；不要擅自改变）进行至少 **600 秒**持续 synthetic 发送/响应试验。
2. 按固定时间间隔采集 Socket TPS、observed/durable response、queue_depth、reservations、backpressure、Projection lag、RSS/CPU、fd/thread 数和失败次数。
3. 验证 Journal writer 长期平均服务能力是否跟上正常事件产生速率；观察 `aggregate checkpoint` 的定期 `fsync` 是否制造可见的 Writer 卡顿。记录 `writer_oldest_age_ms` 峰值及随时间走势。
4. 实验结束后确保 durable backlog 清零或明确 `incomplete/unresolved`，Projection 追平或明确说明不能追平；记录重启后的持久聚合是否重复计数。
5. 如果 600 秒受内存、测试环境或时间因素无法执行，标记 `NOT_RUN` 或 `BLOCKED_ENV`；**不得用 1K/5K 测试外推为 600 秒 PASS**。

这项测试是后续**大规模放量**的关键证据。若用户明确选择在未完成 600 秒前做 1–2 Campaign 的受控 Pilot，报告应区分 `LIMITED_PILOT_CONDITIONAL` 与 `FULL_RELEASE_NO_GO`，明确风险和上线授权要求，不以工程师的默认决定代替业务决策。

## 6. Phase 4 — 1–2 Campaign Pilot 手册（只准备，不执行）

结合 `Docs/Marketing_Copilot_VNext_Pilot_CLI_Statistics_Guide.md`，新增/更新一页式运行手册，至少包括：

1. 上线前核对：目标 OS/文件系统、SHA/配置、Bind 在线状态、SMSC 连通性由**获授权的运维团队**确认、旧队列状态、磁盘余量、统计 root 路径、日志文件权限。
2. 仅选择 1–2 个低风险 Campaign；记录负责人、开始窗口、预期任务数、暂停条件。不得在脚本中内置真实号码/SMSC 账号密码。
3. CLI 启动和过滤示例；展示 **Socket PDU/s 与 Journal Durable PDU/s** 的含义、N/A 与 stale、Campaign accepted/written/resp/unknown、Projection 全局 caught-up。
4. 观察和**暂停新发送**触发条件：持续背压或 backlog 上升、writer error、unresolved、响应超时/unknown 异常、Socket TPS 异常低于预期、Worker 退出、Projection 长期不能追平。
5. 运营级暂停/恢复/取消操作及优先顺序（以源码/API 实际接口为准），带明确“暂停并不撤回已写入 Socket 的 PDU”。
6. 退出与对账：按 Campaign 核对任务/PDU、Journal durable、Response、Projection；逐 Campaign projected 当前为 N/A 时，标注需要使用其他已有受权审计证据，不能写成完整闭环。
7. 回退/故障处理：先暂停新发送并保存 runtime 与统计证据，再处理问题；**不得通过回滚程序重投已 checkpoint 的业务任务**；对 unknown/unresolved 单独清单人工处置。
8. 正式部署/真实短信投递需要另外的显式授权。任务执行完成时不能声称“已上线”。

**注意**：CLI 与监控可以只使用 Python 标准库；不得引入新的 Web UI 依赖。

## 7. 停止条件与最小修改规则

如果出现以下任一事实，停止相关负载/后续扩大执行并保存证据：

- 重复 Socket 发送；`checkpoint-before-socket` 被破坏；unknown 自动重发；原始业务表或 spool/journal 被错误修改。
- `durable` 在 fsync 失败后错误增长，或响应成功/失败统计与事件无法解释性对账。
- Writer 持续落后、队列耗尽且无法按设计恢复，或者出现长时间失控背压。
- CLI 观察行为造成显著 TPS 回退或运行目录被写入。
- 进程崩溃、SIGSEGV、内存不受控、系统资源超过测试主机安全限制。
- 所需远端执行权限/环境不存在：停止远端动作，报告 `BLOCKED_ENV`，不要偷偷替换主机或访问生产环境。

定位到真实缺陷时：必须先有最小复现、根因、before/after 证据；**只做最小修复**，运行相关测试和全量后端回归。若没有可复现收益或正确性问题，保留产品代码不变。

## 8. 交付物

按实际变更范围创建或更新：

1. `Docs/Marketing_Copilot_VNext_Linux_Throughput_and_CLI_Paired_Acceptance.md`  
   - 环境与 SHA，命令/参数，测试矩阵，每轮结果、配对统计、瓶颈判断及限制；截图可选，原始机器数据必需。
2. `Docs/Marketing_Copilot_VNext_Linux_Throughput_and_CLI_Paired_Acceptance.json`  
   - 机器可解析的逐轮指标、测试环境、证据路径和门禁状态；**缺失用 `null` + `NOT_RUN` / `BLOCKED_ENV`，不填虚假 0**。
3. `Docs/Marketing_Copilot_VNext_Limited_Pilot_Runbook.md`  
   - 1–2 Campaign 的 CLI 观察、阈值、暂停、重启与人工对账操作清单。
4. 若修改 CLI/快照：提交对应代码及专项回归测试；更新原 CLI 指南说明新旧速率含义。
5. 如果无法连接授权测试环境：产出本地已验证工作及一份**无需猜测参数即可在目标服务器执行**的 run script/操作命令，远端结果保留 `BLOCKED_ENV`；脚本必须默认隔离 synthetic loopback，绝不默认真实 SMSC。

JSON 至少包含下列机器字段（值必须真实反映测量）：

```json
{
  "task_id": "MC-VNEXT-P1-LINUX-THROUGHPUT-CLI-PILOT-20261019",
  "source_head": null,
  "target_os": null,
  "filesystem": null,
  "linux_env": "NOT_RUN",
  "cli_dual_rate": "NOT_RUN",
  "backend_regression": "NOT_RUN",
  "sles_1k": "NOT_RUN",
  "sles_5k_paired": "NOT_RUN",
  "cli_overhead_paired_percent": null,
  "new_vs_old_socket_tps_percent": null,
  "writer_600ms_reader_independence": "NOT_RUN",
  "backpressure_auto_recovery": "NOT_RUN",
  "campaign_isolation": "NOT_RUN",
  "steady_600s": "NOT_RUN",
  "rss_peak_bytes": null,
  "duplicate_confirmed": null,
  "unresolved_events": null,
  "per_campaign_projected": "N/A",
  "limited_pilot_decision": "NOT_EVALUATED",
  "full_release_decision": "NOT_EVALUATED",
  "production_deployed": false,
  "real_smsc_contacted": false
}
```

> 文件名中 `sles_*` 字段仅对真正的 SLES12SP5 目标测试生效；在 EulerOS/其他 Linux 上测试可另加 `linux_*` 字段，但 **`sles_*` 不得填 PASS**。仓库当前可能忽略 `Docs/*.json`；需要后续经授权提交验收 JSON 时使用 `git add -f`，不要擅自改变全局 `.gitignore`。

## 9. 验收输出格式

任务末尾请给出**可直接复制**的摘要，不能以一句 `IMPLEMENTED_VALIDATED` 掩盖未测项目：

```text
SOURCE_HEAD=<sha>
CODE_CHANGES=<NONE|MINIMAL_CLI_ONLY|MINIMAL_FIX_WITH_EVIDENCE>
CLI_SOCKET_VS_JOURNAL_RATE=<PASS|FAIL|NOT_RUN>
BACKEND_REGRESSION=<passed/skipped or NOT_RUN>
TEST_OS=<name/version or BLOCKED_ENV>
FILESYSTEM=<type or NOT_RUN>
SLES_1K=<PASS|FAIL|NOT_RUN>
SLES_5K_PAIRED=<PASS|FAIL|INCONCLUSIVE|NOT_RUN>
OLD_SOCKET_TPS_MEDIAN=<number or N/A>
NEW_SOCKET_TPS_MEDIAN=<number or N/A>
NEW_VS_OLD_SOCKET_TPS_PERCENT=<number or N/A>
CLI_OVERHEAD_PAIRED_PERCENT=<number or N/A>
WRITER_600MS_READER_INDEPENDENCE=<PASS|FAIL|NOT_RUN>
JOURNAL_BACKPRESSURE_AUTO_RECOVERY=<PASS|FAIL|NOT_RUN>
CAMPAIGN_PAUSE_RESUME_CANCEL=<PASS|FAIL|NOT_RUN>
AT_MOST_ONCE_NO_DUPLICATE=<PASS|FAIL|NOT_VERIFIED>
STEADY_600S=<PASS|FAIL|NOT_RUN>
RSS_16G_PROFILE=<PASS|FAIL|NOT_RUN>
PER_CAMPAIGN_PROJECTION=N/A_KNOWN_GAP
LIMITED_PILOT_DECISION=<GO_FOR_APPROVAL|CONDITIONAL|NO_GO|NOT_EVALUATED>
FULL_RELEASE_DECISION=<GO|NO_GO|NOT_EVALUATED>
REAL_SMSC_CONTACTED=NO
PRODUCTION_DEPLOYED=NO
COMMIT_PUSH=NO
```

**决策口径**：

- `GO_FOR_APPROVAL`：仅表示工程证据支持申请受控真实业务试运行，**不表示已获得生产发送授权**。
- `CONDITIONAL`：列明最少缺项、风险与不触碰的上线边界，不将其隐式视为 PASS。
- `FULL_RELEASE=GO` 必须有约定的完整性能、稳定性、恢复、资源与环境证明；未完成 600 s 或关键目标 OS 验证时通常不能给出全面放量 GO。
- 测试无授权/环境无法达到时，按已有证据交付可复现脚本与明确 `BLOCKED_ENV`，不得无期限继续探索或为了凑 PASS 扩大范围。

## 10. 成功标准（一句话）

**不再大改架构；用真实目标 Linux 数据证明 Socket 发送效率与 CLI 开销，用明确的状态/对账机制支撑 1–2 Campaign 受控试运行决策；任何未测结果如实保留。**
