# Marketing Copilot VNext — Server-First Performance & Bounded Micro-Batching

**Codex 任务 ID**：`MC-VNEXT-SERVER-FIRST-PERFORMANCE-MICROBATCH-20261018`  
**基线**：`origin/main=450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e`（执行前核对并冻结实际 HEAD）  
**目标**：以授权的、代表真实部署规格的 Linux **测试服务器**为主要性能评测场所，识别当前端到端吞吐瓶颈；仅在证据支持的情况下验证有限等待的 Journal 微批合并，并完成真实服务器 A/B 与必要容量验收。  
**默认不变**：单 ProjectionWriter、单 DuckDB owner、A0/vectorized + batch 512、逐事件 Journal durability、offset durability、event-id 幂等、at-most-once、跨天禁止重发。  
**禁止**：真实 SMSC、真实客户号码、生产数据库/生产部署、擅自连接未授权服务器、修改全局内核/系统配置、随意引入新依赖或多 Writer、擅自 commit/push。

---

## 0. 技术判断：本轮必须纠正的测试方法

**开发机负责**：单元与回归、故障注入、离线微基准、候选初筛；不能以 Windows/WSL 的微基准判定真实服务器吞吐或上线容量。

**测试服务器负责**：真实目标 OS/CPU/磁盘环境下的纯 Writer 与完整 SMPP loopback 吞吐、四 Campaign、公平 A/B、Journal/Projection 长稳、资源画像、进程恢复和 release gate。服务器若与生产规格不同，需在结论中明确 `PROXY_ENV` 或 `TARGET_ENV`，不能以其结果冒充等效生产环境。

**历史事实（全部是“已有证据”，不可伪造成本轮结果）**：

| 项目 | C0 A0/512 | C1 A0/1024 | 解释 |
|---|---:|---:|---|
| WSL 100K/10% 纯 Writer wall 中位数 | 11.7557 s | 8.4612 s | C1 改善 28.02%，不代表 E2E |
| WSL 500K/30% 纯 Writer wall 中位数 | 70.7893 s | 46.7601 s | C1 改善 33.94%，不代表 E2E |
| WSL 单组 50K E2E wall | 552.694 s | 484.376 s | C1 改善 12.36%；仅一对，不能外推 |
| Producer active | 324.979 s | 236.712 s | 上游可能影响全局 wall |
| SMPP drain（从 feed stop） | 216.289 s | 233.530 s | C1 更长 |
| Projection drain（从 feed stop） | 218.023 s | 236.828 s | 包含 SMPP 收尾，**非纯数据库 drain** |
| 最后响应至 Projection 追平的近似完成差 | 1.734 s | 3.298 s | 仅是终点差值，需以真实时间戳确认 |
| 实际 Writer batch P50/P95 | 24/68 | 24/66 | 配置 batch 512/1024，实际严重 underfill |
| Writer batch/commit 次数 | 3,454 | 3,465 | 100K events，均值约 29 events/batch |
| App RSS peak | 750,576 KiB | 732,988 KiB | 不是 8 GiB 硬件等效容量证明 |

另：A2 PyArrow 在隔离纯 Writer 中有收益，但不在已确认的生产离线 wheelhouse 中，本任务**不优先引入 Arrow**。

过去某报告记录 `EulerOS 2.0 SP15`、64 CPU 与 `staging≈98.1%`；最近接入的测试服务器实测 `SLES 12 SP5`、80 CPU，而其纯 Writer 100K 数据与 WSL 同数量级。**绝不假定二者是同一台机器**。必须核对 hostname/IP 对应的实际 OS、源码、依赖、资源和 workload；只有同源、同版本、同资源口径可以直接比较。

**最重要的新假设**：Journal 读循环发现第一批非空事件后即 flush，Producer/Worker 持续分散产出，造成高频小事务；但要先证明服务器上 Writer underfill 确实制约 E2E，而不能为了提高 batch p50 改变系统。

---

## R0 — 源码、测试服务器与部署目标冻结

1. 阅读根目录 `AGENTS.md`。如 `.codegraph/` 存在，先使用 `codegraph explore`；保持既有 dirty/untracked 文件及用户修改不变。
2. 冻结 Git SHA、source tree/测试脚本哈希、Python、DuckDB、NumPy、Pandas、现有配置，建立每个实验独立 Run root。未获得明确服务器授权/网络不可用时，只完成本地分析，远端 `BLOCKED_ENV`，**不得暗中改连生产设备**。
3. 确认真正的生产目标 OS：从部署脚本、部署包、运行文档和已确认需求判断。仓库已有 SLES12SP5 打包支持；如果生产实际是 SLES12SP5，不再把“不是 EulerOS”作为 SLES 生产验收的阻断项；EulerOS 旧事件单列为历史未复现。若目标确实是 EulerOS，SLES 只能作预验证，不得宣称目标平台 GO。
4. 在已授权服务器以只读方式记录 hostname、`/etc/os-release`、kernel/glibc、CPU 数与实际 affinity、`nice`/`ionice`、`free -h`、`df -T`/mount、可读 cgroup、CPU quota、磁盘使用率、当前负载、Python/duckdb freeze。记录同一台服务器是否在其他任务下繁忙。
5. **公平资源 Envelope**：匹配规划的生产 CPU、进程、内存、磁盘权限；不要机械沿用旧 `taskset -c 0-3; nice +10; ionice idle`。若需模拟 4 核受限情况，应作为第二套独立对照并保留真实配置。80 CPU/375GiB 测试服务器的 6GiB RSS guard 不能证明 8GiB 物理机性能与页缓存行为。
6. 不使用 sudo、不修改全局 sysctl、cgroup 或其他进程。无法在非 root 权限构建 8/16 GiB 等效环境则准确标记 `HARDWARE_EQUIVALENCE=NOT_VERIFIED`。

**产出**：`R0_server_environment.json` 与实际部署对应关系；不明处显式 UNKNOWN。

## R1 — 在服务器测量**未修改**的 C0 基线

### R1.1 正确性预检

- 在隔离服务器目录用现有 `backend/tools/vnext_streaming_acceptance.py` 运行 1K/5K synthetic loopback，确认 4 Campaign、各自计数、PDU/response、Journal、Projection 精确对账。先阅读 `--help`，不要猜测 CLI flags。
- 再做服务器 C0（A0/512）50K baseline；初期 1 次即可，未证实候选前**无需重复六轮 50K**。
- 记录实际供给/发送/响应吞吐，不用 `route_tps×pool_size` 当实测速率；single-part 任务与 PDU、progress events 分别计数（约 2 progress events/PDU），multipart 单独对账。

### R1.2 完整时间线与三类 Drain

采集单调时钟事件：feed start/stop、last task taken、last socket write、last response received、last response Journal durable、last projection DB commit、last projection offset durable、Journal EOF caught up。

- `SMPP_POST_FEED_DRAIN = last_response - feed_stop`。
- `PROJECTION_POST_FEED_DRAIN = journal_caught_up - feed_stop`（保留与历史报告的可比口径）。
- `PROJECTION_POST_RESPONSE_TAIL = max(0, journal_caught_up - last_response_journal_durable)`：用于评估只剩 Projection 追平所需的尾部时间；若缺乏真实时间戳，填 `NOT_MEASURED`，不可用相同起点的两项 drain 直接当作数据库真实运行时间。

另外采集：Producer accepted/s 与 backpressure、Spool backlog、SMPP TPS credit/window wait、socket PDU/s、response/s、Journal fsync P95、Journal durable events/s、Projection committed events/s、Journal backlog bytes/events 随时间变化。

### R1.3 小批次是否真实限制吞吐？

- `batch P50/P95/P99`、batch/second、实际读取事件数、Journal 逐轮可读事件数、每次 commit 前 backlog、batch 填充率、每事件 SQL/commit/offset/status 成本。
- 特别区分：是“其他 Journal 尚未有事件”，还是“已有数据但 reader 在扫描第一个非空 batch 后直接提交”，抑或“上游/PDU 处理速度过慢”。
- CPU（按 Producer、Worker、Writer 角色）、I/O wait、RSS、磁盘空间。明确哪些指标采集不到。
- **若 Writer 大部分时间追平且最后响应到 Projection EOF 的时间短，而 Producer/Window 限制 PDU/s，则先诊断上游或测试 harness，不允许假定 Writer underfill 是主瓶颈。**

**R1 判定字段**：`PRIMARY_BOTTLENECK=PRODUCER|SMPP|JOURNAL|WRITER_SMALL_BATCH|DUCKDB_STAGING|I/O|UNKNOWN`、`WRITER_BATCH_UNDERFILL_ON_SERVER=YES|NO|UNKNOWN`、证据和瓶颈占用率。

---

## R2 — 有依据时才试验 Journal Reader 有界微批合并

**触发条件**：服务器 R1 同时证明 Writer 存在小批次且它对提交次数、backlog/响应后尾部或 E2E 吞吐有实质影响。否则明确 `NO_CHANGE_WITH_EVIDENCE` 或 `UPSTREAM_OR_HARNESS_LIMITED`，跳过本阶段代码修改。

### R2.1 最小实现规范

在 `backend/vnext/projection_writer.py` 的 journal read/flush 路径进行小改动，采用 `journal_batch_coalesce_ms` 之类**默认 0ms** 的实验开关，原 512/A0 路径不变。

- 仅在已有第一批可读事件且未达到 batch 上限时，允许**非阻塞/受控限时**短轮询其他 Journal。实验候选最大等候 `0/10/25/50ms`；不要无依据写死 100ms 或等待填满整个 512。
- 截止条件：batch 满、deadline、stop/shutdown、无可读数据、读取异常或已有恢复条件；无数据时不 busy spin、不制造低负载状态延迟。
- 每 Journal 继续使用公平轮转和 bounded quantum，避免小 Campaign 被热 Campaign 饿死；只保留有界事件列表。
- 本批读取必须维护**暂存 segment offsets**；对同 segment 下一次读取基于暂存 offset 防重复，不能提前提交全局 durable offsets。仅在 DuckDB commit 成功后才 fsync offsets，之后才允许 reclaim。异常/崩溃从最近的已持久化 offset 幂等恢复。
- 绝不移除逐事件 Journal fsync、Journal 状态、event-id 去重、SMPP unknown 与不重发规则，也不增建多 Writer/DB。
- 如证明固定成本来自 status/diagnostic/segment 扫描，可只做有证据的局部低频化；禁止重新引入曾失败且未有因果证据的 segment-path-cache 重构。
- 低开销计数：实际合批等待时间 P50/P95、每次合并轮数、读到的新事件数、batch P50/P95、commit/s、CPU，默认 profiling OFF。

### R2.2 Server 小规模预筛（不先做大规模长跑）

| Arm | Staging | 上限 | max coalesce wait | 用途 |
|---|---|---:|---:|---|
| B0 | A0 | 512 | 0 ms | 生产基线 |
| B1 | A0 | 512 | 10 ms | 最低等待代价 |
| B2 | A0 | 512 | 25 ms | 中等候选 |
| B3 | A0 | 512 | 50 ms | 预设实验上界 |
| B4（仅条件满足） | A0 | 1024 | 优胜窗口 | 仅真实 fill 接近 512 后才试 |

每臂先完成 5K/10K 服务器 loopback、相同 workload/seed/CPU/I/O 优先级，严格比较实际发送速率、Projection 速率、响应尾部、Journal backlog 和资源；不是单纯比较“batch P50 变大”。没有稳定改善即停止，不跑大量 50K。

若其中一个候选有明确收益，再仅对胜出者 Bx 与 B0 运行**三轮成对 50K E2E**，运行次序交错减少噪声，并归档所有失败实验。

---

## R3 — 完整 E2E、故障恢复和决策

### R3.1 50K 端到端门禁

- 与 B0 同时验证 4 Campaign，任务、socket_written、PDU、submit_sm_resp、Journal durable events、Projection committed、每 campaign 统计，全部精确对账。
- 上述三个 drain、Producer active、真实 socket PDU/s、Journal events/s、Projection events/s、batch P50/P95、commit/s、CPU/RSS、backlog 时间序列都要保留；绝不将总 Wall 的偶发改善归因于特定 Writer 改动。
- **推广门槛**：三轮同 server paired median，E2E wall 或真实完成吞吐获得稳定、可解释的约 **≥15%** 改善，Projection post-response tail/backlog 不实质恶化，响应质量和 at-most-once 完全保持。不达标则不切生产默认。

### R3.2 重点安全和恢复验证

- 当合并后事件还未进 DB 就 kill Writer；事务提交后 offsets 尚未落盘、partial JSONL line、正常 stop、重启、GC、跨天切换；检查补偿重放幂等、无重复发送、无遗漏 response 事件。
- 原有 T6 business-day cutoff（旧日不重新派发、统计不串日）必须通过。
- 当前源码仅定义 `T7-4...T7-9` 共六个场景：严格报告 `T7_CURRENT=6/6`；针对新的合并窗口补充其特有 crash case，但不谎称历史 T7-1...3 已存在，也不为凑 9/9 增加无关实现。
- Windows 和 native-ext4 backend 检查、E2E smoke、multipart 都通过之后才能讨论 candidate ready。
- 如发生事件重复、缺失、响应窗口超时、Journal capacity/offset 失配、内存持续增长、跨日重发，立即停用实验开关并保留基线。

---

## R4 — 真实服务器容量验收（有条件执行）

**仅当 R3 有真实 E2E 收益且 Producer/SMPP 能供给目标负载时运行**；否则应输出 `NO_CHANGE_WITH_EVIDENCE` 或 `HARNESS_LIMITED`，不要仅为了达成任务形式而耗费 600s 长跑。

- 单段目标：**实际 >=800 socket PDU/s 持续 600s**，对应约 **1600 durable events/s**（按真实任务事件语义调整）；应以服务端计数、整段平均和分钟窗口判定，不以瞬时 Max 或配置 TPS 代替。
- Writer 持续 committed events/s 应跟上 Journal 生成速率，不得全程 backlog 单调发散、也不允许无限延迟到停发后才追平；最终 Journal EOF、Projection、SMPP responses 全部 reconcile。
- 如 Producer/harness 无法持续产生真实 800 PDU/s，要独立定位并记 `HARNESS_LIMITED`，先完善 benchmark 供给能力，**不可判产品 Writer 失败或成功**。
- 资源门禁根据**实际计划的生产 CPU/内存/磁盘/OS**冻结：`8 GiB host → application RSS ~6 GiB` 可作参考预算，`16 GiB host → ~12 GiB` 可作模型；若仅是测试服务器进程级 RSS guard，不得报告真实 8/16 GiB 等效验证。能在现有授权下用隔离容器/cgroup 实施可信限额方可签署该规格。
- 真实生产目标若为 SLES12SP5，则该平台签署 test-server gate；若为 EulerOS，SLES 的通过不能转换成 EulerOS target GO。没有真实 SMSC/生产部署时，最高只能得出 `SYNTHETIC_LOOPBACK_CANARY_READY`。

---

## R5 — 输出（最多四份报告，不新增非必要系统）

放在 `Docs/`：

1. `Marketing_Copilot_VNext_Server_First_Performance_and_Platform_RCA.md` — 服务器身份与部署对应、完整基线、BottleNeck、服务器 vs WSL差异、真实 batch p50 和三种 drain。
2. `Marketing_Copilot_VNext_Bounded_Journal_Microbatch_AB_and_Recovery.md` — 假设、候选最小实现、Server A/B、故障注入/跨日结果、是否保留代码。
3. `Marketing_Copilot_VNext_Production_Profile_and_Canary_Decision.md` — 目标资源、600s gate、完整对账和剩余阻断项。
4. `Marketing_Copilot_VNext_Server_First_Evidence_Summary.json` — source/env freeze、原始 run SHA/path、性能指标、资源口径、机器结论（不含凭据和敏感数据）。

**结束状态只选择证据支持的值**：`NO_CHANGE_WITH_EVIDENCE`、`CONFIG_ONLY_CANDIDATE`、`BOUNDED_MICROBATCH_CANDIDATE`、`UPSTREAM_OR_HARNESS_LIMITED`、`BLOCKED_ENV`、`NO_GO`。禁止编造 PASS；不自动部署、commit、push。

### End card（真实字段，不存在填 NOT_VERIFIED）

```text
BASELINE_HEAD=
TARGET_DEPLOYMENT_OS=
TEST_SERVER_HOST_OS_KERNEL=
SERVER_REPRESENTATIVE=YES|NO|PARTIAL
SERVER_AUTHORIZED=YES|NO
RESOURCE_ENVELOPE_CPU_MEM_IO=
SOURCE_AND_DEPENDENCY_FREEZE=
SERVER_C0_50K_WALL=
SERVER_C0_PDU_PER_SEC=
SERVER_C0_PROJECTION_EVENTS_PER_SEC=
SERVER_C0_BATCH_P50_P95=
PRODUCER_LIMITED=YES|NO|UNKNOWN
SMPP_WINDOW_LIMITED=YES|NO|UNKNOWN
WRITER_UNDERFILL_IS_BOTTLENECK=YES|NO|UNKNOWN
WINNING_ARM=NONE|B1|B2|B3|B4
PAIRED_50K_MEDIAN_E2E_DELTA=
PROJECTION_POST_RESPONSE_TAIL_DELTA=
COMMIT_COUNT_AND_BATCH_DELTA=
JOURNAL_BACKLOG_END_AND_TREND=
SMPP_AND_RESPONSE_RECONCILIATION=
UNKNOWN_OR_DUPLICATE_PDU=
OFFSET_AND_CRASH_RECOVERY=
T6_CROSS_DAY=
T7_CURRENT_6_OF_6=
APP_RSS_PEAK_AND_SLOPE=
HARDWARE_EQUIVALENCE=VERIFIED|NOT_VERIFIED
SUSTAINED_600S_800_PDU=PASS|FAIL|HARNESS_LIMITED|NOT_VERIFIED
PRODUCTION_DEFAULT_CHANGED=YES|NO
FINAL_DECISION=
CANARY_READY=YES_SYNTHETIC_ONLY|NO
COMMIT_PUSH=NOT_REQUESTED
```

**完成标准**：解决“真实服务器究竟哪里慢”的问题；如果 Writer 小事务确为瓶颈，用最小有界合批在服务器 E2E 证明收益。若瓶颈在 Producer/SMPP 或测试工具，就停止 Writer 优化，不将 WSL 的微基准快慢视为生产结论。
