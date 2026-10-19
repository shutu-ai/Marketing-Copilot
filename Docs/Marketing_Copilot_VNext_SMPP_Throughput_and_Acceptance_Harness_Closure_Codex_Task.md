# Marketing Copilot VNext — SMPP Throughput & Acceptance Harness Closure

> **Codex 任务编号：** `MC-VNEXT-SMPP-THROUGHPUT-HARNESS-CLOSURE-20261018`  
> **GitHub 仓库：** `shutu-ai/Marketing-Copilot`  
> **当前远端基线：** `2c847b55abe4eddfa9dfafba34ce207fa0847313`（执行时再次检查 `origin/main`；如有更新，记录实际 SHA）  
> **目标平台：** SLES 12 SP5，现有授权测试服务器，native ext4。Windows/WSL 只用于定向单测与小规模辅助验证。  
> **最终目标：** 修复验收测量失真，区分 TPS Credit、SMPP Window、Worker/Harness 限制与产品真实发送能力，并在服务器上取得可信的持续负载证据。  
> **核心原则：** 性能第一、数据正确、不重复发送、不过度设计；默认配置不随实验改变。

## 1. 为什么需要这轮任务

上一轮提交 `2c847b55` 已完成服务器真实运行，结论 `UPSTREAM_OR_HARNESS_LIMITED`。进一步优化 Journal Microbatch 没有证据基础，保留 `A0 + Projection batch 512`、单 Writer/单 DuckDB 是正确决定。

### 已证实的服务器数据（不得写成生产 SMSC 容量）

| 指标 | SLES 服务器，C0/50K Loopback |
|---|---:|
| E2E wall | 94.671 s |
| 发送 PDU / 收到响应 | 50,000 / 50,000 |
| Journal / Projection | 100,000 / 100,000 |
| 发送阶段平均速率 | 687.2 PDU/s |
| 以完整 wall 计算速率 | 528.2 PDU/s |
| 实际 Writer Batch P50 / P95 | 58 / 188（配置上限 512） |
| 最后响应 Journal durable → DB commit | 46.584 ms |
| 最后响应 Journal durable → offset durable | 58.512 ms |
| TPS credit wait P50 / P95 | 462.037 / 636.319 ms |
| SMPP window wait P50 / P95 | 126.006 / 334.362 ms |
| Producer backpressure waits | 0 |
| Journal backlog 结束值 | 0 |
| unknown / write_failed | 0 / 0 |
| 100K / 500K 独立 Writer | 8,757 / 7,821 events/s |
| T6 / 当前 T7 | PASS / 6 of 6 PASS |

关键异常：

- `5K` 运行使用默认 `projection-status-interval=1.0s` 时，**实际事件均已对账完成，却在 ~308s 触发 projection drain timeout**；改成 `0.1s` 后 ~12.657s PASS。需要修复状态新鲜度/停止条件的**测试工具缺陷**，而不是把 0.1s 当成永久工作绕过。
- SLES 上 E2E application/harness RSS 采样返回 `0`，不能将 0 当成真实内存。必须得到有效的多进程 RSS 与资源上限测量，否则内存 Profile 必须显示 `NOT_CAPTURED`。
- 4 binds × 200 TPS = **配置名义上限 800 PDU/s**。TPS credit wait 可能是**正常的限速遵从**；window wait 也可能反映响应速率、bind 调度或测试 SMSC 的限制，不能先验认定为实现缺陷。
- 测试服务器 OS 与目标 SLES12SP5 一致，但实际是有共享负载的 80-CPU/375-GiB 主机；这不是 8/16 GiB 目标硬件等价证据。

**结论：本轮不实施 Writer/Microbatch/PyArrow 优化，重点是测量、诊断和 SMPP 真实发送容量。**

## 2. 硬性约束和工作规范

1. 首先阅读 `AGENTS.md`。若存在 `.codegraph/`，使用 `codegraph explore`；保持既有 `dirty`、`untracked` 和前端锁文件的状态，不要清理他人变更。
2. **仅在现有授权与 VPN 可达的测试服务器**运行 synthetic loopback；不得连接真实 SMSC、生产数据库、生产 API、真实手机号或客户数据；不得修改系统全局参数、部署生产应用或请求 `sudo`。
3. 默认 SMPP TPS/Window、`RouteCreditBroker` 配额语义、真实通道契约、重试/unknown/at-most-once、跨日截止、Journal fsync、DB offset fsync、单 Writer/单 DB owner 全部不变。**严禁以牺牲防重发、数据持久性或真实线路配额来取得性能数字。**
4. 若要改变窗口/TPS，只能在**测试 Loopback** 使用明确的实验参数、独立 run root，标记 `SYNTHETIC_CAPACITY_EXPERIMENT`；绝不能合并成生产默认值。
5. 不能因为为了达到 800 而将合同 TPS 从 200/bind 提高，或绕过 credit broker。必须分别出示 `RATE_LIMIT_CONFORMANCE` 和 `ENGINE_CAPACITY` 证据。
6. 先可靠性修复、再基线诊断、最后条件式持续负载。任何无效、环境阻断或不存在收益的分支都允许有明确的 `NO_CHANGE_WITH_EVIDENCE`；不要为凑门禁运行无用的长跑。
7. **本任务不自动 commit/push。** 仅产出源码改动、测试与文档，检查 diff；显式要求后再提交。

## 3. 执行前冻结与可审计记录 R0

读取并对照以下已有证据（勿重写历史结论）：

- `Docs/Marketing_Copilot_VNext_Server_First_Performance_and_Platform_RCA.md`
- `Docs/Marketing_Copilot_VNext_Bounded_Journal_Microbatch_AB_and_Recovery.md`
- `Docs/Marketing_Copilot_VNext_Production_Profile_and_Canary_Decision.md`
- `Docs/Marketing_Copilot_VNext_Server_First_Evidence_Summary.json`

重点代码：

- `backend/tools/vnext_streaming_acceptance.py`：`_process_tree_metrics`、`_projection_caught_up`、`_sample`、完成条件/超时、`CountingLoopbackSMSC`、参数及返回 JSON。
- `backend/vnext/projection_writer.py`：`ProjectionWriterProcess.snapshot()`、status 文件发布/offset/replay。
- `backend/vnext/route_credits.py`：TPS credit 预留、8-credit lease、per-bind vs route-shared、公平性/绑定关系。
- `backend/vnext/smpp_transport.py`：window acquire/release、SMPP 响应 reader、异步 callback 和 pending。
- `backend/vnext/worker.py` 和 `backend/vnext/runtime.py`：route config、transport 数量、in-flight、生产配置映射。
- `backend/vnext/critical_path_profiler.py`：累积计数、分布、采样时间。

冻结 Git SHA、测试服务器 hostname/OS/kernel、Python 3.11.x / DuckDB 1.2.0、CPU/affinity、负载、可用磁盘、运行路径、依赖 SHA 与测试参数。服务器路径仅放在授权独立目录，保证原始结果可复现；报告绝不包含秘密。

## 4. R1 — 修复 Acceptance Harness 的两个可信度问题（P0）

### R1-A：修复 SLES RSS/CPU 采样为零

**已发现源码路径：** `_process_tree_metrics()` 执行 `ps -eo pid=,ppid=,pcpu=,rss=`；命令出错或无法解析时直接返回 `(0,0,0)`，从而产生表面上零内存。需在真实 SLES 上分辨是 `ps` 方言差异、输出格式、PID 树发现失败、进程生命周期竞争还是权限问题。不要从源码猜出唯一根因。

必须做到：

- 先保存 `ps` 原始输出、退出码、进程 PID/PPID 和采样时点的诊断（脱敏）；与 `/proc/<pid>/status`、`/proc/<pid>/statm` 或可用 psutil 做交叉验证。
- 实现**跨 SLES 与 WSL 可用**的采集方案，优先选轻量 `/proc` 或兼容 `ps` 方法；如果复用 psutil，必须确认目标 SLES 离线运行环境已有相容版本。禁止仅为测量强行改变生产依赖。
- 分别计算 `application-only` 与 `harness-inclusive` 的 RSS/CPU/进程数；过滤退出进程和重复 PID，防止包含其他测试进程。需区分 `RSS=0`（真实可能性极小）与 `NOT_CAPTURED`（采集不可用），不可再把后者作为 0 通过 guard。
- 增加 `sampler_ok`, `sampler_error`, `sampled_pid_count`, `application_rss_kib`, `harness_rss_kib`, `sampling_source`。数据不可信则容量 Gate 自动标记 `NOT_VERIFIED`，**不能因为无法测量就 PASS**。
- 添加采样器单测：标准 ps、SLES 兼容示例、格式缺项、一个 PID 消失、多子进程、命令失败，以及 Linux `/proc` 读取失败时的语义。
- 在授权 SLES 进行 1K/5K 运行，采集 RSS 时间线，交叉验证三个时间点；至少证明 Writer、Workers 和 Harness 等运行期进程都被正确纳入指定口径。

### R1-B：修复 Projection 已追平却被判超时

**现象：** 5K、`status interval=1.0s` 耗时 308s、最终全部对账而 `projection_caught_up_before_stop=false`；0.1s 可以通过。这是状态发布与父进程 stop-condition 的可信度问题，不应靠缩短刷新间隔掩盖。

必须做到：

- 绘制证据化的状态机：`feed finished → spool pending=0 → SMPP response complete → Journal durable → Projection DB commit → offset durable → caught up → stop`。
- 对照 `_projection_caught_up()` 的输入：status mtime/age、live child、Journal segment/offset，识别到底哪个信号滞后；检查 `status` 缓存和父进程 polling 是否会一直拿旧值。
- 让测试 Harness 使用**足够新鲜的、可靠的完成证据**。若状态文件陈旧，可触发已有的只读/受控刷新或重新检验 committed offset 与 Journal EOF；不要绕开“持久化 offset 已追平”这一真实完成条件。
- 严禁凭 `projection_events_applied == expected_count` 单独判 PASS；仍需核对 durable offset、final backlog=0、进程结束/Journal 状态。
- 将 `COMPLETED`、`INCOMPLETE`、`STALE_STATUS`、`SAMPLER_ERROR`、`TIMEOUT` 等明确区分；需要报告 `completion_detection_lag_ms` 与 `status_age_ms`。
- 单测模拟 `1.0s` 状态发布、慢刷新、Writer crash、Journal 末尾 partial line、stop 时 pending offset 未落盘；保证不误 PASS。
- **回归门槛：** SLES 上 5K `status interval=1.0s` 至少连跑 3 次，全部完整对账并在合理收尾时间内退出；0.1s 作为辅助 A/B，不再是唯一通过路径。重测旧 308s 异常并写 RCA。

> **R1 退出门槛：** 两项缺陷均得到证据化修复；RSS 真实有效；5K 使用正常 1s status cadence 稳定结束。否则停止后续“性能 GO”判定，输出 `HARNESS_NOT_TRUSTWORTHY`。

## 5. R2 — 分离 SMPP 限速、Window、生产者和模拟 SMSC（P0）

### R2-A：建立同时可观察的吞吐模型

按照 route / bind / worker 四个维度记录：

- `configured_tps_scope`、`configured_tps_per_bind`、`configured_tps_total_ceiling`、`bind_acquired`、`bind_active_time`、每个 bind 实际 PDU/s 和 idle/gap；
- Credit：申请量、获得量、实际消费量、剩余/浪费的 lease credits、refill 次数与成本、正常配额排队时间、锁/I/O 额外开销；注意 `CREDIT_LEASE_SIZE=8` 是设计约束不是自动的 Bug；
- SMPP Window：窗口配置、按 bind 实际 inflight/P50/P95/max、被占满时间比例、wait P50/P95、response latency/callback latency；
- Worker/Producer：产生、accepted、spool pending、worker task taken、背压、批次节奏、route 分配及每秒已发送事件；
- Loopback SMSC：接收和 response PDU/s、服务端响应队列/回调耗时、是否每个 bind 的同步 `await sleep` 形成头阻塞；
- Writer：Journal durable、Projection committed、backlog 的时间序列，证明它是否已离开瓶颈路径。

按阶段区分 `offered_load`, `admitted`, `socket_written`, `response_received`, `journal_durable`, `projection_committed`；所有速率必须以实际时间窗口为分母，不能用设置的 TPS 代替实测。

**不要把跨 worker 汇总的等待秒数（如 19,002s）直接理解为单个 E2E wall 的等待时间**；等待可并行，需以每进程分布与关键路径作判断。Window credit 在 `_read_responses()` 收到响应后释放，随后执行 callback；callback 可能延迟读取**后续**响应，但不能说它在当前响应 callback 之后才释放 credit。

### R2-B：严格区分 rate-limit correctness 与 engine capacity

先使用**生产等价的模拟配额**（例如 4 binds × 200 TPS，window 50）验证 broker 没有超额发射、跨 Worker 公平、重连 lease 行为无误；作为 `RATE_LIMIT_CONFORMANCE`。

随后另行在**授权 synthetic-loopback 隔离测试**中执行性能余量探测：在报告中显著标记 `TEST_ONLY_SYNTHETIC_HEADROOM`，使用较高模拟允许 TPS（例：每 bind 220/250；最终由安全的测试范围决定）或已验证的测试 route 配额，以判定引擎是否能处理 800 PDU/s。**这不是修改生产 SMSC 合同或产品默认限速**。

理由：设置 4×200=800 为名义上限时，调度和测量开销下持续**恰好达到** 800 不一定现实；若要测试引擎是否具有 ≥800 的处理能力，需要合法的 synthetic 容量余量。报告必须同时保存“合规限速场景”和“有余量性能场景”，不可偷换。

**最低、低成本 A/B：**

| 场景 | rate/window | 目标 | 结果性质 |
|---|---|---|---|
| S0 | 4×200、window 50 | 复现生产配额下实测 687/s 与等待原因 | 配额遵从基线 |
| S1 | 4×200、window 100（仅模拟） | 判定 response window 对吞吐的敏感性 | 测试窗口敏感度 |
| S2 | 4×220 或 4×250、window 50（仅模拟） | 观察引擎在有余量 TPS 下的极限 | 测试容量，不是生产配置 |
| S3（条件） | 同 S2、window 100（仅模拟） | 若 S1/S2 提示两者交互才运行 | 条件实验 |

先跑 5K，再对最有信息价值的两三组跑 50K；在一个服务器、相同 source/seed、相同资源 envelope、相似主机负载下**成对轮换运行**。先至少各一次筛选，有明确可复现的差异再做 3 轮配对。不要为了流程完整而做无价值的所有排列。

**退出决策：**

- 若 S0 正确遵守 configured 800，S2/S3 显著高于 800 且 Writer 跟得上，则归类 `CONFIGURED_TPS_CEILING_OR_SMSC_LATENCY`；不得“修复”正常限速。
- 若 S2/S3 仍达不到 800 且 Producer、Loopback 充足，则检查 credit refill、bind 调度、worker 线程/事件循环、callback 与网络 I/O 的独立成本；只对已证实的实际障碍做最小修改。
- 若 Loopback 接收/响应服务端自身限制速度，则归类 `HARNESS_LOOPBACK_LIMITED`，优先修复/隔离测试工具，**不动生产发送语义**。
- 若 50K 看似正常但丢包、unknown/duplicate/write_failed 出现，回到 correctness RCA；**绝不通过增加重发换取吞吐**。

## 6. R3 — 限定的源码变更与正确性验证（只有确认问题后才做）

优先只改 `backend/tools/vnext_streaming_acceptance.py` 及测试：resource sampler、stale-status/stop condition、必要的低开销采样和 diagnostic。

如确实查明 SMPP 生产代码存在**非预期**资源浪费，才允许对 `route_credits.py`、`smpp_transport.py`、`worker.py` 做最小、可单独回滚的候选实现，先在独立开关/测试范围内验证。不得变更真实 TPS 限额、窗口契约、unknown 无重发规则、bind lease 限制。

必须覆盖：

- 1K、5K、50K synthetic loopback 的 accepted、socket、PDU、responses、Journal、Projection、每 Campaign 精确对账；multipart 中 `task != PDU`，正确区分任务和 PDU；
- T6 business-day cutoff：旧日不再发送、跨日恢复无旧消息重发、统计不串日；
- 当前仓库 T7-4..9 六项 6/6；没有 T7-1..3 就明确写 `NOT_AVAILABLE`，无需无目的补编号；
- 断连/Window timeout 后 unknown 状态不自动重发，至少一次对应回归；
- Windows + WSL targeted/full backend pytest，服务器 native ext4 定向与 E2E；如时间或工具受限必须明确列出跳过项和原因；
- 新 sampler 与 stop condition 的回归测试以及 profiler OFF/ON 开销观测；避免每 PDU 文件 I/O 或昂贵扫描。

若修复 harness 而产品逻辑不变，报告注明 `PRODUCT_CODE_CHANGE=NONE`；修复测试工具不自动算作吞吐优化成就。

## 7. R4 — 600 秒 / 800 PDU/s 持续负载：有门槛才执行

执行前必须满足：

1. `R1 HARNESS_RELIABILITY=PASS`（RSS 非零有效、完成条件不误报）；
2. S2/S3 synthetic headroom 验证 Producer/Loopback 能**持续**供给目标，Writer 能跟上且无明显增长积压；
3. 服务器授权持续有效、磁盘空间足、主机负载允许、无真实系统影响；
4. 成功完成 50K E2E 正确性和主要 T6/T7 回归。

**测试定义：**

- 600s 的计时是稳定供给/发送区间（非把尾部 drain 加进平均吞吐掩饰问题）；记录总 socket_written、response_received、Journal durable、Projection committed，并以时间戳窗口计算。
- 理论目标至少 480,000 socket PDU（single-part、800 PDU/s × 600s）；需要相应 response 和实际生成的 Journal events，不得仅用 feeder accepted 计数；若不足，报告准确 `FAIL` 或 `HARNESS_LIMITED`。
- 每 10s 与 60s 聚合吞吐、Window usage、Credit wait、CPU/RSS、backlog slope；需要完整窗口速率及末尾完整对账，不得挑瞬时 p95 峰值代替 600s 平均。
- 若测试仍采用 `route_tps=200 × 4`，保留 `RATE_LIMIT_CONFORMANCE` 但不把 near-800 自动签成 `ENGINE_800_CAPACITY_PASS`。若用测试专用更高 ceiling，必须在全部结果明示 `TEST_ONLY_SIMULATED_HEADROOM`。
- 持续运行期间允许 Journal backlog 短期波动，但不得长时间单调发散；最后所有 committed offsets 应追到已完成 Journal EOF，backlog/queue/spool pending=0，重复、未知和异常按各自语义精确对账。
- RSS 报告分 application-only/harness-inclusive，带进程数、采样有效性、peak、P95 与最后 180s slope；若目标 8GiB /16GiB 的真实硬件或有效 cgroup 不可用，标 `HARDWARE_EQUIVALENCE=NOT_VERIFIED`，不因 375GiB 主机上的 6GiB 护栏虚报容量 PASS。
- 仅 Loopback 与 SLES 服务器验收通过，可签 `SYNTHETIC_SERVER_ACCEPTANCE=PASS`，但不自动等于真实 SMSC 与生产环境 Canary Ready。

如上述前置条件不足，则不浪费时间强跑 600s；给出具体阻断条件和最小后续动作。

## 8. 最小交付物与证据要求

放到 `Docs/`：

1. `Marketing_Copilot_VNext_Harness_Reliability_and_Resource_RCA.md`：RSS=0 根因与修复、status stale 根因与复现/对照证据；
2. `Marketing_Copilot_VNext_SMPP_Credit_Window_Throughput_AB_Report.md`：S0–S3 所有实际运行、限速遵从 vs 引擎余量、Producer/Window/Loopback 瓶颈判定；
3. `Marketing_Copilot_VNext_SLES_Sustained_Capacity_and_Canary_Decision.md`：50K、条件式 600s、部署规格、最终 GO/NO-GO 决策；
4. `Marketing_Copilot_VNext_SMPP_Harness_Closure_Evidence.json`：source+env SHA、测试参数、结果路径/hash、有效资源采样、真实速率、数据对账、门禁汇总。

大体量 raw Run、Journal、DB、日志、临时 venv 保存在隔离服务器目录或 `.tmp/`，不要提交；只有脱敏的摘要和必要元数据入库。

在最终审查中明确区分：`DEMONSTRATED`, `NOT_VERIFIED`, `BLOCKED_ENV`, `HARNESS_LIMITED`, `NO_CHANGE_WITH_EVIDENCE`；没有结果的指标输出 `null`，不能输出 `0` 冒充已测。

## 9. 终态判断与停止条件

| 结果 | 含义 | 是否改变生产默认 |
|---|---|---|
| `HARNESS_FIXED_ONLY` | 采样和完成条件修复；未证明产品吞吐问题 | 否 |
| `RATE_LIMIT_CONFORMANT` | 在真实配置配额下等待正常、没有违规 | 否 |
| `HARNESS_LOOPBACK_LIMITED` | 测试工具限制吞吐，需要限定修复 | 否 |
| `ENGINE_HEADROOM_CONFIRMED` | 测试专用余量下证明 800 能力 | 否，等待正式环境配置核对 |
| `PRODUCT_SMPP_OPTIMIZATION_CANDIDATE` | 非预期开销得到服务器 A/B 证明，正确性与限额均正常 | 仅提交最小候选，不自动启用 |
| `NO_CHANGE_WITH_EVIDENCE` | 不存在充分有价值的产品代码优化 | 否 |
| `BLOCKED_ENV` / `NO_GO` | 未完成可信持续负载/部署等价与必要条件 | 否 |

**任何**重复发送、at-most-once 失效、超额发送违反声明的路由合同、跨日误发送、Journal/Projection 持久化不一致、实测内存异常暴涨，应立即停止当前候选并给出 RCA；不为追求吞吐修复式重发。

## 10. Codex 最终输出卡（必须填写真实数据）

```text
SOURCE_HEAD=
CHANGED_FILES=
UNRELATED_WORKTREE_CHANGES_PRESERVED=YES|NO
TEST_SERVER_OS_AND_AUTHORIZATION=
TARGET_OS=SLES12SP5
R1_RSS_SAMPLER_ROOT_CAUSE=
R1_RSS_SAMPLER_VALID=PASS|FAIL|NOT_VERIFIED
R1_APPLICATION_RSS_PEAK_KIB=
R1_HARNESS_RSS_PEAK_KIB=
R1_STATUS_TIMEOUT_ROOT_CAUSE=
R1_5K_STATUS_INTERVAL_1S_THREE_RUNS=PASS|FAIL|NOT_RUN
R1_HARNESS_RELIABILITY=PASS|FAIL|BLOCKED
RATE_LIMIT_CONFORMANCE=PASS|FAIL|NOT_VERIFIED
S0_CONFIGURED_TPS_PER_BIND=
S0_CONFIGURED_TOTAL_TPS_CEILING=
S0_MEASURED_SOCKET_PDU_PER_SEC=
S0_MEASURED_RESPONSE_PDU_PER_SEC=
S0_CREDIT_WAIT_P95_MS=
S0_WINDOW_WAIT_P95_MS=
S0_BIND_UTILIZATION=
S1_WINDOW_SENSITIVITY=
S2_TEST_ONLY_TPS_HEADROOM=
S3_TEST_ONLY_WINDOW_HEADROOM=
PRIMARY_BOTTLENECK=CONFIGURED_TPS|WINDOW|WORKER|PRODUCER|LOOPBACK|CREDIT_IMPLEMENTATION|OTHER|UNKNOWN
SERVER_50K_CORRECTNESS=PASS|FAIL|NOT_RUN
JOURNAL_PROJECTION_BACKLOG_END=
T6_CROSS_DAY=PASS|FAIL|NOT_RUN
T7_CURRENT_6_OF_6=PASS|FAIL|NOT_RUN
R4_600S_COMPLETED=YES|NO
R4_600S_ACTUAL_SOCKET_PDU=
R4_600S_AVG_SOCKET_PDU_PER_SEC=
R4_600S_MIN_60S_RATE=
R4_600S_RESPONSE_COUNT=
R4_600S_JOURNAL_EVENTS=
R4_600S_PROJECTION_EVENTS=
R4_600S_APPLICATION_RSS_PEAK=
R4_600S_BACKLOG_TREND=
TARGET_MEMORY_HARDWARE_EQUIVALENCE=YES|NO|NOT_VERIFIED
PRODUCT_DEFAULT_CHANGED=NO
FINAL_DECISION=HARNESS_FIXED_ONLY|RATE_LIMIT_CONFORMANT|HARNESS_LOOPBACK_LIMITED|ENGINE_HEADROOM_CONFIRMED|PRODUCT_SMPP_OPTIMIZATION_CANDIDATE|NO_CHANGE_WITH_EVIDENCE|BLOCKED_ENV|NO_GO
CANARY_READY=YES_SYNTHETIC_ONLY|NO
COMMIT_PUSH=NOT_REQUESTED
```

---

**任务执行指令：** 先使测试工具可信，再在 SLES 上区分合规限速、Window 与真正的引擎容量；只有确凿证据才改变生产 SMPP 实现。禁止继续无依据地优化 Writer 或为跑出 800 PDU/s 偷换生产限额。
