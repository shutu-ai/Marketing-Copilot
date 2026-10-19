# Marketing-Copilot VNext — SLES 严格同条件源码 A/B 与 SMPP 吞吐根因闭环

> **Codex 执行任务** · `MC-VNEXT-SLES-STRICT-SOURCE-AB-SMPP-ISOLATION-20261018`  
> **任务目标**：解释 SLES 测试服务器上，同为 50K PDU 的完整 E2E 平均吞吐为什么从 **528.2 PDU/s** 降至约 **162–169 PDU/s**；通过可复现、单变量、服务器本地独立运行的实验，将原因定位为**源码/Harness 变化、实际配置差异、主机负载/I/O、Loopback SMSC、Bind 生命周期或其他明确路径**，而不是继续猜测或盲目优化。  
> **当前主线**：`origin/main = 2579d8d26c1881b42d3c480fe3056e73b982492e`（执行时先核对 SHA，不可盲信）。该提交只含 3 份网络故障/复测证据文档，产品代码与 `3e73ab8030b46b78e109cbf1f23e330c72d2d136` 相同。  
> **历史快轮版本 A**：`450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e`。  
> **历史慢轮版本 B**：`3e73ab8030b46b78e109cbf1f23e330c72d2d136`。  
> **最终默认**：继续 A0/vectorized、Projection batch 512、单 Writer/单 DuckDB；不改变生产 TPS、SMPP window、Journal fsync、offset、at-most-once 或跨日不重发规则。

## 0. 必读与执行原则

1. 阅读仓库根目录 `AGENTS.md`。存在 `.codegraph/` 时先运行 `codegraph explore` 定位相关符号，再查源码。不得覆盖既有 dirty/untracked 文件；**不自动 commit/push**（需要用户明确批准）。
2. 先查阅并复核以下现有证据，不重跑已充分证明的 Writer 微基准：
   - `Docs/Marketing_Copilot_VNext_Server_First_Performance_and_Platform_RCA.md`
   - `Docs/Marketing_Copilot_VNext_SMPP_Credit_Window_Throughput_AB_Report.md`
   - `Docs/Marketing_Copilot_VNext_Harness_Reliability_and_Resource_RCA.md`
   - `Docs/Marketing_Copilot_VNext_SLES_Throughput_Recheck_and_Comparison.md`
   - `Docs/Marketing_Copilot_VNext_Network_Incident_and_SSH_Process_RCA.md`
   - `Docs/Marketing_Copilot_VNext_Network_Performance_Recheck_Evidence.json`
3. **所有量化性能实验在已授权的 SLES 12 SP5 测试服务器本地完成**。Windows/WSL 仅进行源码静态 diff、单测与故障复现；不得拿 WSL 吞吐替代 SLES 基线。
4. 仅限 Synthetic/Loopback，测试号码、隔离数据库、隔离 Journal 和独立 Run root。**严禁接入真实 SMSC、客户号码、生产 API/DB、生产部署或修改共享服务器系统配置。** 使用已授权连接；不可把凭据、密钥或私有服务器认证信息加入报告。
5. 测试的生产数据面必须不依赖 Windows VPN/SSH 的持续在线。使用远端 `nohup`/`setsid` 等已有允许的方式运行，记录真实 PID/PPID、start/end UTC、exit、result SHA。SSH 只负责启动和读取结果。
6. 性能一旦证明为 Host/Harness 限制，优先停止生产代码修改。任何“优化”必须有明确且可复现的瓶颈，不允许用不相关的安全层、重试、复杂调度器、额外数据库、多 Writer 解决。
7. 只因实验确证需要修改代码时才产生最小 patch；所有候选先实验性启用，生产默认保持不变。绝不能用**提高生产 TPS、无限放大 Window、降低 Journal durability、允许自动重发、少记响应/事件**换取数据上的提速。

## 1. 已知证据与目前不能成立的结论

### 1.1 历史与最新 50K 结果：必须使用相同分母

| 运行 | Git SHA | Workers | Status interval | Instrumentation | 50K campaign 分布 | 完整 E2E Wall | 完整 E2E PDU/s | feed 阶段 PDU/s |
|---|---|---:|---:|---|---|---:|---:|---:|
| 历史快轮 | `450e84d8` | 8 | 0.1s | ON | 12,500×4 | 94.671s | **528.2** | 687.2 |
| 历史慢轮 | `3e73ab80` | 4 | 1.0s | ON | 12,500×4 | 308.882s | **161.9** | 181.257 |
| 最新均匀对照 | `3e73ab80` | 8 | 1.0s | OFF | 12,500×4 | 308.829s | **161.902** | 183.528 |
| 最新正式复测 | `3e73ab80` | 8 | 1.0s | OFF | 20K/20K/5K/5K | 296.301s | **168.747** | 183.768 |

**严禁将历史快轮的 687.2（feed rate）与慢轮 169（完整 E2E 或 loopback receive）直接当作同口径退化。** 全程 E2E 对比是约 528 → 162–169 PDU/s；feed/loopback 各自单独比较。

网络 RCA 已发现 Windows VPN 故障不覆盖历史慢轮，服务器 detached + localhost Loopback 复测仍慢；`NETWORK_EVENT_NOT_CAUSAL` 可保留，但**慢性能的实际根因仍为 `UNKNOWN/INCONCLUSIVE`**。不再围绕本机 GitHub 443 做无意义的网络排查。

已有结果均正确对账：50,000 accepted/socket/response、100,000 Journal/Projection、unknown=0、duplicate=0、backlog=0；T6 和现有 T7-4..9 6/6 历史通过。**不要为了填验收矩阵反复跑 T6/T7；仅在实际产品行为改动时重跑相关定向测试。**

### 1.2 需要优先证明/否定的假设

- **H1 源码回归**：`450e84d8 → 3e73ab80` 的观测改动不一定免费；新增 Credit、Transport、Worker 计数/快照或 Harness 完成条件可能改变热路径或 Worker 生命周期。
- **H2 Harness 差异**：status interval 0.1→1.0、instrumentation ON/OFF、workers 8→4、campaign 分布、过程启动方式造成看似“性能回退”。
- **H3 SMPP/Bind**：新正式 50K 记录 **182 次 Bind 生命周期，最大同时 4 Bind，且无 reconnect error**；可能存在频繁建连/释放/worker pass churn 或 Bind Lease 等待，需用逐阶段时间而非单一计数判断。
- **H4 Loopback 服务/窗口**：Window wait P95 在新 5K ON 诊断约 1.83–2.26 秒，而响应 callback P95 仅约 0.8–0.9 ms。需调查 loopback 响应排队、每连接串行处理、SMPP reader 调度、window 实际占用及延迟来源。
- **H5 Host/存储**：共享 80 logical CPU / 375 GiB SLES，loadavg 随时间波动、native ext4、上一轮约 4.3 GiB 可用；历史**未采到 iowait/await**。不得仅凭 `loadavg` 认定磁盘/主机是根因。

## 2. R0 — 冻结两份可复现源代码及公平测试条件（先做，不修改源码）

### 2.1 精确 diff 与装载隔离

1. 用 `git diff 450e84d8 3e73ab80 -- backend/vnext backend/tools/vnext_streaming_acceptance.py backend/tests` 定位所有 runtime 和 harness 变化（可生成 diff 摘要但不要以修改文档替代代码检查）。
2. 特别检查 `route_credits.py` 的 `acquire`/refill/Lease/lock、`smpp_transport.py` 的 send/reader/window/callback、`worker.py` 的 `run_until_idle`、transport 创建/关闭和 Worker 重用，及 `vnext_streaming_acceptance.py` 的 loopback/server、计时、进程拆装与停止条件。
3. 准备两套**互不污染的源码快照**（A=旧，B=新）；强制冻结两边的源码包 SHA、`PYTHONPATH`、Python/glibc、`duckdb`/其余运行依赖及实际入口点；**不要把 A 的后端模块意外地 import 到 B 进程**。各自使用隔离 Run root/数据库/Journal/credit leases/port。
4. 公平运行配置**完全一致**：workers=8、pool/binds=4、tps_scope=`per_bind`、route_tps=200/bind、SMPP window=50、producer batch=250、Projection A0/vectorized batch512、4 campaigns、response-delay=0；相同 seed 和测试号码生成方式。
5. 将 `projection-status-interval=0.1`、`instrumentation=ON` 作为**对齐历史快轮**的共同首选配置。若旧版命令行/构造器无法支持，采用最小**测试驱动兼容适配**，在报告记录不同及其可能影响，不修改旧产品源码来“凑兼容”。必要时另测两边的 1.0s status 参数对照；不能将不等价测试判为 strict A/B。
6. 首轮 5K `1250×4`，后续 50K `12500×4` 严格对应历史快轮；`20K/20K/5K/5K` 只能作为**额外 campaign skew 敏感性实验**，不能混入主要 A/B。
7. 冻结 CPU/affinity/nice、可用内存、运行目录文件系统、process start method、status/profiler、feed interval、max pending 等。**不要未经说明引入 taskset 0-3 / nice+10**。所有变量及操作系统实际值都写入机器 JSON。
8. 实验运行前后记录 `hostname`、`/etc/os-release`、kernel、Python/venv、内存、`df -T`/空闲量、`loadavg`、`/proc/stat` CPU/iowait、进程数、服务端本地时间；如果 `iostat` 可用才额外记录 await，不新增系统依赖、不用 sudo。
9. **磁盘安全门禁**：按上次 50K 的实际 Run 增量估计峰值+预留；如果剩余空间不能覆盖所有隔离 Run 与安全余量，停止长跑而不是占满共享 `/`。不可清理用户或其他人员数据。

**R0 交付**：`SOURCE_A_SHA`/`SOURCE_B_SHA`、模块 diff 清单、统一环境 JSON、可重复的远端独立启动命令、旧/新不兼容点。若源码或运行环境无法等价，标 `AB_COMPARABILITY=BLOCKED`，不得产生因果结论。

## 3. R1 — 服务器小样本「严格版本 A/B」(核心任务)

### 3.1 分级运行，避免无意义 50K 长跑

1. 每个版本先 1K smoke：complete/response/Journal/Projection 完整对账、无未知与重复；若旧版本 Harness 有历史资源采样缺陷，标记 `RSS_NOT_COMPARABLE`，不可把历史零采样当有效。
2. 在同一 SLES 节点对 **A (450e84d8)** 和 **B (3e73ab80)** 进行 **5K 配对**。建议先 `A→B→B→A`（两个配对循环），若结果差异明显但主机负载不稳定，再加一个反向配对；每个测试 Run root 独立，不能多个大实验同时竞争磁盘和 CPU。
3. 每对固定环境和参数，记录从第一 socket write 到最后 receive、feed active、完整 wall、最后 response durable→Projection catch-up；**同一种吞吐指标必须使用相同起止规则**。不能只比较两份 Harness 输出字段名而不核实统计语义。
4. 每一轮记录每个 bind 的 `connected_at`, `bind_slot`, `close_at`, `received`, `response`, `inflight/window peak`, `lease_wait`, `reconnect_count`；记录 Worker pass 数、transport 创建/关闭数、active binds 的时间积分和各 bind 的 PDU/s。
5. 从稳态 5s/10s 分段中计算 socket-write/s、loopback-receive/s、responses/s、Journal/Projection events/s、Window wait、Credit wait、响应回调耗时、producer pending/backpressure。不要用测试配置 TPS 代替实测速率。
6. 样本都正确对账、服务器负载差异可接受，且出现**稳定的两版本差异**才继续 R2；若两边都慢，则优先独立测试 Loopback/Host；若两边都快，则寻找旧实验特殊环境条件，不需要改产品代码。

### 3.2 只在短跑定位不清时做交叉验证

实验需逐步回答以下问题，不必强行全部执行：

- **Status interval 对照**：在两版本一致的参数下仅切换 0.1/1.0 秒。验证 1 秒状态刷新只影响检测/输出还是 Worker/DB 的关键路径。
- **Instrumentation 成本**：同代码、同配置的 ON/OFF 小样本配对，不以不同进程生存期的简单平均臆测 overhead。
- **Workers 4/8**：在匹配的版本/参数下对照。若 4→8 的收益显著则定位 Worker/pass/bind 调度，不把影响归因于 SMPP window 本身。
- **Harness/产品交叉**：如果 A 快、B 慢且静态 diff 指向 Harness 观测路径，优先使用隔离的最小兼容桥进行 `OLD product + common harness` vs `NEW product + common harness`。桥仅用于实验，不得在测试中改坏语义。无法兼容就报告 `HARNESS_VS_ENGINE_ATTRIBUTION=UNRESOLVED`，不强行写因果。
- **历史成功运行原始配置**：复核历史 fast/slow 的 source/archive SHA、started/ended、CPU/priority、Campaign/route、bind 活跃记录和其他运行时差异；不可把文档声称的“同条件”当作事实。

**R1 停止规则**：若 5K 出现任一 duplicate/missing/unknown、SMPP error、资源上限或无法独立停止，终止该候选。若 A/B 受共享负载淹没无法比较，结论是 `ENV_NOISY`，不跑更多 50K。

## 4. R2 — 独立 SMPP Loopback 容量与路径分层隔离（有条件执行）

如果 A/B 均慢，或者 5K 指向 bind/window/loopback 而无法归因，使用**只在测试目录内的独立 benchmark**隔离：

| 实验层级 | 模块 | 目的 | 不允许省略的证据 |
|---|---|---|---|
| L0 | 纯 Loopback SMSC + 精简合规 SMPP client | 证明模拟器独立收发能力与每-bind 延迟 | binds=4、每 bind PDU/response、latency、窗口实际占用 |
| L1 | L0 + 生产 RouteCreditBroker（保持 4×200） | 定位 credit grant/refill/lease 成本和正常等待 | credit wait、refill成本、未超额发送 |
| L2 | L1 + SMPPTransport/window/response reader | 区分 transport 和 callback/reader 的开销 | Window wait、每-bind pending、响应 callback耗时 |
| L3 | 完整 Worker/Spool/Journal/Projection | 定位引入完整业务链路之后的增量 | 同口径 rate、Journal/Projection、完整对账 |

1. 精简 client 必须使用有效的 SMPP Bind、submit_sm 和响应语义；不能用裸 TCP bytes/sec 冒充 SMPP 容量。Loopback simulator 不可虚造响应成功，全部 PDU/响应计数可对账。
2. 先定位**同一连接上的响应是否被串行化或 sleep 阻塞**；统计每 Bind 的 response latency、window occupancy、Write/Drain、响应队列长度。如果确有 simulator 缺陷，仅修**测试 simulator**并用独立合规 client 验证；不得通过绕过 Worker 正确性来声称 E2E 已达标。
3. 单独观测 **频繁 Bind 生命周期**：历史新版本 50K 可达 182 次 bind lifecycle / max active=4 / reconnect=0。定位新建/销毁发生在哪一层和什么阶段，有无本可复用的长连接；必须分清“正常 Worker pass 结束释放”与“不必要的 churn”。如是 Harness Worker 反复启停，在 Harness 修复时保证本来需要的响应 drain 不被缩短。
4. 不预先认定 Window 是根因。Window wait 是症状，必须解释是 response service rate、线程/协程串行化、event-loop 饥饿，还是正常限速形成的排队。
5. 默认 `4×200 TPS` 的生产限额不变。若为了测试 headroom 使用 `4×250`，**只允许 isolated synthetic mode** 且清晰标记 `TEST_ONLY`，不可据此自动建议生产提高限额。
6. 如 Host 性能变动明显，在稳定负载窗口重复最小 L0/L1 对照并采集 CPU/iowait 与磁盘指标；不能凭单次负载高就宣布 `HOST_IO_ROOT_CAUSE=CONFIRMED`。

## 5. R3 — 必要时验证 50K，闭合最终因果链

只有出现可复现的 A/B 差异或明确独立瓶颈时继续：

1. 先采用 **5K A/B → 50K 对照** 的最小成本路径。正式 50K 至少 A/B 同环境配对一次；关键结论波动较大时重复至 3 对中位数。总实验量以能明确区分原因即可，避免机械长跑。
2. 统一 `counts=12500×4`、workers=8、pool=4、200 TPS/bind、window=50、Producer batch=250、status=0.1s、instrumentation=ON（如兼容且两边一致）、writer=A0/512。
3. 对比 `E2E wall PDU/s`、`feed PDU/s`、`loopback receive PDU/s`、active-bind time、PDU per bind/s、Window/Credit wait、event loop/response、worker pass/transport lifecycle、I/O 和 Journal backlog。
4. 用**反向控制**支持因果：撤销单一候选改动性能应回到旧值，再启用应重复改善；不能只凭“新代码快了一轮”签根因。若确实是新观测代码造成回退，优先降低观测频率/移离热路径或以可选开关控制，并证明关闭观测不会影响发信正确性。
5. 每个 50K 验证 accepted/socket/PDU/response=50,000；Journal/Projection=100,000（single-part 约定）；per campaign 计数正确、duplicates=0、unknown=0、write_failed=0、backlog=0、offset durable。保留 T6/T7 的既有不变量。
6. 对可能的临时修复先用测试分支或 `git diff` 呈现，不直接修改生产配置；需要用户批准才 commit/push。
7. 严格区分结果：
   - `ROOT_CAUSE_CONFIRMED_SOURCE_REGRESSION`
   - `ROOT_CAUSE_CONFIRMED_HARNESS_OR_LOOPBACK`
   - `ROOT_CAUSE_CONFIRMED_BIND_LIFECYCLE`
   - `ROOT_CAUSE_CONFIRMED_HOST_RESOURCE`
   - `PERFORMANCE_RECOVERED_CONFIG_OR_ENV`
   - `ROOT_CAUSE_INCONCLUSIVE`
   - `BLOCKED_ENV`
   只有完成严格同条件配对和可复现的因果证据，才写 `CONFIRMED`。

## 6. R4 — 修复范围、发布准备与长跑决策

- **R4 不是默认的 600 秒测试阶段。** 本任务主要是 528→169 PDU/s 差异 RCA；先修根因并复测稳定 5K/50K，不要因为任务编号就提前跑 600 秒。
- 若问题只来自 Harness/Loopback，则**仅修测试工具**；产品源码、TPS、Window、Writer 默认全部不动。
- 若确实发生生产代码吞吐回退，则实施最小修复、对应单测、Windows/WSL 既有回归及服务器 5K/50K 对照；保证原本的 at-most-once、Journal fsync、T6、T7 current 6/6 语义。
- 长跑 600s/800 PDU/s 仅作为**后续候选**：独立 simulator 和完整 E2E 均证明稳定供给余量、SLES 目标资源/磁盘可用、无 backlog 发散、全链路正确性成立后另行进行。`800` 正好等于配置 4×200 的理论上限，不得把毫秒级 overhead 造成的略低于上限视为已证实产品瓶颈；可使用测试专用额外 headroom 量化容量，但绝不能更改生产限额来伪造成绩。
- 大内存共享测试机的 application RSS guard 不等于 8/16 GiB 真实机器等价性；硬件门禁没验证就写 `NOT_VERIFIED`，不要伪造 GO。
- 若仍没找到根因，明确给出**最值得做的下一项小实验**，而不是继续发布架构重构/安全强化/巨型验证计划。

## 7. 交付文档：控制在 3 份

1. `Docs/Marketing_Copilot_VNext_528_to_169_Source_AB_RCA.md` — 冻结配置、源码差异、A/B 真实数值、主机证据、根因与置信度。
2. `Docs/Marketing_Copilot_VNext_SMPP_Loopback_Bind_Lifecycle_Isolation.md` — 仅记录实际完成的 L0–L3 实验；如不执行说明触发条件未满足，不填假指标。
3. `Docs/Marketing_Copilot_VNext_Throughput_RCA_Evidence.json` — 可追溯每轮环境/参数/source SHA/exit code/raw JSON SHA、分母、rate、bind/window/credit 计时、原始数据路径、正确性与决策。

原始日志、测试 DB/Journal、进程诊断、venv 和较大 raw JSON 放服务器隔离目录/本地 `.tmp`，**不得提交运行数据、凭据或用户工作树的无关改动**。最后输出对比表和简短结论。

## 8. Codex 最终输出卡（必须真实填写；未测用 null/NOT_VERIFIED）

```text
REPOSITORY_HEAD_AND_ORIGIN_MAIN=
SOURCE_A_SHA=450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e
SOURCE_B_SHA=3e73ab8030b46b78e109cbf1f23e330c72d2d136
SRC_DIFF_HOT_PATH_FILES=
TEST_SERVER_OS_AND_IDENTITY=
SERVER_AUTHORIZED=
SERVER_LOCAL_DETACHED_RUNS=
AB_STRICTLY_MATCHED=YES|NO|BLOCKED
AB_SHARED_PARAMETERS=
AB_HARNESS_COMPATIBILITY=
HISTORICAL_FAST_E2E_PDU_PER_SEC=528.2
HISTORICAL_SLOW_E2E_PDU_PER_SEC=161.9
A_5K_E2E_WALL_PDU_PER_SEC_MEDIAN=
B_5K_E2E_WALL_PDU_PER_SEC_MEDIAN=
A_5K_FEED_PDU_PER_SEC_MEDIAN=
B_5K_FEED_PDU_PER_SEC_MEDIAN=
A_5K_LOOPBACK_RECEIVE_PDU_PER_SEC_MEDIAN=
B_5K_LOOPBACK_RECEIVE_PDU_PER_SEC_MEDIAN=
A_B_5K_WALL_SPEED_RATIO=
STATUS_INTERVAL_AND_INSTRUMENTATION_EFFECT=
BIND_LIFECYCLE_A_B=
RESPONSE_WINDOW_P95_A_B=
CREDIT_WAIT_P95_A_B=
RESPONSE_CALLBACK_P95_A_B=
LOOPBACK_ISOLATION_RESULT=
HOST_CPU_IOWAIT_AND_DISK_EVIDENCE=
A_B_50K_WALL_PDU_PER_SEC=
A_B_50K_FULL_RECONCILIATION=
PRODUCT_CODE_CHANGED=YES|NO
PRODUCTION_DEFAULT_CHANGED=NO
AT_MOST_ONCE_AND_NO_REDISPATCH=PASS|FAIL|UNCHANGED_NOT_RERUN
600S_800PDU=NOT_RUN_UNLESS_SEPARATELY_JUSTIFIED
ROOT_CAUSE=SOURCE_REGRESSION|HARNESS_LOOPBACK|BIND_LIFECYCLE|HOST_RESOURCE|CONFIG_ENV|INCONCLUSIVE|BLOCKED_ENV
CAUSE_CONFIDENCE=HIGH|MEDIUM|LOW
FINAL_DECISION=ROOT_CAUSE_CONFIRMED|PERFORMANCE_RECOVERED|INCONCLUSIVE|BLOCKED_ENV
CANARY_READY=NO_UNTIL_SUSTAINED_CAPACITY_AND_TARGET_PROFILE_VALIDATED
UNRELATED_WORKTREE_CHANGES_PRESERVED=YES
COMMIT_PUSH=NOT_REQUESTED
```

**执行导向**：优先回答一件事——在同一 SLES 服务器、同参数、同依赖、同统计口径下，**旧源码是否仍显著快于新源码**？如果是，定位具体 diff；如果两者都慢，转向 Loopback/Bind/Host 分层；如果两者都快，定位历史实验配置/环境。没有根因证据绝不修改生产路径。
