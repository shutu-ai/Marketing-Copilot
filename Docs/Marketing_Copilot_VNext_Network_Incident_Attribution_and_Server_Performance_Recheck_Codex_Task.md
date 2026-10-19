# Marketing Copilot VNext — Network Incident Attribution & SLES Performance Recheck

> **Codex 任务编号**：`MC-VNEXT-NETWORK-INCIDENT-SERVER-PERF-RECHECK-20261018`  
> **基线**：`origin/main = 3e73ab8030b46b78e109cbf1f23e330c72d2d136`（执行前重新核验，若已更新则冻结实际 SHA）  
> **任务类型**：测试环境事件调查、性能结果复核、证据闭环；**不是性能重构**  
> **推荐执行方式**：在本地 Windows/WSL 做事件时间线核查；在已授权的 **SLES 12 SP5 测试服务器**运行服务器本地独立的 synthetic loopback 复测。  
> **最终原则**：先证明网络故障是否影响远端测试，再确定是否存在真正的 SMPP 引擎瓶颈。缺乏证据时允许 `INCONCLUSIVE` 或 `NO_CHANGE_WITH_EVIDENCE`。

---

## 1. 任务背景和必须解释的历史数据

### 1.1 近两轮已验证事实

历史较快的一轮，见：

- `Docs/Marketing_Copilot_VNext_Server_First_Performance_and_Platform_RCA.md`，基线 `450e84d8`，随后作为 `2c847b55` 的报告提交。
- 主机 `paas-core01`，SLES 12 SP5，服务器本地 ext4、synthetic loopback，A0/vectorized，`projection_batch=512`。
- `50K accepted/socket/PDU/response = 50K`，`Journal/Projection events = 100K`，数据正确。
- **total E2E wall = 94.671s**，因此**全流程平均 = 50,000/94.671 ≈ 528.2 PDU/s**。
- **Producer/feed 阶段 = 687.2 PDU/s**；它与全流程平均 528.2 **不是同一个统计口径**。
- Writer/Projection 最后 response Journal durable → DB commit 仅约 `46.584ms`，不是当轮主要限制。

最新较慢的一轮，见：

- `Docs/Marketing_Copilot_VNext_Harness_Reliability_and_Resource_RCA.md`
- `Docs/Marketing_Copilot_VNext_SMPP_Credit_Window_Throughput_AB_Report.md`
- `Docs/Marketing_Copilot_VNext_SLES_Sustained_Capacity_and_Canary_Decision.md`
- `Docs/Marketing_Copilot_VNext_SMPP_Harness_Closure_Evidence.json`
- 已提交的源码/报告 `3e73ab80`。`50K` S0：total E2E wall **308.882s**，**全流程平均约 161.9 PDU/s**；报告的 Loopback first-to-last receive rate 为 **169.112 PDU/s**，其分母定义又不同于 E2E wall。
- Window wait P95 从较快轮的约 `334.362ms` 变成较慢轮 `2532.409ms`；credit wait P95 分别约 `636.319ms`、`426.990ms`。**这些数值来自不同运行/源码/观测参数，未构成严格配对回归证据。**
- 慢轮完整 `50K PDU/response` 和 `100K Journal/Projection` 仍对账，unknown/write_failed=0。
- 慢轮测试服务器记录约 `80 CPU / 375GiB` 的共享主机、高 load、根分区约 **90% 已用**；此条件可能干扰结果，也不等于实际 8/16 GiB 生产资源。
- Harness RSS 采样和完成状态误判已在 `3e73ab80` 修复并经过回归；不得倒退。

### 1.2 本次用户提出的假设

> 两轮服务器吞吐异常差异，可能与测试期间本地 Windows/电脑发生 VPN、代理、GitHub 443 或 SSH 网络故障有关。

**必须审慎区分的三个链路：**

1. 本地电脑 → GitHub HTTPS/443：可能阻断 push/pull，但**不能单独证明**服务器上 `127.0.0.1` SMPP Loopback 变慢。
2. 本地电脑 → SLES SSH/VPN：若测试依赖前台 SSH、终端管道或远程会话，其断线可能造成远端进程退出、阻塞或结果采集中断；要通过远端 PID、退出码、log、时间戳证实。
3. SLES 服务器内部 → `127.0.0.1` Loopback SMPP：如果 Client、Worker、模拟 SMSC、Journal 和 DuckDB 全部在同台服务器运行，且进程脱离 SSH 会话独立运行，电脑网络故障**通常不会直接影响内部吞吐**。

**本任务不预设网络故障必然是根因；必须建立带时间戳的证据链。**

---

## 2. 不可触碰的产品边界

- 不修改生产 SMPP TPS、Window、RouteCredit 规则，不增加隐含重试，不绕过发送限速。
- 保持单 Writer、单 DuckDB、A0/vectorized、`projection_batch_size=512`、Journal fsync、offset 持久化、dedup、at-most-once、business-day cutoff 和“不因未知结果自动重发”。
- 本阶段**不进行 PyArrow、Writer microbatch、Worker 并发架构重构**。
- 只运行 synthetic loopback、虚拟目标号码；禁止连接真实 SMSC、生产数据库或生产短信投递。
- 遵守 `AGENTS.md`：保护原有 dirty/untracked 文件；如果存在 `.codegraph/`，先用 CodeGraph；未经本次明确要求**不得 commit/push**。
- 远端必须已获得授权；不要求用户在聊天、日志或报告中提供密码、Token、私钥；不要把 `Proxy-Authorization`、SSH 配置密钥、VPN 凭据写入报告。
- 本地网络异常不应通过绕过企业网络策略/安全控制的手段处理。若访问被策略阻断，只记录为 `NETWORK_BLOCKED`；不要为了完成任务更改全局 VPN/防火墙或企业代理策略。
- 测试机磁盘已接近 90% 使用：**先检查空闲容量与其他用户负载**，每次运行独立 run root，严格限制 disk/RSS；不能为腾空间删除其他应用或其他人的文件。

---

## 3. R0 — 冻结版本、配置与运行时间线

### 3.1 源码/配置冻结

1. `git rev-parse HEAD`、`git status --short`、`git rev-parse origin/main`，确认提交身份；只读比较 `2c847b55 → 3e73ab80` 期间和吞吐相关的变更。
2. 从两轮 raw JSON、可用 server 日志中恢复：运行开始/结束 UTC 时间、`hostname`/`boot_id`、PID/PPID/进程组、source archive SHA、Python/DuckDB、仪表模式、`projection-status-interval`、Worker/Bind/Window/TPS、SMSC response delay、CPU affinity/nice、Run dir、真实返回码、resource samples。**缺失填 `NOT_AVAILABLE`，禁止编造时间。**
3. 特别标注旧轮 `--projection-status-interval 0.1` 与新轮 `1.0` 等不同设置；之前的宿主机 load、磁盘占用、状态采样实现、代码版本、不同报告中的分母也要进入差异清单。
4. 将历史数据整理为统一口径：

| 指标 | 定义 |
|---|---|
| `e2e_wall_pdu_per_sec` | 成功计数 PDU / 完整 run wall，从启动至彻底收尾 |
| `feed_phase_socket_pdu_per_sec` | 相同明确定义的 producer/feed 活跃区间内 socket-written PDU / 区间长度 |
| `loopback_receive_pdu_per_sec` | Loopback first-receive → last-receive 的速率（另列） |
| `window_wait_p95` | 等待窗口可用的 P95，不等于 response RTT |
| `credit_wait_p95` | 等待信用额度的 P95，不直接视为错误 |
| `response_latency_p95` | 已 socket write → 收到 SMPP response |
| `post_response_projection_tail` | 最后 response Journal durable → 最后 Projection offset durable |

**不可再用旧 687.2 与新 169.112 直接计算退化比例。**

### 3.2 故障时序核查

5. 在用户电脑的已存在日志中查找故障时间（若可访问）：Windows VPN 断连/重连、SSH 连接报错、GitHub 443 失败、PowerShell/Codex 会话异常。可用 PowerShell `Get-Date -Format o`、授权范围内的 Windows Event Log 和已存在的本地终端日志；日志为空则写 `NOT_AVAILABLE`。
6. 对齐电脑时间与服务器时间/时区，统一成 UTC，确认时钟漂移；将失败事件和 `50K` 的 run start/end、socket/responses 每秒时间线放在同一轴上。
7. 区分 **“GitHub HTTPS 失败”** 和 **“对测试服务器 SSH/VPN 失败”**；不能把 GitHub 443 超时视为 SSH 断线的替代证据。
8. 查服务器上当时是否有 SIGHUP/SIGTERM、进程退出/重启、stdout/stderr 写入异常、网络断开、系统负载/IO wait 激增，及真实测试是否依赖活跃 SSH 控制会话。

**R0 判定：** `NETWORK_INCIDENT_TIMELINE=CORRELATED|NOT_CORRELATED|INSUFFICIENT_EVIDENCE`；`CLIENT_TO_GITHUB`、`CLIENT_TO_SSH`、`SERVER_LOOPBACK` 必须分别标记。

---

## 4. R1 — 恢复网络后，在 SLES 服务器上独立短跑

### 4.1 正确启动方式：避免 SSH 断线改变测试生命周期

1. 使用**已经授权**的 SLES 12 SP5 测试节点 `paas-core01`，确认 hostname/OS 与以前相同，先做 SSH 可达性、服务器端 `127.0.0.1` 可用性检查；不修改网络防火墙。
2. 冻结 `3e73ab80` 最新基线，使用隔离工作目录/venv、独立 Run root。运行前检查剩余磁盘、CPU load、I/O wait、现存 SMPP 测试进程、是否存在与自己以往 Run 冲突的 PIDs。
3. **由远端服务器自行运行测试并落地全部结果**，不在本地 Python 中转发 SMPP、不经本地 SSH 隧道承载测试消息。允许用已有 `tmux`/`screen` 或 `nohup` + server-local log/result/PID/exit code 文件实现持久运行；实际机制按机器可用工具选择，不需要 systemd/root 权限。
4. 如果只可执行前台 SSH 测试，明确标记 `SSH_SESSION_DEPENDENT=YES`，其输出**不能**用于断网影响排除实验。
5. 不主动切断用户 VPN、不进行未经用户授权的断网实验；如在已授权隔离环境可安全测试 SSH 客户端退出，**仅在 1K smoke 中**执行：确认任务在服务器继续完成且结果文件完整。对于 50K/更长运行不故意断网。
6. 用服务器独立写入的 `pid`, `ppid`, `proc_start`, `proc_end`, `exit_code`, `result.json SHA256` 证明测试不依赖 SSH 窗口存活；结束后校验没有残留测试进程/Bind 占用。

### 4.2 小样本稳定性复测（不预先改业务代码）

保持：

```text
server=SLES12SP5 / local ext4 / localhost loopback
source=3e73ab80 (unless superseded; freeze actual SHA)
A0/vectorized / projection batch 512
counts=1250,1250,1250,1250  (5K tasks, single-part)
workers=8 / binds=4 / tps_scope=per_bind / tps_per_bind=200
window=50 / response_delay_ms=0 / synthetic destinations
status_interval=1.0s (fixed harness, can also perform 0.1s sensitivity)
```

- 做 **3 次同环境、不同 run root 的 5K**，尽量轮换运行次序与轻重负载时段；记录 server load、CPU/RSS、磁盘 await/IO wait（不可用则标缺失）、Loopback RX/TX 和各 Bind 生命周期。
- 如吞吐波动大，再做 1 次同参数 5K 在 `0.1s` status interval 下的对照，但不能用不同 source/不同 Window 当作同配置复测。
- 每一轮单独对账：`accepted = socket-written tasks = loopback PDU = response = 5K`、`Journal/Projection=10K`、`unknown=0`、`duplicate PDU=0`、`backlog end=0`、`completion_state=COMPLETED`、Sampler Valid、exit code 0。
- 报告各轮 `e2e wall`、feed phase、Loopback first-to-last、credit/window/response RTT P50/P95、每 Bind PDU/s、bind opens/closes 和 window inflight peak；这些指标**不能只记录最终一个 worker 的快照**。
- 如本阶段同配置吞吐仍保持慢轮区间且与网络异常完全无关，写 `SLOW_REPRODUCED`；如速率恢复并且是可复现的，写 `PERFORMANCE_RECOVERED`；若 CPU/load/disk 波动主导，写 `HOST_RESOURCE_INTERFERENCE`。

**R1 不需要达到 800 PDU/s，不得把 5K 短跑当成生产容量验收。**

---

## 5. R2 — 条件触发的 50K 同口径服务器复测

**只有满足以下全部前置条件才开始：**

- 3×5K 数据正确，Harness `sampler_ok=true`，无 SSH 生存期依赖；
- SLES 服务器负载相对稳定，磁盘具备足够安全余量：建议剩余空间不小于历史 50K 实际峰值所需的 **3 倍**且至少 **3 GiB**；不满足则换已授权更大 native ext4 空间或 `BLOCKED_DISK`；
- 无未知遗留进程干扰，无近期 SSH/VPN 非预期终止，能准确冻结相同参数；
- 5K 数据已显示有必要调查吞吐差异，而不是仅因任务表上有 50K 就重复高成本运行。

### 5.1 先用现有 `3e73ab80` 复测

- `counts=20000,20000,5000,5000`、workers=8、4 binds×200 per_bind、window=50、producer batch=250、A0/512、instrumentation 的 on/off 模式明确冻结、status interval 固定、同一 synthetic Loopback / response delay。
- 默认先执行 **一次** 50K，完整对账通过后再决定是否需要第二次；不得为“凑三轮”无限运行。
- 收集每秒 `socket PDU/s`、`response PDU/s`、`Journal/Projection events/s`、credit/window/response RTT、Producer active、Bind 连接生命周期、`post-response projection tail`、application-only/harness RSS 峰值与斜率、host CPU/IO wait 和 run 目录磁盘增长。
- 对比历史旧快轮 `94.671s / 528.2 wall PDU/s` 与新慢轮 `308.882s / ~161.9 wall PDU/s`，**首先统一参数和时间分母**。不同参数导致的不一致必须单列，不能说“完全复现”或“恢复”而不解释。

### 5.2 如依然显著变慢，做最小 A/B

仅在确实存在稳定回归且能重现实验的前提下：

1. 对比 `3e73ab80` 的 performance instrumentation OFF / ON，保障观测本身不会带来较大开销；不关闭数据持久化、response callback 或安全规则。**尽量使用相同源码，仅改变观测开关**。
2. 复核 `2c847b55 → 3e73ab80` 改动范围。若需跑老源码，只允许在隔离 Run/venv 下回放 synthetic workload，保证测试参数与完成判定可比较；**老 Harness 曾存在 stale-status 与 RSS 采样问题，不能只用老 Harness 的 overall wall 直接判断回归**。
3. 使用最短可复现 5K/10K 完成对照，不因少量噪声就修改业务引擎；只有看到同条件、可重复、可归因的时间线异常，才提出一个最小修复候选。
4. 如果网络故障未与服务器进程/负载/速率变化同步，不把它标记为根因，即使本地 GitHub/PowerShell 确实出现过 443 失败。

**R2 完成结论可为**：`PERFORMANCE_RECOVERED`、`NETWORK_SSH_SESSION_EFFECT_CONFIRMED`、`HOST_RESOURCE_INTERFERENCE`、`HARNESS_INSTRUMENTATION_REGRESSION`、`SMPP_ENGINE_BOTTLENECK_EVIDENCED`、`INCONCLUSIVE`。必须有源数据与时序支撑。

---

## 6. R3 — 仅在发现真实缺陷时最小修复；不提前执行 600 秒长跑

- 如果问题仅来自 Windows/VPN/SSH 会话或测试启动方式，**只修改测试运行脚本或执行流程**，保持生产源码零差异。
- 如果问题来自服务器共享负载/磁盘状态，记录可测的干扰并换合适的测试窗口/隔离盘，不引入架构修改。
- 仅当测试结果确认某处真实产品/测试工具 bug，才做最小源码修复、定向单测，必要时重新跑 5K 和一组 50K；任何对发送限速、Window、响应处理/缓存有语义变化的改动须单独论证。
- 任何修改都必须保持 at-most-once：已 checkpoint/socket_written、结果 unknown 的请求绝不被自动补发；不跨业务日派发；Journal → Projection 数据完整。
- 全量 Windows backend/WSL ext4 pytest 仅在有代码 diff 时运行；无代码差异时执行必要 targeted 检查即可，避免无收益的多轮大测试。
- 本轮 S0 合成测试参数保持 `4 binds × 200 TPS`、window=50；实际生产线路的限额必须以既有 route 配置为准，不得把本轮测试参数误称为生产默认。生产 Writer 保持 `A0 + batch 512`；**不因为本轮异常就提高生产 TPS 或默认 Window**。
- **600秒/800 PDU/s** 是后续正式容量验证，只有可复现的稳定服务器供给、代表目标生产的资源 envelope、可用磁盘和可靠每秒采样成立后再申请执行；本轮未满足时标 `NOT_VERIFIED`，不强行运行。

---

## 7. 建议交付文件（总量精简）

请仅生成三份核心文件，除非有已确认的代码缺陷需要源码/测试 diff：

1. `Docs/Marketing_Copilot_VNext_Network_Incident_and_SSH_Process_RCA.md`  
   本地 GitHub/SSH/VPN 事件和服务器时间线、真实进程生存期、网络是否能影响 localhost Loopback，归因置信度、缺失证据。
2. `Docs/Marketing_Copilot_VNext_SLES_Throughput_Recheck_and_Comparison.md`  
   3×5K 和有条件 50K 的同口径对照、host load/disk、Config/source 差异、Window/Binds、Instrument ON/OFF、完整对账。
3. `Docs/Marketing_Copilot_VNext_Network_Performance_Recheck_Evidence.json`  
   `run_id`、hostname/OS、source/dep SHA、UTC start/end、launch/detach mode、PID/exit、result path + SHA256、PDU/response/Journal/Projection、rates 定义、network incident window、cause evidence、final gate。

原始 Run 日志与数据库放服务器隔离目录；Git 中只放去敏的结论、指纹和最少必要证据。未经显式指令不得 commit/push；不修改用户现有 dirty/untracked 文件。

## 8. 验收 Gate 与终态规则

| Gate | PASS 条件 |
|---|---|
| `G0_ENV_IDENTITY` | OS/host/source/配置/磁盘/资源已冻结 |
| `G1_NETWORK_CORRELATION` | 明确客户端 GitHub、客户端 SSH、服务器 localhost 三链路；带 UTC 事件证据；否则 NOT_VERIFIED |
| `G2_DETACHED_RUN` | 服务器测试无需保持客户端 SSH 存活，PID/exit/result 可回溯 |
| `G3_5K_STABILITY` | 3次完整对账、有效内存采样、相同速率分母和资源快照 |
| `G4_50K_COMPARABILITY` | 如执行 50K，完整对账且量纲可比；否则 NOT_RUN_WITH_REASON |
| `G5_CORRECTNESS` | zero unknown/duplicate/missing，Journal/Projection EOF 对账、无重复发送 |
| `G6_NO_UNNECESSARY_PRODUCT_CHANGE` | 未经证据不修改生产逻辑、限速、Writer |
| `G7_600S_800PDU` | 本任务通常 NOT_RUN；不虚报 PASS |

**允许最终结果**：

- `NETWORK_SSH_SESSION_EFFECT_CONFIRMED`：具有实际服务器进程/输出/时间线证据支持，而不是仅电脑网络有故障；
- `NETWORK_EVENT_NOT_CAUSAL`：电脑网络出错，但服务器本地测试独立正常，时序和结果不支持因果；
- `PERFORMANCE_RECOVERED_ENV_VARIANCE`：网络稳定后速率恢复，尚不能唯一确定是网络、共享主机资源还是测试程序造成；
- `HOST_LOAD_OR_DISK_LIMITED`：主机资源限制可重复证实；
- `HARNESS_OBSERVABILITY_OR_LIFECYCLE_BUG`：可复现并定位测试工具问题；
- `PRODUCT_SMPP_REGRESSION_EVIDENCED`：同平台同参数可重现的产品回归；
- `INCONCLUSIVE`：关键历史时间戳或环境证据缺失；
- `BLOCKED_ENV`：服务器/磁盘/权限/网络条件不允许安全完成。

不要强行输出 `CANARY_READY=YES`。只在真正完成正式目标性能、资源和恢复验收后，才允许转入发布决策。

## 9. Codex 最终输出卡（逐项填写，无数据用 NOT_VERIFIED）

```text
SOURCE_HEAD=
ORIGIN_MAIN=
UNRELATED_WORKTREE_PRESERVED=
TARGET_SERVER_OS=
TEST_SERVER_HOST=
SERVER_AUTHORIZED=
NETWORK_FAILURE_UTC_WINDOW=
CLIENT_GITHUB_443_INCIDENT=YES|NO|UNKNOWN
CLIENT_SSH_VPN_INCIDENT=YES|NO|UNKNOWN
SSH_SESSION_DEPENDENT=YES|NO|UNKNOWN
SERVER_LOOPBACK_INDEPENDENT=YES|NO|UNKNOWN
REMOTE_TEST_PID_AND_EXIT_CAPTURED=YES|NO
R0_HISTORICAL_CONFIG_DIFF=
HISTORICAL_FAST_E2E_WALL_PDU_PER_SEC=528.2
HISTORICAL_SLOW_E2E_WALL_PDU_PER_SEC=161.9
NEW_5K_THREE_RUNS_E2E_WALL_RATE=
NEW_5K_THREE_RUNS_LOOPBACK_RATE=
NEW_5K_THREE_RUNS_WINDOW_P95=
NEW_5K_THREE_RUNS_CREDIT_P95=
SERVER_HOST_LOAD_AND_IOWAIT=
SERVER_FREE_DISK_AND_RUN_SIZE=
NEW_50K_STATUS=PASS|FAIL|NOT_RUN
NEW_50K_E2E_WALL_RATE=
NEW_50K_FEED_PHASE_RATE=
NEW_50K_LOOPBACK_RECEIVE_RATE=
NEW_50K_WINDOW_P95=
NEW_50K_BIND_LIFECYCLES=
NEW_50K_JOURNAL_PROJECTION_RECONCILIATION=
NEW_50K_UNKNOWNS_DUPLICATES=
APPLICATION_RSS_SAMPLER_VALID=
COMPARED_METRICS_SAME_DENOMINATOR=YES|NO
CAUSE_EVIDENCE_AND_CONFIDENCE=
BACKEND_SOURCE_CHANGED=YES|NO
PRODUCTION_DEFAULT_CHANGED=NO
R4_600S_800PDU=NOT_RUN|NOT_VERIFIED
CANARY_READY=NO
FINAL_DECISION=
COMMIT_PUSH=NOT_REQUESTED
```

**交付重点：只做网络故障归因与服务器复测，不为了追求测试数值而做新的 SMPP/Writer 架构改造。**
