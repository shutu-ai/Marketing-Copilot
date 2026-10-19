# Marketing Copilot VNext — End-to-End Critical Path Profiling & Bottleneck Closure（Codex 任务）

> Task ID: `MC-VNEXT-E2E-CRITICAL-PATH-PROFILING-CLOSURE-20261017`  
> 仓库：`https://github.com/shutu-ai/Marketing-Copilot`  
> 冻结起点：`origin/main = 24708c8eb183d689d4f1e8e3df7f63a0ed81c222`（执行前核对；若 HEAD 有变更须先记录）  
> 类型：**精确性能定位 → 最小局部优化 → 同环境 A/B → 证据闭环**。**不是架构重构任务。**  
> 默认权限：仅本地检查/修改/验证/输出报告。**没有新授权不得 SSH/VPN 连接 EulerOS、真实 SMSC、生产 API/DB，不得部署、commit、push。**

## 0. 任务背景与已确认结论

### 0.1 已知事实（不得当作本轮重新通过）

- 最新提交 `24708c8e` 的本地基线测试：AlmaLinux-8 WSL2 **native ext4**，backend `418 passed, 2 skipped`；Windows targeted `13 passed`。这些属于历史已报告结果，本轮仍需完成修改后的定向回归。
- 本地 baseline T3 四 Campaign 50K：`50,000` tasks / socket PDU / SMPP responses 与 `100,000` Journal/Projection events 全对账；`duration=446.393s`，`producer=229.181s`，从供给停止计量的 SMPP drain `207.144s`、Projection drain `208.577s`；约 `218.2 PDU/s` 为**本地供给阶段平均**，不是 800 TPS 能力证明。
- Pure Writer A3 在大样本 `100K–500K` 事件上的吞吐约 `10K–12K events/s`；这是**直接喂给 DuckDB 的离线基准**，不是含多进程 Journal/SMPP fsync、Response Reader、Spool 的持续 E2E 能力。先前 100K/10% A/B 三次中位数仅 `+1.247%`，不支持继续无目的 SQL 改造。
- 上次临时 `segment path cache + low-frequency status` 实验 T3 `50,000 accepted` 但仅 `49,982` PDU/response，出现 `18` 条 `SMPP response window did not release before timeout`；实验已回滚。**不可把该实验重新提交或当作已知加速方案。**
- 上次 EulerOS 历史测试用过 `taskset 0-3 + nice +10 + ionice -c3 + 2 GiB` 聚合 RSS 护栏；T3 projection drain `742.003s`，T4 在 `331.293s` 达 2 GiB RSS 护栏。上述运行资源限制**不能替代**目标 8–16 GiB 生产 profile。
- T6 服务级跨天/禁止旧任务重新派发曾 PASS；T7-4…T7-9 在 EulerOS 曾 6/6 PASS；当前版本没有整套 T7 1–9 同 SHA 联合签署。
- `CANARY_READY = NO_GO`、`TARGET_PROFILE = NOT_VERIFIED` 必须保持，直到后续真实目标资源环境和强制 Gate 被有效验证。

### 0.2 本轮核心问题

纯 Writer 可处理上万 events/s，而完整发送链路仅观察到约 218 PDU/s 且响应和 Projection 收尾时间接近。需要精确回答：

1. 到底是哪一段限制 **实际 socket PDU/s**：feeder/Spool、Worker 调度、route TPS credit、SMPP window/bind，还是**响应读取线程内的同步 Journal fsync**？
2. 在 `feed stop` 之后，真正消耗的时间如何分别归属：**Spool backlog 发送 → SMPP window response drain → Journal durable → DuckDB committed projection → offset durable**？不能仅凭“drain 208s”就判断 DuckDB 处理了 208s。
3. 当高并发 backlog 变大时，`ProgressJournal` 路径扫描、文件打开/fsync、Writer offsets、状态/诊断文件更新是否成为热路径高频 I/O？
4. 在 8 GiB/16 GiB 合理资源模型下，系统是否能**在不放松 at-most-once、跨天截止和数据对账**的前提下持续接近 800 PDU/s？

本轮以**找出制约因素并完成最小、可证实的优化**为成功标准，绝不以大量重构或漂亮 microbenchmark 代替 E2E 证据。

## 1. 绝对设计约束

1. 保留 **单 `ProjectionWriterProcess`、单 DuckDB、单 DB owner、单条完整 Journal 回放事实来源**；本轮不增加每 Campaign Writer/DB，不引入新消息中间件、RPC 或额外常驻后台服务。
2. **严格 at-most-once**：不因 response 超时、进程 crash、`unknown`、multipart partial、Projection 延迟而自动重发已 taken/可能已写入 socket 的原短信。宁可少发，不重复发。
3. 业务日 hard cutoff 必须在真正 `socket.write` 前最后一道检查；跨天的旧日任务绝不发出新 PDU，restart / prefetch / Bind / TPS / window wait 也不能绕过。新日同号码的新任务不等于重发昨日任务。
4. 不擅自关闭 `ProgressJournal` 逐事件 `fsync`，不降低 DB COMMIT、event_id 去重、持久化 offset、Journal segment 只在确认安全后 GC 的语义。不允许为了提升速度静默丢弃响应事件。
5. **禁止把 `route_tps=200 × pool_size=4 = 800` 配置上限当成实际 800 PDU/s。** 同时记录 socket-written PDU/s、loopback received PDU/s、`submit_sm_resp`/s、Journal *所有* events/s、committed projection events/s；multipart 按 part 计 PDU。
6. 所有新仪表都应**低扰动**：进程内累计计数 + 直方图或 bounded ring-buffer，默认按 1 秒输出采样；不能在每条 SMS 或每条响应上写额外 JSON/CSV 或同步 fsync；instrumented vs uninstrumented 必须对比。
7. 保留用户已有 dirty/untracked 文件；按仓库 `AGENTS.md` 执行。若存在 `.codegraph/`，先 `codegraph explore` 再读对应模块。只提交任务范围内文件（本任务默认**不 commit/push**）。
8. 不把 Windows `/mnt/d` 或 WSL 网络盘当成 native ext4 性能基线；目标运行必须确认 `df -T`、源码、venv、DB/WAL、Journal、Spool 与临时目录的挂载归属。
9. 不连接真实 SMSC/生产 DB/API，不生产部署；所有 SMPP loopback 绑定 `127.0.0.1`，使用合成 MSISDN，无秘密流出证据包。

## 2. 必须优先审计的真实热路径（以当前 HEAD 为准）

| 文件/符号 | 已知机制 | 需要验证的热点与风险 |
|---|---|---|
| `backend/vnext/runtime.py` / `_runtime_worker_entry.emit` | `journal.append(event)` 后 `projection_queue.put_nowait()`；queue 只是 wake-up hint | 一个 journal append/fsync 是否阻塞 Worker event loop/SMPP reader；queue Full 不应丢 durable event |
| `backend/vnext/smpp_transport.py` / `_read_responses` | SMPP response `pop(_pending)`、`_window.release()`、然后 `await response_callback(event)` | response callback 的同步日志写入是否阻滞后续 response PDU 读取；callback 失败/容量满时是否退出 reader、是否丢响应事实 |
| `backend/vnext/worker.py` / `_response_event`、`_emit` | per-PDU response 转 durable Journal event；`drain_transports()` 等待窗口清零 | response 事件唯一性与 `unknown` 统计；SMPP response drain vs Journal durable 的完成点不得混淆 |
| `backend/vnext/projection_writer.py` / `ProgressJournal._append_encoded` | `can_append` 和 append 检查总 segment bytes，扫描 path；每 append open/write/flush/fsync/close | 单次 fsync 时间、segment glob/stat、文件频繁 open/close、ext4 抖动；避免重复“缓存必然提速”的错误推断 |
| `backend/vnext/projection_writer.py` / `_process_main` | round-robin tail Journals，批量 `flush`，逐批 `_persist_offsets`，`reclaim`、`diagnostic`、`_write_process_status` | 读/解码、DuckDB apply、offset fsync/dir fsync、诊断写入分别耗时；Journal backlog 是否长期增长 |
| `backend/vnext/projection_writer.py` / `_apply_batch` | vectorized LIST/UNNEST + reused staging，event_id dedupe，set-based aggregate | 保留已确认较优的 A3；只有新证据才尝试局部 SQL 变化 |
| `backend/tools/vnext_streaming_acceptance.py` | 有 `producer`、`smpp_drained_at`、`projection_caught_up_at`，Resource sampling | 确保是**彼此独立的时间戳与耗时**，且完成判断包括 Journal EOF、committed offset 和 DuckDB 数据，不被 shutdown cleanup 误判 |
| `backend/vnext/diagnostics.py` / `write_process_metadata` | 原子 JSON replace 写入进程状态 | 写状态频率/体积与 I/O 干扰；禁用或降频实验仅可在独立 candidate 中完成并过相同 E2E |

如源文件已变动，任务执行者需逐项复核并在报告注明实际行号、函数及差异，禁止按过时猜测直接改代码。

## 3. R0 — 冻结源码、基线与口径（不依赖测试服务器）

- 从 Git 记录 `HEAD/branch/tree/origin/main`、dirty/untracked 列表、关键文件 SHA；保留 unrelated 修改，不 reset 用户文件。源树与实验目录隔离，基线和候选必须独立运行目录。
- 读取本任务前六份报告：
  - `Docs/Marketing_Copilot_VNext_Production_Sizing_and_Memory_RCA_Report.md`
  - `Docs/Marketing_Copilot_VNext_Single_Writer_Dedupe_Aggregate_Optimization_AB_Report.md`
  - `Docs/Marketing_Copilot_VNext_Production_Profile_Final_Canary_Gate_Report.md`
  - `Docs/Marketing_Copilot_VNext_Production_Sizing_Comparison_Matrix.md`
  - `Docs/Marketing_Copilot_VNext_Single_Writer_Performance_RCA_and_AB_Report.md`
  - `Docs/Marketing_Copilot_VNext_Production_Response_Journal_T7_Closure_Report.md`
- 检查 `runtime` 路由、CPU affinity/nice/ionice、cgroup memory.max/memory.current（如可用）、`RLIMIT`、page cache、filesystem、DuckDB 和 Python 等。每个 Run 都应输出机器可读的 effective config。
- 建立三类指标的严格口径：
  - `task` = 单条 SMS 逻辑任务；`PDU` = submit_sm part；`Journal event` = task event + 每个 SMPP response event + 可能的 unknown/错误事件。单段成功短信常见 2 events/task，多段短信**不是**固定 2。
  - `end_to_end` 与 `feed duration`、`socket completion`、`response completion`、`projection commit completion`、`offset durable completion` 是不同时间区间，不能相互替代。
  - `app-only RSS/PSS`（8 Worker + Writer + VNext application）与 `harness-inclusive RSS`（含 feeder/SMSC/pytest）分别计量；主验收使用实际部署可用的 application/cgroup 口径。Linux `cgroup memory.current` 包含不同于 RSS 的统计对象，不混用。

**R0 Gate**：配置、source、统计口径、各计数来源可复核。若源不一致，停止性能对比并明确报告原因。

## 4. R1 — 低扰动完整关键路径埋点（本任务主工作）

### 4.1 按阶段建立 monotonic 时间点与累计耗时

优先复用已有计数器、status snapshots 和 `time.perf_counter_ns()`/`time.monotonic_ns()`；跨进程仅在**同一主机**可比较 monotonic 时刻；跨主机不要直接相减。

#### Producer / Ingress / Spool

- accepted tasks/s、admission wait/s、spool durable append+manifest/cursor fsync 时间、pending records/bytes、生产 chunk size、producer starvation 次数。
- 每次 feed 开始/结束的 monotonic 时刻；发送的 backlog 完全清空时刻。

#### Worker / SMPP

- 取任务至 durable taken fence、route bind wait、TPS credit wait、response window wait、实际 socket.write+drain 的次数与时间。
- 实测 `socket_written_pdu/s`、`response_received_pdu/s`、`window_inflight` 的时间序列、bind lease/连接数、response error/timeout/unknown 数。
- `response_callback` 调用 wall time，按内部 `Journal JSON encode` / `segment_paths glob/stat` / `open/write/flush` / `fsync` / `IPC hint` 细分；标明**event loop 同步阻塞时长**。
- response callback 失败/Journal 满不得被“只是 SMPP 超时”掩盖：记录独立失败计数和故障结果；**禁止为测量而改动确认窗口及 at-most-once 行为**。

#### Journal / Projection

- `journal_event_append_rate/s`、`fsync_count/s`、`fsync latency P50/P95/P99`（按 bounded histogram/抽样）、段扫描和 stat 次数、单 event 平均字节数；不可每事件落盘 trace。
- Writer 的 `journal_read+decode`、`staging`、`dedupe`、`aggregate`、`DB COMMIT`、`offset persist/dir fsync`、`GC/status/diagnostics` 耗时/次数和每秒 `new/replayed/applied`。
- 真实 backlog：Journal durable EOF - committed offsets（每个 segment 带 generation）；不能只看 `projection_lag_seconds=0` 或 process-local applied。

### 4.2 结束时间线（至少输出以下字段）

```text
feed_start_monotonic
feed_stop_monotonic
last_task_taken_monotonic
last_socket_write_monotonic
last_smpp_response_received_monotonic
last_response_journal_durable_monotonic
last_projection_db_commit_monotonic
last_projection_offset_durable_monotonic

feed_active_seconds
pending_send_drain_seconds
smpp_response_drain_seconds
response_journal_drain_seconds
projection_commit_drain_seconds
offset_durable_drain_seconds
journal_backlog_bytes_timeline
journal_backlog_events_estimate_timeline
```

若某字段没有可靠的事件源，置 `null` + `not_measured`，**严禁硬编码 0**。对于已被废弃或回收的 segment，要按事件日志和持久化 offset 合规计算 EOF，不能把“文件不存在”简单认定为积压为零。

### 4.3 仪表本身开销

- 每个测量点只做小整数累计 / bounded hist；输出频率默认 ≤1/s；诊断可临时采样关闭做 paired comparison。
- 做 instrumentation ON/OFF 同环境 smoke 和 P5500 A/B，报告对总 TPS/CPU/RSS 的影响。如性能下降明显，优化仪表后再大测。

**R1 Gate**：所有关键阶段有可用 source-backed 指标，P5500 correctness 不下降，新增 metrics 不能显著影响热路径或制造数据丢失。

## 5. R2 — 对照诊断矩阵：先定位，再优化

依次运行以下矩阵，不允许直接跳到高压长稳。每个模式至少采集 per-second CSV + bounded JSON 汇总；关键对比重复 3 次并报告 median、差异范围。

| 用例 | 负载 | 用途 |
|---|---|---|
| B0 | `backend/tools/vnext_single_writer_benchmark.py`，100K/300K/500K events；单 Writer DB | 离线纯 SQL upper-reference，确认现有 A3 无退化；不当作 E2E SLA |
| B1 | loopback P5500，8 Worker / 4 Bind / 200 per_bind / window 50，single-part | 端到端阶段耗时与 events/PDU 基线 |
| B2 | loopback P5500；response 0ms / 小幅延迟 / 受限 window（保持其他条件相同） | 鉴别 SMPP response reader/window wait 对吞吐的敏感性 |
| B3 | 4 Campaign streaming 50K，20K+20K+5K+5K；baseline，单段 | 真正 producer/send/response/Writer 并发 + drain；独立时间线 |
| B4 | mixed multipart 5,500 tasks / 11,000 或实际 parts | 检查 response event/part 身份、window 与 Writer 的倍率 |
| B5 | 600s 稳态的 **短时预检**（如 60–120s 有界运行）+ 完整 drain | 确认资源与增长曲线后才继续长跑 |

> 不能只拿纯 Writer 11K eps 和完整链路 218 PDU/s 作简单倍数比较；必须把正常响应事件翻倍、每事件同步 fsync、SMPP credit 和 window 的时延同时计入。不同运行若不是同 CPU/I/O 限制，不得合并速度结论。

在报告里用一张 `critical-path attribution` 表写清：各阶段耗时、累计耗时是否重叠、调用次数、每秒吞吐、进程占 CPU、独立 drain 区间；选择**一个最主要且有证据支持**的瓶颈做下一步 candidate。

## 6. R3 — 最小局部性能候选（条件式，不强行改代码）

**严格按照 R2 结果选择**。每次只改变一类行为，candidate 必须独立 source tree/feature switch（可退出/回滚）；旧 T3 18 条 window timeout 不能重现。优先顺序如下：

### H1：SMPP response reader 被同步 Journal append/fdatasync/fsync 阻塞

- 若 `response_callback` 延迟与 `window` 占满/response read backlog 明显相关，首先量化 JSON 编码、segment paths/glob/stat、open/flush/fsync 的占比。
- 仅优化**不改变 durability**的高频元数据操作（例如可证明正确的 segment-path 元数据/active-size 缓存、一致性失效刷新、重复 stat 消除、有限文件句柄复用）；关注跨进程同时写、rotate、GC、重启与异常恢复。**上一轮同方向 candidate 失败，必须解释原因且提供针对性回归，不能复制旧补丁。**
- 若证据显示真正问题是每个 response callback 的强同步磁盘持久化，**只提交诊断报告和可选设计比较，不得未经授权直接改为异步 ACK 或移除逐事件 fsync。** 评估有界 response Journal 队列或批量 fsync 只能作为独立未来方案，需先完整证明 shutdown/crash/hard-limit 下的 durable/unknown/at-most-once 语义。

### H2：单 Writer 每批 GC / offset / status / diagnostic I/O

- 按 stage metrics 确认 `_persist_offsets()`、目录 fsync、`journal.segment_paths()`、`reclaim()`、`_write_process_status()` 对吞吐的贡献。
- 如高频状态写明显，优化到 bounded interval **不能延迟实际 DB COMMIT/offset durability**，也不能让 crash 恢复读到过期或不可信 offset；可先将纯诊断更新频率从每 batch 降到周期性，但仍要在启动/异常/停止时强制刷新正确状态。
- `offset persist` 可以做独立 A/B 设计，但默认**每个已提交批次仍持久化 offsets**；若考虑批量 checkpoint，必须先证明崩溃重放、segment GC 和重复统计正确，并在获得明确决策后实施。

### H3：Worker/route/Spool 热路径

- 若 CPU/prefetch/partition 扫描或 bind 租约导致发送明显限速，仅修复可定位的热点；不得通过突破 per_bind TPS、绕过 window 或压低 cutoff 检查换取高 TPS。
- 若 feeder 自身不能持续供给，修测试 feeder 而非改产品主逻辑；保持独立 producer rate 与 socket write rate。

### H4：DuckDB SQL 层

- 若真实 E2E 指标再次表明 DB stage 消耗主导，才运行 A3 vs candidate 的同环境大样本对照；已验证 `100K–500K events` A3 高效，**没有实证绝不重复做大范围 dedupe/aggregate 重构**。

**R3 候选采用门槛**：三次 paired median 至少约 **+15%** 的相关瓶颈指标，或明确显著减少 600s backlog/drain 且无发送/响应/正确性/RSS 回退；若没有候选达标，保留 `NO_CHANGE` 并停止该方向，报告瓶颈而不是编造收益。预设 +15% 为有意义差异筛选，不等于生产 SLA。

## 7. R4 — 正确性、安全恢复与故障回归（不可删减）

针对 R3 实际改动，新增/复用定向测试，至少覆盖：

1. `socket_written` / `submit_sm_resp_success` / `submit_sm_resp_error` / `unknown` 每个阶段正确计数；multipart 1 task 对多 PDU，response event ID 在 reconnect/sequence reset 下不发生错误抵消；不可把 `unknown` 冒充已确认响应。
2. **Journal hard-limit 在 SMPP window 尚有 pending 响应时发生**：不卡死、不因抛异常静默退出 reader；保证异常结果可见、无原短信重发；持久化失败时不虚报响应 durable。
3. Worker restart、Writer restart、DB COMMIT 后 offset persist 前 crash、offset persist 后 GC 前 crash、replay 再读：`event_id` 幂等、aggregate 无重复，segment 严格安全回收。
4. 业务日 23:59:59 → 00:00:00：Worker 预取、等待 Bind/TPS/window、multipart 部分发送、跨天重启；验证**旧日无新 socket write**、不重派、不污染新日统计。
5. `feeder pause 30–60s`、Route A 断开/Route B 健康、IPC hint queue full、Spool 满后新日回收、Journal high-water 后解除，保持 bounded stage pressure。优先复用现有 T6 和 T7-1…T7-9 执行器。
6. 修改后先跑相关 targeted tests，再跑 native ext4 backend full pytest；前端未改无需重复构建（若修改前端则 `npm run build`）。

注意：`response window` 与 durable sink 在异步回调链上，必须对 `callback` raise / IO Error / full Journal / reader disconnect 显式断言；不要用“没有进程 crash”替代完整性测试。

## 8. R5 — 生产资源 Profile（8–16 GiB），与 2 GiB stress 分离

### Profile 定义（初始假设，需在目标机冻结真实配置）

| Profile | 总内存目标 | 初始应用进程 RSS guard | 用途 |
|---|---:|---:|---|
| `PROD_8G` | 8 GiB | 6 GiB | 最低目标生产规格验收 |
| `PROD_16G` | 16 GiB | 12 GiB | 扩展规格容量验收 |
| `LOW_MEM_2G` | 2 GiB | 2 GiB | 专项降级/回压压力测试，**不作为 8–16G 的否决指标** |

- 上表是**待确认的 sizing 模型**，不是已证明的目标服务器配置。还需查 `RAM 实际可用`、`container/cgroup memory.max`、CPU quota、CPU affinity、nice/ionice、swap、磁盘 I/O/限额；如果与建议有差异，记录真实值而不是硬编码“已符合生产”。
- 8 Worker/4 Bind/200 per_bind/window 50 作为统一测试输入；监控 per-role RSS、PSS（若能可靠获得）、cgroup peak、RSS slope 及吞吐/Journal backlog；**不能单纯因为总内存更大就忽略无界增长**。
- 在没有当前回合明确授权和必要网络访问条件时，**只完成本地 ext4 的 R0–R4/R5 本地代理 profile**；EulerOS、真正 8/16 GiB 主机高压 Gate 明确 `BLOCKED_ENV / NOT_VERIFIED`，不得远程连接或擅自要求用户改变 VPN。不得以 WSL 15 GiB 内存冒充 16 GiB 生产环境。
- 若后续获得服务器授权，再在**独立新 Run 根目录**执行同 SHA、同环境 A/B，不能覆盖旧运行数据；推荐先 P5500 → T3 50K → 600s/800 PDU/s，再按需要小量 multipart。测试期间使用 1 秒采样与有界资源 guard；尽量避免影响已有服务。

### 最终高压 Gate 预期

- 持续 600s 实测 `socket-written ≈ 800 PDU/s`（允许明确记录 workload/SMSC 限制；没有达到则不签高压 PASS）。正常单段成功短信将产生约 `1600 Journal events/s`；按实际事件结构而非固定数字评估。
- 整个 steady 期间 Worker/SMPP/Journal/Projection 同时运作；以 `committed projection events/s` 和 **backlog slope** 判断长期能力。
- feed stop 后分别给出 send/response/journal/projection/offset 时刻。**目标**：在合理有界时间（建议 Projection 独立排空 ≤60s）完成全部已产生事件；阈值若不符合真实业务需求则必须事先确定，而非跑完后改口。
- Journal/DB/summary 的数量、ID、跨 Campaign、跨日、multipart parts 及 replay 正确；duplicate *send* 为 0、old-day 新 PDU 为 0、unknown 分类无混淆。
- 旧/新同资源三次 paired baseline，不能把历史 EulerOS 4CPU+nice/ionice 受限试验和本地 22CPU 结果混作回归依据。

## 9. 建议输出及 Gate 结构

请创建或更新以下新交付文件（尽量使用已存在工具，减少新永久代码）：

1. `Docs/Marketing_Copilot_VNext_E2E_Critical_Path_Profiling_and_RCA_Report.md`：源码实际链路、阶段耗时、response window 与 Journal fsync 的因果定位、占比、瓶颈证据。
2. `Docs/Marketing_Copilot_VNext_E2E_Performance_Optimization_AB_Report.md`：基线/candidate 各三次 median、发/收/Journal/Projection 曲线、SQL 和 IO 阶段成本、候选采用/拒绝理由。
3. `Docs/Marketing_Copilot_VNext_Production_Profile_and_Canary_Gate_Closure_Report.md`：8/16 GiB 模型与实际机器区别、R0–R5/T6/T7、可用于发布判断的明确 Gate。
4. 可选 `Docs/Marketing_Copilot_VNext_E2E_Performance_Evidence_Summary.json`：机器可读结果；**只引用受控 Run**，不得上传 DB/WAL/Spool/Journal、真实电话号码、凭证、venv 或过大日志。

最终 Gate 表至少包括：

| Gate | 满足条件 |
|---|---|
| `SOURCE_AND_ENV_IDENTITY` | Git SHA/tree、代码 hash、真实 CPU/FS/内存设置及依赖冻结 |
| `CRITICAL_PATH_MEASURED` | 所有关键完成时间点及 stage time 有效、跨进程时钟口径一致 |
| `INSTRUMENTATION_LOW_OVERHEAD` | ON/OFF A/B 不引入显著运行时回退 |
| `SMPP_RESPONSE_DURABILITY` | response event / PDU、unknown、window、callback 错误路径无缺口 |
| `SINGLE_WRITER_SUSTAINABILITY` | 实测 committed events/s、backlog 稳态与可排空时间可信 |
| `PROD_8G` / `PROD_16G` | 实际目标环境配置满足，guard、steady 与 drain 完整对账；模型值不可标 PASS |
| `T6_BUSINESS_DAY` | 服务级旧日不发送、不重派、隔离/回收测试 PASS |
| `T7_RECOVERY_9_OF_9` | 当前 SHA 完整或明确 `PARTIAL/NOT_VERIFIED` |
| `EULEROS_600S_800PDU` | 经授权真实目标环境 600s steady + drain 成立；无服务器则 `BLOCKED_ENV` |
| `CANARY_READY` | 所有强制项真正通过才 `YES`；否则 `NO_GO` |

每个 Gate 必须写 `PASS/FAIL/NOT_VERIFIED/BLOCKED_ENV` 中一个（可另加说明）；区分单元测试、native loopback 和真实目标环境测试。失败记录与对照必须保留，不得只汇报成功样本。

## 10. 任务顺序与停止规则

1. **R0**：先冻结环境与历史基线；检查真实代码，不需要任何额外环境授权即可推进。
2. **R1**：增加最低侵入性的关键路径测量。先跑小测试并校验 counters 正确。
3. **R2**：明确完整链路耗时归属，输出 bottleneck ranking。若无有力证据，停止功能性优化，交 RCA 报告。
4. **R3**：仅针对已经证明占主导的热点做一个小 candidate，走 ON/OFF、三次 paired A/B。未取得收益或出现新 response/window 失败则**全部回滚产品代码**、保留测量改进与报告。
5. **R4**：对保留的改动完成 T6/T7 关联正确性回归及 full native backend pytest。
6. **R5**：本地完成 production-sizing 模型及适当测试；无当前授权则目标服务器门禁 `BLOCKED_ENV`。无需为凑齐测试而安装/连接无关外部工具。
7. 最终报告明确 `NO_CHANGE` 或已接受的 minimal change、所有成功/失败证据以及剩余阻断。**不要再产生“大规模优化完成但实际速度不明”的模糊结论。**

### 必须回报的简洁结论格式

```text
BASELINE_SHA=24708c8eb183d689d4f1e8e3df7f63a0ed81c222
CURRENT_SOURCE_SHA=<actual>
PRODUCT_CODE_CHANGE=NONE | ACCEPTED_MINIMAL_CHANGE | CANDIDATE_ROLLED_BACK
PRIMARY_LIMITING_STAGE=<measured stage or NOT_VERIFIED>
SECONDARY_LIMITING_STAGE=<...>
SOCKET_PDU_RATE_P50_P95=<measured>
RESPONSE_PDU_RATE_P50_P95=<measured>
JOURNAL_DURABLE_EVENTS_PER_SEC=<measured>
PROJECTION_COMMITTED_EVENTS_PER_SEC=<measured>
RESPONSE_CALLBACK_P95_MS=<measured>
JOURNAL_APPEND_FSYNC_P95_MS=<measured>
SMPP_WINDOW_WAIT_P95_MS=<measured>
FEED_STOP_TO_LAST_SOCKET_SECONDS=<measured>
FEED_STOP_TO_LAST_RESPONSE_SECONDS=<measured>
FEED_STOP_TO_JOURNAL_DURABLE_SECONDS=<measured>
FEED_STOP_TO_PROJECTION_COMMIT_SECONDS=<measured>
FEED_STOP_TO_OFFSET_DURABLE_SECONDS=<measured>
JOURNAL_BACKLOG_BYTES_SLOPE=<measured>
RSS_APPLICATION_PSS_CGROUP=<measured or NOT_AVAILABLE>
RESOURCE_PROFILE=PROD_8G | PROD_16G | LOCAL_REFERENCE | UNCONFIRMED
T6_BUSINESS_DAY=<PASS/FAIL/NOT_VERIFIED>
T7_RECOVERY_9_OF_9=<PASS/FAIL/NOT_VERIFIED>
FULL_BACKEND_TEST=<result>
EULEROS_600S_800PDU=<PASS/FAIL/BLOCKED_ENV/NOT_VERIFIED>
CANARY_READY=YES | NO_GO
COMMIT_PUSH=NOT_DONE (unless explicitly requested)
```

---

## 11. 直接给 Codex 的执行指令

> 阅读本 MD 与仓库 `AGENTS.md`，复核 `origin/main` 当前 SHA 和全部已知报告。按 **R0 → R1 → R2 → R3（仅在有证据时）→ R4 → R5** 实施。不要连接远程服务器、真实 SMSC/API/DB、不要部署、不要 commit/push，保留其他 dirty/untracked 文件。优先精确量化 `SMPP response reader → Worker._response_event → ProgressJournal.append(fsynchronize) → ProjectionWriter Journal replay/COMMIT/offset` 的实际时间与背压关系，区分 feed、socket、response、Journal durable、DB commit、offset durable 的独立完成时刻。验证 instrumentation 自身无明显开销，再在同 native ext4、同资源条件下做 paired A/B。不要移除逐事件 fsync、自动重发 unknown、放宽跨天截止、拆分 Writer 或增加数据库。只有明确实测收益且通过完全对账的最小代码变更才保留；失败实验必须回滚。输出三份 RCA/AB/Gate 报告与机器可读证据摘要；所有未验证项如实记 `NOT_VERIFIED/BLOCKED_ENV`，不得替代真实验收。

**总体完成标准：用可靠数据回答当前系统哪里慢、为什么慢、是否能安全优化，以及下一步是否真的需要远端 8–16 GiB 正式验收。**
