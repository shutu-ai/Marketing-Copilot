# Marketing-Copilot VNext — Worker 生命周期卡滞 RCA 与 8/12/16/24 清洁退出闭环（Codex 任务）

**Task ID:** `MC-VNEXT-WORKER-LIFECYCLE-CLEAN-EXIT-20261018`  
**Repository:** `https://github.com/shutu-ai/Marketing-Copilot`  
**已核对的 origin/main HEAD:** `a25726d47d5963df85b7da754733029591345539`  
**目标环境:** 已授权 `paas-core01`，SLES 12 SP5 / native ext4，synthetic `127.0.0.1` SMSC  
**优先级:** P0 — 多 Worker 正确性门槛失败，阻断扩容测试  
**执行方式:** 先取得卡滞现场证据 → 归因 → 最小修复 → 复现/回归 → 有门槛的性能复测；**不是再次只出一份 RCA 的任务**。

## 0. 任务目标与最终交付

**要解决的问题：** 8 Workers 的 1K smoke 能完整退出，但 12 Workers 缺 1 个清洁退出/结果文件，24 Workers 两次分别缺 3 个和 1 个；16 Workers 曾运行约 230 秒并出现高 CPU 子进程与僵尸进程而被中止。异常的已完成运行中，`accepted/socket_written/submit_sm_resp_success=1000/1000/1000`、Journal/Projection `2000/2000`、unknown/duplicates `0`、最大活动 Bind `4`。因此数据面已正确，并不等于 Worker 生命周期正确。必须找到**不能清洁退出的具体调用链**，不能继续仅增加 `join()` 超时。

**成功定义：** 在 SLES 服务器、固定 `4 binds × 200 TPS/per-bind`、`window=50`、相同 synthetic workload 下，**8/12/16/24 Workers 各至少连续两轮 1K smoke 全部完成**：每个 Worker 写出带校验的最终结果文件；父进程收到全部清洁 `exitcode=0`；无存活测试进程或遗留 Bind；所有 1K 数据面和 durable 结果完整对账。然后才允许进行两轮反序 5K 对照；24 Workers 仍是重点候选，不是预设赢家。

**交付物：** 必要的最小源码/测试修改 + 3 份文档（RCA、Closure/Scaling、JSON Evidence）。报告须指出确切阻塞位置、可重现原因、修复前后证据和有无有效吞吐改进；若证据不足，不得虚构根因或宣称 PASS。

### 范围边界

- **不改变**：生产 `route_tps=200 per bind`、`pool_size=4`、`window=50`、TPS/Bind 全局约束、Journal 每事件 durability/fsync、TaskSpool dispatch checkpoint、at-most-once-after-dispatch、unknown 禁止自动重发、T6 跨日门禁、T7 恢复语义、单 ProjectionWriter/单 DuckDB owner、batch 默认值。
- 允许先改**诊断 Harness**（`backend/tools/vnext_streaming_acceptance.py`、测试工具与测试）。**如果已经用现场证据确定问题在生产的 `backend/vnext/worker.py`、`spool.py`、`route_credits.py`、`smpp_transport.py`，则允许实现最小、可测试的生产修复**；禁止以“不得动生产代码”为由无限重复失败测试。每项生产改动都要逐条说明为什么不会改变发送/持久化语义。
- 严禁用跳过 fsync、提前 checkpoint、抹掉 pending、放宽 clean-exit gate、降低安全超时门槛或直接 `SIGKILL` 把测试伪装成 PASS。强制中止仅用于异常样本停止与现场保全。
- 不连接真实 SMSC、真实号码、真实生产数据库/API；不修改生产环境，不部署上线。
- 保留工作区其他 dirty/untracked 内容；**未经明确请求，不 commit、不 push**。不要提交测试 DB、Journal、日志、密钥、个人信息和服务器运行数据。

## 1. 已有真实证据（需引用并复核，不能重复猜测）

| 1K smoke | 实际 Worker | 结果文件/清洁退出 | 数据面 | 判定 |
|---|---:|---:|---|---|
| `smoke-8` | 8 | 8 / 8 | 1000/1000/2000/2000 | PASS，14.222s |
| `smoke-12` | 12 | 11 / 11，缺 #5 | 完整对账 | FAIL，137.530s |
| `smoke-24` | 24 | 21 / 21，缺 #11/#13/#17 | 完整对账 | FAIL，65.692s |
| `smoke-24-r2` | 24 | 23 / 23，缺 #18 | 完整对账 | FAIL，140.463s，120s join 仍不足 |
| `smoke-16` | 观察到 16 进程 | 未得到有效结果 | 未完整捕获 | ABORTED，高 CPU/僵尸，约 230s |

来源：

- `Docs/Marketing_Copilot_VNext_Worker_8_12_16_24_Scaling_RCA.md`
- `Docs/Marketing_Copilot_VNext_Worker_Scaling_AB_BA_and_Recommendation.md`
- `Docs/Marketing_Copilot_VNext_Worker_Scaling_Evidence.json`

**代码审计的重要事实与假设（待实证，不得直接认定）**：

1. `backend/tools/vnext_streaming_acceptance.py::_worker_process.loop()` 只有在 `await worker.run_until_idle(close_transports=False)`、`await _drain_transport_responses()`、`worker.close_transports()`、`spool.lightweight_snapshot()` 全部返回后才检查 `stop_event`。因此 `stop_event` 置位不保证阻塞中的 `run_until_idle` 立即返回。
2. 同文件的 `_worker_bind_target(pool_size, worker_count)` 在 Worker 多于 Bind 数时每 Worker 仍可能创建连接目标；实际物理 Bind 同时最多 4，多个 Worker 进入 `AsyncSmppTransport.connect() → RouteCreditBroker.acquire_bind()` 竞争，等待或重试可能阻碍清洁停止。`acquire_bind` 包含文件锁与 `asyncio.to_thread`；要区分等待 Bind、等待 `_connect_lock`、执行器线程未收尾与真正死锁。
3. `ShardWorker.run_until_idle()` 自有 `_stop` 事件，但当前 Harness 外部 `multiprocessing.Event` 不直接调用 `worker.stop()`；其内部 `pending` Task 会等待全部完成才返回。应检查这些 Task 的实际 `await` 栈及“global pending=0、local pending>0”的情况。
4. `TaskSpool.lightweight_snapshot()` 每次会检查 256 个分区的 cursor/taken 文件签名；多 Worker 在约 20ms polling 中可能形成较高聚合 metadata I/O 和高 CPU。**只有采样证明才允许针对这一点改动**。
5. 目前结果文件只在 `asyncio.run(loop())` 返回之后落盘；缺 `worker-N.json` 仅证明没完成整条退出路径，不证明 worker 没做任何工作。`asyncio.run` 结束时的默认 executor 关闭也要纳入定位。
6. 父进程先等待完整 Journal/Projection，再统一 `stop_event.set()`，然后逐 Worker `join(WORKER_JOIN_TIMEOUT_SECONDS)`。需要区分“Worker 还在处理合法工作”“Worker 已空闲但没收到停机信号”和“子进程已经退出、父进程收尸/文件状态异常”。**僵尸 (`Z`) 不是高 CPU 活任务**，不要混称。

## 2. R0：冻结现场与证明测试有效

1. 检查 Git HEAD、`origin/main`、当前 dirty/untracked；遵循 `AGENTS.md`：有 `.codegraph/` 先用 `codegraph explore` 定位符号。不要清除无关修改。
2. 冻结 SLES source archive 和 SHA-256、Python 3.11、DuckDB 1.2.0、wheelhouse、kernel、native ext4 挂载点、CPU/RAM、`df`、`ulimit -n/-u`、进程/线程数量、load 与 `iostat`；所有测试以**新独立** `run-root`、detached 进程运行。保证足够余量；若磁盘/进程资源不足，先 STOP_ENV。
3. 验证 Harness 实际 `--workers 8|12|16|24` 未被静默截断，256 PartitionMap owner 完整且唯一；每个子进程独立 Journal 路径、最终结果路径唯一；物理 Bind 限制仍是 4。
4. 记录测试用到的 `WORKER_JOIN_TIMEOUT_SECONDS`、`stop_event`/`feeder_done` 时序、父进程 completion 时刻、进程退出时刻。结果文件、退出码、pid/pid-start-time 必须和同一运行匹配。
5. 尽量**复用现有失败 Run 原始证据**，只做一次小规模受控现场复现；若当前工作空间已含带堆栈的有效失败日志，优先分析，避免无效重复跑。

## 3. R1：可观察卡滞现场（此阶段只准改测试诊断）

### 3.1 Worker 状态机追踪（必须）

在测试 Harness 中增加低开销 per-worker state heartbeat，**固定频率或阶段变化才写入**原子诊断 JSON，不在每条短信热路径同步 fsync：

- `worker_id/pid/ppid/pid_start_time`, `phase`, `phase_entered_monotonic`, `last_progress_monotonic`, `loop_pass_count`；
- 关键阶段：`BOOT`, `SPOOL_INIT`, `RUN_UNTIL_IDLE_ENTER/EXIT`, `WAIT_BIND`, `DRAIN_RESPONSES_ENTER/EXIT`, `CLOSE_TRANSPORT_ENTER/EXIT`, `SPOOL_SNAPSHOT_ENTER/EXIT`, `STOP_EVENT_OBSERVED`, `FINAL_SNAPSHOT`, `ASYNCIO_RUN_RETURNED`, `RESULT_FILE_WRITTEN`, `PROCESS_EXIT`；
- `stop_event.is_set`、`feeder_done.is_set`、本 Worker 拥有的分区数、局部已处理量与 local inflight、**全局** pending、active bind leases、response pending、journal durable count；`global_pending` 必须标明只是全局值，不当作本 Worker 队列。
- 记录父进程 `PRODUCER_DONE`, `ALL_PDUS_RESPONDED`, `JOURNAL_EOF`, `PROJECTION_CAUGHT_UP`, `STOP_SIGNAL_SET`, `JOIN_START/END`, `TERMINATE_REQUESTED` 的 UTC 与 monotonic。不要只记录末尾状态。
- 防止 diagnostical path 自己制造大量竞争：阶段标记写入无 fsync、限频，单进程本地文件；正式性能复跑应提供诊断 ON/OFF 可控模式。

### 3.2 Python / OS 栈和活跃任务（必须）

- 受控测试启动时为 Worker 预置 stdlib `faulthandler`（如使用 SIGUSR1 触发仅落到隔离日志）；在检测到 Worker 多秒**无状态推进**时，自动采集一次或少量 Python **线程栈**、`asyncio.all_tasks()` 的协程栈与 pending Task 名称、Executor 工作线程；不能仅依赖事后日志。尽可能不打断进程或改变业务状态。
- 同步采样 `/proc/<pid>/stat/status/wchan`、线程状态/CPU ticks、PPID、fd 数、`ps` 进程树、Bind lease 文件内容中**非敏感字段**（slot/pid/age；遮盖 token）。工具不可用或权限不够则标 `NOT_CAPTURED`，不能假报零。
- 分析高 CPU 与 BLOCKED 的区别：高 CPU 是否在 `_seed_buffers/fill`、`spool.refresh/lightweight_snapshot` 或循环重复创建 Task；阻塞是否在 `acquire_bind`、`asyncio.Lock`、`asyncio.to_thread`、`await asyncio.wait`、journal/fsync、socket `wait_closed`、`asyncio.run` executor shutdown。
- 父进程针对 Worker `exitcode is None/0/非0` 与 OS state `R/S/D/Z` 分开保存；不要把 zombie 当成“仍在执行 Python”。结果文件写入须记录异常，当前未捕获的异常要明确上报。
- 在运行结束前采集；异常时保留少量堆栈/时序，随后按现有超时边界清理进程组，禁止为了诊断无限挂住或产生残留。

### 3.3 小规模有对照复现（必须）

1. 在相同环境依次运行 **8、12、16、24 Worker 的 1K**；若 12 已重复且证据足够，优先定位 12 以减少成本。每个 run 独立目录且串行执行。
2. 重点故障不是 PDU 数，而是 **哪个 Worker 具体在哪个阶段停住**，有无持有 Bind、是否拥有未 checkpoint 任务、是否有 pending asyncio Task。
3. 建立 `failure_signature`：例 `GLOBAL_DONE_LOCAL_BIND_WAIT`, `SPOOL_SNAPSHOT_POLL_LOOP`, `ASYNCIO_EXECUTOR_SHUTDOWN_STALL`, `WAIT_CLOSED_STALL`, `PARENT_JOIN_REAP_ONLY`；只使用确有证据的类型，无法定位则 `INCONCLUSIVE`。
4. 在无卡滞情况下不为了凑失败次数无限重复；至少保留一次可复现有效故障证据或者证明已无法复现，并正确区分这两种结果。

**R1 Gate**：必须输出至少一条**缺失 Worker 的 phase + 栈 / wait condition + 父子时序**或明确 `NOT_CAPTURED` 的证据差距；只有有效证据才能进入原因特定的代码改动。

## 4. R2：判定根因，选择最小修复（按证据分支）

请先提供 1 页 `RCA hypothesis → observed proof → chosen fix`，避免同时改多个无关模块。

- **如果**外部 `stop_event` 已置位，但 Worker 卡在 `run_until_idle()`，先区分合法 pending Task、已耗尽本地任务但仍等待 Bind、空转循环。考虑在现有**有界 await/循环边界**安全地消费明确的 shutdown/cancel 意图，且必须保留已 dispatch 的结果落 Journal 与 response drain。禁止粗暴取消已写 socket 的任务或抛弃 unknown 事件。
- **如果**连接竞争产生大量等待任务，定位 `_connect_lock → acquire_bind → to_thread` 的等待与重复任务创建；在确认当前 Worker 没有待发送任务、全局 durable 结果完成时允许其清洁停止，或仅修复不必要的空转/重复连接；不能改变 Bind 数、路由限速或绕过超时与失败记录。
- **如果**Spool poll 成本导致卡滞，优先限制**测试循环的无用全局 metadata 扫描**、引入合理的事件/有限退避与准确 stop 检查；任何 cache 必须保证别的进程新写入的记录、taken/cursor、day cutoff 可见，不能因 stale snapshot 发生漏发/重发。
- **如果**`asyncio.run` 退出后仍卡在 Executor，定位未收尾线程任务并有界回收；不能“强行跳过”未完成的 durable 任务。不要把结果文件写在尚未真实清洁结束之前来刷 PASS。
- **如果**仅父进程 `join()`/reap 实现有问题，修正父进程生命周期管理并保留真实退出码：可考虑一个**全局截止时间**、并行非阻塞轮询全部 Worker（避免 `N × timeout` 的串行延迟），但**只优化 join 无法解决子进程真实卡滞**。
- **如果**结果文件写失败，捕获异常并做最小文件写入修复；只报告实际成功完成且进程 `exitcode==0`。文件存在与 exit success 要分别满足。

最小修复允许修改的路径以证据为准：

- 首选 `backend/tools/vnext_streaming_acceptance.py`（若确定只属于 harness 停机/观测协议）；
- 若是产品同样会遇到的真实问题，可最小改 `backend/vnext/worker.py` / `spool.py` / `route_credits.py` / `smpp_transport.py`，必须附同场景故障复现单测与生命周期/恢复单测；
- 必须明确：每条修改解决什么等待条件、为什么不会丢已接受任务、为什么不会触发已 dispatch 自动重发。

**禁止作为修复**：单独延长 join 到 180/300/600s、压低 Worker 目标、取消缺文件检查、让异常退出 `exitcode!=0` 也算 PASS、降低 durability、改成 8 Bind / 提升 TPS、清空 spool pending 或以结果文件代替完整对账。

## 5. R3：确定性回归与正确性门禁

### 5.1 单测 / 集成测试

新增定向测试覆盖：

1. `stop_event` 在 Worker idle/等待 Bind/扫描 Spool/response drain/close 过程中触发的退出路径；
2. 4 Bind × 8/12/16/24 Worker（用测试替身控制竞争），不会超配 Bind、不会在不安全的 pending 状态提前退出；
3. 模拟 `acquire_bind` 等待后解锁、超时、异常与 shutdown 并发；证明不会永远挂住，也不会造成漏写/重发；
4. Spool 多进程 cursor/taken 可见性、分区 owner 正确、被暂停/取消任务及 day cutoff 行为不破坏；
5. Worker result file 写入错误、部分 Worker 异常退出、父进程 join/reap 与清理动作能够准确标 `FAIL`；
6. T6 当前业务日切换、T7 当前 `6/6` 恢复矩阵和核心 at-most-once/failure tests；不得捏造不存在的 T7-1..3；
7. 其他后台 Worker 不影响总体 durable 进展，最终 `Journal/Projection` offset durable 到 EOF。

先做 Harness 定向 tests，再跑仓库既有 backend suite。旧 Windows 原子替换锁 `WinError 5` 如再出现，应原样记录、复跑并归类，不能私自忽略或误称全量首次 PASS。

### 5.2 SLES 1K 真实 smoke 闭环：**硬门槛**

按照固定 workload (`250,250,250,250`) 进行 **8/12/16/24 Workers，每档至少连续两轮**；为避免时序偏差，第二轮采用反向顺序。每轮都必须同时满足：

- `requested_workers == effective_workers == actually_spawned_workers == worker_result_files_count == workers_exited_cleanly_count`；所有 Worker `exitcode==0`、无 `Z`/存活遗留进程；
- `accepted == socket_written_tasks == socket_written_PDU == loopback_received == submit_sm_resp_success == 1000`，各 campaign 一致、unknown/duplicate/write_failed 均为零；
- Journal durable `2000`、Projection applied/unique `2000`、durable offsets 到 EOF、backlog=0、completion=`COMPLETED`；
- 每轮 `max_active_binds <=4`、每 Bind `200 TPS` 约束未违反、`window=50`、memory/resource gate PASS 且采样真实；
- 记录每个 Worker stop signal → exit 的耗时分布（P50/P95/Max），只要求可解释且在原定生命周期预算内完成，**不能放宽预算才过门槛**；若原预算是 120s，可作为诊断上限，不等于“120s 内的高延迟合理”；
- 不再出现 `16 Worker` 高 CPU 无界空转及强制退出。

**任一档失败即停止正式吞吐矩阵，判为 `CORRECTNESS_BLOCKED`，但保留完整 RCA 和实施证据。**

## 6. R4：仅在 1K 全 PASS 后恢复扩容性能对照

### 冻结配置

- `4 campaigns × 1250 = 5000`、`workers=8/12/16/24`，pool=4，`TPS=200 per bind`，window=50；Producer batch=250，Projection A0/vectorized/batch=512；status interval=0.1s；`PROD_8G` application RSS gate。正式 Run `instrumentation=ON`；如果证据显示观测显著影响速率，可追加 OFF paired diagnostic，但不能静默混用统计。
- 所有 Run 使用同一 SLES 原生 ext4、同一 source archive、同一 Python/依赖、相同日期业务时钟窗口、独立 run-root；按 `8→12→16→24` 和 `24→16→12→8` 做两轮顺序对照。如 24 出现资源或正确性风险立即停止该档。

### 统一统计，防止吞吐假阳性

每轮必须单独记录：

- `stage_active_PDU_per_sec`（first → last socket write），`feed_phase_PDU_per_sec`，`e2e_wall_PDU_per_sec`（整个运行），以及 `response_phase_PDU_per_sec`，分母写清；
- 实际 4 Bind 活动时间/利用率、Bind acquire wait P50/P95/Max、并发持有量曲线、等待 Worker 数、重连数、Credit refill/unused、TPS wait/window wait P95；
- Worker per-id `tasks_taken/socket_written/response`, 局部 active/idle, pass 数, 队列与 checkpoint 耗时；
- Journal append/fsync 与总事件率、Projection commit/staging/offset/durable drain、`backlog_bytes` 在 feed/发送期间的变化和最终是否归零；
- 进程数、CPU、RSS（app 与 harness 分开）、FD/线程、磁盘读写与 snapshot 采样有效性；
- Per-campaign 完整对账、unknown/duplicate/late response 与 T6/T7 不回退证据。

**比较门槛（建议，不等同于生产 SLA）：** 24 vs 8 在两轮都有效正确时，发送阶段中位数收益至少约 `+15%` 且 E2E wall throughput 收益至少约 `+10%` 才将 24 列入下一步候选；若 16 已达到 24 的 95% wall throughput 且消耗明显更少，则优先 16。收益随 host 负载波动或 Projection backlog 持续增加时，不能宣布赢。没有价值时选择 8/12 或 `NO_SCALING_BENEFIT`。不要在未测的 Worker 数量上推定性能。

### 有门槛的扩大测试

只有两个 5K 交叉顺序都通过、收益稳定、后台 Journal/Projection 不发散、资源合理且磁盘容量足够时，**可**对 8 与赢家做一次成对 50K 正式对照；不是必做。600 秒/800PDU/s 作为后续另行授权容量验收，不在本任务自动执行。不部署 Canary。

## 7. 交付文件与机器证据

在仓库下生成/更新：

1. `Docs/Marketing_Copilot_VNext_Worker_Shutdown_Lifecycle_RCA.md`：故障因果链、源码调用栈、PID/阶段/等待对象、修复前后对照、未证实假设/证据缺口。
2. `Docs/Marketing_Copilot_VNext_Worker_Clean_Exit_and_Scaling_Decision.md`：8/12/16/24 的两轮 1K 正确性、条件性 5K AB/BA、性能/资源/风险比较、明确赢家或拒绝扩容理由。
3. `Docs/Marketing_Copilot_VNext_Worker_Lifecycle_Closure_Evidence.json`：机器可读，至少包含以下字段：

```text
SOURCE_HEAD / ORIGIN_MAIN / SOURCE_SHA256 / HARNESS_SHA256 / PRODUCT_DIFF
SERVER_OS_NATIVE_EXT4 / PYTHON_DUCKDB_FREEZE / HOST_RESOURCE_PREFLIGHT
FAILURE_SIGNATURE / FAILED_WORKER_IDS / STUCK_PHASE / LAST_PROGRESS_TIMESTAMP
STOP_EVENT_SET_AT / PER_WORKER_STOP_OBSERVED_AT / WORKER_EXITCODE / RESULT_FILE_PRESENT
STACK_TRACE_PATH_AND_SHA256 / ASYNCIO_TASKS_SUMMARY / THREAD_WCHAN_CPU_SAMPLES
BIND_SLOTS_OWNED_AND_WAITERS / SPOOL_LOCAL_GLOBAL_PENDING / JOURNAL_DURABLE_OFFSETS
ROOT_CAUSE_CONFIDENCE / PATCH_REASON / SEMANTIC_INVARIANTS
SMOKE_W8_W12_W16_W24_TWO_RUNS / PER_WORKER_CLEAN_EXIT / SMSC_AND_DURABLE_COUNTS
5K_AB_BA_STATUS_AND_RATES_OR_NOT_RUN / 50K_STATUS_OR_NOT_RUN
DUPLICATE_UNKNOWN_REDISPATCH / T6 / T7_CURRENT_6_OF_6 / TEST_RESULTS
PRODUCTION_DEFAULT_CHANGED / PRODUCT_CODE_CHANGED / COMMIT_PUSH
FINAL_DECISION / CANARY_READY
```

诊断日志允许保留在服务器隔离路径，并用 SHA-256 索引；提交的报告只写非敏感摘要，不上传完整生产/个人数据。

### 允许的最终决策

- `WORKER_LIFECYCLE_FIXED_AND_SCALING_VERIFIED`：全 Worker 清洁退出；有效 5K 对照选出候选；不等于已经完成容量门槛。
- `WORKER_LIFECYCLE_FIXED_NO_SCALING_BENEFIT`：清洁退出修好，但 >8 Worker 无稳定收益，继续保持 8。
- `WORKER_LIFECYCLE_FIXED_SCALING_NOT_RUN`：1K gate 全过、5K 因合规环境限制未跑；不得宣称扩容有效。
- `CORRECTNESS_BLOCKED`：仍有 Worker 不退出、结果文件缺失或数据完整性失败；不得跑 5K/50K。
- `ROOT_CAUSE_INCONCLUSIVE`：证据不足以安全修复，必须给明确待补的**单个**证据项，而非猜测性重构。

## 8. Codex 最终回复模板

请最终回答以下项目，不要只写“已完成”：

```text
HEAD=...
CHANGED_FILES=...
ROOT_CAUSE=... (exact stack/wait/phase; confirmed or inconclusive)
WHY_RESULT_FILE_MISSING=...
CHILD_PROCESS_AND_ASYNCIO_THREAD_EXIT=...
MINIMAL_FIX_AND_SEMANTIC_REVIEW=...
SLES_1K_W8_W12_W16_W24=... (two each, valid exit counts)
5K_AB_BA=... (or NOT_RUN_BY_GATE)
24_VS_8_STAGE_AND_WALL=... (or NOT_VERIFIED)
BIND_4_TPS_200_WINDOW_50=UNCHANGED
SMPP_RESPONSE_JOURNAL_PROJECTION_RECONCILIATION=...
AT_MOST_ONCE_UNKNOWN_T6_T7=...
BACKEND_TESTS=...
FINAL_DECISION=...
CANARY_READY=NO (unless separately authorized and validated)
COMMIT_PUSH=NOT_REQUESTED
REPORT_PATHS=...
```

**执行要求：** 优先完成当前 task 的根因定位与必要代码修复，不要把 1K 正确性失败归结为“只需更多 Worker”或继续无限期跑性能测试。任何未通过的 gate 必须如实保留。
