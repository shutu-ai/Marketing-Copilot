# Marketing Copilot VNext — Static Code Audit & Targeted Native SIGSEGV RCA

> Codex 执行任务（静态证据优先；小规模动态验证辅助；禁止未经授权重跑 EulerOS D7/R2/P5500）  
> 审查日期：2026-10-16  
> **远端源码基线**：`2a8d1778b4f86bdaf75696aa1540bad45e5881e2` (`main`)  
> 当前历史结论：`RCA_STRONGLY_INDICATED`、`VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`。本文不覆盖历史结果。  
> 工作目标：追踪约 34–35 秒 `SIGSEGV (-11)` 的真实执行边界，清除可确认的诊断/观察开销缺陷，明确哪些问题可以仅静态证明、哪些必须由 Native Faulting Frame 或定向实验验证。**不是二次架构重构。**

## 0. 执行原则

1. 先阅读 `AGENTS.md`。有 `.codegraph/` 时先使用 CodeGraph 定位调用链；随后交叉阅读真实源码，而不是只根据符号搜索结论。
2. `git status --short`、`git rev-parse HEAD`、`git rev-parse origin/main`；在执行报告记录 SHA 与已有 dirty 文件。**不**执行 `git reset`、`git clean`、覆盖用户文件、`commit` 或 `push`（除非用户后续明确批准）。
3. 读取现有报告：
   - `Docs/Marketing_Copilot_VNext_Native_SIGSEGV_RCA_and_Runtime_Stabilization_Report.md`
   - `Docs/Marketing_Copilot_VNext_SIGSEGV_Evidence_Index.md`
   - `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`
   - `Docs/Marketing_Copilot_VNext_Implementation_and_Acceptance_Report.md`
   - `Docs/Marketing_Copilot_VNext_Performance_Comparison_Report.md`
4. 如工作目录内存在 `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/`，先验证 B2/D7 归档 SHA 与内部 manifest；**不得因未挂载到本机就伪称已再次读取证据**。未提供时以已提交报告作为二级证据，并标注缺口。
5. 不接入真实 SMSC/号码、真实数据库或业务任务；只允许合成数据与 `127.0.0.1` Loopback。禁止修改系统级 `sysctl`、全局 core 策略、系统 Python、系统库或公共目录。
6. 不增 Kafka、Redis、RabbitMQ、双轨 Reliable/Fast 或逐条 DuckDB 发送授权；不借 RCA 名义调整业务确认语义与吞吐验收合同。
7. 任何新证据必须关联：源码 SHA、文件与行号、进程角色/PID、运行参数、采集时间、运行目录、结果和证据路径。区分 `STATIC_CONFIRMED`、`HIGH_RISK_STATIC`、`HYPOTHESIS`、`DYNAMIC_CONFIRMED`、`UNAVAILABLE`。

## 1. 冻结的历史事实，不允许改写

- 修复后唯一获授权的 EulerOS D7/R2/P5500：4 Campaign `2500+2500+250+250=5500`、4 Worker、64 Virtual Partitions、Route TPS=80、Loopback 延时 2 ms、显式 `spawn`。
- D7 `SIGSEGV (-11)` 退出于 **34.361 s**；guard RSS 峰值 **559,325,184 B**，磁盘增量 **0 B**，没有触发资源 guard，`canary.json` 未生成。
- D7 Guard root 为 standalone CPython Canary command（报告 PID 37226），崩溃进程的 C/C++ Faulting Frame、core 均**未采集**。已清理的 resource tracker/worker 孤儿进程不能据此认定为 SIGSEGV 主因。
- B2/R7 在 EulerOS 上 `4000/4000` PASS，约 611.721 s，峰值 RSS 576,663,552 B；R2 FAIL 不能被 R7 PASS 覆盖。
- 修复后显式 `spawn` 仍然 SIGSEGV，故 `fork` 是历史风险假设，**不是已证根因，更不是单一确定根因**。
- 历史 `INTEGRATED_NO_GO` 与目前 `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL` 保持不变。不得用小负载通过发布生产许可。
- EulerOS 2.0 SP15 只是 SLES12SP5 性能/兼容性代理，不是原生 SLES12SP5 实机证据。

## 2. 本轮静态审计已发现的源码级证据与问题

### F01 — P0 / STATIC_CONFIRMED：30 秒 Watchdog 无条件以运行时长认定 stall

**文件**：`backend/database.py`，重点 `start_stall_watchdog()` / `_stall_watchdog_loop()`（当前 `main` 约 335–435 行）；`backend/tools/vnext_runtime_canary_acceptance.py` 的 `CanaryRunner.setup_environment()`；`backend/main.py` 的 `startup()`。

完整调用关系：

```text
CanaryRunner.setup_environment()
    MC_STALL_DIAGNOSTICS_DIR=<run-root>/stall-diagnostics
  -> Uvicorn lifespan -> main.startup()
  -> start_stall_watchdog(threshold=30s)
        -> faulthandler.dump_traceback_later(30, repeat=False)
        -> watchdog thread each 5s:
             run_stalled = now - _stall_watchdog_started_at >= 30
             -> faulthandler.dump_traceback(all_threads=True)
```

**确认问题**：`run_stalled` 未检查任何业务进度，健康运行超过 30 秒也会触发堆栈输出；“运行时间 >= 30 s”被标注成 `local_run_age`/stall。30 秒附近还存在独立的 `dump_traceback_later` 触发，两种诊断源可能叠加。D7 在约 34 s 崩溃，仅具时间关联；R7 通过说明 Watchdog 不是单独的充分条件。

**Codex 要求**：

- 证明实际启用、首次定时点、线程数、日志目标 fd 的生命周期；检查 `faulthandler.dump_traceback_later()` 和 `dump_traceback()` 是否可能并发、是否有关闭正在被底层处理的文件句柄风险。
- 明确“按时长采样”和“因失去进度而诊断”不是同一件事；修正误标记，不应让正常 Canary 运行自动被归为 stall。
- 创建**测试用** `watchdog enabled/disabled` 对照开关（应在启动前生效），使用单独 Native crash 捕获机制保证关闭 Watchdog 不会丢失崩溃证据。
- 用低风险静态+小型单元测试确认 30 s 到达时，仅 `run_age` 不再被误判为 `stalled`；不要用“关掉诊断后一次跑过”宣布 SIGSEGV 根因已经证实。

### F02 — P0 / STATIC_CONFIRMED：`/api/vnext/idle` 高频同步重建整份 Spool 状态

**文件**：

- `backend/tools/vnext_runtime_canary_acceptance.py`: `gate_r2()`、`gate_live()`、`wait_idle()`。
- `backend/routers/vnext.py`: `GET /api/vnext/idle`。
- `backend/vnext/runtime.py`: `VNextRuntime.wait_for_idle()` 每约 `0.05 s` 调用 `self.status()`；`VNextRuntime.status()` 每次新建 `TaskSpool(...)`。
- `backend/vnext/spool.py`: `TaskSpool.__init__()` → `_recover_index()` 扫描 Manifest / 各 Segment、索引和 JSON；`status()` 再调用 `refresh()`。

**确认问题**：前台 API 的 **async 路由**中执行同步全量文件 I/O 与 JSON 反序列化；R2 持续发送期间，这一路径会被频繁执行，对比 R7 的低频输入/状态采样负载形态不同。

**Codex 要求**：

- 静态画出频率、调用位置、每次分配、I/O 与所持锁；区分一次 `TaskSpool` 初始化的 O(历史任务数) 成本和 `status()` 相对便宜的计数。
- `wait_for_idle` 应使用一个**轻量、可验证的状态快照**或事件/进度通知；不要每 50 ms 重新构造整个 Spool 索引，不应阻塞 ASGI 事件循环。
- `pending=0` 不等于所有 Socket、Journal、Projection 已完成；保持 Worker inflight、SMPP Window、Journal offset、Writer Health 的正确收敛条件。
- 可建立周期性慢速对账兜底，但不要为了性能删掉完整状态诊断 API；高频热路径与人工详细状态 API 分离。
- 提供小/中规模只读 CPU/I/O/耗时 A/B，输出每 1,000 次查询的对象创建、JSON 解析量、文件读取量或可复核代理指标；不得把减少轮询频率误写为“修复 SIGSEGV”。

### F03 — P1 / STATIC_CONFIRMED：验收观察器在活动收尾存在额外 O(n²) 工作

**文件**：`backend/tools/vnext_runtime_canary_acceptance.py`：`gate_live()` → `campaign_summary()` + `loopback_summary()`；后者在集合循环内执行 `actual.count(value)`。

**确认问题**：在 N 个接收号码中对每个唯一号码再次线性扫描 `actual`，最坏 O(N²)；`campaign_summary()` 每次重新从所有 `worker-*.jsonl` 读取和解析事件。R2 首次 Campaign N=2500，在完成附近可能带来观察器突发负载。

**Codex 要求**：

- 使用 `collections.Counter` 或增量聚合替换 N² 查重，并继续执行 `missing/duplicate/unexpected` 对账，不能省略正确性检查。
- 对 Journal 的实时观察只用 bounded counters / incremental offset；完整内容对账尽量在停机后完成，且不要与正在发送的热路径抢资源。
- 保留与历史 Runner 同等的最终事实证据；明确修改的是测试工具，不是业务协议。

### F04 — P0 / STATIC_CONFIRMED：真实 Canary 主进程缺少完整 SIGSEGV 入口级取证

**文件**：

- `backend/tools/vnext_native_sigsegv_diagnostics.py`: 专项工具已显式调用 `faulthandler.enable(all_threads=True)`。
- `backend/tools/vnext_runtime_canary_acceptance.py`: 真实 R2 主入口并未建立等效的独立、常驻 SIGSEGV 捕获文件与进程角色对应关系。
- `backend/vnext/diagnostics.py`: 已写 PID、role、phase，但不是 native stack。
- `backend/database.py`: stall 的定时线程 Dump 与 SIGSEGV Crash Dump 是不同来源，不能互相替代。

**Codex 要求**：

- 在真实 Canary 主进程**最早可行的入口**启用 `PYTHONFAULTHANDLER=1` / `faulthandler.enable(file=...)`，文件句柄必须一直存活到进程结束；子进程分别保存 role/PID 的故障记录。避免启动后才配置，以免错过初始化阶段。
- 为 root、API、Worker、Projection Writer、Resource Tracker 分别记录进程树 / pid / ppid / 启动方式 / native module versions / 最后阶段，不记录 MSISDN、正文或密码。
- Native C/C++ Faulting Frame 若无法取得，保持 `UNAVAILABLE`；Python faulthandler 栈即使可用，也不能冒充 Native Frame。
- 如系统允许用户态进程的独立 core 或 `gdb`，先核查安装、权限、磁盘预算及现场授权；**不得**执行 `sysctl`、root、全局 core_pattern 修改或对共享服务 attach。

### F05 — P1 / HIGH_RISK_STATIC：业务/测试线程与两个 DuckDB 所有权边界必须明确

**文件**：`backend/main.py`、`backend/database.py`、`backend/tools/vnext_runtime_canary_acceptance.py`、`backend/vnext/projection_writer.py`、`backend/services/activity_scheduler.py`。

- Canary 根进程启动时 `seed_database()` 先创建和关闭根 DuckDB Owner，再在 `Uvicorn` 后台线程启动 `main.app`；同步 FastAPI 路由使用线程池。
- `main.startup()` 建立数据库 Owner 和后台 Scheduler；即使 VNext 发送已从旧 sender 分离，Scheduler 仍对主 DuckDB 执行周期性读写。
- `ProjectionWriterProcess` 在独立进程中持有**另外一个独立的 Projection DB**，不是主数据库；正常情况下不应混淆这两个数据库的 Owner。
- `database.py` 的 `_ROOT_CONNECTION` / `.cursor()` 所有权此前曾涉及 Native SIGSEGV 历史；不能仅因为已经做过修复就忽略调用者跨线程和关闭阶段的检查。

**Codex 要求**：

- 画出实际根进程各线程、DuckDB connection/cursor 开闭、GIL/线程池、子进程边界，以及 main DB / projection DB 是否真正为不同路径。
- 审计哪些 `duckdb.connect()`、cursor、relation 或 result 在创建线程以外被使用或延迟析构；审计 `close_root_connection()` / `set_database_file()` / `shutdown()` 是否可能与其他持有 cursor 的线程并发。
- 审计 Scheduler 在 VNext Canary 期间仍执行的 SQL 读写，以及是否会造成主进程的 Native 压力；此项为候选因素，**不是已证明根因**。
- 若设计可控实验，只允许在测试入口中隔离 scheduler/主库读写，不得通过生产默认关闭功能假装修复。

### F06 — P1 / HIGH_RISK_STATIC：34 秒附近的状态收敛与线程峰值

**文件**：`gate_r2()`、`gate_live()`、`wait_idle()`、`loopback_summary()`、`runtime.status()`、`projection_writer.py`。

- 80 SMS/s × 首 Campaign 2500 条 → **理论最短 31.25 s**（假设单条单 PDU、无额外发送前等待）。
- D7 root process 34.361 s SIGSEGV，与首个 Campaign 边界相邻，但没有最后一次 API trace、Native Frame 或 `canary.json`，因此**无法判定崩溃发生在完成等待、日志读取、数据库写入或其它环节**。
- 报告 Guard 最后记录 root 进程线程数较高；应读取真实 `/proc/<pid>/task` 样本和 Native/Python 线程名称，不要将线程数直接等同于泄漏/根因。

**Codex 要求**：

- 生成严格时序图：启动、Watchdog 30s、2,500 条的 Socket/write/responses、`wait_idle` 轮询、Journal offset 对齐、收尾对账。
- 若缺精确事件时戳就标记 `UNAVAILABLE`，不要按 34s 推测已经进入某个函数。
- 在真实 Runner 使用单调时钟的微型阶段标记，只记录阶段 ID / PID / 序列计数，不写正文/号码；避免每 Task 日志影响性能。

### F07 — P2 / STATIC_CONFIRMED：当前 R2 和 R7 并非等价压力

R2：依次处理四 Campaign（首个一次性摄取 2500，10 个 250 Row Batch），`gate_live()` 等该活动 drain 后继续下一个。R7：约 10 分钟、间隔约 5s 输入，批量一般为 20/100 行，最终 4000 条。因此不能从 R7 PASS 推断 R2 高瞬时负载 PASS，也不能把 R2 当作四活动同时发送的公平性证明。文档和下一轮 A/B 报告须明确注明此事实。

## 3. 任务分阶段（先静态、后低风险验证）

### S0 — 基线 / 证据冻结（必须首先完成）

- 核对 Git commit、dirty 列表；列出本地报告版本是否已包含 `2a8d1778` 及其后续变更。
- 如归档存在，SHA 核验：B2 `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`；D7 `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e1`；**不能擅改历史证据**。
- 摘取最后有来源的真实事件时间线，记录 `canary.json` 缺失、core/native frame 缺失、主进程 exit -11。
- 建立静态风险表（F01–F07），每条输出证据行号与真伪边界。
- G-S0 PASS 条件：已交付可复核静态报告；证据缺失已显式标记。无需运行任何负载测试。

### S1 — 完成调用图与资源所有权审计（静态为主）

至少覆盖以下三条路径：

```text
A. CanaryRunner.main
   -> setup_environment -> seed_database -> UvicornThread.start
   -> main.startup -> init_db -> VNextRuntime.start / start_scheduler
   -> gate_r2 -> gate_live -> /api/vnext/idle -> wait_for_idle/status
   -> campaign_summary -> loopback_summary

B. VNext worker
   -> ShardWorker._seed_buffers/_process
   -> TaskSpool.checkpoint
   -> AsyncSmppTransport.send_task / _read_responses
   -> ProgressJournal.append -> Projection Writer offsets/DB

C. Native and diagnostics
   -> main DuckDB owner/cursors + Scheduler threads
   -> separate Projection DuckDB owner
   -> stall watchdog / faulthandler + child spawn context
```

静态审计必须给出：

- 每一进程与线程的 Owner/Reader/Writer 表（含生命周期、fork/spawn、fd、Native 对象）。
- 每条跨进程交接的持久化事实源和 ack 点。
- 30–40 秒附近所有计划任务、`timeout`、watchdog 和轮询器的触发时间。
- 对 `canary.json` 不存在时如何保留事件证据的分析。
- 明确目前能解释为 Python 业务异常的事项不能自动当作 native `-11` 根因。
- G-S1 PASS 条件：调用图/时间线/资源表完整，有代码行号，不声称已锁定 Native Faulting Frame。

### S2 — 只修复已确认的诊断/观察器缺陷（保持业务链不动）

在隔离分支或干净 worktree（不能覆盖既有 dirty 文件）上实施最小改动：

1. F01：为 Watchdog 区分 `scheduled_stack_snapshot` 与真正的 `no_progress_stall`。低成本诊断允许显式开启，默认不把 30s 运行龄作为异常。添加进程级持久 SIGSEGV 捕获。
2. F02：将高频 idle 判定由 `TaskSpool` 全量重载改为 lightweight snapshot/事件信号；明确后台对账的低频兜底和 startup/restart 时的正确性。既有 `/api/vnext/status` 功能保持。
3. F03：验收 Loopback 去重改 `Counter`，尽量用增量进度统计；保持完整数据对账和输出 schema 语义。
4. F04：真实 Canary Runner 与子进程取证闭环，确保 `-11` 时可留下 PID/phase/trace/last marker；不以普通 Python 堆栈假装 C++ frame。
5. 任何修改都必须带专项单测：30s 正常运行不被误记 stall、循环 idle 查询下无完整 Spool 重建、Writer catch-up、Pause/Cancel/Worker crash/restart 不回归。
6. 不修改 `Route TPS`、`SMPP Window`、MSISDN Hash、Worker 数、Spool checkpoint 语义或 `socket_written` 定义。
- G-S2 PASS 条件：确认的问题通过最小变更解决，原有可靠性合约不降级；未凭小规模结果声称 SIGSEGV 修复成功。

### S3 — 有目的的低风险动态 A/B（只用于证伪静态假设）

首先在本地 AlmaLinux-8 WSL 原生 ext4 上运行，**不是** EulerOS 5500 条重跑。

建议按单变量顺序：

- A/B1：同一健康负载，Watchdog 30 秒栈采集开启/关闭；任务量保持不变；独立 Native Crash Log 必须始终开启。观察 30–45 秒的线程与诊断事件。
- A/B2：同一中等量任务，旧 full-`TaskSpool` idle status 与轻量状态路径对照（等价业务事实、相同 CPU affinity）。记录 JSON 解析、读取字节、HTTP idle 延迟、CPU、RSS、事件循环延迟。
- A/B3：相同发送配置下，独立控制“真实 Uvicorn 主进程 / 轻量无 API Worker”与“Projection Writer 有/无独立负载”；在可控单因素范围检验 API Native 热点。
- A/B4：同一小型业务量在 30 秒附近主动触发 `wait_idle` 与不触发的对照，捕获阶段标记；不得借此说 2500 或 5500 已通过。

**执行守则**：

- 先跑 10/100/1K 上限范围的诊断，只有证明小负载不能覆盖特定状态且需要更大规模时才提交带成本/资源的下一次申请；不要自动上升到 R2/P5500。
- 运行结束记录 PID、返回码、Native Crash Log 是否捕获到堆栈、系统和 Python 版本、资源峰值、事件数；不重复运行失败门禁寻找偶然 PASS。
- 有 `SIGSEGV` 时立即停止该矩阵；继续保留原始隔离证据，不删除日志、spool 或 crash marker。
- G-S3 PASS 条件：至少对 F01/F02 的真实开销与行为做了可复核对照；若仍无 crash frame，则仍为 `RCA_STRONGLY_INDICATED`/`RCA_UNRESOLVED`，不得升级 `RCA_PROVED`。

### S4 — 仅有证据时实施真正的 Native 根因最小修复

- 若 Native Faulting Frame 指向 DuckDB / CPython / 依赖扩展，则提供最小触发链、Owner 关系、重现条件和版本约束后再修复。
- 若实证只指向观察器/Watchdog，则修改观察器，不能顺带重写 Projection/Spool/SMPP。
- 若没有 Native Frame，仅定位到高风险时间窗口或某函数名，标记 `STRONGLY_INDICATED`，输出尚需哪一个观测点；禁止以 `A/B 一次 PASS` 认定真正根因。
- 不同时升级 DuckDB、CPython、OpenSSL、glibc、SMPP 和多进程策略；任何兼容性替换都必须单变量 A/B 且离线包可回滚。
- G-S4 PASS 条件：范围与证据相称；业务事件不丢失、不重复 Socket Write；对照与回滚路径明确。

### S5 — 回归、交付和服务器门禁

**本地**：

- `python -m pytest backend/tests -q`（使用项目已存在 venv/脚本，不安装全局依赖）；前端如有受影响代码再 `npm run build`。
- 专项测试覆盖 F01–F04、30s 健康诊断、零事件/极少事件、Writer 暂停与恢复、Worker crash、Online Ingress、Pause/Cancel、Journal offset、无双 Owner。
- 静态检查：`git diff --check`、新增文件依赖扫描、部署包内容清单验证、无日志/数据库/WAL/运行数据进正式包。

**EulerOS**：

- **本任务默认禁止自动运行 D7/R2/P5500，也禁止把历史 D7 的失败结果改写为 PASS**。只有完成静态归因、验证 Native Crash 采集，并获得用户对目标服务器负载的**新的明确授权**，才可申请单次目标复验。
- 与现有双 VPN 流程保持一致：先 DEV/GitHub 收集/冻结源码，用户手动切换 SERVER VPN，在 `/home/paas/marketing-copilot-test/runs/<new-run>/` 下隔离执行，结束后再手动切回并校验证据。
- 无 `sudo/root/systemd/sysctl/全局 core_pattern/系统包升级`；合成号码 + 127.0.0.1；最多 4 CPU affinity、`nice +10`、`ionice -c3`；RSS 2 GiB / 新 Run 磁盘增量 5 GiB guard。现场更低的空闲空间约束应优先适用。
- 任何越界、无法取证的 `-11` 或资源异常立即停止，收集证据并清理**仅当前 Run** 的孤儿进程。
- G-S5：只对本轮**真实执行**过的步骤写 PASS。没有新的 EulerOS 实测不算闭环；不能用 WSL PASS 代替。

## 4. 交付文件与最终状态合同

最少交付：

1. `Docs/Marketing_Copilot_VNext_Static_Audit_and_Native_Crash_RCA_Report.md`：逐文件/行静态审计、F01–F07 分类、资源 Owner 表、最小调用图、30–40s 时间线、原始证据来源、修复前后差异、未解决证据缺口。
2. `Docs/Marketing_Copilot_VNext_Crash_Diagnostics_and_AB_Evidence.md`：低风险对照参数、诊断开关、结果、采样指标、日志/归档 SHA；没有执行则注明 `NOT_RUN`。
3. 测试代码：Watchdog 生命周期、Idle Hot Path、Journal/Projection、并发停止/恢复与 Runner 诊断相关专项。
4. 如有修复，列出修改文件和风险、回退条件、测试结果、`git diff --check`、完整 SHA/运行环境。不要只交付一份设计方案。

结果输出必须单独列出：

```text
STATIC_AUDIT_STATUS = PASS | PARTIAL | FAIL
NATIVE_FAULTING_FRAME = CAPTURED | UNAVAILABLE
RCA_LEVEL = RCA_PROVED | RCA_STRONGLY_INDICATED | RCA_UNRESOLVED
WATCHDOG_FALSE_STALL = FIXED | NOT_FIXED | NOT_VERIFIED
IDLE_HOTPATH_AMP = FIXED | NOT_FIXED | NOT_VERIFIED
RUNNER_OBSERVER_OVERHEAD = FIXED | NOT_FIXED | NOT_VERIFIED
LOCAL_TARGETED_AB = PASS | FAIL | NOT_RUN
EULEROS_R2_P5500 = NOT_RUN_UNAUTHORIZED | FAIL | PASS
VNEXT_RUNTIME_CANARY_CLOSURE = FAIL  # 历史值保留；无新的获授权完整验收不可提升
INTEGRATED_STATUS = INTEGRATED_NO_GO  # 历史值保留
```

规则：

- 没有 Native Frame 就不能标 `RCA_PROVED`；时间关联、性能下降、30s 定时栈或 A/B 单次 PASS 均不够。
- 观察器优化的本地 PASS ≠ EulerOS R2/P5500 PASS；不能用 4000 条 R7 替代 5500 条 R2。
- 本轮原则上**不改正式交付包的原生 Runtime 版本、不引入旧兼容代码**。
- `AGENTS.md`、`frontend/package-lock.json` 及其他既有用户 dirty/untracked 文件原样保留；不运行清理命令。

## 5. 可直接交给 Codex 的执行指令

> 仔细执行本任务 MD。先以 GitHub `main` 的 `2a8d1778b4f86bdaf75696aa1540bad45e5881e2` 为基线复核本地源码和三份闭环报告。重点做真实 `R2 -> gate_live -> /api/vnext/idle -> runtime.status -> TaskSpool` 调用链静态分析，确认 `database.py` 30 秒 Watchdog 的必达条件、`faulthandler` 生命周期、主进程/子进程 DuckDB Native Owner、测试 Runner 的 N² 对账及观测器额外负载。先交付有行号的风险矩阵和根因证据边界；只修复可确认的观察/诊断缺陷并补自动化测试。必要的动态测试仅在本地隔离环境以低风险单变量对照进行，不得盲目在 EulerOS 再跑 D7/R2/P5500。没有 Native Faulting Frame 继续使用 `RCA_STRONGLY_INDICATED`，不得宣布 Canary PASS。保留 VNext MSISDN 分片、异步 SMPP、持久 Spool 和独立 Projection Writer 主架构。提交完整源码改动、测试结果、分析报告和证据索引，但**未经用户后续明确授权不得执行 git commit/push**。
