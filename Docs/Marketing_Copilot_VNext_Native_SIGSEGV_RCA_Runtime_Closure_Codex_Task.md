# Marketing Copilot VNext — Native SIGSEGV RCA & Runtime Canary Closure

**Codex 工程任务｜证据优先、最小修复、运行时真实链路验收｜2026-10-16**

## 0. 任务目标与事实基线

直接分析仓库现有代码、开发机未提交报告与服务器证据，定位 VNext **R2/P5500 两次 EulerOS `SIGSEGV (-11)`** 的崩溃进程、触发路径和原生调用原因；在不恢复旧发送架构、不扩大兼容负担的前提下修复并验证 VNext 长驻运行主流程。**任务要求实际改源码、添加有针对性的回归与诊断、生成最终报告，而不是只提供建议。**

### 0.1 GitHub 已核实的远端事实（以执行时重新核对为准）

- Repository：`shutu-ai/Marketing-Copilot`；截至本任务生成时，`origin/main` = `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`。
- 该提交为 `feat: replace SMS send path with VNext sharded pipeline`，包含 41 个文件（VNext 源码、测试、工具、部署配置、实现和性能报告）。
- 远端已提交的 VNext 离线四活动 Loopback P5500：`generated=socket_written=submit_sm_received=projection.applied=5500`，EulerOS 17.416 s，原配置 `route_tps=0`，只证明本轮不限速 Loopback 表现。
- 远端 `main` **尚无** `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`，也尚无本轮 R2/R7 证据归档：**不得声称已经在线读取、核实了这些未提交资料。**

### 0.2 用户提供的最新未提交结果（执行时在本地复核）

- `VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV`。
- R7 隔离重跑 PASS：`4000/4000`，运行 guard 记录 RSS peak `900640768` bytes（约 859 MiB）。
- R2/P5500：两次均在 EulerOS 真实长驻链路出现 `SIGSEGV(-11)`；禁止无诊断价值的第三次盲目重跑。
- 报告：`Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`。
- 服务器归档：`test-artifacts/vnext-runtime-canary-closure-20261016/stage-b/server/stage-b-r2-r7-transfer.tar.gz`。
- 用户记录 15 份证据核验；归档 SHA256：`ab2a131d642aa290cda80dabda94d5f3a7c4d469140a23aa7e1ace25cc6581bf`。
- 本地存在原有 dirty worktree；不得 `git reset --hard`、`git clean -fd`、覆盖用户文件或自动提交/推送。

**保留历史判定：** `INTEGRATED_NO_GO` 不变；VNext 旧版预填任务 Loopback `VNEXT_PIPELINE_READY_FOR_CANARY` 不代表本轮 Runtime Canary PASS；本轮 `BLOCKED_ENV` 不得自动写成 OS 根因或代码 PASS。

## 1. 技术路线：只保留 VNext 简化主链

```
Input (Realtime / Batch / SFTP)
  -> Normalize MSISDN -> Virtual Partition Hash
  -> Durable Task Spool -> Bounded Shard Worker(s)
  -> Async SMPP Socket -> Compact Progress Journal
  -> Dedicated ProjectionWriter -> DuckDB Projection
```

**不可违反的边界：**

1. 不重新引入逐短信 DuckDB Claim、同步 Send Intent、旧 Reliable/Fast 双发送链或新中间件（Kafka/Redis/RabbitMQ）。
2. 不将 `socket_written` 伪称 `submit_sm_resp accepted` 或最终 `delivered`；Response 继续异步消费以释放 PDU Window、维持连接健康。
3. 不为提升吞吐关闭 Spool 已确认任务的持久化、破坏 MSISDN 固定分片、允许双 Worker 同时拥有相同虚拟分片。
4. 崩溃前已形成 `dispatch_taken` 而 Socket 结果未知的短信，默认不自动重发；记录可审计的 unknown/at-most-once 语义。
5. 只优化 VNext 主路径；不以“多加一层兼容、再保留一套旧 Writer/Scanner”为解决方案。
6. 不在共享服务器上使用真实 SMSC、真实号码、生产数据库、root/systemd/global pip、修改代理或 VPN 策略。
7. 不隐藏 `SIGSEGV`、不将系统自动重启视为 crash 已修复、不降低验收完整性标准。

## 2. Phase A0 — 基线、仓库与本地证据清点（先完成再改代码）

- 阅读仓库 `AGENTS.md`；若 `.codegraph/` 存在，先用 CodeGraph 定位调用链。
- 记录 `git status --short`、`git diff --stat`、`git rev-parse HEAD`、`git rev-parse origin/main`；保留原有 dirty 文件，新增本任务代码和报告单独标识。
- 核实 GitHub 新提交是否已改变基线；如有变更，列出新旧差异、调整任务，不盲目 cherry-pick/rebase。
- 本地读取未提交的 Closure Report 和归档；**实际存在时**核对归档 `sha256sum` 是否等于上面的 SHA256，核对其 15 个成员及内部 manifest；列出缺少、校验失败或格式不可读项目。
- 对归档只读解包到独立诊断目录。验证 tar 成员没有绝对路径、`../` 逃逸或危险链接，避免覆盖现有工作树。
- 形成**证据索引**：R2-1/R2-2/R7 的执行命令、时序、PID/PPID、退出码、日志、trace、JSON、snapshot、资源 guard、环境版本、core 是否存在。
- 记录 EulerOS 2.0 SP15、glibc、CPython 3.11.5、DuckDB 1.2.0、kernel、文件系统、Python 包和原生库依赖；区分测试代理 EulerOS 与原生 SLES12SP5。

**交付 A0：** `Docs/Marketing_Copilot_VNext_SIGSEGV_Evidence_Index.md`，每一项标 `VERIFIED / INCOMPLETE / UNAVAILABLE`，不补造缺失的 core 或 C++ 调用栈。

## 3. Phase A1 — 崩溃进程定位与首因归属

### 3.1 必须先回答的 8 个问题

1. R2 是 API 主进程、`ShardWorker`、`ProjectionWriterProcess` 还是独立测试 Runner 退出 `-11`？父进程观察到的 `-11` 是否准确对应某个子进程？
2. 首次异常发生在 `fork/spawn`、DuckDB `connect/execute/close`、Spool 初始化/扫描、SMPP 读写还是 Shutdown/Recovery？
3. R2-1/R2-2 的最后共同阶段是什么？R7 正常时的对应时序是什么？
4. 崩溃前是否发生 native `malloc/free`、DuckDB WAL/connection、Python multiprocessing fork/Queue/feeder-thread、套接字生命周期异常？
5. 是否存在工作进程启动前 API 已初始化 DuckDB、线程、`asyncio`、日志或进程间 Queue 的情况？
6. 旧 P5500 Runner 显式 `spawn`，真实长驻服务 Supervisor/Projection Writer 使用默认 multiprocessing context；两者能否解释测试差异？**这是高优先级假说，不是已证根因。**
7. 崩溃前 `ProgressJournal`、Projection IPC、TaskSpool 是否已经积压、容量耗尽或发生异常？纯 Python 异常不能直接解释 SIGSEGV，需建立时间关系。
8. Crash 同时或之后是否出现其它业务问题（Spool 空读、任务未发现、投影积压），并将其与原生崩溃明确分离。

### 3.2 静态检查范围（实际代码路径）

- `backend/main.py`：启动顺序、`init_db()`、线程与进程的创建时机。
- `backend/vnext/runtime.py`：`_runtime_worker_entry()`、`VNextRuntime.start/stop()`、`ProgressJournal` → IPC Queue。
- `backend/vnext/supervisor.py`：`_context()` 默认进程启动方式、`_launch/restart_worker()`、lease/epoch 生命周期。
- `backend/vnext/projection_writer.py`：`multiprocessing.get_context()`、进程内 `duckdb.connect()`、Queue/Stop、事务异常和 flush。
- `backend/vnext/spool.py`：父/子进程缓存的 manifest、分段读取、cursor/checkpoint、运行中增量任务可见性。
- `backend/vnext/worker.py`、`smpp_transport.py`、`route_credits.py`：Socket Loop、资源释放及可能跨线程或进程共享的对象。
- `backend/database.py`：旧主库原生 DuckDB owner/connection 的启动时状态；特别与曾经修复过的 Native SIGSEGV 问题做**证据对比**，不能直接套旧结论。

### 3.3 有边界的本地复现/诊断

- 仅在开发机独立 WSL ext4 环境复现**最小必要规模**：先真实 `main` 启动/空闲/关闭，再 R2 同类在线入队与小规模负载；不是一开始反复 P5500。
- 为主进程和每个子进程写入非敏感的 `pid,ppid,role,start_method,python,duckdb_version,run_id,phase,exitcode,last_progress`。
- 使用 Python `-X faulthandler` / `PYTHONFAULTHANDLER=1`，独立 Run 中可选 `PYTHONMALLOC=debug`；捕获线程栈、父子进程 exitcode 和最后事件；不能把 Python traceback 当作 C++ native 栈。
- 如果隔离环境已有 `gdb`/`coredumpctl` 可使用，且不需要 root/全局修改，则按只读或隔离策略采集 faulting native frame；如不可用明确 `NATIVE_FRAME_UNAVAILABLE`，不能伪造。
- 对比 Arm-F 默认 context 与 Arm-S 全部 `spawn`：同源码、同小规模场景、同 Run guard，记录崩溃/时延/资源。避免比较不同任务量得出性能结论。
- 若有最小触发条件，采用单因素隔离（Writer 是否启动、DuckDB 是否先初始化、Worker 数等），每项有上限，记录停机理由，禁止无意义循环复现。

**交付 A1：** `SIGSEGV` 归因表。区分 `PROVED`、`STRONGLY_INDICATED`、`HYPOTHESIS`、`NOT_REPRODUCED`；没有 native stack 时只作相应等级结论。

## 4. Phase A2 — 最小原生生命周期修复

以下为**按证据决定**的首选方案，不预设一定是 fork：

- 若进程启动方式被证实或强烈指向问题，则将 VNext 统一到**显式 `spawn`**（或有等效证据且更优的安全模式），修改 `WorkerSupervisor` 和 `ProjectionWriterProcess`，并统一使用**同一个** `multiprocessing.get_context('spawn')` 创建 `Process/Queue/Pipe/Event/Lock`；禁止混用不同 context 的对象。
- 在子进程入口**再**导入/初始化 DuckDB、I/O 线程、SMPP、事件循环与本地文件句柄；不要继承父进程中已初始化的 native connection。
- 检查 `spawn` 下的可 pickle 入口、Windows/Linux 启动保护、API 生命周期、进程命名、关闭、重启、Queue feeder drain；不因 `spawn` 需要额外构造而产生无限重启或死锁。
- 处理子进程异常退出：状态必须可查询、记录 exitcode 和角色、重启退避有上限、正确区分主动停止与 crash；不要让 Supervisor 无限快速 fork/spawn 打满 CPU。
- 保留 Worker 分片独占/epoch；任何进程重启都不得创建两个并发持有同一分片的有效 Worker。
- 若已证实 Native 原因不在 `fork`，只修实际最小根因，报告说明为何没有改成 `spawn`。

**验收 A2：** startup/shutdown 循环、小规模长驻在线输入、Worker restart、Writer restart、没有 native crash/孤儿进程/双 owner；报错时有准确 exit reason。

## 5. Phase A3 — 必须顺手闭合的在线运行正确性（不是第二次重构）

这些问题来自已提交源码直接可见的运行路径，可能不是 `SIGSEGV` 原因，但会阻止 Runtime Canary：

### A3.1 已启动 Worker 能及时发现新任务

- 父进程 Ingress 向 Spool 新增 segment/manifest 后，已存活的 Worker 不得仅凭初始化时缓存的 `_manifest` 判断队列为空。
- 采用**单一 manifest owner + 轻量增量 reload/version/wakeup**；可选事件通知加安全周期兜底，不给每个 Worker 引入全库扫描或冗余数据库。
- 避免并发 Worker 修改全局 manifest；验证按分片有序、跨 Campaign 公平、CRC/幂等和已 taken 记录不重新发送。
- 专门测试：先启动真实服务 → 系统空闲 → 新加 10/100 个任务 → 不重启 Worker 即发现；重复文件不重复入队；持续追加多轮仍无错漏。

### A3.2 进程间 Progress Journal 自动回放

- 当前正式 Worker 每事件 `journal.append()` 后 `projection_queue.put_nowait()`；Queue full 时可出现 Journal 已持久而投影缺失。应将 Journal 作为**恢复权威**，IPC 仅作快通知。
- Writer 启动、重启、IPC 断开或恢复时，按持久 `event_id`/消费位置增量回放所有 Worker Journal；不使用 `read()` 全量读入无限增长的文件作为长期路径。
- 保持短批量事务和 DB `event_id` 唯一键幂等；异常/重启前后 `applied_unique == durable_progress_unique`，可监测 lag；不需要逐短信同步 DuckDB 写入。
- 处理 Queue full 时不能让 Progress Sink 抛异常而阻断其它有容量的 Worker；可保留本地持久事件，触发有界背压与异步补偿。

### A3.3 ProjectionWriter 小批量不能无限期挂起

- 当前 `_process_main()` 只有达到 `batch_size` 或 Stop Sentinel 才 flush。为小于 batch_size 的长驻事件流增加**可配置 bounded flush interval**（建议先取 50–200ms 的诊断区间，由测试确定默认）；`queue.Empty` 时执行到期 flush。
- 低事件率也必须在可界定时间内反映到报表；高事件率仍允许合并提交，不能恢复逐事件事务。
- Worker/Writer 崩溃后保证短事务完整回滚与下次重放；记录每批 size、DB wait/commit、lag、flush failures。

### A3.4 Pause/Cancel 状态重启不丢失

- `VNextRuntime.start()` 当前覆盖写入空 `control-state.json`；修复为**先加载/验证原控制状态**，不清空已有有效的 paused/cancelled。
- 活动长批次执行时控制需要及时可见，不能只在下一个 `run_until_idle()` 入口读取一次。
- 明确“已进入 Socket write 的消息无法撤回”；对尚未发送的新工作实施控制边界即可，不重新引入旧 DB per-SMS 授权。

**验收 A3：** 在线输入、DB Writer 停顿后自动追平、低负载实时投影、暂停/取消/恢复/进程重启全部 PASS；所有验证使用本地合成号码。

## 6. Phase A4 — 故障隔离、观测和容量门禁

必须记录：

- 各角色 PID/exitcode/restart count、进程启动方法、CPU/RSS **运行期间时序/峰值**（非仅结束后内存）、磁盘、fd、线程数。
- ingress accepted、spool pending、worker taken、socket_written、unknown、ProgressJournal durable、projection applied、projection lag、每 Campaign/Route 处理进度。
- Queue high/low watermark、Pause/Resume 次数和恢复延时；无资源 Worker 不持有其它 Campaign 共享执行锁。
- `socket_written`、模拟器 `submit_sm_received`、响应成功数分列，防止把业务完成口径混成运营商接收/投递口径。
- 每项 fault test 都保存**命令、环境快照、运行目录、指标、失败和 exitcode**，禁止缺失证据时标记 PASS。

### 分级 Fault/Gates（顺序不可颠倒）

| Gate | 场景 | 放行要求 |
| --- | --- | --- |
| D0 | Python 多进程与 native 启动/停止，小规模 10/100 | 正常退出，无 -11/孤儿进程 |
| D1 | 真正 `main + API + VNextRuntime`，空闲后在线入队多轮 | 自动发现并完整处理，无手工重启 |
| D2 | Writer pause/恢复、IPC Queue 满、Writer kill/restart | Journal 幂等补偿，最终 projection 追平 |
| D3 | Worker kill/restart，Spool cursor/owner epoch | 不双 owner，不重发 taken，真实进度一致 |
| D4 | 在线 Pause/Cancel/Resume，期间持续输入 | 控制及时生效，重启后状态保留 |
| D5 | 受控 Route TPS、慢 `submit_sm_resp` / Window 压力 | 有界积压、其它可运行任务继续，解除后恢复 |
| D6 | R7 等价诊断（4000 tasks） | 4000/4000，RSS 时序，进程与资源 guard PASS |
| D7 | 原 R2/P5500 真实主流程 | 只有通过 A0–D6 与共享服务器授权后才准**单次有诊断价值的重跑** |

D7 保留原合同：4 Campaign × (2500,2500,250,250)、300 秒业务窗口、真实 TCP Loopback、完整 `generated/socket_written/submit_sm_received/projection` 对账。若原 R2 合同的其它参数不同，以归档冻结参数为准，并清楚记录差异。不要把预填任务离线 Runner 的 PASS 替代真实长驻流程 D7。

**通过判定：** 任何 native `SIGSEGV`、父子进程归属不明的异常死亡、数据不一致、分片重入、无法恢复的 IPC/Journal 堵塞、R2 重现，均为 FAIL/BLOCKED；不得用自动重启掩盖。

## 7. Phase B — 双 VPN / EulerOS 服务器小心验证

与现有双 VPN 规则一致：

### B1：开发/GitHub VPN

- 本地 WSL 原生 ext4 完成 A0–A4/D0–D5；记录基线、补丁、日志、Python/DuckDB/依赖列表。
- 生成离线 app 包、必要的已有 wheelhouse 增量、运行时、sha256 manifest 和本次诊断脚本；严格 `--no-index` 安装合同。
- 含 `PYTHONFAULTHANDLER`、角色 PID/退出状态、最小崩溃采样配置，不强制全局 core dump。
- 写入 `PHASE_B_READY_FOR_MANUAL_VPN_SWITCH`；不自动修改 VPN/代理或网络路由。

### B2：手动切换服务器 VPN 后

- 目标：EulerOS 2.0 SP15，作为 SLES12SP5 **性能代理**，不伪称原生 SLES。
- 用户 `paas` 非 root；仅在 `/home/paas/marketing-copilot-test/runs/<unique-run>/` 下执行。
- 默认最多 4 CPU affinity、4 Worker、`nice +10`、`ionice idle`，保持现有共享服务器允许的内存/磁盘/RSS guard；发现资源紧张立即停止。
- 严格禁止真实 SMSC、真实号码、生产数据库、全量 G4/G6、52K/102K、无限 soak、root、systemd、全局 pip、宿主机全局 sysctl/core_pattern 修改。
- **先 D0–D5 小规模测试，再独立授权 D6/R7。** R7 PASS 后核查 crash 归因和补丁证据；D7/R2 需要明确许可及当次预检查 PASS。
- D7 最多一次获批准的有诊断价值执行。若 `-11` 再现，立即停止并保存证据，不再继续试第三、第四次。
- 运行期间采样 RSS/CPU/磁盘/进程数、记录 crash PID/core 状态；保留完整 Run 证据和 sha256，对外仅传必要的脱敏结果。
- 生成 `PHASE_C_READY_FOR_MANUAL_VPN_SWITCH`；由操作者手动切回开发/GitHub VPN。

### B3：开发/GitHub VPN 收口

- 校验服务器归档及每项证据 hash，不依据缺失证据推断通过。
- 输出**修改后的代码、回归结果、RCA 结论等级和仍未解决问题**，更新闭环报告。
- 遵守仓库 AGENTS：**不自动 commit/push**；用户明确要求后才可针对本任务文件选择性提交，绝不携带 dirty、DB、WAL、Spool、手机号或密钥。

## 8. 工程交付清单

1. **实现代码与测试**（仅必要路径）：重点 `backend/vnext/{runtime,supervisor,spool,worker,projection_writer,smpp_transport}.py`，涉及入口时对应改动 `backend/main.py`、`backend/services/vnext_service.py`。
2. **Crash 定位工具**：可复用的主/子进程角色-退出码-线程/native 诊断，独立目录、默认不输出真实号码及密码。
3. **专项自动化测试**：显式 spawn/context、在线 Spool 可见性、Writer 异步 flush/replay、IPC Queue full、控制状态恢复、Crash+restart，无上限循环禁止。
4. **真实长驻 E2E Runner**：从 `main + API + VNextRuntime` 启动，在线追加任务、故障注入、按 Campaign/Route 多维对账；不可改成预填 Spool Runner 冒充。
5. `Docs/Marketing_Copilot_VNext_SIGSEGV_Evidence_Index.md`。
6. `Docs/Marketing_Copilot_VNext_Native_SIGSEGV_RCA_and_Runtime_Stabilization_Report.md`。
7. `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md` 更新（保留旧结论、失败记录、原始证据链接，不覆盖）。
8. 阶段 B/C 的证据 manifest、`sha256sum`、回归命令、exitcode、资源峰值、未通过项清单。

本地基本回归：

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
cd frontend
npm run build
```

如环境不同，Codex 可选择仓库现存有效 Python venv，并在报告记录实际命令；不擅自删除或覆盖用户虚拟环境。离线打包遵守 `AGENTS.md` 并检查 Release Manifest 与无 runtime data。

## 9. 最终状态规则

只允许以下有依据的判定：

- `RCA_PROVED`：有直接 native stack、可重复隔离触发/修复对照等充分证据。
- `RCA_STRONGLY_INDICATED`：多来源证据指向一致机制，但缺 native faulting frame。
- `RCA_UNRESOLVED`：证据不足或复现失败，**不能写已解决**。
- `VNEXT_RUNTIME_CANARY_CLOSURE=PASS`：全部约定门禁（尤其批准的真实长驻 R2/P5500）通过，并有完整观测/哈希证据。
- `VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV`：因为环境、权限、诊断工具不可得而无法完成目标验收。
- `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`：复现 SIGSEGV 或功能/资源/恢复门禁实质失败。

不论 VNext 阶段结果如何，历史 `INTEGRATED_NO_GO`、旧 G4/G6/原生 SLES/真实 SMSC/生产部署结论不得被自动篡改。

## 10. Codex 执行指令（可直接复制）

> 以 `origin/main` 最新已提交代码为基线，严格执行本 MD 的 A0–A4、B1–B3。先读本地未提交的 `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md` 及 R2/R7 服务器归档并验证 SHA256，定位 `SIGSEGV -11` 是哪个进程在哪个阶段发生，不允许把 `fork` 猜想写成已证根因。完成有证据的最小原生进程生命周期修复，同时闭合真实运行时在线 Spool 发现、Projection Writer 定时批量 flush + Journal 增量恢复、控制状态持久性；不得恢复旧 Reliable/Fast 双路径。先完成本地和小规模隔离测试，只有满足门禁和经授权后才在共享 EulerOS 运行一次 R2/P5500。保留所有 dirty 文件及失败证据，完成报告、实际代码和回归；未获明确请求不提交也不推送。
