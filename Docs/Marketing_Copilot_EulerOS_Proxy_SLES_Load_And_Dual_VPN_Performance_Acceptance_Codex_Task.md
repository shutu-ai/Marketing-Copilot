# Marketing Copilot — EulerOS 作为 SLES12SP5 压测等效目标 / 双 VPN 分段性能归因 Codex 任务

- **Task ID**：`MC-EULEROS-SLES-PROXY-PERF-DUAL-VPN-20261016`
- **任务性质**：分阶段隔离压测、性能对照与故障归因；非生产部署，不是默认架构改造任务。
- **起始代码 SHA**：`d6b482f2a330570cf25d5d54802b8ba1bcee2b08`（执行前确认最新 `main`，若发生变化先记录并冻结明确的实测 SHA，不能混用）。
- **参考报告**：`Docs/Marketing_Copilot_Dual_VPN_Offline_DuckDB_WAL_Loopback_Closure_Report.md`、`Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Report.md`、`Docs/validation_contract.json`。
- **服务器唯一授权工作区**：`/home/paas/marketing-copilot-test`。仅在此路径之下的**新建** Run 目录读写测试资产；既有 Run 不覆盖、不删除。
- **目标服务器真实系统**：EulerOS 2.0 SP15，最近一次报告为 kernel 6.6、glibc 2.38、64 vCPU、ext4；以每轮 T0 实测为准。
- **用户认可的验收定位**：**将当前 EulerOS 作为本项目本轮 SLES12SP5 的压测等效目标（`SLES_PROXY_PERFORMANCE_TARGET`）**，准许用其开展分级容量测试、确认应用性能瓶颈及作为本轮性能验收的目标平台；**所有原始系统字段必须如实记录 EulerOS，严禁在证据中改写为实际运行 SLES12SP5**。原生 SLES/glibc 2.22 二进制兼容性若另有要求，仍属独立验证维度，但**不得仅以当前是 EulerOS 为由拒绝本轮性能压测**。
- **当前集成状态**：`INTEGRATED_NO_GO`，本任务的局部性能 PASS 不自动改变集成发布 Gate。

## 一、不可更改的安全合同（优先级最高）

本服务器承担其他业务。**用户授权的是在专用目录内实施受控压测，并非授权修改服务器系统或抢占业务资源。**

1. 不允许修改、覆盖、删除 `/home/paas/marketing-copilot-test` 以外的**任何现有文件**。不得改写 `/home/paas` 下其他业务文件。原有测试 Run、证据和包同样不得覆盖。
2. 禁止 `sudo`/root 执行应用、测试、安装；禁止 `systemctl start/stop/restart/enable`、`daemon-reload`、安装 polkit/systemd、修改 `/etc`、`/usr`、`/opt/anaconda3`、系统 Python、代理、VPN、DNS、防火墙、路由、SSH 配置。
3. 禁止修改已有业务进程、数据库、连接、端口；禁止 `killall`、`pkill`、通配 PID 清理。只允许停掉由**本轮 Run** 启动且 PID/启动信息已核对的子进程。
4. 运行对象仅为**本地进程 API + 127.0.0.1 SMPP simulator**，不连接真实 SMSC、真实短信号码、真实用户数据库、远程 API、告警 Webhook、生产数据目录或外部测试服务。
5. 所有 runtime、venv、src、DuckDB、WAL、Journal、CSV、缓存、HOME、TMPDIR、日志、离线扩展元数据、PID、统计与最终 evidence **必须处于本轮专用目录内**；工具默认输出写入 repo `Docs/` 或 `/tmp` 的情况必须覆盖、审计，不能直接运行默认命令。
6. 不允许运行时联网安装：`pip --no-index`、本地匹配 wheelhouse、本地扩展路径，`MC_DUCKDB_OFFLINE_ONLY=1`，禁用自动扩展安装。若发现未预期的对外下载/连接尝试，立即 fail-closed，保存证据。没有网络层抓包证据时不得宣称绝对无出站流量。
7. 不允许 `git reset --hard`、`git clean`、删除其他 dirty 文件/缓存/证据，不修改 `AGENTS.md`、`frontend/package-lock.json`，不随意更新依赖。
8. 初期使用保守资源预算（建议**最多 4 个逻辑 CPU 的亲和范围、测试进程组聚合 RSS 警戒 2 GiB、当前 Run 额外磁盘使用警戒 5 GiB、低优先级 nice/ionice**）。此预算是共享主机保护门槛，不是压测目标；**CPU affinity、nice、ionice 并不是硬配额**，不能据此声称资源严格隔离。如无现成授权的 cgroup/系统限额机制，使用观察器按阈值主动停止；不得为建立硬配额而修改系统配置。
9. 只有完成每一级资源/正确性 Gate 才能进入下一档；CPU/RSS/I/O/宿主业务压力异常时立即停止进一步升级。**禁止在本任务中自动运行 52K/102K、完整 G4/G6 或持续长稳测试**；那些需要额外单独批准的共享主机资源窗口与可达性合同。
10. **所有变更仅在开发机编写/构建**；服务器仅部署冻结测试包与执行受控测试，除非有仅用于当前 Run 的临时脚本且完全限定在授权目录。禁止在服务器上直接修复系统级问题。

不满足任一强制隔离前置条件 -> `BLOCKED_SAFETY`；优先保留证据，不要想办法绕过。

## 二、已经完成的事实与本轮要回答的问题

已有证据（不可覆盖历史结论）：

- 在 EulerOS 上，项目正式独立 Python 3.11.5、venv、DuckDB 1.2.0、本地扩展、离线 WAL 2,000 行回放 PASS。
- 服务器小规模 10-task Loopback 重启流程 `10/10` accepted、10 submit/response、无丢失/重复/未知；`SHARED_HOST_SMALL_E2E_PASS`。
- 开发机 WSL AlmaLinux/ext4 G0 `355 passed, 2 skipped`；EulerOS 旧阶段独立 pytest 小套件 PASS；不将不同范围的 pytest 数量相互冒充。
- 当前高并发 WSL 5500-task 对照：OCC 在 300s 内约 3,134 submit，serialized 约 1,829 submit，两者严格未完成；OCC 曾记录 68 次 conflict retry，serialized 有 write-lock watchdog。均为待归因的失败证据，不代表 EulerOS 会得到相同结论。
- 双 Route 的 bind 修复已进仓，但此前 Windows smoke 有 `mapped_submit_sm=0` 的完整业务审计问题；本任务不以双 Route 的 bind PASS 冒充两路发送 E2E PASS。
- `Docs/validation_contract.json` 存在 52K/102K 活动映射以及 180 秒单 Route 200 TPS 的数学不可达问题。**不得借更换压测主机偷偷修改合同**。

本轮核心研究问题：

1. **相同源码和测试负载下，EulerOS 压测目标与 WSL 开发环境差异有多大？**
2. 当前瓶颈发生在输入文件解析与白名单、DuckDB Claim/发送前授权、SMS Result/Journal、Checkpoint、SMPP Socket、还是测试 Observer/Runner？
3. OCC 与 serialized 哪个仅表现出吞吐差异、哪个存在 correctness/WAL 失败？必须将资源变化、观察器影响与事务冲突单独列出。
4. 1,000 和 5,500 任务的公平性/完成度能否被真实、可对账地证明？如果不能，应该留下精确故障阶段、积压数据和原因，而不是笼统说明 `timeout`。

## 三、阶段 A — DEV/GitHub VPN（不连接测试服务器）

### A0. 冻结与审计

- 确认 SHA = `d6b482f2a330570cf25d5d54802b8ba1bcee2b08`，验证新增 `backend/duckdb_config.py` 和 Loopback 的离线扩展准备、每轮 Run 的 `--run-root`/`--output` 接口。
- 审阅 `AGENTS.md`、部署/打包脚本、测试入口是否隐含写 `/tmp`、`$HOME`、`Docs/`、服务安装、真实外连。所有可能写入路径列入 `WRITE_PATH_MANIFEST`；不确定的路径不得在共享服务器执行。
- 审阅测试源中 `local_e2e_acceptance.py`、`multi_campaign_fair_acceptance.py`、`mixed_campaign_capacity_acceptance.py` 的 CLI 参数，确保没有猜测不存在的 flag。确认关闭应用后的对账才打开只读 DuckDB，不启动高频全表 Observer。
- 不在本阶段解决所有历史失败；仅为本轮测量所需的最小诊断修复补测试，不能更改发送安全、DB/WAL 或验收阈值。

### A1. 冻结离线传输包

包中应包含：

- 与冻结 SHA 对应的应用源及静态前端（如需）/完整 SLES 离线应用 wheelhouse。
- 与项目正式部署包相同的独立 Python 3.11.5 运行时及本地 DuckDB 1.2.0 `core_functions` 扩展。
- 单独的 Linux pytest 测试包（与正式应用分离，不打进生产发布包）。
- 一份小型、可审阅、无外部网络调用的 `perf-runner`/对照脚本（如有必要），含进程 PID 台账、只读宿主机资源采样、超时及自动停止本轮测试能力。
- `scenario-manifest.json`、`package-manifest.json`、`checksums.sha256`；每项包括 SHA256、Python、DuckDB、操作系统目标、场景参数与预期任务数。

每个 A 阶段产物都放在开发机独立的版本目录，不能覆盖旧包；离线包必须校验完整后才允许切换 VPN。构建过程中不触碰与当前任务无关的 dirty 工作树。

### A2. DEV 性能对照基线

在 WSL AlmaLinux-8 **native ext4** 上使用上述冻结 SHA 与固定场景；不得使用 Windows `/mnt/d` 盘作为数据库基线。统一如下条件（除测试软件自身必须固定的参数外）：

| 场景 | 负载 | 主要参数 | 目的 |
| --- | --- | --- | --- |
| C0 | 10 task / restart | loopback、可靠默认路径 | 重验已闭环能力 |
| P100 | 100 task，2 文件 | `fair/reliable/serialized`、pool=1、window=10、route TPS=200、fast simulator | 第一性能对照 |
| P1K | 1,000 task，10 文件 | 同上，`stress-1k` | 可重复吞吐/积压对照 |
| P1K-DB | 1,000 task | 仅改变 OCC vs serialized 的受控对照；若成功且安全再比较 Fast | 找出事务与发送模式贡献 |
| P5500 | 四活动 2×2500 + 2×250 | fair/reliable；pool=4、window=50、单 Route TPS=200 | 与历史 5500 失败对齐；开发机可先跑但报告独立 |

每种正式对照场景至少保留 1 次完整审计的 baseline；对开销较大的场景可在共享服务器只执行 1 次并注明样本不足，不伪造重复性结论。不要为了“改善成绩”改变超时窗口、SMS Content、Chunk/Journal durability 或 TPS 定义。

**重要：** `--route-tps=200` 是路由每秒 submit_sm **PDU parts** 约束，不是每秒多少条长短信。软件 TPS 测量分别给出 task accepted、socket submit PDU 和 SMSC response；不得混算。

A 阶段结束状态：`READY_FOR_SERVER_VPN_SWITCH` 或 `BLOCKED_DEV_PACKAGE`；此时不声称服务器已验证。

## 四、人工 VPN 切换边界

- Codex **不得自动切换** VPN、代理、系统路由，也不得声称 A/C 能和服务器 B 同时联网。
- A 完成后写出 `handoff-to-server.md`，包含冻结 SHA、离线包路径、SHA256、运行账号、预估数据量、端口机制、资源保护规则和可直接复制的命令。
- 由用户手动切换到可以访问测试服务器的 VPN，完成包传输；必要时服务器完全不联网。
- B 结束后输出证据包与 SHA256，用户手工带回开发机，再切回 GitHub VPN 处理 C 阶段。
- A/B/C 都必须支持独立重新进入，不要求同一个 Codex 会话持续在线；不得在换 VPN 时留下服务器后台无限期任务。

## 五、阶段 B — SERVER VPN：EulerOS 等效 SLES 压测

### B0. 只读准入与当前 Run 隔离

1. 确认真实 OS/kernel/glibc、可用 CPU、NUMA、内存、Swap、磁盘文件系统、空闲空间、负载、`vmstat`、`iostat`（如果安装），记入 `environment.json`。真实 OS 字段始终为 `EulerOS`；另设 `acceptance_target=SLES_PROXY_PERFORMANCE_TARGET`。
2. 确认**非 root**，工作区 canonical realpath 位于 `/home/paas/marketing-copilot-test`，且目标不存在指向区外的 symlink；只有管理员已批准的目录权限可以使用。
3. 在工作区的 `runs/` 下创建本轮**全新唯一 Run**；禁止复用或清理前轮 Run。设置 `umask 077`。
4. 所有环境变量、venv、packages、src、data、db、WAL、journal、tmp、cache、HOME、XDG、duckdb-home、pycache、logs、evidence 和临时压测文件指向本轮 Run；检查所有解压条目，无 `../`、绝对路径、symlink/hardlink 逃逸、特殊文件或 overwrite。
5. 严格检查包与扩展 SHA256；使用冻结的独立 Python 3.11.5 安装到 **Run 内**，使用 `--prefix` 时禁止 `--force`，不调用带 systemd 副作用的默认 `run.sh`，不安装任何全局依赖。
6. venv `pip --no-index --find-links`，本地扩展仅从 Run 载入；加载期间如外部下载被尝试 -> `BLOCKED_ENV`。
7. 检查 `ss`/`lsof`（仅只读），API/SMSC 动态绑定 `127.0.0.1`；发现真实 SMSC hostname/IP、业务 API 或来源不明 DB -> 立即阻断。
8. 确认 Run 输出目录可写、磁盘剩余空间充足和服务端口空闲。**不得使用 `/tmp`、`/var` 或系统 HOME 作为写入目标**。

### B1. 资源保护与停止条件

- 为本轮启动的进程记录 PID、PPID、start-time、可执行文件、Run 根目录、角色（Runner/API/SMSC/observer），不以通配模式结束其他程序。
- 默认低优先级 `nice`/`ionice`，可用时 `taskset` 绑定最多 4 个事先选择的逻辑 CPU；优先避开正在繁忙的 CPU/NUMA 热点。若没有可用的安全进程级限制与监测，不开始 1K/5500。
- 额外资源警戒线的初始建议：该 Run 全部子进程聚合 RSS **2 GiB**，当前 Run 写入数据 **5 GiB**，单轮 5500 业务计时 **300 秒**；系统负载、iowait、业务风险由 T0 采样和运行期间采样共同判断。硬件目标允许重新审批阈值，但不能在执行中静默上调。
- `--max-memory-mb` 如果只覆盖部分 API/SMSC 进程，必须增加**Runner 全子进程聚合**监视，不能把某单 PID 的 RSS 当作总额；对于 CPU/磁盘 I/O 采用观察式停止而非虚构硬限额。
- 观察器采样尽量低开销（如 2–5 秒一次 `/proc` + OS 摘要；性能观察 A/B 另按协议处理），不得在业务运行期间遍历所有任务/事件或全量扫描 WAL。
- 任何高负载可能影响其他业务、磁盘空间不足、非本轮监听地址、异常增长、进程卡死或测试失败 -> 不再升级负载，停止**本轮**服务，保存所有证据和退出码。
- 进程终止仅限核对后的本轮子进程；不得清理旧 Run、其他用户 PID、系统缓存或其他业务日志。

### B2. 梯度负载与自动升级门禁

对当前 SHA，在 EulerOS 上执行下表。**100 条和 1K 都是有效压测，不需要再因“非原生 SLES”被归类 BLOCKED。**

| Gate | 规模 | 运行条件 | 是否进入下一档 |
| --- | --- | --- | --- |
| E0 | 已有 10 task restart WAL/Loopback | 新包/新 Run 小样本回归 | 校验重启、Journal、离线扩展、无重复 |
| E1 | 100 task / full E2E | 单 Route、pool 1、window 10、TPS 200、fair/reliable/serialized | 全部接受且映射完整、资源未越界 -> E2 |
| E2 | 1K / full E2E | 与开发环境 P1K 完全一致，10 文件、300s 原始压力约束 | 完成态审计正确 + 资源安全 -> 可以进入 E3 |
| E2b | 1K / sender-only 或 ingestion-only | 仅在 E2 数据显示需要拆解时执行；固定样本和模式 | 取得阶段耗时定位，不能算独立 E2E PASS |
| E3 | 1K OCC vs serialized | 与 E2 除数据库并发模式外一致；Fast 只有明确必要时追加 | 可量化锁/冲突影响；任何不一致/未知重复/Crash 则停止 |
| E4 | 5500 / 四活动 | 仅 E0–E3 完成、资源持续平稳、取得共享主机当次运行窗口许可后执行；先 serialized，再 OCC；pool4/window50/route TPS200/300s | 一轮一审计，禁止以等待上限修改换取 PASS |

E4 是用户本次允许的**分级压测上限**。若未获得可安全占用的测试时段，仍交付 E0–E3 结果并标明 `E4_DEFERRED_SHARED_HOST`，不得判整个基础测试任务为失败。若 E1/E2/E3 出错，不得绕过失败直接跑 E4。

**禁止默认自动执行双 Route 52K、102K、长期 600s G6、G4 大矩阵或真实 SMSC。** 需要未来单独批准，先解决 52K/102K、180s 合同歧义。

### B3. 受控示例命令（Codex 应先核对本轮包内 CLI 再执行）

此处仅为正式命令模板，`$RUN`/`$PY`/`$SRC` 必须是 T0 创建并校验的当前 Run 绝对路径。以下所有 `--run-root` 和 `--output` 均位于当前 Run 内。

```bash
# 禁止在 root shell 下执行以下测试。
[ "$(id -u)" -ne 0 ] || { echo 'BLOCKED_SAFETY: root'; exit 1; }
BASE=/home/paas/marketing-copilot-test
# RUN 为阶段 B0 使用 mktemp 在 $BASE/runs 下创建的全新目录；不要复用旧路径。
# export RUN=/home/paas/marketing-copilot-test/runs/<new-run>
# export SRC="$RUN/src"; export PY="$RUN/venv/bin/python"
# export TMPDIR="$RUN/tmp" HOME="$RUN/home" XDG_CACHE_HOME="$RUN/cache"
# export PYTHONPYCACHEPREFIX="$RUN/pycache" DUCKDB_HOME="$RUN/duckdb-home"
# export MC_DUCKDB_EXTENSION_DIR="$RUN/src/offline/extensions"
# export MC_DUCKDB_OFFLINE_ONLY=1

# E1：100 task，2 files（必须先完成 SHA、资源、端口和路径预检查）
"$PY" "$SRC/backend/tools/local_e2e_acceptance.py" \
  --scenario stress-1k --gate full --events 100 --files 2 --expected-tasks 100 \
  --python "$PY" --send-mode reliable --dispatch-mode fair --db-concurrency serialized \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast \
  --task-workers 2 --ingest-workers 2 --workers 2 --claim-batch 20 \
  --timeout-seconds 300 --max-memory-mb 2048 \
  --run-root "$RUN/runs-e1" --output "$RUN/evidence/e1-100.json" \
  --keep-artifacts

# E2：1,000 task，10 files；E1 PASS 且共享主机预算安全才允许启动
"$PY" "$SRC/backend/tools/local_e2e_acceptance.py" \
  --scenario stress-1k --gate full --events 1000 --files 10 --expected-tasks 1000 \
  --python "$PY" --send-mode reliable --dispatch-mode fair --db-concurrency serialized \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast \
  --task-workers 2 --ingest-workers 2 --workers 2 --claim-batch 20 \
  --timeout-seconds 300 --max-memory-mb 2048 \
  --run-root "$RUN/runs-e2" --output "$RUN/evidence/e2-1k.json" \
  --keep-artifacts

# E4：仅示意四活动 5500 配置，不得在 E0–E3 及共享主机资源准入前直接执行
"$PY" "$SRC/backend/tools/multi_campaign_fair_acceptance.py" \
  --python "$PY" --large-rows 2500 --small-rows 250 \
  --dispatch-mode fair --send-mode reliable --db-concurrency serialized \
  --pool-size 4 --smpp-window 50 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast \
  --timeout-seconds 300 --max-memory-mb 2048 \
  --run-root "$RUN/runs-e4-serialized" \
  --output "$RUN/evidence/e4-5500-serialized.json" --keep-artifacts
```

模板必须由 Codex 在 DEV 环境做 `--help` 和 smoke 检查。**示例命令本身不实现聚合 RSS 限额、CPU 占用限制或超时后的整组进程回收**，这些能力必须由 B1 的受控 Runner/观察器补齐并验证后才能使用。`--max-memory-mb` 只能作为已有应用自检，不能替代共享宿主的进程树资源门禁。

### B4. 必须采集的可比证据

采样时**使用无量纲或有明确口径的指标**，避免把不同硬件/VM 的计数直接比较：

- `source_sha`、build/package SHA、应用/测试 wheelhouse SHA、独立 Python binary、DuckDB/扩展 SHA、PDU/SMSC 模拟器配置与消息正文长度、物理 Route TPS。
- `host_os_actual`（EulerOS）、`accepted_target`（SLES proxy）、kernel/glibc、vCPU/NUMA、内存、测试文件系统/磁盘、亲和 CPU 列表、nice/ionice 与资源预算。
- E2E 计时：从任务到齐/输入发布到全部 accepted、各阶段耗时、每秒 `generated / claimed / send-intent / socket_submit_sm / submit_sm_resp / journal_append / db_committed / terminal`，失败时记录分母和中止点。
- DB 热路径：Claim 候选扫描、每次 pre-send durable authorization 事务、OCC retries、WAL/write-lock waits、batch result transaction、checkpoint/fsync，记录 count、total、p50、p95、p99（没有足够样本则标 N/A）。
- 入队/排队：输入文件、白名单匹配、chunk checkpoint、ReadyPool/DRR 队列深度、每 Campaign 与 Route 积压及最早等待时间。
- Socket/SMPP：每 Route 的 declared/persisted/effective pool/window/TPS、bind 数、实际 submit PDU 数、响应数与 HTTP API 统计；response=accepted != delivered。
- Runner/Observer：live API 次数、响应字节、累计时间、shutdown 对账耗时。需要测 Observer off/on 时只改变观察器设置，防止测量本身压垮应用。
- 进程树聚合 CPU/RSS、宿主 vmstat/iowait、磁盘可用量/写入速率、网络监听和**原有业务是否出现异常**（只能从授权的只读系统指标判断）。
- 正确性：任务数、文件完成数、attempt/part、SMSC event 与 DB 映射、duplicate/unmapped/unknown/partial、paused/cancelled 发送栅栏、Journal durable replay。**完整停止之后**再做高成本对账。
- 全部 stderr/stdout、退出码、异常栈；出现 native signal 保留能安全取得的 native/core 证据，但不修改系统 `core_pattern`、`ulimit` 等全局设置。

至少输出 `run-manifest.json`、`resource-samples.jsonl`、`preflight.json`、各 Gate JSON、`post-shutdown-audit.json`、`server-evidence.sha256` 与有真实边界说明的报告。仅复制脱敏测试证据回开发机。

### B5. 自动停止与回滚（仅本轮测试）

若发生 `BLOCKED_SAFETY`、`BLOCKED_RESOURCE`、`SIGSEGV`、OCC/WAL corruption、unexpected outbound、任务丢失/重复、无法复核的 PDU 或任务归属：

1. 不开启下一档压力负载；本轮 Runner 发出优雅停止，只有确认属于本轮 Run 的进程才能逐级终止。
2. 不重试任何可能已被 SMSC 接受但状态为 unknown/partial 的短信；即使只连接模拟器也要保持真实语义。
3. 保留完整 JSON、Journal、WAL、日志和资源证据；不删损坏状态、不得以重新初始化覆盖失败证据。
4. 输出 `blocked_reason`、`last_safe_stage`、PID 回收情况与主机业务影响的观察结果；若仅是性能未达目标仍保留 `FAIL` 而不是 `PASS_WITH_WARNING`。

## 六、阶段 C — 切回 DEV/GitHub VPN、对照与收口

1. 在服务器 VPN 阶段完成证据哈希清单；开发机导入文件后重新校验 SHA256，不覆盖历史证据，不上传真实业务数据、凭证或未经审查的日志。
2. 把 **WSL 与 EulerOS 的同 SHA / 同 CLI / 同数据量 / 同执行模式** 放在一张表中；有硬件限制或任务模式不同的组不得直接作为因果 A/B。
3. 形成性能拆解结论：`HARDWARE_RESOURCE_BOUND`、`DB_TRANSACTION_LOCK_BOUND`、`DISPATCH_SYNC_WAIT`、`HARNESS_OBSERVER_OVERHEAD`、`SMPP_ROUTE_LIMIT`、`MIXED_OR_UNKNOWN`，每个类别标注 `PROVED / INDICATED / NOT_PROVED` 和对应证据。
4. 单独输出：`EULEROS_PROXY_P100_STATUS`、`P1K_STATUS`、`P5500_STATUS`、`OFFLINE_WAL_STATUS`、`HOST_SAFETY_STATUS`、`INTEGRATED_STATUS`。正确性与性能 PASS 必须分开。
5. 特别说明「EulerOS 是用户接受的本轮 SLES 等效性能目标」与「未经实际验证的原生 SLES 特性」；绝不将 EulerOS 结果伪写为 SLES 主机实测，也不因缺少原生 SLES 自动让本轮 proxy 性能验收 FAIL。
6. `Docs/validation_contract.json` 不变，除非用户另行批准 52K/102K、G4 180s 语义和 G6 600s 义务。`INTEGRATED_NO_GO` 仍保持，直到真正消除剩余 Gate。
7. 汇总为**唯一主报告**：`Docs/Marketing_Copilot_EulerOS_SLES_Proxy_Load_and_Dual_VPN_Performance_Acceptance_Report.md`；含原始命令、冻结源码、运行条件、对比数据、是否允许进一步放大压力与建议优先优化路径。
8. 代码修复只在有充分证据支持且不改变安全语义时进行。**不能为了通过压测而默认调高 checkpoint、放松 Result durability、删去审计、放宽超时或改变 SMS 重发规则。**
9. 遵守 `AGENTS.md`：**不得自动 commit/push**，仅在用户另行明确要求后执行；如 GitHub VPN/443 不通，只记录失败原因，保留本地工作，不改系统代理/VPN，不回滚既有修改。

## 七、验收结果与最终交付

| 状态 | 含义 |
| --- | --- |
| `DEV_PACKAGE_READY` | 开发机 A 阶段完成；可手动换 VPN，不代表服务器测试通过 |
| `BLOCKED_SAFETY` | 路径、账号、网络、进程保护或宿主影响条件不满足；停止 |
| `BLOCKED_RESOURCE` | 资源预算触发或服务器繁忙；保留当前部分证据 |
| `EULEROS_PROXY_P100_PASS` | 100 条真实 E2E + 独立对账通过；仅本轮等效目标 |
| `EULEROS_PROXY_P1K_PASS` | 1K 严格完成且无安全/正确性问题 |
| `EULEROS_PROXY_P5500_PASS` | 四活动 5500 严格完成、审计一致、宿主资源安全；**不等于 G4/G6 PASS** |
| `E4_DEFERRED_SHARED_HOST` | 共享主机负载或授权窗口不允许 5500，基础阶段可独立收口 |
| `FAIL` | 当前既定场景未满足其不可放宽的正确性/时间/容量门禁 |
| `INTEGRATED_NO_GO` | 目前全项目状态，持续保留至 G4/G6 等后续正式门禁闭环 |

**交付完成条件**：固定源/依赖包可追溯；开发机与 EulerOS 有可比较的 E1/E2 数据或明确资源阻断证据；共享主机没有因测试修改外部文件、系统配置或既有业务；服务端所有本轮子进程核验退出；证据完整可复核；原集成门禁判定真实且未放宽。

## 八、Codex 启动提示词

> 执行 `Docs/Marketing_Copilot_EulerOS_Proxy_SLES_Load_And_Dual_VPN_Performance_Acceptance_Codex_Task.md`。先完成阶段 A，不主动切 VPN、不连接测试服务器。`d6b482f2` 为冻结测试代码基线。用户已批准将 EulerOS 2.0 SP15 作为本轮 SLES12SP5 等效**性能**测试目标，请据此实施分级 100/1K/受控 5500 压测，不再因非原生 SLES 自动阻断；但报告必须真实标注 EulerOS。服务器所有写入限定 `/home/paas/marketing-copilot-test`，严格保持非 root、独立 Python、离线 WAL/扩展、仅 loopback、进程/资源保护。E4 须在低档 Gate 与服务器测试窗口均满足后才运行；52K/102K G4/G6 未经单独批准不得执行。完成 A 阶段后形成冻结交接包，待手工换 VPN 才执行 B；再手工切回进行 C。不可改变安全/正确性与正式容量门禁。不可自行提交、推送或修改系统 VPN/代理。
