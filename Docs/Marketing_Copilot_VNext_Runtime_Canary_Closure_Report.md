# Marketing Copilot VNext Runtime Canary Closure Report

## 状态

- Baseline: `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`
- `HEAD` / `origin/main`: 均为 `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`
- `INTEGRATED_NO_GO`: 保持不变
- 历史状态 `VNEXT_PIPELINE_READY_FOR_CANARY`: 保持不变
- 本阶段：`STAGE_C_EVIDENCE_VERIFIED`
- 当前总状态：`VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`（Stage A 已通过；本轮唯一一次修复后授权 D7/R2/P5500 仍以 `SIGSEGV (-11)` 退出，因此按任务规则不宣称闭环 PASS）
- R2/P5500：已获本任务显式授权并执行两次；第二次使用完全独立环境路径，仍在 `34.726 s` 以退出码 `-11` 结束，未生成 canary 结果；按 fail-closed 原则不进行第三次重跑
- 修复后 D7/R2/P5500：已获本任务显式授权并仅执行一次；在 `34.361 s` 以退出码 `-11` 结束，perf guard 未触发 RSS/磁盘限制；不再重跑
- EulerOS 长稳 R7：已获本任务显式授权，并在全新独立 Run Root 完成；canary 与 guard 均 PASS
- 真实 SMSC / 公网号码：未使用；所有新验收仅绑定 `127.0.0.1` 和合成数据

Stage A 与 EulerOS 远端 Stage B 分开记录；本报告不把任一侧的 PASS 扩大解释为另一侧的 PASS。Stage C 已完成证据核验与报告收口；R7 已通过，但修复后 D7/R2/P5500 仍复现 SIGSEGV，当前总体为 `FAIL`。

## C0 RCA

### 修复前复现

证据：`test-artifacts/vnext-runtime-canary-closure-20261016/c0-before.json`。

真实 `VNextRuntime.start()` 先启动空闲 Worker，再在线 `enqueue_chunk()` 10 条，结果为：`accepted=10`、`pending_records=10`、`smpp_received=0`、`socket_written=0`。随后重启 Runtime，之前写入的 Pause 控制也没有出现在控制状态中，状态为 `FAIL_REPRODUCED`。

根因归纳：

1. 长驻 Worker 持有启动时的 Spool Manifest/Segment 视图，空闲后看不到其他进程追加的 Manifest、旧 Segment 追加和新 Segment。
2. Writer 正式启动链以 IPC Queue 为主要事实入口，没有证明并发 Journal 增量 catch-up；Queue 满/Writer 故障可能阻断 Worker 或使已持久化的进度失去在线投影。
3. Runtime 启动时会覆盖控制状态；Worker 只在批次边界读取控制，Prefetch 后的 Pause/Cancel/Resume 语义和重启持久化没有闭环证明。

### 修复后复现

证据：`test-artifacts/vnext-runtime-canary-closure-20261016/c0-after-p0.json`。

同一路径结果为 `NO_FAILURE`：`accepted=10`、`pending_records=0`、`smpp_received=10`、`socket_written=10`；重启后保留 `batch/902=paused`。

## 代码收口

- `backend/vnext/spool.py`：增加 Manifest/Segment 签名与增量 `refresh()`；`iter_from()` 在每轮读取前发现在线追加；状态统计不再对全部记录做 O(total data) 扫描。
- `backend/vnext/projection_writer.py`：ProgressJournal 按 offset 增量读取；DB 事务成功后才持久化 offset；IPC 仅作唤醒提示；`event_id` 保持幂等；Writer 支持 watchdog restart 和 Journal replay；删除未使用的旧进程入口。
- `backend/vnext/runtime.py`：Journal append 是权威事实流；Queue 满不再阻断 Worker；持久化控制状态使用原子 replace/fsync 和版本号；Writer 自动恢复；`wait_for_idle()` 等待 Spool、Worker、Transport 和 Journal offset 全部追平。
- `backend/vnext/worker.py`：运行中轮询控制文件；Pause 不 checkpoint Prefetch 记录；Cancel/Resume 语义闭环；提供 inflight 快照和长驻 Transport 生命周期。
- `backend/services/vnext_service.py` / `backend/routers/vnext.py`：增加仅 loopback、显式 canary mode 的真实 API 摄取入口，并修正 `paused` 状态映射。
- `backend/tools/vnext_runtime_canary_acceptance.py`：真实 `main + FastAPI + Uvicorn + VNextRuntime.start()` 的 R0–R7 工具；默认 4 Worker、R2 不在默认门禁；证据不记录 MSISDN 全文。
- `backend/tools/vnext_runtime_c0_repro.py`：修复前后 C0 最小复现工具。
- `deploy/build-sles12sp5-system-python-manual.sh`：正式应用包排除 `backend/tests`、canary/loopback/perf guard 测试工具；pytest 测试包独立交付。
- `deploy/build-vnext-runtime-canary-test-bundle.sh` / `deploy/vnext-runtime-canary-test-runner.sh`：生成独立离线 pytest/canary 包，显式 CPU 数、`nice +10`、`ionice -c3`、RSS 2 GiB、Run 磁盘 5 GiB 保护。

## 本地回归

- Windows：`380 passed, 4 skipped, 4 warnings`。
- 前端：`npm run build` PASS；只有既有 Rolldown `#__PURE__` 注释和 chunk size warnings。
- C0 闭环专项：`4 passed`。
- 离线测试包：从独立 CPython 3.11.5 runtime 创建 venv，使用 `--no-index` 安装 app wheels 与 pytest wheels，闭环专项 `4 passed`。
- 正式应用包从同一最终源码重建，并通过包内 `checksums.sha256`、`release.json`、`frontend/dist/index.html`、`offline/wheels/`、`run.sh` 校验。

## WSL AlmaLinux-8 原生 ext4 Stage A 门禁

正式 Linux 运行使用原生 ext4，不使用 `/mnt/d` 作为定量验收目录。最终 Run 配置：4 Worker、64 partitions、`route_tps=80`、Loopback response delay 2 ms、CPU 绑定 0–3。

预检：

- `df -T .`: `/dev/sdd ext4`，`/` 使用率 10%。
- Python：CPython `3.11.5`，`/tmp/mc-vnext-stage-a-20261016/venv/bin/python`。
- 依赖：结果 JSON 已记录 `pip freeze`；正式 runtime 是独立 CPython 3.11.5，不修改系统 Python。
- Loopback binds：仅 `127.0.0.1`。

### R0–R6

证据：`test-artifacts/vnext-runtime-canary-closure-20261016/stage-a/results/r0-r6-wsl-ext4-final.json`。

| Gate | 结果 | 关键结果 |
|---|---|---|
| R0 | PASS | 在线空闲后注入 100；received=100；projection unique=100 |
| R1 | PASS | 多 Campaign/多波次 1,000；projection unique=1,000；Retry duplicate 仅为入口去重，Loopback 无 duplicate |
| R3 | PASS | Route TPS=80；1,000 条全部 received；无 Window/Route 错误 |
| R4 | PASS | Writer crash 后自动 restart=1；400 条最终投影 |
| R5 | PASS | Pause/Resume、B 正常、Cancel、控制重启持久化均通过；取消 20 条独立计数 |
| R6 | PASS | Worker crash/restart 后 200 条全部完成；无双 Owner 证据 |

汇总：`unique_events=2940`、`taken=2940`、`socket_written=2920`、`cancelled=20`、Loopback received/response success=`2920`、`taken_without_socket_written=20` 仅对应明确取消记录。

### R7

证据：`test-artifacts/vnext-runtime-canary-closure-20261016/stage-a/results/r7-10m-wsl-ext4-final.json`。

- 结果：PASS
- 运行时长：`612.825 s`
- 采样点：`507`
- 发送/投影唯一事件：`3960/3960`
- Loopback received/response success：`3960/3960`
- 峰值 RSS：`193,785,856 bytes`
- 峰值 FD：`91`
- 峰值 Run Root：`9,771,140 bytes`
- 最大 Writer lag：`0.0 s`
- pending/Journal 未出现无界增长；R7 结束后 `taken_without_socket_written=0`

### WSL 环境异常与复测

一次未绑定 CPU 的重跑在约 30 秒处发生 SIGSEGV；内核日志指向 WSL2 的 `libpython3.11.so`、CPU 20，且没有测试子进程残留。该 Run Root 保留在 WSL，不作为 PASS 证据。

随后使用同一源码、同一 ext4 和 CPU 绑定 0–1 的 R0 复测 PASS；最终使用 CPU 0–3、4 Worker 重跑 R0–R7 全部 PASS。该环境异常及复测策略将在最终报告中保留，不隐去失败记录。

## EulerOS 2.0 SP15 服务器 Stage B

服务器：`10.202.245.70`，账户：小写 `paas`，OS：`EulerOS 2.0 (SP15x86_64)`。既有 R0/R1/R3/R4 使用：
`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-b-2047/`；本次授权扩展门禁使用全新独立 Run Root：
- R2 首次尝试：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r2-2108/`
- R2 隔离重试：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r2-retry-2115/`
- R7 隔离重跑：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r7-isolated-2130/`

- 文件系统：`df -T` 确认为 `/dev/sda2 ext4`；既有 R0/R1/R3/R4 Run Root 运行结束约 `699 MiB`，当时可用空间约 `6.5 GiB`；扩展门禁结束时服务器可用空间约 `3.7 GiB`。
- 运行时：离线安装正式 standalone CPython `3.11.5`，应用与测试依赖均使用 `--no-index` wheel；`pip check` 通过，测试包回归 `4 passed`。
- 资源边界：每个门禁独立进程组和独立子目录；R0/R1/R3/R4 CPU 绑定 `0,1`，R2 隔离重试与 R7 绑定 `0,1,2,3`（均不超过 4 CPU）；统一使用 `nice +10`、`ionice -c3`、RSS 上限 `2 GiB`、Run Root 磁盘增量上限 `5 GiB`。
- 网络边界：canary 仅绑定 loopback；未连接真实 SMSC、公网号码或外部消息中间件。

| Gate | 结果 | 对账/恢复结果 | 守护峰值 RSS | 磁盘增量 |
|---|---|---|---:|---:|
| R0 | PASS | 100/100 accepted，projection 100，loopback 100 | 763,424,768 B | 0 B |
| R1 | PASS | 4 campaign 合计 1,000/1,000；API 重复 500；projection 1,000；loopback 1,000 | 798,097,408 B | 0 B |
| R3 | PASS | 1,000/1,000 accepted，projection 1,000，loopback 1,000，无丢失/重复 | 790,376,448 B | 0 B |
| R4 | PASS | 400/400 accepted，projection/loopback 400，writer 自动重启 1 次 | 789,917,696 B | 0 B |

四个门禁的性能守护均为 PASS，所有受管进程均已退出；服务器证据中仅残留收集检查命令自身的 `awk` 行，无测试进程。有限证据已回传到：
`test-artifacts/vnext-runtime-canary-closure-20261016/stage-b/server/`，回传包 SHA256 为 `fe5a7d15d914b55b05730c16f23eabf68343e1c9a6a3473bbf20566a7ecf9ddc`。

### 授权扩展门禁：R2/P5500 与 EulerOS 长稳 R7

| Gate | 结果 | 原始守护结果与关键结果 | 守护峰值 RSS | 磁盘增量 |
|---|---|---|---:|---:|
| R2 首次尝试 | `BLOCKED_ENV` | guard 原始结果 `FAIL`，`35.406 s`，退出码 `-11`；该次环境路径审计不计入正式隔离验收 | 817,946,624 B | 0 B |
| R2 隔离重试 | `BLOCKED_ENV` | guard 原始结果 `FAIL`，`34.726 s`，退出码 `-11`；`HOME/TMPDIR/XDG_CACHE_HOME` 均归属本轮 Run Root；未生成 `canary.json` | 887,840,768 B | 0 B |
| R7 隔离重跑 | PASS | canary `608.767 s`、发送 `4000` 条；loopback received/response `4000/4000`，projection unique/taken/socket_written `4000/4000/4000`；`539` 个样本；guard `609.799 s`、退出码 `0`；峰值 FD `90`、峰值 Run Root `9,866,961 B`、最大 Writer lag `0.0 s`、最大 pending `62`、Writer 全程存活 | 900,640,768 B | 0 B |

R2 两次均在受控进程组内以 `SIGSEGV (-11)` 退出，未触发 RSS 或磁盘保护；第二次是正式隔离重试，仍复现相同环境级故障。两次失败后仅按 PID/启动时间/命令行精确清理 guard 记录的孤儿子进程，未使用模式匹配，也未触碰业务进程；最终四个相关 Run Root 的进程审计均为 `matching_processes=[]`。该重复故障归类为 EulerOS 运行时环境阻塞，不将原始 guard `FAIL` 偷换为业务功能 FAIL，也不将 R2 计为 PASS。

上一轮 R7 因 guard 记录的 `HOME/TMPDIR/XDG_CACHE_HOME` 指向其他 Run Root，虽未发现跨目录运行时文件，也不纳入正式定量证据。本次 R7 使用全新目录并显式绑定 `HOME`、`TMPDIR`、`XDG_CACHE_HOME`、`PYTHONPYCACHEPREFIX`，确认 `/dev/sda2 ext4`、CPython `3.11.5`、`pip check` 通过；本次结束时文件系统可用空间约 `3.7 GiB`，但每轮 guard 磁盘增量均为 `0 B`，未超过每轮 `5 GiB` 限制。

扩展门禁的最小证据归档已回传并在 GitHub VPN 下本地核验：
`test-artifacts/vnext-runtime-canary-closure-20261016/stage-b/server/stage-b-r2-r7-transfer.tar.gz`；包含 `15` 份证据文件，归档 SHA256 为 `ab2a131d642aa290cda80dabda94d5f3a7c4d469140a23aa7e1ace25cc6581bf`。归档不包含数据库、WAL、Spool、运行日志或上传数据。

## Stage C GitHub VPN 收口核验

- `HEAD` 与 `origin/main` 均为冻结基线 `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`；未执行 reset、clean、commit 或 push。
- 本地逐项核验新增服务器回传包内 15 份证据文件的 SHA256，且三份离线包哈希与服务器记录一致；新增 R2/R7 回传归档 SHA256 为 `ab2a131d642aa290cda80dabda94d5f3a7c4d469140a23aa7e1ace25cc6581bf`。
- 任务相关 tracked diff：8 个源码/部署文件，`630 insertions(+), 94 deletions(-)`；另有 6 个任务相关新增测试、验收工具、测试包脚本和本报告。`git diff --check` 通过。
- tracked 文件为：`backend/routers/vnext.py`、`backend/services/vnext_service.py`、`backend/tools/vnext_loopback_acceptance.py`、`backend/vnext/{projection_writer,runtime,spool,worker}.py`、`deploy/build-sles12sp5-system-python-manual.sh`；新增文件为闭环测试、C0/Canary 工具、独立测试包构建/运行脚本和本报告。
- 既有 `AGENTS.md`、`frontend/package-lock.json` 以及其他 dirty/untracked 用户文件均保持原状；测试包、运行数据、数据库、WAL、Spool、日志和凭据未进入正式应用包或 Git 变更。
- 真实 SMSC、公网号码和生产发布仍未进行；是否进入真实 SMSC 只能作为后续现场评审，不能由本次合成 Loopback 结果直接放行。

## Stage A 离线交付包

目录：`test-artifacts/vnext-runtime-canary-closure-20261016/stage-a/packages/`

| 包 | 用途 | SHA256 |
|---|---|---|
| `marketing-copilot-sles12sp5-system-python-20261016-vnext-runtime-canary.tar.gz` | 正式应用包；不含 pytest、测试目录、运行时 DB/WAL/日志 | `67a7add867b6902d74c3de2060cb7b87d14de9ec5e768a3e48e1017594109148` |
| `sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz` | 正式 standalone CPython 3.11.5 runtime；独立于应用包 | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` |
| `marketing-copilot-vnext-runtime-canary-test-20261016-vnext-runtime-canary.tar.gz` | 测试专用 pytest/canary/guard 包；不合入正式应用包 | `f329f1b255566eeef7a1b02d819ea1280f2ab59515cc94813c17d54b7a83c144` |

测试包内 pytest wheels 仅为 `pytest==8.3.5` 及其离线依赖；没有应用 wheels、standalone runtime、数据库、WAL、Spool、日志、上传、备份或凭据。正式应用包内没有 `backend/tests`，也没有 pytest/psutil。

## 未执行项与下一步

1. R2/P5500 已获授权但被 EulerOS 运行时 `SIGSEGV (-11)` 阻塞；R7 已完成并通过。当前状态为 `BLOCKED_ENV`，不是服务器证据校验失败，也不是最终闭环 PASS。
2. 如需将 R2 推进为 PASS，必须先完成 EulerOS/Python/底层运行时故障定位与环境变更评审；本次不自动扩大资源授权，也不再盲目重跑。
3. 真实 SMSC、生产发布、SLES12SP5 实机放行仍属于后续评审范围。

本阶段没有执行提交或推送；既有 `AGENTS.md`、`frontend/package-lock.json` 以及其他 dirty/untracked 用户文件均未清理或覆盖。

## Native SIGSEGV RCA / Runtime Stabilization follow-up（2026-10-16）

本节追加本轮证据，不覆盖上文历史结论、R2 失败记录或 `INTEGRATED_NO_GO`。

### 当前 RCA 等级

`RCA_STRONGLY_INDICATED`。历史 R2/P5500 两次真实主链均在约 35 秒内以 `SIGSEGV (-11)` 结束；静态调用链和修复前生命周期均指向“已初始化 DuckDB/native/线程/async 状态的 API 进程再创建多进程 worker/writer”这一高风险边界。归档没有 core 或 faulting native frame，因此不能写成 `RCA_PROVED`。本地最小 `fork`/`spawn` 对照均 PASS，进一步说明 fork 是强假设而非已证明唯一根因。

### 本轮代码与回归

- Windows backend full regression：`382 passed, 4 skipped`。
- WSL native ext4 full regression：`384 passed, 2 skipped`。
- native ext4 clean real-main R0/R1/R3/R4/R5/R6：全部 PASS；R3 route-credit burst 在增加进程内 credit lock 后恢复为 `1000/1000`。
- Exact R7：`4000/4000`，loopback response `4000/4000`，projection `4000/4000/4000`，无 `taken_without_socket_written`。
- spawn/fork 最小诊断均 PASS；worker/writer exit code、role metadata、restart backoff、低速 flush、journal replay 和 control-state restore 均有专项回归。
- 前端 `npm run build` PASS，仅保留既有 Rolldown 注释和 chunk-size warning。

### 离线交付边界

正式 SLES12SP5 应用包、独立 CPython 3.11.5 runtime 包、pytest/diagnostic 测试包分别生成并通过内部 checksum。正式包排除了 tests、pytest、acceptance/perf/diagnostic 工具、运行时 DB/WAL、日志、上传、备份和凭据；测试包明确标记为 `formal_deployment_package_modified=false`，不合入正式部署包。

### EulerOS B2

本轮已在 EulerOS 2.0 SP15、非 root `paas`、原生 ext4、4 CPU、`nice +10`、`ionice -c3` 和 loopback-only 条件下完成 D0–D6/R7：D0 最小诊断、R0/R1/R3/R4/R5、R6/R7 及 perf guard 均 PASS；R2/P5500 本轮未执行。服务器归档 SHA256 为 `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`，已生成 `PHASE_C_READY_FOR_MANUAL_VPN_SWITCH`。

### Overall closure

历史小节中的 `VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV` 记录保持不变；当前最终状态由本轮 D7 receipt 更新为 `FAIL`。本地和 EulerOS R7 的 PASS 只能证明 bounded runtime path 在这些条件下稳定，不能覆盖修复后 R2/P5500 的 `-11`；未执行 commit/push。

### Stage C receipt（2026-10-16，VERIFIED）

本轮服务器 B2 归档已下载并完成本地核验：

- `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`
- SHA256：`e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`；863 个成员，路径逃逸 0，链接条目 0。
- 独立解包完成；`B2-files.sha256` 670/670 项通过；core 文件清单和残留进程清单均为空。
- R6 PASS：200/200；R7 PASS：target/sent 4000/4000，611.721 秒，peak RSS 576,663,552 B，peak FD 119，peak Run Root 10,157,749 B，writer lag 0.0 秒。
- R6/R7 合计 loopback/response 4200/4200，projection unique/taken/socket_written 4200/4200/4200，`taken_without_socket_written=0`；perf guard max aggregate RSS 582,787,072 B，disk delta 0 B，exit code 0。

该段记录的是 D7 执行前的中间 receipt；最终结果见下方 D7 小节，当前总体状态为 `FAIL`。未执行 commit/push。

正式应用包已在 Stage C 边界审计后重建，最终 SHA256 为 `7a9b4106d8dd7a86fa27670b8ff81ad0b3412342cecdc796ba137cd764a7fdc1`；应用包内部 checksum 通过，且不含 `backend/tools`、tests、`*.bak*`、日志、DB/WAL、uploads、backups、runtime data 或 native test diagnostic。测试包 SHA256 为 `bb02f7e902fc7caac15a1ef906eae1c11b84cb5f1e5209bbf1c320698515b130`，standalone runtime SHA256 为 `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`；测试包仍未合入正式部署包。

### D7/R2/P5500 最终结果（FAIL）

- Run Root：`/home/paas/marketing-copilot-test/runs/vnext-native-sigsegv-rca-20261016-d7-r2-p5500`
- 证据归档：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`
- 内部 `d7-evidence-final.sha256`：7/7 项本地复核通过。
- SHA256：`ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e`。
- Perf guard：`FAIL`，退出码 `-11`，耗时 `34.361 s`，峰值 aggregate RSS `559325184 B`，磁盘增量 `0 B`；未触发资源限制。
- 进程归属：guard root PID `37226` 是 standalone CPython canary 命令，最终以 `-11` 退出；guard 未提供 worker/writer native frame。PID `42553`/`42557` 为崩溃后 reparent 的 resource tracker/spawn worker，已精确清理。
- `canary.json` 未生成；core 清单为空；两个 reparented 子进程已按精确 PID 清理，post-cleanup 审计确认 `42553`/`42557` 均不存在。

最终 `RCA_STRONGLY_INDICATED` 保持不变（缺 native faulting frame）；显式 `spawn` 不是完整修复，不能把 fork 假说写成唯一根因。总体 `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`。不再执行第三次 D7/R2；未执行 commit/push。
