# Marketing Copilot VNext Runtime Canary 最终验收与发布就绪报告

任务 ID：`MC-VNEXT-FINAL-CANARY-ACCEPTANCE-RELEASE-READINESS-20261017`

执行日期：2026-10-17

## 结论前置

本报告只复核当前工作区已有本地证据，没有连接服务器，没有执行新的 R2/P5500、R7、实际 SMSC、正式部署或生产变更。

当前代码基线为：

```text
HEAD       = 01409126a5c315ee91696c135db52a5c5ece2287
origin/main= 01409126a5c315ee91696c135db52a5c5ece2287
```

当前合成 Loopback 的机器证据支持 R2/P5500 聚合 PASS：`5500/5500` accepted，Loopback `submit_sm_received=5500`、`submit_sm_resp_success=5500`，Projection `unique/taken/socket_written=5500/5500/5500`，Guard `PASS/exit=0`。但四个 Campaign 的 Projection unique/taken/written 和 `submit_sm_resp_success` 没有逐 Campaign 字段，因此当前验收决定为：

```text
CURRENT_VNEXT_LOOPBACK_CANARY=PASS_REPORTED_EVIDENCE_GAP
LATEST_EULEROS_R2_P5500=PASS                 # 聚合机器证据已核验
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
HISTORICAL_D7_R2_NATIVE_FAILURE=PRESERVED
LATEST_EULEROS_R7=PASS
RELEASE_CANDIDATE_DOCUMENTED
PRODUCTION_APPROVAL=NOT_GRANTED
INTEGRATED_STATUS=INTEGRATED_NO_GO           # 历史集成架构结论保持不变
```

正式应用包和测试包的字节身份已核验，但正式包 `release.json` 记录 `8f14e82d-dirty`，测试包 `bundle.json` 记录 `337f3e623431a37705ee12041186dd0fe6b692d2`，均不是当前 clean `main` 的精确来源声明。因此在真实 SMSC 或生产发布前必须从 clean `01409126...` 重建并重新建立 hash traceability；本次 R2 PASS 不因该发布元数据问题被改写，但也不能据此授予生产发布。

## 1. 执行边界与工作树审计

已读取 `AGENTS.md`、任务书指定的历史报告、当前源码和部署/测试包脚本。工作树在任务开始前已有用户改动：tracked dirty 文件包括 `AGENTS.md`、`frontend/package-lock.json`，并存在大量既有 untracked Docs、test-artifacts、数据库和测试文件。本任务未执行 `reset`、`clean`、覆盖、服务器连接、VPN 操作或代码重构。

最近两个相关提交的边界如下：

| Commit | 实际变化 | 复核结果 |
|---|---|---|
| `a3ea4ab617c39bd3d0c5740c2396ef047f8ebc61` | `backend/tools/vnext_runtime_canary_acceptance.py`、`backend/tests/test_vnext_static_audit.py` | 仅测试工具 HTTP timeout 与单测；blob 分别为 `93a7c55ac82feb649690c0dfd99898315de9a656`、`ed7dcc80124bfb3e5c3769cc456fb4f47f5da7f9` |
| `01409126a5c315ee91696c135db52a5c5ece2287` | 新增 R2 300 秒追加验收报告 | 只新增 `Docs/Marketing_Copilot_VNext_EulerOS_R2_300s_Followup_Acceptance_Report.md`，blob 为 `c1ee6a2abaf66eeefc7e39eed790fa0008ce7344` |

当前源码中的 `CANARY_HTTP_TIMEOUT_SECONDS=300.0` 与受测测试包中的同一文件一致；这只证明测试工具内容已包含 300 秒修复，不改变正式应用业务代码的发布授权边界。

## 2. E0：提交与包身份冻结

### 2.1 当前使用的包和归档

| 对象 | 本地文件 | 外层 SHA256 | sidecar | 内部 manifest | 内容边界/结果 |
|---|---|---|---|---|---|
| 正式应用包 | `test-artifacts/vnext-targeted-native-20261017-r3f7c2/transfer/marketing-copilot-sles12sp5-system-python-20261017-vnext-euleros-targeted.tar.gz` | `bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44` | `VERIFIED` | `179/179` | 195 个 tar 成员；`backend/tests=0`、`backend/tools=0`；数据库/WAL、Spool segment、Journal JSONL、logs/uploads/backups 等运行数据文件为 0；`VERIFIED` |
| standalone runtime | `test-artifacts/vnext-targeted-native-20261017-r3f7c2/transfer/sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz` | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | `VERIFIED` | standalone runtime，无应用 manifest | CPython 3.11.5；路径安全、链接检查 `0`；`VERIFIED` |
| 测试专用包 | `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/marketing-copilot-vnext-runtime-canary-test-20261017-vnext-euleros-targeted-r2-300s.tar.gz` | `f1d59bf5e6817c8915a754eca53d73a4cb8c8cbce175f666a8a191d35d5f6e9a` | `VERIFIED` | `19/19` | 26 个 tar 成员；包含 pytest、canary、guard、diagnostic、`backend/tests`/`backend/tools`，不含应用 runtime、DB、WAL、Spool、Journal、logs、uploads 或 credentials；`VERIFIED` |
| 当前 R2 精简证据 | `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/r2-evidence-sanitized-300s.tar.gz` | `39e918deb75f4b5659d20976c6f17816e0299389c3fb36168e7c8bec52521013` | `VERIFIED` | `19/19` | 28 个成员；不含 DB/WAL/Spool/Journal/上传数据/凭据；`VERIFIED` |
| 历史 EulerOS B2/R7 证据 | `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz` | `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd` | 未提供同名 sidecar；外层 hash 已复核 | `670/670` | 历史原始证据归档，成员中实际包含 `app.duckdb`/WAL、Spool、state logs/backups 等运行数据；不是部署包，也不能标为 sanitized；保留为历史证据并单独标注 |
| 历史 D7/R2/P5500 证据 | `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz` | `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e` | `VERIFIED` | `7/7` | 8 个成员；路径/链接安全检查通过；`VERIFIED` |

所有六个归档均完成绝对路径、盘符路径和 `..` 路径检查，未发现路径逃逸；tar verbose 类型检查未发现符号链接或硬链接。当前正式包中的 `core_functions.duckdb_extension` 是离线依赖扩展，不是运行数据库。

### 2.2 源码身份与可复现性

| 身份问题 | 结论 | 依据 |
|---|---|---|
| `TESTED_PACKAGE_IDENTITY_VERIFIED` | `YES` | 三个当前包的外层 hash、sidecar（可用时）和内部 manifest 均核验；R2 evidence 的 `package-hashes.txt` 与三包 hash 一致 |
| `SOURCE_PROVENANCE_EXACT` | `NO` | 正式包 `backend/release.json.git_sha=8f14e82d-dirty`；测试包 `bundle.json.source_commit=337f3e623431a37705ee12041186dd0fe6b692d2`；当前 HEAD 为 `01409126...` |
| `BUILD_REPRODUCIBLE` | `NOT_PROVEN` | 当前没有从 clean `01409126...` 重建的正式包 receipt；正式包当时带 dirty provenance |
| 受测正式业务源码与当前工作树 | 关键 VNext 文件 hash 相同 | `backend/vnext/{diagnostics,runtime,spool,worker,projection_writer,smpp_transport}.py`、`backend/services/vnext_service.py`、`backend/routers/vnext.py`、`backend/main.py` 已逐文件比较；这不等于 release metadata provenance exact |

E0 的发布阻断项是 provenance/rebuild，不是把已完成的服务器 R2 运行判为无效。未来必须 clean checkout、固定 wheelhouse/runtime、生成新 `release.json`、复核包内 manifest，并将正式包、测试包、standalone runtime 和受测包建立一对一 hash 链。

## 3. E1：R2/P5500 全链路与四 Campaign 对账

### 3.1 机器证据与执行配置

当前机器证据来自：

```text
Archive: test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/r2-evidence-sanitized-300s.tar.gz
JSON:    evidence-export/r2/canary.json
Guard:   evidence-export/r2/guard/perf-guard.json
Request: evidence-export/r2/canary-launcher-request.json
Run:     /home/paas/marketing-copilot-test/runs/vnext-targeted-native-r2-20261017-retest300/r2/canary
```

服务器环境字段由现有报告与归档 preflight 共同支持：EulerOS 2.0 SP15 x86_64、非 root 小写账号 `paas`、`/dev/sda2 ext4`、standalone CPython 3.11.5、Loopback-only；本任务没有重新连接该服务器。

| 字段 | Launcher request / Guard | `canary.json` 实际记录 | 结论 |
|---|---|---|---|
| requested/executed gates | `r2` | `requested_gates=[R2]`、`executed_gates=[R2]`、`gate_contract.matches=true` | `VERIFIED` |
| P5500 authorization | `authorize_p5500=true`、命令含 `--authorize-p5500` | `gates.R2.authorized=true` | `VERIFIED` |
| Route TPS | `80` | `config.route_tps=80.0` | `VERIFIED` |
| response delay | `2 ms`，Guard command 含 `--response-delay-ms 2` | 观察器 config 未单独持久化该字段 | 请求/命令 `VERIFIED`；effective config 字段 `EVIDENCE_GAP` |
| Workers / partitions | `4 / 64` | `config.workers=4`、`config.partitions=64` | `VERIFIED` |
| projection queue | `128` | `config.projection_queue=128` | `VERIFIED` |
| max inflight / prefetch | `32 / 16` | `config.max_inflight=32`、`config.prefetch=16` | `VERIFIED` |
| watchdog | `enabled` | `config.watchdog=enabled`，watchdog config `enabled=true` | `VERIFIED` |
| network | Loopback | `config.loopback_host=127.0.0.1`、`real_smsc=false` | Loopback-only `VERIFIED` |
| Python/filesystem | standalone CPython 3.11.5；`df -T` 为 `/dev/sda2 ext4` | `preflight.python`、`preflight.filesystem_df_T`、freeze、`pip check` | `VERIFIED` |

`diagnostics/phase-markers.jsonl` 还确认这是按 Campaign 顺序执行、每个 Campaign 完成 idle boundary 和 reconciliation 后才进入下一个 Campaign：`1201 -> 1202 -> 1203 -> 1204`。因此本次合同是顺序批量验收，不是四 Campaign 同时竞争的并行容量压测。

### 3.2 四 Campaign 对账

说明：`gates.R2.campaigns[*].summary.states.socket_written`、`accepted`、`loopback` 和 `runtime.pending_records` 有逐 Campaign 记录。`runtime.projection.applied` 是截至该 Campaign 的累计值，不是该 Campaign 的 unique/taken/written，因此不把 `2500/5000/5250/5500` 推断为逐 Campaign Projection 数量。`responses` 是 API ingress batch response，不是 SMPP `submit_sm_resp`，不能替代协议响应字段。

| Campaign ID | expected | ingress accepted | socket_written | Loopback received | submit_sm_resp_success | Projection unique/taken/written | pending | duplicate/missing/unexpected | 证据路径 |
|---:|---:|---:|---:|---:|---:|---|---:|---|---|
| 1201 | 2500 | 2500 | 2500 | 2500 | `EVIDENCE_GAP`（仅聚合 5500） | `EVIDENCE_GAP`（仅累计 projection.applied=2500） | 0 | `0 / 0 / 0` | `gates.R2.campaigns[0]` |
| 1202 | 2500 | 2500 | 2500 | 2500 | `EVIDENCE_GAP`（仅聚合 5500） | `EVIDENCE_GAP`（仅累计 projection.applied=5000） | 0 | `0 / 0 / 0` | `gates.R2.campaigns[1]` |
| 1203 | 250 | 250 | 250 | 250 | `EVIDENCE_GAP`（仅聚合 5500） | `EVIDENCE_GAP`（仅累计 projection.applied=5250） | 0 | `0 / 0 / 0` | `gates.R2.campaigns[2]` |
| 1204 | 250 | 250 | 250 | 250 | `EVIDENCE_GAP`（仅聚合 5500） | `EVIDENCE_GAP`（仅累计 projection.applied=5500） | 0 | `0 / 0 / 0` | `gates.R2.campaigns[3]` |
| **TOTAL** | **5500** | **5500** | **5500** | **5500** | **5500** | **5500 / 5500 / 5500** | **0** | **0 / 0 / 0** | `loopback`、`projection_database`、`progress` |

聚合字段的机器路径和值为：

```text
status                                      = PASS
gates.R2.status                             = PASS
gates.R2.expected / accepted                = 5500 / 5500
loopback.submit_sm_received                 = 5500
loopback.submit_sm_resp_success             = 5500
projection_database.unique_events/taken/
  socket_written                            = 5500 / 5500 / 5500
progress.unique_events                      = 5500
progress.taken_without_socket_written       = 0
projection_database.cancelled               = 0
projection_database.pending_records         = 0（在各 campaign runtime 中均为 0）
```

因此可以确认当前 R2 是机器证据支持的聚合 PASS；由于逐 Campaign Projection 和协议响应细分缺失，按 fail-closed 规则不升级为 `PASS_VERIFIED`。

### 3.3 Guard、清理与资源

| 项目 | 机器证据 | 结果 |
|---|---|---|
| Guard | `r2/guard/perf-guard.json` | `status=PASS`、`exit_code=0` |
| Guard duration | `duration_seconds` | `85.542 s`；canary 内部 duration `84.672 s` |
| RSS | `max_aggregate_rss_bytes` | `585,560,064 B`，低于 2 GiB |
| Run disk delta | `max_disk_delta_bytes` | `0 B`，低于 5 GiB |
| process cleanup | `termination.all_recorded_pids_gone`、`evidence/post-run-processes.txt` | `true`、`NONE` |
| post-run audit | `evidence/post-run-audit.txt` | `R2_POST_RUN_PROCESS_AUDIT=PASS` |
| native crash evidence | `r2/canary/diagnostics/native-crash/canary-root-faulthandler.log` | `0 B`；本轮无 SIGSEGV |

`5500 / 85.542 ~= 64.3 tasks/s` 仅是包含 setup/shutdown 的 Guard 有效观测比例，不是独立 SMSC TPS SLA；`route_tps=80` 是 Loopback 限速设置，不代表真实 SMSC 端到端容量。

## 4. E2：历史失败、当前 PASS 与归档边界

### 4.1 独立时间线

| 阶段 | 事实 | 当前解释 |
|---|---|---|
| 历史 EulerOS D7/R2/P5500 | `5500` 合同；Guard `-11`，`34.361 s`；RSS `559,325,184 B`；磁盘增量 `0 B`；无 `canary.json`、core、native frame | `HISTORICAL_D7_R2_NATIVE_FAILURE=PRESERVED`；不是业务对账 PASS |
| 当前代码稳定化 | 显式 `spawn`、child-local initialization、Journal durable replay、Worker control、watchdog false-stall 分离、idle lightweight snapshot、增量 reconciliation | 静态/专项证据支持这些变化；不等于函数级 Native RCA 已证明 |
| 本轮首个 30 秒观察器失败 | 首个 Campaign 完成后，`urllib` 30 秒超时触发 `TimeoutError` | 是测试工具长轮询缺陷，不写成业务丢失或 SIGSEGV；本任务冻结归档没有该次完整机器 JSON，精确原始归档 `NOT_LOCALLY_AVAILABLE` |
| 300 秒观察器修复后 R2 | 新 Run、EulerOS、standalone CPython 3.11.5、Loopback；`5500/5500` 聚合 PASS，Guard `85.542 s`、exit `0` | 当前合成 Loopback 的强正向恢复证据；仍有逐 Campaign 字段 gap |
| 历史 EulerOS R7 | 独立 B2/R7 归档中的 R7 target/sent `4000/4000`、Loopback/response `4000/4000`、Projection `4000/4000/4000`、writer lag `0` | 单独的 4000 长稳 PASS，不是 5500 容量，也不是同一连续 Run |

### 4.2 Native RCA 边界

```text
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
```

当前 300 秒超时修复证明观察器可以覆盖该长轮询窗口，不能证明 DuckDB、C/C++、SMPP 或退出生命周期是历史 `SIGSEGV` 的已解决根因。历史 D7 仍以 `-11` 结束，且没有 native faulting frame；不能写 `RCA_PROVED`，也不能把 R7 或当前 R2 PASS 覆盖历史失败。

旧报告中的 `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`、`INTEGRATED_NO_GO` 和历史失败字段不改写。本报告只给出当前证据范围内的新分层决定。

## 5. E3：当前分层验收决定

| Scope | source baseline | evidence run | evidence grade | current verdict | unresolved risk | deployment authorization |
|---|---|---|---|---|---|---|
| 合成 Loopback R2/P5500 聚合合同 | 目标基线 `01409126...`；实际包 metadata 非 exact | `vnext-targeted-native-r2-20261017-retest300` | 外层/内部 hash verified；aggregate machine counters verified；逐 Campaign Projection/协议响应缺字段 | `CURRENT_VNEXT_LOOPBACK_CANARY=PASS_REPORTED_EVIDENCE_GAP` | per-Campaign unique/taken/written 与 submit response breakdown 未记录；response delay effective field 未落盘 | 不授权真实 SMSC/生产 |
| Native 历史稳定性与 RCA | 历史 D7 受测包；报告与 D7 archive | `vnext-native-sigsegv-rca-20261016-d7-r2-p5500` | Guard `-11`、internal 7/7 verified；frame/core unavailable | `RCA_STRONGLY_INDICATED` | faulting instruction、core、gdb/coredumpctl unavailable | 不授权生产 |
| EulerOS R7 对照 | 历史 B2/R7 evidence | `results/d6-r7-canary` | R7 机器 JSON、Guard 与 internal 670/670 verified | `LATEST_EULEROS_R7=PASS` | 仅 4000 合同；EulerOS 是 SLES 兼容代理，不是原生 SLES12SP5 实机 | 不授权生产 |
| 发布候选文档状态 | 当前 `main=01409126...` | 当前包 hash/release metadata | package identity verified；source provenance exact=no | `RELEASE_CANDIDATE_DOCUMENTED` | clean rebuild、精确 release SHA、SLES 实机和真实 SMSC 未完成 | `PRODUCTION_APPROVAL=NOT_GRANTED` |

## 6. E4：真正发布前的最小准备清单

### Must-have before real SMSC

| 优先级 | 项目 | 完成标准 |
|---|---|---|
| P0 | Clean source/package traceability | 从 clean `01409126...` checkout 重建正式应用包；`release.json.git_sha` 精确等于 clean source；独立测试包和 standalone runtime 分开；重新核验 wheelhouse、包内 manifest 和外层 SHA256 |
| P0 | 原生 SLES12SP5 环境 | 单独在允许的 SLES12SP5 实机核验 CPython 3.11.5、glibc/`manylinux2014` ABI、`_ctypes`、ssl/OpenSSL、offline venv、`pip check` 和启动；EulerOS 只能作为兼容代理证据 |
| P0 | 真实 SMSC 专项授权 | 单独确认地址、认证、Bind Transceiver、window、TPS、重连、submit_sm_resp 错误、timeout、编码、长短信 PDU、网络策略和允许的测试号码；本任务不自动连接 |
| P0 | 运行生命周期与恢复 | 评审 `MC_PIPELINE_MODE=vnext`、受控 legacy fallback、暂停/取消、Spool/Journal/Projection 持久性、Worker/Writer 崩溃恢复、正确停机、双 owner fence、SMSC 抖动/链路错误/磁盘不足停止规则 |
| P0 | 生产资源与数据策略 | 按真实任务规模评估 CPU/RSS/磁盘、保留空间和清理策略；测试环境的 max 4 CPU、2 GiB RSS、5 GiB Run delta 只属于共享测试条件，不写成生产 SLA |
| P1 | 现场可观测性与责任人 | 确认发送阶段、协议响应、Projection、DLR/终端送达的独立指标和 owner；不得把 `socket_written` 写成终端送达 |
| P0 | 发布批准边界 | 只进入受控现场评审/小流量真实 SMSC 专项，不自动声明生产上线通过；真实数据保护和现场变更批准由用户/现场责任方另行决定 |

### Not required for current Loopback acceptance

- 不重复 R2/P5500、R7，也不为了补齐报告而连接服务器。
- 不要求本任务证明真实 SMSC、真实号码、DLR、终端收件、生产数据库或生产长期运行。
- 不要求 52K/102K、严格独立吞吐 SLA、原生 Beeline 或新的大架构重构。
- 不要求在没有新 Native frame 的情况下把 `RCA_STRONGLY_INDICATED` 升级为 `RCA_PROVED`。
- 不要求把历史 `INTEGRATED_NO_GO` 改写为 VNext PASS。

## 7. E5：交付物与本任务验收

本报告与证据字段映射报告已创建；机器可读 receipt 位于隔离的 untracked 证据目录，不是 Git 部署产物：

```text
Docs/Marketing_Copilot_VNext_Runtime_Canary_Final_Acceptance_and_Release_Readiness_Report.md
Docs/Marketing_Copilot_VNext_Final_Acceptance_Evidence_Reconciliation.md
test-artifacts/vnext-final-acceptance-20261017/acceptance-receipt.json
```

检查结果：

| 检查项 | 结果 |
|---|---|
| 当前 `main` SHA / worktree audit | `01409126...` 与 `origin/main` 一致；无关 dirty/untracked 未触碰 |
| R2 四 Campaign 对账 | accepted/socket/Loopback/pending/duplicate/missing/unexpected 已逐项核验；逐 Campaign Projection 与 protocol response 保留 gap |
| Guard、85.542 s、RSS、磁盘、清理 | 本地精简机器证据已核验 |
| 顺序 Campaign 合同 | phase markers 确认 `1201 -> 1202 -> 1203 -> 1204`，每项经过 idle/reconciliation |
| R7 与历史 D7 | 分别指向准确历史 archive/run；`SIGSEGV` 不改写 |
| 三个范围独立 | Loopback、Native RCA、生产批准分别决定 |
| 新服务器/真实 SMSC/正式部署 | 本任务未执行 |
| `git diff --check` | 生成报告后执行并记录在最终回复 |
| commit/push | 本任务未执行，等待用户明确授权 |
