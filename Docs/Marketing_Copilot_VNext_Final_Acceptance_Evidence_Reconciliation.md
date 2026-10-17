# Marketing Copilot VNext 最终验收证据对账

任务 ID：`MC-VNEXT-FINAL-CANARY-ACCEPTANCE-RELEASE-READINESS-20261017`

本文件是机器证据索引，不覆盖历史报告，不补造缺失字段。所有路径均为当前工作区内本地归档路径，归档内的 JSON path 使用归档成员相对路径。

## 1. Source / package identity ledger

| 证据文件 | 实际 SHA256 | 内部核验 | JSON/manifest 关键字段 | 证据强度 |
|---|---|---|---|---|
| `test-artifacts/vnext-targeted-native-20261017-r3f7c2/transfer/marketing-copilot-sles12sp5-system-python-20261017-vnext-euleros-targeted.tar.gz` | `bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44` | `checksums.sha256 179/179`；unsafe/link `0/0` | `release.json.git_sha=8f14e82d-dirty`；`python_version=3.11.5` | `TESTED_PACKAGE_IDENTITY_VERIFIED`; provenance `MISMATCH` vs current HEAD |
| `test-artifacts/vnext-targeted-native-20261017-r3f7c2/transfer/sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz` | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | archive path/link audit `0/0` | standalone CPython 3.11.5 runtime | `VERIFIED`; no Git source provenance field |
| `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/marketing-copilot-vnext-runtime-canary-test-20261017-vnext-euleros-targeted-r2-300s.tar.gz` | `f1d59bf5e6817c8915a754eca53d73a4cb8c8cbce175f666a8a191d35d5f6e9a` | `checksums.sha256 19/19`；unsafe/link `0/0` | `bundle.json.source_commit=337f3e623431a37705ee12041186dd0fe6b692d2`；`formal_deployment_package_modified=false`；`r2_authorized=false` 是包默认策略 | identity `VERIFIED`; provenance `MISMATCH` vs current HEAD |
| `test-artifacts/vnext-targeted-native-20261017-r3f7c2-retest300/transfer/r2-evidence-sanitized-300s.tar.gz` | `39e918deb75f4b5659d20976c6f17816e0299389c3fb36168e7c8bec52521013` | `checksums.sha256 19/19`；unsafe/link `0/0` | `evidence/test-bundle.json` 记录三包 hash、EulerOS/SLES-compatible target、`network_access=false`、`runtime_data_included=false` | `VERIFIED` sanitized evidence |
| `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz` | `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e` | `d7-evidence-final.sha256 7/7`；unsafe/link `0/0` | `guard/perf-guard.json` 记录历史 D7 `-11` | `VERIFIED` historical failure |
| `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz` | `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd` | `results/B2-files.sha256 670/670`；unsafe/link `0/0` | `results/B2-finalization.txt`、`results/d6-r7-canary/canary.json` | `VERIFIED` historical raw archive；成员含 runtime data，不能当 sanitized/package |

当前正式应用包的 tar 成员为 195，`backend/tests` 和 `backend/tools` 均为 0；当前测试包的 tar 成员为 26，其中 tests/tool 成员分别为 5/7，且没有 DB/WAL、Spool segment、Journal JSONL、logs/uploads/backups 等 runtime data 文件名。正式包内的 `offline/extensions/.../core_functions.duckdb_extension` 是静态离线依赖；源码模块名 `spool.py`/`result_journal_service.py` 不属于运行数据。

## 2. R2 machine JSON field map

证据归档：`r2-evidence-sanitized-300s.tar.gz`，以下 JSON path 均相对于其根目录 `evidence-export/`。

### 2.1 Gate/config/guard

| JSON path | 实际值 | 结论 |
|---|---|---|
| `r2/canary.json.status` | `PASS` | 机器结果 PASS |
| `r2/canary.json.requested_gates` | `R2` | 请求门禁 |
| `r2/canary.json.executed_gates` | `R2` | 实际门禁 |
| `r2/canary.json.gate_contract.matches` | `true` | 请求/执行一致 |
| `r2/canary.json.gate_contract.authorize_p5500` | `true` | 5500 授权已记录 |
| `r2/canary-launcher-request.json` | `r2`、authorize `true`、TPS `80`、delay `2`、workers `4`、partitions `64`、queue `128`、inflight `32`、prefetch `16`、watchdog `enabled` | Launcher request |
| `r2/guard/perf-guard.json.command` | 含 `--gates r2 --authorize-p5500 --route-tps 80 --response-delay-ms 2 --workers 4 --partitions 64 --projection-queue 128 --max-inflight 32 --prefetch 16 --watchdog enabled` | Guard command |
| `r2/canary.json.config` | `route_tps=80.0`、`workers=4`、`partitions=64`、`projection_queue=128`、`max_inflight=32`、`prefetch=16`、`watchdog=enabled`、`loopback_host=127.0.0.1`、`real_smsc=false` | Effective fields |
| `r2/canary.json.config.response_delay_ms` | 字段不存在 | `EVIDENCE_GAP`；只能由 launcher/guard 命令确认请求值 2 ms |
| `r2/guard/perf-guard.json.limits` | timeout `420 s`、RSS `2048 MB`、disk `5 GB`、CPU `[0,1,2,3]`、nice `10`、ionice class `3` | 共享服务器资源边界 |
| `r2/guard/perf-guard.json.status/exit_code` | `PASS / 0` | Guard PASS |
| `r2/guard/perf-guard.json.duration_seconds` | `85.542` | Guard elapsed |
| `r2/guard/perf-guard.json.max_aggregate_rss_bytes` | `585560064` | RSS peak |
| `r2/guard/perf-guard.json.max_disk_delta_bytes` | `0` | Run disk delta |
| `r2/guard/perf-guard.json.termination.all_recorded_pids_gone` | `true` | 受管进程已退出 |
| `evidence/post-run-audit.txt` | `R2_POST_RUN_PROCESS_AUDIT=PASS` | 清理 PASS |

### 2.2 Aggregate counters

| JSON path | 值 | 证据强度 |
|---|---:|---|
| `r2/canary.json.gates.R2.expected` | 5500 | machine JSON |
| `r2/canary.json.gates.R2.accepted` | 5500 | machine JSON |
| `r2/canary.json.loopback.submit_sm_received` | 5500 | protocol-boundary aggregate |
| `r2/canary.json.loopback.submit_sm_resp_success` | 5500 | protocol response aggregate |
| `r2/canary.json.projection_database.unique_events` | 5500 | projection DB aggregate |
| `r2/canary.json.projection_database.taken` | 5500 | projection DB aggregate |
| `r2/canary.json.projection_database.socket_written` | 5500 | projection DB aggregate |
| `r2/canary.json.progress.unique_events` | 5500 | journal reconciliation aggregate |
| `r2/canary.json.progress.taken_without_socket_written` | 0 | explicit guard field |
| `r2/canary.json.projection_database.cancelled` | 0 | explicit projection field |
| each `gates.R2.campaigns[*].runtime.pending_records` | 0 | four campaign records |
| `r2/canary/diagnostics/native-crash/canary-root-faulthandler.log` | 0 bytes | no native crash log content |

### 2.3 Per-Campaign field map

| Campaign | JSON path | expected | accepted | summary `socket_written` | Loopback expected/received | dup/missing/unexpected | per-Campaign protocol response | per-Campaign Projection |
|---:|---|---:|---:|---:|---:|---|---|---|
| 1201 | `gates.R2.campaigns[0]` | 2500 | 2500 | 2500 | 2500/2500 | 0/0/0 | `EVIDENCE_GAP`; only aggregate field exists | `EVIDENCE_GAP`; `runtime.projection.applied=2500` is cumulative |
| 1202 | `gates.R2.campaigns[1]` | 2500 | 2500 | 2500 | 2500/2500 | 0/0/0 | `EVIDENCE_GAP`; only aggregate field exists | `EVIDENCE_GAP`; `runtime.projection.applied=5000` is cumulative |
| 1203 | `gates.R2.campaigns[2]` | 250 | 250 | 250 | 250/250 | 0/0/0 | `EVIDENCE_GAP`; only aggregate field exists | `EVIDENCE_GAP`; `runtime.projection.applied=5250` is cumulative |
| 1204 | `gates.R2.campaigns[3]` | 250 | 250 | 250 | 250/250 | 0/0/0 | `EVIDENCE_GAP`; only aggregate field exists | `EVIDENCE_GAP`; `runtime.projection.applied=5500` is cumulative |

`gates.R2.campaigns[*].responses[*]` 的 `accepted` 和 `success=true` 是 API ingress batch response；它们分别为 1201/1202 各 10 个 batch、1203/1204 各 1 个 batch，不能命名为 `submit_sm_resp_success`。

### 2.4 Sequential contract

`r2/canary/diagnostics/phase-markers.jsonl` 的关键顺序为：

```text
1201: gate_start -> ingress_complete(2500) -> idle_boundary_reached
     -> projection_reconcile_begin/end -> reconciliation_complete(2500)
1202: gate_start -> ingress_complete(2500) -> idle_boundary_reached
     -> projection_reconcile_begin/end -> reconciliation_complete(2500)
1203: gate_start -> ingress_complete(250)   -> idle_boundary_reached
     -> projection_reconcile_begin/end -> reconciliation_complete(250)
1204: gate_start -> ingress_complete(250)   -> idle_boundary_reached
     -> projection_reconcile_begin/end -> reconciliation_complete(250)
```

这证明本次是单 Campaign 依次提交、每 Campaign 批量 250 并等待 Idle 的受控合同。

## 3. Historical R7 and D7 field map

### R7

本地 archive：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`。

```text
JSON: results/d6-r7-canary/canary.json
R7 target/sent                    = 4000/4000
R7 duration_seconds               = 611.721
R7 loopback received/response     = 4000/4000
R7 projection unique/taken/write = 4000/4000/4000
R7 max_writer_lag_seconds         = 0.0
Guard combined R6/R7 status/exit  = PASS/0
Guard max RSS/disk delta          = 582787072 B / 0 B
Guard duration_seconds             = 620.143
```

这是历史独立 4000 长稳对照，不与当前 5500 R2 合并成一个容量结论。该历史 archive 的内部 `results/B2-files.sha256` 通过 670/670，但归档是 raw evidence，含运行数据文件名；不把它复制到正式发布包。

### D7/R2/P5500

本地 archive：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`。

```text
JSON: guard/perf-guard.json
status/reason                  = FAIL / guarded command exited with -11
exit_code                      = -11
duration_seconds               = 34.361
max_aggregate_rss_bytes        = 559325184
max_disk_delta_bytes           = 0
canary.json                    = absent
core-files.txt                 = empty
native faulting frame          = unavailable
post-cleanup PID audit         = PASS; recorded orphan PIDs absent
```

该失败事实与当前 R2 PASS 并列保留，不能通过 300 秒测试工具修复改写成“Native 根因已解决”。

## 4. Evidence gaps / unavailable items

1. 正式包 `release.json` 没有当前 clean `01409126...` provenance；测试包 `bundle.json` 也不是当前 HEAD。需要 clean rebuild 后才能把 `SOURCE_PROVENANCE_EXACT` 置为 `YES`。
2. 当前 R2 JSON 没有逐 Campaign `Projection unique/taken/socket_written`；累计 `runtime.projection.applied` 不可替代。
3. 当前 R2 JSON 没有逐 Campaign `submit_sm_resp_success`；只有 Loopback aggregate `5500`。API batch `responses[*].success` 不可替代。
4. 当前 R2 `canary.config` 未单独保存 `response_delay_ms`；该值只在 launcher request 和 Guard command 中可核验。
5. 首次 30 秒 HTTP timeout 的完整机器 JSON/原始 stdout 不在本任务冻结的本地归档中，标记 `NOT_LOCALLY_AVAILABLE`；只引用已提交报告的事实描述。
6. 历史 D7 没有 core、C/C++ faulting frame、gdb/coredumpctl 记录，故 `NATIVE_FAULTING_FRAME=UNAVAILABLE`、`RCA_PROVED=UNAVAILABLE`。
7. EulerOS 2.0 SP15 是 SLES12SP5-compatible proxy；本任务没有原生 SLES12SP5 实机或真实 SMSC 证据。

## 5. Current decision receipt

```text
CURRENT_VNEXT_LOOPBACK_CANARY=PASS_REPORTED_EVIDENCE_GAP
LATEST_EULEROS_R2_P5500=PASS
LATEST_EULEROS_R7=PASS
HISTORICAL_D7_R2_NATIVE_FAILURE=PRESERVED
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
RELEASE_CANDIDATE_DOCUMENTED
PRODUCTION_APPROVAL=NOT_GRANTED
INTEGRATED_STATUS=INTEGRATED_NO_GO
```

本文件与最终验收报告只提交文档变化；本任务未执行 commit/push。
