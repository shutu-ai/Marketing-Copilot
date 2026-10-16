# Marketing Copilot VNext 静态审计与定向 Native Crash RCA 报告

## 1. 范围、基线与结论边界

本报告执行 `Docs/Marketing_Copilot_VNext_Static_Code_Audit_and_Targeted_Native_RCA_Codex_Task.md` 的 S0–S5。代码基线为当前 GitHub `main`：`2a8d1778b4f86bdaf75696aa1540bad45e5881e2`（`fix: stabilize VNext runtime diagnostics and canary closure`）。本轮不访问 EulerOS 服务器，不重新执行 R2/P5500，也没有执行 commit/push；工作树中与本任务无关的既有修改保持不动。

结论采用 fail-closed：本地定向审计和修复可以 PASS，但不能覆盖历史 EulerOS D7/R2/P5500 的 `SIGSEGV (-11)`。历史归档没有 core 或 faulting native frame，因此不能把 RCA 升级为 `RCA_PROVED`。

历史证据已只读复核：

- D7/R2/P5500 合同为 `2500 + 2500 + 250 + 250 = 5500`；运行 `34.361 s` 后以 `-11` 退出，峰值 aggregate RSS `559325184 B`，磁盘增量 `0 B`，没有 `canary.json`、core 或 native faulting frame。
- D7 归档 `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz` 外层 SHA256 为 `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e1`；内部 `d7-evidence-final.sha256` 为 `7/7`。
- EulerOS B2/R7 归档 `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz` 外层 SHA256 为 `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`；内部 `results/B2-files.sha256` 为 `670/670`，无 unsafe path 或链接。B2/R7 是 `4000` 合同，不能证明 `5500` R2。

## 2. S1 静态调用图与 owner 审计

```text
main startup
  ├─ init_db / root DuckDB owner
  ├─ start_stall_watchdog
  └─ _start_background_services
       └─ VNext service
            └─ VNextRuntime.start
                 ├─ ProjectionWriterProcess (独立 projection DuckDB，spawn)
                 └─ WorkerSupervisor (spawn)
                      └─ _runtime_worker_entry
                           ├─ child TaskSpool / lease / journal
                           └─ async SMPP transport

GET /api/vnext/status ──> detailed VNextRuntime.status
GET /api/vnext/idle   ──> wait_for_idle
                             └─ 50 ms lightweight snapshot loop
                                  ├─ TaskSpool.lightweight_snapshot
                                  ├─ worker progress
                                  └─ projection snapshot
```

| Owner / boundary | 当前事实 | 审计结论 |
|---|---|---|
| 主 DB | `backend/database.py:1318-1362` 的 `_ROOT_CONNECTION` 由 `_root_connection()` 建立，`get_connection()` 返回其 cursor wrapper | 单 owner + lock 是高风险边界，未据此断言根因 |
| Projection DB | `backend/vnext/projection_writer.py:118` 建立独立 DuckDB，进程入口在 `:314-431` 重放/增量读取 journal | 与主 DB 物理分离；journal offset 是恢复边界 |
| Runtime worker | `backend/vnext/runtime.py:24-137` 在 child entry 创建 child spool、lease、journal 与 SMPP worker | 仍保留 worker、inflight、window、journal 语义 |
| API/main | canary 中 API 与 root observer 是同一 PID 的不同 role metadata；不是伪造独立 API 进程 | 角色事实被显式记录 |
| resource tracker | `backend/tools/vnext_runtime_canary_acceptance.py:356-375` 只观察 PID，标记 `observed_only`，不声称 native capture | 避免把 tracker 误报成可捕获 native frame |
| legacy scheduler | `services/activity_scheduler.py` 仍拥有 batch/realtime/SFTP 线程；VNext 启动关系只作为高风险候选 | 未删除业务生产链，未宣称它是 SIGSEGV 根因 |

### 时间线

| 时间边界 | 事实 | 可证明范围 |
|---|---|---|
| 历史 D7 `T+34.361 s` | 5500 合同的 guarded canary 以 `-11` 退出 | 证明命令级 native abnormal exit；没有证明 faulting function |
| 本地 F01 `T+0` | root faulthandler、环境 marker、watchdog config 建立 | 证明诊断文件早于 seed/API 生命周期 |
| 本地 F01 `T+~2 s` | `api_started` marker；API/main 与 runtime child role metadata 已出现 | 证明 API、runtime parent、projection、worker 的角色边界 |
| 本地 F01 `T+30 s` | enabled run 产生 scheduled stack snapshot | 证明 scheduler snapshot 与 stall signal 分离；不等于 no-progress stall |
| 本地 F01 `T+35.392 s` | idle/reconciliation 完成，300/300/300 对账，API graceful stop | 证明本地 bounded path 的行为收敛 |
| 本地 F02 `10/100/1000 queries` | full recovery work 随 query 次数放大，lightweight 保持 metadata-only | 证明 F02 观察成本变化，不外推业务吞吐 |

历史 D7 在 crash 前没有本轮新增 phase marker，因此不能把 `34.361 s` 映射到某个精确函数；本地 marker 只改善未来复现的定位能力。

## 3. F01–F07 静态发现、修复与风险判定

| ID | 静态发现 | 本轮处理 | 证据等级 |
|---|---|---|---|
| F01 P0 | `database.py` 原先把 30 s `dump_traceback_later` 与运行年龄/锁年龄检查混在一起，容易把长时间运行误报为 stall | `:337-468` 增加 `MC_STALL_WATCHDOG_ENABLED` 开关；scheduled snapshot 与 `no_progress_stall` 分开；只有持有锁且无进展超过阈值才触发人工 dump；启动/停止均可回收 | 静态确认 + 单测 + 本地 enabled/disabled A/B |
| F02 P0 | `/api/vnext/idle` 每 50 ms 调完整 `status()`；原 `status()` 新建 `TaskSpool` 并恢复索引 | `runtime.py:379-453` 复用长生命周期 spool；新增 `_idle_snapshot()`；`spool.py:366-455` 只读 manifest、cursor、taken 元数据，不扫描 segment JSON/CRC；`/status` 仍保留详细 full status | 静态确认 + 1000-query ext4 A/B |
| F03 P1 | loopback reconciliation 在 expected 集合上反复 `actual.count()`，为 O(n²)；campaign summary 重复读取 journal | `CanaryRunner` 使用增量 `Counter`、received index、`ProgressJournal.read_from()` offset/cache；保留 missing/duplicate/unexpected，并以 additive 字段输出 `unexpected_count` | 静态确认 + 单测 + 真实 canary 对账 |
| F04 P0 | 真实 canary 入口原先没有持久化的 root/child native capture 与阶段标记 | `diagnostics.py:38-171` 增加独立 faulthandler 文件、模块版本、role/PID metadata；root、worker、projection writer 在最早可行入口启用；runner `:324-375` 写 monotonic phase markers；不记录 MSISDN、正文或密码 | 静态确认 + 真实 canary diagnostics |
| F05 P1 | 主 DB owner/cursor/scheduler 与独立 projection DB 并存，存在 native/线程/锁边界风险 | 只做 owner/生命周期审计，不改业务链，不把候选写成根因 | 高风险候选，未证明 |
| F06 P1 | 历史 D7 在约 34 s 边界退出，但归档没有 faulting frame，不能定位到 DuckDB、Spool、SMPP 或 shutdown | runner 增加 root/API/idle/phase 的 monotonic markers；报告只建立时间线，不反推精确函数 | 高风险候选，未证明 |
| F07 P2 | D7/R2 的 `5500` 合同与 B2/R7 的 `4000` 合同不等负载 | 明确分层报告；不使用 R7 PASS 覆盖 R2 FAIL | 事实确认 |

本轮没有引入 Kafka、Redis、RabbitMQ，也没有建立第二条业务发送链；发送、spool、journal、projection、恢复语义保持原有边界。runner 的 loopback HTTP 请求使用显式无代理 opener，仅影响测试观察器，避免企业代理劫持 `127.0.0.1`。

## 4. RCA 证据与不越界结论

当前最强证据链是：

1. 历史真实 EulerOS `main + FastAPI + Uvicorn + VNextRuntime` R2 在约 35 s 内以 `SIGSEGV (-11)` 结束。
2. 修复后的授权 D7 仍在显式 `spawn` 下以 `-11` 结束，因此“默认 fork 单一根因”被排除为充分解释。
3. 本地 native ext4 的最小 `fork/spawn` 路径均可通过；本轮真实本地 canary 也通过，说明 bounded path 稳定，但不能推出 EulerOS 的 C/C++ faulting instruction。
4. 新增 faulthandler 能保存 Python thread stack 和 native-backed module versions；它不是 debugger，未发生 SIGSEGV 时不会产生 faulting frame。

因此：`fork` 仍是历史高优先级风险假设，但不是已证实的唯一根因；更宽的“已初始化 native/线程/DB/API/SMPP 生命周期与子进程/退出边界”仍是 `RCA_STRONGLY_INDICATED`。本轮没有 native frame，故不写 `RCA_PROVED`。

## 5. 代码变更、回滚与兼容性

任务相关代码变更集中于：

- `backend/database.py`：watchdog 生命周期与 false-stall 判定。
- `backend/main.py`：传递 watchdog enable/disable 环境开关。
- `backend/vnext/diagnostics.py`：native capture 与进程元数据。
- `backend/vnext/runtime.py`、`backend/vnext/spool.py`：长生命周期 owner 与 lightweight idle snapshot。
- `backend/vnext/projection_writer.py`：projection child capture。
- `backend/tools/vnext_runtime_canary_acceptance.py`：phase/PID/observer 诊断、增量对账、loopback 直连 opener、watchdog 参数。
- `backend/tests/test_vnext_static_audit.py`、`backend/tools/vnext_static_audit_ab.py`：测试与 synthetic A/B，仅用于验证，不进入正式包。

风险与回滚点：

- F01：低风险；回滚只需恢复 watchdog 判定与启动开关，业务 DML/发送不变。
- F02：中低风险；`lightweight_snapshot()` 仅用于 idle polling，详细 `/status` 仍走 full refresh；若出现快照不一致，可关闭新 idle 路径并保留 full status。
- F03/F04：低风险；只改 observer/diagnostics，不改变 SMPP payload 或业务状态迁移。
- 新增的 `backend/tools/vnext_static_audit_ab.py` 与测试文件由部署脚本的 `--exclude='tests' --exclude='tools'` 排除。

## 6. S5 验证

### Windows 回归

- 定向测试：`16 passed`。
- 全量 `backend/tests` 首次为 `389 passed, 4 skipped, 1 failed`，唯一失败是既有 route-credit 时间边界 `0.132 s < 0.15 s`；同一测试连续 5 次单独重跑全部通过。
- 全量重跑：`390 passed, 4 skipped, 4 warnings`，耗时 `171.66 s`。警告均为 FastAPI `on_event` deprecation，不是本轮失败。

### 原生 ext4 定向 A/B

正式定量结果来自 WSL `AlmaLinux-8` 原生 ext4，不使用 `/mnt/d`：

- `df -T .`：`/dev/sdd ext4`。
- Python：CPython `3.11.13`。
- 依赖：离线 SLES12SP5/manylinux2014 wheelhouse，DuckDB `1.2.0`、FastAPI `0.115.0`、Uvicorn `0.30.6`、SMPPlib `2.2.4`。
- F02 synthetic A/B：`records=1000`、`partitions=16`、queries `10/100/1000`，结果 `PASS`。1000 query 下 full path `42.248379 s`、`16000` segment scans、`1001000` JSON loads、`349847000 B` segment bytes；lightweight path `0.413651 s`、`16` scans、`2001` JSON loads、`349847 B`，pending/total 均为 `1000`。
- F01 真实 canary enabled/disabled 对照：参数为 `R7`、stability `32 s`、1 worker、16 partitions、loopback-only、route TPS `80`、响应延迟 `0 ms`；两次均 PASS，均发送 `300`，projection `unique/taken/socket_written = 300/300/300`。

### 正式应用包边界

从当前代码工作树构建的 slim application package 通过内部 checksums：

- `180` 个文件，`33` 个离线 wheel。
- `backend/release.json`、`frontend/dist/index.html`、`offline/wheels/`、`run.sh`、`checksums.sha256` 均存在。
- `backend/tests`、`backend/tools`、nested `backend/backend`、DuckDB/WAL/DB/log/runtime data 均为 `0`。
- 外层包 SHA256：`4bf4edcd51a831d77dfcbab38802ec96f44aef3a6c4dd95efdc87bc9faef7f94`。
- 没有升级正式 standalone Python runtime 版本，也没有把测试包合入正式部署包。

EulerOS R2/P5500/R7 本轮不重跑；只核验已有归档。没有新的服务器资源、VPN 或外部服务操作。

## 7. 最终状态

```text
STATIC_AUDIT_STATUS=PASS
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
WATCHDOG_FALSE_STALL=FIXED
IDLE_HOTPATH_AMP=FIXED
RUNNER_OBSERVER_OVERHEAD=FIXED
LOCAL_TARGETED_AB=PASS
EULEROS_R2_P5500=NOT_RUN_UNAUTHORIZED
VNEXT_RUNTIME_CANARY_CLOSURE=FAIL
INTEGRATED_STATUS=INTEGRATED_NO_GO
```

`VNEXT_RUNTIME_CANARY_CLOSURE=FAIL` 与 `INTEGRATED_NO_GO` 是对历史 D7/R2/P5500 事实的保留，不被本地 PASS、B2/R7 PASS 或 observer 修复覆盖。
