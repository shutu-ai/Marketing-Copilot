# Marketing-Copilot VNext Linux Throughput and CLI Paired Acceptance

任务：`MC-VNEXT-P1-LINUX-THROUGHPUT-CLI-PILOT-20261019`

执行日期：2026-10-19

## 结论摘要

本轮已经在授权测试服务器上完成正式测试，不使用 Windows 或 WSL 结果替代 SLES 结果。

- 目标系统：SUSE Linux Enterprise Server 12 SP5，原生 `/dev/sda1` ext4。
- 运行时：独立 CPython 3.11.5，安装于 `/home/paas/marketing-copilot-test/python`，未修改 `/usr/bin/python3`、PATH、systemd 或生产服务。
- SMSC：仅使用 `127.0.0.1` synthetic loopback，未连接真实 SMSC、手机号或生产数据库。
- L1 1K：PASS。
- L2 5K legacy/decoupled 配对：四轮端到端对账 PASS；发送侧测量值显示 decoupled 高于 legacy 17.965%，但实际负载受 route credit/协议配置限制，不能据此宣称已验证 800 PDU/s 上限，因此总门禁为 `INCONCLUSIVE_PROTOCOL_LIMITED`。
- L3 CLI OFF/ON：四轮端到端对账 PASS；观测到的配对差异为 +4.004%，高于 3% 建议噪声门槛且 CLI 运行期间缺少 live worker status，故不把它判为无开销 PASS。
- L4 writer 延迟 600 ms：PASS，Reader 在 14.262 ms 内观察到全部 50 个响应，远低于 450 ms 判定阈值。
- L5 Journal 临时容量不足、回收和重试：PASS。
- L6 Campaign A pause/resume/cancel、Campaign B 继续：PASS。
- 600 s steady：PASS，实际 steady 600.997 s，150,000 task/PDU 对账完成，projection lag 与 Journal backlog 归零。
- 限定 Pilot：`CONDITIONAL`，仅支持申请受控低风险试运行；不构成生产发送授权。
- Full release：`NO_GO`。原因是真实 SMSC 未验证、5K paired 受协议/credit 限制、CLI ON/OFF 的统计差异超过建议 3% 且 live worker status 接口尚未接入完整 harness。

## 为什么必须在测试服务器上测试

任务要求所有吞吐、延迟、并发、长稳、资源、Journal/WAL、恢复和审计结果来自目标 Linux 原生文件系统。测试服务器本身是 SLES 12 SP5，测试目录位于 `/home` 对应的 ext4；因此本报告的 SLES 门禁来自目标服务器原生 ext4，而不是 `/mnt/<drive>` 或 Windows 文件系统。

服务器侧测试根目录为：

```text
/home/paas/marketing-copilot-test/runs/p1-sles-20261019-standalone
```

服务器侧证据文件：

```text
environment.txt
final-environment.txt
dependency-freeze.txt
runtime.tar.gz
runs/<case>/config.txt
runs/<case>/result.json
runs/<case>/console.log
```

没有把这些运行数据、数据库、Journal、日志或上传文件复制进仓库交付物。

## Phase 0：环境和来源

### 来源

```text
SOURCE_HEAD=122f47d42b5fbcc2ca871d887286ac93ce665e4d
```

工作树原有 dirty/untracked 文件均保留；没有 commit 或 push。当前任务相关代码变化为：

| 文件 | 作用 |
| --- | --- |
| `backend/tools/vnext_stats.py` | 增加 Socket write、Journal durable、response observed 三类独立速率，以及 worker/run identity、stale/reset/N/A 原因。 |
| `backend/tests/test_vnext_stats_cli.py` | 覆盖 counter reset、双速率、缺失 transport counter 和 watch/JSON 行为。 |
| `backend/tools/vnext_streaming_acceptance.py` | 仅修复验收完成边界的 stale lightweight spool 快照，并保留每个关闭 transport 的发送证据；不改发送、checkpoint、Journal 或 projection 语义。 |
| `Docs/Marketing_Copilot_VNext_Pilot_CLI_Statistics_Guide.md` | 更新双速率和 N/A/reset 说明。 |

服务器当前源码文件 SHA-256：

```text
backend/tools/vnext_streaming_acceptance.py  483217b5ee5e77a2b978aa67642f0797a7d190e609361607dc25ab103db4a3e4
backend/tools/vnext_stats.py                 b736002966df652ae22837766585a7330b3a530ba001e684487748e7fc82f376
```

### 服务器环境

```text
OS: SLES 12 SP5 (VERSION_ID=12.5)
Kernel: 4.12.14-122.307-default x86_64
Filesystem: /dev/sda1 ext4
CPU: 80 logical CPUs
Memory: approximately 375 GiB total
ulimit -n: 1000000
Python: 3.11.5
Python executable: /home/paas/marketing-copilot-test/python/bin/python3.11
OpenSSL: 1.1.1v
```

正式 run 目录的 `df -T .`、Python 版本和完整 `pip freeze` 记录在服务器的 `final-environment.txt`。应用 venv 使用离线 wheelhouse 安装，未访问公网依赖源。运行时包：

```text
release/sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz
SHA256=11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97
```

服务器安装验证通过：`_ctypes`、`ctypes`、`ssl`、`venv` 导入成功；Python 3.11.5 和 OpenSSL 1.1.1v 可用。wheelhouse SHA-256：

```text
504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda
```

### 固定基线配置

除任务明确标注的 `progress-mode`、CLI 开关和 steady 参数外，所有配对保持：

```text
pool_size=4
route_tps=200
tps_scope=per_bind
configured_total_ceiling=800 PDU/s
window_size=50
workers=8
projection_staging_mode=vectorized
synthetic SMSC=127.0.0.1
```

`per_bind` 表示每个物理 Bind 200 PDU/s，4 Bind 的配置上限为 800 PDU/s；它不是 200 PDU/s 的全局共享上限。所有长短信在本轮均为单 part，因而 task 数和 Socket PDU 数可直接对账。

## 速率和对账口径

### Socket 速率

Socket 速率来自 `AsyncSmppTransport.snapshot()` 的 `socket_written_parts` 和发送时间戳，不使用 response 数代替发送数，也不使用包含 drain 的总运行时间：

```text
socket_wall_tps = sum(socket_written_parts)
                  / (max(last_socket_write_monotonic)
                     - min(first_socket_write_monotonic))
```

第二个 harness 修复保留每个 worker 每次 Bind 关闭前的 transport snapshot，避免最后一次空 snapshot 覆盖真实发送证据。

### Journal 和 response

每个单 part task 的正常闭环为：

```text
accepted task
  -> checkpoint/progress reservation
  -> socket_written PDU
  -> submit_sm_resp
  -> Journal durable
  -> projection applied
```

`journal_event_count` 正常为 Socket task 的两倍，因为分别记录发送和响应事件。`pending=0`、`writer_errors=0`、`unresolved_events=0`、projection `lag=0` 和 `duplicates=0` 才能作为完整末态证据。

### CLI 速率

CLI JSON 中明确区分：

```text
socket_write_rate_pdu_per_second
journal_durable_rate_pdu_per_second
response_observed_rate_pdu_per_second
```

旧兼容字段 `rate_pdu_per_second` 保留为 durable alias。缺失 worker live status、stale snapshot 或 counter identity reset 时输出 `null` 和原因，不伪造 0。

## Phase 2：SLES 1K / 5K 结果

### L1：1K decoupled smoke

服务器证据目录：

```text
runs/l1-1k-transport-history
```

| 指标 | 结果 |
| --- | ---: |
| accepted | 1,000 |
| socket-written PDU | 1,000 |
| response success | 1,000 |
| Journal events | 2,000 |
| projection applied | 2,000 |
| Socket wall TPS | 165.068 |
| total duration | 9.630 s |
| pending / lag / duplicates | 0 / 0 / 0 |
| resource gate | PASS |

第一轮未经修复的 1K 证据 `runs/l1-1k-decoupled-cli-off` 曾因父进程 stale lightweight spool 快照等待 600 s；其 Socket、response、Journal、projection 和 full spool status 均已完成。修复后 `runs/l1-1k-authoritative-spool` PASS；随后加入 transport 历史保留后 `l1-1k-transport-history` 再次 PASS。失败证据没有删除或覆盖。

### L2：5K old/new，CLI OFF

配对顺序为 `legacy → decoupled → decoupled → legacy`。每轮 5,000 task、5,000 socket PDU、5,000 response success、10,000 Journal events、10,000 projection events、pending=0、duplicates=0、resource gate=PASS。

| 轮次 | progress mode | Socket wall TPS | duration | producer active | drain |
| --- | --- | ---: | ---: | ---: | ---: |
| old-1 | legacy | 217.129 | 27.184 s | 16.865 s | 8.023 s |
| new-1 | decoupled | 239.224 | 25.155 s | 17.842 s | 5.063 s |
| new-2 | decoupled | 260.027 | 22.852 s | 16.970 s | 3.977 s |
| old-2 | legacy | 206.089 | 28.154 s | 15.915 s | 10.054 s |

```text
OLD_SOCKET_TPS_MEDIAN=211.609
NEW_SOCKET_TPS_MEDIAN=249.626
NEW_VS_OLD_SOCKET_TPS_PERCENT=17.965
```

每轮实际速率低于 800 PDU/s 配置 ceiling，且 route credit wait 明显（约 811–865 s 的跨 worker 累计等待）。因此这些数值是同一配置下的 observed send-side comparison，不是 800 PDU/s 饱和能力证明；L2 总门禁保持 `INCONCLUSIVE_PROTOCOL_LIMITED`，不外推到更大负载或生产 SLA。

## Phase 2：L3 CLI OFF/ON

配对顺序为 `OFF → ON → ON → OFF`，模式均为 decoupled。

| 轮次 | CLI | Socket wall TPS | duration | CLI NDJSON | CLI RSS peak |
| --- | --- | ---: | ---: | ---: | ---: |
| off-1 | off | 208.788 | 27.007 s | — | — |
| on-1 | on | 239.129 | 25.112 s | 26 lines | 18,916 KiB |
| on-2 | on | 236.618 | 24.262 s | 25 lines | 18,696 KiB |
| off-2 | off | 248.644 | 23.295 s | — | — |

```text
CLI_OFF_SOCKET_TPS_MEDIAN=228.716
CLI_ON_SOCKET_TPS_MEDIAN=237.8735
CLI_OVERHEAD_PAIRED_PERCENT=4.004
```

这不是“CLI 造成 4% 下降”：ON 组反而略高，说明两轮噪声大于当前结论力；按任务定义的配对百分比仍记录为 +4.004%，不把它压成 0 或 PASS。CLI 运行期间 `socket_write_rate_pdu_per_second` 因没有 live worker status 保持 N/A；Journal durable rate 可见，且未显示伪造 0。L3 需要后续把 live worker status 的只读聚合快照纳入同一验收运行，才能重新做严格的 CLI overhead gate。

## Phase 3：600 s steady 和资源

服务器证据目录：

```text
runs/steady-600s
```

配置使用 `PROD_16G`，application RSS guard 为 12 GiB，run-bytes guard 为 1 GiB，synthetic feeder 运行 600 s 后等待完整 drain。

| 指标 | 结果 |
| --- | ---: |
| requested / actual steady | 600.000 / 600.997 s |
| total duration | 666.494 s |
| accepted tasks | 150,000 |
| socket-written PDU | 150,000 |
| response success | 150,000 |
| Journal events | 300,000 |
| projection applied | 300,000 |
| Socket wall TPS over send span | 238.208 |
| max pending records | 2,855 |
| max Journal backlog bytes | 4,224 |
| max application RSS | 1,555,756 KiB (~1.48 GiB) |
| max harness RSS | 2,328,976 KiB (~2.22 GiB) |
| max application CPU sample | 807.349% of host capacity |
| max harness CPU sample | 949.756% of host capacity |
| final pending / Journal backlog / projection lag | 0 / 0 / 0 |
| duplicates / response errors / unresolved | 0 / 0 / 0 |
| resource gate | PASS |

四个 Campaign 各自均为 37,500 task、37,500 socket PDU、37,500 response success，`write_failed_tasks=0`。steady 末态 projection `exitcode=0`，没有 restart。

## L4：Writer 600 ms 隔离故障

服务器证据目录：

```text
runs/l4-writer-delay-600ms
```

该实验仅对测试进程的 `ProgressJournal.append_many` 注入 600 ms 延迟，未修改仓库生产代码、未延迟真实磁盘、未影响其他测试。50 个 PDU 的结果：

```text
socket_written_tasks=50
responses_observed=50
reader_observed_elapsed_s=0.014262
threshold_s=0.45
pending_durability_before_drain=50
drained=true
durable_events_after_drain=50
writer_errors=0
writer_fsync_total_ms=1202.503
```

结论：`WRITER_600MS_READER_INDEPENDENCE=PASS`。Reader/response 路径没有同步等待 600 ms writer；随后 durable drain 仍然完成。

## L5：Journal 容量、回收、重试

服务器证据目录：

```text
runs/l5-journal-capacity-recovery
```

使用独立的 2,400 byte Journal、1,200 byte segment 和 1,024 byte event 上限，仅在测试目录中制造容量不足。结果：

- 初始 segment 形成并成功回收已被确认的 inactive segment。
- 临时 capacity snapshot：`blocked=true`，剩余容量 241 bytes。
- 回收两个已确认的 inactive segment 后容量恢复，剩余容量 1,970 bytes。
- 新 recovery event 可追加，回收后 Journal 重新可写。
- 保留事件中重复 event id 为 0。
- 额外执行一次 `ProjectionBackpressure` 后自动 retry：attempts=2、durable=1、writer_errors=0。

结论：`JOURNAL_BACKPRESSURE_AUTO_RECOVERY=PASS`。这是隔离 Journal/Progress contract 验证，不等价于故意填满测试服务器生产磁盘。

## L6：Campaign 控制和隔离

服务器证据目录：

```text
runs/l6-campaign-controls
```

使用已有 `vnext_runtime_canary_acceptance.py --gates r5`，仅 loopback。结果 `R5=PASS`：

- Campaign A：pause 后在恢复前没有继续增长；resume 后完成 100 socket-written、100 response success。
- Campaign B：没有被 A 的 pause/cancel 影响，完成 100 socket-written、100 response success。
- 独立 persistent campaign：重启 API 后 pause control 仍存在，resume 后完成 20/20。
- cancel campaign：20/20 进入 cancelled，没有 socket PDU。
- loopback 总接收 220、总 response success 220、错误为空。

## CLI 只读验证

在 steady 完成后对同一 run 执行：

```bash
/home/paas/marketing-copilot-test/runs/p1-sles-20261019-standalone/venv/bin/python \
  /home/paas/marketing-copilot-test/runs/p1-sles-20261019-standalone/source/backend/tools/vnext_stats.py \
  --root /home/paas/marketing-copilot-test/runs/p1-sles-20261019-standalone/runs/steady-600s \
  --once --json --no-color
```

执行前后 run 目录文件路径、大小和 mtime 完全一致：`tree_changed=false`。CLI return code 为 0，stderr 为空。由于 run 已结束且没有 live worker status：

```text
health=OFFLINE
socket_write_rate_pdu_per_second=null
journal_durable_rate_pdu_per_second=null  # stale snapshot；不是 0
response_observed_rate_pdu_per_second=null
rate_unavailable_reasons=[live_worker_status, stale_snapshot]
durable_socket_written_pdus=150000
projection.caught_up=true  # aggregate snapshot still provides this fact
```

CLI 单元回归覆盖了“Socket counter 先增长、durable 尚未增长；durable 追平；multipart；worker identity reset；缺失快照；watch JSON；运行目录不写入”等情形，因此 `CLI_SOCKET_VS_JOURNAL_RATE=PASS` 指的是语义、N/A 和只读契约，不是 L3 overhead gate 已通过。

## 回归测试

最终工作树上的 Windows backend 回归（用于代码回归，不作为 SLES 性能基线）：

```text
472 passed, 5 skipped, 4 warnings in 103.21s
```

定向 CLI、harness 和 acceptance tests：

```text
38 passed in 0.98s
```

## Pilot 边界

当前证据只支持 `CONDITIONAL` 的受控 Pilot 申请，不支持直接生产发送。Pilot 必须满足：

1. 由运维/业务负责人另行确认真实 SMSC、账号、号码范围、窗口和停止责任人；本轮没有保存或使用任何真实凭据。
2. 只选择 1–2 个低风险 Campaign，先做小批量，再进入限时窗口；不得把 600 s synthetic steady 直接外推为生产容量。
3. CLI 观测必须同时查看 Socket write、Journal durable、response observed、pending durability、unknown/unresolved、projection lag 和 worker live status。
4. 出现 unresolved、writer error、response timeout/unknown、pending 长时间增长、worker 退出、projection 长时间不追平或磁盘空间不足时，立即停止新发；已 socket-written PDU 不因暂停而撤销或重发。
5. unknown/unresolved 只能人工对账，不能通过回滚脚本或自动 retry 重新投递。

详细操作清单见 `Docs/Marketing_Copilot_VNext_Limited_Pilot_Runbook.md`。

## 机器摘要

```text
SOURCE_HEAD=122f47d42b5fbcc2ca871d887286ac93ce665e4d
CODE_CHANGES=MINIMAL_FIX_WITH_EVIDENCE
CLI_SOCKET_VS_JOURNAL_RATE=PASS
BACKEND_REGRESSION=472 passed, 5 skipped, 4 warnings
TEST_OS=SLES 12 SP5
FILESYSTEM=ext4 (/dev/sda1)
SLES_1K=PASS
SLES_5K_PAIRED=INCONCLUSIVE
OLD_SOCKET_TPS_MEDIAN=211.609
NEW_SOCKET_TPS_MEDIAN=249.626
NEW_VS_OLD_SOCKET_TPS_PERCENT=17.965
CLI_OVERHEAD_PAIRED_PERCENT=4.004
WRITER_600MS_READER_INDEPENDENCE=PASS
JOURNAL_BACKPRESSURE_AUTO_RECOVERY=PASS
CAMPAIGN_PAUSE_RESUME_CANCEL=PASS
AT_MOST_ONCE_NO_DUPLICATE=PASS
STEADY_600S=PASS
RSS_16G_PROFILE=PASS
PER_CAMPAIGN_PROJECTION=N/A_KNOWN_GAP
LIMITED_PILOT_DECISION=CONDITIONAL
FULL_RELEASE_DECISION=NO_GO
REAL_SMSC_CONTACTED=NO
PRODUCTION_DEPLOYED=NO
COMMIT_PUSH=NO
```
