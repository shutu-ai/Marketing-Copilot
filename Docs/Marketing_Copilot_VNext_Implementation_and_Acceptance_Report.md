# Marketing Copilot VNext MSISDN Sharded Async Pipeline

状态：`VNEXT_PIPELINE_READY_FOR_CANARY`（仅限合成号码、loopback SMSC 的 VNext 门禁；旧 `INTEGRATED_NO_GO` 保持不变）

本轮基线为当前 `origin/main`：
`5650b8fc6bcdd620858258db3937cd5c5ad59d14`。工作区在任务开始前已有用户变更；本轮没有提交或推送。

## V0–V5 交付状态

| 阶段 | 实际交付 | 回归/验收 | 状态 |
| --- | --- | --- | --- |
| V0 | 基线、业务完成语义、owner、迁移映射和门禁记录 | 原有相关回归 `41 passed, 1 skipped` | PASS |
| V1 | `partition.py`、`spool.py`、`supervisor.py`；SHA-256 MSISDN 分片、CRC spool、cursor/taken、owner epoch | 分片稳定性、CRC/recovery、supervisor 测试 | PASS |
| V2 | `worker.py`、`smpp_transport.py`、`route_credits.py`、`metrics.py`；异步 SMPP、PDU window、共享 TPS、socket_written | VNext 专项回归、loopback P10/P100/P1K/P5500 | PASS |
| V3 | `ingress.py`、`pressure.py`；chunk ingress、阶段背压、重入幂等、pause/cancel 控制 | 多 campaign spool 对账、压力/公平性测试 | PASS |
| V4 | `projection_writer.py`、`runtime.py`；唯一 DuckDB owner、批量 projection、journal replay、restart/fault recovery | writer pause、worker crash/restart、control fence | PASS |
| V5 | main/API/UI/deploy 切换到 VNext；legacy sender 仅显式 `MC_PIPELINE_MODE=legacy|test` | backend 全量 `376 passed, 4 skipped`；VNext `23 passed`；frontend build PASS | PASS |

## 新主链与旧实现边界

生产默认链路为：

```text
Input -> chunk normalize -> SHA256 virtual shard -> durable spool
      -> shard worker -> async SMPP -> progress journal
      -> dedicated ProjectionWriter -> DuckDB projection
```

`socket_written` 是本链路业务完成状态；`submit_sm_resp` 只作为异步诊断和窗口释放证据，不作为逐任务数据库写入触发器。Worker 不打开共享生产 DuckDB，ProjectionWriter 是 VNext projection 的唯一 DB owner。没有引入 Kafka、Redis 或 RabbitMQ。

旧 `task_worker`/`sms_write_buffer`/逐任务发送授权链不再由默认 main 启动；保留的显式 legacy/test 入口只用于受控回退和既有回归，未作为 VNext 的长期发送双轨或 API 默认路径。旧路径没有被伪装成 VNext 状态，`accepted/delivered` 没有映射为 `socket_written`。

## 阶段 A：本地离线交付与 ext4 验收

正式运行时：

- CPython `3.11.5` standalone runtime package：`release/sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz`
- runtime SHA256：`11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`
- 运行时合同：`ssl`、`_ctypes`/`ctypes`、`venv` 均通过；OpenSSL `1.1.1v`
- app wheelhouse：`marketing-copilot-sles12sp5-wheelhouse-20261013-batch-upload-tolerance.tar.gz`
- wheelhouse SHA256：`504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda`
- app package：[marketing-copilot-sles12sp5-system-python-20261016-vnext-dirty.tar.gz](/D:/dev-projects/MarketingCopilot/release/marketing-copilot-sles12sp5-system-python-20261016-vnext-dirty.tar.gz)
- app SHA256：`4229425bbf7af893d835ecddabafe677522eaf768088494b76703582c27d22fe`
- app package sidecar：[marketing-copilot-sles12sp5-system-python-20261016-vnext-dirty.tar.gz.sha256](/D:/dev-projects/MarketingCopilot/release/marketing-copilot-sles12sp5-system-python-20261016-vnext-dirty.tar.gz.sha256)

正式验证根目录为 WSL 原生 ext4：`/tmp/mc-vnext-stage-a-20261016`；`df -T .` 为 `/dev/sdd ext4`。没有把 `/mnt/d` 结果作为 Linux 性能基线。app venv 由上述 standalone runtime 创建，并使用 `pip --no-index --find-links offline/wheels` 安装；package checksums、runtime checksums、依赖 freeze、native DuckDB `readelf/ldd` 检查均通过。

### loopback gate 对账

每个门槛均满足 `generated == socket_written == submit_sm_received == submit_sm_resp_success == projection.applied`，`pending=0`，无 duplicate/missing destination，4 worker、256 virtual shards：

| Gate | generated | socket_written | projection.applied | duration |
| --- | ---: | ---: | ---: | ---: |
| P10 | 10 | 10 | 10 | 1.245s |
| P100 | 100 | 100 | 100 | 2.117s |
| P1K | 1000 | 1000 | 1000 | 13.360s |
| P5500 | 5500 | 5500 | 5500 | 62.401s |

P5500 的四个 campaign 对账为 `2500 + 2500 + 250 + 250`，每 campaign 的 loopback 收件数与预期一致。

### 故障与恢复

- worker crash/restart：crash exit `17`，crashed `5`，recovered `15`，unique `20`，pending `0`：PASS。
- ProjectionWriter pause/resume：暂停期间阻塞，恢复后 applied `20`，duplicates `0`：PASS。
- pause control fence：暂停期间 writes `0`，恢复后 writes `5`：PASS。

## 服务器阶段 B/C

服务器 B/C 已完成。服务器为 EulerOS 2.0 SP15 x86_64，本轮按任务约定作为 SLES12SP5 性能代理；使用小写用户 `paas`，并严格使用：

```text
/home/paas/marketing-copilot-test/runs/<unique-run>/
```

阶段 B 只允许 loopback、独立目录、standalone CPython 3.11.5、最大 4 CPU、`nice +10`、`ionice idle`、RSS 2 GiB warning、磁盘 5 GiB warning，并按 `10 -> 100 -> 1000` 后人工确认 `5500`；实际四门均 PASS：P10 10/10/10、P100 100/100/100、P1K 1000/1000/1000、P5500 5500/5500/5500，均 pending=0、无 missing/duplicate。禁止真实 SMSC、52K/102K、systemd/root/global pip 和共享服务器生产目录。阶段 C 已在切回 GitHub VPN 后导入 evidence 并生成 [性能对照报告](Marketing_Copilot_VNext_Performance_Comparison_Report.md)。

最终 readiness：`VNEXT_PIPELINE_READY_FOR_CANARY`，仅允许进入合成/loopback 约束下的 canary 评审；不得将其解释为真实 SMSC、原生 SLES 实测或生产发布许可。
