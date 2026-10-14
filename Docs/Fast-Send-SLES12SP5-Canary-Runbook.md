# Fast Send SLES12SP5 canary runbook

状态：`RUNBOOK_ONLY / LIVE_CANARY_NOT_RUN`  
默认：`MC_SMS_SEND_MODE=reliable`

本 runbook 只定义受控 canary 的检查和回滚动作；本次交付没有在真实 SLES12SP5 或真实 SMSC 上执行任何生产切换。

## 0. Entry criteria

未满足以下任一项时，不进入 Fast canary：

- 1K/3K/10K/50K equal-window A/B 三轮结果完整且无 duplicate/lost mapping；
- SLES12SP5 x86_64、CPython 3.11、manylinux2014/glibc 2.17 兼容性验证完成；
- 真实 SMSC 的 submit response、DLR、拒绝、断链、超时和 multipart 行为已获批准；
- 单进程 instance lock、备份、监控、告警和人工 unknown reconciliation 已演练；
- canary route、号码范围、时间窗口、最大任务量和停止负责人已书面确认。

## 1. Preflight (read-only first)

```bash
uname -a
python --version
systemctl status marketing-copilot --no-pager
ss -ltnp | grep -E ':19090|:2775' || true
du -sh data logs uploads monitor_dir || true
```

确认没有第二个实例；Fast 不允许通过 `MC_ALLOW_MULTI_INSTANCE=1` 绕过本地 fence。检查数据库、journal、日志、备份目录空间和最近一次 `checksums.sha256`。

生产/正常 canary 的 unit environment 应明确写出：

```text
MC_SMS_SEND_MODE=reliable
MC_ALLOW_MULTI_INSTANCE=0 (或未设置)
```

## 2. Controlled Fast canary proposal

只有 entry criteria 全部通过后，才允许在隔离 unit/route 中设置：

```text
MC_SMS_SEND_MODE=fast
MC_ALLOW_MULTI_INSTANCE=0
MC_FAST_SEND_CHECKPOINT_QUEUE=<经过容量测试的值>
MC_FAST_SEND_CHECKPOINT_BATCH=<经过容量测试的值>
MC_FAST_SEND_CHECKPOINT_MS=<经过容量测试的值>
MC_FAST_SEND_MAX_DURABLE_BACKLOG=<经过容量测试的值>
```

使用单进程、固定 route TPS、固定 SMPP window/pool、固定号码子集。canary 必须先发送小批量，再逐步扩大；不得在全量 campaign 上直接启用。

## 3. Observe during canary

每 30 秒记录以下指标和数据库快照：

- Fast `checkpoint_pending`、`checkpoint_lag_seconds`、`unresolved_count`、`fail_closed`、`backpressure`；
- `queued/sending/submitted/accepted/delivered/partial/unknown`；
- submit_sm sent、submit_sm_resp success/reject/timeout、SMSC event count；
- DLR mapping、unknown submit ids、duplicate send tasks、duplicate message bindings；
- DuckDB writer pending/journal/quarantined、WAL、RSS、CPU、API latency、SMPP connection/window state。

立即停止条件：任意 duplicate/misdelivery/lost mapping；unknown 被自动 retry；checkpoint lag 持续增长；writer/journal quarantine；API/worker hang；SMSC 响应异常；或 pause/cancel 后出现新 socket admission。

## 4. Stop and rollback

1. 先执行 campaign pause/cancel，并等待 SMSC response / callback / checkpoint backlog 收敛。
2. 对仍为 `sending`、`partial`、`unknown` 的任务做人工 reconciliation；不要点击自动 retry 作为第一动作。
3. 停止服务前保留 journal、数据库、WAL、应用日志和 SMSC correlation log。
4. 将环境恢复为 `MC_SMS_SEND_MODE=reliable`，确认没有 `MC_ALLOW_MULTI_INSTANCE=1`，再按标准 systemd restart。
5. 重启后的 Fast claim 会按设计进入 `unknown`，不得自动 reclaim；Reliable 恢复只处理有明确 pre-submit 证据的失败。
6. 通过健康检查、writer flush、SMPP bind、状态对账后，才恢复可靠发送。

禁止使用 `git reset --hard`、删除数据库/WAL/journal 或清空 unknown 记录作为回滚手段。

## 5. Post-canary evidence

保留以下内容并写入结果 JSON：版本/工作树状态、unit environment、route/window/pool/TPS、开始结束时间、任务总数、每种最终状态、SMSC submit/DLR counts、unknown/partial 清单、资源峰值、所有异常和是否发生人工处置。真实 SLES/SMSC 未执行时必须明确写 `NOT_RUN` 或 `UNVERIFIED`，不能用本机 loopback 结果替代。
