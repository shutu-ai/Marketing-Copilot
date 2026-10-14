# SMPP Send Hotpath TPS Acceptance

整体状态：`PASS_WITH_LIMITATIONS`。`SEND_CORRECTNESS` 未失败；`PERFORMANCE_AB` 和 `THROUGHPUT_TARGET` 尚未证明，不能用局部授权改善替代正式 A/B。

## 门禁结果

| 门禁 | 状态 | 证据/限制 |
|---|---|---|
| `SEND_CORRECTNESS` | `PASS_WITH_LIMITATIONS` | 35 focused tests；loopback stress-1k 1000/1000 accepted、1000/1000 submit_sm、无重复/丢失/unknown mapping。最终版本的 drop-response/partial/restart 全量组合未在本轮重跑。 |
| `THROTTLE_SEMANTICS` | `PASS_WITH_LIMITATIONS` | route-shared API 字段、共享 reservation、动态 revision/stop-aware 等待、multiplier 上限有单测；1/4 Bind 与动态配置压力矩阵未执行。 |
| `AUTHORIZATION_HOTPATH` | `PASS_WITH_LIMITATIONS` | 单轮隔离 benchmark：mean 190.997ms → 169.238ms，P95 242.876ms → 218.627ms；仅一轮，未满足正式 A/B 三轮。 |
| `SMPP_WINDOW_UTILIZATION` | `NOT_PROVEN` | API 暴露每 Slot queue/inflight，代码保留 window；最终 1K 没有保存可审计的 per-slot window utilization 时间序列。 |
| `PERFORMANCE_AB` | `NOT_PROVEN` | 未按 1K/3K/10K、同观察窗口、同资源、至少三轮完成 A/B；不能报告 TPS 提升。 |
| `THROUGHPUT_TARGET` | `NOT_RUN` | 未执行 fake SMSC ≥200 parts/s 阶梯；500 parts/s 仍不是生产门槛。 |
| `NO_REAL_SMS_SEND` | `PASS` | 自动化仅 `127.0.0.1` fake SMSC；最终 stress-1k 报告 `smpp_loopback_only=true`。 |
| `SLES12SP5` | `NOT_RUN` | 本轮未部署/验证 SLES12SP5。 |
| `LIVE_SMSC` | `NOT_RUN` | 未连接或修改生产 SMSC。 |
| `LONG_SOAK` | `NOT_RUN` | 未执行 5 分钟非空队列稳态和 HIGH→LOW→RESUME 周期。 |

## 最终 loopback 1K

命令：

```powershell
.\.p0-test-venv\Scripts\python.exe backend/tools/local_e2e_acceptance.py `
  --scenario stress-1k --timeout-seconds 300 --smsc-tps 1000 `
  --delivery-receipt-mode disabled --task-workers 8 --ingest-workers 4 `
  --smpp-window 10 --smpp-send-queue-capacity 100 `
  --output .tmp\smpp-hotpath-e2e-stress1k-final-20261014.json
```

结果文件：`.tmp/smpp-hotpath-e2e-stress1k-final-20261014.json`。最终实现实测 `PASS`：1000/1000 tasks accepted、1000/1000 submit_sm、无 duplicate/lost/unknown mapping，loopback-only；`business_sms/s = submit_sm_parts/s = 3.794`，P50 task→response `118095.008ms`、P95 `234863.969ms`、P99 `246647.997ms`，queue peak 916，final 0。消息全部为单 part，因此这不是 multipart TPS 或 ≥200 parts/s 证明。

前一轮同场景（fairness activity 尚未移出授权事务）结果为：1000/1000 accepted、1000/1000 submit_sm、无重复/丢失，`submit_sm/s = 3.558`，队列峰值 924；最终代码为 3.794，单次差异约 6.6%，仍不能构成正式 A/B。

## A/B 公平性

- A：`398e0e8b0d06cd774480f8240333406857d01459`。
- B：当前工作树实现；正式 SHA 在提交后填入 `sms-send-hotpath-tps-results.json`。
- 已完成 benchmark 使用同一机器、同一独立 DuckDB、1000 历史任务和 50 次迭代，但只有一轮且 `planned_parts=1`；4-part 只验证 statement reduction。因此仅支持 `AUTHORIZATION_HOTPATH=PASS_WITH_LIMITATIONS`，不支持 `PERFORMANCE_AB`。
- 未把 180 秒与 600 秒累计 accepted 数量混作 TPS；未把完整 SMS、parts、response 或 DLR delivered 混为一个指标。

正式 A/B 必须固定：`pool_size=4`、`window_size`、`task_workers=8`、`ingestion_workers=4`、route `tps`、同一 fake SMSC、同一消息分片分布、DLR mode、输入速率、预热、停止条件；每个 1K/3K/10K 至少重复三轮，报告 business SMS/s、parts/s、response/s、P50/P95/P99、window utilization、authorize wait/hold、journal age、backlog slope、CPU/RSS/I/O。

## Focused tests

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest `
  backend/tests/test_smpp_send_hotpath_optimization.py `
  backend/tests/test_smpp_async_submission.py `
  backend/tests/test_phase2_write_reduction.py `
  backend/tests/test_live_duckdb_hotpath_backpressure.py -q
```

最后一次执行：`35 passed in 33.94s`。

新增回归覆盖：multipart planned parts 一条 set-based INSERT、attempt/claim durable fence、route-shared parts/s API 口径、multiplier 不得提高 route limit。既有 async window、Unknown/Partial、result batch 和 DuckDB backpressure 测试继续通过。

## 只读现网采样手册

以下命令只读，不发送短信、不修改配置、不重启服务；不要把 token、密码、MSISDN、正文写入结果。

```bash
BASE='http://127.0.0.1:8000'
curl --fail --silent --show-error "$BASE/api/smpp/stats" > /tmp/mc-smpp-stats.json
curl --fail --silent --show-error "$BASE/api/smpp/status" > /tmp/mc-smpp-status.json
curl --fail --silent --show-error "$BASE/api/metrics/status" > /tmp/mc-metrics-status.json
curl --fail --silent --show-error "$BASE/metrics" > /tmp/mc-prometheus.txt
```

每 30 秒连续 5–10 分钟记录：`marketing_copilot_smpp_submit_sm_sent_per_second_1m`、`marketing_copilot_smpp_submit_sm_resp_per_second_1m`、`marketing_copilot_smpp_send_queue_depth`、`marketing_copilot_smpp_window_wait_seconds_*`、`marketing_copilot_smpp_submit_authorize_seconds_*`、`marketing_copilot_database_write_lock_wait_seconds_*`、`marketing_copilot_sms_result_stage_seconds_*`，以及只读任务状态的 backlog/Unknown/Partial 增量。判定优先级：`ROUTE_THROTTLED` → `DB_AUTHORIZATION_BOUND` → `SMSC_RESPONSE_BOUND` → `DISPATCH_BOUND` → `RESULT_JOURNAL_BOUND`，无法区分则 `MIXED/UNDETERMINED`。

## 回滚步骤

1. 停止自动化或受控实例，确认没有真实 SMSC 测试连接。
2. 以发布系统保留的上一个实现 SHA 回滚代码；不要覆盖 DuckDB、result journal 或运行数据目录。
3. 恢复本轮前端/API artifact，保留 route-shared 字段消费者的兼容别名。
4. 只读检查 `sms_send_attempts`、`sms_send_parts`、`task_events`、result journal pending/replay 状态，再按既有启动恢复流程启动。
5. 重跑 focused correctness tests；Unknown/Partial 不得自动重发，journal terminal marker 仍需 event-id 幂等。

## 生产限制

运营商 TPS scope（账号/route、Bind、source IP）和对照程序“近 500 TPS”的计量单位尚未确认，标记 `CAP_UNKNOWN` / `NOT_COMPARABLE`。不得提高 `tps`、扩大真实发送速率、部署 SLES、重启服务或运行真实 SMSC 压测作为本任务自动步骤。
