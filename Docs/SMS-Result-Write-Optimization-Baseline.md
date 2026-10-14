# SMS Result Write Optimization Baseline

日期：2026-10-14  
基线提交：`a3440e57824c2452e1ee934e5fe03c631bb0f11d` (`a3440e5`)  
实现状态：工作区未提交（`UNCOMMITTED`）

## 结论摘要

现网 RCA 的主要矛盾不是“批次太小”本身，而是结果提交、发送授权、Claim 和实时摄取在同一 DuckDB 单写 gate 上竞争。题目提供的观测锚点显示：`sms_result_batch` 平均持锁从 42.6 ms 增至 346.2 ms，`submit_authorize` 平均等待从 116 ms 增至 419 ms，并出现 5 秒写锁超时；实时摄取从暂停恢复后，4 个 worker 以单文件约 700～850 个匹配任务的速度突发入库。

这些生产数字没有连接生产库重跑，本文件将其作为 `PROVIDED_PRODUCTION_ANCHOR`，不作为修复后的性能结果。

## 基线调用链

1. `smpp_gateway._store_outcome()` 接收明确的 `submit_sm_resp`，调用 `sms_task_service.record_part_result()`；任务级完成由 `task_worker._async_send_complete()` 调用 `mark_submitted()`。
2. `sms_write_buffer.submit()` 先写 `result_journal_service.append_event()` 并等待 Journal fsync，再进入按类型队列。
3. `_commit_transaction_batch()` 在一个 `sms_result_batch` 事务内写入 `sms_result_events` 幂等门禁，然后调用 `apply_buffered_writes()`，成功提交后才追加 Journal terminal marker。
4. no-DLR 成功已经使用 `UPDATE sms_tasks ... FROM (VALUES ...)` 集合更新；但在基线中，`sms_send_attempts`、`batch_campaign_batches`、`batch_campaign_daily_stats` 仍由 `executemany` 分别执行，事件汇总也按 Python 分组逐条插入。
5. `realtime_ingestion_worker._run()` 在 `backpressure_service.wait_for_ingestion()` 通过后领取文件；`receive_file()` 在每个 chunk 入库前再次等待，但基线没有暂停解除后的共享恢复预算。

## 基线参数与边界

代码默认值（运行时环境变量优先）：

| 参数 | 基线默认值 | 语义 |
|---|---:|---|
| `MC_SMS_WRITE_BATCH_SIZE` | 1000 | 收集窗口上限，不等于单事务条数 |
| `MC_SMS_WRITE_TRANSACTION_SIZE` | 250 | 单个结果事务上限 |
| `MC_SMS_WRITE_FLUSH_MS` | 100 ms | 结果收集窗口 |
| `MC_SMS_RESULT_JOURNAL_FSYNC_BATCH` | 250 | Journal writer 的 fsync 聚合上限 |
| `MC_REALTIME_INGEST_WORKERS` | 4 | 实时文件 worker 数 |

`delivery_receipt_mode=disabled` 只是不请求 DLR；仍必须收到明确的 `submit_sm_resp=0` 才能把任务记为 `accepted`。`accepted` 表示 SMSC 接受，不表示 handset delivered。

## 可观测性缺口与本次补强

基线已有：

- `sms_result_batch`、`sms_task.submit_authorize`、`sms_tasks.claim` 等 operation 级写事务和 gate wait/hold histogram；
- Journal pending/retry/quarantine 状态；
- 结果批次条数、提交事务数、任务/分片 transport 计数。

基线不足：

- 结果事务内部没有 Journal 等待、event fence、任务状态折叠和 secondary update 的阶段计时；
- `WrappedConnection` 没有显式保存首次取得 gate 的业务 operation，容易让后续 execute/commit 阶段的诊断语义被误读；
- 没有暂停解除后的摄取 admission rate、token、throttle 计数。

本次实现增加：

- `marketing_copilot_sms_result_stage_seconds` 和 `..._stage_items`，覆盖 Journal append/fsync、event-id fence、intent/part result、disabled submitted、regular submitted、message-id、failure/receipt；
- gate 首次获取时固定 `write_operation`，commit 阶段不再改变持锁 operation 标签；
- `marketing_copilot_realtime_ingestion_ramp_rate_tps`、token、throttled gauges。

## 可复现基准

基准均在本机独立临时 DuckDB、独立 Journal、Python 3.11、`MC_SMS_RESULT_JOURNAL_FSYNC_MS=2`、`MC_SMS_RESULT_JOURNAL_FSYNC_BATCH=250`、`MC_SMS_WRITE_FLUSH_MS=100` 下运行；未连接真实 SMSC。每组均使用同一脚本结构，基线使用 `a3440e5` 独立 worktree，实现使用当前工作树。

### no-DLR 成功路径

| 样本 | 基线耗时 | 实现耗时 | 基线 commits | 实现 commits | accepted（基线/实现） | 结果事件（基线/实现） |
|---:|---:|---:|---:|---:|---:|---:|
| 1K | 15.126 s | 15.406 s | 56 | 50 | 1000 / 1000 | 1000 / 1000 |
| 3K | 45.242 s | 45.177 s | 148 | 161 | 3000 / 3000 | 3000 / 3000 |

结论：该单线程 producer 基准的墙钟变化在噪声范围内，不能宣称整体 TPS 已提升。它证明了 Journal→DB→terminal 的成功数和事件数没有变化；性能收益需要在并发 SMPP callback、混合事件和实际写锁竞争下继续采样。

### 含 attempt/批次统计路径

| 样本 | 基线耗时 | 实现耗时 | 基线 commits | 实现 commits | accepted / attempt accepted / batch delivered / daily sent |
|---:|---:|---:|---:|---:|---:|
| 1K | 15.609 s | 15.413 s | 39 | 41 | 1000 / 1000 / 1000 / 1000（两组均一致） |
| 3K | 45.568 s | 45.699 s | 115 | 128 | 3000 / 3000 / 3000 / 3000（两组均一致） |

实现路径在每个事务中将 `sms_send_attempts` 由按行参数执行改为带 attempt/claim fence 的单个 `UPDATE ... FROM (VALUES ...)`；批次进度、日统计以及多分组 task-event summary 也使用集合语句。当前基准没有显示稳定的墙钟改善，因此本次交付将其定位为降低 SQL/行更新放大和锁内往返的安全优化，而不是 TPS 保证。

## 尚未具备的基线证据

- 未取得生产数据库、原始指标采样或真实 SMSC 授权；生产观测数字仅来自任务材料。
- 当前本地基准没有采集 gate wait/hold P50/P95/P99、CPU、RSS、I/O、SMPP RTT 或长时间积压斜率。
- 没有在本机执行 10K、10～30 分钟 soak、SLES12SP5 现地升级或真实运营商 DLR 验收。

因此，后续性能报告必须把上述项目标为 `NOT_RUN`/`UNVERIFIED`，不能用本单元测试替代。
