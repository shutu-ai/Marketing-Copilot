# Marketing-Copilot VNext Response / Journal 解耦实现

任务：`MC-VNEXT-P0-RESPONSE-JOURNAL-DECOUPLING-CLI-20261019`

- 源 HEAD：`91257109f16158948d4e74f8b54ec13eab257fde`
- 实施日期：2026-10-19
- 实施环境：Windows / Python 3.13（`.p0-test-venv`）
- 目标 Linux 验收环境：SLES12SP5 原生 ext4（本工作区不可用，未执行）

## 1. 结论

本轮完成了实际代码交付，而不是 RCA 或 `NO_CHANGE_WITH_EVIDENCE`：

1. SMPP Reader 的 `submit_sm_resp` 回调不再在 Reader 任务中等待 Journal `fsync`；
   响应只做 `_pending` 匹配、Window release 和 O(1) 入队。
2. `socket_written` 终态事件也不再在 Worker asyncio 事件循环里同步写盘；发送结果、
   响应、unknown、cancel/expire 事件统一经过每 Worker 一个有序持久化 actor。
3. 发送前按最坏情况预留 **1 个任务终态事件 + 每个编码分段 1 个响应事件** 的条目数和
   字节上界；预留不足时在 `dispatch_taken`/Socket Write 之前 fail closed。
4. 新增只读 CLI `backend/tools/vnext_stats.py`，不依赖 FastAPI/Vue/curses/rich，
   通过低频快照展示 Worker、Bind、Journal、Projection 和 Campaign 状态。
5. 新增持久 Campaign 聚合检查点，支持 Worker 重启、Journal segment rollover 和
   已 reclaim segment 后的累计统计；无法补齐时显式输出 `incomplete` 而不是伪造。

## 2. 结构

```text
ShardWorker._process
  |  1. 计算编码分段数
  |  2. progress_store.reserve(items = 1 + parts, bytes = per_slot * items)
  |  3. durable TaskSpool checkpoint（仍在 Socket Write 之前）
  |  4. AsyncSmppTransport.send_task(progress_reservation=...)
  v
AsyncSmppTransport
  |  send: 每个 PDU 写入前 claim 一个响应预留槽
  |  reader: match _pending -> release Window -> 同步 O(1) callback
  v
ShardWorker._response_event_nowait
  |  构造聚合事件（不序列化整条 SMS 到日志）
  |  progress_store.submit(event, reservation)
  v
ProgressPersistence（每 Worker 一个 writer thread）
  |  bounded queue / reservation budget / journal hard budget
  |  batch -> ProgressJournal.append_many(events) -> flush + fsync
  |  fsync 成功后释放 pending，更新 durable 计数和 Campaign 聚合
  v
ProgressJournal segments -> 现有 ProjectionWriter / 单 DuckDB owner
```

### 2.1 双重额度

- **SMPP Window**：收到对应 `submit_sm_resp` 后 release；只约束协议在途。
- **Progress durability credit**：事件真正 fsync 成功后才释放；只约束可写入的进度容量。
- 两者独立计数，不互相抵扣。Window 为空不代表响应已 durable。

### 2.2 状态不变量

```text
reserved
  -> possible_socket_write（checkpoint 已 durable，PDU 可能已越过 Socket 边界）
  -> response_observed（Reader 已解析，Window 已 release，事件可能仍在内存队列）
  -> response_durable（append_many + fsync 成功）
  -> projected（单 DuckDB owner 应用）
```

- `checkpoint`/`socket_written` durable 而 response 未 durable：重启后只能保持
  `UNKNOWN`/`UNRESOLVED`，不得自动重发。
- 进程崩溃会丢失“已观察但尚未 fsync”的事件；本轮不承诺零丢失，只承诺不把这类
  事件报告成 durable，并在 writer 失败时留下 unresolved 计数和 failure marker。
- 响应可能先于本任务的 `socket_written` 事件入队；Journal 保留提交顺序，Projection
  以 `event_id` 去重并按计数聚合，因此该顺序不会造成重复或错账。

## 3. 可靠性行为

### 3.1 发送前预留

- 预留条目数：`1 + count_parts(message)`。
- 预留字节：发送终态模板和响应模板中较大者 × 条目数，另加 4 KiB 错误文本上限。
- 预留同时检查：
  - `max_pending_items`
  - `max_pending_bytes`
  - `ProgressJournal.max_bytes - 当前 journal 已用字节`
- 任一不足：`ProgressPersistenceBlocked`，Worker 返回未 checkpoint 的 Spool 记录，
  下轮自动重试；不会先 checkpoint 再偷偷发送。

### 3.2 Writer 失败

`ProgressJournal.append_many()` 抛错（权限、No space、fsync 错误等）时：

- `accepting=False`，停止新 dispatch；
- 保留 pending 计数和已排队事件，不重放可能已部分写盘的批次；
- 写入 `worker-N.progress-aggregate.failure.json`，记录 unresolved event/byte 数；
- CLI 显示 `Health: BLOCKED`、`unresolved_events` 和 `Fsync errors`；
- 重启后 failure marker 仍会阻止新发送，必须由运维在对账后人工清除 marker
  （默认 fail closed，避免自动“修好”后重复写）。

只有 `ProjectionBackpressure`（写入前判定 Journal hard budget 不足，未写任何字节）
会进入**可重试背压**：writer thread 保留该批次、按刷新间隔重试，并在 Projection
reclaim 后自动继续；不增加 writer error，也不把事件报成 durable。

Reader 回调本身抛错时，Transport 记录 `response_callback_errors` 并释放该预留槽；
CLI 将这类“已观察但未入队”的事件标记为 `BLOCKED` 和 `response_callback_errors`，
不会计入 durable。

### 3.3 关闭 / Drain

两类边界分开：

1. `drain_transports()`：等待 `window_inflight == 0` 且 response callback 不再排队；
2. `close_transports()`：关闭 Reader/Writer，对未应答 PDU 生成 `unknown` 事件；
3. `drain_progress()`：等待最后一批事件 fsync；
4. `close_progress()`：停止 writer thread，持久化最终 Campaign 聚合。

正常 shutdown、人工 pause/cancel 和 WorkerSupervisor restart 都使用该顺序。

### 3.4 Campaign 控制

- `pause` 在 `dispatch_taken` 之前检查；已暂停记录不 checkpoint，恢复后可重投。
- `cancel` 在发送前将记录 durable 地移出 Spool，并写 `cancelled` 事件。
- `before_write` 在每条 PDU 前重新检查业务日、pause/cancel；控制文件的观察延迟
  受 `refresh_campaign_states()` 的 50 ms 节流约束。
- 恢复流程不会重建或覆盖已有的 Campaign pause 状态。

## 4. 代码接入点

| 文件 | 变更 |
| --- | --- |
| `backend/vnext/progress.py` | 新增 `ProgressPersistence` / `ProgressReservation`，单 writer thread、有界队列、预留、fsync 后确认、失败 marker、Campaign 聚合检查点 |
| `backend/vnext/smpp_transport.py` | Reader 回调改为非阻塞安全路径；观测/持久化计数分离；响应延迟 bounded histogram；`count_parts()`；PDU 级 claim/release |
| `backend/vnext/worker.py` | `progress_store` 注入；发送前预留；multipart 预留；`socket_written`/response 全部走 actor；Campaign 控制、drain、健康状态 |
| `backend/vnext/runtime.py` | 每个 Worker 创建持久化 actor；低频 status heartbeat；两阶段 drain；`progress_queue_*` 配置 |
| `backend/vnext/projection_writer.py` | 启动时修复 crash 留下的 partial JSONL tail；保留既有 append_many / segment / reclaim |
| `backend/vnext/metrics.py` | Worker 状态快照不再复制最近 4096 条事件样本 |
| `backend/vnext/critical_path_profiler.py` | 为 writer/heartbeat 并发访问增加 profiler 计数器锁 |
| `backend/tools/vnext_stats.py` | 新增只读 CLI |
| `backend/tools/vnext_streaming_acceptance.py` | 新增 `--progress-mode decoupled|legacy`，默认走新 actor，旧源码自动回退 legacy |

## 5. 统计信号

Worker 状态文件新增/充实：

- `progress.queue_depth` / `items_bytes` / `reservations_items` / `reservations_bytes`
- `progress.pending_durability` / `pending_durability_bytes`
- `progress.writer_batches` / `writer_events_durable`
- `progress.writer_fsync_total_ms` / `writer_oldest_age_ms`
- `progress.writer_errors` / `unresolved_events` / `blocked_reason`
- `progress.durable_response_success/error/unknown`
- `progress.durable_socket_written_pdus` / `durable_events`
- `campaign_counters`（持久聚合 + 当前 pending）

Transport 状态文件新增：

- `response_observed_success/error/unknown`
- `unexpected_response_pdus`
- `response_callback_errors` / `response_callback_pending`
- `response_latency_p50_ms` / `response_latency_p95_ms`
- `bind_active` / `bind_acquires` / `reconnect_count`

CLI 不读取 MSISDN、短信正文、SMSC credentials，也不打开 DuckDB 或扫描 Journal segment。

## 6. 验证

```text
python -m pytest backend/tests -q
469 passed, 5 skipped, 4 warnings in 144.66s
```

基线要求为 `447 passed, 5 skipped`；新增 22 项专项测试，全部通过。

专项覆盖：

- Reader 在注入 600 ms `append_many` 延迟时仍读完全部 50 条响应；
  `response_observed` 先于 `response_durable`。
- 预留容量不足时在 checkpoint/Socket Write 之前 defer；释放后下一轮自动恢复。
- Journal append/fsync 抛错时不产生 fake durable，failure marker 记录 unresolved。
- multipart task 的 1 个发送事件 + 3 个响应事件、reservation 归零。
- A pause / B continue；A resume；cancel；Spool cursor/taken 正确。
- duplicate / unknown / out-of-order `submit_sm_resp` 与 `enquire_link`。
- Reader close 生成 unknown 事件，并在 writer 关闭前 durable。
- Journal segment rotation + reclaim 后 aggregate replay 不重复计数。
- 真实 `VNextRuntime` 入口：2 Campaign、1 Worker、loopback SMSC、
  Spool→Journal→Projection 全部 caught-up，CLI 同时读取成功。
- CLI JSON / 文本 / `--campaign` / stale / NDJSON watch / rate reset。

## 7. 限制与未闭环

1. 本工作区是 Windows/Python 3.13，不是 SLES12SP5；`SLES_1K` / `SLES_5K_PAIRED`
   未执行。正式 A/B 必须在 SLES12SP5 原生 ext4 上按任务书跑 1K smoke 和两轮交错
   5K，不能用本轮 Windows 结果替代。
2. `CLI_OVERHEAD_PAIRED_PERCENT` 未测量；本轮只证明 CLI 只读、低频、不触碰发送热路径。
3. 每 Campaign 的 `Projected` 列当前为 `N/A`，CLI 会把它列入 `incomplete_metrics`；
   本轮没有为 Projection 增加逐 Campaign 状态聚合，避免在 DB owner 热路径增加 SQL。
4. `ProgressPersistence` 的 Campaign 聚合最多保留 256 个 Campaign；超出时按最旧
   `last_event_at` 淘汰并标记不完整，适合 1–2 个活动试运行，不适合无限 Campaign 历史。
5. 进程被 SIGKILL/断电时，已观察但未 fsync 的事件无法在重启后证明；系统保持
   at-most-once（不重发），但会把缺失视为 unknown/unresolved gap，而不是补写成功。
6. 本轮未跑 50K、600 s 稳定性、真实 SMSC、生产部署；这些仍需独立授权。

## 8. 机器摘要

```text
SOURCE_HEAD=91257109f16158948d4e74f8b54ec13eab257fde
PRODUCT_CHANGES=per-worker bounded progress actor; non-blocking SMPP reader callback; pre-dispatch reservation; batch fsync off event loop; durable campaign aggregate; read-only vnext_stats CLI; partial journal tail repair
RESPONSE_READER_JOURNAL_DECOUPLED=PASS
SOCKET_WRITTEN_JOURNAL_IO_OFF_EVENT_LOOP=PASS
PROGRESS_QUEUE_BOUNDED=PASS
PREDISPATCH_RESERVATION=PASS
JOURNAL_DURABILITY_AND_CRASH_GAP_HANDLED=PASS
READER_AND_DURABLE_DRAIN=PASS
STAGE_INDEPENDENCE_AND_AUTO_BACKPRESSURE=PASS
CAMPAIGN_PAUSE_RESUME_CANCEL=PASS
AT_MOST_ONCE_NO_REDISPATCH=PASS
CLI_NO_WEB_DEPENDENCY=PASS
CLI_CAMPAIGN_STATS=PASS
CLI_OVERHEAD_PAIRED_PERCENT=NOT_RUN
SLES_1K=NOT_RUN_WINDOWS_WORKSPACE
SLES_5K_PAIRED=NOT_RUN
STAGE_THROUGHPUT_OLD_NEW=NOT_RUN_UNPAIRED
E2E_THROUGHPUT_OLD_NEW=NOT_RUN_UNPAIRED
JOURNAL_AND_PROJECTION_RECONCILIATION=PASS
BACKEND_TESTS=469_PASSED_5_SKIPPED
50K=NOT_RUN_UNLESS_AUTHORIZED
600S=NOT_RUN
CANARY_READY=NO_UNTIL_LIVE_SMSC_PILOT_APPROVED
FINAL_DECISION=IMPLEMENTED_VALIDATED
```

`FINAL_DECISION=IMPLEMENTED_VALIDATED` 表示实现与正确性验证通过；不表示
SLES 配对吞吐或 CLI 开销已经达标，后两项明确为 `NOT_RUN`。
