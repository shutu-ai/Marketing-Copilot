# SMS Result Write Optimization Acceptance

日期：2026-10-14  
基线：`a3440e57824c2452e1ee934e5fe03c631bb0f11d`  
实现：`UNCOMMITTED`（当前工作树）

## Phase 0：性能画像与最小复现

状态：`PASS_WITH_LIMITATIONS`

- 已沿 `smpp_gateway → sms_task_service → sms_write_buffer → result_journal` 和 `realtime_ingestion_worker → backpressure_service → realtime_campaign_service` 核对调用顺序。
- 结果事务保留 event-id 幂等门禁；Journal append/fsync 仍在进入异步处理前完成，DB commit 后才写 terminal marker。
- 增加了结果写入阶段 histogram，阶段不包含手机号、内容、凭据或完整 JSON。
- 提供了独立 DuckDB 的 1K/3K no-DLR 与含 attempt/统计基准；生产锁等待数字未重跑，记录在 `SMS-Result-Write-Optimization-Baseline.md`。

## Phase 1：SMS Result Batch 写入

状态：`PASS`

代码行为：

1. `sms_tasks` 原有集合更新和 event-id fence 保留；成功任务仍要求当前 claim。
2. payload 含 attempt token 时，任务完成额外要求 `sms_send_attempts` 中存在匹配的 task/claim/attempt；stale callback 不得完成当前任务。
3. `sms_send_attempts` 改为集合式 `UPDATE ... FROM (VALUES ...)`，只更新真正由任务状态转移成功返回的 task。
4. `batch_campaign_batches`、`batch_campaign_daily_stats` 改为按聚合 delta 的集合更新；统计不会因 event replay 再次增加。
5. 多 campaign/batch event summary 可合并为一条 INSERT；单组仍走既有 helper 兼容最小 schema 测试。
6. no-DLR 只减少成功 message-id 映射和宽 task JSON 更新；失败、Partial、Unknown、required/optional DLR 仍走原有完整路径。

安全断言：`submit_sm_resp` 仍由 SMPP 层消费和计数；未响应、断连和 delivery unknown 不会被 no-DLR 快路径变成 accepted，也不会自动重发。

## Phase 2：实时摄取恢复斜坡

状态：`PASS_WITH_LIMITATIONS`

- 保留 `TASK_HIGH/TASK_LOW`、WRITE、CHUNK、disk、Journal unhealthy 的原有硬暂停。
- 任务高水位曾经触发后，暂停解除时启用进程内共享 token bucket；所有 realtime worker 共享预算，chunk 入库前按匹配记录数 admission。
- 预算依据写队列深度、最老 pending age、重试深度和当前 DuckDB gate 持有年龄动态降速；无压力时逐步提升，且不阻塞已经发生的结果落库或发送状态更新。
- 支持关闭：`MC_REALTIME_INGEST_RAMP_ENABLED=0`。参数：`MC_REALTIME_INGEST_RAMP_INITIAL_TPS`、`MC_REALTIME_INGEST_RAMP_MAX_TPS`、`MC_REALTIME_INGEST_RAMP_MIN_TPS`、`MC_REALTIME_INGEST_RAMP_BURST_SECONDS`。
- `realtime_ingestion_worker` 的 lease、chunk checkpoint、跨重启文件恢复、campaign round-robin 未改写。

已验证共享预算和 stop-aware 等待；未在高水位持续积压、4 worker 实际文件洪峰下采集长时间写锁数据，因此“避免约 3 SMS/s 崩塌”仍需现地验收。

## Phase 3：故障注入与语义验收

状态：`PASS`（自动化覆盖范围有限）

已覆盖并通过：

- no-DLR accepted、集合状态更新、attempt accepted、batch/daily 统计各增加一次；
- stale claim/attempt 不得越权完成任务；
- required/optional/disabled 状态差异；
- Flash SMS 单分片与 multipart PDU 参数；
- multipart 等待所有响应、明确拒绝、Partial、Unknown/断连不自动重发；
- DuckDB 事务冲突整事务重试；
- DB commit 后 terminal marker 注入失败，Journal replay 幂等且 task/event summary 不重复；
- Journal terminal ack outage 分组和有限等待；
- realtime file lease 单 owner、过期恢复、chunk checkpoint、并发 campaign round-robin；
- 摄取 stop/capacity pause 不写 retry 状态。

尚未在本地自动化中模拟操作系统级强杀进程、真实 socket 中断后的全进程重启和 10K 混合负载，标为 `NOT_RUN`，不伪报 PASS。

## Phase 4：同负载 A/B

状态：`PASS_WITH_LIMITATIONS`

1K/3K 独立数据库 A/B 均达到 100% accepted、100% result event、100% attempt/batch/daily 计数一致，且没有观察到重复 event-id。单线程 benchmark 的耗时差异不稳定：no-DLR 1K `15.126s → 15.406s`、3K `45.242s → 45.177s`；含 secondary writes 1K `15.609s → 15.413s`、3K `45.568s → 45.699s`。

这组数据不能证明吞吐提升，但证明了优化没有通过放大事务或跳过安全证据来换取数字。需要在固定 4 SMPP slots、并发 callback、混合 DLR/Flash/multipart、摄取洪峰的环境重新采集：业务 SMS/s、parts/s、gate wait/hold P50/P95/P99、timeouts、operation Top-N、Journal oldest age、queue depth slope、CPU/RSS/I/O/RTT。

## Phase 5：交付与回滚

- 代码注释解释了 no-DLR `accepted` 与 DLR 的区别，以及 `submit_sm_resp` 不可跳过。
- 新斜坡策略没有外部消息系统依赖，有关闭开关和 gauges。
- 本次没有修改生产配置、生产数据库、真实 SMSC，也没有生成部署包或执行 SLES12SP5 现地升级。
- 回滚优先设置 `MC_REALTIME_INGEST_RAMP_ENABLED=0` 并重启应用使配置生效；结果写入集合 SQL 需要代码回滚。不得清理 Journal 或删除 pending 结果。

## 验证命令

```text
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
```

结果：`154 passed, 3 skipped in 148.99s (0:02:28)`。

## 限制与后续验收

`SLES12SP5`、`LIVE_SMSC`、10K、长期 soak 和生产锁竞争均未在本次工作区执行。部署前应在独立 SLES12SP5 数据副本上使用现有离线包流程验证；生产升级时先备份并确认 Journal、DuckDB 和 runtime data 不在部署包内。
