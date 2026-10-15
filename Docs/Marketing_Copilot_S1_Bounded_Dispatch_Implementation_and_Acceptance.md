# Marketing Copilot S1 有界多 Campaign 派发实施与验收

任务：`MC-SCHED-S1-BOUNDED-DISPATCH-20261015`  
判定日期：2026-10-15  
最终源码：`91a8cc299c5c327091e03fd82d0723ab3e48b79e`  
`origin/main`：同一 SHA  
最终源码 manifest：`.s1-evidence-20261015/source-manifest.json`  
manifest SHA256：`43E1553BF33D068AE0ECDDCDEBB04FA73692122DEA610CE4164CD86F1541C43B`

## 结论

本轮已实现 S1 的有界 Claim 前容量预留、活动 × resolved route ready/prepare 派发、局部背压、共同停止/结果安全入口和幂等资源回收。默认配置仍是 `legacy`；`fair` 由进程启动时冻结的 `MC_CAMPAIGN_DISPATCH_MODE=fair` 显式开启。S1 判定为 **`S1_NO_GO`**，原因是结果 Journal/write-buffer 的最坏 bytes 硬边界尚未被现有格式和实现证明；代码在诊断中明确报告 `result_pipeline_hard_bytes=NOT_VERIFIED`，没有把 `queue.put` 或任务级预留冒充成 durable bytes 门禁。S2 仍不得开始真实 socket 公平宣传。

## 实施边界

`backend/services/bounded_dispatch_service.py` 新增进程内 `legacy|fair` 启动冻结、固定 task/part/byte、活动/route/global、prepare 和保守 result 预算；每个 reservation 包含唯一 ID、owner epoch、task/claim/generation、状态和幂等 release。内存池是可丢失派生视图，DuckDB `sms_tasks` 仍是权威队列。

`backend/services/sms_task_service.py` 增加有界候选 keyset 窗口和 durable cursor；S1 路径先读取候选并取得 reservation，再执行已有单 Claim Dispatcher 事务，最后 publish ready descriptor。Claim 或 publish 异常释放未发布 reservation；已提交但未 publish 的 durable `sending` 不因缓存缺失重排，沿用 stale/fast recovery 的 unknown 证据规则。

`backend/services/task_worker.py` 在 fair 模式使用共享 ReadyPool，prepare 完成后用 `try_send_async` 非阻塞交接；Slot FIFO 满时回到 bounded ready，不占用通用 worker 等待。结果回调使用 reservation 的一次性 completion，只有观察到 Fast checkpoint 和 durable result buffer/Journaling 已排空才释放 result debt；重复 callback/release 计数而不重复结算。停止等待未退出的 worker/dispatcher 时保留旧 epoch 并拒绝 restart。

`backend/services/smpp_gateway.py` 为 fair handoff 增加 resolved route revision 检查、非阻塞 Slot 入队和每个 PDU 写前公共安全检查；fair 模式拒绝直连同步/旧异步 gateway 绕行。`backend/services/sms_sender.py` 和 `rabbitmq_service.py` 在 fair 模式拒绝 RabbitMQ 直发入口。SMPP bind/reconnect 由 fair dispatcher 的启动/周期路径执行，prepare worker 不承担连接等待。

没有新增 DuckDB DDL、Journal 格式、外部 MQ、第二主库 owner、S2 PDU window 或 S3/S4 worker/文件重构；现有 `frontend/package-lock.json` 及其他用户未提交资料保留。

## 状态机与不变量

```text
queued/retrying
  --(resolved route + reservation)--> sending(claim_token, dispatch_protocol=s1_bounded_v1)
  --> ready --> prepare --> handoff --> result_pending --> released
       |         |          |             |
       +---------+----------+-------------+-- cancel/timeout/error: fenced defer/unknown, one release
```

每个 reservation 的 `acquired - released = live`；`used(unit) <= hard limit`；活动 × route 子账与 global 账一致。`route=0` 不能直接获取物理 reservation；发送前使用实际已启用 route/config revision。已写 socket 的 attempt/part 仍由现有 attempt、generation、Journal 和 unknown/partial 合同负责，不承诺外部 exactly-once。

## 配置和入口

| 项目 | legacy | fair |
|---|---|---|
| `MC_CAMPAIGN_DISPATCH_MODE` | 默认；保留现有 Claim/Queue 语义 | 启动冻结；ReadyPool + reservation + non-blocking handoff |
| `MC_FAIR_TASK_SCHEDULER_ENABLED` | 原有 DRR 开关，含义不变 | 不替代新 flag |
| `MC_SMS_SEND_MODE` | reliable/fast 原语义保留 | reliable/fast 均经 S1 reservation；Fast checkpoint 仍保守 unknown |
| Batch/Realtime 常规入口 | 保留 | 覆盖候选/Claim/prepare 路径 |
| Direct synchronous gateway | 保留 | `SmppSubmissionNotAllowedError` |
| RabbitMQ consumer/direct send | 由既有配置控制 | 显式拒绝，未纳入 fair 支持范围 |

Claim/ready/prepare 层单 Campaign A 的拥塞不会传播到有 reservation 且 route 健康的 B/C；A 的 DB 任务留在权威队列或本地有界 ready。共享 DuckDB、Journal、磁盘和 maintenance 故障仍全局 fail-closed。实际 SMPP socket 层尚未完成 Slot/route 公平重构，结论为 **`NOT_VERIFIED`**（S2 依赖）。

## 验收证据

环境：WSL `AlmaLinux-8`、ext4、CPython 3.11、DuckDB 1.2.0、`MC_DUCKDB_THREADS=1`；loopback/mock SMSC；无真实号码、真实 SMSC 或生产数据库。

| 门禁 | 命令/证据 | 结果 |
|---|---|---|
| S1 定向及相关回归 | `pytest test_s1_bounded_dispatch.py test_multi_campaign_queue_isolation.py test_multi_campaign_fair_scheduling.py test_fast_send.py test_startup_recovery.py`；`/tmp/mc-s1-20261015/evidence/targeted-current.log` | `62 passed, 4 warnings`，exit 0 |
| 最终源码 G0 | `MC_DUCKDB_WRITE_CONCURRENCY=serialized pytest backend/tests -q -p no:cacheprovider --basetemp=/tmp/mc-s1-20261015/pytest-g0-r2`；`/tmp/mc-s1-20261015/evidence/g0-final-r2.log` | `331 passed, 2 skipped, 6 warnings`，exit 0 |
| fair loopback E2E | `MC_CAMPAIGN_DISPATCH_MODE=fair MC_SMS_SEND_MODE=reliable local_e2e_acceptance.py --scenario quick --gate full ...`；`/tmp/mc-s1-20261015/evidence/e2e-fair-quick-r3.json` | PASS；3 tasks accepted，3 submit_sm/3 success，1 file/1 chunk completed，0 duplicate/unknown retry |
| frontend | `npm run build`（重试使用允许 Node spawn 的执行环境） | exit 0；1794 modules transformed；仅既有 Rolldown annotation/chunk-size warnings |
| Windows pytest | `.p0-test-venv` | 未计门禁；业务断言通过后被 Windows ACL 清理 basetemp 的 `PermissionError` 终止，已用 WSL ext4 重跑 |

确定性测试还覆盖：A ready/credit 饱和时 B/C claim/prepare；同/异 route；候选窗口外活动轮转；task/part/byte/global/per-group 限额；claim commit/publish 注入失败；进程退出后 durable sending 不按内存缺失重发；重复 callback/double release；worker stop epoch；route revision 变化；公共 disk/Journal/DB maintenance fail-closed；RabbitMQ/direct bypass；ready retry 和结果交接失败。

## 阻断项和风险

1. **结果容量硬边界（S1 阻断）**：现有 Journal `load_state`、write-buffer pending/retry heap 和记录 bytes 没有与本次 reservation 账本形成可验证的全局最坏 bytes 原子门禁。fair 只采用保守任务级 result reservation，并在 drain 后释放；诊断明确为 `result_pipeline_hard_bytes=NOT_VERIFIED`。在补齐 Journal/write-buffer 分层 bytes 限额、故障注入和 recovery debt 对账前，不能升级为 `S1_READY`。
2. **socket 公平**：旧 Slot FIFO、route TPS 和 PDU window 仍是 S2 范围；公平只在 dispatch/prepare/handoff 层成立，不能宣称 Campaign socket 隔离。
3. **RabbitMQ**：fair 模式显式拒绝可选 RabbitMQ 直发入口；如果生产必须启用，需在 S2 前接入同一 reservation/attempt/Journal 合同并重跑全入口门禁。
4. 真实 SMSC、SLES12SP5、G3 四组合 × 3、长稳 600 秒均未执行，保持 `NOT_VERIFIED/NO_GO`。

## 回滚

重启时保持 `MC_CAMPAIGN_DISPATCH_MODE=legacy` 即可回到旧派发路径；不修改数据库、WAL、Journal、上传文件或未确认任务。切换前先停 intake/claim，等待旧 worker epoch 完全退出；若超时，进程拒绝 restart，按现有 durable claim/attempt recovery 继续处理。fair 生成的 `sending` 不因切回 legacy 而自动整条重发，unknown/partial 证据优先。

## 最终判定

**`S1_NO_GO`**。S1 dispatch/prepare 增量、legacy 回退、全后端回归和 loopback fair 小型 E2E 已有证据；结果 Journal/write-buffer 的全局最坏容量尚未证明，且实际 socket 公平属于 S2。补齐结果容量硬门禁并完成对应故障恢复与审计后，方可重新评估 S1_READY，再进入 S2。
