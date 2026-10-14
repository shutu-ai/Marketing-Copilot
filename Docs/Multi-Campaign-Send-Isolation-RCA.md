# 多活动发送隔离 RCA

日期：2026-10-14  
实际基线：`f391868` (`main`，SMPP Hotpath 任务后的真实 HEAD)  
工作区实现状态：未提交；没有连接真实 SMSC，也没有修改生产数据库或生产配置。

## 结论摘要

本次离线 50,000 + 1,000 + 1,000 任务重现通过了 Track A 的正确性门禁：暂停 A 后，A 的 50,000 条仍为 `queued`，B/C 都继续被持久 Claim；暂停前已经 Claim 但尚未授权的 A 任务在 `record_send_intent` 前被拒绝；模拟 socket 只对 B/C 产生出站调用。

旧版“活动 A 停止后 B/C 也停发”的生产因果链没有在本地复刻真实 SMSC 现场，因此不能标记为 `OBSERVED_PRODUCTION` 或宣称旧现场 RCA 已被完全证实。源码路径是 `SOURCE_CONFIRMED`，离线修复后的隔离不变量是 `REPRODUCED`；真实 SMSC 速率、连接窗口和生产采样仍是 `MISSING`。

## 证据等级

| ID | 等级 | 事实 |
|---|---|---|
| EVD-A01 | SOURCE_CONFIRMED | 原基线的 `claim_next_tasks()` 仅以任务字段为主要候选条件，未在公平和 FIFO 路径中校验活动状态。 |
| EVD-A02 | SOURCE_CONFIRMED | 原 `p1_service.campaign_action()` 先提交活动状态，再用另一事务处理任务，存在停止与 Claim 的时序窗口。 |
| EVD-A03 | SOURCE_CONFIRMED | 原 Worker 在 `campaign is not enabled` 分支可把任务标成 `expired`；暂停任务因此不具备可恢复语义。 |
| EVD-A04 | SOURCE_CONFIRMED | 每个 SMPP 连接使用共享 FIFO `_send_queue`；原路径没有活动代际失效机制。 |
| EVD-A05 | SOURCE_CONFIRMED | 主 DuckDB 的应用写入仍由进程内 `_write_lock` 串行化；分片 PoC 不改变生产路径。 |
| EVD-A06 | REPRODUCED | `backend/tools/multi_campaign_isolation_acceptance.py` 在 50K/1K/1K 离线数据库中复现暂停前 Claim、暂停后授权拒绝以及 B/C 继续 Claim。 |
| EVD-A07 | REPRODUCED | `test_fake_socket_continues_b_after_a_pause_without_real_smsc` 使用 fake client 验证 A 不写 socket、B 能写 socket；未连接网络。 |
| EVD-A08 | HYPOTHESIS | 生产观察中的 B/C 长时间停发可能由共享 Claim/任务队列/per-slot FIFO 与 DuckDB 写竞争共同放大；本地没有生产指标或现场时线证明各层的最终占比。 |

## 原基线调用链和故障边界

```text
queued/retrying
  -> sms_task_service.claim_next_tasks()
  -> task_worker claim dispatcher / bounded task queue
  -> SmppGateway -> per-slot _send_queue
  -> record_send_intent()  [durable claim + campaign fence]
  -> submit_sm socket write
  -> submit_sm_resp
  -> result journal / sms_write_buffer
  -> sms_tasks + attempts + parts
```

关键边界：

1. 持久候选层如果把暂停活动选入，A 会先占用 `sending`、进程内队列和 per-slot FIFO，随后才在 Worker 或发送前失败；旧路径的失败发生得太晚，可能造成无效 Claim/Defer 循环和队列头阻塞。
2. 活动控制和任务清理若分属两个提交，Claim 可以在活动状态已经变化但任务清理尚未完成的窗口内成功。
3. 发送队列不是授权边界。只有 `record_send_intent()` 成功创建 durable attempt/parts 后，才允许 socket 写；Unknown/Partial 不会自动重发。

## 状态和代际语义

| 活动状态 | 未授权任务 | 已授权/已 socket 任务 |
|---|---|---|
| `in_progress` | 可在窗口内 Claim | 继续按 attempt/part 证据处理 |
| `paused` | Claim 排除；无 attempt 的 `sending` 回到 `queued`；恢复时使用新 generation | 保留 `sending` 和 attempt，等待响应/结果，不被强行取消 |
| `cancelled` | 以 `cancelled` 终结 | 保留已授权证据，禁止凭停止状态重发 |
| `completed` | 以 `expired` 终结未授权任务 | 保留已授权证据，等待正常结果折叠 |
| 日窗口关闭 | 不在 Claim 候选；最终发送前仍以 Python schedule validator 为准 | 已授权任务不因窗口变化被推断为失败 |

`dispatch_generation` 同时存在于活动和任务。活动控制在同一 DuckDB 写事务中递增 generation 并协调任务；Claim 查询和实际 `record_send_intent` 都比较两者。内存队列只把旧 generation 当作快速失效提示，数据库 fence 才是权威。

## Track A 修复

- `backend/database.py`：增加活动/任务 `dispatch_generation`、迁移和候选索引。
- `backend/services/sms_task_service.py`：公平/FIFO 候选与 `UPDATE ... RETURNING` 都加入活动存在、`in_progress`、窗口/日期和 generation fence；状态转换与未授权任务回收在同一事务执行；授权再次读取活动和 generation；增加活动级 Claim/预取/授权/失效观测。
- `backend/services/batch_campaign_service.py`、`realtime_campaign_service.py`：PUT 状态变更与任务协调在同一写事务，直接 API 与 Operations API 共享语义。
- `backend/services/p1_service.py`：pause/cancel/complete/resume 使用 CAS 状态转换，不再“先活动、后任务”双提交；控制完成后通知 live SMPP pool 失效指定活动代际。
- `backend/services/task_worker.py`：暂停/暂不可发送不再被误标为 `expired`，无授权任务立即可恢复排队。
- `backend/services/smpp_gateway.py`：每 slot 保存活动 generation 失效标识，发送线程在取出队列项后、授权前快速丢弃旧活动项；不 flush 其他活动、不 disconnect 全局、不触碰已有 attempt。

## 未知项和边界

- 本地没有生产日志、SLES12SP5 现场指标或真实 SMSC 响应，所以不能拆分生产停发的 route TPS、SMSC window、数据库锁或队列等待占比。
- 50K/1K/1K 工具验证的是 durable Claim/fence/task flow 和 fake socket；未声称 4 slot/8 worker 的真实 `submit_sm` 吞吐。
- Track B 的跨数据库 stop fence、outbox/inbox handoff 和迁移恢复契约尚未作为生产能力实现，见架构决策文档。
