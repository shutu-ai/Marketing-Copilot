# Fast Send mode architecture decision

状态：`CONDITIONAL_ACCEPTANCE / NO_PRODUCTION_CUTOVER`  
日期：2026-10-14

## Decision

保留两条明确的发送模式：

```text
MC_SMS_SEND_MODE unset/reliable  -> Reliable (默认、生产允许)
MC_SMS_SEND_MODE=fast            -> Fast (实验、单进程、需专项验收)
```

模式在 worker 启动时冻结；运行中修改环境变量不切换模式。Fast 在发现 `MC_ALLOW_MULTI_INSTANCE=1` 时 fail fast。此次实现不迁移既有 Send Store，不改变 Reliable 的 durable intent/result 语义。

## Why

Reliable 的正常发送路径把 claim、attempt、part response 和结果写入组织成可恢复边界，适合默认生产行为。Fast 只压缩**正常成功响应的持久化频率**：批量 claim 时先持久化 owner/batch/attempt 身份，socket 响应期间只在内存中累积事实，之后通过现有结果 journal 和 writer 一次批量 checkpoint。

Fast 不能把“socket write 已发生但 response 未知”转换成 accepted，也不能把 unknown 自动重发为追求吞吐。这个约束比单次吞吐数字优先级高。

## Invariants

1. **Default safety**：未配置或配置非法时不会默默进入 Fast；Reliable 是默认。
2. **Frozen mode**：一个进程的 worker 生命周期内只使用一个 mode。
3. **Durable ownership**：Fast claim 在一个 DuckDB transaction 中写入 `sms_fast_batches`、`sms_fast_batch_items` 和 authorized `sms_send_attempts`。
4. **No unsafe resend**：restart、timeout、connection loss、socket-admitted-but-unconfirmed 和 partial 均不自动重发。
5. **Linearizable control**：campaign pause/cancel/complete 与新 socket admission 共享短生命周期 fence；已进入 socket 的 claim 不会因 `planned_parts=0` 被错误重排。
6. **Batch checkpoint only for normal success**：Fast success 从 SMPP response 线程进入有界内存队列，再由 journal-backed writer 批量落库；异常结果立即进入 durable failure path。
7. **Truthful states**：`socket_submitted`、SMSC submit response accepted、DLR delivered 分开表达；partial/unknown 不被伪装成 accepted/delivered。
8. **Backpressure**：checkpoint queue、result writer durable backlog 或 journal 异常达到阈值时停止新的 Fast admission。
9. **Single-process boundary**：当前 stop fence 是进程内；多实例 Fast 被拒绝，除非未来提供跨实例 fencing/lease 服务。

## Rejected alternatives

- **仅取消逐任务 DB 写而不保留 batch owner**：重启后无法证明 claim 是否已到达 SMSC，违反 at-most-once 边界。
- **超时后切备用 route 自动重试**：会把 unknown 变成潜在 duplicate。
- **仅依赖内存 `planned_parts` 判断 pause 安全**：checkpoint 前该字段可能仍为 0，已进入 socket 的任务可能被错误 requeue；已修正为 task fence + conservative fallback。
- **把 loopback SMSC 速率直接当真实运营商 TPS**：无法证明真实链路、窗口、DLR 和网络抖动。

## Operational contract

生产配置保持：

```text
MC_SMS_SEND_MODE=reliable
```

Fast 只能在隔离单实例、loopback 或经批准的 canary route 使用。必须能观测 checkpoint lag、unresolved、unknown、partial、duplicate mapping、SMSC response 和 DLR。没有真实 SMSC + SLES12SP5 + 长稳态结果前，不做生产 cutover。

## Consequences

正面效果是 Fast 正常成功响应不再在 SMPP 线程中逐任务写 journal/DB，且已有 batch result writer 可复用。代价是：

- Fast 对进程崩溃窗口采用 unknown，而不是自动恢复；
- 正常成功仍需最终 checkpoint，不能把“内存已收集”当成 durable accepted；
- 单进程 fencing 是硬约束；
- 当前 1K tuned 本地压测超时，性能验收仍是 `NO_GO`。

下一步只有在修复并重跑等窗口 A/B 后，才可考虑扩大 canary；本交付不包含生产切换。
