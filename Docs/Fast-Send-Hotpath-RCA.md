# Fast Send hot-path RCA

日期：2026-10-14  
基线：`6b498a597aca9bf0d9a311f0cfac85b1087d4a51`  
实现状态：工作树未提交；未 push。Fast Send 是显式 opt-in 的实验路径。

## 结论

Fast Send 已接入真实本地 loopback SMPP、批量 claim、durable batch ownership、内存成功聚合、批量 checkpoint、停止栅栏和保守恢复策略。功能性与小样本故障路径通过，但两次 1K tuned 压测在 Windows 本地环境中均在 600 秒超时：第一次为 200 个任务中 119 accepted/61 sending/20 queued，checkpoint batching 优化后的复跑为 80 accepted/20 sending/100 queued。因此当前结论是：

**Fast Send 不能进入生产，也不能宣称已通过高吞吐性能验收；默认继续使用 Reliable。**

外部真实 SMSC、SLES12SP5、长稳态 soak、1K/3K/10K/50K 的等窗口 A/B 均未完成或未验证。

## 根因与路径对比

### Reliable baseline

```text
claim task
  -> async sender
  -> per-task durable send_intent / sms_send_parts authorization
  -> submit_sm socket write
  -> submit_sm_resp
  -> per-part result journal event + fsync
  -> buffered DuckDB task/part update
```

Reliable 的优势是每次外部提交前都有可恢复的 durable attempt；代价是正常成功路径仍会产生较多逐任务/逐 part 的持久化工作。

### Fast experimental path

```text
one claim transaction
  -> sms_fast_batches + sms_fast_batch_items + authorized attempt identities
  -> local campaign stop fence / window / backpressure admission
  -> submit_sm socket write
  -> in-memory per-part facts
  -> one bounded fast_checkpoint journal event for many task outcomes
  -> one bounded DuckDB transaction
```

Fast 的 durable ownership 在 claim 时建立；正常 socket/response 线程不调用 `record_send_intent`，也不逐任务 fsync。成功事实在有限内存队列中聚合，随后经现有 journal-backed writer 批量落库。拒绝、partial、连接断开、响应超时和 checkpoint 失败走 durable anomaly path。

## 已确认的问题与保护

| 场景 | 事实边界 | 处理 |
|---|---|---|
| socket admission 前被 pause/cancel | 未写 socket | 仅当本进程能明确证明 claim 尚未进入 socket 才 requeue/cancel |
| socket 已 admission 或已写出 | 可能到达 SMSC | 保留 claim；不自动重发 |
| submit_sm_resp 明确 reject | SMSC 明确拒绝 | `failed`，可按现有人工策略处理 |
| multipart 部分成功 | 至少一个 part 已接受 | `partial`，保存每个已知 part，禁止整条短信自动重发 |
| response timeout / connection loss | accepted 状态未知 | `unknown / delivery_unknown`，不自动重发 |
| process restart 时存在 Fast claim | 外部提交状态未知 | 启动恢复统一标记 `unknown`，不 reclaim |
| checkpoint queue/backlog 超阈值 | durable success 未建立 | Fast fail-closed；新 socket admission 被拒绝 |
| DLR | 仅说明 delivered 状态，不改变 submit uncertainty | 继续使用 source namespace + message-id 映射；partial 不被晚到 DLR 晋级为 delivered |

2026-10-14 的停止栅栏修订移除了“仅凭 `planned_parts=0` 即可重排”的不安全推断：Fast item 的 planned 字段在正常热路径直到 checkpoint 才持久化更新，因此只有本进程 task fence 明确记录为未进入 socket 的 claim 才可立即重排；缺证据时保守保留或在重启时标记 unknown。

## 运行参数与观测

- `MC_SMS_SEND_MODE=reliable|fast`：进程启动时冻结；缺省为 `reliable`。
- `MC_ALLOW_MULTI_INSTANCE=1`：Fast 启动时拒绝，避免本地 stop fence 被多实例绕过。
- `MC_FAST_SEND_CHECKPOINT_QUEUE`：内存成功聚合容量，默认 10000。
- `MC_FAST_SEND_CHECKPOINT_BATCH`：checkpoint event 的任务上限，默认 250。
- `MC_FAST_SEND_CHECKPOINT_MS`：checkpoint interval，默认 100 ms。
- `MC_FAST_SEND_MAX_DURABLE_BACKLOG`：durable writer/backlog 安全阈值，默认 2000。
- `MC_SMPP_WINDOW_SIZE`、`MC_SMPP_SEND_QUEUE_CAPACITY`、route TPS、worker 数和 pool size 必须随性能结果一起记录。
- `/api/metrics/status` 增加 `fast_send`，包括 pending、lag、backpressure、fail-closed、unresolved 和 uncertain-after-claim policy。

关键业务指标必须同时看 `submit_sm_sent`、`submit_sm_resp`、`accepted/submitted/delivered/partial/unknown`、SMSC event log、duplicate mapping 和 checkpoint lag；不能用单一 socket 写入速率代替最终状态。

## 证据等级

- `SOURCE_CONFIRMED`：代码、schema、单元测试和静态检查可复核。
- `LOCAL_E2E_MEASURED`：本机 loopback-only SMSC 的真实 API + DuckDB + SMPP 流程。
- `PERFORMANCE_BLOCKED`：压测有明确失败/超时，不得包装成通过。
- `LIVE_ENV_UNVERIFIED`：真实 SMSC、SLES12SP5 或生产网络尚未执行。

完整的数字结果见 [fast-send-performance-results.json](fast-send-performance-results.json)，验收策略见 [Fast-Send-Performance-Acceptance.md](Fast-Send-Performance-Acceptance.md)。
