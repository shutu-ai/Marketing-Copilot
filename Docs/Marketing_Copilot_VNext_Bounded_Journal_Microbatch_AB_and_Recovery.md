# Marketing Copilot VNext Bounded Journal Microbatch A/B and Recovery Closure

任务：`MC-VNEXT-SERVER-FIRST-PERFORMANCE-MICROBATCH-20261018`

结论：`NO_CHANGE_WITH_EVIDENCE`，并标注 `UPSTREAM_OR_HARNESS_LIMITED`。R2 要求“服务器证据证明 Writer 小 batch 是 E2E 实质瓶颈”才允许进入 microbatch A/B。本次服务器 50K C0 不满足该条件：Writer underfill 存在，但 response-window/TPS-credit wait 占据供给等待，Projection 在最后 response 后约 46.6 ms 完成 DB commit。因此 B1/B2/B3/B4 没有被错误地当成可比实验执行，也没有新增 bounded coalesce 实现。

## R2 gate decision

| Gate | 结果 | 证据 |
|---|---|---|
| B0 baseline | PASS | Server 50K C0：batch configured `512`，actual p50/p95 `58/188`，100,000 events，1,397 commits，backlog `0` |
| `WRITER_UNDERFILL_IS_BOTTLENECK` | NO | staging p50/p95 `8.132/16.304 ms`；response Journal durable → DB commit `46.584 ms`；E2E 主等待为 TPS credit/response window |
| B1 A0/512/10 ms | NOT RUN | R2 trigger 未满足 |
| B2 A0/512/25 ms | NOT RUN | R2 trigger 未满足 |
| B3 A0/512/50 ms | NOT RUN | R2 trigger 未满足 |
| B4 A0/1024 | NOT RUN | 只能在 B0→B1→B2→B3 有证据且 fill 条件明确时进入 |
| paired 50K for winner | NOT RUN | 没有 winning arm |
| `WINNING_ARM` | `NONE` | 不存在 candidate delta |

本次不允许为了“完成表格”而引入 100 ms、任意等待或默认配置变更。默认 `flush_interval=0` 的逐事件 durability、单 Writer、单 DuckDB owner、dedup、at-most-once 和 cross-day 语义均保持不变。

## C0 server Writer capacity evidence

纯 Writer benchmark 用同一服务器、同一 CPython 3.11.4、DuckDB 1.2.0、A0、vectorized、batch 512：

| Run | 结果 | wall | events/s | read/new/replayed | batch p50/p95 | RSS peak |
|---|---|---:|---:|---:|---:|---:|
| 100K, duplicate fraction 0.1 | PASS | `11.419 s` | `8,756.964` | `100,000 / 90,002 / 9,998` | `512 / 512` | `171,716 KiB` |
| 500K, duplicate fraction 0.3 | PASS | `63.928 s` | `7,821.304` | `500,000 / 333,334 / 166,666` | `512 / 512` | `207,628 KiB` |

这两组结果证明当前 Writer/Projection 语义和容量在独立 workload 下正确且有余量；它们不替代 E2E 供给瓶颈判断，也不构成 microbatch candidate。

## Recovery closure

### T6 cross-day fault acceptance

服务器 T6 结果为 `PASS`，business timezone `UTC`，old day/new day 各注入 8 条：

- old enqueue accepted `8`，new enqueue accepted `8`；
- cutoff 后 old-day socket PDU `0`；
- restart 后 old-day redispatch `0`；
- new-day stats contamination `0`；
- pending records `0`，recovery issues `[]`；
- cutoff、restart、service-level 和 stats-contamination 证据标志均为 true。

### T7 current-source matrix

当前 source 的 `vnext_t7_recovery_matrix.py` 只提供以下六个场景，服务器 native `ext4` 上 `6/6 PASS`：

`T7-4`、`T7-5`、`T7-6`、`T7-7`、`T7-8`、`T7-9`。

本轮明确记录 `T7_CURRENT=6/6`。当前源代码没有 T7-1、T7-2、T7-3 的 runner/contract，因此不把它们补写成 PASS，也不宣称历史意义上的 9/9。

## Harness observation

5K C0 使用默认 projection status interval `1.0 s` 时，parent 在 `308.136 s` 后报告 `projection_drain_timeout`；raw result 已经有完整 5,000 response、10,000 Journal/Projection、backlog `0`、lag `0`，只是 `projection_caught_up_before_stop=false`。同 workload 显式改为 status interval `0.1 s`、timeout `30 s` 后 `12.657 s PASS`。这是一项 stop-condition/status freshness 观察，不是产品数据丢失或重复发送证据；50K 正式基线采用 `0.1 s`。

## Recovery decision

| Item | Decision |
|---|---|
| code change | `NONE` |
| default change | `NONE` |
| microbatch candidate | `NONE` |
| recovery | T6 PASS；T7 current 6/6 PASS |
| final state | `NO_CHANGE_WITH_EVIDENCE` / `UPSTREAM_OR_HARNESS_LIMITED` |
| commit/push | `NOT_REQUESTED` |

