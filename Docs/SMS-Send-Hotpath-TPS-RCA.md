# SMPP Send Hotpath TPS RCA

状态：`PASS_WITH_LIMITATIONS`（代码审计、隔离基线和 focused 回归已完成；生产只读采样、正式三轮 A/B、SLES 和真实 SMSC 未执行）。

## 范围与安全边界

- 执行基线：`398e0e8b0d06cd774480f8240333406857d01459`，当前工作树未提交实现。
- 所有自动化 SMPP 测试均使用 loopback-only fake SMSC、隔离 DuckDB 和虚拟号码；未连接真实 SMSC，未修改生产配置。
- 现网 JSONL 指标路径 `paas-core01:/opt/marketing-copilot/mc-metrics-20261014-150142.jsonl` 是任务提供的外部证据，本环境未读取该主机，故相关数字标为 `EXTERNAL_PROVIDED_NOT_REPLAYED`。

## 发送链路

```text
sms_tasks queued/retrying
        │  claim_next_tasks()  ── task claim transaction / queue
        ▼
bounded dispatcher queue
        │  gateway.send_async()
        ▼
per-route pool ── per-slot bounded send queue ── one sender thread/slot
        │
        ├─ SMPP window.acquire()
        ├─ connect/bind
        ├─ record_send_intent()
        │    ├─ claim + campaign active fence
        │    ├─ durable sms_send_attempts
        │    ├─ durable planned sms_send_parts
        │    └─ durable submit_authorized event
        ├─ shared route TPS reservation
        └─ socket submit_sm ── submit_sm_resp reader/window release
                                      │
                                      └─ result journal → batched DuckDB write
```

关键可靠性约束仍然成立：授权和 attempt/claim fence 在 socket write 之前；超时/断连保留 `unknown`；multipart 混合结果保留 `partial`；结果先进入 durable journal，再异步回写 DuckDB。

## 事实与归因

### 代码事实

1. `backend/services/smpp_gateway.py` 的 `_SHARED_TPS_NEXT` 按 `config_id` 共享，`pool_size` 不会乘大路由 TPS；每个 Slot 只有一个 sender loop，但 SMPP window 仍允许多个 task/PDU 在响应前飞行。
2. `record_send_intent()` 原来在每个任务事务中逐 part INSERT，并额外写一次 fairness `last_submit_at`。该 fairness 字段只被容量展示读取，不是发送 fence。
3. `/api/smpp/stats` 原来将 `tps * pool_size` 暴露为 `configured_tps`，与实际共享 route throttle 语义不一致。
4. `database.write_transaction()` 已提供 `sms_task.submit_authorize` 的事务、锁等待、锁持有和重试指标；gateway 已提供 queue/window/throttle/socket/response 指标；result journal 和 result batch 已提供 fsync/batch 指标。

### 隔离测量

| 测量 | 基线 | 当前实现 | 解释 |
|---|---:|---:|---|
| 环境 | Windows + `.p0-test-venv` + 独立 DuckDB | 同机、同脚本、同 1000 历史任务 | 非生产证明 |
| iterations | 50 | 50 | 仅一轮，不能作为正式 A/B |
| planned parts | 1 | 1 | 另有 4-part 语句回归 |
| submit_authorize mean | 190.997 ms | 169.238 ms | 约 11.4% 的单轮改善，需三轮复测 |
| submit_authorize P95 | 242.876 ms | 218.627 ms | 抖动仍明显 |
| writer lock hold mean | 47.22 ms | 43.59 ms | 仅观察样本 |
| INSERT 语句数 | 350 | 300 | 每 50 次授权少一个 fairness 写入 |
| multipart planned-part INSERT | 每 part 一条 | 每 task 一条 set-based INSERT | 4-part 由测试验证 |

证据文件：`.tmp/smpp-hotpath-baseline-50-20261014.duckdb`、`.tmp/smpp-hotpath-optimized-auth-20261014.duckdb`；运行工具：`backend/tools/live_duckdb_hotpath_benchmark.py`。

第一次 loopback `stress-1k`（fairness 写入仍在授权事务内）完成了 1000/1000 accepted、1000/1000 submit_sm、无重复/丢失，但吞吐只有 `3.558 SMS/s`，`task_creation_to_submit_sm_resp` P50 `122709.429 ms`、队列峰值 924；最终实现重跑为 `3.794 SMS/s`、P50 `118095.008 ms`、队列峰值 916。该差异只有一次运行且全部单 part，证明发送链路在高积压时受限，但不能当作正式 A/B 或 multipart 性能恢复。

### Top 3 阻塞原因

| 排名 | 阻塞阶段 | 证据 | 可信度 | 可能收益 | 是否改代码 |
|---:|---|---|---|---|---|
| 1 | DuckDB `sms_task.submit_authorize` writer gate | 外部现网报告授权锁等待 `127–153ms`；隔离基线授权均值 `190.997ms`，优化后 `169.238ms`；本地 1K 队列峰值 924 | 高（本地可复现；现网数字未重放） | 中等；减少每任务写放大并把非 fence metadata 移出授权 | 是 |
| 2 | 发送 route/Slot 背压与单 sender | 现网提供的四 Slot queue `198–200/200`；代码为每 Slot 一个 sender queue；本地 1K 观察队列峰值 924 | 中高 | 需要结合最终 window/queue 指标；不能据此提高 TPS | 本轮只补指标，未改可靠性拓扑 |
| 3 | SMPP window/响应 RTT 与实际 route quota | 外部现网 inflight 多为 `0–5`，但 `window_size`、RTT、账号 quota 未在本环境核实；本地 fake 只能证明 loopback 行为 | 中 | 可能成为剩余瓶颈；收益未证明 | 不采用未经证据的 window/配额重构 |

## 采用的最小实现

- `sms_send_parts` 由 Python 逐行写改为 DuckDB 一条 `INSERT ... SELECT range(?)`，仍在 durable authorization 事务中，仍有 attempt token、claim token、TTL 和 fencing。
- fairness `last_submit_at` 从 pre-wire authorization 移到结果批次，按 campaign/route 去重后写入；它不是发送授权条件，不扩大恢复窗口。
- 授权新增总耗时、success/denied/timeout 指标；dispatcher 补 queue enqueue wait/full 指标。
- route TPS 仍是共享上限；`MC_SMPP_INTERNAL_TPS_MULTIPLIER` 只允许降速，超过 1 被限制为 1，不能作为隐藏配额倍率。
- API/UI 显式区分 `configured_route_tps`、`configured_per_bind_tps`、`effective_total_limit_tps`、`observed_parts_tps`，单位固定为 `submit_sm_parts/s`。无法证明 per-Bind 配额，保留 `route_shared`。

## 未采用方案

本轮未采用批量预授权、授权执行器解耦、新消息队列、数据库替换或增大生产 TPS。原因是当前已验证的最小写放大改善尚未取得正式三轮 A/B；批量预授权还需要独立的有限 token 窗口、过期清理、重启恢复和撤销证明，不能用来掩盖 route quota 或 SMSC RTT。

## 结论

当前最可信根因是发送前 DuckDB 授权事务的 writer-gate/写放大与发送队列背压共同限制了热路径；route quota 与 SMSC RTT 尚未被隔离证据排除。代码改善已验证语义和局部授权指标，但整体 TPS 恢复仍为 `NOT_PROVEN`，不得宣称 200/500 parts/s 或生产能力提升。
