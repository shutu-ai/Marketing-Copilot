# Marketing Copilot VNext：Stage Independence 与 Backpressure Contract

## 任务与范围

- Task ID：`MC-VNEXT-STAGE-INDEPENDENCE-BACKPRESSURE-SAFE-PERF-20261019`
- Source：`86b1bee4a5fcbc005f43b4382c83dfe0781db14d`；`origin/main` 与工作区 `HEAD` 一致。
- 目标环境：SLES 12 SP5，原生 Linux `ext4`，CPython 3.11.4，DuckDB 1.2.0。
- 测试边界：仅使用 `127.0.0.1` loopback SMSC 和 synthetic 数据；未连接真实 SMSC，未触碰生产 Campaign、数据库或部署配置。
- 本轮未修改产品代码、未 commit、未 push、未部署。

## 实际组件连接

| 环节 | 实际 owner / 边界 | 当前合同与证据 |
|---|---|---|
| Ingress | `VNextRuntime` 创建 `StageCredit` 并传给 `IngressRouter`（`backend/vnext/runtime.py:266-273`） | `IngressRouter.enqueue_chunk()` 先 `try_reserve()`，再由单一 `TaskSpool.append_many()` 持久化；失败不增加 accepted，最后释放本次 reservation（`backend/vnext/ingress.py:128-148`）。 |
| Spool | 单 writer `TaskSpool` | `append_many()` 在打开 segment 前校验整批容量，超过 `max_bytes` 抛出 `SpoolCapacityError`（`backend/vnext/spool.py:788-812`、`737-739`）；manifest、segment、CRC 和 cursor 持久化。 |
| Worker admission | `VNextRuntime` worker | `journal_admission()` 在 dispatch checkpoint/socket 前检查结果 Journal 容量（`backend/vnext/runtime.py:124-143`），并传入 `ShardWorker`（`runtime.py:171-177`）。 |
| Dispatch fence | `ShardWorker` | `checkpoint()` 在 socket call 前持久化 taken cursor（`backend/vnext/worker.py:285-301`）；暂停 race 会在 checkpoint 后再次检查，避免新的非法 socket write（`worker.py:302-307`）。 |
| Journal / Projection | Journal 是 durable source of truth；ProjectionWriter 是单 DuckDB writer | Journal append 后 IPC hint 使用 `put_nowait()`；hint queue 满只记录计数，不丢 durable event、不杀 worker（`backend/vnext/runtime.py:111-122`）。Projection offset 仅在 DB commit 后持久化并随后 reclaim。 |
| Campaign control | 每个 worker 本地按 `(campaign_type,campaign_id)` 控制 | `paused`、`running`、`cancelled` 状态按 Campaign 过滤；未被暂停的其他 Campaign 继续调度（`backend/vnext/worker.py:97-130`、`445-469`）。 |
| Bind / TPS / Window | `RouteCreditBroker` + SMPP transport | 本轮冻结并验证 `pool_size=4`、`per_bind=200 TPS`、`window=50`；未改 `route_credits.py`、`smpp_transport.py`。 |

## 背压状态机

1. `StageCredit` 以 items/bytes 预算做 high-water 拦截；释放后只有降到 low-water 才清除 `blocked_reason` 并通知等待者（`backend/vnext/pressure.py:63-130`）。
2. Ingress 的同步 API 对 reservation 失败返回 `SpoolCapacityError`；调用方必须按既有 producer 合同重试。`IngressRouter` 本身不把拒绝伪装为 accepted，也不在内部启动无界重试。
3. Spool 达到硬字节预算时拒绝整批追加；已 accepted 的记录与 Spool record 一致，未 accepted 记录不进入投递路径。
4. Journal 达到硬容量时，runtime 的 `journal_admission()` 在 checkpoint/socket 前停止该 Worker 的进一步 dispatch；已写入 Journal 的事件仍由 ProjectionWriter 按 segment/offset 消费。
5. Projection 暂停只阻塞 Projection 自身；Journal 保留 durable backlog，恢复后从 offset 继续，已完整提交的 segment 才允许 reclaim。
6. 人工 Campaign pause 与容量 gate 是不同状态：容量释放不会清除人工 pause，人工 resume 也不能绕过仍生效的容量 gate。

## R2 合同门禁

| Gate | 结果 | 实际范围 / 证据 |
|---|---|---|
| BP-1 Ingress/Spool capacity | `PASS`（StageCredit/Spool 合同范围） | `backend/tests/test_vnext_ingress_pressure.py`、`backend/tests/test_vnext_spool.py`：high/low-water、async wait/wake、硬容量拒绝、CRC/恢复通过。Ingress 的同步拒绝/调用方重试边界已记录，未宣称不存在的 producer retry wiring。 |
| BP-2 Journal/Projection | `PASS` | `bp2-writer-pause-evidence/result.json`：pause 时 flush 被阻塞，恢复后 20/20 applied、queue=0、duplicates=0；T7-6/T7-7 通过，证明 hint queue 满时 Journal 仍权威、hard limit/reclaim 不提前删除。 |
| BP-3 Manual pause/resume/cancel | `PASS` | 同日双 Campaign：暂停项 0 次 socket write，另一项先推进 3 条，resume 后两项各 10 条且 pending=0；cancel 探针 3/3 cancelled、0 次 socket write，另一 Campaign 3 次 socket write、pending=0。 |
| BP-4 Crash/restart at-most-once | `PASS` | `bp4-restart-evidence/result.json`：crash 后 5，恢复 15，unique=20，pending=0；T7-4～T7-9 全部通过。 |
| BP-5 Observation/limits | `PASS` | 5K：8 workers、4 active binds、per-bind 200 TPS、window=50；8/8 worker clean exit，no residual test process。关键状态字段和 source line audit 已记录。 |

## 已知 wiring gap（未静默修复）

Acceptance harness 在 `backend/tools/vnext_streaming_acceptance.py:1841-1852` 尝试传入 `status_interval_seconds`，但当前 `ProjectionWriterProcess.__init__()`（`backend/vnext/projection_writer.py:1223-1231`）没有该参数，因而捕获 `TypeError` 后回退到历史 per-loop status 行为。`VNextRuntime` 的构造调用（`backend/vnext/runtime.py:295-305`）也未传该参数。

因此本轮只把 `--projection-status-interval 0.1` 记为“请求值”，不把它误写成“实际 bounded status interval 已生效”。这是现有接口/测量 wiring gap，不作为本任务的架构重构目标；没有为了掩盖该差异而修改生产代码。

此外，R1 streaming harness 的 worker 直接使用 standalone `ShardWorker` 与 Journal sink，不经过完整 `VNextRuntime` ingress admission；R2 使用真实 `StageCredit`、Spool、Journal、ProjectionWriter、Worker 和 runtime daily path 补足组件合同验证。因此 R1 主要代表 loopback worker/projection scaling path，不代表完整生产入口的端到端 ingress 压力曲线。

## 结论

在本轮可执行范围内，Stage 独立性、受控背压、人工控制、durable recovery 和 at-most-once 门禁均通过；现有 status-interval fallback 与 harness/runtime 边界已显式保留。没有观察到需要为本任务引入跨 Stage 中央协调器、扩大 queue、改变 checkpoint 语义或修改 bind/TPS/window 合同的依据。
