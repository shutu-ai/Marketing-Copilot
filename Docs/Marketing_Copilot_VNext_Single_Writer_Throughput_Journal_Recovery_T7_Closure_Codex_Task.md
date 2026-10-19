# Marketing Copilot VNext — 单 Writer 吞吐瓶颈、Journal 生命周期与 T7 恢复闭环 Codex 任务

> **任务 ID**：`MC-VNEXT-SINGLE-WRITER-PERF-RESP-JOURNAL-T7-CLOSURE-20261017`  
> **任务类型**：源码级定点优化 + EulerOS 原生验证；**禁止整体架构重构**  
> **冻结输入**：`shutu-ai/Marketing-Copilot`，`origin/main = 65e3d6f16810cbe1ab46940c23dd00f715f3cd4f`（开始执行时先核对）  
> **性能对照的产品基线**：`71ca9e25f40d9c57932ab8c02a6b69877a099eba`；新任务的工作起点为 `65e3d6f1`，不能忽略此后的 harness / test-clock 变更  
> **状态**：待 Codex 实施和现场验证；本文不是已完成报告  
> **目标部署环境**：EulerOS 2.0 SP15 / native ext4 / CPython 3.11.6 / DuckDB 1.2.0；仅授权的 loopback 测试环境  
> **最高原则**：实时性、吞吐量、低复杂度；**旧日未发任务绝不跨日重发**；重复发送比漏发更不能接受；可靠数据回放不能重复累计统计。

## 0. 先执行的 Codex 指令

1. 读取仓库 `AGENTS.md`，保留全部已有 dirty/untracked 文件；如存在 `.codegraph/`，**先用 CodeGraph 定位**，再做精确源码阅读与修改。不得 `git reset --hard`、`git clean`、覆盖原始运行数据。
2. 核实 `git rev-parse HEAD`、`origin/main`、`git status --short`、关键文件 SHA 和目标服务器实际环境。若 `main` 已推进，先报告差异，再在最新代码基础上重审；不能静默套用旧 SHA。
3. **先复现 / 测量，后修改产品热路径**。只在已证实的瓶颈上动刀；不通过引入新框架、多进程 DB Writer、锁层、反复状态轮询或额外数据拷贝解决问题。
4. 不连接真实 SMSC、生产数据库、生产 API，不进行生产部署。所有号码和 Campaign 必须为合成数据，SMPP 仅绑定 `127.0.0.1`；不改变系统时钟。
5. 优先复用现有 EulerOS 测试服务器的隔离 Run 和离线依赖。**不要因 WSL AlmaLinux-8 缺 Python 依赖而强行在线安装**，也不将 `/mnt/d` 当作原生 ext4 性能环境。
6. 未经用户明确授权，**不执行 commit/push**。最终交付代码差异、测试用具、机器结果 JSON、精简 Markdown 报告和阻断项；运行时 DB/WAL/Spool/Journal、venv、密码不入 Git。

## 1. 已核实的事实与本轮要解决的问题

来源：

- [T3/T4/T6/T7 证据闭环报告](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/Docs/Marketing_Copilot_VNext_T3_T4_T6_T7_Evidence_Closure_Report.md)
- [Harness 与瓶颈审计](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/Docs/Marketing_Copilot_VNext_Harness_and_Bottleneck_Audit.md)
- [Projection Writer 实现](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/backend/vnext/projection_writer.py)
- [Runtime 主链](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/backend/vnext/runtime.py)
- [SMPP Transport](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/backend/vnext/smpp_transport.py)
- [Streaming Harness](https://github.com/shutu-ai/Marketing-Copilot/blob/65e3d6f1/backend/tools/vnext_streaming_acceptance.py)

### 1.1 不能掩盖的验收结论

| 项目 | 实际结果 | 解释 |
|---|---|---|
| T3，4 Campaign 50K | 修复后 **3/3 PASS**；`20K+20K+5K+5K` | 发送/接收/响应/投影最终闭合；停止供给后 drain **250–256 秒**，说明端到端同步追赶仍慢 |
| 混合长短信 | 5,500 tasks / 11,000 PDU / 11,000 responses，PASS | **task 与 PDU 不可混为一个计数** |
| T4 高压 600 秒 | 492,000 tasks/PDU/response；最终 Projection 155,166；drain 未闭合 | single Writer journal catch-up 为阶段瓶颈；不是 native crash，也不是 feeder 20K 卡死 |
| T4 低压 600 秒 | 45,050 tasks；Projection 45,050，4.424 秒 drain，PASS | **只证明约 75 PDU/s 这一负载区间**，不能推广到 800 |
| T6 跨日 | 旧日跨界 Socket 0 / restart re-dispatch 0，新日成功 8，PASS | 保留跨天 fencing、spool reclaim 与注入时钟行为 |
| T7 | Worker crash、Writer pause、Campaign pause：**3 类 PASS** | 其余 6 类缺乏实测，整体 `NOT_VERIFIED` |
| Canary | `NO` | 不得通过换指标名称或者只看 lag=0 变成 PASS |

**不要将“800 PDU/s 配置额度”写成 800 PDU/s 的完整端到端可持续能力。** 前两轮正式测试在 64 CPU、nice 0、无 ionice 限制下运行；任何新旧 A/B 必须使用同一真实资源配置。

### 1.2 源码级已识别的高优先级嫌疑

1. `ProjectionWriter._apply_batch()` 虽有 set-based group-upsert，但临时 staging 仍使用 **25 列 `executemany` 将 Python tuple 逐行灌入**。每一批还要 `DROP/CREATE` 两张临时表，然后再做 anti-join、新事件插入和聚合更新。**该导入路径优先做向量化 A/B**，不能未经测量直接声称它是唯一根因。
2. 单 Writer 每轮仅取 `batch_size=512`；Journal 按 Worker 分成 `quantum<=64`，每个 batch 后执行 DuckDB COMMIT 与 `journal-offsets.json` 持久化。需要测**实际批次填充率、Journal 读取/解码、staging、SQL、commit、offset 持久化**分别花多久；不要把 `sql_statements` 计数当 SQL wall-time。
3. **生产响应收尾疑似有真实缺口**：`runtime.py` Worker 循环仍 `run_until_idle(close_transports=True)`；`ShardWorker.run_until_idle()` 在 `finally` 立即 `close_transports()`；`AsyncSmppTransport.close()` 清空未处理 `_pending`。而验收 harness 已改成 `close_transports=False -> 等待 window_inflight=0 -> close`。需要用 **真实 Runtime 的延迟响应 loopback**复现，并把必要修复放在生产链，不是只留在 harness。
4. `ProgressJournal.append()` **逐事件 `flush + fsync`**；当前单文件 append-only、`size()+event <= max_bytes` 为硬上限，已投影数据没有清理路径。**是否导致 T4 延迟须计量；但长期运行 Journal 的可回收容量需要单独解决**。禁止无条件关闭 fsync 或盲目 truncate/rename。
5. Streaming harness 的 worker pass 会释放 `_event_index` 和 destination lock cache；**生产 Runtime 是否同步具备必要的 reader cache 生命周期管理**要核对。之前 T4-01 有 2 GiB RSS guard 失败，不允许测试修了缓存就推断生产已经修好。
6. `projection_lag_seconds=0` 不是已追平：应使用 Journal committed offsets / 生产事件量 / DB 已应用事件量计算真正 backlog。`applied` 可能是 Writer 当前进程内的计数，**不得在 crash/restart 后直接作为全生命周期累计值**。

## 2. 不可改变的设计约束

- 维持 **一个 `ProjectionWriterProcess`、一个 DuckDB 文件、一个 DB 写入 Owner**。每 Campaign 一个 Writer / 独立 DuckDB **暂缓，不得在本任务偷偷引入**。
- Worker 只操作 Spool/Async SMPP/持久化 ProgressJournal；业务 DB 仍只由单 Writer 写入。
- **先 durable `dispatch_taken`，再触碰发送 Socket**；允许该崩溃窗口导致漏发，但绝不能因此补发已可能送出的短信。`unknown` / `partial` / `expired` 必须各自保持语义。
- BusinessClock、`business_day`/`expires_at`、SMPP 每 PDU 最终 cutoff、跨天 Spool 隔离及旧日不重派不退化；迟到响应归属旧业务日，不能阻塞新日发送。
- 事务正确顺序：**Journal events durable → DB batch event-ID 去重与 aggregate 同事务 COMMIT → journal committed offsets durable → 允许回收被完全确认的 Journal segment**。
- DB commit 后 offsets 写入前崩溃，要允许重新 replay，依靠 `event_id` 幂等去重，不能重复累计 `socket_written`、`response` 或 expired。
- 逐条业务日志、逐 PDU profiler、无限缓冲、不受控进程数、复杂信用调度平台，均不采用。
- 不额外要求账号体系、RBAC、审批链、正式线上短信测试；集中于性能与正确性。

## 3. P0 — 低扰动分阶段 profiling（先交付数据）

目标：**真正解释单 Writer 的可持续处理速率**，而不只说“投影落后”。

### 3.1 添加采样级累积计数，不新增每事件 I/O

修改 `backend/vnext/projection_writer.py`（必要时 `runtime.py` 状态面），以 `time.perf_counter_ns()` 在批次边界计量，并**只每秒发布/汇总**：

- `journal_read_ns`、`json_decode_ns`、每次读取返回 event 数、每 Journal offset/size；
- `rows_prepare_ns`、`staging_load_ns`（独立覆盖 `executemany`）；
- `new_event_dedupe_ns`、`events_insert_ns`、`aggregate_upsert_ns`；
- `db_commit_ns`、`offset_persist_ns`、batch end-to-end ns；
- `batches_total`、`events_read`、`events_new`、`events_replayed`、**batch_size P50/P95**、SQL calls（区分 `executemany` 包含的逐行工作）；
- `journal_total_bytes`、`journal_committed_offsets`、`journal_backlog_bytes`、估算的 backlog events；`projection_events_applied`（标明 process-local 或 DB total）；
- `worker_socket_written_tasks`、`SMPP submit_sm PDU`、`submit_sm_resp`、各业务日/活动的完成计数。

保留低频快照，在 EulerOS `T3`/短压场景冻结机器 JSON/CSV 与 top contributing stages；避免高频诊断反向拖慢性能。**先对 65e3d6f1 现有 Writer 建立可重复的 baseline。**

### 3.2 正确理解事件吞吐需求

`800 PDU/s` 并不必然等于 `800 Journal events/s`：一个 SMS 任务写入事件之外，还可能产生一个或多个 `submit_sm_resp` 事件。让工具从真实生产者测出 `events_per_task` / `events_per_pdu`，给出 **Writer 实际需要追上的 events/s**。

验收指标不以 `projection_lag_seconds==0` 为唯一证明；至少检查 `journal_backlog_bytes` 增长斜率、committed offset 持续前进、DB 内明细和聚合与实际发送/响应事件严格一致。

### 3.3 用最小基准隔离组件

同一 EulerOS/ext4 和相同资源护栏下，通过小型独立 benchmark 复用**当前相同 DuckDB schema、相同 25 列形状、相同 event-id 去重比例及事务逻辑**，对比：

- A0：现有 `executemany` + drop/create + SQL + commit + offset；
- A1：不变的 SQL 路径，仅临时表生命周期调整；
- A2：不变的后续 SQL，替换 staging 导入为向量化方式；
- A3：A2 + 合理批次规模/Journal quantum 调整。

报告每个处理阶段的吞吐与耗时比例、峰值 RSS、结果 hash、异常及所用 DuckDB 版本；**先确认 A2 在 DuckDB 1.2.0 可用**，不依赖最新版本特性。

## 4. P1 — 单 Writer 首选优化方案（基于 A/B 分批实施）

### 4.1 首选：替换 25 列 Python `executemany` 逐行导入

可供实验的方案，按**无额外依赖、吞吐实测、可靠性**决定：

**方案 A（首选实验）：参数化 LIST + 多列 `UNNEST` 向量化 staging**。

- 在 Python 中将一个**有界** batch 转置成长度完全相等的 25 个列向量（明确 `VARCHAR[]`、`INTEGER[]`、`BIGINT[]`、`DOUBLE[]` 类型）；
- 单次 `conn.execute()` 将并排 `UNNEST` 结果写入 staging，替代 Python 的逐行 `executemany`；
- `UNNEST` 多列表按下标对齐，若长度不等会补 NULL，**所以必须先验证 25 列长度相等且全字段对应**，特别是 empty string、nullable `message/error`、bool→int、业务日和 long SMS 文本；
- 保留 `event_id` 去重、`_vnext_projection_new`、INSERT、GROUP/UPSERT、同一 DB 事务和 offsets 边界；
- 测试 Python DuckDB 1.2.0 列表参数绑定及 SQL 类型转换，若语义/性能不合格则废弃，不在生产中猜测兼容。

**方案 B（候补）：DuckDB 原生向量扫描导入**，如有界临时 NDJSON/CSV 通过 `read_json`/`read_csv` 导入 staging，或现有依赖里**已存在**的 DataFrame/Arrow 注册。必须把临时写文件/序列化成本、清理与事务异常恢复算入 E2E；不得为此强制在线安装 pandas/pyarrow，也不得把临时 staging 当新的持久化权威数据。

**方案 C（无收益则保留旧逻辑）**：如果 staging 不是主要瓶颈，不强行替换，直接沿采样结果优化实际最慢的 SQL/commit/journal 阶段。

DuckDB 参考：
- <https://duckdb.org/docs/current/sql/query_syntax/unnest>
- <https://duckdb.org/docs/current/guides/python/execute_sql>
- <https://github.com/duckdb/duckdb/discussions/6360>（社区关于 `executemany` 与向量扫描差异的案例，非本项目实测）

### 4.2 避免每批重建临时表（仅在实测有收益时）

测试在 **单 Writer 私有连接上复用 typed TEMP staging**，通过清空并重填替代每批 `DROP/CREATE`；与现有方式作 A/B，观察 temp table 与 DuckDB WAL/内存行为。

- 必须在同一 batch 原子事务内完成 staging → new-event filter → insert events → aggregate update；
- ROLLBACK 后不得留下影响下一批的脏 staging，也不得出现 replay 双计数；
- 数据库和日志不能在测试中持有互相竞争的 DuckDB write connection。

### 4.3 批次 / 读取 quantum：小范围而不是参数爆炸

保留按 Worker Journal 的 round-robin fairness，在 256、512、1024、2048（必要时 4096）等 **有限选项**做 A/B。每个对比同时记录 P50/P95 batch fullness、Journal backlog 增减、最小 Campaign 饥饿时间和 RSS。

- 数据量少时仍要在合理 flush 时间内可见，不能为了大批次让小活动长期看不到进度；
- 不要新增十几个公开环境变量。只暴露**一个有证据支持的 batch 配置**（或采用代码内部自适应、简明上限）；
- 如 Writer 在 replay 期间存在零消费的空 journal，避免不必要的轮询和文件反复打开，但不得牺牲新活动响应时间。

### 4.4 保留 fsync；offset 持久化只在可证明安全时合并

测量以下成本：worker Journal 每事件 `fsync`、Writer DB COMMIT、writer offset 文件 fsync。**默认保留逐事件 Journal fsync**；只有拿到同等级 durable 安全替代设计和崩溃证据，才能考虑改动，不得把关闭 fsync 作为简单提速手段。

offset 若做批量 checkpoint，只允许**落后于已成功 COMMIT 的进度**，重放靠 event_id 幂等；不可提前推进 offset，更不能在删除 journal 数据前缺失对应 durable ACK。

## 5. P0 — 修复生产 SMPP 响应生命周期不一致

当前可疑链路：

```
backend/vnext/runtime.py:run()
  -> ShardWorker.run_until_idle(close_transports=True)
  -> ShardWorker.close_transports()
  -> AsyncSmppTransport.close() 取消 reader、清空 pending
```

而测试 harness：

```
run_until_idle(close_transports=False)
  -> 等待 window_inflight=0
  -> close_transports()
```

**先新增失败复现测试**：真正由 `VNextRuntime`/spawned Worker 启动，loopback SMSC 以 0.1–2 秒随机/固定延迟返回响应；发送 100–1000 单段、混合多段，观察是否 `socket_written==receive` 但 `submit_sm_resp` 计数偏少。保留原始失败证据（若无法复现则明示）。

若复现风险属实，将**有界 response drain** 提升为 `ShardWorker` 或 `AsyncSmppTransport` 的公共、可复用生命周期步骤：

1. Worker 发送 pass 结束后先停止新提交，等待当前 transport 的 `window_inflight` 响应收尾；
2. 成功/错误响应通过原有 durable response sink 写入 Journal，再释放 bind slot；
3. 异常 / timeout / 断连后，未确定响应按 `unknown`/超时诊断处理，**绝不自动重发已写出的 PDU**；必须有有界 timeout 与正常的断开/释放过程；
4. 响应收尾不能无限占有 route bind、造成其他 Campaign 饿死；双 Route 时一个响应慢不应拖死另一个；
5. **跨天不能发出旧日任何新 PDU**；已发送 PDU 的迟到响应按旧业务日入库，不刷新发送资格。

让验收 harness **调用产品复用的方法**或用正式 Runtime 的路径测试，不再维护语义不同的第二套 close 逻辑。回归 `response=socket PDU`、长短信分段、延迟响应、断连、暂停、多个 Worker/Bind。

另外检查 test harness 中释放 `_event_index` / `_number_locks` 的动作是否也是生产 Runtime 需要的 reader-only cache 收尾。**如果是，通过安全的产品生命周期点释放已经不再引用的数据；不能在仍有活跃迭代器/发送任务时清空索引。** 用长时间 RSS 采样证明没有与 Worker 数成倍增长的 retained index。

## 6. P0 — Journal 硬上限之后的恢复与安全回收

当前 `ProgressJournal` 是单 worker 单 JSONL 文件；即使 Writer offset 已赶上 EOF，文件 bytes 仍累计增加。先做**“已消费 100%，Journal 仍到达硬上限”**的真实测试，避免误把“无 backlog”当作“仍有可写空间”。

若存在无法在同一业务日持续发送/跨天恢复的风险，实施**最简单可验证的分段轮转/回收**，而非通用消息总线：

- Worker 单写者按有界 segment 追加，Writer 保持一个 DB owner；segment 使用稳定 generation / ID + offset，防止轮转后旧整数 offset 指向新文件导致跳过或错误重读；
- 文件到达水位切换新 segment；只删除 **DB 已提交 + durable committed cursor 已覆盖 + 无活跃 reader/writer 文件引用** 的完整 segment；
- `COMMIT` 后 durable offset 未更新之前故障：必须能找到原 segment 并安全幂等 replay；
- 旧日迟到响应仍可落 Journal 并归属旧业务日；旧日 Spool 回收与 Journal 安全 GC 是**独立生命周期**；
- 达到 hard limit 时应暂停 dispatch admission，不能先发送后发现无法写入结果；Writer 恢复/GC 释放容量后自动继续；如已过业务日则旧任务过期而不是补发；
- 占用与 backlog 指标必须区分：**journal 总磁盘 bytes、已消费 bytes、未消费 bytes、可回收 bytes、是否 blocked**；
- 不设计昂贵的数据库式每条 ACK、分布式事务或跨进程全局重锁。优先按 segment 批量处理并复用现有所有权。

**如果经过计量证明当前硬上限不会在目标每日最大业务量前发生**，可以先交付风险量化和受控操作方案，推迟轮转，但 T7 的 `journal hard-limit -> recovery` 仍须有真实 PASS；不能永久以“磁盘没满”替代恢复能力。

## 7. P0 — 完成 T7 剩余 6 类故障矩阵

新增**隔离测试用例 / 少量复用驱动**，避免重复实现 Worker/Writer，覆盖：

| ID | 故障场景 | 必须看到的结果 |
|---|---|---|
| T7-4 | Feeder 暂停 30–60s 后继续 | 已有工作正常发送；恢复 feeder 不造成死锁/无限忙等待/重复 source event |
| T7-5 | Route-A unavailable，Route-B healthy | A 被局部背压，B 有可验证进展；A 恢复后继续，健康活动无无关长期阻塞 |
| T7-6 | Projection IPC hint queue full | Journal 仍是 authority；hint 丢弃不丢 durable event；Writer 追赶正确 |
| T7-7 | Journal high-water 和 hard-limit | 到阈值能停；消费/GC 后能恢复；不在记录不可信的情况下提交新 PDU |
| T7-8 | Spool capacity exhausted → 过期/回收/再启用 | 对上游拒绝/背压而非死循环；容量释放后新日可正常摄取；旧日不派发 |
| T7-9 | 跨午夜 Worker/Writer crash + restart + prefetch/TPS 等待 | 旧日新 PDU=0、跨日重派=0；已写未知结果不重发；日统计不串 |

重点检查 crash 时的 `not_sent / unknown / partial / expired / written / response` 六类业务状态；写入/未写入边界不能被一条“全部发送成功”掩盖。测试场景失败必须保存具体现象及机器证据；不要用 `NOT_VERIFIED` 自动变成 PASS。

可在一个小规模自动化矩阵中覆盖**最后写入前**的 cutoff、多段部分写入、等待 Bind/Window/TPS、Writer 在 COMMIT 前/后崩溃、Journal 在 offset durable 前/后崩溃。

## 8. 性能 A/B 与闭环门禁

### 8.1 性能测试次序（避免无效长跑）

1. **S0：离线 Microbench**：对真实字段形状/真实列数和 1%、10% 重复事件，比较旧 staging vs 新 staging。正确性不一致的候选不进入 E2E。
2. **S1：小 E2E**：P100/P1K/P5500、2 Campaign、mixed multipart；正确性、响应收尾、单 Writer/DB Owner、batch latency，检查回归。
3. **S2：50K 4 Campaign streaming**：`20K+20K+5K+5K`，两轮以上；同环境同参数与未改版独立 Run 对比，不将短时 per-second burst 当稳态 SLA。
4. **S3：600 秒持续输入 + 独立 drain**：使用 8 Worker、4 Bind、`per_bind=200`、window 50、真实 loopback；保持有界 feeder，记录 1s samples、真实 socket PDU/s、业务 task/s、**全部 Journal event/s**、Writer event/s、committed-offset backlog、CPU/RSS、Run bytes。供给 600s 结束后**禁止将 backlog 强制清零**；允许额外 drain 但须明确时间。
5. **S4：必要时 600s repeat / 单 Writer 改动定稿**：只对已通过 S3 的有效候选做重复验证；失败候选保留结果，不继续堆各种配置。

推荐目标：**在约 800 PDU/s offered 负载下，Journal 未消费 bytes 长期不持续增长，且停止供给后约 60 秒内完成全量 DB replay 与进度对账**。这是本任务目标，**不是已经证明的性能 SLA**；若受资源、协议或业务事件倍数影响不能达成，报告稳定能力区间和瓶颈，而不是假 PASS。需记录长期吞吐、Journal event/PDU 倍数与可持续 Writer events/s。

**同配置比较要求**：同 EulerOS/ext4、DuckDB1.2.0、CPython3.11.6、CPU affinity/nice/ionice、SMPP 配置、Campaign mix、数据文本长度、文件系统剩余空间、Producer、同一 harness。无法满足时只给“描述性差异”，不得出具正式非回归签署。

### 8.2 数据对账与性能不退化

- `accepted tasks`、`dispatch-taken`、`socket-written tasks`、`socket-written PDU parts`、`loopback received PDU`、`submit_sm response`、`journal total progress events`、`DB progress events`、`vnext_projection SUM(...)` 分别核对；不要用 `events_applied == task_count` 取代真实事件口径。
- 全量完成时 `journal uncommitted events=0`、Writer committed cursor 追至每个 closed journal EOF（或已 ACK segments）且无重复 event_id、统计双计数。
- `old_day_redispatch_after_restart`、`old_day_new_pdu_after_cutoff` 必须**来自真正执行跨日场景的实测**；普通同日 loopback 保持 `executed=false, value=null`。
- 最多 2 GiB RSS、单 Run 数据不超过 5 GiB 等护栏仅在与现有测试配置一致时适用；磁盘保护或 CPU 约束被触发必须标记 `FAIL / BLOCKED_ENV`，不伪装成功。
- 通过 `Writer kill`, `DB COMMIT-before-offset-kill`, repeated journal replay，核对幂等聚合和恢复；旧日任务永不变成待发。
- 修改配置的 API / UI 若不是必需，不进行前端大范围改造；原有后端回归和前端 build 必须保持通过。

### 8.3 Gate 矩阵（必须真实记录）

| Gate | PASS 的条件 |
|---|---|
| `SOURCE_ENV_IDENTITY` | 核实 branch/SHA/dirty、EulerOS/ext4/Python/DuckDB、隔离 Run 与实际资源配置 |
| `PRODUCTION_RESPONSE_DRAIN` | 真实 Runtime delayed-response/timeout/bind release，不丢可正常返回的 response；故障不重派 |
| `WRITER_STAGE_RCA` | 阶段耗时、批次填充、吞吐/lag 成本归因有机器数据 |
| `SINGLE_WRITER_VECTOR_BATCH_CORRECTNESS` | staging/new events/aggregates 与旧版等价；dup/replay/rollback/multipart/跨日 PASS |
| `T3_50K_MULTI_STREAMING` | 修订后多活动链路可完整对账，无重复/跨活动污染 |
| `T4_HIGH_PRESSURE_STEADY_AND_DRAIN` | 在真实约 800 PDU/s 或所声明测试负载下持续 600s，包含完整 Writer catch-up 及 drain 时长 |
| `JOURNAL_BOUND_AND_REUSE` | 长期可恢复空间；hard limit 停止并在空间回收后恢复，不丢 committed evidence |
| `T6_BUSINESS_DAY_E2E` | 原有 T6 PASS，并覆盖跨界 waiting / multipart / restart 组合 |
| `T7_STAGE_RECOVERY` | 剩余 6 类补齐，合计 9/9 与错误窗口对应 PASS；若不可执行如实 NOT_VERIFIED |
| `NATIVE_STABILITY` | 适用的 EulerOS 受控长稳无 native fault、RSS/磁盘无未解释增长 |
| `PERFORMANCE_NON_REGRESSION` | 同环境 old/new A/B，吞吐、延迟、资源与正确性不退化 |
| `CANARY_READY` | 仅当任务范围内**所有强制门禁**有真实 PASS，可标注 `READY_FOR_SEPARATE_DEPLOYMENT_REVIEW`，不等于上线许可 |

## 9. 工作成果与报告规范

建议输出：

- `Docs/Marketing_Copilot_VNext_Single_Writer_Performance_RCA_and_AB_Report.md`
- `Docs/Marketing_Copilot_VNext_Production_Response_Journal_T7_Closure_Report.md`
- 用于复核的简洁 benchmark / replay tests / fault tests（放在 `backend/tests/` 与 `backend/tools/`，优先扩展现有工具）
- 每场景机器 JSON、1s time-series CSV、有效配置、退出码、底层错误和 evidence manifest（放隔离 Run；不包含 secrets/DB/WAL/raw Journal）

在最终报告中分别写：

1. **What changed**：实际代码差异与可退回点；说明是否只有单 Writer + 单 DB。
2. **Why**：最慢阶段实测占比，为什么选 A2（或为什么保留旧代码）。
3. **How much**：每秒 Journal events vs DB persisted events、5500/50K/T4 对比、drain 时长、CPU/RSS/disk。
4. **Correctness**：逐 Campaign + 业务日 + 事件类型 + task/PDU/response 的 hash 或数值对账，replay/crash/fault 结果。
5. **What remains**：任何 FAIL、NOT_VERIFIED、环境限制、生产/真实 SMSC/SLES12SP5 未执行情况；若仍未 Ready，保留 `NO_GO`。
6. 不混入其他无关文件。未经用户另行授权，**不提交、不推送**；任务结尾提供可执行的推荐下一步与必要的最小 diff，而不是新的整体架构蓝图。

## 10. 优化决策顺序（不得跳级）

```
生产响应 close 风险与 Writer 基线测量
    ↓
向量化 staging / SQL / batch 轻量 A/B
    ↓
单 Writer 完整 50K 与 600s steady+drain
    ↓
Journal 可回收容量与剩余 T7 真实故障矩阵
    ↓
若达到目标：停止架构调整，报告可审查状态
若仍不达标：报告测得的真实 stage 级上限与负载前提
    ↓
【仅供未来独立决策】每 Campaign 一个 Writer + 独立 DuckDB 的实验设计
```

**禁止提前实施多 Writer。** 只有在单 Writer 已证明自身 DuckDB apply/commit 不可突破，且业务目标确实要求持续 800 PDU/s、硬件与其他阶段已排除瓶颈的情况下，才提交单独设计议题。本任务不包含该设计的实际开发。
