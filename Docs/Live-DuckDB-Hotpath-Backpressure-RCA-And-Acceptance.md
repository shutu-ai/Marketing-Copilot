# Live DuckDB 热路径与实时摄取背压 RCA / Acceptance

## 结论

本轮确认了三个高频路径问题，并以最小改动修复：

1. `record_send_intent()` 在每个 socket 前 durable authorization 写事务中执行路由积压 `COUNT(*)`；该查询不是授权或 fencing 的判定条件。
2. `claim_next_tasks()` 在 claim 写事务中按已 claim route 重复执行 backlog `COUNT(*)`；公平调度使用候选组、durable cursor 和 DRR，不依赖这个展示性 backlog。
3. realtime worker 先 claim 文件、再在 `receive_file()` 发现任务/写队列/磁盘等容量压力，导致 `RESOURCE_LIMIT` retry 状态写入形成 claim → fail → retry 循环。

根因状态为 `PARTIAL`：高频无效计数和背压时机已由源码、EXPLAIN、计数审计和真实链路确认；公平候选扫描仍保留原设计，且没有在本轮重放生产 6.61M 历史任务和多进程争用，因此不能把本轮结果写成完整生产容量证明。

## 修改

- `backend/services/sms_task_service.py`
  - 删除 submit authorization 内的 route backlog 全表计数。
  - 删除 claim 内按 route 的 backlog 全表计数。
  - 保留 `campaign_capacity_service.record_activity_event()`，但以 `backlog=None` 记录活动时间；准确 backlog 仍由 capacity snapshot 的权威聚合计算，公平选择不读取该字段。
  - 未改变 claim token、campaign active/window 校验、attempt、每个 multipart part、task event 或 socket 前授权边界。
- `backend/services/realtime_ingestion_worker.py`
  - 在 claim 前调用 `wait_for_ingestion(..., producing_chunks=True)`，容量暂停时不再 claim 文件。
  - 将停止信号传给文件处理；容量等待被停止时返回 `stopped`，不执行 retry 状态转换，保留 processing lease 由既有 lease recovery 接管。
- `backend/services/realtime_campaign_service.py`
  - CSV/TXT 每个 chunk 和 Excel 处理前使用可停止的容量等待；容量压力不再抛出普通异常触发 RESOURCE_LIMIT retry。
  - 真实解析错误、过期文件、原子复制、去重、chunk lease 和持久恢复语义保持不变。
- `backend/tools/live_duckdb_hotpath_benchmark.py`
  - 新增一次性隔离 benchmark：真实 service function + DuckDB，采集 SELECT/COUNT/INSERT/UPDATE/commit/lock-hold 的实测 p50/p95/p99，并输出 route COUNT 次数。
- `backend/tests/test_live_duckdb_hotpath_backpressure.py`
  - 新增 4 个回归：claim/authorize 不再 route COUNT、worker 回压前不 claim、停止等待不写 retry、HIGH→LOW 自动恢复。

## SQL 与锁证据

测试在 WSL2 AlmaLinux-8、ext4 `/tmp` 临时工作区执行。配对 benchmark 使用相同代码入口、同一 DuckDB schema、相同 10,000 条 eligible tasks、60 次 serial samples；另有 500,000 条 accepted history + 1,000 条 eligible tasks 的副本。

| fixture | operation | baseline p95 | fixed p95 | 结果 |
|---|---:|---:|---:|---|
| 10K | claim | 314.123 ms | 314.129 ms | 基本持平，候选扫描仍在 |
| 10K | submit authorize | 164.418 ms | 164.612 ms | 统计噪声范围，commit 主导 |
| 10K | writer lock hold | 140.050 ms | 138.150 ms | 轻微下降，未达 20% |
| 500K history | claim | 721.456 ms | 756.512 ms | 本轮无改善，不能宣称优化了候选扫描 |
| 500K history | submit authorize | 196.405 ms | 186.346 ms | 轻微改善 |
| 500K history | writer lock hold | 389.850 ms | 405.650 ms | p95 波动/回退，需后续并发 profile |

最确定的提升不是把 serial p95 宣称为大幅性能提升，而是 DML 路径删除：

- 10K fixture：baseline route COUNT `120` 次，fixed `0` 次。
- 500K-history fixture：baseline route COUNT `80` 次，fixed `0` 次。
- `EXPLAIN ANALYZE` 在隔离副本上显示 route backlog 查询是 `Sequential Scan`；baseline/fixed 总时间分别为 0.0038s/0.0041s，过滤扫描到 9,480 行。它证明了查询形态和存在性，但不支持在本机小规模上宣称生产级 20% 性能收益。

本轮 benchmark 是单 writer serial harness，不包含多进程 lock wait/timeout 竞争；因此“writer lock 利用率下降”未被过度推断。完整数字和所有原始字段见 [live-duckdb-hotpath-backpressure-results.json](live-duckdb-hotpath-backpressure-results.json)。

## 背压行为

修改前，realtime 文件已经被 claim，`receive_file()` 检查到 `ingestion backpressure: tasks` 后抛异常，worker 再写 `RESOURCE_LIMIT/retrying`。修改后：

```text
capacity high -> worker waits before claim
                       or waits between chunks
capacity low  -> same file/chunk resumes automatically
stop          -> stopped result, no retry-state write; lease recovery remains authoritative
```

`wait_for_delivery()` 未修改，因此 `paused.tasks=true` 时 task sender/result writer 仍可排空已生成任务。HIGH/LOW 滞回、file reservation、原子复制、chunk checkpoint、过期终态和 lease fencing 均保留。

## 真实 loopback SMPP 验收

链路为真实 Python SMPP client → `127.0.0.1` SMSC simulator → `submit_sm_resp` → DuckDB durable state；配置使用生产模式，delivery receipt 明确为 disabled，未把 DLR 等待混入本专项。

- before quick 三轮：3/3 submit accepted，0 DLR，0 duplicate/lost，最终 backlog 0。
- after quick 三轮：3/3 submit accepted，0 DLR，0 duplicate/lost，最终 backlog 0。
- drop response：3 个 submit 变为 unknown，0 自动重发。
- restart：5/5 accepted，重启前后无 duplicate submit。
- partial submit：1 个六分片任务，1 个 part accepted、5 个 rejected，终态 partial，未整条重发。
- 1K：1,000/1,000 accepted，0 DLR，0 duplicate/lost，peak backlog 892，最终 0，实测 5.652 task/s。
- 3K：3,000/3,000 accepted，0 DLR，0 duplicate/lost，peak backlog 2,657，最终 0，实测 4.399 task/s。
- fairness：88/88 tasks accepted，4 活动、8 fairness state rows、0 duplicate、所有活动均被服务。
- 10K SMPP E2E：`CAPACITY_LIMITED`，未用数据库 benchmark 冒充协议 E2E；按 3K 实测速度预计约 38 分钟。本轮未执行 soak。

## 回归与包

- backend 全量：`134 passed, 2 skipped, 0 failed`；2 个既有 performance mark warning。
- 新增热路径/背压测试：`4 passed`。
- frontend `npm run build`：exit 0；仅有既有 Rolldown pure annotation/chunk-size warnings。
- SLES 包：`release/marketing-copilot-sles12sp5-system-python-20261013-live-duckdb-hotpath-final.tar.gz`
  - size `78,146,217` bytes
  - SHA256 `556478791e3a17264d3c2ea2938917481ad7367dacb14c620f485c9783feb0b5`
  - version `20261013-live-duckdb-hotpath-final`
  - `MC_UPDATE_MODE=systemd`
  - archive checksums：PASS；deployment check `--require-linux`：PASS；数据库、日志、上传、monitor、venv：未打包。

曾尝试运行仓库已有 `realtime_pressure_test.py`，它在旧的 `blocked["moved"]` 断言处退出，未产生可用的恢复结果；该 legacy harness 不作为本轮 PASS 依据，新增的 worker/等待回归和 1K/3K 最终对账结果已单独记录。

## 限制与未执行项

- 未连接现网 DuckDB/SFTP/SMSC，`LIVE_ENV=UNVERIFIED`，`SFTP=BLOCKED_ENV`。
- 未做 SLES 生产压力测试、发布、部署、重启。
- 未复现 6.61M accepted 历史规模下的并发 writer contention；500K history 仅用于隔离 SQL profile。
- 结果 buffer 未改动，因为现有 journal-before-ACK、batch、异常恢复回归已通过。
- 未执行 30 分钟 soak；10K 真实 SMPP E2E 为 `CAPACITY_LIMITED`。

## 最终摘要

```text
BASELINE_SHA: 4ec2c48af5cf2a991ef21cdc05c9ce03dcf6244a
WORKTREE_CHANGES: backend/services/sms_task_service.py; backend/services/realtime_ingestion_worker.py; backend/services/realtime_campaign_service.py; backend/tests/test_live_duckdb_hotpath_backpressure.py; backend/tools/live_duckdb_hotpath_benchmark.py; Docs/live-duckdb-hotpath-backpressure-results.json; Docs/Live-DuckDB-Hotpath-Backpressure-RCA-And-Acceptance.md
ROOT_CAUSE_STATUS: PARTIAL
SUBMIT_AUTHORIZE_HOTPATH: FIXED
CLAIM_HOTPATH: FIXED
INGESTION_BACKPRESSURE_RETRY: FIXED
RESULT_BUFFER: NOT_NEEDED
REGRESSION: 134 passed, 2 skipped, 0 failed
WSL_REAL_SMPP_E2E: PASS
RETRY_UNKNOWN_MULTIPART_SAFETY: PASS
FAIRNESS: PASS
DB_LOCK_BEFORE_AFTER: 10K mean 84.317ms -> 81.550ms; 500K-history mean 192.062ms -> 193.125ms; performance target not met
TASK_TPS_BEFORE_AFTER: not a valid paired performance claim; after 1K 5.652/s, 3K 4.399/s
PART_TPS_BEFORE_AFTER: production 3.69 part/task mix not run; partial safety run 243.053 parts/s for one six-part task
INGESTION_RESUME: PASS
CAPACITY_1K: PASS
CAPACITY_3K: PASS
CAPACITY_10K: CAPACITY_LIMITED
SOAK: NOT_RUN
SLES_PACKAGE: BUILT
SFTP: BLOCKED_ENV
LIVE_ENV: UNVERIFIED
COMMIT_PUSH_DEPLOY: NOT_PERFORMED
REPORT: Docs/Live-DuckDB-Hotpath-Backpressure-RCA-And-Acceptance.md
RESULTS: Docs/live-duckdb-hotpath-backpressure-results.json
```
