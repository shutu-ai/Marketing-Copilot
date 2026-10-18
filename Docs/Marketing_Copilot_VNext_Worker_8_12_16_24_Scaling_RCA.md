# Marketing-Copilot VNext：SLES Worker 8/12/16/24 扩容 RCA

## 结论

本任务未进入 5K 正式矩阵。Harness 的静默 `min(8, workers)` 已修复，8/12/16/24 参数均能真实进入 Worker 创建路径；但 12 和 24 Worker 的 1K smoke 均未通过“每个 Worker 结果文件 + 清洁退出”硬门槛，16 Worker smoke 在约 3 分 50 秒仍有高 CPU Worker 与僵尸子进程，已按边界中止。因此本轮结论为 `CORRECTNESS_BLOCKED`，不产生 Worker 吞吐赢家或生产推荐。

失败不是 PDU/Projection 对账失败：已完成的结果中 `accepted/socket_written/response_success=1000`、Journal=2000、Projection=2000、unknown=0，且物理 Bind 峰值均为 4；失败点是多 Worker 收尾时结果文件/清洁退出不完整。现有证据足以阻止性能扩容，但不足以在不修改生产 `worker.py`、Spool、RouteCredit 或 Projection 实现的前提下断言具体锁点。

## 变更范围

- `backend/tools/vnext_streaming_acceptance.py`：
  - `--workers` 严格接受 `8/12/16/24`，默认仍为 8；0、负数、7、25 等值明确报错，不再静默 clamp。
  - `PartitionMap`、Journal 路径和 Worker 进程按请求数量创建。
  - 结果 JSON 增加 `requested_workers`、`effective_workers`、`worker_result_files_count`、`partition_owner_distribution`、`workers_alive_peak`、`workers_exited_cleanly_count`、进程/Journal 文件证据。
  - Worker 收尾 join 窗口从 15 秒扩大到 120 秒；仍超时则保留失败并终止，不把结果标为 PASS。
- `backend/tests/test_vnext_streaming_acceptance.py`：增加 Worker 参数、256 分区覆盖和共享物理 Bind 上限的轻量回归。
- 未修改 `backend/vnext/worker.py`、`route_credits.py`、`smpp_transport.py`、`spool.py`、`projection_writer.py` 或生产默认配置。

## 环境与冻结条件

| 项目 | 值 |
|---|---|
| `SOURCE_HEAD` | `14be326fdf9d33f2a52e871ce798170a876ff8e6` |
| 服务器 | `paas-core01`，SLES 12 SP5，kernel `4.12.14-122.307-default` |
| 文件系统 | `/dev/sda1`，native `ext4` |
| Python / DuckDB | CPython 3.11.4 / `duckdb==1.2.0` |
| 运行根目录 | `/home/paas/mc-vnext-worker-scaling-20261018-01` |
| 物理 Bind / TPS / Window | `4 / 200 per Bind (800 total) / 50` |
| SMSC | 仅 `127.0.0.1` synthetic loopback |
| 资源 Profile | `PROD_8G`，application RSS guard，limit 6,291,456 KiB |

运行前磁盘约 4.22 GB 可用；中止后约 3.80 GB 可用。服务器 80 CPU，运行期间 load 约 8–13，未执行 50K 或 600 秒长跑。

## Smoke 证据

| Run | 请求/实际 | Worker 结果/清洁退出 | 峰值存活 | 结果 | 关键证据 |
|---|---:|---:|---:|---|---|
| `smoke-8` | 8 / 8 | 8 / 8 | 8 | PASS | 1000 PDU，Projection 2000，Bind 4，14.222s |
| `smoke-12` | 12 / 12 | 11 / 11 | 12 | FAIL | Worker 5 无结果，父进程最终 SIGTERM；Projection 2000 |
| `smoke-24` | 24 / 24 | 21 / 21 | 24 | FAIL | Worker 11/13/17 无结果；Projection 2000 |
| `smoke-24-r2` | 24 / 24 | 23 / 23 | 24 | FAIL | Worker 18 无结果；120s join 后仍 SIGTERM |
| `smoke-16` | 16 / 未形成结果 | NOT_CAPTURED | 运行中观察到 16 个子进程 | ABORTED | 约 3m50s 仍有高 CPU Worker/僵尸子进程，按测试边界终止进程组 |

12/24 的失败结果均为 `completion_state=COMPLETED`、`resource_gate=PASS`，但 Harness 的 PASS 条件额外要求所有 Worker 结果文件和清洁退出，因此没有被吞吐统计接受。24 的两次 smoke 均真实达到 `workers_alive_peak=24`，不存在“请求 24、实际 8”的静默折算。

## RCA 判断与限制

1. **已确认：** Worker 数量参数和 256 分区 owner 分布真实生效；共享物理 Bind 上限没有随 Worker 放大，已完成结果的 `max_active_binds=4`。
2. **已确认：** 失败 Run 的业务 PDU、response Journal 和 Projection 对账已完成，问题不是把错误吞吐写成 PASS。
3. **高置信度边界判断：** 多 Worker 下存在尚未闭合的 Worker/Spool/lease 收尾争用或等待，导致单个或多个 Worker 无法在合理 join 窗口内生成结果；增加 join 窗口只能延迟失败，不能视为修复。
4. **未确认：** 具体阻塞是在 Worker dispatch、跨进程 Spool/lease、Transport 收尾还是文件结果写入；任务禁止修改生产实现，故本轮不做猜测性重构。

因此不能用这些 1K smoke 的 stage/wall 数字宣称 12/16/24 的吞吐收益。5K AB/BA、50K 和 600s/800PDU/s 均因前置正确性门槛未通过而不执行。

## 本地验证

- Harness 定向回归：`23 passed`。
- 完整后端回归首次：`443 passed, 5 skipped, 1 failed`；唯一失败为已有 Windows 文件替换锁定测试 `WinError 5`。
- 重跑该单测：`1 passed`，判定为瞬时 Windows 文件锁/环境波动；未修改该生产路径。

## 机器摘要

```text
SOURCE_HEAD=14be326fdf9d33f2a52e871ce798170a876ff8e6
TARGET_SERVER_OS=SLES12SP5
NATIVE_EXT4=YES
PRODUCT_CODE_CHANGED=NO
HARNESS_WORKER_CAP_FIXED=YES
REQUESTED_WORKERS_MATRIX=8,12,16,24
EFFECTIVE_WORKERS_PER_RUN=8->8;12->12;24->24;16=NOT_CAPTURED_AFTER_ABORT
MAX_CONCURRENT_PHYSICAL_BINDS=4 (completed result runs)
GLOBAL_CONFIGURED_TPS_CEILING=800
WINDOW_SIZE=50
FIVE_K_AB_BA_VALID_RUNS=0 (stopped at smoke correctness gate)
W8_STAGE_MEDIAN=NOT_RUN_BY_GATE
W12_STAGE_MEDIAN=NOT_RUN_BY_GATE
W16_STAGE_MEDIAN=NOT_RUN_BY_GATE
W24_STAGE_MEDIAN=NOT_RUN_BY_GATE
W8_E2E_WALL_MEDIAN=NOT_RUN_BY_GATE
W12_E2E_WALL_MEDIAN=NOT_RUN_BY_GATE
W16_E2E_WALL_MEDIAN=NOT_RUN_BY_GATE
W24_E2E_WALL_MEDIAN=NOT_RUN_BY_GATE
W24_VS_W8_STAGE_DELTA_PERCENT=NOT_RUN_BY_GATE
W24_VS_W8_WALL_DELTA_PERCENT=NOT_RUN_BY_GATE
WORKER_QUEUE_AND_BIND_CONTENTION=WORKER_CLEANUP_OR_LEASE_CONTENTION_UNRESOLVED; max_bind=4
JOURNAL_PROJECTION_BACKLOG_AND_DRAIN=COMPLETED_FOR_RESULT_RUNS; projection_backlog=0
APPLICATION_RSS_PEAK_AND_RESOURCE_GATE=0.26/0.38/0.77 GiB for 8/12/24; resource_gate=PASS
CORRECTNESS_AND_AT_MOST_ONCE=BLOCKED_BY_WORKER_RESULT_AND_CLEAN_EXIT_GATE
50K_RUN=NOT_RUN_BY_GATE
600S_800PDU=NOT_RUN
PRODUCTION_DEFAULT_CHANGED=NO
CANARY_READY=NO
RECOMMENDED_WORKERS=NOT_VERIFIED
FINAL_DECISION=CORRECTNESS_BLOCKED
COMMIT_PUSH=NOT_REQUESTED
```
