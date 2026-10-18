# Marketing-Copilot VNext：Worker Scaling AB/BA 与推荐

## 最终决策

`FINAL_DECISION=CORRECTNESS_BLOCKED`。本任务没有可用于吞吐比较的 5K AB/BA Run，因此不推荐 12、16 或 24 Worker，也不把测试候选写入生产配置。`RECOMMENDED_WORKERS=NOT_VERIFIED`。

## 预筛矩阵

| 顺序/Run | Workers | 5K 资格 | 状态 | 原因 |
|---|---:|---|---|---|
| smoke-8 | 8 | 否（仅 1K） | PASS | 8/8 结果文件，8/8 清洁退出 |
| smoke-12 | 12 | 否 | FAIL | 12 个真实进程峰值，但仅 11 个结果文件/清洁退出；Worker 5 被 SIGTERM |
| smoke-16 | 16 | 否 | ABORTED | 约 3m50s 仍有高 CPU Worker 和僵尸子进程，未形成 result.json |
| smoke-24 | 24 | 否 | FAIL | 24 个真实进程峰值，但 Worker 11/13/17 缺结果；仅 21 个清洁退出 |
| smoke-24-r2 | 24 | 否 | FAIL | 延长 join 至 120s 后仍缺 Worker 18；仅 23 个清洁退出 |

由于 12、16、24 未通过 1K smoke 的正确性/生命周期门槛，按任务定义不启动 `8→12→16→24` 或 `24→16→12→8` 的 5K 正式矩阵。故：

```text
FIVE_K_AB_RUNS=0
FIVE_K_BA_RUNS=0
VALID_5K_RUNS=0
PAIRWISE_MEDIANS=NOT_AVAILABLE
50K_RUN=NOT_RUN_BY_GATE
600S_800PDU=NOT_RUN
```

## 口径与已观测值

Smoke 不是吞吐赢家依据，但用于确认前置边界。已完成结果的共同硬数据：

- `accepted=1000`、`socket_written_PDU=1000`、`submit_sm_resp_success=1000`；
- `journal_events_durable=2000`、`projection_events_applied=2000`、`unknown=0`；
- `max_active_binds=4`、`resource_gate=PASS`、Projection completion 为 `COMPLETED`；
- 12/24 的 FAIL 只由 `worker_result_files_count != requested_workers` 和 `workers_exited_cleanly_count != requested_workers` 触发。

因此本轮不能把 12/24 的较低或较高 1K socket rate 与 8 Worker 作 stage/wall 比较，也不能把 Worker 收尾失败解释成吞吐收益或生产容量。

## 使用的冻结命令口径

每个 smoke 使用同一源树、同一 Python/DuckDB、同一 native ext4 run root 体系，仅改变 `RUN_ROOT`、Run ID 和 `--workers`；核心参数为：

```text
--counts 250,250,250,250
--workers 8|12|16|24
--pool-size 4
--route-tps 200 --tps-scope per_bind --window-size 50
--batch-size 250 --projection-batch-size 512
--projection-staging-mode vectorized --projection-status-interval 0.1
--instrumentation on --profile-interval 1.0 --response-delay-ms 0
--resource-profile PROD_8G --rss-guard-scope application
--projection-drain-timeout 120
```

服务器结果根目录：`/home/paas/mc-vnext-worker-scaling-20261018-01/runs`。没有连接真实 SMSC、真实短信、生产数据或生产 API。

## 推荐

当前不推荐任何扩容值。下一步若要继续，应先开独立的 Worker/Spool/RouteCredit 生命周期 RCA 任务：要求能稳定完成 12/16/24 的 1K smoke，所有 Worker 结果文件和清洁退出一致，再重新执行两轮反向 5K 矩阵。该修复在本轮不应通过修改生产实现或放宽 at-most-once、Bind、TPS、Window、durability 门槛来完成。

生产默认值、Canary、600 秒容量验收和 50K 均未执行或发布。
