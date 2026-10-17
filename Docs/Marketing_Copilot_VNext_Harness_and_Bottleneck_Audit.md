# Marketing Copilot VNext Harness 与瓶颈审计

## 审计范围

本审计对应 Run `vnext-t3-t4-t6-t7-closure-20261017-codex02`，基于 Git `ee2591839a86806397284497d30eefe1f282054f`、产品源代码基线 `71ca9e25f40d9c57932ab8c02a6b69877a099eba`，只使用 EulerOS 原生 ext4、127.0.0.1 synthetic SMPP loopback 和隔离 Run。审计不把 one-shot 50K、历史 480K 或旧 harness 结果替代本轮 T3/T4 证据。

本轮 `409 passed, 5 skipped` 的本地 backend pytest 与新增脚本 `py_compile` 均通过。生产默认路径未启用测试时钟；本轮没有连接真实 SMSC、生产数据库或生产 API。

## 完整源代码链路映射

```text
bounded producer
  -> vnext.ingress.IngressRouter
  -> vnext.spool.TaskSpool (durable append / cursor / taken fence)
  -> vnext.worker.ShardWorker (prefetch / fair groups / bind lease)
  -> vnext.route_credits.RouteCreditBroker (TPS scope)
  -> vnext.smpp_transport.AsyncSmppTransport (window / socket write / response)
  -> ProgressJournal (one journal per worker)
  -> one ProjectionWriterProcess / one DuckDB owner
```

T6 额外经过 `VNextRuntime`、worker supervisor、control state、BusinessClock rollover 和 restart；T7 复用了已有 `vnext_fault_injection.py`，没有在工具外复制第二套 worker 或 writer 实现。

## Harness 问题与修复证据

### 早期 50K 缺响应

`t3-multi-50k-01` 中 `socket_written_pdu_count=50,000`，但 `submit_sm_resp_success=48,867`，同时 projection=50,000、spool pending=0。原因不是 SMSC loopback 丢包：worker 在 `run_until_idle` 返回后直接关闭 transport，清理了尚未被 response reader 消费的 pending window。

修复后的 `vnext_streaming_acceptance.py`：

1. 每个 worker pass 使用 `run_until_idle(close_transports=False)`。
2. 读取 transport snapshot，等待 `window_inflight=0`，再关闭 transport 和释放 physical bind。
3. response drain 超时有界，不能无限等待。
4. 每个 pass 结束释放 reader-only `_event_index` 和已完成的 destination locks，避免多 worker 复制整个 decoded spool index。
5. worker snapshot 落盘，不再输出 `null` worker result。

修复后 `t3-multi-50k-02/03/04` 均为 50,000 socket PDU、50,000 loopback receive、50,000 response、50,000 projection，且 max active binds=4。该修复属于测试 harness 生命周期修复，不改变 SMSC TPS、window 或 at-most-once 合同。

### 20K watermark / 全量扫描

feeder 热循环只使用 `TaskSpool.lightweight_snapshot()`，采样使用受限计数器和 1 秒周期；完整 `status()`、journal 全量读取和最终 reconciliation 只在收尾阶段发生。`t4-timeseries.csv` 记录了每个 Run 的时间序列，包含 pending、journal bytes、projection applied、SMPP received、CPU/RSS 和 Run bytes。

### 身份与多段计数

loopback 的 Campaign plan 可以显式指定 `20K,20K,5K,5K`；expected identity 以业务字段和 event/part 语义核对。mixed multipart 独立 Run `multipart-mixed-7500` 得到：5,500 task、11,000 PDU、11,000 response、5,500 projection。SMS 数不再被错误地等同于 PDU 数。

### T6 占位零值

同日 loopback 输出：

```json
{
  "business_day_checks": {
    "executed": false,
    "reason": "not_exercised_by_same_day_loopback_gate"
  },
  "old_business_day_socket_writes_after_cutoff": null,
  "old_business_day_re_dispatch_after_restart": null,
  "day_stats_cross_contamination": null
}
```

只有 `t6-daily-boundary-r2` 的 service-level Run 实测跨日并输出这些计数。

## 阶段指标与资源证据

| Run | active / drain | loopback PDU 采样 | max RSS | max pending | max Run bytes | projection 结果 |
|---|---:|---:|---:|---:|---:|---|
| T3-02 | 约 44.765s feed + 252.357s drain | P50 771.84 / P95 829.30 / max 859.28 | 950,160 KiB | 16,322 | 55,467,791 | 50,000/50,000 |
| T3-03 | 约 44.838s feed + 250.567s drain | P50 775.33 / P95 825.84 / max 874.29 | 951,124 KiB | 16,076 | 54,965,375 | 50,000/50,000 |
| T3-04 | 约 44.468s feed + 256.006s drain | P50 767.69 / P95 821.75 / max 851.74 | 947,180 KiB | 16,377 | 55,432,412 | 50,000/50,000 |
| T4-01 | guard 前未完成 | P50 796.23 / P95 831.73 | 2,109,300 KiB | 20,329 | 187,619,863 | 89,047；RSS guard fail |
| T4-02 | 600.292s + 371.071s | P50 797.51 / P95 838.92 / max 1,038.24 | 1,396,876 KiB | 20,351 | 535,334,638 | 155,166/492,000；drain fail |
| T4-03 | 600.313s + 4.424s | P50 75.55 / P95 86.91 / max 96.64 | 704,260 KiB | 48 | 63,954,725 | 45,050/45,050 |

采样 rate 是 1 秒窗口的 loopback received 增量；短窗口可能出现高于 4×200 配置上限的 burst，不能把单点峰值写成长期 SLA。正式 T3/T4 运行的实际调度配置为 all 64 CPUs、nice 0、ionice none；另有资源 guard smoke 证明 CPU 0–3/nice 10/ionice class 3 wrapper 能执行，但不混入正式数值。

## 瓶颈归因

### 已由证据排除或降低可能性

- **feeder 全量扫描卡死**：修复后 T3 三次、T4-03 均持续推进；producer 有 accepted/batches/backpressure 统计，未出现固定 20K watermark 卡死。
- **transport response 丢失**：修复 response window 后 T3-02/03/04 和 T4-02 的 socket PDU、SMPP receive、response 完全一致。
- **bind 泄漏导致 worker starvation**：每轮关闭前 drain，`max_active_binds=4`，没有超过 physical pool；早期 stream smoke 的 bind lease starvation 已保留为 harness RCA。
- **ext4 容量耗尽**：T4-02 Run bytes 约 0.50 GiB，结束后仍有约 13.1 GB 可用；没有因为磁盘保护阈值停止。
- **native crash**：已完成 Run 没有 SIGSEGV/native fault；T4-01 是显式 RSS guard，T4-02 是 bounded projection stop timeout，均不是 native crash。

### 主要瓶颈：单 ProjectionWriter 的 journal catch-up

T4-02 在约 800 task/PDU/s 的 active producer 下，492,000 socket PDU 和 492,000 response 全部闭合，RSS 在修复后约 1.33 GiB；但同一时期单 ProjectionWriter 的 replay/catch-up 明显低于 journal 产生速度。active 结束后又运行 371.071s，最终只应用 155,166 events，触发 bounded stop timeout。

T4-03 将 offered rate 降至 projection 可持续区间后，45,050 task 在 600.313s active 期间和 4.424s drain 内全部投影。这个对照把问题定位到 **ProjectionWriter journal replay / DuckDB batch apply 的服务能力与 offered rate 之间的差距**，而不是 feeder 或 loopback transport。`projection_lag_seconds=0` 只表示 writer 最近一次已应用事件的时间差，不能代替“journal 全部追平”；T4-02 的 `projection_events_applied` 与总 task 数的差异是更强的证据。

目前没有直接测量 SQL elapsed、commit elapsed、fsync 次数、journal read latency 或 ext4 IOPS，因此不能进一步断言瓶颈具体是 DuckDB SQL、fsync、journal 读取还是 Python 调度。下一步需要低扰动 stage counters 或离线 timing，不能凭末尾 lag=0 猜测。

### 资源瓶颈的第一处 RCA

T4-01 在约 160K 级别任务时越过 2 GiB aggregate RSS；worker 进程各自刷新并保留完整 `_event_index`，8 份 reader-side decoded records 与 destination locks 造成乘数效应。harness pass-boundary release 后：

- T4-02 492K active 任务仍将 max RSS 控制在 1,396,876 KiB；
- T4-03 max RSS 为 704,260 KiB；
- T4-02 的 transport 和 disk guard 均未触发。

这是测试/reader cache 生命周期的局部修复，不是多 Writer、第二 DuckDB 或架构重构。

## 未采用的优化

本轮没有：

- 增加每 Campaign ProjectionWriter 或 DuckDB；
- 删除 journal fsync、改变 at-most-once/unknown/partial 语义或放宽 cutoff fence；
- 提高 SMSC/route TPS 上限来制造“800+”结论；
- 把 high-pressure projection backlog 当作 PASS；
- 把 old-day 同日零值当成 T6 证据；
- 用旧 baseline 的 one-shot 结果替代 continuous producer + drain。

建议的最小下一步是先做 ProjectionWriter batch size、journal replay quantum、DuckDB staging/commit 和 admission backpressure 的固定条件 A/B；在有完整 drain 证据前，不做产品架构扩容。

## 故障矩阵审计

| 场景 | 本轮结果 |
|---|---|
| worker crash/restart | PASS，exit 17，5 + 15 恢复，20 unique |
| ProjectionWriter pause/resume | PASS，20 applied，duplicates 0 |
| campaign control pause/resume | PASS，pause 0 write，resume 5 write |
| feeder pause 30–60s / incremental resume | NOT_VERIFIED |
| single route unavailable / healthy route isolation | NOT_VERIFIED |
| IPC hint queue full | NOT_VERIFIED |
| journal high-water / hard limit | NOT_VERIFIED |
| spool capacity exhausted / recovery | NOT_VERIFIED |
| cross-day Worker/Writer fault combination | NOT_VERIFIED |

故障窗口必须继续按 `not_sent / unknown / partial / expired / written / response` 归类；crash 后允许的 at-most-once 漏发窗口不能被重试脚本改写成成功。

## 审计结论

```text
HARNESS_TRUSTWORTHY=PASS
BOTTLENECK_ATTRIBUTED=YES (coarse stage: ProjectionWriter journal catch-up)
PERFORMANCE_NON_REGRESSION=NOT_VERIFIED
T7_STAGE_RECOVERY=NOT_VERIFIED
CANARY_READY=NO
```

原始 evidence 位于：

`/home/paas/marketing-copilot-test/runs/vnext-t3-t4-t6-t7-closure-20261017-codex02/evidence/`

其中包括三次 T3 JSON、T4 JSON 与 CSV、T6 daily boundary、T7 子场景、R0 环境/依赖/磁盘证据、stage-bottleneck 摘要和失败样本。数据库、WAL、spool、journal、venv 未复制到 Git 或本报告。
