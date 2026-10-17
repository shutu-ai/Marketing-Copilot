# Marketing Copilot VNext Daily Expiry / Stage Recovery / Single Writer 闭环报告

任务：`MC-VNEXT-DAILY-EXPIRY-STAGE-RECOVERY-SINGLE-WRITER-20261017`  
报告时间：2026-10-17  
参考基线：`origin/main = 439a1842c4f1ed9144fd7605695435fca76c472c`

## 1. 结论先行

本轮 D0-D3 的代码、单元回归和本地 loopback 最小闭环已完成：业务日持久化与硬截止、旧日隔离/回收、StageCredit 丢唤醒修复、bounded Journal admission、Projection 单 Writer + 单 DuckDB、set-based 批量聚合和并发 harness 时序均已落地。

最终发布门禁仍为：

```text
EULEROS_CONCURRENT_E2E_GATE = BLOCKED_ENV
PERFORMANCE_NON_REGRESSION = NOT_VERIFIED
NATIVE_STABILITY_GATE = NOT_VERIFIED
OVERALL = VNEXT_DAILY_STAGE_PROJECTION_NO_GO
CAMPAIGN_PER_WRITER_DUCKDB = DEFERRED_NOT_IMPLEMENTED
```

`NO_GO` 只表示远端/原生性能门禁尚未完成，不表示本地代码回归失败。没有把 Windows smoke、历史 P5500 或 credit-only 测试冒充 EulerOS 端到端通过。

## 2. 设计闭环

### 2.1 Daily expiry

`BusinessClock` 统一提供业务时区、业务日、当前时间和次日午夜 cutoff。SpoolRecord 将 `business_day`/`expires_at` 作为 CRC 覆盖的 durable 字段；旧 V1 缺少日期或 expiry 时 fail-closed。Ingress admission、Worker 取任务后、Bind 后、checkpoint 后、每个 PDU 的 `writer.write` 紧邻处都检查当前 epoch/cutoff。

过期只 checkpoint 并生成 `expired` 进度，不调用 transport；若 multipart 已经写出一部分，transport 返回 `partial`/`unknown` 语义，禁止把不确定结果重新变成可派发任务。

### 2.2 Daily Spool recovery/reclaim

manifest 保存 `current_business_day`、`business_epoch`、`closed_days`、每日记录/字节统计；同一 partition 的 segment 不跨日复用。`iter_from()` 只扫描当前业务日；restart/recover 不会把旧日记录重新送出。`reclaim_business_day()` 只回收已经关闭且可明确归属的旧日 segment，旧 V1 无归属时保留。

Runtime 先切换并 fence 旧日，再停止 Worker，最后 cleanup closed days；不会在仍有旧 Worker 引用时删除目录。

### 2.3 Stage isolation and recovery

StageCredit 的 reserve predicate 与 wait 位于同一 Condition 临界区，并用 wake sequence 防止 release-before-wait 丢唤醒。Runtime 给 IngressRouter 传入 ingress credit；ProgressJournal 有最大事件/最大字节限制，Worker 在 dispatch 前检查 Journal 可追加，避免 socket 写入后无法记录结果。

### 2.4 Single Projection Writer

Worker 只写 durable journal，不打开业务 DuckDB。一个 `ProjectionWriterProcess` 独占一个 `vnext-projection.duckdb`，通过 journal offsets 恢复。每个 batch 内先去重 event_id，再用临时 staging 表插入新事件集合，最后按日、Campaign、partition、route 做 set-based 聚合 UPSERT。duplicate replay 不增加聚合计数。

多 journal 以 bounded quantum round-robin 读取；低流量 Campaign 不会因某个大 Campaign 占满单批预算而长期饥饿。

### 2.5 Concurrent acceptance harness

`backend/tools/vnext_loopback_acceptance.py` 现在先启动 ProjectionWriter，再启动 Worker；Worker、真实 SMPP loopback socket、ProgressJournal tailer 和 DuckDB writer 在同一时间窗运行，最终同时对账 generated/accepted/taken/socket_written/response/projection/expired/unknown，并检查旧日重派、旧日 socket 写入、重复 wire 和跨日统计污染。

## 3. 变更清单

| 文件 | 变更摘要 |
|---|---|
| `backend/vnext/business_day.py` | 新增可注入时钟、时区和 cutoff helper |
| `backend/vnext/spool.py` | 日期字段、epoch、过期 fence、日统计、旧日回收、并发读取容错 |
| `backend/vnext/ingress.py` | 日 admission、日期 namespace、Ingress credit |
| `backend/vnext/pressure.py` | StageCredit predicate/wake race 修复 |
| `backend/vnext/worker.py` | 多阶段 cutoff、expired 终态、响应进度、Journal admission |
| `backend/vnext/smpp_transport.py` | 每 PDU 最终 write fence、响应 context 与 response sink |
| `backend/vnext/runtime.py` | daily epoch loop、Ingress credit、Journal 上限、停止顺序 |
| `backend/services/vnext_service.py` | 当前业务日 progress/task stats 过滤与 rollover wrapper |
| `backend/services/activity_scheduler.py` | VNext daily rollover 调度接入 |
| `backend/vnext/projection_writer.py` | bounded Journal、set-based batch、按日 projection、round-robin journal replay |
| `backend/vnext/metrics.py` / `backend/vnext/__init__.py` | 状态字段和异常导出 |
| `backend/tools/vnext_loopback_acceptance.py` | 并发 send + projection loopback harness |
| `backend/tests/test_vnext_daily_expiry_projection.py` | 新增日期、回收、过期、StageCredit、Projection 测试 |

## 4. Gate 矩阵

| Gate | 状态 | 证据/限制 |
|---|---|---|
| `DAILY_EXPIRY_GATE` | `PASS` | 新增跨日/过期 Worker 测试；受影响 VNext 测试 32 passed |
| `SPOOL_RECLAIM_GATE` | `PASS` | 旧日 segment 回收、重启只见当前日、未归属旧 V1 保留测试通过 |
| `STAGE_ISOLATION_RECOVERY_GATE` | `PASS` | StageCredit release-before-wait、bounded Journal 和全量回归通过 |
| `SINGLE_WRITER_SET_BASED_GATE` | `PASS` | Projection batch 去重/按日隔离/daily summary 测试通过；harness 仅产生一个 DuckDB writer |
| `CAMPAIGN_FAIRNESS_GATE` | `PASS` | Worker fairness、journal round-robin 相关既有回归通过；未将其解释为吞吐率证明 |
| `EULEROS_CONCURRENT_E2E_GATE` | `BLOCKED_ENV` | 未取得 EulerOS 专用环境；Windows 多 worker 运行被宿主 SIGINT 打断，不能替代 native ext4 |
| `PERFORMANCE_NON_REGRESSION` | `NOT_VERIFIED` | 未运行要求的三轮 old/new A/B 和持续吞吐/lag/RSS/CPU/I/O 采样 |
| `NATIVE_STABILITY_GATE` | `NOT_VERIFIED` | 未在目标 EulerOS/SLES 运行长稳、故障注入和 native crash 证据采集 |
| `OVERALL` | `VNEXT_DAILY_STAGE_PROJECTION_NO_GO` | 必需远端/性能门禁尚未具备真实 PASS |
| `CAMPAIGN_PER_WRITER_DUCKDB` | `DEFERRED_NOT_IMPLEMENTED` | 本轮明确不实现每 Campaign 独立 Writer/DB |

## 5. 已执行验证

### 5.1 后端回归

```text
.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
409 passed, 5 skipped, 4 warnings in 160.82s
```

受影响集合单独回归：

```text
32 passed in 16.16s
```

### 5.2 新增日期/Projection 专属测试

覆盖：

- 23:59:59 -> 次日的 epoch 切换；
- 旧日不再被 iter/restart/recover 派发；
- 同 source 跨日生成新 event ID；
- 旧日 segment 只在 closed 后回收；
- 缺少 `business_day`/`expires_at` 的 V1 记录 fail-closed；
- Worker 过期只 checkpoint，不调用 transport；
- release-before-wait 不丢唤醒；
- 同批 event_id 去重、跨日聚合隔离、daily summary。

### 5.3 前端构建

```text
workdir=frontend
npm run build
✓ built in 1.86s
```

构建成功；输出的 `INVALID_ANNOTATION`（@vueuse）和大 chunk 是依赖/既有 bundle warning，不是本任务新增编译错误。

### 5.4 本机 loopback smoke

独立临时 run root 下执行了单 worker 最小并发时序 smoke：

```text
generated=1
accepted=1
taken=1
socket_written=1
submit_sm_received=1
submit_sm_resp_success=1
projection_applied=1
pending=0
old_business_day_socket_writes_after_cutoff=0
old_business_day_re_dispatch_after_restart=0
projection_started_before_workers=true
execution_mode=concurrent_worker_journal_projection
status=PASS
```

双 worker、多个任务在本机 Windows 控制台尝试时，进程树收到宿主 `SIGINT`，Worker/Projection 打印 `KeyboardInterrupt`，没有把不完整结果纳入通过证据。双 worker、单任务 smoke 可结束，但因只有一个有效任务，不能证明多 worker 吞吐或公平性。

## 6. 性能与环境门禁

下列项目刻意标为未验证：

1. EulerOS 2.0 SP15 / native ext4 / CPython 3.11.5 的 T1-T8；
2. P5500 历史串行基线与新并发 send+projection 的三次 A/B；
3. 8 Worker / 4 Bind / per-bind 200 TPS / 800 PDU/s 上限；
4. 长稳 600 秒、Projection backlog、RSS、CPU、I/O、journal lag；
5. Writer pause/restart、Worker crash、Route failure、SLES12SP5 目标 OS 验证；
6. 真实 SMSC、生产数据、生产部署与远端账户。

正式测试必须在 WSL 分发版的 native ext4 工作目录执行，并记录：

```text
df -T .
python --version
python -m pip freeze
```

`/mnt/<drive>` 或本机 Windows 临时目录只能做 smoke/同步，不能作为 Linux 性能基线。

## 7. 交付判断与后续动作

代码和本地回归可以交给后续验收，但当前不应宣称生产 READY。要把 `OVERALL` 提升为 `VNEXT_DAILY_STAGE_PROJECTION_READY`，需要在有授权的专用 EulerOS/ext4 环境完成并保存机器 JSON、运行配置、退出码、资源采样、fault evidence 和三轮 A/B；若环境仍不可用，保持 `BLOCKED_ENV/NOT_VERIFIED`，不要修改门禁含义。

本任务未提交或推送任何 Git 变更，也未删除仓库中既有 dirty/untracked 文件。
