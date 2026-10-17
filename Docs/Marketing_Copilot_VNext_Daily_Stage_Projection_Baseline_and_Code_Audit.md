# Marketing Copilot VNext Daily / Stage / Projection 基线与代码审计

审计时间：2026-10-17（Africa/Cairo）  
任务：`MC-VNEXT-DAILY-EXPIRY-STAGE-RECOVERY-SINGLE-WRITER-20261017`

## 1. 基线身份与工作区边界

| 项目 | 值 |
|---|---|
| `origin/main` | `439a1842c4f1ed9144fd7605695435fca76c472c` |
| 审计时 `HEAD` | `439a1842c4f1ed9144fd7605695435fca76c472c` |
| 审计时 `HEAD^{tree}` | `876d2e1c77805f183266349326a38e784d6e8600` |
| 参考源码基线 | 任务文档指定的 `439a1842` |
| 仓库状态 | dirty；存在任务开始前的 `AGENTS.md`、`frontend/package-lock.json` 及多个未跟踪材料 |
| 提交/推送 | 本任务未执行 `git commit` 或 `git push` |

仓库存在 `.codegraph/`；本次源码定位先使用了 CodeGraph，再使用精确的 `rg`/行号核对。没有执行 `reset`、`clean`、`stash`，也没有清理或覆盖既有 dirty/untracked 文件。

### 1.1 本轮改动的源码边界

本轮只触及与任务直接相关的 VNext、调度接入、验收工具和新增测试：

- `backend/vnext/business_day.py`（新增）：可注入 BusinessClock、明确时区、业务日与午夜 cutoff。
- `backend/vnext/spool.py`：持久化 `business_day`/`expires_at`、epoch、旧日隔离、过期检查、回收与每日统计。
- `backend/vnext/ingress.py`：业务日 admission、日期参与 event namespace、Ingress StageCredit 接线。
- `backend/vnext/pressure.py`：修复 StageCredit 的 release-before-wait 丢唤醒竞态。
- `backend/vnext/worker.py`、`backend/vnext/smpp_transport.py`：取任务/Bind/Window/TPS/每个 PDU 写入前的双重 cutoff fence，以及响应事件 journal。
- `backend/vnext/runtime.py`、`backend/services/vnext_service.py`、`backend/services/activity_scheduler.py`：daily epoch owner、服务层日过滤和 VNext 日切调度接入。
- `backend/vnext/projection_writer.py`：单 Writer/单 DuckDB、journal 回放、受限 Journal、staging + set-based 聚合、按日隔离。
- `backend/vnext/metrics.py`、`backend/vnext/__init__.py`：状态字段与异常导出。
- `backend/tools/vnext_loopback_acceptance.py`：Projection Writer 先启动，Worker/SMPP/journal/projection 同时运行的 E2E harness。
- `backend/tests/test_vnext_daily_expiry_projection.py`（新增）：日期边界、旧日回收、StageCredit、worker 过期和 Projection 批量去重测试。

## 2. 基线审计结果

| 审计项 | 基线观察 | 本轮处置 | 代码证据 |
|---|---|---|---|
| A1 日切调度 | Legacy 每日 reset 与 VNext epoch 不是同一条边界 | VNext active 时调用 `vnext_daily_rollover`，不让 legacy reset 代替 VNext spool fence | `backend/services/activity_scheduler.py:132` |
| A2 Spool 日期与回收 | 原 Spool 只有单 root、cursor/taken 和全局容量，没有 durable business day | 新增 BusinessClock、manifest epoch/closed day、日期同段、旧日 fail-closed、显式回收 | `backend/vnext/business_day.py:92`、`backend/vnext/spool.py:181`、`:906` |
| A3 Socket 截止 | 原 Worker 在 checkpoint 后直接进入 transport，缺少最终写入前的业务日判断 | Worker、transport credit/window 后及每个 `writer.write` 紧邻处均再次检查；已写部分保持 partial/unknown，不自动重派 | `backend/vnext/worker.py:228`、`backend/vnext/smpp_transport.py:338` |
| A4 Ingress namespace | event ID 未绑定业务日 | business day 加入 event namespace；同 source 跨日会产生新 event ID；错误日直接拒绝 | `backend/vnext/ingress.py:47`、`:67` |
| A5 StageCredit | reserve 可能在检查和 wait 之间丢通知；Runtime 未给 ingress 接入 credit | 条件谓词在同一 Condition 下检查，加入 wake sequence；Runtime 创建并传递 ingress credit | `backend/vnext/pressure.py:19`、`:81`、`backend/vnext/runtime.py:104` |
| A6 Journal/IPC 背压 | IPC hint 不是 durable source，Journal 需要有界 admission 与恢复 | Journal 保留硬字节上限；Worker 在 dispatch 前检查 Journal capacity；Projection 以 Journal offset 回放为准 | `backend/vnext/projection_writer.py:26`、`backend/vnext/runtime.py:104` |
| A7 Projection SQL | 旧实现对事件逐条执行 INSERT/UPSERT | 批内 event_id 去重、临时 staging、只插入 truly-new event，再按 `(business_day,campaign_type,campaign_id,partition_id,route_id)` set-based aggregate UPSERT | `backend/vnext/projection_writer.py:261` |
| A8 Journal 公平性 | 固定 journal 顺序可能让小 Campaign 长时间得不到预算 | 多 journal 使用有界 quantum 和 round-robin cursor | `backend/vnext/projection_writer.py:455` |
| A9 Journal fsync | 每事件 fsync 是既有 durable fence，不能未经证明删除 | 本轮保留；只优化 Projection 内部 batch，不把减少 fsync 当成安全性证明 | `backend/vnext/projection_writer.py:26` |
| A10 E2E 时序 | 原 harness 是 Worker 完成后才启动 Projection | Projection 在 Worker 进程启动前启动，结果同时从真实 journal 和 socket simulator 对账 | `backend/tools/vnext_loopback_acceptance.py:170`、`:219` |
| A11 覆盖 | 原测试缺少跨日、旧日重启、丢唤醒和 set-based 日隔离 | 新增专属测试并通过全量回归 | `backend/tests/test_vnext_daily_expiry_projection.py` |

## 3. 已确认的业务契约

### 3.1 业务日、时区与 cutoff

- 默认业务时区为 UTC；可由 `MC_VNEXT_BUSINESS_TIMEZONE` 配置，兼容 `MC_BUSINESS_TIMEZONE`。
- 所有新 admission 都写入 `business_day` 和对应业务日下一个午夜的 `expires_at`。
- `business_day` 缺失或 `expires_at <= 0` 的旧记录只允许恢复审计，不允许进入 live dispatch；这是 fail-closed 兼容策略。
- 业务日切换提升 durable `business_epoch`，关闭旧日并让旧日 Worker/Socket admission 失效。
- 旧日已经写入 socket 的 PDU 可以收到迟到响应，但响应仍按旧日归属，不重新写入旧日 PDU，也不污染当前日统计。

### 3.2 Spool 与恢复

- 同一 partition 的 segment 不跨业务日复用；manifest 保留每日记录数/字节数、closed summary、cursor/taken 状态。
- `iter_from()` 只返回当前业务日且未 checkpoint 的记录；旧日不会因 restart/recover 被重新 dispatch。
- `reclaim_business_day()` 只删除已关闭且能明确归属该日的 segment；缺少日期归属的旧 V1 文件保留，不猜测删除。
- Runtime 在 Worker 停止后执行 closed-day cleanup，避免日切回收与仍持有旧文件引用的 Worker 并发。

### 3.3 Stage 与单 Writer

- Ingress、Worker journal admission、Projection IPC/Journal 都是 bounded；阶段阻塞通过 credit/backpressure 传播，不以无限内存队列掩盖压力。
- Worker 不打开应用 DuckDB；ProjectionWriterProcess 独占一个 DuckDB 文件。
- Projection 的提交边界是：`BEGIN -> staging/新事件集合 -> set-based aggregate UPSERT -> COMMIT -> journal offset`。
- DB commit 后 offset 写入前崩溃时，Journal 重放通过 event_id 去重，不重复明细或聚合计数。
- “每 Campaign 一个 Writer / 独立 DuckDB”本轮明确保持 `DEFERRED_NOT_IMPLEMENTED`，没有加入占位架构。

## 4. 可复核验证命令

以下命令均在独立临时目录或仓库测试环境执行；没有使用生产 DuckDB、真实 SMSC 或仓库内历史 evidence 目录作为临时运行目录。

```powershell
.p0-test-venv\Scripts\python.exe -m pytest backend/tests/test_vnext_spool.py backend/tests/test_vnext_ingress_pressure.py backend/tests/test_vnext_projection_writer.py backend/tests/test_vnext_worker.py backend/tests/test_vnext_smpp_transport.py backend/tests/test_vnext_runtime_canary_closure.py backend/tests/test_vnext_runtime.py backend/tests/test_vnext_service.py backend/tests/test_vnext_daily_expiry_projection.py -q
# 32 passed in 16.16s

.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
# 409 passed, 5 skipped, 4 warnings in 160.82s

npm run build  # workdir=frontend
# ✓ built in 1.86s
```

本机 loopback 最小 smoke：

```powershell
.p0-test-venv\Scripts\python.exe backend/tools/vnext_loopback_acceptance.py `
  --count 1 --workers 1 --pool-size 1 --timeout 5 `
  --run-root <独立临时目录> --quiet
# PASS: generated=1, accepted=1, socket_written=1, projection_applied=1,
#      submit_sm_received=1, submit_sm_resp_success=1, pending=0
```

该 smoke 的结果 JSON 明确包含 `execution_mode=concurrent_worker_journal_projection` 和 `projection_started_before_workers=true`；它证明工具时序已改变，但不是 EulerOS 规模性能证明。

## 5. 未执行或不应冒充通过的验证

- 未连接真实 SMSC、生产数据或生产部署。
- 未在 EulerOS 2.0 SP15/native ext4/CPython 3.11.5 上执行 T1-T8。
- 未在 SLES12SP5 目标系统上执行兼容性验证。
- 未执行 3 次 old/new A/B、P50/P95、RSS/CPU/I/O、journal lag 或 800 TPS 的定量性能门禁。
- 当前 Windows 宿主上尝试 `workers=2` 且多个任务的 loopback 运行时，宿主控制台向 multiprocessing 进程树发送 `SIGINT`，子进程打印 `KeyboardInterrupt`，未生成可接受的完整结果 JSON；该结果不能替代 native ext4 并发 E2E，因此报告标记为 `BLOCKED_ENV`，而不是 PASS。

本文件的结论是“代码级审计和本地回归完成”，不是“远端生产/性能验收完成”。
