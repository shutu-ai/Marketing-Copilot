# Marketing-Copilot VNext：Worker Shutdown Lifecycle RCA

## 结论

本次卡滞的直接根因已经确定为测试 Harness 的停机信号没有传入 `ShardWorker` 自身的 asyncio stop boundary：父进程设置了 multiprocessing `stop_event`，但 `ShardWorker._stop` 仍未置位。一个 Worker 在 `run_until_idle()` 返回后看到全局 Spool 的瞬时/陈旧 `pending_records=1`，于是继续进入 `run_until_idle()`/snapshot polling；外部 stop 信号不能打断这条循环。父进程最终在 join deadline 后终止子进程，因此 Worker 没有执行到 `asyncio.run()` 返回和最终结果文件写入。

这不是 SMPP 数据面、Bind 超配、Journal 或 Projection 数据丢失。受控失败现场中所有 1000 条 PDU 都得到成功响应，Journal/Projection 都到达 EOF；卡住的 Worker 没有活动 Bind lease，`response_pending=0`。修复只改 Harness 生命周期和观测协议，没有改生产 `backend/vnext` 代码、默认配置、发送限速、窗口、fsync、checkpoint 或 at-most-once 语义。

## 冻结环境与证据位置

- `SOURCE_HEAD=ORIGIN_MAIN=a25726d47d5963df85b7da754733029591345539`
- SLES 12 SP5，`paas-core01`，kernel `4.12.14-122.307-default`，80 CPU。
- 正式 workspace 在 `/dev/sda1` native `ext4`：`/home/paas/mc-vnext-worker-scaling-20261018-01`。
- CPython `3.11.4`，`duckdb==1.2.0`，`pip freeze sha256=502422a5a1be6e2cea2e0ae206c01940b3104c6d906545d8219cdbda72a04227`。
- 冻结 source archive provenance：`805bec45f8e7ec6576a729704eccb405ba8c5cb7b0b145d82474fc2832f4b551`；本任务在该源树上叠加的 Harness SHA-256 为 `6c559cf8c2809e6e5c23e19a0d12c5759f4678eeb27b585b74fbb7bc3257217b`。
- 配置保持 `pool_size=4`、`route_tps=200`、`tps_scope=per_bind`、`window=50`、synthetic SMSC `127.0.0.1`。

服务器原始失败、诊断和修复后的 `result.json` 仍保留在服务器隔离目录；没有复制进 Git：

```text
/home/paas/mc-vnext-worker-scaling-20261018-01/runs/smoke-12
/home/paas/mc-vnext-worker-scaling-20261018-01/runs/lifecycle-12-r1
/home/paas/mc-vnext-worker-scaling-20261018-01/runs/lifecycle-12-r2
```

## 修复前的现场复现

旧 `smoke-12` 已完成数据对账但失败于生命周期门禁：12 个真实 Worker 中只有 11 个结果文件/clean exit，Worker `5` 的 exitcode 为 `-15`；`accepted/socket_written/submit_sm_resp_success=1000/1000/1000`，Journal/Projection 为 `2000/2000`。

为避免继续扩大失败样本，使用同一 1K workload 做了一次受控诊断复现 `lifecycle-12-r1`。关键现场如下：

| Worker | 最后 phase | loop pass | stop_event | feeder_done | global/local pending | response pending |
|---:|---|---:|---|---|---:|---:|
| 2 | `RUN_UNTIL_IDLE_ENTER` | 703 | true | true | 1 / 1 | 0 |
| 5 | `RUN_UNTIL_IDLE_ENTER` | 722 | true | true | 1 / 1 | 0 |
| 6 | `SPOOL_SNAPSHOT_ENTER` | 713 | true | true | 1 / 1 | 0 |
| 9 | `RUN_UNTIL_IDLE_ENTER` | 725 | true | true | 1 / 1 | 0 |

父进程的 `STOP_SIGNAL_SET` 时间为 `2026-10-18T20:34:46.680Z`；四个 Worker 的最终 heartbeat 在约 120 秒后仍停留在上述阶段。`join-timeout-forensics.json` 的 reason 为 `worker_join_deadline_exceeded`。Worker 9 的 `/proc` 采样为 `R (running)`、`wchan=0`、主线程 CPU ticks `11102`，另有两个 `futex_wait_queue_me` 线程，fd 数为 12；Bind lease 列表为空。

独立重新加载同一 run 的 `TaskSpool.status()` 得到 `pending_records=0`、无 recovery issue。因而现场显示的是 shutdown loop 对全局 Spool 状态的瞬时/陈旧观察，而不是仍有一条合法待发送任务或一个未回收 Bind。

### 诊断限制

该失败样本的 `/proc`、phase、CPU/wchan、父子时序和 lease evidence 已捕获，但自动 Python/faulthandler 栈文件为零字节，asyncio task dump 也没有产出；不能把某个具体 `asyncio` await、executor 或 socket close 栈伪报为已捕获。零字节 stack artifact 的 SHA-256 为标准 empty-file hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。这是一项观测证据缺口，不影响下面由状态机、源码调用链和修复后回归共同确认的停机信号归因。

## 确切调用链

```text
parent completion boundary
  -> stop_event.set()
  -> worker process loop
     -> external multiprocessing.Event is set
     -> (before fix) ShardWorker._stop is still unset
     -> await worker.run_until_idle(close_transports=False)
        -> run_until_idle() can return only at its own _stop/buffer/pending boundary
     -> await _drain_transport_responses()
     -> close_transports()
     -> spool.lightweight_snapshot()
     -> transient pending_records=1
     -> sleep(0.02) and repeat
  -> asyncio.run(loop()) never returns
  -> result file write is never reached
  -> parent join deadline expires -> SIGTERM
```

相关代码位置：

- `backend/tools/vnext_streaming_acceptance.py::_worker_process()`：外部 `stop_event`、`run_until_idle()`、transport drain、Spool snapshot 和结果写入的完整边界。
- `backend/vnext/worker.py:94`：`ShardWorker.stop()` 只设置其内部 asyncio event。
- `backend/vnext/worker.py:445`：`run_until_idle()` 的循环条件读取内部 `_stop`，并等待已创建 pending task。
- `backend/vnext/spool.py:615`：热循环使用的 `lightweight_snapshot()`；`backend/vnext/spool.py:887` 的 `status()` 是 shutdown reconciliation 使用的完整扫描。

## 最小修复

1. Worker loop 在每次 `run_until_idle()` 前检测外部 `stop_event`，只在第一次观察到时调用 `worker.stop()`。父进程仍只在 Producer 完成、所有 PDU 响应、Journal EOF 和 Projection caught-up 后设置该信号。
2. 停机边界使用一次 `spool.status()` 做完整 cursor/taken reconciliation；非停机热路径继续使用原有 `lightweight_snapshot()`。这不修改 cursor、taken、fsync 或 dispatch，只改变退出判断的读路径。
3. 增加低开销 per-worker 原子 heartbeat、parent lifecycle JSONL、faulthandler/asyncio 诊断入口，以及超时前 `/proc`/wchan/fd/thread/Bind lease forensics。诊断文件无 fsync，不在每条短信热路径同步写入。
4. 将父进程 join 改为一个全局 deadline 的非阻塞轮询，避免串行 `N × timeout`；仍保留真实 exitcode、缺结果文件和超时失败，不通过放宽门禁制造 PASS。
5. 捕获 Worker 异常和结果文件写入异常；只有 `exitcode==0`、结果文件齐全并通过数据面对账才算 PASS。

### 语义审查

- 已经写 socket 的任务仍由原有 response drain、Journal durable 和 Projection EOF 路径收尾。
- `worker.stop()` 只阻止 shutdown boundary 之后继续取得新任务，不清空 pending、不回退 cursor、不伪造结果文件。
- 仍为 4 physical binds、200 TPS per bind、window 50；unknown response 不重发。
- Journal 每事件 durability、TaskSpool checkpoint、单 ProjectionWriter/单 DuckDB owner、T6/T7 语义均未改动。
- 本任务 `git diff -- backend/vnext` 为空：没有生产代码修改。

## 修复后证明

`lifecycle-12-r2` 是同配置的修复后受控复测：12/12 Worker 都进入 `PROCESS_EXIT`，结果文件齐全，exitcode=0，final global pending=0；`status=PASS`，耗时 `17.013s`，Journal/Projection 为 `2000/2000`，无 join forensics、无残留 lease、无非空 stack/task dump。随后 8/12/16/24 的 1K 双轮和反序 5K 双轮全部通过，详细矩阵见 [Worker_Clean_Exit_and_Scaling_Decision.md](Marketing_Copilot_VNext_Worker_Clean_Exit_and_Scaling_Decision.md)。

## 本地回归

```text
targeted lifecycle/worker/transport tests: 35 passed in 3.40s
backend/tests: 447 passed, 5 skipped, 4 warnings
```

新增覆盖包括 heartbeat shutdown contract、stall dump thread evidence，以及 `ShardWorker.stop()` 在 shutdown boundary 阻止新 dispatch；原有后端测试保持通过。

