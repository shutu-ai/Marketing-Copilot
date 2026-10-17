# Marketing Copilot VNext 生产响应、Journal 与 T7 Closure 报告

任务：`MC-VNEXT-SINGLE-WRITER-PERF-RESP-JOURNAL-T7-CLOSURE-20261017`

结论：本轮修复的局部闭环为 `PASS`；current-SHA native ext4 的 50K 与 600 秒 loopback correctness 已完成，但整体发布结论仍为 `NO_GO`，因为实际 steady 约 164.2 PDU/s、drain 274.014s，且尚未连接 EulerOS/真实测试服务器。

## 1. 生产响应生命周期

旧链路在 `runtime.py` 中以 `run_until_idle(close_transports=True)` 结束；`AsyncSmppTransport.close()` 会清空 pending，存在“socket 已写出但 response reader 尚未入 Journal”的窗口。修复后生产 runtime 与 harness 共用：

```text
run_until_idle(close_transports=False)
    -> ShardWorker.drain_transports(timeout)
    -> close_transports()
    -> release_reader_caches()
```

另外修正 Worker 的参数适配：`AsyncSmppTransport.send_task()` 虽然使用显式 `progress_context` 参数，旧逻辑仍把它当作不支持的可选参数删掉；这会使 response callback 没有 `event_id`。现在只删除目标 callable 未声明的参数。

正常响应由原有 durable sink 写入 Journal，事件 ID 为 `<task-event-id>:resp:<sequence>`；关闭/断连的未确定响应写 `unknown`，释放 window，但绝不自动重发。

## 2. Response evidence

Run：`/tmp/mc-vnext-t7-codex-20261017-r1/stream-delay-final/result.json`。

| 检查 | 结果 |
|---|---:|
| synthetic tasks | 40 |
| socket-written PDU | 40 |
| loopback `submit_sm_resp` | 40 |
| durable response events | 40 |
| total Journal/DB events | 80 |
| unknown response events | 0 |
| committed offset / Journal bytes | 41160 / 41160 |
| backlog bytes | 0 |
| drain | 2.240s |

在修复前的 delayed-response 试跑中，SMSC 侧 response counter 为 80，但 Journal 只有 80 个 task event；该现象定位为 context 参数被删，而不是把它记录成 PASS。修复并加入 `response_event_count == response PDU` 门禁后，最终 Run PASS。

## 3. Journal generation / hard-limit recovery

`ProgressJournal` 保留逻辑 base path，达到 segment 水位后使用稳定 generation 文件。Writer offsets 以完整路径为 key；因此轮转不会把旧整数 offset 误用于新文件。顺序为：

```text
Journal event durable
  -> DuckDB event-id dedupe + aggregate 同事务 COMMIT
  -> journal-offsets.json flush/fsync + replace
  -> 仅 GC 已完整确认的 inactive segment
```

`storage_snapshot()` 分开报告 total bytes、committed bytes、backlog bytes、backlog event estimate、reclaimable bytes、capacity remaining 和 blocked。`blocked` 对最大事件大小采用保守判定；实际 admission 仍按待写 event 的精确编码长度检查。逐事件 Journal fsync 没有关闭。

## 4. T7-4～T7-9 实测矩阵

工具：`backend/tools/vnext_t7_recovery_matrix.py`；它先在缺 pytest wheel 的离线阶段调用同一测试函数，并将异常保留为 FAIL。随后通过外网 VPN 补齐 `pytest==8.3.5`、`psutil==6.1.1` 及 requirements-dev 依赖到 native `wheelhouse/`，以 `--no-index` 安装并完成 native ext4 全量 `backend/tests`：`420 passed, 2 skipped, 4 warnings`（248.78s）。最终 JSON：`/tmp/mc-vnext-t7-codex-20261017-r1/t7-matrix-final/result.json`；filesystem type=`ext4`，Python=`3.11.13`，DuckDB=`1.2.0`。

| ID | 场景 | 结果 | 证据重点 |
|---|---|---|---|
| T7-4 | feeder 暂停后继续 | PASS（bounded pause） | 两批 source event 唯一、无死锁、pending=0；未冒充 30–60s soak |
| T7-5 | Route-A unavailable / Route-B healthy | PASS | A 局部 write_failed，B socket_written，健康 route 有进展 |
| T7-6 | projection hint queue full | PASS | hint queue 满时 Journal 仍收到完整 durable events |
| T7-7 | Journal hard-limit / GC / recovery | PASS | 满时 admission stop；只 GC acknowledged inactive segment；释放后恢复 |
| T7-8 | Spool capacity exhausted / day reclaim | PASS | 上游得到 capacity error；跨日 rollover 后 closed-day reclaim，新日重新接收 |
| T7-9 | 跨午夜 cursor + Writer crash/restart | PASS | old-day cursor 不 redispatch；Writer replay event-id 幂等，聚合不双计数 |

各 scenario duration 分别约为 `0.080s, 0.027s, 0.024s, 0.026s, 0.024s, 0.848s`。这是隔离故障证据，不等同于 600 秒长稳。

## 5. Current-SHA native T3/T4 补充证据

- T3 50K：`/tmp/mc-vnext-t7-codex-20261017-r1/t3-50k-continue/result.json`，20K+20K+5K+5K 全部闭合，50,000 tasks/PDU/response、100,000 Journal events、unknown=0、backlog=0，drain `210.393s`。
- T4 600s：`/tmp/mc-vnext-t7-codex-20261017-r1/t4-600s-continue/result.json`，steady `600s`，98,750 tasks/PDU/response、197,500 Journal events、unknown=0、backlog=0，duration `875.269s`，drain `274.014s`。
- T4 配置的 effective route quota 是 800 PDU/s，但 600 秒供给阶段实际约 `164.2 PDU/s`（约 `328.5 events/s`），有 184 次 feeder backpressure waits；因此这是 correctness/受控长跑证据，不是 800 PDU/s 性能 PASS。

## 6. 状态与边界

- 单 Writer、单 DuckDB、单 DB owner 保持不变。
- `projection_events_applied` 明确标为 `process_local`；最终对账同时使用 Journal events、committed offsets、DB event rows 和 aggregate，不使用 `lag_seconds=0` 单独宣称追平。
- `TaskSpool.release_reader_cache()` 只在 worker pass 无 inflight 后调用；文件和 manifest 仍是 authority，下次 reader 会重建缓存。
- T7-4～T7-9 本轮为 PASS；历史 T7-1～T7-3 未在本轮修改后重新执行，因此不把整体 T7 9/9 写成已签署。
- EulerOS/SLES12SP5、真实 SMSC、生产 API/数据库均未连接；native ext4 已完成 `420 passed, 2 skipped, 4 warnings` 的全量 backend pytest，以及当前 SHA 的 T3/T4 loopback 运行。Windows 沙箱 ACL 试跑曾阻断，但升权重标准 backend pytest 已为 `417 passed, 5 skipped, 4 warnings`。报告保留真实外部环境边界。

下一步如需连接测试服务器，应先通知用户切换服务器 VPN；连接前复用同一 source/config 做 EulerOS/真实 SMSC 与 old/new paired 验证。

最终建议：在上述外部环境和约 800 PDU/s 目标验证完成前维持 `NO_GO`，不提交 canary 部署评审。
## 7. Final EulerOS server addendum (supersedes the preliminary server-next-step note)

After the user switched to the external VPN, the final source package was transferred offline to new, unique Run directories on the authorized EulerOS test server. Existing Runs were not modified. No real SMSC, production API, or production database was contacted.

Formal server identity/freeze: EulerOS 2.0 SP15 x86_64; `/dev/sda2 ext4` from `df -T`; CPython 3.11.6; DuckDB 1.2.0; `pytest==8.3.5`; `psutil==6.1.1`; `pip check` passed. The process ran as non-root `paas` with CPU affinity `0-3`, `nice 10`, and `ionice -c3`; the host had pre-existing load average about 9–11.

Server Run outcomes:

- `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex05/t7-recovery/result.json`: T7-4…T7-9 all PASS (6/6, ext4).
- `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex04/t3-50k/result.json`: FAIL / NO_GO. All 50,000 socket writes and responses matched, unknown=0, and 100,000 Journal events were eventually applied during shutdown cleanup; however the bounded drain guard fired at an observed 742.003 s, so this is not a T3 gate PASS.
- `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex05/t4-600s/result.json`: FAIL / NO_GO. RSS reached 2,099,128 KiB against the 2,097,152 KiB guard at sample 331.293 s; 178,000 task/PDU/response counts had no unknowns, but steady 600 s was not completed and projection applied only 142,505/356,000 events at final collection.

The closure decision remains `NO_GO`. The server data confirms the bottleneck is projection/Journaling catch-up under the constrained resource envelope, not response loss: the T4 loopback response counts are complete while the projection backlog and RSS guard fail independently. The acceptance harness now checks committed Journal EOF rather than only spool quiescence and records `projection_drain_timeout`; final harness SHA-256 is `32AF9A59EC28F31FBF9612F35E44F81E8303E125DE4725D71D6C6B006DE2CB4C`.
