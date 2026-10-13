# Marketing Copilot 实时任务调度可靠性闭环报告

日期：2026-10-13  
分支：`main`  
基线/当前 `HEAD`：`bb7ea690ca73d9f3da02c95c965fa1d449b197ec`  
结论：`CODE_ACCEPTANCE_PASS`；`LIVE_ENV_UNVERIFIED`

## 1. 审查边界与基线

本次按真实调用链审查并修改：

`watch_dir/SFTP/upload → staging → inbox → ingestion_files/ingestion_chunks → realtime ingestion worker → sms_tasks → task worker → SMPP submit_sm → submit_sm_resp/DLR → task state/journal`。

没有提交 commit、push、PR、tag 或 release；没有连接生产 SMSC，也没有向真实手机号发短信。工作区原有的 `frontend/package-lock.json`、`.rollback-bb7-parent/`、`cache/` 以及用户提供的任务文件均保留，未执行回滚或覆盖。

## 2. F-01～F-06 发现与修复

| 编号 | 优先级 | 根因 | 修复与证据 |
|---|---|---|---|
| F-01 | P0-A | `send_async()` 先进入内存队列，实际 socket 写入前没有重新核验 claim、活动窗口和 DB 任务状态；终止窗口可能清掉 token。 | `sms_task_service.record_send_intent()` 在第一条 `submit_sm` 前通过 `write_transaction` 同时核验 `status='sending' + claim_token + task_date + campaign_is_active()`，写入 `sms_send_attempts=authorized` 和 planned parts。`finalize_daily_tasks()`、`cancel_pending_campaign_tasks()` 仅取消没有 active attempt 的未开始任务；已经 authorized/responding/partial 的任务保留 claim/attempt。`SmppSubmissionNotAllowedError` 在 sync/async worker 中只 defer，不制造失败或 unknown。 |
| F-02 | P0-B | 监听目录存在“看起来稳定但仍在写入”、copy 中断、DB 登记失败留下正式孤儿文件等窗口。 | 本地 watch 使用 inbox 同目录隐藏 `.part`、fsync、复制前后 `(size,mtime_ns,inode)`、SHA-256 和最终 stat 校验，再以 `os.replace` 发布；只在正式文件发布后登记 `ingestion_files`。登记失败会删除本次发布的目标、释放 reservation、保留源文件。SFTP、upload `queue_file()` 使用同样的 staging/replace/登记顺序。支持 `.part`、`.ready`；兼容旧生产者时保留稳定时间启发式，但文档明确它不是绝对完整性证明。 |
| F-03 | P0-C | 普通 retry 只按 task 存在判断，可能清除 SMSC ID/时间并把已提交、unknown 或 partial 任务重新入队。 | `retry_task()` 只接受 `status='failed'` 且 failure class 属于 definite pre-submit (`transport/window_exhausted/submit_sm_resp/scheduler`)，并要求无 SMSC ID、无 submitted/delivered 时间；最终使用条件 UPDATE/CAS。拒绝抛出 `TaskRetryRejected`，API 返回 409 和原因。`requeue_transport_failures()` 同样检查证据和 active attempts，并修正 batch counter 的同一条件。 |
| F-04 | P1-A | 文件首次发现后若目录 mtime 不变，只能等待下一次完整扫描。 | `_watch_pending_stability` 保存单文件快照、首次发现和下次检查时间；pending 到期时只 stat/检查 pending 文件，目录变化、日期/分钟变化或周期兜底时仍走 full scan。增加 full/pending scan、stability pending/accepted、first-seen/last-scan 时间戳及 histogram 指标。 |
| F-05 | P1-B | 大文件复制在 Activity Scheduler 线程内同步执行，会拖慢其他活动扫描和维护。 | scheduler 只做轻量发现、reservation 和有界入队；`start_watch_copy_workers()` 启动受控 worker，默认 2 个、队列默认 32，可通过 `MC_REALTIME_COPY_WORKERS`/`MC_REALTIME_COPY_QUEUE_SIZE` 调整。队列满时释放 reservation 并记录 backpressure；停止时回收未消费 reservation。 |
| F-06 | P1-C | 输入缺少 `msisdn` 时退回第一列，可能把语言、时间或业务字段当成手机号。 | `_resolve_msisdn_column()` 只接受显式配置 `MC_REALTIME_MSISDN_COLUMN`/`realtime_msisdn_column` 或审计过的别名：`msisdn`、`msisdn_number`、`mobile`、`mobile_number`、`phone`、`phone_number`、`recipient`。缺失、重复、疑似 pandas `.1` 重复列和空表头进入 quarantine，保留错误原因和原文件；CSV/TXT/Excel 共用解析器。 |

## 3. 状态与文件流转

### 3.1 任务状态

```text
queued/retrying
    │ claim_next_tasks() + claim_token
    ▼
sending
    │ record_send_intent()：同一 DB transaction 核验 claim/窗口并写 attempt
    ├─ gate denied：defer → queued（没有 submit_sm）
    └─ first submit_sm socket write
          ├─ submit_sm_resp pending → responding/unknown（结果未知，禁止普通 retry）
          ├─ 部分 multipart 接受 → partial（禁止整条重发）
          └─ 全部接受 → accepted/submitted → DLR delivered（若有）
```

取消/跨日/手动停止只处理 queued、retrying，以及没有 `intent/authorized/responding/partial` active attempt 的 sending。晚到回调继续使用原 claim/attempt/source namespace fencing，不能覆盖新 attempt，也不能把已实际提交的任务伪装成 cancelled。

### 3.2 文件状态

```text
watch_dir/file producer
  ├─ filename.part / hidden temporary：忽略
  ├─ final + .ready：authoritative ready
  └─ legacy final：稳定快照兼容路径（启发式）
          │
          ▼
inbox/.<filename>.<random>.part
  └─ copy + fsync + stat/hash/stat
          │ os.replace，同一 inbox 文件系统
          ▼
inbox/<filename> → ingestion_files pending → processing/checkpoint
          ├─ completed → received/archive
          ├─ duplicate → duplicates archive
          ├─ invalid schema/read error → quarantine
          └─ retryable worker/lease issue → pending/retrying
```

watch 源文件不被删除；upload 临时源文件只在正式登记成功后删除。登记失败会回滚本次正式目标并保留源，避免“目标名已占用导致永久跳过”。reservation 与 manifest 的业务文件名去重保持一致，进程重启后以磁盘正式文件和 DB manifest 恢复。

## 4. 并发、原子性与恢复协议

- DB 协议：claim、授权 attempt、finalizer/cancel、retry CAS、late callback 都通过现有 `write_transaction`/claim token fencing；没有跨文件复制、CSV 解析、socket 等长操作持有 DuckDB writer transaction。
- SMPP 协议：attempt 先进入 `authorized`，每个 part 的 `sms_send_parts` 保留 planned/responded/accepted/rejected 信息；sync multipart 在“已有 accepted part、后续拒绝”时抛出 typed `SmppPartialSubmitError`，不能降级为普通失败重试。
- 文件协议：staging 文件位于 inbox 目标目录，`os.replace` 是同文件系统原子发布；复制或源变化只留下可清理的 staging，不留下可被 worker 读取的正式半成品。
- 容量协议：local、SFTP、upload 共享 `realtime_inbox_reservations`；入队、消费、失败、停止均有 reservation consume/release 路径。
- 结果协议：submit response、part result、DLR 继续走现有 `sms_write_buffer`、result journal 和 source-scoped message binding；本次同时修正 `sms_send_parts` upsert 中 DuckDB 对 `CURRENT_TIMESTAMP` 的解析兼容问题，避免结果缓冲永久失败。

## 5. 可观测性与性能影响

新增或补强的指标/时间记录包括：

- `realtime_watch_full_scans_total`、`realtime_watch_pending_rescans_total`；`realtime_watch_first_seen_timestamp`、`realtime_watch_last_pending_rescan_timestamp`、`realtime_watch_last_full_scan_timestamp`。
- `realtime_watch_stability_pending_total`、`realtime_watch_stability_accepted_total`、`realtime_watch_stability_ready_total`、`realtime_watch_copy_queue_depth`、`realtime_watch_copy_deferred_total`、`realtime_watch_copy_failures_total`、`realtime_watch_last_registered_timestamp`。
- `realtime_watch_atomic_copy_seconds_*` 与 `realtime_watch_handoff_seconds_*` 为累计 histogram bucket/count/sum/latest，可通过 `/metrics` 或 `/api/metrics/status` 计算窗口内 P50/P95/P99；submit socket/submit response 继续使用现有 event、rate、latency 指标。
- `sms_tasks.created_at`、`ingestion_files`/`processed_files` 的 detected/progress/completed 时间字段以及 SMPP submit/response 时间指标构成发现→登记→任务创建→submit→SMSC response 的追踪链。

代码级性能策略是：未变化目录仍按周期 full scan；pending 文件用小范围 stat；大 copy 移出 scheduler；queue 和 worker 数有界。此次没有把本地测试耗时当作生产 SLA，也没有伪造 P50/P95/P99 数值。`114 passed` 的全量测试耗时约 106 秒，包含隔离 DB、重试和 filesystem fixture，不代表生产吞吐。

## 6. 验证证据

测试环境：`.p0-test-venv`，DuckDB 隔离临时文件；Windows pytest 默认临时目录受到 ACL 限制，因此使用显式 elevated `--basetemp`，这不是代码失败。

最终执行：

```text
.p0-test-venv\Scripts\python.exe -m pytest backend/tests -p no:cacheprovider --basetemp <elevated-temp> -q
114 passed, 2 skipped in 106.06s
```

重点回归覆盖：

- P0-A：窗口关闭前排队但未 socket 写入、授权 attempt 的 late submit、队列停止不发送、跨日 finalizer 保留 in-flight、sync multipart partial。
- P0-B：源文件写入稳定性、copy 中源内容变化、`.part` 清理、DB 登记失败回滚、pending 轻量复查、重复/容量/manifest 相关现有测试。
- P0-C：accepted/submitted/delivered/sending/unknown/partial 全部拒绝普通 retry；明确失败只 CAS 入队一次；状态证据不被清除。
- P1-C：无 MSISDN 列拒绝，显式别名可解析；现有 realtime ingestion 测试继续通过。
- 现有可靠性、SMPP async、result recovery、realtime ingestion/progress、phase1/phase2/phase3 测试均纳入全量命令。

两个 skipped 是有意的性能大 fixture：`RUN_LARGE_CAPACITY=1` 的百万行测试和 `RUN_TEN_MILLION_CAPACITY=1` 的千万行测试；没有因本次失败而跳过功能测试。

## 7. 兼容性、风险与验收门禁

| 门禁 | 结论 | 证据 |
|---|---|---|
| G0 | PASS | 基于当前 `HEAD` 的 CodeGraph/真实调用链审查；未依赖 README 猜测。 |
| G1 | PASS | SMPP 19 个 focused 测试通过；全量无回归。 |
| G2 | PASS | 原子 staging、hash/stat、replace、reservation、登记失败回滚与恢复路径有 focused/既有测试。 |
| G3 | PASS | retry service/API 409/CAS/metadata fence 与 batch requeue 条件已覆盖。 |
| G4 | PASS | pending/full scan、bounded copy worker、严格 schema 和指标代码已通过 targeted/full tests。 |
| G5 | PASS | `backend/tests`：114 passed，2 个仅性能规模测试 skipped。 |
| G6 | PASS | attempt/file/task/result 状态、事件、时间戳、metrics、报告已形成可追溯闭环。 |

### 已关闭

- 过期窗口中新 submit 被 DB gate 拦截。
- 已开始/部分接受的 attempt 被错误取消或普通 retry。
- watcher 半成品进入正式 inbox、DB 登记失败孤儿、reservation 泄漏。
- unchanged directory 导致 pending 文件额外等待完整 rescan。
- 大文件 copy 阻塞 Activity Scheduler。
- 缺少 MSISDN 时静默取第一列。
- DuckDB attempt/part upsert 在结果缓冲中因 `CURRENT_TIMESTAMP` 解析失败而持续重试。

### 剩余风险与上线前动作

1. `LIVE_ENV_UNVERIFIED`：尚未在 SLES12SP5、真实 Paramiko/SFTP 服务和真实 SMSC 上验证；上线前必须使用隔离号码/测试 SMSC 完成 submit_sm、超时、断线、DLR、跨日和重启演练，禁止把 fake gateway 结果等同生产验证。
2. `.ready`/原子 rename 是推荐生产者协议；旧生产者仍使用 mtime/size heuristic，完整性由复制前后 stat/hash 防护，但无法证明“未来不会继续写”。建议生产者迁移到同文件系统临时文件 + `os.replace` 或 `.ready`。
3. histogram 已暴露 bucket/count/sum，但本次没有生产样本，因此没有声称实际 P50/P95/P99 或 SLA；上线后应按 campaign、source、SMSC config 观察这些指标并设置告警。
4. 默认 copy queue/workers 是保守值；应根据实际磁盘、文件大小、活动数和 SMSC TPS 做容量压测，确认 backpressure 不会让 inbox reservation 长时间占满。
5. 本次不包含前端视觉回归和真实端到端浏览器/SLES 验证；API 成功格式保持原样，retry 拒绝新增明确 409。

## 8. 变更文件

本次功能/验证新增或修改：

- `backend/services/realtime_campaign_service.py`
- `backend/services/sms_task_service.py`
- `backend/services/smpp_gateway.py`
- `backend/services/task_worker.py`
- `backend/services/sms_sender.py`
- `backend/services/activity_scheduler.py`
- `backend/services/metrics_service.py`
- `backend/routers/sms_tasks.py`
- `backend/tests/test_realtime_scheduling_reliability.py`
- `Docs/realtime-scheduling-reliability-closure.md`

工作区中其他已有修改未被本任务覆盖或清理。
