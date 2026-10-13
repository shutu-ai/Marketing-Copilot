# Marketing Copilot WSL DuckDB 并发与全链路验收报告

## FINAL_STATUS

- `CODE_ACCEPTANCE_PASS`
- `WSL_LOCK_ACCEPTANCE_PASS`
- `LOCAL_E2E_PASS`
- `STRESS_ACCEPTANCE_BLOCKED_RESOURCE`
- `LIVE_ENV_UNVERIFIED`

本轮没有执行 commit、push、tag 或 release。Windows 工作区中的既有用户改动保持原样；验收在独立 WSL ext4 副本 `/home/jabing/marketing-copilot-wsl-acceptance` 完成。

## 环境与可追溯性

- Git SHA：`6750dc825a01311708790666d99e5294a21f36a5`；本地 `HEAD` 与 `origin/main` 一致，工作树仍为 dirty。
- WSL：AlmaLinux 8.10，WSL2 kernel `6.18.33.2-microsoft-standard-WSL2`，glibc 2.28，x86_64。
- Python：3.11.13；DuckDB 1.2.0；FastAPI 0.115.0；Uvicorn 0.30.6；pytest 8.3.5；psutil 6.1.1。
- 资源：22 vCPU，15 GiB 内存（验收时约 14 GiB available），4 GiB swap；数据库所在 `/home/...` 为 `/dev/sdd` 上的 ext4，约 944 GiB free、2% used。
- 所有测试 DB、WAL、日志、watch/inbox、备份和更新目录均在上述 ext4 副本；SMSC 仅绑定 `127.0.0.1`，业务测试使用 `MC_SYSTEM_MODE=production` 以执行真实 SMPP client 路径。

主要命令：

```text
cd /home/jabing/marketing-copilot-wsl-acceptance
.venv-linux/bin/python -m pytest -q backend/tests
.venv-linux/bin/python backend/tools/local_lock_e2e_acceptance.py --profile smoke|stress|soak
.venv-linux/bin/python backend/tools/local_e2e_acceptance.py --scenario <scenario>
```

最终全量结果：`120 passed, 2 skipped in 104.65s`。锁、事务、在线备份聚焦结果：`3 passed`。

## ROOT_CAUSE

### DB-01：进程内 RLock 不能解决跨进程 DuckDB owner

复现前的源码路径是 `database.get_connection() -> duckdb.connect(DATABASE_FILE)`，进程外工具不受 `_write_lock` 保护。WSL 中让真实 Uvicorn owner 持有测试 DB 连接后，独立 contender 返回非零并报告：

```text
IO Error: Could not set lock on file ...
Conflicting lock is held in /usr/bin/python3.11 (PID ...)
```

第二个 Uvicorn 使用同一 DB 和同一 instance lock 返回码 3，并报告 `another Marketing Copilot instance is running (pid=...)`。没有使用 `MC_ALLOW_MULTI_INSTANCE=1`。

### DB-02：旧备份 CLI 会与运行中的服务争夺 DuckDB 文件锁

修复前在 WSL stability 服务活动期间执行与 systemd service 等同的独立 `tools/backup_database.py`，得到实际错误：

```text
Cannot open database for emptiness check ...
IO Error: Could not set lock on file ...
Conflicting lock is held in /usr/bin/python3.11 (PID 79747)
duckdb.duckdb.IOException: IO Error: Could not set lock on file ...
```

备份目录为空。这是旧测试没有发现的真实缺陷：此前只验证停止服务后的 CLI，未在主服务持有 DB 时执行独立备份进程。

### DB-03：写事务需要可诊断的短边界与有界等待

写 gate 的 owner 原来只在通用 `execute/commit` 路径可见，业务操作名无法贯穿整个事务；长持锁时也缺少统一的 wait/hold/transaction 证据。本轮保留事务原子性，未通过无限延长重试掩盖竞争。

### DB-04/05：辅助连接需要区分主 DB 与独立索引/临时库

直接 `duckdb.connect()` 盘点结果：

- `backend/database.py`：唯一主业务连接工厂，写入经过 `WrappedConnection` gate。
- `services/db_config.py`：动态配置的只读 emptiness/test probe；运行中 owner 备份不再重复执行该 probe。
- `realtime_campaign_service.py`、`realtime_whitelist_service.py`：白名单索引/临时 DuckDB，路径独立于主业务库。
- `local_e2e_acceptance.py`、聚焦测试和恢复工具：仅在服务停止后或独立副本上读取。
- 旧 `backup_database.py` 是影响主 DB 的辅助工具，已改为 single-owner 协议。

## FIX_EVIDENCE

- `backend/database.py`
  - `write_transaction()` 将业务 `operation` 绑定到 connection，并记录事务耗时、结果和完整事务重试。
  - writer gate 记录 `thread_id/thread_name/operation/acquired_at`，记录等待、持有、超时和 holder 诊断；标签只使用有限业务操作名，不记录 SQL 参数、短信正文或手机号。
  - 异常路径仍 rollback/close，并由测试验证 gate 释放后可继续写入。
- `backend/services/runtime_service.py`
  - owner 进程内执行 `CHECKPOINT`，并在 `CHECKPOINT -> copy` 整个关键区间持有应用 writer gate。
  - 在线 owner 路径跳过会二次打开主库的动态配置 emptiness probe；停止服务的 CLI 仍保留配置解析。
- `backend/routers/internal.py`、`backend/auth.py`、`backend/tools/backup_database.py`
  - 新增 loopback-only、token-authenticated `POST /api/internal/backup`。
  - instance lock 检测到活动 owner 时，独立 CLI 只发 HTTP 请求，不直接打开主 DB；无活动 owner 时才执行停止服务备份。
  - 另有 `MC_ACCEPTANCE_HOOKS=1` 才启用的本地持锁测试端点，生产默认不可用。
- `backend/tools/local_e2e_acceptance.py`
  - 修复 venv launcher 被 `.resolve()` 解引用导致子进程丢失依赖的问题。
  - 增加 DLR 延迟、SMSC 断连和混合终态验收；只在服务停止后做最终 DuckDB reconciliation。
- 新增自动化回归：
  - `backend/tests/test_duckdb_lock_concurrency.py`
  - `backend/tests/test_online_backup_recovery.py`
  - `backend/tools/local_lock_e2e_acceptance.py`

## CONCURRENCY_RESULTS

### 进程与 HTTP 竞争

`Docs/wsl-lock-smoke-final.json`：

- 4 个外部 HTTP client 进程，80/80 写成功，105.814 req/s，P50 28.927ms、P95 57.521ms、P99 70.034ms。
- 第二 Uvicorn 明确拒绝；独立 DuckDB contender 在真实 owner 持锁期间明确拒绝。
- 在线 owner 备份后 `/api/ready` 仍为 200，副本停止服务后可读，marker rows=80。

`Docs/wsl-lock-stress.json`：

- 8 个外部 HTTP client 进程，1000/1000 写成功，105.184 req/s，P50 72.641ms、P95 99.993ms、P99 114.499ms。
- 在线备份 marker rows=1000，服务健康，独立恢复可读。

`Docs/wsl-lock-soak-30s.json`：

- 4 个 client 持续 30.162s，3291/3291 成功，109.112 req/s，P50 35.287ms、P95 48.449ms、P99 54.818ms。
- 在线备份 marker rows=3291，服务健康，独立恢复可读。

### 0.5/3/6/12 秒持锁注入

四档均得到约 0.250s 的有界 contender timeout；错误包含 owner operation、holder age 和 thread id。每档释放后 `write_lock_status().held=false`，随后 progress write 成功；最终 probe rows=4，无死锁、无半提交。

指标系列已由聚焦测试验证存在：

```text
marketing_copilot_database_write_lock_wait_seconds_count
marketing_copilot_database_write_lock_hold_seconds_count
marketing_copilot_database_write_transaction_seconds_count
marketing_copilot_database_write_lock_timeouts_total
```

## BACKUP_RESULTS

修复前 DB-02 的独立 CLI 失败已被复现。修复后，smoke/stress/soak 三档均由 systemd 等价 CLI 通过 instance lock 检测转发到 owner HTTP 入口：

- smoke：在线备份成功，服务保持 ready，独立打开副本并读取 80 条 marker。
- stress：在线备份成功，服务保持 ready，独立读取 1000 条 marker。
- soak：在线备份成功，服务保持 ready，独立读取 3291 条 marker。

聚焦备份测试还验证了 owner checkpoint 后副本可独立读取业务行；DuckDB 1.2.0 不提供 `PRAGMA integrity_check` 时使用实际业务行读取作为恢复校验。未在运行中采用未经协调的活文件复制。

## E2E_RESULTS

以下均为真实 FastAPI/Uvicorn 单 worker、Scheduler、ingestion workers、task workers、DuckDB、SMS worker、SMPP client 和本地 loopback SMSC，不使用 fake sender；JSON 均位于 `Docs/`：

| 场景 | 结果与对账 |
|---|---|
| quick | 5 输入行经白名单/去重后 3 tasks；3/3 submit_sm accepted；队列归零。 |
| reject | 5 tasks failed；5 submit_sm、0 success、5 rejected；无重复。 |
| drop-response | 3 tasks unknown；3 submit_sm 被 SMSC 接收但 response 丢失；无自动重发、无 retry violation。 |
| partial-submit | 1 multipart task partial；6 submit_sm 中 1 success + 5 rejected；无重复。 |
| restart | 5/5 accepted；API 重启前后 submit_sm 数量不增加。 |
| dlr-delay | required receipt 延迟 2s；5/5 delivered、5 submit_sm、5 DLR。 |
| disconnect | SMSC 第 2 次提交后断连；2 accepted + 1 unknown、仅 2 submit_sm、无重复或未知重发。 |

代表性证据：`wsl-local-e2e-quick-final.json`、`wsl-reject-final.json`、`wsl-drop-response-final.json`、`wsl-partial-submit-final.json`、`wsl-restart-final.json`、`wsl-dlr-delay-final.json`、`wsl-disconnect-final.json`。

这些 JSON 只输出聚合数量、状态、message-id 计数和本地测试路径，不输出真实手机号或短信正文。所有环境字段均为 `system_mode=production`、SMPP host `127.0.0.1`，`LIVE_ENV_UNVERIFIED` 保持不变。

## STRESS_RESULTS

### 业务 1K：完整通过

`Docs/wsl-stress-1k-final.json`：10 文件、1000 输入事件，1000/1000 持久任务 accepted，1000/1000 submit_sm success，无 duplicate send、无 lost mapping、队列峰值 981、最终 0；端到端吞吐 5.19 events/s，最大采样 RSS 约 290.6 MB。task creation -> submit response P50 70.7s、P95 137.3s，已作为真实性能基线记录。

### 10K：明确阻塞，不伪造 PASS

`Docs/wsl-stress-10k-blocked.json` 记录了 10K 的实际中止证据：50 文件全部创建 10000 tasks；受控停止时 261 accepted、4202 queued、3476 retrying、2061 sending，263 submit_sm，重复 attempt task 数为 0。进程停止后独立打开 DB 并完成业务行读取；由于 DuckDB 1.2.0 不支持 `PRAGMA integrity_check`，该项记录为 unsupported，而非伪造结果。

8 worker 配置下观察到约 3.2 submit_sm/s，按实测速率完成 10K 将超过 45 分钟；为避免无限占用本机，按资源安全边界中止并标记 `BLOCKED_RESOURCE`。这是性能/容量风险，不是数据正确性 PASS。

100K 未执行：在 10K 已被资源边界阻塞的前提下，按任务要求标记为 `BLOCKED_RESOURCE`，不宣称通过。

## 遗留风险与边界

- 当前业务压力的完整 1K 档数据正确性通过，但 10K/100K 的容量结论未通过；需要后续专门优化/容量窗口确认，不能把 1K 结果外推为生产 SLA。
- 本轮完成了真实 loopback SMPP、HTTP 多进程 producer、SMSC reject/unknown/partial/disconnect/DLR delay、owner backup 和独立恢复；没有连接真实 SMSC、SFTP 或现网 DB。
- SLES 12 SP5、生产 SMSC、真实手机号、真实 SFTP 和现网参数的上线验收仍为 `LIVE_ENV_UNVERIFIED`。
