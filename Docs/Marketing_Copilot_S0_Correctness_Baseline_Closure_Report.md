# Marketing Copilot S0 Correctness Baseline / P0 Closure Report

执行日期：2026-10-15
任务：`MC-SCHED-S0-CORRECTNESS-20261015`
设计基线：`e50ac1528ff2e6efb26b472806ed5e8cf3afba11`

## 结论

最终判定：`S0_NO_GO`。`P0_CORRECTNESS_READY = NO`。

R02、R03、R17 的确定性回归和成功闭环已通过。R01 的“结果事务错误不能被假确认为 committed”缺陷已完成最小修复；但是当前 OCC + Fast 1K 端到端验收出现一次 API `SIGSEGV (0xFFFFFFF5)`，进程退出时只有 600 个任务、500 个 socket 提交，数据库中存在 301 accepted、199 sending、100 queued，并留下一个非法 chunk 状态 `completedcompletedcompletedcompletedcompletedcompleted`。同一参数随后重跑通过，但这不能消除前一次当前目标环境中的业务崩溃和不完整状态。因此不能宣称 S0 正确性已稳定闭合。

按任务规则，S1–S4 不得提前进入；G3 仍为 `NO_GO / NOT_VERIFIED`，生产 Fast 仍为 `NO_GO`。本次没有真实 SMSC、SLES12SP5 或生产数据库验收。

## 冻结基线与环境

- Windows 工作树初始 HEAD 为上面的设计提交；未回退、未 reset、未 commit、未 push。
- 最终测试源快照由 git-tracked 后端/前端、现有本地验收测试和本次 S0 测试构成；manifest SHA256：`1846B1FC9B89A415C0BC338A5715A4DA747ECEA467EE5DF3EFC76E908BBBF405`。验收 harness 在无 `.git` 的隔离副本中报告 `git_commit=unknown`，因此以 HEAD + manifest 双重标识源代码。
- WSL 隔离目录：`/tmp/mc-s0-20261015-closure`，AlmaLinux-8、Linux 6.18.33.2 WSL2、glibc 2.28、CPython 3.11.13、DuckDB 1.2.0、`MC_DUCKDB_THREADS=1`。
- 端到端统一使用 OCC、loopback-only SMSC、`--smsc-tps 200`、SMPP window 50、send queue 2000、task workers 8、ingest workers 2、copy workers 2、`--timeout-seconds 180`、DLR disabled；所有新运行都有独立 DB、Journal、Watch、端口和 artifacts 目录，并显式绕过 loopback 代理。
- Windows 前端 `npm run build` 通过；初始 `frontend/package-lock.json` 是用户已有修改，未覆盖。工作树中的其他运行数据、日志、缓存和未提交资料均保留。

## 复现、修复与证据

### R01：结果事务假确认与 OCC+Fast 不收敛

最小注入测试在真实结果事务中分别触发 catalog SQL 错误和同进程 OCC 冲突：`WrappedConnection` 在语句错误后回滚，但 `_record_submit_activity` 吞掉异常，外层仍把 Journal 事件标为 committed；OCC 冲突也没有进入完整事务重试。证据在 `evidence/r01-minimal-repro.log`，对应测试在 `backend/tests/test_s0_correctness.py` 的结果元数据用例。

最小修复删除 `_record_submit_activity` 的逐调用 SQL 异常吞掉分支，使错误向上冒泡，由现有 `write_transaction` 对完整事务进行有界重试或 fail-closed；没有新增队列、schema 或存储引擎版本。修复后的确定性回归已通过，结果 Journal 与数据库事件 ID 的独立审计也加入了门禁。

但端到端证据仍不稳定：

| 运行 | 结果 | 关键证据 |
|---|---|---|
| `sender-pool1-v4`（与完整回归并行） | FAIL | 1000 tasks；951 accepted、32 queued、17 sending；951 socket success；180 秒业务状态超时 |
| `sender-pool1-isolated` | PASS | 1000/1000 accepted，1000/1000 socket success，独立审计 PASS |
| `s0-occ-fast-1k-isolated` | FAIL | API `SIGSEGV`；500 socket success；600 tasks（301 accepted、199 sending、100 queued）；审计发现不完整文件和非法 chunk 状态 |
| `s0-occ-fast-1k-isolated-rerun2` | PASS | 1000/1000 tasks accepted，1000/1000 socket success，10 files completed，独立审计 PASS |

失败运行的 API 退出快照为 `runs/marketing-copilot-stress-1k-full-0ad5ef80/diagnostics/process-exit-api-20261015T074114Z.json`；应用日志显示 realtime ingestion OCC 冲突达到第 5 次重试后进程退出。没有证据支持把这次崩溃直接归因于 DuckDB 数据损坏，也没有证据证明它已被修复，所以 R01 只能记为“最小应用缺陷已修、S0 稳定性未闭合”。

### R02：非法 chunk 状态与权威摄取

`is_completed_chunk_status` 现在只接受精确值 `completed`；`prepare/complete` 对 `completedcompleted`、`processingcompleted`、`completed-unproven` 和 `garbage` fail-closed，保留原始值；complete 额外校验 chunk/file 归属和 lease token，未证明的状态不会 replay 或累加统计。确定性非法枚举、旧 lease、跨 file complete 和真实 T01 复测均覆盖。相关位置：`backend/services/ingestion_job_service.py:602`、`backend/services/realtime_ingestion_state.py:468`。

### R03：Reliable/ Fast 停止栅栏

可靠模式复用现有 campaign barrier、generation、window 和 socket admission 合同；barrier 不跨网络响应或 throttle 等待，socket lock 只包围实际写入。pause/cancel/completed/window 在 authorize 后、throttle 中、socket lock 前和 multipart part 间共 48 个组合均验证：停止成功后没有新的 `submit_sm`，已经可能写出的路径保留 attempt/part 证据。批量和 realtime 直接更新接口也纳入相同 barrier。相关位置：`backend/services/fast_send_service.py:181`、`backend/services/smpp_gateway.py:811`、`:998`、`backend/services/sms_task_service.py:1721`。

### R17：DLR 关联与禁止不安全重发

receipt 现在必须匹配 source namespace、task、attempt、part/message binding 和 planned parts；旧 attempt、重复 receipt、mixed multipart、unknown/partial、负 DLR 和发送后异常均按证据保留状态。只有可证明尚未写出或明确全量拒绝且允许重试的路径才可 retry；负 DLR 不会把整条消息伪装成未发送。确定性状态矩阵、旧 attempt 和 multipart 计数用例均通过，相关回归在 `backend/tests/test_sms_result_recovery.py`。

## 回归与端到端验收

### G0 / T02

- 修复后 WSL 全套：`288 passed, 2 skipped, 6 warnings`，耗时 `638.26s`，exit 0。日志：`evidence/g0-final-v4.log`。
- OCC 定向门禁：`161 passed, 4 warnings`，exit 0。日志：`evidence/t02-final-v4-occ.log`。
- 修复前冻结基线：`196 passed, 2 skipped, 6 warnings`；基线日志：`evidence/g0-baseline.log`。
- Windows 后端由于临时目录 ACL 使用问题未作为 PASS；按任务规则使用全新 ext4 隔离副本完成核心验证，未删除旧临时目录。

### T01、Sender-only 与 Fast

独立停止 owner 后，`audit.py` 只读打开 DB，重算 source hash/dedupe key，核对 task/file/chunk/attempt/part/socket/Journal；成功运行均为 `missing_keys=0`、`extra_keys=0`、无重复 attempt、Journal pending/quarantine 为 0、已 committed Journal event ID 缺失 DB event 为 0。

| 门禁 | 结果 | 守恒摘要 |
|---|---|---|
| T01 `t01-ingest-v4` | PASS | 10 files × 100 rows；1000 unique tasks；10 chunks completed；0 socket/attempt |
| Reliable pool=4 `sender-pool4-v4` | PASS | 1000 accepted；1000 socket success；1000 attempts；1000 planned part rows |
| Reliable pool=1 `sender-pool1-isolated` | PASS | 1000 accepted；1000 socket success；1000 attempts；1000 planned part rows |
| Multipart `sender-multipart-v4` | PASS | 1 task；6 actual PDUs；partial 状态保留；6 durable part rows |
| OCC+Fast `s0-occ-fast-1k-isolated-rerun2` | PASS（重跑） | 10 files completed；1000 accepted；1000 socket success；Fast items 1000 planned/accepted |
| OCC+Fast `s0-occ-fast-1k-isolated` | FAIL | SIGSEGV 后 600 tasks、500 socket、301 accepted；非法 chunk 状态，不能计入 PASS |

所有 sender/E2E 均为 loopback/mock SMSC。DLR disabled 只证明提交响应和本地业务状态，不证明运营商交付。

## 修改范围与回滚边界

本次只涉及摄取状态精确判断、发送停止/授权/结果事务、DLR 关联、campaign direct-control barrier、启动回归 fixture 以及 S0 测试；保留单服务进程、单 DuckDB RW owner、现有 Journal/attempt/part/WAL 和生产配置。未实施 ready pool/credits、slot fairness、PDU window 重构、worker 横向扩容、600 秒长稳态或部署包改造。回滚边界是恢复上述修改文件和本次新增测试，不需要数据库 schema 迁移；运行时 artifacts 不属于部署包。

## 最终门禁判定

- R02：确定性回归通过；真实失败运行暴露的非法状态被审计并保留，不能被宽松匹配掩盖。
- R03：确定性停止栅栏、Reliable pool=1/4、multipart 证据通过。
- R17：确定性 DLR/attempt/part 关联和重发保护通过。
- R01：最小 false-commit 修复和隔离 Reliable 通过；OCC+Fast 仍有一次真实 API 崩溃与不完整状态，虽重跑成功，稳定正确性未证明。

因此本交付不满足 `P0_CORRECTNESS_READY`，最终状态为 `S0_NO_GO`。不得以本次重跑成功替代失败证据，也不得据此进入 S1–S4 或改变 G3/生产 `NO_GO`。后续应先对该 SIGSEGV、OCC ingestion 冲突耗尽与非法 chunk 状态建立可重复根因和最小修复，再重新运行同一 S0 门禁。
