# Marketing Copilot — S0.1 OCC + Fast SIGSEGV 与摄取状态异常专项闭环报告

- 任务：`MC-SCHED-S0.1-OCC-FAST-CRASH-CLOSURE-20261015`
- 执行日期：2026-10-15
- 上游基线提交：`a856dc05b5b3272a7196a86a71b4ad9ff01ea922`
- 本地 `HEAD`：`a856dc05b5b3272a7196a86a71b4ad9ff01ea922`
- 本地 `origin/main`：`a856dc05b5b3272a7196a86a71b4ad9ff01ea922`
- 最终判定：`S0.1_READY`；`P0_CORRECTNESS_READY = YES`
- 范围声明：G3 仍为 `NO_GO / NOT_VERIFIED`，生产切换仍为 `NO_GO / NOT_VERIFIED`；本报告不代替 S1–S4、真实 SMSC、SLES12SP5 或生产验收。

## 1. 结论摘要

### 1.1 根因结论

已证实的最低根因边界是：DuckDB 1.2.0 CPython 集成在 OCC 高冲突窗口内，由多个业务线程反复执行“每事务 `duckdb.connect()` → 使用 → `close()`”的独立连接生命周期，会进入不安全 native 状态。直接可观察到的结果是：

1. 多个线程会阻塞在 native `duckdb.connect()` 中；受控运行的线程快照在 30 秒无进展时同时捕获到两个 ingestion/task 线程停在 `backend/database.py:1268` 的 `duckdb.connect(...)`。
2. 在写写冲突和回滚重试后，`VARCHAR` 状态列与时间戳列会在持久层中发生跨事务值拼接，例如 `completedcompletedcompletedcompleted`、`completedcompletedcompletedcompletedcompletedcompleted`，以及多个时间戳首尾相连。
3. 原始故障的 API `SIGSEGV` 与上述状态异常出现在同一 OCC 冲突窗口内；`returncode=-11` 是父进程观察到的内核 `SIGSEGV` 信号，`0xFFFFFFF5` 只是 `-11 & 0xFFFFFFFF` 的显示形式，不是应用定义的业务状态。
4. OCC 冲突重试耗尽与 `SIGSEGV` 是同一 native 故障窗口内的触发关系和异常结果关系，不是“重试次数本身直接抛出 SIGSEGV”的普通 Python 异常因果。

最小修复是：进程只保留一个 native DuckDB database owner，业务每次获取独立的 `.cursor()` 事务上下文，不再在每个短事务上并发打开/关闭 native database。该修复保留了 OCC 事务并发、连接级隔离、事务重试和 fail-closed 边界，没有降低 worker 数、没有增加 sleep/timeout，也没有关闭 OCC。

### 1.2 原始 SIGSEGV 帧的未证边界

原始失败运行的 `api.log` 为 0 字节，现场没有 core dump，WSL 环境没有可用的 gdb，因此无法给出当时的 C/C++ faulting frame。报告将这一点明确标为“未证”；但 native 根因类别已通过“重复 native connect/close + OCC 冲突”的受控复现、连接所有权修复前后 A/B、持久层原始值和 3 次最终端到端运行闭环。

## 2. 冻结源码、环境与证据

### 2.1 最终修改文件

| 文件 | SHA256 | 作用 |
| --- | --- | --- |
| `backend/database.py` | `D84BA9146C8383DB49C71F0332A0FD3B58BB96FF124F5B512A670E6BDA217C39` | 保留单一 native database owner，提供独立 cursor；路径切换时关闭旧 owner |
| `backend/tests/test_duckdb_occ_coordinator.py` | `3BE9BE57E307A53A09AC4FFAAB978908BCEECA041AD1507DEF52ADB50B2F88EB` | 新增变量宽度 OCC 冲突完整性和 native owner 复用回归 |

最终隔离源码快照位于 WSL ext4：

```text
/tmp/mc-s01-20261015-root/source
```

最终源码 manifest：

```text
/tmp/mc-s01-20261015-root/evidence/s01-source-manifest.json
```

manifest 统计：233 个 `backend/`、`frontend/` 与 `AGENTS.md` 源文件，组合 SHA256：

```text
D57C3F28AF86D94A50874A198F0BF78C923EAF1CFBEA6BD2EAC2EB74595A3F2A
```

说明：源码文件从 `git archive HEAD` 解包到 WSL，line ending 与 Windows 工作区的 CRLF 表示不同，因此以 WSL 快照中实际被测试的 bytes 计算 manifest；`backend/database.py` 和新增测试文件另有上表单文件 SHA256。

### 2.2 运行环境

```text
AlmaLinux 8.10
Linux 6.18.33.2-microsoft-standard-WSL2 x86_64
CPython 3.11.13
DuckDB 1.2.0
22 logical CPUs
MC_DUCKDB_WRITE_CONCURRENCY=occ
MC_DUCKDB_THREADS=1
```

完整依赖与运行时记录：

```text
/tmp/mc-s01-20261015-root/evidence/environment-runtime.txt
/tmp/mc-s01-20261015-root/evidence/environment-dependencies.txt
```

隔离运行使用新 API 进程、新 DuckDB、新 Journal、新端口和新输出目录；数据库、WAL 和 Journal 均位于 WSL ext4。没有连接真实 SMSC、真实号码或生产数据库。

### 2.3 最终修复逻辑

`backend/database.py` 中新增：

```python
_ROOT_CONNECTION = None
_ROOT_CONNECTION_PATH = None
_ROOT_CONNECTION_LOCK = threading.Lock()

def _root_connection():
    global _ROOT_CONNECTION, _ROOT_CONNECTION_PATH
    with _ROOT_CONNECTION_LOCK:
        if _ROOT_CONNECTION is not None and _ROOT_CONNECTION_PATH != DATABASE_FILE:
            _ROOT_CONNECTION.close()
            _ROOT_CONNECTION = None
            _ROOT_CONNECTION_PATH = None
        if _ROOT_CONNECTION is None:
            _ROOT_CONNECTION = duckdb.connect(
                DATABASE_FILE, config=duckdb_connection_config()
            )
            _ROOT_CONNECTION_PATH = DATABASE_FILE
        return _ROOT_CONNECTION

def get_connection():
    return WrappedConnection(_root_connection().cursor())
```

不变量：

- 同一进程同一路径只有一个 native database owner；
- 每个逻辑事务仍拿独立 cursor，具有独立事务上下文；
- `set_database_file()` 切换数据库路径时关闭旧 owner；
- 测试直接 monkeypatch `DATABASE_FILE` 时，路径比较会在下一次取连接时关闭旧 owner；
- OCC 写写冲突、完整事务回滚和有界重试逻辑不变；
- Journal、attempt、part、socket、unknown/partial、DLR 栅栏不变；
- 不改变业务 schema，不增加 ready pool、credit、slot fairness、PDU window 或 worker 扩容。

## 3. 原始故障证据链

### 3.1 原始失败运行

历史报告指向的原始运行仍保留，未重写、未清理：

```text
/tmp/mc-s0-20261015-closure/runs/marketing-copilot-stress-1k-full-0ad5ef80/
```

进程退出快照：

```text
diagnostics/process-exit-api-20261015T074114Z.json
```

事实：

| 项目 | 原始值 |
| --- | --- |
| `returncode` | `-11` |
| `returncode_hex` | `0xFFFFFFF5` |
| 观察方式 | 父验收进程 `waitpid`/`Popen.poll()` 观察到 API 异常退出 |
| `api.log` | 0 字节，无 Python traceback |
| 现场 core/gdb native stack | 不可取得 |
| 退出前 live progress | `socket_submit=500`、`response_observed=500`、`task_accepted=0` |
| API 日志最后阶段 | `realtime.ingestion.state` OCC 冲突重试至 `attempt=5/5`，之后进程退出 |

`0xFFFFFFF5` 是 32 位显示的 `SIGSEGV` 数值 `11` 的补码形式，不是独立 native 状态码。

### 3.2 原始 DB/Journal 状态

原始失败审计：

```text
/tmp/mc-s0-20261015-closure/evidence/s0-occ-fast-1k-isolated.audit.json
```

| 项目 | 值 |
| --- | --- |
| task 总数 | 600 |
| task 状态 | accepted 301、sending 199、queued 100 |
| socket submit | 500 success / 0 reject |
| attempts | 500（301 accepted、199 authorized） |
| file 状态 | completed 4、retrying 1、processing 1 |
| chunk 状态 | completed 5，非法状态 1 |
| 非法 chunk | `completedcompletedcompletedcompletedcompletedcompleted` |
| Journal | 53 records，pending 17，quarantined 0 |
| committed Journal event 缺失 DB event | 0 |

非法 chunk 的 `status`、`created_at`、`updated_at`、`completed_at` 都是多个事务值的持久化拼接；这不是审计脚本解码错误，因为查询直接返回了拼接后的 DB 值。

### 3.3 原始故障链的标签

| 结论 | 标签 |
| --- | --- |
| API 被内核以 `SIGSEGV` 终止 | 事实 |
| 退出前处于 ingestion OCC 冲突和重试耗尽窗口 | 事实 |
| `0xFFFFFFF5` 是 `-11` 的 32 位显示 | 事实 |
| 原始进程没有 core/C++ stack，无法定位 faulting frame | 未证 |
| OCC 重试耗尽直接导致 SIGSEGV | 未证；只能确认同一窗口和同一 native 故障簇 |
| 非法 chunk 来自 DB 持久层而非内存/审计解码 | 事实 |
| 非法 chunk 与 SIGSEGV 共享写路径 | 事实：同一 run 的 ingestion state、task enqueue、chunk checkpoint、Journal 路径 |

## 4. 受控复现与 native 根因

### 4.1 未修复基线的重复失败

使用历史基线源码副本，未应用修复：

```text
/tmp/mc-s0-20261015-closure/final-v4-source
```

在 WSL ext4、`occ`、`threads=1`、10 files × 100 events、Fast、pool=4、task workers=8、ingest workers=2、window=50、queue=2000、TPS=200、DLR disabled、timeout=180 下：

```text
/tmp/mc-s01-baseline-crash/evidence/baseline-occ-fast-10x100-a1.json
/tmp/mc-s01-baseline-crash/runs/marketing-copilot-stress-1k-full-e1e9f664/
```

结果：

| 门禁 | 结果 |
| --- | --- |
| run status | FAIL |
| 业务完成 | 994 / 1000 tasks |
| socket | 997 submit / 997 success / 0 reject |
| file | completed 8、retrying 1、processing 1 |
| 非普通 chunk | file 6 和 file 9 均出现 `completedcompletedcompletedcompleted` |
| 修复前性质 | 同一参数下无法稳定守恒，且能重复产生非法 VARCHAR 状态 |

同类 6 × 100 未修复运行还提供了线程证据：

```text
/tmp/mc-s01-20261015-root/runs/marketing-copilot-stress-1k-full-d50072f7/diagnostics/application-threads-20261015T082052Z.txt
```

该快照中两个业务线程在 30 秒内无进展，堆栈都停在：

```text
backend/database.py line 1268 in get_connection
duckdb.connect(DATABASE_FILE, config=duckdb_connection_config())
```

这证明慢点不是业务 SQL 之后、也不是 Python 锁内，而是 native database 创建/缓存生命周期。

### 4.2 连接所有权修复后的 A/B

| 场景 | 未修复 | 修复后 |
| --- | --- | --- |
| 6 × 100 OCC+Fast | 可复现非法 chunk 状态 | 连续诊断运行通过 |
| 10 × 100 OCC+Fast | 1/4 受控尝试失败，994 tasks，2 个 chunk 拼接 | 最终 3/3 独立 1K 通过，1000 tasks |
| 关键 native 现象 | 线程阻塞在 `duckdb.connect()` | 只在启动初始化一次 owner，不再并发 open/close |
| 变量宽度 OCC 冲突回归 | 原始基线可观察到非法状态 | `test_duckdb_occ_coordinator.py` 确定性冲突测试通过 |

因此根因不是“重跑后偶然成功”，而是通过“未修复重复失败 + native connect 栈 + 持久层原始值 + 修复前后 A/B + 独立门禁”形成的可审计闭环。

## 5. 最小修复与测试

### 5.1 源码差异范围

只修改：

```text
backend/database.py
backend/tests/test_duckdb_occ_coordinator.py
```

没有修改：

```text
业务表结构、Journal 协议、attempt/part 状态机、SMSC 真实连接、worker 数量、
OCC 模式开关、前端代码、部署包、用户已有的 frontend/package-lock.json
```

### 5.2 新增确定性回归

`backend/tests/test_duckdb_occ_coordinator.py` 新增：

1. `test_occ_conflict_preserves_variable_width_state_exactly`
   - 多个线程在同一 `VARCHAR` 状态行上制造冲突重试；
   - 要求最终状态严格等于 `completed`，时间戳必须是单个可解析值；
   - 防止回滚 replay 再次产生拼接字符串。
2. `test_concurrent_cursors_reuse_the_initialized_database_owner`
   - native owner 初始化后，并发业务线程只能获得 cursor；
   - monkeypatch `duckdb.connect()` 为失败函数，若发生重复 native open 则测试失败；
   - 防止修复回退为每事务 `connect/close`。

为覆盖原始非法状态查询，既有 R02/R17/S0 测试继续保留：

```text
backend/tests/test_s0_correctness.py
backend/tests/test_occ_ingestion_consistency.py
backend/tests/test_sms_result_recovery.py
```

## 6. 最终验证结果

### 6.1 G0 默认全套后端

命令：

```text
cd /tmp/mc-s01-20261015-root/source
MC_DUCKDB_WRITE_CONCURRENCY=serialized MC_DUCKDB_THREADS=1 \
/tmp/mc-s0-20261015-closure/venv/bin/python -m pytest backend/tests -q \
  -p no:cacheprovider --basetemp=/tmp/mc-s01-20261015-root/pytest-final-g0
```

原始日志：

```text
/tmp/mc-s01-20261015-root/evidence/g0-final-snapshot.log
```

结果：exit `0`；`290 passed, 2 skipped, 4 warnings in 228.84s`。

### 6.2 OCC 定向门禁

命令覆盖：

```text
test_s0_correctness.py
test_occ_ingestion_consistency.py
test_realtime_progress_batches.py
test_fast_send.py
test_multi_campaign_queue_isolation.py
test_sms_result_recovery.py
test_startup_recovery.py
test_duckdb_occ_coordinator.py
```

原始日志：

```text
/tmp/mc-s01-20261015-root/evidence/t02-occ-final-snapshot.log
```

结果：exit `0`；`163 passed, 4 warnings in 118.09s`。

### 6.3 确定性 OCC owner/变量宽度回归

```text
/tmp/mc-s01-20261015-root/source/backend/tests/test_duckdb_occ_coordinator.py
```

结果：`6 passed in 2.37s`；exit `0`。

### 6.4 T01 / sender-only 最终快照

| 门禁 | report | 退出码 | 独立审计 | 关键守恒 |
| --- | --- | --- | --- | --- |
| T01 ingest-only | `evidence/s01-t01-ingest-final.json` | 0 | PASS | 1000 tasks；socket=0；attempt=0；10 files completed；10 chunks completed |
| sender pool=1 | `evidence/s01-sender-pool1-final.json` | 0 | PASS | 1000 accepted；1000 socket success；1000 attempts；1000 durable parts |
| sender pool=4 | `evidence/s01-sender-pool4-final.json` | 0 | PASS | 1000 accepted；1000 socket success；1000 attempts；1000 durable parts |
| multipart partial-submit | `evidence/s01-sender-multipart-final.json` | 0 | PASS | 1 task partial；6 socket submits；1 success、5 reject；1 attempt；6 durable parts |

对应日志和 `.audit.json` 位于：

```text
/tmp/mc-s01-20261015-root/evidence/
```

### 6.5 OCC+Fast 1K 三次独立新进程运行

最终源码快照上的三次运行：

| 运行 | 退出码 | 独立审计 | tasks | socket | attempts | files/chunks |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `s01-occ-fast-1k-final-r1` | 0 | PASS | 1000 | 1000 submit / 1000 success | 1000 accepted | 10 / 10 completed |
| `s01-occ-fast-1k-final-r2` | 0 | PASS | 1000 | 1000 submit / 1000 success | 1000 accepted | 10 / 10 completed |
| `s01-occ-fast-1k-final-r3` | 0 | PASS | 1000 | 1000 submit / 1000 success | 1000 accepted | 10 / 10 completed |

三次运行的审计均为：

```text
expected_unique_keys=1000
missing_keys=0
extra_keys=0
invalid_chunk_states=0
unexpected_task_states=0
duplicate_attempt_tasks=0
journal pending=0
journal quarantined=0
journal_committed_missing_db_event_ids=0
```

报告/审计文件：

```text
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r1.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r1.audit.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r2.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r2.audit.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r3.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r3.audit.json
```

### 6.6 Frontend build

命令：

```text
cd frontend
npm run build
```

结果：exit `0`；Vite `1794 modules transformed`，`dist/index.html` 生成成功；只有 Rolldown pure annotation 和 chunk size warning，没有构建错误。

原始输出：

```text
.tmp/mc-s01-frontend-build.log
```

### 6.7 原始失败与修复后门禁的关系

原始失败证据完整保留，没有被成功重跑覆盖。`S0.1` 的三次 OCC+Fast 运行只证明最终修复源码在固定参数下稳定收敛；它们不升级 G3 四组合验收，也不证明真实运营商送达。

## 7. 对账与状态不变量

### 7.1 最终 OCC+Fast 对账

每个最终运行均完成双向 key 集合比对：

```text
输入业务 key 集合 == sms_tasks dedupe_key 集合
task → attempt → part/socket → result Journal/event → DB 事件
socket submit 数 == planned/actual PDU 数
```

最终三次运行均为：

- 10 files completed，10 chunks completed；
- 1000 tasks，全部 accepted；
- 1000 attempts，全部 accepted；
- 1000 actual socket submit_sm，全部 response success；
- Fast items 1000 checkpointed；
- Journal pending/quarantined 均为 0；
- committed Journal event ID 没有缺失 DB event；
- 没有未知/partial 自动整条重发；
- DLR disabled，只证明本地提交和业务状态，不证明运营商送达。

### 7.2 停止、重试和结果边界

- OCC 冲突按完整逻辑事务重放，不提交部分事务；
- 非法 `completed...` 不再被 substring 判断为完成；
- 未证明的 chunk 状态 fail-closed 保留原始值；
- 只有 lease/version fence 成立的转换能提交；
- 已可能写出的 attempt/part/Journal 证据保持不变；
- Journal 已提交事件与 DB 事件一致；
- 没有新增 late submit、重复 socket 或跨 attempt 污染。

## 8. 测试缺口、未证分支和剩余风险

1. 原始 `SIGSEGV` 没有 core 或 native C/C++ faulting frame；本报告只证实 native database owner 生命周期根因类别，不伪造具体 C++ 栈。
2. 最终 OCC+Fast 运行在修复后没有触发 OCC 冲突重试；冲突路径由确定性单元测试和未修复基线 A/B 覆盖，而不是靠最终重跑碰运气。
3. 所有运行均为 loopback/mock SMSC；没有真实 SMSC、SLES12SP5、REAL_SMSC 或生产数据验收。
4. `DLR disabled` 不能充当真实送达证明。
5. G3 的 `serialized/reliable`、`occ/reliable`、`serialized/fast`、`occ/fast` 四组合 × 3 不在本任务范围；本次只完成 S0.1 所需的最终源码 3 次 OCC+Fast 1K。
6. 本修复没有升级 DuckDB；如果后续升级版本，必须重新执行本报告中的 OCC、Journal、attempt/part、停止栅栏和端到端门禁。
7. 工作区原有 `frontend/package-lock.json` 修改和未跟踪用户资料均保留；本次没有提交或推送任何改动。
8. 最终 `git diff --check` 没有 whitespace error；仅提示工作区 line-ending 会被 Git 规范化，未发现本次源码的格式错误。

## 9. 最终门禁判定

| 门禁 | 结论 |
| --- | --- |
| R01 最小故障闭环 | PASS：false commit、OCC 完整性、连接生命周期和非法 chunk 回归通过 |
| R02 非法 chunk/lease | PASS |
| R03 Fast/Reliable 停止栅栏 | PASS |
| R17 DLR/unknown/partial | PASS |
| G0 最终源码后端全套 | PASS：290 passed / 2 skipped |
| OCC 定向 | PASS：163 passed |
| G1/T01 | PASS：1000 tasks、10/10 files、socket=0 |
| sender-only pool=1/4 | PASS |
| multipart 关键路径 | PASS |
| OCC+Fast 1K × 3 | PASS，审计全 PASS |
| S0.1 稳定性回归 | `S0.1_READY` |
| P0_CORRECTNESS_READY | `YES` |
| G3 四组合 / SLES12SP5 / REAL_SMSC | `NO_GO / NOT_VERIFIED` |
| 生产切换 | `NO_GO` |

本结论仅针对 S0.1 范围内“已证 native 连接生命周期根因、最小修复、确定性冲突回归、守恒审计和最终源码 3 次独立 OCC+Fast 1K”。它不自动升级 G3、生产、真实 SMSC 或 S1–S4。

## 10. 复现入口

最终复核命令入口均保留在隔离目录；关键原始文件如下：

```text
/tmp/mc-s01-20261015-root/source
/tmp/mc-s01-20261015-root/evidence/s01-source-manifest.json
/tmp/mc-s01-20261015-root/evidence/g0-final-snapshot.log
/tmp/mc-s01-20261015-root/evidence/t02-occ-final-snapshot.log
/tmp/mc-s01-20261015-root/evidence/s01-t01-ingest-final.json
/tmp/mc-s01-20261015-root/evidence/s01-sender-pool1-final.json
/tmp/mc-s01-20261015-root/evidence/s01-sender-pool4-final.json
/tmp/mc-s01-20261015-root/evidence/s01-sender-multipart-final.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r1.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r2.json
/tmp/mc-s01-20261015-root/evidence/s01-occ-fast-1k-final-r3.json
```

本次没有提交或推送。工作区最终 `HEAD` 仍是上游基线 `a856dc05b5b3272a7196a86a71b4ad9ff01ea922`，最终源码改动以本报告中的 manifest 和两个修改文件 SHA256 标识。
