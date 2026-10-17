# Marketing Copilot VNext 单 Writer 性能 RCA 与 A/B 报告

任务：`MC-VNEXT-SINGLE-WRITER-PERF-RESP-JOURNAL-T7-CLOSURE-20261017`

结论：`NO_GO`。本轮已完成单 Writer 热路径定点优化、延迟响应收尾、Journal 分段回收、T7-4～T7-9 隔离故障矩阵，以及 current-SHA native ext4 的 50K 与 600 秒 loopback run；但实际 steady 供给约 `164.2 PDU/s`、独立 drain `274.014s`，尚未在 EulerOS/真实测试服务器验证约 800 PDU/s 目标，不能标记 canary ready。

## 1. 身份与验证环境

- 起点：branch `main`，`HEAD=origin/main=65e3d6f16810cbe1ab46940c23dd00f715f3cd4f`；工作区开始时已存在 dirty `AGENTS.md`、`frontend/package-lock.json` 和多个 untracked 文件，均保留，未 commit/push。
- 正式兼容验证 Run：`/tmp/mc-vnext-t7-codex-20261017-r1`，源、数据库、Journal、Spool、结果均在同一 native ext4：`/dev/sdd ext4`。
- Python：`3.11.13`；DuckDB：`1.2.0`。本轮通过外网 VPN 下载 `pytest==8.3.5`、`psutil==6.1.1` 及 requirements-dev 依赖到 native `wheelhouse/`，再用 `--no-index` 安装；native ext4 完整 `backend/tests` 为 `420 passed, 2 skipped, 4 warnings`（248.78s）。
- 沙箱内 Windows pytest/真实多进程 harness 曾受到 pytest 临时目录 ACL 与 `multiprocessing.Pipe -> WinError 5` 阻断；升权重跑标准命令 `.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q --tb=short` 后为 `417 passed, 5 skipped, 4 warnings`。Windows 仍不作为 Linux 性能基线。
- native venv 的完整 freeze 已记录：`duckdb==1.2.0`、`pytest==8.3.5`、`psutil==6.1.1`、`pandas==2.3.2`、`pydantic==2.9.2`、`fastapi==0.115.0`、`uvicorn==0.30.6`、`smpplib==2.2.4`、`paramiko==3.5.1`、`cryptography==41.0.7`、`bcrypt==4.2.1`、`PyNaCl==1.5.0`、`numpy==2.2.6`、`watchfiles==0.24.0`、`websockets==13.0`、`pika==1.3.2`、`openpyxl==3.1.5`、`xlrd==2.0.1`、`aiofiles==24.1.0`、`python-multipart==0.0.12` 及其 freeze 中的传递依赖。
- 本轮没有连接真实 SMSC/生产数据库/API；SMPP 只绑定 loopback。

关键工作树文件 SHA-256：

| 文件 | SHA-256 |
|---|---|
| `backend/vnext/projection_writer.py` | `21448FAA0DD5F0C33C3556D2F8734EF106AD19377F47F21295F6252EF5A87BC2` |
| `backend/vnext/runtime.py` | `0DECFD464DE8B86160F36D6490CD246F1A7187BF277EAB63A1105D9FBDED0001` |
| `backend/vnext/worker.py` | `1F10A899EA4B7E22DFA9254FA70D97C9A7EFBF8F21A9EF7A53E36C68B11C976C` |
| `backend/vnext/smpp_transport.py` | `38CAB5A0868EC1B0343AD1F7B60BA2BF5C5ED824FCE177478078FDD478FDF17B` |

## 2. What changed

保持一个 `ProjectionWriterProcess`、一个 DuckDB 文件和一个 DB owner，没有引入按 Campaign 分裂的 Writer/DB。

1. `ProgressJournal` 从单文件改为稳定逻辑路径下的 generation segment：`worker.jsonl`、`worker.g00000001.jsonl`……；每个 segment 有独立 offset，只有 DB COMMIT 后 offset durable 且已完整消费的 inactive segment 才能 GC。保留逐事件 fsync；offset 文件写入增加 flush/fsync，并在 POSIX 上 fsync 父目录。
2. Writer 的 25 列 staging 增加 DuckDB 1.2.0 兼容的 parameterized `LIST + list_zip + UNNEST` vectorized 路径，显式列类型、长度检查、event-id 去重、aggregate upsert 和同一事务边界均保留。临时 staging table 生命周期改为复用；默认路径为 `vectorized + reuse_staging`。
3. 增加低频累积 profiling：Journal read/decode、rows prepare、staging、dedupe、events insert、aggregate、DB commit、offset persist、batch end-to-end、SQL calls、batch P50/P95、Journal total/committed/backlog、replay/new counts，并标明 `projection_events_applied=process_local`。
4. `ShardWorker` 增加公共 `drain_transports()` 和 reader-cache 生命周期收尾；runtime 与 streaming harness 统一使用 `run_until_idle(close_transports=False) -> drain -> close`。修正了显式 `progress_context` 参数被旧签名适配逻辑误删的问题，使正常 SMPP response 能生成 durable response Journal event。
5. SMPP 断连/关闭时，未确定的 pending PDU 释放 window 并写入 `unknown` response event，不自动重发已写 PDU。
6. 多 Journal 仍 round-robin，但 quantum 从固定 `<=64` 改为有界 `64～256`，不改变 worker 数、DB owner 或调度架构。

## 3. A0–A3 microbench

工具：`backend/tools/vnext_single_writer_benchmark.py`；4096 events、10% deterministic duplicate、真实 25 列、同一 DuckDB 事务/去重/聚合逻辑，16 个 case，结果：
`/tmp/mc-vnext-t7-codex-20261017-r1/benchmark/result.json`。

以下为 native ext4、DuckDB 1.2.0 的 events/s；每个 case 的 `result_sha256` 与 A0 相同，`all_correct=true`。

| batch | A0 executemany/drop-create | A1 executemany/reuse | A2 vectorized/drop-create | A3 vectorized/reuse |
|---:|---:|---:|---:|---:|
| 256 | 185.757 | 184.825 | 274.290 | 281.067 |
| 512 | 186.982 | 191.869 | 278.454 | 290.763 |
| 1024 | 193.069 | 195.666 | 280.488 | 291.002 |
| 2048 | 187.695 | 191.771 | 277.683 | 289.332 |

512 batch 的 A3 相对 A0 约 `+55.5%`，相对 A2 约 `+4.4%`。因此采用 vectorized、staging reuse 和 bounded quantum；没有因为 SQL call 数量直接臆断 wall-time，报告使用各 stage 的 perf-counter 累计值。

## 4. 端到端证据

### Delayed response

最终小闭环：`/tmp/mc-vnext-t7-codex-20261017-r1/stream-delay-final/result.json`。

- 40 tasks / 40 socket PDU / 40 loopback response / 80 durable Journal+DB events。
- `response_unknown_event_count=0`，`journal_backlog_bytes=0`，committed offset 等于 Journal bytes `41160`。
- drain `2.240s`；最终 stage metrics：batch P50/P95=`2/4`，staging=`869.09ms`，DB commit=`225.51ms`，offset persist=`225.95ms`。

### P5500 mixed multipart

最终 Run：`/tmp/mc-vnext-t7-codex-20261017-r1/s1-p5500-final/result.json`，配置为 2 workers、2 binds、window 50、per-bind 200 TPS、projection batch 512、vectorized+reuse。

| 指标 | 实测 |
|---|---:|
| tasks | 5,503 |
| socket-written tasks / PDU | 5,503 / 16,503 |
| loopback received / response events | 16,503 / 16,503 |
| Journal/DB events | 22,006 |
| events/task；events/PDU | 3.99891；1.333455 |
| duration；停止供给后 drain | 114.363s；97.615s |
| batch P50/P95；batches | 106 / 512；88 |
| Journal total / committed / backlog | 12,625,538 / 12,625,538 / 0 bytes |
| events read / new / replayed | 22,006 / 22,006 / 0 |
| staging / dedupe / insert / aggregate | 106.052s / 0.915s / 0.437s / 1.489s |
| DB commit / offset persist / Journal read+decode | 0.794s / 0.681s / 0.293s+0.226s |
| last sample CPU sum / RSS sum / run bytes | 170.9% / 247,528 KiB / 21.9 MB |

同配置、未启用 reuse 但已启用 bounded quantum 的对照为 duration `119.980s`、drain `101.724s`、staging `111.467s`；reuse 路径约减少 5% staging 时间和 4% drain 时间。两次均 PASS 且结果完整对账。

### T3 50K / T4 600s current-SHA native ext4

两轮均使用 AlmaLinux-8 native ext4、DuckDB 1.2.0、loopback SMSC、8 workers、4 binds、window 50、`per_bind=200`、vectorized+reuse；配置额度为 800 PDU/s，但必须以实际 socket 结果为准。

| Run | 结果 | 对账 | 时间与资源 |
|---|---|---|---|
| `/tmp/mc-vnext-t7-codex-20261017-r1/t3-50k-continue/result.json` | PASS | 20K+20K+5K+5K tasks/PDU/response 全部闭合；100,000 Journal events；unknown=0；projection=100,000；backlog=0 | duration `451.672s`，drain `210.393s`，Journal `51,814,760` bytes |
| `/tmp/mc-vnext-t7-codex-20261017-r1/t4-600s-continue/result.json` | PASS（正确性/600s 运行） | 98,750 tasks/PDU/response；197,500 Journal events；unknown=0；projection=197,500；backlog=0 | steady `600s`；duration `875.269s`；drain `274.014s`；Journal `102,429,414` bytes |

T4 的 600 秒供给阶段实际约 `601.255s`，因此实际供给约 `164.2 PDU/s`、`328.5 Journal events/s`，不是 800 PDU/s 实测能力；feeder backpressure waits=`184`。最大 RSS sum=`1,808,952 KiB`（护栏 2,097,152 KiB），最大 run bytes=`200,366,984`，最大 CPU sum=`410.5%`。T4 Writer stage 累计约为 staging `103.493s`、new-event dedupe `331.029s`、aggregate `142.665s`、DB commit `82.941s`、offset persist `88.989s`；这解释了停止供给后的长 drain，不能把该 Run 写成 800 PDU/s PASS。

## 5. Gate 状态

| Gate | 状态 | 说明 |
|---|---|---|
| `SOURCE_ENV_IDENTITY` | PASS | SHA/branch/dirty、native ext4、Python/DuckDB、隔离 Run 已记录 |
| `WRITER_STAGE_RCA` | PASS | 16-case A0–A3，stage timing、吞吐、hash、DuckDB 版本齐全 |
| `SINGLE_WRITER_VECTOR_BATCH_CORRECTNESS` | PASS | A0–A3 结果 hash 等价；direct projection/replay/segment tests PASS |
| `PRODUCTION_RESPONSE_DRAIN` | PASS | 最终 delayed-response loopback，durable response event 逐 PDU 对账 |
| `JOURNAL_BOUND_AND_REUSE` | PASS（场景级） | hard-limit、generation、GC、恢复场景 PASS；尚未做 600 秒长期容量曲线 |
| `T7_STAGE_RECOVERY` | PARTIAL | 本轮 T7-4～T7-9 全部 PASS；历史 T7-1～T7-3 证据未在本次 diff 后重跑，整体 9/9 不签署 |
| `T3_50K_MULTI_STREAMING` | PASS（native loopback） | current-SHA 50K 完整对账；但尚非 EulerOS/真实 SMSC |
| `T4_HIGH_PRESSURE_STEADY_AND_DRAIN` | PARTIAL / NO_GO | 已完成 600s steady+drain，但实际约 164.2 PDU/s，drain 274.014s，未达到约 800 PDU/s/60s 目标 |
| `NATIVE_STABILITY` | PARTIAL | AlmaLinux native ext4 600s run 无 crash/guard failure；没有 EulerOS 长稳证据 |
| `PERFORMANCE_NON_REGRESSION` | PARTIAL | 当前 SHA 的 50K/600s 证据已补齐；仍缺同配置 old/new paired run |
| `CANARY_READY` | NO_GO | 由上述未验证门禁阻断 |

## 6. 推荐下一步

下一步应在授权的 EulerOS/测试服务器复用同一 source/config，确认当前 native 结果是否可迁移，并执行同配置 old/new paired run；重点继续沿 staging/aggregate/commit/offset 阶段数据优化，目标是降低当前 `274.014s` drain，不引入多 Writer 架构。若需要连接测试服务器，先切换到服务器 VPN，再开始该阶段。
## 7. Final EulerOS server addendum (supersedes the preliminary server-next-step note)

The user-authorized EulerOS test-server runs were completed after switching to the external VPN. All writes were confined to new, unique directories under `/home/paas/marketing-copilot-test/runs/`; no existing Run was overwritten.

Environment evidence:

- Host: EulerOS 2.0 SP15 x86_64, `paas-core`, 64 CPUs; formal Run filesystem: `/dev/sda2 ext4` (`df -T` recorded), with about 13.25 GiB free at the final check.
- Runtime: CPython 3.11.6, DuckDB 1.2.0, pytest 8.3.5, psutil 6.1.1; `pip check` passed. The server was offline-installed from the transferred wheelhouse with `--no-index`.
- Execution envelope: non-root `paas`, CPU affinity `0-3`, `nice 10`, `ionice -c3`, max RSS 2,097,152 KiB, max Run bytes 5 GiB. The host had pre-existing load average about 9–11 at T0.

Server evidence:

| Run | Result | Evidence |
|---|---|---|
| `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex04/t7-recovery/result.json` | PASS | T7-4 through T7-9: 6/6 scenarios, filesystem `ext4`. |
| `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex04/t3-50k/result.json` | FAIL / NO_GO | 50,000 tasks/PDU/response and 100,000 Journal events, unknown=0; `projection_drain_timeout`, duration 817.644 s, measured drain 742.003 s. The final stop cleanup eventually reported 100,000 applied and zero backlog, but not within the 600 s drain guard. |
| `/home/paas/marketing-copilot-test/runs/vnext-single-writer-t7-20261017-codex05/t4-600s/result.json` | FAIL / NO_GO | RSS guard at sample `331.293 s`: max RSS 2,099,128 KiB versus 2,097,152 KiB. Before/around shutdown, 178,000 task/PDU/response events were accounted with unknown=0, but only 142,505/356,000 Journal events were applied; steady 600 s was not completed. Max Run bytes were 268,101,460 and disk capacity was not the limiting guard. |

The server result is therefore a real platform bottleneck signal, not a canary approval: the single-writer projection catch-up is too slow under the constrained 4-CPU envelope, and the high-pressure run reaches the RSS boundary before 600 seconds. The final gate remains `NO_GO`.

The acceptance harness was corrected after the first server T3 run exposed a measurement bug: feeder/spool quiescence is no longer treated as projection completion; the harness now requires live Journal EOF to be at or below committed offsets with zero Journal backlog/queue, and exposes a bounded `--projection-drain-timeout`. Final harness SHA-256: `32AF9A59EC28F31FBF9612F35E44F81E8303E125DE4725D71D6C6B006DE2CB4C`.

Updated server gate summary: `SOURCE_ENV_IDENTITY=PASS`; `T7_STAGE_RECOVERY` is `PASS` for T7-4…T7-9; `T3_50K_MULTI_STREAMING=FAIL`; `T4_HIGH_PRESSURE_STEADY_AND_DRAIN=FAIL`; `NATIVE_STABILITY=FAIL/PARTIAL` under the server RSS guard; `CANARY_READY=NO_GO`. No real SMSC, production API, or production database was contacted.
