# Marketing Copilot VNext EulerOS 并发性能与恢复最终验收报告

- 任务：`MC-VNEXT-EULEROS-CONCURRENT-FINAL-ACCEPTANCE-20261017`
- Run：`vnext-daily-projection-final-20261017-codex01`
- 目标机：EulerOS 2.0 SP15 / native ext4 / 非 root `paas`
- 业务时区：UTC；通过 VNext `BusinessClock` 记录，未修改系统时间
- 总体结论：`NO_GO`
- `EULEROS_VNEXT_FINAL_CANARY_READY=NO`

本轮没有修改产品源码、没有 commit/push、没有部署生产、没有连接真实 SMSC。`src` 是冻结归档的原始源码；因仓库脚本存在 CRLF，仅复制到 `src-linux` 后对执行副本的 shell runner 做了临时换行规范化。流式 feeder 是验收用临时工具，不属于产品源码。

## 1. 来源、环境与依赖身份

- 当前基线：`71ca9e25f40d9c57932ab8c02a6b69877a099eba`
- 当前 tree：`4fd9eb904e95661ca90ce2474d5fdc03d8a9d374`
- 旧 A/B 基线：`439a1842c4f1ed9144fd7605695435fca76c472c`
- 当前源码归档 SHA256：`75c797cb5390387bc63a12edac5b2fe4924a9d53979db448da2f620da3d7c8e2`
- EulerOS kernel：`6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`
- CPython：`3.11.5`；DuckDB：`1.2.0`
- 文件系统：`/dev/sda2 ext4`
- 资源护栏：CPU `0-3`、`nice +10`、`ionice class=3`、RSS `2 GiB`、单测试 Run 数据上限 `5 GiB`
- 依赖冻结：`evidence/deps.freeze.txt`

详情见 `evidence/source-identity.json`、`evidence/env-check.json`、`evidence/config-effective.json`。

## 2. E1 回归与基础并发闭环

- Linux normalized shell execution：`409 passed, 2 skipped, 4 warnings`，`125.45s`，exit `0`。
- VNext daily/spool/projection focused tests：`12 passed`，`5.98s`。
- T1 限额矩阵：控制、低 Bind、window 1、window 2 均 PASS；低 Bind 的 `max_active_binds=1`，目标配置为 `Worker=8 / Bind pool=4 / per_bind=200 / window=50`，配置授权上限为 `800 PDU/s`，不是实测保证值。
- T2 P5500：PASS。4 个 Campaign 分布为 `2500+2500+250+250`；accepted/socket/loopback receive/response/projection 均 `5500`；pending `0`；duplicates `0`；projection lag `0`；max active binds `4`；Projection 在 Worker 前启动；单 Writer/单 DuckDB。

| 场景 | 用时 | 实测 socket PDU/s | 结果 |
|---|---:|---:|---|
| P10 | 1.758s | 5.69 | PASS |
| P100 | 1.887s | 52.99 | PASS |
| P1K | 2.904s | 344.35 | PASS |
| P5500 | 8.699s | 632.26 | PASS |
| one-shot 50K | 77.935s | 641.56 | PASS；单 Campaign，不是 T3 证据 |

P10/P100/P1K/P5500 的 projection lag 均为 `0`、pending 均为 `0`，并且每个场景都在独立 Run 根目录内执行。

## 3. 大容量、持续负载与 A/B

### T3：50K+ 多 Campaign 持续供给

`multi-campaign-50k.json` 的冻结 CLI one-shot 50K PASS，但该工具对非 5500 数量只生成一个 Campaign，因此不能冒充 T3 多 Campaign streaming PASS。

随后使用仅测试用 bounded feeder 尝试 50K、4 Campaign。40 条四 Campaign smoke（含两组 multipart）PASS；正式 50K 流式组合在约 20K feeder watermark 附近无新增 task event，分别保留了 multipart 和单段两组现场，exit `143`。该现象在本轮被归类为 `TEST_HARNESS_ERROR/INCONCLUSIVE`：临时 feeder 的回压和重复全量 spool 状态扫描需要进一步独立修正，未据此声称产品缺陷，也未修改产品源码。因此 T3 为 `NOT_VERIFIED`。

详情：`evidence/multi-campaign-50k-streaming.json` 及对应 `stream-*.log`；现场目录留在受控服务器 Run 内。

### T4：连续 600 秒

执行了约 `681.091s` 的 `480000` PDU one-shot 长负载候选，但不是持续 feeder，且结果 FAIL：

- generated `480000`
- socket/receive/response/projection `450341`
- pending/missing `29659`
- projection lag `0`、journal pending `0`、duplicates `0`
- spool logical bytes `226866321`
- 观测发送吞吐约 `661.1 PDU/s`；配置上限 `800 PDU/s`
- exit `2`，无 native crash；失败原因是未完全排空

因此 T4 连续供给门禁为 `NOT_VERIFIED`，长负载候选记录为 `FAIL`，不能把一次性 480K 供给当成 600 秒稳态 PASS。当前没有足够的 per-stage 观测数据把瓶颈归因到某一个组件；报告不虚构 Producer、fsync、Window、Journal 或 Projection 的单独占比。

### T5：旧/新 A/B

使用同一 EulerOS/ext4、CPython/DuckDB、CPU/nice/ionice 护栏、Worker/Bind/TPS/window 配置和同一冻结 loopback CLI，各运行 3 次 P1K concurrent E2E：

- 旧基线 3/3 PASS，平均 `4.731333s`
- 当前基线 3/3 PASS，平均 `2.361667s`
- 描述性 `old_mean / new_mean = 2.003387x`
- 所有运行 socket/projection/pending 对账闭合

这是 P1K 的描述性 A/B，不是 50K 或 600 秒的性能签署；旧基线缺少新版本的部分 metadata 字段，但核心计数和配置一致。详情见 `evidence/projection-ab.json`、`evidence/full-e2e-ab.json`、`evidence/ab-comparison.json`。整体 `PERFORMANCE_NON_REGRESSION` 仍因 T3/T4 未闭环而为 `NOT_VERIFIED`。

## 4. 日界、Spool 回收与恢复

T6 的注入时钟单测 PASS，覆盖：

- `2026-10-17T23:59:59Z → 2026-10-18T00:00:00Z` 的 BusinessClock rollover
- 旧日 Spool namespace 隔离、closed segment reclaim、重开后当前日记录保留
- 旧 V1 记录无 `business_day` 时 fail-closed
- 过期记录 durable checkpoint，且不调用 transport
- StageCredit release-before-wait race
- ProjectionWriter 跨日 set-based 去重与 daily summary 隔离

这些单测断言 old-day socket writes `0`、重启重派 `0`、expired transport calls `0`。但本轮没有通过正式 API/调度入口完成完整的服务级注入时钟跨日、Worker prefetch/Bind wait/TPS wait/multipart partial-write/restart-at-cutoff/多日跳跃组合。因此 `BUSINESS_DAY_EXPIRY=NOT_VERIFIED`，`spool-reclaim=NOT_VERIFIED`；不能把同日 P5500 中的两个零计数当成完整跨天证明。

详情见 `evidence/daily-boundary.json`、`evidence/spool-reclaim.json`、`evidence/t6-unit-tests.log`。

## 5. T7 故障矩阵

已有工具实证通过：

- Worker crash/restart：crash exit `17`，`5` 条 crashed、`15` 条 recovered，pending `0`，unique `20`
- Projection Writer pause/resume：applied `20`，blocked `true`，duplicates `0`，queue depth `0`，lag `0`
- control pause fence：paused writes `0`，resume 后 writes `5`
- StageCredit 有单测级 PASS

尚未由现有故障工具闭环：feeder pause 30–60s、双 Route 单路不可用隔离、Writer kill/crash replay、IPC queue full、Journal high-water/hard-limit、Spool capacity exhaustion、跨天与故障组合。因此整体 `STAGE_RECOVERY=NOT_VERIFIED`，详情见 `evidence/fault-matrix.json`。

## 6. Gate 判定

| Gate | 判定 | 依据 |
|---|---|---|
| SOURCE_IDENTITY | PASS | frozen archive/hash、依赖、EulerOS/ext4/Python/DuckDB 已固定 |
| EULEROS_CONCURRENT_E2E | PASS | P10/P100/P1K/P5500 全量对账闭合 |
| BUSINESS_DAY_EXPIRY | NOT_VERIFIED | 注入时钟单测通过，但服务级跨天矩阵未闭环 |
| STAGE_RECOVERY | NOT_VERIFIED | 关键三类通过，完整矩阵缺口明确 |
| SINGLE_WRITER_PROJECTION | PASS_FOR_COMPLETED_E2E / NOT_VERIFIED_FOR_LONG_STEADY | 完成场景单 Writer/单 DB/lag 0；长负载未完全排空 |
| PERFORMANCE_NON_REGRESSION | NOT_VERIFIED | P1K A/B PASS；T3/T4 不满足完整条件 |
| NATIVE_STABILITY | NOT_VERIFIED | 已完成场景无 native crash，但没有完整 600s/故障资源审计闭环 |
| REAL_SMSC | NOT_RUN_BY_SCOPE | 仅 127.0.0.1 synthetic loopback |
| SLES12SP5 | NOT_VERIFIED | 本轮目标机是 EulerOS，不等同 SLES12SP5 |
| PRODUCTION_DEPLOYMENT | NOT_RUN_BY_SCOPE | 未部署、未改服务 |

## 7. 收尾审计与证据

- 测试结束后未发现本 Run 的 acceptance worker、ProjectionWriter、fault tool 或 streaming harness 残留进程/监听器。
- 审计时目标 ext4 分区约 `82%` 使用率；Run 根目录总量约 `6.3G`，其中包含复用的隔离 Python runtime/venv 和受控诊断现场；各负载子 Run 大小记录在 `post-run-audit.txt`。
- 没有删除历史 Run；本轮只在新 Run `vnext-daily-projection-final-20261017-codex01` 内写入数据。被终止的临时 streaming 进程仅为明确 PID，现场日志保留。
- `evidence-manifest.json` 只列人读/机器证据和日志，排除 DB/WAL/Spool/Journal/venv；原始运行数据仅保存在受控测试服务器。
- 无密码、真实 MSISDN、生产数据库或生产 SMSC 证据进入 bundle。

最终结论：当前版本在 EulerOS native ext4 上的基础并发闭环和单 Writer 机制已获得实证，但 T3 真正多活动 50K+ 持续供给、T4 连续 600 秒无积压、完整 T6/T7 尚未满足。故不能给出 Canary Ready，必须保持 `EULEROS_VNEXT_FINAL_CANARY_READY=NO`。
