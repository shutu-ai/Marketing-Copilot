# Marketing Copilot VNext 生产容量与内存 RCA 报告

任务：`MC-VNEXT-PRODUCTION-SIZING-SINGLE-WRITER-THROUGHPUT-CLOSURE-20261017`

结论：`NO_GO`，并标记为 `BLOCKED_ENV/NOT_VERIFIED`。本轮在 WSL native ext4 完成了可复现的单 Writer、单 DuckDB 本地测量、容量模型、A/B 正确性和全量 backend 测试；实际 8 GiB/16 GiB 部署规格、目标 cgroup/CPU quota、测试服务器本轮运行和约 800 PDU/s 高压门禁均未验证，因此不能给出生产容量或 canary 通过结论。

## 1. 范围与不变量

- 保持一个 `ProjectionWriterProcess`、一个 DuckDB 文件和一个 DB owner。
- 不按 campaign 拆 Writer，不修改 schema，不关闭 event fsync、offset durability 或 event-id 幂等。
- 本轮生产代码 `backend/vnext/projection_writer.py`、`backend/vnext/runtime.py` 在实验回滚后与基线保持一致；只保留 benchmark/harness 的测量增强。
- 不把数据库、WAL、Journal、Spool、上传文件、日志、venv、缓存或凭据写入交付/报告产物。原始 Run 目录只作为受控本地证据路径引用，不应 Git add。

## 2. R0 环境冻结

| 项目 | 已冻结值 |
|---|---|
| Git HEAD | `f4ab33735e657f1734db13d90e223c38047a0f22` |
| HEAD tree | `82284ae4c1f6c7f8fec911594019582735b78b24` |
| 候选范围 | working-tree measurement-only；生产 Writer/runtime 实验已回滚 |
| WSL | AlmaLinux-8 WSL2，`x86_64` |
| Kernel | `6.18.33.2-microsoft-standard-WSL2` |
| CPU | 可见 22；正式本地 run 使用 `taskset -c 0-3` |
| 优先级 | `nice 0`，`ionice default` |
| 文件系统 | `/dev/sdd ext4`，源、venv、DuckDB、Journal、Spool、临时目录同一 native 文件系统 |
| mount | `rw,relatime,discard,errors=remount-ro,data=ordered` |
| Python | CPython `3.11.13` |
| DuckDB | `1.2.0` |
| 依赖 | `pytest 8.3.5`、`psutil 6.1.1`、`fastapi 0.115.0`、`smpplib 2.2.4`、`numpy 2.2.6`、`pandas 2.3.2` |
| `df -T .` | `/dev/sdd ext4`；1,055,762,868 KiB total，109,650,936 KiB used，892,408,460 KiB available，11% |

机器总内存约 15 GiB、swap 4 GiB；这是可复现的本地 ext4 参考环境，不是目标生产机器规格。完整冻结值见：
`Docs/Marketing_Copilot_VNext_Production_Sizing_R0_Environment.json`。

## 3. 资源模型

任务要求的 profile 分开定义如下；目前只有模型值，实际部署值没有确认：

| Profile | 机器总 RAM | 初始 application-only RSS budget | reserve | 状态 |
|---|---:|---:|---:|---|
| `PROD_8G` | 8 GiB | 6 GiB | 2 GiB | `NOT_VERIFIED` |
| `PROD_16G` | 16 GiB | 12 GiB | 4 GiB | `NOT_VERIFIED` |
| `LOW_MEM_2G` | 2 GiB | 2 GiB stress guard | 不作生产 reserve 结论 | stress-only |

RSS 口径固定为两套同时记录：

1. `application-only`：从 Writer/worker root PID 递归汇总应用进程。
2. `harness-inclusive`：包含 feeder、loopback SMSC、测试进程等 harness 子树。

二者不能互换。历史 2 GiB guard 的服务器结果也不能直接当作 `PROD_8G` 或 `PROD_16G` 预算。

## 4. 本地 T3 资源结果

两次 run 均在同一 native ext4、同一基线源和同一 harness envelope 下执行；本地 T3 配置为 8 workers、4 binds、per-bind route quota 200、window 50、projection batch 512、600 秒 drain guard，resource profile 使用 `PROD_8G` 的 6 GiB application-only guard。

| Run | 结果 | tasks / PDU / response | Journal / projection | duration | producer | SMPP drain | projection drain | app RSS peak | harness RSS peak |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | `PASS` | 50,000 / 50,000 / 50,000 | 100,000 / 100,000 | 446.393s | 229.181s | 207.144s | 208.577s | 716,884 KiB / 0.684 GiB | 1,219,948 KiB / 1.163 GiB |
| temporary optimization | `FAIL`，已回滚 | 50,000 accepted；49,982 PDU/response | 99,982 / 99,982 | 601.522s | — | 285.531s | 287.083s | 729,528 KiB / 0.696 GiB | 1,217,556 KiB / 1.161 GiB |

临时实验产生 18 个 `write_failed`，原因均为 `SMPP response window did not release before timeout`。这不是把失败隐藏成吞吐数字：基线完整闭合，实验 candidate 因响应窗口失败被判失败，随后 production Writer/runtime 变更全部回滚。两次本地 app RSS 都远低于 6 GiB，但这只能说明该本地样本没有触及模型 guard，不能证明 8 GiB/16 GiB 部署在目标流量下安全。

本地 baseline 生产阶段约 `50,000 / 229.181 = 218.2 PDU/s`；因此不能把本地 T3 写成约 800 PDU/s PASS。原始结果：

- baseline：`/tmp/mc-production-sizing-20261017-f4ab-baseline/run-t3-50k/result.json`
- temporary candidate：`/tmp/mc-production-sizing-20261017-candidate/run-t3-50k/result.json`

## 5. Memory RCA

### 5.1 单 Writer 阶段分布

在纯 Writer、DuckDB 1.2.0、vectorized + staging reuse 的 A3 大样本中，累计耗时排序稳定表现为：

`staging` 最大，其次是 `dedupe`、`DB commit` 和 `aggregate`；rows prepare、events insert、offset persist 相对较小。baseline 累计阶段值如下：

| events | rows prepare | staging | dedupe | insert | aggregate | DB commit | batch E2E |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 0.217s | 3.750s | 1.403s | 0.536s | 0.634s | 1.067s | 7.496s |
| 300,000 | 0.667s | 11.692s | 7.660s | 1.722s | 1.981s | 3.592s | 26.984s |
| 500,000 | 0.883s | 15.637s | 13.562s | 2.496s | 3.313s | 5.048s | 40.609s |

这说明高压长跑的主要风险是单 Writer 的持续 catch-up 能力和 backlog，而不是单次 SQL 调用数；内存预算还必须把 Journal、DuckDB、进程树和 harness 的口径分开。

### 5.2 临时生产优化的失败信号

实验 candidate 只在临时 source tree 中加入了 segment path cache 与低频 status/diagnostic 写入，未改变数据模型。它没有得到可接受的端到端收益，反而在高压 T3 中出现 18 次 response-window timeout。由于 event/response 完整性和 drain 门禁优先于微优化，candidate 不合入，生产代码恢复到 `f4ab3373` 的实现。

### 5.3 历史服务器信号

以下只作为历史证据，不是本轮新验证：

- EulerOS 历史 T3 50K 在约 2 GiB guard、4 CPU、低优先级 envelope 下 `projection_drain_timeout`，观察到 drain `742.003s`。
- EulerOS 历史 T4 在同类 2 GiB guard 下于 `331.293s` 触发 RSS：`2,099,128 KiB` 对 `2,097,152 KiB`。

这些信号支持“受限资源下 projection/Journaling catch-up 是瓶颈”的 RCA，但不能用来替代本轮实际 8/16 GiB profile 验证；本任务没有重新连接测试服务器。

## 6. 结论与容量边界

- `PROD_8G` 和 `PROD_16G` 的数值是待确认的容量模型，不是实际部署通过值。
- 本地 app RSS 峰值约 `0.684–0.696 GiB`，不能线性外推到 600 秒、800 PDU/s 或不同 cgroup。
- 本地 baseline projection 最终追平、backlog 为 0；temporary candidate 因 18 个 write failure 未通过。
- `SINGLE_WRITER_CAPACITY_ESTIMATE=NOT_VERIFIED`。不提交“可支撑 800 PDU/s”的生产声明。
- 继续优化前应沿 `staging -> dedupe -> commit -> aggregate` 采集阶段和 backlog 曲线，保持单 Writer/单 DB/逐事件 durability。

## 7. 复现命令与证据

纯 Writer 示例：

```bash
taskset -c 0-3 /tmp/mc-vnext-t7-codex-20261017-r1/venv/bin/python \
  backend/tools/vnext_single_writer_benchmark.py \
  --run-root /tmp/mc-production-sizing-20261017-f4ab-baseline/run-500k-30 \
  --events 500000 --duplicate-fraction 0.30 --campaigns 4 \
  --events-per-sms 2 --business-days 2 --batch-sizes 1024 --labels A3
```

全量 backend 验证：

```bash
<venv>/bin/python -m pytest backend/tests -q
```

最终本地结果：`418 passed, 2 skipped, 6 warnings in 238.50s`。Windows 隔离可写 pytest 临时根目录的 targeted 结果为 `13 passed in 10.26s`；frontend 未改动，本轮未重跑 build。

## 8. End card

```text
BASELINE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22
CANDIDATE_HEAD=f4ab33735e657f1734db13d90e223c38047a0f22+working-tree measurement-only
RESOURCE_PROFILE=UNCONFIRMED
REAL_DEPLOYMENT_RAM_TOTAL_GIB=NOT_VERIFIED
REAL_DEPLOYMENT_VNEXT_BUDGET_GIB=PROD_8G 6 GiB initial / PROD_16G 12 GiB initial (unconfirmed)
CPU_QUOTA_OR_AFFINITY=local taskset 0-3; target unconfirmed
NICE_AND_IONICE=local nice 0/default
FS_AND_DUCKDB_VERSION=ext4 /dev/sdd; DuckDB 1.2.0
SOURCE_DIFF_SUMMARY=benchmark/harness only; production Writer/runtime rollback/no diff
RSS_APPLICATION_PEAK_GIB=baseline 0.684; temporary candidate 0.696
RSS_HARNESS_PEAK_GIB=baseline 1.163; temporary candidate 1.161
RSS_SLOPE_LAST_180S=negative for both local runs
SOCKET_PDU_RATE_600S=NOT_VERIFIED; local baseline producer phase about 218 PDU/s
JOURNAL_EVENT_RATE_600S=NOT_VERIFIED
PROJECTION_COMMITTED_EVENT_RATE_600S=NOT_VERIFIED
JOURNAL_BACKLOG_BYTES_END=0 after local baseline T3 projection catchup; target profile NOT_VERIFIED
JOURNAL_BACKLOG_TREND_LAST_180S=negative/zero at local baseline T3 end; target NOT_VERIFIED
SMPP_DRAIN_SECONDS=baseline 207.144; temporary candidate 285.531
PROJECTION_DRAIN_SECONDS=baseline 208.577; temporary candidate 287.083
T3_50K_MULTI=local baseline PASS; temporary candidate FAIL, rolled back
T4_600S_800_PDU=NOT_VERIFIED
T6_BUSINESS_DAY=local benchmark has 2 business days; production gate NOT_VERIFIED
T7_9_OF_9=NOT_VERIFIED; current local matrix covers T7-4..T7-9 only
CORRECTNESS_DIGEST_MATCH=YES for pure Writer A/B at 0/10/30% duplicate and 100/300/500K
PERFORMANCE_AB_COMPARABLE=YES on same local ext4; no +15% improvement
PRODUCT_BOTTLENECK_RCA=staging then dedupe/commit/aggregate; experimental high-pressure response-window failure
SINGLE_WRITER_CAPACITY_ESTIMATE=NOT_VERIFIED
CANDIDATE_READY=NOT_VERIFIED
RELEASE_DEPLOYMENT_AUTHORIZED=NO
UNVERIFIED_OR_BLOCKED_REASONS=actual 8/16 GiB target, cgroup/CPU/I/O and authorized target-server run absent
```
