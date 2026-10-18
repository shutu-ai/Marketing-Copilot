# Marketing Copilot VNext 分层 SMPP 吞吐 RCA

任务：`MC-VNEXT-LAYERED-THROUGHPUT-ISOLATION-20261018`

结论先行：在冻结参数下，L0/L1 证明 loopback、SMPP transport、credit 和 window 本身可以达到约 4×200 TPS；主要可复现掉点出现在 L1→L2（加入真实 `TaskSpool`/`ShardWorker` 的单 worker bounded-pass 调度），5K stage-active 从 793.045 降到 260.860 PDU/s。L2→L3 再加入逐事件 fsync 的 `ProgressJournal` 后降到 197.140 PDU/s。3 组 5K AB/BA 边界交叉复跑保持同一结论。没有找到足以支持产品默认值或生产源代码修改的单一安全补丁，因此本任务采用 `NO_CHANGE_WITH_EVIDENCE`。

## 1. 范围、基线与环境

- 基线：`origin/main = 3a9a1a4e6029fa965b4b7fff4ca4c49fddd54b58`。
- 源：服务器 `source-b-3e73ab80`，未改动产品源码；新增的诊断工具是 [`backend/tools/vnext_layered_throughput.py`](../backend/tools/vnext_layered_throughput.py) 并通过 `--source-root` 注入源目录。
- 服务器：`paas-core01`，SLES 12 SP5，kernel `4.12.14-122.307-default`，x86_64，`/dev/sda2`、ext4；CPython 3.11.5；`duckdb==1.2.0`。
- 正式数据、spool、journal、profile、日志均位于 `/home/paas/mc-vnext-strict-ab-20261018` 的原生 ext4；运行通过 detached `nohup` 启动，结束时无残留测试进程，SMSC active bind 为 0。
- 固定参数：4 campaigns；workers=8（L4）或 1（L1–L3 隔离）；pool=4；per-bind TPS=200；`tps_scope=per_bind`；总 ceiling=800 PDU/s；window=50；5K 为每 campaign 1250；L2/L3 bounded pass=250；response delay=0；loopback；instrumentation=ON。

服务器 L4 5K 的 pre/post 采样显示这是共享主机：80 CPU、load average pre `7.97/7.15/6.71`、post `8.59/7.55/6.88`；iostat 第一完整区间 user 9.73%、system 3.09%、iowait 0.28%、idle 86.89%，sdb util 56.19%。因此没有把共享主机上的绝对吞吐宣称为独占容量；但交叉复跑的层间排序稳定。

## 2. 分层定义与测量语义

| 层 | 实际组件 | 明确排除 |
| --- | --- | --- |
| L0 | 既有 direct socket/SMSC simulator reference | 应用 route credit、worker、spool、journal、projection |
| L1 | `AsyncSmppTransport` + `RouteCreditBroker` + 实际 window + loopback SMSC；response sink 为内存诊断 sink | `ShardWorker`、`TaskSpool`、`ProgressJournal`、DuckDB projection |
| L2 | L1 + 实际 `IngressRouter`、`TaskSpool`、`ShardWorker`、checkpoint；response sink 为内存诊断 sink | durable journal、projection |
| L3 | L2 + 实际 `ProgressJournal.append()`，每个事件 JSON encode/write/flush/fsync | projection DB、offset consumer/reclaim |
| L4 | 现行 `vnext_streaming_acceptance.py` 完整路径 | 无组件排除 |

所有层使用同一组定义：

```text
stage_active_pdu_rate       = successful socket_written PDU / (last_socket_write - first_socket_write)
response_phase_pdu_rate     = successful response PDU / (last_response - first_socket_write)
e2e_wall_pdu_rate           = successful socket_written PDU / (test_end - test_start)
projection_committed_events_rate = committed progress events / relevant observation interval
```

L1–L3 的 `first/last` 来自 transport snapshot；L4 的 stage 时间由 loopback per-bind 的 first/last received/response monotonic timestamp 聚合。L4 wall 使用现行 harness `duration_seconds`。L1–L3 的 profiler duration 是单进程 bounded deque；5K 运行中 `sample_count=4096` 的指标只代表最近 4096 个样本，计数器才代表全量 count。

## 3. 1K smoke 与 5K pre-screen

| 层 | 1K stage | 1K wall | 5K stage | 5K wall | 5K bind count | 5K 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| L0（既有 reference） | — | — | 14,239.716 | — | 4 attempts | reference PASS |
| L1 | 788.078 | 779.044 | **793.045** | **791.372** | 4 | PASS |
| L2 | 441.928 | 415.681 | **260.860** | **255.996** | 80 | PASS |
| L3 | 322.280 | 307.581 | **197.140** | **194.221** | 80 | PASS |
| L4 | 322.798 | 40.061 | **384.391** | **85.953** | 46 | PASS |

单位均为 PDU/s，L4 的 stage 与 wall 不是同一运行阶段：stage 是 socket/response active window，wall 包含 feeder、response drain、projection commit/offset drain。L4 5K 的完整 wall 为 58.171 s，因此 `5000/58.171=85.953` PDU/s。

### 数据面与 durable 面校验

- L1/L2/L3 5K：accepted=5000、socket_written=5000、SMSC received=5000、responses=5000、unknown=0、write_failed=0、duplicate=0、最终 active bind=0。
- L3 5K：journal durable events=10000，其中 response events=5000，journal event count 与 sink count 一致。
- L4 5K：现行 harness acceptance counts 为 accepted/socket/response 各 5000，journal/projection 各 10000，unknown/write_failed/duplicate=0，projection queue/backlog=0，offset 到 EOF，resource gate=PASS。
- L4 进程 profile 的聚合 counter 在最后一个 profile snapshot 时少于 acceptance summary（例如 socket 4986、journal durable 9971），这是采样/进程收尾时序差异；不以 profile counter 覆盖 harness 的最终 durable acceptance count。

## 4. 边界 RCA

### L0→L1：不是网络或 SMPP 基础路径瓶颈

L0 direct reference 为 14,239.716 PDU/s。加入实际 route credit、per-bind=200 和 window=50 后，L1 5K stage=793.045，已经贴近明确的 800 PDU/s 配置 ceiling。L1 credit consumed=5000、granted=5024、wasted tail=24，4 个 bind acquire/release 对称；四个 bind 的 rate key 均为 200.0。由此 L1 的吞吐下降是配置上限与真实 transport/window 语义的结果，不是 SMSC simulator 的异常。

L1 5K 的关键诊断样本：response-window wait P95=0.257 ms、SMPP response latency P95=0.834 ms、TPS credit wait P95=330.513 ms（均为最近 4096/5000 样本），credit wait 的累计时间是并发 task 的求和，不能直接当 wall time。

### L1→L2：真实 worker/spool bounded-pass 调度边界

L2 5K 在相同 800 ceiling 下 stage=260.860，较 L1 下降 67.107%。L2 使用真实 spool checkpoint 和 1 worker、250-task bounded pass；5000 条任务形成 20 个 pass、80 次 bind acquire/release。L2 的 response-window P95=11.590 ms、SMPP response latency P95=40.965 ms、TPS credit wait P95=101.267 ms（最近 4096/5000 样本）。

这个边界不是由“关闭 transport”单独证明的。额外的保持 transport 对照使用同样的 L2/5K/250 pass：bind 从 80 降至 4，pass-start→first-write proxy 从 10.195 s 降至 0.342 s，close/rebind proxy wall share 从 52.231% 降至 1.740%；但 stage 仅为 258.431、wall 为 253.587，和默认 close-per-pass 的 260.860/255.996 在共享主机波动内，没有形成改善。这个 proxy 包含 worker scheduling 与 credit wait，不是纯 TCP bind duration；实际 `close()` 本身只有约 21.7 ms。

因此当前最可信的 RCA 是：L1→L2 的掉点来自真实单 worker/spool/checkpoint/credit 调度组合在 bounded pass 下的串行化与等待；不能把全部差异归因于 bind close/rebind，也没有证据支持只改 transport 生命周期。

### L2→L3：durable ProgressJournal 边界

L2 与 L3 保持相同的 1 worker、20 pass、80 bind 生命周期；加入逐事件 journal 后，5K stage 从 260.860 降至 197.140，下降 24.427%，wall 从 255.996 降至 194.221 PDU/s。

L3 5K 全量 durable 计数为 10000 events、10000 fsync。profiler 全量 counter：`journal_append_total_ns=4.308081420 s`，`journal_fsync_total_ns=2.362700184 s`；最近 4096 个 fsync 样本 P50/P95/P99 为 0.209/0.339/0.391 ms。fsync 本身不是唯一耗时，逐事件 append 的 open/segment scan、同步 sink callback 和 event-loop 串行化共同构成 L2→L3 边界。

### L3→L4：不能作为单一 projection 负向结论

L4 采用 8 workers、完整 feeder、journal、ProjectionWriter；它的 active stage=384.391，高于单 worker L3，因为并行度与 pass 生命周期不同，不能把 L4/L3 直接当作 projection 的 AB 负向对照。完整 E2E wall=85.953，主要尾部包括 response-journal drain=4.068 s、projection/offset commit drain=31.621/31.622 s。

L4 的 vectorized `staging_bind_execute` 共 23 batches；结果中的 P95=2055.263 ms、P50=1999.232 ms，确实存在约 2 s staging anomaly。但该 P95 是 projection-writer 单进程样本的 per-process P95，并由现有聚合器取各进程 P95 的 max，不是 pooled/global P95；样本 count=23，不能用它精确描述所有 worker 的全局分布。它说明 projection staging 是完整 E2E 尾部的高价值观察点，但不推翻 L1→L2 的首要隔离边界。

## 5. 结论与产品影响

1. `RouteCreditBroker`、per-bind TPS、window=50 和 loopback SMPP 基础路径符合冻结 contract；没有调整 route TPS/window。
2. L1→L2 的吞吐掉点可复现，但保持 transport 的对照没有改善，因此没有足够证据把生产修复限定为 transport close/rebind。
3. L2→L3 的 journal durable 代价可复现；没有绕过 journal、改成非 durable sink 或削弱 fsync。
4. L4 的 projection staging 约 2 s anomaly 已证实并正确标注统计来源，但没有以非 pooled P95 直接推出代码改动。
5. 未修改 route TPS、window、projection batch/status 默认值，也未改产品源码；最终决策为 `NO_CHANGE_WITH_EVIDENCE`。50K 与 600 s 稳态不是本次边界判断所需，均未运行；`600S_800PDU=NOT_RUN`。

## 6. 证据索引

完整机器可读证据见 [`Marketing_Copilot_VNext_Layered_Throughput_Evidence.json`](Marketing_Copilot_VNext_Layered_Throughput_Evidence.json)。AB/BA 与最终决策见 [`Marketing_Copilot_VNext_Layered_Performance_AB_and_Decision.md`](Marketing_Copilot_VNext_Layered_Performance_AB_and_Decision.md)。服务器结果根目录为 `/home/paas/mc-vnext-strict-ab-20261018/runs`；所有层的 result SHA256、参数、计数和 timing 均在 JSON 中列出。
