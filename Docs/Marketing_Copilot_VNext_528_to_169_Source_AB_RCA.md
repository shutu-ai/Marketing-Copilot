# Marketing Copilot VNext：528.2 → 169 PDU/s 严格源码 A/B RCA

## 结论卡

本次严格复测没有确认 `A → B` 的源码回归，也没有确认 SMPP loopback 是瓶颈。结论为：

> `ROOT_CAUSE_INCONCLUSIVE`：完整 E2E 的主要可疑区段是应用侧 route credit / SMPP window 等待，以及 projection staging bind 执行；测试服务器是共享主机，主机运行态噪声尚未被完全隔离，因此不能把现象归因给某一个源码提交。

历史 50K E2E 记录为 **528.2 PDU/s**；既有材料中的当前参考区间为 **162–169 PDU/s**，本次不对该参考值重新包装。按任务规定的严格参数在同一台 SLES 服务器上进行 5K A/B 后，四次正式运行得到 **89.16–120.86 PDU/s**，A/B 配对差异反转，A 的中位数仅比 B 快约 **1.16×**，不满足“稳定源码回归”的判定条件。

本次没有运行 50K：5K A/B 没有形成可复现的源码差异，且 L0 loopback 已证明 SMPP 模拟器本身远高于完整链路；继续扩大到 50K 不会增加当前因果证据。

## 1. 范围、来源与安全边界

- 测试对象：`paas-core01`，SLES 12 SP5，原生 Linux `ext4` `/dev/sda2`；所有正式吞吐与延迟结果均在该文件系统完成。
- 测试只连接本机 `127.0.0.1` synthetic SMSC simulator；没有真实 SMSC、客户数据或生产 API，不改变生产配置。
- A 源码：`450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e`。
- B 源码：`3e73ab8030b46b78e109cbf1f23e330c72d2d136`。
- 当前仓库基线：`2579d8d26c1881b42d3c480fe3056e73b982492e`，与 `origin/main` 一致。
- A 源码归档 SHA-256：`97DF322C883EF04A00887F503AAAFD4546C1D63586F94785A1CB26E105CFDDD4`。
- B 源码归档 SHA-256：`805BEC45F8E7EC6576A729704ECCB405BA8C5CB7B0B145D82474FC2832F4B551`。
- 为保证完成判定、resource gate 和结果 schema 完全相同，A 的正式 5K 运行使用“**A product source + B common harness**”适配包；适配包只替换 `backend/tools/vnext_streaming_acceptance.py`，不替换 `backend/vnext`。适配包归档 SHA-256：`AA5481A6AEA8BD172AD71A7674363EBA2ABEE60FC8FCA9CA527E3F054E0D0082`。

## 2. 固定参数与运行环境

| 项目 | 固定值 |
|---|---:|
| workers | 8 |
| SMPP pool / binds | 4 |
| route TPS | 200 PDU/s per bind，合计 ceiling 800 PDU/s |
| SMPP window | 50 |
| producer batch | 250 |
| projection batch | 512，vectorized |
| campaigns | 4 |
| response delay | 0 |
| status interval | 0.1 s |
| instrumentation | ON |
| projection drain timeout | 120 s |
| 数据规模 | 1K smoke；5K strict A/B |

运行环境冻结：SLES `12-SP5`，kernel `4.12.14-122.307-default`，x86_64，CPython `3.11.5`，DuckDB `1.2.0`。依赖来自现有离线 wheelhouse，没有在线下载；wheelhouse SHA-256 为 `504043E2E0FBF31E6C8DCA572392519836289F0F415DE933309BCC6AD5578BDA`。

主机是共享服务器，没有设置 CPU affinity 或改变 nice/cgroup。正式运行前后均记录了 `df -T .`、Python 版本、内存、swap、loadavg、`/proc/stat` 和 iostat。

## 3. 源码差异审计

A→B 共 5 个相关文件，`976 insertions(+), 80 deletions(-)`：

| 文件 | 变更 | 因果风险判断 |
|---|---|---|
| `backend/vnext/route_credits.py` | 增加 credit wait/granted/consumed/wasted/refill cost 计数并输出 snapshot | 代码路径主要是观测；未见 pacing 算法意图变化 |
| `backend/vnext/smpp_transport.py` | 增加 bind 生命周期、window peak、首末写入/响应时间；集中 `_release_bind_lease()` | 有生命周期语义风险，需用异常/重连专测确认；本轮无错误、unknown 或 pending 泄漏 |
| `backend/vnext/worker.py` | 在 worker snapshot 中加入 route credit snapshot | 观测输出，不改变任务派发 |
| `backend/tools/vnext_streaming_acceptance.py` | resource sampler、durable projection completion、loopback/throughput 观测和严格完成状态 | 测试 harness 变更；A 正式运行统一采用 B harness 以消除停止条件差异 |
| `backend/tests/test_vnext_streaming_acceptance.py` | 对新 harness 与观测字段增加测试 | 只影响测试覆盖 |

旧 A harness 的 5K 诊断运行出现 `projection_drain_timeout`，但 accepted/socket/PDU/response 均为 5000，journal/projection 均为 10000，unknown=0，max active binds=4。原因是旧停止条件把多个 journal segment 的总字节与单一 committed offset 比较，形成假阴性；该运行不计入严格 A/B。这个结果说明 harness 兼容性本身必须隔离，不能把它写成数据路径失败。

## 4. 实际运行结果

速率统一按 `count / duration_seconds` 计算；每次正式运行均要求完整计数、durable projection、resource gate 和 completion state 同时通过。

| Run | product / harness | N | duration (s) | E2E PDU/s | status | accepted / socket / resp | journal / projection | max active binds | result SHA-256 |
|---|---|---:|---:|---:|---|---:|---:|---:|---|
| `common-a-1k-01` | A / common B harness | 1,000 | 10.015 | 99.85 | PASS | 1000 / 1000 / 1000 | 2000 / 2000 | 4 | `330426a3a5bd66faf1d0eafe4f9f741f287699dc139bb4e827436c7521018106` |
| `b-1k-01` | B / B harness | 1,000 | 39.959 | 25.03 | PASS | 1000 / 1000 / 1000 | 2000 / 2000 | 4 | `7bbcd1a388763793f366902b7a9ccbec07312a4dc266c1041713a02a4d464831` |
| `common-a-5k-pair-01` | A / common B harness | 5,000 | 41.369 | 120.86 | PASS | 5000 / 5000 / 5000 | 10000 / 10000 | 4 | `eb0cea77d986fcc7ea21d07a08d25dcb473e66519f47395a4dfff46e28eb4dbd` |
| `b-5k-pair-01` | B / B harness | 5,000 | 56.078 | 89.16 | PASS | 5000 / 5000 / 5000 | 10000 / 10000 | 4 | `46dd248e7d3d6cd00f842268f1cc72198afe2fd24a5eacc28c53aae659c16106` |
| `common-a-5k-pair-02` | A / common B harness | 5,000 | 44.353 | 112.73 | PASS | 5000 / 5000 / 5000 | 10000 / 10000 | 4 | `71c63bfd1eb56fb91a718c2bbd41983f2847ef2d1e1fc109b1c0e997c9d40765` |
| `b-5k-pair-02` | B / B harness | 5,000 | 43.350 | 115.34 | PASS | 5000 / 5000 / 5000 | 10000 / 10000 | 4 | `d908ac20c1ab860ef0e497da73494958ddb06860cb34db494df2f4c1136a3f82` |

所有正式 5K 运行还满足：`write_failed=0`、unknown response=0、SMPP error=0、projection backlog=0、completion state=`COMPLETED`、resource gate=`PASS`。

### 4.1 严格 A/B 统计

- A durations：41.369 s、44.353 s；中位数 **42.861 s**，中位数速率 **116.66 PDU/s**。
- B durations：56.078 s、43.350 s；中位数 **49.714 s**，中位数速率 **100.58 PDU/s**。
- A/B 中位数速率比：**1.16×**。
- 配对 1：A 120.86 > B 89.16。
- 配对 2：B 115.34 > A 112.73。

第二组反转直接否定了“B 必然慢于 A”的稳定判断。1K 运行也受共享主机与启动/热身影响，不能替代重复 5K 结论。

## 5. 瓶颈证据与因果边界

在完整 E2E 中，关键路径采样显示等待项明显大于 SMPP simulator 的响应处理时间：

| 运行 | response window wait P95 | SMPP response latency P95 | TPS credit wait P95 | staging bind execute P95 |
|---|---:|---:|---:|---:|
| A pair 1 | 442.916 ms | 658.088 ms | 488.225 ms | 1972.614 ms |
| B pair 1 | 611.682 ms | 716.582 ms | 577.268 ms | 1965.352 ms |
| A pair 2 | 463.730 ms | 682.749 ms | 556.343 ms | 2136.318 ms |
| B pair 2 | 448.824 ms | 667.374 ms | 542.082 ms | 2115.034 ms |

这些值是完整链路中观测到的累计 per-event duration 分位数，不等同于单个请求的 wall-clock 加和；它们用于定位等待类别，不用于虚构唯一根因。A 适配运行使用旧 product source，因而新 route-credit counters 在 A 的聚合 snapshot 中不可用；A 的 loopback bind 生命周期和完整计数仍由 common harness 记录，不能把 A 的 credit 字段 `0` 解读为“没有等待”。

### 5.1 独立 L0 SMPP loopback

独立 benchmark 不启动 Marketing Copilot worker、journal、projection 或 route credit，只测 4 连接、window=50 的本机 SMPP client ↔ simulator：

- 5,000/5,000 response 成功，unknown=0，pending at close=0。
- 客户端观测：**14,239.716 PDU/s**，elapsed 0.351 s，P50/P95/P99 = 13.613/15.879/18.259 ms。
- simulator 自身响应：P50/P95/P99 = 0.041/0.056/0.081 ms，received/responded 均 5000。
- bind attempts/bind count=4，max inflight=150；audit `VERIFIED`，backlog=0，overflow=0。

L0 比完整 E2E 中位数高约 122×。这不是生产容量承诺，因为 L0 有意去掉了应用限速、持久 journal、projection 和 staging；它只证明 SMPP loopback/bind/socket 基础段不是把完整链路压到约 100 PDU/s 的直接原因。

### 5.2 主机与磁盘

正式运行的 host 采样显示：80 CPU、约 375G RAM；运行期间可见约 114G free memory，swap 使用约 158M。common A pair 1 的第一段 iostat 为 `%user=9.74`、`%system=3.09`、`%iowait=0.28`、`%idle=86.88`，`sdb %util=22.33`；运行前后 loadavg 大约在 6.5–10.7 之间。服务器是共享主机，没有 CPU 隔离，load 不能归因给本测试进程。

因此目前只能说“主机运行态是重要控制变量”，不能根据这组采样宣称 CPU、内存或磁盘已经确认根因。此前网络事件复核也没有发现足以解释该差异的网络因果证据。

## 6. 判定与后续门槛

| 假设 | 本次判定 | 证据 |
|---|---|---|
| B 源码造成稳定吞吐回归 | 未确认 | 两组 5K 配对反转；中位数只差 1.16× |
| SMPP loopback / bind 基础段是主瓶颈 | 基本排除 | L0 14,239.716 PDU/s，0 error；E2E bind max active 始终为 4 |
| 网络事件是当前根因 | 未确认/不支持 | 既有网络复核与本轮本机 loopback 证据均不支持 |
| 共享 host 运行态影响 | 可能，未单独确认 | load 变化、无 CPU 隔离；iowait 本身不高 |
| credit/window/staging 是优先调查方向 | 支持为工作假设 | E2E 关键路径等待与 staging bind P95 明显偏高 |

下一轮只有在安静/可隔离的 SLES 主机上重跑并保持同一参数，且 A/B 差异在重复配对中稳定，才应启动 50K 复验。当前不应把 528.2、162–169 与本次 89–121 直接拼成单一时间序列，也不应把共享服务器上的某一次快/慢运行写成源码因果。

## 7. 原始证据索引

原始结果保留在测试服务器的隔离目录：

`/home/paas/mc-vnext-strict-ab-20261018/runs/`

本轮结果归档（服务器侧）SHA-256：`31332f64b5ad76815b14d333d0922442e014e522e081ed1ff0b15fcbc90c1a60`。对应的结构化证据见 `Docs/Marketing_Copilot_VNext_Throughput_RCA_Evidence.json`；SMPP 生命周期隔离见 `Docs/Marketing_Copilot_VNext_SMPP_Loopback_Bind_Lifecycle_Isolation.md`。
