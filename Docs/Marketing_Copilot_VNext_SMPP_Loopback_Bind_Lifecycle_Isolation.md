# Marketing Copilot VNext：SMPP Loopback 与 Bind Lifecycle 隔离报告

## 结论

独立 L0 loopback 在同一台 SLES 12 SP5 服务器、同一原生 ext4 文件系统上，以 `pool=4/window=50` 完成 5K 请求：客户端观测 **14,239.716 PDU/s**，所有 response 成功，P95 15.879 ms。完整 E2E 只有 89.16–120.86 PDU/s，差异来自应用完整链路中的限速、窗口等待、journal/projection/staging 和共享主机运行态；没有证据表明 SMPP simulator 或 bind slot 上限把 E2E 压低到该水平。

本报告是隔离证据，不是对真实 SMSC、运营商链路或生产容量的承诺。

## 1. 隔离层次

| 层 | 启动组件 | 保留 | 去除 | 目的 |
|---|---|---|---|---|
| L0 | `benchmark_smsc_simulator.py` + local simulator | SMPP encode/socket、4 connections、window=50、bind/unbind、response | Marketing Copilot worker、route credit、journal、DuckDB projection、staging | 测量 SMPP loopback/bind/socket 基线 |
| E2E | `vnext_streaming_acceptance.py` | L0 段 + worker、route credit、journal、projection、staging、resource gate | 真实外部 SMSC | 判断完整链路的额外等待与生命周期行为 |

L0 使用 `127.0.0.1`、response delay=0、simulator TPS=0（不额外限速）、fast audit；E2E 使用任务规定的 per-bind 200 TPS 和完整持久化路径。因此两者的速率不能当作同一容量数字，只能用于定位增量成本。

## 2. L0 实测结果

运行目录：

`/home/paas/mc-vnext-strict-ab-20261018/runs/l0-5k-fast/`

结果 SHA-256：`68f346c65b4cc0e060a8559828bc16f38e5865e22da9ab0e023a466f33e9b0fe`。

| 指标 | 实际值 |
|---|---:|
| requested / submit_sm | 5000 / 5000 |
| response success / failed | 5000 / 0 |
| unknown response | 0 |
| pending at close | 0 |
| elapsed | 0.351 s |
| client submit/response rate | 14,239.716 PDU/s |
| client latency P50/P95/P99 | 13.613 / 15.879 / 18.259 ms |
| simulator response P50/P95/P99 | 0.041 / 0.056 / 0.081 ms |
| pool / window | 4 / 50 |
| bind attempts / bind count | 4 / 4 |
| simulator max inflight | 150 |
| simulator connected / disconnected | 5 / 5 |
| audit enqueued / written | 10010 / 10010 |
| audit backlog / overflow | 0 / 0 |
| audit status | `VERIFIED` / `READY` |
| benchmark max RSS | 26,615,808 bytes |
| event loop lag P95 | 0.075 ms |

`connected=5`/`disconnected=5` 是 simulator 的连接统计；bind 维度仍以 `bind_attempts=4`、`bind_count=4` 为准。L0 运行时间很短，resource sampler 只有 1 个样本，不能用于长稳态资源结论。

## 3. 完整 E2E Bind 生命周期

以下数据来自同一服务器、同一 `pool=4`、同一 `window=50` 的 5K 正式运行。`binds` 是整个运行期间的 bind/unbind 生命周期数量，不是并发槽位数；并发上限以 `max_active_binds` 为准。

| Run | product | bind lifecycle count | max active binds | active binds at end | counts / errors |
|---|---|---:|---:|---:|---|
| `common-a-5k-pair-01` | A | 48 | 4 | 0 | 5000/5000/5000，unknown=0 |
| `b-5k-pair-01` | B | 43 | 4 | 0 | 5000/5000/5000，unknown=0 |
| `common-a-5k-pair-02` | A | 47 | 4 | 0 | 5000/5000/5000，unknown=0 |
| `b-5k-pair-02` | B | 46 | 4 | 0 | 5000/5000/5000，unknown=0 |

四次运行均为 resource gate=`PASS`、completion=`COMPLETED`、projection backlog=0；没有出现 bind slot 超限、SMPP error、unknown response、pending at close 或 write failure。

### 3.1 B 版本的 credit/bind 观测

B product source 暴露了新 route-credit snapshot，说明完整链路确实在 credit wait 上消耗了大量时间；A product source 没有这些新增字段，所以 common-A 结果中的 credit 聚合为 0 只能表示“不可观测”，不能表示“没有等待”。

| Run | bind acquires | credit consumed | credit waits | wait seconds | refill cost seconds | remaining / wasted |
|---|---:|---:|---:|---:|---:|---:|
| `b-5k-pair-01` | 37 | 4370 | 4252 | 808.030 | 2.459 | 86 / 86 |
| `b-5k-pair-02` | 46 | 5000 | 4827 | 828.695 | 2.986 | 72 / 72 |

这些是 instrumentation 的累计观测，不是把等待秒数直接加到 E2E duration 的公式。它们与关键路径的 TPS credit wait P95（B1 577.268 ms，B2 542.082 ms）方向一致。

## 4. 关键路径对照

| Run | response window wait P95 | SMPP response latency P95 | TPS credit wait P95 | staging bind execute P95 | E2E rate |
|---|---:|---:|---:|---:|---:|
| A pair 1 | 442.916 ms | 658.088 ms | 488.225 ms | 1972.614 ms | 120.86 |
| B pair 1 | 611.682 ms | 716.582 ms | 577.268 ms | 1965.352 ms | 89.16 |
| A pair 2 | 463.730 ms | 682.749 ms | 556.343 ms | 2136.318 ms | 112.73 |
| B pair 2 | 448.824 ms | 667.374 ms | 542.082 ms | 2115.034 ms | 115.34 |

L0 simulator response P95 0.056 ms，远小于 E2E 的 response/window/credit/staging 观测。因此本轮支持的工作假设是：SMPP socket 能力充足，增量时间主要在完整应用路径；但由于服务器共享，仍不能把 staging 或 credit 中的某一个组件单独宣布为最终根因。

## 5. 生命周期代码审计边界

A→B 的 `backend/vnext/smpp_transport.py` 变化包括：

- 记录 bind acquired/released、最后 bind slot、bind active seconds；
- 记录首个/最后 socket write 和 response 时间；
- 记录 window inflight peak；
- 将异常 reader 路径和正常 close 路径统一到 `_release_bind_lease()`。

这解释了为什么本轮能够确认“结束时 active binds=0”。它也意味着 `_release_bind_lease()` 的异常/重连语义值得单独做故障注入测试；本次任务的无故障 loopback 与 5K E2E 只证明没有观察到生命周期泄漏，不能替代断连、半包、超时、重连测试。

## 6. 原始证据与限制

- L0 原始 JSON：`.../runs/l0-5k-fast/result.json`。
- E2E 原始 JSON：`.../runs/common-a-5k-pair-01/result.json`、`b-5k-pair-01/result.json`、`common-a-5k-pair-02/result.json`、`b-5k-pair-02/result.json`。
- 所有路径均位于测试服务器隔离目录 `/home/paas/mc-vnext-strict-ab-20261018/`。
- 只使用 synthetic loopback；没有真实 SMSC、客户或生产数据。
- 不能用 L0 的 14,239.716 PDU/s 作为生产容量；它的作用是排除 loopback 基础段作为本轮 E2E 低速的直接原因。
