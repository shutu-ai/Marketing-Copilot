# Marketing Copilot VNext — SLES Throughput Recheck and Comparison

任务：`MC-VNEXT-NETWORK-INCIDENT-SERVER-PERF-RECHECK-20261018`

本报告只复核服务器本地 synthetic loopback 吞吐，不是生产容量承诺，也没有修改 SMPP/Writer 生产逻辑。

## 1. 统一口径与冻结配置

速率定义：

- `e2e_wall_pdu_per_sec = successful socket-written PDU / complete run wall`。
- `feed_phase_socket_pdu_per_sec = socket-written PDU / producer_active_seconds`；不与 E2E wall 混用。
- `loopback_receive_pdu_per_sec` 使用 harness 的 loopback first-receive → last-receive 口径，单独列出。
- Window/Credit/Response P95 仅在 `instrumentation=on` 时有有效 duration 样本；instrumentation OFF 结果中的 `null` 表示未采样，不是 0 ms。

正式 R1/R2 配置：SLES 12 SP5、native ext4、localhost loopback、4 binds、workers=8、pool=4、200 TPS per bind、window=50、producer batch=250、A0/vectorized、projection batch=512、status interval=1.0 s、response delay=0、`PROD_8G`、application RSS guard、projection drain timeout=120 s。R1 的正式 3×5K 使用 `1250,1250,1250,1250`；R2 正式 50K 使用任务指定的 `20000,20000,5000,5000`。

源码冻结：`HEAD=origin/main=3e73ab8030b46b78e109cbf1f23e330c72d2d136`。服务器执行源包 SHA-256 为 `b6d71818e5a0b3e701c2eca775c19a604bcdc2d797d83932176ce1acad7d473d`；CPython `3.11.4`、DuckDB `1.2.0`，依赖 freeze SHA-256 为 `502422a5a1be6e2cea2e0ae206c01940b3104c6d906545d8219cdbda72a04227`。

## 2. 历史数据和参数差异

| 运行 | source / instrumentation | workers | status interval | counts | wall | E2E wall rate | feed rate | loopback | Window P95 | Credit P95 |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 历史快 C0 | `450e84d8` / ON | 8 | 0.1 s | 12500×4 | 94.671 s | 528.2 | 687.2 | `NOT_AVAILABLE` | 334.362 ms | 636.319 ms |
| 历史慢 S0 | `3e73ab80` / ON | 4 | 1.0 s | 12500×4 | 308.882 s | 161.9 | 181.257 | 169.112 | 2532.409 ms | 426.990 ms |
| 本轮 50K sensitivity | `3e73ab80` / OFF | 8 | 1.0 s | 12500×4 | 308.829 s | 161.902 | 183.528 | 177.921 | 未采样 | 未采样 |
| 本轮 50K formal | `3e73ab80` / OFF | 8 | 1.0 s | 20000/20000/5000/5000 | 296.301 s | 168.747 | 183.768 | 177.350 | 未采样 | 未采样 |

不能把历史快轮的 0.1 s status interval、8 workers、旧 source 与历史慢轮的 1.0 s、4 workers、3e73 source 静默视为同配置回归。当前正式轮将 workers=8 和任务指定 counts 冻结，但仍使用 status interval=1.0；因此结论是“网络非因果、慢轮区间可重现”，不是严格的 source-only 回归证明。

## 3. R1：3×5K 正式 OFF 复测

| Run | UTC start–end | wall s | E2E PDU/s | feed PDU/s | loopback PDU/s | Window/Credit/Response P95 | app/harness RSS KiB | Sampler | Result SHA-256 |
|---|---|---:|---:|---:|---:|---|---:|---|---|
| `5k-w8-c0-02` | 16:38:15–16:39:12 | 56.535 | 88.441 | 167.628 | 135.587 | 未采样 / 未采样 / 未采样 | 303848 / 579740 | 62/62 valid | `722f64e6…8afb6b1` |
| `5k-w8-c0-03` | 16:43:40–16:44:35 | 54.421 | 91.876 | 173.762 | 144.604 | 未采样 / 未采样 / 未采样 | 304324 / 572364 | 60/60 valid | `89acc929…f36ef150` |
| `5k-w8-c0-04` | 16:45:10–16:45:53 | 42.263 | 118.307 | 173.052 | 134.024 | 未采样 / 未采样 / 未采样 | 303968 / 565012 | 64/64 valid | `b61a9b24…ff73d0a` |

三轮均满足：`PASS/COMPLETED`、accepted/socket/response `5000/5000/5000`、Journal/Projection `10000/10000`、unknown `0`、duplicate PDU `0`、write_failed `0`、backlog `0`、exit `0`、`resource_gate=PASS`。正式 OFF E2E 速率范围为 `88.441–118.307`，median `91.876`；loopback 范围为 `134.024–144.604`，median `135.587`。速率波动明显，但没有回到 528.2 PDU/s。

### 3.1 instrumentation-ON 诊断复测

因为 OFF 模式按设计不产生 Window/Credit duration 样本，追加了 3 次同 workload、同 workers/binds/TPS/window/status 的 ON 诊断；这三次不替换正式 OFF 样本，也不作为生产容量验收。

| Run | wall s | E2E PDU/s | feed PDU/s | loopback PDU/s | Window P50/P95/P99 ms | Credit P95 ms | Response callback P95 ms | app/harness RSS KiB |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| `5k-w8-inst-on-01` | 44.380 | 112.663 | 175.162 | 126.960 | 803.424 / 2212.822 / 2336.256 | 506.841 | 0.934 | 306896 / 573692 |
| `5k-w8-inst-on-02` | 42.700 | 117.096 | 174.010 | 137.782 | 752.179 / 1832.590 / 2427.747 | 524.950 | 0.832 | 307408 / 577336 |
| `5k-w8-inst-on-03` | 55.030 | 90.860 | 172.105 | 142.358 | 772.554 / 2258.624 / 2382.291 | 524.743 | 0.917 | 306028 / 573792 |

ON 诊断的 Window P95 范围为 `1832.590–2258.624 ms`，Credit P95 为 `506.841–524.950 ms`，Response callback P95 为 `0.832–0.934 ms`；每轮采样均 `sampler_ok=true`。由于没有在同一秒内做 OFF/ON paired run，不能从这 3+3 轮单独估计 instrumentation overhead。

## 4. R2：正式 50K 对账

正式 Run：`50k-w8-c0-02`，参数为 `counts=20000,20000,5000,5000`、workers=8、4×200 per-bind、window=50、batch-size=250、A0/512、vectorized、instrumentation OFF。

| 指标 | 实测 |
|---|---:|
| wall / E2E rate | 296.301 s / 168.747 PDU/s |
| feed active / feed rate | 272.082 s / 183.768 PDU/s |
| loopback first-to-last receive | 177.350 PDU/s |
| accepted / socket-written task / PDU / response | 50000 / 50000 / 50000 / 50000 |
| Journal / Projection | 100000 / 100000 |
| unknown / duplicate / write_failed | 0 / 0 / 0 |
| projection caught-up / lag / backlog | true / 0.0 s / 0 bytes |
| `resource_gate` / exit | PASS / 0 |
| app RSS peak / harness RSS peak | 767084 / 1192096 KiB |
| sampler | 426 records，426 `sampler_ok=true` |
| bind lifecycle | 182 binds，`max_active_binds=4`，reconnect/error=0 |
| Window/Credit/Response P95 | 未采样（instrumentation OFF） |

Projection 的非关键路径 metrics 仍有效：actual batch p50/p95 `24/168` events，batch count `2123`，stage batch p50/p95 `6.135/15.380 ms`，Journal bytes `51,787,919`，projection duplicate `0`，offset 到 EOF，Projection process exitcode `0`。最后 response → Projection tail 的关键 monotonic 时间戳在 OFF 模式为 `null`，不能编造毫秒值；可使用 `projection_drain_seconds=12.557` 作为完整 drain 口径。

均匀分布的 `50k-w8-c0-01` 是敏感性样本，不是 formal R2：它使用 `12500×4`，wall `308.829 s`、E2E `161.902 PDU/s`、loopback `177.921 PDU/s`，同样完整对账。它与 formal 的 4.2% 速率差异不足以改变“处于历史慢轮区间”的判断。

## 5. Host 资源和磁盘

- 主机为共享 `80 logical CPU / 约 375 GiB RAM`；本轮没有更换到隔离硬件。
- 正式 50K preflight：`df -T .` 为 `/dev/sda1 ext4`、`78%`，可用 `4,439,468 KiB`；postflight 可用 `4,328,180 KiB`。运行目录约 `109 MiB`。
- 正式 50K pre/post loadavg 分别约 `8.95/12.09/12.21` 和 `13.84/12.95/12.50`；最终核验 `17:13:22Z` 可用 `4,280,236 KiB`、loadavg `7.32/10.87/11.91`，无活动 harness/测试端口。
- `iowait/await`：`NOT_CAPTURED`。RSS sampler 的主采样源为 `/proc`，SLES `ps -o` 交叉解析不兼容，但不影响 `/proc` 主采样；所有正式/诊断轮的 `sampler_ok` 均为 true。

## 6. 对比判断

1. 正式 50K `168.747` 相对历史快轮 `528.2` 低约 `68.05%`，没有性能恢复；相对历史慢轮 `161.9` 高约 `4.23%`，属于同一慢轮区间。
2. 3×5K OFF 与 3×5K ON 都在约 `90–118 PDU/s` E2E 范围内，且 loopback 仅约 `126.960–144.604 PDU/s`；这证明 server-local loopback 可以稳定完成，但不能承诺 800 PDU/s。
3. 当前正式轮的外部控制链路与测试数据面解耦；GitHub/SSH 事件不足以解释服务器内部 loopback 速率。共享 host load、低磁盘余量、credit/window 调度和历史 source/config 差异仍需在更安静、更可观测的环境单变量拆分，不能通过本轮直接归一为产品 SMPP regression。

## 7. Gate / final card

```text
SOURCE_HEAD=3e73ab8030b46b78e109cbf1f23e330c72d2d136
ORIGIN_MAIN=3e73ab8030b46b78e109cbf1f23e330c72d2d136
UNRELATED_WORKTREE_PRESERVED=YES
TARGET_SERVER_OS=SLES 12 SP5 / native ext4
TEST_SERVER_HOST=paas-core01
SERVER_AUTHORIZED=YES
NETWORK_FAILURE_UTC_WINDOW=2026-10-18T11:31:22Z–11:35:10Z VPN dial failures/recovery; no overlap with historical slow run
CLIENT_GITHUB_443_INCIDENT=UNKNOWN_FOR_HISTORICAL_WINDOW; CURRENT_PROBE_FAILED
CLIENT_SSH_VPN_INCIDENT=YES_BUT_NOT_OVERLAPPING_SLOW_RUN
SSH_SESSION_DEPENDENT=NO
SERVER_LOOPBACK_INDEPENDENT=YES
REMOTE_TEST_PID_AND_EXIT_CAPTURED=YES
R0_HISTORICAL_CONFIG_DIFF=FAST 450e84d8, workers8, status0.1, instrumentation ON; SLOW 3e73ab80, workers4, status1.0, instrumentation ON; CURRENT workers8/status1.0, official R2 instrumentation OFF
HISTORICAL_FAST_E2E_WALL_PDU_PER_SEC=528.2
HISTORICAL_SLOW_E2E_WALL_PDU_PER_SEC=161.9
NEW_5K_THREE_RUNS_E2E_WALL_RATE=OFF 88.441–118.307; ON diagnostic 90.860–117.096
NEW_5K_THREE_RUNS_LOOPBACK_RATE=OFF 134.024–144.604; ON diagnostic 126.960–142.358
NEW_5K_THREE_RUNS_WINDOW_P95=ON diagnostic 1832.590–2258.624 ms; OFF NOT_CAPTURED
NEW_5K_THREE_RUNS_CREDIT_P95=ON diagnostic 506.841–524.950 ms; OFF NOT_CAPTURED
SERVER_HOST_LOAD_AND_IOWAIT=loadavg observed 8.28–15.08/9.78–14.99/10.87–13.01; IOWAIT NOT_CAPTURED
SERVER_FREE_DISK_AND_RUN_SIZE=official pre 4,439,468 KiB, post 4,328,180 KiB; run ≈109 MiB; final 4,280,236 KiB
NEW_50K_STATUS=PASS
NEW_50K_E2E_WALL_RATE=168.747
NEW_50K_FEED_PHASE_RATE=183.768
NEW_50K_LOOPBACK_RECEIVE_RATE=177.350
NEW_50K_WINDOW_P95=NOT_CAPTURED (instrumentation OFF)
NEW_50K_BIND_LIFECYCLES=182 binds, max_active=4, reconnect/error=0
NEW_50K_JOURNAL_PROJECTION_RECONCILIATION=50000/50000 PDU-response; 100000/100000 Journal-Projection; backlog=0; caught_up=true
NEW_50K_UNKNOWNS_DUPLICATES=0/0; write_failed=0
APPLICATION_RSS_SAMPLER_VALID=YES; /proc sampler valid on all records
COMPARED_METRICS_SAME_DENOMINATOR=YES for explicitly defined E2E/feed/loopback rates; P95 only ON diagnostic
CAUSE_EVIDENCE_AND_CONFIDENCE=NETWORK_EVENT_NOT_CAUSAL (HIGH for network non-causality; MEDIUM/INCONCLUSIVE for exact host/harness bottleneck)
BACKEND_SOURCE_CHANGED=NO
PRODUCTION_DEFAULT_CHANGED=NO
R4_600S_800PDU=NOT_RUN
CANARY_READY=NO
FINAL_DECISION=NETWORK_EVENT_NOT_CAUSAL
COMMIT_PUSH=NOT_REQUESTED
```

原始结果保留在服务器隔离目录；本地机器证据摘要见：[Network/SSH RCA](Marketing_Copilot_VNext_Network_Incident_and_SSH_Process_RCA.md) 和 [Evidence JSON](Marketing_Copilot_VNext_Network_Performance_Recheck_Evidence.json)。
