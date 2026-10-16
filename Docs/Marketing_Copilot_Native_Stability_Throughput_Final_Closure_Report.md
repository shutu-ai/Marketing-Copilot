# Marketing Copilot Native Stability / Throughput Final Closure Report

任务：`MC-INTEGRATED-NATIVE-THROUGHPUT-FINAL-CLOSURE-20261016`
执行日期：2026-10-16
最终结论：`INTEGRATED_NO_GO`

## 1. 判定摘要

本次没有放宽原有正确性、安全性、容量或 600 秒长稳标准，也没有新增调度层或进行大规模架构重构。

已闭合的事项：

- `origin/main` 已更新并核对；`HEAD` 与 `origin/main` 均为 `95efa42491af5bb0aa0b08560bf293fd26a92553`。
- G4 runner 的 pool/window/route TPS 参数已落到数据库、Gateway、实际 bind 和 socket 审计；同一路由矩阵通过，双路由真实配置审计失败并被保留。
- 运行期间观察器改为有界的进程内计数；全量 task/event 审计只在停止后执行。
- 原始 OCC+Fast API `SIGSEGV (-11)` 的失败证据未删除；修复后的精确参数复跑未再 SIGSEGV，但更高负载诊断仍出现超时，OCC 另有 WAL write-write conflict，不能宣称 native/容量风险完全闭合。
- DB/Result 只做了一个有边界的 Fast checkpoint coalescing 优化；默认值保持 300 ms，500/1000 ms 仅作为显式可选 profile，未将未证明的吞吐提升写入默认配置。

阻断 S5 的事项：

1. 原 G4 严格大规模合同仍未完成；按当前 runner 映射是 `2 * 50000 + 2 * 1000 = 102000`，而命名 A/B/C 口径是 52000，规格存在未决歧义。
2. 在 route TPS=200、单 route、单 PDU 的不变约束下，52000 至少需要 260 s，102000 至少需要 510 s；若仍要求 180 s/600 s 内严格完成，分别是 `SPEC_INFEASIBLE_NEEDS_DECISION`，不能暗改合同。
3. pool=4/window=50 的 5500-task 四活动诊断在 OCC 和 serialized 下均未在 300 s 内完成；OCC 留下 WAL write-write conflict。
4. 双 route 实际只 bind 了第一条配置，第二条 route 为 TCP connected 但未 bind；配置真实性门禁明确失败。
5. 既有 G6 有效约 631 s 运行只完成 11270 generated、5283 accepted，严格容量目标失败。

## 2. 冻结合同、源码与环境

验收合同写入 [validation_contract.json](validation_contract.json)，状态是 `FROZEN_CONTRACT_NOT_A_PASS`，包含正确性不可放宽、G4 活动映射、route TPS scope、观察器合同、native/core policy 和各 gate 输出。

正式运行根目录均在 AlmaLinux-8 WSL 的 native ext4：

```text
/tmp/mc-final-closure-20261016-c
source: /tmp/mc-final-closure-20261016-c/src
environment: /tmp/mc-final-closure-20261016-c/evidence/environment.txt
```

环境记录包含 `df -T .`、CPython 3.11.13、DuckDB 1.2.0 和完整 `pip freeze`；正式源目录所在文件系统为 ext4。所有 formal DB、WAL、Journal、日志和临时目录都在同一 native ext4 根目录。

最终工作区变更文件 SHA256：

| 文件 | SHA256 |
|---|---|
| `backend/routers/metrics.py` | `af568274e6c0f14fa4072790b5b70537c1eeafd026c0b8efd85096f8b8195f73` |
| `backend/services/fast_send_service.py` | `967abc116fba6e80e494df99d2b151124256e32b9778a9921cd9bc677158da7a` |
| `backend/tests/test_fast_send.py` | `9fea9caea0eb48b50abdc27f8cb2beea11b39a93486375dcf0f14da5906db226` |
| `backend/tools/local_e2e_acceptance.py` | `41fe2fb18b37fe41ef1f7769cb4d6e7fbe2a1b11b06a1b0d30c3c1fd58a32b4c` |
| `backend/tools/mixed_campaign_capacity_acceptance.py` | `8dd72daa7dc0c7b3faf593b1d725ee5a2d585a0238eb3d1b8517bc03f7801133` |
| `backend/tools/multi_campaign_fair_acceptance.py` | `1d7a4a984637bdf2860b6262c8a33814e31eb47dd07a8e3d265c6ca4d5c2e293` |
| `backend/tests/test_local_e2e_acceptance_tools.py` | `58def07c215f2c93b8e9275176874e1e4386d4cd806d1860001a4e5e5abe2e81` |

工作树已有用户修改、未跟踪文档、证据、数据库、WAL、Journal、缓存和临时目录均保留；本次没有 reset、clean、删除、commit 或 push。

## 3. G4 配置真实性与观察器开销

### 3.1 配置真实性

同一路由 smoke/matrix 已核对 CLI declared、DB persisted、Gateway effective slots、SMSC bind events 和 route shared TPS：

| 证据 | 结果 | 有效配置 |
|---|---|---|
| `evidence/multi-same-pool1-window1.json` | PASS | pool=1, window=1, route TPS=200, bind=1 |
| `evidence/multi-same-pool4-window2.json` | PASS | pool=4, window=2, route TPS=200, bind=4 |
| `evidence/same-pool4-window50.json` | PASS | pool=4, window=50, route TPS=200, bind=4 |
| `evidence/sig-target-occ-fast-1k-full-pool4-window50.json` | PASS | pool=4, window=50, route TPS=200, bind=4 |
| `evidence/mixed-two-route-pool1-window1.json` | `HARNESS_CONFIG_FAIL` | route 1 bind=1；route 2 connected=0、bind events=0 |

双路由失败不是观察器误判：当前 `SmppGateway.connect()` 遍历 enabled configs 后在第一条成功配置处返回，第二条只留下 simulator TCP connection，未进入 bind。该失败作为系统配置/实现问题保留，没有改成“通过”。

### 3.2 观察器 A/B

旧 runner 证据 `mixed-two-route-pool1-window1-pre-bounded-observer.json` 在约 60 s 内累计约 22.54 MB 响应，包含 228 次 `/api/sms-tasks` 和 114 次 `/api/metrics/status`，并反复扫描 event log。

修订后的同类失败 `mixed-two-route-pool1-window1.json` 使用 live counters：60 次 `/api/metrics/live`、62 次 `/api/smpp/status`，约 134 KB；不再运行期间分页读取全部 task 或从头扫描完整 event JSONL。最终完整 task/event 对账仍在 shutdown 后执行。

1K patched sender PASS 还证明了最终快照策略：187 次 bounded live calls、1 次 full `/api/metrics/status`，总观察响应约 764 KB；final metrics 文件保留在对应 run artifact 中。

## 4. Native SIGSEGV 与高并发结果

### 4.1 原始失败证据

历史原始失败仍保留在工作区报告和 WSL artifact 引用中：

```text
Docs/Marketing_Copilot_S0_Correctness_Baseline_Closure_Report.md
Docs/Marketing_Copilot_S0_1_OCC_Fast_SIGSEGV_RCA_and_Closure_Report.md
/tmp/mc-s0-20261015-closure/evidence/s0-occ-fast-1k-isolated.audit.json
/tmp/mc-s01-baseline-crash/evidence/baseline-occ-fast-10x100-a1.json
/tmp/mc-s01-baseline-crash/runs/marketing-copilot-stress-1k-full-e1e9f664/
```

原始现场是 API `returncode=-11`，只有约 600 tasks、500 socket submits、301 accepted/199 sending/100 queued，并出现非法 chunk 状态；没有 core 或可用 gdb/native C++ faulting frame。因此本报告不伪造 faulting frame，也不把同时存在的 OCC conflict 直接说成已证实的 DuckDB C++ 根因。

历史 RCA 的可复核结论是 native database owner/connection 生命周期问题类别；连接 owner/cursor 最小修复后已有 3/3 独立 OCC+Fast 1K 闭环。它不能替代本次更高负载的稳定性和容量门禁。

### 4.2 本次复现与控制

精确历史参数复跑：`stress-1k/full + OCC + Fast + task-workers=8 + pool=4 + window=50 + queue=2000 + SMSC TPS=200`。

```text
evidence/sig-target-occ-fast-1k-full-pool4-window50.json: PASS
tasks: 1000/1000 accepted
submit/response: 1000/1000
files: 10/10 completed
audit: VERIFIED
core/native frame: none produced
```

另有更高负载诊断：`large_rows=2500, small_rows=250, pool=4, window=50`，四活动合计 5500 tasks。两轮均保留：

```text
evidence/multi-sig-occ-pool4-window50-2500-250.json
evidence/multi-sig-serialized-pool4-window50-2500-250.json
```

两轮均为 300 s 内业务未闭环；OCC report 还保留 `write-write conflict on key: "realtime, 1, 1"` 的 WAL replay error。两轮没有 SIGSEGV/core，但 timeout、WAL conflict 和未完成业务仍是严格失败，不能用精确 1K PASS 覆盖。

## 5. DB / Result 性能优化与 A/B

唯一系统性能改动是 `backend/services/fast_send_service.py` 的受边界约束配置：

```text
MC_FAST_SEND_FAIR_CHECKPOINT_MS
default: 300 ms
allowed: 300..5000 ms
```

它只改变 Fast 成功 checkpoint 的 coalescing wait，不改变 Result Journal fsync、hard-byte capacity、credit、attempt/part identity、unknown/partial policy、fail-closed 或 queue limit。默认仍是 300 ms。

同一 1K/full、OCC/Fast、pool4/window50、route TPS=200、10 files 参数下的结果：

| fair checkpoint | 结果 | events/s | Result batches | batch time sum | Result DB tx time | journal fsync |
|---:|---|---:|---:|---:|---:|---:|
| 300 ms | PASS | 6.491 | 408 | 35.223 s | 34.429 s | 2.767 s |
| 500 ms | PASS | 5.805 | 339 | 31.263 s | 30.578 s | 2.283 s |
| 1000 ms | PASS | 4.568 | 219 | 25.420 s | 24.988 s | 1.463 s |

所有三轮均为 1000/1000 tasks accepted、1000/1000 socket submit/response、0 unknown/partial/duplicate，capacity live=0、failure 为空、Result Journal healthy。500/1000 ms 的 DB/Result 写放大确实下降，但本次受后台 AlmaLinux service CPU 争用影响的端到端吞吐没有提升；所以 300 ms 保持默认，较高值只可作为显式、经业务延迟预算批准的 profile，不能标作普遍吞吐 PASS。

修订后的 focused tests：`22 passed in 12.97s`。修订后全后端 G0：`348 passed, 2 skipped, 4 warnings`，exit=0。

## 6. 统一 Gate 状态

| Gate | 状态 | 证据/范围 |
|---|---|---|
| G0 | PASS | formal ext4 `g0-final-patched.log`；348 passed/2 skipped；frontend `npm run build` 通过，1794 modules，只有既有 warning |
| G1 | PASS（有界 1K） | patched sender-only PASS、pool/window matrix PASS、multipart/fault 历史证据；不代表 G4 52K |
| G3 | PASS（历史 formal ext4 matrix；当前默认路径聚焦复核） | 既有 4 组合×3=12/12 ext4 PASS 保留于 `.s2-evidence-20261015/g3/`；当前 patched source 另有 22 focused tests 和精确 OCC/Fast full PASS。12 轮未因可选 env knob 再全部重跑，故不把它写成新 SHA 的 12/12 证据 |
| G4_CONFIG / OBSERVER | PASS（same-route）；FAIL（two-route） | same-route declared/effective/bind 审计通过；`mixed-two-route-pool1-window1.json` 为 `HARNESS_CONFIG_FAIL` |
| G4_FAIRNESS | FAIL | scaled four-activity evidence 可对账；严格八项/52K capacity matrix 未完成 |
| G4_CAPACITY | `SPEC_INFEASIBLE_NEEDS_DECISION` | 52000/102000 活动映射和 260/510 s TPS 下限与 180 s deadline 矛盾；保留原 strict FAIL |
| G4_NATIVE | FAIL | 精确 1K 历史崩溃参数复跑无 SIGSEGV，但 5500-task OCC/serialized 两轮 timeout，OCC WAL conflict；native 风险未能按严格高并发合同闭合 |
| G5 | PASS（既有 fault/recovery 范围） | reject/drop/partial/late DLR/disconnect/quarantine/replay/pause/cancel 历史证据；没有把它外推为 G4 长稳 |
| G6 | FAIL | 既有有效约 631.248 s：11270 generated、5283 accepted；严格多 Campaign 容量未完成，证据保留 |
| G-SIM | PASS（既有 simulator contract） | fast/fault、ASCII/UCS2/Flash、multipart、audit VERIFIED 历史证据；不等同真实 SMSC/SLES |

## 7. 失败证据索引与交付边界

本次保留的失败/限制证据包括：

- `evidence/mixed-two-route-pool1-window1.json` 及其前序 harness failures：namespace、connected-count、event-log、bounded-observer 修复过程全部保留。
- `evidence/multi-sig-occ-pool4-window50-2500-250.json`：timeout + OCC WAL write-write conflict。
- `evidence/multi-sig-serialized-pool4-window50-2500-250.json`：serialized timeout control。
- `evidence/multi-same-pool4-window2-pre-live-narrow.json`：旧 full/live observer 造成的 stall/timeout 证据，与修订后的 bounded observer PASS 分开保存。
- 历史 S0/S0.1 API `SIGSEGV (-11)`、非法 chunk、无 core/native frame 的报告和 artifact 引用。
- 历史 `.tmp/mc-g6-ext4-safe-p4-w50.json`：600 s 级有效长稳但 strict capacity FAIL。

未执行真实 SMSC、SLES12SP5 部署、生产切换或 S5。最终交付状态保持：

```text
INTEGRATED_NO_GO
```

进入 S5 前至少需要用户对 52K/102K 活动映射和 deadline 合同作出决定，并重新完成严格 G4 capacity/native matrix、双 route bind/configuration correctness、高并发 WAL/timeout 收敛以及 G6 strict completion；不得以本报告中的 scaled PASS 或可选 profile 替代这些门禁。
