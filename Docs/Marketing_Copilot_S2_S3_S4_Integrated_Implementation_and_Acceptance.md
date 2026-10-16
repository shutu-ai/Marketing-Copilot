# Marketing Copilot S2/S3/S4 一体化实施与统一验收报告

任务：`MC-INTEGRATED-ARCH-FAST-SMSC-CLOSURE-20261016`

执行日期：2026-10-16（Africa/Cairo）

## 结论

`INTEGRATED_BLOCKED_ENV`

M0 的 Fast SMSC 模拟器能力、Route TPS/Simulator TPS 解耦、真实 SMPP socket、Fair/PDU Window/multipart、故障恢复与核心数据守恒已实施并取得可复核证据。当前不能宣布 `INTEGRATED_READY_FOR_S5`，原因是：

1. 当前 Windows CPython 3.13 + DuckDB 1.2.0 native runtime 在 OCC + Fast 全链路压力下出现 `0xC0000374` heap corruption；这不是可以通过重试或放宽对账条件掩盖的结果。该环境不能作为 OCC 性能/稳定性通过环境。
2. G0 的 Windows 全后端 pytest 受到历史 AppData 临时目录 ACL/清理错误影响；另一次外部权限重跑长时间无进展后被中断，未声称全量 PASS。
3. G4 要求的 A=50K、B/C=1K、pool1/4、window1/2/50 完整矩阵，以及 G6 有效持续 600 秒长稳，当前 Windows 环境未完成。已完成的是 4 活动 scaled Fair smoke，不替代 50K 门禁。
4. 精确的“旧同步 debug/JSONL”模拟器版本不在当前工作树；SIM-5 已完成同硬件 fast-audit/full-audit 对照，但没有伪造旧版本基线。

生产安全结论：继续保持 `reliable` 为默认，`dispatch_mode=legacy` 为默认回滚路径；不要在当前 Windows 环境启用 OCC 或据此宣称生产吞吐。完成 WSL AlmaLinux-8/ext4 隔离环境中的 G0/G3/G4/G6 后，才能重新判定 S5 前置条件。

## 1. 版本、范围与工作区保护

### 1.1 源码冻结信息

- 起点 `HEAD`：`fd07c5b2c8c2b03dd71f3a3fa3754c83ec93e34f`
- `origin/main`：`fd07c5b2c8c2b03dd71f3a3fa3754c83ec93e34f`
- 未创建 commit，未 push。
- 当前为 dirty worktree；本报告使用“HEAD + 下列文件 SHA256”作为未提交工作区 manifest。G3 运行的 `git_commit` 均记录了上述 HEAD；G4 harness 兼容性修复发生在 G3 运行之后，因此 G3 不被包装成“当前完整 workspace manifest 的全矩阵 PASS”。

实现/验收相关文件的最终 SHA256：

```text
595d262e6b18d0cd9ab317a70f68409956d0b998d37b18f8fc9e5f570204e7a8  backend/main.py
bde00f1981f8f3678f199e2b599c8abc1d8eee5dc0ad16fe82340a76d31baac4  backend/services/bounded_dispatch_service.py
f11153f6c5468c160422771b489234ce743f0564dfdd7bb5992f18edbae665ce  backend/services/realtime_campaign_service.py
ba9310f366bbb8b8321c6b7c31379dbd26c42fff9f67a84b8e24a656baa6d1e0  backend/services/smpp_gateway.py
636517a41a3708c78def7acf70f0e5eb2ff456d473622af736c5286eeac9c96d  backend/tests/test_startup_recovery.py
b44cfa88a5310571e239eb2d615e4a4a78a025d5f32d1df4be38f1398f79c122  backend/tests/test_integrated_s2_s3_s4_controls.py
16dddaaa7f20b247ee9e751d996950b4b5da5cda407d67fd1843e147a49ed5e8  backend/tools/local_e2e_acceptance.py
1d9ebfdb38c8f036c1375d4df74d82272bf2d7a4810b0acdfff1c0c0f297212  backend/tools/local_e2e_scenarios.json
81bb680539c06ffaac55e216ecaa263b6996c48de6cf043bda376a26990e6c80  backend/tools/multi_campaign_fair_acceptance.py
117f5c0ba1f5b2a45a96f439fc4a765902a4e67ba3f73c5e2dc3b8c2ccff48f5  backend/tools/smsc_simulator.py
d496d5ef16c26e1d8faadc54377e6f526bf89374ce39bd774ae981f80515cba0  backend/tools/benchmark_smsc_simulator.py
4c985615dc0581163b92504ee732e40c883c096b1b1d7d0a9b9176404686532e  Docs/Marketing_Copilot_Integrated_Architecture_Convergence_Plan.md
```

### 1.2 用户工作区保护

- 未使用 `git reset --hard`、`git checkout`、`git clean`、删除历史证据或删除运行数据库/WAL/Journal/日志。
- `frontend/package-lock.json` 的既有 dirty 状态保留，未将其纳入本次实现文件清单。
- `.local-e2e-runs/`、历史 `.s*` 证据、`cache/`、`memory`、用户文档、数据库及临时压测产物均保留。
- 未接触真实 SMSC、真实号码、真实任务数据或生产数据库；所有 E2E 使用 loopback、合成号码和独立运行目录。

## 2. 实施内容

### 2.1 M0：高性能 SMSC 模拟器与 TPS 解耦

`backend/tools/smsc_simulator.py` 现在有明确的 `fast`/`fault` 模式：

- Fast 仍走真实 TCP、SMPP bind、PDU 解析、sequence、`submit_sm_resp` 和 message-id，不使用直接回调成功计数。
- `simulator_tps=0` 表示模拟器不做人为限速；`route_tps` 只注入 Marketing Copilot route-shared pacing，不能乘以 pool size。
- Fast 默认零人工 response delay、DLR disabled、无每 PDU 同步 stdout/正文/手机号输出。
- 每连接有界 submit handler、受控 writer/写锁和 max-inflight；审计使用有界队列/批量写入。overflow、write error 或计数缺口会标记 `SIMULATOR_AUDIT_INCOMPLETE`/`TEST_HARNESS_FAIL`，不会静默丢证据。
- 保留 fault 模式的 reject、drop-response、disconnect、partial submit、DLR delay 等注入。
- 输出 received/response/error/bind/inflight/max-inflight、server response latency P50/P95/P99、审计 backlog/overflow 等指标。

`local_e2e_acceptance.py` 新参数：

```text
--simulator-mode fast|fault
--simulator-tps <只影响模拟器>
--simulator-response-delay <只影响模拟器>
--route-tps <只影响 Marketing Copilot route>
--simulator-audit fast|full|off
```

旧 `--smsc-tps` 保留兼容，但新参数与旧参数同时出现会 fail-fast；报告写出 `tps_scope`，不会把 simulator TPS=0 错写成 route TPS=0。

### 2.2 S2：Route/Slot Fair、PDU Window 与 multipart

- ReadyPool/route admission 使用 `(campaign_type,campaign_id,resolved_route_id)` 的 parts-based DRR，而不是按 task 数伪公平。
- socket 层保留 per-connection PDU window hard limit；credit acquisition/release 可对账。
- multipart continuation 每次最多让出一个 PDU 的 socket 写机会，保留 connection generation、sequence ledger 和 unknown/partial 保守语义。
- `submit_sm_resp` 到达后立即释放对应 PDU credit，避免 window=2 等待整个 multipart task 完成。
- 旧 generation、晚到 response、写出后不明 attempt 不会因为 route/config 变化而自动整条重发。

### 2.3 S3：文件/内存准入与 worker 边界

- XLSX 解析增加文件 bytes、估算内存、expansion factor 和 bounded admission；超限返回 retryable/resource-limit，而不是无界 `read_excel`。
- 保留文件 lease、chunk checkpoint、hash/dedupe、authoritative task insert 和 bounded worker pool。
- G1 sender-only 结果显示 pool1 与 pool4 近似同速，证明当前瓶颈在 DB/result/调度链路而非模拟器；没有用增加 worker 数伪造线性扩容。

### 2.4 S4：启动恢复与故障安全

- startup 顺序调整为：唯一 Owner → SMS write buffer replay/flush barrier → scheduler/task worker 等 producer。
- replay barrier 超时或不健康时 fail-closed，不允许新发送先于旧结果恢复。
- 保留 Journal/Write Buffer、Fast checkpoint、result/dispatch credit、attempt/part state machine、unknown/partial quarantine 和 DLR 状态机。
- Windows runner 使用 process group 的 bounded CTRL_BREAK 优雅停机，只有超时才 force kill；sender-only seed 使用 DuckDB 当前 `CURRENT_DATE`，避免本地时区与 UTC 日期错配导致误取消任务。

### 2.5 新增交付物

- `Docs/Marketing_Copilot_Integrated_Architecture_Convergence_Plan.md`
- `backend/tools/benchmark_smsc_simulator.py`
- `backend/tests/test_integrated_s2_s3_s4_controls.py`
- `backend/tests/test_local_e2e_acceptance_tools.py`（工作区既有文件保留）
- 本主报告：`Docs/Marketing_Copilot_S2_S3_S4_Integrated_Implementation_and_Acceptance.md`

## 3. 收敛后的资源/锁/credit 所有权

```text
file/chunk lease
      │ authoritative DB transaction
      ▼
Claim → ReadyPool(parts DRR) → Prepare(bytes/parts credit)
      │                         │
      │                         └─ result/journal credit
      ▼
Route-shared TPS → Slot/PDU window → real submit_sm socket write
      ▼
submit_sm_resp / disconnect ambiguity
      ▼
Journal/Write Buffer → single DuckDB Owner/OCC or serialized tx
      ▼
terminal accepted|failed|unknown|partial → release task/part/byte/result credits
```

| 资源 | 单位 | reserve/owner | release/异常 |
|---|---|---|---|
| File/chunk | file、chunk、bytes | realtime/batch ingestion lease + authoritative DB | checkpoint/lease fence；过期 worker 不能回写 |
| Ready/claim | task、parts、bytes | Fair ReadyPool / task worker | 未准入可安全回退；已 claim 由 token/generation 管理 |
| Route pacing | submit_sm parts/s | route-shared limiter | 不随 pool size 放大；跨 route 隔离 |
| Socket window | PDU | connection slot + generation | response 立即释放；写出后不明转 unknown/保守隔离 |
| Journal/result | event bytes/items | bounded writer/flush barrier | overflow/failure fail-closed；幂等 replay |
| DuckDB | transaction | one process owner/cursor + coordinator | serialized 默认；OCC 只显式 opt-in，冲突重跑完整短事务 |
| DLR | receipt state | fault/required 模式独立 | Fast/performance 模式完全关闭，不把 accepted 当 delivered |

不在 DB transaction 内等待 socket、长解析或无界队列；不持调度锁进行网络/DB 阻塞；单 Campaign/Route 拥塞不能取消健康活动，但 DB/Journal/磁盘共同故障仍是全局安全阻断。

## 4. 全链路瓶颈审计

### 4.1 模拟器瓶颈已解除并独立测量

原风险是每 PDU 同步打印/同步 JSONL、连接内串行 `await handle_submit`、人工 TPS/response sleep 把被测业务限制在模拟器处理能力内。当前 30 秒独立压测显示：

- pool=4、window=50、fast、simulator_tps=0、DLR off：275,204 submit/response，客户端 9,165.72/s；client RTT P50/P95/P99=21.611/25.176/28.310ms。
- 模拟器 received/response_sent=275,204/275,204；server parse-to-response-write P50/P95/P99=0.039/0.070/0.247ms；event-loop lag P50/P95/P99=1.552/5.409/6.400ms；bind=4、max_inflight=200、error=0、DLR=0、audit backlog=0、overflow=0。
- Route 目标 R=200 parts/s 时，模拟器独立能力约为 `45.8 × R`，满足 SIM-6 的 2×余量门槛。

### 4.2 应用端瓶颈

在 G1 sender-only 与 G3 serialized Full E2E 中，模拟器仍保持 fast/0 throttle/audit VERIFIED，而业务真实 submit/accepted 约 5–7 parts/s；pool1 与 pool4 sender-only 约 7.362/7.318 parts/s，说明当前 Windows 应用观测瓶颈主要在 ingestion、DuckDB transaction、Journal/result 和控制面，不是 SMSC 模拟器。

### 4.3 OCC native runtime 阻断

补充环境影响：本轮执行期间 Windows C 盘曾出现可用空间耗尽，现已完成清理；本次终结核验时 C 盘可用空间约 `81.26 GB / 200 GB`。磁盘空间耗尽通常不会损坏磁盘硬件，但会导致 `%TEMP%`、DuckDB/WAL、pytest 临时目录、pagefile 或构建缓存写入失败、截断、超时及进程异常退出。因此，清理前发生的 IO/临时目录类错误不能作为业务代码回归结论；清理后的完整 G0/G3/G4/G6 仍需在目标 WSL/ext4 环境重跑。该因素也不能单独解释或掩盖已记录的 Windows `0xC0000374` native heap corruption。

G3 OCC/fast：

- r1：1000/1000 accepted，PASS。
- r2：API `0xC0000374` 退出，最终仅 507 accepted、508 submit。
- r3：API `0xC0000374` 退出，最终仅 532 accepted、534 submit。

清理 C 盘后的失败重跑：`g3-occ-fast-rerun-after-disk-clean-r1.json` 发生 `0xC0000005` access violation，最终 124 accepted/128 submit，SMSC audit VERIFIED；第二轮 `g3-occ-fast-rerun-after-disk-clean-r2.json` 为 1000/1000 accepted、1000 submit/response、audit VERIFIED。即使磁盘已恢复，OCC/Fast 仍表现出 native runtime 非确定性，不能作为稳定性 PASS。

G3 OCC/reliable：r1/r3 PASS；r2 live wait 超过 300 秒，但停机后 DB 对账为 1000 accepted、1000 submit/response。该轮仍按门禁 FAIL/timeout，不用 post-shutdown PASS 覆盖 runtime 超时。

清理 C 盘后的 `g3-occ-reliable-rerun-after-disk-clean-r1.json` 为 PASS：1000/1000 accepted、1000 submit/response、无 duplicate/misdelivery/unknown retry、audit VERIFIED。

诊断快照显示 API 进程以 Windows heap corruption 退出，模拟器没有审计缺口；这属于当前 native runtime/环境阻断，不能靠扩大 timeout、隐藏 unknown 或增加 retry 解决。OCC 保持 opt-in，不进入生产默认。

## 5. 统一验收矩阵

状态含义：`PASS` 是该门禁按自身条件通过；`NOT_RUN` 是没有足够证据；`ENV_BLOCKED` 是当前环境不能安全执行/解释；未把历史旧 SHA 拼入当前 PASS。

### 5.1 G-SIM / M0

| 门禁 | 状态 | 证据与结果 |
|---|---|---|
| SIM-1 独立能力 | PASS | `.tmp/sim-final-30s-r2.json`；30s、pool4/window50、275,204 submit/response、9,165.72/s、audit VERIFIED。 |
| SIM-2 限速对照 | PASS | `.tmp/sim-final-cap50-fast-r1.json`≈50.01/s；`.tmp/sim-final-cap200-fast-r1.json`≈199.348/s；均只限制 simulator。 |
| SIM-3 协议一致性 | PASS | 多 bind、多 PDU in-flight；客户端和模拟器计数一致，无 unknown response；30s audit enqueued/written=550,418/550,418。 |
| SIM-4 编码/兼容 | PASS | `.tmp/sim-final-unicode-r1.json`、`sim-final-flash-r1.json`、`sim-final-multipart-r1.json`；均 120/120、DLR=0、audit VERIFIED；fault/disconnect/partial 另有 G5 证据。 |
| SIM-5 日志成本 A/B | PASS（当前替代 A/B） | `.tmp/sim-ab-fast-r1.json`：84,705/10s=8,448.833/s；`.tmp/sim-ab-full-r1.json`：79,759/10s=7,955.937/s；full audit 相对 fast 约低 5.84%，两者均无 overflow、audit VERIFIED。精确旧 debug/sync-JSONL 版本不在工作树，未伪造旧版本结论。 |
| SIM-6 不限制 Route | PASS | 9,165.72/s ÷ 200/s≈45.8×。 |
| G-SIM 总门禁 | ENV_BLOCKED | 模拟器自身能力已 PASS；精确旧实现 A/B 与目标 Linux/ext4 复测仍缺，故不把全部 M0 历史语义扩展为生产性能结论。 |

### 5.2 G0

| 项目 | 状态 | 证据 |
|---|---|---|
| Frontend build | PASS | `frontend/` 执行 `npm run build` 成功，Vite v8.1.3、1794 modules、`frontend/dist/index.html` 生成；只有 PURE annotation/chunk size warnings。 |
| 最终定向后端回归 | PASS | 39 passed, 4 warnings：OCC coordinator、integrated controls、PDU fairness、async/encoding、local harness、startup recovery。 |
| 标准全后端 pytest | ENV_BLOCKED | 清理 C 盘后使用隔离 basetemp 重跑：`343 passed, 3 skipped, 3 failed`。失败稳定复现于 `test_backup_copy_is_readable_after_owner_checkpoint`（Windows `WinError 32` 文件占用）、`test_no_dlr_successes_use_one_set_based_update_without_message_ids`（34 个窗口，阈值≤30）和 `test_no_dlr_secondary_state_updates_are_set_based_and_fenced`（42 个窗口，阈值<30）；详见 `.tmp/pytest-full-rerun-after-disk-clean.log`。 |
| G0 总门禁 | ENV_BLOCKED | Frontend 与目标回归通过，但没有可复核的最终全量 pytest exit=0。 |

### 5.3 G1

| 子项 | 状态 | 结果 |
|---|---|---|
| ingest-only 10×100 | PASS | `.tmp/g1-ingest-1k-r1.json`：10 files、1000 input rows、1000 queued tasks、10 files complete、socket submit=0、audit VERIFIED。 |
| sender-only pool1 | PASS | `.tmp/g1-sender-p1-r3.json`：1000/1000 task accepted、1000 submit/response、sim observed≈7.362/s、无 duplicate/misdelivery/unknown retry。 |
| sender-only pool4 | PASS | `.tmp/g1-sender-p4-r1.json`：1000/1000 task accepted、1000 submit/response、sim observed≈7.318/s、无 duplicate/misdelivery/unknown retry。 |
| ASCII/UCS2/Flash/Multipart | PASS | SIM-4 与 `.tmp/e2e-multipart-fast-r6.json`；multipart 5 PDU、5/5 response、1 task accepted，PDU/continuation 证据完整。 |
| window 1/2/50 | PASS（unit/E2E 组合证据） | `backend/tests/test_s2_pdu_window_fairness.py` 与 multipart-fast；每 PDU acquired/released 守恒。 |
| G1 总门禁 | PASS | 性能场景 fast、simulator_tps=0、route_tps=200、DLR disabled；故障场景独立保留在 G5。 |

### 5.4 G3

固定参数：Fair、10 files×100=1K、pool4、window50、route=200、simulator=0、fast、response delay=0、DLR disabled、task_workers=8、ingest_workers=2、copy workers=2。

| 组合 | 结果 | 轮次证据 |
|---|---|---|
| serialized/reliable | PASS 3/3 | `.tmp/g3-serialized-reliable-r1.json`、`r2.json`、`r3.json`；每轮 1000 task/submit/response/accepted，audit VERIFIED。 |
| serialized/fast | PASS 3/3 | `.tmp/g3-serialized-fast-r1.json`、`r2.json`、`r3.json`；每轮 1000 task/submit/response/accepted，audit VERIFIED。 |
| occ/reliable | NOT PASS（历史 2/3；清理后重跑 1/1） | 历史 r2 曾 300s live wait timeout；清理后 `.tmp/g3-occ-reliable-rerun-after-disk-clean-r1.json` PASS，1000 accepted、1000 submit/response。仍不将一次重跑覆盖历史非确定性。 |
| occ/fast | ENV_BLOCKED（清理后 1 PASS/1 FAIL） | `.tmp/g3-occ-fast-rerun-after-disk-clean-r1.json` 为 `0xC0000005`，124 accepted/128 submit；r2 为 1000/1000 PASS。与历史 `0xC0000374` 一起说明 Windows native runtime 非确定性。 |
| G3 总门禁 | ENV_BLOCKED | 清理磁盘后仍出现 OCC/Fast native access violation 与不稳定通过，不能安全宣称 OCC 子矩阵稳定；也不能拼接历史 Linux/ext4 或旧 SHA 的 3/3。 |

### 5.5 G4

现有 runner 已补齐显式 simulator/route/dispatch/send/DB 参数和 fast audit digest 对账。可复核 scaled smoke：

| 配置 | 状态 | 结果 |
|---|---|---|
| 4 activities、large=40/small=4、pool1/window10 | PASS（smoke） | `.tmp/g4-fairness-smoke-r3.json`；88/88 tasks、88/88 submit/response；batch-A/B、realtime-A/B 均 served；max first-service wait≈8,787.8ms；Fairness telemetry PASS。 |
| 同规模、pool4/window50 | PASS（smoke） | `.tmp/g4-fairness-smoke-p4w50-r1.json`；88/88 tasks、88/88 submit/response；max first-service wait≈5,873.6ms；所有活动 served。 |
| A=50K、B/C=1K 完整矩阵 | NOT_RUN / ENV_BLOCKED | 当前 Windows 环境没有安全、可解释的 50K/多矩阵执行条件；scaled smoke 不替代要求。A pause/cancel、window1/2/50、long multipart continuation 的完整 G4 组合也未宣称通过。 |
| G4 总门禁 | ENV_BLOCKED | 只有 scaled fairness/socket 证据，缺少指定规模全矩阵。 |

### 5.6 G5

| 故障 | 状态 | 关键结果 |
|---|---|---|
| reject | PASS | `.tmp/e2e-fault-reject-r1.json`；5 failed、5 rejected、无 retry/duplicate/misdelivery、credit 5/5。 |
| drop response | PASS | `.tmp/e2e-fault-drop-response-r1.json`；3 unknown、SMSC received=3/response=0、无自动重发、credit 3/3。 |
| partial multipart | PASS | `.tmp/e2e-fault-partial-r1.json`；1 partial、4 PDU（1 success/3 reject）、无重复，audit VERIFIED。 |
| disconnect before response | PASS | `.tmp/e2e-fault-disconnect-r2.json`；3 submit、SMSC response=2、2 accepted+1 unknown、无 unknown retry。 |
| DLR delay | PASS | `.tmp/e2e-fault-dlr-delay-r1.json`；5 delivered、5 DLR、DLR violation=false。 |
| restart/replay | PASS | `.tmp/e2e-restart-r1.json`；5 accepted，restart 前后 submit 数不变。 |
| G5 总门禁 | PASS（本地 fault/recovery 范围） | 未将 DLR 压测混入性能数字；保留必要 DLR 状态机回归。DB/Journal outage 与 600s long-soak 仍不等于本表已覆盖。 |

### 5.7 G6 与 Legacy

- G6 600 秒有效多 Campaign 长稳：`NOT_RUN / ENV_BLOCKED`。G1 pool1/pool4 sender-only 只能说明当前短跑 worker scaling 没有收益，不能替代 600 秒。
- Legacy：默认配置、单元回归和兼容 CLI 保留；本轮没有把 legacy 与 Fair 混入性能结果，也没有宣称完整生产 legacy acceptance 已通过。

## 6. 正确性、安全与恢复不变量

已验证范围内：

- Full/sender/fault runs 的 task key、文件/chunk、submit/response、simulator audit 可对账；成功路径无 duplicate task、misdelivery、unknown retry violation。
- `submit_sm_resp=0` 仅为 accepted；required DLR 才变成 delivered；Fast 性能模式 DLR generated=0。
- disconnect/drop-response/partial 不会把可能已经写出的 attempt 整条自动重发；unknown/partial 保守落盘。
- multipart continuation 的 PDU credits、socket writes、generation ledger 可观测；成功 multipart 5/5 response，PDU limit 没有被突破。
- 启动 replay barrier 在 scheduler/task worker 前执行；Journal/Write Buffer 不健康时 fail-closed。
- 模拟器 audit overflow/write failure 不会被吞掉；测试 harness 不会仅凭 socket 收包数伪造业务 accepted。

## 7. 残余风险与下一步

1. 在全新 WSL `AlmaLinux-8` ext4 目录中，以当前最终应用源码和最终 harness manifest 重跑：全后端 pytest、OCC coordinator、G3 受影响矩阵及必要的完整四组合。
2. 在同一 WSL/ext4 环境完成 G4 A=50K、B/C=1K 的 pool/window 矩阵、pause/cancel、long multipart，并保存 per-campaign socket writes、eligible wait P50/P95/max、credit 和 generation 证据。
3. 完成 G6 有效 600 秒多活动进展，记录 CPU/RSS、DB/Journal/PDU 限制与最差等待；不能以 worker 数或进程存活代替。
4. 在目标环境确认 DuckDB/Python 版本组合后，重新评估 OCC；在此之前只允许显式实验，不切换默认 serialized。
5. SLES12SP5 离线部署包、真实 SMSC、真实号码、生产数据库与生产切换不属于本任务授权范围，仍需独立 S5/部署验收。

最终交付不包含 commit/push；本报告是本任务唯一主报告，历史任务书和运行证据均未被删除或覆盖。
