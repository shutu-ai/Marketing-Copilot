# Marketing Copilot — S2 Route/Slot Socket Fairness & PDU Window Implementation and Acceptance

**任务 ID**: `MC-SCHED-S2-SOCKET-FAIRNESS-20261015`
**最终门禁**: **`S2_NO_GO`**
**基线核对**: 本地 `HEAD == origin/main == 319878bfbc9fcd2f7a083be15d22e9a45a78825e`。实施前工作区已存在 `frontend/package-lock.json` 修改和大量未跟踪用户/历史资料；本轮未删除、未覆盖这些内容，也未 commit/push。

## 1. 结论摘要

S2 核心发送安全和浅 handoff 的最小实现已落地，并通过确定性测试、最新 Fair loopback smoke 和 `serialized/reliable` 三轮 1K 验收；但 **G3 四组合 ×3 未完成**：`serialized/fast` 首轮在 300 秒超时时只完成 900/1000，按任务规则立即阻断后续 8 轮和 G4。此外，当前最终源码在 G3 证据之后又增加了仅审计状态字段，严格按“当前最终源码 SHA 证据”要求，G3 也需要重新冻结重跑。

因此不能宣称 `socket_fairness=VERIFIED`、`pdu_window_hard_limit=VERIFIED` 或 `S2_READY`。生产切换、Fair 生产启用、S3/S4 均保持 **NO_GO / NOT_VERIFIED**。

## 2. 基线所有权与锁/路径图

### Fair 发送路径

1. `_fair_claim_once()`：观察 result buffer -> `sms_task_service.fair_dispatch_candidates()` -> `gateway.resolve_dispatch_route()` -> `ReadyPool.reserve()` -> DuckDB claim transaction -> `ReadyPool.publish()`。
2. SMS worker `_process_fair_task()`：schedule/stop/backpressure gate -> `_make_task_parts()` 与 reservation cost 核对 -> `gateway.try_send_async()`。
3. `SmppGateway.try_send_async()`：校验 reservation、route id、route revision -> `_fair_pool()` 选择 revision 隔离的物理 Slot -> 按 idle/DRR 排序 -> `_SmppConnection.try_send_async()`。
4. `_SmppConnection.try_send_async()`：Fair 模式 acquire 一个 handoff permit 后 `put_nowait`；失败立即返回 False，worker 不阻塞。
5. `_async_sender_loop()`：转移 permit/active handoff -> campaign invalidation fence -> `_send_windowed_async()`。
6. `_send_windowed_async()`：每 PDU 前 stop/TPS/safety/PDU credit -> 短 `_send_lock` 写 socket -> bind `(generation, sequence)` credit -> response 到达后按 sequence 恰好一次 release。

### 锁顺序与不变量

- ReadyPool condition 内不做 DB、gateway、网络、fsync 或阻塞等待。
- Fair handoff permit 只在 Slot sender 拿到 task 后释放；每 Slot 最多一个未启动/正在启动的 descriptor。
- Response path 的 `_condition` 内可进入独立 `_pdu_window_lock` 结算 credit；不反向在 PDU lock 内获取 response condition。
- Socket write 只持 `_send_lock` 短锁；TPS 等待不持共享 TPS lock。
- Connection generation 断开/重连前 retire 当前代 PDU credit；旧代迟到 response 因 sequence/context 已清理而无法释放新代 credit。
- 写入结果不明确的异常保留 unbound credit，直到该代被隔离关闭；不把 timeout 当作“SMSC 已停止处理”的证据。

### 旧实现确认的竞态

旧 `_send_windowed_async()` 在 task 入口一次 acquire task-wide `_window`，由 `_take_async_completion_locked()`、timeout reaper 或 disconnect 按 task release。迟到 response 在 reaper/失败路径清理 sequence 后只会落入 early bucket，不能再触发 callback；本地 permit 已由 timeout 路径释放。该模型避免 permit 双释放，但窗口单位是 task 而不是 PDU，长 multipart 占用窗口的方式与 S2 合同不一致。新 Fair ledger 改为 per-PDU reserve/bind/release。

## 3. 实施范围

### `backend/services/smpp_gateway.py`

- 新增 Fair per-Slot shallow handoff permit：最多一个未启动 descriptor；legacy `send_async()`/同步路径单位与行为保持不变。
- 新增 per-connection PDU ledger：
  - `unbound reservation`：write 前 1 credit；
  - `(connection_generation, sequence)`：成功写出后 bind；
  - response/completion/reaper/exception 恰好一次 release；
  - ambiguous write 不提前 release，generation disconnect/retire 后清算；
  - retired generation 中的 `_pdu_reserve()` fail-closed。
- `try_send_async()` 每 part 循环前重新检查 campaign invalidation；写后异常按 submitted/ambiguous 分类，不自动整条重发。
- 新增 `SmppGateway._fair_pool()`：route revision 隔离、不倍增 TPS、不在 task worker 中 connect。
- Fair Slot 选择：优先 idle Slot，再按 campaign group 已服务计数 DRR，再 round-robin；route 固定为 reservation group route。
- status 增加 `window_unit=submit_sm_pdu`、`pdu_window`、`fair_active_handoffs` 和全局审计字段；global `dispatch_fairness=IMPLEMENTED_NOT_VERIFIED`、`socket_fairness=NOT_VERIFIED`、`pdu_window_hard_limit=NOT_VERIFIED`。
- 保留 Fair 对 direct send、legacy async、RabbitMQ/direct 入口的拒绝；默认 dispatch mode 仍为 legacy。

### `backend/tools/local_e2e_acceptance.py`

- 新增显式 `--dispatch-mode legacy|fair` 并写入 child env 与报告 `dispatch_mode_effective`。
- 新增显式 `--db-concurrency serialized|occ` 并写入 child env 与报告。
- 这是隔离验收 harness 的可观测性/配置修复，不改变产品路径。

### `backend/tests/test_s2_pdu_window_fairness.py`

新增 4 个确定性 oracle：

- PDU hard window 和 sequence 恰好一次 release。
- ambiguous write + generation retire fail-closed。
- Fair Slot 只接受一个未启动 handoff。
- Gateway 在同 route idle Slot 上做浅选择，不把未来 Slot 深队列 reservation 交给 busy descriptor。

## 4. 测试与门禁

### G0 / G1

| 项目 | 结果 | 证据 |
|---|---|---|
| WSL AlmaLinux-8 / Python 3.11 ext4 basetemp | EXECUTED | `.s2-wsl-venv`（验收后已清理），basetemp `/tmp/mc-s2-pytest` |
| S2 + S1 deterministic | PASS，45/45 | `test_s2_pdu_window_fairness.py` + `test_s1_bounded_dispatch.py` |
| 全量 backend pytest（诊断状态变更前的当前行为源码） | 342 passed，2 skipped；`test_occ_non_conflicting_transactions_overlap` 子进程 15 秒超时 | 随后单独复跑同用例 PASS |
| OCC timeout targeted retry | PASS | 1/1 |
| 最新审计状态源码 Fair quick loopback | PASS | `.s2-evidence-20261015/final-source-fair-smoke.json` |
| Legacy quick loopback control | PASS | `/tmp/mc-s2-quick-legacy.json` |
| Frontend `npm run build` | PASS | final source build succeeded |
| Formal G1 ingest-only / sender-only matrix | **NOT_VERIFIED** | 只用 full G3/smoke 覆盖，未单独执行全部规定矩阵 |

### G3：四组合 ×3 硬门禁

统一场景：`stress-1k`，10 files × 100 events，Fair explicit，pool4，window50，route TPS200，task workers8，ingest2，copy2，loopback only。
运行根：`/tmp/mc-s2-g3-final/<name>`。

| 组合 | Run | 状态 | 业务/socket 对账 |
|---|---:|---|---|
| serialized/reliable | 1 | **PASS** | 1000 created / 1000 accepted / 1000 submit_sm / 1000 success resp；0 unknown |
| serialized/reliable | 2 | **PASS** | 1000 created / 1000 accepted / 1000 submit_sm / 1000 success resp；0 unknown |
| serialized/reliable | 3 | **PASS** | 1000 created / 1000 accepted / 1000 submit_sm / 1000 success resp；0 unknown |
| serialized/fast | 1 | **FAIL** | 1000 created / 900 accepted / 900 submit_sm / 900 success resp；300 秒超时时 100 queued；0 unknown/partial |
| serialized/fast | 2 | **NOT_EXECUTED** | G3 FAIL 阻断 |
| serialized/fast | 3 | **NOT_EXECUTED** | G3 FAIL 阻断 |
| occ/reliable | 1–3 | **NOT_EXECUTED** | G3 FAIL 阻断 |
| occ/fast | 1–3 | **NOT_EXECUTED** | G3 FAIL 阻断 |

`serialized/fast-r1` 观察：无重复 submit、无 unknown retry、无丢 submit mapping；但 300 秒门禁超时本身就是 FAIL。最终 watchdog 显示 Fair dispatcher 仍有 reserved/result_pending work，task backpressure depth 约 118，未发现 checkpoint fail-closed。不要将该 FAIL 解释为 PASS，也不能用延长 timeout 掩盖。

重要证据状态：上表三轮 reliable 在当前 SHA 之前、仅新增 global audit 状态字段前执行；按“当前最终源码”严格口径需要重新冻结并重跑。它只支持机制未回归的中间结论，不支持当前 SHA 的 G3 PASS。

### G4 socket 公平与隔离

**NOT_VERIFIED / NOT_EXECUTED**。G3 未完成，未执行 50K backlog、B/C oldest eligible age、P50/P95、最长零进展区间、pause/cancel、跨 route 隔离、单 Slot 慢 ACK/断线、heavy multipart、window1/2 等真实 socket oracle。

当前实现也不是完整 S2 终态 arbiter：

- 有 per-Slot shallow handoff 和 campaign-group DRR；
- 但单个 multipart 在一个 Slot sender 内仍非抢占，`parts > window` 会通过等待 PDU credit继续，未实现受控 continuation/yield；
- 尚未实现持续 oldest eligible age、P50/P95 等待、最长零 socket-write 进展的独立 fairness watchdog。

## 5. 审计诊断

| 诊断 | 当前值 | 说明 |
|---|---|---|
| `dispatch_fairness` | `IMPLEMENTED_NOT_VERIFIED` | S1 ReadyPool DRR + S2 route/slot 选择，但缺 G4 socket 份额证据 |
| `socket_fairness` | `NOT_VERIFIED` | 保留 ReadyPool 原字段，并以 gateway status 重复暴露 |
| `pdu_window_hard_limit` | `NOT_VERIFIED` | per-Slot ledger 实现并有单元证据，但未通过当前完整 G3/G4 |
| `result_pipeline_hard_bytes` | `VERIFIED` | Fair G3/smoke 观察到 `result_pipeline_hard_bytes=VERIFIED`，S1.1 安全能力保留 |

## 6. Source manifest

| 文件 | SHA256 |
|---|---|
| `backend/services/smpp_gateway.py` | `AE92DFD7B410EA3FAD9B2987D05033FCBC7E5A8730E2F9F66D470BFDF6FC6E5B` |
| `backend/tools/local_e2e_acceptance.py` | `5164ABBCB8E4DDD588ABCED1A9DF6BA7888E5B454D763EDCEC9BAD2EBD63DEC8` |
| `backend/tests/test_s2_pdu_window_fairness.py` | `7C42B71210BA4321ADD740DA8953FA2A2080BFC7CE78BE3295D48D6014A693FF` |
| `Docs/Marketing_Copilot_S2_Route_Slot_Socket_Fairness_Implementation_and_Acceptance.md` | 本报告 |

用户既有脏文件 `frontend/package-lock.json` 未作为 S2 变更处理，保持原样。

## 7. Raw evidence

已复制到本工作区：

- `.s2-evidence-20261015/g3/serialized-reliable-r{1,2,3}.json`：三轮 PASS 摘要。
- `.s2-evidence-20261015/g3/serialized-fast-r1.json`：FAIL 摘要。
- `.s2-evidence-20261015/g3/<run>/smsc.events.jsonl`：真实 loopback `submit_sm`/response 时序。
- `.s2-evidence-20261015/final-source-fair-smoke.json`：最新源码 Fair quick PASS。

WSL ext4 原始目录保留在验收机器当前 `/tmp` 下（生命周期受重启影响）：`/tmp/mc-s2-g3-final/*`。不要用旧 SHA 的 PASS 顶替当前源码复核。

Evidence SHA256:

- `serialized-fast-r1.json`: `0647C4318B2212DF7EE81E68A2D5BD709D7C80D6A08DF67AC2ACA177FEFA35C9`
- `serialized-reliable-r1.json`: `E4602D4BD78014321C263B2AF2E696A8053E19E423F7AACF06E28E6281009120`
- `serialized-reliable-r2.json`: `23B6D846B90EA19040B4A80FFB39CED3D607CB7CD127F58F481FD1161A9355BE`
- `serialized-reliable-r3.json`: `D34AF2270134723E0C4E7B490FFFB37506A07266B0DE25339F0090CA06930801`

## 8. 未闭工作

1. 修正/解释 `serialized/fast` 300 秒内 100 任务未 claim/send 的吞吐瓶颈；不得用加大 timeout、worker、TPS 或 queue 掩盖。
2. 冻结当前最终 SHA，重跑完整 G3 四组合 ×3，每轮新进程/数据目录/port/evidence。
3. 补齐 G1 ingest-only 与 sender-only 显式矩阵。
4. 实现/验证 heavy multipart continuation 或证明其非抢占时间可接受。
5. 实现并采集 G4 所需 oldest eligible age、P50/P95、最长零实际 socket-write 进展诊断。
6. 执行 G4 同 route/异 route、pool1/4、window1/2/50、single/multipart、慢 ACK、断线、pause/cancel、跨 route 健康隔离测试。
7. 全部守恒、Journal↔DB、attempt/part、result hard-bytes、legacy 回归和当前 SHA 复核。

## 9. 回滚流程

本轮均为未提交工作区变更。回滚 S2 时：

```powershell
git checkout -- backend/services/smpp_gateway.py backend/tools/local_e2e_acceptance.py
Remove-Item -LiteralPath 'backend/tests/test_s2_pdu_window_fairness.py','Docs/Marketing_Copilot_S2_Route_Slot_Socket_Fairness_Implementation_and_Acceptance.md' -Force
```

不要 reset/clean 工作区；不要删除用户既有 `frontend/package-lock.json`、运行数据、Journal、备份、日志或其他未跟踪资料。默认 `MC_CAMPAIGN_DISPATCH_MODE=legacy` 时未启用新 Fair handoff/PDU ledger。

## 10. 最终判定

**`S2_NO_GO`。**

未授予 S3 Worker/File/Batch/SFTP 扩容、S4 600 秒长稳/replay 重构、SLES12SP5、真实 SMSC 或生产切换验收。这些继续保持独立 `NOT_VERIFIED / NO_GO`。
