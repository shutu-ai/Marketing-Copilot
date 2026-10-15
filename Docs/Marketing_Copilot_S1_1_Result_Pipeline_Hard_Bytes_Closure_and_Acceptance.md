# Marketing Copilot S1.1 Result Pipeline Hard-Bytes Closure and Acceptance

## 结论

**S1_READY**。本次只关闭 S1.1 Result Journal / Write Buffer / Dispatch Reservation 的硬字节边界；未开展 S2 Route/Slot socket fairness、S3 Worker/文件扩容或 S4 长稳重构。

Fair 运行时诊断为：

```text
result_pipeline_hard_bytes=VERIFIED
socket_fairness=NOT_VERIFIED   # S2 保留项
```

外部真实 SMSC、SFTP、生产切换和 SLES12SP5 长稳仍是独立门禁，不由本报告宣称通过。

## 基线、源码与环境

| 项目 | 结果 |
|---|---|
| 基线 | `HEAD=origin/main=9aa6b27a29f07bccc64a19069ecf0fea8e1c8892` |
| 工作树 | dirty；保留既有 `frontend/package-lock.json`、运行产物和用户文件，未 reset/checkout/delete |
| Fair 源码快照 | `.s11-evidence-20261015/source.tar` |
| 快照 manifest SHA-256 | `c0f31d8c53a5a216193534523b9349a50ebeab2f12f65ba5bacc21d10be6bc7e` |
| Linux 验收环境 | WSL `AlmaLinux-8` / ext4，CPython 3.11.13，`/tmp/mc-s0-20261015-closure/venv` |
| 数据库并发 | `MC_DUCKDB_WRITE_CONCURRENCY=serialized`；保持 S0.1 单 DuckDB owner/cursor |
| 发送环境 | synthetic loopback SMSC；未连接真实号码/真实 SMSC |

## 分阶段实施记录

1. **A — 现状/失败复现**：冻结 `origin/main` 基线，沿 submit_sm/response/DLR → Journal → Write Buffer → DB → terminal 全链路盘点驻留对象和释放点。
2. **B — 硬字节合同**：先落地 `Ledger/Credit`、event framing 上限、Dispatch parent reservation、terminal/emergency 隔离和 Fair admission fence。
3. **C — durable replay/compact**：再替换全量历史 materialization 为流式 SQLite index，加入 CRC/protocol fence、bounded replay、atomic compact 和磁盘工作集上限。
4. **D — 异常闭环**：接入 fsync/queue/disk/DB/quarantine/rejected evidence 失败路径，补 Fast checkpoint 交接和空 Journal recovery 修复。
5. **E — 验收**：先跑 N01–N09 与 S0/S0.1 定向回归，再跑全量 backend、Fair loopback/低字节多 Campaign、frontend build；未提前进入 S2–S4。

## 旧阻断与修复

此前阻断是 Journal 只限制 request 数量、`load_state()` 全量构造历史对象、Write Buffer 的 `_pending/_retry_heap` 没有统一驻留字节预算，且 fsync 后到 DB terminal marker 之间没有可审计的 credit owner；因此只能报告 `NOT_VERIFIED`。

本次修复包括：

- 新增 `backend/services/result_capacity_service.py`：每个 event 取得带 owner/id/epoch/stage 的 credit；`64 * framed_utf8_bytes + 4096` 保守计费，child event 从 Dispatch Reservation 转移，terminal 使用独立 lane，所有 release 幂等。
- Journal append 在进入 bounded queue 前完成结构上限检查和实际 UTF-8 framing；fsync 成功前不交付 durable；失败/队列饱和写入有界 `.rejected.jsonl` 证据并 fail-closed。
- `load_state()` 改为逐行 CRC 校验、SQLite 临时索引和可迭代 Records，不把历史全量 materialize 到 Python；未知 durable kind、缺失 event_id、未知 terminal state 或超大历史行保留原 Journal 并阻断 Fair recovery。
- compaction 只保留 pending/quarantine，采用临时文件 + fsync + atomic replace + 目录 fsync；compact 输出和 SQLite index work file 受 disk budget 约束。
- Write Buffer replay/pending/retry heap 共用同一 credit；replay 每轮最多 64 条，队列满时转 bounded retry heap；空 Journal 不再错误地永久保持 recovering；quarantine/rejected evidence 会阻止新的 Fair admission。
- ReadyPool 在 claim 前预留完整的 part/final result envelope；send admission 检查 ledger、Journal、DB/disk backpressure；terminal marker 不能被普通 event 债务饿死。
- Fair Fast checkpoint 改为直接走 durable Write Buffer；Legacy 路径保留既有行为。

## 硬字节合同

```text
submit_sm / response / DLR
        │
        ▼
Dispatch Reservation (normal parent credit: part + 2 × final envelope)
        │ child credit / claim_token
        ▼
Journal append → bounded queue → in_writer batch → fsync
        │                         │ error: bounded rejected evidence + fence
        ▼                         │ success
durable_handoff ────────────────┘
        │
        ▼
Write Buffer pending / DB batch / retry heap / bounded replay
        │ DB idempotent commit
        ▼
terminal marker (separate terminal lane) → release child → release parent
```

| 边界 | Fair 硬限制 |
|---|---:|
| 单 event 最大 framed JSON | 256 KiB |
| `part_result` | 4 KiB |
| final/terminal event | 128 KiB；terminal marker 最多 250 IDs/record |
| 单字符串 / 节点 / 深度 | 64 KiB / 16,384 nodes / depth 16 |
| ledger normal 默认 | 256 MiB (`MC_RESULT_LIVE_BYTES`) |
| emergency 默认 | 32 MiB (`MC_RESULT_EMERGENCY_BYTES`)；至少容纳一个最大 event |
| terminal lane | `2 × charge(256 KiB)`，不占 normal/emergency |
| Journal durable file | 1 GiB (`MC_RESULT_JOURNAL_DISK_BYTES`) |
| compact/index work | 各自不超过 Journal disk budget；超限保留原 Journal 并阻断 |
| rejected evidence | 32 MiB；满时拒收/断连，不静默截断 |

`queue_capacity=100000` 仍是数量上限，但不是容量证明：Fair admission 先取得 credit，实际 framed bytes 再 resize，故 queue、WriteItem、retry/replay 和 framing scratch 不得绕过 ledger。`stage` 统计是审计视图（parent 与 child 可能同时显示），不能把各 stage 相加替代 `used_bytes`。

## N01–N09 验收追溯

| 门禁 | 证据 / 命令 | 结果 |
|---|---|---|
| N01 bytes 满而 queue 未满 | `test_pressure_rejects_without_loss_then_terminal_releases_credit`、`test_terminal_lane_does_not_consume_normal_dispatch_budget`；34,000,000-byte Fair E2E backlog peak 65、最终 0 | PASS |
| N02 可变 UTF-8、multipart、Fast batch、错误文本 | `test_aggregate_payload_rejected_before_event_encoding`、bounded framing；全量回归覆盖 multipart/Fast | PASS |
| N03 DB outage / pending / retry / replay | `test_buffer_retries_whole_failed_transaction_without_losing_result`、`test_db_commit_before_terminal_marker_replays_once`；replay 每轮 64、retry heap 受 credit 限制 | PASS |
| N04 fsync timeout/failure | `test_journal_queue_and_fsync_share_timeout`、S0/S0.1 reliability tests；失败保持 evidence，Fair admission 关闭 | PASS |
| N05 terminal debt / duplicate callback / double release | `test_fsync_handoff_keeps_credit_until_terminal`、terminal lane、`test_quarantine_groups_preserve_errors` | PASS |
| N06 历史 replay/compact/未知协议 | `test_result_journal_compaction_retains_only_replayable_state`、`test_unknown_event_and_terminal_state_block_fair_recovery`、CRC/index streaming | PASS |
| N07 stale epoch / claim / unknown / partial | `test_stale_sending_becomes_unknown_not_automatically_retried`、S0 correctness/Fast tests | PASS |
| N08 多 Campaign 共用 Journal | Fair multi-campaign E2E：88/88 tasks、88/88 submit_sm success、0 duplicate/missing，queue backlog final 0 | PASS（socket fairness 仍 S2） |
| N09 Legacy compatibility | 全量 Legacy 默认回归；Fair strict protocol 仅在 opt-in 模式启用；旧 Journal 仍按 v1 JSONL/CRC 读取 | PASS |

## 实际验收结果

1. **新增 S1.1 边界测试**：Windows isolated `.p0-test-venv`，`8 passed`。
2. **S0/S0.1/S1 关联回归**：WSL isolated targeted run，`95 passed, 4 warnings`；Windows isolated Journal 路径 run，`41 passed`。
3. **最终后端全量**：

   ```text
   MC_DUCKDB_WRITE_CONCURRENCY=serialized
   python -m pytest backend/tests -q -p no:cacheprovider --basetemp=/tmp/mc-s11-20261015/pytest-g0-final
   339 passed, 2 skipped, 4 warnings in 271.34s
   ```

   原始日志：`/tmp/mc-s11-20261015/evidence/g0-final.log`。

4. **Fair reliable loopback quick**：`e2e-fair-quick-final-r2.json`，3/3 tasks accepted，3/3 submit_sm success，0 duplicate/unknown/lost，Journal drained，诊断 `result_pipeline_hard_bytes=VERIFIED`。
5. **Fair low-byte multi-campaign pressure**：`e2e-byte-pressure-final-r2.json`，normal `34,000,000`、emergency `16,781,312`；4 activities 共 88/88 tasks、88/88 submit_sm success、0 duplicate/missing，backlog peak 65、final 0。
6. **Fair low-byte restart recovery**：`e2e-byte-pressure-restart-final.json` 同一预算，5/5 tasks、5/5 submit_sm success，0 duplicate/lost/unknown mapping。
7. **Frontend**：`npm run build` exit 0，Vite 8.1.3，1794 modules transformed；仅有既存 Rolldown pure-annotation/chunk-size warnings。

`SFTP=BLOCKED_ENV`、`LIVE_ENV_UNVERIFIED` 仅表示外部环境门禁未启用，不是本地结果丢失。

## 剩余边界与回滚

- Legacy 默认不接入 Fair ledger；切回 `MC_CAMPAIGN_DISPATCH_MODE=legacy` 保持原兼容行为。
- 外部 DLR/异常输入不是无限接收承诺：超过 event/evidence/disk budget 时保留有界证据、SYSERR/断连并阻止新 Fair send。
- 超过 256 KiB 的历史 Journal 行、未知 Fair 协议记录会原地保留并阻断恢复，需人工 reconciliation；不会静默跳过或 compact 删除。
- `socket_fairness=NOT_VERIFIED`、真实 SMSC/SFTP、SLES12SP5、G3 四组×三次、S4 长稳均明确留在后续门禁，不能从本次 S1.1 结果推导通过。
- 本次未执行 commit/push，遵守仓库默认约束；所有既有用户文件和运行证据均保留。
