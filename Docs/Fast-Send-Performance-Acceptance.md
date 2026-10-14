# Fast Send performance acceptance

日期：2026-10-14  
结论：`NO_GO_FOR_FAST_PERFORMANCE / RELIABLE_DEFAULT`

## Acceptance rules

每个 mode、每个规模至少 3 次；A/B 必须使用相同输入、相同 SMSC simulator/真实 SMSC、相同 route TPS、pool、window、worker、ingestion 和 DLR 配置。报告必须保存命令、commit/worktree 状态、环境、RSS/CPU、p50/p95/p99、queue backlog、submit_sm 数、响应数、最终状态、duplicate/lost mapping 和 unknown retry violation。

必须同时满足：

1. 无 duplicate send、lost mapping、misdelivery、unknown 自动重发。
2. `socket_submitted`、submit response accepted、DLR delivered 的计数可对账。
3. pause/cancel/complete stop fence 在并发边界上不产生新 admission 或错误 requeue。
4. 1K/3K/10K/50K 每档 3 次不出现容量失控、长期 backlog 或 API/worker hang。
5. Fast 相对 Reliable 的收益必须在相同窗口和相同环境中测出；不能用不同参数或不同样本替代 A/B。
6. 真实 SLES12SP5、真实 SMSC 和长稳态 soak 完成前，最多只能是 canary evidence。

## Executed evidence

| Evidence | Mode | Result | Key facts |
|---|---|---|---|
| `fast-send-local-e2e-quick-final.json` | Fast | PASS | 3/3 accepted；3 submit_sm/3 success；0 duplicate；0 unknown；loopback-only |
| `fast-send-local-e2e-drop-response.json` | Fast | PASS | 3/3 unknown；3 SMSC submits；无自动重发；unknown submit ids 被显式报告 |
| `fast-send-local-e2e-partial-submit.json` | Fast | PASS | 6 multipart submits 中 1 accepted、5 rejected；任务为 partial；无重发 |
| `fast-send-local-e2e-reject.json` | Fast | PASS | 5/5 failed；5 reject；无 duplicate |
| `fast-send-local-e2e-restart.json` | Fast | PASS | 5/5 accepted；无 duplicate；本 runner restart path 通过 |
| `fast-send-local-e2e-dlr-delay.json` | Fast | PASS | required DLR；5/5 delivered；5 DLR；无 DLR violation |
| `fast-send-local-e2e-stress-1k-tuned.json` | Fast | FAIL | 600 秒超时；仅 200 tasks generated，119 accepted、61 sending、20 queued；180 submit_sm；不能作为性能通过 |
| `fast-send-local-e2e-stress-1k-tuned-rerun.json` | Fast | FAIL | batching 优化后仍 600 秒超时；仅 200 tasks generated，80 accepted、20 sending、100 queued；100 submit_sm；仍不能作为性能通过 |
| `test_fast_send.py` + scheduling/fairness regression | Fast/relevant | PASS | 24 passed；含 claim、checkpoint、startup unknown、socket no-intent、pause fence 和 pause-after-admission |

所有 Fast loopback 结果的 `live_env_status` 为 `LIVE_ENV_UNVERIFIED`。上述 PASS 只表示对应本地故障路径通过，不表示真实运营商或生产通过。

## Required matrix status

| Scale | Reliable ×3 | Fast ×3 | Equal-window A/B |
|---:|---|---|---|
| 1K | NOT_RUN as current paired run | FAIL / two exploratory runs | NOT_RUN |
| 3K | NOT_RUN | NOT_RUN | NOT_RUN |
| 10K | NOT_RUN | NOT_RUN | NOT_RUN |
| 50K | NOT_RUN | NOT_RUN | NOT_RUN |

仓库中已有其他时期/其他环境的 Reliable stress and mixed-campaign artifacts，但它们不是本次 Fast change 的同窗 A/B，因此只作为参考，不提升本矩阵状态。

## Reproduction commands

最小 Fast functional run：

```powershell
.\.p0-test-venv\Scripts\python.exe backend/tools/local_e2e_acceptance.py `
  --scenario quick --send-mode fast --timeout-seconds 120 `
  --smsc-tps 200 --task-workers 2 --ingest-workers 2 --pool-size 1 `
  --smpp-window 10 --smpp-send-queue-capacity 100 `
  --delivery-receipt-mode disabled `
  --output Docs/fast-send-local-e2e-quick-final.json
```

1K tuned run which currently fails the gate：

```powershell
.\.p0-test-venv\Scripts\python.exe backend/tools/local_e2e_acceptance.py `
  --scenario stress-1k --send-mode fast --task-workers 8 `
  --smpp-window 50 --smpp-send-queue-capacity 2000 --smsc-tps 200 `
  --timeout-seconds 600 --keep-artifacts `
  --output Docs/fast-send-local-e2e-stress-1k-tuned.json
```

Backend regression baseline：

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
```

当前完整 suite 曾出现 1 个 Windows online-backup 文件锁失败；单独重跑 `backend/tests/test_online_backup_recovery.py` 为 1 passed。该环境波动不被计入 Fast 性能通过。

## Acceptance disposition

功能保护：`PASS_WITH_LIMITATIONS`。  
Fast 高吞吐性能：`NO_GO`，因为 1K tuned 未收敛且 A/B 矩阵未完成。  
生产切换：`NOT_DONE`。  
默认 mode：`reliable`。
