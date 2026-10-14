# Track A 多活动发送隔离验收

日期：2026-10-14  
基线：`f391868`  
范围：离线 DuckDB + fake socket；不连接真实 SMSC。

## 验收命令

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests/test_multi_campaign_queue_isolation.py -q
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests/test_realtime_scheduling_reliability.py backend/tests/test_multi_campaign_fair_scheduling.py -q
.\.p0-test-venv\Scripts\python.exe backend/tools/multi_campaign_isolation_acceptance.py --output .tmp\track-a-50k --a-tasks 50000 --bc-tasks 1000 --claim-after-pause 200
```

## 场景和结果

| 场景 | 证据 | 结果 |
|---|---|---|
| A=50K、B/C 各=1K，A 先 Claim 后 pause | `.tmp/track-a-50k/track-a-result.json` | PASS；A=50,000 queued，B/C 各有 4 条 sending，A 未获得新的授权 |
| pause 后 B/C 继续 Claim | 同上 `bc_campaigns_claimed_after_pause=[2,3]` | PASS；离线结果中 B/C 均继续进度 |
| pause 前 Claim 的 A 在授权前失效 | 同上 `pre_authorization_denied_after_pause=true` | PASS；没有 socket write |
| fake socket：A 不写、B 写 | `test_fake_socket_continues_b_after_a_pause_without_real_smsc` | PASS；fake client 只收到 B 的一条提交 |
| fairness/FIFO 活动存在与状态 fence | `test_multi_campaign_fair_scheduling.py`、`test_duckdb_sms_reliability.py` | PASS；测试 fixture 已显式创建合法活动，孤立任务不会被当作可发送候选 |
| claim/event 同一事务 | `test_claim_and_event_are_one_transaction` | PASS；注入 event 失败后任务保持 queued |
| Unknown/Partial 不自动重发 | `test_realtime_scheduling_reliability.py`、现有 SMPP result recovery tests | PASS |

## 门禁矩阵

| Gate | 状态 | 说明 |
|---|---|---|
| `PAUSE_CANCEL_RESUME_ISOLATION` | PASS | 50K/1K/1K 离线重现和 fake socket |
| `FAIR_AND_FIFO_ELIGIBILITY` | PASS | 两种 Claim 路径都使用活动/generation fence |
| `QUEUE_PURGE_AND_FENCING` | PASS | 活动级 slot invalidation 单测 + durable pre-wire denial |
| `JOURNAL_CRASH_REPLAY` | PASS | Send Store PoC 的 apply=false + replay 单测 |
| `MULTIPART_UNKNOWN_PARTIAL` | PASS | 既有 SMPP multipart、Unknown、Partial 测试 |
| `SHARD_CROSS_DB_STOP_FENCE` | NOT_RUN | PoC 将控制状态与任务放在同一 shard，未证明跨库协议 |
| `EQUAL_WINDOW_AB` | NOT_RUN | 尚未达到同负载、同窗口、同 SMSC 模拟器的多臂 A/B |
| `SLES12SP5` | NOT_RUN | 当前验证环境为 Windows；未构建/运行 SLES 包 |
| `LIVE_SMSC` | UNVERIFIED | 按任务约束不连接真实 SMSC |
| `LONG_SOAK` | NOT_RUN | 未用 1K/3K/10K 替代长 soak |

Track A 的离线正确性门禁为 PASS；端到端真实 `submit_sm` 连续性和生产环境门禁保持未验证。整体发布结论因此不是“生产已关闭问题”。
