# Send Store Sharding Architecture Decision

日期：2026-10-14  
结论：`MORE_EVIDENCE_NEEDED`；PoC 通过可靠性样例，但不具备生产迁移资格。

## 决策追溯

| ID | 需求/证据 | 决策 |
|---|---|---|
| DEC-001 | REQ-A01～A06；EVD-A01～A07 | 生产 Track A 先采用活动 generation fence + 同事务任务协调，不拆现有主库。 |
| DEC-002 | REQ-B01～B08 | Track B 仅新增隔离实验模块；`SHARDED_SEND_STORE` 默认 `OFF`，生产启动路径不导入 PoC。 |
| DEC-003 | REQ-B02、B04 | PoC 使用固定 1/4 shard DuckDB 文件，`sha256(campaign_type:campaign_id) mod N` 映射，禁止 Python 随机 hash 和每活动建库。 |
| DEC-004 | REQ-B05、B06 | PoC 中活动控制、任务、attempt、part 和结果 Journal 同 shard；跨数据库 stop fence 不假设由最终一致通知提供。 |
| DEC-005 | G-B | 小规模 1/4 shard 结果不能证明收益；推荐继续收集受控同窗口 A/B，不推荐生产迁移。 |

## 候选方案

| Arm | 结构 | 优点 | 主要风险/取舍 | 当前状态 |
|---|---|---|---|---|
| A | 当前主 DuckDB + Track A | 改动最小，现有 Journal/attempt 语义保留 | 主库发送热路径仍与管理/摄取共享 | 冻结基线 |
| B | 同主库，活动限定预取 + 短批量授权 | 迁移成本低 | 写锁仍是单点；需继续测 gate wait | 未单独实现 |
| C | 1 个独立 Send Store DuckDB | 可隔离发送热路径 | handoff、备份和跨库 stop fence 复杂 | PoC 可运行 |
| D | 4 个独立 Send Store DuckDB shard | 观察真正横向写并行 | 需要唯一授权来源、稳定 map 和跨库运维 | PoC 可运行 |
| E | SQLite WAL | 可能改变写并发特征 | 引入第二种技术和维护栈 | 未测试 |

## PoC 的数据权威和事务边界

`backend/services/send_store_sharding_poc.py` 只用于实验：

```text
campaign control + dispatch_generation
                 │ same shard transaction
                 ├─ tasks(status, claim_token, generation)
                 ├─ attempts(claim/attempt evidence)
                 ├─ parts(socket/result evidence)
                 └─ result_journal(event_id, applied, replay)
```

- `campaign_type + campaign_id` 通过 SHA-256 稳定定位到固定 shard；实验目录会保存 `shard-map.json`，重启时校验 shard 数不变。
- 每个 shard 有独立 DuckDB connection 和 `RLock`。PoC 没有共享生产 `_write_lock`，所以不会把串行锁伪装成分片并行。
- `claim` 在同 shard 内以活动组轮转并再次检查 `status='in_progress'` 与 generation。
- `authorize` 仅在 claim token、活动状态和 generation 同时有效时创建 attempt 与所有 planned parts；这是模拟 socket 前的 durable fence。
- `pause` 只回收没有 authorized/partial attempt 的 sending 任务；`cancel/complete` 只终结未授权任务；Unknown/Partial 不会回到 retry queue。
- Result Journal 使用 `event_id` 幂等键；`apply=false` 后 `replay_journal()` 可以补齐 task/part 状态，重复 replay 不改变结果。
- 报表投影不在 PoC 中作为授权来源；发送资格、去重、停止和 attempt 证据只认 Send Store 的同 shard 事务。

## 生产迁移前必须补齐的跨库协议

当前生产主库仍是唯一任务和发送证据来源。若未来采用 C/D，必须先设计并验证：

1. 主库任务创建到 Send Store 的 durable outbox/inbox 或可恢复 ingest journal；不能出现主库成功而 Send Store 永久缺任务。
2. pause/cancel/complete 先在实际授权所在边界建立 fence，再向管理 API 返回成功；不能靠最终一致消息直接停发。
3. 同一 multipart task 的 claim、attempt、part evidence、result event 归属于唯一 shard；两个系统不能同时拥有发送资格。
4. Journal、Send Store、shard map、主库、投影游标必须由同一逻辑恢复点备份；强杀后只能有一个事件归属和顺序。
5. 切换前后做任务唯一性、Unknown/Partial、socket evidence、未确认发送不重发和统计投影延迟对账。
6. SLES12SP5 离线 wheelhouse、文件句柄、磁盘容量、备份时延和回滚后的未知状态核对需有现场证据。

这些是迁移设计门禁，不是本次 PoC 已实现的生产能力。

## 代价与判定

- 当前 Track A 的 generation fence 和 bounded queue invalidation 没有新增数据库类型，优先级高于拆库。
- PoC 证明了固定 shard map、每 shard 独立 gate、同 shard stop fence 和 Journal replay 可以在隔离模块中成立。
- 1/4 shard 的小样本结果并非同一完成任务量、同一窗口和同一资源压力，不能用于宣称 TPS 提升；4 shard 甚至在本次小样本中存在更高的尾部 hold 值。
- 因此架构判定为 `MORE_EVIDENCE_NEEDED`，不自动修改 `DATABASE_FILE`，不创建生产库，不迁移真实任务，不切换配置。
