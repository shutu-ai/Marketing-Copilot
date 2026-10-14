# Send Store Sharding PoC Benchmark

日期：2026-10-14  
原始结果：`.tmp/send-store-poc-final/result.json`  
范围：离线、单进程、DuckDB PoC；不连接真实 SMSC。

## 启动入口

```powershell
.\.p0-test-venv\Scripts\python.exe backend/tools/send_store_sharding_poc.py `
  --output .tmp\send-store-poc-final --shards 1,4 --tasks-per-campaign 24
.\.p0-test-venv\Scripts\python.exe backend/tools/send_store_sharding_poc.py `
  --output .tmp\send-store-disabled --check-disabled
```

默认 `SHARDED_SEND_STORE=OFF`；只有显式调用实验入口时才以 `enabled=True` 打开 PoC。生产 `backend/main.py` 没有导入该模块。

## 本次实际运行

每个 Arm 建立 3 个 realtime 活动，每活动 24 个任务；包含 1/2 part 混合、A pause → B/C progress → A resume、部分 Unknown、Journal apply=false/replay 和一致性核对。两臂均报告 `PASS`，但不是 B3 要求的生产规模 A/B。

| 指标 | D-1shard | D-4shard |
|---|---:|---:|
| 任务总数 | 72 | 72 |
| `elapsed_seconds` | 1.021169 | 1.275621 |
| `business_sms/s`（PoC 模拟授权/结果折叠） | 31.337 | 37.629 |
| `submit_sm parts/s`（PoC 计划 part 口径） | 41.129 | 50.172 |
| pause 后 B/C 进度数 | 32 | 48 |
| Journal replayed | 1 | 1 |
| consistency violations | 0 | 0 |
| write gate wait P50/P95/P99 | 0.000002 / 0.000002 / 0.000002 s | 0.000002 / 0.000002 / 0.000002 s |
| write gate hold P50/P95/P99 | 0.084521 / 0.185836 / 0.185836 s | 0.008906 / 0.104313 / 0.104313 s |

## 口径和不可比较项

- `business_sms/s` 和 `submit_sm parts/s` 明确分开；两者都不是 SMSC `submit_sm` 的真实响应吞吐。
- D-4 在本次运行中 Claim 了更多 B/C 任务，导致完成工作量和结果事件数不同；这不是 equal-window A/B。
- 没有收集真实 SMSC 成功/拒绝率、socket window、CPU/RSS、磁盘 I/O/fsync、投影延迟、10 分钟 backlog 斜率或 1K/3K/10K soak；这些字段在本次结果中是 `NOT_RUN`，不能用 24 替代。
- 没有测试 SQLite WAL，因此没有引入第二种数据库的收益/维护成本结论。

## 判定

`track_b = MORE_EVIDENCE_NEEDED`。当前结果足以证明 PoC 的状态/恢复样例和稳定定位，不足以证明分片能在相同 SMSC、同 TPS 限额、同窗口、同磁盘和同完成工作量下稳定降低锁竞争并改善端到端吞吐；不推荐生产迁移。
