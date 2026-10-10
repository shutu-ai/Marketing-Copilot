# DuckDB 实时入库两批优化实施记录

2026-10-08：本地实现及回归验证；本轮未同步生产服务器、未重启服务、未生成部署包。

## 第一批：进度事务与恢复

- 新增 `backend/services/realtime_ingestion_state.py`，两张进度表在显式短事务中一起更新，任意失败整体回滚。
- 文件级展示进度默认每 2 秒最多尝试一次；阶段交替不会绕过节流。普通进度的写锁预算为 0.1 秒，遇到锁超时或事务冲突保留最新快照，不重跑输入文件。
- 关键状态使用最多 3 次整笔短事务重试；每次关闭连接、释放锁后退避。没有改变全局连接的自动事务行为，也没有把文件解析/索引读取/文件移动放入事务。
- 领取 token 限定进度、任务实际插入、chunk 检查点、重试、失败及完成状态。旧 worker 失去领取权时停止写入。
- 文件 heartbeat 独立于解析线程，每 `min(30秒, 文件租期/3)` 续租文件及正在处理的 chunk；原有文件租期默认 600 秒。
- 领取与续租统一采用数据库当前时间生成带时区的租约期限，避免 Python 本地时间与数据库时区混用。
- 运行维护每 30 秒回收真正过期的文件租约并释放未完成 chunk，保留已完成检查点。若连失败状态都无法落库，保留文件并停止续租，等待到期恢复。
- CSV/Excel 创建任务与 chunk 完成检查点在同一短事务中提交，防止任务已创建、检查点未保存时重试统计失真。
- 最终文件元数据及两张表的 completed 状态一起提交。文件已移到 received 但最终提交失败时，从 received 复用已完成 chunk，不重复创建任务。
- 主库进度更新移出白名单索引打开的异常范围，避免数据库竞争被误判为白名单损坏。修复白名单恢复到 ready 后仍落入失败分支的问题。

新增可选配置（无需修改现有 env 也会采用默认值）：

```ini
MC_REALTIME_PROGRESS_INTERVAL_SECONDS=2
```

chunk 检查点和终态不受该展示间隔限制。续租独立于进度节流。

## 第二批：chunk 批量去重

CSV 的规范化号码按输入顺序保留首行，每个 chunk 通过有界 DataFrame 关系一次性写入临时 DuckDB 去重表，使用返回的新号码决定哪些行创建任务。

保留跨 chunk 去重、规范化、首行字段/语言、无效行数、匹配行数与唯一匹配用户数；不使用全文 Python set 替代磁盘去重库。不匹配白名单的文件仍正常分析并完成。

2,500 行现在只有 1 次去重 INSERT，而非 2,500 次 Python/SQL 调用。

## 观测与验证

新增指标：

- `realtime_file_progress_commits_total`：实际进度提交次数。
- `realtime_file_progress_deferred_total`：普通进度因竞争延迟次数。
- `realtime_state_transaction_retries_total`：短事务重试次数。
- `realtime_file_lease_renew_failures_total`：续租失败次数。
- `realtime_inbox_active_phase_files{phase=...}`：仅统计 processing 文件；退出阶段后旧标签清零。
- `realtime_file_stage_seconds{phase=...}` 及 count/sum：相邻阶段回调之间的墙钟耗时，包含调用期间开销，并非纯 CPU 计算时间。

既有 `realtime_inbox_phase_files` 保留历史全状态统计，旧标签也会清零；不能将历史 phase 总数当作活跃 worker 数。

本地 DuckDB 1.2.0、2,500 个不同号码、三轮内存库微测试：旧版耗时 1.840/3.301/3.085 秒，新版 0.036/0.049/0.074 秒，中位耗时约降低 63.5 倍。这只衡量去重环节，不能折算为线上短信 TPS。

新增测试覆盖两表事务回滚与锁释放、全文件节流、瞬态进度失败、旧 token 拦截、重试不重复计数、租约续期与运行期恢复、跨 chunk 首行去重、归档后完成提交失败恢复、任务/检查点原子性、四个入库 owner 并发、真实 CSV 到短信任务完整流程、活跃 phase 指标清零。既有短信批量写、任务领取、白名单及更新回归同时验证。

最终关联回归：**54 passed, 1 skipped，58.95 秒**；包含 `test_realtime_progress_batches`、`test_realtime_ingestion_optimization`、`test_phase2_write_reduction`、`test_phase3_single_host_scale`、`test_smpp_async_submission`。Python 编译检查与范围内 `git diff --check` 通过。

默认跳过千万行容量测试；线上长时间负载与 TPS 尚待部署后对比验证。不能承诺 DuckDB 引擎执行、磁盘故障或全部业务路径永不阻塞。文件移动、输出 CSV 和数据库之间仍无跨系统原子事务，短信送达仍不承诺 exactly-once。

线上验证应保持 worker、claim batch、SMPP 连接/窗口参数不变，按同一时间区间采集文件处理数/耗时、两种独立 transport 计数、队列深度、写锁超时增量及 Web 延迟，再进行至少一个完整业务窗口的观察。
