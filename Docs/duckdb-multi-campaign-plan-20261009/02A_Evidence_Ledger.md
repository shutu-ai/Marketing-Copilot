## 2. 代码证据与需要先修复的问题

本节事实以当前磁盘源码为依据；行号仅用于本版本定位。历史采样证明曾出现故障，不用于推算本轮实际容量。

| ID | 位置 | 已确认事实及影响 |
|---|---|---|
| EVD-001 | backend/database.py：write_transaction、WrappedConnection | 显式事务帮助函数已有 BEGIN；全局应用写锁自首个写入持有到 commit/rollback/close。此锁是本程序策略，DuckDB 本身并非只能有一个线程写入 |
| EVD-002 | backend/services/sms_task_service.py：claim_next_tasks | 按 created_at,id 从全局队列领取；未显式 BEGIN。任务 UPDATE 与事件记录不是天然一个多语句事务 |
| EVD-003 | backend/services/task_worker.py：_claim_fetch_size、_claim_dispatch_loop | 一个全局 dispatcher；领取量 min(500, workers×claim_batch)。worker 数不会为每个活动创建独立配额 |
| EVD-004 | backend/services/sms_write_buffer.py：_commit、_run | _commit 未显式 BEGIN；三次失败后依然 done/task_done，wait=False 调用者没有等待该错误。存在批次部分提交/结果未可靠恢复的风险 |
| EVD-005 | backend/services/realtime_ingestion_worker.py：_claim_realtime_file、_run_maintenance | 文件按活动轮询；领取到整个文件完成才释放 worker；白名单维护/构建共用一个维护锁并占用入库 worker |
| EVD-006 | backend/services/realtime_campaign_service.py：sync_watch_dirs_to_inboxes、process_realtime_inboxes | campaign 顺序扫描，目录 enumerate/sort；注册没有轮次时间预算。长目录或慢复制会延迟后续活动 |
| EVD-007 | backend/services/realtime_whitelist_service.py：build_pending、build_version | 构建候选按 created_at,id 串行选择；索引按活动/版本保存在独立 DuckDB 文件。默认构建限制并不等于整个进程内存上限 |
| EVD-008 | backend/services/smpp_gateway.py：send_async、_connection_for、_send_windowed_async | 连接公共 FIFO，连接轮询；满队列产生可重试失败。window semaphore 按整条任务获取，状态 inflight 按 sequence 计数 |
| EVD-009 | backend/services/backpressure_service.py：reasons、snapshot | task_depth 包含 queued/retrying/sending；paused 状态在 reasons 更新，snapshot 返回已有状态；缓存快照不能当作实时调度决定 |
| EVD-010 | backend/services/sms_task_service.py：mark_submitted、_apply_disabled_submitted_batch、recover_stale_tasks | disabled 不存 message_id，成功响应仍产生 submitted 写入；accepted 同时填写 delivered_at；sending 超过 5 分钟会恢复为 retrying |
| EVD-011 | backend/services/metrics_service.py、smpp_service.py | 实际发出和响应按 config/slot 聚合，任务统计主要是全局；尚无完整 campaign 发送份额/等待时间指标 |
| EVD-012 | backend/services/realtime_ingestion_state.py | 文件 token fencing、独立心跳、进度合并节流已有。文件级 transaction 帮助函数需与统一重试接口对齐 |
| EVD-013 | Docs/duckdb-realtime-batches-implementation-20261008.md；Docs/duckdb-realtime-progress-study-20261008.md | 前期短事务、批量去重和扫描研究已存在，不能把旧设计描述当作当前未修复事实 |
| EVD-014 | 本地 DuckDB 1.2.0 实验与现有测试 | 未 BEGIN 的 INSERT 已提交，rollback 报无活动事务；显式 BEGIN 后 INSERT 可回滚。现有文件轮询和单所有者恢复两项测试通过 |
| EVD-015 | 历史用户日志/采样 | 曾有 write-write conflict、写锁超时、积压和零 TPS；多个不同配置和版本混在历史中，不是受控 A/B |
| EVD-016 | DuckDB 1.2 官方文档 | 单进程支持多个线程并发读写，同一行竞争可能失败；多语句业务原子性需显式事务 |
| EVD-017 | 独立代码审计：成功/失败回调、recover_stale_tasks、message-id映射和DLR路径 | claim_token未用于短信完成写回；部分分片成功后临时错误可重试整条；映射主键仅message-id；DLR先入内存队列即确认。审计报告含内存故障复现，不是线上容量测试 |

优先修复 EVD-002/EVD-004/EVD-010：状态持久化不可靠时不能继续扩大发送预取，避免“数据库还显示 sending、短信已发出、恢复又重发”的风险。

另一个容易误判的参数事实：当前 MC_SMS_WRITE_BATCH_SIZE 被限制到 100～1000，FLUSH_MS 被限制到 50～200ms；env 填 2000 不会使有效批次变为 2000。容量状态应同时显示 configured 和 effective。这两个参数属于当前已支持配置；后文新增策略字段均尚未实现。

