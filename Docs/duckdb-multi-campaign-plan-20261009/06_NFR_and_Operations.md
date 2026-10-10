## 9. 指标、Web 保护与运维判断

新增指标以内存聚合为主，按 campaign/type/config 限定标签；禁止手机号、filename、task_id 成为常驻标签。分片高频计数使用固定秒桶/有界直方图，避免为每个事件保存长 deque 后每次查询遍历全部样本。老活动时间桶到期释放，历史存入低频汇总。

| 层次 | 关键指标 | 判断 |
|---|---|---|
| 扫描 | last_success、enumerated/candidates/copied、scan_seconds、oldest_candidate_age | 有源文件但长期未复制是 intake 问题 |
| 文件 | pending/processing/retrying、rows/matched/tasks、stage p50/p95、oldest_wait_age | 识别零匹配、依赖等待、大文件或缺少 worker |
| 活动任务 | enqueue/claim/ready/oldest_due_age、share/borrowed_share、no_progress_reason | 验证新活动能获得处理机会 |
| 物理发送 | send_message_parts_total、send_tps、inflight_tasks/parts、queue_credit | 独立于主库统计 |
| 响应 | resp_total/status、resp_tps、p95 latency、timeout、unknown | 判断窗口是否被等待占据 |
| 持久化 | uncommitted/in_progress/retry/log_age、queue/execute/commit p95、class占用时间 | 判断发送结果是否跟不上 |
| Web | endpoint p95、retry/503/timeout、snapshot_age、command_status | 监测是否牺牲交互响应 |
| 系统 | CPU、RSS、disk latency/queue、temp/log/WAL字节 | 验证资源准入假设 |

建议 API：GET /api/capacity/campaigns（策略、份额、各层积压与暂停原因）、GET /api/operations/commands/{request_id}（写结果），POST /api/campaigns/{type}/{id}/capacity-policy（管理员/营销权限和审计）。这些为拟新增接口，不是当前可调用 API。既有 metrics/stats 保持兼容，增加 fields 而非替换计数口径。

有 due 队列但 60 秒无实际发出：先核对活动窗口/白名单/局部限额，再看 claimed 增量、ready、连接 queue/window、结果提交推进，最后按具体卡点报警。一个活动静止但其他活动推进记为局部停滞；所有活动静止且 writer/current_command_age 增长记为疑似共享写入链路阻塞，结合线程栈、SQL执行和IO状态确认。线程 heartbeat 与 last_success 分开，纯循环轮询不能刷新成功推进时间。

Web 写操作只重试确认为瞬态且幂等的事务；超时不等于未提交。重读命令结果优先于重复创建。Web 大导入、导出和历史统计移入有界后台任务；读重试次数不能放大为几十秒同步请求。页面降级显示最后成功快照及时间，不能用空数据伪装正常。
## 10. 部署、权限、备份和回退

本次没有同步远端、重启或更改 env。新能力实施后采用逐阶段开关；现有参数保持对照基线，先默认等权，再在压测后配置保底。worker 数根据 sender/计算供给决定，不按活动数线性增加。

已有配置实验基线可沿用最近确认值 MC_TASK_WORKERS=24、MC_CLAIM_BATCH=30、MC_REALTIME_INGEST_WORKERS=4、MC_SMS_WRITE_BATCH_SIZE=1000、MC_SMS_WRITE_FLUSH_MS=100；这是实验起点，非本次重新确认的远端实际配置。保留窗口/连接/TPS设置做可比较试验，不同时改五个变量。

新增策略建议先存 app_settings 或独立 campaign_capacity_policies 表；兼容旧行自动等权。attempt/result_ack/command_status 采用可增量迁移的新表；主表原状态保持兼容，unknown 可先用附属状态字段表达。migration 有版本标记，所有约束和索引在目标 1.2.0 副本库验证。

结果日志与临时文件限制路径、权限、容量和保留时间；尽量只保存 task_id/token/状态/摘要，业务号码/内容按实际恢复需要最小化，继承现有认证授权，日志不得输出密码/token/完整清单。磁盘故障时不依赖另一个位于同一满磁盘的“紧急日志”无限兜底。

升级前停止新准入并排空已发结果，制作一致备份，验证恢复副本可打开和离线扩展齐全。数据库/WAL/结果日志要作为相关恢复材料；禁止删除 WAL 绕过启动错误。备份时使用服务协调的 checkpoint/backup 流程或停止服务后完整复制，不能复制正在变化的单个 .duckdb 后宣称一致。

回退应用代码时先重放新结果日志并停止新调度；旧版本若不认识新恢复协议不得直接接管新 sending 任务。不可简单恢复旧数据库备份后继续发送，因为备份后已发送任务可能重新出现。关键恢复协议启用后必须提供前向恢复兼容版本；普通开关回退不删除任务/日志。

历史 terminal 数据按已验证归档策略移出热路径，未完成任务不归档。去重 tombstone 在业务去重期限内保留，不能删除 terminal 后让同一天同用户再次建任务。归档、清理、checkpoint 与索引构建使用维护时间预算；大范围 DELETE、全表 backfill 不在正常 startup/web 请求中运行。

运维责任：应用团队负责事务/状态/幂等；运营配置份额与窗口；部署维护者负责运行时、扩展、备份/回滚；性能结论必须有同负载观测记录与批准的降级策略。

