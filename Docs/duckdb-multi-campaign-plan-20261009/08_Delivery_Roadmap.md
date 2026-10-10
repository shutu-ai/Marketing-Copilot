## 11. 分阶段实施与文件级改动

| 阶段 | 改动 | 主文件/新组件 | 通过后才进入下一阶段 |
|---|---|---|---|
| A | claim/缓冲显式事务、失败保留、attempt fencing、独立指标和结果恢复原型 | database.py、sms_task_service.py、sms_write_buffer.py、smpp_gateway.py；拟新增 result_journal_service.py | 回滚/重放/未知状态测试通过；低负载 Web/发送无回归 |
| B | 公平领取、发送信用、按片 DRR、每 route 配额；逐步写协调 | task_worker.py、sms_task_service.py、smpp_gateway.py、backpressure_service.py；拟新增 campaign_capacity_service.py、db_write_coordinator.py | 大小活动/混合活动/双中心公平测试通过，吞吐不劣化 |
| C | 扫描轮询时间预算、统一容量 reservation、活动入库并发、构建独立线程 | activity_scheduler.py、realtime_campaign_service.py、realtime_ingestion_worker.py、ingestion_job_service.py、realtime_whitelist_service.py | 大目录/大文件/索引构建不饿死其他活动 |
| D | Web后台任务/命令结果、秒桶指标、低频聚合、受控关闭与72小时扩展观测 | metrics_service.py、smpp_service.py、routers/metrics.py、相关Web服务、deploy | 24小时门禁通过再延长验证；升级/恢复可追溯 |

预计工作量依赖验收发现，不预设“只改一个参数就完成”。A 的发送结果持久化是正确性需求，需单独测量 fsync/批量提交成本。公平策略默认可开关灰度，但幂等/恢复修复不能以关闭开关重启旧危险恢复流程作为普通回退。


