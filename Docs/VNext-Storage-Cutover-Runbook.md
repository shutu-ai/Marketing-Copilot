# VNext 新库切换与启动性能 Runbook

## 目标

- 业务运行时只打开新业务库和 VNext 投影库，不再查询旧业务库。
- 任务列表、活动类型、号码过滤和任务导出统一读取 `vnext_task_index`。
- 短信发送 Worker 仍只写 Spool/Progress Journal；任务索引由唯一 ProjectionWriter 批量更新，不增加每短信同步 DuckDB 写入。
- 旧库保留为离线回滚归档，验收完成前不得删除。

## 一次性切库

业务必须停止，使用停止后的数据库快照或备份执行：

```bash
PYTHONPATH=backend python -m tools.cutover_vnext_store \
  --source /opt/marketing-copilot/data/marketing_copilot.duckdb \
  --business-target /opt/marketing-copilot/data/vnext/marketing_copilot-vnext.duckdb \
  --projection-target /opt/marketing-copilot/data/vnext/projection/vnext-projection.duckdb \
  --manifest /opt/marketing-copilot/backups/vnext-cutover-manifest.json
```

切换 `db_config.json` 或服务环境中的 `DUCKDB_PATH` 指向
`marketing_copilot-vnext.duckdb`，确认服务启动后再做验收。不要在服务运行时
直接覆盖源库或投影库。

## 验收

1. 健康检查和登录正常。
2. 运营中心任务列表跨页查询 `campaign_type=batch`。
3. 使用已知 `normalized_msisdn` 查询，确认总数和结果不受当前页限制。
4. 检查任务导出、活动详情、统计和 SMPP 配置。
5. 观察 ProjectionWriter backlog、写入失败、Spool pending 和发送吞吐；任务索引延迟只能影响列表可见性，不能阻塞发送。
6. 记录源库、目标库、投影库路径及 manifest；验收通过前保留源库只读归档。

## 启动性能

`db_canonical_ready_v1` 只由离线切库工具在维护、索引、去重、回填和校验完成后写入。
带该标记的启动只执行轻量基础 DDL 和默认配置检查，不再重复扫描 `sms_tasks`、重建
`processed_files` 或回填消息 ID。若标记缺失，服务仍执行完整修复流程并记录耗时，不能
在未完成维护时强行写入标记。

## 回滚

回滚只切回经过校验的旧归档路径并重启服务；不要在服务运行中删除或覆盖新库。新库
验收通过后，旧库仍按保留策略保存，删除由运维审批单单独执行。
