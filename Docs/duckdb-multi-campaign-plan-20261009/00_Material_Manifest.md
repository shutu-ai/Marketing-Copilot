# 材料范围与处理回执

| 材料 | 范围 | 处理状态 |
| --- | --- | --- |
| MAT-001 | backend/database.py：write_transaction、WrappedConnection | PROCESSED_LOCAL |
| MAT-002 | backend/services/sms_task_service.py：claim_next_tasks | PROCESSED_LOCAL |
| MAT-003 | backend/services/task_worker.py：_claim_fetch_size、_claim_dispatch_loop | PROCESSED_LOCAL |
| MAT-004 | backend/services/sms_write_buffer.py：_commit、_run | PROCESSED_LOCAL |
| MAT-005 | backend/services/realtime_ingestion_worker.py：_claim_realtime_file、_run_maintenance | PROCESSED_LOCAL |
| MAT-006 | backend/services/realtime_campaign_service.py：sync_watch_dirs_to_inboxes、process_realtime_inboxes | PROCESSED_LOCAL |
| MAT-007 | backend/services/realtime_whitelist_service.py：build_pending、build_version | PROCESSED_LOCAL |
| MAT-008 | backend/services/smpp_gateway.py：send_async、_connection_for、_send_windowed_async | PROCESSED_LOCAL |
| MAT-009 | backend/services/backpressure_service.py：reasons、snapshot | PROCESSED_LOCAL |
| MAT-010 | backend/services/sms_task_service.py：mark_submitted、_apply_disabled_submitted_batch、recover_stale_tasks | PROCESSED_LOCAL |
| MAT-011 | backend/services/metrics_service.py、smpp_service.py | PROCESSED_LOCAL |
| MAT-012 | backend/services/realtime_ingestion_state.py | PROCESSED_LOCAL |
| MAT-013 | Docs/duckdb-realtime-batches-implementation-20261008.md；Docs/duckdb-realtime-progress-study-20261008.md | PROCESSED_LOCAL |
| MAT-014 | 本地 DuckDB 1.2.0 实验与现有测试 | PROCESSED_LOCAL |
| MAT-015 | 历史用户日志/采样 | PROCESSED_LOCAL |
| MAT-016 | DuckDB 1.2 官方文档 | PROCESSED_LOCAL |
| MAT-017 | 独立代码审计：成功/失败回调、recover_stale_tasks、message-id映射和DLR路径 | PROCESSED_LOCAL |

只认证所列符号/证据摘录，不认证全仓库和远端现状。17项材料均有处理回执；来源及未确定性见 model。

