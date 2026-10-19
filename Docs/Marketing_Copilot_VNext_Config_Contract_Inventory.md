# Marketing Copilot VNext 配置契约与绑定审计清单

审计对象：`Docs/Marketing_Copilot_VNext_Configuration_Contract_Performance_Cleanup_Codex_Task.md`。

审计基线：本次工作开始时的 `HEAD=6ebfe088fbc19ab61539999fd627570d4d9b5a10`；工作区已有修改和未跟踪材料均保留，未 commit/push。`.codegraph/` 已先用于符号定位；其索引未覆盖当前 VNext 关键实现，后续以当前源码和可执行测试为准。

## 1. 结论

VNext 的有效容量契约现在固定为：

| 维度 | 唯一来源 | 真实语义 | 生效消费者 | 是否乘 Worker 数 |
|---|---|---|---|---|
| Worker | `MC_VNEXT_WORKERS`，默认 4，服务入口限制 1–8 | 业务数据并发/分片进程数 | `VNextRuntime`、`ShardWorker` | 否 |
| Bind | `smpp_configs.pool_size`，默认 1，限制 1–20 | 一个 route 允许的物理 SMPP bind 上限；bind 生命周期持有 lease | `AsyncSmppTransport`、`RouteCreditBroker.acquire_bind` | 否 |
| Window | `smpp_configs.window_size`，默认 10，限制 1–100 | 每个物理 bind 的 `submit_sm` 在途 PDU 上限 | `AsyncSmppTransport._window` | 否 |
| TPS | `smpp_configs.tps`，默认 20，单位 `submit_sm_parts/s` | PDU part 发送节奏 | `RouteCreditBroker.acquire` | 否 |
| TPS scope | `smpp_configs.tps_scope`，固定 `per_bind` | 每个实际 bind 独立 TPS；Web/API 不再允许选择 `route_shared` | `RouteCreditBroker`、状态 API/UI | 否 |

有效总 TPS：`per_bind=tps × 实际物理 bind 数`，最多不超过 `pool_size`。Worker 数不会再把 TPS 或 Bind 数隐式放大。有效 PDU 在途上限：`实际物理 bind 数 × window_size`。

## 2. SMPP 配置字段全量去向

下表不列出密码值；`system_id/password` 只可进入连接进程，不进入审计 JSON。

| Key/字段 | Owner / 默认值 | 存储 → 读取链路 | 行为与生命周期 | 性能/正确性分类 |
|---|---|---|---|---|
| `name` | `smpp_configs`，无默认 | DB → `smpp_service`、UI | 管理标识 | 控制面 |
| `host`,`port` | `smpp_configs`，port=2775 | DB → `_load_routes` → transport | bind 连接端点；仅下次启动/重启替换连接 | 控制面 |
| `system_id`,`password` | `smpp_configs`，无默认 | DB → `_load_routes` → `build_bind`；密码只做 masked API 返回 | SMPP bind 认证 | 密钥/控制面 |
| `source_addr` | `smpp_configs`，空值回退 `Marketing`/transport 默认 `VNEXT` | DB → `send_task` → `submit_sm` | submit 源地址 | 协议正确性 |
| `bind_type` | `transceiver` | DB → `build_bind`；支持 receiver/transmitter/transceiver | 由响应 command 校验；receiver 不发送 submit_sm | 协议正确性 |
| `address_ton`,`address_npi` | 0/0 | DB → `build_submit_sm` source TON/NPI | 不再硬编码 source TON/NPI | 协议正确性 |
| `data_coding` | 0 | DB → `build_submit_sm` | 显式编码覆盖；未配置时按消息检测 | 协议正确性 |
| `pool_size` | 1，1–20 | DB → Worker per-route bind target → `acquire_bind` | 连接生命周期 lease；跨 Worker 共享上限；不等于 Worker 数 | 容量/资源 |
| `window_size` | 10，1–100 | DB → 每个 `AsyncSmppTransport` Semaphore | 每个 bind 独立 PDU window；响应释放 | 容量/正确性 |
| `enquire_interval` | 30 秒，0–300 | DB → transport heartbeat | `ENQUIRE_LINK`；0 可关闭 | 连接保活 |
| `timeout` | 10 秒，1–120 | DB → socket/connect/window 等待 | 连接及响应等待上限 | 稳定性 |
| `tps` | 20，0–10000 | DB → RouteCreditBroker | 单位固定为 `submit_sm_parts/s` | 性能/限流 |
| `tps_scope` | `per_bind`（固定） | DB → credit key | 使用 `route-id:bind:slot`，不再提供 route-shared 选项 | 契约 |
| `priority` | 100，1–10000 | DB → route load / default route | 默认路由按 `(priority,id)`，不再按最小 id 偶然选择 | 路由正确性 |
| `reconnect_max_seconds` | 60 秒，0–300 | DB → bounded reconnect backoff | 连接失败退避；0 表示不退避 | 稳定性 |
| `delivery_receipt_mode` | `optional` | DB → `registered_delivery` | required 请求 DLR；optional/disabled 不强制请求 | 协议/业务 |
| `enabled` | 1 | DB → `_load_routes` | 仅启用 route 进入 VNext；修改须重启生效 | 控制面 |
| `created_at`,`updated_at` | DB 元数据 | DB/API | 审计与 UI 排序 | 元数据 |

新增 DB 字段：`smpp_configs.tps_scope TEXT NOT NULL DEFAULT 'per_bind'`，并在初始化迁移中将空值、旧 `route_shared` 和非法值统一回填为 `per_bind`。旧数据库只在应用 `init_db()` 正常启动时迁移；本次没有直接改写工作区原始数据库。

## 3. VNext 环境配置

| Key | 默认/边界 | 读取路径 | 语义 |
|---|---|---|---|
| `MC_PIPELINE_MODE` | `vnext`；`legacy/test` 不启动 VNext | `services/vnext_service.pipeline_mode` | 总开关；VNext 的 SMPP 配置固定使用 `per_bind` |
| `MC_VNEXT_RUN_ROOT` | `$MC_STATE_DIR/vnext` | `vnext_service.run_root` | spool、credits、progress、projection 根目录 |
| `MC_STATE_DIR` | `backend/data` | `run_root`、auth、update | 状态根；不是 Worker 数 |
| `MC_VNEXT_WORKERS` | 4，1–8（Runtime 上限 32） | `vnext_service.start` | 业务分片并发 |
| `MC_VNEXT_PARTITIONS` | 256，16–4096 | `vnext_service.start` | 稳定 MSISDN 逻辑分片数 |
| `MC_VNEXT_MAX_SPOOL_BYTES` | 默认 4 GiB，最小 4 GiB；可配置 16/32 GiB 等更大值 | `vnext_service.start` | spool 硬容量 |
| `MC_VNEXT_PROJECTION_QUEUE` | 8192，128–100000 | `vnext_service.start` | 投影 IPC 有界队列 |
| `MC_VNEXT_MAX_INFLIGHT` | 128，1–2048 | `vnext_service.start` | 每 Worker 业务 task 并发，不改变 route TPS |
| `MC_VNEXT_PREFETCH_PER_PARTITION` | 64，1–512 | `vnext_service.start` | 每分片预取，不是 PDU window |
| `MC_VNEXT_PROJECTION_FLUSH_MS` | 100 ms，10–5000 ms | `VNextRuntime` | 投影批量刷新间隔 |
| `MC_RUN_ID` | run root 名称 | `VNextRuntime` | 诊断标识 |
| `MC_VNEXT_CANARY_MODE` | 0 | VNext API canary router | 仅 canary API 门控 |

运行时状态 API 现在返回 `config_contract`、每 route 的 `pool_size/window_size/tps/tps_scope`、`active_bind_count/slots`、effective TPS、`tps_unit` 和 `restart_required`。Web/API 修改 `tps` 时可热应用；修改 `window_size`、`pool_size` 等结构字段后返回 `effective_state=pending_restart`，安全重启后才清除 pending 状态。

## 4. 旧/并行配置的边界

这些配置确实仍被 legacy `smpp_gateway` 或通用服务消费，不属于 VNext 的有效容量来源，因此不删除、不映射到 VNext，避免“双重限流”。

| 配置 | Owner | VNext 是否读取 | 处理 |
|---|---|---|---|
| `MC_SMPP_WINDOW_SIZE` | legacy gateway 默认 window | 否 | 保留给 legacy；VNext 使用 DB `window_size` |
| `MC_SMPP_INTERNAL_TPS_MULTIPLIER` | legacy gateway 内部倍率 | 否 | 保留给 legacy；VNext 不乘倍率 |
| `MC_SMPP_PARALLEL_BIND_REQUIRED` | legacy bind pool | 否 | 保留给 legacy；VNext 使用 `pool_size` lease |
| `MC_SMPP_SEND_QUEUE_CAPACITY` | legacy send queue | 否 | 保留给 legacy；VNext 使用 spool/worker 边界 |
| `MC_TASK_WORKERS`,`MC_CLAIM_BATCH`,`MC_DISABLE_TASK_WORKER` | legacy task worker | 否 | VNext 不读取，避免 Worker 概念串线 |
| `MC_SMS_SEND_MODE`,`MC_CAMPAIGN_DISPATCH_MODE` | legacy/Fast pipeline | 否 | VNext 由 `MC_PIPELINE_MODE` 选择 |

没有发现可安全删除的 DB 字段：其余 SMPP 字段均仍用于 legacy API、测试、连接协议或 VNext。删除项集中在热路径冗余行为，而非删除仍有消费者的兼容字段。

## 5. RouteCreditBroker 审计

旧实现每个 PDU 都执行 route 文件锁、JSON 临时文件、`fsync`、rename，并使用 1.15 safety factor，导致配置 200 parts/s 时有效节奏约为 `200/1.15`，且磁盘同步位于发送热路径。

当前实现：

- 本地 lease 每 8 个 PDU 消耗一次；跨进程只在 lease refill 做文件锁、JSON 原子替换和 `fsync`。
- `route_shared` 的 durable key 为 route；`per_bind` 的 durable key 为 route+实际 bind slot。
- `pool_size` 的 bind lease 只在连接生命周期创建/释放，bind 文件不会每 PDU 同步。
- 先 durable 预留完整 lease，再本地消费；进程崩溃最多损失未消费配额，不会因此超发。
- snapshot 暴露 `credit_refills/file_credit_refills/credit_wait_seconds/bind_acquires`，可审计而不是猜测。

## 6. 机器检查与证据

新增只读检查器：`backend/tools/vnext_config_contract_check.py`。它检查 schema、字段范围、bind/scope 组合、effective TPS 和 window 容量，输出不含密码的 JSON。新增单测覆盖 route_shared、per_bind、receiver outbound 拒绝和隐私字段排除。

完整发现的 `MC_*` 名称见下方附录；其中前缀扫描中的 `MC_DISPATCH_` 只有前缀文本，没有发现可配置的具体键。

### 附录 A：仓库发现的全部 MC_* 名称

以下清单由 `rg -o --no-filename 'MC_[A-Z0-9_]+' backend frontend deploy Docs | Sort-Object -Unique` 复核；值、密码、token 不在此清单中。

```text
MC_ACCEPTANCE_HOOKS MC_ADMIN_PASSWORD MC_ADMIN_USERNAME MC_ALERT_FAILURE_RATE
MC_ALERT_QUEUE_DEPTH MC_ALERT_REALTIME_PENDING_FILES MC_ALERT_WEBHOOK_TIMEOUT
MC_ALERT_WEBHOOK_URL MC_ALLOW_MULTI_INSTANCE MC_APP_BACKEND_DIR MC_AUDIT_QUEUE_CAPACITY
MC_AUTH_DISABLED MC_AUTH_SECRET MC_AUTH_TOKEN_TTL_HOURS MC_BACKUP_DIR MC_BACKUP_KEEP
MC_BACKUP_REQUEST_TIMEOUT_SECONDS MC_BACKUP_REQUEST_TOKEN MC_BACKUP_REQUEST_URL
MC_BATCH_CANCEL_BATCH_SIZE MC_BATCH_CANCEL_CAMPAIGNS_PER_RUN MC_BATCH_CANCEL_INTERVAL_SECONDS
MC_BATCH_INGEST_HIGH_WATERMARK MC_BATCH_INGEST_LOW_WATERMARK MC_BATCH_INGEST_QUANTUM
MC_BATCH_QUANTUM_SECONDS MC_BATCH_RESET_INTERVAL_SECONDS MC_CAMPAIGN_DISPATCH_MODE
MC_CHUNK_HIGH_WATERMARK MC_CHUNK_LOW_WATERMARK MC_CLAIM_BATCH MC_DB_CONFIG_FILE
MC_DETAIL_LOGGING MC_DISABLE_TASK_WORKER MC_DISK_HIGH_WATERMARK_PCT MC_DISK_LOW_WATERMARK_PCT
MC_DISK_MIN_FREE_GB MC_DISPATCH_ MC_DUCKDB_EXTENSION_DIR MC_DUCKDB_OCC_RETRY_JITTER_MS
MC_DUCKDB_OFFLINE_ONLY MC_DUCKDB_READ_RETRIES MC_DUCKDB_READ_RETRY_DELAY_MS
MC_DUCKDB_THREADS MC_DUCKDB_TRANSACTION_ADMISSION_TIMEOUT_SECONDS MC_DUCKDB_WRITE_CONCURRENCY
MC_DUCKDB_WRITE_LOCK_TIMEOUT_SECONDS MC_ENV_FILE MC_FAIR_ACTIVE_CAMPAIGNS
MC_FAIR_CLAIM_PER_CAMPAIGN MC_FAIR_CLAIM_TOTAL MC_FAIR_DRR_QUANTUM_PARTS
MC_FAIR_TASK_SCHEDULER_ENABLED MC_FAST_SEND_CHECKPOINT_BATCH MC_FAST_SEND_CHECKPOINT_MS
MC_FAST_SEND_CHECKPOINT_QUEUE MC_FAST_SEND_FAIR_CHECKPOINT_MS MC_FAST_SEND_MAX_DURABLE_BACKLOG
MC_FAST_SEND_STARTUP_REPLAY_TIMEOUT MC_FRONTEND_DIR MC_HOST MC_INGEST_CHUNK_LEASE_SECONDS
MC_INGEST_FILE_LEASE_SECONDS MC_INSTANCE_LOCK MC_LOCAL_E2E_RUN_ROOT MC_LOG_BACKUPS
MC_LOG_LEVEL MC_LOG_MAX_BYTES MC_LOG_PATH MC_MAX_UPLOAD_BYTES MC_PIPELINE_MODE MC_PORT
MC_PROBE_URL MC_PYTHON_BIN MC_REALTIME_COPY_QUEUE_SIZE MC_REALTIME_COPY_WORKERS
MC_REALTIME_DISCOVERY_QUANTUM MC_REALTIME_FILE_RETENTION_DAYS MC_REALTIME_INDEX_BUILD_TIMEOUT
MC_REALTIME_INGEST_RAMP_BURST_SECONDS MC_REALTIME_INGEST_RAMP_ENABLED
MC_REALTIME_INGEST_RAMP_INITIAL_TPS MC_REALTIME_INGEST_RAMP_MAX_TPS MC_REALTIME_INGEST_RAMP_MIN_TPS
MC_REALTIME_INGEST_WORKERS MC_REALTIME_MATCH_CHUNK_SIZE MC_REALTIME_MAX_INBOX_FILES
MC_REALTIME_MAX_INBOX_GB MC_REALTIME_MAX_PENDING_FILES_PER_CAMPAIGN MC_REALTIME_MAX_RETRIES
MC_REALTIME_MSISDN_COLUMN MC_REALTIME_NO_PROGRESS_SECONDS MC_REALTIME_OUTPUT_RETENTION_DAYS
MC_REALTIME_PROGRESS_INTERVAL_SECONDS MC_REALTIME_REQUIRE_READY MC_REALTIME_RESOURCE_RETRY_DELAYS
MC_REALTIME_RETRY_DELAYS MC_REALTIME_WAITING_RETRY_DELAYS MC_REALTIME_WATCH_PENDING_RECHECK_SECONDS
MC_REALTIME_WATCH_RESCAN_SECONDS MC_REALTIME_WATCH_STABILITY_SECONDS
MC_REALTIME_WHITELIST_BUILD_CHUNK MC_REALTIME_WHITELIST_BUILD_MEMORY_LIMIT
MC_REALTIME_WHITELIST_BUILD_THREADS MC_REALTIME_WHITELIST_RETENTION_DAYS
MC_REALTIME_XLSX_MEMORY_EXPANSION_FACTOR MC_REALTIME_XLSX_MEMORY_LIMIT_MB
MC_RESULT_EMERGENCY_BYTES MC_RESULT_JOURNAL_DISK_BYTES MC_RESULT_JOURNAL_INDEX_BYTES
MC_RESULT_LIVE_BYTES MC_RUN_ID MC_RUN_UNDER_SYSTEMD MC_SERVICE_UNIT MC_SKIP_SYSTEMD_CHECK
MC_SMPP_INTERNAL_TPS_MULTIPLIER MC_SMPP_PARALLEL_BIND_REQUIRED MC_SMPP_RESPONSE_ERROR_LOG_INTERVAL
MC_SMPP_SEND_QUEUE_CAPACITY MC_SMPP_WINDOW_SIZE MC_SMS_RESULT_JOURNAL_COMPACT_BYTES
MC_SMS_RESULT_JOURNAL_COMPACT_INTERVAL_SECONDS MC_SMS_RESULT_JOURNAL_FSYNC_BATCH
MC_SMS_RESULT_JOURNAL_FSYNC_MS MC_SMS_RESULT_JOURNAL_PATH MC_SMS_RESULT_JOURNAL_QUEUE_CAPACITY
MC_SMS_SEND_MODE MC_SMS_WRITE_BATCH_SIZE MC_SMS_WRITE_FLUSH_MS MC_SMS_WRITE_MAX_RETRIES
MC_SMS_WRITE_QUEUE_CAPACITY MC_SMS_WRITE_TRANSACTION_SIZE MC_SMS_WRITE_TRANSIENT_LOG_INTERVAL_SECONDS
MC_SOCKET_DRR_QUANTUM_PARTS MC_STALL_DIAGNOSTICS_DIR MC_STALL_DIAGNOSTICS_THRESHOLD_SECONDS
MC_STALL_WATCHDOG_ENABLED MC_STARTUP_REPLAY_TIMEOUT_SECONDS MC_STATE_DIR MC_SYSTEM_MODE
MC_TASK_ARCHIVE_DIR MC_TASK_ARCHIVE_ENABLED MC_TASK_CLEANUP_BATCH MC_TASK_WORKERS
MC_TEMPLATE_WRITE_RETRIES MC_UPDATE_DIR MC_UPDATE_HEALTH_INTERVAL MC_UPDATE_HEALTH_TRIES
MC_UPDATE_KEEP_BACKUPS MC_UPDATE_KEEP_UPLOADS MC_UPDATE_MAX_CHUNK_BYTES MC_UPDATE_MAX_TOTAL_BYTES
MC_UPDATE_MODE MC_UPDATE_RUNNER_LOG MC_UPDATE_STATE_FILE MC_UPDATE_STOP_GRACE MC_VENV_DIR
MC_VNEXT_CANARY_MODE MC_VNEXT_MAX_INFLIGHT MC_VNEXT_MAX_SPOOL_BYTES MC_VNEXT_PARTITIONS
MC_VNEXT_PREFETCH_PER_PARTITION MC_VNEXT_PROJECTION_FLUSH_MS MC_VNEXT_PROJECTION_QUEUE
MC_VNEXT_RUN_ROOT MC_VNEXT_WORKERS MC_WRITE_QUEUE_HIGH_WATERMARK MC_WRITE_QUEUE_LOW_WATERMARK
```
