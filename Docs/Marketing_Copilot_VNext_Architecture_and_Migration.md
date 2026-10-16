# Marketing Copilot VNext Architecture and Migration

任务：`MC-VNEXT-SHARD-ASYNC-REPLACE-20261016`
冻结基线：`5650b8fc6bcdd620858258db3937cd5c5ad59d14` (`main`)
V0 状态：`BASELINE_RECORDED`

## 1. 目标与不变量

VNext 只有一条新的发送主链：

```text
Input → Chunk/normalize → MSISDN virtual shard → durable task spool
      → shard worker process → async SMPP socket → compact progress
      → dedicated projection writer → DuckDB projections
```

本版本的业务完成口径是 `socket_written`：操作系统 socket write 成功返回；它不等于
`submit_sm_resp_success`，也不等于 SMSC delivered。写出结果不明的任务默认不自动重发，
因此允许崩溃窗口漏发但不会制造重复发送。`queued`、`taken`、`socket_written`、
`write_failed`、`response_success`、`response_error` 和 `unknown` 均独立计数。

必要不变量：

1. 最终标准化号码决定分片；同一号码在一个 deployment 内始终进入同一个虚拟分片。
2. 分片是逻辑资源，不与 PID 绑定；worker 扩缩容通过版本化 map 和 owner epoch 接管。
3. Spool 是待处理任务的权威边界；只有 group fsync 完成的记录才算 durable enqueue。
4. Worker/Router 不打开共享生产 DuckDB；发送路径不执行逐短信 DB transaction。
5. ProjectionWriter 是唯一的 VNext DuckDB 写 Owner；投影滞后只产生独立背压。
6. Route TPS/PDU window 是全局共享配额，不随 worker 数量倍增。
7. pause/cancel 只阻止新的 socket write，已写出的 PDU 不撤回。
8. 所有测试使用合成号码和 loopback SMSC；不会连接真实 SMSC 或生产 API。

## 2. V0 基线与边界

远端 `origin/main` 与工作区 HEAD 均为上述 SHA。开始时工作区已有大量与本任务无关的
dirty 文件、数据库、测试证据和离线包；它们不被覆盖、不纳入本任务提交。V0 相关回归
测试在 Windows 快速环境执行，结果为 `41 passed, 1 skipped`，覆盖现有 SMPP async/
encoding/routes、PDU window、批量流式和多 Campaign 公平入口。

上一轮已冻结的 WSL/EulerOS 对照仍然有效且不被本任务改写：P100、P1K、OCC 通过，
旧链路 P5500 在 WSL 与 EulerOS 均未完成；历史总体结论仍是 `INTEGRATED_NO_GO`。新链路
的 `socket_written` 门禁与旧的 `accepted` 门禁分开记录。

## 3. 复用、替换与删除映射

| 领域 | 复用 | VNext 替换 | 迁移/删除边界 |
| --- | --- | --- | --- |
| 号码规则 | 现有 `phone_prefix` 配置语义和输入清洗规则 | `backend/vnext/partition.py` 的最终号码规范化与 SHA-256 shard | 不再使用 Python `hash()`；旧发送路径不调用新 shard |
| 文件解析 | Batch/Realtime 的 CSV/XLSX/SFTP 解析、模板选择、源去重 | `backend/vnext/ingress.py` 的 chunk→spool 入口 | 不再等待完整 Manifest 或固定 3 秒轮询才允许发送 |
| 待发送任务 | 现有业务字段作为迁移输入 | append-only segment spool、manifest、cursor、CRC | 不把 `sms_task_service.claim_next_tasks` 接入新 worker |
| SMPP | `smpplib` 编码知识、Route 配置、loopback simulator | 独立 asyncio transport/reader/window/credits | 新路径不调用旧 `task_worker`、`record_send_intent` 或逐条 result journal |
| 状态投影 | 现有 UI/API 字段作为兼容读模型 | compact progress + ProjectionWriter 批量投影 | `accepted/delivered` 不映射成 `socket_written` |
| 背压 | 现有容量阈值作为迁移参考 | 每 stage×partition×campaign 的 `StageCredit` 和 watchdog | 不再依赖全局 3 秒调度轮询恢复 |
| 生命周期 | instance lock、健康检查、配置/认证路由 | `IngressRouter / WorkerSupervisor / ProjectionWriter` | V5 从 `main.py` 移除旧 Scheduler+TaskWorker+Journal 启动组合 |

旧的 `reliable/fast/fair/serialized/occ` 组合只作为历史回滚资料和迁移参考；VNext 不
新建 adapter 去长期维持两套发送状态机。迁移前必须停止旧输入、冻结旧 Sender、审计
未完成任务；不自动把旧 `sending/unknown/partial` 转成可发送任务。

## 4. 进程与文件所有权

| Owner | 进程/对象 | 唯一写入内容 |
| --- | --- | --- |
| IngressRouter | API/源解析进程 | 每个 shard spool 的 append；通过单写者队列进入 |
| WorkerSupervisor | 父进程 | partition-map、owner lease/epoch、worker control channel |
| ShardWorker | 每个 worker 子进程 | 自己的 dispatch cursor、compact progress channel |
| AsyncSmppTransport | worker 内 event loop | 自己的 socket、sequence/window reader |
| ProjectionWriter | 独立子进程 | VNext projection DuckDB 和 progress watermark |
| Control plane | supervisor/Writer | pause/cancel generation、route credit leases |

所有运行时文件必须在本轮独立 Run 目录；源代码、venv、DuckDB、Spool、WAL、日志和
临时文件不放入 Git。单机初始 256 virtual shards、4 workers；map revision 创建后不
在线修改，扩缩容只做安全冻结后的 owner 接管。

## 5. 阶段门禁

| 阶段 | 必须交付 | 通过证据 |
| --- | --- | --- |
| V0 | 基线、合同、映射 | 本文、基线回归记录 |
| V1 | partition/spool/supervisor | shard 稳定性、CRC/recovery、无双 owner、crash injection |
| V2 | worker/async SMPP/route credits | P10/P100/P1K socket audit、response reader、window/TPS/断连隔离 |
| V3 | batch/realtime/SFTP ingress | 4 活动 2×2500+2×250 持续入 spool、重输入去重、公平准入 |
| V4 | pressure/projection/metrics/recovery | Writer 30–60s 堵塞恢复、局部背压、崩溃接管、pause/cancel |
| V5 | main/API/UI/deploy 切换 | 单一启动链、业务功能清单、旧引用静态审计、离线包与最终报告 |

每阶段报告代码变更、删除的兼容路径、测试、对账口径和已知限制。任一 Gate 未通过时
不得宣称 `VNEXT_PIPELINE_READY_FOR_CANARY`。
