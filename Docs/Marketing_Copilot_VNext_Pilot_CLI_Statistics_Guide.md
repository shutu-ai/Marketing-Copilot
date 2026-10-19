# Marketing-Copilot VNext 试运行 CLI 统计指南

适用对象：1–2 个真实 Campaign 的受控上线观察窗口。

工具：`bin/marketing-copilot-cli stats`（底层为 `backend/tools/vnext_stats.py`，仅 Python 标准库）

## 1. 先决条件

- 运行时通过现有 `marketing-copilot.service` 启动；不要为本 CLI 新建服务。
- CLI 与运行时使用同一个运行根目录：优先读取 `MC_VNEXT_RUN_ROOT`，未设置时使用
  `MC_STATE_DIR/vnext`（标准部署通常为 `/opt/marketing-copilot/data/vnext`）。
- CLI 只需要对运行目录的**读权限**，不需要数据库、API 或 SMSC 凭据。
- 不要把 `MSISDN`、短信正文或 SMSC 密码写入统计文件或终端重定向文件。

标准部署默认路径：

```bash
/opt/marketing-copilot/data/vnext
```

## 2. 命令

```bash
# 单次快照，适合 SSH 排障
/opt/marketing-copilot/bin/marketing-copilot-cli \
  stats

# 默认每 2 秒刷新，Ctrl+C 退出
/opt/marketing-copilot/bin/marketing-copilot-cli \
  stats --watch

# 只看两个 Campaign
/opt/marketing-copilot/bin/marketing-copilot-cli \
  stats --watch \
  --campaign batch:101 --campaign batch:102

# 机器可读（单次一条 JSON；watch 为 NDJSON，每行一条）
/opt/marketing-copilot/bin/marketing-copilot-cli \
  stats --once --json

# 自定义刷新频率 / 禁用 ANSI
/opt/marketing-copilot/bin/marketing-copilot-cli \
  stats --watch --interval 1 --no-color
```

`--root` 通常不需要填写。CLI 会自动沿用服务的运行根目录；只有观察非默认目录或历史
Pilot 目录时才需要显式指定。如果旧环境变量指向已不存在的目录，CLI 会自动回退到标准
默认目录。直接执行
`backend/tools/vnext_stats.py` 仍可用于源码目录排障，但部署包中的正式入口是
`marketing-copilot-cli stats`。

非 TTY 环境自动输出纯文本并保留历史滚动；TTY 下默认清屏。

`--json --watch` 不清屏，适合 `jq`、日志采集或脚本。

## 3. 输出字段

```text
VNEXT | run=<run_id> | HH:MM:SS | UPDATED <age> ago | state=RUNNING
Ingress      accepted=...    rejected=...     paused-by-backpressure=NO
Spool        pending=...     disk=... MB       source=lightweight-snapshot
Workers      active=8/8      taken=...         inflight=...
SMPP         binds=4/4       written=... PDU   observed_total=... PDU
SMPP rates   socket_write=... PDU/s   journal_durable=... PDU/s
Responses    observed=...    durable=...     unknown=...  pending_durable=...
Journal      durable=...     backlog=...      oldest_undurable=... ms
Projection   applied=...     lag=...          queue=...     caught_up=...
Campaign     Status    Accepted  SocketWritten  RespOK  RespErr  Unknown  Projected
```

定义：

- `accepted`：Spool 中已接收的历史记录数；`pending` 是当前业务日尚未 consume 的记录。
- `taken`：已经 durable checkpoint 的任务数；不代表已发送成功。
- `written`：durable `socket_written` PDU 数；**不等于** `submit_sm_resp_success`。
- `observed`：Reader 已解析的 `submit_sm_resp`；可能仍在持久化队列中。
- `durable`：已经 Journal `append_many + fsync` 成功的响应事件。
- `pending_durable`：已观察/已入队但尚未 fsync 的事件数；进程崩溃时这些事件可能丢失。
- `unknown`：Reader 断开、超时或无法匹配的 PDU；**不等于 delivered**。
- `Projected` / `projected`：当前版本未提供逐 Campaign Projection 计数，显示 `N/A`。
- `socket_write_rate_pdu_per_second`：Worker/Transport 快照中 Socket 写出路径的
  `socket_written_parts` 计数器 5 秒滚动差分；它表示发送侧实际观测，不表示
  SMSC 接受成功或终端送达。
- `journal_durable_rate_pdu_per_second`：Journal `append_many + fsync` 成功后的
  `durable_socket_written_pdus` 计数器 5 秒滚动差分；它表示持久化进展，允许
  短时落后于 Socket 写出。
- `response_observed_rate_pdu_per_second`：Reader 观察到的
  `submit_sm_resp` PDU 计数器 5 秒滚动差分。
- `rate_pdu_per_second`：兼容旧脚本的别名，保持原 durable socket-written 语义；
  新脚本应使用上面三个明确字段。
- 计数器按 Run ID、Worker 进程 PID/代际和 Worker 实例识别重置；快照缺失、
  stale 或代际切换时显示 `N/A`，并在 `rate_unavailable_reasons` 说明原因，
  不把缺失数据填成 0。
- `caught_up`：Projection 已提交到 Journal EOF 且无 pending 事件。
- `STALE`：Worker/Projection 状态文件超过 `max(5s, 3×interval)` 未更新。
- `OFFLINE`：Runtime 未运行或进程已停止；仍会显示 durable 聚合计数。
- `BLOCKED`：Journal/Progress writer 报错或存在 unresolved 事件。
- `BACKPRESSURE`：发送因进度/Journal 容量暂停；不是数据丢失。

`incomplete_metrics` 明确列出无法可信计算的字段；不要用 `0` 解释 `N/A`。

JSON 的 `workers.checkpoint_count` 与 `workers.control_counters.{pause,resume,cancel}`
来自 Worker 心跳，用于观察 checkpoint 和人工控制动作；human 输出保留主要指标。

## 4. JSON 结构

每条 JSON 至少包含：

```json
{
  "schema_version": 1,
  "observed_at": 1790000000.0,
  "observed_at_iso": "2026-10-19T10:00:00+03:00",
  "snapshot_age_seconds": 0.8,
  "run_id": "pilot-001",
  "source": "worker-status+durable-aggregate+projection-status",
  "stale": false,
  "incomplete_metrics": [],
  "runtime": {"state": "RUNNING", "started": true},
  "spool": {"pending": 0, "disk_mb": 12.3},
  "workers": {"active": 8, "configured": 8, "inflight": 0},
  "smpp": {
    "written_pdus": 1000,
    "socket_write_rate_pdu_per_second": null,
    "journal_durable_rate_pdu_per_second": null,
    "response_observed_rate_pdu_per_second": null
  },
  "socket_write_rate_pdu_per_second": null,
  "journal_durable_rate_pdu_per_second": null,
  "response_observed_rate_pdu_per_second": null,
  "rate_pdu_per_second": null,
  "rate_unavailable_reasons": ["first_sample"],
  "responses": {"observed_success": 1000, "durable_success": 1000},
  "journal": {"durable_events": 2000, "fsync_errors": 0},
  "projection": {"applied": 2000, "caught_up": true},
  "campaigns": [{"campaign_type": "batch", "campaign_id": 101}],
  "health": "OK",
  "backpressure": "NONE"
}
```

## 5. 数据来源与开销

CLI 只读取：

- `progress/worker-*.status.json`（Worker 低频心跳，默认 1 s）
- `progress/worker-*.progress-aggregate.json`（fsync 成功后的持久 Campaign 聚合）
- `projection/projection-writer-status.json`
- `spool/manifest.json` + `spool/cursors/` + `spool/taken/` 小型 JSON
- `control-state.json`、`diagnostics/processes/*.json`

CLI **不会**：

- 调用 `VNextRuntime.status()` 或 `TaskSpool.status()`；
- 扫描 Journal segment 内容或 Spool segment 内容；
- 打开/查询 DuckDB；
- 写入运行目录或修改发送状态。

因此 CLI 的 watch 频率不会随短信量成倍增加发送负担。建议正式 A/B 仍按任务书
测量 `CLI OFF / ON` 的配对吞吐差异；本轮 Windows 工作区未测量该百分比。

## 6. 1–2 个有限 Campaign 试运行操作单

> 以下仅为试运行操作说明，不代替上线授权。自动测试只允许 `127.0.0.1`
> loopback / synthetic SMSC；不得主动触达真实 SMSC 或生产号码。

### 6.1 启动

```bash
sudo systemctl edit marketing-copilot.service   # 或编辑 /etc/marketing-copilot.env
sudoedit /etc/marketing-copilot.env
```

确认至少：

```text
MC_PIPELINE_MODE=vnext
MC_VNEXT_RUN_ROOT=/opt/marketing-copilot/data/vnext/pilot-001
MC_VNEXT_WORKERS=8
MC_VNEXT_PARTITIONS=256
MC_VNEXT_MAX_INFLIGHT=128
MC_VNEXT_PREFETCH_PER_PARTITION=64
MC_VNEXT_PROJECTION_QUEUE=8192
```

```bash
sudo systemctl restart marketing-copilot.service
sudo systemctl status marketing-copilot.service --no-pager
python3 backend/tools/vnext_stats.py --root /opt/marketing-copilot/data/vnext/pilot-001 --once
```

### 6.2 观察

```bash
python3 backend/tools/vnext_stats.py \
  --root /opt/marketing-copilot/data/vnext/pilot-001 \
  --watch --interval 2 \
  --campaign batch:101 --campaign batch:102
```

判定顺序：

1. `state=RUNNING`，`Workers active` 与配置一致；
2. `Spool pending` 下降，`SMPP written` 增长；
3. `Responses observed` 与 `durable` 最终相等，`pending_durable=0`；
4. `Journal backlog` 下降，`Projection caught_up=YES`；
5. `Health=OK`，`Fsync errors=0`，`unresolved_events=0`。

### 6.3 人工暂停 / 恢复 / 取消

API 默认监听 `19090`（以实际部署为准）：

```bash
API=http://127.0.0.1:19090

# 暂停 A，B 继续
curl -fsS -X POST "$API/api/vnext/campaigns/batch/101/pause"

# 恢复 A
curl -fsS -X POST "$API/api/vnext/campaigns/batch/101/resume"

# 取消 A（A 尚未 socket write 的记录会 durable 地 expire）
curl -fsS -X POST "$API/api/vnext/campaigns/batch/101/cancel"

# 查看控制状态
curl -fsS "$API/api/vnext/status" | python3 -m json.tool | head -80
```

暂停后 CLI 应显示 A 的 `Status=PAUSED`、B 的 `Status=RUNNING`；A 的
`Spool pending` 可以保留，但不应继续增加 `written`。

### 6.4 正常退出

```bash
sudo systemctl stop marketing-copilot.service
python3 backend/tools/vnext_stats.py --root /opt/marketing-copilot/data/vnext/pilot-001 --once
```

退出后应满足：

- `Spool pending=0`（若仍有业务活动，需先排空或人工取消）；
- `pending_durable=0`；
- `unresolved_events=0`；
- `Projection caught_up=YES` 或明确显示 Projection 不可用。

### 6.5 故障恢复

1. CLI 显示 `Health=BLOCKED` 或 `Fsync errors>0` 时，先停止新发送：

```bash
curl -fsS -X POST "$API/api/vnext/campaigns/batch/101/pause"
curl -fsS -X POST "$API/api/vnext/campaigns/batch/102/pause"
```

2. 查看 durable 失败证据（只包含事件/字节计数与原因，不含号码或正文）：

```bash
ls -l /opt/marketing-copilot/data/vnext/pilot-001/progress/*.failure.json
cat /opt/marketing-copilot/data/vnext/pilot-001/progress/worker-0.progress-aggregate.failure.json
journalctl -u marketing-copilot.service -n 200 --no-pager
```

3. 对账确认这些事件是否需要补录/人工处理；**不要直接重新发送**已 checkpoint 的任务。

4. 只有在确认失败批次不会因重放产生重复后，才清除对应 failure marker：

```bash
sudo mv /opt/marketing-copilot/data/vnext/pilot-001/progress/worker-0.progress-aggregate.failure.json \
  /opt/marketing-copilot/data/vnext/pilot-001/progress/worker-0.progress-aggregate.failure.json.reconciled
sudo systemctl restart marketing-copilot.service
```

5. 重启后重新跑 CLI 单次快照，确认 `unresolved_events=0`、`Fsync errors=0`，
   再决定是否恢复 Campaign。

## 7. 常见问题

| 现象 | 处理 |
| --- | --- |
| `runtime root does not exist` | 检查 `--root` 与 `MC_VNEXT_RUN_ROOT` 是否一致 |
| `state=NOT_STARTED` | Runtime 未启动；CLI 仍可显示聚合/历史文件的可用部分 |
| `state=OFFLINE` | 进程已停止或崩溃；先看 `diagnostics/processes/*.json` 与 journal |
| `STALE` | 状态文件超过阈值未更新；检查 Worker/Projection 进程和磁盘 |
| `pending_durable` 持续上升 | Journal 写入变慢；检查磁盘、权限、fsync 延迟和 Journal 容量 |
| `backpressure != NONE` | 正常保护动作；不要手工放大队列，先查 Journal/磁盘/Projection |
| CLI 被 Ctrl+C | 只退出观察者，不会停止或改变 Sender |
| JSON 行解析失败 | 检查是否混入了非 JSON 日志；`--json` 模式只向 stdout 写 JSON |
