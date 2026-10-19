# Marketing-Copilot VNext Limited Pilot Runbook

状态：`CONDITIONAL`

适用范围：1–2 个低风险 Campaign 的受控、限时 Pilot 申请。

本手册不是生产发送授权，也不包含真实 SMSC 凭据、真实号码或生产路径。真实 Pilot 必须由业务负责人、SMSC/网络运维负责人和平台运维负责人另行批准。

## 1. Pilot 前置条件

### 必须确认

- 目标 OS、文件系统、源代码 SHA、Python runtime 和 dependency freeze 已记录。
- 运行目录位于目标 Linux 原生 ext4；不要使用 `/mnt/<drive>` 结果作为 Linux 性能基线。
- 使用独立的 venv 和独立的 data/Journal/projection/run root，不复用其他实例。
- 已确认 4 Bind、每 Bind 200 TPS、Window 50 的实际配置及 `per_bind` scope；不能把 200 当作全局 TPS，也不能擅自扩大 TPS/Window 来追求更高结果。
- 已确认应用 RSS、run bytes、Journal capacity、磁盘剩余空间和 fd/thread 限制。
- SMSC 连接、账号、号码范围、时间窗口、责任人和停止责任已由有权限的运维团队确认。
- 只选 1–2 个低风险 Campaign，明确预计 task 数、开始/结束时间、暂停条件和人工对账人。

### 禁止事项

- 不连接未经明确授权的真实 SMSC，不发送真实手机号，不改生产服务或 systemd unit。
- 不把 synthetic loopback、1K/5K 或 600 s steady 直接写成生产容量或 SLA。
- 不删除或覆盖旧的 Journal、数据库、日志、备份、uploads 或 unknown/unresolved 证据。
- 不通过增大队列、放宽 window、自动 retry 或回滚脚本掩盖背压、unknown 或 durable 问题。
- 不把 `response_success` 当作 `socket_written`，不把 `rate_pdu_per_second` 当作 Socket write rate；CLI 中必须分别看 Socket、Journal durable 和 response observed。

## 2. 变量和启动

以下示例使用测试服务器独立 runtime；正式 Pilot 必须替换为已审批的 staging/Pilot 路径，并由运维确认权限：

```bash
export MC_PYTHON=/home/paas/marketing-copilot-test/python/bin/python3.11
export MC_VENV=/home/paas/marketing-copilot-test/runs/p1-sles-20261019-standalone/venv
export MC_VNEXT_RUN_ROOT=/home/paas/marketing-copilot-test/runs/pilot-<id>
export MC_STATS="$MC_VENV/bin/python /path/to/backend/tools/vnext_stats.py"
```

创建新目录后，先记录：

```bash
date -u
df -T .
uname -a
"$MC_VENV/bin/python" --version
"$MC_VENV/bin/python" -m pip freeze > "$MC_VNEXT_RUN_ROOT/dependency-freeze.txt"
```

只允许使用 loopback/synthetic SMSC 的验证命令进行工程验收。真实 Pilot 的应用启动命令、SMSC host/port 和凭据由已授权运维系统注入，不能写入此手册或脚本。

## 3. CLI 观察

启动只读 CLI：

```bash
"$MC_VENV/bin/python" /path/to/backend/tools/vnext_stats.py \
  --root "$MC_VNEXT_RUN_ROOT" \
  --watch --interval 2 --json --no-color \
  > "$MC_VNEXT_RUN_ROOT/stats.ndjson"
```

实时关注以下字段：

```text
health
socket_write_rate_pdu_per_second
journal_durable_rate_pdu_per_second
response_observed_rate_pdu_per_second
smpp.socket_written_observed_pdus
smpp.durable_socket_written_pdus
smpp.response_observed_pdus
smpp.pending_durability
journal.backlog_bytes
journal.writer_errors
journal.unresolved_events
projection.caught_up
projection.projection_lag_seconds
incomplete_metrics
rate_unavailable_reasons
```

解释规则：

- Socket write 是发送侧已执行的 PDU 写出，不表示 SMSC 业务成功。
- Journal durable 是 fsync 后可恢复的发送进度，不要求每个短时间窗口都与 Socket rate 相等。
- response observed 是 Reader 观察到的 `submit_sm_resp`，不等于 durable。
- `null`/`N/A` 必须连同 `incomplete_metrics` 或 `rate_unavailable_reasons` 一起保留，不能改成 0。
- `Projected=N/A` 时不要在 CLI 中补写“已完成”；使用已有的 durable/response/audit 证据人工对账。

CLI 只读，不调用 Web UI/FastAPI/DuckDB，不扫描 Journal 内容；运行前后应确认 run root 没有新增或修改文件（CLI 输出应重定向到 run root 之外或单独审计目录）。

## 4. 开始前检查清单

按以下顺序记录到 Pilot 运行单：

1. 记录负责人、审批号、Campaign ID、预计 task 数和窗口。
2. 确认旧 Campaign/旧 worker 没有继续发新 PDU，旧 epoch 已退出。
3. 确认 `pending_durability=0`、`unresolved_events=0`、`writer_errors=0`、磁盘空间充足。
4. 确认 CLI 能读取 run root，首次 `N/A` 有明确原因，不把首次采样当成零速率。
5. 先提交很小的 canary batch，核对 `accepted → socket_written → response → durable → projection`。
6. canary 对账通过后，才开始受控 Campaign 窗口。

## 5. 运行和阈值

工程上建议使用以下停止阈值；最终值必须由业务/运维批准：

| 观察项 | 暂停新发条件 |
| --- | --- |
| writer error | 任意非零，立即暂停 |
| unresolved/unknown | 任意新增，立即暂停并人工对账 |
| pending durability | 持续增长且未在批准窗口内回落 |
| Journal backlog | 连续采样上升或接近硬容量 |
| projection lag | 长时间无法追平，或 projection 进程异常退出 |
| response | response timeout/error/unknown 超出批准阈值 |
| Socket rate | 明显低于配置预期且不是已知 TPS/window 限制 |
| RSS/run bytes/disk | 达到 guard 或磁盘剩余量低于运维阈值 |
| worker/process | worker 退出、重启、生命周期异常 |

暂停只停止新发入口，不撤销已 socket-written PDU。不要用 retry 把不确定发送重新投递。

## 6. Campaign 控制

当前源码验证过的 API 形式为：

```text
POST /api/vnext/campaigns/batch/<campaign_id>/pause
POST /api/vnext/campaigns/batch/<campaign_id>/resume
POST /api/vnext/campaigns/batch/<campaign_id>/cancel
GET  /api/vnext/status
```

真实 Pilot 使用前必须以部署版本的 OpenAPI/路由实现为准，不能仅凭本手册猜测接口。

### 暂停

1. 记录当前 CLI JSON、run root、Campaign counters 和责任人。
2. 调用 pause，确认该 Campaign 不再新增 task/PDU；保留已经 `socket_written` 的 PDU。
3. 继续观察其他 Campaign，确认它们没有被误暂停。
4. 检查 pending durability、Journal 和 projection 是否能继续 drain。

### 恢复

1. 只有暂停原因已消除、writer/projection/response 均健康时才 resume。
2. 先 resume 一个小窗口，检查 accepted/socket/response/durable 对账，再恢复计划批量。
3. 恢复期间如果出现 unknown/unresolved，不自动再次发送，转人工对账。

### 取消

1. 取消只作用于尚未 socket-written 的 task；不得声称它撤回了已写出的 PDU。
2. 保存 cancel 前后的 Campaign summary、event id 关联和 CLI 快照。
3. 确认取消 task 进入 `cancelled`，未产生新的 socket PDU，其他 Campaign 仍可继续。

## 7. 退出和人工对账

结束窗口后，等待并记录：

```text
accepted task count
socket_written PDU count
submit_sm_resp success/error/unknown
durable socket_written PDU count
writer error/unresolved/pending durability
Journal backlog and oldest age
projection applied/caught-up/lag
duplicate destination PDU count
worker exit and resource peaks
```

按 Campaign 和 event id/PDU 关联核对：

```text
accepted = socket_written + cancelled/expired/blocked（按实际状态拆分）
socket_written = response_success + response_error + unknown（按实际 response 证据拆分）
durable >= 已报告的可恢复发送事件
projection 只作为已追平的独立证据，不能替代 Socket/Journal 对账
```

出现 unknown/unresolved 时，先保存原始证据，标记人工处理项；不要通过恢复脚本删除、重置 cursor 或重投。

## 8. 回滚和故障处理

故障顺序固定为：

1. 停止新的 intake/dispatch。
2. 保存 CLI NDJSON、状态快照、Journal/aggregate/checkpoint 路径和进程诊断。
3. 让已观察到的 response、Journal writer 和 projection 完成安全 drain；超过批准超时则保持暂停。
4. 仅在运维批准下处理服务 restart/版本回退；回退前确认旧 worker epoch 已完全退出。
5. 重启后先读取 durable aggregate 和 Journal offset，核对是否存在 unknown；禁止把不确定项自动重发。
6. 对仍未解决的 task 建立人工清单，包含 Campaign、event id、PDU、时间和当前状态。

回退不等于重投。`checkpoint-before-socket`、`unknown-never-resend` 和 at-most-once 约束优先于“尽量补发”。

## 9. 本轮结果对应的边界

本轮证据支持：目标 SLES/ext4 运行时、1K/5K synthetic 对账、writer 独立性、Journal backpressure contract、Campaign 控制和 600 s steady。

本轮证据不支持：真实 SMSC 业务成功率、生产号码投递、800 PDU/s 饱和容量、完整 per-campaign projection、直接 Full release GO。

Pilot 结束后，只有在业务负责人和运维负责人审阅 unknown/unresolved、资源、磁盘、projection 和人工对账结果后，才可以决定是否扩大窗口。
