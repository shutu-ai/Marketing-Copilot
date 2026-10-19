# Marketing Copilot 本地 E2E、故障与压力验收

## 当前结论

本次 Windows 本地真实进程矩阵已达到 `LOCAL_E2E_PASS`。所有短信发送验收场景按当前测试合同使用 `delivery_receipt_mode=disabled`：不请求 DLR，`submit_sm_resp` 是发送结果；SMSC 模拟器仍真实接收 SMPP bind、submit_sm 并返回成功或拒绝。

真实运行组件包括：Marketing Copilot API/主程序、Activity Scheduler、Realtime Ingestion Worker、DuckDB、SMS Task Worker 和 SMPP Gateway。只模拟外部事件文件生产者、SFTP/SMSC；验收器不直接写业务 DuckDB 表。

`LIVE_ENV_UNVERIFIED` 保留：SMSC 是 `127.0.0.1` 模拟器，不代表真实 SMSC 或 SLES 生产验收。

## 交付物

- `backend/tools/local_e2e_acceptance.py`：隔离环境、启动/停止、HTTP 配置、文件投递、等待业务完成、资源采样和三方对账。
- `backend/tools/local_e2e_data.py`：`.part -> 原子 rename -> .ready` 的外部数据生产器。
- `backend/tools/local_e2e_scenarios.json`：quick、1K/10K/100K、拒绝、响应丢失、multipart、重启场景。
- `backend/tools/smsc_simulator.py`：loopback-only SMPP 模拟器，支持 TPS、响应延迟、拒绝比例、响应丢失、断线、multipart 部分成功、DLR 延迟/丢失和 JSONL 接收日志。
- `backend/tests/test_local_e2e_acceptance_tools.py`：工具级故障配置及文件协议自动化测试。
- `Docs/local-e2e-acceptance-results.json`：最近一次 quick 运行的完整机器结果；其余场景结果使用同目录下对应 JSON 文件保存。

## 快速执行

在 Windows PowerShell、仓库根目录执行：

```powershell
& .\.venv\Scripts\python.exe -m unittest backend.tests.test_local_e2e_acceptance_tools -v
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario quick --keep-artifacts --output Docs\local-e2e-acceptance-results.json
```

失败或指定 `--keep-artifacts` 时，诊断目录保存在 `.local-e2e-runs\marketing-copilot-<scenario>-<id>`，包括独立 DuckDB、watch/campaign 文件、API 日志、应用日志、SMSC 日志和 `smsc.events.jsonl`。

## 完整与压力命令

```powershell
# 故障和恢复矩阵
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario reject --keep-artifacts --output Docs\local-e2e-acceptance-reject.json
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario drop-response --keep-artifacts --output Docs\local-e2e-acceptance-drop-response.json
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario partial-submit --keep-artifacts --output Docs\local-e2e-acceptance-partial-submit.json
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario restart --keep-artifacts --output Docs\local-e2e-acceptance-restart.json

# 逐级放大：1K、10K、100K 事件；默认分别为 10、50、100 文件
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario stress-1k --keep-artifacts --output Docs\local-e2e-acceptance-stress-1k.json
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario stress-10k --keep-artifacts --output Docs\local-e2e-acceptance-stress-10k.json
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario stress-100k --keep-artifacts --output Docs\local-e2e-acceptance-stress-100k.json

# 可控 TPS、响应延迟、生产者慢复制和内存保护
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario stress-1k --smsc-tps 25 --producer-chunk-size 50 --producer-chunk-delay 0.02 --max-memory-mb 2048 --keep-artifacts

# 稳定性运行：示例 30 分钟；60 分钟将 duration 改为 3600
& .\.venv\Scripts\python.exe backend\tools\local_e2e_acceptance.py --scenario quick --duration-seconds 1800 --smsc-tps 20 --keep-artifacts --output Docs\local-e2e-acceptance-stability-30m.json
```

CLI 支持 `--task-workers`、`--ingest-workers`、`--workers`、`--claim-batch`、`--smpp-window`、`--timeout-seconds`、`--events`、`--files` 和 `--scenario-file`。100K 长跑应在专用工作目录执行并设置资源阈值；没有实际运行的场景不得标记 PASS。

## 三方对账与指标

每个场景独立产生 PASS/FAIL/BLOCKED。结束子进程后才打开只读 DuckDB 审计连接，避免验收观察器与真实 Worker 争用 DuckDB 写锁。对账来源为：

1. DuckDB：`ingestion_files`、`sms_tasks`、`sms_send_attempts`、`sms_message_bindings_v2`、`task_events`。
2. SMSC JSONL：bind、submit_sm、submit 状态、目标号码、响应丢失、DLR 事件。
3. 业务事件/API：文件完成、任务终态、重启后的提交次数和恢复日志。

报告记录输入文件/行数、有效事件、任务数、submit_sm 总数与成功/拒绝、DLR 违规、重复发送、丢失任务、错投递、未知结果重试违规、队列积压、清空时间、文件发现到入库、入库到任务创建、任务创建到 submit_sm_resp 的 P50/P95/P99，以及采样 CPU、RSS 和恢复证据。

无回执模式下不要求 message-id 映射；响应丢失场景的 SMSC message-id 仅记录为 `unknown_submit_ids`，任务必须为 `unknown` 且不得普通重发。multipart 部分成功必须为 `partial`，即使已成功分片的后续 DLR 到达也不能晋级为 `delivered`。

## 已实际执行结果

环境：Windows 11 主机，Python `.venv` 3.13.15，DuckDB 1.2.0；源码提交 `6750dc825a01311708790666d99e5294a21f36a5`；system mode 为 `production`；SMPP 目标均为随机临时 `127.0.0.1` 端口。每次运行使用独立目录、数据库、端口和日志。

| 场景 | 结果 | 关键实测结果 |
|---|---|---|
| quick | PASS | 5 输入行，3 有效任务，3 submit_sm 成功，0 拒绝，0 DLR，0 重复/错投递 |
| reject | PASS | 5 任务，5 submit_sm，0 成功，5 拒绝，全部 `failed`，无普通重试 |
| drop-response | PASS | 3 任务，3 submit_sm 均在 SMSC 接收，响应丢失，全部 `unknown`，无自动重发 |
| partial-submit | PASS | 1 个 6 分片任务，1 成功/5 拒绝，终态 `partial`，未被后续状态改成 `delivered` |
| restart | PASS | 5 任务，5 submit_sm，API 重启后无重复提交 |
| 工具自动化 | PASS | 3/3 unittest 通过 |
| WSL AlmaLinux-8 | BLOCKED_ENV | WSL2 可启动、Python 3.11.13 存在，但 DuckDB/FastAPI/Uvicorn/smpplib/paramiko/pandas 均缺失，未运行真实服务矩阵 |

quick 最近一次结果中的实际延迟为：文件发现到入库 P50/P95/P99 `379/379/379 ms`，入库到任务创建 `1410/1410/1410 ms`，任务创建到 submit_sm_resp `754.877/914.265/928.433 ms`；submit_sm 吞吐 `6.101/s`，最大/最终队列积压 `3/0`，采样最大 RSS `10,338,304 bytes`。这些是当前 Windows 环境观测值，不是预先定义的 SLA。

## WSL 判定

应当在 WSL 再跑一次，因为它更接近 SLES/Linux 的路径、原子 rename、信号、进程生命周期和 DuckDB 锁行为。本次已发现 `AlmaLinux-8` WSL2，但由于依赖缺失，只能标记 `BLOCKED_ENV`。准备好 Linux 虚拟环境后执行：

```bash
cd /mnt/d/dev-projects/MarketingCopilot
python3 -m venv .venv-linux
.venv-linux/bin/pip install -r backend/requirements.txt
.venv-linux/bin/python backend/tools/local_e2e_acceptance.py --scenario quick --keep-artifacts
```

该 WSL 结果必须单独记录，不能用 Windows 的 `LOCAL_E2E_PASS` 覆盖 WSL 的 `BLOCKED_ENV`。真实 SMSC、SLES 生产网络和真实短信发送仍属于 `LIVE_ENV_UNVERIFIED`。

## 问题清单

- 已修复：multipart 场景中，部分分片失败后，先成功分片的 DLR 会把 `partial` 错误晋级为 `delivered`。当前实现保留 `partial` 终态；本地真实故障场景已复跑通过。
- 已修复：验收器在业务进程运行期间直接打开 DuckDB 会干扰真实 Worker；当前运行态只用 API 统计，服务停止后再做只读对账。
- 已知环境限制：AlmaLinux-8 WSL2 缺失项目运行依赖，因此 WSL 真实矩阵为 `BLOCKED_ENV`，不是 PASS。
- 未声称：真实 SMSC、真实 SLES 12 SP5 部署和真实短信发送均未验证。
