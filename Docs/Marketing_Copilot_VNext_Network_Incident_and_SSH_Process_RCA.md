# Marketing Copilot VNext — Network Incident and SSH Process RCA

任务：`MC-VNEXT-NETWORK-INCIDENT-SERVER-PERF-RECHECK-20261018`

执行结论：`NETWORK_EVENT_NOT_CAUSAL`。本次服务器复测全部在 SLES 节点本地 loopback 上由 detached 进程完成；客户端 VPN/SSH 和 GitHub 443 只影响控制面，不承载 SMPP 测试消息。历史 Windows 日志确实显示过 VPN 拨号失败，但失败窗口不覆盖历史慢轮；当前复测仍落在历史慢轮区间，且没有 SSH 会话生存期依赖。因此不能把本地网络事件认定为吞吐下降根因。

性能仍未恢复到历史快轮的 528.2 PDU/s。当前正式 50K 为 168.747 wall PDU/s，接近历史慢轮 161.9 PDU/s；共享主机 load、磁盘余量和 SMPP credit/window 调度仍是候选干扰，但本轮没有 I/O wait 原始采样，不能进一步声称唯一根因。

## 1. 版本、环境和边界

- `HEAD=3e73ab8030b46b78e109cbf1f23e330c72d2d136`，`origin/main` 相同。
- 后端源码相对 HEAD 无改动；只使用 `.tmp` 中的 detached 启动脚本和服务器隔离 run root。生产 TPS、Window、RouteCredit、Writer、DuckDB durability 和 at-most-once 语义均未修改。
- 服务器：`paas-core01`，SUSE Linux Enterprise Server `12 SP5`，kernel `4.12.14-122.307-default`，x86_64，`/dev/sda1 ext4`。
- CPython `3.11.4`，DuckDB `1.2.0`；依赖 freeze 为 `duckdb==1.2.0`、`numpy==1.26.4`、`pandas==2.3.2`、`pyarrow==12.0.1` 等，freeze SHA-256 为 `502422a5a1be6e2cea2e0ae206c01940b3104c6d906545d8219cdbda72a04227`。
- 本轮执行源包（`backend/tools/vnext_streaming_acceptance.py` 和 `backend/vnext/*`）SHA-256：`b6d71818e5a0b3e701c2eca775c19a604bcdc2d797d83932176ce1acad7d473d`。
- 运行根目录均位于服务器 native ext4；禁止真实 SMSC、真实号码、生产 DB/API。

## 2. R0 网络时间线

Windows Event Log 的 `RasClient` 事件已转换为 UTC。历史快/慢结果的 epoch 时间来自各自 raw JSON。

| UTC 时间 | 证据 | 含义 |
|---|---|---|
| 2026-10-18 10:00:55 | VPN 连接成功（RasClient 20221–20225） | 控制面恢复 |
| 2026-10-18 10:17:02–10:18:36 | 历史快 50K，94.671 s | VPN 事件未覆盖该运行 |
| 2026-10-18 11:07:42 | VPN 终止，reason 829 | 后续控制面事件 |
| 2026-10-18 11:31:22–11:31:24 | VPN 拨号失败，RasClient 20227 | 网络故障证据 |
| 2026-10-18 11:33:06–11:33:08 | VPN 再次拨号失败，RasClient 20227 | 网络故障证据 |
| 2026-10-18 11:35:09–11:35:10 | VPN 连接成功 | 故障窗口结束 |
| 2026-10-18 11:52:08–11:57:17 | 历史慢 50K，308.882 s | 在 VPN 恢复约 17 分钟后开始，无 VPN 事件与其重叠 |
| 2026-10-18 16:56:56–17:01:53 | 本轮正式 50K detached | SSH 窗口不承载测试生命周期 |
| 2026-10-18 17:04:14.9909876 | 当前 Windows `github.com:443` TCP probe 失败 | 仅证明当前 GitHub 443 不通；不等价于历史慢轮故障 |
| 2026-10-18 17:04:14.9909876 | `10.125.189.196:22` 成功，接口 `Array Networks SSL VPN` | SSH/VPN 控制链路可达 |

因此：

- `NETWORK_INCIDENT_TIMELINE=INSUFFICIENT_EVIDENCE_FOR_HISTORICAL_CAUSALITY`：VPN 故障时间可见，但没有覆盖历史慢轮；历史 GitHub 443 失败日志和历史 SSH 断线日志不可用。
- `CLIENT_TO_GITHUB=UNKNOWN_FOR_HISTORICAL_WINDOW`；当前 probe 为失败，但发生在本轮正式 50K 结束后。
- `CLIENT_TO_SSH=INCIDENT_OBSERVED_BUT_NOT_OVERLAPPING_SLOW_RUN`。
- `SERVER_LOOPBACK=INDEPENDENT`：测试消息、SMSC simulator、worker、Journal 和 DuckDB 均在服务器本机。

## 3. SSH 生存期和进程证据

启动方式是服务器本地 `nohup`，SSH 命令只负责创建目录、启动和返回 PID。后续通过新的 SSH 查询结果文件；没有前台管道、SSH 隧道或本地 Python 转发。

| Run | runner PID / PPID | UTC start–end | exit | result SHA-256 |
|---|---:|---|---:|---|
| `5k-w8-c0-02` | launch `34150`; PPID 未在旧启动器中记录 | 16:38:15–16:39:12 | 0 | `722f64e6128b787f75eafb8dbf9a5255b3fe6d3e1c379a470f04cc3158afb6b1` |
| `5k-w8-c0-03` | `34485 / 1` | 16:43:40–16:44:35 | 0 | `89acc9296eee1a9fde3c3180c185890d3471eac70ede3851d5cff220f36ef150` |
| `5k-w8-c0-04` | `7480 / 1` | 16:45:10–16:45:53 | 0 | `b61a9b24a3c8a9dfd556db152ae2f903d63a3a792b4224545bfb09909ff73d0a` |
| `50k-w8-c0-01`（非正式均匀分布） | `51999 / 1` | 16:49:08–16:54:17 | 0 | `ac655a89490077734fad47499363d42828473c9e13833a9dcee8e3c262064d4d` |
| `50k-w8-c0-02`（正式分布） | `624 / 1` | 16:56:56–17:01:53 | 0 | `a59259dade7afe7849c8cba22039c6cf658ad98b005cc93fdc980476c57a9ff4` |
| `5k-w8-inst-on-01` | `4061 / 1` | 17:07:16–17:08:01 | 0 | `e80dea1140bd5855f2c4e51c4c5dbe9820415530f0bbc0364ebf55039e4d4990` |
| `5k-w8-inst-on-02` | `23904 / 1` | 17:08:48–17:09:32 | 0 | `75c0d661d4579d5135301b91d3275e0ecdd2dd9ee4558ef2a96cf787181e63b3` |
| `5k-w8-inst-on-03` | `28180 / 1` | 17:10:26–17:11:22 | 0 | `b90ac822c85edb5e43c84f08a099e23857ccbcf6e51f5d92863d8523b209de88` |

每轮均写入 `start.utc`、`end.utc`、`exit.code`、`runner.pid`、`runner.ppid`、`runner.pid-start.txt`、`result.sha256`。最终服务器核验时间 `17:13:22Z`：没有活动 `vnext_streaming_acceptance`/detached runner、没有测试端口监听，所有完成轮次 exit code 为 0。由此 `SSH_SESSION_DEPENDENT=NO`、`REMOTE_TEST_PID_AND_EXIT_CAPTURED=YES`。

## 4. 被排除的启动器失败

重新开始前的隔离批次中有三类启动器问题，均未计入性能样本：

1. 远端 shell 引号截断，只留下环境输出，没有执行 Python。
2. 旧 source 路径不存在，Python 返回 exit 2。
3. 服务器旧 venv 路径已清理，ProjectionWriter 报 `ModuleNotFoundError: duckdb`；随后定位到现存 venv 并修正测试脚本路径。

这些失败轮没有结果 JSON、没有有效 PDU 计数，且核验时没有残留进程；正式结论只使用上表的完整 `PASS/COMPLETED` 轮次。

## 5. 归因结论和证据缺口

支持“网络事件不是 loopback 吞吐根因”的证据：

- 正式 50K 在服务器 detached runner 中完成，退出码 0，50K/50K/100K/100K 全部对账；SSH 会话没有维持其生命周期。
- 当前 GitHub 443 probe 失败时，SSH/VPN 到服务器仍成功；服务器本地测试不访问 GitHub，也不通过客户端网络承载 SMPP。
- 历史 VPN 拨号失败结束于 11:35:10Z，而历史慢轮始于 11:52:08Z；没有与慢轮重叠的客户端断线证据。
- 新正式 50K 仍为 168.747 wall PDU/s，接近历史慢轮 161.9，而不是恢复到历史快轮 528.2；这说明“恢复客户端网络”没有带来性能恢复。

尚不能单独确认的内容：

- 没有历史慢轮对应的 SSH 客户端错误、服务器 `SIGHUP/SIGTERM`、GitHub 443 失败原始终端日志。
- 本轮保存了 loadavg 和磁盘，但没有保存运行时 `iowait/await` 原始采样；`SERVER_HOST_LOAD_AND_IOWAIT` 中 I/O wait 为 `NOT_CAPTURED`。
- 当前 host 是共享 80 logical CPU / 375 GiB 主机，正式 50K 期间 1/5/15 分钟 loadavg 约从 `8.95/12.09/12.21` 到 `13.84/12.95/12.50`，根分区由约 4.44 GB 降至约 4.33 GB 可用；这是明显的环境干扰候选，但没有单变量配对实验证明其唯一因果。

最终：

```text
FINAL_DECISION=NETWORK_EVENT_NOT_CAUSAL
CAUSE_CONFIDENCE=网络非因果：HIGH；慢性能的具体资源/调度根因：MEDIUM/INCONCLUSIVE
BACKEND_SOURCE_CHANGED=NO
PRODUCTION_DEFAULT_CHANGED=NO
R4_600S_800PDU=NOT_RUN
CANARY_READY=NO
COMMIT_PUSH=NOT_REQUESTED
```

完整速率、对账和机器证据见：[SLES 吞吐复测与对比](Marketing_Copilot_VNext_SLES_Throughput_Recheck_and_Comparison.md) 和 [机器证据 JSON](Marketing_Copilot_VNext_Network_Performance_Recheck_Evidence.json)。
