# WSL 多活动公平调度验收报告

## 结论

- `LOCAL_E2E_PASS`
- `BATCH_INGEST_FAIRNESS=PASS`
- `REALTIME_INTAKE_FAIRNESS=PASS`
- `SMS_DISPATCH_FAIRNESS=PASS`
- `CROSS_TYPE_E2E=PASS`
- `FAIRNESS_TELEMETRY=PASS`
- `SFTP=BLOCKED_ENV`：该 WSL 环境没有 `sshd`/Paramiko，未伪造 SFTP 通过。
- `LIVE_ENV_UNVERIFIED`：本验收使用 WSL ext4 隔离目录、loopback SMPP 和隔离 DuckDB，不代表现网业务库已验证。

## 环境与版本

- 验收日期：2026-10-13
- 代码基线（执行前）：`5d56c7f0b2bee4e212bfad5cb0e975fdb309fd74`
- WSL 发行版：`AlmaLinux-8`，WSL2
- Python：`/opt/marketing-copilot/venv/bin/python`
- DuckDB：`1.2.0`
- 数据目录：`/tmp/mc-fair-20261013130644/...`（ext4）
- 现网参数同步要求：已在项目默认/环境参数中保留现网生产值语义；本次 E2E 使用隔离端口、目录和数据库，不触碰现网库。

## 执行命令

```bash
cp /mnt/d/dev-projects/MarketingCopilot/backend /tmp/mc-fair-20261013130644/backend
/opt/marketing-copilot/venv/bin/python \
  /tmp/mc-fair-20261013130644/backend/tools/multi_campaign_fair_acceptance.py \
  --python /opt/marketing-copilot/venv/bin/python \
  --timeout-seconds 180 \
  --keep-artifacts \
  --output /mnt/d/dev-projects/MarketingCopilot/Docs/wsl-multi-campaign-fairness.json
```

工具通过 HTTP 创建活动、模板、SMPP 配置和批量输入，通过本地 watch 目录的 `.part -> rename -> .ready` 生产契约发布实时文件；停止服务后才以只读方式打开 DuckDB 做对账。

## 活动与结果

| 活动 | 类型 | 期望任务 | 生成任务 | accepted | 首服务等待 |
|---|---|---:|---:|---:|---:|
| batch-A | batch | 40 | 40 | 40 | 669.8 ms |
| batch-B | batch | 4 | 4 | 4 | 5942.1 ms |
| realtime-A | realtime | 40 | 40 | 40 | 2542.3 ms |
| realtime-B | realtime | 4 | 4 | 4 | 6327.2 ms |

汇总：`88/88` 任务生成，`88/88` `submit_sm` 成功，重复发送任务为空，重复消息 ID 为空，缺失 MSISDN 为空；最大首服务等待 `6327.2 ms`，最终 backlog 为 `0`，公平遥测状态行 `8`，四个活动均被服务。

## 证据

- 结构化结果：[wsl-multi-campaign-fairness.json](./wsl-multi-campaign-fairness.json)
- 验收工具：[multi_campaign_fair_acceptance.py](../backend/tools/multi_campaign_fair_acceptance.py)
- 确定性公平测试：[test_multi_campaign_fair_scheduling.py](../backend/tests/test_multi_campaign_fair_scheduling.py)
- WSL ext4 运行产物：`/tmp/mc-fair-20261013130644/.local-e2e-runs/marketing-copilot-multi_campaign_fairness-d4290b4e`

## 限制与后续

SFTP 需要目标环境提供 `sshd`、Paramiko 及可访问的远端目录，本次仅验证了代码的公平轮转逻辑和本地 watch/批量/SMPP 实链路。现网验证仍需在维护窗口内使用真实业务参数、现网数据备份和可回滚发布流程完成。
