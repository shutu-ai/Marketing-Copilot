# Flash SMS 回归与性能验收

日期：2026-10-13  
范围：实时活动、批量活动、任务快照、数据库兼容性、SMPP PDU、回归可靠性和本地混合活动容量。

## 结论

本次回归修复已通过代码级测试、后端全量测试、前端构建、同步/异步 SMPP PDU 验证，以及 Windows/WSL2 本地 loopback SMSC 验收。真实 Ethio Telecom SMSC、SLES12SP5 原地升级和 10K 独立压测未执行，因此不宣称生产链路或这些项目已通过。

## 修复范围

- 移除实时/批量活动保存时对既有 `sms_tasks.flash_sms` 的批量回写；任务在生成时快照活动配置，历史任务不会随活动编辑改变。
- 保留既有 `smpp_config_id` 传播逻辑。
- 实时活动状态规则：`not_started` 可设置闪信；运行中禁止修改闪信和运行控制字段，但仍允许 SMPP 配置传播；`ended` 可编辑结束态 UI 所需的非运行控制字段（包括闪信、名称和结束时间），不允许修改运行控制字段。
- 批量活动运行中仍禁止闪信等运行配置修改，结束态可编辑闪信，历史任务不变。
- 闪信默认关闭；复制活动保留源活动的闪信配置；未选择 SMPP 时前端闪信复选框禁用。
- 接入验收工具的 `--flash-sms`、PDU DCS/ESM/分片统计和混合活动交替闪信覆盖。

## 状态与快照矩阵

| 场景 | 闪信设置 | 保存行为 | 任务历史 |
|---|---|---|---|
| 实时 `not_started` | 可修改 | 保存活动字段 | 不改写既有任务 |
| 实时运行中 | UI/API 禁止修改 | SMPP 配置仍可传播 | 不改写既有任务 |
| 实时 `ended` | 允许闪信及结束态 UI 字段 | 禁止运行控制字段 | 不改写既有任务 |
| 批量运行中 | UI/API 禁止修改 | 返回状态错误 | 不改写既有任务 |
| 批量 `ended` | 允许闪信 | 保存活动字段 | 不改写既有任务 |
| 新建/复制 | 默认 `false`；复制保留源值 | 已覆盖 true/false/省略 | 按生成时值快照 |

证据：`backend/tests/test_flash_sms_regression.py` 覆盖 true/false/省略、活动状态矩阵、固定任务时间戳、SMPP 配置传播、创建/复制和任务生成快照；SQL spy 验证活动保存路径不执行 `UPDATE sms_tasks SET flash_sms`。源码中保留的同名 SQL 仅是迁移测试为验证 NULL 语义而建立的测试数据。

## SMPP 协议验收

闪信只改变 `data_coding`，不改变 `esm_class`、UDH、分片数量、分片 payload 或 DLR 配置。

| 内容 | 普通短信 DCS | 闪信 DCS | ESM/UDH/payload |
|---|---:|---:|---|
| GSM 7-bit 短消息 | `0x00` | `0xF0`（240） | 相同 |
| GSM 7-bit 长消息 | `0x00` | `0xF0`（240） | 相同，多分片 UDH 相同 |
| UCS2/Amharic | `0x08` | `0x18` | 相同 |
| UCS2 长消息/emoji | `0x08` | `0x18` | 相同，多分片 UDH 相同 |

`test_smpp_gateway_encoding.py` 覆盖编码、长短信、UDH 和 `esm_class`；`test_smpp_async_submission.py` 捕获实际同步/异步 `send_message` PDU，验证闪信 DCS 和原有 DLR 设置不变。

## 自动化测试结果

| 验收项 | 结果 |
|---|---|
| Flash 回归/活动状态/迁移测试 | PASS，28 passed |
| 后端全量测试 | PASS，150 passed，3 skipped |
| 前端构建 | PASS，`npm run build` |
| Windows loopback quick Flash | PASS，3/3 accepted，DCS `240` |
| Windows loopback 1K 普通短信 | PASS，1000/1000，DCS `0` |
| Windows loopback 1K 闪信 | PASS，1000/1000，DCS `240` |
| loopback reject | PASS，5/5 failed，无普通重试 |
| loopback drop-response | PASS，3/3 unknown，无重发 |
| loopback restart | PASS，重启后 5/5 accepted，无重复提交 |
| loopback partial-submit | NOT_RUN，工具在白名单索引 ready 阶段超时，0 任务产生 |
| 真实 SMSC | UNVERIFIED |

## 性能和混合活动结果

所有结果均为隔离数据库、localhost/loopback SMSC；WSL2 运行在 `/tmp/mc-flash-wsl` ext4 副本，避免 Windows DrvFs 环境误报。`accepted`、submit 数、重复数如下：

| 环境/模式 | 任务 | submit / success | 重复 | 业务等待 | 结果 |
|---|---:|---:|---:|---:|---|
| WSL2 rca1k 普通 | 1040 | 1040 / 1040 | 0 | 170.949s | PASS |
| WSL2 rca1k 闪信 | 1040 | 1040 / 1040 | 0 | 176.135s | PASS |
| WSL2 rca3k 普通 | 3040 | 3040 / 3040 | 0 | 176.551s | PASS |
| WSL2 rca3k 闪信 | 3040 | 3040 / 3040 | 0 | 171.915s | PASS |
| Windows 单活动普通 | 1000 | 1000 / 1000 | 0 | — | PASS |
| Windows 单活动闪信 | 1000 | 1000 / 1000 | 0 | — | PASS |

WSL2 混合验收的批量摄取、公平性、实时摄取、SMS dispatch、跨活动端到端均为 PASS；DuckDB lock regression 和 SFTP 项由工具标记为 `NOT_RUN`/`BLOCKED_ENV`。Windows 单活动 1K 的普通/闪信吞吐分别为 3.856/3.981 submit/s，闪信相对差异约 +3.24%；任务到响应 p50 分别为 115407.921/110950.905ms，p95 分别为 233227.177/225915.464ms。本地结果未发现闪信性能回退。

10K 独立压测：NOT_RUN。不能用 1K/3K 结果替代 10K 结论。

## 数据库升级兼容性

迁移测试构造了缺少新列的历史 DuckDB schema，并插入 2100 条历史任务（7 种状态各 300 条），连续执行 `init_db()` 两次：

- `batch_campaigns.flash_sms`、`realtime_campaigns.flash_sms`、`sms_tasks.flash_sms` 均幂等补列；第二次启动不重复失败。
- 历史任务的 status/content 保持不变，没有批量回写历史闪信值。
- 新列默认值为 `false`；显式历史 `NULL` 在再次启动后仍为 `NULL`，业务读取按 false 语义处理。
- 未为 `flash_sms` 新增索引；迁移只依赖 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`。

SLES12SP5 原地升级/备份回滚演练：NOT_RUN。上线前应先备份 DuckDB，低峰执行一次启动迁移，确认备份可恢复后再切换真实 SMSC。

## 结果文件和复现

机器可读结果见 [flash-sms-regression-performance-results.json](flash-sms-regression-performance-results.json)。主要复现命令：

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests/test_flash_sms_regression.py backend/tests/test_smpp_gateway_encoding.py backend/tests/test_smpp_async_submission.py -q
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
cd frontend; npm run build
```

本次未连接真实 SMSC，未执行提交、推送或生产数据库操作。

FINAL_STATUS: PASS_WITH_LIMITATIONS  
RELIABILITY_UNKNOWN_RESTART: PASS  
PERFORMANCE_1K: PASS  
PERFORMANCE_3K: PASS  
PERFORMANCE_10K: NOT_RUN  
SLES_SCHEMA_UPGRADE_COPY: NOT_RUN  
LIVE_SMSC: UNVERIFIED  
COMMIT_PUSH: NOT_DONE
