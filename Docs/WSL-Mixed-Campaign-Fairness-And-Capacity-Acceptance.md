# WSL Mixed Campaign Fairness and Capacity Acceptance

## 结论

在 WSL2 AlmaLinux-8 原生 ext4 隔离副本中，S0 和三次 S1 混合活动闭环通过：无丢任务、无实际重复 `submit_sm`、无 MSISDN 对账差异，DuckDB single-owner/锁回归通过。S2 和 S3 在显式 wall-clock budget 下持续推进但未完成，正确归类为 `CAPACITY_LIMITED`，不是容量 PASS。

生产现网未连接、未改库、未重启服务；最终 `LIVE_ENV=UNVERIFIED`。

## 基线、工作树和改动

- `BASELINE_SHA`: `bad3dd4e4403a45194da40430d78d73cb720c22a`
- `HEAD_SHA`: 同上；本轮改动均未提交，因此 HEAD 未变化。
- 工作树：执行前已存在 `frontend/package-lock.json` 修改、历史验收产物、旧任务说明和其他未跟踪文件；本轮未 reset、删除、覆盖这些文件，也未修改生产部署。

本轮新增/修改的验收交付物：

- [mixed_campaign_capacity_acceptance.py](../backend/tools/mixed_campaign_capacity_acceptance.py)：WSL 编排、seed、资源边界、真实 API/local watch/TCP SMPP、停服务后的只读 DuckDB 对账。
- [mixed_campaign_scenarios.json](../backend/tools/mixed_campaign_scenarios.json)：smoke、fairness、capacity、soak、scale10k 场景。
- [test_multi_campaign_capacity_fairness.py](../backend/tests/test_multi_campaign_capacity_fairness.py)：确定性活动计划、指标聚合、按业务 MSISDN 映射事件测试。
- [wsl-mixed-campaign-fairness-capacity-results.json](wsl-mixed-campaign-fairness-capacity-results.json)：机器可读汇总。

业务核心代码没有因本轮压测结果而修改；新增修改集中在验收编排和证据生成。编排器还修正了一个验收误判：任务被多次 claim 不等于向 SMSC 重复提交，重复发送现在按模拟器真实 `submit_sm` 的目标号码判定，claim retry 单独报告。

## ENV

| 项目 | 实际值 |
|---|---|
| WSL | WSL2 / AlmaLinux-8 |
| Kernel | `6.18.33.2-microsoft-standard-WSL2` |
| CPU | 22 logical CPUs |
| 内存 / swap | 15 GiB / 4 GiB，检查时 swap 使用 0 |
| Python | 3.11.13，`/opt/marketing-copilot/venv/bin/python` |
| DuckDB | 1.2.0 |
| 压测副本 | `/tmp/mc-mixed-20261013`，原生 ext4 |
| SMPP | `smsc_simulator.py`，实际 loopback TCP，仅 `127.0.0.1` |
| 生产环境 | 未连接；无真实号码、真实 SMSC 或现网服务写入 |

用户提供的现网参数作为基线记录在机器结果中，密码已脱敏；验收进程使用独立 `DUCKDB_PATH`、日志、state、backup、updates、watch/inbox 和端口，并仅对隔离进程设置 `MC_AUTH_DISABLED=1`。

## SCENARIO_MATRIX

### F1：大 backlog 与迟到小活动

`PASS`。S0 和 S1 使用大/小活动分组，先激活大活动，再延迟激活小活动；每个迟到活动获得了任务和真实 submit，且大活动仍有 backlog 时调度继续前进。对账中的实际重复 `submit_sm=0`。

### F2：8 活动、连续输入、三次固定 seed

`PASS`。三次公平运行均为 4 realtime + 4 batch，均生成/提交 `152/152`：

| seed | 创建顺序 | task | submit_sm | duplicate | missing |
|---:|---|---:|---:|---:|---:|
| 20261013 | 5,6,0,3,2,4,1,7 | 152 | 152 | 0 | 0 |
| 20261014 | 0,6,4,7,3,1,5,2 | 152 | 152 | 0 | 0 |
| 20261015 | 6,2,4,0,5,7,1,3 | 152 | 152 | 0 | 0 |

### F3：候选窗口、fair 开关、权重和 multipart 成本

`NOT_RUN`（部分观测不升级为完整 PASS）。本轮确实使用了 8 个候选活动和 `MC_FAIR_ACTIVE_CAMPAIGNS=2/3`，并采到了 candidate group/window 指标；但没有执行 fair disabled/enabled 对照，也没有完成按 SMPP parts 成本的 weighted multipart 矩阵。

### F4：watch/copy/inbox/dedupe/lease/quarantine

`NOT_RUN`（完整故障矩阵）。本轮实际验证了 local watch、原子 `.part -> rename -> .ready`、复制/入库和最终去重对账；确定性测试也验证了 producer 文件契约。lease 过期和 quarantine 注入未在本轮混合 profile 中执行。

### F5：双路 TCP SMPP

`PASS`（正常和限速/响应延迟路径）。S1/S2/S3 使用两路独立 loopback TCP 模拟器，S2/S3 使用低 TPS/响应延迟，所有已提交事件从模拟器 event log 对账。unknown/partial 故障不在本轮混合 profile 中注入，沿用既有后端故障回归证据，不把它们写入本轮覆盖范围。

### F6：HTTP 并发、scheduler、backup、锁

`PASS`。每次启用的隔离 lock regression 均返回 PASS，包含第二 owner 拒绝、直接 DuckDB contender、HTTP 并发、0.5/3/6 秒锁持有和 online backup 路径；没有启动第二个业务 DB writer。

## S0～S4 容量结果

| 档位 | 结果 | 真实结果 |
|---|---|---|
| S0 / 88 条 | `PASS` | `88/88` task，`88/88` submit，0 duplicate，0 missing，115.489s |
| S1 / 混合重复 | `PASS` | 3 次 `152/152`，0 duplicate，0 missing；原始文件见 `Docs/wsl-mixed-fairness-seed-*.json` |
| S2 / 约 3K 有界 | `CAPACITY_LIMITED` | `1480` task 已生成，`880` submit，0 duplicate/missing，120 秒 budget 停止，双路 SMPP |
| S3 / 约 10K staged | `CAPACITY_LIMITED` | `10600` task 已生成，`707` submit，0 duplicate/missing，150 秒 budget 停止，未宣称完成 |
| S4 / 100K | `NOT_RUN` | 可选档位；S3 未完成闭环，因此不启动 |

S2/S3 的 `CAPACITY_LIMITED` 表示在安全边界内任务继续推进且对账没有丢失/重复，不表示全部发送完成。S3 运行的最大采样 RSS 为 `320331776` bytes；最终磁盘仍有充足空间，未触发资源阻断。

## PER_CAMPAIGN_TIMELINE

以下为 S1 seed `20261013` 的 8 活动时序摘要；完整 backlog curve、fairness state、eligibility 和原始时间戳在对应 JSON 中。

| type/id | rows | first service wait ms | max no-progress s | actual submit_sm | first file claim / first batch quantum |
|---|---:|---:|---:|---:|---|
| batch/1 | 4 | 12970.989 | 52.742 | 4 | batch `10:53:02.206Z` |
| batch/2 | 4 | 727.657 | 66.368 | 4 | batch `10:53:00.453Z` |
| batch/3 | 4 | 9315.329 | 59.280 | 4 | batch `10:52:59.646Z` |
| batch/4 | 4 | 11088.174 | 56.372 | 4 | batch `10:53:01.066Z` |
| realtime/1 | 52 | 3450.359 | 58.217 | 52 | file `10:53:01.231Z` |
| realtime/2 | 16 | 2695.731 | 56.915 | 16 | file `10:53:01.727Z` |
| realtime/3 | 16 | 6877.794 | 55.878 | 16 | file `10:53:06.940Z` |
| realtime/4 | 52 | 5549.293 | 54.297 | 52 | file `10:53:02.752Z` |

每条 timeline 同时包含 `eligible_since`、`became_ineligible_at`、`first_task_created_at`、`first_task_claim_at`、`first_submit_sm_at`、`last_progress_at`、`max_consecutive_skipped_rounds` 和 backlog curve。当前持久 fairness 表没有历史 ineligible transition 字段，因此 `became_ineligible_at` 明确为 null，而不是伪造时间。

## BOTTLENECK_EVIDENCE

S3 最新采样的 runtime 聚合如下：

| 指标 | count | sum s | mean s | P50/P95/P99 |
|---|---:|---:|---:|---|
| DB write transaction | 1631 | 643.898291 | 0.394787 | null / null / null |
| DB writer lock wait | 1846 | 501.912093 | 0.271892 | null / null / null |
| DB writer lock hold | 1845 | 170.095625 | 0.092193 | null / null / null |
| task claim | 372 | 154.780722 | 0.416077 | null / null / null |
| realtime file processing | 5 | 33.998420 | 6.799684 | null / null / null |

这里的 null 是事实：当前这些生产指标是 bounded count/sum/latest observation，不是 histogram，编排器不凭均值伪造分位数。每个活动的 `task_to_submit_sm_resp` 已用业务时间样本计算 P50/P95/P99 并写入原始 JSON；后续若要运行时分位数，应增加有限 bucket 指标。

S3 观察到 `10600` 个 task 生成、`707` 个真实 submit，按本轮 elapsed 约 `187.13s` 折算约 `3.78 submit/s`；瓶颈证据同时显示 DB lock wait/hold 与 task claim 的累计成本，不能仅归因于 SMPP TPS。S2/S3 的实际 SMSC 配置、窗口和响应延迟在场景 JSON 及原始结果中保留。

## DATA_RECONCILIATION

- 已完成的 S0/S1：有效输入行、DB task、SMSC event log 三方一致；`duplicate actual submit_sm=0`、`missing_msisdns=0`、`unexpected_msisdns=0`。
- S2/S3：虽然未完成，但已生成的 task 与输入集合一致，当前已观察到的 submit 没有重复、丢失或越活动/越路由映射。
- task 的 `attempts > 1` 作为 `retry_claimed_tasks` 单独报告；它不被当成 duplicate send。实际重复只按模拟器 `submit_sm` 目标号码计数。
- 本轮未注入 unknown/partial；禁止把它们按普通失败重试的既有安全语义仍由后端回归覆盖。

## REGRESSION

执行命令：

```powershell
.p0-test-venv\Scripts\python.exe -m pytest -q backend/tests
```

实际结果：`125 passed, 3 skipped in 143.03s (0:02:23)`。新增确定性测试：`3 passed`。WSL 应用运行时没有 pytest 模块，所以 WSL 只执行真实应用验收；仓库测试在已安装测试依赖的隔离 Python 中执行。

## CAPACITY_LIMITS

- 10K 没有被强行等待全部发送；S3 在 150 秒安全时间窗停止并标为 `CAPACITY_LIMITED`。
- 100K 未运行，因为 S3 尚未完成闭环，符合任务的安全门禁。
- 当前结果不能证明生产配置下的最终容量，也不能把双路 loopback 模拟器吞吐直接等同于外部 SMSC 吞吐。

## REMAINING_RISKS

1. SFTP 因缺少 sshd/paramiko acceptance endpoint 为 `BLOCKED_ENV`；只覆盖 local watch。
2. 未执行 SLES 12 SP5 离线包部署和真实 systemd 更新流程。
3. 未连接真实 SMSC、真实手机号或现网服务；`LIVE_ENV=UNVERIFIED`。
4. F3 的 fair 开关/权重/multipart 成本矩阵与 F4 的 lease/quarantine 故障矩阵为 `NOT_RUN`。
5. runtime DB 指标缺少 histogram P50/P95/P99；报告保留 null，避免误导。
6. S2/S3 显示容量边界，不应被解释为容量 PASS；需要在目标部署机用相同 profile 和其实际 worker/TPS 再复测。

## FINAL_STATUS

```text
CODE_ACCEPTANCE=PASS
WSL_MIXED_FAIRNESS=PASS
LOCAL_MIXED_E2E=PASS
DUCKDB_LOCK_REGRESSION=PASS
CAPACITY_ACCEPTANCE=CAPACITY_LIMITED
LIVE_ENV=UNVERIFIED
```

本任务按说明未执行 commit、push、PR、生产部署或生产服务重启；等待用户另行明确发布动作。
