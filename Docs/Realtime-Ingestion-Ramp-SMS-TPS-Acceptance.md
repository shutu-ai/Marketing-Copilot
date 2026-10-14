# 实时摄取恢复与 SMS TPS 验收记录

## 执行环境

- Windows 开发工作区，WSL 分发版 `AlmaLinux-8`。
- CPython `3.11.13`；未修改系统 Python。
- 使用安装包 `marketing-copilot-sles12sp5-system-python-20261013-flash-sms-regression.tar.gz` 解出的 `offline/wheels`，按 `offline/requirements.txt` 离线安装。
- loopback-only SMSC；拓扑为 4 ingestion workers、8 task workers、4 SMPP slots、`--smpp-window 20`、send queue capacity 200。
- 所有运行均隔离 DuckDB；未连接生产数据库或真实 SMSC。

## 可重复命令

后端回归：

```text
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests/test_live_duckdb_hotpath_backpressure.py backend/tests/test_realtime_progress_batches.py -q
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
git diff --check
```

WSL 运行使用安装包 wheelhouse 创建的 Python 环境，并设置较长的隔离测试 lease：

```text
MC_INGEST_FILE_LEASE_SECONDS=600 MC_INGEST_CHUNK_LEASE_SECONDS=300 \
python backend/tools/local_e2e_acceptance.py --scenario stress-10k \
  --events <N> --files <F> --expected-tasks <N> --timeout-seconds <T> \
  --smsc-tps 200 --task-workers 8 --ingest-workers 4 --workers 4 \
  --claim-batch 50 --smpp-window 20 --smpp-send-queue-capacity 200 \
  --flash-sms --keep-artifacts
```

## 自动化验证

| 门禁 | 状态 | 证据/说明 |
|---|---|---|
| 大 chunk `1/750/1500/5000` | PASS | 有界容量、实际 grant 和 stale permit 单元测试通过 |
| stale permit / stop-aware | PASS | grant 会按当前容量缩小，停止事件可打断等待 |
| 冷启动 writer pressure | PASS | 无历史 task pause 时，实时 writer/Journal/lock 压力也会启用 ramp |
| 子批次 checkpoint | PASS | `2/2/1` 子批次累计 5 条，最终计数不被 duplicate 覆盖 |
| 全量后端回归 | PASS | `158 passed, 3 skipped` |
| diff / Python 编译 | PASS | `git diff --check` 和变更 Python 文件 py_compile 通过 |

## WSL 1K 回归

已有 4×8×4 loopback 运行中，当前实现和基线均完成 4/4 文件、1000/1000 task、1000/1000 成功 `submit_sm`，无 duplicate、lost mapping、unknown submit id、misdelivery。窗口结束时两组均为 `999 accepted + 1 sending`，所以只能判定摄取和提交映射正确，不能判定业务终态完整通过。

## WSL 3K A/B

### 当前实现 B

- 参数：12 files、3000 events、600 秒窗口、`--flash-sms`。
- 12/12 文件完成，3000/3000 task 创建。
- `submit_sm=2418`，成功 2418，拒绝 0。
- task 状态：`accepted=2418`、`sending=240`、`queued=342`。
- duplicate、lost mapping、unknown submit id、misdelivery、DLR violation 均为 0/空。
- ingestion → task creation：P50/P95/P99 = `1620.5/2225/2225 ms`。
- events/s = `5.017`，submit_sm/s = `4.044`；最终 backlog = `582`。
- 完整业务终态：FAIL，原因是 600 秒窗口结束时仍有 queued/sending task。

### 基线 A

- 参数：12 files、3000 events、180 秒窗口、其余拓扑相同。
- 12/12 文件完成，3000/3000 task 创建。
- `submit_sm=881`，成功 881，拒绝 0。
- task 状态：`accepted=879`、`sending=207`、`unknown=1`、`queued=1913`。
- `unknown_submit_ids` 为空，duplicate、lost mapping、misdelivery、DLR violation 均为 0/空。
- ingestion → task creation：P50/P95/P99 = `1546/2180/2180 ms`。
- events/s = `16.882`，submit_sm/s = `4.958`；最终 backlog = `2120`。
- 完整业务终态：FAIL，原因是 180 秒窗口超时。

两组窗口为 600 秒与 180 秒，且发送端仍受独立的 submit 吞吐瓶颈影响，因此不把 accepted 数量直接解释为 B 优于 A；可确认的是 B 的全部文件和任务摄取完成，且没有新的提交映射丢失或重复发送。

## WSL 10K 运行

- 参数：40 files、10000 events、180 秒窗口、4×8×4、`--flash-sms`。
- 40/40 文件完成，10000/10000 task 创建。
- `submit_sm=903`，成功 903，拒绝 0。
- task 状态：`accepted=900`、`sending=227`、`queued=8873`。
- duplicate、lost mapping、unknown submit id、misdelivery、DLR violation 均为 0/空。
- ingestion → task creation：P50/P95/P99 = `1624/2237.55/2277 ms`。
- events/s = `56.11`，submit_sm/s = `5.067`；最终 backlog = `9100`。
- 完整业务终态：FAIL，原因是 180 秒窗口结束时发送队列仍有大量未完成任务。

10K 运行证明了 ingestion scale 和 checkpoint 可以完成全部输入，但不证明当前 SMS 发送端满足生产 TPS。后续应单独分析 task claim/submit authorization、SMPP slot 利用率和 result write gate。

## 门禁矩阵

| Gate | 判定 | 说明 |
|---|---|---|
| `STATIC_AUDIT` | ISSUES_FOUND_FIXED | 已确认大 chunk、stale permit、读连接热点和过密压力衰减并完成最小修复 |
| `RAMP_BIG_CHUNK` | PASS | `1/750/1500/5000` 有界 admission 有自动断言 |
| `RAMP_COLD_START` | PASS | live writer/Journal/lock pressure 可在无历史 pause 时启用 ramp |
| `RAMP_MULTIPLE_CYCLES` | NOT_RUN | 未完成默认 50K/20K 的重复 HIGH → LOW → RESUME 5 分钟观测 |
| `NO_DLR_AND_JOURNAL_SAFETY` | PASS_WITH_LIMITATIONS | 现有 Journal/no-DLR 回归和本次 WSL 映射检查通过；本次未覆盖全部 DLR 组合 |
| `PRODUCTION_LIKE_4_8_4` | FAIL | ingestion 子门禁通过，但 1K/3K/10K 业务终态窗口均未全部完成 |
| `PERFORMANCE_RECOVERY` | NOT_PROVEN | 没有 5 分钟 steady-state、两段连续 30 秒区间和历史 25.8→3.1 对照 |
| `DUCKDB_GATE_P95_P99` | NOT_RUN | 尚未取得修复后的 gate wait/hold P95/P99/timeout 样本 |
| `1K_AB` | FAIL | 摄取和提交映射正确，但 A/B 均在窗口尾部保留 1 个 sending task |
| `3K_AB` | FAIL | 两组均未在各自窗口内达到全量 accepted；B 的 ingestion 正确性通过 |
| `10K_AB` | FAIL | 当前实现完成全量 ingestion，但 180 秒业务终态未通过，未执行等窗口基线 |
| `LONG_SOAK` | NOT_RUN | 未执行 10–30 分钟 soak |
| `SLES12SP5` | NOT_RUN | 仅用 SLES 目标 wheelhouse 在 AlmaLinux-8 做兼容性验证，未在真实 SLES12SP5 主机部署 |
| `LIVE_SMSC` | UNVERIFIED | 明确禁止连接真实 SMSC |
| `FULL_BACKEND_TESTS` | PASS | `158 passed, 3 skipped` |

## 回滚与交付边界

本次改动仅涉及 backpressure admission、实时分批写入和 chunk 进度计数。回滚目标为基线 SHA；回滚会恢复已确认的 stale permit / 大 chunk 风险。未自动部署、重启生产服务或修改短信中心配置。

实现已提交为 `4a4e39d`。`git push origin main` 因当前环境连接 GitHub 443 超时而阻塞，报告因此明确记录 `COMMIT_PUSH: BLOCKED`，未把本地提交伪报为远端已推送。
