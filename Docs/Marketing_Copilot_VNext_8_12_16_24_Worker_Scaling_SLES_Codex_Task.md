# Marketing-Copilot VNext — SLES12SP5 Worker 8/12/16/24 受控扩容与吞吐验证（Codex 任务）

> **Task ID:** `MC-VNEXT-WORKER-SCALING-8-12-16-24-20261018`  
> **优先级：** 24 Workers 是重点候选，8 Workers 是冻结基线；12、16 Workers 用于寻找收益拐点。  
> **已验证基线：** `origin/main = 14be326fdf9d33f2a52e871ce798170a876ff8e6`（开始执行前重新核实 SHA 和 working tree，若 HEAD 不同先记录，不得盲目覆盖）。  
> **目标服务器：** 经授权的 `paas-core01`，SUSE Linux Enterprise Server 12 SP5，原生 ext4；仅使用服务器本机 synthetic `127.0.0.1` SMSC。  
> **交付形式：** 允许最小**测试 Harness / 测试代码**修改；不做生产产品代码优化，不修改生产默认配置。先 5K 交叉复跑；仅在事先定义的门槛达标后，条件性开展较大规模验证。  
> **重要限制：** 不连接真实 SMSC、不发送真实短信、不访问生产客户数据/数据库/API；不得自动 commit/push，不修改或提交既有无关 dirty/untracked 内容。

---

## 0. 背景和待回答问题

历史 SLES 分层性能测试在固定 `pool_size=4`、`tps_scope=per_bind`、`tps=200`、`window_size=50` 条件下获得：

| 层级 | 测量口径 | 已有实测值 | 注意事项 |
|---|---|---:|---|
| L0 | 独立 SMPP simulator/socket | 14,239.716 PDU/s | 无应用限速，不是产品容量 |
| L1 | SMPP Transport + Credit + Window | 793.045 PDU/s | 接近 4×200 的配置上限 |
| L2 | 单 Worker + Spool，250-task bounded pass | 260.860 PDU/s | 与 L4 的进程数不相同 |
| L3 | L2 + durable ProgressJournal | 197.140 PDU/s | 单 Worker，包含逐事件 fsync |
| L4 | 完整 E2E，8 Workers，发送阶段 | 384.391 PDU/s | 与 L2/L3 的测试口径和并发不同 |
| L4 | 完整 E2E，8 Workers，完整 wall | 85.953 PDU/s | 5K 总用时 58.171s，包含收尾 |

L4 5K 已记录 response-Journal drain 4.068s、Projection commit/offset drain 约 31.621/31.622s，以及 Projection 单进程 `staging_bind_execute` 23 batches、P95 约 2055ms。报告位置：

- `Docs/Marketing_Copilot_VNext_Layered_SMPP_Throughput_RCA.md`
- `Docs/Marketing_Copilot_VNext_Layered_Performance_AB_and_Decision.md`
- `Docs/Marketing_Copilot_VNext_Layered_Throughput_Evidence.json`

**本次唯一要回答的核心问题：** 在全局 **4 个物理 Bind、每 Bind 200 TPS、Window=50** 不变的前提下，将完整 E2E 的 Worker 进程数由 **8 提高到 12、16、24**，是否能为 SMPP 持续提供更多待发送任务，并显著缩短真实 E2E wall？增加 Worker 是否反而加剧 Bind Slot 争用、跨进程锁、Spool/Journal 文件数和 Projection drain？

**不得预先假设 24 一定优于 16 或 8。** 以证据选择最小有效 Worker 数量；如果 24 没有收益，就明确 `NO_CHANGE_WITH_EVIDENCE`。

---

## 1. P0：先修复测试 Harness 的静默 Worker 截断（强制前置）

**源码确认：** `backend/tools/vnext_streaming_acceptance.py::main()` 当前存在：

```python
args.workers = max(1, min(8, args.workers))
```

因此命令行传入 `--workers 12/16/24` **目前都会被静默折算为 8 Workers**。如果不先修复，后续所有 Worker 扩容数据无效。

### 1.1 允许修改范围

1. **仅修改测试 Harness**（上述文件及其必要测试），使 `--workers` 支持 **8/12/16/24**，可设置诊断上限 24；不支持的数量应报错或清晰拒绝，**禁止再次静默 clamp**。
2. 保留原有 `--workers` 默认值 **8**；不改变生产部署服务的 Worker 默认配置。
3. 确认 `PartitionMap.create(..., 256, args.workers)`、`journal_paths`、Worker 进程创建、结果收集均按实际 Workers 生效。
4. 结果 JSON 增加或复用能交叉核实的字段：`requested_workers`、`effective_workers`、`worker_result_files_count`、`partition_owner_distribution`、`workers_alive_peak`、`workers_exited_cleanly_count`。如无法稳定采集，明确输出 `NOT_CAPTURED`，不得编造。
5. 检查 24 Workers 时是否存在 `worker_result` 缺失、分区未分配、跨 Worker 重复分区、过多 Journal 文件引发 Projection 启动或状态异常。
6. 给测试 Harness 增加轻量自动测试：8/12/16/24 参数被接受且真实生效；0、负数、超安全上限不被静默接受；4 个物理 Bind 上限不因 Worker 增加而变化。

**禁止修改：** `backend/vnext/worker.py`、`route_credits.py`、`smpp_transport.py`、`spool.py`、`projection_writer.py` 的生产行为；除非发现独立、可复现的正确性缺陷，须先报告并停止本次性能优化，不得借任务之名直接重构。

### 1.2 通过条件

对 `--workers 24` 至少完成一次短 1K smoke：真实创建 24 个工作进程及相应结果/Journal 路径；分区 owner 总数 256、无重复遗漏；物理 Bind 同时占用峰值不超过 4；退出后没有残留进程和 lease。任何“请求 24、实际 8”的情形直接 `HARNESS_INVALID`，不进入正式测量。

---

## 2. 冻结运行配置和安全边界

所有 5K 正式对照保持下列参数完全一致，除 `--workers` 之外**只允许运行目录与测试次序变化**：

| 参数 | 冻结值 |
|---|---|
| `workers` | 8 / 12 / 16 / **24** |
| `pool_size` | **4（全局物理 Bind，不随 Worker 放大）** |
| `tps_scope` | `per_bind` |
| `route_tps` | **200 PDU/s per Bind**，总 ceiling **800 PDU/s** |
| `window_size` | **50** |
| `counts` | `1250,1250,1250,1250`（5,000 条单 PDU） |
| `batch_size`（feeder） | 250 |
| `projection_batch_size` | 512 |
| `projection_staging_mode` | `vectorized` |
| `projection_status_interval` | **0.1s**，用于与最近 L4 诊断配置一致 |
| `response_delay_ms` | 0 |
| `instrumentation` | `on`（正式诊断矩阵统一启用） |
| `profile_interval` | 使用相同值，避免采样密度偏差 |
| `resource_profile` | `PROD_8G`，`rss_guard_scope=application` |
| durability | Spool Checkpoint、Journal append/flush/fsync、Projection DB commit/offset 保持原样 |
| 发送语义 | **at-most-once after dispatch**；不自动重发 unknown、不放宽跨天 cutoff 或分区 fencing |
| SMSC | 仅服务器内 `127.0.0.1` synthetic loopback |

- 使用同一冻结后端源码、Python 3.11.x、DuckDB 1.2.0、依赖 wheelhouse SHA-256，统一 CPU affinity/nice（不要求擅自修改系统配置）。
- 全部在同一 SLES 原生 ext4 上测试；各 Run 使用**唯一的独立 run root**，不复用 Spool、ProgressJournal、Projection DB、Credit lease state、PartitionMap。
- 不得在另一个运行还活着时启动新测量，避免竞争污染。
- 测试由服务器 detached 进程执行，不依赖笔记本/SSH 会话存活；记录 PID、PPID、exit code、UTC 开始结束时间、结果 SHA-256。
- 运行前检查 ext4 剩余空间和磁盘水位、测试路径可写性、CPU/loadavg、`/proc/stat`、`iostat`（可用时）、内存和 swap；如果服务器处于明显异常负载或空间不足，记录 `ENV_BLOCKED` 并停止扩容，不得强制长跑。
- 采样 `application-only` 与 `harness-inclusive` RSS/CPU/进程数；任何无效采样均记为 `NOT_CAPTURED`，不得用 RSS=0 判定合格。
- 如 24 Workers 导致内存资源 gate 触发、进程启动失败、Bind 锁超时或响应错误，必须保留错误证据并停止该候选，不可为了通过而提高 RSS/磁盘安全限额。

---

## 3. R1：5K 预筛及正反顺序交叉验证

### 3.1 Smoke

1. 用 8 和 24 Workers 分别执行 1K smoke（4 campaigns × 250），只验证参数真实生效、正确性、资源限制、Bind 持有数、进程退出。**1K 不作为吞吐赢家依据**。
2. 检查 `/proc` sampler、result.json、worker results、journals 和 Projection offsets 完整性。
3. 检查 `effective_workers=24`，且 24 Worker 不会悄悄退化成 8；允许部分 Worker 因无任务而低利用，但要与“实际未启动”区分。

### 3.2 正式 5K 矩阵（8 Run，顺序交叉）

以 5K 完整 L4/E2E Harness 执行：

| Round | 顺序 | 用途 |
|---|---|---|
| AB | **8 → 12 → 16 → 24** | 观察正向扩容趋势 |
| BA | **24 → 16 → 12 → 8** | 反转执行顺序、削弱主机时间漂移 |

每次运行使用相同 seed、campaign 内容及 workload；提交时附完整命令、配置 SHA、源包 SHA、依赖 SHA、原始 JSON hash。每个正式 Run 必须 `status=PASS`、`completion_state=COMPLETED`、无 guard failure，否则不能用于吞吐比较。

### 3.3 24 Worker 专项配对（仅在预筛达到合理条件时）

若 24 Workers 在上述两轮中均有稳定提升迹象且资源正常，优先补做 **至少两组 8↔24 交叉配对**（如 `8→24`、`24→8`），以增大样本量；若 16 已非常接近或超过 24，则追加 **16↔24** 配对确认性价比。无稳定改善可提前停止，输出 `NO_CHANGE_WITH_EVIDENCE`。

不要机械地做大量 50K；也不要由于 5K 单次异常值就声称 24 为最佳。

---

## 4. R2：指标、真实瓶颈与分母口径

### 4.1 每 Run 必须输出的吞吐指标

严格区分三种速率，禁止把它们互换：

- **Active-stage socket rate**：实际成功发送的 PDU / 首条 socket write 到最后一条 socket write 的时间差（如果使用 Loopback receive 时间作为代理，字段必须标明来源，不与 Worker socket 时间混合）。
- **Feed-phase rate**：实际成功发送 PDU / producer active seconds；单列。
- **E2E wall rate**：实际成功发送 PDU / 整个完成测试的 wall seconds（包含 feeder、Worker、response drain 和 Projection offset 完成）。

还需输出：`test_wall`、`producer_active`、`last_socket_write`、`last_response_received`、`last_response_journal_durable`、`last_projection_db_commit`、`last_offset_durable`、最终 `backlog=0`。单阶段统计无法采集时明确标注 `NOT_CAPTURED`。

**注意：** 5K 短跑中的 Worker spawn、Ingress 初始化和 Projection 收尾对 wall 的占比偏高；所有相对比较必须同 Run 类型、同样的分母与配置，不能把 L1 793 与 L4 wall 86 当成同路径扩容预期。

### 4.2 Worker 供给能力和队列

逐 Worker（必要时做紧凑聚合，不能按 PDU 落盘日志）记录：

- 实际任务数、被分配分区数、Spool pending/队列深度及随时间变化；`task_taken/s`、`socket_written/s`、`responses/s`；
- 任务从可领取到 Worker 实际开始处理的等待（若现有源码没有足够时间戳，可新增仅 Harness 侧的有限抽样观察；说明该时间来源与采样占比），以及生产者是否真正提供了足够任务；
- `max_inflight` 的峰值及典型区间、CPU/RSS、空闲与被阻塞 Worker 数量；
- TaskSpool `checkpoint` 及 prefetch/partition 扫描开销（必要时使用有限抽样，严禁修改 dispatch checkpoint 的先后次序）；
- 8/12/16/24 各自 256 分区 owner 分布及任务倾斜；24 Workers 是否出现大量空 Worker。

**判定思路：** 若 24 Workers 仍无法让 4 Bind 长时间满负荷，需分清“Feeder 没有足够待处理任务”“Worker 尚未读取/Checkpoint”“Worker 等待 Bind Slot”还是“Credit/Window 限流”——不要泛称前端能力不足。

### 4.3 Bind/RouteCredit/Window

- 物理 Bind **同时活跃峰值 ≤4**，每个 slot 的 active time、last write、send rate、轮换次数和 idle ratio；
- 各 Worker 的 `acquire_bind` 成功/失败次数、排队时长/超时（若无计时字段，仅 Harness 抽样；不得把 `pass-start→first-write` 错当纯 Bind 延迟）；
- RouteCredit granted/consumed/remaining/wasted/refill cost/wait；分析“合法限速等待”与“锁实现开销”；
- Window inflight peak/典型利用率、wait P50/P95、timeout，SMPP response latency；
- Bound-pass close/rebind 频度与总时间，但**不得直接把 Bind 数量多等同于主瓶颈**：前轮保持长连接未带来吞吐收益。

### 4.4 Journal + Projection 的下游承载能力

- 8/12/16/24 对应的 Journal 文件数、progress events 和 response events；每条进度事件依然 durable；
- Journal append/fsync count、延迟与持续速率；
- Projection `events_committed/s`、队列和 backlog（逐秒或相同采样周期）、commit batch count/size、staging bind execute P50/P95/总量；
- `response-journal drain`、`projection commit drain`、`offset durable drain`；比较这些项在 Worker 扩容时是否扩大；
- Projection staging P95 需说明它是单进程样本 P95，还是多个进程 P95 的 max；不可声称为全局 pooled P95。

**优先判断：** 如果 24 Worker 发送阶段变快但 Projection backlog 和完成尾部明显变长，以至 E2E wall 无收益或倒退，24 **不算成功**。

### 4.5 数据正确性硬门槛（每 Run）

对于本轮固定单 PDU 5K，必须：

```text
accepted = 5000
socket_written_PDU = 5000
loopback_submit_sm_received = 5000
submit_sm_resp_success = 5000
response_event_count = 5000
journal_events_durable = 10000
projection_events_applied = 10000
unknown = 0
write_failed = 0
duplicate_PDU = 0
max_active_binds <= 4
final_backlog = 0
projection_offset_reaches_journal_EOF = true
worker_exit_and_cleanup = PASS
resource_gate = PASS
```

任何结果未达标均视为正确性/测试有效性失败，不得把吞吐数字记入获胜统计。若分区 rebalance/Worker 数改变触发已知恢复风险，在扩大前运行有针对性的单测或小规模故障注入；不得更改 at-most-once、旧业务日 cutoff、T6/T7 语义。

---

## 5. R3：决策门槛及条件性扩大

### 5.1 性能比较方法

- 先按相同主机条件、同轮顺序形成配对差值和中位数；同时给出单 Run、每轮、合并 median、范围和异常值，不只写最佳值。
- 对 24 Workers，优先相对 8 Workers 评估 **stage rate** 与 **E2E wall rate**。为了避免“微小且不稳定的快”，建议采用**可调整但必须测试前冻结**的工程判据：
  - 24 在独立交叉轮次中不出现持续反转；
  - `stage median` 相对 8 **至少 +15%**，并且 `E2E wall median` **至少 +10%**；
  - 无错误、无 Bind/TPS 越界，application RSS 与磁盘位于实际 Profile 限额内；
  - Journal/Projection 最终对账，backlog 不持续扩大；
  - 额外资源成本合理，且不存在 12/16 Workers 性能接近而明显更省资源的替代。
- 如果 16 Workers 的 E2E median 达到 24 的 **95% 以上**，且资源开销明显更低，则优先推荐 16；若 8 已足够或 24 的提升仅在 stage 而非 wall，给出 `KEEP_8` 或 `OPTIMAL_12_OR_16`，不要默认选择 24。
- 不把所设阈值写成业界标准；报告可补充实际业务对吞吐的最低要求和边际资源成本。

### 5.2 分支决策

| 证据 | 动作 |
|---|---|
| 24 相比 8 稳定更快、E2E 也改善、资源安全 | `24_WORKER_PROMISING`，进入有限 50K 对照资格评估 |
| 12 或 16 更划算、24 开销过高 | `OPTIMAL_12_OR_16`，不推广 24 |
| 增加 Worker 造成 Bind 争用/Journal 数过多/Projection tail 扩大 | `WORKER_SCALING_COUNTERPRODUCTIVE`，保留 8 |
| 所有候选差距小于主机噪声 | `NO_CHANGE_WITH_EVIDENCE`，停止扩大 |
| 24 未真实启动、采样失败或环境异常 | `HARNESS_INVALID` / `ENV_BLOCKED`，不声称性能结论 |
| 正确性、at-most-once 或 durable 失败 | `CORRECTNESS_BLOCKED`，立即终止当前候选 |

### 5.3 可选 50K（不强制）

**只有** 5K 交叉结果证明了真实、稳定的 E2E 增益，所有正确性和资源门槛通过，服务器空间与负载适宜，才考虑在同一 SLES 上对 **8 和实际最优候选**（优先 24，若 16 更好则 16）各做一轮固定 50K；如果环境噪声大，可选择不跑，并在结果中明确 `NOT_RUN_BY_GATE`。

- 50K counts 冻结为 `20000,20000,5000,5000`（不能拿它与不同分布直接作严格对照）；
- 严格遵守 4 Bind×200 TPS、Window 50、durability/at-most-once；
- 记录完整对账、RSS/CPU、磁盘、逐时段吞吐、Credit/Bind 争用、Projection backlog 与收尾时间；
- 50K 配对只证明一定规模下的表现，**不能替代 600 秒稳定 800 PDU/s 及真实 SMSC Canary 验收**。

**本任务禁止自动开启 600 秒长跑和生产 Canary。** 如 50K 合格，只输出是否建议另开正式容量验收任务；不进行任何生产配置发布。

---

## 6. 推荐执行与复现实例

使用 `backend/tools/vnext_streaming_acceptance.py` 作为唯一 L4 口径，**在 P0 生效并测试通过以后**，采用类似以下 SLES 命令。实际 Python/venv、source/root 应基于服务器当前可用且已冻结的目录，不可照抄已过期路径；不得为方便安装修改系统 Python。

```bash
# 示例：仅用于文档展示；在授权的 SLES server 上替换为经验证的路径。
SOURCE_ROOT=/home/paas/mc-vnext-worker-scaling/source
PYTHON=/home/paas/mc-vnext-worker-scaling/runtime/python/bin/python3
SITE_PACKAGES=/home/paas/mc-vnext-worker-scaling/site-packages
ROOT=/home/paas/mc-vnext-worker-scaling/runs
WORKERS=24
RUN_ID="5k-worker-${WORKERS}-ab-01"
RUN_ROOT="${ROOT}/${RUN_ID}"
mkdir -p "${RUN_ROOT}"
PYTHONPATH="${SOURCE_ROOT}/backend:${SITE_PACKAGES}" "$PYTHON" \
  "${SOURCE_ROOT}/backend/tools/vnext_streaming_acceptance.py" \
  --source-root "${SOURCE_ROOT}" \
  --run-root "${RUN_ROOT}" \
  --counts 1250,1250,1250,1250 \
  --workers "${WORKERS}" \
  --pool-size 4 \
  --route-tps 200 \
  --tps-scope per_bind \
  --window-size 50 \
  --batch-size 250 \
  --projection-batch-size 512 \
  --projection-staging-mode vectorized \
  --projection-status-interval 0.1 \
  --instrumentation on \
  --profile-interval 1.0 \
  --response-delay-ms 0 \
  --resource-profile PROD_8G \
  --rss-guard-scope application \
  --projection-drain-timeout 120 \
  --output "${RUN_ROOT}/result.json"
```

需以 detached `nohup`/`setsid` 脚本实际执行（与已有服务器任务保持一致），独立记录 PID/exit 状态。命令中 `RUN_ROOT`/`WORKERS`/`RUN_ID` 唯一变化；若运行工具不支持某预期 flag，先以 `--help` 和源码确认，**不允许伪造执行命令或测试结果**。

提醒：SLES 进程 24 Workers 可能使 `_worker_bind_target` 仍至少为 1，但**物理 Bind 槽位总数必须依靠 `RouteCreditBroker` 的共享 lease 保持为 4**。不得通过给每个 Worker 单独建一个未共享的 RouteCredit root 或 pool 来“扩大”实际绑定数量。

---

## 7. 交付文件（Docs，最多三份正式报告）

1. `Docs/Marketing_Copilot_VNext_Worker_8_12_16_24_Scaling_RCA.md`  
   环境/源码冻结、Harness clamp 修复、有效 Worker 数验证、每阶段对照、Bind 竞争、Worker/Spool 供给、Journal/Projection drain、实际根因及限制。
2. `Docs/Marketing_Copilot_VNext_Worker_Scaling_AB_BA_and_Recommendation.md`  
   8/12/16/24 完整逐 Run 表、交叉对照统计、24 vs 8 / 24 vs 16 推荐、50K 是否执行及原因、最终配置建议（**不应用配置**）。
3. `Docs/Marketing_Copilot_VNext_Worker_Scaling_Evidence.json`  
   机器可读：commit/env/config/source SHA、每个 Run 的实际 requested/effective Workers、commands、result SHA、counts、stage/feed/wall、Bind/Credit/Window、RSS、Journal、Projection、资源 gate、decision 和 missing/null 字段。

代码仅可提交必要的 Harness 与测试修改，例如：

- `backend/tools/vnext_streaming_acceptance.py`
- `backend/tests/test_vnext_streaming_acceptance.py`

不要提交 `.tmp/`、原始大型日志、Journal、Spool、DuckDB 文件、用户数据、认证凭据或服务器密钥。机器证据可引用服务器原始目录和 SHA-256，但报告必须能独立解释指标和决策。

---

## 8. 测试与完成标准

- Harness 新增的 12/16/24 功能有自动化回归，原有 `backend/tests` 中相关测试 PASS；能完成则运行完整后端测试，并记录 passed/skipped；不能执行则如实说明原因。
- 在 SLES native ext4 证明 **24 Workers 实际启动**；全程同时 Bind ≤4、无超额 TPS、所有任务/Journal/Projection 事件对账；动态资源监测有效。
- 8/12/16/24 **至少完成两轮顺序相反的有效 5K 矩阵**，或因正确性/资源/环境门槛提前中止并明确停在哪里。
- 重点候选 24 的额外配对有证据支持，或明确因无改善而不继续；任何性能结论均不能基于单次成功或单次最快结果。
- 清楚报告 Worker 扩容能否改善 **SMPP stage**，以及能否同步改善 **E2E wall**；回答 24 是否造成新的 Bind/Journal/Projection 瓶颈。
- 无 durable/fencing/at-most-once 行为修改；无默认生产配置改变；无 600s/Canary 实际运行。
- 结束时检查测试子进程、测试端口、lease、服务器磁盘，确认无测试残留；`git diff --check`、结构化 JSON 校验及敏感信息扫描 PASS。
- 遵守 `AGENTS.md`，保留全部无关工作区内容；**不 commit/push**，除非另行明确授权。

### 最终必须输出的机器摘要字段

```text
SOURCE_HEAD=
TARGET_SERVER_OS=SLES12SP5
NATIVE_EXT4=YES|NO
PRODUCT_CODE_CHANGED=NO
HARNESS_WORKER_CAP_FIXED=YES|NO
REQUESTED_WORKERS_MATRIX=8,12,16,24
EFFECTIVE_WORKERS_PER_RUN=...
MAX_CONCURRENT_PHYSICAL_BINDS=...
GLOBAL_CONFIGURED_TPS_CEILING=800
WINDOW_SIZE=50
FIVE_K_AB_BA_VALID_RUNS=...
W8_STAGE_MEDIAN=...
W12_STAGE_MEDIAN=...
W16_STAGE_MEDIAN=...
W24_STAGE_MEDIAN=...
W8_E2E_WALL_MEDIAN=...
W12_E2E_WALL_MEDIAN=...
W16_E2E_WALL_MEDIAN=...
W24_E2E_WALL_MEDIAN=...
W24_VS_W8_STAGE_DELTA_PERCENT=...
W24_VS_W8_WALL_DELTA_PERCENT=...
WORKER_QUEUE_AND_BIND_CONTENTION=...
JOURNAL_PROJECTION_BACKLOG_AND_DRAIN=...
APPLICATION_RSS_PEAK_AND_RESOURCE_GATE=...
CORRECTNESS_AND_AT_MOST_ONCE=...
50K_RUN=NOT_RUN_BY_GATE|PASS|FAIL
600S_800PDU=NOT_RUN
PRODUCTION_DEFAULT_CHANGED=NO
CANARY_READY=NO
RECOMMENDED_WORKERS=8|12|16|24|NOT_VERIFIED
FINAL_DECISION=24_WORKER_PROMISING|OPTIMAL_12_OR_16|KEEP_8|NO_CHANGE_WITH_EVIDENCE|WORKER_SCALING_COUNTERPRODUCTIVE|HARNESS_INVALID|ENV_BLOCKED|CORRECTNESS_BLOCKED
COMMIT_PUSH=NOT_REQUESTED
```

**最终原则：优先性能、正确性、稳定性与低复杂度。只有 24 Workers 对完整 E2E 产生稳定、可复现且资源可接受的净收益，才建议下一步将其作为候选；不得将测试候选自动变成生产默认。**
