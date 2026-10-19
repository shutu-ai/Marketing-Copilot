# Marketing Copilot VNext — EulerOS 并发吞吐、跨天时效、阶段恢复与单 Writer 最终验收任务

**任务编号**：`MC-VNEXT-EULEROS-CONCURRENT-FINAL-ACCEPTANCE-20261017`  
**执行者**：Codex（先验证环境与证据，再执行；仅针对测试环境）  
**任务类型**：最终性能/可靠性验收；不做新的架构重构  
**源码冻结基线**：`71ca9e25f40d9c57932ab8c02a6b69877a099eba`（2026-10-17 GitHub `origin/main` 已核实）  
**仓库**：`https://github.com/shutu-ai/Marketing-Copilot`  
**对照旧基线**：`439a1842c4f1ed9144fd7605695435fca76c472c`，仅用于同环境、同工作负载 A/B；历史报告不自动转成新版 PASS。  
**首选目标环境**：此前已成功运行 VNext P5500 的 **EulerOS 2.0 SP15 / native ext4 / CPython 3.11.5 / DuckDB 1.2.0** 专用测试服务器（历史记录：`paas@10.202.245.70`）。**该地址是历史环境信息，不代表本任务已获得网络连接或授权；只在用户现有合法授权与可达条件下访问。**  
**当前验收状态**：代码和本地测试已完成；EulerOS 并发 E2E、持续负载和 native 稳定性 **未验证 / NO_GO**。本任务目标是补齐真实机器证据，而不是默认发布通过。

---

## 0. 不可变业务与工程约束（必须遵守）

1. **时效性第一**：前一业务日未向 SMSC Socket 写出的短信，在新业务日绝不可再次派发。允许过期未发送，禁止旧任务跨天补发。实际 Socket 已写出的 PDU/迟到响应只归属旧日，不得再次发送；不要把“已写出 Socket”误写为“已送达用户”。
2. **不为高 TPS 牺牲发信安全**：一条消息在 socket/write 状态不明确时不自动重试；保持现有 at-most-once-after-dispatch 语义。若检查点后、Socket 前崩溃可能少发，必须如实计为状态缺口，不能为了过关自动补发。
3. **本阶段只用一个 `ProjectionWriterProcess` 和一个 `vnext-projection.duckdb`**。保留 Set-based / Campaign 分组批量写入；**不实现每 Campaign Writer、不拆库、不加中间件、不建设复杂调度体系**。
4. **阶段可堵塞、可恢复**：发生短期局部故障，健康环节尽可能继续运行；安全容量用尽允许上游背压；不能无限排队、忙等待、死锁或重复发送。
5. **真实部署 TPS 契约**：Worker=8 属于业务并发，Bind pool=4 属于真实连接数，`tps=200` 与 `tps_scope=per_bind` 才表示 `4 × 200 = 800 submit_sm PDU/s` 的理论路由授权上限。不得将 `route_shared=200` 偷换为 800，也不得把多段短信数与短信任务数混为一谈。生产 SMSC 额度仍需按运营商合同独立核实。
6. **绝不接入生产 SMSC**：只允许 `127.0.0.1` 合成 SMPP Loopback；生成合成 MSISDN；不使用真实业务数据库/手机号/账号凭据；禁止正式部署及线上服务更改。
7. **测试环境安全边界**：非 root；在唯一新建 Run 根目录内放源码、venv、DuckDB、WAL、Spool、Journal、日志与结果；**不得操作前几轮 Run、历史证据和生产目录**。不要修改测试服务器防火墙、系统时间、系统服务或路由。旧日切测试通过注入 BusinessClock / 虚拟时钟完成，而非修改机器时间。
8. 遵守仓库 `AGENTS.md`：存在 `.codegraph/` 时先使用 CodeGraph 定位源码；保留所有 unrelated dirty/untracked 文件；**未经用户明确要求不得 `git commit` 或 `git push`**；不要把 DB/WAL、缓存、日志、运行数据、秘密值加入交付包。
9. **优先验证，最小修复**：发现阻断才对当前 VNext 源码做局部修复与回归。不得无证据开展第二轮架构重构。若不能获得测试服务器授权/访问/依赖，标为 `BLOCKED_ENV` 并提供缺口，不可宣称 PASS。

## 1. 先核对 GitHub 代码与已有证据，不要重复发明工具

在冻结源代码或临时 worktree 内检查并记录真实内容与参数，至少核对：

- `backend/vnext/business_day.py`：业务日、时区、cutoff、可注入时钟；当前默认 UTC，部署必须与实际营销业务时区一致，不允许默默根据服务器当前时区猜测。
- `backend/vnext/spool.py`：`business_day`/`expires_at`、epoch、closed-day、`iter_from` 的旧日拒绝、reclaim、restart/recovery 防跨天重派、容量会否回收；未知归属 V1 数据须 fail-closed。
- `backend/vnext/ingress.py` / `backend/services/vnext_service.py` / `backend/services/activity_scheduler.py`：新的 admission、跨日重置、日窗口结束后排空、不同 Campaign 的日统计与去重命名空间；检查旧 Legacy daily reset 与 VNext rollover 的先后顺序。
- `backend/vnext/worker.py` / `backend/vnext/smpp_transport.py`：取任务、获取 Bind、TPS Credit、Window 等待后，到**每个 PDU 紧邻 `writer.write`** 的 cutoff fence；multipart 已部分写入后的 partial/unknown 处理。
- `backend/vnext/pressure.py` / `backend/vnext/runtime.py`：`StageCredit` release-before-wait race 修复、Ingress 与 Journal 阶段的真实 wiring；背压不会造成死锁或无界循环。
- `backend/vnext/projection_writer.py`：单写入进程、Journal/offset、分组 set-based 批量 SQL、event_id 幂等、事务原子提交后才推进 offset、Campaign 公平轮询、每日统计隔离、暂停/重启恢复。
- `backend/tools/vnext_loopback_acceptance.py`：**已经改为先启动 Projection，再启动 Worker**；当前接受 `--count --workers --pool-size --route-tps --tps-scope --window-size --timeout --run-root --output --quiet`，读取实际 CLI `--help` 确认，不能虚构参数。
- `backend/tools/vnext_fault_injection.py`：核对是否与新增必填业务日兼容；旧工具可能尚需日期参数，必须先 smoke，不可把夹具错误冒充产品失败。
- `backend/tests/test_vnext_daily_expiry_projection.py`、`backend/tests/test_vnext_spool.py`、`backend/tests/test_vnext_projection_writer.py`。
- 两份当前报告：`Docs/Marketing_Copilot_VNext_Daily_Stage_Projection_Baseline_and_Code_Audit.md` 与 `Docs/Marketing_Copilot_VNext_Daily_Expiry_Stage_Recovery_Single_Writer_Closure_Report.md`。

**必须首先输出机器可读的来源证据**：Git SHA、tree SHA、源码归档 SHA256、测试脚本 SHA256、依赖清单、服务器内核/ext4/Python/DuckDB/CPU 资源边界、采用哪个业务时区。验证源代码身份后才开始性能试验。

## 2. 测试服务器准备与约束

### E0：合法授权、可达性与环境隔离

1. 使用已授权 SSH/受信连接确认此前 EulerOS 测试服务器可达。不可达时**停在环境阻断**，不绕过 VPN、账号、安全策略；也不自动退回 Windows `/mnt/d` 冒充 native ext4。
2. 查看上次报告中 `paas` 非 root、原生 ext4、CPython 3.11.5、DuckDB 1.2.0 的记录，确认当前机器条件是否一致；若服务器更换，客观记录差异。
3. 在用户授权的测试工作区建立一个**全新** Run，示例 `.../runs/vnext-daily-projection-final-20261017-<唯一后缀>`。所有生成文件都只能进入此 Run。拒绝覆盖旧路径，尤其不能直接操作 `backend/marketing_copilot.duckdb` 和既有 evidence archive。
4. 首选复用已有离线 CPython 3.11.5 运行时及 wheelhouse。不要无必要 `pip install` 在线拉包；严禁覆盖服务器全局 Python；安装不足必须列出具体版本/文件缺失。
5. 默认采用上次已有的资源护栏思路（如测试进程 CPU 集合、nice/ionice、RSS 2 GiB、磁盘上限/预警等）。**数字以当前服务器授权资源和已有 guard 脚本为准**，不可为赶进度偷偷取消护栏。因 50K/600 秒测试磁盘占用可能超过上次小场景，提前预估并经允许提高独立 Run 配额；磁盘将满时安全停止并保留证据。
6. 严格将 Socket 连接限制到 loopback。记录监听端口归属、PID、端口探测结果；禁止对外部 IP/真实 SMSC 发包。测试完只清理本 Run 的残留子进程。
7. Git 源与发布包一致性：避免用旧 `439a1842` 的打包产物测试新源码。禁止修改原始源码后仍声称测试 SHA `71ca9e25`。若必须修复代码，用独立补丁 SHA 标明新的 **Candidate**，重新执行全部相关门禁。

以下为**示意命令**，先确认仓库脚本、venv 和真实目录再执行；不可直接照抄历史目录：

```bash
set -euo pipefail
# 在新 Run 的 native ext4 路径下执行
pwd
uname -a
df -T .
python3.11 --version || python3 --version
python3 -c 'import duckdb; print("duckdb", duckdb.__version__)'
python3 -m pip freeze > deps.freeze.txt
# 先确认 sha256sum、ulimit、CPU/RSS/磁盘护栏工具是否可用
```

### E1：复现基础门禁

- 使用 71ca9e25 源码和干净隔离 Run；运行项目可用的 backend 全量回归及 VNext 重点测试；不要要求 Windows 409 passed 数字必须在 Linux 原样复制，需如实给出 Linux 结果及 skip。
- 前端此前已经 build PASS；若未改动前端，无需为性能试验重新编译；保留已有本地构建证据即可。
- 先运行 P10 → P100 → P1K，再进入 P5500；每阶段失败就暂停下一阶段并保留失败快照。所有场景只使用合成号码。
- 允许复用原 `--quiet` / `--output`，避免向屏幕输出 50K 条手机号或每 PDU 大日志。

示意调用（脚本参数以 `--help` 为准）：

```bash
python backend/tools/vnext_loopback_acceptance.py \
  --gate p5500 --workers 8 --pool-size 4 \
  --route-tps 200 --tps-scope per_bind --window-size 50 \
  --timeout 300 --run-root "$RUN_DIR/p5500-r1" \
  --output "$RUN_DIR/evidence/p5500-r1.json" --quiet
```

**补充注意**：`p5500` 工具中的 Campaign 分布为 `2500+2500+250+250`；其 `--count` 可以生成更大场景，但是否真覆盖“持续同时摄取”和“600s 不断流”，需要检查工具是否只是先一次性 enqueue。不得以一个扩大的 `--count` 替代在线流式长稳测试。

## 3. 容量与性能基准：真实并发发送 + 单 Projection Writer

### T1：配置/限额矩阵（隔离 Run）

| 场景 | Worker | 物理 Bind | TPS 设置 | Scope | 目标 |
|---|---:|---:|---:|---|---|
| 控制 | 4 | 4 | 200 | route_shared | 全 Route 总上限约 200 PDU/s |
| 低 Bind | 8 | 1 | 200 | per_bind | 总限额约 200 PDU/s，max_active_binds ≤1 |
| 目标 | **8** | **4** | **200** | **per_bind** | **总许可上限 800 PDU/s**，max_active_binds ≤4 |
| 窄窗口 | 8 | 4 | 200 | per_bind | window 1/2/50 的限流与无死锁 |

- 输出并核实实际启用 Worker、活跃 Bind、per-bind window、实际 PDU/s、峰值一分钟窗口（若有）、响应延时、排队等待、Worker Inflight、Projection lag。
- 800 是**配置上限**而不是任何测试环境必然达到的实测速度。若未接近该值，必须定位瓶颈来自 Producer、Spool/CRC/fsync、Journal、SMPP 模拟器、Window、Credit、Projection 或资源护栏，不能伪造 TPS。
- 统计中区分：`accepted_message_count`、`socket_written_pdu_count`、`submit_sm_resp_success`、`projection_events_applied`；分段短信不能按单 PDU 报任务完成。

### T2：P5500 并发正确性门禁

- 4 Campaign（`2500 + 2500 + 250 + 250`）、8 Worker、4 Bind、per_bind 200、window 50；**Projection 在发送开始前启动并持续写入**。
- 对账从业务明细到 wire 和投影：accepted / taken / expired / socket_written / socket_received / submit_sm_resp / unknown / pending / projection / duplicates / unexpected；“socket_written”与“response success”分别记录。
- 未注入故障时要求无非预期重复与遗漏、pending=0、max_active_binds≤4、journal offsets 全部追平、projection 数据准确。根据实际系统语义区分 `event_id` 与响应事件的不同唯一键，不得将合法 response update 当作重复抑制。
- 采集起止秒、整体吞吐与稳态区间的短窗口 PDU/s、RSS、CPU、磁盘与最大 lag。

### T3：50K+ 多 Campaign 积压及公平性

- 新建至少 4 个 Campaign，实时与批量均有代表；单 Campaign 大小不一致（如两个大活动与多个小活动），不允许一项任务长期饿死其他健康活动。
- 至少 50,000 **任务**，同时独立统计 submit_sm **PDU**；输入数据完全合成，消息长短可分两档覆盖单段/多段。
- 在 Producer 持续 enqueue 与 Sender/Projection 同步运行的条件下检查：长 Campaign 与小 Campaign 的进展曲线、不同 Route 的独立性、Projection 消费不影响 SMPP Socket 速率、bounded queue 内存/磁盘。
- 由观测数据决定需要调优哪里；不许仅为了让一个局部 benchmark 高分而拆分 Writer 或引入多 DuckDB。

### T4：持续负载 600 秒（独立于 50K 用例）

- 持续时长 ≥600 秒，**必须在测量窗口中持续提供足够任务供给**。50K 单段 PDU 即使达到 800/s，约一分钟多即可发完，不能作为真正的十分钟稳态负载。
- 可使用连续分批的合成流生产器或已有工具的 streaming 模式；若不存在则加最小的仅测试用 bounded feeder，不能使用无限内存列表。
- 若目标 800 PDU/s 满载运行 600s，需要约 480,000 PDU 的供给；以磁盘和内存护栏为约束事先预算，超出可承受范围时**客观降低速率/时长并标记门禁未满足**，不要假装达到满载 600s。
- 记录每 10s/60s TPS、Window/Bind 配置、producers backlog、spool pending/size、journal bytes/lag、writer batch SQL 时间、投影积压、RSS、CPU、磁盘、native crash、异常与自动恢复情况。
- 目的不是强求模型所推算的吞吐，而是取得**可重复、无持续积压、稳定的实际吞吐**与真实瓶颈。

### T5：旧/新源码性能 A/B

- 比较 `439a1842` 与 `71ca9e25`，各至少三次；**同一服务器、相同 native ext4、相同依赖/资源护栏、相同消息分布、same TPS/window/worker/pool，并避免缓存顺序偏置**。
- 重要：旧提交内 loopback harness 曾经是发送完毕后才启动 Projection；新提交改为并发。**两版不可直接拿旧历史 P5500 耗时与新 concurrent P5500 耗时作 apples-to-apples 结论。** 若需要准确 A/B，使用冻结的、版本无关的同一外部 harness 或同时测量可比较阶段指标；不可私自改旧提交生产逻辑并仍称源代码未变。
- 分别报告 send-only、concurrent E2E、Projection backlog catch-up、Writer 单独写入吞吐；说明对照可比性、置信限制和性能回退原因。
- 工程评判：优先确保完整链路无持续积压和无明显性能回退。可以把 800×90%=720 PDU/s 作为**暂定排查参考值**，**不得把它冒充已签署的产品 SLA**。未达到时写明实际吞吐及阻断因素，是否 GO 依据用户部署需求和可靠性门禁综合判定。

## 4. 日界、Spool 回收与禁止旧日重发

### T6：可控时钟端到端跨天测试（不得修改系统时钟）

必须采用 `backend/vnext/business_day.py` 的可注入 `BusinessClock`、独立时区和隔离 Run，模拟 23:59:59 → 次日 00:00:00 及日界附近窗口；先从正式部署配置识别真实营销业务时区，不能以服务器时区代替。

至少覆盖：

1. 当天发送窗口结束后（但未跨业务日）：**停止新源生成；当日 Spool 已入队任务继续发送/排空**，直到当日终点。检查与旧 Legacy `send_end_time` 规则的冲突：VNext 不应因为生成窗口结束而错误取消当天积压。
2. 到新业务日：旧日仍处于 Spool 未取、Worker 已预取、等待 Bind、等待 TPS Credit、等待 Window、以及 Worker 进程崩溃后重启的任务——全部**无新的旧日 PDU 被写入 Socket**。
3. 多段短信：在 cutoff 前已写入的部分不能再被重派或续发过期后新 PDU；必须保留 partial/unknown 标记与可对账证据。
4. 次日同 Campaign 同 source 重新生成**新业务日任务**：日命名空间新的 event_id 合法、允许新日按业务配置正常发送，但旧日队列不能混入；新日统计不含旧日延迟响应。
5. 未归属业务日的旧 V1 Spool 记录只能 fail-closed，不能借“兼容恢复”跨日写出。
6. 旧日 segment 回收：先 durable fence 与安全停止旧读者，再回收有明确归属的已关闭 segment；当前日元数据、Journal、仍需对账的发送结果不被误删；重复执行 cleanup 幂等。已完成回收后的磁盘容量可重新用来接收新日数据。
7. 重启日切、连续多日跳跃、日切中断、同一进程同时处理多个 Campaign、业务时区切换的边界。默认使用 UTC 还是指定业务时区必须记录并测试。

**T6 判定重点是时效性，而非昨天任务全部发完**：`old_day_socket_writes_after_cutoff == 0`、`old_day_redispatch_after_restart == 0`，只允许截止前 already-written PDU 的晚到响应。

## 5. 堵塞、背压与自动恢复

### T7：故障注入矩阵（独立隔离 Run，短暂停顿后自动恢复）

| 故障 | 注入方式 | 预期 |
|---|---|---|
| 文件/摄取停 30–60s | 暂停合成 feeder | Spool 已有任务继续发送；恢复后继续接收；不重复 ingest |
| 单 Route SMSC 不可用 | 仅操作 Loopback route/server | 其他健康 Route 继续；受影响 Route 不忙等待、不误重发未知结果；恢复后按现行语义继续 |
| 单 Worker 崩溃并重启 | 只终止本 Run 中可识别的 Worker 子进程 | 其他 Worker 正常；Supervisor 接管；cursor/lease 不产生双 owner；不自动重发已 dispatch 任务 |
| Projection 暂停 60s / crash | 注入 Writer 暂停或杀死本 Run 的 Writer 子进程 | Durable Journal 保存事件；有限积压；恢复后 offset 追平；幂等不重复累计 |
| Projection IPC Queue 满 | 用既有测试 hook/受限队列 | Hint 可丢、Journal 不丢；正常恢复；不过早阻塞其他活动 |
| Journal 到高水位/硬上限 | 受限容量的独立 Run | 未持久化结果之前不能继续无保护发送；有限背压、容量释放后唤醒，无死锁 |
| StageCredit 唤醒竞态 | 定向 fault test：release-before-wait 多轮 | 不能“有容量仍永久等待”；CPU 无忙轮询 |
| Spool 容量耗尽 | 小容量单独 Run | ingress 可见背压；既有安全任务尽可能排空；容量回收后自动恢复 |
| 跨天与上述故障叠加 | 虚拟 BusinessClock | 任何恢复路径不能重新发送前一天任务 |

要求：每个测试记录 paused/resumed 时间、queue 水位、恢复时间、Campaign×Route 进展、CPU/RSS、Journal bytes、Socket 事件与 offset。需要人工触发持续故障的测试，应有明确截止和清理，不能产生永远挂住的进程。

**重要业务权衡**：对瞬间 Writer 暂停可依赖 Journal 解耦；对长时间磁盘/Journal 容量耗尽必须停止新的危险发送准入，不能为了“环节互不影响”无界扩大 Journal。只修复观测到的故障，不引入新的多级调度平台。

## 6. 单 Projection Writer 的验证重点

- 确保只有一个数据库写 Owner；新版本 batch 采用 staging + set-based 聚合 `INSERT/UPSERT`，同一事务按 `(business_day, campaign_type, campaign_id, partition_id, route_id)` 分组汇总。
- 批内与跨批 `event_id` 去重，包括 Journal 重放和 commit-after-offset-before 场景。**同一事件只计一次，但真正不同的 `socket_written` 与 `response_success` 状态事件不可意外合并。**
- 单大 Campaign、大小混合 Campaign、低频 Campaign 同时存在时，检查 Reader round-robin + writer batch 调度公平性与最迟落库延迟，不要求每 Campaign 一个事务。
- 验证老代码 `for event in batch: _apply()` 相比新 Set-based SQL 在一致负载下的 SQL 数、事务数、CPU、I/O、TPS、P50/P95 lag，并同时检查 total send throughput 非回退。
- **禁止**本轮建设每 Campaign 独立进程 / DB；若日志证明单 Writer 是持续瓶颈，只在报告写 `DEFERRED_PER_CAMPAIGN_WRITER_AB_PROPOSAL`，以证据列出后续方案的预期收益和成本。

## 7. 重要缺口与防误判

1. **测试工具不是生产服务**。`vnext_loopback_acceptance.py` 直接使用 Spool、Worker、Projection；必须把它与实际 API/调度/Ingress 全链路验收分开标记。必要时复用现有服务级测试工具加最小 adapter，不得声称一个底层 harness 覆盖所有正式服务入口。
2. **P5500 当前 4 Campaign 但单路由**；Route 故障隔离需要另建至少两路由 Loopback 场景，不能用一个 Route 的 4 Campaign 冒充多 Route 隔离。
3. **原 `vnext_fault_injection.py` 可能缺失新增日期字段**；运行前兼容性核查，必要时只修测试夹具。失败必须区分 `TEST_HARNESS_ERROR` / `PRODUCT_BUG` / `BLOCKED_ENV`。
4. **测试机器环境不是原生 SLES12SP5**。EulerOS 验收只能称 `EULEROS_NATIVE_EXT4=PASS`，不能等同 `SLES12SP5=PASS`，也不代表真实 SMSC/生产上线。
5. 真实吞吐由发送、Journal、Projection、Spool 和资源共同决定；Credit-only benchmark 的 796/s **不是 E2E 800 TPS 实证**。
6. 正确性报告同时保留多层数量和逾期状态，**禁止把已按策略过期的短信统计为非预期遗漏，也禁止把非预期丢失掩盖在 expired 之中**。
7. **版本可信性**：若发现并修复产品缺陷，旧测试结果不自动覆盖新 patch；需要冻结修复后 SHA 并重跑相关全链路，不能宣称原 `71ca9e25` 通过修复后的门禁。

## 8. 交付证据与目录建议

保存到**新的**测试 Run `evidence/`，每场景一套机器可读文件；文件命名可以调整，但必须有：

- `source-identity.json`：baseline/new commit/tree/archive SHA256，命令、脚本 SHA、依赖 freeze、OS/kernel、ext4、资源护栏及业务时区；不要包含密码、真实号码或生产路径。
- `env-check.json` + `config-effective.json`：8 Worker / pool 4 / per_bind 200 / window 50 等当前真实生效值、SMSC loopback socket 标记、max binds。
- `e2e-p10.json`、`e2e-p100.json`、`e2e-p1k.json`、`e2e-p5500.json`、`multi-campaign-50k.json`、`steady-600s.json`；必须包含退出码、计数、起止、延迟、lag、峰值资源与卫护结果。
- `daily-boundary.json` + `spool-reclaim.json`：业务时区、虚拟时间、expired 计数、旧日写出 0、跨日 re-dispatch 0、可回收空间与重复运行结果。
- `fault-matrix.json`：每类故障的停止/恢复/继续工作证据、异常归属、恢复时长以及注入结束后的无残留进程记录。
- `projection-ab.json`、`full-e2e-ab.json`：至少 3 轮有效可比 runs、旧新源码、p50/p95、CPU/RSS/I/O、TPS、对账、是否可比及原因。
- `post-run-audit.txt`：Run 磁盘/日志总量、过程退出码、残留进程/监听检查、禁止覆盖历史文件的审计结果。
- `Marketing_Copilot_VNext_EulerOS_Concurrent_Final_Acceptance_Report.md`：最终人类可读报告；另存对应 JSON 总表。
- 合成测试 evidence bundle（不含 DB/WAL/Spool/Journal、venv、密码/源 CSV）。如果需要保留法证 raw evidence，应放在受控测试服务器专用目录，不能作为 Git 附件推送。

未提供的机理分析、日志、时间统计、吞吐数值不能凭空补写。出现旧版 SIGSEGV 或新 native crash 时收集退出码、PID、core/fault capture 能取得的堆栈；若 faulting frame 不可用就明确标记，不要用推测描述为已经证实。

## 9. 发布门禁与最终决策

| 门禁 | PASS 条件 | FAIL / BLOCKED 处理 |
|---|---|---|
| `SOURCE_IDENTITY` | 服务器测试源码、依赖、工具与新候选身份固定可复核 | 不明确 = BLOCKED，不运行大型场景 |
| `EULEROS_CONCURRENT_E2E` | 当前版本在 EulerOS/ext4 完成真实同时运行的 P10/P100/P1K/P5500；全量对账、无异常重复 | FAIL，排除测试夹具错误后局部修复并复验 |
| `BUSINESS_DAY_EXPIRY` | 旧任务次日写出=0、重启后重派=0；新日合法任务工作正常；安全回收 | 任一旧日新增 socket write = HARD FAIL |
| `STAGE_RECOVERY` | 至少关键 pause/resume、Worker crash、Writer crash、Journal 背压自动恢复测试；无死锁/异常重发 | FAIL；必要时局部修复 |
| `SINGLE_WRITER_PROJECTION` | 单 Writer/单 DB、Set-based 批处理、重放幂等、Lag 可测且可追赶 | 无证据或持续积压 = FAIL / NOT_VERIFIED |
| `PERFORMANCE_NON_REGRESSION` | 可比 A/B、50K+ 和 600s 负载有数据，整体吞吐与资源指标没有不可接受回退 | 无可比 A/B = NOT_VERIFIED；异常回退 = FAIL |
| `NATIVE_STABILITY` | 目标机器运行与异常注入期间没有无法解释的 native crash，资源/退出/残留审计完整 | 出现 crash = FAIL；缺证据 = NOT_VERIFIED |
| `REAL_SMSC` | **本任务不执行** | 永远 `NOT_RUN_BY_SCOPE`，不能用模拟器冒充 PASS |
| `SLES12SP5` | **如未在实际目标系统执行，不属于 EulerOS PASS** | `NOT_VERIFIED` |
| `PRODUCTION_DEPLOYMENT` | **本任务不执行** | `NOT_RUN_BY_SCOPE` |

**总体判定**：只有要求的 EulerOS/native/性能/正确性门禁都满足，才可写 `EULEROS_VNEXT_FINAL_CANARY_READY=YES`。此标记只代表通过了目标代理服务器上的合成验收，**不等同正式上线许可**。不满足时如实保留 `NO_GO / NOT_VERIFIED / BLOCKED_ENV`，不得通过修改验收条件来获得 PASS。

### 缺少实际完整 800TPS 的处理

如果 configured 800 PDU/s 但完整链路实测低于预期，必须提供瓶颈定位表（source backlog / spool append + fsync / Worker CPU / RouteCredit wait / SMPP window / Journal fsync / Projection transaction / ext4 flush / guard CPU 限额）。先根据真实瓶颈做**最小修复**，新代码标记修复后的 SHA 再复验；暂不实施多 Writer。

## 10. Codex 执行完成标准与输出格式

执行阶段：`E0 环境与源码身份 → E1 Smoke/回归 → T1/T2 并发门禁 → T3/T4 大容量与 600s → T5 A/B → T6 跨日 → T7 恢复 → 总结/收尾审计`。若硬失败则停止后续可能干扰诊断的测试并保存证据；独立故障场景可在隔离 Run 继续进行诊断。

最终答复应包含：

1. Git 新旧 SHA、是否修复修改源码、服务器 OS/ext4/Python/DuckDB/资源护栏、时区；
2. 全量回归、P5500、50K+、600s、A/B、跨天、故障注入各自真实 PASS/FAIL/BLOCKED/NOT_VERIFIED；
3. 实测稳态 PDU/s、Campaign 公平性、Projection lag/追赶、Journal 大小、CPU/RSS/磁盘；
4. 旧日未发送任务是否彻底作废且无次日派发；
5. 残留进程/Run 数据处理情况和归档 SHA256；
6. 两份报告（人读版 + 机器 JSON）及 evidence 路径；
7. 结论：`EULEROS_VNEXT_FINAL_CANARY_READY=YES/NO`，只在证据闭环时 YES；
8. **不得自动 commit/push，不得部署生产。**

**任务最终目标**：实测证明 VNext 在 8 Worker / 4 SMPP Bind / 每 Bind 200 TPS 的授权配置下，发送、Journal 与单 Projection Writer 可以真实并发运行；既能快速堵塞/恢复，又不会跨天补发过期短信，并有可重复的高吞吐和稳定性证据。只有实测确认单 Writer 才是持续瓶颈时，另立任务讨论活动级多 Writer / 多 DuckDB；本任务不实施。
