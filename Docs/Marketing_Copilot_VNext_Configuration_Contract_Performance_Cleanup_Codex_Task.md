# Marketing Copilot VNext — Configuration Contract Audit, Binding Repair & Performance-First Cleanup

> **Codex 可执行任务书**  
> Task ID: `MC-VNEXT-CONFIG-CONTRACT-PERF-CLOSURE-20261017`  
> Project: `shutu-ai/Marketing-Copilot`  
> 工作方式：**一次完成配置审计、必要修复、冗余配置清理、针对性回归及最终报告**；不要再次拆成多份 Sx.y 任务。  
> 代码调查参考：`207c074c`（两份 release 文档提交），其文档内冻结的应用源码为 `c588cd2fc6967be6ee3d84f8e34e379530f969cd`。**实际执行前确认 `origin/main`、当前 HEAD、dirty worktree，按实际版本重新核实每项发现，不能假定文件仍未改变。**

## 0. 任务背景、目标与授权边界

Marketing Copilot 已由旧的 `smpp_gateway`/任务 Worker 模式转向 VNext 的 Ingress → Durable Spool → 分片 Worker → SMPP Transport → Progress Journal/Projection Writer 架构。现有数据库、API、前端和部署脚本仍继承大量旧参数；部分参数在 VNext 不再使用，部分虽被读取却未作用于真实行为，也可能有配置与运行时限速语义冲突。

**用户明确授权：如果某项配置或其附带功能对处理效率、吞吐、延迟或资源消耗具有已核实的明显负面影响，并且不是实际业务必须能力，允许取消该功能及其配置，不必为保留历史配置而增加兼容层。** 但绝不以破坏数据正确性、发送安全、可恢复性、实际 SMSC 协议限制来换取 TPS。

本任务目标：

1. 列出所有配置的**唯一来源、完整消费链、语义、生效时点、实际行为、性能成本**，找出遗漏绑定、死配置、冲突、旧架构残留和隐藏常量。
2. 将配置从数据库/UI/API/环境变量/部署脚本一路正确传递到 VNext Worker、SMPP 连接、Window、TPS、路由、心跳、重连和发送行为；**配置正确生效比维持旧的参数表面兼容更重要**。
3. 对经过验证无业务价值且明显拖慢热路径的配置/功能进行简化或删除；对重要能力保留但实现低成本；**不新增复杂配置中心、多级代理或不必要的安全抽象**。
4. 明确 Worker 数、SMPP Bind 数、TPS 配额的**不同维度**，按真实 SMSC 授权实现；最终能在服务器上按**有效运行配置**压测，而非依赖猜测的配置数值。
5. 产出代码、自动化行为测试、端到端配置核验、精简迁移说明及一份最终主报告；先完成此任务，再决定是否继续大型测试/新制品发布。

**范围内**：后端、前端、配置数据库及迁移、环境变量、启动/部署、VNext Runtime/Worker/Transport、限速与连接池、统计监控、压测工具、相关文档及测试。必要且证据充分时可以做**最小热路径优化**。  
**范围外**：重新发明整个 VNext 架构、切换数据库、引入额外服务/消息队列、连接真实 SMSC、真实号码发送、改动生产库、目标服务器正式部署、自动发布/推送。

### 0.1 仓库/环境保护

- 遵守仓库 `AGENTS.md`。若有 `.codegraph/`，先 CodeGraph 定位代码，再按需阅读。
- **不得未经用户新授权执行 `git commit`、`git push`、部署、真实 SMSC 测试。**
- 不得执行 `git reset --hard`、`git clean`，不得覆盖原有 dirty/untracked、`frontend/package-lock.json`、运行 DB/WAL/Journal、历史日志、`test-artifacts/`、offline wheelhouse 或旧证据。
- 使用独立 WSL AlmaLinux-8 原生 ext4 临时环境与数据目录，CPython 3.11，尽量复用现有依赖；勿在 `/mnt/d`/NTFS 目录混合测 DuckDB native 并发。
- 所有 E2E 仅使用 `127.0.0.1` SMPP Fast Simulator、合成号码/数据；性能模式 DLR off、模拟器不人为限速，故障模式单独执行。
- 此前干净发布包（冻结 SHA `c588cd2f...`）**不能代表本任务变更后的新源码**；不得将旧制品 SHA/旧 R2/R7 PASS 迁移成新版本的认证。

## 1. 第一里程碑：全仓配置调查（必须先完成，但不要停在仅出报告）

### 1.1 覆盖范围

同时检查并交叉关联：

- `backend/models/`、`backend/routers/`、`backend/services/`、`backend/vnext/`；包括 `smpp_configs` 表、Schema/migration、Pydantic 默认值、CRUD API、启动时配置加载、运行时 reload/update。
- `frontend/` 中的 SMPP/发送/Worker/系统设置/容量监控页面：显示的值、提交的字段、后端是否接收、是否有效、是否准确反馈重启要求。
- `.env*` 示例、systemd/manual/nohup 启动方式、`deploy/`、SLES/EulerOS 离线构建、安装配置、容器/测试 harness（如果存在）、文档与 README。
- 所有 `MC_*` 环境变量、代码中 `getenv`/默认值、CLI/测试参数，及所有显式/隐式数值常量：Window、Pool、TPS、Worker、Batch Size、Queue、Polling、Retry、Timeout、Journal fsync、Spool Limit、Projection flush 等。
- 真实生产 VNext 路径与 Legacy/test-only 路径分开统计，避免将旧路径的有效配置错误标记为全局无效。

### 1.2 对每项配置提供机器可读表

生成：

`Docs/Marketing_Copilot_VNext_Config_Contract_Inventory.md`

至少包含以下列（按值逐项，不得仅给总数）：

| 字段 | 要求 |
|---|---|
| `key` | 唯一配置键名/字段名，注明 env/DB/UI/CLI 来源 |
| `owner` | 归属 VNext / Legacy / shared / test / deploy |
| `declared_default` | 当前实际代码默认，查证而不是猜测 |
| `stored_value` / `effective_value` | 合成隔离环境中的加载值/最终值，保护密码和私有信息 |
| `read_path` | 代码位置、API/UI 入口 |
| `consumer` | 真实运行消费者及生效行为；不得以“传入 dict”代替消费证明 |
| `lifecycle` | 启动生效、热生效、需重启、无效/遗失 |
| `semantics` | 每 Worker / 每 Bind / 每 Route / 进程全局 / 活动级别及单位 |
| `performance_cost` | 热路径执行频次、锁、IO/fsync、IPC、CPU、内存影响；注明实测或推断 |
| `classification` | KEEP / FIX_BINDING / MERGE / REMOVE / LEGACY_ONLY / TEST_ONLY / NEEDS_CONTRACT_DECISION |
| `action_and_proof` | 保留/修复/删除/迁移方案、可复核测试 |

另提供完整机器可读 `config-inventory.json`（放隔离测试证据目录，若作为文档资产入库则确保无秘密）。

### 1.3 已核实的重点审计线索（不是允许跳过全仓审计的结论）

在此前 VNext 源码中发现：

1. `backend/services/vnext_service.py::_load_routes()` 查询 `smpp_configs` 时包含 `tps/window_size/timeout/...`，**没有包含 `pool_size`、`delivery_receipt_mode`、`bind_type`、`enquire_interval`、`reconnect_max_seconds`**。
2. `backend/vnext/worker.py::ShardWorker._transport()` 按 **Worker×Route** 懒创建一个 `AsyncSmppTransport`；并非由配置的 `pool_size` 严格决定 SMPP Bind 总数。
3. `backend/vnext/smpp_transport.py` 固定 `bind_transceiver`，部分 TON/NPI/编码被固定或重新计算；需要检查与 UI/DB 字段的一致性。
4. `backend/services/vnext_service.py::start()` 使用 `MC_VNEXT_WORKERS` 默认 `4`（夹取至 1..8），而不是 SMPP 连接池数；VNext 是否能跟踪配置修改后的 reload 需核实。
5. `backend/vnext/route_credits.py::RouteCreditBroker` 使用**每 PDU 跨进程文件锁、JSON 状态替换及 `fsync`**，同时采用 `SCHEDULE_SAFETY_FACTOR=1.15`；疑似明显热路径成本，且 200 TPS 配置在该算法下理论间隔约为 174 parts/s。须基于服务器实测分析实际损耗。
6. 旧 `backend/services/smpp_gateway.py` 与 `/api/smpp/stats` 已把 `tps` 定义为**整 Route 共享的 parts/s**；用户描述的旧运行配置是“4 个发送连接，每个 200 TPS，预期共 800 TPS”。两套语义**不能未经澄清就静默互换**。
7. SMPP 修改接口主要调用旧 Gateway 的 `invalidate_config_cache()`；VNext 启动时读取 route configs，修改后是否热生效仍要实际验证。
8. `priority` 在配置查询中参与排序，但默认路由回退存在最小 ID 选择逻辑；需要证明真正的优先级语义。

**核查期间如发现以上实现已被更新，以当前源码和行为测试为准；不能复制旧发现作为新结果。**

## 2. 第二里程碑：统一配置契约与性能优先的删改决策

### 2.1 配置契约：Worker、Bind、TPS、Window 完全分离

必须明确并落实：

- `worker_count`：业务数据处理并发，不自动等于实际 SMPP Bind 数。
- `pool_size`：**每个 SMPP 配置允许的物理 Bind 数**；多 Worker 共享/调度时同一配置的活跃 Bind 必须受此上限约束，不能随 Worker 数自动倍增。
- `window_size`：**单个物理 Bind** 的 PDU in-flight 上限，准确统计 acquire/release 与超时。
- `tps`：统一说明单位 `submit_sm parts/s`。必须有清晰的 **per-bind 与 route/account-shared 两种可能的业务配额语义**；仅在 SMSC 实际授权每 Bind 200 TPS 的前提下，才可以把 4 Bind × 200 作为 800 parts/s 名义总容量。
- 若配置目前不包含 `tps_scope` 或等效显式语义，提出**最小、清晰、兼容现存数据的迁移**；不得默认把旧 `route_shared=200` 静默升级为 800。需要时提供 `per_bind`/`route_shared` 显式选项，并给出明确默认、迁移和运行时告警。
- 如果物理 SMSC 只有账号共享的 200 TPS，4 Bind 也只能共享 200，严禁把连接数乘上账户配额。考虑同一 SMSC 账号下多个 route config 的共享限额与路由配置重复性。
- Bind 类型、地址 TON/NPI、编码、DLR、心跳、重连策略：有实际用处的正确绑定；没必要的高成本可简化或去除，但对仍展示给用户的选项必须给出一致的实现和文档。
- `enabled`、路由优先级、活动路由选择：保证完整行为正确，不能因一条 route 变化影响其他无关 route。

**性能设计要求**：物理 Bind 资源与限速配额都应由稳定且可观测的轻量级所有权实现。不得为每条 PDU 引入更多跨进程数据库事务、同步 RPC 或串行磁盘 fsync。如果现有架构可以复用进程内独占 Bind + 有界分发/低频额度租约，优先选择简单方案；不要为了处理极端情形新引入独立重型组件。

### 2.2 允许删除的配置和功能

满足以下条件的配置/功能可**直接删除或合并**，而非一律保留 Deprecated 兼容层：

1. 当前 VNext 正式业务路径没有消费者，或语义与其他配置完全重复。
2. 其存在引入可量化的额外 SQL、锁争用、IO、fsync、线程/进程唤醒、轮询、队列复制、内存或人为延迟，且没有真实业务收益。
3. 删除不会丢失仍需的核心业务能力，也不会弱化数据/发送守恒、持久化、at-most-once-after-dispatch、Unknown/Partial 隔离、Crash Recovery、合法 SMSC TPS/Window 边界。
4. 相关 UI、API、Schema/默认值、环境变量、部署示例、测试和帮助文档**一起同步清理**；已落库字段若不宜直接 DROP，则迁移为无消费者/不展示状态，说明向后兼容策略，不能在核心热路径反复解释无效字段。
5. 旧配置被删除时，旧环境变量不得悄悄继续控制不同功能。关键废弃项应启动时给出清晰告警或 fail-fast；无需维护永久的热路径兼容适配层。

无需为了“参数看起来功能全面”保留无效心跳、过细 retry/debug 旋钮、重复限流器、旧调度控制、慢速统计查询或多级回退设置。**是否移除由业务必要性和可复核的性能/复杂度收益共同决定**，不能只因为参数名看起来旧就删除。

### 2.3 始终不可通过“性能优化”牺牲的能力

- 已可能 Socket write 的任务，断线/超时/进程故障后不进行不安全自动重发；Unknown/Partial 的保守语义不变。
- Durable Spool、dispatch checkpoint、Journal/Projection 的准确性、持久化、恢复与对账。
- Campaign pause/cancel generation、授权路由/配额上限、PDU Window 与多 Campaign 独立进展。
- 真正配置为 required 的 DLR 不能伪造 delivered；性能模式可禁用 DLR，但不能借禁用模式误标为已送达。
- 不以删除故障可观测性、关闭审计缺口检测或忽略 SIGSEGV 的方式提高 TPS。

## 3. 第三里程碑：真正完成代码端到端修复与清理

### 3.1 绑定修复（必须有行为证明）

对每个 `FIX_BINDING` 配置修完定义→存储→加载→分发→最终消费者及状态/metrics；至少覆盖：

- `pool_size` 修改为 1/4 后真实 Bind 数变化；8 Worker、4 Bind 时同一路由**不能因为 Worker=8 而建立 8 个 Bind**。
- `tps`/`tps_scope` 生效并体现真实 PDU/s 的上界；如果合同确认 per-bind 模式，4×200 能具备接近 800 的名义预算；route-shared 模式下 4×200 不能凭空提高总路由速率。
- `window_size` 更改后每 Bind PDU window 与运行时指标一致。
- 启停、路由优先级、Campaign route mapping、生效时刻、负载中配置更新和旧 Bind/generation 安全排空。
- `bind_type`、地址参数、编码、DLR、心跳、重连等决定继续保留的设置，都必须用真实 SMPP PDU/故障测试证明，不得只检查属性/字典值。
- 用户可在配置页面/状态接口获取**脱敏的 effective config**、协议类型、每 route Bind 配额与当前活跃数、限速作用域/上限、Worker 数和变更需重启标记。不能暴露密码、真实号码等。

### 3.2 配置更新与 reload：追求简单明确，不追求万能热更新

- 变更无需安全热更新的高成本设置时，**可以选择“需安全重启后生效”**，而不是实现复杂的动态热替换；API/UI 必须明确提示并校验实际生效状态。
- 支持热更新的项目必须有 generation/fence、旧 Bind 排空、in-flight/unknown 处理及无额外重复发送证据。
- 对运行中无法安全修改的配置，拒绝变更或保存为 pending/restart-required；不得表现为成功生效却继续用旧缓存。
- 每次启动/测试输出统一 effective-config snapshot 和来源，便于测试服务器按实际部署值运行。

### 3.3 性能成本收敛（优先热路径）

- 特别审计 `RouteCreditBroker` 的每 PDU 锁文件、JSON write/rename、`fsync`、`asyncio.to_thread`、跨进程争用与 `1.15` 安全系数；记录 TPS-200、400、800 下 credit wait P50/P95/P99、CPU、IOPS、fsync/s、吞吐损失。
- 如证明确为热点，采用**最小且跨进程配额正确**的替代（如低频批量额度租约、唯一发送所有者、已有 Worker 的本地有界配额等），禁止无证据的新抽象和每 PDU 额外持久化。
- 不把“把限速关掉”作为优化：新实现要提供可验证的物理上限与必要的短期突发边界，包含多 Bind、多 Worker、重启/崩溃时的限额一致性。
- 删除配置系统引入的反复全表查询、不必要的配置 polling、重复缓存刷新、每发送热路径的无用日志或快照；优先启动加载或低频控制面方式。
- 不必把所有配置都热更新。少数高成本、低价值、难以安全热更新的选项可以干净地移除。
- 高性能不是唯一目标：确保失败退避、取消/暂停、旧 Socket/response 处理及 Durability 不发生退化。

## 4. 第四里程碑：分层验收（所有测试使用最终实现 SHA）

### 4.1 G-CFG — 配置清单及行为测试

- 全量 inventory 覆盖率 100%：环境变量、DB、API、UI、部署启动、VNext 核心模块和测试工具；每项归属、消费者/无消费者、默认值、生效时点、删改理由都可复核。
- 静态核查与运行中 effective-config snapshot 相符；随机选择的配置变更必须产生预期真实行为或明确拒绝/要求重启。
- P0 已知遗漏参数不再表现为“UI 已保存但 VNext 不生效”；无无声兼容/隐藏固定 TPS。
- 删除配置的代码/前端/文档/迁移完整，配置环境中不存在有害的双重控制。

### 4.2 G-SMPP — 真实 loopback Socket 配置矩阵

在专用隔离环境使用 Fast Simulator（`simulator_tps=0`，`response_delay=0`，DLR off）和合成数据；测试以下组合：

| Case | Business Workers | SMPP pool_size | TPS scope | 配置速率 | 核验重点 |
|---|---:|---:|---|---:|---|
| A | 4 | 1 | per_bind（若合同支持） | 200 | 只有 1 Bind，不超过其配额 |
| B | 8 | 4 | per_bind（若合同支持） | 每 Bind 200 | 精确最多 4 Bind，名义总 800，不自动创建 8 Bind |
| C | 8 | 4 | route_shared | 总 200 | 多 Bind 共享 200，不乘以 4 |
| D | 8 | 4 | per_bind（若合同支持） | 每 Bind 200 | 改 Window 1/2/50，PDU credit 严格守恒 |
| E | 8 | 4 | 任一已支持作用域 | 按实际值 | pause/cancel、重连、动态变更/需重启生效行为 |
| F | 8 | 4 | 任一已支持作用域 | 按实际值 | UCS2、multipart、Flash、未知响应、失败恢复 |

**如用户部署合同只有 route-shared 200，而不是每 Bind 200，则 Case B 只在明确的合成 per-bind 测试环境验证，不允许将 800 用于真实 SMSC。** 多 Route/同账号共享配额须覆盖至少一个边界用例。

分别记录 `tasks/s`、`submit_sm parts/s`、`submit_sm_resp parts/s`、每 Bind TPS、总 Route TPS、真实 Bind 数、Window 使用、响应延迟、Unknown/Partial、Journal/Projection 对账。负载不高时无需强行追求满速；吞吐验收要让待发送积压足够、排除模拟器成为瓶颈。

### 4.3 G-PERF — 删改前后同机同负载对照

- 基于原源码快照和最终源码快照，使用**相同服务器、运行时、模拟器、输入、实际连接配额、限速语义与采样方式**做 A/B；记录 CPU、RSS、fsync/s、IPC/lock wait、PDU/socket 响应 TPS、durable progress、尾延迟及运行异常。
- 将 Worker 扩容、Bind 数增多、TPS scope 改变的收益与**配置系统/热路径优化本身**的收益分开陈述，不能把获得更高配额当作性能优化成果。
- 对此前 `1.15` 因子和 per-PDU fsync 的收益/损失单列证据；若为真实瓶颈则修复，若不是则不要额外扩展架构。
- 如果配置目标为 4 Bind×每 Bind 200，报告写出 **800 为名义 PDU/s 配额，不是已实现的实际 TPS**；提供持续实际吞吐和瓶颈。低于名义能力明显时不得只宣布“配置绑定 PASS”；必须给出瓶颈和未达标状态。
- 测试工具不得每秒扫描全部 50K+ 任务或同步解析整个审计文件导致污染测量；采用增量计数、低频采样与可对账摘要。

### 4.4 G-STABILITY — 以应用正确性为硬门禁

- 后端全量 pytest、前端 build、VNext 关键协议单元测试与配置兼容迁移测试均按最终 SHA 执行。
- 至少执行 5,500 条应用层 loopback 回归，以及足够积压的多 Campaign+多 Bind 持续测试；在有实际授权的**专用测试服务器**上再执行 50K+ 和至少 600 秒有效压力测试。
- 结果必须可按 Campaign、Route、Worker、Bind、PDU 与持久化状态对账，不允许重复发送、伪造 ACK、丢失任务映射或忽略 native crash。
- SIGSEGV、坏账、无限增长队列、无界资源、Unknown/Partial 不安全自动重发任何一项出现即 `NO_GO`；不能仅以另一轮 PASS 覆盖。
- 保留既有 R2/R7 历史结果**只作为旧制品证据**；本任务并不自动授权 EulerOS 连接真实 SMSC、生产部署或发布替换。

### 4.5 测试无法执行时的准则

- 无目标测试服务器访问权限时，本地完成可执行的静态/单元/隔离 loopback 及性能对照，服务器规模门禁记为 `NOT_VERIFIED`，**不要声称 PASS**。
- 安全可解释的数学上限检查：如果测试期限短于 `required_parts / configured_aggregate_parts_tps`，如实提示参数矛盾，不能仅因超时认定应用性能问题，也不能私自修改性能门槛后宣布通过。
- 测试配置必须按运行时生效值记录：`worker_count`、route/bind count、每 Bind/Route TPS、Window、max_inflight、part count、模拟器独立能力和资源上限。

## 5. 最终交付与判定

### 5.1 必须交付

1. 修改后的最小必要源码、前端、Schema migration/启动参数，以及相应自动化回归测试。
2. `Docs/Marketing_Copilot_VNext_Config_Contract_Inventory.md`：完整配置矩阵，包括保留、移除、合并、遗漏修复、旧架构隔离、性能开销、配置生效证据。
3. `Docs/Marketing_Copilot_VNext_Config_Performance_Closure_Report.md`：**唯一主报告**，记录冻结 SHA、源码变更、配置 before/after、真实 Bind/TPS/Window、热路径基线/优化、各门禁、未解决风险、历史失败证据链接。
4. 如有重大 UI/DB 字段移除，提供最小迁移/回滚说明；不得在正式制品中额外捆绑调试产物、真实配置/凭据或测试数据。
5. 如项目具备自动化测试脚本，添加/更新**一个可重复执行的配置合同验证入口**，无需手工核查几十个接口。

### 5.2 统一完成判定

- `VNEXT_CONFIG_CONTRACT_READY`：全量配置已分类、实际生效行为已验证、P0 遗漏绑定/语义冲突已消除、代码/界面已同步清理，且自动化测试通过。
- `VNEXT_CONFIG_PERF_READY`：在上述基础上，性能对照证明确无明显配置系统热点遗留，或已针对真实热点完成最小优化并验证正确性；目标部署配置有可信的实际 TPS 数据。
- `VNEXT_CONFIG_NO_GO`：配置仍有失效项、Bind/TPS 意义不清、配额违约、数据一致性缺陷、native crash 或必需测试失败。
- `VNEXT_CONFIG_BLOCKED_ENV`：仅在目标环境无法开展关键测试时标识**哪些门禁未验证**；不得把其余 PASS 拼成全部完成。

**这两个 READY 只代表配置契约及其性能收敛，不等于新制品 EulerOS R2/R7 PASS、真实 SMSC 验收或生产上线批准。**

### 5.3 实施纪律

- 做一次完整配置审计，随后直接实施有证据的必要修复和精简；不要用“先审计，再等待新任务书”的方式停止。
- 每个删除/合并项只需简短说明影响与证据，不要为不使用的旧功能构建复杂兼容层。
- 以**吞吐优先、正确性硬约束、实现简单**为取舍原则；无需引入 RBAC、额外审批或无关安全层。
- 若发现现有 Legacy 功能确有回滚用途，保留其**隔离的 Legacy 路径**，而不是让 VNext 每条 PDU 为兼容它支付代价。若真的可以完全移除，也要交代删除后的回退方式。
- 本轮未经用户额外明确授权不得 `commit/push`；完成后汇报具体 diff、测试数据与门禁状态。

---

## 6. 给 Codex 的直接执行指令

按本文件连续执行。先读取 AGENTS.md、确认当前 HEAD、保护现有 dirty/untracked，并完成全仓配置清单；随后修复 VNext 中配置未绑定或语义不一致的问题。对没有实际业务价值而拖慢性能的配置功能，允许直接简化或移除，不必维持多余的兼容层。重点解决 `pool_size`/Worker/Bind/TPS 配额关系、配置热更新或安全重启、RouteCreditBroker 热路径性能，以及前后端运行时 effective-config 一致性。最后在隔离环境完成 G-CFG、G-SMPP、G-PERF、G-STABILITY，统一更新最终主报告。不得借提高吞吐绕过真实 SMSC 配额、持久化、发送安全或故障证据；不得擅自提交推送、连接真实 SMSC 或部署生产。
