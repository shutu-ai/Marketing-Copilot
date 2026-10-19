# Marketing Copilot — 双 VPN 离线 DuckDB WAL / Loopback 验证收口 Codex 任务

**任务 ID**：`MC-DUAL-VPN-OFFLINE-DUCKDB-WAL-LOOPBACK-CLOSURE-20261016`  
**任务性质**：测试工具离线修复、离线交付、共享业务主机隔离复核；非架构重构、非正式 G4/G6 容量验收  
**开发仓库**：`shutu-ai/Marketing-Copilot`  
**已核实远端基线**：`origin/main = d307b6701c616d47e6ecce682fcd36548016c2eb`（2026-10-16）  
**目标环境**：实际检测为 EulerOS 2.0 SP15，视为 SLES12SP5 **兼容性对照环境**，不得标记为原生 SLES12SP5  
**共享服务器唯一允许的测试写入根**：`/home/paas/marketing-copilot-test`  
**当前状态**：`SHARED_HOST_VALIDATION_BLOCKED_OR_FAIL`，不改写此前失败结论  
**已完成证据**：独立 Python 3.11.5 / venv / 应用导入 PASS；G0 `135 passed / 51.53 s`；Loopback 被只读 DuckDB WAL 回放触发的外部扩展仓库访问尝试阻断；未开展双 Route、高负载、G4/G6  
**关键网络现实**：访问 GitHub/外部下载与访问内部共享测试服务器需要手动切换 VPN；同一执行阶段不得假设二者同时可达。

---

## 0. Codex 总目标

在**不变更共享服务器现有业务、系统或网络配置**的条件下，解决本地 E2E Runner 对隔离 DuckDB 执行只读 WAL 审计时，因未正确传递项目已打包的离线 `core_functions` 扩展路径而尝试访问远端 DuckDB 扩展仓库的问题。

先在开发机完成可回归的最小修复，并构造完全离线的、可校验的传输包；待用户**手动切换到服务器 VPN**，在指定目录执行独立 Python 环境的低影响 Loopback 验证；回到 GitHub VPN 后根据用户授权提交代码、同步报告。**绝不为测试而自动切换 VPN、调整路由或代理、修改服务器安全策略。**

**通过条件**不只是“命令不报错”，还包括：完整 WAL 审计能离线结束、SMPP Loopback 真实发送/响应对账正确、无外部连接、测试产生的所有写入不逃逸允许目录、证据能够复现。

## 1. 两种 VPN 和断点交接合同

| 阶段 | 网络条件 | 可以做 | 禁止做 | 阶段输出 |
|---|---|---|---|---|
| A：DEV / GitHub VPN | 可访问仓库和必要的外部依赖来源 | 源码审计、最小修复、单测、生成离线包和 SHA256 | 连接目标生产/共享服务器；假定能同时 SSH；改动现有系统代理/VPN | `dev-closure.md`、离线测试包、manifest、SHA256 |
| B：SERVER VPN | 可访问内部服务器（不假定能访问 GitHub） | 手动传输已冻结包、只读 T0、独立测试目录内小负载验证 | GitHub/pip 在线下载；系统服务安装；访问真实 SMSC/业务 API；修改系统配置 | `server-closure.md`、全部 Loopback/离线证据、失败日志 |
| C：DEV / GitHub VPN | 再次可访问仓库 | 导入 B 阶段证据、结果比对、必要的代码补丁与文档 | 丢失失败证据；将远端根因用开发机通过冒充；未授权 push | 最终主报告、测试摘要、待办结论 |

阶段切换是**人工操作**：Codex 只输出明确的 `PHASE_A_READY_FOR_VPN_SWITCH` / `PHASE_B_READY_FOR_VPN_SWITCH` 标记、文件清单和 SHA256；用户切换 VPN 并在可访问的一侧重新执行相应阶段。不得在断网后反复尝试 git/pip，也不得在服务器 VPN 下自动调用公有 API。

所有交接使用可复制的离线文件及 `sha256sum`，不得依赖远端 `git clone` 或服务器到互联网的镜像仓库。离线复制可以使用用户许可的 `scp`/SFTP/受控中转；**不得假定或猜测服务器登录名、IP、端口、跳板机、认证材料**。

## 2. 红线：共享服务器不可侵犯

1. 只允许读取主机 OS、CPU、内存、磁盘、挂载、当前负载以及被明确授权的测试内容；任何写入必须位于 `/home/paas/marketing-copilot-test` 下**本次新建且此前不存在**的 `runs/<unique-run-id>/`。禁止覆写、清理旧 Run 或删除任何用户现有文件。
2. 目录必须真实路径解析后仍在许可根下；用 `realpath`/`Path.resolve()` 校验根目录、输出文件、符号链接、解压 entry；禁止 `../` 逃逸和 symlink、hardlink、device node、FIFO 等风险归档条目。
3. 不使用 root 启动程序、测试、pip、DuckDB、SMSC 模拟器。不能证明非 root 账户拥有新 Run 目录写权限时输出 `BLOCKED_PERMISSIONS` 并停止。即使已 root 登录，也只能运行只读 T0。
4. 不修改 `/etc`、`/usr`、`/opt`、`/var`、`/tmp`、系统 Python/Anaconda、系统 OpenSSL、网络/防火墙、systemd、cron、真实数据库、客户数据、真实监控配置、其他用户 HOME 或业务文件。特别禁止标准包中的 `install_systemd_service`、`systemctl start/stop/restart`、`sudo pip install`、默认的 `/opt/marketing-copilot` 部署流程和任何 `--force` 覆盖安装。
5. 用正式 SLES12SP5 独立 Python 3.11.5 运行时（不是 `/opt/anaconda3`）；项目正式应用 wheelhouse 与之前独立 pytest 测试包分离；仅在 Run 本地创建 venv，离线安装 `pip --no-index --find-links`；不能改变正式交付包中的测试依赖策略。
6. `TMPDIR`、`TMP`、`TEMP`、`XDG_CACHE_HOME`、Python bytecode/pytest cache、DuckDB 扩展目录、DB/WAL、Journal、watch/inbox、上传、备份、锁、日志、输出、core dump（若可控）、控制器和子进程的 cwd 全部绑定本轮 Run 下。验证没有残留默认外写路径；无法确保时 FAIL-CLOSED。
7. SMSC 只能连接 `127.0.0.1` 回环测试模拟器；API 只能监听 `127.0.0.1` 动态未占用端口；不得打开公网监听、调用任何真实号码、外部 SMSC、外部 Webhook/代理或远程业务 API。不要操作已有监听端口；只清理由本轮自己启动并记录 PID 的进程，验证 PID 属于本次 Run，不允许 `pkill` 影响其他进程。
8. 共享资源保护：优先非侵入式低优先级运行；有界 CPU/内存/进程/IO；自检系统负载和可用磁盘/ inode，出现异常立即终止**本轮**测试；`nice`/`ionice` 只是优先级提示，不是假定有硬性隔离。硬性配额不可用时遵循更小规模上限，不能自行修改 cgroup、limits、systemd 或防火墙。业务高峰不运行测试。
9. 本任务仅允许先测试 10～100 条、授权后最多 1,000 条；**禁止自动启动 5,500、52K、102K、G4/G6、600 秒长稳**；不因测试未完成就提高负载、无限重试、延长运行时间。
10. 所有安全无法证明之处优先 `BLOCKED_*`，保留已有证据；不以 “忽略检查” 或 “临时允许联网” 绕过。

## 3. 已知技术线索（以实际源码复核为准）

GitHub `d307b670` 中的关键文件：

- `backend/database.py`：`duckdb_connection_config()` 使用 `MC_DUCKDB_EXTENSION_DIR` 或应用内 `offline/extensions`，按目录存在性传递 `extension_directory`；正常服务的 DuckDB 连接使用该配置。
- `backend/tools/local_e2e_acceptance.py`：`_db_query()` 当前为 `duckdb.connect(str(self.db_path), read_only=True)`，**未传 `config=`**；该入口被 `_db_snapshot()`、`_reconcile()` 等路径重复调用。
- 同文件 `_seed_sender_only_tasks()` 在 API 停止后使用 `duckdb.connect(str(self.db_path))`，也需排查离线一致性；可先处理本次涉及的只读路径，必要时扩展为受边界控制的工具统一连接入口。
- `backend/services/db_config.py` 的只读连接已经显式使用 `duckdb_connection_config()`，可作为语义参考，但不要为了测试侵入生产 DB owner/事务逻辑。
- `deploy/build-sles12sp5-system-python-manual.sh` 会随应用包携带匹配 DuckDB 版本的 `offline/extensions/<version>/<platform>/core_functions.duckdb_extension`；不要假设实际复制的旧包一定包含正确版本，须核对 manifest、目录和 SHA256。
- `test-artifacts/pytest-linux-bundle-20261016/README.md` 是**仅用于测试**的独立离线 pytest 依赖；`d307b670` 已推送，不能把该测试包混入正式应用生产发布包。

**待验证假设**：只读 WAL 回放触发 DuckDB 对 `core_functions` 的自动加载；Runner 没有传递应用已配置的离线扩展目录，因而触发访问外部扩展仓库。要用可重复的失败/通过 A/B 证明，不能根据症状直接宣布根因完全确诊。

## 4. 阶段 A：开发机审计、最小修复和离线打包

### A0. 冻结源与证据

1. 确认 `git status`、HEAD、remote，并记录基线 `d307b6701c616d47e6ecce682fcd36548016c2eb`；如在新提交上执行，记真实 SHA 和相对差异，禁止自动 reset/clean 覆盖用户工作区。
2. 阅读 `AGENTS.md`；如果存在 `.codegraph/`，先依仓库指引使用 CodeGraph。
3. 收集用户先前 EulerOS 测试报告（若本机可见）；如果不可见，只引用用户报告的事实，不假造远端日志详情。
4. 把历史 `SHARED_HOST_VALIDATION_BLOCKED_OR_FAIL`、135 passed、离线 WAL 回放网络失败记为固定基线。

### A1. 连接路径审计及离线修复

1. 全面搜索 Runner/test-tools 自身发起的 DuckDB 连接，包括 `_db_query()`、`_seed_sender_only_tasks()`、shutdown audit、测试辅助工具。区分应用自己的连接与 Runner 额外连接。不得把监控与审计迁回业务运行期间的频繁全库扫描。
2. 采用**最小公共连接配置**：Runner 自己的 DuckDB 连接使用与应用一致的 `extension_directory` 路径、DuckDB 1.2.0 及必要的只读语义。优先复用纯函数 / 轻量配置层；若直接导入 `backend/database.py` 会意外初始化应用全局 DB owner/后台状态，改用无副作用的共享 helper 并添加单元测试。避免复制容易偏移的硬编码路径。
3. 明确 `MC_DUCKDB_EXTENSION_DIR` 从父 Runner 传递到 API/SMSC/审计子进程；路径必须为本轮 Run 内的有效目录；若打包时确实需要复制已签名的扩展，应复制到本轮 Run 内并校验 SHA256 与 DuckDB 版本/平台对应关系。不得把 `.duckdb` 扩展下载到用户 HOME 目录。
4. 在测试环境实现**失去离线扩展时立即报错**，禁止把 `extensions.duckdb.org` 等网络下载当作自动修复。审查 DuckDB 1.2.0 实际支持的离线/自动安装开关；仅在确认不会妨碍已打包扩展的 WAL 正常回放后使用（不要盲目禁用必需的自动加载）。对数据库配置变更需限制在 Runner/测试进程范围内，不得更改全局生产默认。
5. Runner 的所有 `--run-root` 和 `--output` 必须显式指定到隔离目录；如果发现默认 `REPO_ROOT/Docs`、`~/.duckdb`、`~/.cache` 或 `/tmp` 写入，补充明确配置、预检或 `BLOCKED_ENV` 保护。不能仅依赖 `cwd` 假设安全。
6. 不改变 SMS 发送授权、结果 Journal fsync、unknown/partial、attempt/part ID、pause/cancel fence、路由默认主备、OCC/serialized 策略，也不改变 G4 合同或 checkpoint 默认值。

### A2. 测试覆盖（必须有能失败的反例）

- 缺少扩展目录、版本不匹配、扩展文件缺失、断网场景：应输出明确 `OFFLINE_EXTENSION_MISSING`/`BLOCKED_ENV`，不得偷偷下载或将失败视为 PASS。
- 在开发机上构造**独立测试数据库**、真实可回放 WAL 记录，并模拟 Runner 的只读连接；已配套离线扩展时必须能读取，且路径属于测试目录；如果无法稳定产生待回放 WAL，说明模拟覆盖不足，不能假报 WAL replay PASS。
- 验证只读审计查询与内部 Row/Task/Part 计数结果一致、连接关闭、没有业务数据库被写入。
- 覆盖 sender-only 的直接 DuckDB 连接，以及 `_reconcile` 的全部读库调用；对无关正式代码改动保持最小。
- 在本机复核 1K 或更小 Loopback 的 smoke；若外部依赖不可用，不允许改动当前合同冒充成功。
- 独立记录开发机 Python/DuckDB/扩展版本、OS、CPU/DB 热路径简要指标；不得把 WSL 成绩包装为服务器生产性能。

### A3. 生成完全离线的交付包

构造单一版本化交付包，内容至少包含：

```text
marketing-copilot-wal-offline-closure-<source-id>/
  MANIFEST.json                   # 源 SHA、commit/dirty 状态、依赖/运行时版本
  checksums.sha256                # 所有交付文件校验
  README-OFFLINE.md               # B 阶段非 root 手工执行说明与止损门槛
  app/                            # 本次精确源码/应用差异或完整独立应用副本
  runtime/                        # 已验证且已授权复制的独立 Python 运行时包（可单独传输）
  wheels/                         # 正式 app wheelhouse 与测试 pytest 包分开索引
  extensions/                     # 符合 DuckDB 1.2.0 的项目离线扩展
  tests/                          # 离线 smoke/只读 WAL 回放及报告生成入口
  reference/                      # 本地基线、预检及期望输出
```

实际包结构可简化，不得填入现有业务 DB、真实号码、SMSC 密码、VPN 配置、SSH key、生产环境变量或服务器上的用户文件。存放打包产物到开发机新目录（勿清理原有证据）。如运行时/官方 wheel 原包已存在，优先复用其 hash，不必重复下载。

包必须显式区分：`source_commit`、`application_artifact_sha256`、`python_runtime_sha256`、`pytest_bundle_sha256`、`extension_sha256` 和 `test_contract_version`。`d307b670` 的 pytest 包可作已知离线依赖基线，但不能代替修复后的应用源码包。

**A 阶段结束条件**：每个文件有 hash、没有外部依赖遗漏、README 能在无 GitHub/PyPI/DuckDB 网络的主机按顺序运行；打印 `PHASE_A_READY_FOR_VPN_SWITCH`。若不能做到，停在 `A_BLOCKED`，不得进入 B。

## 5. 阶段 B：切换至 SERVER VPN 后的隔离验证

### B0. 只读预检，不安装

核对真实 `uname`、`/etc/os-release`、`glibc`、文件系统、DuckDB 版本、当前业务负载、测试目录所有权及是否有本轮 Run 同名目录；将目标环境标记为 `EULEROS_2_0_SP15_COMPAT`，不假装原生 SLES12SP5。与上轮成功的环境基线比较，尤其注意服务器还承载其他业务。

只以已授权的非 root 账号进入；原 `/home/paas/marketing-copilot-test/runs/` 下历史 Run **只读**，不能覆盖或删除。本次 Run ID 新生成且排除命名冲突。目录无法安全创建、余量不足、宿主负载不适合，直接 `BLOCKED_SHARED_HOST`。

### B1. 先传文件、后验证 SHA，再离线安装

1. 用户手动切换 VPN；采用经批准的 SFTP/SCP 将 A 阶段包复制到本轮新 Run 内 `packages/`。不得从服务器执行 `git clone`、`git pull`、`curl`、`wget`、在线 pip 或网络访问 DuckDB 扩展仓库。
2. 解包前校验 archive SHA256 和归档所有条目：拒绝绝对路径、父目录逃逸、symlink 等非普通文件风险；以受限目标路径解包。**不要使用会覆盖已有目录的安装脚本或 `--force`**。
3. 在新 Run 内部署官方 SLES12SP5 独立 Python 3.11.5，创建隔离 `venv`；正式 app 离线 wheels 与独立 pytest wheels 分开安装，不修改系统 Anaconda。
4. 启动测试前显式配置所有 Runner 和子进程的 DB/extension/log/tmp/cache 路径为本轮 Run 绝对路径。记录有效配置及 `realpath`，确认离线 `core_functions` 版本/平台目录和 SHA256；缺失则 `BLOCKED_OFFLINE_DEPENDENCY`。
5. 校验 API/SMSC 仅监听新的 127.0.0.1 loopback 端口；绝不绑定公网、真实 SMSC 或业务系统；任何端口冲突都退出，不杀其他进程。

### B2. 有界、渐进的验证门禁

| 门禁 | 工作 | PASS 最低要求 |
|---|---|---|
| B2.1 | Python 3.11.5 + ssl/ctypes/venv、DuckDB import、扩展文件校验 | 完全离线、全在 Run 内 |
| B2.2 | 只读 WAL 审计专用复现 | 已存在 WAL 的测试 DB 在离线环境下可读；无远程扩展下载 |
| B2.3 | 小规模 G0 与 Runner 专项单测 | 通过，独立列出 skipped / blocked |
| B2.4 | 10～100 条完整本地 Loopback | queued/submit/response、result journal/DB、task/part/route 对账准确，无未知外发/重复 |
| B2.5 | 用户明确允许且资源安全时的 1K 对照 | `--run-root`/`--output` 显式隔离；采集耗时、CPU/RSS、磁盘延迟及审计结果 |

当 B2.2 未过，**不继续 B2.3/B2.4/B2.5**；任何阶段发生 DuckDB 网络请求、外写路径、异常 I/O、业务压力、真实 SMSC 连接企图，立刻 fail-closed，终止本轮自有进程并保留诊断证据。

双 Route 仅在 10～100 条 Loopback 先通过之后、明确确认独立两模拟器/端口和资源边界、并获得再次允许的前提下安排单独小规模验证；尤其关注 `mapped_submit_sm=0` 的历史问题。不通过时区分 config bind PASS 和完整 E2E FAIL。**本轮不执行 G4 正式矩阵和高并发容量测试。**

### B3. 验证没有越界与完成交接

- 记录 PID、进程树、监听端口、运行前后目录 manifest 和资源采样；只停止本轮自己创建的进程。
- 核对运行日志及可行的进程级连接审计；DuckDB/应用不得访问外部扩展仓库，应用不得访问任何外部服务。若只能证明“连接失败”而不能证明“无尝试”，结论写 `NETWORK_ATTEMPT_NOT_FULLY_VERIFIED`，不能称完全无外部访问。
- 同步生成 `server-closure.md`、`server-result.json`、`sha256sum` 和关键日志，只保存/导出必要脱敏证据；不得覆盖之前的 `SHARED_HOST_VALIDATION_BLOCKED_OR_FAIL` 报告。
- 保留 Run 原状、不得以 `rm -rf` 清理旧目录；只有用户明确授权时，才可对严格验证属于本次 Run 的临时文件做有界清理。
- 输出 `PHASE_B_READY_FOR_VPN_SWITCH`，提示用户**手动**切回 GitHub VPN 继续 C。

## 6. 阶段 C：本地整合、版本管理和收口报告

1. 获取 B 阶段导出证据，在开发机独立新目录校验 SHA256，确认来源 Run ID、OS、源码/应用/扩展 manifest、实际硬件和 VPN 分段条件。
2. 对照之前 EulerOS G0 135 passed、DuckDB 只读 WAL 阻断证据，明确列出 `OLD_FAIL → NEW_RESULT`，不能把不同配置或不同环境的值直接作等价 TPS。
3. 故障仍在时只定位下一关键阻断，不自动扩大实验范围、修改系统代理/网络设置或联系公网进行“热修复”。新代码需要返回 A 阶段重新打包并人工切 VPN，不能直接在服务器临时补丁后称源仓库已验证。
4. 在仓库内提交**最小必要代码修复与测试**和唯一新增 Closure 文档；遵守 `AGENTS.md`，**未经用户明确授权不得 commit 或 push**。对于已有 dirty 文件、历史日志、DB/WAL、缓存、测试运行物、`frontend/package-lock.json` 等一律保留且不纳入提交。
5. 如获得用户单独授权，且已经切回 GitHub VPN，再执行限定文件的 git commit/push，并核对真实远端 SHA。网络失败时保持 `LOCAL_COMMIT_NOT_PUSHED`，不自动操作系统网络代理、证书或 VPN。

建议最终报告：

`Docs/Marketing_Copilot_Dual_VPN_Offline_DuckDB_WAL_Loopback_Closure_Report.md`

报告至少包含：测试所用 **EulerOS 兼容环境** 标记、开发/服务器源码 SHA、Python 与 DuckDB 配置、两个 VPN 阶段交接文件清单及校验、只读 WAL 离线结果、Loopback 的 task/part/SMPP 映射、网络尝试证据、资源保护、之前失败证据索引、剩余高并发/WAL/observer 与 G4/G6 限制、是否适合进入下一阶段。

## 7. 严格状态判定

| 条件 | 状态 |
|---|---|
| Runner 离线 WAL 审计仍触发远程扩展请求 | `BLOCKED_OFFLINE_EXTENSION` |
| 没有安全的测试子目录、权限或资源预算 | `BLOCKED_SHARED_HOST` |
| 离线 Python/扩展/wheel 安装包不匹配 | `BLOCKED_OFFLINE_DEPENDENCY` |
| 只读 WAL 离线 PASS，但 Loopback E2E 未完整闭环 | `WAL_OFFLINE_PASS_E2E_FAIL` |
| 小规模 Loopback E2E 和结果审计均 PASS | `SHARED_HOST_SMALL_E2E_PASS`（仅限已验证负载） |
| 网络出口与文件外写无法得到足够观察证据 | `ISOLATION_NOT_FULLY_VERIFIED`，不能宣告隔离 PASS |
| 仍无 G4/G6 正式容量证明 | 整体仍保持 `INTEGRATED_NO_GO` |

**硬门禁**：不放宽正确性、unknown/partial、重复发送、发送授权、回滚与 Journal/WAL 要求；不为通过测试而开启外部下载；不把 EulerOS 的结果写成原生 SLES12SP5 验收结果；不把小规模通过替代 G4/G6。

## 8. Codex 执行方式（分阶段、无后台承诺）

首次在 **DEV/GitHub VPN** 下：

```text
执行 Docs/Marketing_Copilot_Dual_VPN_Offline_DuckDB_WAL_Closure_Codex_Task.md 的阶段 A。
当前仓库基线 d307b670，已知问题是 Runner 直接只读 DuckDB 连接遗漏离线 extension config。
只做代码最小修复、失败对照单测、离线交付包及 SHA256。
切换 VPN 由我手动操作，禁止你自动修改 VPN/代理/路由。
达到 PHASE_A_READY_FOR_VPN_SWITCH 后停止，不尝试直接连接远端共享服务器。
```

用户切到 **SERVER VPN** 后：

```text
执行同一任务的阶段 B，先读取阶段 A 的离线交付包和 manifest。
只使用 /home/paas/marketing-copilot-test 下本次新建 runs/<run-id> 目录。
使用正式独立 Python 3.11.5 和离线 app/test wheels，严禁访问 GitHub、PyPI 或扩展仓库。
先验证 WAL 离线回放，再经门禁执行 10～100 条 Loopback；1K 需单独允许。
保护服务器现有业务，禁止 root 运行测试，禁止高负载、真实 SMSC、系统级安装及全局配置修改。
达到 PHASE_B_READY_FOR_VPN_SWITCH 后停止，交付报告和证据包。
```

用户切回 **DEV/GitHub VPN** 后：

```text
执行阶段 C：导入并校验 B 阶段 evidence，完成唯一主报告。
未经我进一步明确授权，不要 commit、push 或修改当前 Git 工作树中无关文件。
保持 INTEGRATED_NO_GO，除非正式 G4/G6 后续另行通过。
```

---

**任务验收性质**：这是“离线 WAL 阻断 + 双 VPN 分段交付 + 共享服务器低负载验证”专项；不得解读为新的部署、重构或正式 S5 开放许可。
