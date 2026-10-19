# Marketing Copilot VNext — Clean Release Build & Source Provenance Closure

**Codex 执行任务｜2026-10-17｜Release Engineering / Offline Packaging / Evidence Closure**

> **定位**：在不重复 EulerOS 压测、不连接真实 SMSC、不修改生产环境的前提下，把已完成 Loopback 验收的 VNext 整理成**可追溯、干净、可核验**的候选离线发布包。本任务针对 P0 *package/source provenance*；不是 Native SIGSEGV RCA，也不是生产发布授权。

## 0. 权威基线与已知事实

- Repo：`https://github.com/shutu-ai/Marketing-Copilot`
- **冻结提交（本次任务开始的期望 SHA）**：`c588cd2fc6967be6ee3d84f8e34e379530f969cd`（`docs(vnext): finalize canary acceptance readiness`）。开始执行时先检查 HEAD、`origin/main`、工作树；即使远端又有新提交，也**不得静默切换基线**。若目标 SHA 缺失，停止并报告。
- `AGENTS.md`：保护已有 dirty/untracked；未经用户明确要求，不得执行 `git commit` / `git push`。如有 `.codegraph/`，先按仓库约定使用 CodeGraph 定位。
- 现有报告：
  - `Docs/Marketing_Copilot_VNext_Runtime_Canary_Final_Acceptance_and_Release_Readiness_Report.md`
  - `Docs/Marketing_Copilot_VNext_Final_Acceptance_Evidence_Reconciliation.md`
  - `Docs/Marketing_Copilot_VNext_EulerOS_R2_300s_Followup_Acceptance_Report.md`
  - `Docs/Marketing_Copilot_VNext_Native_SIGSEGV_RCA_and_Runtime_Stabilization_Report.md`
- 历史机器证据：EulerOS R2/P5500 最近一次 **聚合 PASS**：5500 accepted、Loopback `submit_sm_received=5500`、`submit_sm_resp_success=5500`、Projection unique/taken/socket_written `5500/5500/5500`；Guard `85.542s`、exit `0`、RSS `585560064 B`、清理 PASS。EulerOS R7 4000/4000 PASS。**本任务不重新执行**。
- 已有 `EVIDENCE_GAP`：R2 JSON 没有逐 Campaign `Projection unique/taken/socket_written`、逐 Campaign `submit_sm_resp_success`，以及独立 effective `response_delay_ms`；只能说明聚合 PASS，不能虚构逐 Campaign 机器字段。
- 历史 D7/R2 `SIGSEGV(-11)` 仍需保留；`RCA_LEVEL=RCA_STRONGLY_INDICATED`，`NATIVE_FAULTING_FRAME=UNAVAILABLE`，不能以新包或 300s HTTP 客户端修复称其已被证明解决。
- **P0 待闭环**：历史正式包 `release.json.git_sha=8f14e82d-dirty`，历史测试包 `bundle.json.source_commit=337f3e62...`，而最后验收基线为 `01409126...`、当前冻结基线为 `c588cd2...`。这些历史包保留证据身份，不覆盖、不改写。
- 原正式应用包外层 SHA256：`bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44`；历史独立 runtime SHA256：`11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`。
- 已知最新源码构建脚本 `deploy/build-sles12sp5-system-python-manual.sh` 目前执行 `git rev-parse --short HEAD` 写入 `release.json.git_sha`（**只提供短 SHA**），且使用 `git status --porcelain --untracked-files=no` 标记 `-dirty`；测试包脚本使用 `git rev-parse HEAD` 写入完整 `bundle.json.source_commit`。**不要误将短 SHA 与完整 SHA 字符串视为严格相等**。

## 1. 目标与非目标

### 必须完成

1. 从准确冻结的 Git commit 得到**隔离的 clean source snapshot**，不使用原始含用户未提交修改的工作目录打包。
2. 使用明确记录 SHA256 的离线 wheelhouse、DuckDB `core_functions` 扩展和 standalone CPython runtime，生成新的**正式应用包**与**独立测试包**；二者源码 commit 相同且二者互不混装。
3. 使正式包、测试包、依赖/运行时、构建输入及最终归档形成一条机器可校验的 provenance 链。正式包 `backend/release.json` 与根 `release.json` 应一致；目标为 `git_sha == 冻结完整 40 位 SHA`、`SOURCE_WORKTREE_CLEAN_AT_FREEZE=true`，杜绝 `-dirty`。
4. 检验正式包内外 SHA256/manifest、归档路径安全、内容隔离、依赖完整性、离线安装基本可用性。生成审计 JSON 和 Markdown 报告。
5. 明确区分**新发布候选的包构建 PASS**、**旧版在 EulerOS 验收的受测字节**、**真实 SMSC/生产批准**，绝不偷换。

### 禁止 / 不在范围

- 不连接 EulerOS 服务器、不切换 VPN、不登录其他共享服务器、不执行 R2/P5500、R7、52K/102K 或任何其他大负载压测。
- 不连接真实 SMSC，不调用外部业务接口，不触碰生产数据库、账号、配置、短信内容/真实号码；不发布到生产。
- 不升级 Python/DuckDB/Node/依赖包版本，不引入新消息队列，不重构分片、Spool、SMPP、Projection、发送完成语义。
- 不执行 `git reset --hard`、`git clean -fdx`、删除用户目录、覆盖历史包、擅自 `commit/push`；不要把新产物放入已有历史证据目录并覆盖原文件。
- 不因构建成功而宣称 `NATIVE_RCA_PROVED`、原生 SLES12SP5 实机测试 PASS 或 `PRODUCTION_APPROVAL=GRANTED`。

## 2. 执行阶段

### Phase P0 — 代码与工作区预检（只读）

1. 读 `AGENTS.md`、上述三份最新验收/对账报告与构建脚本。查看 `git status --porcelain=v1`、`git rev-parse HEAD`、`git rev-parse origin/main`；把冻结 commit 的对象身份、tree SHA、执行时间、现有 dirty/untracked **仅记摘要**，避免收集敏感路径。
2. 只在固定 SHA 存在且可访问时执行后续；如果起始 HEAD != 冻结 SHA，**不要**擅自 rebase、pull、checkout 或改主分支；另建隔离快照。
3. 检查所需离线输入在本地是否可用：`cache/wheelhouse/`、`release/` 中现有 wheelhouse，`cache/duckdb-extensions/<version>/linux_amd64_gcc4/core_functions.duckdb_extension`，standalone CPython 3.11.5 archive，pytest wheel set，前端本地可用的 npm 依赖缓存。为每项记录相对来源、实际 SHA256、版本、用途、许可输入标识；绝不把用户名下的证书、token、`*.env` 作为构建输入。
4. 明确正式包使用 `deploy/build-sles12sp5-system-python-manual.sh`，它生成的是**manual/nohup 型离线应用包**；仓库 `AGENTS.md` 中的 `MC_UPDATE_MODE=systemd` 默认与手动包的适用场景应在报告单列。**只构建，不实际选择或改变现场部署模式。**

### Phase P1 — 干净源码隔离与环境准备

1. 优先在 WSL AlmaLinux-8 的**原生 ext4** 下采用独立、未使用的临时路径做 detached worktree，例如 `git worktree add --detach <isolated-path> <frozen-full-sha>`；如该方式不适用于环境，可用本地 Git clone + detached checkout，但不得修改原工作区。`git worktree` 的添加/移除仅限本次明确创建的路径，不得触碰其他已有 worktree。
2. 在任何依赖、缓存、前端构建输出放入之前，校验：`rev-parse HEAD == expected_sha`；`git status --porcelain` 为空；`git diff --exit-code`；`git diff --cached --exit-code`。记录**冻结时源码 clean**，并保存关键 VNext 源文件的 Git blob SHA / file SHA256。
3. 外部输入、缓存、输出、临时测试数据库都应放在隔离 build-root 的单独目录；仅在构建脚本绝对需要仓库相对路径时，为可验证的离线 `cache` 输入建立严格 allowlist / 单独只读副本。记录该 hydration，不准因此把仓库中**用户**已有 untracked/dirty 文件带入产物。
4. 前端从冻结源码构建 `frontend/dist`（优先锁定 `package-lock.json` 并使用可用的缓存/已批准依赖；能用 `npm ci --offline` 则优先离线）。不得从原始脏工作树直接复制旧 `dist`。记录 node/npm 版本、lockfile SHA、构建命令和 `dist/index.html` SHA。如依赖确实缺失且无经批准的补充途径，写 `BLOCKED_ENV` 而非用旧文件伪装新 build。
5. 构建前后检查 tracked 源码 diff。允许明确列出的、构建所必需的生成物和输入位置；任何意外源文件修改、untracked 后端运行数据或秘密文件均为 FAIL。

### Phase P2 — 构建正式应用与测试包（不改变历史 artifact）

1. 由**隔离源码**运行现有正式构建脚本，显式传入既有离线 wheelhouse 的准确路径以及唯一 `--version` / `--output`；不使用 `find` 的第一个匹配文件作为未记录依赖。
2. 如缺少 DuckDB 离线 `core_functions` 对应版本文件，按 FAIL/BLOCKED_ENV 处理；不能自动联网下载或改 DuckDB 版本绕过。
3. 由同一冻结快照运行 `deploy/build-vnext-runtime-canary-test-bundle.sh --pytest-wheel-dir ... --version ... --output ...`；测试包要包含当前 300s Canary HTTP timeout 代码，必须保留明确的 `r2_authorized=false` **默认策略**，不能因为源包准备好就授权运行。
4. 复用已验证的 standalone CPython 3.11.5 原始 runtime 字节，不盲目重建或改写；复制/引用 runtime 并验证 SHA256，不将其合入应用包。
5. 两个新包及各自 `.sha256` 必须输出到本次全新的 release-candidate 目录；保留原始中间 build archive 与 final archive 的不同 SHA，禁止覆盖/冒名历史 tarball。

#### **关键 P0：完整 Git SHA 的可审计处理**

现有正式 build 脚本只能将短 SHA 写入 `release.json`，故**单纯从 clean worktree 执行脚本仍不能满足 `git_sha==完整 40 位 SHA` 的严格门禁**。

- 推荐采用 **clean frozen source + 显式 release-metadata finalization**：先由**未经修改**的冻结源码和其原版构建脚本产出 intermediate tarball，记录 intermediate SHA；在单独 staging 目录执行受控、可复现的 metadata finalizer，将**已验证的**完整 Git SHA 写入正式包 `backend/release.json` 和根 `release.json`，保持字段一致；重新计算整个包内部 `checksums.sha256`，生成**新的、不同名** final tarball 和 sidecar。finalizer 的脚本正文/hash、输入 SHA、输出 SHA、修改清单（仅两份 release metadata + checksum）必须写入 provenance receipt。不得直接在已经校验完成的 tar.gz 内字节替换，也不得对未受控文件执行自动重写。
- 也可在仓库中提出最小的正式构建脚本修复（`git rev-parse HEAD` 而非 `--short`）及其测试，但**本任务不允许擅自提交**；如果采用未提交的脚本修改打包，不得宣称“所有构建工具字节来自 clean frozen SHA”。必须单独记录 patched builder hash 与操作，且 `RELEASE_REPRODUCIBILITY=NOT_PROVEN/PENDING_COMMIT`，除非选择上述可审计 finalization 路径。
- `release.json.git_sha` 和 `bundle.json.source_commit` 若不能**准确**等于冻结完整 SHA，`SOURCE_PROVENANCE_EXACT=NO`；不得只比较前 8 位就报 `YES`。对“clean source”与“未经修改构建工具”使用**两个独立指标**，避免混淆。
- Release metadata 中保留 `build_time` 可使每次归档字节不同。没有明确的 timestamp 规范化时，不要求两次 gzip SHA 一样；必须至少证明**payload/manifest/source**一致，并把 `BYTE_FOR_BYTE_REPRODUCIBLE=NOT_PROVEN` 独立记录。不得混淆 hash 完整性与可复现性。

### Phase P3 — 安全、身份和数据边界审计

对**最终**正式包、测试包、runtime 包及输入分别检查：

1. 外层 `sha256sum` 与 `.sha256` sidecar 一致；包内 `checksums.sha256` 在安全、隔离目录下 100% PASS；成员数量实报，不预设必须等于旧包的 195/180/26。
2. Tar 成员名无绝对路径、盘符、`..` 逃逸、不可预期 hardlink/symlink/特殊文件；**先审计后解包**。不要使用可能覆盖敏感目录的 `tar -xf` 目标。
3. **正式包包含** `backend/release.json`、根 `release.json`、`frontend/dist/index.html`、`offline/wheels/`、`offline/requirements.txt`、`run.sh`、`checksums.sha256`、必要离线 extension。两个 release JSON 内容相同，完整 SHA 正确，版本字符串与 artifact 对应。
4. **正式包不能包含** `backend/tests`、`backend/tools`、pytest、测试 runner、`*.bak*`、`.env` 私密配置、key/cert/token/ssh 凭据、DB/WAL/Spool/Journal、logs、backups、uploads、runtime state、OS 临时文件或本地诊断数据；检查是按**文件角色**判别，勿误伤 `.duckdb_extension` 或业务源码里的 `spool.py` 名称。
5. **测试包不能包含**真实业务数据、正式应用软件包本体、已安装的运行环境、DB/WAL/日志、真实配置及凭据；测试包 `source_commit` 与正式包完全一致，`formal_deployment_package_modified=false`。
6. 对 `offline/wheels` 核实 wheel 数量、文件名、SHA、CPython tag、manylinux2014/glibc 2.17 适配声明、`requirements.txt` pin 与包内文件的匹配。若已有对应证据，可链接/重用；不为证明内容安全而擅自更换 wheel。
7. 做源码 blob vs 包内主要业务模块 byte-hash 的一一对照：`backend/vnext/{runtime,spool,worker,projection_writer,smpp_transport,diagnostics}.py`、`backend/services/vnext_service.py`、`backend/routers/vnext.py`、`backend/main.py` 等。允许仅记录明示的换行规范化文本变换，不得隐式更改生产源码。

### Phase P4 — 离线本地检查（无服务器/无高负载）

1. 在独立 WSL ext4 temporary Run Root 验证最终新包能安全展开、满足内部 manifest，校验 `python3.11` ABI/runtime 合同（若本机相应 runtime 不能执行，则 `BLOCKED_ENV`，不能声称 PASS）。
2. 若 Python runtime、离线依赖齐备：通过已隔离 venv 和 `pip --no-index --find-links <offline-wheels>` 安装，运行 `pip check`，测试 `ssl`, `_ctypes/ctypes`, `duckdb`, `smpplib` 等基本 imports；禁止真实发送和依赖外部服务。
3. 只运行构建/metadata/包边界的有针对性测试与必要 backend 静态/回归；不要运行 R2/P5500、R7，也不要将 smoke PASS 外推为服务器包运行验证。
4. 特别审计构建系统的离线路径/权限/可重复解包及 release.json 的读取逻辑；若涉及脚本小修，限于**打包逻辑与测试**，不修改核心发送代码。

### Phase P5 — 机器 Receipt、报告与状态判定

创建（不要覆盖既有报告）：

- `Docs/Marketing_Copilot_VNext_Clean_Release_Build_and_Source_Provenance_Closure_Report.md`
- `Docs/Marketing_Copilot_VNext_Release_Artifact_Identity_and_Safety_Audit.md`
- `test-artifacts/vnext-clean-release-<date-or-run-id>/release-provenance-receipt.json`
- 同一目录下 `SHA256SUMS`、输入/输出包 sidecar、finalizer/构建配方 hash、必要的脱敏验证日志。

**Receipt 必备字段**（至少）：

```json
{
  "task": "MC-VNEXT-CLEAN-RELEASE-PROVENANCE",
  "frozen_source_sha": "c588cd2fc6967be6ee3d84f8e34e379530f969cd",
  "source_tree_sha": "<from git>",
  "clean_at_freeze": true,
  "source_dirty_at_build": false,
  "builder_mode": "original_builder_plus_audited_metadata_finalizer",
  "builder_sha256": "<computed>",
  "finalizer_sha256": "<computed or not_applicable>",
  "frontend_lockfile_sha256": "<computed>",
  "wheelhouse_sha256": "<computed>",
  "duckdb_extension_sha256": "<computed>",
  "runtime_sha256": "11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97",
  "intermediate_application_sha256": "<computed>",
  "final_application_sha256": "<computed>",
  "test_bundle_sha256": "<computed>",
  "release_git_sha": "<read from final archive>",
  "test_source_commit": "<read from test archive>",
  "manifest_checks": "<pass/fail and counts>",
  "archive_safety": "<pass/fail and counts>",
  "forbidden_content_count": 0,
  "local_offline_smoke": "PASS|FAIL|BLOCKED_ENV|NOT_RUN",
  "tested_bytes_identical_to_historical_euleros_r2": false,
  "euleros_retest_performed": false,
  "real_smsc_test_performed": false,
  "production_approval": "NOT_GRANTED"
}
```

`<...>` 是需要 Codex 从实际文件/命令**填充**的占位符；不要保留占位符却标 PASS。`source_dirty_at_build` 应针对 Git tracked 源码和不允许的 untracked 源内容进行诚实判断；允许已审计的前端 `dist`、外部依赖 hydration、构建输出不能被误认为源码污染。

#### 最终状态矩阵

- `CLEAN_SOURCE_SNAPSHOT=PASS` 仅在固定 SHA、Git tree 和冻结时 clean 都已验证。
- `SOURCE_PROVENANCE_EXACT=YES` 仅在新正式包 full SHA、新测试包 full SHA、来源文件身份均正确且构建过程附有 provenance receipt。
- `RELEASE_PACKAGE_INTEGRITY=PASS` 仅在 final tar、sidecar、内部 manifest、敏感文件/角色排除、安全审计通过。
- `OFFLINE_INSTALL_SMOKE=PASS|BLOCKED_ENV|FAIL` 按真实检查填写。
- `CLEAN_RELEASE_BUILD_CLOSURE=PASS` 仅在 **clean source + exact SHA + artifacts integrity + offline smoke** 均达标；任一重要证据缺失则 `PARTIAL/BLOCKED_ENV/FAIL`，不允许模糊的 READY。
- `CURRENT_VNEXT_LOOPBACK_CANARY=PASS_REPORTED_EVIDENCE_GAP`：引用历史当前验收，不因 rebuild 擅自升级。
- `LATEST_EULEROS_R2_P5500=PASS`、`LATEST_EULEROS_R7=PASS`：引用现有**旧包的**受测证据；**新 clean 包尚未在 EulerOS 重验**，在 receipt 中必须单列 `NEW_RELEASE_CANDIDATE_EULEROS_VERIFIED=NO`。
- `HISTORICAL_D7_R2_NATIVE_FAILURE=PRESERVED`、`RCA_LEVEL=RCA_STRONGLY_INDICATED`、`NATIVE_FAULTING_FRAME=UNAVAILABLE`。
- `RELEASE_CANDIDATE_DOCUMENTED=YES`（在正式包身份闭环后），但 `PRODUCTION_APPROVAL=NOT_GRANTED`、`REAL_SMSC_APPROVAL=NOT_GRANTED`。
- `INTEGRATED_NO_GO` 属于历史原架构验收结果，**不覆盖、也不直接当作新 VNext clean release 的业务 FAIL**。

## 3. 工程细节与失败规则

- 严格 source / builder / intermediate archive / finalized archive / test archive / runtime archive 五类身份，不允许一句“都是同一版本”代替 SHA 证据。
- 不要求本次扩大 VNext 架构、添加 RBAC/审批系统、进行宏观 SLA 或重新分析 SIGSEGV；focus 是 Release Artifact provenance。
- 如果离线输入不足、依赖无法解析、打包脚本污染源码、正式包含运行数据、源 SHA 只能短匹配或 finalization 不可复现：保留已完成检查，标出具体阻断项，停止构建放行，**不要自动修改目标标准**。
- 原仓库 dirty 文件绝不复位、不清理；隔离 worktree 的回收须明确归属于本任务且确认无用户文件，再做安全清理。遇到安全策略拒绝删除时保留并报告目录，不使用强制系统级清理。
- `git diff --check`、生成文件 JSON schema/格式核验和所有适用回归完成后汇总提交候选文件清单；**没有显式用户授权则不 commit/push**。

## 4. Codex 最终答复要求

最终直接报告：冻结 commit；干净源码快照来源；新正式包/测试包/runtime 的**真实路径+真实 SHA256**；release.json 与 bundle.json 的真实完整 SHA；内外 manifest 对账；排除文件数量；本地离线安装与测试结果；`CLEAN_RELEASE_BUILD_CLOSURE` 明确 PASS 或阻断；历史 R2/R7 与新包尚未复验的边界；两份 Docs 报告与 JSON receipt 路径；Git 工作区影响、是否执行 commit/push（默认 NO）。

**成功定义：完成发布包的身份和安全闭环，而非重复证明 5500 条吞吐，更非授权生产上线。**
