# Marketing Copilot — SLES12SP5 共享业务服务器严格隔离验证（Codex 任务书）

**任务 ID**：`MC-SLES12SP5-SHARED-HOST-ISOLATED-VALIDATION-V3-20261016`  
**用户指定的测试路径范围**：`/home/paas`（仍须核对实际权限；已存在数据一律视为不可更改）。  
**本任务实际唯一测试写入根目录**：`/home/paas/marketing-copilot-test`（从该路径下创建新的唯一 `runs/<run-id>`；绝对不能直接向 `/home/paas` 根目录散落结果或覆盖既有文件）。  
**运行时硬性要求**：使用与 SLES12SP5 正式部署包配套的 **standalone CPython 3.11.5** 安装包，在每轮 `RUN_ROOT/python/` 独立安装，再由该解释器创建 `RUN_ROOT/venv/`；禁止以 `/opt/anaconda3/bin/python3.11` 为本次性能/兼容性正式测试解释器。  
**已知代码基线**：`shutu-ai/Marketing-Copilot`，`c3d31a99ff0aa3e0dd0472e197c6fe182f854e1d`（每次执行前只读核对远端 `origin/main`；若发生变化，应冻结明确 SHA，不得未经审核混用证据）。  
**参考合同**：`Docs/validation_contract.json`  
**最新主报告**：`Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Report.md`（新 SHA 的当前结果；报告正文内“未 commit/push”属于提交前记录，以远端 commit 为准）。  
**历史收口报告**：`Docs/Marketing_Copilot_Native_Stability_Throughput_Final_Closure_Report.md`。  
**上一基线**：`f5b72b8cc25e78bb5e7759b0f1ac358833187874`（仅作历史对照，禁止混同为最新源码）。  
**目的**：验证 SLES12SP5 native 运行/依赖兼容性、只读资源画像和有界的本地模拟短信测试，为开发机与准生产环境性能差异提供可解释的证据。**不是生产部署，也不授权运行 5500-task、G4/G6 大容量压测。**

---

## V3 变更摘要与当前源验收上下文

本版仅更新**测试目标与安全验收要求**，不是新的架构重构/生产切换授权。相对 V2 的变化：

1. 测试源固定为已推送 `origin/main` 的 **`c3d31a99ff0aa3e0dd0472e197c6fe182f854e1d`**。禁止误用原 V2 的 `f5b72b8c` 源码或把旧应用部署包性能视为新 SHA 性能；源码、部署包、Python runtime、wheelhouse 与本轮测试数据分别记录 SHA256/版本。
2. 新增 **双 Route 的明确语义**：默认 `SmppGateway.connect()` 仍为主备；仅在授权的显式并行模式 `MC_SMPP_PARALLEL_BIND_REQUIRED=1` 下要求两条 Route 真实 bind。必须核对 `gateway connected`、模拟器 bind、真实 socket submit、响应、attempt/part/任务映射与 fail-closed；**bind PASS ≠ E2E PASS**。
3. 开发侧当前源 G0 `351 passed, 2 skipped`，前端 build PASS。Windows 两种双 Route smoke 的配置审计已 PASS，但整体 `mapped_submit_sm=0`、`STALL_CAPTURED`/fairness FAIL；WSL 双路 runner API readiness 阶段也失败。共享服务器测试不得隐瞒这些未闭环项。
4. 开发侧 5500-task：OCC 在 300s 内约 `3134` submit、`68` 次 OCC conflict retry；serialized 约 `1829` submit、write-lock watchdog；**两种模式均严格 FAIL**。这不是授权在共享服务器重跑 5500 的理由；T2 只能用经批准的小规模负载进行有资源指标的诊断对照。
5. `Docs/validation_contract.json` 保持**只读且原样**，其中 `52K / 102K` 映射和单 Route 200 TPS、180s 完成期限矛盾仍是 `SPEC_INFEASIBLE_NEEDS_DECISION`。禁止本任务代用户修改合同、宣布 G4/G6 通过或执行完整容量测试。
6. 所有主机保护条件、独立 standalone Python 3.11.5、测试目录 `/home/paas/marketing-copilot-test`、非 root、无现有系统/业务文件改动等**原有硬门禁保持不变**。

## 0. 最优先执行的主机安全合同（不可协商）

本机 `paas-core01` 是承载其他业务的**共享生产/业务服务器**。本任务优先级：**现有业务安全 > 文件/环境隔离 > 结果正确性 > 性能测试进度**。默认只执行只读预检（T0）；写入、启动进程、压力测试均须满足相应门禁。

### 0.1 严格禁止

1. **严禁**对 `/home/paas/marketing-copilot-test` 以外的任何现有文件、目录、数据库、数据表、配置、日志、缓存、用户文件执行新建、修改、覆盖或删除；不能仅凭脚本声称“测试模式”而豁免。所有可控的测试写入应落在指定根目录下。操作系统可能自行产生日志/进程记账，无法靠应用目录完全消除这一点，因此不能宣称绝对零宿主副作用。
2. **严禁**运行 `zypper`、`rpm` 安装/卸载、`sudo pip`、`pip install --user`、系统 Python/Anaconda 升级、`ldconfig`、`sysctl`、`mount/umount`、修改 swap/cgroups/NUMA 全局配置、`firewall-cmd`/`iptables`、`useradd`、`chown -R` 既有目录、修改 `/etc`、`/usr`、`/var`、`/tmp`、`/root`、`/opt/anaconda3` 或现有业务路径。
3. **严禁**执行仓库中的生产应用安装、更新、systemd 启停、注册服务、备份清理脚本（例如生产包根目录的应用 `install.sh`、`deploy/install-sles12sp5-offline.sh`、`systemctl ...`、`deploy/run-*`）；不创建系统定时器、服务、容器、后台常驻程序。**唯一例外**是 §3.1a 指定、且已逐行审计、严格 `--prefix "$RUN_ROOT"`、非 root 执行的 **standalone Python runtime 包自带 `install.sh`**；任何其他安装器均禁止。
4. **严禁**连接真实 SMSC、真实用户/客户号码、生产数据库、生产 RabbitMQ/SFTP、外部 Webhook、远程 API 或任何业务端口；模拟器/API 只允许 `127.0.0.1`（可显式核验 IPv6 loopback，但不得绑定 `0.0.0.0`）。仅使用合成号码、合成白名单、合成文件。
5. **严禁**对任何不由本次测试启动的 PID 发送信号，禁止 `killall`、`pkill -f`、全局进程清理。禁止 `rm -rf`、`git clean`、`git reset --hard`；即使是本次产生的证据，也默认保留，不自动删除。
6. **严禁**因兼容性/性能失败，自动放宽现有正确性、Journal fsync、attempt/part fencing、Unknown/Partial 不盲重试、隔离、公平性、G4/G6 目标或 validation contract。
7. **严禁**“悄悄修复”共享服务器的系统权限/依赖/内核设置。发现阻断，记录 `BLOCKED_ENV` 并停止当前阶段，提交所需**最小人工操作说明**，但 Codex 不代执行。
8. **严禁**在此机上使用 `root` 身份执行安装、Python 单元测试、SMPP 模拟器或应用压测。已有 root SSH 会话**仅允许只读 T0 采集**；使用已经批准、具有该目录权限的非 root 用户运行后续步骤。若无此用户，暂停并请求管理员安排，不自行创建或改变用户权限。

### 0.2 写路径与执行边界

- 只有 `/home/paas` **已经获授权**、真实路径及所属挂载核对通过、非符号链接、且测试账号对其有既有写权限，才可开始 T1。如果专用测试目录不存在，**仅允许非 root 测试账号在获授权后创建 `/home/paas/marketing-copilot-test`（0700）**；如果已存在，须证明确属本次测试且不含他人文件后才能使用。不可自动修复权限。
- 不得修改/覆盖该根目录内可能已经存在的文件。新一轮使用事先检查不存在的**全新唯一子目录**：`/home/paas/marketing-copilot-test/runs/<run-id>/`；只向本轮创建的子目录写入。若根目录已存在业务文件、symlink、异常挂载或归属不明，先阻断，不递归变更权限或清理。
- 程序、依赖、DB/WAL/Journal/SQLite、临时文件、pip/cache/pycache、用户 home、前端/静态数据、日志、pid/lock、模拟器 audit、报表及异常诊断文件都必须在本轮子目录。任何默认写出界路径不能确认安全时不启动程序。
- 单独冻结 `source SHA` + `runtime/wheel SHA` + 配置 + 端口 + 运行目录 manifest。不可直接引用开发环境的数据库、日志、业务文件或已有运行数据。
- 所有子进程必须由本次测试 Runner 可追踪，并有明确的优雅停止和收尾逻辑；测试结束后检查**本次**端口、子进程、文件句柄是否释放，不操作其他业务进程。
- 禁止自动上传/推送文件或 Git commit/push；报告、脚本、测试结果先留在本轮目录，供用户人工决定是否回传开发仓库。

---

## 1. 已知主机信息（只作为初始输入，T0 必须复核）

| 项目 | 已知信息 |
| --- | --- |
| 操作系统 | SUSE Linux Enterprise Server 12 SP5，Linux 4.12.14-122.307-default，x86_64 |
| CPU | 2 × Intel Xeon Gold 6230N；40 物理核 / 80 逻辑 CPU；2 NUMA nodes |
| 内存 | 375 GiB；采样时 `MemAvailable` 约 187.6 GiB；Swap 8 GiB |
| 文件系统 | `/home/paas` 的实际挂载和可用容量未知（须用 `findmnt -T /home/paas` 与 `df -hT /home/paas` 实测，不能套用 `/opt` 的磁盘结论） |
| Python | `/opt/anaconda3/bin/python3.11`，3.11.5；不允许修改 |
| glibc | 2.22 |
| 当前负载 | 采样时 load average ≈12、CPU idle ≈88–93%；**只是瞬时值** |
| 业务性质 | 非专用主机，拥有现有业务服务及数据库；**不能将总可用资源视为测试配额** |

开发报告使用 CPython 3.11.13 / DuckDB 1.2.0（WSL AlmaLinux-8/native ext4）。SLES 端必须使用交付包 standalone Python 3.11.5 与 glibc 2.22 的组合单独校验，不能自动继承 WSL 性能结论，也不能以现有 Anaconda 代替正式包 runtime。

## 2. T0 — 只读预检查（可在 root 会话进行；只读，不落盘到主机）

先人工或以只读方式运行以下命令；若需保存输出，**仅在通过 T1 写权限门禁后**由非 root 用户写到指定 run 下，或复制终端文本回开发电脑。

```bash
id
cat /etc/os-release
uname -a
lscpu | sed -n '1,45p'
free -h
uptime
vmstat 1 3

df -hT /home/paas
findmnt -T /home/paas
stat -c '%F %U:%G %A %n' /home /home/paas /home/paas/marketing-copilot-test 2>&1 || true
readlink -f /home/paas 2>&1 || true
readlink -f /home/paas/marketing-copilot-test 2>&1 || true
command -v python3.11 || true
/opt/anaconda3/bin/python3.11 --version
/opt/anaconda3/bin/python3.11 -m pip --version 2>&1 || true
getconf GNU_LIBC_VERSION
command -v taskset || true
command -v nice || true
command -v ionice || true
command -v timeout || true
command -v iostat || true
# 对现有测试目录只读探测（禁止递归扫描 /home/paas）
ls -ld /home/paas /home/paas/marketing-copilot-test 2>&1 || true
```

再核查（不改变系统）：

- `/home/paas` 必须已获授权、确认为预期目录、非 symlink，且不能把它当成空目录；只允许轻量元数据检查。`/home/paas/marketing-copilot-test` 如已存在则只读核对归属和用途；若不存在，可以在 **T0 结束且获得 T1 执行授权后**由已授权非 root 账号以 `mkdir -m 0700` 新建这一个专用目录；不得对 `/home/paas` 本身进行递归 chmod/chown/mkdir/清理。
- 目标 `/home/paas` 实际挂载的可用空间和是否存在共享 I/O 压力；不对其做破坏性 I/O benchmark。
- 当前允许使用的非 root 账号、其目录写权限和允许的 CPU/内存/磁盘 I/O **业务配额**；若未明确授权，停在 T0。
- 测试工具是否默认在 `/tmp`、用户 HOME、源码树、`/var/log`、systemd 或全局 Python 安装路径写入；无法控制的一律记录阻断。
- 读取仓库脚本而不执行：`AGENTS.md`、`deploy/README.md`、SLES 手动部署说明、`backend/tools/local_e2e_acceptance.py`、`backend/tools/mixed_campaign_capacity_acceptance.py`、`backend/database.py`、`backend/services/result_journal_service.py`；特别注意仓库部署样例默认 `MC_HOST=0.0.0.0`、`MC_PORT=19090` 和 `/opt/marketing-copilot/...`，**本任务不能沿用这些默认值**。

输出 `T0_Preflight.md`（先在本地/终端整理；T1 许可后才写本轮测试目录），标记 `SAFE_FOR_T1` 或 `BLOCKED_ENV`，列出每个阻断项与需要的人工操作；**不得自行越权处理**。

## 3. T1 — 创建单轮隔离工作区与独立运行环境（必须获授权）

前提：T0 `SAFE_FOR_T1`、获得共享服务器业务负责人测试窗口许可、用户已通过非 root 账号登录、根目录已获授权、可用空间与资源配额已确认。

### 3.1 必要的 fail-closed 检查

在写入前，检查运行用户 `id -u != 0`、`realpath -e /home/paas == /home/paas`、专用目录非 symlink 且真实路径 == `/home/paas/marketing-copilot-test`、该目录确实属于本任务且具有权限、`runs/<run-id>` 不存在；任何失败立即停止。目录不存在时仅在已批准 T1 的前提下、非 root 用户使用不可覆盖的 `mkdir -m 0700` 创建新目录。**不可为绕过检查而修复既有权限。**

只允许在该根目录内创建本次全新 `runs/<run-id>`，其中建议：

```text
/home/paas/marketing-copilot-test/runs/<run-id>/
├── packages/           # 本轮复制的正式 SLES Python/runtime 与 App 离线包
├── python/             # 来自正式交付包的 Python 3.11.5（全路径仅在本轮内）
├── src/                # 精确冻结的源码副本，只从已审核包/仓库获取
├── offline/            # 本轮离线 app wheels / DuckDB 扩展等
├── wheels/             # 审核过的、兼容 SLES 的本地 wheelhouse（如适用）
├── venv/               # 由本轮 python/bin/python3.11 创建的 venv
├── env/                # 运行环境记录、非秘密配置
├── home/               # 本进程 HOME
├── tmp/                # 所有测试临时文件
├── cache/              # pip/XDG/bytecode 等缓存
├── data/               # 合成文件/白名单
├── db/                 # 唯一测试 DuckDB / WAL
├── journal/            # 结果 Journal
├── state/              # db_config/auth/instance lock/状态
├── logs/               # API/runner/simulator 日志
├── evidence/           # 指标、审计、崩溃/失败证据
└── output/             # 验收 JSON、阶段报告
```

**重要细节**：不允许解压不受信任 tar 直接覆盖目标；先审查 archive 条目中 `..`、绝对路径、symlink/hardlink 和 post-install scripts，确认为只在新 run 内展开后才执行。只允许本轮严格审计通过的 standalone Python runtime 安装器使用安全前缀；任何可能修改宿主环境的应用安装脚本均不得执行。

### 3.1a — 正式部署包 Python 3.11.5 的独立安装（本次新增关键要求）

使用**项目真实交付的** `sles12sp5-python-3.11.5-*.tar.gz` runtime 包（包括可能采用 `*-script.tar.gz` 命名的正式版本），以及与此 runtime 配套的 `marketing-copilot-sles12sp5-system-python-*.tar.gz` 应用包；由用户提供或经批准的离线拷贝，不能假定服务器已经存在相同文件，也不可读取/覆盖 `/opt/marketing-copilot` 的现有安装。

1. **先证实来源**：记录完整包名、大小、SHA256、交付版本、是否含 runtime `install.sh`、`runtime.json`、`checksums.sha256`、应用包的 `offline/wheels`、`offline/extensions` 和 `offline/requirements.txt`。禁止只凭文件名认为两包一定配套。若缺失正式 Python 包，不得用现有 `/opt/anaconda3/bin/python3.11` 悄悄代替，标记 `BLOCKED_RUNTIME_PACKAGE_MISSING`。
2. **只读审核归档条目**：用 `tar -tzf` 或开发机上的审核脚本识别绝对路径、`..` 穿越、链接指向目录外、特殊文件、PAX 特性和非预期安装钩子；禁止未经审核直接解压。只在本轮 `RUN_ROOT/packages/` 下提取受信任归档，不准 `tar -C /opt` 或原地覆盖 `/home/paas` 既有文件；不以 root 身份解包和安装。
3. **核对真实脚本**：仓库 `deploy/install-sles12sp5-python-runtime.sh` 已支持 `--prefix`；其默认安装位置是 `/opt/marketing-copilot/python`，并支持危险的 `--force` 替换旧运行时。本任务必须读取**实际交付包内的 `install.sh`**，证明它也支持 `--prefix`，不会创建系统文件、调用 `sudo/systemctl`，且其所有临时/目标/失败清理路径都限制在 `RUN_ROOT`。不满足则 `BLOCKED_ENV`，先在开发侧准备安全的可迁移安装方案。
4. **有条件才允许执行**：当本轮目录独占、安装目标 `$RUN_ROOT/python` **不存在**、脚本逐行审计 PASS，且所有资源/写路径门禁 PASS 时，使用**非 root**执行：

   ```bash
   # 示例：只允许在已审计通过的本轮目录执行，禁止默认 prefix/--force
   cd "$RUN_ROOT/packages/<actual-runtime-unpacked-dir>"
   bash ./install.sh --prefix "$RUN_ROOT"
   test -x "$RUN_ROOT/python/bin/python3.11"
   ```

   如果实际包不支持 `--prefix`，不能直接照此执行；不能为图省事指定 `/opt/marketing-copilot`，更不能给 `--force`。安装器如使用内部临时清理，只能清理由本轮创建且可证明属于本轮的 staging 文件；绝不对已存在文件进行替换/清理。
5. **独立验证 runtime**：由此 Python 检查 `sys.executable/sys.prefix/sys.base_prefix`、`sys.version_info == (3,11,5)`、`ssl`、`_ctypes/ctypes`、`venv`、`sqlite3`、`ldd` 解析、OpenSSL 链接及 glibc 兼容性。只读取系统动态库，严禁修改系统链接器配置；若其编译时绝对依赖 `/opt/marketing-copilot` 且无法按本轮目录迁移，应判为 `BLOCKED_RUNTIME_RELOCATION`，不可在宿主机补软链。按需要在**仅本轮子进程**设置 `LD_LIBRARY_PATH` 指向包内私有库，切忌写 `/etc/ld.so.conf`、执行 `ldconfig` 或修改 profile。
6. **只使用本轮 runtime 创建 venv**：

   ```bash
   "$RUN_ROOT/python/bin/python3.11" -m venv "$RUN_ROOT/venv"
   "$RUN_ROOT/venv/bin/python" -c 'import sys; print(sys.version); print(sys.executable); print(sys.base_prefix)'
   "$RUN_ROOT/venv/bin/python" -m pip install --no-index \
      --find-links "$RUN_ROOT/offline/wheels" \
      -r "$RUN_ROOT/offline/requirements.txt"
   ```

   上述 pip 命令仅在应用包 `offline/` 已完整、安全提取、requirements 与 runtime 对应、且写缓存等全部重定向后执行。如果 pip 试图走公网或调用系统安装脚本，立即 `SAFETY_STOP`。`venv` 不得复用任何旧目录，版本不一致不允许采用兼容性猜测。
7. **应用包启动审计**：项目 `deploy/run-system-python-manual.sh` 会读取 `.env`、可能规范化并改写环境文件、可能 `pip install`，并且若不设置 `MC_SKIP_SYSTEMD_CHECK=1` 还会调用 `systemctl`。即使应用包没有修改系统，仍优先使用直接受控的 `venv/bin/python -m uvicorn main:app --host 127.0.0.1 --port <approved-port> --workers 1` 和当前 E2E Runner；如确需调用 `run.sh`，必须先审计实际交付包的 `run.sh` 并证明无外部副作用。
8. **必须出具 runtime provenance**：`Runtime_Provenance.json` 包含 runtime tar SHA、安装脚本 SHA、Python 可执行文件 SHA、关键私有 `.so` 与 wheel 版本、配套 App 包 SHA、离线扩展路径、所用解释器、venv `sys.base_prefix`。**不允许用服务器 Anaconda 版本“正好也是 3.11.5”来代替这些证明**。

注意：源码包 `backend/requirements.txt` 与**已构建应用包**的 `offline/requirements.txt` 必须校验是否一致；本次以冻结的代码 SHA 与完整交付包关联为准。若 package 内源码 SHA 不等于本轮目标 SHA，则区分“原样部署包兼容性测试”和“最新源码性能测试”，不能混成同一组正式证据。

### 3.1b — 最新源码与旧部署包必须分开标记

- 如果现有应用 tar 包不包含 `c3d31a99` 的四项改动，不能通过安装旧包声称测试了最新代码。可以在开发机用冻结的 `c3d31a99` 构建**新应用测试包**，但必须保证离线 Python/wheel/extension 版本相容；如需构建依赖，**仅在开发环境完成**，禁止在共享 SLES 主机安装编译器/构建工具或全局依赖。
- 同一服务器可分别做旧交付包“部署兼容性”和当前 `c3d31a99` 源“开发性能对照”，但必须放在不同的 `runs/<run-id>`，使用不同 DB、日志、端口和审计 manifest，严禁混算。
- 最新代码的本轮仓库 checkout 或归档必须有已验证 source SHA；不得将 GitHub 检出引起的文件/证书/网络缓存写出界。离线复制是优先选项；若缺源包，标记 `BLOCKED_SOURCE_PACKAGE`，不自行改动生产目录取源码。

### 3.2 必须传给所有测试进程的环境

下例只展示应满足的范围约束，**不是可不经审计直接启动的最终命令**。`RUN_ROOT` 必须指向已实际创建、本轮独占且位于指定根目录内的路径。

```bash
export RUN_ROOT=/home/paas/marketing-copilot-test/runs/<run-id>
export HOME="$RUN_ROOT/home"
export TMPDIR="$RUN_ROOT/tmp"
export TMP="$RUN_ROOT/tmp"
export TEMP="$RUN_ROOT/tmp"
export XDG_CACHE_HOME="$RUN_ROOT/cache/xdg"
export XDG_CONFIG_HOME="$RUN_ROOT/env/xdg-config"
export XDG_DATA_HOME="$RUN_ROOT/state/xdg-data"
export PIP_CACHE_DIR="$RUN_ROOT/cache/pip"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1
export PYTHONPATH="$RUN_ROOT/src/backend"
export MC_PYTHON_BIN="$RUN_ROOT/python/bin/python3.11"
export MC_VENV_DIR="$RUN_ROOT/venv"
export MC_SKIP_SYSTEMD_CHECK=1
export MC_UPDATE_MODE=manual
export MC_DUCKDB_EXTENSION_DIR="$RUN_ROOT/offline/extensions"
export DUCKDB_PATH="$RUN_ROOT/db/marketing_copilot.duckdb"
export MC_DB_CONFIG_FILE="$RUN_ROOT/state/db_config.json"
export MC_STATE_DIR="$RUN_ROOT/state"
export MC_INSTANCE_LOCK="$RUN_ROOT/state/instance.lock"
export MC_LOG_PATH="$RUN_ROOT/logs/api.log"
export MC_SMS_RESULT_JOURNAL_PATH="$RUN_ROOT/journal/sms-results.jsonl"
export MC_BACKUP_DIR="$RUN_ROOT/state/backups"
export MC_UPDATE_DIR="$RUN_ROOT/state/updates"
export MC_STALL_DIAGNOSTICS_DIR="$RUN_ROOT/evidence/stall"
export MC_LOCAL_E2E_RUN_ROOT="$RUN_ROOT/data/e2e-runs"
export MC_HOST=127.0.0.1
export MC_ALERT_WEBHOOK_URL=
export MC_TASK_ARCHIVE_ENABLED=false
export MC_DETAIL_LOGGING=0
export OPENBLAS_NUM_THREADS=2
export OMP_NUM_THREADS=2
export MKL_NUM_THREADS=2
umask 077
```

- 同时审查并封闭可能写出界的自定义 `MC_*` 路径、pytest basetemp、进程监控输出、SFTP、whitelist index、轮询器、异常处理和操作系统 `core_pattern`。当前仓库 `.env.example` 中默认的一切生产路径均不得继承。
- 所有 HTTP API 和 SMSC sockets 使用临时检查确实空闲的 **127.0.0.1 高位端口**；不可假定 `19090` 空闲，不修改防火墙；启动后用 `ss` 核验实际监听地址。真实网络连通性测试不在范围内。
- 必须使用**本任务本轮 standalone Python** 创建的 `venv`，从正式应用包 `offline/wheels` 使用 `pip --no-index --find-links` 安装匹配依赖；不得以任何方式改动 Anaconda、系统 Python 或使用互联网补依赖。若 wheel/runtime 版本、ABI/glibc 不兼容，标记 `BLOCKED_ENV`，不尝试全局修复。详见 §3.1a。
- 一般情况下 `ulimit -c 0`，避免 native core dump 被系统全局 `core_pattern` 导向其他路径。保留 `PYTHONFAULTHANDLER=1` 与本轮 stderr 日志；是否允许在本轮路径中收集 core 必须先核查内核 core 行为并取得单独批准，严禁更改全局 `core_pattern`。

### 3.3 资源隔离与主动停止

初始 T1/T2 为**低影响档**（建议值，最终由业务方批准后写入冻结配置）：

| 资源 | 初始建议/上限 | 原则 |
| --- | --- | --- |
| CPU | 2 个已获批准的逻辑 CPU affinity；`nice -n 10` | `taskset` 仅约束可运行 CPU，不能保证不与其他业务抢占 |
| I/O | `ionice -c3`（可用且生效时）；小规模磁盘写入 | `ionice` 不保证 RAID/SSD 的硬 IOPS 上限 |
| RAM | 目标测试进程组 RSS < 2 GiB；优先使用**管理员已配置**的专属 cgroup 硬限制 | 没有内存硬限制时，只执行短时小规模 smoke；禁止依赖“机器有 375G 内存”无限使用 |
| 磁盘 | 本轮新增文件目标 < 3 GiB；到达预警门槛立即停止新负载 | 同时观察 `/home/paas` 所在文件系统剩余容量和 I/O 时延 |
| 测试持续时间 | 单项短时，必须明确超时、取消和进程回收 | 不能留下无界后台作业 |
| 流量 | 仅 loopback，本地模拟器，无真实号码 | 测试不得触发真实业务 |

- 开始前和运行中采集**轻量级**主机资源，采样间隔建议 5–10 秒。若出现持续明显业务压力（例如 CPU idle 连续下降到 <20%、`MemAvailable` 显著恶化、主机 IO wait >10%、授权目录所在磁盘响应异常、本轮目录文件超额、现有业务告警），**停止本次测试子进程并保留证据**；不得改变其他业务的进程或运行参数。阈值应在执行前与业务方确认，作为保护性止损而非性能 PASS 标准。
- 不可用 `timeout` 粗暴中断父进程后遗留子进程。建立明确的**本轮 PID/进程组追踪**与 SIGTERM → 有界等待 → 仅对本轮仍存活 PID 强制终止逻辑。不能误杀已有业务同名进程。

输出 `T1_Isolation_Manifest.json`、`T1_Compatibility.md`、`Runtime_Provenance.json`，明确目录、端口、进程、SHA、轮询和资源限制是否生效；任何越界写路径或非预期连接为 `SAFETY_STOP`。

## 4. T2 — 受控兼容性与小规模 E2E（默认上限；仅 T1 PASS 后）

按以下顺序执行，每项单独 run 子目录/输出，先验证前一阶段 PASS 再继续：

1. **兼容性 smoke**：交付包 standalone Python 创建的独立 venv 中导入关键模块（DuckDB、pandas、numpy、smpplib、FastAPI），记录库路径、版本、ABI、动态加载错误；只读取系统 glibc，不加载生产 DB。
2. **G0 定向单元测试**：选择不访问真实服务、不修改系统的后端测试集；`pytest --basetemp="$RUN_ROOT/tmp/pytest-<id>"`，禁用全局缓存（如 `-p no:cacheprovider`），`TMPDIR/HOME` 全部在本轮目录。先审查 fixtures、subprocess、网络/备份/杀进程行为，跳过/阻断不满足隔离条件的测试，不可将“跳过”汇报成全量 PASS。
3. **本地 SMSC 协议 smoke**：仅在两端均为 `127.0.0.1` 且端口独占时启动模拟器与独立 API；建议 10–50 条合成数据，pool=1、window=1、低 Route TPS。核对实测 bind、submit、response、Journal 和数据库状态。
4. **小规模端到端**：按获批准资源配额再进行 100 条、最多 1000 条合成 SMS 的独立测试，保留现有正确性、stop/cancel、unknown/partial、不重复提交、Journal replay 语义。**1000 只是上限，不是必须完成的目标**；若出现争用、性能告警或任何越界，立即停止。
5. **受控 A/B**（只有上述均 PASS 后）：在同一环境、冻结负载及同 SHA 下，对比可靠/快速结果路径或 serialized/OCC 的小规模运行；记录真实有效 pool/window/TPS、每阶段耗时、主机竞争状态。出现 write-write conflict、超时或 Native crash 时保留证据并停止升级负载。

如果使用现有 `backend/tools/local_e2e_acceptance.py`，必须显式指定 `--run-root`、`--output`、`--python`、`--events`、`--files`、`--expected-tasks`、`--timeout-seconds`、`--route-tps`、`--simulator-tps`、`--pool-size`、`--smpp-window`、`--task-workers`、`--ingest-workers`、`--workers`、`--max-memory-mb`、`--keep-artifacts` 等适用参数，并完成所有派生目录/子进程/网络地址代码审计。上述 CLI 参数应以当前冻结 SHA 的 `--help` 和源码为准，不得凭此列表盲调用。**禁止调用其默认输出 `Docs/local-e2e-acceptance-results.json`，必须重定向到本轮 `output/`。**

### 4.1 V3：双 Route、审计映射与开发机对照（小规模，非容量验收）

- 本轮默认先完成**单 Route**安全 smoke。只有 T1/T2 的资源和网络边界已 PASS，且本轮测试窗口明确涵盖双 Route loopback 时，才可增加 **10～50 条合成数据**的双 Route 最小测试；独立场景目录、独立 loopback 端口，不和其他场景并发。
- 分开记录：**A. 默认主备模式**（主路由成功后不要求备路由同时 bind），**B. 显式并行模式**（本轮 API 子进程设置 `MC_SMPP_PARALLEL_BIND_REQUIRED=1`，不能写入系统环境或影响其他应用）。逐一验证两个实际物理 Route 各自 bind、pool/window、route-shared TPS，不得仅以 TCP connected 判 PASS。
- 若引入失败注入，只允许作用于**本轮自己的模拟器和进程**；验证第二必需 Route 不可用时新发送准入 fail-closed，同时特别确认：第一条已经 bind 的 Route 不会在该失败条件下继续接收未获授权的新测试发送。保留已经发生 socket I/O 的 unknown/partial 保守语义，不自动整条重发。
- 必须核对每个真实 `submit_sm` 的 `campaign / route / task / claim / attempt / part / sequence` 映射与 `submit_sm_resp`，以及 Journal、DuckDB、simulator audit 的计数守恒。**`mapped_submit_sm=0`、映射缺失、timeout、stall、watchdog 或任何错路由都必须为 E2E FAIL**，即便 bind audit PASS；不要修改审计门禁以掩盖缺口。
- 当前 `mixed_campaign_capacity_acceptance.py` 在开发侧存在双 Route Windows `mapped_submit_sm=0` 和 WSL API-ready failure。先在开发机静态排查是否属于测试工具配置/映射缺陷，只有证明其有界且不越界后才可以在 SLES T2 使用。否则只运行已审计可用的本地脚本，并将双 Route E2E 标记 `NOT_RUN/BLOCKED_HARNESS`；绝不在共享主机一边修 runner 一边试错。
- 开发机 vs SLES 仅比较**同源码 SHA、相同实际 workload/route TPS、pool/window、DB 模式、发送模式、审计模式**的匹配场景；分别记录物理 CPU 亲和限制、CPU/IO 争用和 Python 3.11.13 vs 正式 SLES Python 3.11.5 差异。T2 低资源配额下的 TPS 仅说明受控负载表现，不得外推为服务器总容量。

**不得执行**：`mixed_campaign_capacity_acceptance.py --profile capacity/soak/scale10k`、G4 52K/102K、G6 600s 高负载、5500-task 或任何未经另行批准的持续压测。共享业务服务器上即使硬件充足，也不能自动升级负载。

### 审计证据（不得缺省）

- `git SHA / dependency SHA / OS / CPU affinity / actual run root / mount / Python / DuckDB`；
- 实际 API/SMSC bind 地址与 route 数、`pool/window`、route TPS vs simulator TPS；
- `tasks`、`parts`、submit/response、accepted/unknown/partial、journal entries 和 DB 结果守恒；
- 测试前、中、后的 CPU/RSS、主机负载、磁盘 I/O、进程组、写入数据规模、磁盘余量；
- `claim`、`submit_authorize`、result batch/checkpoint、journal fsync、DB write lock wait/hold 的 P50/P95/P99 与最慢阶段；
- 如果测试失败，保存 traceback、返回码、原始失败日志、WAL conflict、timeout、faulthandler 等，不做无依据根因断言；
- 标记“共享服务器资源竞争 / affinity / 低影响配额可能压低 TPS”，不能将有限额度的 T2 性能结果直接外推为正式服务器容量。

## 5. T3/T4 — 只出方案，不在本任务内运行

T2 结束后，可生成单独的 **“更高负载测试提议”**，内容包括：T3 5500-task 与 T4 G4/G6 的预计 CPU、RAM、磁盘 I/O、运行时长、业务窗口、回滚、资源硬限制、性能观察器开销和审批门禁。**不创建/启动高负载实验**；用户另行明确批准之后才能创建新任务执行。

必须先在开发侧明确：G4 `A/B/C=52K` 与四活动 `102K` 的映射歧义、180 s 与 route TPS=200 的数学下限矛盾。**双路由未 bind 的代码缺陷已经局部修复**，但双 Route E2E 映射/完整业务结果仍 FAIL，不能把旧问题表述为“完全没修复”或“全部闭环”。禁止用本次低影响测试覆盖原 `INTEGRATED_NO_GO`。

## 5.1 V3 延续的特别门禁：禁止沿用旧路径和 Anaconda

- `PATH_SCOPE`: 本任务所有源码、包、配置、日志、数据库、测试输出及临时缓存必须位于 `/home/paas/marketing-copilot-test/runs/<run-id>`（仅允许启动前由非 root 账号创建其必要父目录）；不允许任何写入 `/opt/marketing-copilot-test`、`/opt/marketing-copilot`、`/opt/anaconda3`、服务器真实业务目录。
- `PYTHON_PROVENANCE`: 运行时必须由**正式 SLES12SP5 standalone Python package** 在本轮创建，严格检查 `sys.base_prefix`；从 Anaconda 运行的结果只能作为单独“现有宿主环境只读信息”，不能记为本任务 T1/T2 PASS。
- `NO_FORCE`: 不得对安装器传 `--force`；只允许全新目标，任何既有 `python` / `venv` / `state` 检测到冲突即停止。
- `NO_SYSTEMD`: `MC_SKIP_SYSTEMD_CHECK=1`；优先不调用 `run.sh`，更不允许调用生产 `install.sh`、系统服务管理和软件包管理器。
- `NO_HOST_RESOURCE_ASSUMPTIONS`: 服务器 80 逻辑 CPU、375 GiB 内存为共享资产；不等于测试配额。不可因硬件宽裕跳过 T0/T1 审批门禁。

## 6. Codex 执行工作流程

1. 在开发机或只读 GitHub 上核对完整 SHA `c3d31a99ff0aa3e0dd0472e197c6fe182f854e1d`、最新 G4 报告与 `validation_contract.json`；严格遵守 `AGENTS.md`，但其标准“安装/部署/测试”命令**不得突破本共享主机安全合同**。
2. 首先静态审计所有写路径和网络路径（包括安装包内脚本、独立 Python 路径及 wheel 解包行为），列出可能修改 `/tmp`、HOME、源码目录、系统服务和外部网络的风险；必要时在**开发环境**改造 isolated runner，而不是直接在共享服务器试错。
3. 仅执行 T0；只读证明通过后，检查非 root 账号及 `/home/paas/marketing-copilot-test` 授权，再进行 T1。没有授权则只输出 T0 报告并退出。
4. T1 通过并具备可操作的资源止损手段后，按照 T2 的短小梯度执行，不并行叠加多个压测或多个无界 simulator。
5. 每一阶段完成后先清理**仅本次创建的运行进程**（不删除任何历史文件），执行端口及资源复核，保留工件，然后才能继续。
6. 结果只写本轮目录，不执行 commit/push，不执行生产软件安装/重启，不碰其他业务。需要报告返回开发仓库时先给出文件列表和复制建议，交由用户批准。

## 7. 最终报告与验收状态

在本轮 `output/` 内生成单一报告：`Marketing_Copilot_SLES12SP5_Shared_Server_Isolated_Validation_Report.md`，至少包含：

1. 基线版本、服务器只读画像（**记录 `/home/paas` 实际文件系统，不能沿用 `/opt`**）、授权/隔离范围、所有实际写路径与 PID/端口、权限检查；runtime 包/安装脚本/应用包 SHA、venv 解释器来源，证明未使用 Anaconda 运行正式测试。
2. T0/T1/T2 每项 `PASS / FAIL / BLOCKED_ENV / NOT_RUN / SAFETY_STOP`，明确未执行项及原因。
3. 兼容性结论（**项目交付包的独立** 交付包 Python 3.11.5、glibc 2.22、DuckDB/native wheel），不能以开发 WSL 环境代替。
4. 低影响测试的业务正确性与 TPS；并列宿主资源压力与执行限制，不作生产吞吐外推。
5. 原始失败证据目录和 SHA；禁止用重跑 PASS 擦除历史 FAIL。
6. 逐条列出双 Route `config bind` / socket submit/response / `mapped_submit_sm` / Journal/DB 对账 / fail-closed 的结果，区分 `CONFIG_PASS_E2E_FAIL` 与 `E2E_PASS`；出现 `mapped_submit_sm=0` 不得判 E2E PASS。当前 G4/G6 和整个 Marketing Copilot 的准入结论仍然是 `INTEGRATED_NO_GO`，除非未来有独立授权且正式完整合同验收通过（不在本任务范围内）。
7. T3/T4 需要的审批、资源限制和业务窗口；给出“是否推荐继续在共享服务器提高测试强度”的建议，默认不推荐未授权升级。

**最终输出二选一：**

- `SHARED_HOST_LIMITED_VALIDATION_PASS`：仅表示安全隔离、兼容性和被授权的小规模测试通过；**不代表 `INTEGRATED_READY_FOR_S5`**。
- `SHARED_HOST_VALIDATION_BLOCKED_OR_FAIL`：任何安全阻断、关键测试失败或环境准备不全。若触发 SAFETY_STOP，必须显著标明。

**执行完成时务必在终端摘要中列出：本轮唯一写入目录、实际执行用户、源代码 SHA、standalone Python runtime/package SHA、运行的测试、资源使用峰值、未执行高压测试的事实，以及服务器原有业务未被操作的审计依据。无法证明的项写“未验证”，不得宣称绝对未影响其他业务。**
