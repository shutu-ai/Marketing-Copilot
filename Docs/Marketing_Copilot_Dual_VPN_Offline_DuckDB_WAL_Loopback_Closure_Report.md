# Marketing Copilot 双 VPN 离线 DuckDB WAL / Loopback 收口报告

## 结论

当前收口状态为 `INTEGRATED_NO_GO`。本次仅证明了 EulerOS 2.0 SP15 兼容目标上的离线 WAL 只读审计和小规模本地 Loopback E2E；没有进行正式 SLES12SP5 原生验收、G4/G6、1K 容量或长稳测试，因此不能作为正式发布或生产部署放行结论。

阶段 B 的受限门禁为 `SHARED_HOST_SMALL_E2E_PASS`，服务器证据已在开发机重新校验并导入。服务器 Run 的阶段标记为 `PHASE_B_READY_FOR_VPN_SWITCH`。

## 基线与目标环境

- 代码基线：`d307b6701c616d47e6ecce682fcd36548016c2eb`；开发机原有 dirty 工作树未被 reset、clean 或覆盖。
- 实际测试系统：EulerOS `2.0 (SP15x86_64)`，标记为 `EULEROS_2_0_SP15_COMPAT`。
- 服务器不是原生 SLES12SP5；服务器 glibc `2.38`，kernel `6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`，64 vCPU，测试文件系统为 ext4。
- 正式独立 Python：CPython `3.11.5`；B 阶段在 Run 内创建 venv，并通过离线 wheelhouse 安装应用和独立 pytest 测试依赖。
- DuckDB：`1.2.0`；扩展 SHA256：`61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640`。

## A 阶段修复与交付物

原问题是 Runner 的只读 DuckDB 连接未传递应用离线扩展配置，导致 WAL 审计路径可能触发外部扩展仓库访问尝试。

本次最小修复包括：

- 新增共享 DuckDB 配置 helper，统一 `extension_directory` 和 `autoinstall_known_extensions=false` 的离线配置。
- Runner 为每个 Run 复制并校验本地 `core_functions` 扩展，在内存连接上显式安装本地绝对路径扩展，再打开既有数据库进行只读审计。
- Runner 的子进程继承本地 Run 的扩展、DB、WAL、日志、临时目录和缓存路径；没有改变生产默认值、短信授权、Journal/WAL 语义或 G4/G6 合约。
- 增加离线 WAL 反例测试和 Runner 专项单测。

主要代码文件：

- `backend/duckdb_config.py`
- `backend/database.py`
- `backend/services/db_config.py`
- `backend/services/runtime_service.py`
- `backend/tools/local_e2e_acceptance.py`
- `backend/tests/test_local_e2e_offline_duckdb.py`

交付物与 SHA256：

| 交付物 | SHA256 | 用途 |
|---|---|---|
| Closure 归档 | `7139a891d8eb4ed689d0fda2a23bf43b98f94ba5d551e2d93dbda6133779e2a9` | A→B 冻结传输包 |
| 正式应用包 | `73b4996cefc67f860d55c71cf2433986095fd6f438a9faef9ba9b66bdde90b23` | 独立应用部署包 |
| 独立 Python runtime | `65bada65662af0109b597e5b46e8687b37f622517d264659b8deebb964bc403d` | Python 3.11.5 |
| pytest 测试包（独立） | `5256b48ec5bdfe1ff4a9adece74376f7b9a3516a6875be668893475d4b28a6d` | 仅测试使用 |
| DuckDB 扩展 | `61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640` | 离线扩展 |

正式应用包不包含独立 pytest 测试包；测试包没有合入正式部署包。正式应用包也不包含测试数据库、WAL、上传数据、备份、运行日志或服务器口令。

## VPN 分段与证据交接

### A：DEV / GitHub VPN

完成源码审计、最小修复、离线 runtime/app/test 包构建和 SHA256 冻结。开发机验证包括：

- Windows 目标测试：`3 passed, 1 skipped`。
- WSL AlmaLinux-8 native ext4 上的正式 G0：`355 passed, 2 skipped`。
- 本地离线 WAL probe：PASS，WAL `27147` 字节，重放 `2000` 行。
- 本地打包 Loopback 重启闭环：10 个任务全部成功。

### B：SERVER VPN

服务器只使用非 root 用户和新建 Run：

`/home/paas/marketing-copilot-test/runs/mc-dual-vpn-offline-wal-closure-20261016-b`

服务器旧 Run 未修改，测试没有使用 systemd、生产安装、root、真实 SMSC、真实业务 API、GitHub、PyPI 或 DuckDB 外部扩展仓库。服务器端 `server-evidence.sha256` 全部通过，包括 Closure 归档本身。

### C：DEV / GitHub VPN

本次已从服务器导入并重新计算本地 SHA256。证据目录为：

`test-artifacts/dual-vpn-offline-wal-closure-20261016/server-evidence/mc-dual-vpn-offline-wal-closure-20261016-b/`

其中包含 `server-result.json`、`server-closure.md`、`server-evidence.sha256`、T0、环境与 pip freeze、WAL 结果、G0 日志、Loopback 结果及资源快照。应用包和 runtime 包没有从服务器重复复制。

## B 阶段结果

### B2.2：离线 WAL 只读审计

- 状态：`PASS`。
- DuckDB `1.2.0`，`autoinstall_known_extensions=false`。
- 网络模式记录为 `offline_config_only`。
- WAL 大小：`27147` 字节。
- 重放行数：`2000`；样本值：`replayed1999`。
- 生成了版本化本地元数据：`v1.2.0/linux_amd64_gcc4/core_functions.duckdb_extension.info`。

### B2.3：小规模 G0 / Runner 专项单测

- `4 passed in 0.16s`。

### B2.4：10 任务本地 Loopback 重启闭环

- 场景：`restart`；1 个文件，10 个任务。
- API 和 SMPP simulator 均绑定 `127.0.0.1`，动态端口为本次 Run 专用端口。
- `tasks_accepted=10`，`socket_submit_sm=10`，`smsc_submit_sm=10`，成功响应 `10`。
- rejected、failed、unknown、retrying、duplicate、lost mapping、misdelivered 均为 0 或空集合。
- 文件状态为 `completed`，处理行数 `10`，最终 backlog `0`。
- Runner 自身创建的 API/SMSC 进程已退出；没有停止或杀死其他服务器进程。
- 预运行负载约 `10.84`，结束时约 `15.74`，服务器为 64 vCPU；可用内存仍约 242 GB，未触发资源中止条件。

## OLD_FAIL → NEW_RESULT

| 项目 | 旧结论 / 风险 | 新结果 |
|---|---|---|
| Runner 只读连接 | `duckdb.connect(path, read_only=True)` 未携带离线扩展配置，WAL 审计可能尝试外部仓库 | 使用统一配置、Run 内本地扩展和显式本地安装；B2.2 离线 WAL PASS |
| 扩展元数据 | 版本化 `.info` 生成位置未被正确识别 | 自动识别 `v1.2.0/.../*.duckdb_extension.info`，并纳入验证 |
| Loopback E2E | 受 WAL 失败阻断，不能证明任务/submit_sm 对账 | 10/10 任务、10/10 submit_sm、10/10 成功响应，对账无重复/丢失/未知/误投递 |
| 测试包边界 | pytest 依赖容易与正式包混合 | pytest 包独立传输和安装，正式应用包中不存在测试包标记 |

上述新结果是 EulerOS 兼容环境上的有界验证，不等价于 SLES12SP5 原生性能，也不等价于正式 G4/G6 容量验收；不能把 Loopback 的吞吐数字外推为生产 TPS。

## 隔离与未覆盖项

- 已验证的隔离：应用、runtime、venv、DuckDB、WAL、日志、临时目录和缓存均位于新 Run；API/SMSC 配置为 loopback；离线 pip 使用 `--no-index`；DuckDB 自动扩展安装关闭。
- 本轮没有执行系统级抓包或出站防火墙审计，因此不把“未观察到外部访问”升级为完整网络隔离证明；这是证据边界，不通过忽略检查绕过。
- 未执行 1K、G4、G6、长稳、正式 SLES 原生验收、真实 SMSC、真实业务 API 或生产部署。
- 本报告不改变共享服务器现有业务、系统服务、代理、VPN、网络路由或系统 Python。

## 收口判定

`SHARED_HOST_SMALL_E2E_PASS` 仅表示本次受限 EulerOS 兼容目标上的小规模闭环通过。

整体保持：`INTEGRATED_NO_GO`。

原因：没有正式 G4/G6/1K/长稳容量证据，实际服务器也不是原生 SLES12SP5。若要进入后续正式容量验收，应另行建立授权、资源预算和测试窗口，不得把本次 Loopback 结果直接替代。

## 提交状态

本阶段只新增本报告并导入脱敏服务器证据；未执行 commit、push、VPN/代理修改或无关文件清理。既有 dirty 工作树内容保持原样。
