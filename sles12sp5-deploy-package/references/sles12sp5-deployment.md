# SLES 12 SP5 部署参考

## 目标机约束

- 目标系统：SLES 12 SP5 x86_64。
- 目标机 glibc：2.22。
- 手动运行包使用的 Python：目标机系统 Python 3.11.5，或单独随包提供的 Python 运行时。
- 手动部署包不能在目标机依赖 Node.js、npm、编译器、Docker、Podman 或网络。
- 依赖包必须使用 `manylinux2014_x86_64` 的 CPython 3.11 轮子，ABI 为 `cp311`。

## 手动部署包结构

```text
sles12sp5-system-python-<version>/
├── backend/
│   ├── ...                    # 后端源码，不包含本地运行产物
│   └── release.json           # 版本元数据（在线更新预检必需）
├── frontend/dist/             # 预先构建的前端资源
├── offline/
│   ├── requirements.txt
│   └── wheels/                # 离线 manylinux2014 wheelhouse
├── release.json               # 顶层副本（与 backend/release.json 相同）
├── app.env.example
├── run.sh                     # 前台启动脚本
├── README.md
└── checksums.sha256
```

`release.json` 由打包脚本自动生成（包含 `version`、`build_time`、`builder`、`git_sha`），无需手工维护。

## 与前端在线更新的关系

- 同一份 tar.gz 既是部署包，也是「系统设置 → 系统更新」页面的上传物，无需专用更新包。
- 上传后系统会校验：外层 SHA-256（可选）→ 安全解包（拒绝绝对路径/`..`/符号链接）→ `checksums.sha256 -c` → 必需文件（`backend/main.py`、`backend/requirements.txt`、`frontend/dist/index.html`、`backend/release.json`）→ Python 语法编译 → 版本比较。
- 应用流程：备份数据库 → 排空任务 → 停止服务 → 切换 `backend`/`frontend`/`offline` →（依赖变更时离线安装 wheel）→ 启动 → 健康检查 → 失败自动回滚。更新不会覆盖数据库、`db_config.json`、`uploads`、`monitor_dir`、`data`、日志等运行期条目。
- 没有 `release.json` 的旧包会被预检拒绝，必须用当前构建脚本重新打包。

## 依赖包准备

在 Windows 上通过 WSL，或在有网络的 Linux 构建机上使用 Python 3.11：

```powershell
scripts/prepare_wheelhouse_wsl.ps1 `
  -Requirements .\backend\requirements.txt `
  -Output .\cache\wheelhouse
```

依赖包构建会下载 SLES 12 SP5 兼容基线所需的二进制轮子，并把 `smpplib` 打成纯 Python wheel。生成的归档不要提交进 Git，应在 `.gitignore` 中忽略 `cache/wheelhouse/`（`release/` 仅作兜底）。

WSL 环境检测、自动准备和兼容性说明见 `wsl-environment.md`。

## 前端构建

只有 `frontend/dist` 会进入部署包，打包前先构建：

```bash
cd frontend
npm ci
npm run build
```

## 必须排除的本地运行产物

不要把本地配置、日志、数据库或上传文件打进部署包：

```text
__pycache__/
*.pyc
db_config.json
logs/
server*.log
*.db*
*.duckdb*
uploads/
monitor_dir/
```

## 校验清单

- `sha256sum -c checksums.sha256` 能通过。
- 包内包含 `backend/`、`backend/release.json`、`frontend/dist/index.html`、`offline/wheels`、`run.sh` 和 `README.md`。
- 包内不包含 `db_config.json`、`logs/`、`*.db*` 或 `*.duckdb*`。
- `run.sh` 具有可执行权限。
- `run.sh` 在读取 env 或交给 systemd 前，将 `$APP_HOME`/`${APP_HOME}` 写成绝对路径，并仅首次创建 `.paths.bak`。
- 启动脚本缺少依赖时能用包内 wheel 通过 `--no-index` 创建 `venv`。
- 应用默认监听 8000 端口，并暴露 `/api/ready` 和 `/api/health`。

## 常见问题

- 缺少 wheelhouse：先运行依赖包准备脚本，把归档放入项目 `cache/wheelhouse/`，再运行部署包构建脚本。
- WSL 缺少 Python 3.11：运行 `scripts/ensure_wsl_environment.ps1 -AllowInstall`，或在已有发行版内运行 `scripts/ensure_wsl_environment.sh --install`。
- 缺少前端 dist：打包前先执行 `npm run build`。
- 包内出现本地运行产物：按上面的排除清单修正打包脚本并重新打包。
- 目标机尝试源码构建 `smpplib`：必须提前在依赖包准备阶段生成 `smpplib` wheel。
- 在线更新预检报 `backend/release.json is missing`：当前包由旧版构建脚本生成，用项目 `deploy/` 下的当前构建脚本重新打包。
- DuckDB 报 `Cannot open file ".../backend/$APP_HOME/data/..."`：systemd 不展开 `EnvironmentFile` 中的 shell 变量。运行新版 `run.sh` 标准化 env，确认其中不再包含 `$APP_HOME` 或 `${APP_HOME}`，然后重启服务；不要把重复的 FastAPI `merged_lifespan` 堆栈误判为根因。
