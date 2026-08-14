---
name: sles12sp5-deploy-package
description: 生成、更新、验证和部署 SLES 12 SP5 离线部署包。包含 manylinux2014 cp311 wheelhouse、后端源码、前端 dist、版本元数据、systemd 直接启动配置，以及目标机首次安装、旧 unit 迁移、服务健康检查和在线更新故障排查。用户要求生成、更新、验证或部署 SLES12SP5 包、离线 wheelhouse、手动部署包或在线更新包时使用。
---

# SLES12SP5 部署包工作流

## 重要原则

- 优先使用项目 `deploy/` 中现有的 `build-sles12sp5-system-python-manual.sh`；不要重复生成或覆盖项目打包脚本。
- 打包前必须在 `frontend/` 执行 `npm run build`，再从仓库根目录执行项目打包脚本。
- 使用 `cache/wheelhouse/` 中的离线 wheelhouse；不存在或不适用时，按下方 WSL 流程重新生成。
- 发布包必须包含 `backend/release.json` 和 `checksums.sha256`。
- `release/` 只保存本地产物，不提交 Git。
- 不把数据库、日志、上传文件、监控目录、运行锁或虚拟环境打入发布包。

## 生成发布包

1. 检查仓库状态、最新提交、`backend/`、`frontend/`、`deploy/` 和 `cache/wheelhouse/`。
2. 在前端目录构建生产文件：

   ```powershell
   cd frontend
   npm run build
   cd ..
   ```

3. 从仓库根目录执行项目脚本：

   ```bash
   bash deploy/build-sles12sp5-system-python-manual.sh --output release
   ```

4. 校验归档：

   ```bash
   tar -tzf release/marketing-copilot-sles12sp5-system-python-*.tar.gz | grep -E \
     'backend/services/|backend/release.json|frontend/dist/index.html|offline/wheels/|run.sh|checksums.sha256'
   ```

5. 检查归档不包含：

   ```text
   backend/db_config.json
   backend/logs/
   backend/server*.log
   backend/*.db*
   backend/*.duckdb*
   backend/uploads/
   backend/monitor_dir/
   backend/data/
   backend/venv/
   ```

报告最终归档的绝对路径、大小、`release.json` 版本和 Git SHA。前端构建警告可以报告，但只有退出码为 0 才算成功。

## WSL 和离线 wheelhouse

SLES 12 SP5 不使用不存在的官方在线 WSL 镜像。优先使用本机已有且带 Python 3.11 的发行版；不满足时执行：

```powershell
.\scripts\ensure_wsl_environment.ps1 -AllowInstall -WslDistro AlmaLinux-8
```

wheelhouse 缺失或需要刷新时，在 Windows PowerShell 中执行：

```powershell
.\scripts\prepare_wheelhouse_wsl.ps1 `
  -Requirements D:\projects\app\backend\requirements.txt `
  -Output D:\projects\app\cache\wheelhouse `
  -Version 20260814
```

项目实际路径变化时替换参数，不要把临时 wheelhouse 放入 `release/`。

## 首次部署到目标机

以下示例使用：

```text
APP_ROOT=/opt/sms_center/marketing-copilot
SERVICE=marketing-copilot.service
```

1. 将 tar.gz 解压到临时目录，确认归档根目录后复制 `backend frontend offline run.sh marketing-copilot.env.example README.md` 到 `APP_ROOT`。保留目标机现有 `data/`、`logs/`、`backups/`、`updates/` 和 `marketing-copilot.env`，不要覆盖运行数据。
2. 创建环境文件：

   ```bash
   cd "$APP_ROOT"
   cp marketing-copilot.env.example marketing-copilot.env
   chmod +x run.sh
   ```

3. 配置 `MC_PORT`、数据库路径和其他业务参数。新版 `run.sh` 必须在读取 env、启动已有 unit 或创建新 unit 之前，把 `$APP_HOME` 和 `${APP_HOME}` 展开为绝对路径，并首次保存 `marketing-copilot.env.paths.bak`。不要仅依赖安装器处理：已有 unit 会跳过安装器，而 systemd `EnvironmentFile` 不执行 shell 变量展开。
4. 如果目标机曾安装过旧版本，先移除旧 unit，避免旧的 `ExecStart=.../run.sh` 与新 unit 形成重启循环：

   ```bash
   systemctl stop "$SERVICE" 2>/dev/null || true
   systemctl disable "$SERVICE" 2>/dev/null || true
   rm -f "/etc/systemd/system/$SERVICE"
   systemctl daemon-reload
   systemctl reset-failed "$SERVICE" 2>/dev/null || true
   ```

   只删除 unit 文件，不删除 `APP_ROOT` 下的数据库、日志、上传目录或环境文件。
5. 以 root 在应用根目录执行：

   ```bash
   cd "$APP_ROOT"
   ./run.sh
   ```

   新版 `run.sh` 会创建并启动 systemd unit；成功后自身退出。创建或启动失败时才回退为前台 Uvicorn。systemd unit 应直接启动 Uvicorn，而不是把 `run.sh` 作为 `ExecStart`。

## 旧版本升级和 unit 迁移

优先使用完整包部署，避免只复制部分后端文件。升级前：

```bash
systemctl stop marketing-copilot
cp marketing-copilot.env marketing-copilot.env.before-update
```

如果 unit 仍然是旧配置，确认并重建：

```bash
systemctl cat marketing-copilot
```

若看到：

```ini
ExecStart=/opt/.../run.sh
```

从新包的 `backend` 目录执行：

```bash
cd "$APP_ROOT/backend"
"$APP_ROOT/venv/bin/python" -m tools.install_systemd_service \
  --service-name marketing-copilot.service \
  --env-file "$APP_ROOT/marketing-copilot.env" \
  --user root \
  --update-mode systemd \
  --force --no-start --no-enable
systemctl daemon-reload
systemctl start marketing-copilot
```

`ExecStart` 预期为：

```ini
ExecStart=/opt/sms_center/marketing-copilot/venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 19090 --workers 1
Environment=MC_RUN_UNDER_SYSTEMD=1
```

不要在 `backend` 目录使用相对路径 `venv/bin/python`；虚拟环境在 `APP_ROOT/venv/`。

## 部署后验证

```bash
systemctl status marketing-copilot --no-pager
systemctl is-enabled marketing-copilot || true
systemctl cat marketing-copilot
ss -ltnp | grep 19090
curl --fail http://127.0.0.1:19090/api/ready
curl --fail http://127.0.0.1:19090/api/health
```

确认以下内容：

- `Active: active (running)`，不是 `activating (auto-restart)`。
- `ExecStart` 直接调用虚拟环境 Python 的 Uvicorn。
- `marketing-copilot.env` 中为绝对路径，或已由安装器展开 `$APP_HOME`。
- `MC_UPDATE_MODE=systemd`，在线更新页面显示 systemd 托管。
- `release.json` 的版本为新版本。
- 19090 端口与 `MC_PORT` 一致。

启动失败时立即查看：

```bash
journalctl -u marketing-copilot -n 100 --no-pager
```

常见错误：

- `status=3`：先查 journal，通常是环境文件中的 `$APP_HOME` 未展开、端口冲突或 Python 模块错误。
- DuckDB 报错路径包含字面量 `backend/$APP_HOME/data/...`：说明 systemd 直接读取了未标准化的 env。先运行新版 `run.sh`，再确认 `grep -n '\$APP_HOME\|${APP_HOME}' "$APP_ROOT/marketing-copilot.env"` 无输出并重启服务。若不能运行脚本，临时将相关值手工改为绝对路径。
- `ExecStart=.../run.sh` 循环重启：旧 unit 未被覆盖；停止服务、删除旧 unit 或使用安装器 `--force` 重建。
- 19090 未监听但 unit 显示 active：等待后查看 journal 和 `ss`，不要只依赖 systemd 的 active 状态。

## 在线更新

在线更新包必须来自当前构建脚本并包含 `backend/release.json`。更新器在 systemd 模式下停止和启动 unit，在手动模式下启动 `run.sh`；health check 默认使用 `MC_PORT`，也可以显式配置：

```bash
MC_PROBE_URL=http://127.0.0.1:19090/api/ready
```

更新失败时依次查看：

```bash
cat "$APP_ROOT/updates/apply.state"
ls -lt "$APP_ROOT/updates/logs/"
tail -n 200 "$APP_ROOT/updates/logs/runner-"*.log
journalctl -u marketing-copilot -n 100 --no-pager
```

如果页面显示“当前版本仍是旧版本”且结果为 `health check failed`，先确认失败前后的服务端口、`MC_PROBE_URL`/`MC_PORT`、systemd unit 的 `ExecStart` 和更新 runner 日志。更新器失败会自动回滚；确认旧版本恢复后再修正配置并重新应用，不要反复强制更新。

## 交付报告

报告：

- 发布包绝对路径和版本。
- Git SHA。
- 前端构建结果及非致命警告。
- 归档关键文件和排除项校验结果。
- 目标机部署时还需执行的命令或已知限制。

不要提交 `release/` 归档，也不要删除用户的运行数据或未授权的备份、抓包文件。
