# Marketing Copilot：SLES 12 SP5 离线部署指导

本文档用于部署 `Marketing Copilot` 至完全离线的 SUSE Linux Enterprise
Server 12 SP5（x86_64）服务器。本文档是交付说明，**不会被打进离线安装包**。

## 1. 交付物与目标机要求

交付文件：

```text
marketing-copilot-sles12sp5-20260726-full.tar.gz
SHA-256: A1E60EEB7B3C3E832EA613FD861110B1AAE07E8F13C56ED203C57BDF48AFEE3F
```

该包包含：

- 预编译的 CPython 3.11.5；
- 私有 OpenSSL 1.1.1w 运行库；
- 已构建的前端静态文件；
- 应用全部 Python 离线 wheel；
- systemd 服务文件、安装脚本与文件 SHA-256 清单；
- 独立的可选 SMSC 模拟器服务。

目标机不需要，也不应安装 Docker/Podman、Node.js、npm、Python 编译器或
Python 开发包。需要具备 root/sudo 权限、systemd、`bash`、`tar`、`sha256sum`
和足够磁盘空间（建议至少 1 GB 可用空间）。

> 该包为 `x86_64` 架构构建；不可用于 ARM 或 IBM Z 等其他 CPU 架构。

## 2. 传输与完整性校验

通过受控介质将 tar.gz 复制到目标服务器，例如 `/opt/install-media`：

```bash
mkdir -p /opt/install-media
cd /opt/install-media
sha256sum marketing-copilot-sles12sp5-20260726-full.tar.gz
```

输出必须与本文档第 1 节给出的 SHA-256 一致。校验失败时，不要安装，应重新
传输文件。

## 3. 安装

```bash
cd /opt/install-media
tar -xzf marketing-copilot-sles12sp5-20260726-full.tar.gz
cd marketing-copilot-sles12sp5-20260726-full
sudo bash install.sh
```

安装脚本会在安装前验证包内每一个文件的 SHA-256，然后：

1. 创建系统账户 `marketing-copilot`（若不存在）；
2. 将应用安装到 `/opt/marketing-copilot`；
3. 安装预编译 Python 3.11.5 与 OpenSSL；
4. 在 `/opt/marketing-copilot/venv` 创建虚拟环境，并从本地 wheel 安装依赖；
5. 创建运行数据目录和 systemd 服务定义。

安装脚本不访问网络。

## 4. 配置

安装后先编辑环境文件，再启动服务：

```bash
sudo vi /etc/marketing-copilot.env
```

默认值可用于本机验证。重点配置项如下：

| 配置项 | 作用 |
| --- | --- |
| `DUCKDB_PATH` | DuckDB 数据库文件路径。 |
| `MC_FRONTEND_DIR` | 前端静态文件路径；默认无需修改。 |
| `MC_TASK_WORKERS` | 内部短信任务工作线程数。 |
| `MC_ALERT_WEBHOOK_URL` | 可选告警 Webhook 地址；离线环境通常保持为空。 |
| `MC_SYSTEM_MODE` | 系统运行模式，生产环境保持 `production`。 |

服务默认监听 `0.0.0.0:8000`。如需限制网络访问，请在主机防火墙或反向代理中
按实际安全策略配置；不要将管理服务直接暴露到不受信任网络。

## 5. 启动与验证

```bash
sudo systemctl start marketing-copilot.service
sudo systemctl status marketing-copilot.service --no-pager

curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
curl --fail http://127.0.0.1:8000/metrics
```

还可执行部署自检：

```bash
sudo -u marketing-copilot bash -c '
  set -a
  source /etc/marketing-copilot.env
  cd /opt/marketing-copilot/backend
  /opt/marketing-copilot/venv/bin/python -m tools.deployment_check --require-linux
'
```

成功后设置开机启动：

```bash
sudo systemctl enable marketing-copilot.service
sudo systemctl enable marketing-copilot-backup.timer
```

日志位置：

```bash
sudo journalctl -u marketing-copilot.service -f
tail -f /var/log/marketing-copilot/app.log
```

## 6. 可选：本地 SMSC 模拟器

SMSC 模拟器仅用于联调和测试，默认不安装、不启动。它独立于应用 wheel 依赖，
只使用已交付 Python 的标准库。

安装时加入参数：

```bash
sudo bash install.sh --with-smsc-simulator
sudo systemctl enable --now marketing-copilot-smsc-simulator.service
```

默认监听 `127.0.0.1:2775`，测试账号为 `test/test`。启动后可查看状态：

```bash
sudo systemctl status marketing-copilot-smsc-simulator.service --no-pager
```

生产发送请配置真实 SMSC，且不要在生产环境暴露模拟器端口。

## 7. 停止、备份与升级

停止服务：

```bash
sudo systemctl stop marketing-copilot.service
```

手动执行数据库备份：

```bash
sudo -u marketing-copilot /opt/marketing-copilot/venv/bin/python -m tools.backup_database
```

备份默认存放在 `/var/lib/marketing-copilot/backups`。升级前应先执行一次备份，
保留原包与对应 SHA-256，并在维护窗口内停止服务后进行升级。

## 8. 常见问题

### `sha256sum -c checksums.sha256` 失败

说明安装目录中的文件不完整或被改动。删除解压目录后，从原始 tar.gz 重新解压；
若仍失败，重新传输交付文件并先核对其外层 SHA-256。

### 服务无法启动

依次检查：

```bash
sudo systemctl status marketing-copilot.service --no-pager
sudo journalctl -u marketing-copilot.service -n 100 --no-pager
ls -ld /var/lib/marketing-copilot /var/log/marketing-copilot /run/marketing-copilot
```

运行目录应允许 `marketing-copilot` 账户写入。不要手工替换
`/opt/marketing-copilot/python` 中的文件，否则可能破坏私有 OpenSSL 链接。

### 浏览器无法访问页面

先在服务器本机检查 `curl http://127.0.0.1:8000/api/ready`。若本机正常，检查
SLES 防火墙、路由、安全组和访问主机地址/端口；应用默认端口为 8000。

### 目标机不能联网

这是预期情况。安装与运行不需要联网；请勿在目标机执行 `pip install`、`npm install`
或系统在线更新命令。
