# SLES 12 SP5：系统 Python 手动离线部署指导

本文档对应以下交付包：

```text
marketing-copilot-sles12sp5-system-python-20260726-system-python.tar.gz
SHA-256: 36054DF937262F81AB4B74A1B43A01D447846F59A094BCFF279C237FD6EAEC40
```


## 1. 前置条件

- 操作系统：SUSE Linux Enterprise Server 12 SP5 x86_64。
- 已安装并可在 `PATH` 中执行 `python3.11`。
- Python 必须精确为 3.11.5，且具备 `ssl` 和 `venv` 模块。
- 有一个可写的部署目录；本文示例使用 `/opt/marketing-copilot-manual`。
- 安装和运行过程不需要网络。

先验证目标机 Python：

```bash
python3.11 - <<'PY'
import ssl
import sys

assert sys.version_info[:3] == (3, 11, 5), sys.version
assert ssl.OPENSSL_VERSION_INFO >= (1, 1, 1), ssl.OPENSSL_VERSION
print(sys.version)
print(ssl.OPENSSL_VERSION)
PY

python3.11 -m venv --help >/dev/null
```

任何命令失败时，停止部署并修复目标机 Python；不要改用其他 Python 小版本。

## 2. 传输与外层完整性校验

将 tar.gz 通过受控介质复制到目标机，例如 `/opt/install-media`：

```bash
cd /opt/install-media
sha256sum marketing-copilot-sles12sp5-system-python-20260726-system-python.tar.gz
```

输出必须与本文档开头的 SHA-256 一致。不一致时重新传输，不能继续安装。

## 3. 解压并验证包内文件

```bash
cd /opt/install-media
tar -xzf marketing-copilot-sles12sp5-system-python-20260726-system-python.tar.gz
cd marketing-copilot-sles12sp5-system-python-20260726-system-python
sha256sum -c checksums.sha256
```

最后一条命令必须全部显示 `OK`。失败时删除此次解压目录并重新解压、重新校验。

## 4. 复制到运行目录

以下命令以 root 或拥有 `/opt` 写权限的账户执行：

```bash
mkdir -p /opt/marketing-copilot-manual
cp -a backend frontend offline marketing-copilot.env.example run.sh README.md \
  /opt/marketing-copilot-manual/
cd /opt/marketing-copilot-manual
cp marketing-copilot.env.example marketing-copilot.env
chmod 0755 run.sh
```

若使用非 root 运行账户，应将运行目录和数据目录授权给该账户。例如运行账户为
`mcapp` 时：

```bash
chown -R mcapp:mcapp /opt/marketing-copilot-manual
mkdir -p /var/lib/marketing-copilot/backups /var/log/marketing-copilot /run/marketing-copilot
chown -R mcapp:mcapp /var/lib/marketing-copilot /var/log/marketing-copilot /run/marketing-copilot
```

`/run` 会在重启后清空；手动运行前如不存在，重新创建并授权该目录。

## 5. 配置运行参数

编辑 `/opt/marketing-copilot-manual/marketing-copilot.env`：

```bash
vi /opt/marketing-copilot-manual/marketing-copilot.env
```

至少确认以下值指向运行账户可写的目录：

```dotenv
DUCKDB_PATH=/var/lib/marketing-copilot/marketing_copilot.duckdb
MC_LOG_PATH=/var/log/marketing-copilot/app.log
MC_INSTANCE_LOCK=/run/marketing-copilot/instance.lock
MC_BACKUP_DIR=/var/lib/marketing-copilot/backups
MC_FRONTEND_DIR=/opt/marketing-copilot-manual/frontend/dist
```

按实际情况配置短信网关、数据库或告警地址。离线环境不需要也不应配置 pip/npm
镜像或在线下载地址。

## 6. 离线创建虚拟环境并安装依赖

以实际运行账户执行以下命令：

```bash
cd /opt/marketing-copilot-manual
python3.11 -m venv venv
venv/bin/pip install --no-index --find-links offline/wheels -r offline/requirements.txt
```

`--no-index` 不允许 pip 访问网络；不要删除该参数。成功后可检查关键依赖：

```bash
venv/bin/python -c 'import duckdb, fastapi, pandas, smpplib; print("offline dependencies OK")'
```

## 7. 手动启动、验证和停止

前台启动：

```bash
cd /opt/marketing-copilot-manual
./run.sh
```

默认监听 `0.0.0.0:8000`。在另一终端验证：

```bash
curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
curl --fail http://127.0.0.1:8000/metrics
```

停止服务时，在运行 `./run.sh` 的终端按 `Ctrl+C`。本包不创建 systemd 单元，也不
提供自动重启或开机启动；如需后台守护，应按组织的运维规范另行配置。

### 后台启动（手工方式）

如需关闭当前终端后继续运行，可使用 `nohup`。先创建日志目录，再启动并记录进程
PID：

```bash
cd /opt/marketing-copilot-manual
mkdir -p logs
nohup ./run.sh > logs/console.log 2>&1 < /dev/null &
echo $! > marketing-copilot.pid
```

检查进程、查看控制台日志和验证服务：

```bash
ps -fp "$(cat marketing-copilot.pid)"
tail -f logs/console.log
curl --fail http://127.0.0.1:8000/api/ready
```

停止后台进程：

```bash
kill "$(cat marketing-copilot.pid)"
rm -f marketing-copilot.pid
```

`nohup` 仅用于手工后台运行，不提供自动重启或开机启动；服务器重启后需要重新执行
上述启动命令。

## 8. 常见故障

### `python3.11` 版本或 SSL 检查失败

目标系统 Python 不符合交付包前提。使用包含私有 Python 的完整离线包，或由系统
管理员安装符合要求的 Python 3.11.5。

### pip 尝试联网或找不到包

确认在 `/opt/marketing-copilot-manual` 目录执行命令，并保留：

```bash
--no-index --find-links offline/wheels
```

### `/api/ready` 返回失败

检查环境文件中的数据库、日志、锁文件和备份目录是否存在且对运行账户可写；再查看
启动终端输出中的错误信息。

### 外部浏览器无法访问

若本机 `curl http://127.0.0.1:8000/api/ready` 正常，检查 SLES 防火墙、网络路由和
安全组是否允许访问 TCP 8000。生产环境应按安全规范使用反向代理或访问控制。
