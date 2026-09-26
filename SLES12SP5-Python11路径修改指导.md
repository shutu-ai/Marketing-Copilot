# SLES 12 SP5 Python 3.11 路径修改指导

适用于使用本地 Python 3.11 的手工部署包。

## 1. `run.sh` 指定 Python 路径

首次部署或需要重建虚拟环境时执行：

```bash
export PYTHON_BIN=/opt/python311/bin/python3.11
./run.sh
```

如果 Python 依赖非系统动态库，同时指定：

```bash
export LD_LIBRARY_PATH=/opt/python311/lib:/opt/openssl/lib
export PYTHON_BIN=/opt/python311/bin/python3.11
./run.sh
```

需要固化到部署包时，修改源码文件：

```text
deploy/run-system-python-manual.sh
```

修改其中的 `PYTHON_BIN` 默认值，然后重新打包。通常不需要修改应用 Python 代码。

## 2. 已有 `venv` 的处理

`run.sh` 创建虚拟环境后，实际启动使用的是：

```text
<应用目录>/venv/bin/python
```

检查它是否仍指向有效的 Python：

```bash
ls -l /opt/marketing-copilot/venv/bin/python
/opt/marketing-copilot/venv/bin/python -c 'import ssl; print(ssl.OPENSSL_VERSION)'
```

如果 Python 被移动导致 venv 失效，先停止服务并备份旧环境，再用新路径重建：

```bash
systemctl stop marketing-copilot.service
mv venv venv.backup
PYTHON_BIN=/opt/python311/bin/python3.11 ./run.sh
```

## 3. systemd 服务

systemd 的 `ExecStart` 通常直接调用 `venv/bin/python`，不需要在 unit 中修改 `PYTHON_BIN`。

如果服务报 `libpython`、`libssl` 等动态库找不到，在实际 `EnvironmentFile` 中加入绝对路径：

```ini
LD_LIBRARY_PATH=/opt/python311/lib:/opt/openssl/lib
```

查看环境文件位置：

```bash
systemctl cat marketing-copilot.service
```

修改后执行：

```bash
systemctl daemon-reload
systemctl restart marketing-copilot.service
```

也可以将 Python 库目录加入系统动态链接器配置并执行 `ldconfig`，这样不必单独修改 unit。

## 4. 不需要修改的配置

- 正常安装不需要设置 `PYTHONPATH`。
- 不建议设置 `PYTHONHOME`，它可能破坏虚拟环境。
- 如果 `ldd /opt/python311/bin/python3.11` 没有显示 `not found`，通常不需要设置 `LD_LIBRARY_PATH`。
