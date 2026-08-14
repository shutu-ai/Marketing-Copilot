# 手动离线部署

本包使用目标机已有的 Python 3.11 在前台运行。

## 校验并解包

```bash
sha256sum -c checksums.sha256
mkdir -p /opt/application-manual
cp -a backend frontend offline app.env.example run.sh README.md /opt/application-manual/
cd /opt/application-manual
cp app.env.example app.env
```

## 启动

```bash
./run.sh
```

首次运行时，`run.sh` 会创建 `venv`，并使用 `--no-index` 从 `offline/wheels` 安装依赖。它还会把 `app.env` 中的 `$APP_HOME`/`${APP_HOME}` 路径写成当前安装目录的绝对路径，并在首次修改时保存 `app.env.paths.bak`，避免 systemd 将变量当作普通文本传给应用。
