# PostgreSQL 15 SLES12SP5 离线服务器包

这个包面向 SLES 12 SP5 x86_64，内含 PostgreSQL 15.19 的官方 PGDG RPM：

- `postgresql15`：客户端工具和基础文件；
- `postgresql15-libs`：客户端共享库；
- `postgresql15-server`：数据库服务器、systemd unit 和 `initdb`；
- `postgresql15-contrib`：常用扩展，例如 `pg_stat_statements`；
- PGDG RPM 签名公钥、`manifest.json` 和 `checksums.sha256`。

RPM 来自 PostgreSQL 官方 SLES 12 仓库。包不包含 SLES 基础系统库、运行数据、备份、账号密码或远程访问配置；安装时不使用 `--nodeps`，缺少系统依赖会直接失败。

## 安装

在 SLES 12 SP5 目标机上以 root 执行：

```bash
tar -xzf postgresql15-sles12sp5-offline-*.tar.gz
cd postgresql15-sles12sp5-offline-*
./install.sh
```

安装器会先校验整个包和 RPM 签名，然后离线安装 RPM、创建 PostgreSQL 15 数据目录并启用 `postgresql-15.service`。默认不启动服务；确认配置后启动：

```bash
systemctl start postgresql-15.service
systemctl status postgresql-15.service --no-pager
sudo -iu postgres /usr/pgsql-15/bin/psql -d postgres -c 'select version();'
```

也可以在安装完成后直接启动：

```bash
./install.sh --start
```

安装器只会在 `/var/lib/pgsql/15/data` 不存在或为空时执行 `initdb`。已有数据目录不是 PostgreSQL 15、或目录非空但未初始化时，安装会停止，不会删除或覆盖文件。

## 初始账号和项目使用

没有默认数据库密码。默认配置只监听本机，适合应用与数据库同机部署；不要为了省事把密码写进脚本或命令历史。

首次配置时：

```bash
sudo -iu postgres /usr/pgsql-15/bin/psql
```

在 psql 中创建项目账号和数据库，并用交互式命令设置密码：

```sql
CREATE ROLE marketing_copilot LOGIN;
\password marketing_copilot
CREATE DATABASE marketing_copilot OWNER marketing_copilot;
\q
```

未来 MarketingCopilot 切换 PostgreSQL 时，另行配置应用的 PostgreSQL 驱动、连接串、迁移和备份恢复流程；本包不改现有 DuckDB 数据，也不执行业务数据迁移。

如果确需远程连接，需明确修改 `postgresql.conf` 的 `listen_addresses`、`pg_hba.conf` 的来源网段和防火墙规则，并重载服务；默认安装不会开放远程端口。

## 升级和卸载边界

- 同一 PostgreSQL 15 大版本内的 RPM 小版本升级，应先备份，再用新包的 `install.sh` 执行升级；不要删除 PGDATA。
- PostgreSQL 大版本迁移（例如 14 到 15）不能用 RPM 覆盖替代，应使用经过验证的 `pg_upgrade` 或 `pg_dump`/`pg_restore` 方案。
- 本包没有自动卸载脚本，避免误删数据库。卸载前先确认备份，再手工停止服务并使用 RPM 工具移除软件；数据目录需要单独按变更审批处理。

## 校验

```bash
sha256sum -c checksums.sha256
rpm --checksig --verbose offline/rpms/*.rpm
systemctl is-enabled postgresql-15.service
```

如果 RPM 安装报依赖错误，先在 SLES 12 SP5 安装介质/已批准的软件源中补齐基础库，再重新执行安装；不要使用 `rpm --nodeps`。
