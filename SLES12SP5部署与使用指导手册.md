# Marketing Copilot：SLES 12 SP5 部署与使用指导手册

> 适用对象：没有 Linux、Python 或短信系统经验的部署及业务人员。  
> 适用环境：SUSE Linux Enterprise Server 12 SP5 x86_64。  
> 推荐方式：离线安装包、系统 Python 3.11.5、单进程 Uvicorn。

## 1. 系统组成

Marketing Copilot 包含后端服务、前端静态页面和 DuckDB 数据库；短信通过 SMPP 连接客户提供的 SMSC。RabbitMQ、Nginx 和 Node.js 不是当前主链路运行的必需组件。发送状态通常为：

```text
queued → sending → submitted → delivered
```

`submitted` 表示 SMSC 已接受短信提交；只有在 SMSC 提供并返回送达回执时，系统才可能将任务更新为 `delivered`。送达回执属于可选能力，未收到回执不能直接判断短信未送达。

## 2. 部署前准备

请先准备：

1. SLES 12 SP5 x86_64 服务器、SSH 账号和可写目录。
2. Python **3.11.5**。标准 CPython 通常已包含 `ssl` 和 `venv`，部署前检查其可用即可。
3. 应用离线安装包。
4. SMSC 的 Host、Port、System ID、Password、Source Address。
5. 服务器 IP、端口访问策略和少量测试号码。

登录服务器检查环境：

```bash
cat /etc/os-release
uname -m
python3.11 --version
python3.11 - <<'PY'
import ssl, sys
assert sys.version_info[:3] == (3, 11, 5), sys.version
assert ssl.OPENSSL_VERSION_INFO >= (1, 1, 1), ssl.OPENSSL_VERSION
print(sys.version)
print(ssl.OPENSSL_VERSION)
PY
python3.11 -m venv --help >/dev/null
```

以上检查必须全部成功。`ssl` 和 `venv` 一般随标准 CPython 提供，但精简安装或自行编译的 Python 可能缺少它们。不要用其他 Python 小版本替代 3.11.5。

## 3. 安装应用

### 3.1 解压应用包

假设安装包已复制到 `/home/paas`，本文统一使用 `/opt/marketing-copilot` 作为应用目录：

```bash
mkdir -p /opt/marketing-copilot
cd /home/paas
tar -xzf marketing-copilot-sles12sp5-system-python-*.tar.gz \
  --strip-components=1 -C /opt/marketing-copilot
cd /opt/marketing-copilot
```

解压完成后，`/opt/marketing-copilot` 下应直接看到 `backend`、`frontend`、`offline`、`run.sh` 和 `marketing-copilot.env.example`。

正常情况下，tar 会保留 `run.sh` 的执行权限。如果执行 `./run.sh` 时提示 `Permission denied`，再执行：

```bash
chmod 0755 /opt/marketing-copilot/run.sh
```

### 3.2 第一次创建和配置 env 文件

本手册统一使用 root 用户部署和启动，不需要另外创建运行账号或手工授权目录。

```bash
cd /opt/marketing-copilot
cp marketing-copilot.env.example marketing-copilot.env
vi marketing-copilot.env
```

至少确认以下内容：

```dotenv
DUCKDB_PATH=/opt/marketing-copilot/data/marketing_copilot.duckdb
MC_FRONTEND_DIR=/opt/marketing-copilot/frontend/dist
MC_HOST=0.0.0.0
MC_PORT=8000
MC_LOG_PATH=/opt/marketing-copilot/logs/app.log
MC_INSTANCE_LOCK=/opt/marketing-copilot/run/instance.lock
MC_BACKUP_DIR=/opt/marketing-copilot/backups
MC_SYSTEM_MODE=production
MC_TASK_WORKERS=4
MC_AUTH_DISABLED=0
MC_ADMIN_USERNAME=admin
MC_ADMIN_PASSWORD=请替换为首次登录密码
MC_UPDATE_MODE=manual
MC_STATE_DIR=/opt/marketing-copilot/data
MC_UPDATE_DIR=/opt/marketing-copilot/updates
```

`run.sh` 启动时会自动创建应用目录下的 `data`、`logs`、`run`、`backups` 和 `updates` 子目录。

配置说明：

- 数据库、日志、锁和备份目录必须对运行账号可写。
- `MC_FRONTEND_DIR` 要指向 `frontend/dist`，不是前端源码目录。
- 生产环境 `MC_SYSTEM_MODE` 必须为 `production`。
- 生产环境 `MC_AUTH_DISABLED` 必须为 `0`，管理员密码必须修改；默认密码为 `Huawei@123`。
- 手工或 nohup 运行使用 `MC_UPDATE_MODE=manual`；改为 systemd 托管后使用 `systemd`。

### 3.3 启动应用

`run.sh` 会自动创建 Python 虚拟环境，并从随包提供的离线 wheel 目录安装依赖。确认 env 后直接执行：

```bash
cd /opt/marketing-copilot
./run.sh
```

另开终端检查：

```bash
curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
```

应分别返回 ready 和健康/数据库已连接状态。浏览器打开 `http://服务器IP:8000`，用 env 中的管理员账号登录。首次登录后进入“系统设置”→“用户与权限”修改密码。

### 3.4 服务管理

root 执行 `run.sh` 时，如果系统存在 systemd，启动脚本会自动创建并启动 `marketing-copilot.service`。服务启动后可使用：

```bash
systemctl status marketing-copilot.service
systemctl restart marketing-copilot.service
systemctl stop marketing-copilot.service
tail -f /opt/marketing-copilot/logs/app.log
```

## 4. 首次登录后的基础设置

1. 进入“系统设置”→“基础设置”，将系统模式设为“生产模式”，点击“保存”。开发模式不会连接 SMSC。
2. 进入“系统设置”→“数据库配置”，点击“测试”，确认连接成功。不要随意修改数据库路径。
3. 准备全局白名单和排除名单。排除名单中的号码不会创建短信任务。

## 5. 配置短信中心（SMPP/SMSC）

### 5.1 参数说明

| 页面字段 | 填写内容 |
| --- | --- |
| 名称 | 便于识别的名称 |
| Host / Port | SMSC 地址和端口，常见端口为 2775，以实际值为准 |
| System ID / Password | SMPP 账号和密码 |
| Source Address | 短信签名、号码或发送方 |
| Bind Type | 推荐 `transceiver`，用于在 SMSC 提供回执时接收 delivery receipt |
| Data Coding | 按 SMSC 要求填写；中文通常使用 UCS-2 |
| TON / NPI | 按 SMSC 要求填写，默认可先用 0 / 0 |
| 连接池 / 保活 / 超时 | 初次可用 1 / 30 秒 / 10 秒 |
| TPS | 不得超过 SMSC 授权值 |

### 5.2 页面配置步骤

1. 进入“系统设置”→“SMPP 配置”。
2. 点击“新增配置”。
3. 填写 Host、Port、System ID、Password 和 Source Address。
4. Bind Type 选择 `transceiver`。
5. 按短信中心要求填写编码、TON/NPI、连接池、保活、超时和 TPS。
6. 点击“保存”。
7. 在列表点击“测试连接”，确认成功。

存在多条启用配置时，系统使用第一条启用配置；生产初期建议只启用已验证的配置。

### 5.3 没有真实 SMSC 时使用内置模拟器

仅在测试环境执行：

```bash
cd /opt/marketing-copilot/backend
/opt/marketing-copilot/venv/bin/python -m tools.smsc_simulator \
  --host 127.0.0.1 --port 2775 --system-id test --password test
```

页面中填写 Host `127.0.0.1`、Port `2775`、System ID `test`、Password `test`、Bind Type `transceiver`，保存并测试连接。系统模式仍必须设置为生产模式，模拟器才会收到真实 SMPP 提交。模拟器只用于联调，不能用于生产。

## 6. 配置短信内容

每个营销活动都必须有短信内容活动和至少一个匹配用户语言的模板。

1. 进入“批量活动”或“实时活动”。
2. 在活动行点击“更多”→“配置短信”。
3. 新建短信内容活动。
4. 为每种语言添加模板并保存。
5. 可使用 `{msisdn}`、`{event_filter_id}`、`{cell_id}`、`{offer_code}` 等变量。

变量名必须和上传文件的列名一致。发送前确认变量已替换，不能发送包含未替换 `{...}` 的内容。

## 7. 创建批量营销任务

### 7.1 准备目标清单

最少需要 `msisdn` 列，推荐使用 UTF-8 CSV，并包含 `language` 和业务字段：

```csv
msisdn,event_filter_id,cell_id,customer_name,offer_code,language
251900001001,BATCH_EVT_001,CELL_A01,Alice,OFFER10,English
251900001002,BATCH_EVT_002,CELL_A02,Berhanu,OFFER20,Amharic
```

手机号应带国家码；`language` 必须与模板语言一致。

### 7.2 创建、上传和配置

1. 进入“批量活动”→“新增活动”。
2. 填写活动名称、开始时间、结束时间、每日发送开始/结束时间。
3. 勾选允许发送的星期。
4. 填写批次大小；首次测试建议 1 或 2。
5. 填写号码前缀，例如 `251`，并选择已测试成功的 SMPP 配置。
6. 点击“保存”。
7. 活动行点击“更多”→“上传目标清单”，选择 CSV 并确认，检查行数和详情。
8. 点击“更多”→“配置短信”，为清单中的每种语言创建模板并保存。
9. 确认活动“就绪度”包含：清单、模板、SMPP、发送计划全部完成。

### 7.3 启动和查看

1. 确认系统模式为生产模式。
2. 点击活动行“开始”。
3. 进入“短信任务”，活动类型选择“批量”。
4. 观察 `queued → sending → submitted`；如果 SMSC 开启并返回送达回执，再观察是否更新为 `delivered`。
5. 打开任务详情，检查号码、正文、尝试次数、SMSC message ID 和错误信息。

第一次生产发送只使用少量测试号码，先确认 SMSC 已接受提交；如果启用了送达回执，再确认回执状态后扩大名单。

## 8. 创建实时营销任务

### 8.1 准备活动白名单

白名单至少包含 `msisdn`，推荐包含 `region`、`language`：

```csv
msisdn,region,language
251900001001,ET,English
251900001002,ET,Amharic
251900001003,ET,Tigrigna
```

### 8.2 创建并配置活动

1. 进入“实时活动”→“新增活动”。
2. 填写名称、起止日期、每日发送时间和允许发送星期。
3. 填写号码前缀并选择 SMPP 配置，保存。
4. 活动行点击“更多”→“上传白名单”，选择 CSV 并确认行数。
5. 点击“更多”→“配置短信”，为白名单语言创建模板并保存。
6. 确认活动就绪度完成。

### 8.3 投递事件文件并发送

事件 CSV/XLS/XLSX 推荐包含：

```csv
msisdn,event_filter_id,cell_id,customer_name,offer_code
251900001001,RT_EVT_001,CELL_R01,Alice,FLASH10
251900001002,RT_EVT_002,CELL_R02,Berhanu,FLASH20
```

1. 点击实时活动“开始”。
2. 点击“更多”→“上传目标文件”，选择事件文件；或将文件完整复制到活动详情显示的 `inbox` 目录。
3. 打开活动详情，查看接收文件、事件用户和匹配用户数。
4. 进入“短信任务”，活动类型选择“实时”。
5. 确认只有白名单号码产生任务，且正文变量已替换。
6. 如果 SMSC 提供送达回执，等待任务变为 `delivered`；如果未启用回执，以 `submitted` 作为 SMSC 已接受提交的结果，并结合短信中心侧记录确认发送情况。

系统会按号码去重，并过滤不在实时白名单中的号码。重复投递同一文件不应产生重复短信。

## 9. 监控、结束和故障处理

### 9.1 页面监控

- “仪表盘”：总体统计。
- “短信任务”：按类型、活动、状态筛选并查看单条短信。
- “容量中心”：查看 Worker 和 SMSC 健康状态。
- “运维中心”：查看活动进度、异常、DLR、审计和报表。
- “日志”：查看应用错误。

活动结束时，在批量/实时活动列表点击“结束”；实时活动结束后不要继续向 inbox 放文件。

### 9.2 常见故障

**页面打不开：**

```bash
systemctl status marketing-copilot.service
curl http://127.0.0.1:8000/api/ready
tail -n 100 /opt/marketing-copilot/logs/app.log
```

本机正常而远程失败时，检查防火墙、路由和 TCP 8000 访问策略。

**SMPP 连接失败：**检查 Host、Port、账号密码、服务器到 SMSC 的网络、配置是否启用、Bind Type 是否为 `transceiver`，以及系统模式是否为生产模式。

**任务一直 queued：**检查活动是否已开始、日期/星期/时间是否允许、清单或事件文件是否存在、模板和 SMPP 是否就绪、号码是否在白名单且不在排除名单。

**任务 submitted 没有 delivered：**先确认 SMSC 是否提供并启用了送达回执。回执是可选的；如果未启用，`submitted` 仅表示 SMSC 已接受提交，不能据此判断短信未送达。若已启用回执，再检查 `transceiver`、`registered_delivery`、SMSC 网络和长短信分片回执。

**任务 failed/retrying：**在短信任务查看错误信息；修复原因后，只对确认不会造成重复触达的任务执行“重新入队”。

## 10. 上线前检查清单

- [ ] SLES 12 SP5 x86_64、Python 3.11.5、ssl/venv 均正常。
- [ ] `marketing-copilot.env` 已创建，管理员密码已修改，认证未关闭。
- [ ] 数据库、日志、锁、备份目录可写。
- [ ] `/api/ready`、`/api/health` 正常，systemd 已启动。
- [ ] 系统模式为生产模式，SMPP 测试连接成功。
- [ ] 白名单、排除名单、短信模板均已配置。
- [ ] 已用少量测试号码验证 SMSC 提交成功；如启用送达回执，再验证回执状态。
- [ ] 已确认短信任务、日志、审计记录可查看。

## 11. 命令速查

```bash
sudo systemctl status marketing-copilot.service
sudo systemctl restart marketing-copilot.service
curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
tail -f /opt/marketing-copilot/logs/app.log
sudo systemctl stop marketing-copilot.service
```
