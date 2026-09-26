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

1. SLES 12 SP5 或 EulerOS 2.0 x86_64 服务器、SSH 账号和可写目录。
2. Python离线安装包。
3. 应用离线安装包。
4. SMSC 的 Host、Port、System ID、Password、Source Address。
5. 服务器 IP、端口访问策略和少量测试号码。

登录服务器检查环境：

```bash
cat /etc/os-release
uname -m
ldd --version | head -1
systemctl --version | head -1
```

以上检查可以确认OS版本，并确认架构为 x86_64，且 glibc 不低于 2.17。

## 3. 安装应用
### 3.0 安装独立 python 包
假设安装包已复制到 `/home/paas`，本文统一使用 `/opt/marketing-copilot` 作为应用目录：
```bash
# 解压再安装独立 Python 包
cd /home/paas
tar -xzf sles12sp5-python-3.11.5-20260917-script.tar.gz 
cd sles12sp5-python-3.11.5-20260917-script

bash install.sh --prefix /opt/marketing-copilot --force

# 安装后验证Python运行时
/opt/marketing-copilot/python/bin/python3.11 -c \
'import _ctypes, ctypes, ssl, venv; print("Python runtime OK")'
```

### 3.1 解压应用包

假设安装包已复制到 `/home/paas`，本文统一使用 `/opt/marketing-copilot` 作为应用目录：

```bash
mkdir -p /opt/marketing-copilot
cd /home/paas
tar -xzf marketing-copilot-sles12sp5-system-python-20260917105732.tar.gz \
  --strip-components=1 -C /opt/marketing-copilot
cd /opt/marketing-copilot
```

解压完成后，`/opt/marketing-copilot` 下应直接看到 `backend`、`frontend`、`offline`、`run.sh` 和 `marketing-copilot.env.example`。


### 3.2 第一次创建和配置 env 文件

本手册统一使用 root 用户部署和启动，不需要另外创建运行账号或手工授权目录。

```bash
cd /opt/marketing-copilot
cp marketing-copilot.env.example marketing-copilot.env

# 修改端口号默认:MC_PORT=19090
vi marketing-copilot.env
```

### 3.3 启动应用

`run.sh` 会自动创建 Python 虚拟环境，并从随包提供的离线 wheel 目录安装依赖。确认 env 后直接执行：

```bash
cd /opt/marketing-copilot
./run.sh
```
正常情况下，tar 会保留 `run.sh` 的执行权限。如果执行 `./run.sh` 时提示 `Permission denied`，再执行：

```bash
chmod 0755 /opt/marketing-copilot/run.sh
```

检查：

```bash
curl --fail http://127.0.0.1:19090/api/ready
curl --fail http://127.0.0.1:19090/api/health
```

应分别返回 ready 和健康/数据库已连接状态。浏览器打开 `http://服务器IP:19090`，用 env 中的管理员账号登录。首次登录后进入“系统设置”→“用户与权限”修改密码。

### 3.4 服务管理

root 执行 `run.sh` 时，如果系统存在 systemd，启动脚本会自动创建并启动 `marketing-copilot.service`。服务启动后可使用：

```bash
systemctl status marketing-copilot.service
systemctl restart marketing-copilot.service
systemctl stop marketing-copilot.service
tail -f /opt/marketing-copilot/logs/app.log
```

## 4. 首次登录后的基础设置
默认登录账号：admin
密码：Huawei@123

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

## 6. 创建批量营销任务

### 6.1 准备目标清单

最少需要 `msisdn` 列，推荐使用 UTF-8 CSV，并包含 `language` 和业务字段：

```csv
msisdn,event_filter_id,cell_id,customer_name,offer_code,language
251900001001,BATCH_EVT_001,CELL_A01,Alice,OFFER10,English
251900001002,BATCH_EVT_002,CELL_A02,Berhanu,OFFER20,Amharic
```

手机号应带国家码；`language` 必须与模板语言一致。

### 6.2 创建、上传和配置

1. 进入“批量活动”→“新增活动”。
2. 填写活动名称、开始时间、结束时间、每日发送开始/结束时间。
3. 勾选允许发送的星期。
4. 填写批次大小；首次测试建议 1 或 2。
5. 填写号码前缀，例如 `251`，并选择已测试成功的 SMPP 配置。
6. 点击“保存”。
7. 活动行点击“更多”→“上传目标清单”，选择 CSV 并确认，检查行数和详情。
8. 点击“更多”→“配置短信”，新建短信内容活动。
9. 为清单中出现的每种语言创建模板并保存。模板可以使用 `{msisdn}`、`{event_filter_id}`、`{cell_id}`、`{offer_code}` 等变量，变量名必须与 CSV 列名一致。
10. 确认活动“就绪度”包含：清单、模板、SMPP、发送计划全部完成。发送前确认正文中没有未替换的 `{...}`。

### 6.3 启动和查看

1. 确认系统模式为生产模式。
2. 点击活动行“开始”。
3. 进入“短信任务”，活动类型选择“批量”。
4. 观察 `queued → sending → submitted`；如果 SMSC 开启并返回送达回执，再观察是否更新为 `delivered`。
5. 打开任务详情，检查号码、正文、尝试次数、SMSC message ID 和错误信息。

第一次生产发送只使用少量测试号码，先确认 SMSC 已接受提交；如果启用了送达回执，再确认回执状态后扩大名单。

## 7. 创建实时营销任务

### 7.1 准备活动白名单

白名单至少包含 `msisdn`，推荐包含 `region`、`language`：

```csv
msisdn,region,language
251900001001,ET,English
251900001002,ET,Amharic
251900001003,ET,Tigrigna
```

### 7.2 创建并配置活动

1. 进入“实时活动”→“新增活动”。
2. 填写名称、起止日期、每日发送时间和允许发送星期。
3. 填写号码前缀并选择 SMPP 配置，保存。
4. 活动行点击“更多”→“上传白名单”，选择 CSV 并确认行数。
5. 点击“更多”→“配置短信”，新建短信内容活动。
6. 为白名单中的每种语言创建模板并保存。模板可以使用事件文件中的 `{event_filter_id}`、`{msisdn}`、`{cell_id}` 等变量，变量名必须与文件列名一致。
7. 确认活动就绪度完成，且模板正文没有未替换的 `{...}`。

### 7.3 投递事件文件并发送

按以下步骤投递事件文件：

1. 点击实时活动“开始”，确认活动状态变为“进行中”。
2. 打开活动详情，查看系统显示的监听目录 `watch_dir`、监听文件名前缀和 `inbox` 目录。
3. 如果使用监听目录，将事件平台导出的文件完整复制到 `watch_dir`。导出的文件应包含 `msisdn` 列；常见格式如下：

   ```text
   eventType|event_filter_id|starttime|msisdn|imsi|imei|cell_id
   8004|CEP_admin_20260717114709359|1785328439|975028665|636019965838629|356701088786390|636011BB0C02
   ```

   文件名默认应类似 `Android_4G_Switch_Somali_20260814123000.csv`：前缀以活动详情为准，后面为 `YYYYMMDDHHMMSS` 时间戳。文件扩展名可为 `.csv`、`.txt`、`.xlsx` 或 `.xls`，文件必须写完后再复制到目录。

4. 打开活动详情，查看“接收文件”“事件用户”和“匹配用户”数量。
5. 进入“短信任务”，活动类型选择“实时”。
6. 确认只有白名单号码产生任务，且短信模板中的 `{event_filter_id}`、`{msisdn}` 等变量已正确替换。
7. 如果 SMSC 提供送达回执，等待任务变为 `delivered`；如果未启用回执，以 `submitted` 作为 SMSC 已接受提交的结果，并结合短信中心侧记录确认发送情况。

系统会按号码去重，并过滤不在实时白名单中的号码。重复投递同一文件不应产生重复短信。

## 8. 监控、结束和故障处理

### 8.1 页面监控

- “仪表盘”：总体统计。
- “短信任务”：按类型、活动、状态筛选并查看单条短信。
- “容量中心”：查看 Worker 和 SMSC 健康状态。
- “运维中心”：查看活动进度、异常、DLR、审计和报表。
- “日志”：查看应用错误。

活动结束时，在批量/实时活动列表点击“结束”；实时活动结束后不要继续向 inbox 放文件。

### 8.2 常见故障

**页面打不开：**

```bash
systemctl status marketing-copilot.service
curl http://127.0.0.1:19090/api/ready
tail -n 100 /opt/marketing-copilot/logs/app.log
```

本机正常而远程失败时，检查防火墙、路由和 TCP 19090 访问策略。

**SMPP 连接失败：**检查 Host、Port、账号密码、服务器到 SMSC 的网络、配置是否启用、Bind Type 是否为 `transceiver`，以及系统模式是否为生产模式。

**任务一直 queued：**检查活动是否已开始、日期/星期/时间是否允许、清单或事件文件是否存在、模板和 SMPP 是否就绪、号码是否在白名单且不在排除名单。

**任务 submitted 没有 delivered：**先确认 SMSC 是否提供并启用了送达回执。回执是可选的；如果未启用，`submitted` 仅表示 SMSC 已接受提交，不能据此判断短信未送达。若已启用回执，再检查 `transceiver`、`registered_delivery`、SMSC 网络和长短信分片回执。

**任务 failed/retrying：**在短信任务查看错误信息；修复原因后，只对确认不会造成重复触达的任务执行“重新入队”。

## 9. 上线前检查清单

- [ ] SLES 12 SP5 x86_64、Python 3.11.5、ssl/venv 均正常。
- [ ] `marketing-copilot.env` 已创建，管理员密码已修改，认证未关闭。
- [ ] 数据库、日志、锁、备份目录可写。
- [ ] `/api/ready`、`/api/health` 正常，systemd 已启动。
- [ ] 系统模式为生产模式，SMPP 测试连接成功。
- [ ] 白名单、排除名单、短信模板均已配置。
- [ ] 已用少量测试号码验证 SMSC 提交成功；如启用送达回执，再验证回执状态。
- [ ] 已确认短信任务、日志、审计记录可查看。

## 10. 命令速查

```bash
sudo systemctl status marketing-copilot.service
sudo systemctl restart marketing-copilot.service
curl --fail http://127.0.0.1:19090/api/ready
curl --fail http://127.0.0.1:19090/api/health
tail -f /opt/marketing-copilot/logs/app.log
sudo systemctl stop marketing-copilot.service
```
