# SMSC模拟器使用说明

## 1. 功能简介

项目内置一个纯 Python SMPP 3.4 SMSC 模拟器，用于在没有真实 SMSC 环境时进行本地端到端短信发送测试。

模拟器入口：

```text
backend/tools/smsc_simulator.py
```

冒烟测试客户端：

```text
backend/tools/smsc_smoke_test.py
```

模拟器支持：

- `bind_transceiver`、`bind_transmitter` 和 `bind_receiver`；
- System ID和密码验证；
- 接收 `submit_sm`；
- 返回成功的 `submit_sm_resp` 和SMSC message ID；
- 打印源号码、目标号码、编码、正文和原始十六进制数据；
- GSM和Unicode/UCS-2短信；
- UDH长短信分片识别与完整正文重组；
- `enquire_link`连接保活；
- `unbind`；
- 自动发送 `DELIVRD` delivery receipt；
- 接收客户端返回的 `deliver_sm_resp`。

模拟器仅用于开发和测试，不应作为生产SMSC使用。

## 2. 环境要求

模拟器本身只使用Python标准库，不需要单独安装第三方依赖。

建议Python版本：

```text
Python 3.11或3.12
```

如果需要运行冒烟测试客户端，则需要安装项目后端依赖，其中包括：

```text
smpplib>=2.2.4,<3
```

在项目虚拟环境中安装：

```bash
python -m pip install -r backend/requirements.txt
```

## 3. 启动模拟器

进入后端目录：

```bash
cd backend
```

使用默认配置启动：

```bash
python -m tools.smsc_simulator
```

默认参数为：

| 参数 | 默认值 |
| --- | --- |
| 监听地址 | `127.0.0.1` |
| 监听端口 | `2775` |
| System ID | `test` |
| Password | `test` |
| 回执延迟 | `0.5`秒 |

完整启动命令：

```bash
python -m tools.smsc_simulator \
  --host 127.0.0.1 \
  --port 2775 \
  --system-id test \
  --password test \
  --receipt-delay 0.5
```

Windows PowerShell可以写成一行：

```powershell
python -m tools.smsc_simulator --host 127.0.0.1 --port 2775 --system-id test --password test --receipt-delay 0.5
```

启动成功后输出：

```text
Local SMSC simulator listening on ('127.0.0.1', 2775)
Credentials: system_id='test', password='test'
Press Ctrl+C to stop.
```

使用 `Ctrl+C` 停止模拟器。

## 4. 启动参数

| 参数 | 说明 | 示例 |
| --- | --- | --- |
| `--host` | 监听地址 | `127.0.0.1` |
| `--port` | SMPP监听端口 | `2775` |
| `--system-id` | SMPP登录账号 | `test` |
| `--password` | SMPP登录密码 | `test` |
| `--receipt-delay` | 提交成功后返回状态报告的延迟秒数 | `0.5` |

如需允许局域网其他机器连接，可以使用：

```bash
python -m tools.smsc_simulator --host 0.0.0.0 --port 2775
```

使用 `0.0.0.0` 时，应确认本机防火墙和网络访问范围，避免将测试SMPP端口暴露到不可信网络。

## 5. Marketing Copilot配置

启动模拟器后，在系统的SMPP配置页面创建配置：

| 配置项 | 测试值 |
| --- | --- |
| Name | `Local SMSC` |
| Host | `127.0.0.1` |
| Port | `2775` |
| System ID | `test` |
| Password | `test` |
| Source Address | `telebirr` |
| Bind Type | `transceiver` |
| Enabled | 启用 |

`Bind Type`必须使用`transceiver`，否则系统无法在同一连接上接收delivery receipt。

### 切换到真实发送模式

进入“系统设置”，将：

```text
system_mode
```

设置为：

```text
production
```

如果保持为：

```text
development
```

系统不会连接SMSC模拟器，而是直接生成开发模式模拟成功结果。

## 6. 独立冒烟测试

保持SMSC模拟器运行，再打开另一个终端：

```bash
cd backend
```

执行：

```bash
python -m tools.smsc_smoke_test \
  --host 127.0.0.1 \
  --port 2775 \
  --system-id test \
  --password test \
  --source telebirr \
  --destination 251900001240 \
  --message "Marketing Copilot local SMPP test"
```

Windows PowerShell示例：

```powershell
python -m tools.smsc_smoke_test --host 127.0.0.1 --port 2775 --system-id test --password test --source telebirr --destination 251900001240 --message "Marketing Copilot local SMPP test"
```

测试成功时，客户端输出类似：

```text
submit_sm_resp: sequence=3, message_id=b'0000000001'
delivery_receipt: message_id=b'0000000001', state=2
PASS: 1 submit_sm_resp and DELIVRD receipt(s) received
```

其中：

- `submit_sm_resp`表示模拟SMSC接受了短信提交；
- `message_id`是模拟SMSC生成的短信编号；每次启动从当前时间戳基数重新递增，重启后不会与历史消息重复，避免后端把回执匹配到旧任务；
- `state=2`是SMPP的`DELIVERED`状态；
- `PASS`表示提交响应和最终状态报告都已收到。

## 7. 长短信测试

可以通过较长内容测试短信分片和重组：

```powershell
$message = "A" * 400
python -m tools.smsc_smoke_test --message $message
```

400个GSM字符通常会拆分为3个短信分片。客户端应输出3个提交响应和3个状态报告：

```text
submit_sm_resp: ... message_id=b'0000000001'
submit_sm_resp: ... message_id=b'0000000002'
submit_sm_resp: ... message_id=b'0000000003'
delivery_receipt: ... state=2
delivery_receipt: ... state=2
delivery_receipt: ... state=2
PASS: 3 submit_sm_resp and DELIVRD receipt(s) received
```

模拟器端会显示：

```text
=== REASSEMBLED multipart SMS (3 parts) ===
完整短信正文
```

系统只有在长短信所有SMSC分片都返回成功回执后，才会将整个短信任务标记为`delivered`。

## 8. 完整业务链路测试

完整测试需要同时运行SMSC模拟器和Marketing Copilot后端。

### 第一步：启动SMSC模拟器

```bash
cd backend
python -m tools.smsc_simulator
```

### 第二步：启动Marketing Copilot后端

打开另一个终端：

```bash
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 19090 --workers 1
```

必须使用单Worker，避免重复启动活动调度器和短信发送Worker。

### 第三步：准备业务配置

1. 将`system_mode`设置为`production`；
2. 创建并启用本地SMPP配置；
3. 导入包含测试号码的全局白名单；
4. 创建批量活动或实时活动；
5. 创建与活动关联的短信内容活动；
6. 为白名单用户语言创建并启用短信模板；
7. 确认活动状态为`in_progress`；
8. 确认当前日期、星期和时间处于活动发送范围。

### 第四步：提交数据

批量活动：

- 上传包含`msisdn`字段的客户名单；
- 活动启用后，后台调度器会自动生成批次并创建短信任务。

实时活动：

- 上传活动白名单；
- 将事件CSV/XLSX文件放入：

```text
{campaign_dir}/inbox
```

- 后台调度器会自动读取文件、匹配名单并创建短信任务。

### 第五步：查看任务状态

进入左侧菜单“短信任务”，预期看到状态变化：

```text
queued → sending → submitted → delivered
```

同时，模拟器控制台会打印收到的`submit_sm`请求及短信内容。

## 9. 模拟器输出说明

收到短信时输出类似：

```json
{
  "message_id": "0000000001",
  "source_addr": "telebirr",
  "destination_addr": "251900001240",
  "registered_delivery": 1,
  "data_coding": 8,
  "text": "Hello local SMSC",
  "concat": null,
  "raw_hex": "..."
}
```

主要字段：

| 字段 | 说明 |
| --- | --- |
| `message_id` | 模拟SMSC生成的短信编号 |
| `source_addr` | 短信发送方 |
| `destination_addr` | 接收手机号 |
| `registered_delivery` | 是否请求状态报告，`1`表示请求 |
| `data_coding` | 短信编码，`8`通常为UCS-2 |
| `text` | 解码后的短信正文 |
| `concat` | 长短信分片信息，普通短信为`null` |
| `raw_hex` | 原始短信字节的十六进制表示 |

返回状态报告时输出：

```text
DELIVERY_RECEIPT message_id=0000000001 state=DELIVRD
```

客户端确认状态报告时输出：

```text
deliver_sm_resp sequence=1000000 status=0
```

## 10. 常见问题

### 10.1 Connection refused

可能原因：

- 模拟器没有启动；
- Host或Port配置错误；
- 端口被防火墙阻止；
- 模拟器启动后立即异常退出。

检查模拟器是否显示：

```text
Local SMSC simulator listening on ...
```

### 10.2 Bind失败

确认Marketing Copilot中的System ID和Password与模拟器启动参数一致。

默认值为：

```text
System ID: test
Password: test
```

### 10.3 系统没有连接模拟器

检查：

- `system_mode`是否为`production`；
- SMPP配置是否启用；
- 是否存在多条启用配置；系统使用第一条启用配置；
- 后端是否已经重新加载最新代码。

### 10.4 任务停留在queued

检查：

- 活动是否为`in_progress`；
- 当前日期是否位于活动起止日期内；
- 当前星期是否允许发送；
- 当前时间是否处于每日发送时段；
- 是否存在启用的短信模板；
- 测试号码是否在全局白名单中；
- 测试号码是否被加入排除名单。

### 10.5 任务进入failed或retrying

在“短信任务”页面查看`error_msg`。常见原因包括：

- SMSC无法连接；
- bind账号或密码错误；
- SMPP连接中断；
- 没有启用的SMPP配置。

发送失败会自动退避重试，达到最大次数后进入`failed`，可以在UI中手动重新入队。

### 10.6 任务停留在submitted

表示已经收到`submit_sm_resp`，但尚未收到全部delivery receipt。检查：

- 发送时是否设置`registered_delivery`；
- 模拟器是否打印`DELIVERY_RECEIPT`；
- 后端SMPP连接是否使用`transceiver`；
- 长短信是否还有部分分片未返回回执。

## 11. 停止模拟器

在模拟器终端按：

```text
Ctrl+C
```

模拟器不会写入业务数据库。它只在内存中保存当前连接的长短信分片和递增message ID，重启后message ID会从`0000000001`重新开始。
