# Marketing Copilot VNext 配置审计、绑定修复与性能闭环报告

日期：2026-10-17
任务：`Marketing_Copilot_VNext_Configuration_Contract_Performance_Cleanup_Codex_Task.md`
基线：`HEAD=6ebfe088fbc19ab61539999fd627570d4d9b5a10`
变更纪律：保留已有工作区数据、历史测试证据、用户修改和未跟踪文件；未 commit/push。

## 结论

VNext 配置契约已闭环：Worker 只表示业务并发；`pool_size` 只表示 route 的物理 SMPP bind 上限；`window_size` 按物理 bind 生效；`tps` 单位固定为 `submit_sm_parts/s`；默认 `tps_scope=route_shared`，不会把旧 route-wide TPS 静默乘以 Worker 或 pool。

`RouteCreditBroker` 已去掉每 PDU 的磁盘锁/JSON/rename/fsync，并改为每 8 PDU 一次 durable lease refill；bind lease 只在连接生命周期同步。性能 A/B 在 WSL 原生 ext4 上显示 durable refill 下降 87.5%，吞吐不降。

本次隔离实现判定：`VNEXT_CONFIG_CONTRACT_READY`、`VNEXT_CONFIG_PERF_READY`。本轮已在用户指定的 EulerOS 测试服务器完成 loopback-only 配置生效、RouteCredit A/B、Bind/Window 矩阵和 P5500；真实 SMSC、真实 DLR、生产部署仍属于 `VNEXT_CONFIG_BLOCKED_ENV`，本报告没有把它们拼成 PASS。

## 实施内容

### 配置与生效

- `_load_routes()` 补齐 `pool_size`、`bind_type`、`tps_scope`、heartbeat、reconnect、DLR、priority 和地址编码字段。
- `smpp_configs` 增加 `tps_scope` 迁移和 `route_shared` 回填；Pydantic/API/UI 增加范围校验和 scope 选择。
- 配置 CRUD 会标记 VNext `restart_required`；状态 API 返回 pending/restart、effective route capacity、active bind slots 和明确语义。
- 默认路由从 `min(id)` 修正为 `priority,id`。
- bind command 支持 receiver/transmitter/transceiver；submit_sm 使用配置的 source TON/NPI、data coding、DLR mode。
- Worker 支持按 route 的 bind target 创建多个物理 transport；共享 `RouteBindLease` 将跨 Worker 总 bind 数限制在 `pool_size` 内。

### 冗余清理与热路径

- 删除旧的 1.15 TPS safety factor。
- 删除每 PDU 的跨进程文件同步；保留批次预留的 crash-safe durability：崩溃只损失未消费配额，不会超发。
- `route_shared` 和 `per_bind` 使用不同 credit key，避免把两个业务语义混成一个限流器。
- 没有删除仍被 legacy gateway/API 使用的 DB 字段或 `MC_SMPP_*` 配置；它们已明确标为 legacy，不再进入 VNext 容量计算。

## 配置审计证据

详细逐字段、环境变量和消费者追踪见 [配置契约全量清单](Marketing_Copilot_VNext_Config_Contract_Inventory.md)。
机器可读清单保留在隔离证据目录 `.tmp/vnext-config-audit-db/config-inventory.json`（33 个有效条目、2 个 Legacy 边界组，无秘密值）。

| 检查 | 结果 | 证据 |
|---|---|---|
| schema/范围/有效容量检查器 | PASS（临时 DB 副本执行 `init_db()` 后） | `backend/tools/vnext_config_contract_check.py`；`backend/tests/test_vnext_config_contract.py` |
| 工作区原始 DB 只读检查 | NOT_VERIFIED：原始文件还没有 `tps_scope`，因此未把旧文件误判为当前契约 PASS | 未改写原始 `backend/marketing_copilot.duckdb`；由下次正常 `init_db()` 迁移 |
| 配置加载/默认路由 | PASS：临时迁移副本加载 2 条 route，默认 route=1 | `_load_routes()` + `_default_route_id()`，无密码输出 |
| 真实关系 | PASS：Worker 不参与 TPS；Bind lease 受 pool 上限；Window 在 transport semaphore；TPS key 按 scope 分离 | `vnext_service.py`, `worker.py`, `smpp_transport.py`, `route_credits.py` |
| 生效状态 | PASS：运行时更新返回 `pending_restart`，安全重启后清除 | `smpp_service.py`, `/api/smpp/configs`, VNext status contract |

## 性能对照（正式 ext4）

环境：WSL `AlmaLinux-8`，原生 `/tmp/mc-vnext-config-perf-20261017b/src`；`df -T .` 原始记录为：`/dev/sdd ext4 1055762868 105991844 896067552 11% /`；Python `3.11.13`；DuckDB `1.2.0`；Pydantic `2.9.2`；FastAPI `0.115.0`。测试 source、结果、DB、WAL 均位于同一 ext4 临时工作区。依赖 freeze：`annotated-types==0.8.0`、`anyio==4.14.2`、`duckdb==1.2.0`、`fastapi==0.115.0`、`idna==3.18`、`pydantic==2.9.2`、`pydantic_core==2.23.4`、`starlette==0.38.6`、`typing_extensions==4.16.0`。

工作负载：320 个 route credit acquire，4 个独立 broker。`lease_size=1` 是旧每 PDU durable I/O 形状的可控对照（不把旧 1.15 因子重新带回）；生产值为 8。等待分位是每次 acquire 的实际 coroutine 等待时间。

| 目标 TPS | 方案 | elapsed | 吞吐 | durable/file refill | wait P50/P95/P99 |
|---:|---|---:|---:|---:|---:|
| 200 | lease=1 对照 | 1.675633 s | 190.973/s | 320 | 0.862954/1.580502/1.643962 s |
| 200 | lease=8 当前 | 1.599957 s | 200.005/s | 40 | 0.794719/1.512053/1.575613 s |
| 400 | lease=1 对照 | 1.110755 s | 288.092/s | 320 | 0.533135/1.037632/1.086538 s |
| 400 | lease=8 当前 | 0.802750 s | 398.630/s | 40 | 0.396180/0.754096/0.786030 s |
| 800 | lease=1 对照 | 1.452959 s | 220.240/s | 320 | 0.602259/1.359991/1.427593 s |
| 800 | lease=8 当前 | 0.404752 s | 790.607/s | 40 | 0.194159/0.372966/0.388937 s |

结果：在 200/400/800 三个目标速率上，file lock/JSON/fsync 次数均 `320 → 40`，下降 `87.5%`；lease=8 分别达到约 200/399/791 acquire/s。对照数据受每 PDU 文件同步成本限制，不能作为 SMSC 实际授权能力；正式证据 JSON 为 `credit-ab-{200,400,800}-{1,8}.json`。

同一 200 TPS 资源计时（shell `time -p`）：lease=1 为 `real 1.77s / user 0.46s / sys 0.39s`，lease=8 为 `real 1.80s / user 0.21s / sys 0.10s`；当前环境没有 `/usr/bin/time -v`，因此不虚构 RSS 数值，资源结论仅使用 user/sys 对照。

正式回环 JSON 证据保留在 `/tmp/mc-vnext-config-perf-20261017b/evidence-final/`，最终源代码的主门禁文件为 `loopback-final-p10-v2.json`、`loopback-final-p100-v2.json`、`loopback-final-p1k-v2.json` 和 `loopback-final-p5500-v2.json`；绑定矩阵为 `loopback-final-a-8w1b-v2.json`、`loopback-final-b-8w4b-per-bind-v2.json`、`loopback-final-c-8w4b-route-shared-v2.json` 及 `loopback-final-d-window{1,2,50}-v2.json`。旧版对照和失败诊断目录均未删除。

## 正确性与回环门禁

正式门禁全部在原生 ext4 执行，使用本地 SMPP simulator，不连接真实 SMSC，也不把生产能力当作已验证。

| 门禁 | 配置 | 结果 |
|---|---|---|
| P10 | 4 Worker / pool 4 / route_shared / `tps=200` / window 50 | PASS；10/10 socket、10/10 response、pending=0、projection=10、max_active_binds=4；duration 1.180 s |
| P100 | 4 Worker / pool 4 / route_shared / `tps=200` / window 50 | PASS；100/100 socket、100/100 response、无重复/遗漏、pending=0、projection=100、max_active_binds=4；duration 4.144 s |
| P100 scope | 8 Worker / pool 4 / `tps=200` / `per_bind` / window 2 | PASS；有效总上限 `200×4=800 parts/s`，max_active_binds=4，100/100 response；duration 4.109 s |
| P1K | 4 Worker / pool 4 / route_shared / `tps=200` / window 50 | PASS；1000/1000 socket、1000/1000 response、projection=1000、pending=0、max_active_binds=4；duration 35.241 s |
| P5500 | 4 Worker / pool 4 / route_shared / `tps=200` / window 50 | PASS；5500/5500 socket、5500/5500 response、projection=5500、pending=0、duplicates=0、projection exitcode=0、max_active_binds=4；duration 196.967 s |

Worker/Bind/Window 矩阵（最终源代码）：8 Worker / pool 1 / `per_bind` / window 1 的 `max_active_binds=1`；8 Worker / pool 4 的 `per_bind` window 2、`route_shared` window 1/2/50 均为 `max_active_binds=4`，全部 100/100 response、projection=100、pending=0。模拟器同时记录了累计握手次数和最大活跃 Bind，门禁只使用后者，避免把 slot 释放后的复用误判为超配。

Windows 工作区的 4 Worker 回环诊断曾观察到所有 PDU 已写入但子进程在 Python 3.13/NTFS 退出路径未及时返回；该结果没有作为正式吞吐基线，也没有伪造 PASS。正式 acceptance 依据遵循仓库规则使用 WSL 原生 ext4；Windows 端全量单元回归仍通过。

## 回归结果

- `\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q`：`404 passed, 5 skipped, 4 warnings`。
- 重点 VNext/配置/RouteCredit 测试：`21 passed`（含 bind pool、scope、协议字段、投影批处理和敏感字段排除）。
- `frontend/npm run build`：PASS；仅有既有 Rolldown `#__PURE__` 注释和大 chunk warning。
- 没有真实 SMSC、生产网络、SLES12SP5 远端或真实 DLR 作为 PASS 依据。

## 测试服务器闭环（本轮新增）

本轮测试只在测试服务器执行；本机仅用于生成无运行数据的传输包、校验哈希和回传脱敏证据。服务器 Run：
`/home/paas/marketing-copilot-test/runs/vnext-config-perf-20261017c`。目标为 EulerOS 性能代理，不标记为原生 SLES12SP5 实测。

环境与边界：EulerOS 2.0 SP15、kernel `6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`、glibc `2.38`、64 vCPU、`/dev/sda2 ext4`、CPython `3.11.5`、DuckDB `1.2.0`；非 root `paas`，所有应用/DB/WAL/tmp/cache/HOME 均在唯一 Run 内，SMPP 仅连接 `127.0.0.1` 合成 simulator。运行约束为 CPU `0-3`、nice `+10`、ionice class 3、聚合 RSS 2 GiB、Run 磁盘预警 5 GiB。源码传输归档 `vnext-backend-source-20261017b-clean.tar.gz` SHA256：`4d0db245de2945280ba0308ccc4754e9e089f2b485027987c991cb868e604a28`。

### 服务器配置生效与审计

- `config-activation-v2` PASS：`smpp_configs` 迁移字段、`pool_size`、`window_size`、`bind_type`、`tps_scope`、DLR、priority、reconnect 均被 `_load_routes()` 读取；priority=10 的 route 2 被选为默认路由。
- effective status PASS：8 Worker 仍表示业务并发；route_shared `tps=200` 的有效总 TPS 为 200；per_bind `tps=150,pool=2` 的有效总 TPS 为 300；运行中变更被标记 `restart_required=true`，未伪装成热生效。
- `vnext_config_contract_check.py` PASS，`errors=[]`、`secrets_included=false`。服务器证据中的第一轮激活噪声也保留，但不计为最终证据；V2 已加 spawn 主入口保护并干净通过。

### 服务器 RouteCreditBroker A/B

服务器同样执行 320 acquires、4 broker、lease=1/8；实际等待分位为 acquire coroutine 等待时间：

| 目标 TPS | 方案 | elapsed | 吞吐 | durable/file refill | wait P50/P95/P99 |
|---:|---|---:|---:|---:|---:|
| 200 | lease=1 | 1.597855 s | 200.269/s | 320 | 0.795660/1.512895/1.576891 s |
| 200 | lease=8 | 1.598236 s | 200.221/s | 40 | 0.795244/1.512402/1.575584 s |
| 400 | lease=1 | 0.801542 s | 399.231/s | 320 | 0.397853/0.756049/0.788408 s |
| 400 | lease=8 | 0.801168 s | 399.417/s | 40 | 0.397362/0.755598/0.787206 s |
| 800 | lease=1 | 0.403802 s | 792.467/s | 320 | 0.199257/0.378384/0.393940 s |
| 800 | lease=8 | 0.401873 s | 796.272/s | 40 | 0.197747/0.376382/0.392705 s |

服务器结果确认每 PDU durable/file refill `320 → 40`，下降 `87.5%`；吞吐没有因 batching 下降，800 TPS 反而由 792.467/s 提升到 796.272/s。该结果仍是 credit 层对照，不代表真实 SMSC 授权吞吐。

### 服务器正确性与容量门禁

| 门禁 | 配置 | 结果 |
|---|---|---|
| P10 | 4 Worker / pool 4 / route_shared / TPS 200 / Window 50 | PASS；10/10 socket、response、projection，pending=0，max_active_binds=3，1.823 s guard |
| P100 | 同上 | PASS；100/100 socket、response、projection，pending=0，max_active_binds=4，1.524 s |
| P1K | 同上 | PASS；1000/1000 socket、response、projection，pending=0，max_active_binds=4，8.076 s |
| P5500 | 4 campaign `2500+2500+250+250`，同上 | PASS；5500/5500 socket、response、projection，pending=0，duplicates=0，max_active_binds=4，40.769 s |

服务器 8 Worker 矩阵全部 PASS：pool=1/per_bind/Window=1 的 max_active_binds=1；pool=4/per_bind/Window=2 的有效 TPS=800、max_active_binds=4；pool=4/route_shared 的 Window=1/2/50 均保持有效 TPS=200、max_active_binds=4。每项均 100/100 response、projection=100、pending=0、无重复/遗漏。

### 服务器回归与资源审计

- 完整 backend pytest V2：`407 passed, 2 skipped, 6 warnings`，112.48 s，guard PASS，峰值聚合 RSS `398,237,696 B`。
- 首轮 `405 passed, 2 skipped` 的两项失败是测试传输包遗漏 `deploy/vnext-runtime-canary-test-runner.sh` 导致的夹具路径 `StopIteration`；保留首轮日志，补齐当前脚本并重新执行后的 V2 才计入最终结果。
- P5500 guard 峰值聚合 RSS `266,027,008 B`；所有 loopback/credit/matrix guard 均 PASS。Run 结束总占用 `4,317,164,944 B`，低于 5 GiB 预警；各 guard 的 `disk delta=0` 只代表其 guard 子目录，整 Run 使用量以 `post-run-audit.txt` 为准。
- 结束审计未发现属于本 Run 的残留进程；服务器既有监听仅做只读记录，未修改系统服务/路由/防火墙。服务器证据归档已回传到 `.tmp/vnext-config-server-evidence-20261017c/`，归档 SHA256：`3e5dc5dfd9f9fd16cf527cbd5cc7af930654b87df20739def0f5a48728325321`，归档不含 DB/WAL/Spool/Journal/venv。

服务器本轮状态：`EULEROS_VNEXT_CONFIG_LOOPBACK=PASS`、`EULEROS_VNEXT_P5500=PASS`、`EULEROS_VNEXT_BACKEND_REGRESSION=PASS`、`REAL_SMSC=NOT_RUN_BY_SCOPE`、`PRODUCTION_DEPLOYMENT=NOT_RUN`。已有 WSL 证据和历史失败证据均未覆盖或删除。

## 保留项与后续边界

1. 工作区原始 DuckDB 尚未直接迁移，避免改写用户数据；部署启动的 `init_db()` 会添加 `tps_scope` 并回填旧行。
2. 真实 SMSC 对 bind_type、TON/NPI、DLR 和 reconnect 的兼容性仍需在授权测试环境验证；本轮服务器只验证 loopback 协议边界。
3. 当前 `route_shared` 的 durable key 是 `route_id`；如果部署把同一 SMSC 账号拆成多个 route 配置，需在授权环境确认账号级共享配额，不能把多个 route 的 `tps` 自动相加。
4. P5500 的 ext4 JSON 对账结果已完成并通过；历史证据未被覆盖或迁移为新版本证据。旧基线和 RCA 仍见 [历史性能对照](Marketing_Copilot_VNext_Performance_Comparison_Report.md) 与 [旧热路径 RCA](SMS-Send-Hotpath-TPS-RCA.md)。
