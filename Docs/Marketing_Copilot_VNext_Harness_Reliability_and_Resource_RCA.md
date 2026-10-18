# Marketing Copilot VNext Harness Reliability and Resource RCA

任务：MC-VNEXT-SMPP-THROUGHPUT-HARNESS-CLOSURE-20261018  
基线：2c847b55abe4eddfa9dfafba34ce207fa0847313  
目标平台：SLES 12 SP5；正式吞吐结果均在授权测试服务器 native ext4 上取得。

## 结论

R1 通过。两个验收工具可靠性问题都已修复并有回归证据：

1. SLES 上的资源采样不再把采样失败伪装成 RSS=0；现在以 /proc 为主、psutil/定向 ps 为后备，并记录采样源、PID 树、原始 ps 诊断和错误状态。
2. Projection 完成判断不再只依赖可能滞后的 status 文件；现在同时核对 durable offsets、Journal EOF、最终 backlog 和 status 新鲜度，并显式区分 COMPLETED、STALE_STATUS、INCOMPLETE、SAMPLER_ERROR、TIMEOUT。

本轮没有修改生产默认 TPS、SMPP window、重试语义、Journal fsync、Projection 持久化或业务逻辑。route_credits、smpp_transport、worker 的改动仅增加计数器和快照观测，PRODUCT_CODE_CHANGE=NONE，PRODUCT_DEFAULT_CHANGED=NO。

## 1. RSS=0 根因与修复

### 根因

旧的 _process_tree_metrics 使用 ps -eo pid=,ppid=,pcpu=,rss=，在 SLES ps 输出格式不兼容或 PID 在采样窗口内退出时，将命令失败、解析失败和空结果统一返回为零。这会把“没有采到数据”误报为“进程 RSS 为零”，从而污染内存 profile 和资源 Gate。

本轮服务器证据还复现了 SLES ps 的实际差异：ps 返回码为 0，但 stdout 只有类似 “,ppid=,pcpu=,rss=” 的字段文本，没有可解析行。该情况现在被记录为 cross-check 诊断，而不是覆盖可靠的 /proc 结果。

### 修复

- 先按 root PID 和递归子进程构造去重后的进程树，避免把测试 runner 或其他无关进程算入 application-only / harness-inclusive。
- Linux 优先读取 /proc/PID/stat、statm、status；PID 消失只影响该 PID，不会让整次采样变成零。
- psutil 只对已选 PID 做定向交叉验证，避免全机进程枚举；ps 作为兼容性诊断后备，并保留 returncode、stdout、stderr。
- 采样结构增加 sampler_ok、sampler_error、sampled_pid_count、sampling_source、selected_pids。
- 采样无效时 RSS/CPU 返回 null，resource_gate 不能以无效采样取得 PASS；不会用 0 填充 NOT_CAPTURED。
- 每次诊断写入 resource-sampler-diagnostics.jsonl，便于审计采样源和 PID/PPID。

### 服务器证据

| 运行 | 采样源 | 采样状态 | application RSS peak | harness RSS peak |
|---|---|---:|---:|---:|
| 1K c0 | /proc | PASS | 130636 KiB | 372972 KiB |
| 5K c0，3 次 | /proc | 3/3 PASS | 165380–165488 KiB | 435504–440332 KiB |
| 50K c0 | /proc | PASS | 396024 KiB | 873116 KiB |
| 50K S2 | /proc | PASS | 395452 KiB | 851632 KiB |
| r6 最终 5K | /proc | PASS | 161944 KiB | 432392 KiB |

50K c0 的诊断样本包含 7 个 harness 进程、4 个 application worker 根进程，sampler_ok=true，status_rss_checked 超过 1200 次且 mismatch=0。ps 交叉检查的非解析输出被保留为诊断，不影响 /proc 主采样。

## 2. Projection stale-status 根因与修复

### 根因

旧 stop-condition 过度依赖 Projection status 的最近一次发布。在 status interval=1.0s、父进程停止与子进程最终发布存在时间差时，实际事件已经写入且 Projection 已经对账，但 harness 仍拿到旧 status，历史上表现为长时间 drain timeout。把 interval 缩短到 0.1s 只能掩盖问题，不能作为修复。

### 修复

完成判断按以下顺序形成证据链：

feed finished → spool pending=0 → SMPP response complete → Journal durable → Projection DB commit → durable offset 到达 Journal EOF → caught up。

实现同时读取 durable offsets、ProgressJournal storage_snapshot、Journal segment EOF 和 status mtime/age。最后一行不完整时不推进 EOF；status 过旧会进入 STALE_STATUS，而不会静默 PASS。结果增加 completion_state、completion_detection_lag_ms、status_age_ms 和 completion_evidence。

### 回归证据

5K、status interval=1.0s 连续三次：

| 运行 | wall | completion | detection lag | status age | 结果 |
|---|---:|---|---:|---:|---|
| c0-1s-1 | 37.722 s | COMPLETED | 880.095 ms | 6.234 ms | PASS |
| c0-1s-2 | 37.873 s | COMPLETED | 838.794 ms | 20.958 ms | PASS |
| c0-1s-3 | 37.580 s | COMPLETED | 898.700 ms | 28.620 ms | PASS |

1K c0 也以 COMPLETED、resource_gate=PASS 结束，completion_detection_lag_ms=923.182。新增单测覆盖 stale status + durable offset、Journal partial final line、缺失 durable offsets、sampler failure 不得变成零。

## 3. 观测范围扩展

验收结果现在同时保留：

- route：配置的 TPS scope、per-bind TPS、total ceiling、pool size、window size；
- credit：申请、grant、consume、remaining、wasted、refill 次数/成本、wait 次数/时间；
- bind/transport：slot、active time、socket/response 时间戳、window inflight peak、错误和 unknown；
- worker/producer：task taken、socket written、responses received、accepted、backpressure；
- loopback SMSC：总接收/响应、每 bind active time 与真实 PDU/s；
- writer：Journal durable、Projection commit、response callback、credit wait；
- resource：application-only 与 harness-inclusive 的 RSS/CPU/进程数和有效性。

r6 最终 5K 结果确认 throughput_observability.binds 非空、worker transport snapshot 非空；50K c0 的 loopback per_bind 记录了 97 个 bind 生命周期，max_active_binds=4。该观测用于定位 harness/loopback 限制，不被解释为生产 SMSC 容量。

## 4. 验证矩阵

| 层级 | 结果 |
|---|---|
| 新增 streaming acceptance 单测 | 8 passed |
| Windows backend/tests | 429 passed, 5 skipped, 4 warnings |
| Windows SMPP/worker/projection 相关套件 | 23 passed；一次 Windows 临时文件替换锁竞争复跑通过 |
| WSL AlmaLinux-8 native ext4 backend/tests | 432 passed, 2 skipped, 6 warnings |
| 服务器 1K | PASS |
| 服务器 5K c0 interval=1s | 3/3 PASS |
| 服务器 r6 最终 5K | PASS |
| 服务器 T6 | PASS |
| 服务器 T7-4..9 | 6/6 PASS |

WSL 正式回归目录为 /tmp/mc-vnext-smpp-wsl-current-20261018，df -T 显示 /dev/sdd ext4，Python 3.11.5；服务器为 paas-core01、SLES kernel 4.12.14-122.307-default、Python 3.11.4、DuckDB 1.2.0。

## 5. 限制与边界

- 服务器是共享的 80 vCPU / 375 GiB 主机，运行时负载和磁盘占用会影响数值；不是目标 8/16 GiB 硬件等价物。
- 服务器 / 根分区在环境采样时为 ext4、90% 已用；该事实进入容量决策，不被隐藏。
- r6 用短 5K 验证最终 transport 快照；50K 数值采用已完成的 r4/r5 运行证据，未因增加观测字段而重复浪费 50K。
- R4 600 秒运行因 S2/S3 未证明供给 headroom 而按门槛跳过，未生成任何 600 秒指标；缺失字段在机器证据中为 null。

