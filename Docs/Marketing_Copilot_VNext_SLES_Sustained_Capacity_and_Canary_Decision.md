# Marketing Copilot VNext SLES Sustained Capacity and Canary Decision

任务：MC-VNEXT-SMPP-THROUGHPUT-HARNESS-CLOSURE-20261018  
目标：在不修改生产默认值的前提下，判断是否具备 600 秒 / 800 PDU/s 持续验证和 Canary 前置条件。

## 最终判断

R4 600 秒运行没有执行。原因不是测试失败后隐瞒结果，而是正式门槛要求 S2/S3 先证明持续供给、Writer 跟得上且 backlog 无增长；本轮 S2/S3 的 synthetic loopback 实测只有约 143–172 PDU/s，50K S2 只有 166.361 PDU/s，远低于 800 目标。因此：

- 600 秒指标：NOT_RUN；所有 R4 数值字段为 null。
- 50K correctness：PASS。
- T6：PASS。
- T7-4..9：6/6 PASS。
- 最终决策：HARNESS_LOOPBACK_LIMITED。
- Canary：NO。
- 生产默认值：未改变。

这不是 ENGINE_800_CAPACITY_PASS，也不是生产 SMSC 容量结论。

## 1. 环境与等价性

| 项目 | 实际值 |
|---|---|
| 主机 | paas-core01 |
| OS / kernel | SLES 12 SP5-compatible；4.12.14-122.307-default |
| Python / DuckDB | 3.11.4 / 1.2.0 |
| 文件系统 | /dev/sda1 ext4 |
| CPU / 内存 | 80 logical CPU；375 GiB |
| 采样 loadavg | 13.92 / 11.63 / 10.83 |
| 根分区 | 90% 已用，约 2.0 GiB 可用 |
| 目标硬件等价性 | NOT_VERIFIED |

这是共享大主机，不是目标 8/16 GiB cgroup 或同等专用机器。不能把当前 RSS 低于 6 GiB 误报成 8/16 GiB 目标容量通过。磁盘接近高水位也是 R4 不应继续拉长运行的理由。

## 2. R4 门槛检查

| 门槛 | 证据 | 结论 |
|---|---|---|
| R1 harness reliability | /proc RSS 非零、1K PASS、5K status interval=1s 三次 PASS | PASS |
| S2/S3 供给 headroom | S2 5K 143.265；S3 5K 171.931；S2 50K 166.361 PDU/s | FAIL，未满足 |
| Writer 跟随与 backlog | 50K S0/S2 Journal=100000、Projection=100000、backlog=0 | PASS，但不能替代 headroom |
| 50K correctness | socket/response=50000/50000，unknown/write_failed=0 | PASS |
| 目标硬件、磁盘和共享负载 | 80 CPU/375 GiB 共享主机，90% 磁盘 | NOT_VERIFIED |

S2/S3 不是生产配置，使用 4×250 TPS 只是为了探测测试专用 headroom。它们没有达到接近 800 的持续供给，因此没有理由启动 600 秒长跑。

## 3. 50K 与恢复证据

50K S0 和 S2 均完成：

- accepted=50000；
- socket_written=50000；
- response_received=50000；
- Journal events=100000；
- Projection events=100000；
- unknown=0、write_failed=0；
- final backlog=0；
- application / harness RSS 均由有效 /proc 采样得到。

T6 跨业务日证据为 PASS：旧日 cutoff 后 socket=0、重启后旧日 redispatch=0、新日 socket=8、新日统计污染=0，旧日 8 条记录回收 3400 bytes，Projection 只保留新日 campaign。T7-4、T7-5、T7-6、T7-7、T7-8、T7-9 全部 PASS。

## 4. Canary 决策

当前不具备生产 Canary Ready 条件。原因是：

1. 当前 loopback/worker/credit/window 组合无法提供接近 800 的持续供给；
2. 测试主机与目标内存硬件不等价，且磁盘 90% 已用；
3. 运行对象是 synthetic loopback，不是真实 SMSC、真实网络或生产数据；
4. 没有完成 R4 600 秒稳定性、10–60 秒吞吐窗口、RSS slope 和持续 backlog 趋势证据。

最小后续动作是先用可独立限速、不会被单个 bind sleep/回调串行化的测试 SMSC simulator 复核 window/credit/worker 调度；在 S2/S3 能持续供给且 Writer 无积压后，再重新申请 600 秒 R4。此动作不授权修改生产默认 TPS 或 SMPP window。

## 5. 最终输出卡

SOURCE_HEAD=2c847b55abe4eddfa9dfafba34ce207fa0847313  
CHANGED_FILES=backend/tools/vnext_streaming_acceptance.py; backend/vnext/route_credits.py; backend/vnext/smpp_transport.py; backend/vnext/worker.py; backend/tests/test_vnext_streaming_acceptance.py; four Docs deliverables  
UNRELATED_WORKTREE_CHANGES_PRESERVED=YES  
TEST_SERVER_OS_AND_AUTHORIZATION=paas-core01 / SLES 12 SP5-compatible kernel / authorized synthetic-loopback server  
TARGET_OS=SLES12SP5  
R1_RSS_SAMPLER_ROOT_CAUSE=兼容性 ps 解析失败被旧实现静默归零；已改为 /proc 主采样并显式报告无效采样  
R1_RSS_SAMPLER_VALID=PASS  
R1_APPLICATION_RSS_PEAK_KIB=396024  
R1_HARNESS_RSS_PEAK_KIB=873116  
R1_STATUS_TIMEOUT_ROOT_CAUSE=status 发布滞后时旧 stop-condition 未核对 durable offset 与 Journal EOF  
R1_5K_STATUS_INTERVAL_1S_THREE_RUNS=PASS  
R1_HARNESS_RELIABILITY=PASS  
RATE_LIMIT_CONFORMANCE=PASS  
S0_CONFIGURED_TPS_PER_BIND=200  
S0_CONFIGURED_TOTAL_TPS_CEILING=800  
S0_MEASURED_SOCKET_PDU_PER_SEC=169.112  
S0_MEASURED_RESPONSE_PDU_PER_SEC=169.112  
S0_CREDIT_WAIT_P95_MS=426.990  
S0_WINDOW_WAIT_P95_MS=2532.409  
S0_BIND_UTILIZATION=null  
S1_WINDOW_SENSITIVITY=DEMONSTRATED；window100 5K 实测 171.719 PDU/s，window P95 773.422 ms  
S2_TEST_ONLY_TPS_HEADROOM=NOT_VERIFIED  
S3_TEST_ONLY_WINDOW_HEADROOM=NOT_VERIFIED  
PRIMARY_BOTTLENECK=WINDOW  
SERVER_50K_CORRECTNESS=PASS  
JOURNAL_PROJECTION_BACKLOG_END=0  
T6_CROSS_DAY=PASS  
T7_CURRENT_6_OF_6=PASS  
R4_600S_COMPLETED=NO  
R4_600S_ACTUAL_SOCKET_PDU=null  
R4_600S_AVG_SOCKET_PDU_PER_SEC=null  
R4_600S_MIN_60S_RATE=null  
R4_600S_RESPONSE_COUNT=null  
R4_600S_JOURNAL_EVENTS=null  
R4_600S_PROJECTION_EVENTS=null  
R4_600S_APPLICATION_RSS_PEAK=null  
R4_600S_BACKLOG_TREND=null  
TARGET_MEMORY_HARDWARE_EQUIVALENCE=NOT_VERIFIED  
PRODUCT_DEFAULT_CHANGED=NO  
FINAL_DECISION=HARNESS_LOOPBACK_LIMITED  
CANARY_READY=NO  
COMMIT_PUSH=NOT_REQUESTED

