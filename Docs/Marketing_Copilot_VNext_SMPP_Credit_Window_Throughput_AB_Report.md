# Marketing Copilot VNext SMPP Credit / Window / Throughput A/B Report

任务：MC-VNEXT-SMPP-THROUGHPUT-HARNESS-CLOSURE-20261018  
测试性质：授权 synthetic loopback；S0 是 RATE_LIMIT_CONFORMANCE，S1/S2/S3 是测试专用 A/B，不是生产容量声明。

## 结论

S0 在 4 binds × 200 TPS、window=50 下没有超出配置 ceiling，50K 端到端对账通过，因此 RATE_LIMIT_CONFORMANCE=PASS。

S1 表明 window=100 对该 loopback 显著降低等待；S2 提高到 4 × 250 TPS 后没有得到接近 800 PDU/s 的持续供给；S3 的 window=100 恢复了短跑吞吐，但仍未证明 800 PDU/s headroom。当前主限制被归类为 WINDOW，伴随 credit wait、loopback/worker 调度和共享主机负载；不能据此修改生产限额。

## 1. 测试条件

- 服务器：paas-core01，SLES 12 SP5-compatible kernel，native ext4，Python 3.11.4，DuckDB 1.2.0。
- 每次 4 个 campaign，5K 使用 1250,1250,1250,1250；50K 使用既定相同 seed/source 与资源 envelope。
- Projection batch=512，vectorized staging，producer backpressure 和 response unknown 单独计数。
- S0/S1 使用 200 TPS per bind，配置总 ceiling=800。
- S2/S3 使用 250 TPS per bind，配置总 ceiling=1000，仅标记 TEST_ONLY_SYNTHETIC_HEADROOM。
- 所有运行使用 response loopback，不连接真实 SMSC、生产 API、生产数据库或真实号码。

## 2. 5K A/B

| 场景 | 配置 | wall | loopback receive/response | 实测 receive PDU/s | window wait P95 | credit wait P95 | 正确性 |
|---|---|---:|---:|---:|---:|---:|---|
| S0 c0-1s，三次 | 4×200，w50 | 37.580–37.873 s | 5000/5000 | 151.584–152.126 | 2300.922–2334.208 ms | 501.716–531.656 ms | 3/3 PASS |
| S1 | 4×200，w100 | 35.260 s | 5000/5000 | 171.719 | 773.422 ms | 500.697 ms | PASS |
| S2 | 4×250，w50 | 40.152 s | 5000/5000 | 143.265 | 2573.327 ms | 281.243 ms | PASS |
| S3 | 4×250，w100 | 50.144 s | 5000/5000 | 171.931 | 953.539 ms | 382.799 ms | PASS |

所有 5K 运行 producer backpressure_waits=0。S3 的某个最终 worker snapshot 只代表关闭时刻，整体 response_event_count 仍为 5000，不能用单个 worker snapshot 代替全局对账。

短跑结论：

- window 从 50 增到 100，在 S0 速率由约 152 提升到 171.719 PDU/s，window P95 从约 2.3 秒降到 0.773 秒。
- S2 的 250 TPS synthetic ceiling 在 window=50 下反而降到 143.265 PDU/s，说明 credit/window/调度等待主导，而不是简单提高配置就能得到 headroom。
- S3 的 window=100 恢复到约 171.931 PDU/s，但未达到 800，也未满足持续供给的 R4 前置条件。

## 3. 50K 成对运行

| 场景 | 配置总 ceiling | wall | socket / response | loopback PDU/s | window P95 | credit wait P95 | app RSS peak | harness RSS peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| S0 c0-r2 | 800 | 308.882 s | 50000 / 50000 | 169.112 | 2532.409 ms | 426.990 ms | 396024 KiB | 873116 KiB |
| S2 t250-w50 | 1000，synthetic | 311.610 s | 50000 / 50000 | 166.361 | 2574.787 ms | 293.637 ms | 395452 KiB | 851632 KiB |

S0 记录 credit wait_count=47615、wait_seconds=5660.168、refill cost=29.201s、wasted credits=48；S2 分别为 45252、2650.492s、29.940s、48。两组均 producer backpressure=0、loopback received=response=50000、Journal=100000、Projection=100000、unknown=0、write_failed=0，最终 backlog=0。

50K 数值明显低于历史基线中的 687.2 PDU/s；本轮主机同时是共享 80 CPU / 375 GiB 环境，loadavg 采样为 13.92/11.63/10.83，根分区 90% 已用。因此本轮结果只用于识别当前 harness/host 的限制，不外推为生产容量。

## 4. 瓶颈判定

### Window

window=50 的多秒 P95、window=100 的明显改善，直接支持 WINDOW 作为本轮 PRIMARY_BOTTLENECK。

### Credit

credit wait 次数和等待秒数很高，但 credit broker 的 lease/grant/consume 计数一致，未发现超额发射或重复 lease。credit wait 是合规限速和当前 harness 调度成本的观测，不是自动应修的 bug。

### Loopback / worker

loopback 收发一致，r6 短跑已经能保留 bind/transport snapshot；50K per_bind 记录 97 个 bind 生命周期且 max_active_binds=4。实际整体速率约 166–169 PDU/s，远离配置 ceiling，故当前只能判定为 HARNESS_LOOPBACK_LIMITED，不能判定生产 SMPP engine 只有该容量。

### Producer / Writer

producer 没有 backpressure wait，Writer/Projection 在收尾完成对账。50K 的 socket、response、Journal、Projection 全局计数一致，故没有证据支持修改 Writer microbatch 或生产 Projection 配置。

## 5. 决策

- RATE_LIMIT_CONFORMANCE：PASS。
- S1 window sensitivity：DEMONSTRATED。
- S2/S3 test-only synthetic headroom：NOT_VERIFIED。
- PRIMARY_BOTTLENECK：WINDOW；次要观测为 CREDIT_IMPLEMENTATION cost、LOOPBACK 和共享主机负载。
- PRODUCT_DEFAULT_CHANGED：NO。
- 不把 4×250 的 synthetic 参数写回生产，不绕过 credit broker，不把 configured ceiling 当作 measured throughput。

