# Marketing Copilot VNext Server-First Performance and Platform RCA

任务：`MC-VNEXT-SERVER-FIRST-PERFORMANCE-MICROBATCH-20261018`

结论：`UPSTREAM_OR_HARNESS_LIMITED`。在授权的 SLES 12 SP5 服务器上，当前单 Writer、单 DuckDB、A0、vectorized、projection batch `512` 的 50K E2E 已完整对账；Writer 确有 underfill，但不是 E2E 主瓶颈。主要等待来自 per-bind TPS credit 和 SMPP response window。没有满足 R2 微批合并的触发条件，因此没有修改生产默认配置、没有引入 microbatch 代码，也没有执行无依据的候选 paired 50K。

## Final card

| Field | Closure value |
|---|---|
| `BASELINE_HEAD` | `450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e`，与 `origin/main` 一致 |
| `TARGET_DEPLOYMENT_OS` | SLES 12 SP5；与现有 standalone Python 部署文档一致。未将本次结果表述为 EulerOS 结果 |
| `TEST_SERVER_HOST_OS_KERNEL` | `SUSE Linux Enterprise Server 12 SP5`；`4.12.14-122.307-default`；x86_64；root filesystem `ext4` |
| `SERVER_REPRESENTATIVE` | `PARTIAL`：OS 与部署目标匹配；80 CPU/约 375 GiB 内存的共享主机不是 8/16 GiB 生产硬件等价物，且运行时 host load 约 `9–13` |
| `SERVER_AUTHORIZED` | `YES`：使用用户提供的测试服务器信息并通过 VPN 访问；报告不记录凭据 |
| `RESOURCE_ENVELOPE_CPU_MEM_IO` | Intel Xeon Gold 6230N，80 logical CPUs；375 GiB RAM；8 GiB swap；`/dev/sda1 ext4`，约 20.46M KiB 总量、84% 已用；cgroup 未发现 CPU/内存硬配额；应用侧 `PROD_8G` RSS guard 为 6 GiB |
| `SOURCE_AND_DEPENDENCY_FREEZE` | source HEAD `450e84d8`；上传 source archive SHA-256 `53b6f61f7c004e1b430cbcf9b97de3243ddceda1328d2ae95221da2a7eacac6d`；site-packages archive SHA-256 `0c464ddce0fee4c1ddde2452bdd2b2f4ff9ead76adf33b4ae4b3d0264fcca303`；CPython `3.11.4`；DuckDB `1.2.0`；freeze hash `502422a5a1be6e2cea2e0ae206c01940b3104c6d906545d8219cdbda72a04227` |
| `SERVER_C0_50K_WALL` | `94.671 s`；50,000 tasks/PDU/response，100,000 Journal events，100,000 Projection events |
| `SERVER_C0_PDU_PER_SEC` | wall average `528.2 PDU/s`；producer phase `687.2 PDU/s`；sampled socket PDU rate p50 `622.852/s`、p95 `873.661/s` |
| `SERVER_C0_PROJECTION_EVENTS_PER_SEC` | wall average约 `1,056.3 events/s`；feed-phase约 `1,374.4 events/s`；sampled committed-event rate p50 `1,233.641/s`、p95 `1,533.124/s` |
| `SERVER_C0_BATCH_P50_P95` | actual Writer commit batch `58 / 188`；configured batch `512`；1,397 commits for 100,000 events |
| `PRODUCER_LIMITED` | `NO`：producer backpressure waits `0`；但是 worker 被 TPS credit wait 约束 |
| `SMPP_WINDOW_LIMITED` | `YES`：TPS credit wait p50/p95 `462.037 / 636.319 ms`；response-window wait p50/p95 `126.006 / 334.362 ms` |
| `WRITER_UNDERFILL_IS_BOTTLENECK` | `NO`：underfill 可见，但 Writer staging p50/p95 `8.132 / 16.304 ms`，最后 response Journal durable 到 DB commit 仅 `46.584 ms` |
| `WINNING_ARM` | `NONE`；R2 没有候选 arm |
| `PAIRED_50K_MEDIAN_E2E_DELTA` | `NOT_RUN`：R2 触发条件未满足 |
| `PROJECTION_POST_RESPONSE_TAIL_DELTA` | last response Journal durable → DB commit `46.584 ms`；→ offset durable `58.512 ms` |
| `COMMIT_COUNT_AND_BATCH_DELTA` | baseline `1,397` commits，batch p50/p95 `58/188`；candidate delta `N/A` |
| `JOURNAL_BACKLOG_END_AND_TREND` | end `0` bytes/events，projection lag `0`；50K 运行无增长 backlog |
| `SMPP_AND_RESPONSE_RECONCILIATION` | accepted/socket-written/submit_sm_resp/response 均 `50,000/50,000`；unknown `0`；write_failed `0` |
| `UNKNOWN_OR_DUPLICATE_PDU` | E2E unknown `0`；纯 Writer 100K/500K 的确定性 replay 分别 `9,998/166,666`，均正确去重 |
| `OFFSET_AND_CRASH_RECOVERY` | T6 PASS；T7 当前矩阵 `6/6 PASS` |
| `T6_CROSS_DAY` | PASS：old/new 各 8；cutoff 后 old-day PDU `0`；restart redispatch `0`；stats contamination `0`；pending `0` |
| `T7_CURRENT_6_OF_6` | PASS：`T7-4`、`T7-5`、`T7-6`、`T7-7`、`T7-8`、`T7-9`；当前 source 没有 T7-1..3，未虚构结果 |
| `APP_RSS_PEAK_AND_SLOPE` | E2E harness RSS sample 在 SLES 上为 `0`，判定 `NOT_CAPTURED`，不当作零内存；纯 Writer 自测峰值 100K/500K 为 `171,716/207,628 KiB`，斜率未捕获 |
| `HARDWARE_EQUIVALENCE` | `NO`：测试机资源明显大于目标 8/16 GiB profile，且共享 host load 非静默 |
| `SUSTAINED_600S_800_PDU` | `NOT_RUN`：当前 SMPP/window 限制和硬件非等价，不满足 R4 前置条件 |
| `PRODUCTION_DEFAULT_CHANGED` | `NO`：保持单 ProjectionWriter、单 DB owner、A0、vectorized、batch `512`、逐事件 durability、dedup、at-most-once、cross-day 语义 |
| `FINAL_DECISION` | `UPSTREAM_OR_HARNESS_LIMITED` |
| `CANARY_READY` | `NO-GO`：没有 Writer candidate，且缺少硬件等价与 R4 600s/800 PDU 证据 |
| `COMMIT_PUSH` | `NOT_REQUESTED`；本轮未 commit/push |

## 1. 环境与运行边界

- 测试根目录为服务器 native Linux filesystem 下的 `/home/paas/marketing-copilot-test/server-first-20261018-01`，source、venv、Journal、DuckDB、logs 和结果均在同一 `ext4` 文件系统。
- 使用 CPython `/opt/gde/python/python-3.11.4/bin/python3.11`，`LD_LIBRARY_PATH=/opt/gde/python/python-3.11.4/lib`。依赖来自已冻结的离线 site-packages archive，没有在服务器修改系统 Python、安装新系统包或修改全局配置。
- 本次 workload 为 loopback/测试 harness，不是真实 SMSC、真实号码、生产数据库或生产部署。服务器是用户授权的测试环境，不代表 EulerOS 或生产硬件等价验证。
- 运行时 host load 较高，且 `/` 已使用约 84%；因此数值是受控测试目录内的实测证据，不是安静主机容量承诺。

## 2. E2E correctness 与 stop-condition 观察

| Run | 结果 | 关键结果 |
|---|---|---|
| 1K C0 | PASS，`4.486 s` | 1,000 accepted/socket/PDU/response；2,000 Journal/Projection；unknown `0`；backlog `0` |
| 5K C0，默认 status interval `1.0 s` | harness `FAIL`，`308.136 s` | 最终 raw JSON 已有 5,000 response、10,000 Journal/Projection、backlog `0`、lag `0`；仅 `projection_caught_up_before_stop=false`，表现为 status publication 未及时满足 parent stop condition |
| 5K C0，status interval `0.1 s` | PASS，`12.657 s` | 与上行相同 workload 完整对账；5,000 response、10,000 Journal/Projection、backlog `0`，caught-before-stop `true` |
| 50K C0，status interval `0.1 s` | PASS，`94.671 s` | 完整对账，unknown `0`，backlog `0`，lag `0` |

5K 的第一次 timeout 被保留为 harness 观察，不被当作数据正确性失败；后续正式 50K 使用显式 `--projection-status-interval 0.1`，避免把过期 status 文件误判为 projection 未追上。

## 3. Bottleneck evidence

50K C0 的关键累计计时如下：

- `tps_credit_wait`：50,000 次，总计约 `19,002.141 s`（跨 worker 聚合），p50/p95/p99 `462.037/636.319/639.370 ms`。
- `response_window_wait`：50,000 次，总计约 `4,627.279 s`，p50/p95/p99 `126.006/334.362/409.084 ms`。
- Journal fsync p50/p95/p99 `0.306/0.456/0.594 ms`；`journal_fsync_total` 约 `30.129 s`。
- Vectorized staging p50/p95 `8.132/16.304 ms`，其中 bind execute p50/p95 `6.531/13.547 ms`；staging total 约 `13.151 s`。
- producer backpressure waits 为 `0`；Journal append 约 `59.069 s`，segment scan 约 `18.958 s`，但末端 response→Projection tail 仅几十毫秒。

因此不能把实际 batch p50 `58` 直接推导成 Writer 阻塞，更不能在没有 paired E2E 证据时引入等待合并。当前首要动作应是拆分/校准 TPS credit、response window、SMPP loopback/harness 的供给能力和 stop-condition 观测，而不是改 Writer 默认行为。

## 4. 证据文件

原始 JSON 保存在本地未跟踪的 `.tmp/server-first-20261018/`，不属于提交产物。SHA-256：

| Evidence | SHA-256 |
|---|---|
| `1k-c0.json` | `F0F481528B7A1BE90A4556508FCB1AFBC68A799D171D716277329CF2E1687DBC` |
| `5k-c0.json` | `2FBDACDA506AE8429A832656295708A35F40EED31A7F6449B230E1351E04EB7F` |
| `5k-c0-status01.json` | `33E62CE3EC5BE7B71A5923C7992910F44E684E70BF6FAABE9C051111119ACE7B` |
| `50k-c0.json` | `ECB304B80FE91E5B55AFBBC31AA802271A291875938FA0AFFC2D8BB79678BE55` |
| `writer-100k-c0.json` | `5F93CF035E15D464B1052442AC9A6F3DAF6BA0050B99907ACA976622FE1D93FF` |
| `writer-500k-c0.json` | `55ACC3B974FF35613B484052D0156FCF0A294D51BF324308847D82691FBD2982` |
| `t6-daily-result.json` | `4BFD594A7C724A5D479532772975CE3AC0DB74ABCE14E96F94B3FFDC5C43C7DF` |
| `t7.json` | `304489ACBC58D3D70C9D495D1F26FBD8A964A47E008301EB5530B2764A70D12E` |

