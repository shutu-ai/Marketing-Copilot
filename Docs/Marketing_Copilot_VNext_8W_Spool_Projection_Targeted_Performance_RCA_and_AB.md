# Marketing Copilot VNext：8W Spool / Projection Targeted Performance RCA

## 判定摘要

`FINAL_DECISION=NO_CHANGE_WITH_EVIDENCE`。R1 的可解释尾部在 loopback SMPP response/window completion；本轮没有证据证明 TaskSpool 或 Projection staging 是可安全推广的主 wall-clock 瓶颈，因此没有实施生产补丁，也没有做 50K、600s/800 PDU 或 Canary。

## Provenance 与冻结参数

- Task：`MC-VNEXT-STAGE-INDEPENDENCE-BACKPRESSURE-SAFE-PERF-20261019`
- `HEAD=origin/main=86b1bee4a5fcbc005f43b4382c83dfe0781db14d`
- server：SLES 12 SP5，Linux 4.12，native `ext4`（`df -T .`：`/dev/sda1 ext4`，80% 使用率）。
- Python：CPython 3.11.4；DuckDB 1.2.0。
- freeze：`duckdb==1.2.0`, `numpy==1.26.4`, `pandas==2.3.2`, `pyarrow==12.0.1`, `python-dateutil==2.9.0.post0`, `pytz==2026.5`, `six==1.17.0`, `tzdata==2026.5`。
- R1 flags：

  `--workers 8 --pool-size 4 --route-tps 200 --tps-scope per_bind --window-size 50 --counts 1250,1250,1250,1250 --batch-size 250 --projection-batch-size 512 --projection-staging-mode vectorized --projection-status-interval 0.1 --instrumentation on --profile-interval 1.0 --response-delay-ms 0 --resource-profile PROD_8G --rss-guard-scope application`

- 每轮另加测试专用 `--max-run-bytes 1073741824 --projection-drain-timeout 600`；没有改变生产默认阈值。
- R1 source tree：当前 tracked backend 190 个文件，远端逐文件比对 `mismatches=0`；harness SHA256 `6c559cf8c2809e6e5c23e19a0d12c5759f4678eeb27b585b74fbb7bc3257217b`。

## R1 结果

| Run | E2E wall | Feed active | Response drain | Projection drain | Reconciliation | Resource |
|---|---:|---:|---:|---:|---|---|
| `r1-1k-baseline` | 14.976 s | — | — | — | accepted/socket/response=1000/1000/1000；Journal=2000；Projection=2000 | PASS |
| `r1-5k-baseline-a` | 45.857 s（109.035 PDU/s） | 29.534 s（169.296 PDU/s） | 12.903 s | 12.907 s | accepted=socket=loopback response=5000；Journal durable=10000；Projection applied/unique=10000；unknown/write_failed/duplicate=0 | PASS |

R1 5K 还验证了 8/8 worker result、8/8 clean exit、`max_active_binds=4`、`resource_gate=PASS`，application RSS peak 308,976 KiB，harness RSS peak 587,152 KiB。RSS slope 只作为短跑观察，不作为单轮结论。

Run result SHA256：

- `r1-1k-baseline/result.json`：`9185b836e1f60070815c12d2a5f85db6d8e039e0710ff7a757deb167dccda5a2`
- `r1-5k-baseline-a/result.json`：`bf059f7339eafd5c3d7d17f6c46d755125e2b10273568ba33e29487b9ca84a9b`

## Critical-path RCA

| 路径 | 观测值 | 判断 |
|---|---:|---|
| Feed active | 29.534 s | Producer 到受控 feeder stop 的 wall 区间。 |
| SMPP response/window | `response_window_wait` 累计 3286.718 s，p95 2371.399 ms；`smpp_response_latency` 累计 3652.242 s；feed stop 后 response drain 12.903 s | 这些 wait 是 8 workers 的并发累计，不能相加当 wall；时间线显示它是 feed 后尾部的主要同步边界。 |
| Journal durable | 10,000 events；durable rate p50 183.621、p95 555.925 events/s | Journal 正常追平，没有形成积压。 |
| Projection | 217 vectorized batches，staging total 2.089 s，batch p50/p95 5.196/15.904 ms；backlog timeline 全程 0 bytes / 0 events；projection drain 12.907 s | 没有出现“单批约 2 s”的当前证据；总 staging 时间不是 E2E wall 的主导项。 |
| Status writes | 1700 次，累计 4.396 s，p50/p95 5.233/7.743 ms | 有可见诊断 I/O，但 `--projection-status-interval` 在当前构造器未接线，且没有 wall 因果证据；不以观测开销冒充产品关键路径收益。 |
| Spool / Worker subpath | 当前 harness 未记录 `_seed_buffers`、`iter_from`、`refresh`、`checkpoint` 的 bounded 调用/耗时分解 | Candidate A 的 critical-path share 未证实，不能安全改缓存、polling 或 cursor 语义。 |

因此：

`PRIMARY_BOTTLENECK=SMPP response/window completion tail (transport-side); outside the safe spool/projection candidate scope`。

`ROOT_CAUSE_CONFIDENCE=MEDIUM`：response/window 的时间线和 histogram 证据充分，但 worker 内部细分指标与 host CPU/iowait 交叉证据不足；没有把并发累计 wait 误加成 wall。

## R3 候选与变更预算

### Candidate A：TaskSpool / 8W 调度

- `root_cause_evidence`：缺少 worker subpath 分解；R1 没有证明重复 `iter_from`、segment CRC 或 cursor stat 占据 wall critical path。
- `critical_path_share`：`NOT_VERIFIED`。
- `source_lines`：`backend/vnext/worker.py:370-408`、`backend/vnext/spool.py:788-882`。
- `minimal_patch`：未创建。
- `revert_plan`：无 patch，无需回滚。
- 不触碰 pause/cancel eligibility、checkpoint-before-socket、PartitionMap 或 Journal schema。

### Candidate B：单 ProjectionWriter / DuckDB 尾部

- `root_cause_evidence`：vectorized staging `2.089 s / 217 batches`、p95 `15.904 ms`，Journal backlog 全程为零；当前没有重复 staging scan 或约 2s/批的证据。
- `critical_path_share`：`NOT_VERIFIED`；Projection 的 12.907s drain 与 response tail 同步，不能归因成 Projection 独占 wall。
- `source_lines`：`backend/vnext/projection_writer.py:899-1219`、`1220-1276`。
- `minimal_patch`：未创建。现有 status interval fallback 作为 `EXISTING_WIRING_GAP` 记录，不借性能任务做 API/架构重构。
- `revert_plan`：无 patch，无需回滚。
- 不改变 event IR、单 DuckDB writer、transaction/offset durability、去重、queue 上限或默认 batch=512/vectorized。

## R2/R4 与回归

R2 全部在同一 native ext4 隔离 run-root 完成：

- BP-1：`test_vnext_ingress_pressure.py`、`test_vnext_spool.py` 覆盖 high/low-water、async wait/wake、hard capacity、CRC/recovery。
- BP-2：`bp2-writer-pause-evidence/result.json`：blocked=true、applied=20、queue=0、duplicates=0；T7-6/T7-7 通过。
- BP-3：`bp3-two-campaign-2/manual-two-campaign.json`、`bp3-cancel-evidence/cancel.json`；暂停项无非法 write，其他 Campaign 推进，resume/cancel 后 pending=0。
- BP-4：`bp4-restart-evidence/result.json`：crash 5、recovered 15、unique 20、pending 0；T7-4～T7-9 全部 PASS。
- BP-5：R1 证明 `max_active_binds=4`，flags 固定 per-bind 200/window 50；运行后无匹配的 acceptance/fault 进程残留。

R2 machine evidence SHA256：

- writer pause：`37d29eac4e17e09f6fdea837f016021ee8ef8b748887061d1509b35f6e4206d0`
- same-day pause/resume：`e174601515eaa316db35fc18bbf47aed3b6255f68932300175ed79fa257b2acf`
- cancel：`2952dd125e527f1c96fee75f15f5303a9458b5018b8b595b2a28c146aa9c4763`
- restart：`dc2af86da8594df75c0e2d4cbd9f73918598df644bfe0d781bb0ab95be2211ba`
- T7 matrix：`d79cac0fcaa2b1cb3a7310aa0c95f2597d27fa64fe94ef2350e0d2211037d952`
- daily rollover/restart：`04dc386127ff6a4d7be2b2a09fc76b1c1d20ee68a78e50b87032b947a2187f26`

完整后端回归：`447 passed, 5 skipped, 4 warnings`。本轮 targeted stage/backpressure/recovery 集合：`22 passed`。由于没有候选 patch，不执行候选 A/B，也不做 50K；因此没有 `PAIRED_E2E_DELTA_PERCENT` 或 `PROJECTION_DRAIN_DELTA`。

## 交付门禁

- `PRODUCT_CODE_CHANGED=NO`
- `50K=NOT_RUN_BY_GATE`
- `600S_800PDU=NOT_RUN`
- `CANARY_READY=NO`
- `COMMIT_PUSH=NOT_REQUESTED`
