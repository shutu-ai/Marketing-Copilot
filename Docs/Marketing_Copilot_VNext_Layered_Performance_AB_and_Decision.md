# VNext 分层吞吐 AB/BA 复跑与决策

任务：`MC-VNEXT-LAYERED-THROUGHPUT-ISOLATION-20261018`

## 1. 冻结 contract

本轮没有修改产品默认值：`route_tps=200`、`tps_scope=per_bind`、`pool_size=4`、`window_size=50`、总 ceiling=800 PDU/s、4 campaigns×1250、response delay=0、instrumentation=ON。L2/L3 的隔离参数是 `workers=1`、`pass_size=250`；该参数只用于边界诊断，不是生产默认值。

L4 使用现有完整 acceptance harness：workers=8、pool=4、batch=250、projection batch=512、vectorized staging、status interval=0.1、resource profile `PROD_8G`。

## 2. 5K L1/L2 边界 AB/BA 交叉复跑

任务要求在出现掉点后围绕相邻边界做至少 3 次 AB/BA 交叉复跑。这里 A=L1、B=L2，每次均为 5K，且每个 run 独立 ext4 root、顺序执行、不并发：

| 交叉组 | 执行顺序 | L1 stage / wall | L2 stage / wall | L1 bind | L2 bind | 两侧校验 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| AB-01 | A → B | 795.415 / 793.726 | 261.927 / 256.903 | 4 | 80 | PASS |
| BA-02 | B → A | 795.014 / 793.276 | 259.089 / 254.102 | 4 | 80 | PASS |
| AB-03 | A → B | 794.678 / 792.954 | 257.302 / 252.599 | 4 | 80 | PASS |

stage/wall 单位均为 PDU/s。三组的 L1 stage 范围为 794.678–795.415，L2 为 257.302–261.927；顺序互换没有改变边界。以三组平均值计，L1→L2 stage 下降约 67.25%，因此不是一次性的 host scheduling 偶然值。

每个交叉 run 都满足：accepted=5000、socket_written=5000、SMSC received/responses=5000、unknown=0、write_failed=0、duplicate=0、最终 active bind=0。

## 3. close/rebind 对照

默认 L2 5K bounded-pass：20 pass、80 binds，stage=260.860、wall=255.996。诊断控制保持 transport 到全部任务/response drain 后再关闭：4 binds，stage=258.431、wall=253.587。

| 对照 | bind count | pass-start→first-write proxy | close seconds | wall | stage |
| --- | ---: | ---: | ---: | ---: | ---: |
| L2 close per pass | 80 | 10.194855 s | 0.021701 s | 255.996 | 260.860 |
| L2 keep until drain | 4 | 0.341730 s | 0.001304 s | 253.587 | 258.431 |

保留 transport 明显减少 bind 次数和 proxy，但吞吐没有改善；E2E 变化为约 -0.941%，落在这台共享主机的复跑波动内。`pass-start→first-write` 不是纯 TCP bind 测量，包含 worker 调度和 credit wait；`close()` 本身仅约 21.7 ms。因此本任务不提交“只保持 transport”生产修复。

## 4. L2/L3 journal AB 对照

L2 与 L3 都是单 worker、20 个 bounded pass、80 bind：

- L2：stage=260.860，wall=255.996；无 durable journal，response sink 仅诊断内存。
- L3：stage=197.140，wall=194.221；10000 journal events、5000 response events 全部 durable，10000 fsync，sink count 与 journal count 一致。
- L2→L3 stage 下降 24.427%。L3 profiler 全量 counter 为 journal append 4.308081420 s、fsync 2.362700184 s；最近 4096 个 fsync 样本 P95=0.339 ms。

结论：journal 的逐事件 durable append/fsync 是可复现的第二个层间成本；不能通过绕过 durable journal 来“修复”吞吐。

## 5. L4 完整路径复核

L4 1K：stage=322.798，wall=40.061，duration=24.962 s；accepted/socket/response=1000，journal/projection=2000，unknown/write_failed=0。

L4 5K：stage=384.391，wall=85.953，duration=58.171 s；accepted/socket/response=5000，journal/projection=10000，unknown/write_failed=0，projection queue/backlog=0，offset drain 到 EOF，resource gate=PASS。完整路径尾部：response-journal drain=4.068 s，projection commit/offset drain=31.621/31.622 s。

`staging_bind_execute` 23 batches，per-process P95 max=2055.263 ms，P50=1999.232 ms。这是约 2 s staging anomaly，但统计是各进程 P95 的 max，不是 pooled/global P95；报告中不把它误写成全局 P95。

## 6. 决策

### 采用

`NO_CHANGE_WITH_EVIDENCE`

理由：

- layer boundary、AB/BA 方向和 journal 成本均可复现；
- keep-transport 对照未改善 stage/wall，不能支持一个单点 transport 生命周期补丁；
- 真实修复需要进一步拆解 worker/spool/credit 的调度组合，当前没有不改变语义且足够小的生产 patch；
- route TPS/window、projection batch/status 默认值均保持不变。

### 不采用

- 不提升 route TPS 或 window；
- 不绕过 journal/fsync；
- 不把 L4 的并行 active stage 与 L3 单 worker stage 做成直接“projection 优化收益”；
- 不执行机械 50K 或 600 s run；本次 5K + 3 组边界交叉已足够作出 no-change 决策。

最终字段：`PRIMARY_DROP_BOUNDARY=L1->L2`；`CAUSE_CONFIDENCE=HIGH_FOR_BOUNDARY_MEDIUM_FOR_SINGLE_SUBCOMPONENT`；`CANARY_READY=NO`；`PRODUCT_CODE_CHANGED=NO`；`APP_ROUTE_TPS_DEFAULT_CHANGED=NO`；`PROJECTION_DEFAULT_CHANGED=NO`；`50K_RUN=NOT_RUN_BY_SCOPE`；`600S_800PDU=NOT_RUN`。

## 7. 重现命令

服务器上使用 standalone CPython 3.11.5：

```sh
PYTHONPATH=/home/paas/mc-vnext-strict-ab-20261018/site-packages \
/home/paas/mc-vnext-strict-ab-20261018/runtime/python/bin/python3 \
/home/paas/mc-vnext-strict-ab-20261018/sources/source-b-3e73ab80/backend/tools/vnext_layered_throughput.py \
  --layer L2 --count 5000 --pass-size 250 \
  --source-root /home/paas/mc-vnext-strict-ab-20261018/sources/source-b-3e73ab80 \
  --run-root /home/paas/mc-vnext-strict-ab-20261018/runs/reproduce-l2-5k
```

保持 transport 的诊断控制只增加 `--keep-transports`。L4 使用服务器既有 `/home/paas/strict-ab-detached-run.sh`，counts=`1250,1250,1250,1250`。正式结果路径、SHA256 和机器字段见 [`Marketing_Copilot_VNext_Layered_Throughput_Evidence.json`](Marketing_Copilot_VNext_Layered_Throughput_Evidence.json)。
