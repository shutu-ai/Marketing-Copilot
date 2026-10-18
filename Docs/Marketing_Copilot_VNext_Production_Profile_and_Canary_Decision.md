# Marketing Copilot VNext Production Profile and Canary Decision

任务：`MC-VNEXT-SERVER-FIRST-PERFORMANCE-MICROBATCH-20261018`

## Decision

最终决策：`NO_GO`，状态 `UPSTREAM_OR_HARNESS_LIMITED`。本轮服务器测试支持保持现有 production profile 和默认配置，不支持宣称 canary ready：

1. 没有证据证明 Writer underfill 是 E2E 主瓶颈，因此没有 bounded Journal microbatch candidate。
2. 50K 的主要等待是 per-bind TPS credit 和 SMPP response window；没有真实 SMSC/生产链路，不能将 loopback 结果外推为生产容量。
3. 测试服务器是 SLES 12 SP5，OS 与部署目标一致，但 80 CPU/375 GiB 内存、共享 host load 和无硬 cgroup quota 不等于 8/16 GiB 生产硬件。
4. E2E harness 在该 SLES 环境没有采到有效 RSS（返回 0），因此没有用它宣称 PROD_8G/PROD_16G 资源通过。
5. R4 的 600 s/800 PDU sustained gate 未执行：R3 没有产生 candidate，且 Producer/SMPP 前置供给证据不足。

## Profile card

| Profile / gate | Decision | Reason |
|---|---|---|
| `PROD_8G` | `NOT_VERIFIED_AS_HARDWARE` | 测试命令使用了 6 GiB application RSS guard；E2E resource sampler 在 SLES 上为 0，主机实际约 375 GiB，不能等同于 8 GiB cgroup/host |
| `PROD_16G` | `NOT_VERIFIED` | 没有 16 GiB 等价 cgroup/host |
| `LOW_MEM_2G` | `NOT_RUN` | 本轮没有 microbatch candidate，不扩展无关长跑 |
| `600s / 800 PDU` | `NOT_RUN` | R3 gain 与 Producer/SMPP feed 前置条件不成立 |
| `CANARY_READY` | `NO-GO` | 缺少 candidate paired gain、硬件等价、真实链路和 R4 evidence |
| default `projection_batch_size` | `512` unchanged | 50K correctness PASS；没有理由改默认值 |
| default durability | per-event unchanged | 保留逐事件 Journal fsync、offset durability、dedup、at-most-once |
| deployment target | SLES 12 SP5 | 与现有 deploy 文档一致；本轮不声称 EulerOS acceptance |

## Production-safe configuration decision

保持以下配置：

```text
one ProjectionWriter / one DuckDB owner
projection_staging_mode=vectorized
projection_batch_size=512
flush_interval=0
dedup=enabled
at-most-once=enabled
cross-day semantics=enabled
```

`projection-status-interval=0.1 s` 只作为本次测试 harness 的 status freshness 参数，不能解释成生产业务默认变更；它修复的是 5K stop-condition 观察滞后。

未来若要重新开启 R2，应先在服务器或等价环境把 TPS credit、response window、Producer feed 与 Writer staging 分离测量，再按 `0/10/25/50 ms` 顺序执行 bounded A/B；只有 winner 完成 3 组 paired 50K 并满足 correctness、tail、backlog、RSS、recovery 条件后，才可讨论 canary。

## Evidence and release hygiene

- source baseline：`450e84d82cca2b8ef5d4d34efe0f2ced604e3c2e`。
- server source/dependency freeze 和原始结果 hash 见 `Marketing_Copilot_VNext_Server_First_Performance_and_Platform_RCA.md` 及 `Marketing_Copilot_VNext_Server_First_Evidence_Summary.json`。
- 未使用真实 SMSC、真实号码、生产数据库、sudo 或全局配置；运行目录与原始 JSON 保持在 `.tmp/`/服务器隔离测试根目录，不进入部署 artifact。
- 本轮没有源代码改动，没有 commit/push；保留工作区已有的用户 dirty/untracked changes。

## Final card

| Field | Closure value |
|---|---|
| `FINAL_DECISION` | `UPSTREAM_OR_HARNESS_LIMITED` / canary `NO-GO` |
| `PRODUCTION_DEFAULT_CHANGED` | `NO` |
| `SUSTAINED_600S_800_PDU` | `NOT_RUN` |
| `HARDWARE_EQUIVALENCE` | `NO` |
| `APP_RSS_PEAK_AND_SLOPE` | E2E `NOT_CAPTURED`; pure Writer 100K/500K `171,716/207,628 KiB` |
| `COMMIT_PUSH` | `NOT_REQUESTED` |

