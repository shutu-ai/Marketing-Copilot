# Marketing Copilot VNext A0/A2 Production Profile and Canary Decision

日期：2026-10-18
基线：`cd8e63537d264a623d347735f38c946caada4f75`

## 最终决定

`FINAL_DECISION=NO_CHANGE_WITH_EVIDENCE`
`CANARY_READY=NO`
`WINNING_PRODUCTION_ARM=NONE`
`DEFAULT_INGEST_MODE_CHANGED=NO`
`COMMIT_PUSH=NOT_REQUESTED`

保留当前生产默认值：**A0 + projection batch 512**。C1（A0 + 1024）仅作为已有配置路径的实验 candidate，不进入生产默认或 canary；C2/C3 的 Arrow 方案不进入生产。

## Gate matrix

| gate | result | evidence / reason |
|---|---|---|
| R0 source/environment freeze | PASS | source/seed/dependency/envelope/raw hashes 已冻结 |
| R1 EulerOS platform discrepancy | NOT_VERIFIED / BLOCKED_ENV | 提供服务器实际为 SLES12SP5，不是 EulerOS；历史 98% staging root cause 未建立 |
| R2 paired correctness | PASS | 100K/500K 四 workload、C0-C3 全部 digest/schema/unique/aggregate 正确 |
| R2 sustained microbenchmark benefit | PASS | C1 vs C0 四 workload median 改善 28.02%–35.13% |
| R3 product opt-in | PASS / EXPERIMENT_ONLY | C1 走现有 `projection_batch_size` 配置；默认仍 512 |
| R4 E2E integrity | PASS_WITH_T7_SCOPE_LIMIT | 400、80 multipart、T6、T7-4～T7-9、50K C0/C1 已对账；当前 source 无 T7-1～T7-3，整体 9/9 未签署 |
| R4 E2E benefit | NOT_VERIFIED / NO_CHANGE | 仅一组 50K pair；C1 overall wall 仅快 12.36%，Projection drain 慢 8.63%，实际 batch underfill |
| R5 600s / 800 PDU/s | NOT_APPLICABLE / BLOCKED | R4 benefit 未通过，且 EulerOS target 未验证；未浪费服务器长跑 |

## Profile decision

### PROD_8G

本次 synthetic 50K run 的进程级 RSS guard 为 6 GiB：C0 application peak 750,576 KiB，C1 732,988 KiB，均通过；queue/backlog 最终为 0。该结果只证明本次 workload 没有触发 guard，不等价于真实 8 GiB 主机 acceptance，因为 WSL cgroup/hardware quota 不可验证，测试服务器也不是目标 EulerOS。

`PROD_8G_RSS_GUARD=PASS`
`PROD_8G_HARDWARE_EQUIVALENCE=NOT_VERIFIED`

### PROD_16G

没有在授权的 16 GiB 目标主机上执行同 envelope acceptance。

`PROD_16G=NOT_VERIFIED`

## Arrow compatibility decision

本地 WSL 和临时 SLES benchmark 环境均可导入 PyArrow 12.0.1，但测试服务器应用 wheelhouse 不包含 PyArrow，在线下载又因代理超时。未完成 SLES/EulerOS 目标 runtime 的离线 wheelhouse、启动、升级、回滚和资源回归验证。

`ARROW_DEPENDENCY_COMPATIBILITY=NOT_VERIFIED_FOR_PRODUCTION`
`ARROW_PRODUCTION_INTEGRATION=NOT_APPROVED`

此外，C2（A2/512）四个 workload 都慢于 C1（A0/1024），C3 的额外收益只在 microbenchmark 中观察到；这不足以覆盖依赖、部署和回滚成本。

## Why no canary

要签署 canary，至少需要：授权的 EulerOS 目标、相同 source/config/dependency freeze、三轮 50K paired median、Projection drain 有明确改善、实际 batch 行为可解释、T7 完整 9/9（或补齐 T7-1～T7-3）、目标资源模型通过，以及 Arrow runtime 可离线复现。本轮缺少其中多个硬条件，因此停止在 R4，不执行生产配置切换、部署或提交。

## Recommended next closure action

在真正的 EulerOS 授权节点上重跑冻结的 R1/R2；若仍要评估 C1，再做至少三轮同 envelope 的 50K paired median。只有在整体 wall 和 Projection drain 达到任务要求的稳定改善后，才可重新审视 C1 opt-in；默认 512 在此之前保持不变。

## Final end card

```text
BASELINE_HEAD=cd8e63537d264a623d347735f38c946caada4f75
CANDIDATE_HEAD=NONE
SOURCE_DIFF=backend:none; docs:evidence-only; pre-existing user changes preserved
DEPLOY_ENV=SLES12SP5_NOT_EULEROS
WINNING_ARM=NONE
A0_512=100K/10%=11.755700s;500K/30%=70.789305s;500K/10%=83.177486s;100K/30%=10.686884s
A0_1024=100K/10%=8.461235s;500K/30%=46.760091s;500K/10%=53.961311s;100K/30%=7.392442s
A2_512=100K/10%=9.672660s;500K/30%=64.361253s;500K/10%=70.745041s;100K/30%=9.691521s
A2_1024=100K/10%=6.002288s;500K/30%=39.988938s;500K/10%=41.352095s;100K/30%=5.688915s
EULEROS_STAGING_98PCT_ROOT_CAUSE=NOT_ESTABLISHED
500K_30PCT_PAIRED=PASS_CORRECTNESS;C1_vs_C0=33.94%;C3_vs_C1=14.48%
ARROW_DEPENDENCY_COMPATIBILITY=NOT_VERIFIED_FOR_PRODUCTION
T3_50K_PAIRED=PASS_INTEGRITY_ONLY;one_pair_only;wall_improvement=12.36%
T6_T7_RECOVERY=PASS_WITH_T7_SCOPE_LIMIT;T6=PASS;T7-4..9=6/6;T7-1..3=NOT_AVAILABLE
T4_600S_ACTUAL_PDU_RATE=NOT_VERIFIED
PROJECTION_EVENTS_RATE=C0_p50/p95/max=194.129/361.274/587.379;C1=232.195/351.272/759.615
APP_RSS_PEAK=C0=750576KiB;C1=732988KiB
JOURNAL_BACKLOG_END_AND_SLOPE=end=0;backlog_slope=NOT_CAPTURED
PROD_8G_PROFILE=RSS_GUARD_PASS;HARDWARE_EQUIVALENT_NOT_VERIFIED
PROD_16G_PROFILE=NOT_VERIFIED
DEFAULT_INGEST_MODE_CHANGED=NO
CANARY_READY=NO
FINAL_DECISION=NO_CHANGE_WITH_EVIDENCE
COMMIT_PUSH=NOT_REQUESTED
```

\n