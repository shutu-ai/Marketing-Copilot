# Marketing Copilot VNext EulerOS vs WSL Staging Discrepancy RCA

日期：2026-10-18

## 结论

本轮没有建立 EulerOS 的 98% staging hotspot 根因，也没有关闭该 discrepancy。任务提供的服务器通过系统身份检查确认是 **SLES 12 SP5**，不是 EulerOS；因此它只能作为当前源码的 SLES 对照，不能作为 EulerOS acceptance evidence。

`EULEROS_STAGING_98PCT_ROOT_CAUSE=NOT_ESTABLISHED`
`EULEROS_DISCREPANCY_CLOSED=NO`
`DEPLOY_ENV=BLOCKED_ENV_NOT_EULEROS`

## 环境身份证据

| 项目 | WSL formal | 测试服务器 |
|---|---|---|
| OS | AlmaLinux-8 WSL2 | `SLES 12 SP5`, `VERSION_ID=12.5` |
| kernel | `6.18.33.2-microsoft-standard-WSL2` | `4.12.14-122.307-default` |
| Python/glibc | 3.11.13 / 2.28 | 3.11.4 / 2.22 |
| CPU | formal 4 cores (`0-3`) | 80 cores |
| memory | 约 16 GiB total | 约 375.5 GiB total |
| filesystem | native ext4 | ext4 `/dev/sda1`，约 84% used |
| cgroup | 配额不可验证 | CPU unlimited；memory effectively unlimited |

服务器 OS 身份与目标不一致是本轮最直接、可复核的差异。未做应用部署、生产 DB 连接或真实 SMSC 操作。

## 当前可比对照

使用同一 source archive、seed、四个 arm、DuckDB 1.2.0 / NumPy 1.26.4 / pandas 2.3.2 / PyArrow 12.0.1 的临时 benchmark 环境。WSL 使用 native ext4；服务器运行同一合成 workload，但服务器只做了一次 11K、一次 100K one-round 和一次 100K three-round。

100K / 10% duplicate 的 3-round wall 中位数：

| arm | WSL native ext4 | SLES 12 SP5 | 观察 |
|---|---:|---:|---|
| C0 A0/512 | 11.755700s | 11.418048s | 同一数量级 |
| C1 A0/1024 | 8.461235s | 9.194200s | 同一数量级 |
| C2 A2/512 | 9.672660s | 8.483322s | 同一数量级 |
| C3 A2/1024 | 6.002288s | 5.715149s | 同一数量级 |

四个 arm 在服务器均 `all_correct=true`；100K workload 的 unique event 为 90,002，aggregate 为 8。当前服务器没有复现历史报告中约 1005.957s staging / 1041.312s total 的数量级。

## 为什么不能把差异标记为已解决

历史 EulerOS 结果与本轮不是同一 acceptance envelope：历史结果来自不同的环境和运行上下文，且本轮服务器 OS 已被确认不是 EulerOS。即使当前 SLES 结果与 WSL 接近，也只能说明“当前 SLES 对照没有复现历史热点”，不能排除以下未验证因素：

- 真正 EulerOS 的 glibc、内核、磁盘/挂载和 Python runtime 差异；
- 历史 source/config/依赖与当前冻结 source 的差异；
- 历史运行时资源、后台进程、存储占用或进程调度差异；
- 历史 staging 统计口径或 workload envelope 与本轮不一致。

本轮已完成的排查包括：source archive SHA-256 固定、seed 固定、依赖版本固定、同一 arm 顺序和 correctness 对账、服务器 OS/Python/filesystem/cgroup 采集，以及服务器 raw JSON SHA-256 记录。仍缺少真正 EulerOS 上的同 envelope 重跑，故不能选择单一根因。

## 重新关闭所需证据

后续必须在授权的 EulerOS 机器上重新采集 `/etc/os-release`、kernel、Python/glibc、`df -T`、cgroup、依赖 freeze 和 source SHA；使用同一 seed、C0-C3、3 rounds、native filesystem，并保留逐阶段 staging/Projection 指标。只有在该对照能复现或排除历史热点后，才能把 `R1_PLATFORM_DIFFERENCE` 从 `NOT_VERIFIED_BLOCKED_ENV` 更新为明确的 RCA 结论。

本轮无源码修复，因为没有足够证据把问题归因到产品代码；也没有进行服务器部署或提交。

\n