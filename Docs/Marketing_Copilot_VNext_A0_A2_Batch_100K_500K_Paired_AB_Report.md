# Marketing Copilot VNext A0/A2 Batch 100K/500K Paired A/B Report

日期：2026-10-18
运行平台：AlmaLinux-8 WSL2 native ext4
源码：`cd8e63537d264a623d347735f38c946caada4f75`
seed：`20261018`；每个 workload 每个 arm 3 rounds

## 结论

R2 correctness 全部通过。C1（A0/1024）相对生产基线 C0（A0/512）在四个 workload 上的 wall median 改善为 **28.02%–35.13%**；C3（A2/1024）相对 C1 的额外改善为 **14.48%–29.06%**。但是 A2/512（C2）相对 A0/1024（C1）在四个 workload 上都更慢，且 PyArrow 不在服务器生产 wheelhouse 中。因此 C1 只能作为实验 opt-in candidate，不能据此批准生产默认值或 Arrow 产品集成。

## 冻结 arm 与 correctness

| arm | adapter | projection batch | 生产含义 |
|---|---|---:|---|
| C0 | A0 | 512 | 当前生产基线 |
| C1 | A0 | 1024 | 仅配置级实验 candidate |
| C2 | A2 | 512 | Arrow/向量化实验 |
| C3 | A2 | 1024 | Arrow + 1024 实验组合 |

四个 workload 的每个 arm 都满足：`all_correct=true`、digest 一致、schema 正确、aggregate=8；unique event 数如下：

| workload | duplicate rate | unique events |
|---|---:|---:|
| 100K | 10% | 90,002 |
| 500K | 30% | 333,334 |
| 500K | 10% | 450,002 |
| 100K | 30% | 66,668 |

## Paired wall median

单位：秒；括号内为相对前一比较基准的改善率。

| workload | C0 A0/512 | C1 A0/1024 | C2 A2/512 | C3 A2/1024 | C1 vs C0 | C2 vs C0 | C3 vs C0 | C3 vs C1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100K / 10% | 11.755700 | 8.461235 | 9.672660 | 6.002288 | 28.02% | 17.72% | 48.94% | 29.06% |
| 500K / 30% | 70.789305 | 46.760091 | 64.361253 | 39.988938 | 33.94% | 9.08% | 43.51% | 14.48% |
| 500K / 10% | 83.177486 | 53.961311 | 70.745041 | 41.352095 | 35.13% | 14.95% | 50.28% | 23.37% |
| 100K / 30% | 10.686884 | 7.392442 | 9.691521 | 5.688915 | 30.83% | 9.31% | 46.77% | 23.04% |

关键解释：C3 相对 C0 的大幅改善同时包含 batch 512→1024 和 A0→A2，不能把整列改善都归因于 Arrow。A2 的真实边际应看 C3 对 C1；该边际跨 workload 波动较大，且 C2 在 batch512 下没有胜过 C1。

## 运行约束与可复核证据

- formal 命令使用 `taskset -c 0-3`、`nice -n 10`，native ext4；`ionice -c 3` 请求被内核忽略 class data，已记录。
- 四个 workload 均使用同一 source/seed、4 campaigns、2 events per task、2 business days。
- raw JSON：
  - 100K/10%：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/100k-10pct.json`，SHA-256 `fbb52c4c83f1ca49ee62e3e1803dfe06756035c906212ec3ad78b517f3d94ed9`；
  - 500K/30%：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/500k-30pct.json`，SHA-256 `326de1fc26729fbe3f6b35f82b23828656cac9d6ff501368bc2ea06e8bc38460`；
  - 500K/10%：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/500k-10pct.json`，SHA-256 `6cf056fede4120627641636387ec08ec5eeaa9fb4302c2b434e5ce15cee668a1`；
  - 100K/30%：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/100k-30pct.json`，SHA-256 `072a9d307496d71a9b9ebbdb3f7adb6878d93097b5b96679e327c2d3e3f31514`。

## R2 判定

`R2_PAIRED_CORRECTNESS=PASS`
`R2_SUSTAINED_BENEFIT=PASS_MICROBENCHMARK`
`R3_PRODUCT_OPT_IN=PASS_EXPERIMENT_ONLY`

本轮没有产品代码 diff；C1 通过现有 `projection_batch_size` 配置路径验证，生产默认值仍为 512。C3/A2 不具备生产 wheelhouse、生产 runtime 和 EulerOS 兼容性证据，故 `ARROW_DEPENDENCY_COMPATIBILITY=NOT_VERIFIED_FOR_PRODUCTION`。

\n