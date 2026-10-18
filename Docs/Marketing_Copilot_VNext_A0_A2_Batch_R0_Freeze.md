# Marketing Copilot VNext A0/A2 Batch R0 Freeze

日期：2026-10-18
任务：`Marketing_Copilot_VNext_A0_A2_Batch_Sizing_EulerOS_E2E_Closure_Codex_Task.md`

## R0 结论

`R0_SOURCE_ENV=PASS`，但 EulerOS 目标环境本身未得到验证：提供的测试服务器实际是 **SLES 12 SP5**，不是 EulerOS。因此本报告冻结的是可审计的 WSL native-ext4 基线、当前 SLES 对照证据和未完成的 EulerOS 环境门禁；不把 SLES 结果冒充 EulerOS 结果。

## 源码与工作树冻结

| 项目 | 冻结值 |
|---|---|
| `HEAD` | `cd8e63537d264a623d347735f38c946caada4f75` |
| `origin/main` | `cd8e63537d264a623d347735f38c946caada4f75` |
| benchmark source archive SHA-256 | `a22a2ea692a0c50d30f7f2b3aeec035daa0c580ee3d652f0a2305b288fe0cd91` |
| benchmark seed | `20261018` |
| campaigns | `4` |
| events per SMS/task | `2` |
| business days | `2` |
| source/config changes | `0` |
| commit/push | 未执行；用户未要求 |

工作树已有的用户修改（`AGENTS.md`、`frontend/package-lock.json`）及未跟踪任务材料均保留。运行证据放在本机临时目录，不写入部署包，不包含生产数据库、日志、上传内容或凭据。

## 不可改变的产品契约

- 单一 `ProjectionWriter`、单一 DB owner、Journal fsync、dedup、unknown 语义保持不变。
- 生产默认 ingest/projection batch 保持 **512**；没有将默认值切换到 1024。
- A2/Arrow 只作为实验 benchmark arm；本轮没有把 PyArrow 接入生产路径，也没有批准生产 wheelhouse 依赖。
- 所有测试均为 synthetic loopback；没有连接真实 SMSC、生产 API 或生产数据库。

## 环境冻结

| 维度 | WSL formal run | 提供的测试服务器 |
|---|---|---|
| OS | AlmaLinux-8 WSL2 | **SLES 12 SP5**（非 EulerOS） |
| kernel | `6.18.33.2-microsoft-standard-WSL2` | `4.12.14-122.307-default` |
| Python | CPython `3.11.13`, glibc `2.28` | CPython `3.11.4`, glibc `2.22` |
| CPU | 22 online；formal `taskset 0-3` | 80 online |
| memory | 约 15.1 GiB available at capture | 约 375.5 GiB total，约 182.2 GiB available |
| filesystem | native ext4 `/tmp`，`/dev/sdd` | ext4 `/home`，`/dev/sda1`，约 84% used |
| cgroup | 当前无法验证硬件配额 | CPU quota `-1`，memory effectively unlimited |
| formal envelope | `taskset -c 0-3`, `nice 10`；`ionice -c3` 被系统忽略 class data | 未用于正式 50K E2E |

WSL formal run root：`/tmp/mc-vnext-a0a2-20261018.kh8Hav`。该目录位于 native ext4；没有使用 `/mnt/<drive>` 作为定量基线。测试服务器上的网络访问通过 Windows VPN/SSH 完成；WSL 直连该服务器超时。

## 依赖冻结

本地 formal venv 与临时 SLES 对照使用同一组已安装版本：

```text
duckdb==1.2.0
numpy==1.26.4
pandas==2.3.2
pyarrow==12.0.1
```

本地 benchmark venv 的完整 `pip freeze --local`：

```text
duckdb==1.2.0
numpy==1.26.4
pandas==2.3.2
pyarrow==12.0.1
python-dateutil==2.9.0.post0
pytz==2026.5
six==1.17.0
tzdata==2026.5
```

完整 freeze 文本（含末尾换行）SHA-256：`502422a5a1be6e2cea2e0ae206c01940b3104c6d906545d8219cdbda72a04227`。
上述四项版本清单 SHA-256：`fdf13e6dc972a619b42136d685b756f9575353932d8d9480c54f7ae1afcbc8ae`。

注意：服务器应用 wheelhouse 未提供 PyArrow；在线依赖下载因代理连接超时未完成。因此该依赖组合只能证明临时 benchmark 可运行，不能证明生产离线包或生产运行时兼容。

## 冻结的 benchmark arms

| Label | ingest/adapter | configured projection batch | 角色 |
|---|---|---:|---|
| C0 | A0 | 512 | 当前生产基线 |
| C1 | A0 | 1024 | 不改变 adapter 的 batch candidate |
| C2 | A2 | 512 | Arrow/向量化实验对照 |
| C3 | A2 | 1024 | Arrow + 1024 实验组合 |

所有 arm 使用同一 source、seed、campaign 数和校验逻辑；每个 workload 3 轮。运行结果均 `all_correct=true`，schema、digest、unique event 数和 aggregate 数一致。

## 已保存的原始证据

WSL native-ext4 raw JSON（未纳入部署包）：

| workload | 文件 | SHA-256 |
|---|---|---|
| 100K / 10% duplicate | `/tmp/mc-vnext-a0a2-20261018.kh8Hav/100k-10pct.json` | `fbb52c4c83f1ca49ee62e3e1803dfe06756035c906212ec3ad78b517f3d94ed9` |
| 500K / 30% duplicate | `/tmp/mc-vnext-a0a2-20261018.kh8Hav/500k-30pct.json` | `326de1fc26729fbe3f6b35f82b23828656cac9d6ff501368bc2ea06e8bc38460` |
| 500K / 10% duplicate | `/tmp/mc-vnext-a0a2-20261018.kh8Hav/500k-10pct.json` | `6cf056fede4120627641636387ec08ec5eeaa9fb4302c2b434e5ce15cee668a1` |
| 100K / 30% duplicate | `/tmp/mc-vnext-a0a2-20261018.kh8Hav/100k-30pct.json` | `072a9d307496d71a9b9ebbdb3f7adb6878d93097b5b96679e327c2d3e3f31514` |

测试服务器 SLES raw JSON 的原始 SHA-256：

- 11K one-round：`726f848af1f80f313b7c03981c7162e5c40ae2d9e21e2bfdf4f3486f7ac97304`
- 100K one-round：`3ebc718d8dcb7914bff792c92b83f63ba857f9dafe2fc47104a51aa62a7de8ae`
- 100K three-round：`24f66109993d50b9432a8b23eb8d8eefe8c4ac7321ffb1e117c5aa019af1b627`

## R0 后续边界

- WSL native-ext4 的 R1/R2、loopback integrity、T6/T7 和一次 50K C0/C1 pair 已执行。
- 因为实际服务器不是 EulerOS，`EULEROS_DISCREPANCY_CLOSED=NO`；不能签署 EulerOS 98% staging hotspot 的 RCA closure。
- 因为正式 50K pair 只有一组，且 C1 整体 wall 仅改善 12.36%、Projection drain 反而变慢，不能签署 R4 candidate benefit，也不执行 R5 600s/800-PDU 长跑。

\n