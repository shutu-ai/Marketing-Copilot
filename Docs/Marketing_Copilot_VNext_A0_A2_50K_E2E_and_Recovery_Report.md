# Marketing Copilot VNext A0/A2 50K E2E and Recovery Report

日期：2026-10-18
运行平台：AlmaLinux-8 WSL2 native ext4
源码：`cd8e63537d264a623d347735f38c946caada4f75`

## 结论

400 single、80 multipart、T6 跨日边界和 T7-4～T7-9 recovery matrix 均通过；正式 50K C0/C1 pair 也完成了完整对账，accepted/socket/PDU/response、Journal、Projection、unknown 和 backlog 均一致且正确。

但本轮只有 **一组** 50K pair，不能替代任务要求的三轮 paired median。C1 的整体 wall 从 552.694s 降到 484.376s，改善 **12.36%**，低于 15% 目标；Projection drain 从 218.023s 增加到 236.828s（变慢 8.63%）。因此：

`T3_50K_PAIRED=PASS_INTEGRITY_ONLY`
`R4_E2E_INTEGRITY=PASS_WITH_T7_SCOPE_LIMIT`
`R4_E2E_BENEFIT=NOT_VERIFIED_NO_CHANGE`
`T4_600S_ACTUAL_PDU_RATE=NOT_VERIFIED`

## 50K formal envelope

- workload：四个 campaign，`20,000 + 20,000 + 5,000 + 5,000 = 50,000` tasks；single-part，每 task 2 events / 2 PDUs。
- fixed settings：8 workers、4 binds、route credits 200 per bind、window 50、batch-size 250、vectorized mode、instrumentation on。
- C0：production default projection batch 512；C1：existing configuration path projection batch 1024。
- formal process：CPU `0-3`、`nice 10`、application RSS guard 6 GiB、max pending 20,000、max run bytes 5 GiB、drain timeout 600s、RSS sample interval 1s。
- 这是 synthetic loopback；未连接真实 SMSC、生产 API 或生产数据库。

## 50K paired result

| metric | C0 A0/512 | C1 A0/1024 | C1 vs C0 |
|---|---:|---:|---:|
| overall duration | 552.694s | 484.376s | 12.36% faster |
| producer active | 324.979s | 236.712s | 27.16% faster |
| SMPP drain | 216.289s | 233.530s | 7.97% slower |
| Projection drain | 218.023s | 236.828s | 8.63% slower |
| accepted / taken / socket | 50,000 / 50,000 / 50,000 | 50,000 / 50,000 / 50,000 | exact |
| PDU / response | 50,000 / 50,000 | 50,000 / 50,000 | exact |
| Journal / Projection events | 100,000 / 100,000 | 100,000 / 100,000 | exact |
| unknown / replay / backlog | 0 / 0 / 0 | 0 / 0 / 0 | exact |
| max active binds | 4 | 4 | exact |

### Projection instrumentation

单位：秒；原始 JSON 的 nanosecond counters 除以 1e9。

| projection metric | C0 | C1 |
|---|---:|---:|
| staging_total | 59.771 | 52.844 |
| staging_bind_execute | 44.639 | 39.020 |
| staging_clear | 13.695 | 12.532 |
| staging_prepare | 0.794 | 0.718 |
| batch_end_to_end | 351.508 | 309.714 |
| aggregate | 87.943 | 73.140 |
| commit | 50.360 | 40.160 |
| dedupe | 127.790 | 120.684 |
| offset persist | 51.115 | 38.477 |

实际 streaming batch 明显 underfill：

| metric | C0 configured 512 | C1 configured 1024 |
|---|---:|---:|
| observed batch p50 | 24 | 24 |
| observed batch p95 | 68 | 66 |
| batches total | 3,454 | 3,465 |
| events read/new | 100,000 / 100,000 | 100,000 / 100,000 |

这说明把配置值从 512 改成 1024 并没有让该 E2E 的实际 commit batch 接近 1024；它不能被简单解释为“1024 在生产链路上稳定赢”。

## Resource guard

| metric | C0 | C1 |
|---|---:|---:|
| application RSS peak | 750,576 KiB | 732,988 KiB |
| harness RSS peak | 1,265,440 KiB | 1,264,948 KiB |
| application RSS guard | PASS (< 6,291,456 KiB) | PASS (< 6,291,456 KiB) |
| backlog/queue at finish | 0 / 0 | 0 / 0 |

RSS guard 通过只代表本次 synthetic run 没有超过进程级 6 GiB 上限；WSL cgroup 硬件等价资源不可验证，故 `PROD_8G_HARDWARE_EQUIVALENCE=NOT_VERIFIED`，`PROD_16G=NOT_VERIFIED`。

## T4 rate evidence（非 R5）

本次 50K 不是 600s steady-state test，不能给出 `800 PDU/s × 600s` 的 acceptance 结论。50K run 的 sample rate 分布仅作为诊断：

| rate | C0 p50 / p95 / max | C1 p50 / p95 / max |
|---|---:|---:|
| socket PDU/s | 96.774 / 193.393 / 435.484 | 111.452 / 194.105 / 369.668 |
| response/s | 98.208 / 209.486 / 510.753 | 113.199 / 190.722 / 470.874 |
| Journal events/s | 194.589 / 395.403 / 940.860 | 229.343 / 380.631 / 800.971 |
| Projection events/s | 194.129 / 361.274 / 587.379 | 232.195 / 351.272 / 759.615 |

这些是采样窗口的分布，不是 600s 持续吞吐；因此 T4/R5 保持 `NOT_VERIFIED`。

## Smaller E2E and recovery

| scenario | result | evidence |
|---|---|---|
| 400 single segment, C0 | PASS，9.579s；400 accepted/socket/PDU/response；Journal/Projection 800；unknown 0 | `e2e-c0-400.json` |
| 400 single segment, C1 | PASS，5.903s；同上 | `e2e-c1-400.json` |
| 80 multipart, C0 | PASS，2.271s；80 tasks，120 PDU/response；Journal/Projection 200；unknown 0 | `e2e-c0-80-multipart.json` |
| 80 multipart, C1 | PASS，2.145s；同上 | `e2e-c1-80-multipart.json` |
| T6 cross-day reclaim/restart | PASS；旧 day8 不再 dispatch，新 day8 正常接收，stats contamination 0 | `t6-native/daily-result.json` |
| T7-4～T7-9 | 6/6 PASS；T7-1～T7-3 在当前 source 中没有对应测试函数，整体 9/9 未签署 | `t7-native.json` |

T7 覆盖 pause/resume、route isolation、projection hint pressure、journal hard limit/GC、day reclaim、writer restart/cross-day no-redispatch。原始结果均保留在 formal run root，未复制进部署包。

当前版本的 `backend/tests/test_vnext_t7_recovery_matrix.py` 和 `backend/tools/vnext_t7_recovery_matrix.py` 都只定义/执行 T7-4～T7-9；仓库历史中也没有同 SHA 可运行的 T7-1～T7-3 实现。本报告只签署可执行的 6/6 局部 recovery gate，不把它扩大为 T7 9/9。

## Test-suite verification

- Windows repository standard command：`422 passed, 5 skipped, 4 warnings`，209.31s。
- native-ext4 copied source：`425 passed, 2 skipped, 6 warnings`，249.09s；使用修正后的临时 source assembly，未修改产品源码；其中 T7 测试覆盖当前实现的 6 个场景。
- frontend `npm run build`：成功；Vite 8.1.3，1,794 modules；仅有已有 dependency PURE annotation 和 chunk size warnings。

## Raw evidence

- C0 formal 50K：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/e2e-c0-50k-r1.json`，SHA-256 `ea9bd5aa6bab0efc0fb5551ac14dbe95e03ff532c974f0b3d2e7e6ec1d9e5340`。
- C1 formal 50K：`/tmp/mc-vnext-a0a2-20261018.kh8Hav/e2e-c1-50k-r1.json`，SHA-256 `0cbecca0d02879bf5a1a6af5407884f189437b711e5934d3998713b7f7fe09fb`。

\n