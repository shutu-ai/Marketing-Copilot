# Marketing Copilot VNext Native SIGSEGV Evidence Index

## A0 结论

本索引只记录当前本地工作树和已回传服务器归档中实际存在的证据，不补造 core、C/C++ faulting frame、测试 stdout 或内核日志。

- 代码基线：本地 `HEAD`、本地 `origin/main`、GitHub `refs/heads/main` 均为 `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e`。
- 基线提交：`feat: replace SMS send path with VNext sharded pipeline`。
- 工作树：存在既有 dirty tracked/untracked 文件；本任务不执行 reset、clean、commit 或 push。
- 历史闭环状态：`VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV`；R7 已通过，历史 R2/P5500 两次在 EulerOS 运行时以 `SIGSEGV (-11)` 退出。本轮最终状态见 D7 receipt，为 `FAIL`。
- 真实 SMSC、公网号码、生产数据库和生产进程均未使用。

## 归档完整性

归档：`test-artifacts/vnext-runtime-canary-closure-20261016/stage-b/server/stage-b-r2-r7-transfer.tar.gz`

- SHA256：`ab2a131d642aa290cda80dabda94d5f3a7c4d469140a23aa7e1ace25cc6581bf`（`VERIFIED`）
- Tar 成员：16 个（15 个证据文件 + 内部 `transfer-sha256.txt`，`VERIFIED`）
- 内部 manifest：15/15 通过 SHA256（`VERIFIED`）
- 绝对路径、`..` 逃逸和符号链接/硬链接：0 个（`VERIFIED`）
- 独立诊断解包目录：`test-artifacts/vnext-native-sigsegv-rca-20261016/a0-archive/`
- 归档安全检查记录：`test-artifacts/vnext-native-sigsegv-rca-20261016/a0-archive-safety.json`

归档中实际存在的关键成员：

```text
r2-first-perf-guard.json
r2-first-resource-samples.jsonl
r2-retry-perf-guard.json
r2-retry-resource-samples.jsonl
r2-summary.json
r7-isolated-canary.json
r7-isolated-perf-guard.json
r7-isolated-resource-samples.jsonl
r7-summary.json
host-preflight.txt
python-freeze.txt
pip-check.txt
package-sha256.txt
process-audit.txt
root-size.txt
transfer-sha256.txt
```

## R2/P5500 执行证据

### R2-1：首次尝试

状态：`INCOMPLETE` 作为正式隔离验收，`VERIFIED` 作为原始失败记录。

- Run Root：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r2-2108/`
- guard：`FAIL`，reason `guarded command exited with -11`，exit code `-11`（`VERIFIED`）
- 持续时间：`35.406 s`（`VERIFIED`）
- 资源：峰值 aggregate RSS `817,946,624 B`，磁盘增量 `0 B`，CPU `[0,1]`，`nice +10`，`ionice -c3`（`VERIFIED`）
- 执行命令：

  ```text
  ionice -c 3 <run-root>/venv/bin/python <run-root>/test-package/marketing-copilot-vnext-runtime-canary-test-20261016-vnext-runtime-canary/backend/tools/vnext_runtime_canary_acceptance.py --run-root <run-root>/canary --output <run-root>/canary.json --gates r2 --authorize-p5500 --route-tps 80 --response-delay-ms 2 --workers 4 --partitions 64 --projection-queue 128 --max-inflight 32 --prefetch 16
  ```

- guard PID 37751，PPID 37535；记录的 Python descendants：49351、49800、49981、50189、50381，均由 guard root 37751 派生（`VERIFIED`）。
- `canary.json`：未生成（`VERIFIED`）。
- 该次 `HOME/TMPDIR/XDG_CACHE_HOME` 指向另一个 Run Root，不计入正式隔离定量结论（`VERIFIED`）。
- guard 记录 `all_recorded_pids_gone=false`；随后只按 PID、启动时间和命令行精确清理孤儿 descendants，最终审计无匹配进程（`VERIFIED`）。
- 原始 command stdout、完整进程日志、core、native frame：归档中不存在（分别 `INCOMPLETE`、`UNAVAILABLE`、`UNAVAILABLE`、`UNAVAILABLE`）。

### R2-2：隔离重试

状态：`BLOCKED_ENV`，原始 guard 失败记录 `VERIFIED`。

- Run Root：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r2-retry-2115/`
- guard：`FAIL`，reason `guarded command exited with -11`，exit code `-11`（`VERIFIED`）
- 持续时间：`34.726 s`（`VERIFIED`）
- 资源：峰值 aggregate RSS `887,840,768 B`，磁盘增量 `0 B`，CPU `[0,1,2,3]`，`nice +10`，`ionice -c3`（`VERIFIED`）
- 正式隔离环境：`HOME/TMPDIR/XDG_CACHE_HOME` 均归属该 Run Root（`VERIFIED`）。
- guard PID 26314，PPID 26107；记录的 Python descendants：34328、34495、34551、34641、34742，均由 guard root 26314 派生（`VERIFIED`）。其中 descendants 在 guard 结束时仍有记录，end state 为 `S`；后续精确清理并在归档 `process-audit.txt` 中确认无匹配进程（`VERIFIED`）。
- `canary.json`：未生成（`VERIFIED`）。
- 原始 command stdout、完整进程日志、core、native frame：归档中不存在（分别 `INCOMPLETE`、`UNAVAILABLE`、`UNAVAILABLE`、`UNAVAILABLE`）。

两次 R2 的共同可证事实是：真实 `main + FastAPI + Uvicorn + VNextRuntime` canary 命令由独立 standalone CPython 3.11.5 启动后，在约 35 秒内以 `-11` 退出；guard 没有触发 RSS、磁盘或超时保护。仅凭该归档尚不能证明 faulting native frame，也不能在 A0 阶段证明 `fork` 是根因。

## R7 正常对照

状态：`VERIFIED PASS`。

- Run Root：`/home/paas/marketing-copilot-test/runs/vnext-runtime-canary-20261016-r7-isolated-2130/`
- canary：`PASS`，`608.767 s`，发送 `4000` 条，loopback received/response success `4000/4000`，projection unique/taken/socket_written `4000/4000/4000`，`taken_without_socket_written=0`。
- canary 采样：539 个样本，峰值进程 RSS `220,020,736 B`，峰值 FD `90`，峰值 Run Root `9,866,961 B`，最大 Writer lag `0.0 s`，最大 pending `62`，Writer 全程存活。
- guard：`PASS`，`609.799 s`，exit code `0`，aggregate RSS 峰值 `900,640,768 B`，磁盘增量 `0 B`，CPU `[0,1,2,3]`，`nice +10`，`ionice -c3`，所有记录进程已退出。
- Loopback bind：仅 `127.0.0.1`；真实 SMSC 未使用。

上一轮 R7 因环境路径记录跨 Run Root 被排除；本条只采用全新目录且显式绑定 `HOME/TMPDIR/XDG_CACHE_HOME/PYTHONPYCACHEPREFIX` 的重跑结果。

## 主机与依赖环境

- OS：EulerOS `2.0 (SP15x86_64)`（`VERIFIED`）
- Kernel：`6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`（`VERIFIED`）
- Account：非 root `paas`（`VERIFIED`）
- Filesystem：`/dev/sda2 ext4`（`VERIFIED`）
- Python：standalone CPython `3.11.5`（`VERIFIED`）
- DuckDB：`1.2.0`（来自 freeze，`VERIFIED`）
- 关键 native/runtime packages：`numpy==2.2.6`、`pandas==2.3.2`、`cffi==2.1.1`、`cryptography==41.0.7`、`psutil` 未列入应用 freeze（测试包不依赖于应用运行时，`VERIFIED`/`INCOMPLETE` 以 freeze 为准）。
- `pip check`：`No broken requirements found.`（`VERIFIED`）
- SLES12SP5：本归档是 EulerOS 性能代理，不是原生 SLES12SP5 实机证据（`VERIFIED` caveat）。
- glibc exact version：归档未记录（`UNAVAILABLE`）。
- `readelf/ldd` native dependency capture：归档未记录（`UNAVAILABLE`）。
- `PYTHONFAULTHANDLER` / `PYTHONMALLOC=debug`：R2 归档未记录（`UNAVAILABLE`）。
- `core_pattern`、core 文件、`gdb`/`coredumpctl` native frame：未提供（`UNAVAILABLE`）。

## A0 后续动作

1. 在本地原生 ext4 Run Root 中进行最小诊断复现，不重复服务器 D7/R2，先比较 default multiprocessing context 与显式 `spawn`。
2. 为主进程、ProjectionWriter、ShardWorker 写入非敏感 PID/PPID/role/start method/exit code/phase 记录，并启用 `faulthandler`；如无 native frame，结论最高为 `RCA_STRONGLY_INDICATED`。
3. 在 A1 归因完成前不把 `fork` 猜想写成根因；在 A2/A3 修复后重新生成本索引引用的最终 RCA 报告，并保留本次失败证据。

## A1-A4 / D0-D6 本轮 follow-up（2026-10-16）

以下条目是本轮新增核验，不覆盖上文历史 R2/R7 记录。每一项只使用实际生成的证据；缺少 native faulting frame 的地方仍标记为 `UNAVAILABLE`。

### RCA 结论

- `RCA_STRONGLY_INDICATED`：历史 R2 两次真实 `main + FastAPI + VNextRuntime` 运行均以 `SIGSEGV (-11)` 结束；静态生命周期风险指向 API 进程已初始化 native/线程状态后再创建子进程的边界，但归档没有 core 或 C/C++ faulting frame。
- `RCA_PROVED`：`UNAVAILABLE`。本地最小 `fork`/`spawn` 对照均 PASS，不能伪造“已证明 fork 是唯一根因”。
- `VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV`：保留历史 fail-closed 判定；本轮本地和 EulerOS R7 的 PASS 不覆盖历史 R2。

### 本地验证（VERIFIED）

- Windows backend full regression：`382 passed, 4 skipped`。
- WSL AlmaLinux-8 native ext4 backend full regression：`384 passed, 2 skipped`；正式运行目录为 `/tmp/mc-vnext-native-sigsegv-rca-20261016`，不是 `/mnt/d`。
- Clean native ext4 R0/R1：`100/100`、`1000/1000`；R3：`1000/1000`；R4 writer restart、R5 control state、R6 worker restart 均 PASS。
- Exact R7：`4000/4000`，loopback received/response `4000/4000`，projection unique/taken/socket_written `4000/4000/4000`，`taken_without_socket_written=0`。
- Exact R7 preflight：`/dev/sdd ext4`、CPython `3.11.5`、DuckDB `1.2.0`、`ctypes`/OpenSSL 可加载；peak RSS `547536896`、peak FD `175`、peak run root `10235267` bytes、writer lag `0.0s`。
- Minimal native diagnostic：`spawn` 与 `fork` 均 PASS；无 SIGSEGV、孤儿进程或重复 event id。

### 本轮代码/运行时修复（VERIFIED）

- `runtime.py`、`supervisor.py`、`projection_writer.py` 共享显式 `spawn` context。
- 子进程写入受限 role metadata；故障退出码、重启退避和 writer flush/replay 可审计。
- `route_credits.py` 增加进程内锁，修复高并发 route credit burst 的短锁超时；修复后 R3 通过。
- `build-sles12sp5-system-python-manual.sh` 排除 native diagnostic；pytest/diagnostic 只在测试包中。

### 离线包（VERIFIED）

- 正式应用包（最终边界审计重建）：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-a/packages/app/marketing-copilot-sles12sp5-system-python-20261016-vnext-native-sigsegv-rca.tar.gz`，SHA256 `7a9b4106d8dd7a86fa27670b8ff81ad0b3412342cecdc796ba137cd764a7fdc1`。
- 测试专用包：`.../packages/test/marketing-copilot-vnext-runtime-canary-test-20261016-native-sigsegv-rca.tar.gz`，SHA256 `bb02f7e902fc7caac15a1ef906eae1c11b84cb5f1e5209bbf1c320698515b130`。
- standalone runtime：`sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz`，SHA256 `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`。
- 两个应用/测试包的内部 `checksums.sha256` 均通过；正式应用包不含 pytest、tests、native diagnostic、DB/WAL、日志、上传或凭据。

### EulerOS B2（VERIFIED；Stage C receipt 见下文）

- Run root：`/home/paas/marketing-copilot-test/runs/vnext-native-sigsegv-rca-20261016/`，EulerOS 2.0 SP15，非 root `paas`，ext4 `/dev/sda2`。
- D0：spawn 最小诊断 10/10 loopback responses，PASS。
- R0/R1/R3/R4/R5：PASS；合计 loopback/response/socket_written `2720/2720/2720`，显式取消对应 20 条 `taken_without_socket_written`。
- R6/R7：命令与 perf guard 返回 PASS；R2/P5500 未执行于该 B2 base run，随后已按授权执行一次独立 D7。
- 服务器证据归档：`server-b2-evidence.tar.gz`，SHA256 `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`。
- `PHASE_C_READY_FOR_MANUAL_VPN_SWITCH` 已生成；服务器归档的本地下载、hash 复核和 Stage C 报告收口已在下文完成。

### 仍不可用（UNAVAILABLE）

- 历史 R2 faulting native frame、core、gdb/coredumpctl 记录：`UNAVAILABLE`。
- 不能把 Python traceback、faulthandler 线程栈或 guard `-11` 当作 C/C++ native faulting frame。

## Stage C 服务器归档核验（VERIFIED；2026-10-16）

- 本地归档：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`。
- SHA256：`e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`，与服务器 B2 记录一致。
- 归档安全性：863 个成员，绝对/盘符/`..` 路径 0 个，符号链接/硬链接 0 个；已只读解包到独立 Stage C 目录。
- 内部 `results/B2-files.sha256`：670/670 项本地复核通过；`core-files.txt` 与 `remaining-processes.txt` 均为空；`PHASE_C_READY_FOR_MANUAL_VPN_SWITCH` 存在。
- R6：PASS，200/200。
- R7：PASS，target/sent `4000/4000`，`611.721 s`，peak RSS `576663552 B`，peak FD `119`，peak Run Root `10157749 B`，max writer lag `0.0 s`。
- R6/R7 合计 loopback/response `4200/4200`，projection unique/taken/socket_written `4200/4200/4200`，`taken_without_socket_written=0`；perf guard max aggregate RSS `582787072 B`，disk delta `0 B`，exit code `0`。
- 本地详细 receipt：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence-verification.md`。

本节完成 `STAGE_C_EVIDENCE_VERIFIED`。历史 R2/P5500 两次 `SIGSEGV (-11)` 仍缺少 native faulting frame；B2 R6/R7 PASS 不能覆盖它们。最终 D7 receipt 见下文；未执行 commit/push。

最终正式应用包已在边界审计后重建：SHA256 `7a9b4106d8dd7a86fa27670b8ff81ad0b3412342cecdc796ba137cd764a7fdc1`，195 个成员，必须文件和内部 checksum 均通过；`backend/tools`、tests、`*.bak*`、日志、DB/WAL、uploads、backups、runtime data 及 native test diagnostic 均未进入正式包。测试包 SHA256 `bb02f7e902fc7caac15a1ef906eae1c11b84cb5f1e5209bbf1c320698515b130`，standalone runtime SHA256 `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`；测试包仍保持独立，不合入正式部署包。

## D7/R2/P5500 最终诊断收口（VERIFIED；FAIL）

- 唯一一次修复后授权 D7 使用冻结合同 `2500 + 2500 + 250 + 250 = 5500`，route TPS `80`、响应延迟 `2 ms`、4 Worker、64 分片、显式 `--authorize-p5500`。
- 服务器 Run Root：`/home/paas/marketing-copilot-test/runs/vnext-native-sigsegv-rca-20261016-d7-r2-p5500`。
- 最终证据归档：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`，SHA256 `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e`；8 个成员，路径/链接安全检查通过。
- 内部 `d7-evidence-final.sha256`：7/7 项本地复核通过。
- Perf guard：`FAIL`，原因 `guarded command exited with -11`，退出码 `-11`，耗时 `34.361 s`；峰值 aggregate RSS `559325184 B`、磁盘增量 `0 B`，未触发 2 GiB/5 GiB 资源限制。
- 进程归属：guard root PID `37226` 为 standalone CPython canary 命令，在最后活动采样中为 84 线程并随后变为 zombie；guard 未提供 worker/writer 的 native frame。PID `42553`/`42557` 分别为 resource tracker 与 spawn worker，崩溃后 reparent，已精确清理。
- `canary.json` 未生成，core 清单为空，没有 native faulting frame；guard 记录的两个孤儿子进程已精确核验并发送 `SIGTERM`，post-cleanup 审计显示 PID `42553`/`42557` 均 `ABSENT`。
- 不再执行第三次 D7/R2。

因此最终 `RCA_STRONGLY_INDICATED` 保持不变，`VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`：修复后的真实长驻 R2/P5500 仍复现 SIGSEGV，显式 `spawn` 不是完整修复，也不能以 R7 或本地 PASS 覆盖；缺少 native frame，不能升级为 `RCA_PROVED`。
