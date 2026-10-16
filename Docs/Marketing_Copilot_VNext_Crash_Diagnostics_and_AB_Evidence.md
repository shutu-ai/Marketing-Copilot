# Marketing Copilot VNext Crash Diagnostics 与 Targeted A/B Evidence

## 1. Evidence policy

本文件只收录本轮本地原生 ext4 定向证据与历史归档的只读核验结果。测试全部是 synthetic/loopback-only：没有真实 SMSC、没有真实生产数据库、没有外部业务请求，也不记录 MSISDN 全文、消息正文或密码。

本轮没有重新连接 EulerOS，也没有执行 R2/P5500。EulerOS 的 D7/R2/P5500 结果只引用既有归档：它仍是 `SIGSEGV (-11)`，因此不能被本地 A/B PASS 覆盖。

## 2. Formal local environment

所有定量 A/B 均在 WSL `AlmaLinux-8` 的原生 ext4 临时目录 `/tmp/marketingcopilot-static-audit-20261017` 执行；没有把 `/mnt/d` 结果当作性能基线。

```text
df -T .
/dev/sdd       ext4  1055762868 104205076 897854320 11% /

Python 3.11.13
```

离线 wheelhouse freeze：

```text
aiofiles==24.1.0
annotated-types==0.8.0
anyio==4.14.2
bcrypt==4.2.1
cffi==2.1.1
click==8.4.2
cryptography==41.0.7
duckdb==1.2.0
et_xmlfile==2.0.0
fastapi==0.115.0
h11==0.16.0
idna==3.18
numpy==2.2.6
openpyxl==3.1.5
pandas==2.3.2
paramiko==3.5.1
pika==1.3.2
pycparser==3.0
pydantic==2.9.2
pydantic_core==2.23.4
PyNaCl==1.5.0
python-dateutil==2.9.0.post0
python-multipart==0.0.12
pytz==2026.3.post1
six==1.17.0
smpplib==2.2.4
starlette==0.38.6
typing_extensions==4.16.0
tzdata==2026.3
uvicorn==0.30.6
watchfiles==0.24.0
websockets==13.0
xlrd==2.0.1
```

## 3. F02 idle hot-path A/B

工具：`backend/tools/vnext_static_audit_ab.py`；run root：`/tmp/marketingcopilot-static-audit-20261017/ab-f02-native`；输出：`ab-f02-native.json`。

基线 full path 每次查询新建 `TaskSpool`，执行完整 recovery/index scan；优化 path 复用一个 owner，调用 `lightweight_snapshot()`，只读取 manifest/cursor/taken 元数据，不读取 segment JSON/CRC。两侧 `pending_records=1000`、`total_records=1000` 保持一致。

| 查询次数 | Full 时间 | Lightweight 时间 | 近似加速 | Full: constructions/scans/JSON loads/bytes | Lightweight: constructions/scans/JSON loads/bytes |
|---:|---:|---:|---:|---:|---:|
| 10 | 0.333723 s | 0.042431 s | 7.9x | 10 / 160 / 10,010 / 3,498,470 | 1 / 16 / 1,011 / 349,847 |
| 100 | 4.066159 s | 0.085907 s | 47.3x | 100 / 1,600 / 100,100 / 34,984,700 | 1 / 16 / 1,101 / 349,847 |
| 1,000 | 42.248379 s | 0.413651 s | 102.1x | 1,000 / 16,000 / 1,001,000 / 349,847,000 | 1 / 16 / 2,001 / 349,847 |

此 A/B 是成本与状态等价性证据，不是业务吞吐承诺。它直接证明原 idle hot path 的重复恢复成本随 query 次数放大，而 lightweight snapshot 保持 bounded metadata work。

## 4. F01 watchdog lifecycle A/B

两个 run 均使用真实 `main + FastAPI + Uvicorn + VNextRuntime` 生命周期、1 个 spawn worker、16 partitions、loopback SMPP、route TPS `80`、响应延迟 `0 ms`、R7 gate、stability `32 s`。首次使用企业代理的 run 因 loopback 代理误配置超时，被排除；runner 已增加 `ProxyHandler({})`，最终证据不依赖外部代理设置。

| Run | Watchdog | 结果 | 时长 | 发送 | Projection unique/taken/socket_written | Peak RSS | Peak FD |
|---|---|---|---:|---:|---:|---:|---:|
| `f01-enabled-final` | enabled | PASS | 35.392 s | 300 | 300/300/300 | 356,114,432 B | 69 |
| `f01-disabled-final` | disabled | PASS | 35.396 s | 300 | 300/300/300 | 354,594,816 B | 69 |

enabled run 的 `stall-diagnostics/watchdog-config.json`：

```json
{
  "enabled": true,
  "no_progress_signal": "no_progress_stall",
  "run_age_is_not_stall": true,
  "scheduled_snapshot": "application-threads-timeout.txt",
  "threshold_seconds": 30.0
}
```

enabled run 因持续超过 30 s 产生了 `application-threads-timeout.txt`（9,540 bytes），这是一次性的 scheduled Python stack snapshot；它没有被写成 `no_progress_stall`。root faulthandler 文件存在但为 0 bytes，因为本地运行没有 native crash。disabled run 不创建 `stall-diagnostics` 目录，证明 startup switch 在实际入口生效。

## 5. Runner diagnostics evidence

`f01-enabled-final` 生成了以下 bounded 诊断角色：

```text
diagnostics/native-crash/canary-root-faulthandler.log
diagnostics/phase-markers.jsonl
diagnostics/processes/canary-root-2480.json
diagnostics/processes/api-main-2480.json
diagnostics/processes/resource-tracker-2802.json
vnext/diagnostics/processes/projection-parent-2480.json
vnext/diagnostics/processes/projection-writer-2803.json
vnext/diagnostics/processes/runtime-parent-2480.json
vnext/diagnostics/processes/worker-supervisor-0-2480.json
vnext/diagnostics/processes/worker-0-2804.json
```

`phase-markers.jsonl` 记录 `sequence`、`phase`、PID、monotonic seconds、wall time 和 bounded progress；阶段包括 `run_start`、`environment_ready`、`seed_complete`、`api_started`、`wait_idle_start`、`wait_idle_complete`、`api_stopped`。process metadata 额外记录 Python/平台、DuckDB/native-backed module versions、start method、role、PID/PPID、thread count；resource tracker 明确标记 `observed_only=true` 和 `native_fault_capture=UNAVAILABLE`。

这些文件能回答“哪个 role/PID 在哪个阶段存在”，但 faulthandler 仍只提供 Python thread stack。没有 SIGSEGV、core、C/C++ frame 或 debugger record 时，不能从它们推导 native faulting instruction。

## 6. Historical archive verification

### D7/R2/P5500

- Archive：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`
- Outer SHA256：`ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e1`
- Internal manifest：`d7-evidence-final.sha256`，`7/7` hash rows passed。
- Result：guarded command `-11` after `34.361 s`，peak aggregate RSS `559325184 B`，disk delta `0 B`；no `canary.json`/core/native frame。

### EulerOS B2/R7

- Archive：`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`
- Outer SHA256：`e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`
- Internal manifest：`results/B2-files.sha256`，`670/670` hash rows passed；unsafe path `0`，symlink/hardlink `0`。
- B2/R7 was a `4000` contract and remains a separate PASS evidence set; it does not cover D7/R2/P5500 `5500`。

## 7. Test and package evidence

- Targeted Windows tests: `16 passed`。
- Full backend regression: `390 passed, 4 skipped, 4 warnings` on the final run。
- One initial full-suite failure was a pre-existing timing boundary (`0.132 s` versus `0.15 s`) in route-credit concurrency; five immediate isolated reruns passed. No source change was made to hide it。
- Slim application package built from the current source boundary: internal checksums passed；`180` files；`33` wheels；required release/frontend/run/checksum files present；`backend/tests=0`、`backend/tools=0`、runtime data `0`、nested backend `0`。
- Package outer SHA256：`4bf4edcd51a831d77dfcbab38802ec96f44aef3a6c4dd95efdc87bc9faef7f94`。

测试文件 `backend/tests/test_vnext_static_audit.py` 覆盖 watchdog false-stall 与开关、native capture metadata、lightweight snapshot 不扫描 segment、runtime 长生命周期 spool owner、idle loop、Counter reconciliation、journal incremental offset；既有 closure 测试覆盖 projection restart/replay、runtime stop/restart、pause/resume 和 worker/projection recovery。

## 8. Reproducibility and limitations

```text
PYTHONPATH=<native-ext4-run-root>/backend \
<native-ext4-run-root>/venv/bin/python \
<native-ext4-run-root>/backend/tools/vnext_static_audit_ab.py \
  --run-root <native-ext4-run-root>/ab-f02-native \
  --output <native-ext4-run-root>/ab-f02-native.json \
  --records 1000 --partitions 16 --queries 10,100,1000
```

本地 A/B 的目标是验证 F01/F02 的行为与观察成本，不是复现 EulerOS native crash。D7/R2/P5500 的历史 `-11` 没有被重新执行；没有 native faulting frame 时，本文件不提供未经证实的函数级 RCA。

## 9. Status mirror

```text
STATIC_AUDIT_STATUS=PASS
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
WATCHDOG_FALSE_STALL=FIXED
IDLE_HOTPATH_AMP=FIXED
RUNNER_OBSERVER_OVERHEAD=FIXED
LOCAL_TARGETED_AB=PASS
EULEROS_R2_P5500=NOT_RUN_UNAUTHORIZED
VNEXT_RUNTIME_CANARY_CLOSURE=FAIL
INTEGRATED_STATUS=INTEGRATED_NO_GO
```
