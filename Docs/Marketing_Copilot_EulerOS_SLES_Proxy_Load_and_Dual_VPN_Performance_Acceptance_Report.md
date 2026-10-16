# Marketing Copilot EulerOS / SLES Proxy Load and Dual-VPN Performance Acceptance Report

任务：`MC-EULEROS-SLES-PROXY-PERF-DUAL-VPN-20261016`<br>
报告日期：2026-10-16<br>
冻结代码基线：`d6b482f2a330570cf25d5d54802b8ba1bcee2b08` (`main`)

## 1. 结论

本轮阶段 C 已完成本地证据导入、哈希复核和 WSL / EulerOS 对照。

```text
EULEROS_PROXY_P100_STATUS = PASS
EULEROS_PROXY_P1K_STATUS  = PASS
EULEROS_PROXY_P5500_STATUS = FAIL
OFFLINE_WAL_STATUS = PASS_FOR_OFFLINE_EXTENSION_AND_E0_RESTART; DEDICATED_2000_ROW_WAL_GATE_NOT_RUN
HOST_SAFETY_STATUS = PASS_FOR_THIS_RUN
INTEGRATED_STATUS = INTEGRATED_NO_GO
```

E0、E1/P100、E2/P1K 和 E3/P1K-OCC 在 EulerOS 目标机上均完成业务对账：无 unknown、partial 或重复任务，SMSC simulator response 全部成功。E4/P5500 在不触发 RSS 或磁盘资源阈值的情况下，因等待四个活动全部 accepted 超时而失败；该失败不应被改写为性能通过，也不应通过放宽业务超时重试。

`INTEGRATED_NO_GO` 继续保持。52K/102K、G4、G6 和长时间稳定性门禁本轮未执行，不能由本报告的局部 PASS 自动放行。

## 2. 结果边界与平台声明

- 服务器真实系统是 **EulerOS 2.0 SP15 x86_64**，不是原生 SLES12SP5。
- 本轮按用户批准，将 EulerOS 作为 `SLES_PROXY_PERFORMANCE_TARGET` 性能等效目标；报告中的服务器数据始终标注为 EulerOS。
- 本报告不证明原生 SLES12SP5 的全部 ABI、内核或发行版行为，也不把 EulerOS 证据伪写成 SLES 证据。
- 测试只使用 Run 内的应用进程 API 和 `127.0.0.1` SMPP simulator；没有连接真实 SMSC、外部业务 API、外部数据库或生产数据。
- Stage A 在 WSL `AlmaLinux-8` native ext4 上执行；没有把 Windows `/mnt/d` 的结果作为 Linux 性能基线。
- Stage B 所有运行时、venv、数据库、临时目录、缓存和证据均位于新建 Run：
  `/home/paas/marketing-copilot-test/runs/euleros-proxy-perf-20261016T122709Z-6305`。

## 3. 可追溯性与证据完整性

| 项目 | 值 |
| --- | --- |
| Frozen source SHA | `d6b482f2a330570cf25d5d54802b8ba1bcee2b08` |
| App package SHA-256 | `c3930f04156151c56503ffab89f95d5523938f351966f63086f96119cb707303` |
| Standalone CPython 3.11.5 archive SHA-256 | `65bada65662af0109b597e5b46e8687b37f622517d264659b8deebb964bc403d` |
| Offline wheelhouse SHA-256 | `504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda` |
| DuckDB `core_functions` extension SHA-256 | `61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640` |
| Test-only pytest package SHA-256 | `bcaf091e2f56392eebe578ec632c11c31721491d40f583bf7a331f834c0fb19c` |
| Final Stage A closure SHA-256 | `237d68dcdbeca0cc81606105a2cc71f3d767d097db4ae5a0b9a26c79a78de3ef` |
| Imported Stage B evidence archive SHA-256 | `cee64a779c39ed75dfe9961996bfb8ad5e52d46b58608dacf614788ff620be56` |

Stage B evidence was imported after the manual VPN boundary. The local `server-evidence.sha256` verification passed for all 25 exported files, and the downloaded archive SHA matched its sidecar. The export contains reviewed JSON, environment records and resource samples only; it excludes database files, WAL/journal files, uploads, raw command logs and credential values.

The repository already contained unrelated dirty changes before packaging. The frozen `git rev-parse HEAD` is the source identity used here. The generated application `release.json` carries a `d6b482f2-dirty` metadata suffix because of that pre-existing worktree state; it is not treated as a different source baseline.

## 4. Runtime and host comparability

| Dimension | Stage A: WSL native ext4 | Stage B: EulerOS proxy target |
| --- | --- | --- |
| OS actually observed | AlmaLinux-8 WSL2 | EulerOS 2.0 SP15 x86_64 |
| Kernel | `6.18.33.2-microsoft-standard-WSL2` | `6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64` |
| glibc | WSL host runtime | `2.38` |
| Test filesystem | `/dev/sdd`, `ext4` | `/dev/sda2`, `ext4` |
| Python | standalone CPython `3.11.5` | standalone CPython `3.11.5` |
| DuckDB | `1.2.0`, local extension | `1.2.0`, local extension |
| Offline mode | `MC_DUCKDB_OFFLINE_ONLY=1`, `pip --no-index` | `MC_DUCKDB_OFFLINE_ONLY=1`, `pip --no-index` |
| CPU policy | affinity `0-3` | affinity `0,1,2,3` |
| RSS warning | aggregate 2 GiB | aggregate 2 GiB |
| Run disk warning | 5 GiB | 5 GiB |
| Priority policy | `nice +10`, `ionice class 3` | `nice +10`, `ionice class 3` |
| Formal filesystem record | `df -T .` captured | `df -T .` captured |

The common application dependency set contains 33 exact pins. The server test venv adds only the separately packaged test-only pytest set, including `pytest==8.3.5`; that package is not part of the formal application deployment package.

At Stage B T0, the host had 64 online CPUs but the Run was restricted to four selected logical CPUs. Observed CPU idle was approximately 80–86%, no swap in/out was observed, and approximately 236 GiB memory was available. The Run reached approximately 1.8 GiB, below the 5 GiB warning. No other `marketing-copilot-test` Run-owned child process was present at the E4 start, and the post-shutdown audit reported `run_owned_child_pids=none`. These are observations for this Run, not a hard cgroup quota or a claim that the shared host was otherwise idle.

## 5. Workload and command equivalence

The paired gates used the same frozen application, standalone runtime, local DuckDB extension, simulator mode, reliability mode, route PDU limit and resource guard. `--route-tps 200` means approximately 200 `submit_sm` PDU parts per second; it is not a task-per-second claim.

| Paired gate | Data shape | Dispatch / DB mode | Pool / window | Runner timeout | Guard timeout |
| --- | --- | --- | --- | ---: | ---: |
| C0 / E0 | 10 tasks, restart, 1 file | legacy / serialized | 1 / 10 | 120 s | 180 s |
| P100 / E1 | 100 tasks, 2 files | fair / serialized | 1 / 10 | 300 s | 360 s |
| P1K / E2 | 1,000 tasks, 10 files | fair / serialized | 1 / 10 | 300 s | 360 s |
| P1K-DB-OCC / E3 | 1,000 tasks, 10 files | fair / OCC | 1 / 10 | 300 s | 360 s |
| P5500 / E4 | 2 × 2,500 + 2 × 250 tasks | fair / serialized | 4 / 50 | 300 s | 420 s |

Common relevant arguments were `--send-mode reliable`, `--route-tps 200`, `--simulator-tps 0`, `--simulator-mode fast`, `--simulator-audit fast`, `--max-memory-mb 2048`, `--keep-artifacts`, and Run-local `--run-root` / `--output` paths. E3 changed only `--db-concurrency occ` from the E2 serialized command. E4 was executed once after E0–E3 and the fresh T0 check; it was not retried.

The guarded command form was:

```text
run_guard <gate> <guard-timeout> "$PY" "$RUN/src/backend/tools/<runner>.py" \
  ... --run-root "$RUN/runs/<gate>/runner" \
  --output "$RUN/runs/<gate>/runner.json"
```

The complete gate arguments and guard/resource samples are retained under the Stage A baseline and the Stage B export listed in §10.

## 6. Gate outcomes

| Paired gate | Stage A WSL result | EulerOS result | EulerOS correctness evidence |
| --- | --- | --- | --- |
| C0 / E0 | PASS | PASS | 10/10 accepted, 10/10 response success, unknown 0, partial 0, simulator audit `VERIFIED` |
| P100 / E1 | PASS | PASS | 100/100 accepted, 100/100 response success, unknown 0, partial 0, simulator audit `VERIFIED` |
| P1K / E2 | PASS | PASS | 1,000/1,000 accepted, 1,000/1,000 response success, unknown 0, partial 0, simulator audit `VERIFIED` |
| P1K-DB-OCC / E3 | PASS | PASS | 1,000/1,000 accepted, 1,000/1,000 response success, unknown 0, partial 0, simulator audit `VERIFIED` |
| P5500 / E4 serialized | FAIL | FAIL | Runner timed out waiting for all four activities accepted; generated 3,500/5,500 on both runs |

Correctness and performance are intentionally separated. The first four rows establish bounded E2E correctness for this proxy target. They do not establish 5,500-task correctness, native SLES behavior, G4/G6 acceptance or integrated release readiness.

## 7. Performance comparison

Durations are guard wall-clock durations. RSS is calculated from each guard's `max_aggregate_rss_bytes / 2^20` and is reported in MiB. The derived rate is `accepted tasks / guard seconds` only for complete gates; it is not SMPP PDU throughput and includes runner setup/teardown.

| Pair | WSL duration | Euler duration | Duration delta | WSL RSS | Euler RSS | RSS delta | WSL accepted/s | Euler accepted/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C0 / E0 | 26.106 s | 19.889 s | -23.8% | 287.1 MiB | 290.7 MiB | +1.2% | 0.383 | 0.503 |
| P100 / E1 | 40.171 s | 27.058 s | -32.6% | 297.8 MiB | 324.3 MiB | +8.9% | 2.489 | 3.696 |
| P1K / E2 | 244.972 s | 153.691 s | -37.3% | 325.4 MiB | 364.6 MiB | +12.1% | 4.082 | 6.507 |
| P1K-DB-OCC / E3 | 154.601 s | 102.249 s | -33.9% | 296.7 MiB | 300.9 MiB | +1.4% | 6.468 | 9.780 |
| P5500 / E4 | 331.372 s | 338.511 s | +2.2% | 394.9 MiB | 457.3 MiB | +15.8% | N/A: incomplete | N/A: incomplete |

The completed bounded gates were faster on this particular EulerOS host under the same four-CPU policy, while using somewhat more RSS at P100/P1K. This is an observation, not a portable hardware-independent throughput claim: the WSL and EulerOS hosts have different kernels, CPU topology, background load and storage environments. The P5500 result must not be interpreted as a successful throughput comparison because neither side completed the four-activity acceptance.

## 8. P5500 failure and root-cause boundary

The EulerOS E4 guard record is:

```text
guard status: FAIL
runner exit code: 2
duration: 338.511 s
max aggregate RSS: 457.3 MiB
Run-root disk delta: 0 GiB
reason: TimeoutError: Timed out waiting for all four activities accepted
```

The runner reconciliation states were:

```text
BATCH_INGEST_FAIRNESS   = PASS
REALTIME_INTAKE_FAIRNESS = PASS
SMS_DISPATCH_FAIRNESS   = FAIL
CROSS_TYPE_E2E          = FAIL
FAIRNESS_TELEMETRY      = PASS
SFTP                    = BLOCKED_ENV
```

The server generated 3,500/5,500 tasks and reported 2,787 successful simulator `submit_sm` responses; the 2,000-task deficit is recorded as evidence, not converted into an accepted count. The WSL P5500 baseline showed the same broad timeout shape and the same dispatch/cross-type failure classification, with 3,500/5,500 generated and 1,914 successful simulator responses. This repeatability indicates a bounded dispatch/four-activity symptom, but it does not prove a single root cause.

The failure was not caused by the guard's RSS or Run-disk limits. No resource breach, crash, WAL corruption or duplicate-send result was recorded. The SFTP state is environment-blocked and should be resolved or explicitly excluded before treating the mixed-campaign gate as complete.

## 9. Attribution assessment

| Category | Assessment | Evidence and limitation |
| --- | --- | --- |
| `HARDWARE_RESOURCE_BOUND` | `NOT_PROVED` | All completed gates and E4 stayed below the 2 GiB aggregate RSS warning; gate disk deltas were 0; E4 failed with application exit 2. Host differences remain a confounder. |
| `DB_TRANSACTION_LOCK_BOUND` | `NOT_PROVED` | Serialized and OCC 1K gates both passed with complete reconciliation and no unknown/partial tasks. The P5500 failure was not isolated to a lock or WAL wait. |
| `DISPATCH_SYNC_WAIT` | `INDICATED` | Both WSL and EulerOS P5500 runs timed out waiting for all four activities accepted and reported `SMS_DISPATCH_FAIRNESS` / `CROSS_TYPE_E2E` failures. Stage-level decomposition is still required. |
| `HARNESS_OBSERVER_OVERHEAD` | `NOT_PROVED` | The runner records `LIVE_ENV_UNVERIFIED`; no observer-off/observer-on controlled A/B exists in this Run. |
| `SMPP_ROUTE_LIMIT` | `NOT_PROVED` | Both runs used one route and the 200 PDU-parts/s cap, but the incomplete P5500 data does not prove route saturation. |
| `MIXED_OR_UNKNOWN` | `INDICATED` | SFTP is `BLOCKED_ENV` and the four-activity reconciliation is incomplete; no causal attribution is justified yet. |

## 10. Evidence locations

Stage A native-ext4 baseline:

```text
test-artifacts/euleros-proxy-perf-20261016-a/desktop/closure/
  marketing-copilot-euleros-proxy-perf-closure-d6b482f2/baseline/
```

Stage B imported, hash-verified export:

```text
test-artifacts/euleros-proxy-perf-20261016-a/server-stage-b/export/
```

Important reviewed files include:

- `stage-b-result-summary.json`
- `preflight.json`
- `run-manifest.json`
- `gates/e0.json`, `gates/e1.json`, `gates/e2.json`, `gates/e3-occ.json`, `gates/e4-serialized.json`
- `resource-samples/*.jsonl`
- `e4-t0.txt` and `post-shutdown-audit.txt`
- `server-evidence.sha256`

For the task's JSON handoff convention, Stage C also created a normalized local copy at `test-artifacts/euleros-proxy-perf-20261016-a/server-stage-b/stage-c-derived/post-shutdown-audit.json`. It is explicitly derived from the hash-verified text record; the original server export was not modified.

The downloaded archive and sidecar are under:

```text
test-artifacts/euleros-proxy-perf-20261016-a/server-stage-b/
  server-evidence-export.tar.gz
  server-evidence-export.tar.gz.sha256
```

## 11. Decision and next actions

1. Keep `INTEGRATED_NO_GO`; do not use P100/P1K PASS to promote the integrated release gate.
2. Do not rerun E4 with a longer timeout, higher resource budget, modified checkpoint/durability settings or altered retry semantics.
3. If a future approved shared-host window is available, first run a bounded diagnostic that separates the four activity paths and records generated/claimed/send-intent/socket-submit/response/journal/DB-terminal counts per activity. Preserve the 300-second business timeout and the same resource guard.
4. Resolve the SFTP test prerequisite or record a formally approved `BLOCKED_ENV` scope before using the mixed-campaign result as an acceptance gate.
5. Run a dedicated offline WAL/replay gate only if it is required for the release decision; this report deliberately does not claim a dedicated 2,000-row WAL PASS.
6. Keep 52K/102K, G4, G6 and long-stability tests out of scope until separately approved with a safe host window and explicit resource contract.

No code, system configuration, VPN configuration, production deployment package or Git history was modified by Stage C. No commit or push was performed.
