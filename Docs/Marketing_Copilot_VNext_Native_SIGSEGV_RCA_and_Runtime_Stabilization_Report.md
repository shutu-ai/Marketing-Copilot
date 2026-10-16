# Marketing Copilot VNext Native SIGSEGV RCA and Runtime Stabilization

## 1. Scope and decision

- Baseline: `4de1e0d1fd548cf0a74fe7942c44ac8ff5d21a0e` (`feat: replace SMS send path with VNext sharded pipeline`).
- The worktree already contained unrelated dirty and untracked user files. They were preserved; no reset, clean, commit, or push was performed.
- Historical R2/P5500 evidence contains two real EulerOS `SIGSEGV (-11)` exits. The original closure decision remains fail-closed:
  `VNEXT_RUNTIME_CANARY_CLOSURE=BLOCKED_ENV`.
- Current final closure after the one authorized post-fix D7/R2/P5500 run: `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`.
- The current RCA classification is `RCA_STRONGLY_INDICATED`, not `RCA_PROVED`: no core file or faulting native frame was available, and the local fork/spawn isolation comparison did not reproduce the crash.

This report is a follow-up to the historical closure report. It does not replace the historical R2 failures with a local PASS or reinterpret the historical `INTEGRATED_NO_GO` decision.

## 2. Evidence-backed RCA

### 2.1 What actually crashed

The two archived R2 runs were real `main + FastAPI + Uvicorn + VNextRuntime` canary runs, not the old prefilled spool-only loopback runner. Both were terminated by the guard with exit code `-11` in about 35 seconds. The first run had an invalid cross-run environment path; the retry used an isolated `HOME/TMPDIR/XDG_CACHE_HOME` under its own run root and still exited `-11`.

The archived records identify the guarded command and descendant PIDs, but do not identify a faulting child stack. `canary.json`, complete stdout, native frame, core, and `gdb/coredumpctl` evidence are unavailable in that archive. The parent guard exit code therefore proves a native-class process termination, but not the exact faulting instruction.

### 2.2 Static lifecycle risk

The pre-fix runtime could create long-lived multiprocessing workers and a projection writer after the API process had already initialized DuckDB, logging, threads, asyncio, and other native-backed state. A default multiprocessing context was also allowed to choose the platform default. That is a high-risk lifecycle boundary for a forked child: inherited native allocator state, DuckDB/native connection state, feeder threads, event loops, and file descriptors can be copied into a process that was not initialized from a clean interpreter state.

The repaired lifecycle now has these explicit boundaries:

1. `VNextRuntime`, `WorkerSupervisor`, and `ProjectionWriterProcess` share one explicit `spawn` context for `Process`, `Queue`, `Pipe`, `Event`, and related IPC objects.
2. Child entry points initialize their own DuckDB/I/O/async/SMPP state and write only non-sensitive role metadata.
3. Worker and writer exit codes, phase, PID/PPID, start method, and bounded progress are recorded under the run-local diagnostics directory.
4. Restart attempts have bounded exponential backoff and do not create a second owner for an active worker slot.
5. The journal is authoritative for projection recovery; IPC is a wake-up/fast path rather than the only copy of progress.

This mechanism is strongly consistent with the historical failure boundary, but it is still an inference because the native faulting frame was not captured. The post-fix EulerOS D7 run still crashed under explicit `spawn`, so the change is not presented as a complete native-cause fix.

### 2.3 Counter-evidence and limits

The new minimal diagnostic passed with both `spawn` and `fork` on native ext4 using the same loopback runtime lifecycle, including restart and pause-state restoration. The post-fix EulerOS D7 run nevertheless reproduced `-11` under explicit `spawn`. Therefore `fork` is a high-priority historical risk hypothesis, not a proven sole cause, and the broader native-backed long-lived runtime boundary remains unresolved. The local evidence demonstrates bounded-path stabilization only; it does not prove the exact C/C++ fault site.

### 2.4 A1 attribution table

| Required question | Evidence-backed conclusion | Classification |
|---|---|---|
| Which process exited `-11`? | The final D7 guard attributed `-11` to the guarded standalone CPython canary command, root PID `37226`; no worker/writer native frame was captured. | `PROVED` at command level; native instruction `UNAVAILABLE` |
| Where did the first abnormal boundary occur? | D7 ended after `34.361 s` before `canary.json`; the record does not identify DuckDB, Spool, SMPP, or shutdown as the exact first faulting operation. | `UNAVAILABLE` |
| What differs between R2 and R7? | Both use the real main/API/runtime path; R2 uses the `5500` contract with throttled/slow loopback and fails, while R7 uses `4000` and passes. This narrows the boundary to a longer/high-load runtime path, not a single proven call. | `STRONGLY_INDICATED` |
| Is there direct malloc/free, DuckDB WAL, Queue, or socket native evidence? | No core, native frame, complete crash stdout, or debugger record is available in either the historical or D7 evidence. | `UNAVAILABLE` |
| Was native state initialized before child creation? | Static pre-fix call-path review showed API/native/thread state could precede default-context child creation; the implementation now uses child-local initialization. | `STRONGLY_INDICATED`, not proved as the sole cause |
| Does fork/spawn explain the difference? | The pre-fix default-context mismatch is a valid risk hypothesis, but the post-fix explicit-spawn D7 crash means fork is not sufficient to explain the failure. | `HYPOTHESIS` |
| Did Spool/Journal/IPC backlog cause the crash? | D7 produced no canary reconciliation and stayed below resource limits; historical R2 has no canary JSON. No backlog causal relation can be established. | `UNAVAILABLE` |
| Were there separate business correctness failures? | No D7 business counters are valid after the native crash; no business PASS is claimed. Local/B2 bounded gates were reported separately and do not mask D7. | `UNAVAILABLE` for D7 |

## 3. Stabilization changes

- Explicit, shared `spawn` context across runtime, workers, writer, and IPC primitives.
- Child-local initialization and bounded role/process metadata in `backend/vnext/diagnostics.py`.
- Projection writer bounded flush interval for low-rate events plus incremental journal replay.
- Runtime control-state preservation across restart instead of overwriting valid pause/cancel state.
- Supervisor and projection watchdog restart backoff with exit-code reporting.
- Per-process route-credit serialization to prevent a burst of executor threads from contending on the same short-lived credit lock.
- Native diagnostic tool with `faulthandler`, optional `PYTHONMALLOC=debug`, spawn/fork comparison, loopback-only traffic, and no message/MSISDN/credential output.
- The official application package excludes tests, pytest, acceptance runners, perf guard, and the native diagnostic. Those files are delivered only in the separate test-only bundle.

No Kafka, Redis, RabbitMQ, legacy reliable/fast dual send path, real SMSC, public MSISDN, production database, system Python, global pip, systemd, or global VPN setting was introduced for this work.

## 4. Verification record

### 4.1 Local regression and native ext4 gates

| Evidence | Result |
|---|---|
| Windows full backend regression | `382 passed, 4 skipped, 4 warnings` |
| WSL AlmaLinux-8 native ext4 full backend regression | `384 passed, 2 skipped, 6 warnings` |
| Frontend `npm run build` | PASS; only existing Rolldown annotation/chunk-size warnings |
| Minimal native diagnostic, `spawn` | PASS; 16 accepted/received/response/journal-unique, control restored, child exit codes 0 |
| Minimal native diagnostic, `fork` | PASS with the same bounded counters; no SIGSEGV |
| Clean ext4 real-main R0 | PASS; 100/100 |
| Clean ext4 real-main R1 | PASS; 1000/1000 |
| Clean ext4 real-main R3 | PASS; 1000/1000, no route-credit lock errors after the per-process lock fix |
| Clean ext4 real-main R4/R5/R6 | PASS; writer restart, pause/resume/cancel/control restore, and worker restart all reconciled |
| Clean ext4 R7 exact run | PASS; `sent=4000`, target `4000`, loopback received/response `4000/4000`, projection `4000/4000/4000`, `taken_without_socket_written=0` |

The formal local runs used the WSL native filesystem, not `/mnt/d`. The exact R7 evidence recorded `/dev/sdd ext4`, CPython `3.11.5`, DuckDB `1.2.0`, `peak_rss_bytes=547536896`, `peak_fd_count=175`, `peak_run_root_bytes=10235267`, and `max_writer_lag_seconds=0.0`.

### 4.2 EulerOS B2, current task

The server run was isolated under:

`/home/paas/marketing-copilot-test/runs/vnext-native-sigsegv-rca-20261016/`

Verified B2 facts:

- EulerOS 2.0 SP15, non-root lowercase `paas`, ext4 `/dev/sda2`.
- Standalone CPython `3.11.5`, OpenSSL `1.1.1v`, `_ctypes` load, DuckDB `1.2.0`, `pip check` clean.
- Runtime was unpacked under the run root; no `/opt`, system Python, systemd, global pip, or global sysctl/core setting was changed.
- CPU list `0,1,2,3`, `nice +10`, `ionice -c3`, loopback-only synthetic SMSC, and perf guard limits were used.
- D0 native diagnostic: PASS, 10/10 loopback responses, no error.
- B2 R0/R1/R3/R4/R5: PASS; aggregate loopback `2720`, response success `2720`, socket-written `2720`; 20 `taken_without_socket_written` records correspond to explicit cancellation semantics.
- B2 R6/R7: command and perf guard returned PASS; R2/P5500 was not executed.
- Server evidence archive SHA256: `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`.
- `PHASE_C_READY_FOR_MANUAL_VPN_SWITCH` was created after process/core checks; the core list and remaining-process list were empty.

At the time this B2 subsection was first written, Stage C verification was pending. The completed Stage C receipt is recorded in Section 7 below.

## 5. Offline package boundary

The current local package directory is:

`test-artifacts/vnext-native-sigsegv-rca-20261016/stage-a/packages/`

| Package | SHA256 | Boundary |
|---|---|---|
| `marketing-copilot-sles12sp5-system-python-20261016-vnext-native-sigsegv-rca.tar.gz` | `7a9b4106d8dd7a86fa27670b8ff81ad0b3412342cecdc796ba137cd764a7fdc1` | Formal application package; no pytest, tests, tools, native diagnostic, DB/WAL, logs, uploads, backups, or credentials |
| `marketing-copilot-vnext-runtime-canary-test-20261016-native-sigsegv-rca.tar.gz` | `bb02f7e902fc7caac15a1ef906eae1c11b84cb5f1e5209bbf1c320698515b130` | Test-only pytest/canary/guard/diagnostic overlay; never merged into the formal package |
| `sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz` | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | Separate standalone CPython runtime |

Both application and test package internal manifests passed `sha256sum -c`. On EulerOS, the runtime's bundled pip was used with a `--without-pip --system-site-packages` venv because the formal runtime's `ensurepip` entry point returned 127; the interpreter and pip remained entirely inside the run-local standalone runtime, and all application/test wheels were installed with `--no-index`.

## 6. Final status

- `RCA_STRONGLY_INDICATED`
- Local stabilization gates A0–D6: PASS.
- EulerOS B2 bounded gates: PASS.
- `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL` is the current status: the one authorized post-fix D7/R2/P5500 run reproduced a guarded-command exit `-11`. No resource limit caused the failure, and no native faulting frame was captured, so the RCA remains strongly indicated rather than proved.
- No commit or push was performed. The completed Stage C archive verification and final report receipt are recorded in Section 7.

## 7. Stage C evidence receipt (completed)

The B2 archive was downloaded and verified locally after the server route became reachable:

- Local archive: `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence.tar.gz`
- SHA256: `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd` — `VERIFIED`.
- Safety audit: `863` tar members, `0` absolute/drive/`..` paths, `0` symlink or hardlink entries; extraction was isolated under `stage-c/server-b2-evidence-extracted/`.
- Internal manifest: `results/B2-files.sha256`, `670/670` file hashes verified locally — `VERIFIED`.
- Cleanup evidence: `core-files.txt` empty, `remaining-processes.txt` empty, `PHASE_C_READY_FOR_MANUAL_VPN_SWITCH` present.

The precise server R6/R7 receipt is now recorded: R6 `PASS` at `200/200`; R7 `PASS` at target/sent `4000/4000` in `611.721 s`, peak RSS `576663552 B`, peak FD `119`, peak run-root `10157749 B`, and maximum writer lag `0.0 s`. The combined R6/R7 loopback/response count was `4200/4200`, projection unique/taken/socket-written was `4200/4200/4200`, and `taken_without_socket_written=0`. The perf guard passed with maximum aggregate RSS `582787072 B`, disk delta `0 B`, and exit code `0`.

The standalone runtime and application/test offline installs remain verified. The formal application package still excludes the test-only pytest/diagnostic overlay and all runtime data. The detailed machine-readable receipt is `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/server-b2-evidence-verification.md`.

The final application package was rebuilt after the boundary audit: its SHA256 is `7a9b4106d8dd7a86fa27670b8ff81ad0b3412342cecdc796ba137cd764a7fdc1`; the archive has `195` members, contains the required release/frontend/run/checksum files, excludes `backend/tools`, tests, `*.bak*`, logs, DB/WAL, uploads, backups, and runtime data, and its internal checksum manifest passes. The test-only bundle remains separate with SHA256 `bb02f7e902fc7caac15a1ef906eae1c11b84cb5f1e5209bbf1c320698515b130`; the standalone runtime remains `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`.

`STAGE_C_EVIDENCE_VERIFIED` is complete. The overall closure is now `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL` because the one authorized post-fix D7/R2/P5500 run reproduced `SIGSEGV (-11)`; this receipt does not promote R2 to PASS. No commit or push was performed.

## 8. D7/R2/P5500 final diagnostic receipt

The single authorized post-fix D7 run used the frozen four-campaign contract `2500 + 2500 + 250 + 250 = 5500` with explicit `spawn` runtime, 4 workers, 64 partitions, route TPS `80`, 2 ms loopback response delay, and the shared-server guard (`0,1,2,3`, `nice +10`, `ionice -c3`, 2 GiB RSS, 5 GiB run-disk delta).

- D7 run root: `/home/paas/marketing-copilot-test/runs/vnext-native-sigsegv-rca-20261016-d7-r2-p5500`
- Final evidence archive: `test-artifacts/vnext-native-sigsegv-rca-20261016/stage-c/d7-r2-p5500-evidence-final.tar.gz`
- Internal `d7-evidence-final.sha256`: `7/7` entries verified locally — `VERIFIED`.
- Archive SHA256: `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e1`; 8 members, no unsafe paths or links — `VERIFIED`.
- Guard: `FAIL`, reason `guarded command exited with -11`, exit code `-11`, duration `34.361 s`, max aggregate RSS `559325184 B`, disk delta `0 B`.
- Process attribution: guard root PID `37226` was the standalone CPython canary command (84 threads in the last live sample) and became a zombie at the final sample; the guard did not identify a worker/writer native frame. Descendants `42553` and `42557` survived as the resource tracker and a spawn worker, which explains the explicit post-crash cleanup.
- `canary.json` was not produced because the main canary process terminated natively; `core-files.txt` was empty and no native frame was available.
- The guard ledger recorded two descendants reparented after the crash. Exact `/proc` verification identified them as the run's resource tracker and spawn worker; only PIDs `42553` and `42557` were sent `SIGTERM`, and the post-cleanup audit verified both absent. No third D7 run was attempted.

This current D7 reproduction changes the final closure from the earlier environment-blocked historical state to `FAIL`. It also shows that explicit `spawn` alone is not a complete fix: the remaining strongly indicated boundary is the broader native-backed long-lived runtime/API/DB/SMPP lifecycle, not a proven fork-only cause. It does not change `RCA_STRONGLY_INDICATED` to `RCA_PROVED` because the native faulting instruction remains unavailable.
