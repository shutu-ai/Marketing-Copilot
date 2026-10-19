# Marketing Copilot VNext — EulerOS Native Crash Targeted Verification & RCA Closure

> **Codex implementation + controlled EulerOS server-testing task**  
> Task ID: `MC-VNEXT-EULEROS-TARGETED-NATIVE-RCA-20261017`  
> Authorized by user: **testing on the existing shared EulerOS server is permitted within the limits below**.  
> Baseline GitHub `origin/main`: **`8f14e82d57988559370cb1a978f99313c92d6754`**  
> Existing decisions: `STATIC_AUDIT_STATUS=PASS`; `RCA_LEVEL=RCA_STRONGLY_INDICATED`; `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`; historical `INTEGRATED_NO_GO` unchanged.  
> This task does **not** authorize production release, real SMSC, real phone numbers, service configuration changes, or unlimited high-load reruns.

## 0. Objective and decision philosophy

Determine whether the VNext static fixes committed in `8f14e82d` eliminate the previously observed **EulerOS `SIGSEGV (-11)` around T+34–35 seconds**; if not, obtain substantially better evidence of the faulting process/thread/phase and, where safely available, a **native** stack. Prioritize **diagnostic information gained per server run**, not blind load-test repetition.

The already shipped architecture remains: `MSISDN hash -> partitioned durable Spool -> supervised asynchronous SMPP Workers -> durable Progress Journal -> independent ProjectionWriter/DuckDB`. Do not reintroduce per-message DB claims, legacy dual-mode sends, Kafka/Redis/RabbitMQ, or a new scheduler/queue architecture.

### Known immutable evidence and limitations

- Latest static audit reports: `Docs/Marketing_Copilot_VNext_Static_Audit_and_Native_Crash_RCA_Report.md` and `Docs/Marketing_Copilot_VNext_Crash_Diagnostics_and_AB_Evidence.md`.
- Prior native RCA: `Docs/Marketing_Copilot_VNext_Native_SIGSEGV_RCA_and_Runtime_Stabilization_Report.md`; historical closure `Docs/Marketing_Copilot_VNext_Runtime_Canary_Closure_Report.md`.
- Previous D7/R2/P5500: 4 campaigns **2500 + 2500 + 250 + 250 = 5500**, explicit spawn, 4 Workers, 64 partitions, route TPS **80**, SMSC response delay **2 ms**, `projection_queue=128`, `max_inflight=32`, `prefetch=16`. Guarded CPython parent exited `-11` after **34.361 s**, aggregate RSS **559,325,184 B**, no resource-guard trip, `canary.json`/core/native faulting frame unavailable. D7 archive SHA256 `ed6157e4d786e7f13c3540a594ae0936212e30de4d9bbfbea0004bd154cd905e1`.
- Previous B2/R7: 4000/4000 PASS over ~612 seconds under a different, spread-out workload; B2 archive SHA256 `e9d0d35ea0deb3210fdd450ad8ad34252b30ab772b335f5f885543ed0f10bffd`.
- First R2 campaign alone has 2500 messages; at TPS 80 its ideal transmit floor is **31.25 seconds**, close to observed crash time. **Correlation only**, not proven completion-boundary causality.
- Static fix F01: watchdog no longer equates total run age with stall; F02: idle polling reuses lightweight Spool snapshot, `1000` queries `42.248379 s -> 0.413651 s` (~102x observer improvement, **not** SMS TPS); F03/F04: lower observer work and new role-specific native diagnostics. Windows tests `390 passed / 4 skipped` on last full pass; local targeted 16 PASS. These do not close the historical server failure.
- Prior R2 failures, D7 and B2 evidence remain immutable. Never overwrite the old report, alter historical counters, or infer `RCA_PROVED` from `faulthandler` Python frames alone.

## 1. Non-negotiable server authorization boundaries

1. **Existing shared server only**: EulerOS 2.0 SP15, unprivileged user **`paas`**. All server activity in an isolated new directory under `/home/paas/marketing-copilot-test/runs/`; use a unique, timestamped run ID; do not alter any previous run roots.
2. **Loopback only**: bind `127.0.0.1` for Uvicorn/SMSC simulator; synthetic numbers and messages only. No real SMSC, production database, public MSISDN, production traffic, or external messaging system.
3. **Resource constraints**: at most **four explicitly permitted CPUs** selected after `taskset`/`nproc`/affinity inspection; `nice +10`; `ionice -c3`; guard aggregate RSS **≤2 GiB**, per-run-root disk delta **≤5 GiB**; bounded wall-clock timeout and bounded sample output. Also verify current free space and require a safe margin; abort rather than filling the shared disk.
4. **OS integrity**: no `sudo`, root, global pip, system Python replacement, `yum/apt`, `systemctl`, kernel/sysctl changes, global core configuration, global environment or proxy changes, other users' process management, or auto-switching VPN. Use standalone CPython 3.11.5 and offline wheels from the verified separate application/runtime/test packages.
5. **Explicit high-load scope**: authorization covers the staged server tests described here and **at most one new, conditional R2/P5500 execution** from a fresh source/package snapshot, **only if all preceding gates pass**. There is no authorization for a second/third R2 after a failure, nor to increase workers/TPS/CPU/RSS/disk limits to force a PASS.
6. Only touch test-specific PIDs that are unambiguously owned by the current isolated run; check PID + command line + start time before cleanup. Never use broad `pkill -f`, `killall` or pattern-based termination on the shared server.
7. Respect repo `AGENTS.md`: prefer CodeGraph if present; preserve all unrelated dirty/untracked files; **do not commit or push** without separate explicit user request. No runtime evidence, credentials, logs or core data in Git.
8. Work in two manual VPN phases: build/review/hash while GitHub is reachable, then pause for manual network switch if required; run offline on EulerOS; return minimal sanitized evidence for local hash/manifest verification. Never attempt to manipulate the user's VPN.
9. If any prerequisite or permissions fail, mark `BLOCKED_ENV` with evidence and stop. If an existing bounded run crashes, stop the corresponding branch; do not automatically escalate.

## 2. Source-of-truth review — mandatory before server action

Read `AGENTS.md` and verify `git rev-parse HEAD`, `git rev-parse origin/main`, `git status --porcelain`, relevant diff and GitHub commit. Expect `8f14e82d...`; if the remote has advanced, record the actual SHA and inspect changed files before deriving a new candidate. Freeze an exact source manifest and hashes for every package; never blend modules from two commits.

Review these **actual committed** paths, not only previous conclusions:

- `backend/tools/vnext_runtime_canary_acceptance.py`: `_LoopbackThread`, `_UvicornThread`, `_Sampler`, `CanaryRunner.setup_environment/seed_database/start_api/wait_idle/gate_live/gate_r2/run`; phase markers and faulthandler entry.
- `backend/main.py` and `backend/database.py`: startup `init_db`, `start_stall_watchdog`, `_root_connection`, DuckDB owner/cursor lifetime and lock; shutdown/release ordering.
- `backend/vnext/runtime.py`: explicit `spawn`, long-lived spool owner, `_idle_snapshot`, worker state file, `_projection_watchdog` and journal-drain boundary.
- `backend/vnext/diagnostics.py`: one real faulthandler destination per **process**; inspect whether root+API roles in one PID overwrite each other's dump destination; verify file-handle lifetime to shutdown.
- `backend/vnext/worker.py`, `spool.py`, `projection_writer.py`, `supervisor.py`, `route_credits.py`, `smpp_transport.py`: crash-prone native/IPC/thread boundaries; ensure the child initializes its own native objects.
- `backend/tools/perf_guard.py` and `deploy/vnext-runtime-canary-test-runner.sh`: actual CLI arguments, timeouts, sampler/output paths, process-group cleanup and guard exit-code preservation.
- `deploy/build-vnext-runtime-canary-test-bundle.sh` and formal deployment builder: application/runtime/test bundle separation.

**Pre-identified tooling gap to close (test-only)**: the committed `deploy/vnext-runtime-canary-test-runner.sh` accepts `--gates` but currently does **not** pass through `--authorize-p5500`, `--route-tps`, `--response-delay-ms`, `--workers`, `--partitions`, `--projection-queue`, `--max-inflight`, `--prefetch`, or watchdog controls. An `--gates r2` invocation without authorization would silently filter R2 in the Python runner. Add explicit, validated, **test-only** options, a positive `--authorize-p5500` flag and an output declaration of **requested gates vs actually executed gates**. Reject omitted/mismatched gates as failure; never pretend R2 ran. Validate parameter compatibility with the actual Python parser, keep default R2 disabled, and test the launcher argument contract without contacting the server.

**Additional packaging gap**: the committed test-bundle builder does not currently copy `backend/tools/vnext_static_audit_ab.py` or `backend/tests/test_vnext_static_audit.py`. Include only the minimum necessary **test-only** tools for this task, update test manifest/checksums, and verify no testing files enter the **formal** package. If not needed on server, explicitly omit them and explain why.

## 3. Stage A — local evidence prep, packaging, diagnostic design (no server tests)

### A0 — Historical reconciliation and source freeze

- Verify historical D7/B2 archives when locally available (outer SHA256 + internal manifest + path traversal audit). If archives are unavailable, mark `UNAVAILABLE` and use committed reports; do not fabricate direct verification.
- Record `run_id`, source SHA, OS/Python/DuckDB/native wheel versions, exact flags, launcher SHA and three bundle SHA256 values. Diff the historical D7 arguments against the new invocation **field by field**, including route TPS/window/response delay, 4 Workers/64 partitions, queue/inflight/prefetch, idle polling, auth/gates and watchdog.
- Construct a short **30–45s causal timeline**: R2 first 2500-message stage, `wait_idle` start, throttled progress, 30-second watchdog tick and expected completion. Do not map historical `T+34.361` to an unseen function.

### A1 — Ensure diagnostic observability is real

- Verify faulthandler is enabled in **canary parent PID before `seed_database()`/`start_api()`**, and independently in each child. One PID must have a valid, writable, persistent destination for crash-time Python thread dumps; do not disable it accidentally through multiple role registrations, or close the file handle while running.
- Record single-line, **bounded** monotonic/UTC phase markers: `run_start`, `native_modules_ready`, `seed_begin/complete`, `api_start/started`, `r2_campaign_begin`, `batch_enqueued`, `wait_idle_begin`, `first_campaign_completed`, `projection_reconcile_begin/end`, `shutdown_begin/end`. Include PID, gate, campaign ID and counts only (no MSISDN, message content or credential).
- Capture crash-time last phase/PID independently of `canary.json`, because native SIGSEGV may prevent its creation. Ensure phase markers and process metadata are flushed durably at critical boundaries without causing one fsync per individual SMS.
- Diagnostic mode should log sampling timestamp, process tree, exited children/code, last bounded worker/projection counts, per-process FD/RSS/thread count, no-progress status, and whether faulthandler log was initialized. Do not make per-message synchronous logging or scan all Journal/Spool records on each sample.
- If existing `faulthandler` is only Python-visible, document its limits. **Native stack acquisition**: determine whether `gdb`, `coredumpctl` or pre-existing per-process debugger capability is permitted; prefer nonprivileged tools and a confined current-run process. Do not alter global `core_pattern`, sysctl or system dump policy. Optional shell-local core settings require free-space check and explicit guard accounting; if unavailable, mark `NATIVE_FRAME=UNAVAILABLE`.
- Failure simulation on a disposable process (not deliberate damage to the shared server) must prove the guard preserves `-11`/failed exit codes and that diagnostic outputs survive missing `canary.json`.

### A2 — Local validation

- Run focused tests for launcher parameter propagation, gate authorization, role-specific dump lifecycle, marker ordering, result-file/exit-code integrity, watchdog enabled/disabled, idle lightweight/full equivalence and journal/worker recovery.
- Re-run ext4 bounded R0/R1/R3 (or smaller representative gates if justified) from the **exact candidate package**, Loopback-only.
- Build formal application package, independent CPython runtime package and independent test package (reuse previously verified runtime only if contents/dependencies unchanged). Verify full SHA256 manifest plus deployment exclusions. Preserve existing 180-file/33-wheel composition as a reference, not a hard requirement if legitimate source changes alter counts.
- Generate a proposed remote test matrix with predicted disk/time/CPU usage. If test packaging, source identity or diagnostic capture fails, **stop before server execution**.

**A gate:** `LOCAL_PREP=PASS` only with a reproducible, hashed exact offline package set, working test launcher, verified real diagnostic path and matching configuration. Otherwise `BLOCKED_LOCAL`.

## 4. Stage B — EulerOS preflight and low-risk bounded verification

Use a new run root, e.g. `/home/paas/marketing-copilot-test/runs/vnext-targeted-native-20261017-<unique-id>/`; choose the actual fresh ID at execution time. Do not reuse or modify earlier B2/D7 directories.

### B0 — Host-safe preflight

- Confirm OS/architecture, `df -T` (native ext4), `df -h`, `ulimit`, CPU affinity, available disk, system load, user, standalone Python path, `pip check`, wheel versions and **no pre-existing owned test processes**.
- Verify all transfer package SHA256 and internal `checksums.sha256`, archive path safety (no traversal/link escapes), source SHA, release metadata, no tests/tools in the formal application package, and test bundle present only under test directory.
- Ensure all HOME/TMPDIR/XDG_CACHE_HOME/PYTHONPYCACHEPREFIX, DuckDB, WAL, Spool, Journal, logs, diagnostics and test outputs resolve under the current run root.
- Run guard with `--rss-limit-mb 2048 --disk-limit-gb 5 --cpu-list <verified <=4 CPUs> --nice-increment 10 --ionice-class 3` or exact equivalent, with explicit bounded `--timeout-seconds`. Record real binding/args and guard PID; never trust CLI flags alone.
- Confirm all network sockets used by the canary are on `127.0.0.1`, no production config/ports used. If server is under pressure, local free space is insufficient, or resource guard cannot track the process tree, stop with `BLOCKED_ENV`.

### B1 — Very small real-main gates

In separate run roots and under the same guard, execute **D0** native lifecycle smoke and **R0/R1** (or equivalent actual real-main small gates) using the updated package and configured diagnostics. Keep route TPS=80 and response delay 2 ms where applicable; record expected, accepted, socket-written, loopback received, response count, Projection unique/taken/socket-written and terminal states. Stop on any unexpected exit, missing diagnostic file, mismatch or lingering process.

### B2 — Targeted 30–45 second boundary, low load

Execute **one** bounded real-main diagnostic that crosses the **30–35-second elapsed-time boundary** without reproducing the full 2500-message peak: e.g. 300 synthetic SMS plus idle/continuous health activity until 40–45 seconds. Keep the exact diagnostic conditions as D7 where safe (watchdog enabled, 4 Workers, 64 partitions, route TPS=80, response delay=2ms, lightweight idle and separate Writer) and record last phase, faulthandler log state, worker/writer PID and resource samples.

If this exits -11, stop immediately; do not escalate to R2. If B2 passes, it **does not prove** the 2500-message boundary safe; it only rules out unconditional elapsed-time crash under that lighter workload.

### B3 — Review whether the high-load gate is justified

Before any R2: independently review evidence from B0–B2 for (1) exact SHA/config identity, (2) usable faulthandler/phase outputs, (3) no unexplained process failures or mismatched counters, (4) available disk/CPU headroom, (5) no orphan test processes, and (6) expected guard cleanup on termination.

Record a **machine-readable** `R2_DECISION=AUTHORIZED_ONCE` or `R2_DECISION=SKIP` with reason and counters. The user's authorization is the upper bound, not a mandate to run R2 if the diagnostic cannot provide new information. If skipped, close with `NATIVE_RCA_PENDING`, retaining prior `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL`.

## 5. Stage C — at most one controlled R2/P5500 (conditional)

Execute **only if B3 explicitly passed**, from a fresh run root. Exact historical R2 contract: **2500+2500+250+250**; real `main + FastAPI + Uvicorn + VNextRuntime.start`, online ingestion, **4 Workers, 64 partitions, TPS=80, response delay=2 ms, `projection_queue=128`, `max_inflight=32`, `prefetch=16`**. Do not mix unrelated optimizations or alternative database modes into the baseline. Use the updated lightweight idle path and capture root/child faulthandler/phase metadata.

- Require explicit launcher `--gates r2` **and** `--authorize-p5500`, and confirm both the requested gate and actual executed gate are R2. Verify canary output contains the R2 result or, after a crash, guard metadata proves the command executed; absent a result, it is a failure, never a PASS.
- Control total guard time and sample cardinality; cap at 2 GiB aggregate RSS and 5 GiB run-root delta. Guard can stop runaway tests early. A capacity/timing result may be `FAIL` without SIGSEGV; distinguish these cases.
- At approximately the first 2500-message boundary (anticipated after ≥31.25s at TPS 80), correlate the **last durable monotonic phase marker** with `wait_idle` intervals, Worker/PDU Window, Spool pending, writer offsets and API thread activity; do not infer function solely from elapsed time.
- If `-11` recurs: **stop**, preserve guard's actual exit code, process tree, stdout/stderr, bounded per-process samples, role logs, Python faulthandler dumps, optional permitted native core/stack and absent-output flags. Do not rerun. Do not claim the fault is DuckDB/Python/SMPP without an actual frame or controlled discriminating evidence.
- If no crash: require full `5500` expected, generated, `socket_written`, Loopback received/response and Projection unique counts all reconcile, pending zero, no duplicate sends, acceptable unknown/partial/cancelled semantics, no unbounded lag. **One R2 PASS is a bounded recovery indicator**, not production readiness or proof the historical fault has no recurrence.

**Failure stop**: any `SIGSEGV`, nonzero unaccounted exit, guard violation, unsafe disk/network exposure, or report mismatch ends server testing for this task immediately.

## 6. Stage D — two-VPN evidence return and final RCA judgement

1. Stop and audit the exact test PIDs; verify no orphans. Do not delete old archives or any data outside current run root. Report cleanup status and any permission-limited checks.
2. Prepare a **minimal redacted evidence set**: manifest, source/package hashes, environment/runtime versions, exact args, R2 decision, guard summary, resource samples, final per-gate counts, process metadata, monotonic phase markers, safe faulthandler logs, last-error/native capture availability. Never include message text, MSISDNs, secrets, full DB/WAL/Spool/Journal or cores in standard Git artifacts. If a core was captured, keep separate with restricted access; only export a sanitized native backtrace.
3. Create an archive with internal SHA256 manifest and outer archive SHA256, verify path safety/no traversal/link escape, and transfer it only after user handles the manual VPN switch. Verify archive and every manifest row locally after download.
4. Update a **new** report `Docs/Marketing_Copilot_VNext_EulerOS_Targeted_Native_Crash_Verification_Report.md`, and a focused evidence index. Keep all historical reports and historical `INTEGRATED_NO_GO` untouched. Include exact tested commit/tree, behavior of previous 8f14 fixes, baseline comparison, runtime symptoms and discovered faulting frame or explicit absence.
5. Final statuses must separately report: `SERVER_TEST_AUTHORIZATION_USED`, `STATIC_AUDIT_STATUS`, `TARGETED_DIAGNOSTIC_STATUS`, `EULEROS_R2_STATUS`, `NATIVE_FAULTING_FRAME`, `RCA_LEVEL`, `VNEXT_RUNTIME_CANARY_CLOSURE`, `INTEGRATED_STATUS`. Never automatically claim production ready or `RCA_PROVED`.

### Evidence-based final classification table

| Observed result | Required classification | Next permitted action |
|---|---|---|
| B0 blocked / instrumentation or package mismatch | `BLOCKED_ENV` / `BLOCKED_LOCAL` | Stop; no R2 |
| B1/B2 crashes | `TARGETED_DIAGNOSTIC_FAIL`, canary remains `FAIL` | Stop; analyze actual frame/marker, no R2 |
| R2 skipped by B3 | `R2_NOT_RUN_GATED`; canary remains historical `FAIL` | No claim of closure |
| R2 exits `-11`, no native frame | `R2_FAIL_SIGSEGV`, `RCA_STRONGLY_INDICATED` unless new evidence justifies change | Stop immediately; no second R2 |
| R2 exits `-11`, native faulting frame available | `R2_FAIL_SIGSEGV`, `RCA_FRAME_CAPTURED`; `RCA_PROVED` only if component/cause is genuinely established | Propose a separate small fix task, no further server rerun here |
| R2 ends without crash but count/time contract fails | `R2_FAIL_CORRECTNESS_OR_CAPACITY` | Diagnose; do not substitute a weaker PASS |
| R2 full 5500 PASS with exact configuration | `R2_BOUNDED_PASS` / `CANARY_REVALIDATION_CANDIDATE` | Report repeatability limitation; no automatic production go-live |

## 7. Codex implementation requirements and stop rules

- Implement real code/test changes **only where required for diagnostic correctness or the test-only launch contract**. Do not silently change message processing, SMPP Window/TPS, task IDs, DuckDB transaction semantics or input business behavior to get R2 to pass.
- Do not reduce checks, increase timeout to hide failures, change route TPS to `0` (unlimited), raise guard limits or exclude a crash from counters. No `try/except` may convert a native crash into a business `PASS`.
- Do not perform another static architecture rewrite; defer unrelated code hygiene/performance work with a clearly labelled follow-up section.
- After each stage, emit a short result + paths to evidence; if blocked, stop at that stage. Keep the task **fully reproducible** in one run rather than doing exploratory untracked server edits.
- Make a source/test-package parity table (file hashes and git SHA), explicit command list, and all meaningful gate results. If a CLI tool is missing a needed flag, change it in the local test-only code, rebuild the bundle and revalidate hashes **before** returning to the server.
- Avoid generating excessive logs/one-event-per-SMS diagnostics during tests. Sample at a bounded interval; the existing journal already contains durable per-message progress.
- `git diff --check`, targeted tests, backend regression where feasible, bundle inspection and artifact hash verification are required before reporting implementation complete. **No commit/push unless the user explicitly requests it later.**

## 8. Acceptance checklist

- [ ] Freeze `8f14e82d` or identify/document any newer exact SHA, source/package parity verified.
- [ ] Test launcher correctly passes every frozen D7 parameter and positively gates one-time R2 authorization; omitted/mismatched gates fail visibly.
- [ ] API root/worker/writer diagnostics survive abnormal exit; role/PID/phase/stack logs actually created and persist before shutdown.
- [ ] Server preflight confirms `paas`, native ext4, offline standalone runtime, loopback-only networking and guard CPU/RSS/disk boundaries.
- [ ] D0 / R0-R1 and one 30–45s light diagnostic recorded with correct counters and cleanup.
- [ ] R2 decision gate documented; **no automatic high-load execution**.
- [ ] If authorized by gates, **at most one** controlled R2/P5500 executed; exit code and final count reconciliation preserved literally.
- [ ] If native crash occurs, no further rerun; native frame available or explicitly `UNAVAILABLE`; observed facts separated from hypotheses.
- [ ] Sanitized evidence returned and locally verified with outer/inner SHA256 manifests; orphan process audit completed.
- [ ] New report and evidence index completed with truthful `RCA_LEVEL` and `VNEXT_RUNTIME_CANARY_CLOSURE` status.
- [ ] Original history, user dirty worktree and production deployment environment unchanged; no Git commit/push.

## 9. Copy-ready Codex starting instruction

> Execute `Marketing_Copilot_VNext_EulerOS_Targeted_Native_Crash_Verification_Codex_Task.md` against `shutu-ai/Marketing-Copilot` as a **real implementation and controlled server-test task**, not a plan-only report. The user authorizes **isolated, bounded tests on the existing EulerOS server** under the specified non-root, Loopback-only, CPU/RSS/disk and VPN boundaries. First inspect the latest GitHub source and current local worktree, fix the test-only launcher/package diagnostic gaps, verify source/package parity and native fault capture, then conduct B0→B1→B2. Only after the B3 decision gate passes may you perform **one** high-load R2/P5500 with exact frozen configuration. On any SIGSEGV stop immediately; do not perform blind reruns. Return immutable, hashed, sanitized evidence and a truthful RCA/Canary verdict. Never commit or push without another explicit instruction.
