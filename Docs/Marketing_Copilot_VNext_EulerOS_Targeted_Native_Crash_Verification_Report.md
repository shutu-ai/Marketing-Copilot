# Marketing Copilot VNext EulerOS Targeted Native Crash Verification Report

Task ID: `MC-VNEXT-EULEROS-TARGETED-NATIVE-RCA-20261017`

Execution date: 2026-10-17

This report is new evidence. The historical D7/R2/P5500 report, the previous B2/R7 result, and `INTEGRATED_NO_GO` are unchanged.

## Verdict

The updated diagnostic/test-only candidate passed D0, R0, R1, and one bounded B2 real-main run on EulerOS 2.0 SP15. B2 crossed the 42-second elapsed boundary with exactly 300 synthetic messages and continuous loopback health polling. No SIGSEGV or other unaccounted exit occurred.

This is not evidence that the historical 2500-message boundary is safe. R2/P5500 was not run because the shared filesystem had only about 2.13 GB free after B2 while the permitted guard is 5 GiB, and the lighter B2 run would not provide a new discriminating explanation for the historical crash.

```text
SERVER_TEST_AUTHORIZATION_USED=YES
STATIC_AUDIT_STATUS=PASS
TARGETED_DIAGNOSTIC_STATUS=PASS
EULEROS_R2_STATUS=R2_NOT_RUN_GATED
NATIVE_FAULTING_FRAME=UNAVAILABLE
RCA_LEVEL=RCA_STRONGLY_INDICATED
VNEXT_RUNTIME_CANARY_CLOSURE=FAIL
INTEGRATED_STATUS=INTEGRATED_NO_GO
R2_DECISION=SKIP
```

## Source and package identity

The GitHub `origin/main` and local HEAD baseline was `8f14e82d57988559370cb1a978f99313c92d6754`. The worktree was already dirty; no unrelated changes were discarded. The formal application package records `8f14e82d-dirty`, while the separate test bundle records the exact baseline commit above.

| Item | SHA256 |
| --- | --- |
| formal application archive | `bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44` |
| standalone CPython 3.11.5 runtime archive | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` |
| B2-capable offline test archive | `32fd0fde2447be49ff48c487082ef700f7694c4e72d3ecb789302566f683c813` |
| returned sanitized evidence archive | `676b70352619035021dc6c3365a09314e6ddce1271e0d2bf86e035b83e1df0f4` |

The formal application archive contains no `backend/tests` or `backend/tools` entries. The B2 runner and pytest wheels exist only in the independent test archive. The runtime, application, and test dependencies were installed offline under the current Run root; no system Python, global pip, sudo, systemd, package manager, or external network was used.

Relevant source hashes:

| File | SHA256 |
| --- | --- |
| `backend/tools/vnext_runtime_canary_acceptance.py` | `f37b720283557e24aa57ffac6c2351ac76232e0ab6558a699430708c6b321edf` |
| `deploy/vnext-runtime-canary-test-runner.sh` | `3ae29d10d9983e081d706a62e9e26af6c74dea0bc0c6f2e5f3a7fadd56682d2b` |
| `deploy/build-vnext-runtime-canary-test-bundle.sh` | `918e5085d7f40bbd328ce2990f3e87ea86763f72df488b3dc4e75e300596fae1` |
| `backend/vnext/diagnostics.py` | `97549fe215547f7c8694163901d2ad33a614b2190c5371648c274d96545d6a9` |
| `backend/tests/test_vnext_euleros_targeted_launcher.py` | `e4fdacb0d422fc27dc022f3a53042fc58a12f6c045da6f1ec18d3e2abbea359b` |
| `backend/tests/test_vnext_static_audit.py` | `bd55b8834c61b15f7aebebee827ca2ea3494cbdbb3ffd319531f22f0f2925d9e` |

## Implementation completed

- The test-only launcher now validates and forwards the frozen D7 controls: route TPS, response delay, worker/partition counts, projection queue, in-flight/prefetch limits, watchdog, explicit CPU list, timeout, and positive `--authorize-p5500`.
- Requested gates and actually executed gates are persisted and must match. R2 remains disabled unless explicitly authorized; no R2 authorization was passed for this run.
- A test-only `b2` gate was added. It sends exactly 300 synthetic messages through the real FastAPI/Uvicorn/VNext runtime and then polls the real status endpoint until a 42-second total elapsed boundary. It records bounded health, phase, process, resource, and faulthandler evidence without per-message diagnostic logging.
- Role-specific native fault capture and child metadata remain separate from the formal package. The test bundle builder includes the static A/B helper and its focused test, while deployment-package exclusion checks remain green.

No business message-processing, SMPP window/TPS, DuckDB transaction, or production deployment behavior was changed for this task.

## Local validation

- Windows backend regression: `395 passed, 5 skipped, 4 warnings`; the unrelated timing-sensitive test was rerun separately and passed repeatedly.
- Frontend build: PASS.
- New test package offline suite: `20 passed`.
- Focused launcher/static audit suite: `15 passed, 1 skipped`.
- Disposable SIGSEGV guard simulation: guard preserved exit `-11`; the canary result was absent as expected and no orphan remained.
- Exact candidate package on native WSL ext4: local B2 PASS; 300 accepted/socket-written/received/responded, Projection 300 unique/taken/socket-written, pending zero, 42.001-second gate boundary.

Formal filesystem evidence recorded `df -T .`, CPython 3.11.5, and the dependency freeze on native ext4. The local and server application/test/runtime roots were separate from the source tree and from previous Run roots.

## EulerOS server evidence

Server: EulerOS 2.0 SP15 x86_64, kernel `6.6.0-72.0.0.76.h21510.1170.eulerosv2r15.x86_64`, unprivileged lowercase account `paas`, native `/dev/sda2` ext4. All application, runtime, venv, database, WAL, Spool, Journal, temporary files, logs, and diagnostics were under the unique Run root:

`/home/paas/marketing-copilot-test/runs/vnext-targeted-native-20261017-r3f7c2/`

The loopback SMSC and API bound to `127.0.0.1`. Each gate used at most CPUs `0,1,2,3`, `nice +10`, `ionice -c3`, aggregate RSS limit 2 GiB, disk-delta limit 5 GiB, and a bounded timeout. The scoped process audit was empty after every gate.

| Gate | Expected | Accepted | Socket written | Loopback received | Responses | Projection unique/taken/written | Pending | Peak RSS | Disk delta | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| D0 lifecycle smoke | 32 | 32 | — | 32 | 32 | — | 0 | 168,595,456 B | 0 B | PASS |
| R0 | 100 | 100 | 100 | 100 | 100 | 100 / 100 / 100 | 0 | 452,382,720 B | 0 B | PASS |
| R1 | 1,000 | 1,000 | 1,000 | 1,000 | 1,000 | 1,000 / 1,000 / 1,000 | 0 | 532,365,312 B | 0 B | PASS |
| B2 | 300 | 300 | 300 | 300 | 300 | 300 / 300 / 300 | 0 | 488,185,856 B | 0 B | PASS |

B2 used the historical diagnostic settings that were safe to retain: four Workers, 64 partitions, route TPS 80, SMPP response delay 2 ms, projection queue 128, max in-flight 32, prefetch 16, and watchdog enabled. The B2 gate duration was 42.000 seconds, with 37 successful health polls, writer lag 0, no duplicate/missing/unexpected Loopback destinations, and native faulthandler log files present but empty because no crash occurred. The outer canary duration was 46.819 seconds including orderly shutdown; the acceptance boundary itself was 42.000 seconds.

Guard result for B2: `PASS`, child exit `0`, `sample_count=34`, `max_aggregate_rss_bytes=488185856`, `max_disk_delta_bytes=0`. The post-run server free space was `2,131,308,544` bytes. No scoped `vnext_runtime_canary`, `perf_guard`, or current-Run processes remained.

## B3 decision and RCA limits

The machine-readable decision is `R2_DECISION=SKIP`. Before B2 the Run filesystem had `2,167,435,264` bytes free; after B2 it had `2,131,308,544` bytes free. That is below the 5 GiB permitted Run-disk guard and is not a safe margin for a 5,500-message R2. The B2 pass also intentionally avoids the historical 2,500-message peak, so it cannot distinguish the prior crash boundary. No R2/P5500 command was executed.

`gdb` was not available for a permitted native frame capture in the prepared environment; `coredumpctl` was only recorded as an available diagnostic capability and no core was requested or exported. Because B2 did not crash, there is no new faulting frame. Therefore `NATIVE_FAULTING_FRAME=UNAVAILABLE`; the evidence supports a targeted diagnostic pass, not `RCA_PROVED` or production readiness.

Historical comparison remains literal: the prior 5,500-message D7/R2 run exited `-11` around T+34.361 seconds without a canary result, while this run used 300 messages and crossed 42 seconds successfully. The old result remains the reason `RCA_LEVEL=RCA_STRONGLY_INDICATED` and `VNEXT_RUNTIME_CANARY_CLOSURE=FAIL` are retained.

## Exact commands

The server B2 command was the independent test-bundle launcher, with the formal application package and standalone venv as inputs:

```text
bin/run-vnext-runtime-canary.sh \
  --app-root <formal-app-package> \
  --run-root /home/paas/marketing-copilot-test/runs/vnext-targeted-native-20261017-r3f7c2/b2 \
  --python /home/paas/marketing-copilot-test/runs/vnext-targeted-native-20261017-r3f7c2/venv/bin/python \
  --cpu-list 0,1,2,3 --gates b2 --b2-seconds 42 --timeout-seconds 120
```

D0 used the test-only native lifecycle diagnostic with `--start-method spawn --count 16 --cycles 2`; R0 and R1 were launched separately with `--gates r0` and `--gates r1`, respectively, under the same route/worker/partition/queue/in-flight/prefetch/watchdog settings. No command included `--authorize-p5500`.

## Evidence return

The returned archive is a minimal, sanitized evidence set with 93 internal manifest rows and its own `checksums.sha256`. It contains environment/runtime records, package metadata and hashes, guard summaries/resource samples, per-gate canary summaries, parent/child process metadata, phase markers, safe role-specific faulthandler logs, and `r2-decision.json`. It excludes credentials, message text, MSISDNs, full DuckDB/WAL/Spool/Journal data, cores, and deployment runtime data.

The local copy was verified with the outer sidecar, every internal manifest row, package hashes, no traversal/link entries, and no runtime-data filename matches. The local archive is retained under the task-specific `test-artifacts` transfer directory and is not a Git deployment artifact.
