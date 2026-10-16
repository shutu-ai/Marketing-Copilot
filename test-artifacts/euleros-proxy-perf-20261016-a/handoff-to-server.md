# Stage A → Stage B handoff

Status: `READY_FOR_SERVER_VPN_SWITCH`.

Final closure archive: `desktop/closure/marketing-copilot-euleros-proxy-perf-closure-d6b482f2-final-v3.tar.gz`; SHA256: `237d68dcdbeca0cc81606105a2cc71f3d767d097db4ae5a0b9a26c79a78de3ef`. Its sidecar is `desktop/closure/marketing-copilot-euleros-proxy-perf-closure-d6b482f2-final-v3.tar.gz.sha256`.

This handoff is for the actual EulerOS 2.0 SP15 host `10.202.245.70`, using the lowercase account `paas`. It is not a native-SLES verification. The accepted comparison label is `SLES_PROXY_PERFORMANCE_TARGET`.

Frozen source and artifacts:

- source: `d6b482f2a330570cf25d5d54802b8ba1bcee2b08`
- application archive: `app/marketing-copilot-sles12sp5-system-python-20261016-euleros-proxy-perf.tar.gz`, SHA256 `c3930f04156151c56503ffab89f95d5523938f351966f63086f96119cb707303`
- standalone runtime: `runtime/sles12sp5-python-3.11.5-20260917-script.tar.gz`, SHA256 `65bada65662af0109b597e5b46e8687b37f622517d264659b8deebb964bc403d`
- separate pytest/perf bundle: `pytest-test-only/pytest-linux-bundle-20261016-euleros-proxy-perf.tar.gz`, SHA256 `bcaf091e2f56392eebe578ec632c11c31721491d40f583bf7a331f834c0fb19c`
- DuckDB extension: `extensions/1.2.0/linux_amd64_gcc4/core_functions.duckdb_extension`, SHA256 `61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640`
- test guard: `perf/perf_guard.py`, SHA256 `a6ce6b1f43b4d9540684682c6dfaba47ade0c3737ad39d97717dc94319f16ecd`
- package wheelhouse input SHA256: `504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda`

The final closure archive is beside this directory as `../marketing-copilot-euleros-proxy-perf-closure-d6b482f2-final.tar.gz`; its authoritative SHA256 is in the adjacent `.tar.gz.sha256` sidecar. Verify that sidecar before transfer. Do not store the SSH password in a file or command history.

## Manual VPN boundary

Stop here until the user manually switches to the server VPN. No server connection or transfer is performed during Stage A. After Stage B, stop all Run-owned processes, export only reviewed evidence, and wait for the user to manually switch back to the GitHub VPN before Stage C.

## Transfer and B0 setup

Run the following from a POSIX shell with the server VPN active. Set `PACKAGE` to the closure archive and `PACKAGE_SHA256` to the value in its sidecar:

```sh
set -eu
PACKAGE=/mnt/d/dev-projects/MarketingCopilot/test-artifacts/euleros-proxy-perf-20261016-a/desktop/closure/marketing-copilot-euleros-proxy-perf-closure-d6b482f2-final-v3.tar.gz
PACKAGE_SHA256="$(awk '{print $1}' "$PACKAGE.sha256")"
BASE=/home/paas/marketing-copilot-test
RUN_ID="euleros-proxy-perf-$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN="$BASE/runs/$RUN_ID"

ssh paas@10.202.245.70 "set -eu; umask 077; test \"\$(id -u)\" -ne 0; test ! -e '$RUN'; mkdir -p '$RUN'/package '$RUN'/runtime-package '$RUN'/app-package '$RUN'/test-package '$RUN'/src '$RUN'/data '$RUN'/db '$RUN'/wal '$RUN'/journal '$RUN'/tmp '$RUN'/home '$RUN'/cache '$RUN'/pycache '$RUN'/duckdb-home '$RUN'/evidence '$RUN'/logs '$RUN'/runs '$RUN'/export; chmod 700 '$RUN'"
scp "$PACKAGE" "paas@10.202.245.70:$RUN/closure.tar.gz"
ssh paas@10.202.245.70 "set -eu; cd '$RUN'; printf '%s  %s\\n' '$PACKAGE_SHA256' closure.tar.gz | sha256sum -c -; tar -tzf closure.tar.gz >/dev/null; tar --no-same-owner --no-same-permissions --extract --file=closure.tar.gz --directory=package; test \"\$(find package -mindepth 1 -maxdepth 1 -type d | wc -l)\" -eq 1; find package -type b -o -type c -o -type p -o -type s | grep -q . && exit 20 || true; realpath package; df -T '$RUN'"
```

The archive is only a transfer container. The extracted closure, venv, source, database/WAL/journal, temporary files, HOME, cache, logs, and evidence remain below this new Run. Do not use `/tmp`, `/var`, system HOME, an old Run, root/sudo, systemd, global pip, or `run.sh`.

## Offline installation and T0 record

On the server, in a shell where `RUN` is set to the exact new Run:

```sh
set -eu
PKG=$(find "$RUN/package" -mindepth 1 -maxdepth 1 -type d -print -quit)
APP_TAR=$(find "$PKG/app" -maxdepth 1 -type f -name '*.tar.gz' -print -quit)
RT_TAR=$(find "$PKG/runtime" -maxdepth 1 -type f -name '*.tar.gz' -print -quit)
TEST_TAR=$(find "$PKG/pytest-test-only" -maxdepth 1 -type f -name '*.tar.gz' -print -quit)
sha256sum -c "$PKG/checksums.sha256"
mkdir -p "$RUN/app-package" "$RUN/runtime-package" "$RUN/test-package"
tar --no-same-owner --no-same-permissions -xzf "$APP_TAR" -C "$RUN/app-package"
tar --no-same-owner --no-same-permissions -xzf "$RT_TAR" -C "$RUN/runtime-package"
tar --no-same-owner --no-same-permissions -xzf "$TEST_TAR" -C "$RUN/test-package"
APP=$(find "$RUN/app-package" -mindepth 1 -maxdepth 1 -type d -print -quit)
RTPKG=$(find "$RUN/runtime-package" -mindepth 1 -maxdepth 1 -type d -print -quit)
TESTPKG=$(find "$RUN/test-package" -mindepth 1 -maxdepth 1 -type d -print -quit)
"$RTPKG/install.sh" --prefix "$RUN/runtime"
PY="$RUN/runtime/python/bin/python3.11"
test -x "$PY"
"$PY" --version > "$RUN/evidence/python-version.txt" 2>&1
"$PY" -m venv --without-pip "$RUN/venv"
PY="$RUN/venv/bin/python"
"$PY" -m ensurepip --upgrade
"$PY" -m pip install --no-index --no-cache-dir --find-links "$APP/offline/wheels" -r "$APP/offline/requirements.txt"
"$PY" -m pip install --no-index --no-cache-dir --find-links "$TESTPKG/wheels" -r "$TESTPKG/requirements-test.txt"
mkdir -p "$RUN/src"
cp -a "$APP/backend" "$RUN/src/"
cp -a "$APP/frontend" "$RUN/src/"
cp -a "$APP/offline" "$RUN/src/"
cp "$PKG/perf/perf_guard.py" "$RUN/src/backend/tools/perf_guard.py"
mkdir -p "$RUN/src/offline/extensions/1.2.0/linux_amd64_gcc4"
cp "$PKG/extensions/1.2.0/linux_amd64_gcc4/core_functions.duckdb_extension" "$RUN/src/offline/extensions/1.2.0/linux_amd64_gcc4/"
export HOME="$RUN/home" USERPROFILE="$RUN/home" TMPDIR="$RUN/tmp" TMP="$RUN/tmp" TEMP="$RUN/tmp"
export XDG_CACHE_HOME="$RUN/cache" PYTHONPYCACHEPREFIX="$RUN/pycache" DUCKDB_HOME="$RUN/duckdb-home"
export MC_SKIP_SYSTEMD_CHECK=1 MC_DUCKDB_OFFLINE_ONLY=1
export MC_DUCKDB_EXTENSION_DIR="$RUN/src/offline/extensions" PIP_NO_INDEX=1 PYTHONPATH="$RUN/src/backend"
export PATH="$RUN/venv/bin:$PATH"
umask 077
id -u > "$RUN/evidence/uid.txt"
uname -a > "$RUN/evidence/uname.txt"
getconf GNU_LIBC_VERSION > "$RUN/evidence/glibc.txt" 2>&1 || true
nproc > "$RUN/evidence/nproc.txt"
lscpu > "$RUN/evidence/lscpu.txt" 2>&1 || true
free -h > "$RUN/evidence/memory.txt" 2>&1 || true
swapon --show > "$RUN/evidence/swap.txt" 2>&1 || true
df -T "$RUN" > "$RUN/evidence/df.txt"
vmstat 1 3 > "$RUN/evidence/vmstat.txt" 2>&1 || true
iostat -xz 1 3 > "$RUN/evidence/iostat.txt" 2>&1 || true
ss -ltn > "$RUN/evidence/listeners-before.txt" 2>&1 || true
"$PY" -m pip freeze > "$RUN/evidence/pip-freeze.txt"
"$PY" -c 'import ctypes, duckdb, ssl, sys; print(sys.executable); print(sys.version); print(duckdb.__version__); print(ssl.OPENSSL_VERSION); ctypes.CDLL(None)' > "$RUN/evidence/imports.txt"
sha256sum "$RUN/src/offline/extensions/1.2.0/linux_amd64_gcc4/core_functions.duckdb_extension" > "$RUN/evidence/extension.sha256"
```

Select no more than four CPUs after T0. Use the standalone interpreter to derive a permitted list, and abort if it cannot set affinity:

```sh
CPU_LIST="$($PY -c 'import os; print(",".join(map(str, sorted(os.sched_getaffinity(0))[:4])))')"
test -n "$CPU_LIST"
taskset -c "$CPU_LIST" true
printf 'cpu_list=%s\n' "$CPU_LIST" > "$RUN/evidence/resource-policy.txt"
printf 'rss_limit_mb=2048\ndisk_limit_gb=5\nnice_increment=10\nionice_class=3\n' >> "$RUN/evidence/resource-policy.txt"
```

## Gated server commands

Do not run a gate until its predecessor has passed. The helper below puts every guard output and runner output under the current gate Run, samples the complete child process tree, uses low priority, and stops only recorded Run-owned PIDs:

```sh
GUARD="$RUN/src/backend/tools/perf_guard.py"
run_guard() {
  gate="$1"; timeout="$2"; shift 2
  root="$RUN/runs/$gate"
  mkdir -p "$root"
  "$PY" "$GUARD" \
    --run-root "$root" --output "$root/guard.json" \
    --samples "$root/resource-samples.jsonl" --stdout "$root/command.stdout.log" \
    --timeout-seconds "$timeout" --sample-seconds 2 \
    --rss-limit-mb 2048 --disk-limit-gb 5 --cpu-list "$CPU_LIST" \
    --nice-increment 10 --ionice-class 3 -- "$@"
  "$PY" -c 'import json,sys; p=json.load(open(sys.argv[1])); assert p["status"] == "PASS", p; assert p["exit_code"] == 0, p' "$root/guard.json"
}
```

E0, 10-task restart/loopback:

```sh
run_guard e0 180 "$PY" "$RUN/src/backend/tools/local_e2e_acceptance.py" \
  --scenario restart --gate full --events 10 --files 1 --expected-tasks 10 \
  --python "$PY" --send-mode reliable --dispatch-mode legacy --db-concurrency serialized \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast --task-workers 2 --ingest-workers 2 \
  --workers 2 --claim-batch 20 --timeout-seconds 120 --max-memory-mb 2048 \
  --keep-artifacts --run-root "$RUN/runs/e0/runner" --output "$RUN/runs/e0/runner.json"
```

E1, 100 tasks / 2 files:

```sh
run_guard e1 360 "$PY" "$RUN/src/backend/tools/local_e2e_acceptance.py" \
  --scenario stress-1k --gate full --events 100 --files 2 --expected-tasks 100 \
  --python "$PY" --send-mode reliable --dispatch-mode fair --db-concurrency serialized \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast --task-workers 2 --ingest-workers 2 \
  --workers 2 --claim-batch 20 --timeout-seconds 300 --max-memory-mb 2048 \
  --keep-artifacts --run-root "$RUN/runs/e1/runner" --output "$RUN/runs/e1/runner.json"
```

E2, 1,000 tasks / 10 files, only after E1 passes:

```sh
run_guard e2 360 "$PY" "$RUN/src/backend/tools/local_e2e_acceptance.py" \
  --scenario stress-1k --gate full --events 1000 --files 10 --expected-tasks 1000 \
  --python "$PY" --send-mode reliable --dispatch-mode fair --db-concurrency serialized \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast --task-workers 2 --ingest-workers 2 \
  --workers 2 --claim-batch 20 --timeout-seconds 300 --max-memory-mb 2048 \
  --keep-artifacts --run-root "$RUN/runs/e2/runner" --output "$RUN/runs/e2/runner.json"
```

E2b is conditional only: run a fixed sender-only or ingestion-only diagnostic if E2 evidence requires stage decomposition. It is not an independent E2E PASS and must not be used to bypass a failed E2.

E3 is the controlled OCC counterpart to E2. Keep all E2 parameters identical and change only `--db-concurrency occ`; stop on any unknown/duplicate/partial task or crash:

```sh
run_guard e3-occ 360 "$PY" "$RUN/src/backend/tools/local_e2e_acceptance.py" \
  --scenario stress-1k --gate full --events 1000 --files 10 --expected-tasks 1000 \
  --python "$PY" --send-mode reliable --dispatch-mode fair --db-concurrency occ \
  --pool-size 1 --smpp-window 10 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast --task-workers 2 --ingest-workers 2 \
  --workers 2 --claim-batch 20 --timeout-seconds 300 --max-memory-mb 2048 \
  --keep-artifacts --run-root "$RUN/runs/e3-occ/runner" --output "$RUN/runs/e3-occ/runner.json"
```

E4/P5500 is not an automatic next command. Run it only if E0–E3 passed, T0 shows the shared host is quiet, the current window is explicitly safe, and no existing business activity is affected. Use one serialized run first, with no timeout relaxation:

```sh
run_guard e4-serialized 420 "$PY" "$RUN/src/backend/tools/multi_campaign_fair_acceptance.py" \
  --python "$PY" --large-rows 2500 --small-rows 250 \
  --dispatch-mode fair --send-mode reliable --db-concurrency serialized \
  --pool-size 4 --smpp-window 50 --route-tps 200 --simulator-tps 0 \
  --simulator-mode fast --simulator-audit fast --timeout-seconds 300 \
  --max-memory-mb 2048 --keep-artifacts \
  --run-root "$RUN/runs/e4-serialized/runner" --output "$RUN/runs/e4-serialized/runner.json"
```

The route TPS is `submit_sm` PDU parts per second, not tasks per second. Never run 52K/102K, G4, or G6 from this handoff. If any guard, reconciliation, host-safety, or shared-window check fails, stop this Run, preserve evidence, and do not retry or escalate.

## Evidence export and VPN return boundary

After all permitted gates have fully shut down, verify no Run-owned PID remains. Copy only reviewed JSON, `run-manifest.json`, `preflight.json`, `resource-samples.jsonl`, gate summaries, post-shutdown audits, environment records, and `server-evidence.sha256` to `$RUN/export`. Exclude database files, WAL/journal files, uploads, credentials, and raw logs unless a reviewer explicitly approves a sanitized excerpt. Hash the export, then wait for the user to manually switch back to the GitHub VPN. Stage C will compare WSL and EulerOS using the same SHA/CLI/data/mode and will not call the server host native SLES.
