#!/usr/bin/env bash
# Build the test-only offline bundle. It is deliberately separate from the
# formal application package and contains pytest wheels plus acceptance code,
# but no application runtime, application wheels, database, WAL, or evidence.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/test-artifacts/vnext-runtime-canary-closure-20261016/stage-a/packages/test"
WHEEL_DIR=""
VERSION="20261016-vnext-runtime-canary"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --pytest-wheel-dir) WHEEL_DIR=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    *) echo "Usage: $0 [--output DIR] --pytest-wheel-dir DIR [--version VALUE]" >&2; exit 2 ;;
  esac
done
[ -n "$WHEEL_DIR" ] && [ -d "$WHEEL_DIR" ] || { echo 'pytest wheel directory is required' >&2; exit 1; }
for wheel in \
  pytest-8.3.5-py3-none-any.whl \
  pluggy-1.5.0-py3-none-any.whl \
  iniconfig-2.0.0-py3-none-any.whl \
  packaging-24.2-py3-none-any.whl \
  pygments-2.18.0-py3-none-any.whl; do
  [ -f "$WHEEL_DIR/$wheel" ] || { echo "missing offline pytest wheel: $WHEEL_DIR/$wheel" >&2; exit 1; }
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="marketing-copilot-vnext-runtime-canary-test-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/backend/tests" "$STAGE/backend/tools" "$STAGE/wheels" "$STAGE/bin"
cp "$ROOT/backend/tests/conftest.py" "$STAGE/backend/tests/"
cp "$ROOT/backend/tests/test_vnext_runtime_canary_closure.py" "$STAGE/backend/tests/"
cp "$ROOT/backend/tools/perf_guard.py" "$STAGE/backend/tools/"
cp "$ROOT/backend/tools/vnext_loopback_acceptance.py" "$STAGE/backend/tools/"
cp "$ROOT/backend/tools/vnext_runtime_c0_repro.py" "$STAGE/backend/tools/"
cp "$ROOT/backend/tools/vnext_runtime_canary_acceptance.py" "$STAGE/backend/tools/"
cp "$ROOT/backend/tools/vnext_native_sigsegv_diagnostics.py" "$STAGE/backend/tools/"
cp "$ROOT/deploy/vnext-runtime-canary-test-runner.sh" "$STAGE/bin/run-vnext-runtime-canary.sh"
chmod 0755 "$STAGE/bin/run-vnext-runtime-canary.sh"
cp "$WHEEL_DIR"/*.whl "$STAGE/wheels/"
cat > "$STAGE/requirements-test.txt" <<'REQ'
pytest==8.3.5
pluggy==1.5.0
iniconfig==2.0.0
packaging==24.2
pygments==2.18.0
REQ
cat > "$STAGE/README.md" <<'README'
# VNext runtime canary — offline test-only bundle

This archive is a test-only overlay for the formal SLES12SP5-compatible
application package. It contains pytest and the runtime canary harness only;
it does not contain the standalone Python runtime, application wheels, a
database, WAL, Spool, logs, uploads, backups, credentials, or production
configuration. Do not copy it into the formal application deployment archive.

Install the wheels into the application venv without network access:

```sh
APP_VENV=/path/to/run-root/app/venv
"$APP_VENV/bin/python" -m pip install --no-index --no-cache-dir --find-links ./wheels -r requirements-test.txt
```

Run from a unique native Linux filesystem Run root. `--cpu-list` is required,
must name no more than four CPUs, and the launcher applies `nice +10`,
`ionice -c3`, aggregate RSS 2 GiB and Run-disk 5 GiB guardrails:

```sh
./bin/run-vnext-runtime-canary.sh \
  --app-root ./run-root/app \
  --run-root ./run-root \
  --python ./run-root/app/venv/bin/python \
  --cpu-list 0,1,2,3 \
  --gates r0,r1,r3,r4,r5,r6
```

The harness is loopback-only and uses synthetic data. R2/P5500 is not in the
default gate list and must never be added without explicit written
authorization and a fresh resource review. Server execution must use the
lowercase `paas` account, no root/systemd/global pip/external network, and a
Run directory under `/home/paas/marketing-copilot-test/runs/`.

The native-lifecycle diagnostic is available as
`backend/tools/vnext_native_sigsegv_diagnostics.py`; it is a bounded local
diagnostic only and does not prove a native stack without a core/native frame.
README
GIT_SHA=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)
WORKTREE_STATUS=$(git -C "$ROOT" status --porcelain --untracked-files=no 2>/dev/null || true)
if [ -n "$WORKTREE_STATUS" ]; then WORKTREE_STATE="dirty-existing-user-changes"; else WORKTREE_STATE="clean"; fi
cat > "$STAGE/bundle.json" <<JSON
{
  "name": "Marketing Copilot VNext runtime canary test-only bundle",
  "version": "$VERSION",
  "purpose": "offline pytest and bounded loopback runtime canary acceptance; never merge into the formal application deployment package",
  "source_commit": "$GIT_SHA",
  "worktree_state": "$WORKTREE_STATE",
  "target": {
    "os": "EulerOS 2.0 SP15 / SLES12SP5-compatible enterprise Linux",
    "architecture": "x86_64",
    "python": "CPython 3.11.5 / cp311",
    "wheel_policy": "manylinux2014_x86_64 / glibc 2.17-compatible"
  },
  "network_access": false,
  "formal_deployment_package_modified": false,
  "runtime_data_included": false,
  "r2_authorized": false
}
JSON
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar --owner=0 --group=0 --numeric-owner -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
(cd "$OUTPUT" && sha256sum "$NAME.tar.gz") > "$OUTPUT/$NAME.tar.gz.sha256"
echo "Created $OUTPUT/$NAME.tar.gz"
