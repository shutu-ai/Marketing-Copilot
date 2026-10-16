#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
APP_ROOT=""
RUN_ROOT=""
PYTHON=""
CPU_LIST=""
MODE="canary"
TIMEOUT_SECONDS="900"
STABILITY_SECONDS=""
GATES="r0,r1,r3,r4,r5,r6"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --app-root) APP_ROOT=$2; shift 2 ;;
    --run-root) RUN_ROOT=$2; shift 2 ;;
    --python) PYTHON=$2; shift 2 ;;
    --cpu-list) CPU_LIST=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS=$2; shift 2 ;;
    --stability-seconds) STABILITY_SECONDS=$2; shift 2 ;;
    --gates) GATES=$2; shift 2 ;;
    *) echo "Usage: $0 --app-root DIR --run-root DIR --python PATH --cpu-list LIST [--mode canary|c0|pytest] [--gates LIST] [--stability-seconds N]" >&2; exit 2 ;;
  esac
done

[ -n "$APP_ROOT" ] && [ -d "$APP_ROOT/backend" ] || { echo 'app backend directory is required' >&2; exit 2; }
[ -n "$RUN_ROOT" ] || { echo 'run root is required' >&2; exit 2; }
[ -n "$PYTHON" ] && [ -x "$PYTHON" ] || { echo 'standalone application Python is required' >&2; exit 2; }
[ -n "$CPU_LIST" ] || { echo 'an explicit CPU list of no more than four CPUs is required' >&2; exit 2; }

mkdir -p "$RUN_ROOT/tmp" "$RUN_ROOT/home" "$RUN_ROOT/cache"
export TMPDIR="$RUN_ROOT/tmp"
export HOME="$RUN_ROOT/home"
export XDG_CACHE_HOME="$RUN_ROOT/cache"
export MC_APP_BACKEND_DIR="$APP_ROOT/backend"
export PYTHONPATH="$APP_ROOT/backend:$ROOT/backend:$ROOT/backend/tools${PYTHONPATH:+:$PYTHONPATH}"

case "$MODE" in
  canary)
    COMMAND=("$PYTHON" "$ROOT/backend/tools/vnext_runtime_canary_acceptance.py"
      --run-root "$RUN_ROOT/canary"
      --gates "$GATES"
      --output "$RUN_ROOT/canary.json")
    [ -n "$STABILITY_SECONDS" ] && COMMAND+=(--stability-seconds "$STABILITY_SECONDS")
    ;;
  c0)
    COMMAND=("$PYTHON" "$ROOT/backend/tools/vnext_runtime_c0_repro.py"
      --run-root "$RUN_ROOT/c0" --output "$RUN_ROOT/c0.json")
    ;;
  pytest)
    COMMAND=("$PYTHON" -m pytest "$ROOT/backend/tests/test_vnext_runtime_canary_closure.py" -q)
    ;;
  *) echo "unsupported mode: $MODE" >&2; exit 2 ;;
esac

exec "$PYTHON" "$ROOT/backend/tools/perf_guard.py" \
  --run-root "$RUN_ROOT/guard" \
  --output "$RUN_ROOT/guard/perf-guard.json" \
  --samples "$RUN_ROOT/guard/resource-samples.jsonl" \
  --stdout "$RUN_ROOT/guard/command.stdout.log" \
  --timeout-seconds "$TIMEOUT_SECONDS" \
  --sample-seconds 1 \
  --rss-limit-mb 2048 \
  --disk-limit-gb 5 \
  --cpu-list "$CPU_LIST" \
  --nice-increment 10 \
  --ionice-class 3 \
  -- "${COMMAND[@]}"
