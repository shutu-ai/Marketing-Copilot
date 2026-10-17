#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
APP_ROOT=""
RUN_ROOT=""
PYTHON=""
CPU_LIST=""
MODE="canary"
TIMEOUT_SECONDS="900"
STABILITY_SECONDS="0"
B2_SECONDS="42"
GATES="r0,r1,r3,r4,r5,r6"
AUTHORIZE_P5500=0
ROUTE_TPS="80"
RESPONSE_DELAY_MS="2"
WORKERS="4"
PARTITIONS="64"
PROJECTION_QUEUE="128"
MAX_INFLIGHT="32"
PREFETCH="16"
WATCHDOG="enabled"

usage() {
  echo "Usage: $0 --app-root DIR --run-root DIR --python PATH --cpu-list LIST [--mode canary|c0|pytest] [--gates LIST] [--authorize-p5500] [--route-tps N] [--response-delay-ms N] [--workers N] [--partitions N] [--projection-queue N] [--max-inflight N] [--prefetch N] [--watchdog enabled|disabled] [--timeout-seconds N] [--stability-seconds N] [--b2-seconds 40..45]" >&2
}

require_value() {
  [ "$#" -ge 2 ] || { usage; echo "missing value for $1" >&2; exit 2; }
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --app-root) require_value "$@"; APP_ROOT=$2; shift 2 ;;
    --run-root) require_value "$@"; RUN_ROOT=$2; shift 2 ;;
    --python) require_value "$@"; PYTHON=$2; shift 2 ;;
    --cpu-list) require_value "$@"; CPU_LIST=$2; shift 2 ;;
    --mode) require_value "$@"; MODE=$2; shift 2 ;;
    --timeout-seconds) require_value "$@"; TIMEOUT_SECONDS=$2; shift 2 ;;
    --stability-seconds) require_value "$@"; STABILITY_SECONDS=$2; shift 2 ;;
    --b2-seconds) require_value "$@"; B2_SECONDS=$2; shift 2 ;;
    --gates) require_value "$@"; GATES=$2; shift 2 ;;
    --authorize-p5500) AUTHORIZE_P5500=1; shift ;;
    --route-tps) require_value "$@"; ROUTE_TPS=$2; shift 2 ;;
    --response-delay-ms) require_value "$@"; RESPONSE_DELAY_MS=$2; shift 2 ;;
    --workers) require_value "$@"; WORKERS=$2; shift 2 ;;
    --partitions) require_value "$@"; PARTITIONS=$2; shift 2 ;;
    --projection-queue) require_value "$@"; PROJECTION_QUEUE=$2; shift 2 ;;
    --max-inflight) require_value "$@"; MAX_INFLIGHT=$2; shift 2 ;;
    --prefetch) require_value "$@"; PREFETCH=$2; shift 2 ;;
    --watchdog) require_value "$@"; WATCHDOG=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) usage; echo "unsupported option: $1" >&2; exit 2 ;;
  esac
done

[ -n "$APP_ROOT" ] && [ -d "$APP_ROOT/backend" ] || { echo 'app backend directory is required' >&2; exit 2; }
[ -n "$RUN_ROOT" ] || { echo 'run root is required' >&2; exit 2; }
[ -n "$PYTHON" ] && [ -x "$PYTHON" ] || { echo 'standalone application Python is required' >&2; exit 2; }
[ -n "$CPU_LIST" ] || { echo 'an explicit CPU list of no more than four CPUs is required' >&2; exit 2; }

is_nonnegative_number() {
  [[ "$1" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]]
}

is_positive_number() {
  is_nonnegative_number "$1" && awk "BEGIN { exit !($1 > 0) }"
}

is_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_integer() {
  is_integer "$1" && [ "$1" -gt 0 ]
}

is_in_range() {
  is_integer "$1" && [ "$1" -ge "$2" ] && [ "$1" -le "$3" ]
}

is_nonnegative_number "$TIMEOUT_SECONDS" || { echo 'timeout must be a non-negative number' >&2; exit 2; }
is_positive_number "$TIMEOUT_SECONDS" || { echo 'timeout must be greater than zero' >&2; exit 2; }
is_nonnegative_number "$STABILITY_SECONDS" || { echo 'stability-seconds must be a non-negative number' >&2; exit 2; }
is_nonnegative_number "$B2_SECONDS" || { echo 'b2-seconds must be a non-negative number' >&2; exit 2; }
is_positive_number "$ROUTE_TPS" || { echo 'route-tps must be greater than zero' >&2; exit 2; }
is_nonnegative_number "$RESPONSE_DELAY_MS" || { echo 'response-delay-ms must be non-negative' >&2; exit 2; }
is_in_range "$WORKERS" 1 4 || { echo 'workers must be an integer in 1..4' >&2; exit 2; }
is_in_range "$PARTITIONS" 16 256 || { echo 'partitions must be an integer in 16..256' >&2; exit 2; }
is_positive_integer "$PROJECTION_QUEUE" && [ "$PROJECTION_QUEUE" -ge 128 ] || { echo 'projection-queue must be an integer >= 128' >&2; exit 2; }
is_in_range "$MAX_INFLIGHT" 1 256 || { echo 'max-inflight must be an integer in 1..256' >&2; exit 2; }
is_in_range "$PREFETCH" 1 128 || { echo 'prefetch must be an integer in 1..128' >&2; exit 2; }
case "$WATCHDOG" in enabled|disabled) ;; *) echo 'watchdog must be enabled or disabled' >&2; exit 2 ;; esac

validate_gates() {
  local raw="$1" item seen
  [ -n "$raw" ] || { echo 'gates must not be empty' >&2; exit 2; }
  IFS=',' read -r -a gate_items <<< "$raw"
  [ "${#gate_items[@]}" -gt 0 ] || { echo 'gates must contain at least one gate' >&2; exit 2; }
  seen=""
  for item in "${gate_items[@]}"; do
    case "$item" in r0|r1|r2|r3|r4|r5|r6|r7|b2) ;; *) echo "unsupported gate: $item" >&2; exit 2 ;; esac
    case ",${seen}," in *",$item,"*) echo "duplicate gate: $item" >&2; exit 2 ;; esac
    seen="${seen:+$seen,}$item"
  done
  case ",$seen," in
    *,r2,*) [ "$AUTHORIZE_P5500" -eq 1 ] || { echo 'R2 requires --authorize-p5500' >&2; exit 2; } ;;
  esac
  if is_positive_number "$STABILITY_SECONDS"; then
    case ",$seen," in *,r7,*) ;; *) echo 'stability-seconds > 0 requires explicit r7 in --gates' >&2; exit 2 ;; esac
  fi
  case ",$seen," in
    *,b2,*)
      awk "BEGIN { exit !($B2_SECONDS >= 40 && $B2_SECONDS <= 45) }" || { echo 'b2-seconds must be between 40 and 45' >&2; exit 2; }
      [ "$seen" = "b2" ] || { echo 'b2 must run as the only requested gate' >&2; exit 2; }
      ;;
  esac
}
validate_gates "$GATES"

mkdir -p "$RUN_ROOT/tmp" "$RUN_ROOT/home" "$RUN_ROOT/cache"
APP_ROOT=$(CDPATH= cd -- "$APP_ROOT" && pwd)
RUN_ROOT=$(CDPATH= cd -- "$RUN_ROOT" && pwd)
PYTHON=$(CDPATH= cd -- "$(dirname -- "$PYTHON")" && pwd)/$(basename -- "$PYTHON")
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
      --output "$RUN_ROOT/canary.json"
      --route-tps "$ROUTE_TPS"
      --response-delay-ms "$RESPONSE_DELAY_MS"
      --workers "$WORKERS"
      --partitions "$PARTITIONS"
      --projection-queue "$PROJECTION_QUEUE"
      --max-inflight "$MAX_INFLIGHT"
      --prefetch "$PREFETCH"
      --watchdog "$WATCHDOG")
    [ "$AUTHORIZE_P5500" -eq 1 ] && COMMAND+=(--authorize-p5500)
    [ -n "$STABILITY_SECONDS" ] && COMMAND+=(--stability-seconds "$STABILITY_SECONDS")
    COMMAND+=(--b2-seconds "$B2_SECONDS")
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

if [ "$MODE" = "canary" ]; then
  cat > "$RUN_ROOT/canary-launcher-request.json" <<JSON
{
  "requested_gates": [$(printf '"%s",' $(printf '%s' "$GATES" | tr ',' ' ' ) | sed 's/,$//')],
  "authorize_p5500": $([ "$AUTHORIZE_P5500" -eq 1 ] && echo true || echo false),
  "route_tps": $ROUTE_TPS,
  "response_delay_ms": $RESPONSE_DELAY_MS,
  "workers": $WORKERS,
  "partitions": $PARTITIONS,
  "projection_queue": $PROJECTION_QUEUE,
  "max_inflight": $MAX_INFLIGHT,
  "prefetch": $PREFETCH,
  "watchdog": "$WATCHDOG",
  "b2_seconds": $B2_SECONDS
}
JSON
fi

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
