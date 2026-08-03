#!/usr/bin/env bash
# Build a manual-run package for a target that already provides Python 3.11.5.
# No Python runtime, systemd unit, Docker/Podman artifact, or Node runtime is included.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
VERSION="$(date +%Y%m%d%H%M%S)"
WHEELHOUSE=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    --wheelhouse) WHEELHOUSE=$2; shift 2 ;;
    *) echo "Usage: $0 [--output DIR] [--version VALUE] [--wheelhouse FILE]" >&2; exit 2 ;;
  esac
done
if [ -z "$WHEELHOUSE" ]; then
  WHEELHOUSE=$(find "$ROOT/release" -maxdepth 1 -name 'marketing-copilot-sles12sp5-wheelhouse-*.tar.gz' -print -quit)
fi
[ -n "$WHEELHOUSE" ] && [ -f "$WHEELHOUSE" ] || { echo 'Offline wheelhouse archive was not found. Build it first.' >&2; exit 1; }
[ -f "$ROOT/frontend/dist/index.html" ] || { echo 'frontend/dist is missing. Build the frontend first.' >&2; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="marketing-copilot-sles12sp5-system-python-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/backend" "$STAGE/frontend" "$STAGE/offline"
tar -xzf "$WHEELHOUSE" -C "$WORK"
SOURCE_WHEEL_DIR=$(find "$WORK" -path '*/wheels/app' -type d -print -quit)
[ -n "$SOURCE_WHEEL_DIR" ] || { echo 'Application wheel directory is missing from wheelhouse.' >&2; exit 1; }

tar -C "$ROOT/backend" --exclude='__pycache__' --exclude='*.pyc' --exclude='*.duckdb' --exclude='*.duckdb.wal' --exclude='uploads' --exclude='monitor_dir' -cf - . | tar -C "$STAGE/backend" -xf -
cp -a "$ROOT/frontend/dist" "$STAGE/frontend/"
cp -a "$SOURCE_WHEEL_DIR" "$STAGE/offline/wheels"
cp "$ROOT/backend/requirements.txt" "$STAGE/offline/requirements.txt"
cp "$ROOT/deploy/marketing-copilot.single-dir.env.example" "$STAGE/marketing-copilot.env.example"
cp "$ROOT/deploy/run-system-python-manual.sh" "$STAGE/run.sh"
cp "$ROOT/deploy/README-system-python-manual.md" "$STAGE/README.md"
chmod 0755 "$STAGE/run.sh"
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
echo "Created $OUTPUT/$NAME.tar.gz"
