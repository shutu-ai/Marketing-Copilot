#!/usr/bin/env bash
# Build only the offline Python dependency package. Safe to run in any
# internet-connected Linux/WSL distribution with Python 3.11; it downloads
# manylinux2014 wheels rather than building native extensions on the builder.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
VERSION="$(date +%Y%m%d%H%M%S)"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    *) echo "Usage: $0 [--output DIR] [--version VALUE]" >&2; exit 2 ;;
  esac
done

python3 - <<'PY'
import sys
assert sys.version_info[:2] == (3, 11), 'Use a Python 3.11 builder.'
PY
python3 -m pip --version >/dev/null
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="marketing-copilot-sles12sp5-wheelhouse-$VERSION"
STAGE="$WORK/$NAME"
WHEELS="$STAGE/wheels/app"
mkdir -p "$WHEELS" "$STAGE/wheels/smsc-simulator"

# The binary packages use the glibc 2.17 manylinux baseline, compatible with
# SLES 12 SP5 (glibc 2.22). Do not substitute locally-built Alma/Ubuntu wheels.
grep -v '^smpplib==' "$ROOT/backend/requirements.txt" > "$WORK/requirements-binary.txt"
python3 -m pip download --dest "$WHEELS" --only-binary=:all: \
  --platform manylinux2014_x86_64 --implementation cp --python-version 311 --abi cp311 \
  -r "$WORK/requirements-binary.txt"
# smpplib is pure Python. Creating its wheel on the builder avoids any target
# source-build step while keeping it separate from the SMSC simulator itself.
python3 -m pip wheel --no-deps --wheel-dir "$WHEELS" smpplib==2.2.4
cp "$ROOT/backend/requirements.txt" "$STAGE/requirements.txt"
cp "$ROOT/deploy/requirements-smsc-simulator.txt" "$STAGE/wheels/smsc-simulator/"
printf '%s\n' 'SMSC simulator: no third-party dependencies (Python standard library only).' > "$STAGE/wheels/smsc-simulator/README.txt"
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
echo "Created $OUTPUT/$NAME.tar.gz"
