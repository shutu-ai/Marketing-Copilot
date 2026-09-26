#!/usr/bin/env bash
# Build a standalone SLES 12 SP5 Python runtime package from a previously
# validated SLES-compatible runtime bundle.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
SOURCE_PACKAGE=""
RUNTIME_DIR=""
PYTHON_VERSION="3.11.5"
BUILD_ID="$(date +%Y%m%d%H%M%S)"
INSTALL_PREFIX="/opt/marketing-copilot"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --source-package) SOURCE_PACKAGE=$2; shift 2 ;;
    --runtime-dir) RUNTIME_DIR=$2; shift 2 ;;
    --python-version) PYTHON_VERSION=$2; shift 2 ;;
    --build-id) BUILD_ID=$2; shift 2 ;;
    --install-prefix) INSTALL_PREFIX=$2; shift 2 ;;
    *)
      echo "Usage: $0 [--output DIR] [--source-package FILE | --runtime-dir DIR] [--python-version 3.11.5] [--build-id VALUE] [--install-prefix PATH]" >&2
      exit 2
      ;;
  esac
done

if [ -n "$RUNTIME_DIR" ]; then
  [ -x "$RUNTIME_DIR/bin/python3.11" ] || {
    echo "Python runtime is missing or not executable: $RUNTIME_DIR/bin/python3.11" >&2
    exit 1
  }
else
  if [ -z "$SOURCE_PACKAGE" ]; then
    SOURCE_PACKAGE=$(find "$ROOT/release" -maxdepth 1 \
      -name 'marketing-copilot-sles12sp5-*-full.tar.gz' -print -quit 2>/dev/null || true)
  fi
  [ -n "$SOURCE_PACKAGE" ] && [ -f "$SOURCE_PACKAGE" ] || {
    echo 'A validated SLES12SP5 runtime source package was not found.' >&2
    echo 'Pass --source-package or --runtime-dir pointing to a complete runtime.' >&2
    exit 1
  }
fi

for command in bash tar find sha256sum; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SOURCE_ROOT="$WORK/source"
STAGE_NAME="sles12sp5-python-${PYTHON_VERSION}-${BUILD_ID}"
STAGE="$WORK/$STAGE_NAME"
mkdir -p "$SOURCE_ROOT" "$STAGE/runtime/opt/marketing-copilot"
if [ -n "$RUNTIME_DIR" ]; then
  cp -a "$RUNTIME_DIR" "$STAGE/runtime/opt/marketing-copilot/python"
  SOURCE_LABEL="runtime-dir"
else
  tar -xzf "$SOURCE_PACKAGE" -C "$SOURCE_ROOT"
  SOURCE_RUNTIME=$(find "$SOURCE_ROOT" -path '*/runtime/opt/marketing-copilot/python' -type d -print -quit)
  [ -n "$SOURCE_RUNTIME" ] || {
    echo 'The source package does not contain runtime/opt/marketing-copilot/python.' >&2
    exit 1
  }
  cp -a "$SOURCE_RUNTIME" "$STAGE/runtime/opt/marketing-copilot/python"
  SOURCE_LABEL=$(basename "$SOURCE_PACKAGE")
fi
cp "$ROOT/deploy/install-sles12sp5-python-runtime.sh" "$STAGE/install.sh"
cp "$ROOT/deploy/README-sles12sp5-python-runtime.md" "$STAGE/README.md"
chmod 0755 "$STAGE/install.sh"

# Reject incomplete Python builds before creating a distributable package.
# In particular, pandas imports ctypes during startup, so _ctypes is a hard
# requirement and must be present in the standalone runtime itself.
PYTHON="$STAGE/runtime/opt/marketing-copilot/python/bin/python3.11"
"$PYTHON" - <<'PY'
import _ctypes
import ctypes
import ssl
import sys
import venv

if sys.version_info[:3] != (3, 11, 5):
    raise SystemExit(f"expected Python 3.11.5, got {sys.version}")
print(f"validated Python {sys.version.split()[0]}")
print(f"OpenSSL: {ssl.OPENSSL_VERSION}")
print(f"ctypes: {getattr(_ctypes, '__file__', 'built-in')}")
print("venv: available")
PY

cat > "$STAGE/runtime.json" <<JSON
{
  "name": "SLES12SP5 Python runtime",
  "python_version": "$PYTHON_VERSION",
  "architecture": "x86_64",
  "install_prefix": "$INSTALL_PREFIX/python",
  "build_id": "$BUILD_ID",
  "builder": "build-sles12sp5-python-runtime.sh",
  "source_package": "$SOURCE_LABEL"
}
JSON

(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | while IFS= read -r -d '' file; do
  sha256sum "$file"
done > checksums.sha256)

mkdir -p "$OUTPUT"
tar --owner=0 --group=0 --numeric-owner -C "$WORK" -czf "$OUTPUT/$STAGE_NAME.tar.gz" "$STAGE_NAME"
(cd "$OUTPUT" && sha256sum "$STAGE_NAME.tar.gz") > "$OUTPUT/$STAGE_NAME.tar.gz.sha256"
echo "Created $OUTPUT/$STAGE_NAME.tar.gz"
