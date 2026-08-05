#!/usr/bin/env bash
# Run on an internet-connected SLES 12 SP5-compatible WSL/Linux builder.
# The resulting archive runs on a disconnected SLES 12 SP5 x86_64 host; it has no Node runtime.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
PYTHON_SOURCE=""
OPENSSL_SOURCE=""
VERSION="$(date +%Y%m%d%H%M%S)"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --python-source) PYTHON_SOURCE=$2; shift 2 ;;
    --openssl-source) OPENSSL_SOURCE=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    *) echo "Usage: $0 [--output DIR] [--python-source FILE] [--openssl-source FILE] [--version VALUE]" >&2; exit 2 ;;
  esac
done
for command in python3 npm tar sha256sum curl; do command -v "$command" >/dev/null || { echo "Missing: $command" >&2; exit 1; }; done
BUILDER_GLIBC=$(ldd --version | head -n1 | sed -n 's/.* \([0-9]\+\.[0-9]\+\)$/\1/p')
[ -n "$BUILDER_GLIBC" ] || { echo 'Cannot determine builder glibc version.' >&2; exit 1; }
awk -v version="$BUILDER_GLIBC" 'BEGIN { exit !(version <= 2.22) }' || { echo "Builder glibc is $BUILDER_GLIBC. Use a SLES 12 SP5-compatible WSL rootfs (glibc <= 2.22), not a current Ubuntu WSL." >&2; exit 1; }
python3 - <<'PY'
import sys
assert sys.version_info[:2] == (3, 11), 'Use Python 3.11 on the builder.'
PY
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
STAGE="$WORK/marketing-copilot-sles12sp5-$VERSION"
mkdir -p "$STAGE/payload" "$STAGE/runtime" "$STAGE/offline/wheels/app" "$STAGE/offline/wheels/smsc-simulator"
if [ -z "$PYTHON_SOURCE" ]; then PYTHON_SOURCE="$WORK/Python-3.11.5.tgz"; curl -fL --proto '=https' --tlsv1.2 -o "$PYTHON_SOURCE" https://www.python.org/ftp/python/3.11.5/Python-3.11.5.tgz; fi
if [ -z "$OPENSSL_SOURCE" ]; then OPENSSL_SOURCE="$WORK/openssl-1.1.1w.tar.gz"; curl -fL --proto '=https' --tlsv1.2 -o "$OPENSSL_SOURCE" https://www.openssl.org/source/openssl-1.1.1w.tar.gz; fi
[ -f "$PYTHON_SOURCE" ] && [ -f "$OPENSSL_SOURCE" ] || { echo 'Source archives not found.' >&2; exit 1; }
BUILD_SOURCE="$WORK/source"; mkdir -p "$BUILD_SOURCE"
tar -xzf "$PYTHON_SOURCE" -C "$BUILD_SOURCE"
tar -xzf "$OPENSSL_SOURCE" -C "$BUILD_SOURCE"
# Build against the final /opt paths, then stage them with DESTDIR. Target hosts
# only copy these files; no compiler, headers, Node.js or network are needed.
( cd "$BUILD_SOURCE/openssl-1.1.1w"; ./config --prefix=/opt/marketing-copilot/python-deps/openssl --openssldir=/opt/marketing-copilot/python-deps/openssl shared; make -j"$(getconf _NPROCESSORS_ONLN)"; make install_sw DESTDIR="$STAGE/runtime" )
( cd "$BUILD_SOURCE/Python-3.11.5"; CPPFLAGS='-I/opt/marketing-copilot/python-deps/openssl/include' LDFLAGS='-Wl,-rpath,/opt/marketing-copilot/python-deps/openssl/lib -L/opt/marketing-copilot/python-deps/openssl/lib' ./configure --prefix=/opt/marketing-copilot/python --with-openssl=/opt/marketing-copilot/python-deps/openssl --with-openssl-rpath=auto --with-ensurepip=install; make -j"$(getconf _NPROCESSORS_ONLN)"; make altinstall DESTDIR="$STAGE/runtime" )
"$STAGE/runtime/opt/marketing-copilot/python/bin/python3.11" -c 'import ssl, sys; assert sys.version_info[:3] == (3, 11, 5); print(ssl.OPENSSL_VERSION)'

# Node/npm are used only here. Only the generated dist directory is copied into the package.
(cd "$ROOT/frontend" && npm ci && npm run build)
mkdir -p "$STAGE/payload/backend" "$STAGE/payload/frontend" "$STAGE/payload/deploy"
tar -C "$ROOT/backend" \
  --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='db_config.json' --exclude='logs' --exclude='server*.log' \
  --exclude='*.db*' --exclude='*.duckdb*' \
  --exclude='uploads' --exclude='monitor_dir' --exclude='data' \
  -cf - . | tar -C "$STAGE/payload/backend" -xf -
cp -a "$ROOT/frontend/dist" "$STAGE/payload/frontend/"
cp "$ROOT/deploy/marketing-copilot.env.example" "$ROOT/deploy/marketing-copilot.service" "$ROOT/deploy/marketing-copilot-backup.service" "$ROOT/deploy/marketing-copilot-backup.timer" "$ROOT/deploy/marketing-copilot-smsc-simulator.service" "$ROOT/deploy/polkit-marketing-copilot.rules" "$STAGE/payload/deploy/"

# manylinux2014 uses glibc 2.17, compatible with SLES 12 SP5 glibc 2.22.
# smpplib is pure Python and is built into a universal wheel here, so the target
# never has to process an sdist or invoke a compiler.
grep -v '^smpplib==' "$ROOT/backend/requirements.txt" > "$WORK/requirements-binary.txt"
python3 -m pip download --dest "$STAGE/offline/wheels/app" --only-binary=:all: --platform manylinux2014_x86_64 --implementation cp --python-version 311 --abi cp311 -r "$WORK/requirements-binary.txt"
python3 -m pip wheel --no-deps --wheel-dir "$STAGE/offline/wheels/app" smpplib==2.2.4
printf '%s\n' 'No third-party wheels: the SMSC simulator uses Python standard library only.' > "$STAGE/offline/wheels/smsc-simulator/README.txt"
cp "$ROOT/deploy/requirements-smsc-simulator.txt" "$STAGE/offline/wheels/smsc-simulator/"
cp "$ROOT/deploy/install-sles12sp5-offline.sh" "$STAGE/install.sh"; chmod 0755 "$STAGE/install.sh"
# Version metadata for the online-update feature.
GIT_SHA=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || true)
cat > "$STAGE/payload/backend/release.json" <<JSON
{
  "name": "Marketing Copilot",
  "version": "$VERSION",
  "build_time": "$(date -Is)",
  "builder": "build-sles12sp5-offline.sh",
  "git_sha": "$GIT_SHA"
}
JSON
cp "$STAGE/payload/backend/release.json" "$STAGE/release.json"
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/marketing-copilot-sles12sp5-$VERSION.tar.gz" "$(basename "$STAGE")"
echo "Created $OUTPUT/marketing-copilot-sles12sp5-$VERSION.tar.gz"
