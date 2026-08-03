#!/usr/bin/env bash
# Create the complete SLES 12 SP5 archive. The container is build-only: the
# target host runs neither Docker/Podman nor Node.js and compiles nothing.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
VERSION="$(date +%Y%m%d%H%M%S)"
IMAGE="quay.io/pypa/manylinux2014_x86_64"
ENGINE="${CONTAINER_ENGINE:-}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) OUTPUT=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    --image) IMAGE=$2; shift 2 ;;
    --engine) ENGINE=$2; shift 2 ;;
    *) echo "Usage: $0 [--output DIR] [--version VALUE] [--engine docker|podman] [--image IMAGE]" >&2; exit 2 ;;
  esac
done
if [ -z "$ENGINE" ]; then
  if command -v podman >/dev/null; then ENGINE=podman; elif command -v docker >/dev/null; then ENGINE=docker; else echo 'Docker or Podman is required on the builder.' >&2; exit 1; fi
fi
command -v "$ENGINE" >/dev/null || { echo "Container engine not found: $ENGINE" >&2; exit 1; }
for command in python3 npm curl tar sha256sum; do command -v "$command" >/dev/null || { echo "Missing builder command: $command" >&2; exit 1; }; done
python3 - <<'PY'
import sys
assert sys.version_info[:2] == (3, 11), 'Use Python 3.11 on the builder.'
PY

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="marketing-copilot-sles12sp5-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$WORK/sources" "$STAGE/payload/backend" "$STAGE/payload/frontend" "$STAGE/payload/deploy" "$STAGE/runtime" "$STAGE/offline/wheels/app" "$STAGE/offline/wheels/smsc-simulator"

curl -fL --proto '=https' --tlsv1.2 -o "$WORK/sources/Python-3.11.5.tgz" https://www.python.org/ftp/python/3.11.5/Python-3.11.5.tgz
curl -fL --proto '=https' --tlsv1.2 -o "$WORK/sources/openssl-1.1.1w.tar.gz" https://www.openssl.org/source/openssl-1.1.1w.tar.gz

# The official manylinux2014 image uses the glibc 2.17 compatibility baseline.
# Python and its private OpenSSL are staged under their final target paths.
"$ENGINE" pull "$IMAGE"
"$ENGINE" run --rm --user 0 -v "$WORK:/work:Z" "$IMAGE" bash -lc '
  set -euo pipefail
  mkdir -p /tmp/mc-build && cd /tmp/mc-build
  tar -xzf /work/sources/openssl-1.1.1w.tar.gz
  tar -xzf /work/sources/Python-3.11.5.tgz
  cd openssl-1.1.1w
  ./config --prefix=/tmp/mc-prefix/openssl --openssldir=/tmp/mc-prefix/openssl shared
  make -j"$(getconf _NPROCESSORS_ONLN)"
  make install_sw
  mkdir -p /work/runtime/opt/marketing-copilot/python-deps
  cp -a /tmp/mc-prefix/openssl /work/runtime/opt/marketing-copilot/python-deps/
  cd ../Python-3.11.5
  CPPFLAGS="-I/tmp/mc-prefix/openssl/include" LDFLAGS="-Wl,-rpath,/opt/marketing-copilot/python-deps/openssl/lib -L/tmp/mc-prefix/openssl/lib" ./configure --prefix=/opt/marketing-copilot/python --with-openssl=/tmp/mc-prefix/openssl --with-openssl-rpath=no --with-ensurepip=install
  make -j"$(getconf _NPROCESSORS_ONLN)"
  # setup.py imports extensions during install. The private OpenSSL is not yet
  # at its final /opt path inside the builder, so expose its build-time path.
  LD_LIBRARY_PATH=/tmp/mc-prefix/openssl/lib make altinstall DESTDIR=/work/runtime
  LD_LIBRARY_PATH=/work/runtime/opt/marketing-copilot/python-deps/openssl/lib /work/runtime/opt/marketing-copilot/python/bin/python3.11 -c "import ssl, sys; assert sys.version_info[:3] == (3, 11, 5); print(sys.version); print(ssl.OPENSSL_VERSION)"
'
# The container output is under the work mount; place it into the archive's
# runtime directory before computing checksums and creating the tarball.
cp -a "$WORK/runtime/." "$STAGE/runtime/"

# Node runs only on the builder. The archive gets frontend/dist, never node_modules.
(cd "$ROOT/frontend" && npm ci && npm run build)
tar -C "$ROOT/backend" --exclude='__pycache__' --exclude='*.pyc' --exclude='uploads' --exclude='monitor_dir' -cf - . | tar -C "$STAGE/payload/backend" -xf -
cp -a "$ROOT/frontend/dist" "$STAGE/payload/frontend/"
cp "$ROOT/deploy/marketing-copilot.env.example" "$ROOT/deploy/marketing-copilot.service" "$ROOT/deploy/marketing-copilot-backup.service" "$ROOT/deploy/marketing-copilot-backup.timer" "$ROOT/deploy/marketing-copilot-smsc-simulator.service" "$STAGE/payload/deploy/"

grep -v '^smpplib==' "$ROOT/backend/requirements.txt" > "$WORK/requirements-binary.txt"
python3 -m pip download --dest "$STAGE/offline/wheels/app" --only-binary=:all: --platform manylinux2014_x86_64 --implementation cp --python-version 311 --abi cp311 -r "$WORK/requirements-binary.txt"
python3 -m pip wheel --no-deps --wheel-dir "$STAGE/offline/wheels/app" smpplib==2.2.4
printf '%s\n' 'SMSC simulator: no third-party dependencies (Python standard library only).' > "$STAGE/offline/wheels/smsc-simulator/README.txt"
cp "$ROOT/deploy/requirements-smsc-simulator.txt" "$STAGE/offline/wheels/smsc-simulator/"
cp "$ROOT/deploy/install-sles12sp5-offline.sh" "$STAGE/install.sh"
chmod 0755 "$STAGE/install.sh"
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
echo "Created $OUTPUT/$NAME.tar.gz"
