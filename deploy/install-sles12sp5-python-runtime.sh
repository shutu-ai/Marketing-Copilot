#!/usr/bin/env bash
# Install the standalone Python runtime without changing the system Python.
set -euo pipefail

INSTALL_PREFIX="/opt/marketing-copilot"
FORCE=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)
      [ "$#" -ge 2 ] || { echo '--prefix requires a path' >&2; exit 2; }
      INSTALL_PREFIX=$2
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    *)
      echo "Usage: $0 [--prefix PATH] [--force]" >&2
      exit 2
      ;;
  esac
done
[ -n "$INSTALL_PREFIX" ] || { echo '--prefix cannot be empty' >&2; exit 2; }

PACKAGE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PACKAGE_DIR"
for command in bash sha256sum install mktemp mv date; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done
sha256sum -c checksums.sha256

SOURCE="$PACKAGE_DIR/runtime/opt/marketing-copilot/python"
TARGET="$INSTALL_PREFIX/python"
[ -x "$SOURCE/bin/python3.11" ] || { echo "Python runtime is missing: $SOURCE/bin/python3.11" >&2; exit 1; }

if [ -e "$TARGET" ] && [ "$FORCE" -ne 1 ]; then
  echo "Python runtime already exists: $TARGET (use --force to replace it)" >&2
  exit 1
fi

umask 022
install -d -m 0755 "$INSTALL_PREFIX"
STAGING=$(mktemp -d "$INSTALL_PREFIX/.python-runtime.XXXXXX")
cleanup() { rm -rf "$STAGING"; }
trap cleanup EXIT
cp -a "$SOURCE/." "$STAGING/"

BACKUP=""
if [ -e "$TARGET" ]; then
  BACKUP="${TARGET}.backup.$(date +%Y%m%d%H%M%S)"
  mv "$TARGET" "$BACKUP"
fi
if ! mv "$STAGING" "$TARGET"; then
  if [ -n "$BACKUP" ] && [ ! -e "$TARGET" ]; then mv "$BACKUP" "$TARGET"; fi
  exit 1
fi
trap - EXIT

PYTHON="$TARGET/bin/python3.11"
"$PYTHON" - <<'PY'
import _ctypes
import ctypes
import ssl
import sys
import venv

expected = (3, 11, 5)
if sys.version_info[:3] != expected:
    raise SystemExit(f"expected Python 3.11.5, got {sys.version}")
print(f"Python {sys.version.split()[0]} installed")
print(f"OpenSSL: {ssl.OPENSSL_VERSION}")
print(f"ctypes: available ({getattr(_ctypes, '__file__', 'built-in')})")
print("venv: available")
PY

echo "Installed standalone Python runtime: $TARGET"
echo "This installer does not modify /usr/bin/python, PATH, systemd units, or project data."
if [ -n "$BACKUP" ]; then echo "Previous runtime preserved at: $BACKUP"; fi
