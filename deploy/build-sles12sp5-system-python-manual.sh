#!/usr/bin/env bash
# Build an application package for a target with the standalone SLES12SP5
# Python 3.11.5 runtime installed at /opt/marketing-copilot/python.
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
  WHEELHOUSE=$(find "$ROOT/cache/wheelhouse" -maxdepth 1 -name 'marketing-copilot-sles12sp5-wheelhouse-*.tar.gz' -print -quit 2>/dev/null || true)
fi
if [ -z "$WHEELHOUSE" ]; then
  WHEELHOUSE=$(find "$ROOT/release" -maxdepth 1 -name 'marketing-copilot-sles12sp5-wheelhouse-*.tar.gz' -print -quit 2>/dev/null || true)
fi
[ -n "$WHEELHOUSE" ] && [ -f "$WHEELHOUSE" ] || {
  echo 'Offline wheelhouse archive was not found. Put it in cache/wheelhouse/ or pass --wheelhouse.' >&2
  exit 1
}
[ -f "$ROOT/frontend/dist/index.html" ] || { echo 'frontend/dist is missing. Build the frontend first.' >&2; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="marketing-copilot-sles12sp5-system-python-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/backend" "$STAGE/frontend" "$STAGE/offline" "$STAGE/bin"
tar -xzf "$WHEELHOUSE" -C "$WORK"
SOURCE_WHEEL_DIR=$(find "$WORK" -path '*/wheels/app' -type d -print -quit)
[ -n "$SOURCE_WHEEL_DIR" ] || { echo 'Application wheel directory is missing from wheelhouse.' >&2; exit 1; }

tar -C "$ROOT/backend" \
  --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='tests' \
  --exclude='tools' \
  --exclude='db_config.json' --exclude='logs' --exclude='server*.log' \
  --exclude='backups' --exclude='*.log' --exclude='*.bak*' --exclude='*.backup*' \
  --exclude='*.db*' --exclude='*.duckdb*' \
  --exclude='uploads' --exclude='monitor_dir' --exclude='data' \
  --exclude='.rt-smoke-*' --exclude='.tmp-*' \
  -cf - . | tar -C "$STAGE/backend" -xf -
cp -a "$ROOT/frontend/dist" "$STAGE/frontend/"
cp -a "$SOURCE_WHEEL_DIR" "$STAGE/offline/wheels"
cp "$ROOT/backend/requirements.txt" "$STAGE/offline/requirements.txt"
# DuckDB may auto-load core_functions while opening/replaying a WAL. Keep the
# matching signed extension inside the offline package so a fresh venv never
# depends on internet access or a previous user's ~/.duckdb cache.
DUCKDB_VERSION=$(awk -F'==' '$1 == "duckdb" { print $2; exit }' "$ROOT/backend/requirements.txt")
CORE_FUNCTIONS_EXTENSION="$ROOT/cache/duckdb-extensions/$DUCKDB_VERSION/linux_amd64_gcc4/core_functions.duckdb_extension"
[ -f "$CORE_FUNCTIONS_EXTENSION" ] || {
  echo "Matching offline DuckDB core_functions extension was not found: $CORE_FUNCTIONS_EXTENSION" >&2
  exit 1
}
mkdir -p "$STAGE/offline/extensions/$DUCKDB_VERSION/linux_amd64_gcc4"
cp "$CORE_FUNCTIONS_EXTENSION" "$STAGE/offline/extensions/$DUCKDB_VERSION/linux_amd64_gcc4/"
# Deployment text files must use Unix line endings. The repository may be
# checked out on Windows before the package is built in WSL/Linux.
normalize_text() { tr -d '\r' < "$1" > "$2"; }
# The deployment package intentionally omits the local acceptance and
# benchmark harnesses under backend/tools.  These production tools are part
# of the runtime architecture, however: run.sh uses the systemd installer,
# the online-update service starts apply_update.py, and the documented
# maintenance workflow uses the backup, deployment-check, and reconciliation
# commands, and the read-only VNext statistics observer.  Keep the set
# explicit so test/simulator tooling is not shipped.
PRODUCTION_TOOLS=(
  __init__.py
  apply_update.py
  backup_database.py
  deployment_check.py
  install_systemd_service.py
  reconcile_processed_files.py
  vnext_stats.py
)
mkdir -p "$STAGE/backend/tools"
for tool in "${PRODUCTION_TOOLS[@]}"; do
  normalize_text "$ROOT/backend/tools/$tool" "$STAGE/backend/tools/$tool"
done
normalize_text "$ROOT/deploy/marketing-copilot.single-dir.env.example" "$STAGE/marketing-copilot.env.example"
normalize_text "$ROOT/deploy/run-system-python-manual.sh" "$STAGE/run.sh"
normalize_text "$ROOT/deploy/README-system-python-manual.md" "$STAGE/README.md"
chmod 0755 "$STAGE/run.sh"
normalize_text "$ROOT/backend/tools/mcctl.py" "$STAGE/bin/mcctl.py"
normalize_text "$ROOT/deploy/marketing-copilot-cli" "$STAGE/bin/marketing-copilot-cli"
chmod 0755 "$STAGE/bin/marketing-copilot-cli"
# Version metadata for the online-update feature.
GIT_SHA=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || true)
if [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
  GIT_SHA="${GIT_SHA}-dirty"
fi
cat > "$STAGE/backend/release.json" <<JSON
{
  "name": "Marketing Copilot",
  "version": "$VERSION",
  "build_time": "$(date -Is)",
  "builder": "build-sles12sp5-system-python-manual.sh",
  "git_sha": "$GIT_SHA",
  "python_runtime": "/opt/marketing-copilot/python/bin/python3.11",
  "python_version": "3.11.5"
}
JSON
cp "$STAGE/backend/release.json" "$STAGE/release.json"
(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
# Normalize archive ownership so a package built on a host with an unmapped
# numeric UID/GID cannot carry that identity into a SLES deployment.
tar --owner=0 --group=0 --numeric-owner -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
(cd "$OUTPUT" && sha256sum "$NAME.tar.gz") > "$OUTPUT/$NAME.tar.gz.sha256"
echo "Created $OUTPUT/$NAME.tar.gz"
