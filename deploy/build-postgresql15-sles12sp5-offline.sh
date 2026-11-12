#!/usr/bin/env bash
# Build an offline PostgreSQL 15 server package for SLES 12 SP5 x86_64.
# RPMs are taken from the official PostgreSQL SLES 12 repository.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT="$ROOT/release"
VERSION="$(date +%Y%m%d%H%M%S)"
PROXY="${HTTP_PROXY:-${http_proxy:-}}"
RPM_DIR="$ROOT/cache/postgresql15-sles12sp5-15.19"

# Keep the PostgreSQL minor version and PGDG release in sync with the RPM names.
PG_VERSION="15.19"
PG_RELEASE="420001PGDG.sles12"
ARCH="x86_64"
REPO_BASE="https://download.postgresql.org/pub/repos/zypp/15/suse/sles-12-x86_64"
RPM_FILES=(
  "postgresql15-${PG_VERSION}-${PG_RELEASE}.${ARCH}.rpm"
  "postgresql15-libs-${PG_VERSION}-${PG_RELEASE}.${ARCH}.rpm"
  "postgresql15-server-${PG_VERSION}-${PG_RELEASE}.${ARCH}.rpm"
  "postgresql15-contrib-${PG_VERSION}-${PG_RELEASE}.${ARCH}.rpm"
)
GPG_KEY_URL="https://download.postgresql.org/pub/repos/yum/keys/RPM-GPG-KEY-PGDG"

usage() {
  cat <<'EOF'
Usage: build-postgresql15-sles12sp5-offline.sh [options]

Options:
  --output DIR       archive output directory (default: release)
  --version VALUE    archive version (default: timestamp)
  --proxy URL        temporary download proxy; never written to the archive
  --rpm-dir DIR      use/download RPM inputs from this directory
  -h, --help         show this help
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) [ "$#" -ge 2 ] || { usage >&2; exit 2; }; OUTPUT=$2; shift 2 ;;
    --version) [ "$#" -ge 2 ] || { usage >&2; exit 2; }; VERSION=$2; shift 2 ;;
    --proxy) [ "$#" -ge 2 ] || { usage >&2; exit 2; }; PROXY=$2; shift 2 ;;
    --rpm-dir) [ "$#" -ge 2 ] || { usage >&2; exit 2; }; RPM_DIR=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

die() { echo "ERROR: $*" >&2; exit 1; }
for command in bash curl sha256sum rpm tar mktemp cp find sort xargs tr git date mkdir; do
  command -v "$command" >/dev/null || die "missing required command: $command"
done

mkdir -p "$RPM_DIR"
CURL_ARGS=(--fail --location --retry 3 --connect-timeout 20)
[ -n "$PROXY" ] && CURL_ARGS+=(--proxy "$PROXY")

download_if_missing() {
  local url=$1
  local target=$2
  if [ -s "$target" ]; then
    return
  fi
  echo "Downloading $(basename "$target")"
  rm -f "$target"
  curl "${CURL_ARGS[@]}" "$url" --output "$target"
  [ -s "$target" ] || die "download produced an empty file: $target"
}

for rpm_file in "${RPM_FILES[@]}"; do
  download_if_missing "$REPO_BASE/$rpm_file" "$RPM_DIR/$rpm_file"
done
download_if_missing "$GPG_KEY_URL" "$RPM_DIR/RPM-GPG-KEY-PGDG"

# Verify the PGDG signatures in an isolated RPM database; the build host's
# system keyring is not changed.
VERIFY_DB=$(mktemp -d)
WORK=$(mktemp -d)
cleanup() { rm -rf "$VERIFY_DB" "$WORK"; }
trap cleanup EXIT
rpm --dbpath "$VERIFY_DB" --import "$RPM_DIR/RPM-GPG-KEY-PGDG"
for rpm_file in "${RPM_FILES[@]}"; do
  rpm --dbpath "$VERIFY_DB" --checksig --verbose "$RPM_DIR/$rpm_file"
  metadata=$(rpm --dbpath "$VERIFY_DB" -qp --qf '%{NAME} %{VERSION}-%{RELEASE} %{ARCH}' "$RPM_DIR/$rpm_file")
  case "$rpm_file" in
    postgresql15-libs-*) expected="postgresql15-libs $PG_VERSION-$PG_RELEASE $ARCH" ;;
    postgresql15-server-*) expected="postgresql15-server $PG_VERSION-$PG_RELEASE $ARCH" ;;
    postgresql15-contrib-*) expected="postgresql15-contrib $PG_VERSION-$PG_RELEASE $ARCH" ;;
    postgresql15-*) expected="postgresql15 $PG_VERSION-$PG_RELEASE $ARCH" ;;
    *) die "unexpected RPM filename: $rpm_file" ;;
  esac
  [ "$metadata" = "$expected" ] || die "unexpected RPM metadata: $rpm_file ($metadata)"
done

TEMPLATE="$ROOT/deploy/postgresql15-sles12sp5"
[ -f "$TEMPLATE/install.sh" ] || die "missing package template: $TEMPLATE/install.sh"
[ -f "$TEMPLATE/README.md" ] || die "missing package template: $TEMPLATE/README.md"

NAME="postgresql15-sles12sp5-offline-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/offline/rpms"
for rpm_file in "${RPM_FILES[@]}"; do
  cp "$RPM_DIR/$rpm_file" "$STAGE/offline/rpms/"
done
cp "$RPM_DIR/RPM-GPG-KEY-PGDG" "$STAGE/offline/"
chmod 0644 "$STAGE/offline/RPM-GPG-KEY-PGDG" "$STAGE/offline/rpms"/*.rpm

normalize_text() { tr -d '\r' < "$1" > "$2"; }
normalize_text "$TEMPLATE/install.sh" "$STAGE/install.sh"
normalize_text "$TEMPLATE/README.md" "$STAGE/README.md"
chmod 0755 "$STAGE/install.sh"

GIT_SHA=$(git -C "$ROOT" -c core.autocrlf=true rev-parse --short HEAD 2>/dev/null || true)
if [ -n "$(git -C "$ROOT" -c core.autocrlf=true status --porcelain --untracked-files=no 2>/dev/null)" ]; then
  GIT_SHA="${GIT_SHA}-dirty"
fi
cat > "$STAGE/manifest.json" <<JSON
{
  "name": "PostgreSQL 15 SLES12SP5 offline server",
  "postgresql_version": "$PG_VERSION",
  "platform": "SLES 12 SP5 x86_64",
  "source": "PostgreSQL Global Development Group SLES 12 RPM repository",
  "source_url": "$REPO_BASE",
  "build_time": "$(date -Is)",
  "git_sha": "$GIT_SHA",
  "rpm_packages": [
    "${RPM_FILES[0]}",
    "${RPM_FILES[1]}",
    "${RPM_FILES[2]}",
    "${RPM_FILES[3]}"
  ]
}
JSON

(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar --owner=0 --group=0 --numeric-owner -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
(cd "$OUTPUT" && sha256sum "$NAME.tar.gz") > "$OUTPUT/$NAME.tar.gz.sha256"
echo "Created $OUTPUT/$NAME.tar.gz"
