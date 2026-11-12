#!/usr/bin/env bash
# Install the PostgreSQL 15 SLES12SP5 offline package without removing data.
set -euo pipefail

PACKAGE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PGDATA=/var/lib/pgsql/15/data
START=0

for arg in "$@"; do
  case "$arg" in
    --start) START=1 ;;
    --no-start) START=0 ;;
    *) echo "Unknown option: $arg" >&2; echo "Usage: $0 [--start|--no-start]" >&2; exit 2 ;;
  esac
done

die() { echo "ERROR: $*" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || die "run as root"
for command in bash sha256sum rpm systemctl find getent; do
  command -v "$command" >/dev/null || die "missing required command: $command"
done

cd "$PACKAGE_DIR"
sha256sum -c checksums.sha256

ARCH=$(uname -m)
[ "$ARCH" = "x86_64" ] || die "this package is for x86_64, got $ARCH"

if [ -r /etc/os-release ]; then
  . /etc/os-release
else
  die "/etc/os-release is missing"
fi
[ "${ID:-}" = "sles" ] || die "this package requires SLES 12 SP5 (ID=${ID:-unknown})"
[ "${VERSION_ID:-}" = "12.5" ] || die "this package requires SLES 12 SP5 (VERSION_ID=${VERSION_ID:-unknown})"

rpm --import offline/RPM-GPG-KEY-PGDG
rpm --checksig --verbose offline/rpms/*.rpm
# Do not use --nodeps: missing SLES base libraries must stop installation.
rpm -Uvh --replacepkgs offline/rpms/*.rpm
systemctl daemon-reload
getent passwd postgres >/dev/null 2>&1 || die "postgres user was not created by the RPM"

if [ -e "$PGDATA" ]; then
  [ -d "$PGDATA" ] || die "PGDATA exists but is not a directory: $PGDATA"
  if [ -f "$PGDATA/PG_VERSION" ]; then
    [ "$(tr -d '[:space:]' < "$PGDATA/PG_VERSION")" = "15" ] || {
      die "existing PGDATA is not PostgreSQL 15; no upgrade or deletion was attempted"
    }
  elif [ -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    die "PGDATA is non-empty but not initialized; no files were changed: $PGDATA"
  fi
fi

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  /usr/pgsql-15/bin/postgresql-15-setup initdb
fi

systemctl enable postgresql-15.service
if [ "$START" -eq 1 ]; then
  systemctl start postgresql-15.service
  systemctl is-active --quiet postgresql-15.service || {
    systemctl --no-pager --full status postgresql-15.service || true
    die "postgresql-15.service did not become active"
  }
  echo "PostgreSQL 15 is active."
else
  echo "PostgreSQL 15 is installed and initialized but not started."
  echo "Start it with: systemctl start postgresql-15.service"
fi
echo "PGDATA=$PGDATA"
echo "Local administrative login: sudo -iu postgres /usr/pgsql-15/bin/psql"
