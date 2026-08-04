#!/usr/bin/env bash
# Installs a package produced by build-sles12sp5-offline.sh. No network is used.
set -euo pipefail

PREFIX=/opt/marketing-copilot
SERVICE_USER=marketing-copilot
WITH_SMSC=0
for arg in "$@"; do
  case "$arg" in
    --with-smsc-simulator) WITH_SMSC=1 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

PACKAGE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PACKAGE_DIR"
sha256sum -c checksums.sha256
require() { command -v "$1" >/dev/null || { echo "Missing required command: $1" >&2; exit 1; }; }
for command in bash sha256sum; do require "$command"; done

if ! id "$SERVICE_USER" >/dev/null 2>&1; then useradd --system --home "$PREFIX" --shell /sbin/nologin "$SERVICE_USER"; fi
mkdir -p "$PREFIX"
cp -a payload/backend payload/frontend "$PREFIX/"
cp -a runtime/opt/marketing-copilot/python runtime/opt/marketing-copilot/python-deps "$PREFIX/"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 /var/lib/marketing-copilot /var/lib/marketing-copilot/backups /var/log/marketing-copilot /run/marketing-copilot
chown "$SERVICE_USER:$SERVICE_USER" "$PREFIX"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PREFIX/backend" "$PREFIX/frontend"

PYTHON="$PREFIX/python/bin/python3.11"
[ -x "$PYTHON" ] || { echo "Python 3.11.5 is missing: $PYTHON" >&2; exit 1; }
"$PYTHON" - <<'PY'
import sys
assert sys.version_info[:3] == (3, 11, 5), f"expected Python 3.11.5, got {sys.version}"
PY
"$PYTHON" -m venv "$PREFIX/venv"
"$PREFIX/venv/bin/pip" install --no-index --find-links "$PACKAGE_DIR/offline/wheels/app" -r "$PREFIX/backend/requirements.txt"

install -m 0640 payload/deploy/marketing-copilot.env.example /etc/marketing-copilot.env
chown root:"$SERVICE_USER" /etc/marketing-copilot.env
install -m 0644 payload/deploy/marketing-copilot.service /etc/systemd/system/marketing-copilot.service
install -m 0644 payload/deploy/marketing-copilot-backup.service /etc/systemd/system/marketing-copilot-backup.service
install -m 0644 payload/deploy/marketing-copilot-backup.timer /etc/systemd/system/marketing-copilot-backup.timer
if [ "$WITH_SMSC" -eq 1 ]; then install -m 0644 payload/deploy/marketing-copilot-smsc-simulator.service /etc/systemd/system/marketing-copilot-smsc-simulator.service; fi
# Best-effort polkit rule so the service account can manage its own unit for
# self-update (start/stop between the directory swap). Harmless if absent: the
# update runner then falls back to pid-based stop/start.
if [ -d /etc/polkit-1/rules.d ]; then
  install -m 0644 payload/deploy/polkit-marketing-copilot.rules /etc/polkit-1/rules.d/60-marketing-copilot.rules
  echo "Installed polkit rule for self-update."
fi
systemctl daemon-reload
systemctl enable marketing-copilot.service marketing-copilot-backup.timer
echo "Installed. Review /etc/marketing-copilot.env, then run: systemctl start marketing-copilot.service"
if [ "$WITH_SMSC" -eq 1 ]; then echo "Optional SMSC: systemctl enable --now marketing-copilot-smsc-simulator.service"; fi
