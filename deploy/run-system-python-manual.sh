#!/usr/bin/env bash
# Manual foreground launcher. When run on a systemd host it optionally creates
# the corresponding systemd unit (if missing) so the service can later run as
# a managed background daemon; set MC_SKIP_SYSTEMD_CHECK=1 to skip this.
set -euo pipefail
APP_HOME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ENV_FILE="${MC_ENV_FILE:-$APP_HOME/marketing-copilot.env}"
[ -f "$ENV_FILE" ] || { echo "Environment file not found: $ENV_FILE" >&2; exit 1; }
[ -d "$APP_HOME/offline/wheels" ] || { echo "Offline wheelhouse not found: $APP_HOME/offline/wheels" >&2; exit 1; }
[ -f "$APP_HOME/offline/requirements.txt" ] || { echo "Requirements file not found: $APP_HOME/offline/requirements.txt" >&2; exit 1; }

# Use the separately installed SLES12SP5 runtime before creating the project
# venv. PYTHON_BIN remains supported for one-off overrides; MC_PYTHON_BIN is
# the persistent deployment setting.
set -a
. "$ENV_FILE"
set +a
PYTHON_BIN="${MC_PYTHON_BIN:-${PYTHON_BIN:-/opt/marketing-copilot/python/bin/python3.11}}"
VENV_DIR="${MC_VENV_DIR:-$APP_HOME/venv}"
[ -x "$PYTHON_BIN" ] || {
  echo "Python 3.11.5 runtime not found or not executable: $PYTHON_BIN" >&2
  echo "Install the standalone SLES12SP5 Python package first, or set MC_PYTHON_BIN." >&2
  exit 1
}
"$PYTHON_BIN" - <<'PY'
import _ctypes
import ctypes
import ssl
import sys
import venv

if sys.version_info[:3] != (3, 11, 5):
    raise SystemExit(f"expected Python 3.11.5, got {sys.version}")
print(f"Using Python {sys.version.split()[0]}: {sys.executable}")
print(f"OpenSSL: {ssl.OPENSSL_VERSION}")
print("ctypes: available")
PY

if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "Virtual environment not found; creating $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  "$VENV_DIR/bin/python" - "$PYTHON_BIN" <<'PY'
import os
import sys

if sys.version_info[:3] != (3, 11, 5):
    raise SystemExit(f"existing venv is not Python 3.11.5: {sys.version}")
expected = os.path.realpath(sys.argv[1])
actual = os.path.realpath(getattr(sys, "_base_executable", ""))
if actual != expected:
    raise SystemExit(f"existing venv uses {actual or 'unknown base Python'}, expected {expected}")
PY
fi

# systemd EnvironmentFile entries are not evaluated by a shell, so values such
# as DUCKDB_PATH=$APP_HOME/data/... would otherwise reach the application with
# a literal "$APP_HOME" when an existing unit starts uvicorn directly. Expand
# those deployment-relative paths in the file before either sourcing it here or
# handing it to systemd. Keep a one-time backup for recovery/auditing.
"$VENV_DIR/bin/python" - "$ENV_FILE" "$APP_HOME" <<'PY'
import shutil
import sys
from pathlib import Path

env_file = Path(sys.argv[1])
app_home = sys.argv[2]
original = env_file.read_text(encoding="utf-8", errors="replace")
normalized = original.replace("${APP_HOME}", app_home).replace("$APP_HOME", app_home)
if normalized != original:
    backup = env_file.with_suffix(env_file.suffix + ".paths.bak")
    if not backup.exists():
        shutil.copy2(env_file, backup)
    env_file.write_text(normalized, encoding="utf-8")
    print(f"Expanded APP_HOME paths in {env_file} (backup: {backup})")
PY

if ! "$VENV_DIR/bin/python" - <<'PY' >/dev/null 2>&1
import duckdb, fastapi, pandas, smpplib, uvicorn
PY
then
  echo "Installing Python dependencies from offline wheelhouse"
  "$VENV_DIR/bin/python" -m pip install --no-index --find-links "$APP_HOME/offline/wheels" -r "$APP_HOME/offline/requirements.txt"
fi

mkdir -p "$APP_HOME/data" "$APP_HOME/logs" "$APP_HOME/run" "$APP_HOME/backups" "$APP_HOME/updates"
export MC_STATE_DIR="${MC_STATE_DIR:-$APP_HOME/data}"
export MC_UPDATE_DIR="${MC_UPDATE_DIR:-$APP_HOME/updates}"
export MC_UPDATE_MODE="${MC_UPDATE_MODE:-manual}"
export MC_FRONTEND_DIR="${MC_FRONTEND_DIR:-$APP_HOME/frontend/dist}"

# When invoked by systemd, this script must remain the managed foreground
# process only for legacy units that still use run.sh as ExecStart.
if [ "${MC_RUN_UNDER_SYSTEMD:-0}" = "1" ] || [ -n "${INVOCATION_ID:-}" ]; then
  :
elif [ "${MC_SKIP_SYSTEMD_CHECK:-0}" != "1" ] && command -v systemctl >/dev/null 2>&1; then
  MC_SERVICE_UNIT="${MC_SERVICE_UNIT:-marketing-copilot.service}"
  if systemctl cat "$MC_SERVICE_UNIT" >/dev/null 2>&1; then
    echo "systemd unit $MC_SERVICE_UNIT exists; starting it"
    if systemctl start "$MC_SERVICE_UNIT"; then
      exit 0
    fi
    echo "WARNING: failed to start $MC_SERVICE_UNIT; continuing in foreground." >&2
  elif [ "$(id -u)" -eq 0 ]; then
    echo "systemd unit $MC_SERVICE_UNIT not found; creating it."
    APP_OWNER=$(stat -c '%U' "$APP_HOME" 2>/dev/null || id -un)
    if (cd "$APP_HOME/backend" && "$VENV_DIR/bin/python" -m tools.install_systemd_service \
      --service-name "$MC_SERVICE_UNIT" \
      --env-file "$ENV_FILE" \
      --user "$APP_OWNER" \
      --update-mode systemd \
      --no-start --no-enable); then
      if systemctl start "$MC_SERVICE_UNIT"; then
        exit 0
      fi
      echo "WARNING: failed to start $MC_SERVICE_UNIT; continuing in foreground." >&2
    else
      echo "WARNING: failed to create the systemd unit; continuing in foreground." >&2
    fi
  else
    echo "Not running as root. Continuing in foreground; create the unit manually if needed." >&2
  fi
fi

cd "$APP_HOME/backend"
exec "$VENV_DIR/bin/python" -m uvicorn main:app --host "${MC_HOST:-0.0.0.0}" --port "${MC_PORT:-19090}" --workers 1
