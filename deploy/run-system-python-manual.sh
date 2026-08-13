#!/usr/bin/env bash
# Manual foreground launcher. When run on a systemd host it optionally creates
# the corresponding systemd unit (if missing) so the service can later run as
# a managed background daemon; set MC_SKIP_SYSTEMD_CHECK=1 to skip this.
set -euo pipefail
APP_HOME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ENV_FILE="${MC_ENV_FILE:-$APP_HOME/marketing-copilot.env}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${MC_VENV_DIR:-$APP_HOME/venv}"
[ -f "$ENV_FILE" ] || { echo "Environment file not found: $ENV_FILE" >&2; exit 1; }
[ -d "$APP_HOME/offline/wheels" ] || { echo "Offline wheelhouse not found: $APP_HOME/offline/wheels" >&2; exit 1; }
[ -f "$APP_HOME/offline/requirements.txt" ] || { echo "Requirements file not found: $APP_HOME/offline/requirements.txt" >&2; exit 1; }

if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "Virtual environment not found; creating $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if ! "$VENV_DIR/bin/python" - <<'PY' >/dev/null 2>&1
import duckdb, fastapi, pandas, smpplib, uvicorn
PY
then
  echo "Installing Python dependencies from offline wheelhouse"
  "$VENV_DIR/bin/python" -m pip install --no-index --find-links "$APP_HOME/offline/wheels" -r "$APP_HOME/offline/requirements.txt"
fi

set -a
. "$ENV_FILE"
set +a

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
exec "$VENV_DIR/bin/python" -m uvicorn main:app --host "${MC_HOST:-0.0.0.0}" --port "${MC_PORT:-8000}" --workers 1