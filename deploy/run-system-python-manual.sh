#!/usr/bin/env bash
# Manual foreground launcher; no systemd integration.
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

mkdir -p "$APP_HOME/data" "$APP_HOME/logs" "$APP_HOME/run" "$APP_HOME/backups"
export MC_FRONTEND_DIR="${MC_FRONTEND_DIR:-$APP_HOME/frontend/dist}"
cd "$APP_HOME/backend"
exec "$VENV_DIR/bin/python" -m uvicorn main:app --host "${MC_HOST:-0.0.0.0}" --port "${MC_PORT:-8000}" --workers 1
