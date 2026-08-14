#!/usr/bin/env bash
set -euo pipefail

APP_HOME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ENV_FILE="${MC_ENV_FILE:-$APP_HOME/app.env}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${MC_VENV_DIR:-$APP_HOME/venv}"
APP_MODULE="${APP_MODULE:-main:app}"

[ -f "$ENV_FILE" ] || { echo "Environment file not found: $ENV_FILE" >&2; exit 1; }
[ -d "$APP_HOME/offline/wheels" ] || { echo "Offline wheelhouse not found: $APP_HOME/offline/wheels" >&2; exit 1; }
[ -f "$APP_HOME/offline/requirements.txt" ] || { echo "Requirements file not found: $APP_HOME/offline/requirements.txt" >&2; exit 1; }

if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "Virtual environment not found; creating $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# systemd does not expand shell variables in EnvironmentFile values. Normalize
# deployment-relative paths before this file is sourced or handed to systemd.
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

set -a
. "$ENV_FILE"
set +a

mkdir -p "$APP_HOME/data" "$APP_HOME/logs" "$APP_HOME/run" "$APP_HOME/backups"
export MC_FRONTEND_DIR="${MC_FRONTEND_DIR:-$APP_HOME/frontend/dist}"
cd "$APP_HOME/backend"
exec "$VENV_DIR/bin/python" -m uvicorn "$APP_MODULE" --host "${MC_HOST:-0.0.0.0}" --port "${MC_PORT:-8000}" --workers 1
