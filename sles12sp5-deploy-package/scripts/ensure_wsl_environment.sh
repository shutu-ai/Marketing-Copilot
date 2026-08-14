#!/usr/bin/env bash
set -euo pipefail

MODE=check
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

usage() {
  cat <<'EOF'
Usage: ensure_wsl_environment.sh [--check-only|--install] [--python-bin PATH]

Options:
  --check-only        仅检查 Python 3.11 是否可用（默认）
  --install           在当前发行版中安装 Python 3.11 和 pip
  --python-bin PATH   指定 Python 3.11 可执行文件
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check-only) MODE=check; shift ;;
    --install) MODE=install; shift ;;
    --python-bin) PYTHON_BIN=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_python_311() {
  local candidate=$1
  command -v "$candidate" >/dev/null 2>&1 || return 1
  "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)' >/dev/null 2>&1
}

select_python() {
  local candidates=("$PYTHON_BIN")
  if [ "$PYTHON_BIN" != "python3.11" ]; then candidates+=("python3.11"); fi
  if [ "$PYTHON_BIN" != "python3" ]; then candidates+=("python3"); fi
  local candidate
  for candidate in "${candidates[@]}"; do
    if is_python_311 "$candidate"; then
      PYTHON_BIN=$candidate
      return 0
    fi
  done
  return 1
}

if select_python; then
  if "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
    echo "OK:$PYTHON_BIN"
    exit 0
  fi
  if [ "$MODE" = "check" ]; then
    echo "MISSING_PIP:$PYTHON_BIN" >&2
    exit 4
  fi
else
  if [ "$MODE" = "check" ]; then
    echo "MISSING:$PYTHON_BIN" >&2
    exit 3
  fi
fi

install_python() {
  if command -v dnf >/dev/null 2>&1; then
    dnf -y install python3.11 python3.11-pip
  elif command -v yum >/dev/null 2>&1; then
    yum -y install python3.11 python3.11-pip
  elif command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y python3.11 python3.11-venv python3-pip
  else
    echo "不支持的发行版，请手动安装 Python 3.11 和 pip。" >&2
    exit 5
  fi
}

install_python

if ! select_python; then
  echo "安装后仍找不到 Python 3.11。" >&2
  exit 6
fi

if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
  "$PYTHON_BIN" -m ensurepip --upgrade || true
fi

"$PYTHON_BIN" -m pip --version >/dev/null || { echo "pip 安装失败" >&2; exit 7; }
echo "OK:$PYTHON_BIN"
