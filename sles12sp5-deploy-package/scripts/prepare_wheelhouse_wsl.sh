#!/usr/bin/env bash
set -euo pipefail

REQUIREMENTS=""
OUTPUT=""
VERSION=""
SMSC_REQUIREMENTS=""
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

usage() {
  cat <<'EOF'
Usage: prepare_wheelhouse_wsl.sh --requirements FILE --output DIR [options]

Options:
  --requirements FILE      后端 requirements.txt
  --output DIR             依赖包输出目录
  --version VALUE          归档版本，默认使用时间戳
  --smsc-requirements FILE SMSC 模拟器依赖文件（可选）
  --python-bin PATH        Python 3.11 可执行文件，默认 python3.11
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --requirements) REQUIREMENTS=$2; shift 2 ;;
    --output) OUTPUT=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    --smsc-requirements) SMSC_REQUIREMENTS=$2; shift 2 ;;
    --python-bin) PYTHON_BIN=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$REQUIREMENTS" ] || { echo '缺少 --requirements。' >&2; exit 2; }
[ -n "$OUTPUT" ] || { echo '缺少 --output。' >&2; exit 2; }
[ -f "$REQUIREMENTS" ] || { echo "requirements.txt 不存在: $REQUIREMENTS" >&2; exit 1; }
command -v "$PYTHON_BIN" >/dev/null || { echo "未找到 Python 可执行文件: $PYTHON_BIN" >&2; exit 1; }
"$PYTHON_BIN" - <<'PY'
import sys
assert sys.version_info[:2] == (3, 11), 'WSL/Linux 构建环境需要使用 Python 3.11。'
PY
"$PYTHON_BIN" -m pip --version >/dev/null || { echo "$PYTHON_BIN 中不可用 pip" >&2; exit 1; }

VERSION=${VERSION:-$(date +%Y%m%d%H%M%S)}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="sles12sp5-wheelhouse-$VERSION"
STAGE="$WORK/$NAME"
WHEELS="$STAGE/wheels/app"
mkdir -p "$WHEELS" "$STAGE/wheels/smsc-simulator"

grep -v '^smpplib==' "$REQUIREMENTS" > "$WORK/requirements-binary.txt"
"$PYTHON_BIN" -m pip download --dest "$WHEELS" --only-binary=:all: \
  --platform manylinux2014_x86_64 --implementation cp --python-version 311 --abi cp311 \
  -r "$WORK/requirements-binary.txt"

SMPP_VERSION=$(grep -E '^smpplib==' "$REQUIREMENTS" | head -1 | cut -d= -f3)
SMPP_VERSION=${SMPP_VERSION:-2.2.4}
"$PYTHON_BIN" -m pip wheel --no-deps --wheel-dir "$WHEELS" "smpplib==$SMPP_VERSION"

cp "$REQUIREMENTS" "$STAGE/requirements.txt"
if [ -n "$SMSC_REQUIREMENTS" ]; then
  [ -f "$SMSC_REQUIREMENTS" ] || { echo "SMSC 依赖文件不存在: $SMSC_REQUIREMENTS" >&2; exit 1; }
  cp "$SMSC_REQUIREMENTS" "$STAGE/wheels/smsc-simulator/"
fi
printf '%s\n' 'SMSC simulator: no third-party dependencies (Python standard library only).' > "$STAGE/wheels/smsc-simulator/README.txt"

(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
echo "已生成 $OUTPUT/$NAME.tar.gz"
