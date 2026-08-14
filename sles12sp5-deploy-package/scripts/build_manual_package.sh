#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=""
BACKEND_DIR=""
FRONTEND_DIST=""
DEPLOY_DIR=""
OUTPUT=""
VERSION=""
WHEELHOUSE=""
REQUIREMENTS=""
ENV_EXAMPLE=""
RUN_SCRIPT=""
README=""

usage() {
  cat <<'EOF'
Usage: build_manual_package.sh [options]

Options:
  --project-root DIR       项目根目录，默认使用当前目录
  --backend-dir DIR        后端源码目录
  --frontend-dist DIR      已构建的前端 dist 目录
  --deploy-dir DIR         包含部署模板的目录
  --output DIR             压缩包输出目录
  --version VALUE          包版本，默认使用时间戳
  --wheelhouse FILE        离线 wheelhouse tar.gz
  --requirements FILE      后端 requirements.txt
  --env-example FILE       环境变量示例文件
  --run-script FILE        前台启动脚本
  --readme FILE            部署包 README
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --project-root) PROJECT_ROOT=$2; shift 2 ;;
    --backend-dir) BACKEND_DIR=$2; shift 2 ;;
    --frontend-dist) FRONTEND_DIST=$2; shift 2 ;;
    --deploy-dir) DEPLOY_DIR=$2; shift 2 ;;
    --output) OUTPUT=$2; shift 2 ;;
    --version) VERSION=$2; shift 2 ;;
    --wheelhouse) WHEELHOUSE=$2; shift 2 ;;
    --requirements) REQUIREMENTS=$2; shift 2 ;;
    --env-example) ENV_EXAMPLE=$2; shift 2 ;;
    --run-script) RUN_SCRIPT=$2; shift 2 ;;
    --readme) README=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage >&2; exit 2 ;;
  esac
done

PROJECT_ROOT=${PROJECT_ROOT:-$(pwd)}
PROJECT_ROOT=$(CDPATH= cd -- "$PROJECT_ROOT" && pwd)
BACKEND_DIR=${BACKEND_DIR:-"$PROJECT_ROOT/backend"}
FRONTEND_DIST=${FRONTEND_DIST:-"$PROJECT_ROOT/frontend/dist"}
DEPLOY_DIR=${DEPLOY_DIR:-"$PROJECT_ROOT/deploy"}
OUTPUT=${OUTPUT:-"$PROJECT_ROOT/release"}
REQUIREMENTS=${REQUIREMENTS:-"$BACKEND_DIR/requirements.txt"}
ENV_EXAMPLE=${ENV_EXAMPLE:-"$DEPLOY_DIR/marketing-copilot.single-dir.env.example"}
RUN_SCRIPT=${RUN_SCRIPT:-"$DEPLOY_DIR/run-system-python-manual.sh"}
README=${README:-"$DEPLOY_DIR/README-system-python-manual.md"}

[ -d "$BACKEND_DIR" ] || { echo "后端目录不存在: $BACKEND_DIR" >&2; exit 1; }
[ -f "$FRONTEND_DIST/index.html" ] || { echo "前端 dist 不存在: $FRONTEND_DIST" >&2; exit 1; }
[ -f "$REQUIREMENTS" ] || { echo "requirements.txt 不存在: $REQUIREMENTS" >&2; exit 1; }
[ -f "$ENV_EXAMPLE" ] || { echo "环境变量示例文件不存在: $ENV_EXAMPLE" >&2; exit 1; }
[ -f "$RUN_SCRIPT" ] || { echo "启动脚本不存在: $RUN_SCRIPT" >&2; exit 1; }
[ -f "$README" ] || { echo "README 不存在: $README" >&2; exit 1; }

if [ -z "$WHEELHOUSE" ]; then
  WHEELHOUSE=$(find "$PROJECT_ROOT/cache/wheelhouse" -maxdepth 1 -name '*sles12sp5-wheelhouse-*.tar.gz' -print -quit 2>/dev/null || true)
fi
if [ -z "$WHEELHOUSE" ]; then
  WHEELHOUSE=$(find "$PROJECT_ROOT/release" -maxdepth 1 -name '*sles12sp5-wheelhouse-*.tar.gz' -print -quit 2>/dev/null || true)
fi
[ -n "$WHEELHOUSE" ] && [ -f "$WHEELHOUSE" ] || { echo '未找到离线 wheelhouse，请先构建依赖包。' >&2; exit 1; }

VERSION=${VERSION:-$(date +%Y%m%d%H%M%S)}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
NAME="sles12sp5-system-python-$VERSION"
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/backend" "$STAGE/frontend" "$STAGE/offline"

tar -xzf "$WHEELHOUSE" -C "$WORK"
SOURCE_WHEEL_DIR=$(find "$WORK" -path '*/wheels/app' -type d -print -quit)
[ -n "$SOURCE_WHEEL_DIR" ] || { echo 'wheelhouse 中缺少应用 wheel 目录。' >&2; exit 1; }

tar -C "$BACKEND_DIR" \
  --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='db_config.json' --exclude='logs' --exclude='server*.log' \
  --exclude='*.db*' --exclude='*.duckdb*' \
  --exclude='uploads' --exclude='monitor_dir' --exclude='data' \
  -cf - . | tar -C "$STAGE/backend" -xf -
cp -a "$FRONTEND_DIST" "$STAGE/frontend/"
cp -a "$SOURCE_WHEEL_DIR" "$STAGE/offline/wheels"
cp "$REQUIREMENTS" "$STAGE/offline/requirements.txt"
cp "$ENV_EXAMPLE" "$STAGE/app.env.example"
cp "$RUN_SCRIPT" "$STAGE/run.sh"
cp "$README" "$STAGE/README.md"
chmod 0755 "$STAGE/run.sh"

# 版本元数据：前端在线更新预检必需（backend/release.json）。
GIT_SHA=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || true)
cat > "$STAGE/backend/release.json" <<JSON
{
  "name": "Marketing Copilot",
  "version": "$VERSION",
  "build_time": "$(date -Is)",
  "builder": "sles12sp5-deploy-package:scripts/build_manual_package.sh",
  "git_sha": "$GIT_SHA"
}
JSON
cp "$STAGE/backend/release.json" "$STAGE/release.json"

(cd "$STAGE" && find . -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum > checksums.sha256)
mkdir -p "$OUTPUT"
tar -C "$WORK" -czf "$OUTPUT/$NAME.tar.gz" "$NAME"
echo "已生成 $OUTPUT/$NAME.tar.gz"