#!/usr/bin/env bash
set -euo pipefail
source_root="${1:?repository root required}"
run_root="$(mktemp -d /tmp/mc-sqlite-projection-XXXXXXXX)"
printf 'RUN_ROOT=%s\n' "$run_root"
mkdir -p "$run_root/src/backend" "$run_root/wheels"
cp -a "$source_root/backend/vnext" "$source_root/backend/tools" "$run_root/src/backend/"
python3.11 -m venv "$run_root/venv"
tar -xzf "$source_root/cache/wheelhouse/marketing-copilot-sles12sp5-wheelhouse-20261021-duckdb122.tar.gz" -C "$run_root/wheels"
wheel="$(find "$run_root/wheels" -name 'duckdb-1.2.2-cp311-*.whl' -print -quit)"
export PIP_DISABLE_PIP_VERSION_CHECK=1
"$run_root/venv/bin/python" -m pip install --no-index --find-links "$(dirname "$wheel")" duckdb==1.2.2 numpy pandas
cd "$run_root/src"
df -T . | tee "$run_root/filesystem.txt"
"$run_root/venv/bin/python" --version | tee "$run_root/python.txt"
"$run_root/venv/bin/python" -m pip freeze | tee "$run_root/dependency-freeze.txt"
export PYTHONPATH="$run_root/src/backend"
"$run_root/venv/bin/python" -m tools.benchmark_projection_engines --root "$run_root/results" --events "${2:-100000}" | tee "$run_root/results.jsonl"
