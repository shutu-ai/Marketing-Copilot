"""Benchmark a realtime whitelist build using the production service path."""

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import realtime_campaign_service, realtime_whitelist_service


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("whitelist", type=Path)
    parser.add_argument("--output", type=Path, default=BACKEND_DIR / "uploads" / "realtime" / ".whitelist-benchmark")
    args = parser.parse_args()
    source = args.whitelist.resolve()
    output = args.output.resolve()
    if not source.is_file():
        raise SystemExit(f"whitelist does not exist: {source}")
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    database.set_database_file(str(output / "benchmark.duckdb"))
    database.init_db()
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO realtime_campaigns(name,status,use_whitelist,campaign_dir) VALUES (?,?,?,?) RETURNING id",
        ("whitelist-benchmark", "in_progress", True, str(output / "campaign")),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    started = time.perf_counter()
    uploaded = realtime_campaign_service.upload_customer_list(campaign_id, str(source), source.name)
    upload_seconds = time.perf_counter() - started
    started = time.perf_counter()
    built = realtime_whitelist_service.build_pending()
    build_seconds = time.perf_counter() - started
    version = realtime_whitelist_service.active_version(campaign_id)
    result = {
        "source": str(source),
        "source_bytes": source.stat().st_size,
        "upload_seconds": round(upload_seconds, 3),
        "build_seconds": round(build_seconds, 3),
        "build_jobs_completed": built,
        "row_count": uploaded.get("row_count", 0),
        "normalized_count": version.get("normalized_count", 0) if version else 0,
        "index_path": version.get("index_path", "") if version else "",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if built != 1 or not version or not os.path.isfile(version["index_path"]):
        raise SystemExit("benchmark did not produce a ready index")


if __name__ == "__main__":
    main()
