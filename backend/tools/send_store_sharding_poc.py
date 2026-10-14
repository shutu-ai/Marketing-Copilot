"""Run the isolated Send Store sharding feasibility experiment.

Examples (offline only):
  python backend/tools/send_store_sharding_poc.py --output .tmp/send-store-poc --shards 1,4
  SHARDED_SEND_STORE=OFF python backend/tools/send_store_sharding_poc.py --check-disabled
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.send_store_sharding_poc import (  # noqa: E402
    SendStoreDisabled,
    ShardedSendStore,
    run_minimal_poc,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=".tmp/send-store-poc")
    parser.add_argument("--shards", default="1")
    parser.add_argument("--tasks-per-campaign", type=int, default=24)
    parser.add_argument("--check-disabled", action="store_true")
    args = parser.parse_args()

    if args.check_disabled:
        os.environ.setdefault("SHARDED_SEND_STORE", "OFF")
        try:
            store = ShardedSendStore(Path(args.output) / "disabled-check", 1)
            store.shard_for("realtime", 1)
        except SendStoreDisabled:
            print(json.dumps({"sharded_send_store": "OFF", "production_path_changed": False}))
            return 0
        finally:
            if "store" in locals():
                store.close()
        print(json.dumps({"sharded_send_store": "UNEXPECTEDLY_ON"}))
        return 1

    shard_counts = []
    for value in str(args.shards).split(","):
        count = max(1, int(value.strip()))
        if count not in shard_counts:
            shard_counts.append(count)
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    arms = {}
    for shard_count in shard_counts:
        arms[str(shard_count)] = run_minimal_poc(
            root / f"arm-{shard_count}shard", shard_count,
            max(1, int(args.tasks_per_campaign)),
        )
    result = {
        "experiment": "isolated_send_store_sharding_poc",
        "sharded_send_store": "EXPLICIT_POC_RUN",
        "production_path_changed": False,
        "output": str(root),
        "arms": arms,
    }
    (root / "result.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if all(item["status"] == "PASS" for item in arms.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
