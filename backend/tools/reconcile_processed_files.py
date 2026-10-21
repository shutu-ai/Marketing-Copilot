"""Check or repair the realtime processed-file display projection.

The tool reads ``ingestion_files`` as authority.  Repair only upserts the
scoped ``processed_files`` mirror and never changes leases, chunks, or SMS
tasks.  Keep it bounded with ``--campaign-id`` or ``--file-id`` in production.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import realtime_ingestion_state


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="report projection drift")
    mode.add_argument("--repair", action="store_true", help="repair the scoped projection")
    parser.add_argument("--campaign-id", type=int)
    parser.add_argument("--file-id", type=int)
    parser.add_argument("--filename", default="")
    args = parser.parse_args(argv)

    if args.repair and args.campaign_id is None and args.file_id is None and not args.filename:
        parser.error("--repair requires --campaign-id, --file-id, or --filename")

    # The production store has already been prepared by the stopped-service
    # cutover.  This maintenance command must not repeat schema/index work.
    database.init_runtime_db()
    result = realtime_ingestion_state.reconcile_processed_files(
        campaign_id=args.campaign_id,
        file_id=args.file_id,
        filename=args.filename,
        repair=args.repair,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if args.check:
        return 0 if result["consistent"] else 1
    return 0 if result["consistent"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
