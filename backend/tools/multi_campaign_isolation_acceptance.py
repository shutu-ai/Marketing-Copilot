"""Deterministic offline Track A reproduction; never contacts an SMSC."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database  # noqa: E402
from services import p1_service, sms_task_service  # noqa: E402


def _campaign(conn, name: str, status: str) -> int:
    row = conn.execute(
        """INSERT INTO batch_campaigns(name,status,start_time,end_time,
           send_start_time,send_end_time,send_days)
           VALUES (?,?, '', '', '', '', '[]') RETURNING id""",
        (name, status),
    ).fetchone()
    return int(row["id"])


def _insert_tasks(conn, campaign_id: int, count: int, prefix: str) -> None:
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,task_date,next_attempt_at)
           SELECT 'batch', ?, ?, '251' || CAST(900000000 + i AS VARCHAR),
                  'isolation-poc', ? || CAST(i AS VARCHAR), 'queued',
                  CURRENT_DATE, CURRENT_TIMESTAMP FROM range(?) AS r(i)""",
        (campaign_id, prefix, prefix + "-", int(count)),
    )


def run(args: argparse.Namespace) -> dict:
    temp_dir = Path(args.output) if args.output else Path(tempfile.mkdtemp(prefix="mc-isolation-"))
    temp_dir.mkdir(parents=True, exist_ok=True)
    database.set_database_file(str(temp_dir / "isolation.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    old_active = os.environ.get("MC_FAIR_ACTIVE_CAMPAIGNS")
    old_per = os.environ.get("MC_FAIR_CLAIM_PER_CAMPAIGN")
    os.environ["MC_FAIR_ACTIVE_CAMPAIGNS"] = "8"
    os.environ["MC_FAIR_CLAIM_PER_CAMPAIGN"] = "4"
    started = time.perf_counter()
    conn = database.get_connection()
    try:
        campaign_a = _campaign(conn, "isolation-A", "in_progress")
        campaign_b = _campaign(conn, "isolation-B", "paused")
        campaign_c = _campaign(conn, "isolation-C", "paused")
        _insert_tasks(conn, campaign_a, args.a_tasks, "A")
        _insert_tasks(conn, campaign_b, args.bc_tasks, "B")
        _insert_tasks(conn, campaign_c, args.bc_tasks, "C")
        conn.commit()
    finally:
        conn.close()

    try:
        a_claim = sms_task_service.claim_next_tasks(1)
        a_task = a_claim[0] if a_claim else None
        resume_b = p1_service.campaign_action("batch", campaign_b, "resume")
        resume_c = p1_service.campaign_action("batch", campaign_c, "resume")
        pause_a = p1_service.campaign_action("batch", campaign_a, "pause")
        preauth_denied = False
        if a_task:
            preauth_denied = sms_task_service.record_send_intent(
                a_task, smpp_config_id=7, slot=0, planned_parts=1,
            ) == ""
        bc_claims = sms_task_service.claim_next_tasks(args.claim_after_pause)
        bc_campaigns = sorted({int(task["campaign_id"]) for task in bc_claims})
        conn = database.get_connection()
        try:
            status_rows = conn.execute(
                "SELECT campaign_id,status,COUNT(*) count FROM sms_tasks "
                "GROUP BY campaign_id,status ORDER BY campaign_id,status"
            ).fetchall()
        finally:
            conn.close()
        elapsed = max(0.000001, time.perf_counter() - started)
        result = {
            "scenario": "A backlog with B/C paused-then-resumed around A pause",
            "database": str(temp_dir / "isolation.duckdb"),
            "parameters": {
                "a_tasks": args.a_tasks, "bc_tasks_each": args.bc_tasks,
                "claim_after_pause": args.claim_after_pause,
                "smpp_slots": 4, "task_workers": 8,
            },
            "campaign_ids": {"A": campaign_a, "B": campaign_b, "C": campaign_c},
            "events": {
                "a_claimed_before_pause": bool(a_task),
                "pre_authorization_denied_after_pause": preauth_denied,
                "b_resumed": resume_b["status"] == "in_progress",
                "c_resumed": resume_c["status"] == "in_progress",
                "a_paused": pause_a["status"] == "paused",
                "bc_claim_count_after_pause": len(bc_claims),
                "bc_campaigns_claimed_after_pause": bc_campaigns,
                "status_rows": [dict(row) for row in status_rows],
            },
            "elapsed_seconds": round(elapsed, 6),
            "status": "PASS" if preauth_denied and set(bc_campaigns) == {campaign_b, campaign_c} else "FAIL",
            "smsc": "NOT_CONNECTED",
            "socket_writes": 0,
        }
    finally:
        if old_active is None:
            os.environ.pop("MC_FAIR_ACTIVE_CAMPAIGNS", None)
        else:
            os.environ["MC_FAIR_ACTIVE_CAMPAIGNS"] = old_active
        if old_per is None:
            os.environ.pop("MC_FAIR_CLAIM_PER_CAMPAIGN", None)
        else:
            os.environ["MC_FAIR_CLAIM_PER_CAMPAIGN"] = old_per
    (temp_dir / "track-a-result.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="")
    parser.add_argument("--a-tasks", type=int, default=50000)
    parser.add_argument("--bc-tasks", type=int, default=1000)
    parser.add_argument("--claim-after-pause", type=int, default=200)
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
