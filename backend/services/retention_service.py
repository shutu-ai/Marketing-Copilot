"""Shared, bounded retention helpers for durable and filesystem state.

The application keeps the historical ``sms_task_retention_days`` setting as
the common system-data retention window.  This module only owns generic
housekeeping; business-specific services decide which records are safe to
remove before calling these helpers.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

from database import write_transaction

logger = logging.getLogger(__name__)

CAMPAIGN_MARKER = ".mc-campaign.json"


def ensure_campaign_marker(path: str, campaign_type: str, campaign_id: int) -> None:
    """Mark an application-owned campaign directory for safe orphan cleanup."""

    if not path:
        return
    root = Path(path).expanduser()
    try:
        root.mkdir(parents=True, exist_ok=True)
        marker = root / CAMPAIGN_MARKER
        if not marker.exists():
            temporary = marker.with_name(f".{marker.name}.{os.getpid()}.tmp")
            temporary.write_text(
                json.dumps(
                    {"campaign_type": str(campaign_type), "campaign_id": int(campaign_id)},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            os.replace(temporary, marker)
    except OSError:
        logger.warning("Unable to write campaign directory marker path=%s", root, exc_info=True)


def cleanup_orphan_campaign_dirs(
    base_dir: str,
    campaign_type: str,
    known_paths: set[str],
    retention_days: int,
    *,
    limit: int = 4,
) -> int:
    """Remove only old, explicitly marked directories absent from the DB.

    Custom directories and unmarked user data are deliberately left alone.
    """

    if not base_dir:
        return 0
    root = Path(base_dir).expanduser()
    if not root.is_dir():
        return 0
    cutoff = time.time() - max(1, int(retention_days)) * 86400
    normalized = {
        os.path.normcase(os.path.abspath(str(path)))
        for path in known_paths
        if path
    }
    removed = 0
    try:
        entries = sorted(root.iterdir(), key=lambda item: item.name)
    except OSError:
        return 0
    for entry in entries:
        if removed >= max(1, int(limit)) or not entry.is_dir():
            continue
        if os.path.normcase(os.path.abspath(str(entry))) in normalized:
            continue
        marker = entry / CAMPAIGN_MARKER
        try:
            if not marker.is_file() or entry.stat().st_mtime >= cutoff:
                continue
            payload = json.loads(marker.read_text(encoding="utf-8"))
            if str(payload.get("campaign_type") or "") != str(campaign_type):
                continue
            shutil.rmtree(entry)
            removed += 1
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            logger.warning("Unable to remove orphan campaign directory=%s", entry, exc_info=True)
    return removed


def cleanup_history(retention_days: int | None = None, *, limit: int = 2000) -> dict:
    """Bound growth of non-active history and scheduler state."""

    from services import settings_service

    days = settings_service.get_data_retention_days(retention_days)
    batch_limit = max(100, min(10000, int(limit)))
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    def delete_old(conn, table: str, timestamp_column: str, extra: str = "") -> int:
        rows = conn.execute(
            f"""SELECT id FROM {table}
                WHERE TRY_CAST({timestamp_column} AS TIMESTAMP) < TRY_CAST(? AS TIMESTAMP)
                  {extra}
                ORDER BY id LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        conn.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", ids)
        return len(ids)

    def delete_orphan_campaign_state(conn, table: str) -> int:
        key_columns = "campaign_type,campaign_id,route_id" if table == "campaign_fairness_state" else "campaign_type,campaign_id"
        rows = conn.execute(
            f"""SELECT {key_columns} FROM {table} state
                WHERE TRY_CAST(state.updated_at AS TIMESTAMP) < TRY_CAST(? AS TIMESTAMP)
                  AND ((state.campaign_type='realtime' AND NOT EXISTS (
                         SELECT 1 FROM realtime_campaigns c WHERE c.id=state.campaign_id
                       )) OR
                       (state.campaign_type='batch' AND NOT EXISTS (
                         SELECT 1 FROM batch_campaigns c WHERE c.id=state.campaign_id
                       )))
                ORDER BY state.updated_at LIMIT ?""",
            (cutoff, batch_limit),
        ).fetchall()
        if not rows:
            return 0
        if table == "campaign_fairness_state":
            conn.executemany(
                f"DELETE FROM {table} WHERE campaign_type=? AND campaign_id=? AND route_id=?",
                [(row["campaign_type"], row["campaign_id"], row["route_id"]) for row in rows],
            )
        else:
            conn.executemany(
                f"DELETE FROM {table} WHERE campaign_type=? AND campaign_id=?",
                [(row["campaign_type"], row["campaign_id"]) for row in rows],
            )
        return len(rows)

    def cleanup(conn):
        reservation_rows = conn.execute(
            """SELECT token FROM realtime_inbox_reservations
               WHERE TRY_CAST(expires_at AS TIMESTAMPTZ)<=CURRENT_TIMESTAMP
               ORDER BY expires_at LIMIT ?""",
            (batch_limit,),
        ).fetchall()
        reservation_tokens = [str(row["token"]) for row in reservation_rows if row["token"]]
        expired_reservations = 0
        if reservation_tokens:
            placeholders = ",".join("?" for _ in reservation_tokens)
            expired_reservations = len(conn.execute(
                f"DELETE FROM realtime_inbox_reservations WHERE token IN ({placeholders}) RETURNING token",
                reservation_tokens,
            ).fetchall())
        return {
            "failed_task_archive": delete_old(conn, "failed_task_archive", "archived_at"),
            "audit_logs": delete_old(conn, "audit_logs", "created_at"),
            "sms_log": delete_old(conn, "sms_log", "created_at"),
            "update_history": delete_old(conn, "update_history", "created_at"),
            "resolved_system_events": delete_old(
                conn, "system_events", "created_at", "AND resolved=1"
            ),
            "expired_inbox_reservations": expired_reservations,
            "orphan_fairness_state": delete_orphan_campaign_state(conn, "campaign_fairness_state"),
            "orphan_capacity_policies": delete_orphan_campaign_state(conn, "campaign_capacity_policies"),
        }

    result = write_transaction(
        cleanup,
        attempts=3,
        operation="retention.history_cleanup",
        lock_timeout=1.0,
    )
    from services import task_archive_service
    task_artifacts = task_archive_service.cleanup_orphan_task_artifacts(days, limit=batch_limit)
    task_artifact_counts = [value for key, value in task_artifacts.items() if key != "retention_days"]
    if any(result.values()) or any(task_artifact_counts):
        logger.info(
            "History retention cleanup completed retention_days=%s result=%s task_artifacts=%s",
            days, result, task_artifacts,
        )
    return {"retention_days": days, **result, "orphan_task_artifacts": task_artifacts}
