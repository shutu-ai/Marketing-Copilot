from datetime import datetime, timedelta

from database import read_transaction


def _get_stats(conn, include_legacy_send_metrics: bool = True) -> dict:
    # Batch campaign stats
    total_batch = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns").fetchone()
    batch_in_progress = conn.execute(
        "SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='in_progress'"
    ).fetchone()
    realtime_in_progress = conn.execute(
        "SELECT COUNT(*) AS cnt FROM realtime_campaigns WHERE status='in_progress'"
    ).fetchone()
    ended = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='ended'").fetchone()
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    if include_legacy_send_metrics:
        today_sent = conn.execute(
            """SELECT COUNT(*) AS cnt FROM sms_tasks
               WHERE status IN ('delivered','accepted')
                 AND COALESCE(delivered_at, submitted_at, updated_at) IS NOT NULL
                 AND CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)=CAST(? AS DATE)""",
            (today_str,)
        ).fetchone()
    else:
        # VNext gets its send evidence from the projection snapshot. Avoid
        # scanning the legacy task table on every dashboard refresh.
        today_sent = {"cnt": 0}
    sms_count = conn.execute("SELECT COUNT(*) AS cnt FROM campaigns").fetchone()
    rt_campaigns = conn.execute("SELECT COUNT(*) AS cnt FROM realtime_campaigns").fetchone()
    rt_files = conn.execute(
        """SELECT COALESCE(COUNT(*),0) AS file_count,
                  COALESCE(SUM(total_msisdn),0) AS total_msisdn,
                  COALESCE(SUM(matched_count),0) AS total_matched
           FROM realtime_campaign_files
           WHERE CAST(created_at AS DATE)=CAST(? AS DATE)""",
        (today_str,),
    ).fetchone()
    trend_start = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    if include_legacy_send_metrics:
        trend_rows = conn.execute(
            """SELECT strftime(CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE), '%Y-%m-%d') AS stats_date,
                      COUNT(*) AS sent_count, 0 AS conversion_count
               FROM sms_tasks
               WHERE status IN ('delivered','accepted')
                 AND COALESCE(delivered_at, submitted_at, updated_at) IS NOT NULL
                 AND CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)>=CAST(? AS DATE)
               GROUP BY CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)
               ORDER BY CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)""",
            (trend_start,),
        ).fetchall()
    else:
        trend_rows = []
    trend_map = {row["stats_date"]: dict(row) for row in trend_rows}
    daily_trend = []
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%Y-%m-%d")
        daily_trend.append(trend_map.get(day, {
            "stats_date": day, "sent_count": 0, "conversion_count": 0,
        }))
    exception_rows = conn.execute("""SELECT id,category,severity,code,message,created_at FROM system_events
        WHERE resolved=0 AND CAST(created_at AS DATE)=CAST(? AS DATE)
        ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
        created_at DESC LIMIT 5""", (today_str,)).fetchall()
    exception_count = conn.execute(
        """SELECT COUNT(*) AS cnt FROM system_events
           WHERE resolved=0 AND CAST(created_at AS DATE)=CAST(? AS DATE)""",
        (today_str,),
    ).fetchone()
    rt = dict(rt_files) if rt_files else {"file_count": 0, "total_msisdn": 0, "total_matched": 0}
    batch_in_progress_count = batch_in_progress["cnt"] if batch_in_progress else 0
    realtime_in_progress_count = realtime_in_progress["cnt"] if realtime_in_progress else 0
    return {
        "pipeline": "legacy",
        "total_batch_campaigns": total_batch["cnt"] if total_batch else 0,
        "in_progress_campaigns": batch_in_progress_count + realtime_in_progress_count,
        "in_progress_batch_campaigns": batch_in_progress_count,
        "in_progress_realtime_campaigns": realtime_in_progress_count,
        "ended_campaigns": ended["cnt"] if ended else 0,
        "today_sent_count": today_sent["cnt"] if today_sent else 0,
        "total_sms_campaigns": sms_count["cnt"] if sms_count else 0,
        "realtime_campaigns": rt_campaigns["cnt"] if rt_campaigns else 0,
        "today_realtime_files": rt["file_count"],
        "today_realtime_event_users": rt["total_msisdn"],
        "today_realtime_matched_users": rt["total_matched"],
        "daily_trend": daily_trend,
        "today_open_exception_count": exception_count["cnt"] if exception_count else 0,
        "today_recent_exceptions": [dict(row) for row in exception_rows],
    }


def get_stats() -> dict:
    # VNext intentionally avoids the two legacy sms_tasks scans. Campaign and
    # exception counts still come from the application database, while send
    # metrics come from the projection status contract.
    try:
        from services import vnext_service
        vnext_active = vnext_service.is_active()
    except (RuntimeError, OSError, ValueError, TypeError):
        vnext_active = False
        vnext_service = None
    if vnext_active:
        try:
            result = read_transaction(
                lambda conn: _get_stats(conn, include_legacy_send_metrics=False),
                operation="dashboard.stats",
            )
            result.update(vnext_service.dashboard_stats())
            return result
        except (RuntimeError, OSError, ValueError, TypeError):
            # Keep the legacy/database dashboard available during process
            # startup or a transient VNext status-file read failure.
            pass
    return read_transaction(_get_stats, operation="dashboard.stats")
