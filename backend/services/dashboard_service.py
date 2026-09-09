from datetime import datetime, timedelta
from database import get_connection


def get_stats() -> dict:
    conn = get_connection()

    # Batch campaign stats
    total_batch = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns").fetchone()
    batch_in_progress = conn.execute(
        "SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='in_progress'"
    ).fetchone()
    realtime_in_progress = conn.execute(
        "SELECT COUNT(*) AS cnt FROM realtime_campaigns WHERE status='in_progress'"
    ).fetchone()
    ended = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='ended'").fetchone()

    # Count actual delivered SMS tasks across both batch and realtime campaigns.
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_sent = conn.execute(
        """SELECT COUNT(*) AS cnt FROM sms_tasks
           WHERE status IN ('delivered','accepted')
             AND COALESCE(delivered_at, submitted_at, updated_at) IS NOT NULL
             AND CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)=CAST(? AS DATE)""",
        (today_str,)
    ).fetchone()

    # SMS content campaigns count
    sms_count = conn.execute("SELECT COUNT(*) AS cnt FROM campaigns").fetchone()

    # Realtime campaign stats
    rt_campaigns = conn.execute(
        "SELECT COUNT(*) AS cnt FROM realtime_campaigns"
    ).fetchone()
    rt_files = conn.execute(
        "SELECT COALESCE(COUNT(*),0) AS file_count, COALESCE(SUM(total_msisdn),0) AS total_msisdn, COALESCE(SUM(matched_count),0) AS total_matched FROM realtime_campaign_files"
    ).fetchone()

    trend_start = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")
    trend_rows = conn.execute(
        """SELECT strftime(CAST(delivered_at AS DATE), '%Y-%m-%d') AS stats_date,
                  COUNT(*) AS sent_count,
                  0 AS conversion_count
           FROM sms_tasks
           WHERE status IN ('delivered','accepted')
             AND COALESCE(delivered_at, submitted_at, updated_at) IS NOT NULL
             AND CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)>=CAST(? AS DATE)
           GROUP BY CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)
           ORDER BY CAST(COALESCE(delivered_at, submitted_at, updated_at) AS DATE)""",
        (trend_start,),
    ).fetchall()
    trend_map = {row["stats_date"]: dict(row) for row in trend_rows}
    daily_trend = []
    for offset in range(6, -1, -1):
        day = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
        daily_trend.append(trend_map.get(day, {"stats_date": day, "sent_count": 0, "conversion_count": 0}))
    exception_rows = conn.execute("""SELECT id,category,severity,code,message,created_at FROM system_events
        WHERE resolved=0 ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
        created_at DESC LIMIT 5""").fetchall()
    exception_count = conn.execute("SELECT COUNT(*) AS cnt FROM system_events WHERE resolved=0").fetchone()
    conn.close()
    rt = dict(rt_files) if rt_files else {"file_count": 0, "total_msisdn": 0, "total_matched": 0}
    batch_in_progress_count = batch_in_progress["cnt"] if batch_in_progress else 0
    realtime_in_progress_count = realtime_in_progress["cnt"] if realtime_in_progress else 0
    return {
        "total_batch_campaigns": total_batch["cnt"] if total_batch else 0,
        "in_progress_campaigns": batch_in_progress_count + realtime_in_progress_count,
        "in_progress_batch_campaigns": batch_in_progress_count,
        "in_progress_realtime_campaigns": realtime_in_progress_count,
        "ended_campaigns": ended["cnt"] if ended else 0,
        "today_sent_count": today_sent["cnt"] if today_sent else 0,
        "total_sms_campaigns": sms_count["cnt"] if sms_count else 0,
        "realtime_campaigns": rt_campaigns["cnt"] if rt_campaigns else 0,
        "realtime_files": rt["file_count"],
        "realtime_event_users": rt["total_msisdn"],
        "realtime_matched_users": rt["total_matched"],
        "daily_trend": daily_trend,
        "open_exception_count": exception_count["cnt"] if exception_count else 0,
        "recent_exceptions": [dict(row) for row in exception_rows],
    }
