from datetime import datetime
from database import get_connection


def get_stats() -> dict:
    conn = get_connection()

    # Batch campaign stats
    total_batch = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns").fetchone()
    in_progress = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='in_progress'").fetchone()
    ended = conn.execute("SELECT COUNT(*) AS cnt FROM batch_campaigns WHERE status='ended'").fetchone()

    # Today's sent count
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_sent = conn.execute(
        "SELECT COALESCE(SUM(sent_count), 0) AS cnt FROM batch_campaign_daily_stats WHERE stats_date=?",
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

    conn.close()
    rt = dict(rt_files) if rt_files else {"file_count": 0, "total_msisdn": 0, "total_matched": 0}
    return {
        "total_batch_campaigns": total_batch["cnt"] if total_batch else 0,
        "in_progress_campaigns": in_progress["cnt"] if in_progress else 0,
        "ended_campaigns": ended["cnt"] if ended else 0,
        "today_sent_count": today_sent["cnt"] if today_sent else 0,
        "total_sms_campaigns": sms_count["cnt"] if sms_count else 0,
        "realtime_campaigns": rt_campaigns["cnt"] if rt_campaigns else 0,
        "realtime_files": rt["file_count"],
        "realtime_event_users": rt["total_msisdn"],
        "realtime_matched_users": rt["total_matched"],
    }
