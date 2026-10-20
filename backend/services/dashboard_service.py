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
    completed_today = conn.execute(
        """SELECT COUNT(*) AS cnt
           FROM (
               SELECT id
                 FROM batch_campaigns
                WHERE status IN ('completed', 'ended')
                  AND CAST(updated_at AS DATE)=CAST(? AS DATE)
               UNION ALL
               SELECT id
                 FROM realtime_campaigns
                WHERE status IN ('completed', 'ended')
                  AND CAST(updated_at AS DATE)=CAST(? AS DATE)
           ) AS completed_campaigns""",
        (today_str, today_str),
    ).fetchone()
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
    trend_start = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    realtime_trend_rows = conn.execute(
        """SELECT strftime(CAST(created_at AS DATE), '%Y-%m-%d') AS stats_date,
                  COUNT(*) AS received_file_count,
                  COALESCE(SUM(total_msisdn),0) AS total_msisdn,
                  COALESCE(SUM(matched_count),0) AS matched_count
           FROM realtime_campaign_files
           WHERE CAST(created_at AS DATE)>=CAST(? AS DATE)
           GROUP BY CAST(created_at AS DATE)
           ORDER BY CAST(created_at AS DATE)""",
        (trend_start,),
    ).fetchall()
    realtime_trend_map = {row["stats_date"]: dict(row) for row in realtime_trend_rows}
    rt = realtime_trend_map.get(today_str, {
        "received_file_count": 0,
        "total_msisdn": 0,
        "matched_count": 0,
    })
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
        send_row = trend_map.get(day, {
            "stats_date": day, "sent_count": 0, "conversion_count": 0,
        })
        file_row = realtime_trend_map.get(day, {})
        daily_trend.append({
            **dict(send_row),
            "matched_count": int(file_row.get("matched_count", 0) or 0),
            "received_file_count": int(file_row.get("received_file_count", 0) or 0),
        })
    exception_rows = conn.execute("""SELECT id,category,severity,code,message,created_at FROM system_events
        WHERE resolved=0 AND CAST(created_at AS DATE)=CAST(? AS DATE)
        ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
        created_at DESC LIMIT 5""", (today_str,)).fetchall()
    exception_count = conn.execute(
        """SELECT COUNT(*) AS cnt FROM system_events
           WHERE resolved=0 AND CAST(created_at AS DATE)=CAST(? AS DATE)""",
        (today_str,),
    ).fetchone()
    batch_in_progress_count = batch_in_progress["cnt"] if batch_in_progress else 0
    realtime_in_progress_count = realtime_in_progress["cnt"] if realtime_in_progress else 0
    return {
        "pipeline": "legacy",
        "total_batch_campaigns": total_batch["cnt"] if total_batch else 0,
        "in_progress_campaigns": batch_in_progress_count + realtime_in_progress_count,
        "in_progress_batch_campaigns": batch_in_progress_count,
        "in_progress_realtime_campaigns": realtime_in_progress_count,
        "ended_campaigns": ended["cnt"] if ended else 0,
        "today_completed_campaigns": completed_today["cnt"] if completed_today else 0,
        "today_sent_count": today_sent["cnt"] if today_sent else 0,
        "total_sms_campaigns": sms_count["cnt"] if sms_count else 0,
        "realtime_campaigns": rt_campaigns["cnt"] if rt_campaigns else 0,
        "today_realtime_files": int(rt.get("received_file_count", 0) or 0),
        "today_realtime_event_users": rt["total_msisdn"],
        "today_realtime_matched_users": rt["matched_count"],
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
            projection_result = vnext_service.dashboard_stats()
            projection_trend = projection_result.get("daily_trend") or []
            control_trend = result.get("daily_trend") or []
            control_trend_by_day = {
                row["stats_date"]: row for row in control_trend
            }
            result.update({
                key: value
                for key, value in projection_result.items()
                if key != "daily_trend"
            })
            if projection_trend:
                result["daily_trend"] = [
                    {
                        **dict(row),
                        "matched_count": int(
                            control_trend_by_day.get(row["stats_date"], {}).get(
                                "matched_count", 0
                            ) or 0
                        ),
                        "received_file_count": int(
                            control_trend_by_day.get(row["stats_date"], {}).get(
                                "received_file_count", 0
                            ) or 0
                        ),
                    }
                    for row in projection_trend
                ]
            return result
        except (RuntimeError, OSError, ValueError, TypeError):
            # Keep the legacy/database dashboard available during process
            # startup or a transient VNext status-file read failure.
            pass
    return read_transaction(_get_stats, operation="dashboard.stats")
