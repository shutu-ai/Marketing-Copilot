from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from services import sms_task_service, vnext_service
from vnext import business_day
from vnext.business_day import BusinessClock


def test_default_business_clock_follows_the_host_timezone(monkeypatch):
    monkeypatch.delenv("MC_VNEXT_BUSINESS_TIMEZONE", raising=False)
    monkeypatch.delenv("MC_BUSINESS_TIMEZONE", raising=False)
    monkeypatch.setattr(
        business_day,
        "system_timezone",
        lambda: ("Asia/Shanghai", ZoneInfo("Asia/Shanghai")),
    )

    clock = BusinessClock(datetime(2026, 10, 22, 0, 30, tzinfo=timezone.utc))

    assert clock.timezone_name == "Asia/Shanghai"
    assert clock.business_day() == "2026-10-22"


def test_campaign_schedule_uses_the_deployment_timezone(monkeypatch):
    monkeypatch.setenv("MC_VNEXT_BUSINESS_TIMEZONE", "Africa/Addis_Ababa")
    campaign = {
        "status": "in_progress",
        "start_time": "2026-10-22 09:00:00",
        "end_time": "2026-10-22 17:00:00",
        "send_start_time": "09:00",
        "send_end_time": "17:00",
        "send_days": [],
    }

    before = datetime(2026, 10, 22, 5, 59, tzinfo=timezone.utc)
    active = datetime(2026, 10, 22, 6, 30, tzinfo=timezone.utc)

    assert sms_task_service.campaign_is_active(campaign, before)[0] is False
    assert sms_task_service.campaign_is_active(campaign, active) == (True, "active")


def test_vnext_task_stats_exposes_effective_timezone(monkeypatch):
    class Projection:
        def snapshot(self):
            return {"daily_totals": [], "projection_events_applied": 0}

    class Runtime:
        _started = True
        clock = BusinessClock(
            datetime(2026, 10, 22, 7, 25, tzinfo=timezone.utc),
            "Africa/Addis_Ababa",
        )
        spool = type("Spool", (), {"current_business_day": "2026-10-22"})()
        projection = Projection()

    monkeypatch.setattr(vnext_service, "_RUNTIME", Runtime())

    assert vnext_service.task_stats()["business_timezone"] == "Africa/Addis_Ababa"


def test_vnext_task_list_exposes_effective_timezone_without_extra_queries(monkeypatch):
    class Runtime:
        _started = True
        clock = BusinessClock(
            datetime(2026, 10, 22, 7, 25, tzinfo=timezone.utc),
            "Asia/Shanghai",
        )

    monkeypatch.setattr(vnext_service, "_RUNTIME", Runtime())
    monkeypatch.setattr(vnext_service, "_projection_task_page", lambda *args: (0, []))

    result = vnext_service.list_tasks(page=1, page_size=50)

    assert result["business_timezone"] == "Asia/Shanghai"
