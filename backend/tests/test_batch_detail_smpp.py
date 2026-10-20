from services import batch_campaign_service


def test_batch_detail_smpp_uses_vnext_route_projection(monkeypatch):
    config_rows = [
        {
            "id": 1,
            "name": "configured-route",
            "host": "configured.example",
            "port": 2775,
            "enabled": 1,
            "priority": 100,
        },
        {
            "id": 2,
            "name": "actual-route",
            "host": "actual.example",
            "port": 2776,
            "enabled": 1,
            "priority": 90,
        },
    ]
    monkeypatch.setattr(
        batch_campaign_service,
        "read_transaction",
        lambda _reader, operation="": ([], config_rows),
    )
    monkeypatch.setattr(
        batch_campaign_service,
        "_vnext_route_ids_by_day",
        lambda campaign_id, days: {"2026-10-20": [2]},
    )

    daily_stats = [{"stats_date": "2026-10-20", "sent_count": 10}]
    batch_campaign_service._attach_daily_smpp(
        7,
        daily_stats,
        {"smpp_config_id": 1},
    )

    assert daily_stats[0]["smpp_label"] == "actual-route (actual.example:2776)"


def test_batch_detail_smpp_falls_back_to_campaign_route(monkeypatch):
    config_rows = [{
        "id": 1,
        "name": "configured-route",
        "host": "configured.example",
        "port": 2775,
        "enabled": 1,
        "priority": 100,
    }]
    monkeypatch.setattr(
        batch_campaign_service,
        "read_transaction",
        lambda _reader, operation="": ([], config_rows),
    )
    monkeypatch.setattr(
        batch_campaign_service,
        "_vnext_route_ids_by_day",
        lambda campaign_id, days: {},
    )

    daily_stats = [{"stats_date": "2026-10-20", "sent_count": 0}]
    batch_campaign_service._attach_daily_smpp(
        7,
        daily_stats,
        {"smpp_config_id": 1},
    )

    assert daily_stats[0]["smpp_label"] == "configured-route (configured.example:2775)"


def test_batch_detail_daily_sent_users_use_vnext_socket_written(monkeypatch):
    from services import vnext_service

    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(
        vnext_service,
        "campaign_daily_totals",
        lambda campaign_type, campaign_id: [
            {
                "business_day": "2026-10-20",
                "campaign_type": campaign_type,
                "campaign_id": campaign_id,
                "socket_written": 37,
                "socket_written_pdus": 42,
                "response_success": 35,
                "response_error": 2,
            }
        ],
    )

    result = batch_campaign_service._merge_vnext_daily_stats(
        7,
        {"row_count": 100},
        [{"stats_date": "2026-10-20", "total_prospects": 100, "sent_count": 9}],
    )

    assert result[0]["sent_count"] == 37
    assert result[0]["socket_written_events"] == 37
    assert result[0]["socket_written_pdus"] == 42
    assert result[0]["smsc_accepted_pdus"] == 35
    assert result[0]["stats_source"] == "vnext_projection"
