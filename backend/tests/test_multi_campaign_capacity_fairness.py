import json
from pathlib import Path

from tools.mixed_campaign_capacity_acceptance import (
    MixedCampaignRunner,
    _metric_latency_summary,
    build_activity_plan,
)


def test_activity_plan_is_seeded_and_mixes_types_routes_and_late_work():
    scenario_path = Path(__file__).parents[1] / "tools" / "mixed_campaign_scenarios.json"
    spec = json.loads(scenario_path.read_text(encoding="utf-8"))["profiles"]["fairness"]
    first = build_activity_plan(spec, 20261013)
    second = build_activity_plan(spec, 20261013)
    other = build_activity_plan(spec, 20261014)

    assert first == second
    assert [item["logical_index"] for item in first] != [item["logical_index"] for item in other]
    assert len(first) == 8
    assert sum(item["campaign_type"] == "realtime" for item in first) == 4
    assert sum(item["campaign_type"] == "batch" for item in first) == 4
    assert sum(item["large"] for item in first) == 2
    assert {item["route_id"] for item in first} == {0, 1}


def test_metric_latency_summary_aggregates_operation_labels():
    snapshot = {
        "counters": [
            {"name": "database_write_lock_wait_seconds_count", "labels": {"operation": "a"}, "value": 3},
            {"name": "database_write_lock_wait_seconds_sum", "labels": {"operation": "a"}, "value": 1.5},
            {"name": "database_write_lock_wait_seconds_count", "labels": {"operation": "b"}, "value": 2},
            {"name": "database_write_lock_wait_seconds_sum", "labels": {"operation": "b"}, "value": 0.5},
        ],
        "gauges": [],
    }
    result = _metric_latency_summary(snapshot, "database_write_lock_wait_seconds")
    assert result["count"] == 5
    assert result["sum_seconds"] == 2.0
    assert result["mean_seconds"] == 0.4
    assert result["p95_seconds"] is None


def test_activity_event_mapping_uses_business_msisdn_not_page_order():
    runner = object.__new__(MixedCampaignRunner)
    runner.rows_by_activity = {
        ("realtime", 12): [{"msisdn": "2511001"}],
        ("batch", 4): [{"msisdn": "2512001"}],
    }
    assert runner._event_key({"destination_addr": "2512001"}) == ("batch", 4)
    assert runner._event_key({"destination_addr": "2511001"}) == ("realtime", 12)
    assert runner._event_key({"destination_addr": "2519999"}) is None
