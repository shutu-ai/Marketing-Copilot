from __future__ import annotations

from pathlib import Path

from tools.vnext_single_writer_benchmark import _run_case


def test_isolated_staging_adapters_preserve_digest(tmp_path: Path):
    common = {
        "batch_size": 64,
        "events": 128,
        "duplicate_fraction": 0.10,
        "campaign_count": 4,
        "events_per_sms": 2,
        "business_day_count": 2,
    }
    cases = [
        _run_case(tmp_path / label.lower(), label=label, **common)
        for label in ("A0", "A1", "A3", "A4")
    ]

    assert all(item["status"] == "PASS" for item in cases)
    assert all("matches_a0_result" not in item for item in cases)
    assert len({item["result_sha256"] for item in cases}) == 1
    assert all(len(item["staging_schema"]) == 25 for item in cases)
    assert all(item["metrics"]["staging_total_ns"] == item["metrics"]["staging_load_ns"] for item in cases)
