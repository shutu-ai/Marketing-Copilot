from __future__ import annotations

import asyncio

from vnext.runtime import VNextRuntime


def test_runtime_starts_projection_and_worker_supervisor_then_stops(tmp_path):
    runtime = VNextRuntime(
        tmp_path / "runtime", {1: {"route_id": 1, "host": "127.0.0.1", "port": 1}},
        worker_count=2, virtual_partition_count=8, projection_queue_size=32,
    )
    assert runtime.start_method == "spawn"
    assert runtime.projection.start_method == "spawn"
    try:
        runtime.start()
        status = runtime.status()
        assert status["started"] is True
        assert status["start_method"] == "spawn"
        assert status["supervisor"]["start_method"] == "spawn"
        assert len(status["supervisor"]["workers"]) == 2
        runtime.set_campaign_state("batch", 7, "paused")
        assert runtime.status()["controls"] == [{"campaign_type": "batch", "campaign_id": 7, "state": "paused"}]
    finally:
        runtime.stop()
    assert runtime.status()["started"] is False
