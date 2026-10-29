from __future__ import annotations


def test_vnext_capacity_status_skips_legacy_duckdb_read_models(monkeypatch):
    from routers import metrics
    from services import vnext_service

    snapshot = {
        "supervisor": {
            "worker_count": 2,
            "workers": [{"alive": True}, {"alive": False}],
        },
        "spool": {"pending_records": 7},
        "projection": {"ready": True, "alive": True},
    }
    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(vnext_service, "capacity_snapshot", lambda: snapshot)
    monkeypatch.setattr(
        vnext_service,
        "smpp_stats",
        lambda value: {"pipeline": "vnext", "summary": {}, "items": []},
    )

    def unexpected_legacy_read(*_args, **_kwargs):
        raise AssertionError("VNext capacity must not refresh legacy DuckDB read models")

    monkeypatch.setattr(
        metrics.metrics_service,
        "observe_runtime_state",
        unexpected_legacy_read,
    )
    monkeypatch.setattr(
        metrics.ingestion_job_service,
        "status",
        unexpected_legacy_read,
    )
    monkeypatch.setattr(
        metrics.backpressure_service,
        "snapshot",
        unexpected_legacy_read,
    )
    monkeypatch.setattr(metrics.metrics_service, "snapshot", lambda: {})
    monkeypatch.setattr(metrics.fast_send_service, "status", lambda: {})
    monkeypatch.setattr(metrics.campaign_capacity_service, "status", lambda: {})
    monkeypatch.setattr(metrics, "write_lock_status", lambda: {"held": False})

    result = metrics.capacity_status(view="capacity")

    assert result["workers"] == {
        "configured": 2,
        "running": 1,
        "workers": snapshot["supervisor"]["workers"],
        "dispatch": {"mode": "vnext"},
        "pipeline": "vnext",
    }
    assert result["smpp"]["pipeline"] == "vnext"
    assert result["ingestion"] == {
        "source": "vnext",
        "chunks": {},
        "pending": None,
        "not_applicable": True,
    }
    assert result["backpressure"]["source"] == "vnext"
    assert result["backpressure"]["not_applicable"] is True
