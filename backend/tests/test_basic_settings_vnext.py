import time

import pytest


def test_basic_settings_reject_malformed_runtime_values():
    from services import settings_service

    assert settings_service._normalize_setting_value(
        "realtime_index_build_timeout", "0"
    ) == "0"
    assert settings_service._normalize_setting_value(
        "realtime_whitelist_upload_timeout", "3600"
    ) == "3600"
    assert settings_service._normalize_setting_value(
        "realtime_whitelist_build_memory_limit", "4GB"
    ) == "4GB"

    with pytest.raises(ValueError):
        settings_service._normalize_setting_value(
            "realtime_index_build_timeout", "not-a-number"
        )
    with pytest.raises(ValueError):
        settings_service._normalize_setting_value(
            "realtime_whitelist_upload_timeout", "0"
        )
    with pytest.raises(ValueError):
        settings_service._normalize_setting_value(
            "realtime_whitelist_build_memory_limit", "not-a-memory-limit"
        )


def test_vnext_retention_setting_uses_live_projection_control(monkeypatch):
    from services import vnext_service

    applied = []

    class Runtime:
        _started = True

        def apply_retention_days(self, value):
            applied.append(value)
            return {"ok": True, "reason": "applied"}

    monkeypatch.setattr(vnext_service, "_RUNTIME", Runtime())
    result = vnext_service.apply_retention_days("9")

    assert result == {"ok": True, "reason": "applied", "retention_days": 9}
    assert applied == [9]


def test_projection_retention_control_does_not_restart_writer(tmp_path):
    from vnext.projection_writer import ProjectionWriterProcess

    process = ProjectionWriterProcess(tmp_path / "projection", retention_days=3)
    process.start()
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.snapshot().get("retention", {}).get("retention_days") == 3:
                break
            time.sleep(0.05)
        assert process.snapshot()["retention"]["retention_days"] == 3
        original_pid = process.process.pid

        assert process.set_retention_days(9) is True
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.snapshot().get("retention", {}).get("retention_days") == 9:
                break
            time.sleep(0.05)
        assert process.snapshot()["retention"]["retention_days"] == 9
        assert process.process.pid == original_pid
    finally:
        process.stop(timeout=5)
