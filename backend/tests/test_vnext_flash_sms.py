from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import vnext_service


class _CaptureRuntime:
    _started = True

    def __init__(self):
        self.route_configs = {"7": {"route_id": 7}}
        self.calls = []

    def enqueue_chunk(self, rows, **kwargs):
        rows = list(rows)
        self.calls.append((rows, kwargs))
        return SimpleNamespace(
            accepted=len(rows),
            duplicate=0,
            rejected=0,
            rejected_reasons={},
            accepted_msisdns=[str(row["msisdn"]) for row in rows],
        )


@pytest.mark.parametrize("campaign_type", ["batch", "realtime"])
def test_vnext_campaign_types_forward_flash_to_shared_ingress(monkeypatch, campaign_type):
    runtime = _CaptureRuntime()
    monkeypatch.setattr(vnext_service, "_RUNTIME", runtime)
    monkeypatch.setattr(
        vnext_service,
        "_template_map",
        lambda *_args: {
            "by_lang": {
                "english": {"content": "Hi {msisdn}", "source_addr": "SRC"},
            },
            "fallback": None,
            "default": None,
            "direct": None,
        },
    )
    monkeypatch.setattr(vnext_service, "_excluded", lambda _values: set())

    result = vnext_service.enqueue_rows(
        campaign_type,
        {
            "id": 11 if campaign_type == "batch" else 12,
            "status": "in_progress",
            "smpp_config_id": 7,
            "phone_prefix": "251",
            "flash_sms": True,
            "dispatch_generation": 3,
        },
        [{"msisdn": "0900000001", "language": "english"}],
        source_file=f"{campaign_type}.csv",
        batch_id=4 if campaign_type == "batch" else None,
    )

    assert result["created"] == 1
    assert len(runtime.calls) == 1
    _rows, kwargs = runtime.calls[0]
    assert kwargs["campaign_type"] == campaign_type
    assert kwargs["flash_sms"] is True
