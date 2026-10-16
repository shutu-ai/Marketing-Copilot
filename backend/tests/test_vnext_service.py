from __future__ import annotations

import asyncio

from services import vnext_service
from vnext.spool import TaskSpool


class _FakeRuntime:
    def __init__(self, root):
        self._started = True
        self.root = root
        self.spool = TaskSpool(root / "spool", virtual_partition_count=16, max_bytes=100_000)
        from vnext.ingress import IngressRouter
        self.ingress = IngressRouter(self.spool)
        self.route_configs = {"7": {"route_id": 7}}

    def enqueue_chunk(self, *args, **kwargs):
        return self.ingress.enqueue_chunk(*args, **kwargs)


def test_service_adapter_renders_templates_and_preserves_vnext_fields(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    monkeypatch.setattr(vnext_service, "_template_map", lambda *_args: {
        "by_lang": {"english": {"content": "Hi {msisdn}", "source_addr": "SRC"}},
        "fallback": None, "default": None, "direct": None,
    })
    monkeypatch.setattr(vnext_service, "_excluded", lambda values: {"251900000002"})
    result = vnext_service.enqueue_rows(
        "batch",
        {"id": 11, "status": "in_progress", "smpp_config_id": 7,
         "phone_prefix": "251", "flash_sms": True, "dispatch_generation": 3},
        [
            {"msisdn": "0900000001", "language": "english"},
            {"msisdn": "0900000002", "language": "english"},
            {"msisdn": "0900000001", "language": "english"},
        ],
        source_file="customers.csv", batch_id=4,
    )
    assert result["created"] == 1
    assert result["excluded"] == 1
    assert result["duplicate"] == 1
    records = list(fake.spool.iter_from(0, 0))
    records += [item for partition in range(1, 16) for item in fake.spool.iter_from(partition, 0)]
    assert len(records) == 1
    assert records[0].normalized_msisdn == "251900000001"
    assert records[0].message == "Hi 251900000001"
    assert records[0].flash_sms is True
    assert records[0].source_addr == "SRC"


def test_service_control_maps_campaign_states(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)
    fake.set_campaign_state = lambda campaign_type, campaign_id, state: setattr(
        fake, "last_control", (campaign_type, campaign_id, state)
    )
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    assert vnext_service.set_campaign_state("batch", 5, "in_progress")["state"] == "running"
    assert fake.last_control == ("batch", 5, "running")
