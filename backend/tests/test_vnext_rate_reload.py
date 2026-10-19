from __future__ import annotations

import asyncio
import threading

from models.smpp import SmppConfigUpdate
from services import smpp_service
from services import vnext_service
from vnext.runtime import VNextRuntime
from vnext.smpp_transport import AsyncSmppTransport
from vnext.spool import TaskSpool
from vnext.worker import ShardWorker


def test_smpp_transport_updates_rate_without_touching_bind_state():
    transport = AsyncSmppTransport({
        "route_id": 2,
        "host": "smsc.example",
        "port": 2775,
        "system_id": "system",
        "password": "secret",
        "pool_size": 4,
        "tps": 200,
        "tps_scope": "route_shared",
    })

    result = transport.update_rate_limit(200, "per_bind")

    assert result == {"route_id": "2", "tps": 200.0, "tps_scope": "per_bind"}
    assert transport.tps == 200.0
    assert transport.tps_scope == "per_bind"
    assert transport.pool_size == 4
    assert transport.connected is False
    assert transport.system_id == "system"


class _RateTransport:
    def __init__(self, config, _credits):
        self.route_id = str(config["route_id"])
        self.tps = float(config.get("tps", 0))
        self.tps_scope = str(config.get("tps_scope", "route_shared"))
        self.updates = []

    def update_rate_limit(self, tps, tps_scope):
        self.tps = float(tps)
        self.tps_scope = str(tps_scope)
        self.updates.append((self.tps, self.tps_scope))

    def snapshot(self):
        return {
            "route_id": self.route_id,
            "tps": self.tps,
            "tps_scope": self.tps_scope,
        }


def test_worker_updates_existing_and_future_route_rate_state(tmp_path):
    created = []

    def factory(config, credits):
        value = _RateTransport(config, credits)
        created.append(value)
        return value

    async def exercise():
        worker = ShardWorker(
            0,
            (0,),
            TaskSpool(tmp_path / "spool", virtual_partition_count=1, max_bytes=100_000),
            {2: {
                "route_id": 2,
                "tps": 200,
                "tps_scope": "route_shared",
                "_worker_bind_target": 2,
            }},
            transport_factory=factory,
        )
        await worker._transport(2)
        await worker._transport(2)
        result = worker.apply_route_rates({"2": {"tps": 200, "tps_scope": "per_bind"}})
        return worker, result

    worker, result = asyncio.run(exercise())
    assert result == {"updated_routes": ["2"], "updated_transports": 2}
    assert worker.route_configs["2"]["tps_scope"] == "per_bind"
    assert all(item.updates == [(200.0, "per_bind")] for item in created)


class _BroadcastSupervisor:
    def __init__(self):
        self.messages = []

    def broadcast(self, message):
        self.messages.append(message)
        return {"sent_worker_ids": [0, 1], "failed_worker_ids": []}


def test_runtime_reload_route_rates_broadcasts_without_restarting(tmp_path):
    runtime = VNextRuntime.__new__(VNextRuntime)
    runtime._started = True
    runtime.supervisor = _BroadcastSupervisor()
    runtime.route_configs = {
        "2": {"route_id": 2, "tps": 200, "tps_scope": "route_shared"}
    }
    runtime._route_config_lock = threading.RLock()

    result = runtime.reload_route_rates({
        "2": {"route_id": 2, "tps": 200, "tps_scope": "per_bind"}
    })

    assert result["ok"] is True
    assert result["updated_routes"] == ["2"]
    assert runtime.route_configs["2"]["tps_scope"] == "per_bind"
    assert runtime.supervisor.messages == [{
        "type": "reload_route_rates",
        "routes": {"2": {"route_id": 2, "tps": 200.0, "tps_scope": "per_bind"}},
    }]


def test_smpp_rate_only_update_is_reported_as_hot_applied(monkeypatch):
    row = {
        "id": 2,
        "name": "primary",
        "password": "secret",
        "enabled": 1,
        "tps": 200,
        "tps_scope": "per_bind",
    }
    updated_row = {**row, "tps": 250}
    marked = []

    class _Result:
        def __init__(self, value):
            self.value = value

        def fetchone(self):
            return self.value

    class _Connection:
        def __init__(self):
            self.select_count = 0

        def execute(self, query, _params=()):
            if query.lstrip().upper().startswith("SELECT"):
                self.select_count += 1
                return _Result(row if self.select_count == 1 else updated_row)
            return _Result(None)

    monkeypatch.setattr(
        smpp_service,
        "write_transaction",
        lambda writer, operation: writer(_Connection()),
    )
    monkeypatch.setattr(
        smpp_service,
        "_invalidate_gateway_config_cache",
        lambda **kwargs: marked.append(kwargs),
    )
    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(vnext_service, "apply_route_rates", lambda: {"ok": True})
    monkeypatch.setattr(
        vnext_service,
        "config_state",
        lambda: {"restart_required": False, "effective_state": "applied"},
    )

    result = smpp_service.update_config(2, SmppConfigUpdate(tps=250, tps_scope="per_bind"))

    assert result["hot_applied"] is True
    assert result["restart_required"] is False
    assert result["effective_state"] == "applied"
    assert marked == [{"mark_restart": False}]


def test_smpp_web_full_form_only_tps_change_is_hot_applied(monkeypatch):
    existing = {
        "id": 2,
        "name": "primary",
        "host": "smsc.example",
        "port": 2775,
        "system_id": "system",
        "password": "secret",
        "source_addr": "Marketing",
        "bind_type": "transceiver",
        "address_ton": 0,
        "address_npi": 0,
        "data_coding": 0,
        "pool_size": 4,
        "window_size": 50,
        "enquire_interval": 30,
        "timeout": 10,
        "tps": 200,
        "tps_scope": "per_bind",
        "priority": 100,
        "reconnect_max_seconds": 60,
        "delivery_receipt_mode": "optional",
        "enabled": 1,
    }
    updated = {**existing, "tps": 250}
    marked = []

    class _Result:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class _Connection:
        def __init__(self):
            self.select_count = 0

        def execute(self, query, _params=()):
            if query.lstrip().upper().startswith("SELECT"):
                self.select_count += 1
                return _Result(existing if self.select_count == 1 else updated)
            return _Result(None)

    monkeypatch.setattr(
        smpp_service,
        "write_transaction",
        lambda writer, operation: writer(_Connection()),
    )
    monkeypatch.setattr(
        smpp_service,
        "_invalidate_gateway_config_cache",
        lambda **kwargs: marked.append(kwargs),
    )
    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(vnext_service, "apply_route_rates", lambda: {"ok": True})
    monkeypatch.setattr(
        vnext_service,
        "config_state",
        lambda: {"restart_required": False, "effective_state": "applied"},
    )

    # The Web form submits unchanged structural fields together with tps.
    result = smpp_service.update_config(
        2,
        SmppConfigUpdate(
            name="primary", host="smsc.example", port=2775,
            system_id="system", source_addr="Marketing", bind_type="transceiver",
            address_ton=0, address_npi=0, data_coding=0, pool_size=4,
            window_size=50, enquire_interval=30, timeout=10, tps=250,
            tps_scope="per_bind", priority=100, reconnect_max_seconds=60,
            delivery_receipt_mode="optional", enabled=1,
        ),
    )

    assert result["hot_applied"] is True
    assert result["restart_required"] is False
    assert marked == [{"mark_restart": False}]
