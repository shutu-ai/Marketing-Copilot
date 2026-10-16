"""Route admission tests for primary/backup and explicit parallel binding."""

from __future__ import annotations

import pytest

from services.smpp_gateway import SmppGateway


class FakeConnection:
    def __init__(self, config, slot):
        self.config = dict(config)
        self.slot = slot
        self.connected = False
        self.state = "disconnected"
        self.last_error = ""
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1
        if self.config.get("fail_connect"):
            self.state = "error"
            self.last_error = "injected bind failure"
            raise ConnectionError(self.last_error)
        self.connected = True
        self.state = "bound"

    def disconnect(self):
        self.connected = False
        self.state = "disconnected"

    def status(self):
        return {
            "slot": self.slot,
            "state": self.state,
            "connected": self.connected,
            "last_error": self.last_error,
            "last_connected_at": None,
        }


def _config(config_id: int, priority: int, *, pool_size: int = 1, **extra):
    return {
        "id": config_id,
        "name": f"route-{config_id}",
        "priority": priority,
        "pool_size": pool_size,
        "window_size": 1,
        "updated_at": "rev-1",
        "reconnect_max_seconds": 1,
        "tps": 200,
        **extra,
    }


def test_default_connect_keeps_primary_backup_semantics(monkeypatch):
    monkeypatch.delenv("MC_SMPP_PARALLEL_BIND_REQUIRED", raising=False)
    primary = _config(1, 1)
    backup = _config(2, 2, pool_size=2)
    gateway = SmppGateway(FakeConnection)
    gateway._enabled_configs = lambda: [primary, backup]

    gateway.connect()

    status = gateway.status()
    assert status["active_config_id"] == 1
    assert len(status["pools"][0]["connections"]) == 1
    assert status["pools"][0]["connections"][0]["connected"] is True
    assert status["pools"][1]["connections"] == []


def test_explicit_parallel_connect_binds_every_enabled_route(monkeypatch):
    first = _config(1, 1, pool_size=1)
    second = _config(2, 2, pool_size=2)
    gateway = SmppGateway(FakeConnection)
    gateway._enabled_configs = lambda: [first, second]
    monkeypatch.setenv("MC_SMPP_PARALLEL_BIND_REQUIRED", "1")

    gateway.connect()

    status = gateway.status()
    assert status["active_config_id"] == 1
    assert all(
        connection["connected"]
        for pool in status["pools"]
        for connection in pool["connections"]
    )
    assert [len(pool["connections"]) for pool in status["pools"]] == [1, 2]


def test_parallel_connect_fails_closed_when_a_required_route_does_not_bind():
    first = _config(1, 1)
    second = _config(2, 2, fail_connect=True)
    gateway = SmppGateway(FakeConnection)
    gateway._enabled_configs = lambda: [first, second]

    with pytest.raises(RuntimeError, match="Required parallel SMSC"):
        gateway.connect(parallel=True)

    status = gateway.status()
    assert status["pools"][0]["connections"][0]["connected"] is True
    assert status["pools"][1]["connections"][0]["connected"] is False
