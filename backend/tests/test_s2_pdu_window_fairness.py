"""S2 PDU-window and shallow-handoff ownership oracles."""

from dataclasses import replace
import threading
import time

import pytest

from services import bounded_dispatch_service as dispatch
from services.smpp_gateway import SmppGateway, _SmppConnection


@pytest.fixture(autouse=True)
def fair_mode(monkeypatch):
    monkeypatch.setattr(dispatch, "_frozen_mode", "fair")
    return dispatch


def connection(window=2):
    item = _SmppConnection({"id": 7, "window_size": window, "timeout": 1}, 1)
    item.window_size = window
    return item


def test_fair_pdu_window_is_hard_and_exactly_once_released():
    conn = connection(2)
    first = conn._pdu_reserve(conn._connection_generation, 0)
    second = conn._pdu_reserve(conn._connection_generation, 0)
    third = conn._pdu_reserve(conn._connection_generation, 0.01)
    assert first and second and third is None
    assert conn._pdu_window_status()["used"] == 2
    assert conn._pdu_bind(first, 101, conn._connection_generation)
    assert conn._pdu_window_status()["used"] == 2
    with conn._pdu_window_lock:
        assert conn._pdu_release_sequence_locked(101)
        assert not conn._pdu_release_sequence_locked(101)
    assert conn._pdu_window_status()["used"] == 1
    assert conn._pdu_reserve(conn._connection_generation, 0)


def test_failed_write_and_generation_isolation_fail_closed():
    conn = connection(1)
    reservation = conn._pdu_reserve(conn._connection_generation, 0)
    assert reservation
    # Ambiguous I/O has no sequence; it cannot be paid by a late response.
    old_generation = conn._connection_generation
    conn._pdu_retire()
    assert conn._pdu_reserve(old_generation, 0) is None
    assert conn._pdu_window_status()["used"] == 0
    assert conn._pdu_window_status()["credits"]["retired"] == 1


def test_fair_slot_admits_only_one_unstarted_handoff(monkeypatch):
    conn = connection(2)
    monkeypatch.setattr(conn, "_start_async_sender", lambda: None)
    monkeypatch.setattr(conn, "_start_async_reaper", lambda: None)
    callbacks = []
    assert conn.try_send_async({"id": 1}, lambda *args: callbacks.append(args))
    assert conn.try_send_async({"id": 2}, lambda *args: callbacks.append(args)) is False
    # Sender ownership release admits the next descriptor.
    conn._fair_handoff_permits.release()
    assert conn.try_send_async({"id": 3}, lambda *args: callbacks.append(args))
    conn._sender_stop.set()
    conn._send_queue.get_nowait()
    conn._send_queue.task_done()
    conn._send_queue.get_nowait()
    conn._send_queue.task_done()


def test_gateway_selects_idle_slot_without_deep_fifo_reservation():
    gateway = SmppGateway()
    config = {"id": 7, "updated_at": "rev1", "pool_size": 2}

    class Slot:
        def __init__(self, slot):
            self.config = config
            self.slot = slot
            self._fair_active_handoffs = 1 if slot == 1 else 0
            self.accepted = None
        def try_send_async(self, task, callback):
            self.accepted = task["id"]
            return self._fair_active_handoffs == 0

    slots = [Slot(1), Slot(2)]
    gateway._enabled_configs = lambda: [config]
    gateway._fair_pool = lambda item: slots
    task = {"id": 9}
    reservation = replace(dispatch.Reservation(
        pool=None, group=("batch", 1, 7), task_id=9, generation=0,
        cost={"tasks": 1, "parts": 1, "bytes": 1},
    ))
    reservation.state = "prepare"
    task["_dispatch_route_revision"] = "rev1"
    task["_dispatch_reservation"] = reservation
    assert gateway.try_send_async(task, lambda *args: None)
    assert slots[1].accepted == 9
    assert slots[0].accepted is None
