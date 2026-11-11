from collections import deque
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from services import realtime_ingestion_worker as worker
from services import realtime_campaign_service as campaigns
from datetime import datetime


def test_expired_cleanup_only_removes_inbox_copy(tmp_path):
    watch = tmp_path / "watch"
    inbox = tmp_path / "campaign" / "inbox"
    watch.mkdir()
    inbox.mkdir(parents=True)
    source = watch / "input.csv"
    copy = inbox / "input.csv"
    source.write_text("source")
    copy.write_text("copy")
    assert not worker._remove_inbox_copy(str(source), str(inbox.parent), str(watch))
    assert source.exists()
    assert worker._remove_inbox_copy(str(copy), str(inbox.parent), str(watch))
    assert not copy.exists()
    assert source.exists()
    assert not worker._remove_inbox_copy(str(source), str(tmp_path), str(tmp_path))


def test_different_drive_paths_do_not_block_inbox(tmp_path, monkeypatch):
    inbox = tmp_path / "campaign" / "inbox"
    inbox.mkdir(parents=True)
    copy = inbox / "input.csv"
    copy.write_text("copy")
    monkeypatch.setattr(worker.os.path, "commonpath", lambda _: (_ for _ in ()).throw(ValueError("different drives")))
    assert worker._remove_inbox_copy(str(copy), str(inbox.parent), "other-drive")
    monkeypatch.setattr(campaigns, "cleanup_closed_window_ingestion_files", lambda: None)
    monkeypatch.setattr(campaigns, "_daily_window_ended", lambda _: False)
    calls = []
    def read(_fn, *, operation):
        calls.append(operation)
        return [{"id": 1, "campaign_dir": str(inbox.parent), "watch_dir": "other-drive"}] if len(calls) == 1 else []
    monkeypatch.setattr(campaigns, "read_transaction", read)
    campaigns.process_realtime_inboxes()
    assert "realtime.inbox.processing_files" in calls


def test_campaign_lifecycle_end_closes_window_before_daily_cutoff():
    campaign = {"end_time": "2026-11-11 12:00:00", "send_start_time": "08:00", "send_end_time": "18:00"}
    assert not campaigns._daily_window_ended(campaign, datetime(2026, 11, 11, 11, 59))
    assert campaigns._daily_window_ended(campaign, datetime(2026, 11, 11, 12, 0))


@pytest.fixture
def queues(monkeypatch):
    pending = {1: deque(range(1, 8)), 2: deque(range(101, 108))}
    worker._campaign_inflight.clear()
    worker._campaign_cursor = 0
    monkeypatch.setenv("MC_REALTIME_INGEST_WORKERS", "4")
    monkeypatch.setattr(
        worker.ingestion_job_service, "pending_campaign_ids",
        lambda _: [key for key, values in pending.items() if values],
    )

    def claim(_kind, _worker, campaign_id):
        if not pending[campaign_id]:
            return None
        return {"id": pending[campaign_id].popleft(), "campaign_id": campaign_id}

    monkeypatch.setattr(worker.ingestion_job_service, "claim_file", claim)
    yield pending
    worker._campaign_inflight.clear()


def test_base_slots_take_priority_and_idle_slots_are_borrowed(queues):
    queues[2].clear()
    first = worker._claim_realtime_file("one")
    second = worker._claim_realtime_file("two")
    assert first["campaign_id"] == second["campaign_id"] == 1
    queues[2].append(101)
    assert worker._claim_realtime_file("three")["campaign_id"] == 2
    assert worker._claim_realtime_file("four")["campaign_id"] == 1
    assert worker._claim_realtime_file("five") is None


def test_slot_released_even_when_lease_setup_fails(queues, monkeypatch):
    class BrokenLease:
        def __init__(self, *_args):
            pass

        def __enter__(self):
            raise RuntimeError("lease unavailable")

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(worker.realtime_ingestion_state, "FileLease", BrokenLease)
    with pytest.raises(RuntimeError, match="lease unavailable"):
        worker.process_once("worker")
    assert not worker._campaign_inflight
    assert worker._claim_realtime_file("next") is not None


def test_concurrent_workers_keep_total_bounded_and_reclaim_slots(queues, monkeypatch):
    barrier = threading.Barrier(5)
    release = threading.Event()
    running = []

    class Lease:
        def __init__(self, *_args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    def process(job, _worker):
        running.append(job["campaign_id"])
        barrier.wait(timeout=5)
        assert release.wait(timeout=5)
        return True

    monkeypatch.setattr(worker.realtime_ingestion_state, "FileLease", Lease)
    monkeypatch.setattr(worker, "_process_claimed_file", process)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker.process_once, str(index)) for index in range(4)]
        try:
            barrier.wait(timeout=5)
            assert sorted(running) == [1, 1, 2, 2]
            assert worker.process_once("overflow") is False
            assert sum(worker._campaign_inflight.values()) == 4
        finally:
            release.set()
        assert all(future.result(timeout=5) for future in futures)
    assert not worker._campaign_inflight
