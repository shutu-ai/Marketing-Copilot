from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from vnext.business_day import BusinessClock
from vnext.spool import SpoolCapacityError, SpoolCorruptionError, SpoolRecord, TaskSpool


def record(event_id: str, ordinal: int = 0, partition: int = 2,
           business_day: str = "", expires_at: float = 0.0) -> SpoolRecord:
    return SpoolRecord(
        schema_version=1,
        event_id=event_id,
        partition_id=partition,
        ordinal=ordinal,
        campaign_type="batch",
        campaign_id=7,
        campaign_generation=1,
        route_id=11,
        normalized_msisdn="251900000001",
        payload_ref="source.csv:1",
        message=f"hello {event_id}",
        part_count_hint=1,
        source_id="source.csv",
        created_at=1.0,
        business_day=business_day,
        expires_at=expires_at,
    )


def test_spool_append_is_durable_idempotent_and_checkpointed(tmp_path):
    spool = TaskSpool(tmp_path / "spool", virtual_partition_count=8,
                      segment_max_records=2, segment_max_bytes=512, max_bytes=20_000)
    appended, count = spool.append_many([record("a"), record("b"), record("c")])
    assert count == 3
    assert [item.ordinal for item in appended] == [1, 2, 3]
    duplicate, inserted = spool.append(record("b"))
    assert not inserted and duplicate.ordinal == 2
    assert [item.event_id for item in spool.iter_from(2)] == ["a", "b", "c"]
    spool.checkpoint(2, 1)
    spool.checkpoint(2, 2)
    reopened = TaskSpool(tmp_path / "spool", virtual_partition_count=8,
                         segment_max_records=2, segment_max_bytes=512, max_bytes=20_000)
    assert reopened.cursor(2) == 2
    assert [item.event_id for item in reopened.iter_from(2, reopened.cursor(2))] == ["c"]
    assert reopened.recovery_report()["crc_verified"] is True


def test_spool_recovery_ignores_only_a_partial_final_line(tmp_path):
    root = tmp_path / "spool"
    spool = TaskSpool(root, virtual_partition_count=4, max_bytes=20_000)
    spool.append(record("a", partition=1))
    segment = next((root / "partition-0001").glob("*.jsonl"))
    with segment.open("ab") as handle:
        handle.write(b'{"event_id":"crashed"')
    recovered = TaskSpool(root, virtual_partition_count=4, max_bytes=20_000)
    assert [item.event_id for item in recovered.iter_from(1)] == ["a"]
    assert any("partial final line" in item for item in recovered.recovery_report()["issues"])


def test_spool_rejects_complete_crc_corruption_and_hard_capacity(tmp_path):
    root = tmp_path / "spool"
    spool = TaskSpool(root, virtual_partition_count=4, max_bytes=200)
    with pytest.raises(SpoolCapacityError):
        spool.append(record("long", partition=0))

    other_root = tmp_path / "corrupt"
    other = TaskSpool(other_root, virtual_partition_count=4, max_bytes=20_000)
    other.append(record("a", partition=0))
    segment = next((other_root / "partition-0000").glob("*.jsonl"))
    value = json.loads(segment.read_text(encoding="utf-8"))
    value["message"] = "tampered"
    segment.write_text(json.dumps(value) + "\n", encoding="utf-8")
    with pytest.raises(SpoolCorruptionError):
        list(TaskSpool(other_root, virtual_partition_count=4, max_bytes=20_000).iter_from(0))


def test_spool_batch_capacity_preflight_does_not_leave_a_partial_append(tmp_path):
    probe = TaskSpool(tmp_path / "probe", virtual_partition_count=1, max_bytes=100_000)
    probe.append(record("probe", partition=0))
    one_record_bytes = probe.status()["total_bytes"]

    spool = TaskSpool(
        tmp_path / "batch", virtual_partition_count=1,
        max_bytes=one_record_bytes + 1,
    )
    with pytest.raises(SpoolCapacityError):
        spool.append_many([record("a", partition=0), record("b", partition=0)])

    assert spool.status()["total_records"] == 0
    assert list((tmp_path / "batch").glob("partition-*/*.jsonl")) == []


def test_spool_reconcile_persists_physical_bytes_after_restart(tmp_path):
    root = tmp_path / "reconcile"
    spool = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    spool.append(record("persisted", partition=0))
    spool._recovery_index.close()

    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["total_bytes"] = 0
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    recovered = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    result = recovered.reconcile_capacity(persist=True)
    assert result["physical_bytes"] > 0
    persisted = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert persisted["total_bytes"] == result["physical_bytes"]


class _WallClock:
    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value


def test_spool_gc_reclaims_orphans_and_never_reuses_segment_names(tmp_path):
    wall = _WallClock(datetime(2026, 10, 17, 23, 59, 59, tzinfo=timezone.utc))
    spool = TaskSpool(
        tmp_path / "gc", virtual_partition_count=1,
        segment_max_records=1, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    old, _ = spool.append(record("old", partition=0))
    wall.value += timedelta(seconds=2)
    assert spool.rollover_if_needed()["changed"] is True

    orphan = spool.root / "partition-0000" / "segment-99999999.jsonl"
    orphan.write_text(
        json.dumps(record("orphan", partition=0, business_day=old.business_day).to_dict()) + "\n",
        encoding="utf-8",
    )

    result = spool.cleanup_closed_days(max_segments=None)
    assert sum(int(item.get("reclaimed_records", 0) or 0) for item in result) == 2
    assert not orphan.exists()

    new, _ = spool.append(record("new", partition=0))
    assert new.business_day == "2026-10-18"
    assert new.ordinal == 2
    assert not (spool.root / "partition-0000" / "segment-00000001.jsonl").exists()


def test_spool_keeps_cross_midnight_records_until_window_expiry(tmp_path):
    wall = _WallClock(datetime(2026, 10, 17, 23, 59, tzinfo=timezone.utc))
    expiry = datetime(2026, 10, 18, 0, 30, tzinfo=timezone.utc).timestamp()
    spool = TaskSpool(
        tmp_path / "cross-midnight", virtual_partition_count=1,
        max_bytes=100_000, clock=BusinessClock(wall, "UTC"),
    )
    item, inserted = spool.append(
        record("cross", partition=0, business_day="2026-10-17", expires_at=expiry)
    )
    assert inserted is True

    wall.value += timedelta(minutes=2)
    assert spool.rollover_if_needed()["changed"] is True
    assert [value.event_id for value in spool.iter_from(0)] == [item.event_id]
    retained = spool.cleanup_closed_days(max_segments=None)
    assert retained[0]["reason"] == "blocked_active_window"

    wall.value = datetime(2026, 10, 18, 0, 31, tzinfo=timezone.utc)
    reclaimed = spool.cleanup_closed_days(max_segments=None)
    assert reclaimed[0]["reason"] == "complete"
    assert list(spool.root.glob("partition-0000/*.jsonl")) == []


def test_spool_legacy_gc_requires_record_expiry_when_recovery_index_is_empty(tmp_path):
    wall = _WallClock(datetime(2026, 10, 17, 23, 59, tzinfo=timezone.utc))
    expiry = datetime(2026, 10, 18, 0, 30, tzinfo=timezone.utc).timestamp()
    root = tmp_path / "legacy-gc"
    spool = TaskSpool(
        root, virtual_partition_count=1, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    spool.append(record("legacy-cross", partition=0, expires_at=expiry))
    segment = next(root.glob("partition-0000/*.jsonl"))
    spool._recovery_index.close()

    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["segments"]["0"][0].pop("min_expires_at", None)
    manifest["segments"]["0"][0].pop("max_expires_at", None)
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for path in root.glob("recovery-index.sqlite3*"):
        path.unlink()

    wall.value += timedelta(minutes=2)
    recovered = TaskSpool(
        root, virtual_partition_count=1, max_bytes=100_000,
        clock=BusinessClock(wall, "UTC"),
    )
    assert recovered.rollover_if_needed()["changed"] is True
    retained = recovered.cleanup_closed_days(max_segments=None)
    assert retained[0]["reason"] == "blocked_active_window"
    assert segment.exists()

    wall.value = datetime(2026, 10, 18, 0, 31, tzinfo=timezone.utc)
    reclaimed = recovered.cleanup_closed_days(max_segments=None)
    assert reclaimed[0]["reason"] == "complete"
    assert not segment.exists()


def test_spool_idle_reader_skips_fully_checkpointed_segments(tmp_path, monkeypatch):
    spool = TaskSpool(
        tmp_path / "spool", virtual_partition_count=1,
        segment_max_records=2, segment_max_bytes=20_000, max_bytes=100_000,
    )
    appended, _ = spool.append_many([
        record("a", partition=0), record("b", partition=0),
    ])
    for item in appended:
        spool.checkpoint(0, item.ordinal)

    def fail_if_decoded(_value):
        raise AssertionError("an idle reader decoded a fully consumed segment")

    monkeypatch.setattr(SpoolRecord, "from_dict", staticmethod(fail_if_decoded))
    assert list(spool.iter_from(0, spool.cursor(0))) == []


def test_spool_restart_uses_persistent_offsets_without_scanning_segments(tmp_path, monkeypatch):
    root = tmp_path / "spool"
    spool = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    spool.append(record("persisted", partition=0))
    spool._recovery_index.close()

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("restart scanned an unchanged historical segment")

    monkeypatch.setattr(TaskSpool, "_scan_segment", fail_scan)
    recovered = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    assert recovered.recovery_report()["recovery_index"]["ready"] is True
    assert recovered.recovery_report()["recovery_index"]["segment_offsets"] == 1


def test_spool_without_recovery_index_defers_full_scan_to_offline_rebuild(tmp_path, monkeypatch):
    root = tmp_path / "legacy-spool"
    spool = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    spool.append(record("legacy-persisted", partition=0))
    spool._recovery_index.close()
    for path in root.glob("recovery-index.sqlite3*"):
        path.unlink()

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("missing recovery index triggered a startup scan")

    monkeypatch.setattr(TaskSpool, "_scan_segment", fail_scan)
    recovered = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    report = recovered.recovery_report()
    assert report["recovery_index"]["ready"] is False
    assert report["recovery_index"]["segment_offsets"] == 0


def test_spool_refresh_can_use_recovery_index_from_background_thread(tmp_path):
    root = tmp_path / "threaded-spool"
    writer = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    writer.append(record("first", partition=0))

    # The long-lived runtime creates its spool on the owner thread, then a
    # later manifest update is observed by a background ingestion/scheduler
    # thread.  Keep a second instance as the writer so refresh has a real
    # appended tail to index.
    reader = TaskSpool(root, virtual_partition_count=1, max_bytes=100_000)
    writer.append(record("second", partition=0))
    errors = []

    def refresh_from_worker() -> None:
        try:
            reader.refresh()
        except Exception as exc:  # pragma: no cover - assertion reports it
            errors.append(exc)

    thread = threading.Thread(target=refresh_from_worker, name="spool-refresh-test")
    thread.start()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []

    writer._recovery_index.close()
    reader._recovery_index.close()
