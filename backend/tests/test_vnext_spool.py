from __future__ import annotations

import json

import pytest

from vnext.spool import SpoolCapacityError, SpoolCorruptionError, SpoolRecord, TaskSpool


def record(event_id: str, ordinal: int = 0, partition: int = 2) -> SpoolRecord:
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
