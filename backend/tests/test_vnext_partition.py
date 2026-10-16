from __future__ import annotations

import json

import pytest

from vnext.partition import (
    PartitionMap,
    PartitionMapError,
    normalize_msisdn,
    partition_for_msisdn,
)
from vnext.supervisor import OwnerConflict, OwnerLeaseStore


def test_normalize_msisdn_uses_one_canonical_country_code():
    assert normalize_msisdn("0900 000 001", "251") == "251900000001"
    assert normalize_msisdn("+251 900 000 001", "251") == "251900000001"
    assert normalize_msisdn("00251-900-000-001", "251") == "251900000001"
    assert normalize_msisdn("", "251") == ""


def test_sha256_partition_is_stable_and_uses_all_virtual_shards():
    values = [f"251900000{index:04d}" for index in range(300)]
    first = [partition_for_msisdn(value) for value in values]
    second = [partition_for_msisdn(value) for value in values]
    assert first == second
    assert all(0 <= value < 256 for value in first)
    assert len(set(first)) > 1


def test_partition_map_persists_and_rebalance_fences_owners(tmp_path):
    path = tmp_path / "partition-map.json"
    original = PartitionMap.create(path, virtual_partition_count=16, worker_count=2)
    loaded = PartitionMap.load_or_create(path, virtual_partition_count=16, worker_count=2)
    assert loaded.to_dict() == original.to_dict()
    assert len(loaded.partitions_for_worker(0)) == 8
    with pytest.raises(PartitionMapError):
        loaded.rebalance(4, safe_partitions=[0])
    before = loaded.assignment(0).owner_epoch
    loaded.rebalance(4)
    assert loaded.worker_count == 4
    assert loaded.assignment(0).owner_epoch == before + 1
    assert json.loads(path.read_text(encoding="utf-8"))["map_revision"] == 2


def test_owner_lease_rejects_dual_owner_and_increments_epoch(tmp_path):
    store = OwnerLeaseStore(tmp_path / "leases", lease_seconds=5)
    first = store.acquire(3, "worker-a", map_revision=1, requested_epoch=1)
    with pytest.raises(OwnerConflict):
        store.acquire(3, "worker-b", map_revision=1, requested_epoch=1)
    assert store.heartbeat(first)["owner_epoch"] == first["owner_epoch"]
    assert store.release(first)
    second = store.acquire(3, "worker-b", map_revision=1, requested_epoch=1)
    assert second["owner_epoch"] > first["owner_epoch"]
