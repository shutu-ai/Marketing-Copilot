"""Stable MSISDN normalization, virtual partitioning and owner fencing."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_VIRTUAL_PARTITIONS = 256
MAP_SCHEMA_VERSION = 1
_ASCII_NON_DIGIT = re.compile(r"[^0-9]+")


class PartitionMapError(RuntimeError):
    """The partition map cannot safely be used or changed."""


def _digits(value: object) -> str:
    return _ASCII_NON_DIGIT.sub("", str(value or ""))


def normalize_msisdn(value: object, phone_prefix: object = "") -> str:
    """Return the one canonical number used by every VNext shard decision.

    The existing application stores ``phone_prefix`` separately and strips one
    national leading zero before adding it.  VNext makes that rule explicit:
    punctuation and whitespace are removed, an international ``00`` marker is
    removed, one national leading zero is removed, and the configured prefix
    is added only when it is not already present.  An empty prefix is valid
    for deployments whose input is already in a complete international form.
    """

    number = _digits(value)
    prefix = _digits(phone_prefix)
    if not number:
        return ""
    if number.startswith("00"):
        number = number[2:]
    if number.startswith("0"):
        number = number[1:]
    if not number:
        return ""
    if prefix and not number.startswith(prefix):
        number = prefix + number
    return number


def partition_for_msisdn(normalized_msisdn: str, virtual_partition_count: int = DEFAULT_VIRTUAL_PARTITIONS) -> int:
    """Map a canonical number to a stable virtual partition.

    Python's process-randomized ``hash()`` is intentionally not used.  The
    first eight bytes of SHA-256 are interpreted as a big-endian uint64, which
    keeps the mapping stable across Python versions and processes.
    """

    value = str(normalized_msisdn or "")
    if not value:
        raise ValueError("normalized_msisdn must not be empty")
    count = int(virtual_partition_count)
    if count < 1:
        raise ValueError("virtual_partition_count must be positive")
    digest = hashlib.sha256(value.encode("ascii")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % count


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class PartitionAssignment:
    partition_id: int
    worker_id: int
    owner_epoch: int


class PartitionMap:
    """Versioned logical-shard to worker assignment.

    A map is immutable for a running deployment.  ``rebalance`` is an explicit
    operation and requires the caller to prove that all partitions are safe to
    move; it never silently changes the number of virtual partitions.
    """

    def __init__(self, path: Path, payload: dict):
        self.path = Path(path)
        self._payload = self._validate(payload)

    @staticmethod
    def _validate(payload: dict) -> dict:
        if int(payload.get("schema_version", 0)) != MAP_SCHEMA_VERSION:
            raise PartitionMapError("unsupported partition-map schema")
        count = int(payload.get("virtual_partition_count", 0))
        workers = int(payload.get("worker_count", 0))
        assignments = payload.get("assignments")
        if count < 1 or workers < 1 or not isinstance(assignments, dict):
            raise PartitionMapError("invalid partition-map dimensions")
        if set(assignments) != {str(index) for index in range(count)}:
            raise PartitionMapError("partition-map does not cover every virtual partition")
        for key, item in assignments.items():
            if int(key) != int(item.get("partition_id", -1)):
                raise PartitionMapError("partition assignment key mismatch")
            if not 0 <= int(item.get("worker_id", -1)) < workers:
                raise PartitionMapError("partition assignment worker out of range")
            if int(item.get("owner_epoch", 0)) < 1:
                raise PartitionMapError("partition owner epoch must be positive")
        return json.loads(json.dumps(payload))

    @classmethod
    def create(cls, path: Path, virtual_partition_count: int = DEFAULT_VIRTUAL_PARTITIONS,
               worker_count: int = 4) -> "PartitionMap":
        count = int(virtual_partition_count)
        workers = int(worker_count)
        if count < 1 or workers < 1:
            raise ValueError("virtual_partition_count and worker_count must be positive")
        assignments = {
            str(partition_id): {
                "partition_id": partition_id,
                "worker_id": partition_id % workers,
                "owner_epoch": 1,
            }
            for partition_id in range(count)
        }
        payload = {
            "schema_version": MAP_SCHEMA_VERSION,
            "map_revision": 1,
            "virtual_partition_count": count,
            "worker_count": workers,
            "created_at": time.time(),
            "updated_at": time.time(),
            "assignments": assignments,
        }
        item = cls(path, payload)
        item.save()
        return item

    @classmethod
    def load_or_create(cls, path: Path, virtual_partition_count: int = DEFAULT_VIRTUAL_PARTITIONS,
                       worker_count: int = 4) -> "PartitionMap":
        path = Path(path)
        if not path.exists():
            return cls.create(path, virtual_partition_count, worker_count)
        with path.open("r", encoding="utf-8") as handle:
            return cls(path, json.load(handle))

    def save(self) -> None:
        _atomic_write_json(self.path, self._payload)

    @property
    def map_revision(self) -> int:
        return int(self._payload["map_revision"])

    @property
    def virtual_partition_count(self) -> int:
        return int(self._payload["virtual_partition_count"])

    @property
    def worker_count(self) -> int:
        return int(self._payload["worker_count"])

    def to_dict(self) -> dict:
        return json.loads(json.dumps(self._payload))

    def assignment(self, partition_id: int) -> PartitionAssignment:
        item = self._payload["assignments"][str(int(partition_id))]
        return PartitionAssignment(
            partition_id=int(item["partition_id"]),
            worker_id=int(item["worker_id"]),
            owner_epoch=int(item["owner_epoch"]),
        )

    def partitions_for_worker(self, worker_id: int) -> tuple[int, ...]:
        worker = int(worker_id)
        if not 0 <= worker < self.worker_count:
            raise ValueError("worker_id out of range")
        return tuple(
            partition_id
            for partition_id in range(self.virtual_partition_count)
            if self.assignment(partition_id).worker_id == worker
        )

    def fence(self, partition_ids: Iterable[int] | None = None) -> "PartitionMap":
        """Increment owner epochs without changing the worker assignment."""

        ids = range(self.virtual_partition_count) if partition_ids is None else partition_ids
        for partition_id in ids:
            key = str(int(partition_id))
            item = self._payload["assignments"][key]
            item["owner_epoch"] = int(item["owner_epoch"]) + 1
        self._payload["map_revision"] = int(self._payload["map_revision"]) + 1
        self._payload["updated_at"] = time.time()
        self._validate(self._payload)
        self.save()
        return self

    def rebalance(self, worker_count: int, *, safe_partitions: Iterable[int] | None = None) -> "PartitionMap":
        """Create a new map revision after an explicit safe-freeze boundary."""

        new_count = int(worker_count)
        if new_count < 1:
            raise ValueError("worker_count must be positive")
        safe = set(range(self.virtual_partition_count) if safe_partitions is None else map(int, safe_partitions))
        if safe != set(range(self.virtual_partition_count)):
            raise PartitionMapError("all partitions must be safe before a map change")
        for partition_id in range(self.virtual_partition_count):
            item = self._payload["assignments"][str(partition_id)]
            item["worker_id"] = partition_id % new_count
            item["owner_epoch"] = int(item["owner_epoch"]) + 1
        self._payload["worker_count"] = new_count
        self._payload["map_revision"] = int(self._payload["map_revision"]) + 1
        self._payload["updated_at"] = time.time()
        self._validate(self._payload)
        self.save()
        return self
