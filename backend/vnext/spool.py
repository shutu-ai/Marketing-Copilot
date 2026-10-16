"""Durable append-only task spool used by the VNext data plane."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import zlib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Iterator


SPOOL_SCHEMA_VERSION = 1


class SpoolError(RuntimeError):
    pass


class SpoolCapacityError(SpoolError):
    """The configured hard spool budget would be exceeded."""


class SpoolCorruptionError(SpoolError):
    """A complete spool record failed its CRC or JSON contract."""


@dataclass(frozen=True)
class SpoolRecord:
    schema_version: int
    event_id: str
    partition_id: int
    ordinal: int
    campaign_type: str
    campaign_id: int
    campaign_generation: int
    route_id: int
    normalized_msisdn: str
    payload_ref: str
    message: str
    part_count_hint: int
    source_id: str
    created_at: float
    crc: str = ""
    flash_sms: bool = False
    source_addr: str = ""

    def unsigned_dict(self) -> dict:
        value = {
            "schema_version": int(self.schema_version),
            "event_id": str(self.event_id),
            "partition_id": int(self.partition_id),
            "ordinal": int(self.ordinal),
            "campaign_type": str(self.campaign_type),
            "campaign_id": int(self.campaign_id),
            "campaign_generation": int(self.campaign_generation),
            "route_id": int(self.route_id),
            "normalized_msisdn": str(self.normalized_msisdn),
            "payload_ref": str(self.payload_ref),
            "message": str(self.message),
            "part_count_hint": int(self.part_count_hint),
            "source_id": str(self.source_id),
            "created_at": float(self.created_at),
        }
        # Keep the V1 wire shape byte-for-byte compatible for ordinary SMS
        # records.  Optional business routing fields are present only when
        # they carry a non-default value, so an old spool remains replayable.
        if self.flash_sms:
            value["flash_sms"] = True
        if self.source_addr:
            value["source_addr"] = str(self.source_addr)
        return value

    def with_crc(self) -> "SpoolRecord":
        value = json.dumps(self.unsigned_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        crc = f"{zlib.crc32(value.encode('utf-8')) & 0xffffffff:08x}"
        return replace(self, crc=crc)

    def to_dict(self) -> dict:
        item = self.with_crc()
        return {**item.unsigned_dict(), "crc": item.crc}

    @classmethod
    def from_dict(cls, value: dict, *, verify_crc: bool = True) -> "SpoolRecord":
        required = {
            "schema_version", "event_id", "partition_id", "ordinal", "campaign_type",
            "campaign_id", "campaign_generation", "route_id", "normalized_msisdn",
            "payload_ref", "message", "part_count_hint", "source_id", "created_at", "crc",
        }
        missing = required.difference(value)
        if missing:
            raise SpoolCorruptionError(f"missing spool fields: {sorted(missing)}")
        item = cls(
            schema_version=int(value["schema_version"]),
            event_id=str(value["event_id"]),
            partition_id=int(value["partition_id"]),
            ordinal=int(value["ordinal"]),
            campaign_type=str(value["campaign_type"]),
            campaign_id=int(value["campaign_id"]),
            campaign_generation=int(value["campaign_generation"]),
            route_id=int(value["route_id"]),
            normalized_msisdn=str(value["normalized_msisdn"]),
            payload_ref=str(value["payload_ref"]),
            message=str(value["message"]),
            part_count_hint=int(value["part_count_hint"]),
            source_id=str(value["source_id"]),
            created_at=float(value["created_at"]),
            crc=str(value["crc"]),
            flash_sms=bool(value.get("flash_sms", False)),
            source_addr=str(value.get("source_addr", "")),
        )
        if item.schema_version != SPOOL_SCHEMA_VERSION:
            raise SpoolCorruptionError(f"unsupported spool schema: {item.schema_version}")
        if not item.event_id or not item.normalized_msisdn:
            raise SpoolCorruptionError("spool event_id and normalized_msisdn are required")
        if verify_crc and item.with_crc().crc != item.crc:
            raise SpoolCorruptionError(f"spool CRC mismatch for event {item.event_id}")
        return item


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        replaced = False
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                replaced = True
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.005 * (attempt + 1))
        if not replaced:
            raise SpoolError(f"unable to atomically replace spool state: {path}")
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class TaskSpool:
    """One single-writer spool containing independent logical partitions."""

    def __init__(self, root: str | os.PathLike[str], *, virtual_partition_count: int = 256,
                 segment_max_records: int = 1000, segment_max_bytes: int = 4 * 1024 * 1024,
                 max_bytes: int = 512 * 1024 * 1024, fsync: bool = True):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.virtual_partition_count = int(virtual_partition_count)
        self.segment_max_records = max(1, int(segment_max_records))
        self.segment_max_bytes = max(1024, int(segment_max_bytes))
        self.max_bytes = max(1, int(max_bytes))
        self.fsync = bool(fsync)
        self._lock = threading.RLock()
        self._manifest_path = self.root / "manifest.json"
        self._cursor_path = self.root / "cursors.json"
        self._taken_path = self.root / "taken.json"
        self._cursor_dir = self.root / "cursors"
        self._taken_dir = self.root / "taken"
        self._manifest = self._load_manifest()
        self._cursors = self._load_cursors()
        self._taken_pending = self._load_taken()
        self._event_index: dict[str, SpoolRecord] = {}
        self._recovery_issues: list[str] = []
        self._manifest_signature = self._file_signature(self._manifest_path)
        self._segment_offsets: dict[Path, int] = {}
        self._recover_index()

    @staticmethod
    def _file_signature(path: Path) -> tuple[int, int] | None:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        return int(stat.st_mtime_ns), int(stat.st_size)

    def _add_recovery_issue(self, value: str) -> None:
        if value not in self._recovery_issues:
            self._recovery_issues.append(value)

    def _load_manifest(self) -> dict:
        if self._manifest_path.exists():
            with self._manifest_path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
            if int(value.get("schema_version", 0)) != SPOOL_SCHEMA_VERSION:
                raise SpoolError("unsupported spool manifest schema")
            if int(value.get("virtual_partition_count", 0)) != self.virtual_partition_count:
                raise SpoolError("spool virtual partition count cannot change")
            return value
        return {
            "schema_version": SPOOL_SCHEMA_VERSION,
            "virtual_partition_count": self.virtual_partition_count,
            "segments": {str(index): [] for index in range(self.virtual_partition_count)},
            "next_ordinal": {str(index): 1 for index in range(self.virtual_partition_count)},
            "total_records": 0,
            "total_bytes": 0,
            "updated_at": time.time(),
        }

    def _load_cursors(self) -> dict:
        combined = {}
        if self._cursor_path.exists():
            with self._cursor_path.open("r", encoding="utf-8") as handle:
                combined = json.load(handle)
        values = {}
        for index in range(self.virtual_partition_count):
            path = self._cursor_dir / f"partition-{index:04d}.json"
            if path.exists():
                try:
                    values[str(index)] = int(json.loads(path.read_text(encoding="utf-8")).get("cursor", 0))
                    continue
                except (OSError, ValueError, TypeError):
                    pass
            values[str(index)] = int(combined.get(str(index), 0))
        return values

    def _load_taken(self) -> dict[str, set[int]]:
        combined = {}
        if self._taken_path.exists():
            with self._taken_path.open("r", encoding="utf-8") as handle:
                combined = json.load(handle)
        values = {}
        for index in range(self.virtual_partition_count):
            path = self._taken_dir / f"partition-{index:04d}.json"
            if path.exists():
                try:
                    values[str(index)] = {int(item) for item in json.loads(path.read_text(encoding="utf-8")).get("taken", [])}
                    continue
                except (OSError, ValueError, TypeError):
                    pass
            values[str(index)] = {int(item) for item in combined.get(str(index), [])}
        return values

    def _segment_path(self, partition_id: int, filename: str) -> Path:
        path = self.root / f"partition-{int(partition_id):04d}" / filename
        if path.parent.parent != self.root:
            raise SpoolError("invalid spool segment path")
        return path

    def _recover_index(self) -> None:
        with self._lock:
            self._segment_offsets = {}
            for partition_id in range(self.virtual_partition_count):
                entries = self._manifest["segments"].get(str(partition_id), [])
                for entry in entries:
                    path = self._segment_path(partition_id, str(entry["filename"]))
                    if not path.exists():
                        self._add_recovery_issue(f"missing segment: {path.name}")
                        continue
                    self._scan_segment(path, start=0)
            self._manifest["total_records"] = len(self._event_index)
            self._manifest["total_bytes"] = self._disk_bytes()
            self._manifest_signature = self._file_signature(self._manifest_path)

    def _scan_segment(self, path: Path, *, start: int = 0) -> None:
        """Index complete records from *start*, retaining a safe tail offset."""

        scan_start = max(0, int(start))
        try:
            size = int(path.stat().st_size)
            if scan_start > size:
                scan_start = 0
            with path.open("rb") as handle:
                handle.seek(scan_start)
                while True:
                    line_start = handle.tell()
                    raw = handle.readline()
                    if not raw:
                        self._segment_offsets[path] = handle.tell()
                        return
                    if not raw.endswith(b"\n"):
                        self._add_recovery_issue(f"ignored partial final line: {path.name}")
                        self._segment_offsets[path] = line_start
                        return
                    try:
                        record = SpoolRecord.from_dict(json.loads(raw.decode("utf-8")))
                    except (ValueError, UnicodeError, SpoolError) as exc:
                        self._add_recovery_issue(f"invalid spool record {path.name}: {exc}")
                        self._segment_offsets[path] = handle.tell()
                        continue
                    self._event_index.setdefault(record.event_id, record)
        except OSError as exc:
            self._add_recovery_issue(f"cannot read segment {path.name}: {exc}")

    def refresh(self, partition_id: int | None = None) -> None:
        """Refresh append-only state written by another process.

        Long-lived workers keep a ``TaskSpool`` instance, while ingress owns
        the manifest writer.  Atomic manifest replacement gives readers a
        cheap generation signal; only new bytes in changed segments are
        scanned.  Cursor/taken files are refreshed for the requested shard so
        restart and live polling never depend on constructor-time snapshots.
        """

        with self._lock:
            signature = self._file_signature(self._manifest_path)
            if signature != self._manifest_signature and signature is not None:
                try:
                    loaded = self._load_manifest()
                except (OSError, ValueError, TypeError, SpoolError):
                    loaded = None
                if loaded is not None:
                    self._manifest = loaded
                    for value in range(self.virtual_partition_count):
                        entries = self._manifest["segments"].get(str(value), [])
                        for entry in entries:
                            path = self._segment_path(value, str(entry["filename"]))
                            if not path.exists():
                                self._add_recovery_issue(f"missing segment: {path.name}")
                                continue
                            previous = self._segment_offsets.get(path, 0)
                            size = int(path.stat().st_size)
                            self._scan_segment(path, start=previous if previous <= size else 0)
                    self._manifest["total_records"] = len(self._event_index)
                    self._manifest["total_bytes"] = self._disk_bytes()
                    self._manifest_signature = signature

            if partition_id is None:
                self._cursors = self._load_cursors()
                self._taken_pending = self._load_taken()
            else:
                partition = int(partition_id)
                cursor_path = self._cursor_dir / f"partition-{partition:04d}.json"
                taken_path = self._taken_dir / f"partition-{partition:04d}.json"
                if cursor_path.exists():
                    try:
                        self._cursors[str(partition)] = int(json.loads(cursor_path.read_text(encoding="utf-8")).get("cursor", 0))
                    except (OSError, ValueError, TypeError):
                        pass
                if taken_path.exists():
                    try:
                        self._taken_pending[str(partition)] = {
                            int(item) for item in json.loads(taken_path.read_text(encoding="utf-8")).get("taken", [])
                        }
                    except (OSError, ValueError, TypeError):
                        pass

    def _disk_bytes(self) -> int:
        total = 0
        for path in self.root.glob("partition-*/*.jsonl"):
            try:
                total += path.stat().st_size
            except FileNotFoundError:
                pass
        return total

    def _persist_manifest(self) -> None:
        self._manifest["updated_at"] = time.time()
        _atomic_write_json(self._manifest_path, self._manifest)

    def _persist_cursors(self, partition_id: int | None = None) -> None:
        if partition_id is None:
            _atomic_write_json(self._cursor_path, self._cursors)
            _atomic_write_json(
                self._taken_path,
                {key: sorted(values) for key, values in self._taken_pending.items()},
            )
            return
        partition = int(partition_id)
        _atomic_write_json(
            self._cursor_dir / f"partition-{partition:04d}.json",
            {"partition_id": partition, "cursor": int(self._cursors[str(partition)])},
        )
        _atomic_write_json(
            self._taken_dir / f"partition-{partition:04d}.json",
            {"partition_id": partition, "taken": sorted(self._taken_pending.get(str(partition), set()))},
        )

    def _active_segment(self, partition_id: int, encoded_size: int) -> tuple[dict, Path]:
        key = str(int(partition_id))
        entries = self._manifest["segments"].setdefault(key, [])
        if entries:
            current = entries[-1]
            if (int(current["records"]) < self.segment_max_records
                    and int(current["bytes"]) + encoded_size <= self.segment_max_bytes):
                return current, self._segment_path(partition_id, str(current["filename"]))
        filename = f"segment-{len(entries) + 1:08d}.jsonl"
        current = {"filename": filename, "records": 0, "bytes": 0, "first_ordinal": 0, "last_ordinal": 0}
        entries.append(current)
        return current, self._segment_path(partition_id, filename)

    def _append_locked(self, record: SpoolRecord, handle_cache: dict[Path, object]) -> SpoolRecord:
        partition_id = int(record.partition_id)
        if not 0 <= partition_id < self.virtual_partition_count:
            raise ValueError("record partition_id out of range")
        if record.event_id in self._event_index:
            return self._event_index[record.event_id]
        next_ordinal = int(self._manifest["next_ordinal"].get(str(partition_id), 1))
        if int(record.ordinal) <= 0:
            record = replace(record, ordinal=next_ordinal)
        elif int(record.ordinal) < next_ordinal:
            raise SpoolError("record ordinal must be monotonic within a partition")
        record = record.with_crc()
        encoded = (json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        # ``total_bytes`` is rebuilt from the segment files during recovery and
        # then maintained for every append.  Re-scanning all 256 partition
        # directories for every record turns a large ingress batch into an
        # O(records * partitions) operation, which is especially visible on
        # Windows and on network-backed filesystems.  A single spool writer
        # owns this manifest, so the maintained value is the authoritative
        # capacity counter between recoveries.
        projected = int(self._manifest.get("total_bytes", 0)) + len(encoded)
        if projected > self.max_bytes:
            raise SpoolCapacityError(f"spool byte budget exhausted: {projected}>{self.max_bytes}")
        entry, path = self._active_segment(partition_id, len(encoded))
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = handle_cache.get(path)
        if handle is None:
            handle = path.open("ab")
            handle_cache[path] = handle
        handle.write(encoded)
        entry["records"] = int(entry["records"]) + 1
        entry["bytes"] = int(entry["bytes"]) + len(encoded)
        entry["first_ordinal"] = int(entry["first_ordinal"] or record.ordinal)
        entry["last_ordinal"] = int(record.ordinal)
        self._manifest["next_ordinal"][str(partition_id)] = int(record.ordinal) + 1
        self._manifest["total_records"] = int(self._manifest.get("total_records", 0)) + 1
        self._manifest["total_bytes"] = projected
        self._event_index[record.event_id] = record
        return record

    def append(self, record: SpoolRecord) -> tuple[SpoolRecord, bool]:
        """Append one record; return ``(record, inserted)``."""

        with self._lock:
            if record.event_id in self._event_index:
                return self._event_index[record.event_id], False
            handles: dict[Path, object] = {}
            try:
                inserted = self._append_locked(record, handles)
                for handle in handles.values():
                    handle.flush()
                    if self.fsync:
                        os.fsync(handle.fileno())
                self._persist_manifest()
                return inserted, True
            finally:
                for handle in handles.values():
                    handle.close()

    def append_many(self, records: Iterable[SpoolRecord]) -> tuple[list[SpoolRecord], int]:
        """Append a bounded group and fsync each touched segment once."""

        values = list(records)
        if not values:
            return [], 0
        with self._lock:
            handles: dict[Path, object] = {}
            appended: list[SpoolRecord] = []
            inserted_count = 0
            try:
                for record in values:
                    before = record.event_id in self._event_index
                    item = self._append_locked(record, handles)
                    appended.append(item)
                    inserted_count += int(not before)
                for handle in handles.values():
                    handle.flush()
                    if self.fsync:
                        os.fsync(handle.fileno())
                self._persist_manifest()
                return appended, inserted_count
            finally:
                for handle in handles.values():
                    handle.close()

    def iter_from(self, partition_id: int, cursor: int = 0) -> Iterator[SpoolRecord]:
        """Yield valid records after an ordinal cursor in append order."""

        partition = int(partition_id)
        if not 0 <= partition < self.virtual_partition_count:
            raise ValueError("partition_id out of range")
        self.refresh(partition)
        entries = list(self._manifest["segments"].get(str(partition), []))
        for entry in entries:
            path = self._segment_path(partition, str(entry["filename"]))
            if not path.exists():
                continue
            with path.open("rb") as handle:
                for raw in handle:
                    if not raw.endswith(b"\n"):
                        continue
                    try:
                        record = SpoolRecord.from_dict(json.loads(raw.decode("utf-8")))
                    except (ValueError, UnicodeError, SpoolError) as exc:
                        raise SpoolCorruptionError(f"invalid record in {path}: {exc}") from exc
                    if record.ordinal > int(cursor) and record.ordinal not in self._taken_pending.get(str(partition), set()):
                        yield record

    def checkpoint(self, partition_id: int, ordinal: int) -> None:
        """Record a durable dispatch-taken ordinal.

        Fair scheduling can take ordinal 20 before ordinal 19.  The spool
        keeps a bounded pending set and advances the contiguous cursor when
        the gap is filled, so restart recovery never replays a taken record.
        """

        partition = int(partition_id)
        value = int(ordinal)
        with self._lock:
            current = int(self._cursors.get(str(partition), 0))
            if value < current:
                return
            if value > current:
                self._taken_pending.setdefault(str(partition), set()).add(value)
            pending = self._taken_pending.setdefault(str(partition), set())
            while current + 1 in pending:
                current += 1
                pending.remove(current)
            self._cursors[str(partition)] = current
            self._persist_cursors(partition)

    def cursor(self, partition_id: int) -> int:
        return int(self._cursors.get(str(int(partition_id)), 0))

    def status(self) -> dict:
        with self._lock:
            self.refresh()
            pending_by_partition = {}
            for partition in range(self.virtual_partition_count):
                cursor = self.cursor(partition)
                entries = self._manifest["segments"].get(str(partition), [])
                records = sum(int(entry.get("records", 0)) for entry in entries)
                pending_by_partition[str(partition)] = max(
                    0, records - cursor - len(self._taken_pending.get(str(partition), set()))
                )
            return {
                "schema_version": SPOOL_SCHEMA_VERSION,
                "virtual_partition_count": self.virtual_partition_count,
                "total_records": len(self._event_index),
                "total_bytes": self._disk_bytes(),
                "pending_records": sum(pending_by_partition.values()),
                "pending_by_partition": pending_by_partition,
                "cursors": dict(self._cursors),
                "taken_pending": {key: sorted(value) for key, value in self._taken_pending.items() if value},
                "recovery_issues": list(self._recovery_issues),
            }

    def recovery_report(self) -> dict:
        return {
            "root": str(self.root),
            "total_records": len(self._event_index),
            "issues": list(self._recovery_issues),
            "crc_verified": True,
        }
