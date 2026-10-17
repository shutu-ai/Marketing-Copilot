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

from .business_day import BusinessClock, ensure_clock, normalize_business_day


SPOOL_SCHEMA_VERSION = 1


class SpoolError(RuntimeError):
    pass


class SpoolCapacityError(SpoolError):
    """The configured hard spool budget would be exceeded."""


class SpoolExpiredError(SpoolError):
    """A record belongs to a closed business day or passed its cutoff."""


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
    # These fields are optional on read so a V1 spool can be inspected safely.
    # New admissions are normalized by TaskSpool before they are persisted.
    business_day: str = ""
    expires_at: float = 0.0

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
        if self.business_day:
            value["business_day"] = normalize_business_day(self.business_day)
        if self.expires_at:
            value["expires_at"] = float(self.expires_at)
        return value

    def is_expired(self, now: float | None = None) -> bool:
        # A record without both durable routing fields cannot be safely
        # assigned to a live business epoch.  Legacy V1 data is inspectable
        # for recovery, but it must never cross the socket fence.
        if not self.business_day or self.expires_at <= 0:
            return True
        return float(now if now is not None else time.time()) >= float(self.expires_at)

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
            business_day=str(value.get("business_day", "") or ""),
            expires_at=float(value.get("expires_at", 0.0) or 0.0),
        )
        if item.schema_version != SPOOL_SCHEMA_VERSION:
            raise SpoolCorruptionError(f"unsupported spool schema: {item.schema_version}")
        if not item.event_id or not item.normalized_msisdn:
            raise SpoolCorruptionError("spool event_id and normalized_msisdn are required")
        if item.business_day:
            try:
                normalize_business_day(item.business_day)
            except ValueError as exc:
                raise SpoolCorruptionError(str(exc)) from exc
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
                 max_bytes: int = 512 * 1024 * 1024, fsync: bool = True,
                 clock: BusinessClock | object | None = None,
                 business_timezone: str | object | None = None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.virtual_partition_count = int(virtual_partition_count)
        self.segment_max_records = max(1, int(segment_max_records))
        self.segment_max_bytes = max(1024, int(segment_max_bytes))
        self.max_bytes = max(1, int(max_bytes))
        self.fsync = bool(fsync)
        self.clock = ensure_clock(clock, business_timezone)
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
        self._lightweight_manifest_signature = self._manifest_signature
        self._lightweight_cursor_signatures: dict[str, tuple[int, int] | None] = {}
        self._lightweight_taken_signatures: dict[str, tuple[int, int] | None] = {}
        self._lightweight_cursors = dict(self._cursors)
        self._lightweight_taken_pending = {
            key: set(value) for key, value in self._taken_pending.items()
        }
        self._current_business_day = str(
            self._manifest.get("current_business_day") or self.clock.business_day()
        )
        self._manifest["current_business_day"] = self._current_business_day
        self._manifest.setdefault("business_timezone", self.clock.timezone_name)
        self._manifest.setdefault("business_epoch", 1)
        self._manifest.setdefault("closed_days", {})
        self._manifest.setdefault("daily_records", {})
        self._manifest.setdefault("daily_bytes", {})
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

    @property
    def current_business_day(self) -> str:
        return str(self._current_business_day)

    @property
    def business_epoch(self) -> int:
        return int(self._manifest.get("business_epoch", 1) or 1)

    def _now(self) -> float:
        return float(self.clock.timestamp())

    def _today_business_day(self) -> str:
        return self.clock.business_day()

    def _rebuild_daily_metadata(self) -> None:
        records: dict[str, int] = {}
        bytes_by_day: dict[str, int] = {}
        for record in self._event_index.values():
            day = str(record.business_day or "")
            records[day] = records.get(day, 0) + 1
            # The exact segment byte count is retained separately in the
            # manifest.  This estimate is only a bounded status hint.
            bytes_by_day[day] = bytes_by_day.get(day, 0) + len(
                json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")).encode("utf-8")
            ) + 1
        self._manifest["daily_records"] = records
        self._manifest["daily_bytes"] = bytes_by_day

    def _refresh_current_day_locked(self) -> None:
        value = str(self._manifest.get("current_business_day") or "")
        if value:
            self._current_business_day = value

    def _rollover_locked(self, target_day: str) -> dict:
        target_day = normalize_business_day(target_day)
        old_day = self.current_business_day
        if target_day == old_day:
            return {
                "changed": False,
                "business_day": target_day,
                "previous_business_day": old_day,
                "business_epoch": self.business_epoch,
            }
        summary = self._day_summary_locked(old_day)
        closed = self._manifest.setdefault("closed_days", {})
        summary.update({"closed_at": self._now(), "state": "closed"})
        closed[old_day] = summary
        self._current_business_day = target_day
        self._manifest["current_business_day"] = target_day
        self._manifest["business_epoch"] = self.business_epoch + 1
        self._persist_manifest()
        self._manifest_signature = self._file_signature(self._manifest_path)
        return {
            "changed": True,
            "business_day": target_day,
            "previous_business_day": old_day,
            "business_epoch": self.business_epoch,
            "closed": summary,
        }

    def _day_summary_locked(self, day: str) -> dict:
        total = 0
        pending = 0
        for record in self._event_index.values():
            if record.business_day != day:
                continue
            total += 1
            taken = int(record.ordinal) <= self.cursor(record.partition_id)
            taken = taken or int(record.ordinal) in self._taken_pending.get(
                str(record.partition_id), set()
            )
            if not taken:
                pending += 1
        return {
            "business_day": day,
            "total_records": total,
            "pending_records": pending,
            "reclaimed_records": 0,
        }

    def rollover_if_needed(self, now=None) -> dict:
        """Advance the durable epoch when the injected wall clock changes day."""

        with self._lock:
            self.refresh()
            if now is None:
                value = self._today_business_day()
            elif isinstance(now, str):
                value = normalize_business_day(now)
            else:
                value = ensure_clock(now, self.clock.timezone_name).business_day()
            return self._rollover_locked(value)

    def rotate_business_day(self, business_day: str | None = None) -> dict:
        """Durably close the old day and fence all old socket admissions."""

        with self._lock:
            self.refresh()
            target = business_day or self._today_business_day()
            return self._rollover_locked(normalize_business_day(target))

    def _normalize_for_append(self, record: SpoolRecord) -> SpoolRecord:
        if record.event_id in self._event_index:
            return record
        current = self.current_business_day
        business_day = str(record.business_day or current)
        try:
            business_day = normalize_business_day(business_day)
        except ValueError as exc:
            raise SpoolExpiredError(str(exc)) from exc
        if business_day != current:
            raise SpoolExpiredError(
                f"record business_day={business_day} is not current day={current}"
            )
        expires_at = float(record.expires_at or self.clock.cutoff(business_day))
        if expires_at <= self._now():
            raise SpoolExpiredError(f"business day {business_day} has expired")
        return replace(record, business_day=business_day, expires_at=expires_at)

    def is_record_current(self, record: SpoolRecord, now: float | None = None) -> bool:
        """Return whether a record may cross the next socket-write fence."""

        with self._lock:
            today = self._today_business_day()
            if today != self.current_business_day:
                # Refresh only when a day transition is observed.  The worker
                # calls this fence for every record; rebuilding all cursor and
                # taken state on every call made concurrent Windows workers
                # spend most of their time scanning 256 partition files.
                self.refresh()
                if today != self.current_business_day:
                    self._rollover_locked(today)
            return (
                bool(record.business_day)
                and record.business_day == self.current_business_day
                and not record.is_expired(self._now() if now is None else now)
            )

    def expire_record(self, record: SpoolRecord) -> None:
        """Advance the durable cursor for a record that can no longer send."""

        self.checkpoint(record.partition_id, record.ordinal)

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
            "current_business_day": self.clock.business_day(),
            "business_timezone": self.clock.timezone_name,
            "business_epoch": 1,
            "closed_days": {},
            "daily_records": {},
            "daily_bytes": {},
        }

    def _load_cursors(self) -> dict:
        combined = {}
        try:
            with self._cursor_path.open("r", encoding="utf-8") as handle:
                combined = json.load(handle)
        except (FileNotFoundError, OSError, ValueError, TypeError):
            combined = {}
        values = {}
        for index in range(self.virtual_partition_count):
            path = self._cursor_dir / f"partition-{index:04d}.json"
            try:
                values[str(index)] = int(json.loads(path.read_text(encoding="utf-8")).get("cursor", 0))
                continue
            except (FileNotFoundError, OSError, ValueError, TypeError):
                pass
            values[str(index)] = int(combined.get(str(index), 0))
        return values

    def _load_taken(self) -> dict[str, set[int]]:
        combined = {}
        try:
            with self._taken_path.open("r", encoding="utf-8") as handle:
                combined = json.load(handle)
        except (FileNotFoundError, OSError, ValueError, TypeError):
            combined = {}
        values = {}
        for index in range(self.virtual_partition_count):
            path = self._taken_dir / f"partition-{index:04d}.json"
            try:
                values[str(index)] = {
                    int(item)
                    for item in json.loads(path.read_text(encoding="utf-8")).get("taken", [])
                }
                continue
            except (FileNotFoundError, OSError, ValueError, TypeError):
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
            self._event_index.clear()
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
            self._rebuild_daily_metadata()
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
                    self._refresh_current_day_locked()
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

    def _refresh_lightweight_cursor_state(self) -> None:
        """Refresh only changed cursor/taken JSON, never segment contents."""

        for index in range(self.virtual_partition_count):
            key = str(index)
            cursor_path = self._cursor_dir / f"partition-{index:04d}.json"
            taken_path = self._taken_dir / f"partition-{index:04d}.json"
            cursor_signature = self._file_signature(cursor_path)
            taken_signature = self._file_signature(taken_path)
            if key not in self._lightweight_cursor_signatures or (
                cursor_signature != self._lightweight_cursor_signatures[key]
            ):
                value = int(self._cursors.get(key, 0))
                if cursor_signature is not None:
                    try:
                        value = int(json.loads(cursor_path.read_text(encoding="utf-8")).get("cursor", 0))
                    except (OSError, ValueError, TypeError):
                        value = int(self._lightweight_cursors.get(key, 0))
                self._lightweight_cursors[key] = value
                self._lightweight_cursor_signatures[key] = cursor_signature
            if key not in self._lightweight_taken_signatures or (
                taken_signature != self._lightweight_taken_signatures[key]
            ):
                values: set[int] = set(self._taken_pending.get(key, set()))
                if taken_signature is not None:
                    try:
                        values = {
                            int(item) for item in json.loads(
                                taken_path.read_text(encoding="utf-8")
                            ).get("taken", [])
                        }
                    except (OSError, ValueError, TypeError):
                        values = set(self._lightweight_taken_pending.get(key, set()))
                self._lightweight_taken_pending[key] = values
                self._lightweight_taken_signatures[key] = taken_signature

    def lightweight_snapshot(self) -> dict:
        """Return an idle-safe snapshot without rebuilding the segment index.

        The manifest's maintained record/byte counters and per-partition
        cursor/taken files are sufficient for the pending boundary.  Segment
        JSON and CRC records remain the responsibility of ``refresh`` and the
        detailed ``status``/recovery APIs.
        """

        with self._lock:
            try:
                manifest = self._load_manifest()
            except (OSError, ValueError, TypeError, SpoolError):
                manifest = self._manifest
            manifest_signature = self._file_signature(self._manifest_path)
            self._lightweight_manifest_signature = manifest_signature
            self._refresh_lightweight_cursor_state()
            current_day = str(manifest.get("current_business_day") or self.current_business_day)
            pending_by_partition = {}
            for partition in range(self.virtual_partition_count):
                key = str(partition)
                cursor = int(self._lightweight_cursors.get(key, 0))
                taken = self._lightweight_taken_pending.get(key, set())
                pending = 0
                for entry in manifest.get("segments", {}).get(key, []):
                    if str(entry.get("business_day") or "") != current_day:
                        continue
                    records = int(entry.get("records", 0) or 0)
                    first = int(entry.get("first_ordinal", 0) or 0)
                    last = int(entry.get("last_ordinal", first + records - 1) or 0)
                    consumed = max(0, min(last, cursor) - first + 1)
                    taken_in_segment = sum(
                        1 for ordinal in taken if first <= int(ordinal) <= last
                    )
                    pending += max(0, records - consumed - taken_in_segment)
                pending_by_partition[key] = pending
            return {
                "schema_version": SPOOL_SCHEMA_VERSION,
                "virtual_partition_count": self.virtual_partition_count,
                "total_records": int(manifest.get("total_records", 0) or 0),
                "total_bytes": int(manifest.get("total_bytes", 0) or 0),
                "business_day": current_day,
                "business_epoch": int(manifest.get("business_epoch", 1) or 1),
                "business_timezone": str(manifest.get("business_timezone") or self.clock.timezone_name),
                "pending_records": sum(pending_by_partition.values()),
                "pending_by_partition": pending_by_partition,
                "pending_by_business_day": {
                    current_day: sum(pending_by_partition.values()),
                },
                "cursors": dict(self._lightweight_cursors),
                "taken_pending": {
                    key: sorted(value)
                    for key, value in self._lightweight_taken_pending.items()
                    if value
                },
                "recovery_issues": list(self._recovery_issues),
                "snapshot_mode": "lightweight",
                "full_recovery_required": manifest_signature != self._manifest_signature,
            }

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

    def _active_segment(self, partition_id: int, encoded_size: int,
                        business_day: str) -> tuple[dict, Path]:
        key = str(int(partition_id))
        entries = self._manifest["segments"].setdefault(key, [])
        if entries:
            current = entries[-1]
            if (str(current.get("business_day") or "") == business_day
                    and int(current["records"]) < self.segment_max_records
                    and int(current["bytes"]) + encoded_size <= self.segment_max_bytes):
                return current, self._segment_path(partition_id, str(current["filename"]))
        filename = f"segment-{len(entries) + 1:08d}.jsonl"
        current = {
            "filename": filename, "records": 0, "bytes": 0,
            "first_ordinal": 0, "last_ordinal": 0,
            "business_day": business_day,
        }
        entries.append(current)
        return current, self._segment_path(partition_id, filename)

    def _append_locked(self, record: SpoolRecord, handle_cache: dict[Path, object]) -> SpoolRecord:
        partition_id = int(record.partition_id)
        if not 0 <= partition_id < self.virtual_partition_count:
            raise ValueError("record partition_id out of range")
        if record.event_id in self._event_index:
            return self._event_index[record.event_id]
        record = self._normalize_for_append(record)
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
        entry, path = self._active_segment(
            partition_id, len(encoded), record.business_day,
        )
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
        daily_records = self._manifest.setdefault("daily_records", {})
        daily_bytes = self._manifest.setdefault("daily_bytes", {})
        daily_records[record.business_day] = int(daily_records.get(record.business_day, 0)) + 1
        daily_bytes[record.business_day] = int(daily_bytes.get(record.business_day, 0)) + len(encoded)
        self._event_index[record.event_id] = record
        return record

    def append(self, record: SpoolRecord) -> tuple[SpoolRecord, bool]:
        """Append one record; return ``(record, inserted)``."""

        with self._lock:
            today = self._today_business_day()
            if today != self.current_business_day:
                self.refresh()
                if today != self.current_business_day:
                    self._rollover_locked(today)
            if record.event_id in self._event_index:
                return self._event_index[record.event_id], False
            record = self._normalize_for_append(record)
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
            today = self._today_business_day()
            if today != self.current_business_day:
                self.refresh()
                if today != self.current_business_day:
                    self._rollover_locked(today)
            # Validate the complete batch before opening any segment.  A late
            # source chunk therefore cannot leave a mixed-day partial append.
            prepared = []
            for record in values:
                prepared.append(
                    record if record.event_id in self._event_index
                    else self._normalize_for_append(record)
                )
            handles: dict[Path, object] = {}
            appended: list[SpoolRecord] = []
            inserted_count = 0
            try:
                for record in prepared:
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
        today = self._today_business_day()
        if today != self.current_business_day:
            with self._lock:
                self.refresh()
                if today != self.current_business_day:
                    self._rollover_locked(today)
        self.refresh(partition)
        current_day = self.current_business_day
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
                    # A missing business day is deliberately fail-closed: it
                    # may be inspected for recovery, but it is never sent.
                    if (record.business_day == current_day
                            and record.ordinal > int(cursor)
                            and record.ordinal not in self._taken_pending.get(str(partition), set())):
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
            self.rollover_if_needed()
            self.refresh()
            pending_by_partition = {}
            pending_by_day: dict[str, int] = {}
            expired = 0
            for partition in range(self.virtual_partition_count):
                cursor = self.cursor(partition)
                pending = 0
                taken = self._taken_pending.get(str(partition), set())
                for record in self._event_index.values():
                    if record.partition_id != partition or record.business_day != self.current_business_day:
                        continue
                    if record.is_expired(self._now()):
                        expired += 1
                    if int(record.ordinal) > cursor and int(record.ordinal) not in taken:
                        pending += 1
                pending_by_partition[str(partition)] = pending
            for record in self._event_index.values():
                if record.business_day:
                    pending_by_day.setdefault(record.business_day, 0)
                    if (record.business_day == self.current_business_day
                            and record.ordinal > self.cursor(record.partition_id)
                            and record.ordinal not in self._taken_pending.get(str(record.partition_id), set())):
                        pending_by_day[record.business_day] += 1
            return {
                "schema_version": SPOOL_SCHEMA_VERSION,
                "virtual_partition_count": self.virtual_partition_count,
                "total_records": len(self._event_index),
                "total_bytes": self._disk_bytes(),
                "business_day": self.current_business_day,
                "business_epoch": self.business_epoch,
                "business_timezone": self.clock.timezone_name,
                "pending_records": sum(pending_by_partition.values()),
                "pending_by_partition": pending_by_partition,
                "pending_by_business_day": pending_by_day,
                "expired_records": expired,
                "closed_days": dict(self._manifest.get("closed_days", {})),
                "cursors": dict(self._cursors),
                "taken_pending": {key: sorted(value) for key, value in self._taken_pending.items() if value},
                "recovery_issues": list(self._recovery_issues),
            }

    def reclaim_business_day(self, business_day: str, *, force: bool = False) -> dict:
        """Remove only fully attributable, closed-day segment files.

        The method is intentionally explicit.  Runtime rollover fences old
        admissions first; callers should invoke reclamation after workers have
        stopped or otherwise released their old-epoch references.  Segments
        containing an unattributed V1 record are retained rather than guessed
        into a new day.
        """

        day = normalize_business_day(business_day)
        with self._lock:
            self.refresh()
            if day == self.current_business_day:
                return {"business_day": day, "reclaimed": False, "reason": "current_day"}
            closed = self._manifest.setdefault("closed_days", {}).get(day)
            if not closed and not force:
                return {"business_day": day, "reclaimed": False, "reason": "not_closed"}
            removed_records = 0
            removed_bytes = 0
            retained_segments = 0
            for partition in range(self.virtual_partition_count):
                key = str(partition)
                entries = list(self._manifest["segments"].get(key, []))
                retained = []
                for entry in entries:
                    entry_day = str(entry.get("business_day") or "")
                    path = self._segment_path(partition, str(entry["filename"]))
                    removable = entry_day == day
                    if not removable and not entry_day:
                        # Legacy entries have no trustworthy day metadata.
                        # Scan only this candidate and remove it iff every
                        # complete record explicitly belongs to the closed day.
                        candidate_records = []
                        try:
                            with path.open("rb") as handle:
                                for raw in handle:
                                    if not raw.endswith(b"\n"):
                                        removable = False
                                        break
                                    item = SpoolRecord.from_dict(json.loads(raw.decode("utf-8")))
                                    candidate_records.append(item)
                            removable = bool(candidate_records) and all(
                                item.business_day == day for item in candidate_records
                            )
                        except (OSError, ValueError, UnicodeError, SpoolError):
                            removable = False
                    if removable:
                        try:
                            size = int(path.stat().st_size)
                        except OSError:
                            size = 0
                        try:
                            path.unlink()
                        except FileNotFoundError:
                            pass
                        removed_records += int(entry.get("records", 0) or 0)
                        removed_bytes += size
                    else:
                        retained.append(entry)
                        retained_segments += 1
                self._manifest["segments"][key] = retained
                directory = self.root / f"partition-{partition:04d}"
                try:
                    directory.rmdir()
                except OSError:
                    pass
            if removed_records:
                self._event_index = {
                    key: item for key, item in self._event_index.items()
                    if item.business_day != day
                }
                self._manifest["total_records"] = len(self._event_index)
                self._manifest["total_bytes"] = self._disk_bytes()
                closed = self._manifest.setdefault("closed_days", {}).setdefault(day, {})
                closed.update({
                    "state": "reclaimed",
                    "reclaimed_records": removed_records,
                    "reclaimed_bytes": removed_bytes,
                })
                self._rebuild_daily_metadata()
                self._persist_manifest()
                self._manifest_signature = self._file_signature(self._manifest_path)
            return {
                "business_day": day,
                "reclaimed": bool(removed_records),
                "reclaimed_records": removed_records,
                "reclaimed_bytes": removed_bytes,
                "retained_segments": retained_segments,
                "reason": "complete" if removed_records else "unattributed_or_empty",
            }

    def cleanup_closed_days(self, *, force: bool = False) -> list[dict]:
        with self._lock:
            days = list(self._manifest.get("closed_days", {}))
        return [self.reclaim_business_day(day, force=force) for day in days]

    def recovery_report(self) -> dict:
        return {
            "root": str(self.root),
            "total_records": len(self._event_index),
            "business_day": self.current_business_day,
            "business_epoch": self.business_epoch,
            "closed_days": dict(self._manifest.get("closed_days", {})),
            "issues": list(self._recovery_issues),
            "crc_verified": True,
        }
