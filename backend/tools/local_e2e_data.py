"""Deterministic external-data producer used by local E2E acceptance runs.

The generator deliberately writes through the same local-watch contract as a
real producer: ``.part`` first, atomic rename to the final timestamped name,
then a ``.ready`` marker.  It does not import Marketing Copilot services and
does not write DuckDB.
"""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime
from pathlib import Path


def make_msisdns(count: int, start: int = 1) -> list[str]:
    return [f"251{1000000 + start + index:07d}" for index in range(max(0, int(count)))]


def write_whitelist(path: str | Path, msisdns: list[str], language: str = "English") -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["msisdn", "language"])
        writer.writeheader()
        for msisdn in msisdns:
            writer.writerow({"msisdn": msisdn, "language": language})
    return target


def write_event_file(
    watch_dir: str | Path,
    prefix: str,
    file_index: int,
    rows: list[dict],
    *,
    ready: bool = True,
    chunk_size: int = 0,
    chunk_delay: float = 0.0,
    timestamp: datetime | None = None,
) -> dict:
    """Publish one watch file and return producer-side timing metadata."""
    destination = Path(watch_dir)
    destination.mkdir(parents=True, exist_ok=True)
    stamp = (timestamp or datetime.now()).strftime("%Y%m%d%H%M%S")
    filename = f"{prefix}{stamp}_{int(file_index):04d}.csv"
    staged = destination / f".{filename}.part"
    final_path = destination / filename
    started = time.perf_counter()
    with staged.open("w", encoding="utf-8", newline="") as handle:
        fields = ["msisdn", "language", "event_id"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row_index, row in enumerate(rows):
            writer.writerow({field: row.get(field, "") for field in fields})
            if chunk_size and (row_index + 1) % chunk_size == 0:
                handle.flush()
                os.fsync(handle.fileno())
                if chunk_delay:
                    time.sleep(float(chunk_delay))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staged, final_path)
    ready_path = final_path.with_name(final_path.name + ".ready")
    if ready:
        ready_path.write_text("ready\n", encoding="ascii")
    published_at = time.time()
    return {
        "file_index": int(file_index),
        "filename": filename,
        "path": str(final_path),
        "ready_path": str(ready_path) if ready else "",
        "row_count": len(rows),
        "producer_started_at": started,
        "producer_published_at": published_at,
    }


def partition_rows(rows: list[dict], file_count: int) -> list[list[dict]]:
    count = max(1, int(file_count))
    partitions = [[] for _ in range(count)]
    for index, row in enumerate(rows):
        partitions[index % count].append(row)
    return [part for part in partitions if part]
