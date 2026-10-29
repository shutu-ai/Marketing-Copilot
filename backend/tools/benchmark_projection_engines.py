"""Isolated engine comparison, not a production throughput certification."""
import argparse
import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from vnext.projection_writer import ProjectionWriter
from vnext.sqlite_projection import SQLiteProjectionWriter
from vnext.task_index import connect_reader


def run(root, engine, events, batch):
    path = root / ("projection.duckdb" if engine == "duckdb" else "vnext-projection.sqlite3")
    if path.exists():
        raise RuntimeError("use a fresh output directory")
    cls = ProjectionWriter if engine == "duckdb" else SQLiteProjectionWriter
    writer = cls(path, batch_size=batch, queue_size=batch)
    writer.start()
    stop = threading.Event()
    failures, reads, latencies = [], [], []

    def reader():
        while not stop.is_set():
            conn = connect_reader(writer.task_index_path)
            if conn:
                try:
                    conn.execute("SELECT event_id FROM vnext_task_index ORDER BY business_day,created_at DESC,ordinal DESC,event_id DESC LIMIT 100").fetchall()
                    reads.append(1)
                except Exception as exc:
                    failures.append(str(exc))
                finally:
                    conn.close()
            stop.wait(.02)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for thread in threads:
        thread.start()
    started = time.perf_counter()
    day = datetime.now(timezone.utc).date().isoformat()
    try:
        for offset in range(0, events, batch):
            for i in range(offset, min(events, offset + batch)):
                writer.submit(dict(event_id=str(i), partition_id=i % 4, ordinal=i,
                                   campaign_type="batch", campaign_id=i % 20, route_id=1,
                                   state="socket_written", parts_written=3, parts_total=3,
                                   created_at=time.time(), business_day=day,
                                   message="多语言短信 ሰላም " * 20, normalized_msisdn=str(10000000+i)))
            begin = time.perf_counter()
            writer.flush()
            latencies.append((time.perf_counter() - begin) * 1000)
        elapsed = time.perf_counter() - started
        count = writer._conn.execute("SELECT COUNT(*) FROM vnext_progress_events").fetchone()[0]
        total = writer._conn.execute("SELECT SUM(socket_written) FROM vnext_projection").fetchone()[0]
        assert count == total == events
        assert not failures, failures
        ordered = sorted(latencies)
        return dict(engine=engine, events=events, seconds=round(elapsed,3),
                    events_per_second=round(events/elapsed), reader_queries=len(reads),
                    batch_ms_p50=round(statistics.median(ordered),3),
                    batch_ms_p99=round(ordered[min(len(ordered)-1,int(len(ordered)*.99))],3),
                    wal_bytes=sum(p.stat().st_size for p in root.glob("*wal")))
    finally:
        stop.set()
        for thread in threads:
            thread.join()
        writer.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--events", type=int, default=100000)
    parser.add_argument("--batch", type=int, default=512)
    args = parser.parse_args()
    if args.events < 1 or args.batch < 1:
        parser.error("events and batch must be positive")
    for engine in ("sqlite", "duckdb"):
        root = args.root / engine
        root.mkdir(parents=True, exist_ok=False)
        print(json.dumps(run(root, engine, args.events, args.batch)), flush=True)


if __name__ == "__main__":
    main()
