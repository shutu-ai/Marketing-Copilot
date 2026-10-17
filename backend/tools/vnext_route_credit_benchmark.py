"""Benchmark the VNext route-credit durability granularity.

``--lease-size 1`` reproduces the old per-PDU durable reservation shape for a
controlled comparison.  The production default is the batched lease size of
8.  This tool is evidence-only; it never changes the application setting.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from pathlib import Path

from vnext.route_credits import RouteCreditBroker


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * max(0.0, min(1.0, float(quantile)))
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


async def run(*, count: int, rate: float, workers: int, lease_size: int,
              root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    brokers = [RouteCreditBroker(root) for _ in range(max(1, int(workers)))]
    for broker in brokers:
        broker.CREDIT_LEASE_SIZE = max(1, int(lease_size))

    async def acquire_one(index: int) -> tuple[float, float]:
        started = time.perf_counter()
        delay = await brokers[index % len(brokers)].acquire("benchmark-route", rate)
        return float(delay), max(0.0, time.perf_counter() - started)

    started = time.perf_counter()
    waits = await asyncio.gather(*[
        acquire_one(index) for index in range(max(0, int(count)))
    ])
    elapsed = max(1e-9, time.perf_counter() - started)
    scheduled_waits = [item[0] for item in waits]
    actual_waits = [item[1] for item in waits]
    snapshot = {
        "reservations": sum(int(item.snapshot()["reservations"]) for item in brokers),
        "credit_refills": sum(int(item.snapshot()["credit_refills"]) for item in brokers),
        "file_credit_refills": sum(int(item.snapshot()["file_credit_refills"]) for item in brokers),
        "credit_wait_seconds": round(sum(float(item.snapshot()["credit_wait_seconds"]) for item in brokers), 6),
    }
    return {
        "count": int(count),
        "rate_tps": float(rate),
        "workers": int(workers),
        "lease_size": int(lease_size),
        "root": str(root),
        "elapsed_seconds": round(elapsed, 6),
        "throughput_per_second": round(float(count) / elapsed, 3) if count else 0.0,
        "wait_seconds": {
            "scheduled_p50": round(_percentile(scheduled_waits, 0.50), 6),
            "scheduled_p95": round(_percentile(scheduled_waits, 0.95), 6),
            "scheduled_p99": round(_percentile(scheduled_waits, 0.99), 6),
            "actual_p50": round(_percentile(actual_waits, 0.50), 6),
            "actual_p95": round(_percentile(actual_waits, 0.95), 6),
            "actual_p99": round(_percentile(actual_waits, 0.99), 6),
        },
        "snapshot": snapshot,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=320)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lease-size", type=int, default=32)
    parser.add_argument("--root", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    root = Path(args.root).resolve() if args.root else Path(tempfile.mkdtemp(prefix="mc-vnext-credit-benchmark-"))
    result = asyncio.run(run(
        count=max(0, args.count), rate=max(0.0, args.rate),
        workers=max(1, args.workers), lease_size=max(1, args.lease_size), root=root,
    ))
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
