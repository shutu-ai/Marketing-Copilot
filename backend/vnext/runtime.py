"""VNext runtime composition: ingress, supervisor, workers and writer."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import tempfile
import time
from pathlib import Path

from .ingress import IngressRouter
from .projection_writer import ProjectionBackpressure, ProjectionWriterProcess, ProgressJournal
from .route_credits import RouteCreditBroker
from .spool import TaskSpool
from .supervisor import WorkerSupervisor
from .worker import ShardWorker


def _runtime_worker_entry(worker_id: int, partitions: tuple[int, ...], control, context: dict) -> None:
    spool = TaskSpool(
        context["spool_root"], virtual_partition_count=int(context["virtual_partition_count"]),
        max_bytes=int(context["max_spool_bytes"]),
    )
    credits = RouteCreditBroker(context["credits_root"])
    journal = ProgressJournal(Path(context["progress_root"]) / f"worker-{worker_id}.jsonl")
    projection_queue = context.get("projection_queue")

    def emit(event: dict) -> None:
        journal.append(event)
        if projection_queue is not None:
            try:
                projection_queue.put_nowait(event)
            except queue.Full as exc:
                raise ProjectionBackpressure("VNext projection IPC queue is full") from exc

    worker = ShardWorker(
        worker_id, tuple(partitions), spool, context["route_configs"], credits=credits,
        progress_sink=emit, max_inflight=int(context["max_inflight"]),
        prefetch_per_partition=int(context["prefetch_per_partition"]),
    )

    async def run() -> None:
        while True:
            if control.poll():
                message = control.recv()
                if message == "stop":
                    return
            state_path = Path(context["control_state_path"])
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    for item in state.get("campaigns", []):
                        worker.set_campaign_state(item["campaign_type"], int(item["campaign_id"]), item["state"])
                except (OSError, ValueError, KeyError, TypeError):
                    pass
            before = worker.snapshot()["counters"].get("taken", 0)
            await worker.run_until_idle()
            after = worker.snapshot()["counters"].get("taken", 0)
            if after == before:
                await asyncio.sleep(0.1)

    try:
        asyncio.run(run())
    except (EOFError, OSError):
        return


class VNextRuntime:
    """Own the one VNext lifecycle without starting legacy send services."""

    def __init__(self, root: str | os.PathLike[str], route_configs: dict[int | str, dict], *,
                 worker_count: int = 4, virtual_partition_count: int = 256,
                 max_spool_bytes: int = 512 * 1024 * 1024, projection_queue_size: int = 8192,
                 max_inflight: int = 128, prefetch_per_partition: int = 64):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.spool_root = self.root / "spool"
        self.credits_root = self.root / "route-credits"
        self.progress_root = self.root / "progress"
        self.control_state_path = self.root / "control-state.json"
        self.virtual_partition_count = int(virtual_partition_count)
        self.max_spool_bytes = int(max_spool_bytes)
        self.spool = TaskSpool(
            self.spool_root, virtual_partition_count=self.virtual_partition_count,
            max_bytes=self.max_spool_bytes,
        )
        self.ingress = IngressRouter(self.spool)
        self.route_configs = {str(key): dict(value) for key, value in route_configs.items()}
        self.worker_count = max(1, min(32, int(worker_count)))
        self.max_inflight = max(1, int(max_inflight))
        self.prefetch_per_partition = max(1, int(prefetch_per_partition))
        self.projection = ProjectionWriterProcess(
            self.root / "projection", queue_size=projection_queue_size,
        )
        self.supervisor: WorkerSupervisor | None = None
        self._started = False
        self._control_state: dict[tuple[str, int], str] = {}

    def start(self) -> None:
        if self._started:
            return
        self.progress_root.mkdir(parents=True, exist_ok=True)
        self.control_state_path.write_text(json.dumps({"campaigns": []}), encoding="utf-8")
        self.projection.start()
        context = {
            "spool_root": str(self.spool_root),
            "credits_root": str(self.credits_root),
            "progress_root": str(self.progress_root),
            "control_state_path": str(self.control_state_path),
            "virtual_partition_count": self.virtual_partition_count,
            "max_spool_bytes": self.max_spool_bytes,
            "route_configs": self.route_configs,
            "max_inflight": self.max_inflight,
            "prefetch_per_partition": self.prefetch_per_partition,
            "projection_queue": self.projection.queue,
        }
        self.supervisor = WorkerSupervisor(
            self.root, worker_count=self.worker_count,
            virtual_partition_count=self.virtual_partition_count,
            worker_target=_runtime_worker_entry, worker_args=(context,),
        )
        self.supervisor.start()
        self._started = True

    def stop(self) -> None:
        if self.supervisor:
            self.supervisor.stop()
        if self._started:
            # Let the dedicated DB owner drain its durable progress queue.  A
            # worker journal remains the recovery source if the bounded
            # shutdown deadline is ever exceeded.
            self.projection.stop(timeout=30.0)
        self.supervisor = None
        self._started = False

    def enqueue_chunk(self, *args, **kwargs):
        if not self._started:
            raise RuntimeError("VNext runtime is not started")
        return self.ingress.enqueue_chunk(*args, **kwargs)

    def set_campaign_state(self, campaign_type: str, campaign_id: int, state: str) -> None:
        self._control_state[(str(campaign_type), int(campaign_id))] = str(state)
        payload = {
            "updated_at": time.time(),
            "campaigns": [
                {"campaign_type": key[0], "campaign_id": key[1], "state": value}
                for key, value in sorted(self._control_state.items())
            ],
        }
        temporary = self.control_state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temporary, self.control_state_path)

    def status(self) -> dict:
        spool = TaskSpool(
            self.spool_root, virtual_partition_count=self.virtual_partition_count,
            max_bytes=self.max_spool_bytes,
        )
        return {
            "started": self._started,
            "spool": spool.status(),
            "supervisor": self.supervisor.status() if self.supervisor else None,
            "projection": self.projection.snapshot(),
            "controls": [
                {"campaign_type": key[0], "campaign_id": key[1], "state": value}
                for key, value in self._control_state.items()
            ],
        }

    async def wait_for_idle(self, timeout: float = 30.0) -> dict:
        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            status = self.status()
            if status["spool"]["pending_records"] == 0:
                return status
            await asyncio.sleep(0.05)
        return self.status()
