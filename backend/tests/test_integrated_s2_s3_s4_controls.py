"""Small deterministic checks for the integrated S2/S3/S4 control points."""

from services import bounded_dispatch_service, realtime_campaign_service


def _task(task_id, campaign_id, parts):
    return {
        "id": task_id,
        "campaign_type": "batch",
        "campaign_id": campaign_id,
        "dispatch_generation": 0,
        "claim_token": f"claim-{task_id}",
        "content": "x",
        "msisdn": f"251900{task_id:04d}",
        "_scheduled_parts": parts,
    }


def test_ready_pool_charges_socket_admission_by_parts(monkeypatch):
    monkeypatch.setenv("MC_SOCKET_DRR_QUANTUM_PARTS", "2")
    pool = bounded_dispatch_service.ReadyPool(
        bounded_dispatch_service.Limits(
            tasks=10, parts=20, bytes=2 * 1024 * 1024,
            group_tasks=8, group_parts=10, group_bytes=512 * 1024,
            campaign_tasks=8, route_tasks=10, prepare_tasks=1,
            prepare_bytes=512 * 1024, result_events=100, result_bytes=512 * 1024,
            max_task_bytes=128 * 1024, max_task_parts=10,
        )
    )
    entries = []
    task_id = 1
    for _ in range(4):
        task = _task(task_id, 1, 1)
        item = pool.reserve(task, 7, 1)
        pool.publish(item, task)
        entries.append(item)
        task_id += 1
    for _ in range(2):
        task = _task(task_id, 2, 5)
        item = pool.reserve(task, 7, 5)
        pool.publish(item, task)
        entries.append(item)
        task_id += 1

    order = []
    for _ in range(len(entries)):
        task = pool.get(timeout=0.1)
        order.append((task["campaign_id"], task["_scheduled_parts"]))
        pool.handoff(task["_dispatch_reservation"])
        pool.release(task["_dispatch_reservation"])

    assert order.index((2, 5)) < 4
    assert sum(parts for campaign, parts in order if campaign == 1) == 4
    assert sum(parts for campaign, parts in order if campaign == 2) == 10


def test_xlsx_memory_admission_is_bounded(monkeypatch):
    source = "synthetic-large.xlsx"
    monkeypatch.setattr(realtime_campaign_service.os.path, "getsize", lambda _path: 1024)
    monkeypatch.setattr(realtime_campaign_service, "XLSX_MEMORY_LIMIT_BYTES", 4096)
    monkeypatch.setattr(realtime_campaign_service, "XLSX_MEMORY_EXPANSION_FACTOR", 8)
    admitted, estimate, limit = realtime_campaign_service._xlsx_memory_admission(str(source))
    assert not admitted
    assert estimate == 8192
    assert limit == 4096
