"""In-process campaign fairness and send-capacity accounting.

DuckDB remains the durable queue.  This module only decides the order in
which a bounded candidate set is claimed.  Deficits are deliberately
ephemeral: after a restart every active campaign starts with equal credit,
while ownership is still fenced durably by ``claim_token``.
"""

from __future__ import annotations

import math
import os
import threading
import time
from collections import defaultdict, deque
from typing import Iterable

from database import read_transaction, write_transaction
from services import metrics_service


_lock = threading.RLock()
_deficits: dict[tuple[str, int, int], float] = {}
_virtual_service: dict[tuple[str, int, int], float] = {}
_rate_tokens: dict[tuple[str, int, int], float] = {}
_rate_updated: dict[tuple[str, int, int], float] = {}
_cursor = 0


def enabled() -> bool:
    return os.environ.get("MC_FAIR_TASK_SCHEDULER_ENABLED", "1").strip().lower() not in {
        "0", "false", "no", "off",
    }


def active_campaign_limit() -> int:
    try:
        value = int(os.environ.get("MC_FAIR_ACTIVE_CAMPAIGNS", "128"))
    except (TypeError, ValueError):
        value = 128
    return max(2, min(1000, value))


def per_campaign_candidates() -> int:
    try:
        value = int(os.environ.get("MC_FAIR_CLAIM_PER_CAMPAIGN", "30"))
    except (TypeError, ValueError):
        value = 30
    return max(1, min(200, value))


def quantum_parts() -> int:
    try:
        value = int(os.environ.get("MC_FAIR_DRR_QUANTUM_PARTS", "20"))
    except (TypeError, ValueError):
        value = 20
    return max(1, min(1000, value))


def task_parts(task: dict) -> int:
    """Return the actual SMPP split cost when the codec is available.

    The fallback is deliberately conservative for Unicode.  It is used only
    for scheduling credit; the gateway remains authoritative for encoding.
    """
    cached = task.get("_scheduled_parts")
    if cached:
        return max(1, int(cached))
    text = str(task.get("content") or "")
    try:
        from services.smpp_gateway import _make_parts
        parts = len(_make_parts(text)[0])
    except Exception:
        try:
            text.encode("ascii")
            parts = 1 if len(text) <= 160 else math.ceil(len(text) / 153.0)
        except UnicodeEncodeError:
            parts = 1 if len(text) <= 70 else math.ceil(len(text) / 67.0)
    parts = max(1, int(parts))
    task["_scheduled_parts"] = parts
    return parts


def fairness_key(task: dict) -> tuple[str, int, int]:
    return (
        str(task.get("campaign_type") or ""),
        int(task.get("campaign_id") or 0),
        int(task.get("target_smpp_config_id") or task.get("smpp_config_id") or 0),
    )


def select(candidates: Iterable[dict], limit: int) -> list[dict]:
    """Select a weighted DRR order from per-campaign oldest-first rows."""
    global _cursor
    limit = max(1, int(limit))
    groups: dict[tuple[str, int, int], deque] = defaultdict(deque)
    weights: dict[tuple[str, int, int], float] = {}
    rate_limits: dict[tuple[str, int, int], float | None] = {}
    for candidate in candidates:
        task = dict(candidate)
        key = fairness_key(task)
        groups[key].append(task)
        try:
            weight = float(task.get("scheduling_weight") or 1.0)
        except (TypeError, ValueError):
            weight = 1.0
        weights[key] = max(0.1, min(100.0, weight))
        try:
            configured_rate = float(task.get("max_parts_tps"))
        except (TypeError, ValueError):
            configured_rate = 0.0
        rate_limits[key] = configured_rate if configured_rate > 0 else None
    if not groups:
        return []

    # Stable ordering makes the policy auditable; rotating the start prevents
    # the first key from winning every truncated claim transaction.
    keys = sorted(groups)
    with _lock:
        live = set(keys)
        for stale in list(_deficits):
            if stale not in live:
                _deficits.pop(stale, None)
                _virtual_service.pop(stale, None)
                _rate_tokens.pop(stale, None)
                _rate_updated.pop(stale, None)
        baseline = min((_virtual_service.get(key, 0.0) for key in keys), default=0.0)
        for key in keys:
            _virtual_service.setdefault(key, baseline)
        start = _cursor % len(keys)
        keys = keys[start:] + keys[:start]
        _cursor = (_cursor + 1) % len(keys)

        quantum = float(quantum_parts())
        selected: list[dict] = []
        # Each pass is one DRR round.  Continuing rounds in the same claim
        # lets idle campaign credit be borrowed without sacrificing fairness.
        max_rounds = max(2, limit * 2)
        rank = {key: index for index, key in enumerate(keys)}
        rate_now = time.monotonic()
        for key in keys:
            rate = rate_limits.get(key)
            if rate is None:
                continue
            burst_capacity = max(
                rate,
                max((float(task_parts(task)) for task in groups[key]), default=1.0),
            )
            # A one-second burst absorbs scheduling jitter without allowing a
            # campaign to consume an unbounded dispatcher backlog. Capacity
            # is at least one whole multipart task so a low TPS policy cannot
            # permanently starve a message whose split cost exceeds one second.
            elapsed = max(0.0, rate_now - _rate_updated.get(key, rate_now))
            _rate_tokens[key] = min(
                burst_capacity,
                _rate_tokens.get(key, burst_capacity) + elapsed * rate,
            )
            _rate_updated[key] = rate_now
        for _round in range(max_rounds):
            for key in keys:
                if groups[key]:
                    _deficits[key] = min(
                        quantum * 20.0 * weights[key],
                        _deficits.get(key, 0.0) + quantum * weights[key],
                    )
            made_progress = False
            while len(selected) < limit:
                eligible = []
                for key in keys:
                    if not groups[key]:
                        continue
                    cost = task_parts(groups[key][0])
                    rate = rate_limits.get(key)
                    has_rate_credit = rate is None or cost <= _rate_tokens.get(key, 0.0)
                    if cost <= _deficits.get(key, 0.0) and has_rate_credit:
                        eligible.append((
                            _virtual_service.get(key, 0.0) + cost / weights[key],
                            rank[key], key, cost,
                        ))
                if not eligible:
                    break
                _finish, _rank, key, cost = min(eligible)
                task = groups[key].popleft()
                _deficits[key] -= cost
                if rate_limits.get(key) is not None:
                    _rate_tokens[key] = max(0.0, _rate_tokens.get(key, 0.0) - cost)
                _virtual_service[key] += cost / weights[key]
                selected.append(task)
                made_progress = True
                metrics_service.inc(
                    "marketing_copilot_campaign_scheduled_parts_total",
                    amount=cost,
                    labels={
                        "campaign_type": key[0],
                        "campaign_id": key[1],
                        "smpp_config_id": key[2],
                    },
                )
            if len(selected) >= limit or not any(groups.values()):
                break
            # A legal task may cost more than one quantum. No selection in a
            # round is expected while its bounded deficit accumulates.
            if not made_progress:
                continue
        return selected


def status() -> dict:
    with _lock:
        return {
            "enabled": enabled(),
            "active_deficits": len(_deficits),
            "quantum_parts": quantum_parts(),
            "per_campaign_candidates": per_campaign_candidates(),
            "active_campaign_limit": active_campaign_limit(),
            "enforced_controls": [
                "weight", "max_parts_tps", "max_pending_tasks", "max_ingest_files",
            ],
            "advisory_controls": ["min_parts_tps", "max_outstanding_parts"],
        }


def list_policies() -> list[dict]:
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT campaign_type,campaign_id,weight,min_parts_tps,max_parts_tps,
                      max_outstanding_parts,max_pending_tasks,max_ingest_files,
                      enabled,updated_at
               FROM campaign_capacity_policies
               ORDER BY campaign_type,campaign_id"""
        ).fetchall(),
        operation="capacity_policy.list",
    )
    return [dict(row) for row in rows]


def upsert_policy(campaign_type: str, campaign_id: int, values: dict) -> dict:
    campaign_type = str(campaign_type or "").strip().lower()
    if campaign_type not in {"batch", "realtime"}:
        raise ValueError("campaign_type must be batch or realtime")
    campaign_id = int(campaign_id)
    table = "batch_campaigns" if campaign_type == "batch" else "realtime_campaigns"

    def write(conn):
        exists = conn.execute(
            f"SELECT id FROM {table} WHERE id=?", (campaign_id,)
        ).fetchone()
        if not exists:
            raise ValueError("campaign not found")
        conn.execute(
            """INSERT INTO campaign_capacity_policies
               (campaign_type,campaign_id,weight,min_parts_tps,max_parts_tps,
                max_outstanding_parts,max_pending_tasks,max_ingest_files,enabled,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
               ON CONFLICT(campaign_type,campaign_id) DO UPDATE SET
                 weight=excluded.weight,min_parts_tps=excluded.min_parts_tps,
                 max_parts_tps=excluded.max_parts_tps,
                 max_outstanding_parts=excluded.max_outstanding_parts,
                 max_pending_tasks=excluded.max_pending_tasks,
                 max_ingest_files=excluded.max_ingest_files,
                 enabled=excluded.enabled,updated_at=excluded.updated_at""",
            (
                campaign_type, campaign_id, float(values.get("weight", 1.0)),
                float(values.get("min_parts_tps", 0.0)), values.get("max_parts_tps"),
                values.get("max_outstanding_parts"), values.get("max_pending_tasks"),
                values.get("max_ingest_files"), 1 if values.get("enabled", True) else 0,
            ),
        )
        return dict(conn.execute(
            "SELECT * FROM campaign_capacity_policies WHERE campaign_type=? AND campaign_id=?",
            (campaign_type, campaign_id),
        ).fetchone())

    return write_transaction(write, attempts=3, operation="capacity_policy.upsert")


def delete_policy(campaign_type: str, campaign_id: int) -> bool:
    def write(conn):
        row = conn.execute(
            """DELETE FROM campaign_capacity_policies
               WHERE campaign_type=? AND campaign_id=? RETURNING campaign_id""",
            (str(campaign_type).lower(), int(campaign_id)),
        ).fetchone()
        return bool(row)

    return write_transaction(write, attempts=3, operation="capacity_policy.delete")


def snapshot(limit: int = 200) -> dict:
    """Return bounded active-campaign queue state plus transport rates."""
    limit = max(1, min(1000, int(limit)))

    def read(conn):
        rows = conn.execute(
            """SELECT campaign_type,campaign_id,COALESCE(smpp_config_id,0) config_id,
                      MAX(campaign_name) campaign_name,
                      SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) queued,
                      SUM(CASE WHEN status='retrying' THEN 1 ELSE 0 END) retrying,
                      SUM(CASE WHEN status='sending' THEN 1 ELSE 0 END) sending,
                      SUM(CASE WHEN status='submitted' THEN 1 ELSE 0 END) submitted,
                      SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) partial_count,
                      SUM(CASE WHEN status='unknown' THEN 1 ELSE 0 END) unknown_count,
                      MIN(CASE WHEN status IN ('queued','retrying') THEN created_at END) oldest_pending_at
               FROM sms_tasks
               WHERE status IN ('queued','retrying','sending','submitted','partial','unknown')
               GROUP BY campaign_type,campaign_id,COALESCE(smpp_config_id,0)
               ORDER BY (queued+retrying+sending) DESC,campaign_type,campaign_id,config_id
               LIMIT ?""",
            (limit,),
        ).fetchall()
        policies = conn.execute(
            """SELECT campaign_type,campaign_id,weight,min_parts_tps,max_parts_tps,
                      max_outstanding_parts,max_pending_tasks,max_ingest_files,
                      enabled,updated_at
               FROM campaign_capacity_policies"""
        ).fetchall()
        return rows, policies

    rows, policy_rows = read_transaction(read, operation="capacity_campaigns.snapshot")
    policies = {
        (str(row["campaign_type"]), int(row["campaign_id"])): dict(row)
        for row in policy_rows
    }
    transport = metrics_service.campaign_transport_snapshot(60.0)
    items = []
    for row in rows:
        item = dict(row)
        item["partial"] = int(item.pop("partial_count", 0) or 0)
        item["unknown"] = int(item.pop("unknown_count", 0) or 0)
        key = (str(item["campaign_type"]), int(item["campaign_id"]), int(item["config_id"]))
        if key[2] == 0:
            samples = [value for tkey, value in transport.items() if tkey[:2] == key[:2]]
            transport_values = {
                field: sum(float(sample.get(field, 0.0)) for sample in samples)
                for field in next(iter(samples), {}).keys()
            }
        else:
            transport_values = transport.get(key, {})
        item["transport"] = transport_values
        item["policy"] = policies.get(key[:2], {
            "campaign_type": key[0], "campaign_id": key[1], "weight": 1.0,
            "min_parts_tps": 0.0, "max_parts_tps": None,
            "max_outstanding_parts": None, "max_pending_tasks": None,
            "max_ingest_files": None, "enabled": 1,
        })
        items.append(item)
    return {
        "scheduler": status(),
        "items": items,
        "policies": [dict(row) for row in policy_rows],
        "limit": limit,
        "truncated": len(rows) >= limit,
    }
