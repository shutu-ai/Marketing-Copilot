"""In-process campaign fairness and send-capacity accounting.

DuckDB remains the durable queue.  This module only decides the order in
which a bounded candidate set is claimed.  Deficits are deliberately
ephemeral: after a restart every active campaign starts with equal credit,
while ownership is still fenced durably by ``claim_token``.
"""

from __future__ import annotations

import math
import os
import logging
from bisect import bisect_right
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Iterable

from database import read_transaction, write_transaction
from services import metrics_service

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_deficits: dict[tuple[str, int, int], float] = {}
_virtual_service: dict[tuple[str, int, int], float] = {}
_rate_tokens: dict[tuple[str, int, int], float] = {}
_rate_updated: dict[tuple[str, int, int], float] = {}
_cursor = 0
_disabled_warning_emitted = False


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


def _cursor_key(key: tuple[str, int, int]) -> str:
    return f"{key[0]}|{int(key[1])}|{int(key[2])}"


def _parse_cursor_key(value: str) -> tuple[str, int, int] | None:
    try:
        campaign_type, campaign_id, route_id = str(value or "").split("|", 2)
        return campaign_type, int(campaign_id), int(route_id)
    except (TypeError, ValueError):
        return None


def rotate_candidate_groups(conn, groups: Iterable[dict], limit: int,
                            stage: str = "sms_claim") -> list[tuple[str, int, int]]:
    """Return a durable round-robin window over all eligible campaign groups.

    The old claim query sorted groups by oldest task and then applied LIMIT,
    which made a later campaign invisible forever when the active set exceeded
    the limit.  The cursor is stored in DuckDB so a restart continues from the
    last bounded window instead of resetting the starvation boundary.
    """
    keys = sorted({
        (str(group["campaign_type"]), int(group["campaign_id"]), int(group["route_id"]))
        for group in groups
    })
    if not keys:
        return []
    limit = max(1, min(len(keys), int(limit)))
    state = conn.execute(
        "SELECT cursor_key FROM fair_scheduler_state WHERE stage=?", (stage,)
    ).fetchone()
    previous = _parse_cursor_key(state["cursor_key"] if state else "")
    if previous in keys:
        start = (keys.index(previous) + 1) % len(keys)
    elif previous is not None:
        # The previous window may have been fully claimed and therefore
        # disappeared from the live set. Continue after its durable key rather
        # than resetting to the first remaining activity every time.
        start = bisect_right(keys, previous) % len(keys)
    else:
        start = 0
    ordered = keys[start:] + keys[:start]
    selected = ordered[:limit]
    cursor = selected[-1]
    conn.execute(
        """INSERT INTO fair_scheduler_state(stage,cursor_key,rounds,updated_at)
           VALUES (?,?,1,CURRENT_TIMESTAMP)
           ON CONFLICT(stage) DO UPDATE SET
             cursor_key=excluded.cursor_key,rounds=fair_scheduler_state.rounds+1,
             updated_at=excluded.updated_at""",
        (stage, _cursor_key(cursor)),
    )
    metrics_service.set_gauge(
        "marketing_copilot_fair_candidate_groups",
        len(keys), labels={"stage": stage},
    )
    metrics_service.set_gauge(
        "marketing_copilot_fair_candidate_window",
        len(selected), labels={"stage": stage},
    )
    return selected


def record_activity_event(conn, campaign_type: str, campaign_id: int,
                          stage: str, route_id: int = 0, backlog: int | None = None,
                          skip_reason: str = "") -> None:
    """Persist low-cardinality per-activity fairness timestamps and backlog."""
    campaign_type = str(campaign_type or "")
    campaign_id = int(campaign_id)
    route_id = int(route_id or 0)
    timestamp_column = {
        "file": "last_file_claim_at",
        "task": "last_task_claim_at",
        "submit": "last_submit_at",
        "dispatch": "last_dispatched_at",
    }.get(str(stage or ""))
    if not timestamp_column:
        raise ValueError(f"unsupported fairness stage: {stage}")
    backlog_value = None if backlog is None else max(0, int(backlog))
    updates = [
        f"{timestamp_column}=excluded.eligible_since",
        "consecutive_skip_rounds=0",
        "updated_at=excluded.eligible_since",
    ]
    if stage == "dispatch" and timestamp_column != "last_dispatched_at":
        updates.append("last_dispatched_at=excluded.eligible_since")
    now_text = datetime.now().astimezone().isoformat()
    values = [campaign_type, campaign_id, route_id,
              now_text,
              backlog_value if backlog_value is not None else 0,
              str(skip_reason or "")[:120]]
    update_values = []
    if backlog_value is not None:
        updates.append("backlog=?")
        update_values.append(backlog_value)
    if skip_reason:
        updates.append("last_skip_reason=?")
        update_values.append(str(skip_reason)[:120])
    conn.execute(
        """INSERT INTO campaign_fairness_state
             (campaign_type,campaign_id,route_id,eligible_since,backlog,last_skip_reason)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(campaign_type,campaign_id,route_id) DO UPDATE SET """
        + ",".join(updates),
        values + update_values,
    )


def record_activity_event_transaction(campaign_type: str, campaign_id: int,
                                      stage: str, route_id: int = 0,
                                      backlog: int | None = None,
                                      skip_reason: str = "") -> None:
    write_transaction(
        lambda conn: record_activity_event(
            conn, campaign_type, campaign_id, stage, route_id, backlog, skip_reason,
        ),
        operation=f"fairness.{stage}",
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
            "disabled_behavior": "global_fifo_without_starvation_protection" if not enabled() else "bounded_drr_with_durable_candidate_cursor",
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
            """SELECT t.campaign_type,t.campaign_id,
                       COALESCE(t.smpp_config_id,0) config_id,
                      MAX(campaign_name) campaign_name,
                       SUM(CASE WHEN t.status='queued' THEN 1 ELSE 0 END) queued,
                       SUM(CASE WHEN t.status='retrying' THEN 1 ELSE 0 END) retrying,
                       SUM(CASE WHEN t.status='sending' THEN 1 ELSE 0 END) sending,
                       SUM(CASE WHEN t.status='submitted' THEN 1 ELSE 0 END) submitted,
                       SUM(CASE WHEN t.status='partial' THEN 1 ELSE 0 END) partial_count,
                       SUM(CASE WHEN t.status='unknown' THEN 1 ELSE 0 END) unknown_count,
                       MIN(CASE WHEN t.status IN ('queued','retrying') THEN t.created_at END) oldest_pending_at,
                       MAX(fs.eligible_since) eligible_since,
                       MAX(fs.last_dispatched_at) last_dispatched_at,
                       MAX(fs.last_file_claim_at) last_file_claim_at,
                       MAX(fs.last_task_claim_at) last_task_claim_at,
                       MAX(fs.last_submit_at) last_submit_at,
                       MAX(fs.backlog) fairness_backlog,
                       MAX(fs.consecutive_skip_rounds) fairness_skip_rounds
                FROM sms_tasks t
                LEFT JOIN campaign_fairness_state fs
                  ON fs.campaign_type=t.campaign_type AND fs.campaign_id=t.campaign_id
                 AND fs.route_id=COALESCE(t.smpp_config_id,0)
                WHERE t.status IN ('queued','retrying','sending','submitted','partial','unknown')
                GROUP BY t.campaign_type,t.campaign_id,COALESCE(t.smpp_config_id,0)
                ORDER BY (queued+retrying+sending) DESC,t.campaign_type,t.campaign_id,config_id
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
    max_wait_seconds = 0.0
    max_wait_rounds = 0
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
        eligible_since = item.get("eligible_since")
        wait_seconds = 0.0
        if eligible_since and (int(item.get("queued", 0) or 0) + int(item.get("retrying", 0) or 0)):
            try:
                parsed = datetime.fromisoformat(str(eligible_since).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.astimezone()
                wait_seconds = max(0.0, time.time() - parsed.timestamp())
            except (TypeError, ValueError, OverflowError):
                wait_seconds = 0.0
        item["fairness"] = {
            "eligible_since": eligible_since,
            "last_dispatched_at": item.pop("last_dispatched_at", None),
            "last_file_claim_at": item.pop("last_file_claim_at", None),
            "last_task_claim_at": item.pop("last_task_claim_at", None),
            "last_submit_at": item.pop("last_submit_at", None),
            "backlog": int(item.pop("fairness_backlog", 0) or 0),
            "consecutive_skip_rounds": int(item.pop("fairness_skip_rounds", 0) or 0),
            "wait_seconds": wait_seconds,
        }
        max_wait_seconds = max(max_wait_seconds, wait_seconds)
        max_wait_rounds = max(max_wait_rounds, item["fairness"]["consecutive_skip_rounds"])
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
        "fairness_max_wait_seconds": max_wait_seconds,
        "fairness_max_wait_rounds": max_wait_rounds,
    }
