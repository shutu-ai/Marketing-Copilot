"""Isolated Send Store sharding feasibility prototype.

This module is intentionally not imported by the production startup path.
It uses one DuckDB file and one connection/write gate per fixed shard, while
keeping campaign control, claim, attempt evidence, and result journal in the
same shard.  That co-location makes the stop fence demonstrable without
pretending that an asynchronous cross-database notification is safe enough
for production.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Iterable, Optional

import duckdb


def poc_enabled() -> bool:
    """Return the opt-in flag; the default is deliberately OFF."""
    return str(os.environ.get("SHARDED_SEND_STORE", "OFF")).strip().upper() in {
        "1", "ON", "TRUE", "YES",
    }


def stable_shard_id(campaign_type: str, campaign_id: int, shard_count: int) -> int:
    """Map an activity to a stable shard without Python's randomized hash."""
    count = max(1, int(shard_count))
    key = f"{campaign_type}:{int(campaign_id)}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(key).digest()[:8], "big") % count


class SendStoreDisabled(RuntimeError):
    pass


class SendStoreFenceError(RuntimeError):
    pass


class _DictResult:
    def __init__(self, cursor):
        self._cursor = cursor

    def _columns(self):
        return [item[0] for item in (self._cursor.description or ())]

    def fetchone(self):
        row = self._cursor.fetchone()
        return dict(zip(self._columns(), row)) if row is not None else None

    def fetchall(self):
        columns = self._columns()
        return [dict(zip(columns, row)) for row in self._cursor.fetchall()]


class _DictConnection:
    """Small row adapter so the PoC remains readable with DuckDB's tuples."""

    def __init__(self, connection):
        self._connection = connection

    def execute(self, sql, params=None):
        cursor = self._connection.execute(sql, params) if params is not None else self._connection.execute(sql)
        return _DictResult(cursor)

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _Shard:
    def __init__(self, shard_id: int, path: Path):
        self.shard_id = int(shard_id)
        self.path = path
        self.lock = threading.RLock()
        self.conn = _DictConnection(duckdb.connect(str(path)))
        self.lock_wait_seconds: list[float] = []
        self.lock_hold_seconds: list[float] = []
        self._init_schema()

    def _init_schema(self) -> None:
        with self.lock:
            started = time.perf_counter()
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS campaigns (
                    campaign_type TEXT NOT NULL,
                    campaign_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'in_progress',
                    dispatch_generation BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY(campaign_type,campaign_id)
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_key TEXT PRIMARY KEY,
                    campaign_type TEXT NOT NULL,
                    campaign_id BIGINT NOT NULL,
                    ordinal BIGINT NOT NULL,
                    msisdn TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    dispatch_generation BIGINT NOT NULL DEFAULT 0,
                    claim_token TEXT NOT NULL DEFAULT '',
                    attempt_token TEXT NOT NULL DEFAULT '',
                    planned_parts INTEGER NOT NULL DEFAULT 1,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_token TEXT PRIMARY KEY,
                    task_key TEXT NOT NULL,
                    claim_token TEXT NOT NULL,
                    planned_parts INTEGER NOT NULL,
                    accepted_parts INTEGER NOT NULL DEFAULT 0,
                    failed_parts INTEGER NOT NULL DEFAULT 0,
                    unknown_parts INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'authorized',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS parts (
                    attempt_token TEXT NOT NULL,
                    part_no INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'planned',
                    socket_sent_at TIMESTAMP,
                    response_status TEXT NOT NULL DEFAULT '',
                    smsc_message_id TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(attempt_token,part_no)
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS result_journal (
                    event_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    attempt_token TEXT NOT NULL,
                    task_key TEXT NOT NULL,
                    part_no INTEGER NOT NULL,
                    outcome TEXT NOT NULL,
                    smsc_message_id TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL DEFAULT '{}',
                    applied BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_poc_tasks_eligible "
                "ON tasks(status,next_attempt_at,created_at,ordinal)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_poc_tasks_campaign "
                "ON tasks(campaign_type,campaign_id,status)"
            )
            self.conn.commit()
            self.lock_hold_seconds.append(time.perf_counter() - started)

    def close(self) -> None:
        with self.lock:
            self.conn.close()


class ShardedSendStore:
    """A test-only fixed-shard Send Store with durable stop fences."""

    def __init__(self, root_dir: str | os.PathLike[str], shard_count: int = 1,
                 *, enabled: Optional[bool] = None):
        self.enabled = poc_enabled() if enabled is None else bool(enabled)
        self.shard_count = max(1, int(shard_count))
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.map_path = self.root_dir / "shard-map.json"
        self._assignments: dict[str, int] = {}
        self._load_or_create_map()
        self._shards = [
            _Shard(index, self.root_dir / f"send-store-{index:02d}.duckdb")
            for index in range(self.shard_count)
        ]
        self._cursor = 0
        self._cursor_lock = threading.Lock()

    def _require_enabled(self) -> None:
        if not self.enabled:
            raise SendStoreDisabled(
                "SHARDED_SEND_STORE is OFF; this module is an isolated PoC"
            )

    def _load_or_create_map(self) -> None:
        if self.map_path.exists():
            data = json.loads(self.map_path.read_text(encoding="utf-8"))
            if int(data.get("shard_count", 0)) != self.shard_count:
                raise ValueError(
                    "shard count changed for an existing PoC directory; "
                    "start a new experiment directory"
                )
            self._assignments = {
                str(key): int(value) for key, value in (data.get("assignments") or {}).items()
            }

    def _save_map(self) -> None:
        self.map_path.write_text(
            json.dumps({
                "algorithm": "sha256(campaign_type:campaign_id) mod shard_count",
                "shard_count": self.shard_count,
                "assignments": self._assignments,
            }, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def shard_for(self, campaign_type: str, campaign_id: int) -> int:
        self._require_enabled()
        key = f"{campaign_type}:{int(campaign_id)}"
        shard_id = stable_shard_id(campaign_type, campaign_id, self.shard_count)
        previous = self._assignments.get(key)
        if previous is not None and previous != shard_id:
            raise ValueError(f"shard map changed for {key}")
        if previous is None:
            self._assignments[key] = shard_id
            self._save_map()
        return shard_id

    def _shard_for_task(self, task_key: str) -> _Shard:
        self._require_enabled()
        for shard in self._shards:
            with shard.lock:
                row = shard.conn.execute(
                    "SELECT campaign_type,campaign_id FROM tasks WHERE task_key=?",
                    (task_key,),
                ).fetchone()
            if row:
                return shard
        raise KeyError(f"task not found: {task_key}")

    def _shard_for_attempt(self, attempt_token: str) -> _Shard:
        self._require_enabled()
        for shard in self._shards:
            with shard.lock:
                row = shard.conn.execute(
                    "SELECT 1 FROM attempts WHERE attempt_token=?", (attempt_token,)
                ).fetchone()
            if row:
                return shard
        raise KeyError(f"attempt not found: {attempt_token}")

    def register_campaign(self, campaign_type: str, campaign_id: int,
                          status: str = "in_progress") -> int:
        self._require_enabled()
        shard_id = self.shard_for(campaign_type, campaign_id)
        shard = self._shards[shard_id]
        with shard.lock:
            shard.conn.execute(
                """INSERT INTO campaigns(campaign_type,campaign_id,status)
                   VALUES (?,?,?) ON CONFLICT(campaign_type,campaign_id)
                   DO UPDATE SET status=excluded.status""",
                (str(campaign_type), int(campaign_id), str(status)),
            )
            shard.conn.commit()
        return shard_id

    def enqueue(self, campaign_type: str, campaign_id: int,
                tasks: Iterable[dict]) -> int:
        self._require_enabled()
        shard_id = self.shard_for(campaign_type, campaign_id)
        shard = self._shards[shard_id]
        with shard.lock:
            row = shard.conn.execute(
                "SELECT status,dispatch_generation FROM campaigns "
                "WHERE campaign_type=? AND campaign_id=?",
                (str(campaign_type), int(campaign_id)),
            ).fetchone()
            if not row:
                raise KeyError("campaign must be registered before enqueue")
            generation = int(row["dispatch_generation"] or 0)
            payload = list(tasks)
            for index, task in enumerate(payload):
                ordinal = int(task.get("ordinal", index))
                task_key = str(task.get("task_key") or f"{campaign_type}:{campaign_id}:{ordinal}")
                shard.conn.execute(
                    """INSERT INTO tasks
                       (task_key,campaign_type,campaign_id,ordinal,msisdn,
                        dispatch_generation,planned_parts)
                       VALUES (?,?,?,?,?,?,?) ON CONFLICT(task_key) DO NOTHING""",
                    (task_key, str(campaign_type), int(campaign_id), ordinal,
                     str(task.get("msisdn") or ""), generation,
                     max(1, int(task.get("planned_parts") or 1))),
                )
            shard.conn.commit()
        return len(payload)

    def claim(self, limit: int = 20) -> list[dict]:
        self._require_enabled()
        wanted = max(1, int(limit or 1))
        with self._cursor_lock:
            order = [(self._cursor + offset) % self.shard_count
                     for offset in range(self.shard_count)]
            self._cursor = (self._cursor + 1) % self.shard_count
        claimed: list[dict] = []
        for shard_id in order:
            if len(claimed) >= wanted:
                break
            shard = self._shards[shard_id]
            started = time.perf_counter()
            with shard.lock:
                shard.lock_wait_seconds.append(time.perf_counter() - started)
                tx_started = time.perf_counter()
                shard.conn.execute("BEGIN TRANSACTION")
                try:
                    rows = shard.conn.execute(
                        """SELECT t.*,c.status campaign_status,c.dispatch_generation campaign_generation
                           FROM tasks t JOIN campaigns c
                             ON c.campaign_type=t.campaign_type AND c.campaign_id=t.campaign_id
                           WHERE t.status IN ('queued','retrying')
                             AND t.next_attempt_at<=CURRENT_TIMESTAMP
                             AND c.status='in_progress'
                             AND t.dispatch_generation=c.dispatch_generation
                           ORDER BY t.created_at,t.ordinal,t.task_key
                           LIMIT ?""",
                        (max(wanted * 4, wanted),),
                    ).fetchall()
                    by_campaign: dict[tuple[str, int], list] = defaultdict(list)
                    for row in rows:
                        by_campaign[(row["campaign_type"], int(row["campaign_id"]))].append(row)
                    selected = []
                    groups = list(by_campaign)
                    while len(selected) < min(wanted - len(claimed), len(rows)):
                        progressed = False
                        for group in groups:
                            if by_campaign[group]:
                                selected.append(by_campaign[group].pop(0))
                                progressed = True
                                if len(selected) >= wanted - len(claimed):
                                    break
                        if not progressed:
                            break
                    for row in selected:
                        token = uuid.uuid4().hex
                        updated = shard.conn.execute(
                            """UPDATE tasks SET status='sending',attempts=attempts+1,
                                      claim_token=?,updated_at=CURRENT_TIMESTAMP
                               WHERE task_key=? AND status IN ('queued','retrying')
                                 AND EXISTS (
                                   SELECT 1 FROM campaigns c
                                   WHERE c.campaign_type=tasks.campaign_type
                                     AND c.campaign_id=tasks.campaign_id
                                     AND c.status='in_progress'
                                     AND c.dispatch_generation=tasks.dispatch_generation
                                 )
                               RETURNING *""",
                            (token, row["task_key"]),
                        ).fetchone()
                        if updated:
                            item = dict(updated)
                            item["shard_id"] = shard_id
                            claimed.append(item)
                    shard.conn.commit()
                except Exception:
                    shard.conn.rollback()
                    raise
                finally:
                    shard.lock_hold_seconds.append(time.perf_counter() - tx_started)
        return claimed

    def authorize(self, task_key: str, claim_token: str, slot: int = 0,
                  planned_parts: Optional[int] = None) -> str:
        self._require_enabled()
        shard = self._shard_for_task(task_key)
        with shard.lock:
            shard.conn.execute("BEGIN TRANSACTION")
            try:
                row = shard.conn.execute(
                    """SELECT t.*,c.status campaign_status,c.dispatch_generation campaign_generation
                       FROM tasks t JOIN campaigns c
                         ON c.campaign_type=t.campaign_type AND c.campaign_id=t.campaign_id
                       WHERE t.task_key=?""",
                    (task_key,),
                ).fetchone()
                if not row or row["status"] != "sending" or row["claim_token"] != str(claim_token):
                    raise SendStoreFenceError("claim fence is no longer owned")
                if row["campaign_status"] != "in_progress" or int(row["dispatch_generation"]) != int(row["campaign_generation"]):
                    raise SendStoreFenceError("campaign stop fence is closed")
                attempt_token = str(row["attempt_token"] or "") or f"{claim_token}:{uuid.uuid4().hex}"
                parts = max(1, int(planned_parts or row["planned_parts"] or 1))
                shard.conn.execute(
                    """INSERT INTO attempts(attempt_token,task_key,claim_token,planned_parts,state)
                       VALUES (?,?,?,?, 'authorized') ON CONFLICT(attempt_token) DO NOTHING""",
                    (attempt_token, task_key, str(claim_token), parts),
                )
                shard.conn.execute(
                    """INSERT INTO parts(attempt_token,part_no)
                       SELECT ?,part_no FROM range(1,?) AS r(part_no)
                       ON CONFLICT(attempt_token,part_no) DO NOTHING""",
                    (attempt_token, parts + 1),
                )
                shard.conn.execute(
                    "UPDATE tasks SET attempt_token=?,planned_parts=?,updated_at=CURRENT_TIMESTAMP WHERE task_key=?",
                    (attempt_token, parts, task_key),
                )
                shard.conn.commit()
                return attempt_token
            except Exception:
                shard.conn.rollback()
                raise

    def _apply_journal_event(self, shard: _Shard, event_id: str) -> None:
        event = shard.conn.execute(
            "SELECT * FROM result_journal WHERE event_id=?", (event_id,)
        ).fetchone()
        if not event or event["applied"]:
            return
        outcome = str(event["outcome"])
        part_state = {"success": "accepted", "reject": "rejected", "unknown": "unknown"}.get(
            outcome, "unknown"
        )
        shard.conn.execute(
            """UPDATE parts SET state=?,socket_sent_at=COALESCE(socket_sent_at,CURRENT_TIMESTAMP),
                      response_status=?,smsc_message_id=?,updated_at=CURRENT_TIMESTAMP
               WHERE attempt_token=? AND part_no=?""",
            (part_state, outcome, event["smsc_message_id"], event["attempt_token"], event["part_no"]),
        )
        parts = shard.conn.execute(
            "SELECT state FROM parts WHERE attempt_token=? ORDER BY part_no",
            (event["attempt_token"],),
        ).fetchall()
        accepted = sum(1 for part in parts if part["state"] == "accepted")
        failed = sum(1 for part in parts if part["state"] == "rejected")
        unknown = sum(1 for part in parts if part["state"] == "unknown")
        if unknown:
            task_status = "partial" if accepted else "unknown"
            attempt_state = task_status
        elif failed:
            task_status = "partial" if accepted else "failed"
            attempt_state = task_status
        elif parts and accepted == len(parts):
            task_status = "accepted"
            attempt_state = "accepted"
        else:
            task_status = "sending"
            attempt_state = "authorized"
        shard.conn.execute(
            """UPDATE attempts SET accepted_parts=?,failed_parts=?,unknown_parts=?,state=?,
                      updated_at=CURRENT_TIMESTAMP WHERE attempt_token=?""",
            (accepted, failed, unknown, attempt_state, event["attempt_token"]),
        )
        shard.conn.execute(
            """UPDATE tasks SET status=?,updated_at=CURRENT_TIMESTAMP
               WHERE task_key=? AND status='sending'""",
            (task_status, event["task_key"]),
        )
        shard.conn.execute(
            "UPDATE result_journal SET applied=TRUE WHERE event_id=?", (event_id,)
        )

    def append_result(self, attempt_token: str, part_no: int, outcome: str,
                      *, smsc_message_id: str = "", event_id: str = "",
                      apply: bool = True) -> str:
        self._require_enabled()
        if outcome not in {"success", "reject", "unknown"}:
            raise ValueError("outcome must be success, reject or unknown")
        shard = self._shard_for_attempt(attempt_token)
        with shard.lock:
            shard.conn.execute("BEGIN TRANSACTION")
            try:
                row = shard.conn.execute(
                    "SELECT task_key FROM attempts WHERE attempt_token=?",
                    (attempt_token,),
                ).fetchone()
                if not row:
                    raise KeyError(f"attempt not found: {attempt_token}")
                event_id = event_id or f"{attempt_token}:{int(part_no)}:{outcome}:{smsc_message_id}"
                shard.conn.execute(
                    """INSERT INTO result_journal
                       (event_id,kind,attempt_token,task_key,part_no,outcome,smsc_message_id,payload)
                       VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING""",
                    (event_id, "part_result", attempt_token, row["task_key"], int(part_no),
                     outcome, str(smsc_message_id or ""), json.dumps({"socket_sent": True})),
                )
                if apply:
                    self._apply_journal_event(shard, event_id)
                shard.conn.commit()
                return event_id
            except Exception:
                shard.conn.rollback()
                raise

    def replay_journal(self) -> int:
        self._require_enabled()
        replayed = 0
        for shard in self._shards:
            with shard.lock:
                shard.conn.execute("BEGIN TRANSACTION")
                try:
                    events = shard.conn.execute(
                        "SELECT event_id FROM result_journal WHERE applied=FALSE ORDER BY created_at,event_id"
                    ).fetchall()
                    for event in events:
                        self._apply_journal_event(shard, event["event_id"])
                        replayed += 1
                    shard.conn.commit()
                except Exception:
                    shard.conn.rollback()
                    raise
        return replayed

    def transition(self, campaign_type: str, campaign_id: int,
                   action: str) -> dict:
        self._require_enabled()
        if action not in {"pause", "cancel", "complete", "resume"}:
            raise ValueError("action must be pause, cancel, complete or resume")
        shard = self._shards[self.shard_for(campaign_type, campaign_id)]
        with shard.lock:
            shard.conn.execute("BEGIN TRANSACTION")
            try:
                row = shard.conn.execute(
                    "SELECT * FROM campaigns WHERE campaign_type=? AND campaign_id=?",
                    (str(campaign_type), int(campaign_id)),
                ).fetchone()
                if not row:
                    raise KeyError("campaign not found")
                current = str(row["status"])
                allowed = {
                    "pause": ("in_progress", "paused"),
                    "cancel": ("in_progress", "cancelled"),
                    "complete": ("in_progress", "completed"),
                    "resume": ("paused", "in_progress"),
                }[action]
                if current != allowed[0]:
                    raise SendStoreFenceError(f"cannot {action} from {current}")
                generation = int(row["dispatch_generation"]) + 1
                target = allowed[1]
                shard.conn.execute(
                    """UPDATE campaigns SET status=?,dispatch_generation=?
                       WHERE campaign_type=? AND campaign_id=? AND status=?""",
                    (target, generation, str(campaign_type), int(campaign_id), current),
                )
                if action == "pause":
                    shard.conn.execute(
                        """UPDATE tasks SET status='queued',claim_token='',
                                  dispatch_generation=?,next_attempt_at=CURRENT_TIMESTAMP,
                                  updated_at=CURRENT_TIMESTAMP
                           WHERE campaign_type=? AND campaign_id=? AND status='sending'
                             AND NOT EXISTS (
                               SELECT 1 FROM attempts a
                               WHERE a.task_key=tasks.task_key
                                 AND a.state IN ('authorized','partial')
                             )""",
                        (generation, str(campaign_type), int(campaign_id)),
                    )
                    shard.conn.execute(
                        """UPDATE tasks SET dispatch_generation=? WHERE campaign_type=?
                           AND campaign_id=? AND status IN ('queued','retrying')""",
                        (generation, str(campaign_type), int(campaign_id)),
                    )
                elif action == "resume":
                    shard.conn.execute(
                        """UPDATE tasks SET dispatch_generation=? WHERE campaign_type=?
                           AND campaign_id=? AND status IN ('queued','retrying')""",
                        (generation, str(campaign_type), int(campaign_id)),
                    )
                else:
                    terminal = "cancelled" if action == "cancel" else "expired"
                    shard.conn.execute(
                        """UPDATE tasks SET status=?,claim_token='',dispatch_generation=?,
                                  updated_at=CURRENT_TIMESTAMP
                           WHERE campaign_type=? AND campaign_id=?
                             AND (status IN ('queued','retrying') OR
                               (status='sending' AND NOT EXISTS (
                                 SELECT 1 FROM attempts a WHERE a.task_key=tasks.task_key
                                   AND a.state IN ('authorized','partial')
                               )))""",
                        (terminal, generation, str(campaign_type), int(campaign_id)),
                    )
                changed = shard.conn.execute(
                    "SELECT COUNT(*) count FROM tasks WHERE campaign_type=? AND campaign_id=?",
                    (str(campaign_type), int(campaign_id)),
                ).fetchone()["count"]
                shard.conn.commit()
                return {"campaign": target, "dispatch_generation": generation,
                        "task_count": int(changed), "shard_id": shard.shard_id}
            except Exception:
                shard.conn.rollback()
                raise

    def consistency_check(self) -> dict:
        self._require_enabled()
        violations = []
        shard_summaries = []
        total_tasks = 0
        total_events = 0
        for shard in self._shards:
            with shard.lock:
                tasks = shard.conn.execute("SELECT * FROM tasks").fetchall()
                events = shard.conn.execute("SELECT COUNT(*) count FROM result_journal").fetchone()["count"]
                total_tasks += len(tasks)
                total_events += int(events or 0)
                for task in tasks:
                    if task["status"] in {"accepted", "failed", "partial", "unknown"} and not task["attempt_token"]:
                        violations.append({"task_key": task["task_key"], "reason": "terminal_without_attempt"})
                    if task["status"] == "unknown" and task["next_attempt_at"]:
                        # Unknown is evidence requiring reconciliation, never a retryable queue state.
                        pass
                shard_summaries.append({
                    "shard_id": shard.shard_id,
                    "tasks": len(tasks),
                    "events": int(events or 0),
                    "lock_wait_p50": _percentile(shard.lock_wait_seconds, 50),
                    "lock_wait_p95": _percentile(shard.lock_wait_seconds, 95),
                    "lock_wait_p99": _percentile(shard.lock_wait_seconds, 99),
                    "lock_hold_p50": _percentile(shard.lock_hold_seconds, 50),
                    "lock_hold_p95": _percentile(shard.lock_hold_seconds, 95),
                    "lock_hold_p99": _percentile(shard.lock_hold_seconds, 99),
                })
        return {"ok": not violations, "violations": violations,
                "task_count": total_tasks, "event_count": total_events,
                "shards": shard_summaries}

    def close(self) -> None:
        for shard in self._shards:
            shard.close()


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    values = sorted(float(value) for value in values)
    index = min(len(values) - 1, max(0, int(round((percentile / 100) * (len(values) - 1)))))
    return round(values[index], 6)


def run_minimal_poc(output_dir: str, shard_count: int = 1,
                    tasks_per_campaign: int = 24) -> dict:
    """Run a bounded offline scenario used by the acceptance evidence."""
    root = Path(output_dir)
    store = ShardedSendStore(root, shard_count, enabled=True)
    started = time.perf_counter()
    try:
        for campaign_id in (1, 2, 3):
            store.register_campaign("realtime", campaign_id)
            store.enqueue(
                "realtime", campaign_id,
                ({"ordinal": index, "msisdn": f"251900{campaign_id:02d}{index:06d}",
                  "planned_parts": 2 if index % 3 == 0 else 1}
                 for index in range(tasks_per_campaign)),
            )
        first = store.claim(tasks_per_campaign)
        pause = store.transition("realtime", 1, "pause")
        after_pause = store.claim(tasks_per_campaign * 2)
        progress_b_c = sum(1 for task in after_pause if int(task["campaign_id"]) in {2, 3})
        authorized = 0
        for task in after_pause:
            attempt = store.authorize(task["task_key"], task["claim_token"], planned_parts=task["planned_parts"])
            authorized += 1
            for part_no in range(1, int(task["planned_parts"]) + 1):
                outcome = "success"
                if int(task["campaign_id"]) == 3 and part_no == 2 and task["ordinal"] % 5 == 0:
                    outcome = "unknown"
                store.append_result(
                    attempt, part_no, outcome,
                    smsc_message_id=f"poc-{task['task_key']}-{part_no}",
                )
        resume = store.transition("realtime", 1, "resume")
        recovery_claim = store.claim(1)
        if recovery_claim:
            recovery_attempt = store.authorize(
                recovery_claim[0]["task_key"], recovery_claim[0]["claim_token"],
                planned_parts=recovery_claim[0]["planned_parts"],
            )
            store.append_result(
                recovery_attempt, 1, "success",
                smsc_message_id=f"replay-{recovery_claim[0]['task_key']}-1",
                apply=False,
            )
        replayed = store.replay_journal()
        check = store.consistency_check()
        elapsed = max(0.000001, time.perf_counter() - started)
        accepted_or_partial = max(0, authorized)
        parts = sum(int(task["planned_parts"]) for task in after_pause)
        return {
            "arm": f"D-{shard_count}shard",
            "shard_count": shard_count,
            "tasks_per_campaign": tasks_per_campaign,
            "elapsed_seconds": round(elapsed, 6),
            "business_sms_per_second": round(accepted_or_partial / elapsed, 3),
            "submit_sm_parts_per_second": round(parts / elapsed, 3),
            "claimed_before_pause": len(first),
            "pause": pause,
            "b_c_progress_after_pause": progress_b_c,
            "authorized": authorized,
            "resume": resume,
            "recovery_claimed": len(recovery_claim),
            "journal_replayed": replayed,
            "consistency": check,
            "status": "PASS" if check["ok"] and progress_b_c > 0 else "FAIL",
            "reliability_scope": "POC_ONLY_NOT_PRODUCTION_READY",
        }
    finally:
        store.close()


__all__ = [
    "SendStoreDisabled", "SendStoreFenceError", "ShardedSendStore",
    "poc_enabled", "stable_shard_id", "run_minimal_poc",
]
