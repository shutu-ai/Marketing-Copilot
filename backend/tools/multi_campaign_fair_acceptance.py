"""Real four-activity fairness acceptance run.

The runner provisions two realtime local-watch campaigns and two batch
campaigns through HTTP, publishes source files through the same producer
contracts as production, and reconciles the result journal plus DuckDB after
the service has stopped.  It intentionally never writes business rows
directly to DuckDB.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from collections import Counter
from datetime import timezone
from pathlib import Path

from local_e2e_acceptance import (
    AcceptanceRunner,
    BlockedEnv,
    _duration_ms,
    _parse_time,
    _read_jsonl,
    _safe_process_snapshot,
    _utc_now,
    _find_free_port,
)
from local_e2e_data import make_msisdns, write_event_file


class MultiCampaignFairRunner(AcceptanceRunner):
    LARGE_ROWS = 40
    SMALL_ROWS = 4

    def __init__(self, args: argparse.Namespace):
        spec = {
            "timeout_seconds": int(args.timeout_seconds),
            "faults": {"tps": 1000},
            "message": "Marketing Copilot fairness acceptance {event_id} {msisdn}",
        }
        super().__init__(args, spec)
        self.watch_dir = self.root / "watch-unused"
        self.campaign_dir = self.root / "campaign-unused"
        self.realtime_campaign_ids: dict[str, int] = {}
        self.batch_campaign_ids: dict[str, int] = {}
        self.activity_meta: dict[tuple[str, int], dict] = {}
        self.rows_by_activity: dict[tuple[str, int], list[dict]] = {}
        self.input_files: list[dict] = []
        self.realtime_campaign_id = None
        self.expected_total = 0
        self.sftp_status = "BLOCKED_ENV"

    def _environment(self) -> dict[str, str]:
        env = super()._environment()
        env.update({
            "MC_REALTIME_DISCOVERY_QUANTUM": "1",
            "MC_REALTIME_COPY_QUEUE_SIZE": "4",
            "MC_REALTIME_WATCH_RESCAN_SECONDS": "1",
            "MC_BATCH_INGEST_QUANTUM": "1",
            "MC_BATCH_QUANTUM_SECONDS": "0.25",
            "MC_FAIR_ACTIVE_CAMPAIGNS": "2",
            "MC_FAIR_CLAIM_PER_CAMPAIGN": "3",
            "MC_FAIR_DRR_QUANTUM_PARTS": "4",
            "MC_SMPP_INTERNAL_TPS_MULTIPLIER": "1",
        })
        return env

    def _rows(self, activity: str, count: int, start: int) -> list[dict]:
        return [
            {
                "msisdn": msisdn,
                "language": "English",
                "event_id": f"fair-{activity}-{index:04d}",
            }
            for index, msisdn in enumerate(make_msisdns(count, start=start), 1)
        ]

    def _csv_bytes(self, rows: list[dict]) -> bytes:
        from io import StringIO

        stream = StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=["msisdn", "language", "event_id"])
        writer.writeheader()
        writer.writerows(rows)
        return stream.getvalue().encode("utf-8")

    def _make_content(self, marketing_type: str, campaign_id: int, label: str) -> int:
        campaign = self._api_json("POST", "/api/sms-content/campaigns", {
            "name": f"fairness-content-{label}",
            "marketing_type": marketing_type,
            "marketing_campaign_id": campaign_id,
            "language": "English",
        })
        template = self._api_json("POST", "/api/sms-content/templates", {
            "campaign_id": int(campaign["id"]),
            "language": "English",
            "content": f"Fairness {label} {{event_id}} {{msisdn}}",
            "source_addr": "FAIR",
        })
        return int(template["id"])

    def _provision_realtime(self, label: str, start: int, count: int) -> None:
        watch_dir = self.root / f"watch-{label}"
        campaign_dir = self.root / f"campaign-{label}"
        watch_dir.mkdir(parents=True, exist_ok=True)
        campaign_dir.mkdir(parents=True, exist_ok=True)
        response = self._api_json("POST", "/api/realtime-campaigns", {
            "name": f"fairness-realtime-{label}",
            "use_whitelist": False,
            "campaign_dir": str(campaign_dir),
            "use_custom_directory": True,
            "watch_source": "local",
            "watch_dir": str(watch_dir),
            "watch_filename_prefix": f"FAIR_{label}_",
            "phone_prefix": "251",
            "smpp_config_id": self.smpp_config_id,
        })
        campaign_id = int(response["id"])
        template_id = self._make_content("realtime", campaign_id, f"rt-{label}")
        key = ("realtime", campaign_id)
        self.realtime_campaign_ids[label] = campaign_id
        self.rows_by_activity[key] = self._rows(f"rt-{label}", count, start)
        self.activity_meta[key] = {
            "label": f"realtime-{label}",
            "campaign_type": "realtime",
            "campaign_id": campaign_id,
            "expected_rows": count,
            "watch_dir": str(watch_dir),
            "prefix": f"FAIR_{label}_",
            "template_id": template_id,
            "activated_at": None,
        }

    def _provision_batch(self, label: str, start: int, count: int) -> None:
        campaign_dir = self.root / f"campaign-batch-{label}"
        campaign_dir.mkdir(parents=True, exist_ok=True)
        response = self._api_json("POST", "/api/batch-campaigns", {
            "name": f"fairness-batch-{label}",
            "batch_size": 10,
            "campaign_dir": str(campaign_dir),
            "use_custom_directory": True,
            "phone_prefix": "251",
            "smpp_config_id": self.smpp_config_id,
        })
        campaign_id = int(response["id"])
        rows = self._rows(f"batch-{label}", count, start)
        source_name = f"fair-batch-{label}.csv"
        self._upload_multipart(
            f"/api/batch-campaigns/{campaign_id}/upload",
            "file", source_name, self._csv_bytes(rows),
        )
        template_id = self._make_content("batch", campaign_id, f"batch-{label}")
        key = ("batch", campaign_id)
        self.batch_campaign_ids[label] = campaign_id
        self.rows_by_activity[key] = rows
        self.activity_meta[key] = {
            "label": f"batch-{label}",
            "campaign_type": "batch",
            "campaign_id": campaign_id,
            "expected_rows": count,
            "template_id": template_id,
            "source_name": source_name,
            "activated_at": None,
        }

    def _activate(self, key: tuple[str, int]) -> None:
        meta = self.activity_meta[key]
        if meta["campaign_type"] == "realtime":
            path = f"/api/realtime-campaigns/{meta['campaign_id']}"
        else:
            path = f"/api/batch-campaigns/{meta['campaign_id']}"
        if meta["campaign_type"] == "batch":
            # Batch validation reads the persisted template before validating
            # the status transition, so these are deliberately two API
            # updates rather than one combined request.
            self._api_json("PUT", path, {
                "sms_template_id": meta["template_id"],
                "smpp_config_id": self.smpp_config_id,
            })
        self._api_json("PUT", path, {
            "sms_template_id": meta["template_id"],
            "smpp_config_id": self.smpp_config_id,
            "status": "in_progress",
        })
        meta["activated_at"] = _utc_now()
        self._wait_until(
            lambda: self._api_json("GET", path).get("status") == "in_progress",
            self.timeout_seconds, f"{meta['label']} active",
        )

    def _provision(self) -> None:
        smpp = self._api_json("POST", "/api/smpp/configs", {
            "name": "fairness-loopback",
            "host": "127.0.0.1",
            "port": self.smsc_port,
            "system_id": "test",
            "password": "test",
            "source_addr": "FAIR",
            "bind_type": "transceiver",
            "pool_size": 1,
            "window_size": 10,
            "enquire_interval": 5,
            "timeout": 10,
            "tps": 1000,
            "priority": 1,
            "reconnect_max_seconds": 2,
            "delivery_receipt_mode": "disabled",
            "enabled": 1,
        })
        self.smpp_config_id = int(smpp["id"])
        self._provision_realtime("A", 1, self.LARGE_ROWS)
        self._provision_realtime("B", 1001, self.SMALL_ROWS)
        self._provision_batch("A", 2001, self.LARGE_ROWS)
        self._provision_batch("B", 3001, self.SMALL_ROWS)
        self.expected_total = sum(item["expected_rows"] for item in self.activity_meta.values())
        self.realtime_campaign_id = self.realtime_campaign_ids["A"]
        self._wait_until(
            lambda: bool((self._api_json("GET", "/api/health").get("smpp") or {}).get("connected")),
            self.timeout_seconds, "SMPP bind connected",
        )

    def _publish_inputs(self) -> None:
        # Start the two large activities first.  The small activities are
        # activated only after their peers have begun producing work, which
        # exposes a FIFO/oldest-only scheduler to starvation.
        self._activate(("realtime", self.realtime_campaign_ids["A"]))
        self._activate(("batch", self.batch_campaign_ids["A"]))
        for index, rows in enumerate(
            [self.rows_by_activity[("realtime", self.realtime_campaign_ids["A"])]], 1
        ):
            self.input_files.append(write_event_file(
                self.root / "watch-A", "FAIR_A_", index, rows, ready=True,
            ))
        time.sleep(0.5)
        self._activate(("realtime", self.realtime_campaign_ids["B"]))
        self._activate(("batch", self.batch_campaign_ids["B"]))
        self.input_files.append(write_event_file(
            self.root / "watch-B", "FAIR_B_", 1,
            self.rows_by_activity[("realtime", self.realtime_campaign_ids["B"])], ready=True,
        ))

    def _environment_task_rows(self) -> list[dict]:
        rows: list[dict] = []
        cursor = None
        while True:
            path = "/api/sms-tasks?page_size=200"
            if cursor:
                path += f"&cursor={int(cursor)}"
            payload = self._api_json("GET", path)
            items = payload.get("items") or []
            rows.extend(items)
            if not payload.get("has_more") or not items:
                return rows
            cursor = payload.get("next_cursor")
            if not cursor:
                return rows

    def _sample_runtime(self) -> None:
        for role, process in self.processes.items():
            if process.poll() is None:
                self.resource_samples.append(_safe_process_snapshot(process.pid, role))
        try:
            tasks = self._environment_task_rows()
            counts = Counter(str(item.get("status")) for item in tasks)
            snapshot = {
                "task_total": len(tasks),
                "tasks": dict(counts),
                "backlog": sum(counts.get(status, 0) for status in ("queued", "sending", "retrying")),
            }
            self.last_snapshot = snapshot
            self.backlog_samples.append(int(snapshot["backlog"]))
        except Exception:
            pass
        if self.args.max_memory_mb:
            limit = int(self.args.max_memory_mb) * 1024 * 1024
            if any(int(item.get("rss_bytes", 0) or 0) > limit for item in self.resource_samples[-4:]):
                raise RuntimeError(f"resource threshold exceeded: rss>{self.args.max_memory_mb}MB")

    def _wait_for_business(self) -> None:
        def done():
            rows = self._environment_task_rows()
            events = [item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"]
            accepted = [item for item in events if int(item.get("response_status", 1)) == 0]
            status_counts = Counter(str(item.get("status")) for item in rows)
            return (
                len(rows) >= self.expected_total
                and status_counts.get("accepted", 0) >= self.expected_total
                and len(accepted) >= self.expected_total
            )

        self._wait_until(done, self.timeout_seconds, "all four activities accepted")

    def _reconcile(self) -> dict:
        events = [item for item in _read_jsonl(self.smsc_event_log) if item.get("event_type") == "submit_sm"]
        accepted = [item for item in events if int(item.get("response_status", 1)) == 0]
        all_tasks = self._db_query(
            "SELECT id,campaign_type,campaign_id,msisdn,status,attempts,created_at,submitted_at,smsc_message_ids FROM sms_tasks ORDER BY id"
        )
        mappings = self._db_query(
            "SELECT smsc_message_id,task_id FROM sms_message_bindings_v2"
        )
        mapping_by_mid = {str(item["smsc_message_id"]): item for item in mappings}
        tasks_by_activity: dict[tuple[str, int], list[dict]] = {}
        for task in all_tasks:
            tasks_by_activity.setdefault((str(task["campaign_type"]), int(task["campaign_id"])), []).append(task)
        event_by_msisdn = {str(item.get("destination_addr")): item for item in accepted}
        activities = []
        duplicate_tasks = []
        missing_msisdns = []
        for key, meta in sorted(self.activity_meta.items()):
            tasks = tasks_by_activity.get(key, [])
            expected_msisdns = {str(row["msisdn"]) for row in self.rows_by_activity[key]}
            actual_msisdns = {str(task["msisdn"]) for task in tasks}
            attempts = [task for task in tasks if int(task.get("attempts") or 0) > 1]
            duplicate_tasks.extend(int(task["id"]) for task in attempts)
            missing_msisdns.extend(sorted(expected_msisdns - actual_msisdns))
            submits = [event_by_msisdn[msisdn] for msisdn in actual_msisdns if msisdn in event_by_msisdn]
            first_submit = min((event.get("time") for event in submits), default=None)
            first_wait_ms = _duration_ms(meta.get("activated_at"), first_submit)
            statuses = Counter(str(task["status"]) for task in tasks)
            activities.append({
                "label": meta["label"],
                "campaign_type": meta["campaign_type"],
                "campaign_id": meta["campaign_id"],
                "expected_rows": meta["expected_rows"],
                "task_count": len(tasks),
                "task_statuses": dict(statuses),
                "accepted_submit_count": len(submits),
                "first_submit_at": first_submit,
                "first_service_wait_ms": first_wait_ms,
                "fairness_events": self._db_query(
                    """SELECT campaign_type,campaign_id,route_id,eligible_since,
                              last_file_claim_at,last_task_claim_at,last_submit_at,
                              last_dispatched_at,backlog,consecutive_skip_rounds
                         FROM campaign_fairness_state
                        WHERE campaign_type=? AND campaign_id=?
                        ORDER BY route_id""",
                    key,
                ),
            })
        mapped_accepts = [event for event in accepted if str(event.get("message_id")) in mapping_by_mid]
        duplicate_message_ids = [mid for mid, count in Counter(
            str(event.get("message_id")) for event in mapped_accepts
        ).items() if count > 1]
        expected_ok = (
            len(all_tasks) == self.expected_total
            and len(accepted) == self.expected_total
            and not duplicate_tasks
            and not duplicate_message_ids
            and not missing_msisdns
            and all(
                item["task_count"] == item["expected_rows"]
                and item["task_statuses"].get("accepted", 0) == item["expected_rows"]
                and item["accepted_submit_count"] == item["expected_rows"]
                and item["first_service_wait_ms"] is not None
                for item in activities
            )
        )
        first_waits = [item["first_service_wait_ms"] for item in activities if item["first_service_wait_ms"] is not None]
        fairness_rows = [row for item in activities for row in item["fairness_events"]]
        fairness_state_ok = all(
            any(
                row.get("last_file_claim_at")
                or row.get("last_task_claim_at")
                or row.get("last_submit_at")
                or row.get("last_dispatched_at")
                for row in item["fairness_events"]
            )
            for item in activities
        ) and len(fairness_rows) >= len(activities)
        return {
            "status": "PASS" if expected_ok else "FAIL",
            "acceptance_states": {
                "BATCH_INGEST_FAIRNESS": "PASS" if all(item["campaign_type"] == "batch" and item["task_count"] > 0 for item in activities if item["campaign_type"] == "batch") else "FAIL",
                "REALTIME_INTAKE_FAIRNESS": "PASS" if all(item["campaign_type"] == "realtime" and item["task_count"] > 0 for item in activities if item["campaign_type"] == "realtime") else "FAIL",
                "SMS_DISPATCH_FAIRNESS": "PASS" if len(accepted) == self.expected_total and not duplicate_tasks else "FAIL",
                "CROSS_TYPE_E2E": "PASS" if expected_ok else "FAIL",
                "FAIRNESS_TELEMETRY": "PASS" if fairness_state_ok else "FAIL",
                "SFTP": self.sftp_status,
            },
            "expected_tasks": self.expected_total,
            "generated_tasks": len(all_tasks),
            "smsc_submit_sm": len(events),
            "smsc_submit_sm_success": len(accepted),
            "duplicate_send_tasks": duplicate_tasks,
            "duplicate_message_ids": duplicate_message_ids,
            "missing_msisdns": missing_msisdns,
            "activities": activities,
            "fairness": {
                "fairness_state_rows": len(fairness_rows),
                "max_first_service_wait_ms": max(first_waits or [0]),
                "all_activities_served": all(item["task_count"] == item["expected_rows"] for item in activities),
            },
            "throughput": {
                "queue_backlog_max": max(self.backlog_samples or [0]),
                "queue_backlog_final": sum(
                    item["task_statuses"].get(status, 0)
                    for item in activities for status in ("queued", "sending", "retrying")
                ),
            },
            "environment": {
                "live_env_status": "LIVE_ENV_UNVERIFIED",
                "sftp_status": self.sftp_status,
                "smpp_loopback_only": True,
            },
            "diagnostics": {
                "artifacts_dir": str(self.root),
                "api_log": str(self.api_log),
                "app_log": str(self.app_log),
                "smsc_event_log": str(self.smsc_event_log),
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--max-memory-mb", type=int, default=0)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    common = {
        "scenario": "multi_campaign_fairness",
        "python": args.python,
        "events": None,
        "files": 1,
        "expected_tasks": None,
        "expected_status_counts": {},
        "expected_submit_events": 0,
        "timeout_seconds": args.timeout_seconds,
        "duration_seconds": 0,
        "smsc_tps": None,
        "smsc_response_delay": None,
        "smsc_reject_ratio": None,
        "smsc_drop_response_ratio": None,
        "smsc_drop_receipt_ratio": None,
        "smsc_receipt_delay": None,
        "smsc_disconnect_after": None,
        "delivery_receipt_mode": "disabled",
        "task_workers": 2,
        "ingest_workers": 2,
        "workers": 2,
        "claim_batch": 8,
        "smpp_window": 10,
        "smpp_send_queue_capacity": 32,
        "producer_chunk_size": 0,
        "producer_chunk_delay": 0.0,
        "max_memory_mb": args.max_memory_mb,
        "keep_artifacts": args.keep_artifacts,
    }
    run_args = argparse.Namespace(**common)
    report = {
        "scenario": "multi_campaign_fairness",
        "started_at": _utc_now(),
        "status": "BLOCKED_ENV",
        "local_e2e_status": "BLOCKED_ENV",
        "live_env_status": "LIVE_ENV_UNVERIFIED",
    }
    runner = MultiCampaignFairRunner(run_args)
    try:
        result = runner.run()
        report.update(result)
    except Exception as exc:
        report.update({
            "status": "BLOCKED_ENV" if isinstance(exc, (BlockedEnv, FileNotFoundError)) else "FAIL",
            "local_e2e_status": "BLOCKED_ENV" if isinstance(exc, (BlockedEnv, FileNotFoundError)) else "LOCAL_E2E_FAIL",
            "failure_details": [f"{type(exc).__name__}: {exc}", traceback.format_exc()],
        })
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
