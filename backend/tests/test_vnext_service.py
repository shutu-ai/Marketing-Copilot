from __future__ import annotations

import asyncio

from services import dashboard_service, realtime_campaign_service, vnext_service
from vnext.spool import DEFAULT_MAX_SPOOL_BYTES, TaskSpool


class _FakeRuntime:
    def __init__(self, root):
        self._started = True
        self.root = root
        self.spool = TaskSpool(root / "spool", virtual_partition_count=16, max_bytes=100_000)
        from vnext.ingress import IngressRouter
        self.ingress = IngressRouter(self.spool)
        self.route_configs = {"7": {"route_id": 7}}

    def enqueue_chunk(self, *args, **kwargs):
        return self.ingress.enqueue_chunk(*args, **kwargs)


def test_spool_budget_defaults_to_four_gib_and_accepts_larger_values(monkeypatch):
    monkeypatch.delenv("MC_VNEXT_MAX_SPOOL_BYTES", raising=False)
    assert vnext_service._env_bytes(
        "MC_VNEXT_MAX_SPOOL_BYTES", DEFAULT_MAX_SPOOL_BYTES
    ) == DEFAULT_MAX_SPOOL_BYTES

    monkeypatch.setenv("MC_VNEXT_MAX_SPOOL_BYTES", str(16 * 1024 ** 3))
    assert vnext_service._env_bytes(
        "MC_VNEXT_MAX_SPOOL_BYTES", DEFAULT_MAX_SPOOL_BYTES
    ) == 16 * 1024 ** 3

    monkeypatch.setenv("MC_VNEXT_MAX_SPOOL_BYTES", str(2 * 1024 ** 3))
    assert vnext_service._env_bytes(
        "MC_VNEXT_MAX_SPOOL_BYTES", DEFAULT_MAX_SPOOL_BYTES
    ) == DEFAULT_MAX_SPOOL_BYTES


def test_service_adapter_renders_templates_and_preserves_vnext_fields(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    monkeypatch.setattr(vnext_service, "_template_map", lambda *_args: {
        "by_lang": {"english": {"content": "Hi {msisdn}", "source_addr": "SRC"}},
        "fallback": None, "default": None, "direct": None,
    })
    monkeypatch.setattr(vnext_service, "_excluded", lambda values: {"251900000002"})
    result = vnext_service.enqueue_rows(
        "batch",
        {"id": 11, "status": "in_progress", "smpp_config_id": 7,
         "phone_prefix": "251", "flash_sms": True, "dispatch_generation": 3},
        [
            {"msisdn": "0900000001", "language": "english"},
            {"msisdn": "0900000002", "language": "english"},
            {"msisdn": "0900000001", "language": "english"},
        ],
        source_file="customers.csv", batch_id=4,
    )
    assert result["created"] == 1
    assert result["excluded"] == 1
    assert result["duplicate"] == 1
    records = list(fake.spool.iter_from(0, 0))
    records += [item for partition in range(1, 16) for item in fake.spool.iter_from(partition, 0)]
    assert len(records) == 1
    assert records[0].normalized_msisdn == "251900000001"
    assert records[0].language == "english"
    assert records[0].message == "Hi 251900000001"
    assert records[0].flash_sms is True
    assert records[0].source_addr == "SRC"


def test_vnext_task_list_reads_bounded_projection_pages_and_backfills_response_fields(
    tmp_path, monkeypatch
):
    import duckdb

    fake = _FakeRuntime(tmp_path)
    db_path = tmp_path / "vnext-projection.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute(
        """
        CREATE TABLE vnext_progress_events (
            event_id VARCHAR PRIMARY KEY,
            partition_id INTEGER,
            ordinal BIGINT,
            campaign_type VARCHAR,
            campaign_id BIGINT,
            route_id BIGINT,
            normalized_msisdn VARCHAR,
            language VARCHAR,
            state VARCHAR,
            parts_written INTEGER,
            parts_total INTEGER,
            message VARCHAR,
            error VARCHAR,
            created_at DOUBLE,
            business_day VARCHAR,
            expires_at DOUBLE
        )
        """
    )
    business_day = fake.spool.current_business_day
    connection.executemany(
        """
        INSERT INTO vnext_progress_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "task-1", 0, 1, "batch", 11, 7, "251900000001", "english",
                "socket_written", 1, 1, "Hello 251900000001", "", 3.0,
                business_day, 0.0,
            ),
            (
                "task-1:resp:1", 0, 2, "batch", 11, 7, "", "",
                "submit_sm_resp_success", 1, 1, "", "", 4.0,
                business_day, 0.0,
            ),
            (
                "task-2", 0, 3, "batch", 12, 7, "251900000002", "amharic",
                "write_failed", 0, 1, "Selam 251900000002", "timeout", 2.0,
                business_day, 0.0,
            ),
        ],
    )
    connection.close()

    class Projection:
        def snapshot(self):
            return {"db_path": str(db_path)}

    fake.projection = Projection()
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    monkeypatch.setattr(
        vnext_service,
        "_progress_events",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("task list must use the projection page")
        ),
    )

    first_page = vnext_service.list_tasks(page=1, page_size=1)
    assert first_page["total"] == 3
    assert first_page["page"] == 1
    assert first_page["page_size"] == 1
    assert first_page["has_more"] is True
    assert first_page["stats_source"] == "vnext_projection_page"
    assert first_page["items"][0]["event_id"] == "task-1:resp:1"
    assert first_page["items"][0]["msisdn"] == "251900000001"
    assert first_page["items"][0]["language"] == "english"
    assert first_page["items"][0]["content"] == "Hello 251900000001"
    assert first_page["items"][0]["updated_at"] == "1970-01-01T00:00:04+00:00"

    second_page = vnext_service.list_tasks(page=2, page_size=1)
    assert second_page["items"][0]["event_id"] == "task-1"
    assert second_page["has_more"] is True

    cursor_page = vnext_service.list_tasks(page=1, page_size=1, cursor=2)
    assert cursor_page["page"] == 2
    assert cursor_page["items"][0]["event_id"] == "task-1"

    filtered_page = vnext_service.list_tasks(
        page=1, page_size=10, msisdn="251900000001"
    )
    assert filtered_page["total"] == 2
    assert {item["msisdn"] for item in filtered_page["items"]} == {"251900000001"}


def test_vnext_task_campaign_names_are_resolved_in_one_bounded_control_read(monkeypatch):
    calls = []

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

    class Connection:
        def execute(self, query, params):
            calls.append((query, tuple(params)))
            if "batch_campaigns" in query:
                return Result([{"id": 11, "name": "批量活动 11"}])
            return Result([{"id": 6, "name": "实时活动 6"}])

    monkeypatch.setattr(
        vnext_service,
        "read_transaction",
        lambda callback, **_kwargs: callback(Connection()),
    )

    names = vnext_service._campaign_names_for_rows([
        {"campaign_type": "batch", "campaign_id": 11},
        {"campaign_type": "realtime", "campaign_id": 6},
        {"campaign_type": "realtime", "campaign_id": 6},
    ])

    assert names == {
        ("batch", 11): "批量活动 11",
        ("realtime", 6): "实时活动 6",
    }
    assert len(calls) == 2
    assert {call[1] for call in calls} == {(11,), (6,)}


def test_service_control_maps_campaign_states(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)
    fake.set_campaign_state = lambda campaign_type, campaign_id, state: setattr(
        fake, "last_control", (campaign_type, campaign_id, state)
    )
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    assert vnext_service.set_campaign_state("batch", 5, "in_progress")["state"] == "running"
    assert fake.last_control == ("batch", 5, "running")


def test_vnext_stats_contract_uses_socket_written_as_sent(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)

    class Projection:
        def snapshot(self):
            return {
                "last_applied_at": 100.0,
                "projection_lag_seconds": 0.0,
                "daily_totals": [{
                    "business_day": fake.spool.current_business_day,
                    "socket_written": 3,
                    "socket_written_pdus": 4,
                    "response_success": 2,
                    "response_error": 1,
                    "write_failed": 0,
                    "unknown": 0,
                    "queued": 0,
                    "taken": 3,
                    "accepted": 3,
                    "cancelled": 0,
                    "expired": 0,
                }],
            }

    fake.projection = Projection()
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    stats = vnext_service.task_stats()
    assert stats["socket_written"] == 3
    assert stats["socket_written_events"] == 3
    assert stats["socket_written_pdus"] == 4
    assert stats["sent"] == 3
    assert stats["today_sent_count"] == 3
    assert stats["smsc_accepted"] == 2
    assert stats["stats_source"] == "vnext_projection"
    assert stats["delivery_receipts_available"] is False

    dashboard = vnext_service.dashboard_stats()
    assert dashboard["today_sent_count"] == 3
    assert dashboard["daily_trend"][-1]["sent_count"] == 3
    assert dashboard["sent_metric"] == "socket_written"


def test_campaign_daily_totals_filters_projection_to_one_campaign(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)

    class Projection:
        def snapshot(self):
            return {
                "campaign_daily_totals": [
                    {
                        "business_day": "2026-10-20",
                        "campaign_type": "batch",
                        "campaign_id": 11,
                        "socket_written": 37,
                    },
                    {
                        "business_day": "2026-10-19",
                        "campaign_type": "batch",
                        "campaign_id": 11,
                        "socket_written": 12,
                    },
                    {
                        "business_day": "2026-10-20",
                        "campaign_type": "batch",
                        "campaign_id": 12,
                        "socket_written": 99,
                    },
                ]
            }

    fake.projection = Projection()
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)

    result = vnext_service.campaign_daily_totals("batch", 11)

    assert [(item["business_day"], item["socket_written"]) for item in result] == [
        ("2026-10-20", 37),
        ("2026-10-19", 12),
    ]


def test_vnext_task_stats_use_projection_and_campaign_stats_use_live_aggregate(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)

    class Projection:
        def snapshot(self):
            return {
                "projection_lag_seconds": 0.0,
                "daily_totals": [{
                    "business_day": fake.spool.current_business_day,
                    "queued": 0,
                    "taken": 9,
                    "socket_written": 3,
                    "socket_written_pdus": 5,
                    "response_success": 4,
                    "response_error": 1,
                    "unknown": 0,
                    "write_failed": 0,
                    "cancelled": 0,
                    "accepted": 10,
                    "expired": 0,
                }],
                "campaign_daily_totals": [{
                    "business_day": fake.spool.current_business_day,
                    "campaign_type": "realtime",
                    "campaign_id": 6,
                    "socket_written": 3,
                    "socket_written_pdus": 5,
                    "response_success": 4,
                    "response_error": 1,
                    "unknown": 0,
                    "write_failed": 0,
                }],
            }

    fake.projection = Projection()
    fake.campaign_counter_snapshot = lambda: {
        "campaigns": [{
            "campaign_type": "realtime",
            "campaign_id": 6,
            "accepted_tasks": 10,
            "tasks_taken": 9,
            "socket_written_events": 7,
            "socket_written_pdus": 8,
            "response_success": 6,
            "response_error": 1,
            "unknown": 0,
            "write_failed": 0,
            "cancelled": 0,
            "expired": 0,
            "durable_events": 14,
        }],
        "complete": True,
        "source": "worker_status_aggregate",
    }
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)
    monkeypatch.setattr(
        vnext_service,
        "_progress_events",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("statistics must not replay worker journals")
        ),
    )

    task_stats = vnext_service.task_stats()
    campaign_stats = vnext_service.campaign_stats("realtime", 6)

    assert task_stats["socket_written"] == 3
    assert task_stats["socket_written_pdus"] == 5
    assert task_stats["smsc_accepted"] == 4
    assert task_stats["today_sent_count"] == 3
    assert task_stats["stats_source"] == "vnext_projection"
    assert campaign_stats["sent_count"] == 7
    assert campaign_stats["sent_pdus"] == 8
    assert campaign_stats["smsc_accepted_pdus"] == 6
    assert campaign_stats["stats_complete"] is True
    assert campaign_stats["sent_users"] == 7
    assert campaign_stats["today_sent_users"] == 3
    assert campaign_stats["today_sent_pdus"] == 5
    assert campaign_stats["today_smsc_accepted_pdus"] == 4


def test_vnext_realtime_stats_do_not_query_legacy_task_table(monkeypatch):
    statements = []

    class Result:
        def __init__(self, value):
            self.value = value

        def fetchone(self):
            return self.value

    class Connection:
        def execute(self, sql, *_args):
            statements.append(sql)
            return Result({
                "file_count": 2,
                "total_msisdn_sum": 100,
                "total_matched": 80,
            })

    monkeypatch.setattr(
        realtime_campaign_service,
        "read_transaction",
        lambda callback, **_kwargs: callback(Connection()),
    )
    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(vnext_service, "campaign_stats", lambda *_args: {
        "metric_contract": {"sent_metric": "socket_written"},
        "sent_count": 12,
        "sent_pdus": 14,
        "smsc_accepted_pdus": 10,
        "smsc_rejected_pdus": 1,
        "unknown_pdus": 0,
        "write_failed": 0,
        "accepted_tasks": 20,
        "tasks_taken": 18,
        "event_count": 30,
        "stats_complete": True,
        "stats_source": "worker_status_aggregate",
    })

    stats = realtime_campaign_service.get_realtime_stats(6)

    assert stats["pipeline"] == "vnext"
    assert stats["sent_count"] == 12
    assert stats["file_count"] == 2
    assert not any("sms_tasks" in sql for sql in statements)


def test_smpp_stats_direct_runtime_snapshot_includes_configured_routes(tmp_path, monkeypatch):
    fake = _FakeRuntime(tmp_path)
    fake.status = lambda: {
        "worker_progress": [],
        "projection": {},
        "spool": {},
        "daily_epoch": {},
    }
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)

    stats = vnext_service.smpp_stats()

    assert stats["pipeline"] == "vnext"
    assert [item["config_id"] for item in stats["items"]] == [7]


def test_smpp_stats_uses_lightweight_capacity_snapshot_and_exposes_rejections(
    tmp_path, monkeypatch
):
    fake = _FakeRuntime(tmp_path)
    business_day = fake.spool.current_business_day
    fake.status = lambda: (_ for _ in ()).throw(
        AssertionError("capacity stats must not use the full runtime status")
    )
    fake.capacity_snapshot = lambda: {
        "worker_progress": [],
        "projection": {
            "projection_lag_seconds": 0.7,
            "daily_totals": [{
                "business_day": business_day,
                "socket_written": 12,
                "response_success": 10,
                "response_error": 2,
                "unknown": 1,
                "write_failed": 3,
            }],
        },
        "spool": {"pending_records": 4},
        "daily_epoch": {"business_day": business_day},
    }
    monkeypatch.setattr(vnext_service, "_RUNTIME", fake)

    stats = vnext_service.smpp_stats()

    assert stats["summary"]["today_socket_written"] == 12
    assert stats["summary"]["today_smsc_accepted"] == 10
    assert stats["summary"]["today_smsc_rejected"] == 2
    assert stats["summary"]["projection_lag_seconds"] == 0.7
    assert stats["summary"]["queue_size"] == 4


def test_vnext_dashboard_does_not_scan_legacy_send_table(monkeypatch):
    statements = []

    class Result:
        def __init__(self, value=None):
            self.value = value

        def fetchone(self):
            return self.value if self.value is not None else {"cnt": 0}

        def fetchall(self):
            return []

    class Connection:
        def execute(self, sql, *_args):
            statements.append(sql)
            if "realtime_campaign_files" in sql:
                return Result({"file_count": 0, "total_msisdn": 0, "total_matched": 0})
            return Result()

    monkeypatch.setattr(dashboard_service, "read_transaction", lambda callback, **_kwargs: callback(Connection()))
    monkeypatch.setattr(vnext_service, "is_active", lambda: True)
    monkeypatch.setattr(vnext_service, "dashboard_stats", lambda: {
        "pipeline": "vnext",
        "today_sent_count": 4,
        "daily_trend": [],
    })

    stats = dashboard_service.get_stats()

    assert stats["pipeline"] == "vnext"
    assert stats["today_sent_count"] == 4
    assert not any("sms_tasks" in sql for sql in statements)
