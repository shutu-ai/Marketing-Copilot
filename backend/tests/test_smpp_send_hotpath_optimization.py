from __future__ import annotations

import pytest

import database
from services import sms_task_service, smpp_service, sms_write_buffer
from services.smpp_gateway import _SmppConnection


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "smpp-hotpath.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.flush()


def _sending_task():
    conn = database.get_connection()
    campaign_id = conn.execute(
        "INSERT INTO batch_campaigns(name,status) VALUES ('hotpath','in_progress') RETURNING id"
    ).fetchone()["id"]
    task = conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,
            status,claim_token,attempts,max_attempts)
           VALUES ('batch',?,'hotpath','251900000001','hello','hotpath-task',
                   'sending','claim-1',1,3) RETURNING *""",
        (campaign_id,),
    ).fetchone()
    conn.commit()
    conn.close()
    return dict(task)


def test_multipart_authorization_materializes_parts_in_one_durable_statement(
    isolated_db, monkeypatch,
):
    task = _sending_task()
    statements = []
    original_execute = database.WrappedConnection.execute

    def tracked_execute(connection, sql, params=None):
        if "INSERT INTO sms_send_parts" in str(sql):
            statements.append(str(sql))
        return original_execute(connection, sql, params)

    monkeypatch.setattr(database.WrappedConnection, "execute", tracked_execute)
    attempt = sms_task_service.record_send_intent(
        task, smpp_config_id=7, slot=2, planned_parts=4, connection_generation="gen-1",
    )

    assert attempt
    assert len(statements) == 1
    conn = database.get_connection()
    parts = conn.execute(
        """SELECT part_no,connection_generation,state FROM sms_send_parts
           WHERE attempt_token=? ORDER BY part_no""", (attempt,),
    ).fetchall()
    assert [row["part_no"] for row in parts] == [1, 2, 3, 4]
    assert {row["connection_generation"] for row in parts} == {"gen-1"}
    assert {row["state"] for row in parts} == {"planned"}
    assert conn.execute(
        "SELECT COUNT(*) count FROM task_events WHERE task_id=? AND event_type='submit_authorized'",
        (task["id"],),
    ).fetchone()["count"] == 1
    conn.close()


def test_smpp_stats_expose_route_shared_parts_per_second_limit(isolated_db):
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO smpp_configs
           (name,host,system_id,password,tps,pool_size,enabled)
           VALUES ('route','127.0.0.1','system','secret',120,4,1)""",
    )
    conn.commit()
    conn.close()
    smpp_service._STATS_CACHE = None

    item = smpp_service.get_config_stats()["items"][0]

    assert item["configured_route_tps"] == 120.0
    assert item["configured_per_bind_tps"] is None
    assert item["effective_total_limit_tps"] == 120.0
    assert item["tps_scope"] == "route_shared"
    assert item["tps_unit"] == "submit_sm_parts/s"


def test_internal_multiplier_cannot_raise_route_limit(monkeypatch):
    monkeypatch.setenv("MC_SMPP_INTERNAL_TPS_MULTIPLIER", "4")
    connection = _SmppConnection(
        {"id": 901, "tps": 120, "window_size": 4}, slot=1,
    )

    assert connection.tps_multiplier == 1.0
    assert connection.status()["effective_total_limit_tps"] == 120.0
