from __future__ import annotations

import duckdb

from tools.cutover_vnext_store import _migrate_sms_tasks, _prune_target_sms_tasks


def test_cutover_migrates_historical_tasks_into_the_new_index(tmp_path):
    source = tmp_path / "source.duckdb"
    projection = tmp_path / "projection" / "vnext-projection.duckdb"
    connection = duckdb.connect(str(source))
    try:
        connection.execute(
            """
            CREATE TABLE sms_tasks(
                id BIGINT,
                campaign_type VARCHAR,
                campaign_id BIGINT,
                campaign_name VARCHAR,
                msisdn VARCHAR,
                language VARCHAR,
                status VARCHAR,
                content VARCHAR,
                error_msg VARCHAR,
                smpp_config_id BIGINT,
                transport_smpp_config_id BIGINT,
                submit_sm_parts INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                task_date DATE
            )
            """
        )
        connection.executemany(
            "INSERT INTO sms_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    7, "batch", 3, "Batch 3", "251908341307", "english", "accepted",
                    "Hello", "", 2, 2, 1, "2026-10-20 12:00:00",
                    "2026-10-20 12:00:02", "2026-10-20",
                ),
                (
                    8, "realtime", 6, "Realtime 6", "090000000001", "amharic", "failed",
                    "Selam", "timeout", 1, None, 1, "2026-10-19 12:00:00",
                    "2026-10-19 12:00:03", "2026-10-19",
                ),
            ],
        )
    finally:
        connection.close()

    result = _migrate_sms_tasks(source, projection, batch_size=1)

    assert result["source_sms_task_rows_seen"] == 2
    check = duckdb.connect(str(projection), read_only=True)
    try:
        rows = check.execute(
            """SELECT event_id,campaign_type,campaign_id,normalized_msisdn,
                      campaign_name,state,business_day
                 FROM vnext_task_index ORDER BY event_id"""
        ).fetchall()
    finally:
        check.close()
    assert rows == [
        ("historical:7", "batch", 3, "251908341307", "Batch 3", "accepted", "2026-10-20"),
        ("historical:8", "realtime", 6, "90000000001", "Realtime 6", "failed", "2026-10-19"),
    ]


def test_cutover_min_business_day_excludes_older_tasks(tmp_path):
    source = tmp_path / "source-cutoff.duckdb"
    projection = tmp_path / "projection" / "vnext-projection.duckdb"
    connection = duckdb.connect(str(source))
    try:
        connection.execute(
            """
            CREATE TABLE sms_tasks(
                id BIGINT,
                campaign_type VARCHAR,
                campaign_id BIGINT,
                campaign_name VARCHAR,
                msisdn VARCHAR,
                language VARCHAR,
                status VARCHAR,
                content VARCHAR,
                error_msg VARCHAR,
                smpp_config_id BIGINT,
                transport_smpp_config_id BIGINT,
                submit_sm_parts INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                task_date DATE
            )
            """
        )
        connection.executemany(
            "INSERT INTO sms_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    1, "batch", 3, "old", "251900000001", "english", "accepted",
                    "old", "", 2, 2, 1, "2026-10-18 12:00:00",
                    "2026-10-18 12:00:01", "2026-10-18",
                ),
                (
                    2, "batch", 3, "kept", "251900000002", "english", "accepted",
                    "kept", "", 2, 2, 1, "2026-10-19 12:00:00",
                    "2026-10-19 12:00:01", "2026-10-19",
                ),
            ],
        )
    finally:
        connection.close()

    result = _migrate_sms_tasks(
        source,
        projection,
        batch_size=1,
        min_business_day="2026-10-19",
    )

    assert result["source_sms_task_rows_seen"] == 1
    check = duckdb.connect(str(projection), read_only=True)
    try:
        rows = check.execute(
            "SELECT event_id,business_day FROM vnext_task_index ORDER BY event_id"
        ).fetchall()
    finally:
        check.close()
    assert rows == [("historical:2", "2026-10-19")]


def test_cutover_prunes_only_old_task_rows_from_copied_business_db(tmp_path):
    target = tmp_path / "control.duckdb"
    connection = duckdb.connect(str(target))
    try:
        connection.execute(
            "CREATE TABLE sms_tasks(id BIGINT, task_date DATE, created_at TIMESTAMP)"
        )
        connection.executemany(
            "INSERT INTO sms_tasks VALUES (?,?,?)",
            [
                (1, "2026-10-18", "2026-10-18 10:00:00"),
                (2, "2026-10-19", "2026-10-19 10:00:00"),
            ],
        )
        for table, schema in (
            ("sms_send_parts", "attempt_token VARCHAR"),
            ("sms_message_bindings_v2", "task_id BIGINT"),
            ("sms_task_message_ids", "task_id BIGINT"),
            ("delivery_receipts", "task_id BIGINT"),
            ("task_events", "task_id BIGINT"),
            ("sms_result_events", "task_id BIGINT"),
            ("sms_fast_batch_items", "task_id BIGINT"),
            ("sms_send_attempts", "attempt_token VARCHAR, task_id BIGINT"),
            ("failed_task_archive", "task_id BIGINT"),
        ):
            connection.execute(f"CREATE TABLE {table}({schema})")
        connection.execute(
            "CREATE TABLE task_archive_manifests(id BIGINT, task_date DATE)"
        )
        connection.execute("INSERT INTO sms_send_attempts VALUES ('attempt-old', 1)")
        connection.execute("INSERT INTO sms_send_parts VALUES ('attempt-old')")
        for table in (
            "sms_message_bindings_v2", "sms_task_message_ids", "delivery_receipts",
            "task_events", "sms_result_events", "sms_fast_batch_items",
            "failed_task_archive",
        ):
            connection.execute(f"INSERT INTO {table} VALUES (1)")
        connection.execute(
            "INSERT INTO task_archive_manifests VALUES (1, '2026-10-18')"
        )
    finally:
        connection.close()

    result = _prune_target_sms_tasks(target, "2026-10-19")

    assert result["expired_task_rows_removed"] == 1
    check = duckdb.connect(str(target), read_only=True)
    try:
        assert check.execute("SELECT id FROM sms_tasks").fetchall() == [(2,)]
        for table in (
            "sms_send_parts", "sms_message_bindings_v2", "sms_task_message_ids",
            "delivery_receipts", "task_events", "sms_result_events",
            "sms_fast_batch_items", "sms_send_attempts", "failed_task_archive",
            "task_archive_manifests",
        ):
            assert check.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        check.close()
