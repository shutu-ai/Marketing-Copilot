from __future__ import annotations

import duckdb

from tools.cutover_vnext_store import _migrate_sms_tasks


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
