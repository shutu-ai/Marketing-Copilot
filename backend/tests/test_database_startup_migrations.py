import database


def test_database_startup_maintenance_is_marked_and_not_repeated(tmp_path, monkeypatch):
    previous_path = database.get_database_file()
    database_path = str(tmp_path / "startup-maintenance.duckdb")
    try:
        database.set_database_file(database_path)
        database.init_db()

        conn = database.get_connection()
        markers = {
            row[0]: row[1]
            for row in conn.execute(
                """SELECT key, value FROM app_settings
                   WHERE key LIKE 'db_%' OR key = 'sms_task_message_ids_backfill_v1'"""
            ).fetchall()
        }
        indexes = {
            row[0]
            for row in conn.execute(
                """SELECT index_name FROM duckdb_indexes()
                   WHERE index_name IN ('idx_delivery_message_id', 'idx_receipt_unique')"""
            ).fetchall()
        }
        conn.close()

        assert {
            "db_schema_columns_v1",
            "db_realtime_cleanup_v1",
            "db_processed_files_unique_v1",
            "db_data_normalization_v1",
            "db_receipt_indexes_v2",
            "sms_task_message_ids_backfill_v1",
        } <= markers.keys()
        assert all(markers[key] == "completed" for key in markers)
        assert indexes == {"idx_delivery_message_id", "idx_receipt_unique"}

        def should_not_run(*_args, **_kwargs):
            raise AssertionError("receipt index inspection repeated after completion")

        monkeypatch.setattr(database, "_index_definition_matches", should_not_run)
        database.init_db()
    finally:
        database.close_root_connection()
        database.set_database_file(previous_path)
