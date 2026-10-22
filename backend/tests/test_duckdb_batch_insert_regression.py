import duckdb


def test_delete_then_batch_insert_select_reuses_primary_key(tmp_path):
    database = tmp_path / "batch-insert-select.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "CREATE TABLE target(id INTEGER PRIMARY KEY, payload VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE source(id INTEGER PRIMARY KEY, payload VARCHAR)"
        )
        connection.execute("INSERT INTO target VALUES (1, 'old')")
        connection.execute("INSERT INTO source VALUES (1, 'new')")

        connection.execute("BEGIN")
        connection.execute("DELETE FROM target")
        connection.execute("INSERT INTO target SELECT * FROM source")
        connection.execute("COMMIT")

        assert connection.execute("SELECT * FROM target").fetchall() == [(1, "new")]
    finally:
        connection.close()


def test_bounded_delete_then_batch_insert_with_index(tmp_path):
    database = tmp_path / "indexed-batch-insert.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "CREATE TABLE target(id INTEGER PRIMARY KEY, key_col INTEGER, payload VARCHAR)"
        )
        connection.execute("CREATE INDEX target_key_idx ON target(key_col)")
        connection.execute(
            "INSERT INTO target "
            "SELECT i, i % 100, concat('old-', i) FROM range(1000) s(i)"
        )

        connection.execute("BEGIN")
        connection.execute("DELETE FROM target")
        connection.execute(
            "INSERT INTO target "
            "SELECT i, i % 100, concat('new-', i) FROM range(1000) s(i)"
        )
        connection.execute("COMMIT")

        assert connection.execute(
            "SELECT COUNT(*), MIN(payload), MAX(payload) FROM target"
        ).fetchone() == (1000, "new-0", "new-999")
    finally:
        connection.close()
