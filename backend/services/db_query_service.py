from database import (
    is_transient_read_error,
    is_transient_write_error,
    read_transaction,
    write_transaction,
)
import re


def get_tables() -> list:
    def read(conn):
        rows = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        tables = [dict(r) for r in rows]

        for tbl in tables:
            col_rows = conn.execute("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.ordinal_position
                FROM information_schema.columns c
                WHERE c.table_schema = 'main' AND c.table_name = ?
                ORDER BY c.ordinal_position
            """, (tbl["table_name"],)).fetchall()
            col_list = [dict(r) for r in col_rows]

            pk_rows = conn.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                WHERE tc.table_schema = 'main'
                    AND tc.table_name = ?
                    AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.ordinal_position
            """, (tbl["table_name"],)).fetchall()
            pk_set = {r["column_name"] for r in pk_rows}

            fk_rows = conn.execute("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS ref_table,
                    ccu.column_name AS ref_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema = 'main'
                    AND tc.table_name = ?
                    AND tc.constraint_type = 'FOREIGN KEY'
            """, (tbl["table_name"],)).fetchall()
            fk_map = {r["column_name"]: {"ref_table": r["ref_table"], "ref_column": r["ref_column"]} for r in fk_rows}

            for col in col_list:
                col["is_pk"] = col["column_name"] in pk_set
                col["foreign_key"] = fk_map.get(col["column_name"])

            tbl["columns"] = col_list
        return tables

    return read_transaction(read, operation="db_query.tables")


def execute_query(sql: str, params: list = None) -> dict:
    sql_stripped = sql.strip()

    is_readonly = bool(re.match(r'^\s*(SELECT|WITH)\b', sql_stripped, re.IGNORECASE))

    def execute_read(conn):
        cursor = conn.execute(sql, params) if params else conn.execute(sql)
        columns = [desc[0] for desc in conn._description] if conn._description else []
        rows = cursor.fetchall()
        return {
            "type": "select",
            "columns": columns,
            "rows": [list(r.values()) for r in rows],
            "row_count": len(rows),
        }

    def execute_write(conn):
        cursor = conn.execute(sql, params) if params else conn.execute(sql)
        return getattr(cursor, "rowcount", 0)

    try:
        if is_readonly:
            return read_transaction(execute_read, operation="db_query.execute.read")
        row_count = write_transaction(execute_write, operation="db_query.execute.write")
        return {
            "type": "execute",
            "affected_rows": row_count if row_count > 0 else 0,
            "message": "Query executed successfully",
        }
    except Exception as e:
        # Preserve the query console's existing {type: error} contract for
        # invalid SQL/catalog references, while allowing transient database
        # availability failures to reach the stable API-level 503 handler.
        if is_transient_write_error(e) or is_transient_read_error(e):
            raise
        return {"type": "error", "error": str(e)}
