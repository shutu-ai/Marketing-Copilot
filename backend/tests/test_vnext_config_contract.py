from __future__ import annotations

import duckdb

from tools.vnext_config_contract_check import audit_database


def _create_db(path, *, tps_scope: str = "route_shared", bind_type: str = "transceiver"):
    conn = duckdb.connect(str(path))
    try:
        conn.execute("""
            CREATE TABLE smpp_configs (
                id INTEGER, name VARCHAR, host VARCHAR, port INTEGER,
                system_id VARCHAR, password VARCHAR, source_addr VARCHAR,
                bind_type VARCHAR, address_ton INTEGER, address_npi INTEGER,
                data_coding INTEGER, pool_size INTEGER, window_size INTEGER,
                enquire_interval INTEGER, timeout INTEGER, tps INTEGER,
                tps_scope VARCHAR, priority INTEGER, reconnect_max_seconds INTEGER,
                delivery_receipt_mode VARCHAR, enabled INTEGER
            )
        """)
        conn.execute(
            """INSERT INTO smpp_configs VALUES
                (1, 'primary', '127.0.0.1', 2775, 'sid', 'secret', 'VNEXT',
                 ?, 0, 0, 0, 4, 50, 30, 10, 200, ?, 1, 60, 'optional', 1)""",
            [bind_type, tps_scope],
        )
    finally:
        conn.close()


def test_vnext_config_contract_reports_worker_bind_window_and_tps(tmp_path):
    path = tmp_path / "config.duckdb"
    _create_db(path, tps_scope="per_bind")

    result = audit_database(path)

    assert result["status"] == "PASS"
    route = result["routes"][0]
    assert route["pool_size"] == 4
    assert route["window_size"] == 50
    assert route["effective_total_limit_tps"] == 800.0
    assert route["window_total_pdu_capacity"] == 200
    assert "password" not in route
    assert result["contract"]["worker_semantics"] == "business_data_concurrency"


def test_vnext_config_contract_rejects_receiver_outbound_route(tmp_path):
    path = tmp_path / "config.duckdb"
    _create_db(path, tps_scope="per_bind", bind_type="receiver")

    result = audit_database(path)

    assert result["status"] == "FAIL"
    assert any("receiver bind" in item for item in result["errors"])
