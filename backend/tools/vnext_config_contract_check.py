"""Read-only audit for the VNext worker/SMPP capacity contract.

The checker intentionally emits only non-secret route fields.  It is suitable
for a deployment gate and for producing an evidence artifact without copying
SMPP passwords or system credentials into reports.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = {
    "id", "host", "port", "source_addr", "bind_type", "pool_size",
    "window_size", "tps", "tps_scope", "priority", "enquire_interval",
    "timeout", "reconnect_max_seconds", "delivery_receipt_mode", "enabled",
}
SAFE_ROUTE_COLUMNS = (
    "id", "name", "host", "port", "source_addr", "bind_type", "pool_size",
    "window_size", "tps", "tps_scope", "priority", "enquire_interval",
    "timeout", "reconnect_max_seconds", "delivery_receipt_mode", "enabled",
)


def _rows(cursor) -> list[dict[str, Any]]:
    columns = [str(item[0]) for item in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _bounded_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _route_contract(row: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    pool_size = _bounded_int(row.get("pool_size"), 1, 1, 20)
    window_size = _bounded_int(row.get("window_size"), 10, 1, 100)
    try:
        tps = float(row.get("tps") or 0)
    except (TypeError, ValueError):
        tps = -1.0
    scope = str(row.get("tps_scope") or "route_shared").strip().lower()
    bind_type = str(row.get("bind_type") or "transceiver").strip().lower()
    if bind_type not in {"receiver", "transmitter", "transceiver"}:
        errors.append(f"route {row.get('id')}: invalid bind_type={bind_type!r}")
    if scope not in {"route_shared", "per_bind"}:
        errors.append(f"route {row.get('id')}: invalid tps_scope={scope!r}")
    if tps < 0:
        errors.append(f"route {row.get('id')}: tps must be >= 0")
    if scope == "per_bind" and bind_type == "receiver":
        errors.append(f"route {row.get('id')}: receiver bind cannot be an outbound per_bind route")
    effective_total = tps * pool_size if scope == "per_bind" else tps
    item = {
        key: row.get(key) for key in SAFE_ROUTE_COLUMNS if key in row
    }
    item.update({
        "pool_size": pool_size,
        "window_size": window_size,
        "tps": tps,
        "tps_scope": scope,
        "tps_unit": "submit_sm_parts/s",
        "effective_total_limit_tps": effective_total,
        "window_total_pdu_capacity": window_size * pool_size,
        "semantics": {
            "pool_size": "physical SMPP binds for this route",
            "window_size": "submit_sm PDUs in flight per physical bind",
            "worker_count": "business-data concurrency; never multiplied into bind or TPS capacity",
            "tps": "submit_sm parts per second",
        },
    })
    return item, errors


def audit_database(db_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(db_path).resolve()
    result: dict[str, Any] = {
        "schema_version": 1,
        "status": "NOT_VERIFIED",
        "database": str(path),
        "secrets_included": False,
        "required_columns": sorted(REQUIRED_COLUMNS),
        "schema_columns": [],
        "routes": [],
        "errors": [],
        "contract": {
            "worker_count": _bounded_int(os.environ.get("MC_VNEXT_WORKERS"), 4, 1, 32),
            "worker_source": "MC_VNEXT_WORKERS",
            "worker_semantics": "business_data_concurrency",
            "bind_semantics": "pool_size physical binds per route",
            "window_semantics": "window_size PDUs per physical bind",
            "tps_semantics": "tps submit_sm_parts/s",
            "tps_scope_default": "route_shared",
            "route_shared_effective_total": "tps",
            "per_bind_effective_total": "tps * pool_size",
        },
    }
    if not path.exists():
        result["errors"] = [f"database does not exist: {path}"]
        return result
    try:
        import duckdb

        conn = duckdb.connect(str(path), read_only=True)
    except Exception as exc:
        result["errors"] = [f"unable to open database read-only: {type(exc).__name__}: {exc}"]
        return result
    try:
        schema_rows = _rows(conn.execute("PRAGMA table_info('smpp_configs')"))
        columns = {str(row.get("name")) for row in schema_rows}
        result["schema_columns"] = sorted(columns)
        missing = sorted(REQUIRED_COLUMNS.difference(columns))
        if missing:
            result["errors"].append(f"smpp_configs missing required columns: {', '.join(missing)}")
            result["status"] = "FAIL"
            return result
        select_columns = ",".join(SAFE_ROUTE_COLUMNS)
        rows = _rows(conn.execute(
            f"SELECT {select_columns} FROM smpp_configs ORDER BY priority,id"
        ))
        for row in rows:
            item, errors = _route_contract(row)
            result["routes"].append(item)
            result["errors"].extend(errors)
        result["status"] = "PASS" if not result["errors"] else "FAIL"
        return result
    except Exception as exc:
        result["errors"].append(f"audit query failed: {type(exc).__name__}: {exc}")
        result["status"] = "FAIL"
        return result
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="DuckDB file to inspect read-only")
    parser.add_argument("--output", default="", help="optional JSON evidence path")
    args = parser.parse_args()
    result = audit_database(args.db)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
