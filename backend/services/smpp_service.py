from database import get_connection, read_transaction, write_transaction
from models.smpp import SmppConfigCreate, SmppConfigUpdate
from typing import List, Optional
import asyncio
import logging
import threading
import time

logger = logging.getLogger(__name__)

_STATS_CACHE = None
_STATS_CACHE_AT = 0.0
_STATS_CACHE_TTL = 2.0
_STATS_CACHE_LOCK = threading.Lock()


def _invalidate_gateway_config_cache() -> None:
    try:
        from services.smpp_gateway import gateway
        gateway.invalidate_config_cache()
    except Exception:
        logger.debug("Unable to invalidate SMPP gateway config cache", exc_info=True)

_BIND_STATUS_MESSAGES = {
    0x00000003: "命令无效 (ESME_RINVCMDID)",
    0x0000000E: "密码错误 (ESME_RINVPASWD)",
    0x0000000F: "系统ID无效 (ESME_RINVSYSID)",
}


def _masked(row) -> dict:
    result = dict(row)
    if result.get("password"):
        result["password"] = "********"
    return result


def get_all_configs() -> list:
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM smpp_configs ORDER BY updated_at DESC"
        ).fetchall(),
        operation="smpp.configs.list",
    )
    return [_masked(r) for r in rows]


def get_config(config_id: int) -> Optional[dict]:
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM smpp_configs WHERE id=?", (config_id,)
        ).fetchone(),
        operation="smpp.config.get",
    )
    return _masked(row) if row else None


def _get_config_row(config_id: int) -> Optional[dict]:
    """Raw config row including the real password (never exposed via API)."""
    row = read_transaction(
        lambda conn: conn.execute(
            "SELECT * FROM smpp_configs WHERE id=?", (config_id,)
        ).fetchone(),
        operation="smpp.config.raw_get",
    )
    return dict(row) if row else None


def create_config(data: SmppConfigCreate) -> dict:
    def write(conn):
        cursor = conn.execute(
            """INSERT INTO smpp_configs (name,host,port,system_id,password,source_addr,
               bind_type,address_ton,address_npi,data_coding,pool_size,window_size,enquire_interval,timeout,
               tps,priority,reconnect_max_seconds,delivery_receipt_mode)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data.name, data.host, data.port, data.system_id, data.password, data.source_addr,
             data.bind_type, data.address_ton, data.address_npi, data.data_coding,
             data.pool_size, data.window_size, data.enquire_interval, data.timeout, data.tps,
             data.priority, data.reconnect_max_seconds, data.delivery_receipt_mode),
        )
        return dict(conn.execute("SELECT * FROM smpp_configs WHERE id=?", (cursor.lastrowid,)).fetchone())

    row = write_transaction(write, operation="smpp.config.create")
    _invalidate_gateway_config_cache()
    return _masked(row)


def update_config(config_id: int, data: SmppConfigUpdate) -> Optional[dict]:
    fields = {}
    for field in ["name", "host", "port", "system_id", "password", "source_addr",
                   "bind_type", "address_ton", "address_npi", "data_coding",
                   "pool_size", "window_size", "enquire_interval", "timeout", "tps", "priority",
                   "reconnect_max_seconds", "delivery_receipt_mode", "enabled"]:
        val = getattr(data, field, None)
        if val is not None:
            # Empty or masked password means "keep the stored one" on edit.
            if field == "password" and val in ("", "********"):
                continue
            fields[field] = val

    def write(conn):
        existing = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
        if not existing:
            return None
        if not fields:
            return dict(existing)

        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE smpp_configs SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
            (*fields.values(), config_id),
        )
        row = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
        return dict(row)

    row = write_transaction(write, operation="smpp.config.update")
    if row is None:
        return None
    _invalidate_gateway_config_cache()
    return _masked(row)


def delete_config(config_id: int) -> bool:
    def write(conn):
        exists = conn.execute("SELECT 1 FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
        if not exists:
            return False
        conn.execute("DELETE FROM smpp_configs WHERE id=?", (config_id,))
        return True

    deleted = write_transaction(write, operation="smpp.config.delete")
    _invalidate_gateway_config_cache()
    return deleted


def get_config_stats() -> dict:
    """Return a short-lived snapshot for the Web capacity page.

    The page polls frequently, while each uncached request performs several
    aggregate scans over ``sms_tasks``. A two-second cache coalesces refreshes
    without changing operational semantics and keeps reads from competing
    with the SMS write path unnecessarily.
    """
    global _STATS_CACHE, _STATS_CACHE_AT
    now = time.monotonic()
    with _STATS_CACHE_LOCK:
        if _STATS_CACHE is not None and now - _STATS_CACHE_AT < _STATS_CACHE_TTL:
            return _STATS_CACHE
    result = _get_config_stats()
    with _STATS_CACHE_LOCK:
        _STATS_CACHE = result
        _STATS_CACHE_AT = time.monotonic()
    return result


def _get_config_stats() -> dict:
    """Per-SMSC processing statistics based on the sms_tasks table.

    Queue attribution mirrors the gateway routing rules:
      - tasks with an explicit smpp_config_id belong to that config;
      - tasks without one are routed to the first enabled config
        (ORDER BY priority,id), matching SmppGateway.send();
      - when no enabled config exists, NULL-config tasks are counted
        separately as "unassigned".
    """
    conn = get_connection()
    try:
        from services import metrics_service

        rows = conn.execute("SELECT * FROM smpp_configs ORDER BY priority,id").fetchall()
        configs = [dict(r) for r in rows]
        enabled_ids = [c["id"] for c in configs if c.get("enabled")]
        default_target_id = enabled_ids[0] if enabled_ids else None

        def _where(cfg_id: int):
            # The transport id is written only after submit_sm_resp succeeds.
            # Pending tasks still fall back to their selected config, or to the
            # first enabled config when no explicit selection was made.
            target_expression = "COALESCE(transport_smpp_config_id, smpp_config_id, ?)"
            return f"{target_expression}=?", [default_target_id, cfg_id]

        def _count(where: str, params: list, extra: str) -> int:
            row = conn.execute(
                f"SELECT COUNT(*) cnt FROM sms_tasks WHERE {where} AND {extra}", params,
            ).fetchone()
            return int(row["cnt"] or 0)

        def _window_count(where: str, params: list, column: str, minutes: int) -> int:
            return _count(
                where, params,
                f"CAST({column} AS TIMESTAMP WITH TIME ZONE) >= CURRENT_TIMESTAMP - INTERVAL '{minutes} minutes'",
            )

        def _window_parts(where: str, params: list, minutes: int) -> int:
            row = conn.execute(
                f"""SELECT COALESCE(SUM(GREATEST(1,COALESCE(submit_sm_parts,0))),0) parts
                    FROM sms_tasks WHERE {where}
                    AND CAST(submitted_at AS TIMESTAMP WITH TIME ZONE) >=
                        CURRENT_TIMESTAMP - INTERVAL '{minutes} minutes'""",
                params,
            ).fetchone()
            return int(row["parts"] or 0)

        def _transport_count(event_name: str, cfg_id: int, pool_size: int, result=None) -> int:
            """Read transport events without adding a DuckDB hot-path query."""
            total = 0.0
            for slot in range(1, max(1, int(pool_size or 1)) + 1):
                labels = {"config_id": cfg_id, "slot": slot}
                if result is not None:
                    labels["result"] = result
                total += metrics_service.rolling_count(event_name, labels, 60.0)
            return int(round(total))

        stats = []
        for cfg in configs:
            cfg_id = cfg["id"]
            where, params = _where(cfg_id)
            submitted_1m = _window_count(where, params, "submitted_at", 1)
            submitted_5m = _window_count(where, params, "submitted_at", 5)
            submitted_60m = _window_count(where, params, "submitted_at", 60)
            submitted_parts_1m = _window_parts(where, params, 1)
            submitted_parts_5m = _window_parts(where, params, 5)
            submitted_parts_60m = _window_parts(where, params, 60)
            delivered_60m = _count(
                where, params,
                "status IN ('delivered','accepted') AND "
                "CAST(COALESCE(delivered_at, submitted_at, updated_at) AS TIMESTAMP WITH TIME ZONE) >= "
                "CURRENT_TIMESTAMP - INTERVAL '60 minutes'",
            )
            failed_60m = _count(
                where, params,
                "status='failed' AND CAST(updated_at AS TIMESTAMP WITH TIME ZONE) >= CURRENT_TIMESTAMP - INTERVAL '60 minutes'",
            )
            terminal_60m = delivered_60m + failed_60m
            send_message_1m = _transport_count(
                "marketing_copilot_smpp_submit_sm_sent", cfg_id, cfg.get("pool_size", 1),
            )
            submit_sm_resp_success_1m = _transport_count(
                "marketing_copilot_smpp_submit_sm_resp", cfg_id, cfg.get("pool_size", 1), "success",
            )
            submit_sm_resp_failed_1m = _transport_count(
                "marketing_copilot_smpp_submit_sm_resp", cfg_id, cfg.get("pool_size", 1), "failure",
            )
            submit_sm_resp_1m = submit_sm_resp_success_1m + submit_sm_resp_failed_1m
            stats.append({
                "config_id": cfg_id,
                "name": cfg.get("name", ""),
                "host": cfg.get("host", ""),
                "port": cfg.get("port", 0),
                "enabled": cfg.get("enabled", 0),
                "priority": cfg.get("priority", 100),
                "tps": cfg.get("tps", 0),
                "pool_size": cfg.get("pool_size", 1),
                "window_size": cfg.get("window_size", 10),
                # The gateway reserves one shared route clock per config.
                # Keep the scope explicit so pool_size is not mistaken for a
                # multiplication of the SMSC/account allowance.
                "configured_route_tps": float(cfg.get("tps") or 0) if cfg.get("enabled") else 0.0,
                "configured_per_bind_tps": None,
                "effective_total_limit_tps": float(cfg.get("tps") or 0) if cfg.get("enabled") else 0.0,
                "tps_scope": "route_shared",
                "tps_unit": "submit_sm_parts/s",
                "queue_size": _count(where, params, "status IN ('queued','retrying')"),
                "sending": _count(where, params, "status='sending'"),
                "submitted": _count(where, params, "status='submitted'"),
                "submitted_1m": submitted_1m,
                "submitted_5m": submitted_5m,
                "submitted_60m": submitted_60m,
                "submitted_parts_1m": submitted_parts_1m,
                "submitted_parts_5m": submitted_parts_5m,
                "submitted_parts_60m": submitted_parts_60m,
                "submit_sm_tps_1m": round(submitted_parts_1m / 60.0, 2),
                # Transport counters are independent from the durable task
                # state.  They expose where the current minute is spent:
                # socket send versus submit_sm_resp completion.
                "send_message_1m": send_message_1m,
                "send_message_tps_1m": round(send_message_1m / 60.0, 2),
                "submit_sm_resp_1m": submit_sm_resp_1m,
                "submit_sm_resp_success_1m": submit_sm_resp_success_1m,
                "submit_sm_resp_failed_1m": submit_sm_resp_failed_1m,
                "submit_sm_resp_tps_1m": round(submit_sm_resp_1m / 60.0, 2),
                "delivered_60m": delivered_60m,
                "failed_60m": failed_60m,
                "failure_rate_60m": round(failed_60m / terminal_60m, 4) if terminal_60m else 0.0,
                "today_submitted": _count(
                    where, params,
                    "CAST(submitted_at AS TIMESTAMP WITH TIME ZONE) >= CAST(CAST(CURRENT_DATE AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)",
                ),
                "today_delivered": _count(
                    where, params,
                    "status IN ('delivered','accepted') AND "
                    "CAST(COALESCE(delivered_at, submitted_at, updated_at) AS TIMESTAMP WITH TIME ZONE) >= "
                    "CAST(CAST(CURRENT_DATE AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)",
                ),
                "today_failed": _count(
                    where, params,
                    "status='failed' AND CAST(updated_at AS TIMESTAMP WITH TIME ZONE) >= CAST(CAST(CURRENT_DATE AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)",
                ),
            })

        unassigned = 0
        if not default_target_id:
            unassigned = _count(
                "transport_smpp_config_id IS NULL AND smpp_config_id IS NULL", [],
                "status IN ('queued','retrying')",
            )

        summary = {
            "queue_size": sum(item["queue_size"] for item in stats),
            "sending": sum(item["sending"] for item in stats),
            "submitted": sum(item["submitted"] for item in stats),
            "submitted_1m": sum(item["submitted_1m"] for item in stats),
            "submitted_5m": sum(item["submitted_5m"] for item in stats),
            "submitted_60m": sum(item["submitted_60m"] for item in stats),
            "submitted_parts_1m": sum(item["submitted_parts_1m"] for item in stats),
            "submitted_parts_5m": sum(item["submitted_parts_5m"] for item in stats),
            "submitted_parts_60m": sum(item["submitted_parts_60m"] for item in stats),
            "configured_route_tps": sum(item["configured_route_tps"] for item in stats),
            "configured_per_bind_tps": None,
            "effective_total_limit_tps": sum(item["effective_total_limit_tps"] for item in stats),
            "tps_scope": "route_shared",
            "tps_unit": "submit_sm_parts/s",
            "submit_sm_tps_1m": round(sum(item["submitted_parts_1m"] for item in stats) / 60.0, 2),
            "send_message_1m": sum(item["send_message_1m"] for item in stats),
            "send_message_tps_1m": round(sum(item["send_message_1m"] for item in stats) / 60.0, 2),
            "submit_sm_resp_1m": sum(item["submit_sm_resp_1m"] for item in stats),
            "submit_sm_resp_success_1m": sum(item["submit_sm_resp_success_1m"] for item in stats),
            "submit_sm_resp_failed_1m": sum(item["submit_sm_resp_failed_1m"] for item in stats),
            "submit_sm_resp_tps_1m": round(sum(item["submit_sm_resp_1m"] for item in stats) / 60.0, 2),
            "delivered_60m": sum(item["delivered_60m"] for item in stats),
            "failed_60m": sum(item["failed_60m"] for item in stats),
            "today_submitted": sum(item["today_submitted"] for item in stats),
            "today_delivered": sum(item["today_delivered"] for item in stats),
            "today_failed": sum(item["today_failed"] for item in stats),
            "unassigned": unassigned,
        }
        return {"summary": summary, "items": stats}
    finally:
        conn.close()


def _run_smpp_bind_test(data: dict) -> dict:
    """Connect and perform a real SMPP bind so credentials are actually checked."""
    import smpplib.client
    import smpplib.exceptions

    client = None
    try:
        client = smpplib.client.Client(
            data["host"], int(data["port"]),
            timeout=data.get("timeout", 10),
            allow_unknown_opt_params=True,
        )
        client.logger.disabled = True
        client.connect()
        bind_type = data.get("bind_type") or "transceiver"
        bind_func = getattr(client, f"bind_{bind_type}", client.bind_transceiver)
        bind_func(system_id=data.get("system_id") or "", password=data.get("password") or "")
        return {"success": True, "message": "SMPP 连接成功，账号密码验证通过"}
    except smpplib.exceptions.PDUError as exc:
        status = exc.args[1] if len(exc.args) > 1 else None
        detail = _BIND_STATUS_MESSAGES.get(status) or str(exc)
        return {"success": False, "message": f"SMPP 绑定失败: {detail}"}
    except smpplib.exceptions.ConnectionError as exc:
        message = str(exc)
        if "refused" in message.lower():
            return {"success": False, "message": "连接被拒绝"}
        return {"success": False, "message": message or "连接超时"}
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        if client is not None:
            try:
                client.unbind()
            except Exception:
                pass
            client.disconnect()


async def test_connection(data: dict) -> dict:
    config_id = data.get("config_id")
    if config_id:
        config = _get_config_row(config_id)
        if not config:
            return {"success": False, "message": "配置不存在"}
        # Use the real stored credentials so a masked UI value ("********")
        # is never sent to the SMSC as the password.
        data = {
            **data,
            "host": config["host"],
            "port": config["port"],
            "system_id": config["system_id"],
            "password": config["password"],
            "bind_type": config.get("bind_type") or "transceiver",
            "timeout": config.get("timeout") or data.get("timeout", 10),
        }
    try:
        result = await asyncio.to_thread(_run_smpp_bind_test, data)
        if result.get("success") and config_id:
            from services.smpp_gateway import gateway
            try:
                await asyncio.to_thread(gateway.recover, int(config_id))
            except Exception as exc:
                logger.exception("SMPP test succeeded but live gateway recovery failed")
                result["gateway_recovery"] = {"success": False, "message": str(exc)}
            else:
                result["gateway_recovery"] = {"success": True}
        return result
    except Exception as exc:
        logger.exception("SMPP connection test failed")
        return {"success": False, "message": str(exc)}
