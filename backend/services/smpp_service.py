from database import get_connection
from models.smpp import SmppConfigCreate, SmppConfigUpdate
from typing import List, Optional
import asyncio
import logging

logger = logging.getLogger(__name__)


def _masked(row) -> dict:
    result = dict(row)
    if result.get("password"):
        result["password"] = "********"
    return result


def get_all_configs() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM smpp_configs ORDER BY updated_at DESC").fetchall()
    conn.close()
    return [_masked(r) for r in rows]


def get_config(config_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()
    return _masked(row) if row else None


def create_config(data: SmppConfigCreate) -> dict:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO smpp_configs (name,host,port,system_id,password,source_addr,
           bind_type,address_ton,address_npi,data_coding,pool_size,enquire_interval,timeout,
           tps,priority,reconnect_max_seconds)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (data.name, data.host, data.port, data.system_id, data.password, data.source_addr,
         data.bind_type, data.address_ton, data.address_npi, data.data_coding,
         data.pool_size, data.enquire_interval, data.timeout, data.tps, data.priority,
         data.reconnect_max_seconds)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    return _masked(row)


def update_config(config_id: int, data: SmppConfigUpdate) -> Optional[dict]:
    conn = get_connection()
    existing = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
    if not existing:
        conn.close()
        return None

    fields = {}
    for field in ["name", "host", "port", "system_id", "password", "source_addr",
                   "bind_type", "address_ton", "address_npi", "data_coding",
                   "pool_size", "enquire_interval", "timeout", "tps", "priority",
                   "reconnect_max_seconds", "enabled"]:
        val = getattr(data, field, None)
        if val is not None:
            if field == "password" and val == "********":
                continue
            fields[field] = val

    if not fields:
        conn.close()
        return _masked(existing)

    fields["updated_at"] = "datetime('now','localtime')"
    set_clause = ", ".join(
        f"{k}=?" if k != "updated_at" else f"{k}=datetime('now','localtime')"
        for k in fields
    )
    values = [v for k, v in fields.items() if k != "updated_at"]

    conn.execute(f"UPDATE smpp_configs SET {set_clause} WHERE id=?", (*values, config_id))
    conn.commit()
    row = conn.execute("SELECT * FROM smpp_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()
    return _masked(row)


def delete_config(config_id: int) -> bool:
    conn = get_connection()
    cursor = conn.execute("DELETE FROM smpp_configs WHERE id=?", (config_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


async def test_connection(data: dict) -> dict:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(data["host"], data["port"]),
            timeout=data.get("timeout", 10)
        )
        writer.close()
        await writer.wait_closed()
        return {"success": True, "message": "TCP连接成功 (SMPP协议需进一步验证)"}
    except asyncio.TimeoutError:
        return {"success": False, "message": "连接超时"}
    except ConnectionRefusedError:
        return {"success": False, "message": "连接被拒绝"}
    except Exception as e:
        return {"success": False, "message": str(e)}
