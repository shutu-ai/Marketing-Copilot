import json
import logging
from typing import Optional
from database import get_connection
from services.settings_service import get_setting

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")


def process_message(routing_key: str, message: dict):
    """
    Called by RabbitMQ consumer when a message arrives.
    Uses the message language to find the campaign template and sends SMS.
    """
    msisdn = str(message.get("msisdn", "")).strip()
    if not msisdn:
        logger.warning(f"No msisdn in message: {message}")
        return

    campaign_name = routing_key.replace("campaign.", "")

    conn = get_connection()
    try:
        language = str(message.get("language", ""))

        campaign = conn.execute(
            "SELECT * FROM campaigns WHERE name=? AND enabled=1 LIMIT 1",
            (campaign_name,)
        ).fetchone()

        if not campaign:
            logger.warning(f"Campaign '{campaign_name}' not found or disabled")
            _log_sms(conn, msisdn, campaign_name, language, "", "", "failed",
                     f"Campaign '{campaign_name}' not found")
            return

        campaign = dict(campaign)

        template = conn.execute(
            "SELECT * FROM sms_templates WHERE campaign_id=? AND language=? AND enabled=1 LIMIT 1",
            (campaign["id"], language)
        ).fetchone()

        if not template:
            logger.warning(f"No template for campaign {campaign_name}, lang {language}")
            if language != "en":
                template = conn.execute(
                    "SELECT * FROM sms_templates WHERE campaign_id=? AND language='en' AND enabled=1 LIMIT 1",
                    (campaign["id"],)
                ).fetchone()

        if not template:
            logger.error(f"No template available for campaign {campaign_name}, msisdn {msisdn}")
            _log_sms(conn, msisdn, campaign_name, language, "", "", "failed",
                     "No available template")
            return

        template = dict(template)

        content = template["content"]
        for key, val in message.items():
            placeholder = "{" + key + "}"
            content = content.replace(placeholder, str(val))

        source_addr = template.get("source_addr") or "Marketing"

        is_dev = get_setting("system_mode", "production") == "development"
        if is_dev:
            sms_success = True
        else:
            sms_success = _send_sms_via_smpp(msisdn, content, source_addr)

        status = "sent" if sms_success else "failed"
        error = "" if sms_success else "SMPP send failed"

        _log_sms(conn, msisdn, campaign_name, language, content, source_addr, status, error)
        detail_logger.info(f"SMS {'sent' if sms_success else 'failed'} to {msisdn} [{language}]: {content[:50]}...")

    except Exception as e:
        logger.error(f"SMS processing error for {msisdn}: {e}")
        _log_sms(conn, msisdn, campaign_name, "", "", "", "failed", str(e))
    finally:
        conn.close()


def _send_sms_via_smpp(msisdn: str, content: str, source_addr: str) -> bool:
    """
    Send SMS via the first enabled SMPP config.
    For now this is a stub that logs the send request.
    When python-smpplib is available, wire it up here.
    """
    conn = get_connection()
    try:
        smpp_config = conn.execute(
            "SELECT * FROM smpp_configs WHERE enabled=1 ORDER BY id ASC LIMIT 1"
        ).fetchone()
        conn.close()

        if smpp_config:
            config = dict(smpp_config)
            detail_logger.info(f"[SMPP SEND] via {config['host']}:{config['port']} "
                               f"to {msisdn} from {source_addr}: {content[:50]}")
        else:
            detail_logger.info(f"[SMS STUB] No SMPP config, simulated send to {msisdn}: {content[:50]}")

        return True
    except Exception as e:
        logger.error(f"SMPP send error: {e}")
        return False


def _log_sms(conn, msisdn: str, campaign_name: str, language: str,
             content: str, source_addr: str, status: str, error_msg: str):
    conn.execute(
        """INSERT INTO sms_log (msisdn, campaign_name, language, content, source_addr, status, error_msg)
           VALUES (?,?,?,?,?,?,?)""",
        (msisdn, campaign_name, language, content, source_addr, status, error_msg)
    )
    conn.commit()


def get_sms_log(page: int = 1, page_size: int = 50, status: Optional[str] = None,
                msisdn: Optional[str] = None) -> dict:
    conn = get_connection()
    conditions = []
    params = []

    if status:
        conditions.append("status=?")
        params.append(status)
    if msisdn:
        conditions.append("msisdn LIKE ?")
        params.append(f"%{msisdn}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_row = conn.execute(f"SELECT COUNT(*) as cnt FROM sms_log {where}", params).fetchone()
    total = count_row["cnt"] if count_row else 0

    offset = (page - 1) * page_size
    rows = conn.execute(
        f"SELECT * FROM sms_log {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [page_size, offset]
    ).fetchall()
    conn.close()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [dict(r) for r in rows],
    }
