import json
import logging
from typing import Optional
from database import get_connection
from services.settings_service import get_setting
from services import sms_task_service
from services.smpp_gateway import SmppSubmitError, gateway
from services.sms_task_service import normalize_language_key

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

        templates = [
            dict(row) for row in conn.execute(
                "SELECT * FROM sms_templates WHERE campaign_id=? AND enabled=1 ORDER BY id",
                (campaign["id"],)
            ).fetchall()
        ]
        wanted = normalize_language_key(language)
        template = next(
            (item for item in templates if normalize_language_key(item.get("language", "")) == wanted),
            None,
        )

        if not template:
            logger.warning(f"No template for campaign {campaign_name}, lang {language}")
            for fallback in ("english", "en", "amharic", "tigrigna", "somali", "oromign"):
                template = next(
                    (item for item in templates if normalize_language_key(item.get("language", "")) == fallback),
                    None,
                )
                if template:
                    break

        if not template:
            logger.error(f"No template available for campaign {campaign_name}, msisdn {msisdn}")
            _log_sms(conn, msisdn, campaign_name, language, "", "", "failed",
                     "No available template")
            return

        content = template["content"]
        for key, val in message.items():
            placeholder = "{" + key + "}"
            content = content.replace(placeholder, str(val))

        source_addr = template.get("source_addr") or "Marketing"
        task = sms_task_service.create_external_task(
            campaign_id=campaign["id"], campaign_name=campaign_name,
            msisdn=msisdn, language=language, content=content,
            source_addr=source_addr, payload=message,
        )

        is_dev = get_setting("system_mode", "production") == "development"
        if is_dev:
            sms_task_service.mark_submitted(
                task["id"], [f"DEV-{task['id']}"], "optional",
                submit_sm_parts=1, wait=False,
            )
            sms_success = True
        else:
            sms_success = _send_sms_via_smpp(task)

        status = "sent" if sms_success else "failed"
        error = "" if sms_success else "SMPP send failed"

        _log_sms(conn, msisdn, campaign_name, language, content, source_addr, status, error)
        detail_logger.info(
            "SMS %s campaign=%s:%s msisdn=%s language=%s source_addr=%s content_len=%d content=%s",
            "sent" if sms_success else "failed", campaign.get("id"), campaign_name,
            msisdn, language, source_addr, len(content), content[:1000],
        )

    except Exception as e:
        logger.error(f"SMS processing error for {msisdn}: {e}")
        _log_sms(conn, msisdn, campaign_name, "", "", "", "failed", str(e))
    finally:
        conn.close()


def _send_sms_via_smpp(task: dict) -> bool:
    """Send a legacy RabbitMQ message through the same SMPP gateway as tasks."""
    try:
        message_ids = gateway.send(task)
        sms_task_service.mark_submitted(
            task["id"], message_ids,
            task.get("_delivery_receipt_mode", "optional"),
            transport_smpp_config_id=task.get("_transport_smpp_config_id"),
            submit_sm_parts=task.get("_submit_sm_parts"),
            wait=False,
        )
        return True
    except SmppSubmitError as exc:
        sms_task_service.mark_send_failure(
            task, str(exc), exc.status, retryable=False,
            failure_class="delivery_unknown" if exc.status == -1 else "submit_sm_resp",
            wait=False,
        )
        logger.error("RabbitMQ SMPP send failed for task %s: %s", task["id"], exc)
        return False
    except Exception as e:
        sms_task_service.mark_send_failure(
            task, str(e), retryable=False, failure_class="transport", wait=False,
        )
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
