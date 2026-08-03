from database import get_connection
from models.sms_content import CampaignCreate, CampaignUpdate, SmsTemplateCreate, SmsTemplateUpdate
from typing import Optional, List
import re
from services import operations_service


VARIABLE_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
COMMON_VARIABLES = {"msisdn", "language", "campaign_name", "event_filter_id", "cell_id"}


def _normalize_language(language: str) -> str:
    return (language or "").strip().lower()


def validate_template(content: str, available_variables=None) -> dict:
    content = content or ""
    placeholders = sorted(set(VARIABLE_PATTERN.findall(content)))
    allowed = COMMON_VARIABLES | set(available_variables or [])
    unknown = [name for name in placeholders if name not in allowed]
    stripped = VARIABLE_PATTERN.sub("", content)
    errors = []
    if "{" in stripped or "}" in stripped:
        errors.append("Template contains unmatched or invalid braces")
    if not content.strip():
        errors.append("Template content cannot be empty")
    return {"valid": not errors, "variables": placeholders, "unknown_variables": unknown,
            "warnings": [f"Variable {{{name}}} is not in the standard variable set" for name in unknown],
            "errors": errors}


def _save_version(conn, template: dict, note: str):
    current = conn.execute("SELECT COALESCE(MAX(version_no),0) max_version FROM template_versions WHERE template_id=?",
                           (template["id"],)).fetchone()["max_version"]
    conn.execute(
        """INSERT INTO template_versions(template_id,version_no,language,content,source_addr,enabled,change_note)
           VALUES (?,?,?,?,?,?,?)""",
        (template["id"], int(current or 0) + 1, template["language"], template["content"],
         template.get("source_addr") or "", template.get("enabled", 1), note))


def get_all_campaigns() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_campaign(campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_campaign_by_marketing(marketing_type: str, marketing_campaign_id: int) -> Optional[dict]:
    conn = get_connection()
    row = conn.execute(
        """SELECT * FROM campaigns
           WHERE marketing_type=? AND marketing_campaign_id=?
           ORDER BY id DESC LIMIT 1""",
        (marketing_type, marketing_campaign_id),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def create_campaign(data: CampaignCreate) -> dict:
    conn = get_connection()
    cursor = conn.execute(
        "INSERT INTO campaigns (name, description, marketing_type, marketing_campaign_id) VALUES (?, ?, ?, ?)",
        (data.name, data.description, data.marketing_type, data.marketing_campaign_id)
    )
    campaign_id = cursor.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return dict(row)


def update_campaign(campaign_id: int, data: CampaignUpdate) -> Optional[dict]:
    conn = get_connection()
    existing = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not existing:
        conn.close()
        return None

    fields = {}
    for field in ["name", "description", "enabled", "marketing_type", "marketing_campaign_id"]:
        val = getattr(data, field, None)
        if val is not None:
            fields[field] = val

    if fields:
        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE campaigns SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
                     (*fields.values(), campaign_id))
        conn.commit()

    row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    conn.close()
    return dict(row)


def _template_usage_count(conn, template_id: int) -> int:
    row = conn.execute(
        """SELECT COUNT(*) AS cnt
           FROM sms_tasks st
           JOIN campaigns c
             ON c.marketing_type=st.campaign_type
            AND c.marketing_campaign_id=st.campaign_id
           JOIN sms_templates t ON t.campaign_id=c.id
           WHERE t.id=?
             AND (
               (
                 st.sms_template_id=t.id
                 AND (
                   (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                   OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                 )
               )
               OR (
                 st.sms_template_id IS NULL
                 AND LOWER(COALESCE(st.language,''))=LOWER(COALESCE(t.language,''))
                 AND (
                   (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                   OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                 )
               )
             )""",
        (template_id,),
    ).fetchone()
    return int(row["cnt"] or 0) if row else 0


def _campaign_template_usage_count(conn, campaign_id: int) -> int:
    row = conn.execute(
        """SELECT COUNT(*) AS cnt
           FROM sms_tasks st
           JOIN campaigns c
             ON c.marketing_type=st.campaign_type
            AND c.marketing_campaign_id=st.campaign_id
           JOIN sms_templates t ON t.campaign_id=c.id
           WHERE c.id=?
             AND (
               (
                 st.sms_template_id=t.id
                 AND (
                   (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                   OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                 )
               )
               OR (
                 st.sms_template_id IS NULL
                 AND LOWER(COALESCE(st.language,''))=LOWER(COALESCE(t.language,''))
                 AND (
                   (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                   OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                 )
               )
             )""",
        (campaign_id,),
    ).fetchone()
    return int(row["cnt"] or 0) if row else 0


def _template_activity_reference_count(conn, template_id: int) -> int:
    row = conn.execute(
        """SELECT
             (SELECT COUNT(*) FROM batch_campaigns WHERE sms_template_id=?) +
             (SELECT COUNT(*) FROM realtime_campaigns WHERE sms_template_id=?) AS cnt""",
        (template_id, template_id),
    ).fetchone()
    return int(row["cnt"] or 0) if row else 0


def _campaign_template_activity_reference_count(conn, campaign_id: int) -> int:
    row = conn.execute(
        """SELECT
             (SELECT COUNT(*)
                FROM batch_campaigns bc
                JOIN sms_templates t ON t.id=bc.sms_template_id
               WHERE t.campaign_id=?) +
             (SELECT COUNT(*)
                FROM realtime_campaigns rc
                JOIN sms_templates t ON t.id=rc.sms_template_id
               WHERE t.campaign_id=?) AS cnt""",
        (campaign_id, campaign_id),
    ).fetchone()
    return int(row["cnt"] or 0) if row else 0


def delete_campaign(campaign_id: int) -> bool:
    conn = get_connection()
    exists = conn.execute("SELECT 1 FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not exists:
        conn.close()
        return False
    if _campaign_template_activity_reference_count(conn, campaign_id) > 0:
        conn.close()
        raise ValueError("短信内容已被营销活动引用，不能删除")
    if _campaign_template_usage_count(conn, campaign_id) > 0:
        conn.close()
        raise ValueError("短信内容已被短信任务引用，不能删除")
    cursor = conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def get_all_templates_with_campaigns() -> list:
    conn = get_connection()
    rows = conn.execute("""
        SELECT t.id AS template_id, t.language, t.content, t.source_addr, t.enabled AS template_enabled,
               c.id AS campaign_id, c.name AS campaign_name, c.description, c.enabled, c.marketing_type, c.marketing_campaign_id,
               c.created_at, c.updated_at,
               (
                 (SELECT COUNT(*) FROM batch_campaigns bc WHERE bc.sms_template_id=t.id) +
                 (SELECT COUNT(*) FROM realtime_campaigns rc WHERE rc.sms_template_id=t.id)
               ) AS activity_reference_count,
               (
                 SELECT COUNT(*)
                 FROM sms_tasks st
                 WHERE st.sms_template_id=t.id
                   AND (
                     (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                     OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                   )
                    OR (
                      st.sms_template_id IS NULL
                      AND st.campaign_type=c.marketing_type
                      AND st.campaign_id=c.marketing_campaign_id
                      AND LOWER(COALESCE(st.language,''))=LOWER(COALESCE(t.language,''))
                      AND (
                        (st.campaign_type='batch' AND EXISTS (SELECT 1 FROM batch_campaigns bc WHERE bc.id=st.campaign_id))
                        OR (st.campaign_type='realtime' AND EXISTS (SELECT 1 FROM realtime_campaigns rc WHERE rc.id=st.campaign_id))
                      )
                    )
               ) AS task_reference_count
        FROM sms_templates t
        JOIN campaigns c ON c.id = t.campaign_id
        ORDER BY c.created_at DESC, t.language
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_templates_by_campaign(campaign_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM sms_templates WHERE campaign_id=? ORDER BY language",
        (campaign_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_template(data: SmsTemplateCreate) -> dict:
    validation = validate_template(data.content)
    if not validation["valid"]:
        raise ValueError("; ".join(validation["errors"]))
    conn = get_connection()
    duplicate = conn.execute(
        "SELECT id FROM sms_templates WHERE campaign_id=? AND LOWER(language)=? LIMIT 1",
        (data.campaign_id, _normalize_language(data.language)),
    ).fetchone()
    if duplicate:
        conn.close()
        raise ValueError("同一活动同一种语言只能配置一条短信内容")
    cursor = conn.execute(
        """INSERT INTO sms_templates (campaign_id, language, content, source_addr)
           VALUES (?,?,?,?)""",
        (data.campaign_id, data.language, data.content, data.source_addr)
    )
    row = conn.execute("SELECT * FROM sms_templates WHERE id=?", (cursor.lastrowid,)).fetchone()
    result = dict(row)
    _save_version(conn, result, "Initial version")
    conn.commit()
    conn.close()
    operations_service.audit("template.create", "sms_template", result["id"], {"variables": validation["variables"]})
    return result


def update_template(template_id: int, data: SmsTemplateUpdate) -> Optional[dict]:
    conn = get_connection()
    existing = conn.execute("SELECT * FROM sms_templates WHERE id=?", (template_id,)).fetchone()
    if not existing:
        conn.close()
        return None

    fields = {}
    for field in ["language", "content", "source_addr", "enabled"]:
        val = getattr(data, field, None)
        if val is not None:
            fields[field] = val

    if fields:
        if "language" in fields:
            duplicate = conn.execute(
                """SELECT id FROM sms_templates
                   WHERE campaign_id=? AND LOWER(language)=? AND id<>?
                   LIMIT 1""",
                (existing["campaign_id"], _normalize_language(fields["language"]), template_id),
            ).fetchone()
            if duplicate:
                conn.close()
                raise ValueError("同一活动同一种语言只能配置一条短信内容")
        content = fields.get("content", dict(existing).get("content", ""))
        validation = validate_template(content)
        if not validation["valid"]:
            conn.close()
            raise ValueError("; ".join(validation["errors"]))
        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE sms_templates SET {set_clause}, updated_at=datetime('now','localtime') WHERE id=?",
                     (*fields.values(), template_id))

    row = conn.execute("SELECT * FROM sms_templates WHERE id=?", (template_id,)).fetchone()
    result = dict(row)
    if fields:
        _save_version(conn, result, "Template updated")
    conn.commit()
    conn.close()
    if fields:
        operations_service.audit("template.update", "sms_template", template_id, {"fields": sorted(fields)})
    return result


def get_template_versions(template_id: int) -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM template_versions WHERE template_id=? ORDER BY version_no DESC",
                        (template_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def restore_template_version(template_id: int, version_no: int) -> Optional[dict]:
    conn = get_connection()
    version = conn.execute("SELECT * FROM template_versions WHERE template_id=? AND version_no=?",
                           (template_id, version_no)).fetchone()
    current = conn.execute("SELECT id FROM sms_templates WHERE id=?", (template_id,)).fetchone()
    if not version or not current:
        conn.close()
        return None
    conn.execute(
        """UPDATE sms_templates SET language=?,content=?,source_addr=?,enabled=?,updated_at=CURRENT_TIMESTAMP
           WHERE id=?""",
        (version["language"], version["content"], version["source_addr"], version["enabled"], template_id))
    restored = dict(conn.execute("SELECT * FROM sms_templates WHERE id=?", (template_id,)).fetchone())
    _save_version(conn, restored, f"Restored from version {version_no}")
    conn.commit(); conn.close()
    operations_service.audit("template.restore", "sms_template", template_id, {"version_no": version_no})
    return restored


def delete_template(template_id: int) -> bool:
    conn = get_connection()
    exists = conn.execute("SELECT 1 FROM sms_templates WHERE id=?", (template_id,)).fetchone()
    if not exists:
        conn.close()
        return False
    if _template_activity_reference_count(conn, template_id) > 0:
        conn.close()
        raise ValueError("短信内容已被营销活动引用，不能删除")
    if _template_usage_count(conn, template_id) > 0:
        conn.close()
        raise ValueError("短信内容已被短信任务引用，不能删除")
    conn.execute("DELETE FROM sms_templates WHERE id=?", (template_id,))
    conn.commit()
    conn.close()
    return True


def get_all_templates_for_select() -> list:
    conn = get_connection()
    rows = conn.execute("""
        SELECT t.id AS template_id, t.language, t.content,
               c.name AS campaign_name, c.marketing_type, c.marketing_campaign_id
        FROM sms_templates t
        JOIN campaigns c ON c.id = t.campaign_id
        WHERE c.enabled=1 AND t.enabled=1
        ORDER BY c.name, t.language
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def preview_template(content: str, variables: dict) -> dict:
    def replace_var(match):
        key = match.group(1)
        return str(variables.get(key, match.group(0)))

    rendered = re.sub(r'\{(\w+)\}', replace_var, content)
    char_count = len(rendered)

    if all(ord(c) < 128 for c in rendered):
        seg_size = 160
    else:
        seg_size = 70

    segments = max(1, (char_count + seg_size - 1) // seg_size)

    return {
        "content": content,
        "variables": variables,
        "rendered": rendered,
        "character_count": char_count,
        "sms_segments": segments,
    }
