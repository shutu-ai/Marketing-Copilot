import pytest

import database
from models.sms_content import CampaignCreate
from services import operations_service, sms_content_service, sms_task_service


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "template-mapping.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()
    yield tmp_path
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()


def _realtime_activity(conn, name="activity"):
    return conn.execute(
        """INSERT INTO realtime_campaigns(name,status,use_whitelist,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?) RETURNING id""",
        (name, "in_progress", False, ".", "251"),
    ).fetchone()["id"]


def _content_campaign(conn, activity_id, name, enabled=1):
    return conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id,enabled)
           VALUES (?,?,?,?) RETURNING id""",
        (name, "realtime", activity_id, enabled),
    ).fetchone()["id"]


def _template(conn, content_campaign_id, language, content):
    return conn.execute(
        """INSERT INTO sms_templates(campaign_id,language,content)
           VALUES (?,?,?) RETURNING id""",
        (content_campaign_id, language, content),
    ).fetchone()["id"]


def test_template_map_uses_latest_enabled_content_campaign(isolated_db):
    conn = database.get_connection()
    activity_id = _realtime_activity(conn)
    old_content_id = _content_campaign(conn, activity_id, "old-content")
    _template(conn, old_content_id, "English", "OLD {msisdn}")
    new_content_id = _content_campaign(conn, activity_id, "new-content")
    new_template_id = _template(conn, new_content_id, "English", "NEW {msisdn}")
    conn.execute("UPDATE realtime_campaigns SET sms_template_id=? WHERE id=?", (new_template_id, activity_id))
    campaign = dict(conn.execute("SELECT * FROM realtime_campaigns WHERE id=?", (activity_id,)).fetchone())
    conn.commit()

    template_map = sms_task_service._resolve_template_map(conn, "realtime", activity_id, campaign)

    assert template_map["by_lang"]["english"]["id"] == new_template_id
    conn.close()


def test_foreign_template_pointer_is_not_resolved_or_ready(isolated_db):
    conn = database.get_connection()
    owner_activity_id = _realtime_activity(conn, "owner")
    owner_content_id = _content_campaign(conn, owner_activity_id, "owner-content")
    foreign_template_id = _template(conn, owner_content_id, "English", "FOREIGN {msisdn}")
    target_activity_id = _realtime_activity(conn, "target")
    conn.commit()

    template_map = sms_task_service._resolve_template_map(
        conn,
        "realtime",
        target_activity_id,
        {"id": target_activity_id, "sms_template_id": foreign_template_id},
    )
    assert sms_task_service._select_template(template_map, "English") is None

    campaign_dir = isolated_db / "target"
    watch_dir = isolated_db / "watch"
    campaign_dir.mkdir()
    watch_dir.mkdir()
    source = isolated_db / "targets.csv"
    source.write_text("msisdn\n090001\n", encoding="utf-8")
    smpp_id = conn.execute(
        "INSERT INTO smpp_configs(name,host,system_id,password) VALUES (?,?,?,?) RETURNING id",
        ("template-check", "127.0.0.1", "system", "password"),
    ).fetchone()["id"]
    conn.commit()
    errors = operations_service.validate_campaign(
        "realtime",
        {
            "id": target_activity_id,
            "use_whitelist": False,
            "campaign_dir": str(campaign_dir),
            "watch_dir": str(watch_dir),
            "watch_source": "local",
            "watch_filename_prefix": "Android_",
            "customer_list_path": str(source),
            "sms_template_id": foreign_template_id,
            "smpp_config_id": smpp_id,
        },
    )

    assert "no enabled SMS template" in errors
    conn.close()


def test_enabled_activity_content_configuration_cannot_be_duplicated(isolated_db):
    data = CampaignCreate(
        name="first-content",
        marketing_type="realtime",
        marketing_campaign_id=99,
    )
    sms_content_service.create_campaign(data)

    with pytest.raises(ValueError, match="already exists"):
        sms_content_service.create_campaign(
            CampaignCreate(
                name="second-content",
                marketing_type="realtime",
                marketing_campaign_id=99,
            )
        )
