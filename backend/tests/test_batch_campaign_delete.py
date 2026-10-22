import pytest

import database
from services import batch_campaign_service, sms_task_service


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "batch-delete.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()
    yield tmp_path
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()


def test_delete_commits_batch_children_before_parent(isolated_db):
    campaign_dir = isolated_db / "Flash_sms_test"
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = int(conn.execute(
        """INSERT INTO batch_campaigns(name,status,campaign_dir)
           VALUES (?,?,?) RETURNING id""",
        ("Flash_sms_test", "ended", str(campaign_dir)),
    ).fetchone()["id"])
    manifest_id = int(conn.execute(
        """INSERT INTO batch_campaign_manifests(campaign_id,status)
           VALUES (?, 'ready') RETURNING id""",
        (campaign_id,),
    ).fetchone()["id"])
    conn.execute(
        """INSERT INTO batch_campaign_batches(campaign_id,manifest_id,status)
           VALUES (?,?,'sent')""",
        (campaign_id, manifest_id),
    )
    conn.execute(
        """INSERT INTO batch_campaign_daily_stats(campaign_id,stats_date)
           VALUES (?, CURRENT_DATE)""",
        (campaign_id,),
    )
    conn.commit()
    conn.close()

    assert batch_campaign_service.delete(campaign_id) is True

    conn = database.get_connection()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS count FROM batch_campaigns WHERE id=?", (campaign_id,)
        ).fetchone()["count"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS count FROM batch_campaign_manifests WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()["count"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS count FROM batch_campaign_batches WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()["count"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS count FROM batch_campaign_daily_stats WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()["count"] == 0
    finally:
        conn.close()
    assert not campaign_dir.exists()
