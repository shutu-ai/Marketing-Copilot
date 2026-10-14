import pytest
import duckdb

import database
from models.batch_campaign import BatchCampaignCreate, BatchCampaignUpdate
from models.realtime_campaign import RealtimeCampaignCreate, RealtimeCampaignUpdate
from services import batch_campaign_service, realtime_campaign_service, sms_task_service


HISTORICAL_STATUSES = ("queued", "sending", "accepted", "unknown", "partial", "failed", "expired")


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "flash-regression.duckdb"))
    database.init_db()
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()
    yield tmp_path
    sms_task_service.invalidate_campaign_cache()
    sms_task_service.invalidate_template_cache()


def _insert_smpp(name: str = "flash-test-smpp") -> int:
    conn = database.get_connection()
    row = conn.execute(
        """INSERT INTO smpp_configs(name,host,system_id,password,enabled)
           VALUES (?,?,?,?,1) RETURNING id""",
        (name, "127.0.0.1", "test", "test"),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


def _insert_activity(kind: str, status: str, smpp_id: int, flash_sms: bool = False) -> int:
    conn = database.get_connection()
    if kind == "batch":
        row = conn.execute(
            """INSERT INTO batch_campaigns
               (name,status,phone_prefix,campaign_dir,smpp_config_id,flash_sms)
               VALUES (?,?,?,?,?,?) RETURNING id""",
            (f"{kind}-{status}", status, "251", "/tmp/flash-test", smpp_id, flash_sms),
        ).fetchone()
    else:
        row = conn.execute(
            """INSERT INTO realtime_campaigns
               (name,status,phone_prefix,campaign_dir,watch_dir,smpp_config_id,flash_sms)
               VALUES (?,?,?,?,?,?,?) RETURNING id""",
            (f"{kind}-{status}", status, "251", "/tmp/flash-test", "/tmp/flash-watch", smpp_id, flash_sms),
        ).fetchone()
    conn.commit()
    conn.close()
    return int(row["id"])


def _insert_tasks(kind: str, campaign_id: int, smpp_id: int, statuses=HISTORICAL_STATUSES):
    conn = database.get_connection()
    rows = [
        (kind, campaign_id, f"251900000{index:03d}", f"body-{index}",
         f"{kind}-{campaign_id}-{index}", smpp_id, status)
        for index, status in enumerate(statuses, 1)
    ]
    conn.executemany(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,msisdn,content,dedupe_key,smpp_config_id,
            flash_sms,status,updated_at)
           VALUES (?,?,?,?,?,?,FALSE,?,TIMESTAMP '2000-01-01 00:00:00')""",
        rows,
    )
    conn.commit()
    conn.close()


def _task_snapshot(kind: str, campaign_id: int):
    conn = database.get_connection()
    rows = conn.execute(
        """SELECT status,flash_sms,smpp_config_id,updated_at
           FROM sms_tasks WHERE campaign_type=? AND campaign_id=? ORDER BY id""",
        (kind, campaign_id),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _flash_update_sql_spy(monkeypatch):
    seen = []
    original = database.WrappedConnection.execute

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split()).lower()
        if "update sms_tasks set flash_sms" in normalized:
            seen.append(normalized)
        return original(self, sql, params)

    monkeypatch.setattr(database.WrappedConnection, "execute", execute)
    return seen


@pytest.mark.parametrize("kind", ["batch", "realtime"])
def test_flash_save_only_changes_campaign_and_keeps_task_snapshots(isolated_db, monkeypatch, kind):
    smpp_id = _insert_smpp(f"{kind}-smpp")
    campaign_id = _insert_activity(kind, "not_started", smpp_id)
    _insert_tasks(kind, campaign_id, smpp_id)
    before = _task_snapshot(kind, campaign_id)
    sql_updates = _flash_update_sql_spy(monkeypatch)

    service = batch_campaign_service if kind == "batch" else realtime_campaign_service
    model = BatchCampaignUpdate if kind == "batch" else RealtimeCampaignUpdate

    updated = service.update(campaign_id, model(flash_sms=True))
    assert updated["flash_sms"] is True
    assert _task_snapshot(kind, campaign_id) == before
    assert sql_updates == []

    # Explicit false and omitted fields are both campaign-only writes.
    updated = service.update(campaign_id, model(flash_sms=False))
    assert updated["flash_sms"] is False
    assert _task_snapshot(kind, campaign_id) == before
    updated = service.update(campaign_id, model(name=f"{kind}-renamed"))
    assert updated["name"] == f"{kind}-renamed"
    assert _task_snapshot(kind, campaign_id) == before
    assert sql_updates == []


def test_realtime_status_matrix_keeps_running_flash_immutable_and_ended_editable(
    isolated_db, monkeypatch
):
    smpp_one = _insert_smpp("realtime-smpp-one")
    smpp_two = _insert_smpp("realtime-smpp-two")

    not_started = _insert_activity("realtime", "not_started", smpp_one)
    assert realtime_campaign_service.update(
        not_started, RealtimeCampaignUpdate(flash_sms=True)
    )["flash_sms"] is True

    in_progress = _insert_activity("realtime", "in_progress", smpp_one)
    _insert_tasks("realtime", in_progress, smpp_one, ("queued",))
    with pytest.raises(ValueError, match="already started"):
        realtime_campaign_service.update(
            in_progress, RealtimeCampaignUpdate(flash_sms=True)
        )
    assert _task_snapshot("realtime", in_progress)[0]["flash_sms"] is False

    # Existing SMPP propagation remains available while Flash SMS stays immutable.
    realtime_campaign_service.update(
        in_progress, RealtimeCampaignUpdate(smpp_config_id=smpp_two)
    )
    task = _task_snapshot("realtime", in_progress)[0]
    assert task["smpp_config_id"] == smpp_two
    assert task["flash_sms"] is False

    ended = _insert_activity("realtime", "ended", smpp_one)
    _insert_tasks("realtime", ended, smpp_one, ("accepted", "unknown"))
    before = _task_snapshot("realtime", ended)
    updated = realtime_campaign_service.update(
        ended,
        RealtimeCampaignUpdate(
            name="realtime-ended-renamed",
            end_time="2030-01-01 00:00:00",
            flash_sms=True,
        ),
    )
    assert updated["name"] == "realtime-ended-renamed"
    assert updated["flash_sms"] is True
    assert _task_snapshot("realtime", ended) == before

    conn = database.get_connection()
    conn.execute("UPDATE realtime_campaigns SET use_whitelist=FALSE WHERE id=?", (ended,))
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        realtime_campaign_service.operations_service,
        "validate_campaign",
        lambda *_args, **_kwargs: [],
    )
    restarted = realtime_campaign_service.update(
        ended, RealtimeCampaignUpdate(status="in_progress")
    )
    assert restarted["status"] == "in_progress"
    assert _task_snapshot("realtime", ended) == before


def test_batch_running_flash_is_immutable_and_ended_flash_is_editable(isolated_db):
    smpp_id = _insert_smpp("batch-status-smpp")
    running = _insert_activity("batch", "in_progress", smpp_id)
    with pytest.raises(ValueError, match="already started"):
        batch_campaign_service.update(running, BatchCampaignUpdate(flash_sms=True))

    ended = _insert_activity("batch", "ended", smpp_id)
    _insert_tasks("batch", ended, smpp_id, ("accepted", "failed"))
    before = _task_snapshot("batch", ended)
    updated = batch_campaign_service.update(ended, BatchCampaignUpdate(flash_sms=True))
    assert updated["flash_sms"] is True
    assert _task_snapshot("batch", ended) == before


def test_create_defaults_and_copy_preserves_flash_flag(isolated_db, monkeypatch, tmp_path):
    batch_root = tmp_path / "batch-root"
    realtime_root = tmp_path / "realtime-root"
    monkeypatch.setattr(batch_campaign_service, "_get_batch_base_dir", lambda: str(batch_root))
    monkeypatch.setattr(realtime_campaign_service, "_get_base_dir", lambda: str(realtime_root))

    batch_default = batch_campaign_service.create(
        BatchCampaignCreate(name="batch-default", phone_prefix="251")
    )
    realtime_default = realtime_campaign_service.create(
        RealtimeCampaignCreate(name="realtime-default", phone_prefix="251")
    )
    assert batch_default["flash_sms"] is False
    assert realtime_default["flash_sms"] is False

    batch_flash = batch_campaign_service.create(
        BatchCampaignCreate(name="batch-flash", phone_prefix="251", flash_sms=True)
    )
    realtime_flash = realtime_campaign_service.create(
        RealtimeCampaignCreate(name="realtime-flash", phone_prefix="251", flash_sms=True)
    )
    assert batch_campaign_service.copy(batch_flash["id"])["flash_sms"] is True
    assert realtime_campaign_service.copy(realtime_flash["id"])["flash_sms"] is True


def test_task_generation_captures_flash_flag_once(isolated_db):
    conn = database.get_connection()
    content_campaign_id = conn.execute(
        """INSERT INTO campaigns(name,marketing_type,marketing_campaign_id)
           VALUES (?,?,?) RETURNING id""",
        ("flash-content", "batch", 1001),
    ).fetchone()["id"]
    template_id = conn.execute(
        """INSERT INTO sms_templates(campaign_id,language,content)
           VALUES (?,?,?) RETURNING id""",
        (content_campaign_id, "English", "Hello {msisdn}"),
    ).fetchone()["id"]
    conn.commit()
    conn.close()

    campaign = {
        "id": 1001,
        "name": "flash-snapshot",
        "status": "in_progress",
        "phone_prefix": "251",
        "sms_template_id": template_id,
        "flash_sms": True,
    }
    first = sms_task_service.enqueue_rows(
        "batch", campaign, [{"msisdn": "0900000001", "language": "English"}],
        source_file="first.csv", validate_schedule=False,
    )
    campaign["flash_sms"] = False
    second = sms_task_service.enqueue_rows(
        "batch", campaign, [{"msisdn": "0900000002", "language": "English"}],
        source_file="second.csv", validate_schedule=False,
    )
    assert first["created"] == 1
    assert second["created"] == 1

    conn = database.get_connection()
    rows = conn.execute(
        "SELECT flash_sms FROM sms_tasks WHERE campaign_id=1001 ORDER BY id"
    ).fetchall()
    conn.close()
    assert [row["flash_sms"] for row in rows] == [True, False]


def test_flash_columns_upgrade_is_idempotent_with_historical_tasks(isolated_db):
    legacy_path = isolated_db / "legacy-before-flash.duckdb"
    legacy = duckdb.connect(str(legacy_path))
    legacy.execute(
        "CREATE TABLE batch_campaigns (id INTEGER PRIMARY KEY, name TEXT, status TEXT)"
    )
    legacy.execute(
        "CREATE TABLE realtime_campaigns (id INTEGER PRIMARY KEY, name TEXT, status TEXT)"
    )
    legacy.execute(
        """CREATE TABLE sms_tasks (
           id BIGINT PRIMARY KEY, campaign_type TEXT, campaign_id INTEGER,
           msisdn TEXT, content TEXT, dedupe_key TEXT UNIQUE, status TEXT,
           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           smsc_message_ids TEXT DEFAULT '[]'
        )"""
    )
    legacy.execute("INSERT INTO batch_campaigns VALUES (1,'legacy-batch','ended')")
    legacy.execute("INSERT INTO realtime_campaigns VALUES (1,'legacy-realtime','ended')")
    historical = [
        (index, "batch", 1, f"2519{index:07d}", f"history-{index}", f"legacy-{index}", status)
        for index, status in enumerate((HISTORICAL_STATUSES * 300), 1)
    ]
    legacy.executemany(
        """INSERT INTO sms_tasks
           (id,campaign_type,campaign_id,msisdn,content,dedupe_key,status)
           VALUES (?,?,?,?,?,?,?)""",
        historical,
    )
    before_statuses = legacy.execute(
        "SELECT status,content FROM sms_tasks ORDER BY id"
    ).fetchall()
    legacy.close()

    database.set_database_file(str(legacy_path))
    database.init_db()
    database.init_db()

    conn = database.get_connection()
    columns = {
        row["column_name"]
        for row in conn.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name IN ('batch_campaigns','realtime_campaigns','sms_tasks')"""
        ).fetchall()
    }
    assert {"flash_sms"} <= columns
    after = conn.execute("SELECT status,content,flash_sms FROM sms_tasks ORDER BY id").fetchall()
    assert [(row["status"], row["content"]) for row in after] == [
        (row[0], row[1]) for row in before_statuses
    ]
    assert all(row["flash_sms"] is False for row in after)

    conn.execute("UPDATE sms_tasks SET flash_sms=NULL WHERE id=1")
    conn.commit()
    conn.close()
    database.init_db()
    conn = database.get_connection()
    null_row = conn.execute("SELECT flash_sms FROM sms_tasks WHERE id=1").fetchone()
    conn.close()
    assert null_row["flash_sms"] is None
    assert not bool(null_row["flash_sms"])
