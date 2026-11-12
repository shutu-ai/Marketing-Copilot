from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

import database
from services import (
    batch_campaign_service,
    ingestion_job_service,
    retention_service,
    sms_write_buffer,
    task_archive_service,
)


@pytest.fixture()
def isolated_db(tmp_path):
    sms_write_buffer.flush()
    database.set_database_file(str(tmp_path / "retention.duckdb"))
    database.init_db()
    yield tmp_path
    sms_write_buffer.flush()


def _old_timestamp() -> str:
    return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")


def test_ingestion_retention_removes_terminal_state_but_keeps_active_source(isolated_db):
    old = _old_timestamp()
    active_source = isolated_db / "active.csv"
    active_source.write_text("msisdn\n251900000001\n", encoding="utf-8")
    conn = database.get_connection()
    conn.execute("INSERT INTO batch_campaigns(id,name,status) VALUES (1,'ended','ended')")
    conn.execute("INSERT INTO batch_campaigns(id,name,status) VALUES (2,'active','in_progress')")
    old_file = conn.execute(
        """INSERT INTO ingestion_files
           (campaign_type,campaign_id,filename,file_path,status,created_at,updated_at,completed_at)
           VALUES ('batch',1,'old.csv','', 'completed',?,?,?) RETURNING id""",
        (old, old, old),
    ).fetchone()["id"]
    conn.execute(
        """INSERT INTO ingestion_chunks(file_id,chunk_index,status,created_at,updated_at,completed_at)
           VALUES (?,?, 'completed',?,?,?)""",
        (old_file, 0, old, old, old),
    )
    conn.execute(
        """INSERT INTO processed_files
           (campaign_type,campaign_id,filename,file_hash,status,detected_at,completed_at)
           VALUES ('batch',1,'old.csv','old','completed',?,?)""",
        (old, old),
    )
    active_file = conn.execute(
        """INSERT INTO ingestion_files
           (campaign_type,campaign_id,filename,file_path,status,created_at,updated_at,completed_at)
           VALUES ('batch',2,'active.csv',?,'completed',?,?,?) RETURNING id""",
        (str(active_source), old, old, old),
    ).fetchone()["id"]
    # Production processed_files rows always carry file_path (operations_service
    # and realtime_ingestion_state both populate it).  Retention keeps an active
    # campaign's row only while that source file still exists, so the row must
    # point at the active source for the keep-branch to be exercised.
    conn.execute(
        """INSERT INTO processed_files
           (campaign_type,campaign_id,filename,file_hash,file_path,status,detected_at,completed_at)
           VALUES ('batch',2,'active.csv','active',?,'completed',?,?)""",
        (str(active_source), old, old),
    )
    conn.commit()
    conn.close()

    result = ingestion_job_service.cleanup_retention(7)
    assert result["ingestion_files"] == 1

    conn = database.get_connection()
    assert conn.execute("SELECT COUNT(*) FROM ingestion_files WHERE id=?", (old_file,)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM ingestion_files WHERE id=?", (active_file,)).fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM processed_files WHERE campaign_id=1").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM processed_files WHERE campaign_id=2").fetchone()[0] == 1
    conn.close()


def test_batch_retention_prunes_old_files_and_marked_orphans(isolated_db):
    campaign_dir = isolated_db / "batch-campaign"
    (campaign_dir / "output").mkdir(parents=True)
    (campaign_dir / "whitelist").mkdir()
    old_output = campaign_dir / "output" / "old.csv"
    new_output = campaign_dir / "output" / "new.csv"
    old_output.write_text("old", encoding="utf-8")
    new_output.write_text("new", encoding="utf-8")
    old_source = campaign_dir / "whitelist" / "old.csv"
    old_source.write_text("old", encoding="utf-8")
    old_epoch = (datetime.now() - timedelta(days=30)).timestamp()
    os.utime(old_output, (old_epoch, old_epoch))
    os.utime(old_source, (old_epoch, old_epoch))
    conn = database.get_connection()
    conn.execute(
        "INSERT INTO batch_campaigns(id,name,status,campaign_dir) VALUES (1,'ended','ended',?)",
        (str(campaign_dir),),
    )
    conn.execute(
        "UPDATE app_settings SET value=? WHERE key='batch_base_dir'",
        (str(isolated_db),),
    )
    conn.commit()
    conn.close()

    result = batch_campaign_service.cleanup_batch_retention()
    assert result["files_removed"] == 2
    assert not old_output.exists()
    assert not old_source.exists()
    assert new_output.exists()
    assert (campaign_dir / retention_service.CAMPAIGN_MARKER).exists()

    orphan = isolated_db / "orphan"
    orphan.mkdir()
    retention_service.ensure_campaign_marker(str(orphan), "batch", 99)
    os.utime(orphan, (old_epoch, old_epoch))
    batch_campaign_service.cleanup_batch_retention()
    assert not orphan.exists()


def test_task_archive_retention_removes_manifest_and_directory(isolated_db):
    old = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    conn = database.get_connection()
    conn.execute(
        """INSERT INTO sms_tasks
           (campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status,task_date,created_at)
           VALUES ('batch',1,'archive','251900000001','x','archive-1','delivered','2000-01-01',?)""",
        (old,),
    )
    conn.execute(
        """INSERT INTO sms_send_attempts
           (attempt_token,task_id,state,created_at,updated_at)
           VALUES ('attempt-archive',1,'accepted',?,?)""",
        (old, old),
    )
    conn.execute(
        """INSERT INTO sms_send_parts
           (attempt_token,part_no,state,updated_at)
           VALUES ('attempt-archive',1,'accepted',?)""",
        (old,),
    )
    conn.execute(
        """INSERT INTO sms_message_bindings_v2
           (source_namespace,smsc_message_id,task_id,attempt_token,part_no,created_at)
           VALUES ('legacy','msg-archive',1,'attempt-archive',1,?)""",
        (old,),
    )
    conn.execute(
        """INSERT INTO sms_result_events(event_id,kind,task_id,created_at)
           VALUES ('event-archive','response',1,?)""",
        (old,),
    )
    conn.execute(
        """INSERT INTO sms_fast_batches
           (batch_id,lease_id,claim_token,state,created_at,updated_at)
           VALUES ('fast-archive','lease-archive','claim-archive','completed',?,?)""",
        (old, old),
    )
    conn.execute(
        """INSERT INTO sms_fast_batch_items
           (batch_id,lease_id,task_id,claim_token,attempt_token,state,created_at,updated_at)
           VALUES ('fast-archive','lease-archive',1,'claim-archive','attempt-archive','completed',?,?)""",
        (old, old),
    )
    conn.commit()
    conn.close()
    archived = task_archive_service.archive_terminal_tasks(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1000
    )
    assert archived["archived"] == 1
    conn = database.get_connection()
    for table in (
        "sms_send_attempts", "sms_send_parts", "sms_message_bindings_v2",
        "sms_result_events", "sms_fast_batches", "sms_fast_batch_items",
    ):
        assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    conn.close()
    result = task_archive_service.cleanup_archives(7)
    assert result["manifests_removed"] == 1
    assert not os.path.exists(archived["root_path"])


def test_history_retention_preserves_unresolved_events(isolated_db):
    old = _old_timestamp()
    conn = database.get_connection()
    conn.execute(
        "INSERT INTO failed_task_archive(task_id,campaign_type,campaign_id,msisdn,final_status,archived_at) VALUES (1,'batch',1,'1','failed',?)",
        (old,),
    )
    conn.execute(
        "INSERT INTO audit_logs(action,created_at) VALUES ('old',?)",
        (old,),
    )
    conn.execute(
        "INSERT INTO sms_log(msisdn,campaign_name,content,created_at) VALUES ('1','old','old',?)",
        (old,),
    )
    conn.execute(
        "INSERT INTO sms_result_events(event_id,kind,task_id,created_at) VALUES ('orphan-event','response',NULL,?)",
        (old,),
    )
    conn.execute(
        "INSERT INTO system_events(category,code,message,resolved,created_at) VALUES ('x','old','old',1,?)",
        (old,),
    )
    conn.execute(
        "INSERT INTO system_events(category,code,message,resolved,created_at) VALUES ('x','open','open',0,?)",
        (old,),
    )
    conn.commit()
    conn.close()

    result = retention_service.cleanup_history(7)
    assert result["failed_task_archive"] == 1
    assert result["audit_logs"] == 1
    assert result["sms_log"] == 1
    assert result["resolved_system_events"] == 1
    conn = database.get_connection()
    assert conn.execute("SELECT COUNT(*) FROM system_events WHERE resolved=0").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM sms_log").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM sms_result_events").fetchone()[0] == 0
    conn.close()
