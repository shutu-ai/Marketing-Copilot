"""Authority/projection consistency regressions for realtime ingestion."""

from concurrent.futures import ThreadPoolExecutor

import database
import pytest
from services import ingestion_job_service as jobs
from services import realtime_ingestion_state as state


@pytest.fixture
def authority_file(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DATABASE_FILE", str(tmp_path / "consistency.duckdb"))
    database.init_db()
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    source = campaign_dir / "same.csv"
    source.write_text("msisdn\n090001\n", encoding="utf-8")
    conn = database.get_connection()
    try:
        campaign_id = conn.execute(
            """INSERT INTO realtime_campaigns(name,status,use_whitelist,campaign_dir)
               VALUES ('consistency','in_progress',false,?) RETURNING id""",
            (str(campaign_dir),),
        ).fetchone()["id"]
        conn.commit()
    finally:
        conn.close()
    file_id, duplicate = jobs.register_file(
        "realtime", campaign_id, source.name, str(source),
    )
    assert not duplicate
    claimed = jobs.claim_file("realtime", "consistency-worker")
    assert claimed and claimed["id"] == file_id
    processed_id, _ = state.register_processed(
        campaign_id, str(source), source.name, file_id, claimed["lease_token"],
    )
    return {
        "campaign_id": campaign_id,
        "directory": campaign_dir,
        "path": source,
        "file_id": file_id,
        "job": dict(claimed),
        "processed_id": processed_id,
    }


def _row(file_id):
    conn = database.get_connection()
    try:
        file_row = conn.execute(
            "SELECT * FROM ingestion_files WHERE id=?", (file_id,)
        ).fetchone()
        mirror_row = conn.execute(
            """SELECT * FROM processed_files
               WHERE campaign_type='realtime' AND campaign_id=? AND filename=?""",
            (file_row["campaign_id"], file_row["filename"]),
        ).fetchone()
        return dict(file_row), (dict(mirror_row) if mirror_row else None)
    finally:
        conn.close()


def test_missing_projection_row_does_not_rollback_authority(authority_file):
    item = authority_file
    conn = database.get_connection()
    try:
        conn.execute("DELETE FROM processed_files WHERE id=?", (item["processed_id"],))
        conn.commit()
    finally:
        conn.close()

    progress = state.Progress(
        item["file_id"], item["processed_id"], item["job"]["lease_token"],
    )
    assert progress("matching", processed_rows=7, force=True)
    file_row, mirror_row = _row(item["file_id"])
    assert file_row["processed_rows"] == 7
    assert mirror_row and mirror_row["processed_rows"] == 7

    state.transition(
        item["file_id"], None, item["job"]["lease_token"], "completed",
        rows=1, campaign_id=item["campaign_id"], filename=item["job"]["filename"],
    )
    file_row, mirror_row = _row(item["file_id"])
    assert file_row["status"] == mirror_row["status"] == "completed"
    assert mirror_row["source_state_version"] == file_row["state_version"]


def test_delayed_old_projection_cannot_regress_completed(authority_file):
    item = authority_file
    progress = state.Progress(
        item["file_id"], item["processed_id"], item["job"]["lease_token"],
    )
    assert progress("matching", processed_rows=3, force=True)
    conn = database.get_connection()
    try:
        old_authority = dict(conn.execute(
            "SELECT * FROM ingestion_files WHERE id=?", (item["file_id"],)
        ).fetchone())
    finally:
        conn.close()

    state.transition(
        item["file_id"], None, item["job"]["lease_token"], "completed",
        rows=1, campaign_id=item["campaign_id"], filename=item["job"]["filename"],
    )
    state._project_processed_snapshot(state._projection_snapshot(old_authority))
    file_row, mirror_row = _row(item["file_id"])
    assert file_row["status"] == mirror_row["status"] == "completed"
    assert mirror_row["source_state_version"] == file_row["state_version"]


def test_same_key_projection_upserts_are_single_row(authority_file):
    item = authority_file
    conn = database.get_connection()
    try:
        snapshot = dict(conn.execute(
            "SELECT * FROM ingestion_files WHERE id=?", (item["file_id"],)
        ).fetchone())
    finally:
        conn.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(
            lambda _index: state._project_processed_snapshot(
                state._projection_snapshot(snapshot)
            ),
            range(8),
        ))
    assert all(result == results[0] for result in results)
    conn = database.get_connection()
    try:
        assert conn.execute(
            """SELECT COUNT(*) AS count FROM processed_files
               WHERE campaign_type='realtime' AND campaign_id=? AND filename=?""",
            (item["campaign_id"], item["job"]["filename"]),
        ).fetchone()["count"] == 1
    finally:
        conn.close()


def test_reconcile_repairs_only_the_projection(authority_file):
    item = authority_file
    conn = database.get_connection()
    try:
        before = dict(conn.execute(
            "SELECT status,lease_token,created_tasks FROM ingestion_files WHERE id=?",
            (item["file_id"],),
        ).fetchone())
        conn.execute("DELETE FROM processed_files WHERE id=?", (item["processed_id"],))
        conn.commit()
    finally:
        conn.close()

    checked = state.reconcile_processed_files(file_id=item["file_id"])
    assert checked["issues"]
    repaired = state.reconcile_processed_files(file_id=item["file_id"], repair=True)
    assert repaired["repaired"] and repaired["consistent"]
    conn = database.get_connection()
    try:
        after = dict(conn.execute(
            "SELECT status,lease_token,created_tasks FROM ingestion_files WHERE id=?",
            (item["file_id"],),
        ).fetchone())
    finally:
        conn.close()
    assert after == before


def test_same_name_different_hash_starts_a_new_manifest(authority_file):
    item = authority_file
    jobs.complete_file(item["file_id"])
    item["path"].write_text("msisdn\n090002\n", encoding="utf-8")
    file_id, duplicate = jobs.register_file(
        "realtime", item["campaign_id"], item["path"].name, str(item["path"]),
    )
    assert file_id == item["file_id"]
    assert duplicate is False
    conn = database.get_connection()
    try:
        row = conn.execute(
            "SELECT status,file_hash,state_version FROM ingestion_files WHERE id=?",
            (file_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "pending"
    assert row["file_hash"]
    assert row["state_version"] > 0


def test_invalid_chunk_status_is_preserved_and_fails_closed(authority_file):
    item = authority_file
    conn = database.get_connection()
    try:
        chunk_id = conn.execute(
            """INSERT INTO ingestion_chunks(file_id,chunk_index,start_row,end_row,
               status) VALUES (?,?,?,?,?) RETURNING id""",
            (item["file_id"], 0, 0, 1, "completedcompleted"),
        ).fetchone()["id"]
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError, match="completedcompleted"):
        state.prepare_chunk(item["file_id"], 0, 0, 1, item["job"]["lease_token"])
    conn = database.get_connection()
    try:
        assert conn.execute(
            "SELECT status FROM ingestion_chunks WHERE id=?", (chunk_id,)
        ).fetchone()["status"] == "completedcompleted"
    finally:
        conn.close()
