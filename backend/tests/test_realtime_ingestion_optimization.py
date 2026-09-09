import csv
from pathlib import Path
import sys

import pytest
import duckdb

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import ingestion_job_service, realtime_campaign_service, realtime_whitelist_service
from services import realtime_ingestion_retry_service, sms_task_service
from tools import apply_update


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "test.duckdb"))
    database.init_db()
    yield tmp_path


def _campaign(tmp_path):
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns(name,status,use_whitelist,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?) RETURNING id""",
        ("optimized-realtime", "in_progress", True, str(campaign_dir), "251"),
    ).fetchone()["id"]
    conn.commit()
    conn.close()
    return campaign_id, campaign_dir


def test_whitelist_hash_is_built_once_and_reused(isolated_db):
    campaign_id, _campaign_dir = _campaign(isolated_db)
    source = isolated_db / "whitelist.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["msisdn", "language"])
        writer.writerows([["090001", "en"], ["090001", "en"], ["090002", "fr"]])

    first = realtime_campaign_service.upload_customer_list(campaign_id, str(source), source.name)
    assert first["whitelist_status"] == "building"
    assert realtime_whitelist_service.build_pending() == 1
    active = realtime_whitelist_service.active_version(campaign_id)
    assert active and active["normalized_count"] == 2

    second = realtime_campaign_service.upload_customer_list(campaign_id, str(source), source.name)
    assert second["whitelist_version_id"] == active["id"]
    conn = database.get_connection()
    assert conn.execute("SELECT COUNT(*) AS count FROM realtime_whitelist_versions WHERE campaign_id=?", (campaign_id,)).fetchone()["count"] == 1
    conn.close()


def test_vectorized_whitelist_build_preserves_first_language(isolated_db):
    campaign_id, campaign_dir = _campaign(isolated_db)
    source = campaign_dir / "vectorized-whitelist.csv"
    source.write_text(
        "msisdn,language\n090001,first\n+90 001,second\n090002,fr\nabc,ignored\n",
        encoding="utf-8",
    )
    queued = realtime_campaign_service.upload_customer_list(campaign_id, str(source), source.name)
    assert queued["whitelist_status"] == "building"
    assert realtime_whitelist_service.build_pending() == 1
    version = realtime_whitelist_service.active_version(campaign_id)
    assert version and version["row_count"] == 4 and version["normalized_count"] == 2
    index = duckdb.connect(version["index_path"], read_only=True)
    try:
        rows = index.execute(
            "SELECT msisdn,language FROM whitelist_msisdns ORDER BY msisdn"
        ).fetchall()
    finally:
        index.close()
    assert rows == [("90001", "first"), ("90002", "fr")]


def test_corrupt_active_whitelist_rolls_back_before_consuming_rows(isolated_db):
    campaign_id, campaign_dir = _campaign(isolated_db)

    first_source = isolated_db / "whitelist-v1.csv"
    first_source.write_text("msisdn,language\n25190001,en\n", encoding="utf-8")
    first = realtime_campaign_service.upload_customer_list(campaign_id, str(first_source), first_source.name)
    assert realtime_whitelist_service.build_pending() == 1
    version_one = realtime_whitelist_service.active_version(campaign_id)
    assert version_one and version_one["id"] == first["whitelist_version_id"]

    second_source = isolated_db / "whitelist-v2.csv"
    second_source.write_text("msisdn,language\n25190002,fr\n", encoding="utf-8")
    second = realtime_campaign_service.upload_customer_list(campaign_id, str(second_source), second_source.name)
    assert realtime_whitelist_service.build_pending() == 1
    version_two = realtime_whitelist_service.active_version(campaign_id)
    assert version_two and version_two["id"] == second["whitelist_version_id"]
    Path(version_two["index_path"]).write_bytes(b"corrupt whitelist index")

    incoming = isolated_db / "incoming.csv"
    incoming.write_text("msisdn\n90001\n", encoding="utf-8")
    result = realtime_campaign_service.receive_file(
        campaign_id, str(incoming), incoming.name, whitelist_version_id=version_two["id"]
    )
    assert result["success"] is True

    active = realtime_whitelist_service.active_version(campaign_id)
    assert active and active["id"] == version_one["id"]
    conn = database.get_connection()
    try:
        failed = conn.execute(
            "SELECT status,error_detail FROM realtime_whitelist_versions WHERE id=?", (version_two["id"],)
        ).fetchone()
        binding = conn.execute(
            "SELECT whitelist_version_id FROM ingestion_files WHERE campaign_id=? AND filename=?",
            (campaign_id, incoming.name),
        ).fetchone()
    finally:
        conn.close()
    assert failed["status"] == "failed"
    assert "Cannot read whitelist index" in failed["error_detail"]
    assert binding["whitelist_version_id"] == version_one["id"]


def test_online_update_swap_preserves_runtime_configuration_and_data(tmp_path):
    target = tmp_path / "backend"
    new_tree = tmp_path / "payload-backend"
    (target / "data").mkdir(parents=True)
    (target / "uploads" / "realtime" / "whitelist").mkdir(parents=True)
    (target / "logs").mkdir()
    (target / "monitor_dir").mkdir()
    (target / "data" / "marketing_copilot.duckdb").write_text("business database", encoding="utf-8")
    (target / "uploads" / "realtime" / "whitelist" / "active.duckdb").write_text("whitelist index", encoding="utf-8")
    (target / "db_config.json").write_text("{\"path\":\"persistent\"}", encoding="utf-8")
    (target / "logs" / "app.log").write_text("runtime log", encoding="utf-8")
    (target / "monitor_dir" / "watch.state").write_text("runtime monitor", encoding="utf-8")
    (target / "obsolete.py").write_text("old", encoding="utf-8")
    (new_tree / "services").mkdir(parents=True)
    (new_tree / "services" / "new_service.py").write_text("new", encoding="utf-8")

    apply_update.replace_tree(new_tree, target)

    assert (target / "services" / "new_service.py").read_text(encoding="utf-8") == "new"
    assert not (target / "obsolete.py").exists()
    assert (target / "data" / "marketing_copilot.duckdb").read_text(encoding="utf-8") == "business database"
    assert (target / "uploads" / "realtime" / "whitelist" / "active.duckdb").read_text(encoding="utf-8") == "whitelist index"
    assert (target / "db_config.json").read_text(encoding="utf-8") == "{\"path\":\"persistent\"}"
    assert (target / "logs" / "app.log").read_text(encoding="utf-8") == "runtime log"
    assert (target / "monitor_dir" / "watch.state").read_text(encoding="utf-8") == "runtime monitor"


def test_realtime_file_lease_is_single_owner_and_recovers(isolated_db):
    campaign_id, campaign_dir = _campaign(isolated_db)
    source = campaign_dir / "inbox.csv"
    source.write_text("msisdn\n090001\n", encoding="utf-8")
    file_id, duplicate = ingestion_job_service.register_file("realtime", campaign_id, source.name, str(source))
    assert not duplicate
    first = ingestion_job_service.claim_file("realtime", "worker-a")
    assert first and first["id"] == file_id
    assert ingestion_job_service.claim_file("realtime", "worker-b") is None
    conn = database.get_connection()
    conn.execute("UPDATE ingestion_files SET lease_expires_at='2000-01-01 00:00:00' WHERE id=?", (file_id,))
    conn.commit()
    conn.close()
    assert ingestion_job_service.recover_expired_files() == 1
    recovered = ingestion_job_service.claim_file("realtime", "worker-b")
    assert recovered and recovered["worker_id"] == "worker-b"


def test_retry_policy_classifies_and_backs_off(isolated_db):
    waiting = realtime_ingestion_retry_service.classify("whitelist index is still building", "waiting_for_whitelist")
    assert waiting.error_class == "WAITING_DEPENDENCY"
    assert waiting.retryable is True
    assert realtime_ingestion_retry_service.delay_for(waiting, 0) == 60
    invalid = realtime_ingestion_retry_service.classify("ParserError: columns are missing")
    assert invalid.error_class == "INVALID_INPUT"
    assert invalid.quarantine is True
    locked = realtime_ingestion_retry_service.classify("database is locked")
    assert locked.error_class == "TRANSIENT_DB"


def test_sql_normalization_matches_runtime_normalization(isolated_db):
    values = ["090001", "+251 90002", "abc", "", None, "00123", "١٢٣"]
    conn = database.get_connection()
    conn.execute("CREATE TEMP TABLE normalization_fixture(value TEXT)")
    conn.executemany("INSERT INTO normalization_fixture VALUES (?)", [(value,) for value in values])
    sql_values = [row["normalized"] for row in conn.execute(
        """SELECT regexp_replace(regexp_replace(COALESCE(value,''),'[^\\p{Nd}]','','g'),'^0','') normalized
           FROM normalization_fixture ORDER BY rowid"""
    ).fetchall()]
    conn.close()
    assert sql_values == [sms_task_service.normalize_msisdn(value) for value in values]
