"""Executable smoke test for the realtime ingestion optimization path.

This intentionally uses a temporary DuckDB database and exercises the same
public services used by the API and scheduler.
"""

import csv
import shutil
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database
from services import ingestion_job_service, realtime_campaign_service, realtime_whitelist_service


def _write_csv(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["msisdn", "language"])
        writer.writerows(rows)


def _campaign(root: Path, name: str, whitelist_path: str = ""):
    campaign_dir = root / name
    campaign_dir.mkdir()
    conn = database.get_connection()
    campaign_id = conn.execute(
        """INSERT INTO realtime_campaigns
           (name,status,use_whitelist,customer_list_path,campaign_dir,phone_prefix)
           VALUES (?,?,?,?,?,?) RETURNING id""",
        (name, "in_progress", True, whitelist_path, str(campaign_dir), "251"),
    ).fetchone()["id"]
    content_id = conn.execute(
        "INSERT INTO campaigns(name,marketing_type,marketing_campaign_id) VALUES (?,?,?) RETURNING id",
        (name + " content", "realtime", campaign_id),
    ).fetchone()["id"]
    conn.execute(
        "INSERT INTO sms_templates(campaign_id,language,content) VALUES (?,?,?)",
        (content_id, "english", "Hello {msisdn}"),
    )
    conn.commit()
    conn.close()
    return campaign_id, campaign_dir


def main():
    root = BACKEND_DIR / "uploads" / "realtime" / ".rt-optimization-smoke"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    try:
        database.set_database_file(str(root / "smoke.duckdb"))
        database.init_db()

        whitelist = root / "whitelist.csv"
        _write_csv(whitelist, [("090001", "english"), ("090001", "english"), ("090002", "english")])
        campaign_id, campaign_dir = _campaign(root, "versioned", str(whitelist))
        realtime_campaign_service._realtime_chunk_size = lambda: 2
        uploaded = realtime_campaign_service.upload_customer_list(campaign_id, str(whitelist), whitelist.name)
        assert uploaded["whitelist_status"] == "building" and uploaded["row_count"] == 3
        assert realtime_whitelist_service.build_pending() == 1
        version = realtime_whitelist_service.active_version(campaign_id)
        assert version and version["normalized_count"] == 2, version

        source = root / "input.csv"
        _write_csv(source, [("090001", "english"), ("090001", "english"), ("090003", "english")])
        result = realtime_campaign_service.receive_file(campaign_id, str(source), source.name)
        assert result["success"] and result["matched_count"] == 1
        conn = database.get_connection()
        assert conn.execute(
            "SELECT COUNT(*) count FROM sms_tasks WHERE campaign_type='realtime' AND campaign_id=?", (campaign_id,)
        ).fetchone()["count"] == 1
        conn.close()

        legacy_source = root / "legacy.csv"
        _write_csv(legacy_source, [("090010", "english")])
        legacy_id, _ = _campaign(root, "legacy", str(legacy_source))
        assert realtime_whitelist_service.migrate_legacy_versions() == 1
        assert realtime_whitelist_service.build_pending() == 1
        assert realtime_whitelist_service.active_version(legacy_id)

        bound_path = campaign_dir / "queued.csv"
        bound_path.write_text("msisdn\n090001\n", encoding="utf-8")
        file_id, _ = ingestion_job_service.register_file(
            "realtime", campaign_id, bound_path.name, str(bound_path),
            whitelist_version_id=version["id"],
        )
        whitelist_v2 = root / "whitelist-v2.csv"
        _write_csv(whitelist_v2, [("090010", "english")])
        assert realtime_campaign_service.upload_customer_list(campaign_id, str(whitelist_v2), whitelist_v2.name)["whitelist_status"] == "building"
        assert realtime_whitelist_service.build_pending() == 1
        assert realtime_whitelist_service.cleanup_versions(campaign_id)["retired"] == 0
        ingestion_job_service.complete_file(file_id)
        assert realtime_whitelist_service.cleanup_versions(campaign_id)["retired"] == 1

        file_id, _ = ingestion_job_service.register_file("realtime", campaign_id, "lease.csv", str(bound_path))
        owner = ingestion_job_service.claim_file("realtime", "smoke-worker")
        assert owner and owner["id"] == file_id
        assert ingestion_job_service.claim_file("realtime", "second-worker") is None

        print("realtime optimization smoke passed", {"campaign_id": campaign_id, "version_id": version["id"]})
    finally:
        try:
            shutil.rmtree(root)
        except OSError:
            print("smoke temp retained", root)


if __name__ == "__main__":
    main()
