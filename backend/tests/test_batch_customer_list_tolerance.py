from pathlib import Path

import pandas as pd
import pytest

import database
from services import batch_campaign_service


@pytest.fixture()
def isolated_db(tmp_path):
    database.set_database_file(str(tmp_path / "batch-upload.duckdb"))
    database.init_db()
    yield tmp_path


def _batch_campaign(tmp_path):
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    conn = database.get_connection()
    try:
        campaign_id = conn.execute(
            """INSERT INTO batch_campaigns
               (name,status,campaign_dir,batch_size,phone_prefix)
               VALUES (?,?,?,?,?) RETURNING id""",
            ("upload-tolerance", "not_started", str(campaign_dir), 2, "251"),
        ).fetchone()["id"]
        conn.commit()
    finally:
        conn.close()
    return campaign_id


def test_batch_reader_ignores_invalid_msisdn_and_malformed_rows(tmp_path):
    source = tmp_path / "targets.txt"
    source.write_text(
        "msisdn;language;region\n"
        "025190000001;english;east\n"
        ";english;east\n"
        "not-a-number;english;east\n"
        "251900000002;english;west;unexpected\n"
        "+251 900000003;english;west\n",
        encoding="utf-8",
    )

    result = batch_campaign_service._validate_customer_list_file(str(source), chunksize=2)

    assert result == {
        "row_count": 2,
        "ignored_rows": 3,
        "ignored_msisdn_rows": 2,
        "ignored_malformed_rows": 1,
    }
    frames = list(batch_campaign_service._iter_customer_list_frames(str(source), chunksize=2))
    assert [row["msisdn"] for frame in frames for row in frame.to_dict("records")] == [
        "25190000001", "251900000003"
    ]


def test_batch_upload_reports_ignored_rows_and_preserves_old_file_on_invalid_upload(isolated_db):
    campaign_id = _batch_campaign(isolated_db)
    valid_source = isolated_db / "valid.csv"
    valid_source.write_text(
        "msisdn,language\n025190000001,english\n,english\nabc,english\n251900000002,english\n",
        encoding="utf-8",
    )

    uploaded = batch_campaign_service.upload_customer_list(
        campaign_id, str(valid_source), valid_source.name,
    )

    assert uploaded["row_count"] == 2
    assert uploaded["ignored_rows"] == 2
    assert uploaded["ignored_msisdn_rows"] == 2
    target_path = Path(uploaded["customer_list_path"])
    assert target_path.is_file()
    assert batch_campaign_service.get_prospects(campaign_id)["total"] == 2

    old_content = target_path.read_bytes()
    invalid_source = isolated_db / "invalid.csv"
    invalid_source.write_text("msisdn,language\n,english\nnot-a-number,english\n", encoding="utf-8")

    with pytest.raises(ValueError, match="no valid MSISDN"):
        batch_campaign_service.upload_customer_list(
            campaign_id, str(invalid_source), invalid_source.name,
        )
    assert target_path.read_bytes() == old_content


def test_batch_reader_supports_xlsx_and_keeps_semantic_errors_strict(tmp_path):
    source = tmp_path / "targets.xlsx"
    pd.DataFrame(
        [
            {"MSISDN": "025190000001", "language": "english"},
            {"MSISDN": "", "language": "english"},
            {"MSISDN": "251900000002", "language": "unsupported"},
        ]
    ).to_excel(source, index=False)

    with pytest.raises(ValueError, match="language .* invalid"):
        batch_campaign_service._validate_customer_list_file(str(source))

    clean_source = tmp_path / "clean.xlsx"
    pd.DataFrame(
        [
            {"MSISDN": "025190000001", "language": "english"},
            {"MSISDN": "", "language": "english"},
        ]
    ).to_excel(clean_source, index=False)
    result = batch_campaign_service._validate_customer_list_file(str(clean_source))
    assert result["row_count"] == 1
    assert result["ignored_msisdn_rows"] == 1
