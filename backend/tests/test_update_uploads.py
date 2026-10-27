import pytest

from services import update_service


def test_create_upload_rejects_reusing_upload_id(tmp_path, monkeypatch):
    monkeypatch.setenv("MC_UPDATE_DIR", str(tmp_path / "updates"))

    first = update_service.create_upload("upd-fixed", "first.tar.gz", 1, "", 1)

    with pytest.raises(ValueError, match="upload_id already exists"):
        update_service.create_upload("upd-fixed", "second.tar.gz", 1, "", 1)

    assert first["filename"] == "first.tar.gz"
    assert update_service.load_meta("upd-fixed")["filename"] == "first.tar.gz"
