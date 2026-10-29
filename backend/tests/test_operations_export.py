from __future__ import annotations

import pytest
from fastapi import HTTPException


def test_task_export_surfaces_unavailable_sidecar_as_503(monkeypatch):
    from routers import operations
    from services import vnext_service

    def unavailable():
        raise vnext_service.TaskIndexUnavailable("vnext_task_index_unavailable")

    monkeypatch.setattr(operations.vnext_service, "export_tasks", unavailable)

    with pytest.raises(HTTPException) as error:
        operations.export_report("tasks")

    assert error.value.status_code == 503
    assert error.value.headers["Retry-After"] == "30"
    assert error.value.detail == {
        "code": "VNEXT_TASK_INDEX_UNAVAILABLE",
        "reason": "vnext_task_index_unavailable",
        "message": "VNext task index is unavailable; rebuild it offline before retrying",
    }
