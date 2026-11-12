from pathlib import Path
import sys

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


@pytest.fixture(autouse=True)
def isolated_socket_fences(monkeypatch):
    # Each test can create a new DB whose campaign IDs start at one. A local
    # fence belongs to one owner/database lifetime, not the next test's IDs.
    from services import fast_send_service
    monkeypatch.setattr(fast_send_service, "_fences", {})


@pytest.fixture(autouse=True)
def close_warm_whitelist_index_handles():
    # The shared index opener keeps one warm DuckDB instance per published
    # whitelist file; release those OS handles between tests so tmp_path
    # cleanup works on hosts where removing open files fails.
    yield
    from services import realtime_whitelist_service
    for path in list(realtime_whitelist_service._warm_whitelist_index_connections):
        realtime_whitelist_service.invalidate_warm_index(path)
