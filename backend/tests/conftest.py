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
