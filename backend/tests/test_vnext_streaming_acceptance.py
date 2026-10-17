from pathlib import Path

from tools.vnext_streaming_acceptance import _projection_caught_up


class _Projection:
    def __init__(self, status: dict):
        self._status = status

    def snapshot(self) -> dict:
        return dict(self._status)


def test_projection_caught_up_requires_committed_journal_eof(tmp_path: Path):
    journal = tmp_path / "worker-0.jsonl"
    journal.write_text("x" * 100, encoding="ascii")
    status = {
        "alive": True,
        "projection_lag_seconds": 0.0,
        "journal_committed_offset_bytes": 0,
        "journal_backlog_bytes": 0,
        "journal_backlog_events_estimate": 0,
        "journal_pending_events": 0,
        "queue_depth": 0,
    }

    assert not _projection_caught_up(_Projection(status), [journal])

    status["journal_committed_offset_bytes"] = 100
    assert _projection_caught_up(_Projection(status), [journal])

    status["alive"] = False
    assert _projection_caught_up(_Projection(status), [journal])

    status["journal_backlog_events_estimate"] = 1
    assert not _projection_caught_up(_Projection(status), [journal])
