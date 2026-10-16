"""Regression tests for startup ordering around DuckDB write contention."""

import pytest
import database

from services import activity_scheduler


def test_startup_recovery_defers_transient_writer_contention(monkeypatch, caplog):
    def locked_recovery():
        raise TimeoutError(
            "DuckDB write lock timeout after 5.0s during execute; "
            "holder=sms_result_batch"
        )

    monkeypatch.setattr(
        activity_scheduler.ingestion_job_service,
        "recover_expired_chunks",
        locked_recovery,
    )

    if database.write_concurrency_mode() == "occ":
        # The legacy Python writer gate does not exist in OCC mode. Its
        # timeout text cannot be reclassified as an OCC write-write conflict.
        with pytest.raises(TimeoutError):
            activity_scheduler._recover_expired_chunks_on_startup()
    else:
        with caplog.at_level("WARNING"):
            assert activity_scheduler._recover_expired_chunks_on_startup() == 0
        assert "Startup ingestion recovery deferred" in caplog.text


def test_startup_recovery_still_raises_non_transient_errors(monkeypatch):
    def broken_recovery():
        raise RuntimeError("corrupt ingestion metadata")

    monkeypatch.setattr(
        activity_scheduler.ingestion_job_service,
        "recover_expired_chunks",
        broken_recovery,
    )

    with pytest.raises(RuntimeError, match="corrupt ingestion metadata"):
        activity_scheduler._recover_expired_chunks_on_startup()


def test_main_replays_results_before_starting_producers(monkeypatch):
    # VNext is the production default.  This test covers the explicitly
    # supported legacy rollback path and therefore opts into it rather than
    # relying on the process default.
    monkeypatch.setenv("MC_PIPELINE_MODE", "legacy")
    import main
    from services import sms_write_buffer, task_worker

    calls = []
    monkeypatch.setattr(
        activity_scheduler,
        "start_scheduler",
        lambda: calls.append("scheduler"),
    )
    monkeypatch.setattr(
        sms_write_buffer,
        "start",
        lambda: calls.append("sms_write_buffer"),
    )
    monkeypatch.setattr(
        sms_write_buffer,
        "flush",
        lambda timeout: calls.append("replay_barrier"),
    )
    monkeypatch.setattr(
        task_worker,
        "start_worker",
        lambda: calls.append("task_worker"),
    )

    main._start_background_services()

    # Durable result replay must finish before either scheduler or sender
    # producers can create new work after a restart.
    assert calls == ["sms_write_buffer", "replay_barrier", "scheduler", "task_worker"]
