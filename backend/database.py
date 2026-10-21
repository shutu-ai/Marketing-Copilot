import os
import faulthandler
import json
import logging
import random
import threading
import time
from contextlib import contextmanager

import duckdb

from duckdb_config import duckdb_extension_config, prepare_offline_extensions

logger = logging.getLogger(__name__)

# ``_write_lock`` is the compatibility/fallback gate for the default
# ``serialized`` mode. It is deliberately not used for normal DML when
# ``MC_DUCKDB_WRITE_CONCURRENCY=occ``. The object remains for diagnostics and
# for the safe rollback path while the opt-in mode is being validated.
_write_lock = threading.RLock()
_write_state_lock = threading.Lock()
_write_holder = {
    "thread_id": None,
    "thread_name": "",
    "operation": "",
    "acquired_at": 0.0,
}
_stall_watchdog_stop = threading.Event()
_stall_watchdog_thread = None
_stall_watchdog_dir = ""
_stall_watchdog_threshold = 30.0
_stall_watchdog_emitted = False
_stall_watchdog_started_at = 0.0
_stall_watchdog_stack_handle = None
_stall_watchdog_dump_lock = threading.Lock()

_WRITE_CONCURRENCY_MODES = {"serialized", "occ"}
_WRITE_CONCURRENCY = str(
    os.environ.get("MC_DUCKDB_WRITE_CONCURRENCY", "serialized")
).strip().lower()
if _WRITE_CONCURRENCY not in _WRITE_CONCURRENCY_MODES:
    logger.warning(
        "Unsupported MC_DUCKDB_WRITE_CONCURRENCY=%r; using serialized",
        _WRITE_CONCURRENCY,
    )
    _WRITE_CONCURRENCY = "serialized"

# The process mode is read at import time and becomes immutable after the
# first coordinator use. A running owner must never switch storage semantics
# without an explicit process restart.
_WRITE_CONCURRENCY_FROZEN = False

try:
    _TRANSACTION_ADMISSION_TIMEOUT_SECONDS = max(
        0.1,
        float(os.environ.get("MC_DUCKDB_TRANSACTION_ADMISSION_TIMEOUT_SECONDS", "5")),
    )
except (TypeError, ValueError):
    _TRANSACTION_ADMISSION_TIMEOUT_SECONDS = 5.0

try:
    _OCC_RETRY_JITTER_SECONDS = max(
        0.0,
        float(os.environ.get("MC_DUCKDB_OCC_RETRY_JITTER_MS", "25")) / 1000.0,
    )
except (TypeError, ValueError):
    _OCC_RETRY_JITTER_SECONDS = 0.025


class _AdmissionToken:
    """One tracked transaction admission held by a WrappedConnection."""

    __slots__ = ("_coordinator", "operation", "read_only", "acquired_at", "released")

    def __init__(self, coordinator, operation: str, read_only: bool, acquired_at: float):
        self._coordinator = coordinator
        self.operation = str(operation or "transaction")
        self.read_only = bool(read_only)
        self.acquired_at = float(acquired_at)
        self.released = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self._coordinator._release(self)


class TransactionCoordinator:
    """Coordinate admission without serializing ordinary OCC DML.

    Shared admission counts every connection-level read/write transaction so
    maintenance can establish a real quiescent boundary. In ``occ`` mode it
    is a condition-variable counter, not a writer mutex. The serialized
    compatibility mode still uses the historical Python writer gate for
    normal writes.
    """

    def __init__(self):
        self._condition = threading.Condition(threading.RLock())
        self._active = 0
        self._active_by_operation = {}
        self._active_started = {}
        self._maintenance_requested = False
        self._maintenance_active = False
        self._maintenance_operation = ""
        self._maintenance_started = 0.0
        self._maintenance_wait_started = 0.0
        self._local = threading.local()

    @property
    def mode(self) -> str:
        return _WRITE_CONCURRENCY

    def _mark_mode_used(self) -> None:
        global _WRITE_CONCURRENCY_FROZEN
        _WRITE_CONCURRENCY_FROZEN = True

    def _deadline(self, timeout=None, deadline=None) -> float | None:
        if deadline is not None:
            try:
                return float(deadline)
            except (TypeError, ValueError):
                raise ValueError("transaction deadline must be a monotonic timestamp")
        if timeout is None:
            timeout = _TRANSACTION_ADMISSION_TIMEOUT_SECONDS
        return time.monotonic() + max(0.0, float(timeout))

    def _wait_seconds(self, deadline: float | None, operation: str) -> float:
        if deadline is None:
            return 0.0
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            status = self.status()
            _inc_database_metric(
                "marketing_copilot_database_transaction_admission_timeouts_total",
                labels={"operation": str(operation), "mode": self.mode},
            )
            raise TimeoutError(
                f"DuckDB transaction admission timeout during {operation}; "
                f"active={status['active_transactions']} "
                f"maintenance={status['maintenance_requested']}"
            )
        return remaining

    def acquire(self, operation: str = "transaction", *, read_only: bool = False,
                timeout=None, deadline=None) -> _AdmissionToken:
        self._mark_mode_used()
        operation = str(operation or "transaction")
        started = time.perf_counter()
        deadline_at = self._deadline(timeout, deadline)
        with self._condition:
            while self._maintenance_requested or self._maintenance_active:
                self._condition.wait(self._wait_seconds(deadline_at, operation))
            acquired_at = time.monotonic()
            token = _AdmissionToken(self, operation, read_only, acquired_at)
            self._active += 1
            self._active_by_operation[operation] = self._active_by_operation.get(operation, 0) + 1
            self._active_started[id(token)] = (acquired_at, operation)
            self._local.admission_depth = getattr(self._local, "admission_depth", 0) + 1
        waited = time.perf_counter() - started
        _observe_database_metric(
            "marketing_copilot_database_transaction_admission_wait_seconds",
            waited,
            labels={"operation": operation, "mode": self.mode},
        )
        _observe_database_metric(
            "marketing_copilot_database_transaction_wait_seconds",
            waited,
            labels={"operation": operation, "mode": self.mode},
        )
        return token

    def _release(self, token: _AdmissionToken) -> None:
        with self._condition:
            self._active = max(0, self._active - 1)
            count = self._active_by_operation.get(token.operation, 0)
            if count <= 1:
                self._active_by_operation.pop(token.operation, None)
            else:
                self._active_by_operation[token.operation] = count - 1
            self._active_started.pop(id(token), None)
            self._condition.notify_all()
            depth = getattr(self._local, "admission_depth", 0)
            self._local.admission_depth = max(0, depth - 1)
        _observe_database_metric(
            "marketing_copilot_database_transaction_hold_seconds",
            max(0.0, time.monotonic() - token.acquired_at),
            labels={"operation": token.operation, "mode": self.mode},
        )

    @contextmanager
    def transaction(self, kind: str = "transaction", deadline=None,
                    isolation: str = "occ", read_only: bool = False):
        """Admit a DB transaction for explicit coordinator users.

        The yielded value is an admission token. Application code should use
        ``write_transaction``/``read_transaction`` when it also needs a
        connection lifecycle; this lower-level API is useful for maintenance
        aware integrations and correctness harnesses.
        """
        del isolation  # DuckDB isolation is selected by the process mode.
        token = self.acquire(kind, read_only=read_only, deadline=deadline)
        try:
            yield token
        finally:
            token.release()

    @contextmanager
    def maintenance_exclusive(self, kind: str = "maintenance", deadline=None,
                              timeout=None):
        """Stop new admissions and drain active transactions before maintenance."""
        self._mark_mode_used()
        operation = str(kind or "maintenance")
        if getattr(self._local, "admission_depth", 0):
            raise RuntimeError(
                f"maintenance admission cannot be upgraded from an active transaction: {operation}"
            )
        started = time.perf_counter()
        deadline_at = self._deadline(timeout, deadline)
        with self._condition:
            while self._maintenance_requested or self._maintenance_active:
                self._condition.wait(self._wait_seconds(deadline_at, operation))
            self._maintenance_requested = True
            self._maintenance_operation = operation
            self._maintenance_wait_started = time.monotonic()
            try:
                while self._active:
                    self._condition.wait(self._wait_seconds(deadline_at, operation))
                self._maintenance_active = True
                self._maintenance_started = time.monotonic()
            except BaseException:
                self._maintenance_requested = False
                self._maintenance_operation = ""
                self._maintenance_wait_started = 0.0
                self._condition.notify_all()
                raise
        _observe_database_metric(
            "marketing_copilot_database_maintenance_wait_seconds",
            time.perf_counter() - started,
            labels={"operation": operation, "mode": self.mode},
        )
        try:
            yield
        finally:
            with self._condition:
                self._maintenance_active = False
                self._maintenance_requested = False
                self._maintenance_operation = ""
                self._maintenance_started = 0.0
                self._maintenance_wait_started = 0.0
                self._condition.notify_all()

    def status(self) -> dict:
        with self._condition:
            active = int(self._active)
            by_operation = dict(self._active_by_operation)
            maintenance_requested = bool(self._maintenance_requested)
            maintenance_active = bool(self._maintenance_active)
            operation = self._maintenance_operation
            maintenance_started = self._maintenance_started
            wait_started = self._maintenance_wait_started
            oldest = min(
                (started for started, _op in self._active_started.values()),
                default=0.0,
            )
        now = time.monotonic()
        return {
            "mode": self.mode,
            "active_transactions": active,
            "active_by_operation": by_operation,
            "oldest_transaction_age_seconds": round(
                max(0.0, now - oldest) if oldest else 0.0, 3
            ),
            "maintenance_requested": maintenance_requested,
            "maintenance_active": maintenance_active,
            "maintenance_operation": operation,
            "maintenance_age_seconds": round(
                max(0.0, now - maintenance_started) if maintenance_started else 0.0, 3
            ),
            "maintenance_wait_seconds": round(
                max(0.0, now - wait_started)
                if wait_started and not maintenance_active else 0.0,
                3,
            ),
        }


coordinator = TransactionCoordinator()


@contextmanager
def maintenance_exclusive(kind: str = "maintenance", deadline=None, timeout=None):
    """Public maintenance boundary used by backup, schema and update paths."""
    with coordinator.maintenance_exclusive(kind, deadline=deadline, timeout=timeout):
        yield


def write_concurrency_mode() -> str:
    """Return the immutable process-level DuckDB write mode."""
    return _WRITE_CONCURRENCY


def configure_write_concurrency(mode: str) -> str:
    """Configure the mode before first use; running processes cannot switch it."""
    global _WRITE_CONCURRENCY
    normalized = str(mode or "").strip().lower()
    if normalized not in _WRITE_CONCURRENCY_MODES:
        raise ValueError("DuckDB write concurrency mode must be serialized or occ")
    if _WRITE_CONCURRENCY_FROZEN:
        raise RuntimeError("DuckDB write concurrency mode is frozen for this process")
    _WRITE_CONCURRENCY = normalized
    return _WRITE_CONCURRENCY

try:
    _WRITE_LOCK_TIMEOUT_SECONDS = max(
        0.5, float(os.environ.get("MC_DUCKDB_WRITE_LOCK_TIMEOUT_SECONDS", "5"))
    )
except (TypeError, ValueError):
    _WRITE_LOCK_TIMEOUT_SECONDS = 5.0

try:
    _READ_RETRIES = max(1, min(5, int(os.environ.get("MC_DUCKDB_READ_RETRIES", "2"))))
except (TypeError, ValueError):
    _READ_RETRIES = 2

try:
    _READ_RETRY_DELAY_SECONDS = max(
        0.0, float(os.environ.get("MC_DUCKDB_READ_RETRY_DELAY_MS", "50")) / 1000.0
    )
except (TypeError, ValueError):
    _READ_RETRY_DELAY_SECONDS = 0.05

_READ_ONLY_STATEMENTS = {"SELECT", "EXPLAIN", "DESCRIBE", "SHOW"}


def _env_bool(value, default=False):
    if value is None:
        return bool(default)
    return str(value).strip().lower() not in {"0", "false", "off", "no", "disabled"}


def _stall_watchdog_reason(status: dict, *, now=None):
    """Return a no-progress reason without treating run age as a stall.

    A healthy long-running process has no reason to be classified as stalled.
    The only currently supported no-progress signal is a write admission held
    beyond the diagnostic threshold; wall-clock age is recorded separately as
    a scheduled snapshot and is never a failure classification.
    """

    if not status.get("held"):
        return None
    try:
        age_seconds = float(status.get("age_seconds") or 0.0)
    except (TypeError, ValueError):
        age_seconds = 0.0
    if age_seconds >= float(_stall_watchdog_threshold):
        return "no_progress_stall"
    return None


def start_stall_watchdog(directory=None, threshold_seconds=30.0, enabled=None):
    """Start opt-in thread-stack capture for a local acceptance run.

    The watchdog is deliberately disabled unless the caller supplies a
    directory.  It never touches DuckDB and therefore remains useful when a
    writer gate or DuckDB connection is the thing that is stalled.
    """
    global _stall_watchdog_thread, _stall_watchdog_dir
    global _stall_watchdog_threshold, _stall_watchdog_emitted
    global _stall_watchdog_started_at
    global _stall_watchdog_stack_handle
    if enabled is None:
        enabled = os.environ.get("MC_STALL_WATCHDOG_ENABLED", "1" if directory else "0")
    if not directory or not _env_bool(enabled):
        return False
    if _stall_watchdog_thread and _stall_watchdog_thread.is_alive():
        return True
    _stall_watchdog_dir = os.fspath(directory)
    _stall_watchdog_threshold = max(1.0, float(threshold_seconds or 30.0))
    os.makedirs(_stall_watchdog_dir, exist_ok=True)
    _stall_watchdog_emitted = False
    _stall_watchdog_started_at = time.monotonic()
    _stall_watchdog_stop.clear()
    try:
        stack_path = os.path.join(_stall_watchdog_dir, "application-threads-timeout.txt")
        _stall_watchdog_stack_handle = open(stack_path, "w", encoding="utf-8")
        # The faulthandler timeout is independent of the Python watchdog
        # thread and can still dump stacks when a C extension is holding the
        # GIL.  That is exactly the failure mode this diagnostic is meant to
        # expose.
        faulthandler.dump_traceback_later(
            int(_stall_watchdog_threshold),
            repeat=False,
            file=_stall_watchdog_stack_handle,
        )
        with open(os.path.join(_stall_watchdog_dir, "watchdog-config.json"), "w", encoding="utf-8") as handle:
            json.dump({
                "enabled": True,
                "threshold_seconds": _stall_watchdog_threshold,
                "started_at": time.time(),
                "scheduled_snapshot": "application-threads-timeout.txt",
                "no_progress_signal": "no_progress_stall",
                "run_age_is_not_stall": True,
            }, handle, ensure_ascii=False, sort_keys=True, indent=2)
    except Exception:
        try:
            faulthandler.cancel_dump_traceback_later()
        except Exception:
            pass
        if _stall_watchdog_stack_handle is not None:
            try:
                _stall_watchdog_stack_handle.close()
            except Exception:
                pass
        _stall_watchdog_stack_handle = None
        logger.exception("Database stall watchdog faulthandler setup failed")
    logger.info(
        "Database stall watchdog started directory=%s threshold_seconds=%s",
        _stall_watchdog_dir, _stall_watchdog_threshold,
    )
    _stall_watchdog_thread = threading.Thread(
        target=_stall_watchdog_loop,
        name="database-stall-watchdog",
        daemon=True,
    )
    _stall_watchdog_thread.start()
    return True


def stop_stall_watchdog(timeout=2.0):
    global _stall_watchdog_thread, _stall_watchdog_stack_handle
    _stall_watchdog_stop.set()
    try:
        faulthandler.cancel_dump_traceback_later()
    except Exception:
        pass
    thread = _stall_watchdog_thread
    if thread and thread.is_alive():
        thread.join(timeout=max(0.1, float(timeout or 2.0)))
    _stall_watchdog_thread = None
    if _stall_watchdog_stack_handle is not None:
        try:
            _stall_watchdog_stack_handle.close()
        except Exception:
            pass
        _stall_watchdog_stack_handle = None
    return True


def _stall_watchdog_loop():
    global _stall_watchdog_emitted
    while not _stall_watchdog_stop.wait(5.0):
        status = write_lock_status()
        reason = _stall_watchdog_reason(status)
        if not reason:
            continue
        if _stall_watchdog_emitted:
            continue
        _stall_watchdog_emitted = True
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        stack_path = os.path.join(_stall_watchdog_dir, f"application-threads-{stamp}.txt")
        state_path = os.path.join(_stall_watchdog_dir, f"application-write-lock-{stamp}.json")
        try:
            with _stall_watchdog_dump_lock:
                with open(stack_path, "w", encoding="utf-8") as handle:
                    faulthandler.dump_traceback(file=handle, all_threads=True)
            with open(state_path, "w", encoding="utf-8") as handle:
                json.dump({
                    "captured_at": time.time(),
                    "threshold_seconds": _stall_watchdog_threshold,
                    "write_lock": status,
                    "reason": reason,
                    "thread_dump": stack_path,
                }, handle, ensure_ascii=False, indent=2)
            logger.warning(
                "Database stall watchdog captured no-progress operation=%s age=%.3fs state=%s",
                status.get("operation"), status.get("age_seconds"), state_path,
            )
        except Exception:
            logger.exception("Database stall watchdog capture failed")


def is_occ_conflict_error(error: BaseException) -> bool:
    """Return whether *error* is a same-process DuckDB OCC conflict.

    File-owner locks, WAL replay failures and I/O errors are intentionally
    excluded. Retrying those blindly can amplify an instance/configuration
    failure and is not equivalent to retrying a rolled-back OCC transaction.
    """
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, duckdb.TransactionException):
            text = str(current).lower()
            if any(marker in text for marker in (
                "write-write conflict",
                "transaction conflict",
                "transactioncontext error",
                "write conflict",
            )):
                return True
        text = str(current).lower()
        if any(marker in text for marker in (
            "write-write conflict",
            "transaction conflict",
            "transactioncontext error",
            "conflict on update",
            "conflict with concurrent transaction",
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


def is_external_owner_error(error: BaseException) -> bool:
    """Return whether a failure indicates another process/file owner."""
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current).lower()
        if any(marker in text for marker in (
            "could not set lock on file",
            "conflicting lock is held",
            "database is locked",
            "database busy",
            "file is locked",
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


def is_transient_write_error(error: BaseException) -> bool:
    """Return whether *error* is safe to retry as a whole write transaction.

    In OCC mode this means only a DuckDB write-write conflict. In serialized
    compatibility mode the legacy in-process writer-gate timeout remains
    retryable. An external owner/file lock is an instance-safety failure and
    is never silently retried by this predicate.
    """
    if is_occ_conflict_error(error):
        return True
    if isinstance(error, TimeoutError):
        text = str(error).lower()
        return (
            _WRITE_CONCURRENCY == "serialized"
            and ("duckdb write lock timeout" in text or "database write lock" in text)
        )
    return False


def is_transient_read_error(error: BaseException) -> bool:
    """Return whether a read may succeed after the database settles.

    Read failures are kept separate from write conflicts.  A malformed SQL
    statement must not be retried, while a short-lived file/WAL/lock problem
    should get a bounded retry and a stable retryable HTTP response.
    """
    seen = set()
    current = error
    io_error = getattr(duckdb, "IOException", None)
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if io_error is not None and isinstance(current, io_error):
            return True
        text = str(current).lower()
        if any(marker in text for marker in (
            "could not set lock on file",
            "conflicting lock is held",
            "database is locked",
            "database busy",
            "could not open file",
            "i/o error",
            "io error",
            "input/output error",
            "failure while replaying wal",
            "wal file",
            "extension",
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


def is_database_read_error(error: BaseException) -> bool:
    """Classify DuckDB failures that should be exposed as read unavailability."""
    if is_transient_read_error(error):
        return True
    duckdb_error = getattr(duckdb, "Error", None)
    if duckdb_error is not None and isinstance(error, duckdb_error):
        return True
    return False


def read_transaction(callback, attempts: int = None, operation: str = "read",
                     base_delay: float = None):
    """Run a short read operation with guaranteed connection cleanup.

    ``callback`` must fully materialize any rows it returns before returning;
    the connection is closed before this helper returns.  Retrying the whole
    read avoids returning a partial composite response after a transient file
    or WAL error.
    """
    attempts = _READ_RETRIES if attempts is None else max(1, min(5, int(attempts)))
    delay = _READ_RETRY_DELAY_SECONDS if base_delay is None else max(0.0, float(base_delay))
    for attempt in range(1, attempts + 1):
        conn = None
        try:
            conn = get_connection()
            result = callback(conn)
            return result
        except BaseException as exc:
            if not is_transient_read_error(exc) or attempt >= attempts:
                raise
            _inc_database_metric(
                "marketing_copilot_database_read_retries_total",
                labels={"operation": operation},
            )
            logger.warning(
                "DuckDB read temporarily unavailable; retrying operation=%s attempt=%s/%s",
                operation, attempt + 1, attempts,
            )
            time.sleep(delay * attempt)
        finally:
            if conn is not None:
                conn.close()
    raise RuntimeError(f"DuckDB read did not complete: {operation}")


def write_busy_message(error: BaseException) -> str:
    """Stable user-facing message for transient DuckDB write contention."""
    if is_transient_write_error(error):
        return "当前数据库写入繁忙，请稍后重试"
    return "数据库写入失败，请联系管理员"


def write_transaction(callback, attempts: int = 5, operation: str = "write",
                      lock_timeout=None, base_delay: float = 0.05,
                      deadline=None, retry_jitter: float = None):
    """Run a complete short write transaction with bounded retry.

    The callback must not call ``commit`` or ``close``.  Retrying the whole
    transaction is important: after a DuckDB write-write conflict, replaying
    only the failed statement can leave earlier statements from the logical
    operation missing or partially committed.
    """
    attempts = max(1, min(8, int(attempts or 1)))
    if deadline is not None:
        try:
            deadline = float(deadline)
        except (TypeError, ValueError):
            raise ValueError("write transaction deadline must be a monotonic timestamp")
    jitter = _OCC_RETRY_JITTER_SECONDS if retry_jitter is None else max(0.0, float(retry_jitter))
    for attempt in range(1, attempts + 1):
        conn = None
        transaction_started = time.perf_counter()
        transaction_result = "error"
        try:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"DuckDB write transaction deadline exceeded: {operation}")
            # Connection acquisition can itself fail while DuckDB is replaying
            # a WAL or another process briefly owns the file lock. Keep it
            # inside the operation scope, but do not classify file-owner errors
            # as OCC retries.
            conn = get_connection()
            if hasattr(conn, "set_operation"):
                conn.set_operation(operation)
            conn.write_lock_timeout = lock_timeout
            conn.transaction_deadline = deadline
            conn.execute("BEGIN TRANSACTION")
            result = callback(conn)
            commit_started = time.perf_counter()
            conn.commit()
            _observe_database_metric(
                "marketing_copilot_database_transaction_commit_seconds",
                time.perf_counter() - commit_started,
                labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
            )
            transaction_result = "success"
            return result
        except BaseException as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    logger.debug("DuckDB rollback failed after write error", exc_info=True)
            conflict = is_occ_conflict_error(exc)
            retryable = is_transient_write_error(exc)
            if conflict:
                _inc_database_metric(
                    "marketing_copilot_database_occ_conflicts_total",
                    labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
                )
            if not retryable or attempt >= attempts:
                if conflict:
                    _inc_database_metric(
                        "marketing_copilot_database_occ_failures_total",
                        labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
                    )
                raise
            _inc_database_metric(
                "marketing_copilot_database_write_transaction_retries_total",
                labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
            )
            _inc_database_metric(
                "marketing_copilot_database_occ_retries_total",
                labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
            )
            logger.warning(
                "DuckDB OCC conflict; retrying complete transaction operation=%s "
                "attempt=%s/%s",
                operation, attempt + 1, attempts,
            )
            delay = max(0.0, float(base_delay)) * (2 ** (attempt - 1))
            if jitter:
                delay += random.uniform(0.0, jitter)
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - time.monotonic()))
            _observe_database_metric(
                "marketing_copilot_database_occ_backoff_seconds",
                delay,
                labels={"operation": operation, "mode": _WRITE_CONCURRENCY},
            )
            if delay:
                time.sleep(delay)
        finally:
            _observe_database_metric(
                "marketing_copilot_database_write_transaction_seconds",
                time.perf_counter() - transaction_started,
                labels={"operation": operation, "result": transaction_result},
            )
            if conn is not None:
                conn.close()
    raise RuntimeError(f"DuckDB write transaction did not complete: {operation}")


def _is_read_only_statement(sql: str) -> bool:
    statement = str(sql or "").lstrip()
    while statement.startswith("--"):
        _, separator, remainder = statement.partition("\n")
        if not separator:
            return True
        statement = remainder.lstrip()
    first_token = statement.split(None, 1)[0].upper() if statement else ""
    if first_token in _READ_ONLY_STATEMENTS:
        return True
    if first_token != "WITH":
        return False

    # A read-only CTE starts with WITH but is still safe to run while another
    # connection owns the application writer gate.  Do not use a substring
    # check: SELECT commonly appears inside CTE bodies, and a write CTE must
    # remain serialized.  This small scanner only considers SQL keywords
    # outside quoted strings and nested parentheses.
    depth = 0
    quote = None
    token = []
    top_level_keyword = None
    nested_write = False
    write_keywords = {"INSERT", "UPDATE", "DELETE", "MERGE", "COPY"}

    def flush_token():
        nonlocal top_level_keyword, nested_write
        if not token:
            return
        word = "".join(token).upper()
        token.clear()
        if word in write_keywords:
            if depth == 0:
                top_level_keyword = word
            else:
                nested_write = True
        elif word == "SELECT" and depth == 0 and top_level_keyword is None:
            top_level_keyword = word

    for char in statement:
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            flush_token()
            quote = char
            continue
        if char == "(":
            flush_token()
            depth += 1
            continue
        if char == ")":
            flush_token()
            depth = max(0, depth - 1)
            continue
        if char.isalpha() or char == "_":
            token.append(char)
        else:
            flush_token()

    flush_token()
    return top_level_keyword == "SELECT" and not nested_write


def _observe_database_metric(name: str, value: float, labels=None) -> None:
    try:
        from services import metrics_service
        metrics_service.observe_histogram(name, value, labels=labels)
    except Exception:
        # Database operations must not fail because runtime metrics are down.
        pass


def _inc_database_metric(name: str, amount: int = 1, labels=None) -> None:
    try:
        from services import metrics_service
        metrics_service.inc(name, amount=amount, labels=labels)
    except Exception:
        pass


def _acquire_write_lock(operation: str, timeout=None) -> float:
    timeout = _WRITE_LOCK_TIMEOUT_SECONDS if timeout is None else max(0.0, timeout)
    started = time.perf_counter()
    acquired = _write_lock.acquire(timeout=timeout)
    waited = time.perf_counter() - started
    if not acquired:
        holder = write_lock_status()
        _inc_database_metric(
            "marketing_copilot_database_write_lock_timeouts_total",
            labels={"operation": operation},
        )
        raise TimeoutError(
            f"DuckDB write lock timeout after {timeout:.1f}s "
            f"during {operation}; holder={holder['operation'] or 'unknown'} "
            f"holder_age={holder['age_seconds']:.3f}s "
            f"holder_thread={holder['thread_id'] or 'unknown'}"
        )
    with _write_state_lock:
        _write_holder.update({
            "thread_id": threading.get_ident(),
            "thread_name": threading.current_thread().name,
            "operation": str(operation),
            "acquired_at": time.monotonic(),
        })
    _observe_database_metric(
        "marketing_copilot_database_write_lock_wait_seconds",
        waited,
        labels={"operation": str(operation)},
    )
    return waited


def _update_write_holder(operation: str) -> None:
    with _write_state_lock:
        if _write_holder["thread_id"] == threading.get_ident():
            _write_holder["operation"] = str(operation)


def _clear_write_holder() -> None:
    with _write_state_lock:
        if _write_holder["thread_id"] == threading.get_ident():
            _write_holder.update({
                "thread_id": None,
                "thread_name": "",
                "operation": "",
                "acquired_at": 0.0,
            })


def _transaction_control(sql: str) -> str:
    """Return the top-level transaction control keyword, if any."""
    statement = str(sql or "").lstrip()
    while statement.startswith("--"):
        _, separator, remainder = statement.partition("\n")
        if not separator:
            return ""
        statement = remainder.lstrip()
    token = statement.split(None, 1)[0].upper() if statement else ""
    if token == "START" and statement.upper().startswith("START TRANSACTION"):
        return "BEGIN"
    if token in {"BEGIN", "COMMIT", "ROLLBACK", "END"}:
        return "COMMIT" if token == "END" else token
    return ""


@contextmanager
def write_gate(operation: str = "write", timeout=None):
    """Hold the maintenance boundary across a multi-step file operation.

    The compatibility name is kept for callers that have not migrated yet.
    In OCC mode it is a real maintenance-exclusive admission: new reads and
    writes are blocked and existing tracked transactions drain before the
    caller reaches CHECKPOINT/copy. In serialized mode it additionally holds
    the historical Python writer gate.
    """
    with coordinator.maintenance_exclusive(operation, timeout=timeout):
        gate_acquired = False
        try:
            if _WRITE_CONCURRENCY == "serialized":
                _acquire_write_lock(operation, timeout)
                gate_acquired = True
            yield
        finally:
            if gate_acquired:
                _clear_write_holder()
                _write_lock.release()


def write_lock_status() -> dict:
    """Return non-sensitive writer/admission diagnostics.

    ``held`` remains for existing dashboards, but in OCC mode it describes an
    active maintenance boundary rather than falsely claiming a global writer
    mutex is held by ordinary DML.
    """
    with _write_state_lock:
        thread_id = _write_holder["thread_id"]
        thread_name = _write_holder["thread_name"]
        operation = _write_holder["operation"]
        acquired_at = _write_holder["acquired_at"]
    age = max(0.0, time.monotonic() - acquired_at) if acquired_at else 0.0
    admission = coordinator.status()
    maintenance_age = float(admission.get("maintenance_age_seconds") or 0.0)
    return {
        "held": bool(thread_id) or bool(admission.get("maintenance_active")),
        "thread_id": thread_id,
        "thread_name": thread_name,
        "operation": operation,
        "age_seconds": round(max(age, maintenance_age), 3),
        **admission,
    }


def duckdb_connection_config() -> dict:
    """Return offline-safe DuckDB settings for the application database."""
    config = duckdb_extension_config()
    # DuckDB 1.2.0's OCC path is exercised by several application connections
    # at once. Keep connection-local vectorized execution single-threaded by
    # default; the process-level owner/cursor lifecycle above is the integrity
    # boundary, while transaction concurrency remains OCC across cursors.
    # Operators can override this for a newer DuckDB build after re-running
    # the acceptance matrix.
    if _WRITE_CONCURRENCY == "occ":
        config["threads"] = str(os.environ.get("MC_DUCKDB_THREADS", "1"))
    return config


def prepare_duckdb_connection() -> dict:
    """Prepare offline extension metadata, then return connection settings."""

    config = duckdb_connection_config()
    prepare_offline_extensions(duckdb, config)
    return config

DATABASE_FILE = os.environ.get(
    "MC_CONTROL_DB_PATH",
    os.environ.get(
        "DUCKDB_PATH",
        os.path.join(os.path.dirname(__file__), "marketing_copilot.duckdb"),
    ),
)

TABLE_SEQUENCES = {
    "smpp_configs": "seq_smpp_configs",
    "campaigns": "seq_campaigns",
    "sms_templates": "seq_sms_templates",
    "monitor_configs": "seq_monitor_configs",
    "monitor_files": "seq_monitor_files",
    "rabbitmq_configs": "seq_rabbitmq_configs",
    "sms_log": "seq_sms_log",
    "batch_campaigns": "seq_batch_campaigns",
    "batch_campaign_manifests": "seq_batch_campaign_manifests",
    "batch_campaign_daily_stats": "seq_batch_campaign_daily_stats",
    "batch_campaign_batches": "seq_batch_campaign_batches",
    "app_settings": "seq_app_settings",
    "realtime_campaign_files": "seq_realtime_campaign_files",
    "realtime_campaigns": "seq_realtime_campaigns",
    "sms_tasks": "seq_sms_tasks",
    "delivery_receipts": "seq_delivery_receipts",
    "exclusion_users": "seq_exclusion_users",
    "processed_files": "seq_processed_files",
    "system_events": "seq_system_events",
    "audit_logs": "seq_audit_logs",
    "task_events": "seq_task_events",
    "task_event_batches": "seq_task_event_batches",
    "failed_task_archive": "seq_failed_task_archive",
    "template_versions": "seq_template_versions",
    "update_history": "seq_update_history",
    "users": "seq_users",
    "ingestion_files": "seq_ingestion_files",
    "ingestion_chunks": "seq_ingestion_chunks",
    "realtime_whitelist_versions": "seq_realtime_whitelist_versions",
    "task_archive_manifests": "seq_task_archive_manifests",
}

# Database initialization used to be a collection of idempotent-looking
# statements, but several of the data/index operations were not cheap when
# repeated.  Keep the maintenance boundaries explicit and versioned.  When a
# future schema change is added to one of these groups, bump its marker rather
# than making every process start repeat the work.
_DB_SCHEMA_COLUMNS_MARKER = "db_schema_columns_v1"
_DB_REALTIME_CLEANUP_MARKER = "db_realtime_cleanup_v1"
_DB_PROCESSED_FILES_MARKER = "db_processed_files_unique_v1"
_DB_DATA_NORMALIZATION_MARKER = "db_data_normalization_v1"
_DB_RECEIPT_INDEXES_MARKER = "db_receipt_indexes_v2"
_DB_CANONICAL_READY_MARKER = "db_canonical_ready_v1"

_RECEIPT_INDEX_DEFINITIONS = (
    (
        "idx_delivery_message_id",
        "delivery_receipts",
        False,
        ("source_namespace", "smsc_message_id"),
        "CREATE INDEX IF NOT EXISTS idx_delivery_message_id "
        "ON delivery_receipts(source_namespace,smsc_message_id)",
    ),
    (
        "idx_receipt_unique",
        "delivery_receipts",
        True,
        ("source_namespace", "task_id", "smsc_message_id", "message_state"),
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_unique "
        "ON delivery_receipts(source_namespace,task_id,smsc_message_id,message_state)",
    ),
)


def _db_marker_completed(conn, key: str) -> bool:
    """Return whether a one-time database maintenance marker is complete."""
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key=?",
            (key,),
        ).fetchone()
    except Exception:
        # The base schema is created immediately before these checks.  Treat a
        # missing/unreadable marker as incomplete so a repair is never skipped.
        return False
    return bool(row and str(row[0] or "").strip().lower() == "completed")


def _record_db_marker(conn, key: str) -> bool:
    try:
        conn.execute(
            """INSERT INTO app_settings (key, value) VALUES (?, ?)
               ON CONFLICT (key) DO UPDATE SET value=excluded.value""",
            (key, "completed"),
        )
        return True
    except Exception as exc:
        # Marker persistence is an optimization, not a reason to prevent the
        # service from starting on an older app_settings schema.  The guarded
        # work remains correct and will be retried on the next maintenance run.
        logger.warning("[DB] Could not persist migration marker key=%s: %s", key, exc)
        return False


def _index_definition_matches(conn, name: str, table: str,
                              unique: bool, expressions) -> bool:
    """Check an index definition without rebuilding a valid large-table index."""
    try:
        row = conn.execute(
            """SELECT table_name, is_unique, expressions
               FROM duckdb_indexes() WHERE index_name=?""",
            (name,),
        ).fetchone()
    except Exception:
        return False
    if not row:
        return False
    actual_expressions = str(row[2] or "").lower().replace(" ", "")
    actual_expressions = actual_expressions.replace("'", "").replace('"', "")
    expected_expressions = "[" + ",".join(expressions) + "]"
    return (
        str(row[0] or "").lower() == table.lower()
        and bool(row[1]) == bool(unique)
        and actual_expressions == expected_expressions.lower()
    )


def _ensure_receipt_indexes(conn) -> None:
    """Repair receipt indexes once; completed startups do no large-table work."""
    if _db_marker_completed(conn, _DB_RECEIPT_INDEXES_MARKER):
        return

    started = time.perf_counter()
    complete = True
    for name, table, unique, expressions, create_sql in _RECEIPT_INDEX_DEFINITIONS:
        try:
            # Existing production databases commonly already have the desired
            # definition.  Do not drop/rebuild those large indexes merely
            # because the service restarted.
            if _index_definition_matches(conn, name, table, unique, expressions):
                continue
            conn.execute(f"DROP INDEX IF EXISTS {name}")
            conn.execute(create_sql)
        except Exception as exc:
            complete = False
            logger.warning("[DB] Receipt index maintenance failed index=%s: %s", name, exc)

    marker_recorded = complete and _record_db_marker(conn, _DB_RECEIPT_INDEXES_MARKER)
    logger.info(
        "[DB] Receipt index maintenance complete=%s elapsed=%.3fs",
        marker_recorded,
        time.perf_counter() - started,
    )


class DuckRow:
    """Dict-like row wrapper so existing service code works unchanged."""

    def __init__(self, columns, values):
        self._columns = list(columns)
        self._values = list(values)
        self._dict = dict(zip(self._columns, self._values))

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._dict[key.lower()]

    def __getattr__(self, name):
        if name in self._dict:
            return self._dict[name]
        raise AttributeError(f"Row has no attribute '{name}'")

    def keys(self):
        return self._columns

    def values(self):
        return self._values

    def items(self):
        return self._dict.items()

    def __contains__(self, key):
        return key.lower() in self._dict

    def get(self, key, default=None):
        return self._dict.get(key.lower(), default)

    def __bool__(self):
        return True

    def __repr__(self):
        return str(self._dict)


class WrappedConnection:
    def __init__(self, conn):
        self.conn = conn
        self._last_sql = ""
        self._operation = ""
        self._connection_lock = threading.RLock()
        self._write_lock_held = False
        self._admission_token = None
        self._explicit_transaction = False
        # Keep the business operation captured at first gate acquisition.
        # Later execute/commit phases must not relabel a long transaction as
        # merely ``commit`` in contention diagnostics.
        self._write_operation = ""
        self.write_lock_timeout = None
        self.transaction_deadline = None

    def set_operation(self, operation: str):
        """Attach a bounded business operation name to subsequent DB writes."""
        self._operation = str(operation or "")
        return self

    def _ensure_admission(self, operation: str, read_only: bool = False) -> float:
        operation = self._operation or operation
        if self._admission_token is not None:
            if not read_only:
                self._admission_token.read_only = False
            return 0.0
        timeout = self.write_lock_timeout
        token = coordinator.acquire(
            operation,
            read_only=read_only,
            timeout=timeout,
            deadline=self.transaction_deadline,
        )
        self._admission_token = token
        return max(0.0, time.monotonic() - token.acquired_at)

    def _ensure_write_lock(self, operation: str) -> float:
        operation = self._operation or operation
        admission_wait = self._ensure_admission(operation, read_only=False)
        if self._write_lock_held:
            _update_write_holder(self._write_operation or operation)
            return admission_wait
        if _WRITE_CONCURRENCY != "serialized":
            self._write_operation = operation
            return admission_wait
        try:
            waited = _acquire_write_lock(operation, self.write_lock_timeout)
        except BaseException:
            token = self._admission_token
            self._admission_token = None
            if token is not None:
                token.release()
            raise
        self._write_lock_held = True
        self._write_operation = operation
        return admission_wait + waited

    def _ensure_read_admission(self, operation: str) -> float:
        return self._ensure_admission(operation, read_only=True)

    def _release_write_lock(self) -> None:
        if self._write_lock_held:
            self._write_lock_held = False
            with _write_state_lock:
                acquired_at = _write_holder["acquired_at"]
                operation = _write_holder["operation"]
            if acquired_at:
                _observe_database_metric(
                    "marketing_copilot_database_write_lock_hold_seconds",
                    max(0.0, time.monotonic() - acquired_at),
                    labels={"operation": self._write_operation or operation or "unknown"},
                )
            _clear_write_holder()
            _write_lock.release()
        self._write_operation = ""
        token = self._admission_token
        self._admission_token = None
        if token is not None:
            token.release()

    def _rollback_underlying(self) -> None:
        """Rollback without masking the original DuckDB exception."""
        try:
            self.conn.rollback()
        except Exception:
            # DuckDB can report "no transaction is active" after an auto-
            # committed DDL or a failed statement.  The application-level
            # writer gate still must be released in that case.
            logger.debug("DuckDB rollback was not active", exc_info=True)

    def execute(self, sql, params=None):
        sql = sql.replace("datetime('now','localtime')", "CURRENT_TIMESTAMP")
        self._last_sql = sql
        read_only = _is_read_only_statement(sql)
        control = _transaction_control(sql)
        write_wait = 0.0
        with self._connection_lock:
            if read_only:
                write_wait = self._ensure_read_admission("read")
            else:
                write_wait = self._ensure_write_lock("execute")
            if control == "BEGIN":
                self._explicit_transaction = True
            execution_started = time.perf_counter()
            try:
                if params is not None:
                    if not isinstance(params, (list, tuple)):
                        params = [params]
                    self.conn.execute(sql, params)
                else:
                    self.conn.execute(sql)
            except BaseException:
                if not read_only:
                    self._rollback_underlying()
                    self._release_write_lock()
                else:
                    # A failed read connection is no longer safe to reuse.
                    # Close it here as a last line of defence for legacy
                    # callers that predate the read_transaction helper.
                    self.close()
                raise
            finally:
                execution_seconds = time.perf_counter() - execution_started
            if control in {"COMMIT", "ROLLBACK"}:
                self._explicit_transaction = False
                self._release_write_lock()
            elif (
                not read_only
                and _WRITE_CONCURRENCY == "occ"
                and not self._explicit_transaction
            ):
                # Legacy callers often issue one DML statement without BEGIN.
                # DuckDB auto-commits that statement; do not hold a shared
                # admission token until an unrelated connection close.
                self._release_write_lock()
        kind = "read" if read_only else "write"
        _observe_database_metric(
            "marketing_copilot_database_statement_seconds", execution_seconds,
            labels={"kind": kind},
        )
        return self

    def executemany(self, sql, rows):
        sql = sql.replace("datetime('now','localtime')", "CURRENT_TIMESTAMP")
        self._last_sql = sql
        with self._connection_lock:
            write_wait = self._ensure_write_lock("executemany")
            execution_started = time.perf_counter()
            try:
                self.conn.executemany(sql, rows)
            except BaseException:
                self._rollback_underlying()
                self._release_write_lock()
                raise
            finally:
                execution_seconds = time.perf_counter() - execution_started
            if _WRITE_CONCURRENCY == "occ" and not self._explicit_transaction:
                self._release_write_lock()
        _observe_database_metric(
            "marketing_copilot_database_statement_seconds", execution_seconds,
            labels={"kind": "write"},
        )
        return self

    def fetchone(self):
        try:
            with self._connection_lock:
                row = self.conn.fetchone()
        except BaseException:
            self.close()
            raise
        if row is None:
            return None
        cols = [desc[0] for desc in self.conn.description] if self.conn.description else []
        return DuckRow(cols, list(row))

    def fetchall(self):
        try:
            with self._connection_lock:
                rows = self.conn.fetchall()
        except BaseException:
            self.close()
            raise
        if not rows:
            return []
        cols = [desc[0] for desc in self.conn.description] if self.conn.description else []
        return [DuckRow(cols, list(r)) for r in rows]

    @property
    def _description(self):
        return self.conn.description

    @property
    def lastrowid(self):
        lower = self._last_sql.strip().lower()
        for keyword in ("into ", "from ", "update ", "delete from "):
            if keyword in lower:
                idx = lower.index(keyword) + len(keyword)
                rest = lower[idx:].strip().lstrip("(").strip()
                tbl = rest.split()[0].strip("`\"'")
                if tbl in TABLE_SEQUENCES:
                    seq = TABLE_SEQUENCES[tbl]
                    try:
                        return self.conn.execute(f"SELECT currval('{seq}')").fetchone()[0]
                    except Exception:
                        pass
                break
        return 0

    @property
    def rowcount(self):
        try:
            return self.conn.rowcount
        except Exception:
            return 1

    def commit(self):
        with self._connection_lock:
            write_wait = self._ensure_write_lock("commit")
            execution_started = time.perf_counter()
            try:
                self.conn.commit()
            except BaseException:
                self._rollback_underlying()
                raise
            finally:
                execution_seconds = time.perf_counter() - execution_started
                self._release_write_lock()
                self._explicit_transaction = False
        _observe_database_metric(
            "marketing_copilot_database_statement_seconds", execution_seconds,
            labels={"kind": "commit"},
        )
    def rollback(self):
        with self._connection_lock:
            started = time.perf_counter()
            try:
                self._rollback_underlying()
            finally:
                self._release_write_lock()
                self._explicit_transaction = False
            _observe_database_metric(
                "marketing_copilot_database_rollback_seconds",
                time.perf_counter() - started,
            )

    def close(self):
        try:
            with self._connection_lock:
                if self._admission_token is not None:
                    self._rollback_underlying()
                    self._release_write_lock()
                self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None and self._admission_token is not None:
            self.rollback()
        self.close()
        return False

    def __del__(self):
        # A number of legacy service functions predate context-manager use.
        # Make an uncaught exception unable to strand the process-wide writer
        # gate until an arbitrary future garbage-collection cycle.
        try:
            self.close()
        except Exception:
            pass


# DuckDB 1.2.0's Python module keeps a shared database cache keyed by path.
# Opening and closing that cache while several service threads create their own
# short-lived connections is not a safe ownership lifecycle.  Keep one process
# owner open for the lifetime of this coordinator and hand out independent
# cursors; a cursor has its own transaction context while sharing the already
# initialized native database.
_ROOT_CONNECTION = None
_ROOT_CONNECTION_PATH = None
_ROOT_CONNECTION_LOCK = threading.Lock()


def _root_connection():
    global _ROOT_CONNECTION, _ROOT_CONNECTION_PATH
    with _ROOT_CONNECTION_LOCK:
        if _ROOT_CONNECTION is not None and _ROOT_CONNECTION_PATH != DATABASE_FILE:
            _ROOT_CONNECTION.close()
            _ROOT_CONNECTION = None
            _ROOT_CONNECTION_PATH = None
        if _ROOT_CONNECTION is None:
            config = prepare_duckdb_connection()
            _ROOT_CONNECTION = duckdb.connect(
                DATABASE_FILE, config=config
            )
            _ROOT_CONNECTION_PATH = DATABASE_FILE
        return _ROOT_CONNECTION


def _close_root_connection() -> None:
    global _ROOT_CONNECTION, _ROOT_CONNECTION_PATH
    with _ROOT_CONNECTION_LOCK:
        connection = _ROOT_CONNECTION
        _ROOT_CONNECTION = None
        _ROOT_CONNECTION_PATH = None
    if connection is not None:
        connection.close()


def close_root_connection() -> None:
    """Close the cached owner connection after a maintenance drain.

    Callers must hold ``maintenance_exclusive`` (or an equivalent process
    quiescence boundary).  DuckDB keeps the process owner open deliberately
    for normal request lifetime; Windows cannot copy the database file while
    that owner handle remains open, even after a separate CHECKPOINT
    connection has been closed.  The next ``get_connection`` lazily creates
    the owner again.
    """
    _close_root_connection()


def get_connection():
    return WrappedConnection(_root_connection().cursor())


def reconnect(path: str = None):
    global DATABASE_FILE
    if path:
        DATABASE_FILE = path


def set_database_file(path: str):
    global DATABASE_FILE
    if path != DATABASE_FILE:
        _close_root_connection()
    DATABASE_FILE = path


def get_database_file() -> str:
    return DATABASE_FILE


def init_db():
    started = time.perf_counter()
    try:
        conn = get_connection()
    except Exception as e:
        logger.warning("Cannot initialize DuckDB: %s", e)
        return

    for seq in TABLE_SEQUENCES.values():
        try:
            conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq} START 1")
        except Exception as e:
            logger.warning("[DB] Skipping sequence %s: %s", seq, e)

    ddl = [
        """CREATE TABLE IF NOT EXISTS smpp_configs (
            id INTEGER DEFAULT nextval('seq_smpp_configs') PRIMARY KEY,
            name TEXT NOT NULL, host TEXT NOT NULL,
            port INTEGER NOT NULL DEFAULT 2775,
            system_id TEXT NOT NULL, password TEXT NOT NULL,
            source_addr TEXT NOT NULL DEFAULT '',
            bind_type TEXT NOT NULL DEFAULT 'transceiver',
            address_ton INTEGER NOT NULL DEFAULT 0,
            address_npi INTEGER NOT NULL DEFAULT 0,
            data_coding INTEGER NOT NULL DEFAULT 0,
            pool_size INTEGER NOT NULL DEFAULT 1,
            window_size INTEGER NOT NULL DEFAULT 10,
            enquire_interval INTEGER NOT NULL DEFAULT 30,
            timeout INTEGER NOT NULL DEFAULT 10,
            tps INTEGER NOT NULL DEFAULT 20,
            tps_scope TEXT NOT NULL DEFAULT 'per_bind',
            priority INTEGER NOT NULL DEFAULT 100,
            reconnect_max_seconds INTEGER NOT NULL DEFAULT 60,
            delivery_receipt_mode TEXT NOT NULL DEFAULT 'optional',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER DEFAULT nextval('seq_campaigns') PRIMARY KEY,
            name TEXT NOT NULL, description TEXT DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            marketing_type TEXT NOT NULL DEFAULT 'batch',
            marketing_campaign_id INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_templates (
            id INTEGER DEFAULT nextval('seq_sms_templates') PRIMARY KEY,
            campaign_id INTEGER NOT NULL, language TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '', source_addr TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        )""",
        """CREATE TABLE IF NOT EXISTS monitor_configs (
            id INTEGER DEFAULT nextval('seq_monitor_configs') PRIMARY KEY,
            directory_path TEXT NOT NULL,
            target_directories TEXT NOT NULL DEFAULT '[]',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS monitor_files (
            id INTEGER DEFAULT nextval('seq_monitor_files') PRIMARY KEY,
            filename TEXT NOT NULL, directory_path TEXT NOT NULL,
            file_size INTEGER DEFAULT 0, row_count INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            monitor_config_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (monitor_config_id) REFERENCES monitor_configs(id)
        )""",
        """CREATE TABLE IF NOT EXISTS rabbitmq_configs (
            id INTEGER DEFAULT nextval('seq_rabbitmq_configs') PRIMARY KEY,
            host TEXT NOT NULL DEFAULT 'localhost',
            port INTEGER NOT NULL DEFAULT 5672,
            virtual_host TEXT NOT NULL DEFAULT '/',
            username TEXT NOT NULL DEFAULT 'guest',
            password TEXT NOT NULL DEFAULT 'guest',
            exchange_name TEXT NOT NULL DEFAULT 'marketing_copilot',
            queue_prefix TEXT NOT NULL DEFAULT 'mc_',
            enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS batch_campaigns (
            id INTEGER DEFAULT nextval('seq_batch_campaigns') PRIMARY KEY,
            name TEXT NOT NULL,
            start_time TEXT NOT NULL DEFAULT '',
            end_time TEXT NOT NULL DEFAULT '',
            send_start_time TEXT NOT NULL DEFAULT '',
            send_end_time TEXT NOT NULL DEFAULT '',
            send_days TEXT NOT NULL DEFAULT '[]',
            batch_size INTEGER NOT NULL DEFAULT 500,
            status TEXT NOT NULL DEFAULT 'not_started',
            customer_list_filename TEXT DEFAULT '',
            customer_list_path TEXT DEFAULT '',
            row_count INTEGER DEFAULT 0,
            campaign_dir TEXT DEFAULT '',
            use_custom_directory BOOLEAN NOT NULL DEFAULT FALSE,
           sms_template_id INTEGER DEFAULT NULL,
           phone_prefix TEXT NOT NULL DEFAULT '',
           smpp_config_id INTEGER DEFAULT NULL,
           flash_sms BOOLEAN NOT NULL DEFAULT FALSE,
             active_manifest_id BIGINT DEFAULT NULL,
           dispatch_generation BIGINT NOT NULL DEFAULT 0,
           created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS batch_campaign_manifests (
            id BIGINT DEFAULT nextval('seq_batch_campaign_manifests') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            source_path TEXT NOT NULL DEFAULT '',
            source_filename TEXT NOT NULL DEFAULT '',
            source_size BIGINT NOT NULL DEFAULT 0,
            source_mtime_ns BIGINT NOT NULL DEFAULT 0,
            source_hash TEXT NOT NULL DEFAULT '',
            batch_size INTEGER NOT NULL DEFAULT 500,
            generated_batches BIGINT NOT NULL DEFAULT 0,
            total_rows BIGINT NOT NULL DEFAULT 0,
            total_batches BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'building',
            error_detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            published_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS batch_campaign_batches (
            id INTEGER DEFAULT nextval('seq_batch_campaign_batches') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
             manifest_id BIGINT DEFAULT NULL,
            batch_index INTEGER NOT NULL DEFAULT 0,
            start_row INTEGER NOT NULL DEFAULT 0,
            end_row INTEGER NOT NULL DEFAULT 0,
            total_count INTEGER NOT NULL DEFAULT 0,
            task_count INTEGER NOT NULL DEFAULT 0,
            delivered_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (campaign_id) REFERENCES batch_campaigns(id)
        )""",
        """CREATE TABLE IF NOT EXISTS batch_campaign_daily_stats (
            id INTEGER DEFAULT nextval('seq_batch_campaign_daily_stats') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            stats_date TEXT NOT NULL,
            total_prospects INTEGER NOT NULL DEFAULT 0,
            sent_count INTEGER NOT NULL DEFAULT 0,
            conversion_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (campaign_id) REFERENCES batch_campaigns(id)
        )""",
        """CREATE TABLE IF NOT EXISTS realtime_campaigns (
            id INTEGER DEFAULT nextval('seq_realtime_campaigns') PRIMARY KEY,
            name TEXT NOT NULL,
            start_time TEXT NOT NULL DEFAULT '',
            end_time TEXT NOT NULL DEFAULT '',
            send_start_time TEXT NOT NULL DEFAULT '',
            send_end_time TEXT NOT NULL DEFAULT '',
            send_days TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'not_started',
            use_whitelist BOOLEAN NOT NULL DEFAULT TRUE,
            active_whitelist_version_id INTEGER DEFAULT NULL,
            customer_list_filename TEXT DEFAULT '',
            customer_list_path TEXT DEFAULT '',
            row_count INTEGER DEFAULT 0,
            campaign_dir TEXT DEFAULT '',
            use_custom_directory BOOLEAN NOT NULL DEFAULT FALSE,
            watch_source TEXT NOT NULL DEFAULT 'local',
            watch_dir TEXT NOT NULL DEFAULT '',
            sftp_host TEXT NOT NULL DEFAULT '',
            sftp_port INTEGER NOT NULL DEFAULT 22,
            sftp_username TEXT NOT NULL DEFAULT '',
            sftp_password TEXT NOT NULL DEFAULT '',
            watch_filename_prefix TEXT NOT NULL DEFAULT 'Android_4G_Switch_Somali_',
            sms_template_id INTEGER DEFAULT NULL,
            smpp_config_id INTEGER DEFAULT NULL,
            flash_sms BOOLEAN NOT NULL DEFAULT FALSE,
            dispatch_generation BIGINT NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS realtime_campaign_files (
            id INTEGER DEFAULT nextval('seq_realtime_campaign_files') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            filename TEXT NOT NULL DEFAULT '',
            file_path TEXT NOT NULL DEFAULT '',
            total_msisdn INTEGER NOT NULL DEFAULT 0,
            matched_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER DEFAULT nextval('seq_app_settings') PRIMARY KEY,
            key TEXT NOT NULL UNIQUE,
            value TEXT NOT NULL DEFAULT ''
        )""",
        """CREATE TABLE IF NOT EXISTS sms_log (
            id INTEGER DEFAULT nextval('seq_sms_log') PRIMARY KEY,
            msisdn TEXT NOT NULL, campaign_name TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '',
            source_addr TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            error_msg TEXT DEFAULT '', routing_key TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            sent_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS exclusion_users (
            id INTEGER DEFAULT nextval('seq_exclusion_users') PRIMARY KEY,
            msisdn TEXT NOT NULL UNIQUE,
            reason TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_tasks (
            id INTEGER DEFAULT nextval('seq_sms_tasks') PRIMARY KEY,
            campaign_type TEXT NOT NULL,
            campaign_id INTEGER NOT NULL,
            batch_id INTEGER,
            campaign_name TEXT NOT NULL DEFAULT '',
            msisdn TEXT NOT NULL,
            language TEXT NOT NULL DEFAULT '',
            sms_template_id INTEGER,
            smpp_config_id INTEGER,
            flash_sms BOOLEAN NOT NULL DEFAULT FALSE,
            content TEXT NOT NULL,
            source_addr TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{}',
            source_file TEXT NOT NULL DEFAULT '',
            task_date DATE NOT NULL DEFAULT CURRENT_DATE,
            dedupe_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            next_attempt_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            smsc_message_ids TEXT NOT NULL DEFAULT '[]',
            error_msg TEXT NOT NULL DEFAULT '',
            claim_token TEXT NOT NULL DEFAULT '',
            last_smpp_status INTEGER,
            failure_class TEXT NOT NULL DEFAULT '',
            receipt_retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            submitted_at TEXT,
            delivered_at TEXT,
            transport_smpp_config_id INTEGER,
            submit_sm_parts INTEGER NOT NULL DEFAULT 0,
            dispatch_generation BIGINT NOT NULL DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS delivery_receipts (
            id INTEGER DEFAULT nextval('seq_delivery_receipts') PRIMARY KEY,
            task_id INTEGER,
            source_namespace TEXT NOT NULL DEFAULT 'legacy',
            smsc_message_id TEXT NOT NULL DEFAULT '',
            message_state TEXT NOT NULL DEFAULT '',
            raw_receipt TEXT NOT NULL DEFAULT '',
            received_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_task_message_ids (
            smsc_message_id TEXT PRIMARY KEY,
            task_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_message_bindings_v2 (
            source_namespace TEXT NOT NULL,
            smsc_message_id TEXT NOT NULL,
            task_id BIGINT NOT NULL,
            attempt_token TEXT NOT NULL DEFAULT '',
            part_no INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY(source_namespace,smsc_message_id,task_id,attempt_token,part_no)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_send_attempts (
            attempt_token TEXT PRIMARY KEY,
            task_id BIGINT NOT NULL,
            claim_token TEXT NOT NULL DEFAULT '',
            source_namespace TEXT NOT NULL DEFAULT '',
            smpp_config_id INTEGER,
            slot INTEGER,
            planned_parts INTEGER NOT NULL DEFAULT 0,
            accepted_parts INTEGER NOT NULL DEFAULT 0,
            failed_parts INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'intent',
            lease_until TEXT,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_send_parts (
            attempt_token TEXT NOT NULL,
            part_no INTEGER NOT NULL,
            sequence BIGINT,
            connection_generation TEXT NOT NULL DEFAULT '',
            source_namespace TEXT NOT NULL DEFAULT '',
            response_status INTEGER,
            smsc_message_id TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT 'planned',
            sent_at TEXT,
            responded_at TEXT,
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY(attempt_token,part_no)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_result_events (
            event_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            task_id BIGINT,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS sms_fast_batches (
            batch_id TEXT PRIMARY KEY,
            lease_id TEXT NOT NULL UNIQUE,
            claim_token TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'fast',
            state TEXT NOT NULL DEFAULT 'claimed',
            task_count BIGINT NOT NULL DEFAULT 0,
            checkpointed_count BIGINT NOT NULL DEFAULT 0,
            unresolved_count BIGINT NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            lease_until TEXT,
            checkpointed_at TEXT,
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            last_error TEXT NOT NULL DEFAULT ''
        )""",
        """CREATE TABLE IF NOT EXISTS sms_fast_batch_items (
            batch_id TEXT NOT NULL,
            lease_id TEXT NOT NULL,
            task_id BIGINT NOT NULL,
            claim_token TEXT NOT NULL,
            attempt_token TEXT NOT NULL,
            dispatch_generation BIGINT NOT NULL DEFAULT 0,
            route_id INTEGER,
            planned_parts INTEGER NOT NULL DEFAULT 0,
            accepted_parts INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'claimed',
            message_ids TEXT NOT NULL DEFAULT '[]',
            last_error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            checkpointed_at TEXT,
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY(batch_id,task_id)
        )""",
        """CREATE TABLE IF NOT EXISTS campaign_capacity_policies (
            campaign_type TEXT NOT NULL,
            campaign_id BIGINT NOT NULL,
            weight DOUBLE NOT NULL DEFAULT 1.0,
            min_parts_tps DOUBLE NOT NULL DEFAULT 0,
            max_parts_tps DOUBLE,
            max_outstanding_parts BIGINT,
            max_pending_tasks BIGINT,
            max_ingest_files INTEGER,
            enabled INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY(campaign_type,campaign_id)
        )""",
        """CREATE TABLE IF NOT EXISTS realtime_inbox_reservations (
            token TEXT PRIMARY KEY,
            campaign_id BIGINT NOT NULL,
            filename TEXT NOT NULL,
            file_size BIGINT NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT '',
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            UNIQUE(campaign_id,filename)
        )""",
        """CREATE TABLE IF NOT EXISTS fair_scheduler_state (
            stage TEXT PRIMARY KEY,
            cursor_key TEXT NOT NULL DEFAULT '',
            rounds BIGINT NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS campaign_fairness_state (
            campaign_type TEXT NOT NULL,
            campaign_id BIGINT NOT NULL,
            route_id BIGINT NOT NULL DEFAULT 0,
            eligible_since TEXT,
            last_dispatched_at TEXT,
            last_file_claim_at TEXT,
            last_task_claim_at TEXT,
            last_submit_at TEXT,
            backlog BIGINT NOT NULL DEFAULT 0,
            consecutive_skip_rounds BIGINT NOT NULL DEFAULT 0,
            last_skip_reason TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY(campaign_type,campaign_id,route_id)
        )""",
        """CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER DEFAULT nextval('seq_processed_files') PRIMARY KEY,
            campaign_type TEXT NOT NULL, campaign_id INTEGER NOT NULL,
            filename TEXT NOT NULL, file_hash TEXT NOT NULL,
            file_path TEXT NOT NULL DEFAULT '', file_size BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'detected', total_rows INTEGER NOT NULL DEFAULT 0,
                     created_tasks INTEGER NOT NULL DEFAULT 0, error_detail TEXT NOT NULL DEFAULT '',
                     whitelist_version_id INTEGER, processing_started_at TEXT, last_progress_at TEXT,
                     processing_phase TEXT NOT NULL DEFAULT 'queued', processed_rows BIGINT NOT NULL DEFAULT 0,
                     matched_rows BIGINT NOT NULL DEFAULT 0, current_chunk_index INTEGER NOT NULL DEFAULT 0,
                     total_chunks INTEGER NOT NULL DEFAULT 0, worker_id TEXT NOT NULL DEFAULT '',
                     retry_count INTEGER NOT NULL DEFAULT 0, last_error_class TEXT NOT NULL DEFAULT '',
                     last_error_at TEXT, next_retry_at TEXT, max_retry_count INTEGER NOT NULL DEFAULT 4,
                     source_state_version BIGINT NOT NULL DEFAULT -1,
             detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
            UNIQUE(campaign_type, campaign_id, filename)
        )""",
        """CREATE TABLE IF NOT EXISTS ingestion_files (
            id INTEGER DEFAULT nextval('seq_ingestion_files') PRIMARY KEY,
            campaign_type TEXT NOT NULL,
            campaign_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            file_path TEXT NOT NULL DEFAULT '',
            file_size BIGINT NOT NULL DEFAULT 0,
            file_hash TEXT NOT NULL DEFAULT '',
             manifest_id BIGINT DEFAULT NULL,
            total_rows BIGINT NOT NULL DEFAULT 0,
            chunk_size INTEGER NOT NULL DEFAULT 5000,
            total_chunks INTEGER NOT NULL DEFAULT 0,
            completed_chunks INTEGER NOT NULL DEFAULT 0,
            created_tasks BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            error_detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            completed_at TEXT,
            whitelist_version_id INTEGER,
            processing_started_at TEXT, last_progress_at TEXT,
            processing_phase TEXT NOT NULL DEFAULT 'queued', processed_rows BIGINT NOT NULL DEFAULT 0,
            matched_rows BIGINT NOT NULL DEFAULT 0, current_chunk_index INTEGER NOT NULL DEFAULT 0,
            worker_id TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '',
            lease_expires_at TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
            last_error_class TEXT NOT NULL DEFAULT '', last_error_at TEXT, next_retry_at TEXT,
            max_retry_count INTEGER NOT NULL DEFAULT 4,
            state_version BIGINT NOT NULL DEFAULT 0,
            UNIQUE(campaign_type, campaign_id, filename)
        )""",
        """CREATE TABLE IF NOT EXISTS ingestion_chunks (
            id INTEGER DEFAULT nextval('seq_ingestion_chunks') PRIMARY KEY,
            file_id INTEGER NOT NULL,
            source_batch_id INTEGER,
            chunk_index INTEGER NOT NULL,
            start_row BIGINT NOT NULL DEFAULT 0,
            end_row BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            lease_token TEXT NOT NULL DEFAULT '',
            lease_expires_at TEXT,
            created_tasks INTEGER NOT NULL DEFAULT 0,
            duplicate_rows INTEGER NOT NULL DEFAULT 0,
            excluded_rows INTEGER NOT NULL DEFAULT 0,
            invalid_rows INTEGER NOT NULL DEFAULT 0,
            no_template_rows INTEGER NOT NULL DEFAULT 0,
            error_detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            completed_at TEXT,
            UNIQUE(file_id, chunk_index)
        )""",
        """CREATE TABLE IF NOT EXISTS realtime_whitelist_versions (
            id INTEGER DEFAULT nextval('seq_realtime_whitelist_versions') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            version_no INTEGER NOT NULL,
            source_filename TEXT NOT NULL DEFAULT '',
            source_path TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            normalized_count BIGINT NOT NULL DEFAULT 0,
            index_path TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'building',
            error_detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            ready_at TEXT,
            retired_at TEXT,
            UNIQUE(campaign_id, version_no),
            UNIQUE(campaign_id, source_hash)
        )""",
        """CREATE TABLE IF NOT EXISTS task_archive_manifests (
            id INTEGER DEFAULT nextval('seq_task_archive_manifests') PRIMARY KEY,
            archive_key TEXT NOT NULL UNIQUE,
            task_date DATE NOT NULL,
            campaign_type TEXT NOT NULL,
            campaign_id INTEGER NOT NULL,
            first_task_id BIGINT NOT NULL,
            last_task_id BIGINT NOT NULL,
            task_count INTEGER NOT NULL,
            root_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'completed',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS system_events (
            id INTEGER DEFAULT nextval('seq_system_events') PRIMARY KEY,
            category TEXT NOT NULL, severity TEXT NOT NULL DEFAULT 'error',
            code TEXT NOT NULL DEFAULT '', message TEXT NOT NULL,
            context TEXT NOT NULL DEFAULT '{}', resolved INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER DEFAULT nextval('seq_audit_logs') PRIMARY KEY,
            username TEXT NOT NULL DEFAULT 'system', action TEXT NOT NULL,
            resource TEXT NOT NULL DEFAULT '', resource_id TEXT NOT NULL DEFAULT '',
            detail TEXT NOT NULL DEFAULT '{}', ip_address TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER DEFAULT nextval('seq_task_events') PRIMARY KEY,
            task_id INTEGER NOT NULL, event_type TEXT NOT NULL,
            from_status TEXT NOT NULL DEFAULT '', to_status TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS task_event_batches (
            id INTEGER DEFAULT nextval('seq_task_event_batches') PRIMARY KEY,
            campaign_type TEXT NOT NULL DEFAULT '',
            campaign_id INTEGER NOT NULL DEFAULT 0,
            batch_id INTEGER,
            event_type TEXT NOT NULL,
            from_status TEXT NOT NULL DEFAULT '',
            to_status TEXT NOT NULL DEFAULT '',
            event_count INTEGER NOT NULL DEFAULT 0,
            first_task_id INTEGER,
            last_task_id INTEGER,
            detail TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS failed_task_archive (
            id INTEGER DEFAULT nextval('seq_failed_task_archive') PRIMARY KEY,
            task_id INTEGER NOT NULL UNIQUE, campaign_type TEXT NOT NULL,
            campaign_id INTEGER NOT NULL, campaign_name TEXT NOT NULL DEFAULT '',
            msisdn TEXT NOT NULL, content TEXT NOT NULL DEFAULT '',
            final_status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            failure_class TEXT NOT NULL DEFAULT '', error_msg TEXT NOT NULL DEFAULT '',
            snapshot TEXT NOT NULL DEFAULT '{}', archived_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS template_versions (
            id INTEGER DEFAULT nextval('seq_template_versions') PRIMARY KEY,
            template_id INTEGER NOT NULL, version_no INTEGER NOT NULL,
            language TEXT NOT NULL, content TEXT NOT NULL DEFAULT '',
            source_addr TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
            change_note TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            UNIQUE(template_id, version_no)
        )""",
        """CREATE TABLE IF NOT EXISTS update_history (
            id INTEGER DEFAULT nextval('seq_update_history') PRIMARY KEY,
            run_id TEXT NOT NULL UNIQUE,
            action TEXT NOT NULL DEFAULT 'apply',
            version TEXT NOT NULL DEFAULT '',
            previous_version TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            operator TEXT NOT NULL DEFAULT 'system',
            detail TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER DEFAULT nextval('seq_users') PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'marketing',
            enabled INTEGER NOT NULL DEFAULT 1,
            last_login_at TEXT,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
    ]

    for stmt in ddl:
        try:
            conn.execute(stmt)
        except Exception as e:
            logger.warning("[DB] Skipping DDL: %s", e)

    logger.info(
        "[DB] init stage=base_schema elapsed=%.3fs",
        time.perf_counter() - started,
    )

    # A cutover-built database has already completed every expensive schema,
    # deduplication, normalization, backfill and index operation offline.  A
    # normal service start must not rediscover or revalidate multi-million-row
    # tables; the explicit marker is written only by the offline cutover
    # command after its verification gate passes.
    if _db_marker_completed(conn, _DB_CANONICAL_READY_MARKER):
        conn.commit()
        logger.info(
            "[DB] init_db fast path=canonical_ready elapsed=%.3fs",
            time.perf_counter() - started,
        )
        return

    migrations = [
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS batch_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS claim_token TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS task_date DATE DEFAULT CURRENT_DATE",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS sms_template_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS last_smpp_status INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS failure_class TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS receipt_retry_count INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS tps INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS tps_scope TEXT DEFAULT 'per_bind'",
        "ALTER TABLE smpp_configs ALTER COLUMN tps_scope SET DEFAULT 'per_bind'",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS priority INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS reconnect_max_seconds INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS delivery_receipt_mode TEXT DEFAULT 'optional'",
        "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS use_custom_directory BOOLEAN DEFAULT FALSE",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS use_custom_directory BOOLEAN DEFAULT FALSE",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS use_whitelist BOOLEAN DEFAULT TRUE",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS watch_dir TEXT DEFAULT ''",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS watch_source TEXT DEFAULT 'local'",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS sftp_host TEXT DEFAULT ''",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS sftp_port INTEGER DEFAULT 22",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS sftp_username TEXT DEFAULT ''",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS sftp_password TEXT DEFAULT ''",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS watch_filename_prefix TEXT DEFAULT 'Android_4G_Switch_Somali_'",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS phone_prefix TEXT DEFAULT ''",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS flash_sms BOOLEAN DEFAULT FALSE",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS active_whitelist_version_id INTEGER",
        "ALTER TABLE realtime_whitelist_versions ADD COLUMN IF NOT EXISTS retired_at TEXT",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS whitelist_version_id INTEGER",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS processing_started_at TEXT",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS last_progress_at TEXT",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS processing_phase TEXT DEFAULT 'queued'",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS processed_rows BIGINT DEFAULT 0",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS matched_rows BIGINT DEFAULT 0",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS current_chunk_index INTEGER DEFAULT 0",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS total_chunks INTEGER DEFAULT 0",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS worker_id TEXT DEFAULT ''",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS last_error_class TEXT DEFAULT ''",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS last_error_at TEXT",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS next_retry_at TEXT",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS max_retry_count INTEGER DEFAULT 4",
        "ALTER TABLE processed_files ADD COLUMN IF NOT EXISTS source_state_version BIGINT DEFAULT -1",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS whitelist_version_id INTEGER",
         "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS manifest_id BIGINT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS file_hash TEXT DEFAULT ''",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS processing_started_at TEXT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS last_progress_at TEXT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS processing_phase TEXT DEFAULT 'queued'",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS processed_rows BIGINT DEFAULT 0",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS matched_rows BIGINT DEFAULT 0",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS current_chunk_index INTEGER DEFAULT 0",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS worker_id TEXT DEFAULT ''",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS lease_token TEXT DEFAULT ''",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS lease_expires_at TEXT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS last_error_class TEXT DEFAULT ''",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS last_error_at TEXT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS next_retry_at TEXT",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS max_retry_count INTEGER DEFAULT 4",
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS state_version BIGINT DEFAULT 0",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS flash_sms BOOLEAN DEFAULT FALSE",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS transport_smpp_config_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS submit_sm_parts INTEGER DEFAULT 0",
        "ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS source_namespace TEXT DEFAULT 'legacy'",
        "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
        "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS flash_sms BOOLEAN DEFAULT FALSE",
         "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS active_manifest_id BIGINT",
        "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT DEFAULT 0",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT DEFAULT 0",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT DEFAULT 0",
         "ALTER TABLE batch_campaign_batches ADD COLUMN IF NOT EXISTS manifest_id BIGINT",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS window_size INTEGER DEFAULT 10",
        "ALTER TABLE batch_campaign_batches ADD COLUMN IF NOT EXISTS task_count INTEGER DEFAULT 0",
        "ALTER TABLE batch_campaign_batches ADD COLUMN IF NOT EXISTS delivered_count INTEGER DEFAULT 0",
        "ALTER TABLE batch_campaign_batches ADD COLUMN IF NOT EXISTS failed_count INTEGER DEFAULT 0",
   ]
    schema_migration_started = time.perf_counter()
    if not _db_marker_completed(conn, _DB_SCHEMA_COLUMNS_MARKER):
        schema_migration_complete = True
        for stmt in migrations:
            try:
                conn.execute(stmt)
            except Exception as e:
                schema_migration_complete = False
                logger.warning("[DB] Skipping migration: %s", e)
        if schema_migration_complete:
            _record_db_marker(conn, _DB_SCHEMA_COLUMNS_MARKER)
        logger.info(
            "[DB] schema migration complete=%s elapsed=%.3fs",
            schema_migration_complete,
            time.perf_counter() - schema_migration_started,
        )
    else:
        logger.debug("[DB] schema migration marker already complete")

    cleanup_started = time.perf_counter()
    if not _db_marker_completed(conn, _DB_REALTIME_CLEANUP_MARKER):
        cleanup_complete = True
        for stmt in [
            "DELETE FROM app_settings WHERE key='realtime_watch_dir'",
            "DELETE FROM app_settings WHERE key='realtime_watch_filename_prefix'",
            "ALTER TABLE realtime_campaigns DROP COLUMN IF EXISTS first_listen_offset",
        ]:
            try:
                conn.execute(stmt)
            except Exception as e:
                cleanup_complete = False
                logger.warning("[DB] Skipping obsolete realtime cleanup: %s", e)
        if cleanup_complete:
            _record_db_marker(conn, _DB_REALTIME_CLEANUP_MARKER)
        logger.info(
            "[DB] obsolete cleanup complete=%s elapsed=%.3fs",
            cleanup_complete,
            time.perf_counter() - cleanup_started,
        )
    else:
        logger.debug("[DB] obsolete realtime cleanup marker already complete")
    processed_files_started = time.perf_counter()
    processed_files_marker_completed = _db_marker_completed(
        conn, _DB_PROCESSED_FILES_MARKER
    )
    processed_files_migration_complete = True
    try:
        constraints = [] if processed_files_marker_completed else conn.execute(
            """SELECT constraint_column_names
               FROM duckdb_constraints()
               WHERE table_name='processed_files' AND constraint_type='UNIQUE'"""
        ).fetchall()
        has_hash_unique = any("file_hash" in set(row["constraint_column_names"] or []) for row in constraints)
        has_name_unique = any("filename" in set(row["constraint_column_names"] or []) for row in constraints)
        if has_hash_unique and not has_name_unique:
            conn.execute(
                """CREATE TABLE processed_files_new (
                    id INTEGER DEFAULT nextval('seq_processed_files') PRIMARY KEY,
                    campaign_type TEXT NOT NULL, campaign_id INTEGER NOT NULL,
                    filename TEXT NOT NULL, file_hash TEXT NOT NULL,
                    file_path TEXT NOT NULL DEFAULT '', file_size BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'detected', total_rows INTEGER NOT NULL DEFAULT 0,
                     created_tasks INTEGER NOT NULL DEFAULT 0, error_detail TEXT NOT NULL DEFAULT '',
                     whitelist_version_id INTEGER, processing_started_at TEXT, last_progress_at TEXT,
                     processing_phase TEXT NOT NULL DEFAULT 'queued', processed_rows BIGINT NOT NULL DEFAULT 0,
                     matched_rows BIGINT NOT NULL DEFAULT 0, current_chunk_index INTEGER NOT NULL DEFAULT 0,
                     total_chunks INTEGER NOT NULL DEFAULT 0, worker_id TEXT NOT NULL DEFAULT '',
                     retry_count INTEGER NOT NULL DEFAULT 0, last_error_class TEXT NOT NULL DEFAULT '',
                     last_error_at TEXT, next_retry_at TEXT, max_retry_count INTEGER NOT NULL DEFAULT 4,
                    source_state_version BIGINT NOT NULL DEFAULT -1,
                    detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
                    UNIQUE(campaign_type, campaign_id, filename)
                )"""
            )
            conn.execute(
                """INSERT INTO processed_files_new
                   (id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                     total_rows,created_tasks,error_detail,whitelist_version_id,processing_started_at,last_progress_at,
                     processing_phase,processed_rows,matched_rows,current_chunk_index,total_chunks,worker_id,retry_count,
                     last_error_class,last_error_at,next_retry_at,max_retry_count,source_state_version,detected_at,completed_at)
                   SELECT id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                           total_rows,created_tasks,error_detail,whitelist_version_id,processing_started_at,last_progress_at,
                           processing_phase,processed_rows,matched_rows,current_chunk_index,total_chunks,worker_id,retry_count,
                           last_error_class,last_error_at,next_retry_at,max_retry_count,source_state_version,detected_at,completed_at
                   FROM processed_files
                   QUALIFY row_number() OVER (
                       PARTITION BY campaign_type,campaign_id,filename
                       ORDER BY detected_at DESC,id DESC
                   )=1"""
            )
            conn.execute("DROP TABLE processed_files")
            conn.execute("ALTER TABLE processed_files_new RENAME TO processed_files")
    except Exception as e:
        processed_files_migration_complete = False
        logger.warning("[DB] Skipping processed_files uniqueness migration: %s", e)
    if not processed_files_marker_completed and processed_files_migration_complete:
        _record_db_marker(conn, _DB_PROCESSED_FILES_MARKER)
    logger.info(
        "[DB] processed_files migration complete=%s elapsed=%.3fs",
        processed_files_marker_completed or processed_files_migration_complete,
        time.perf_counter() - processed_files_started,
    )
    data_normalization_started = time.perf_counter()
    data_normalization_marker_completed = _db_marker_completed(
        conn, _DB_DATA_NORMALIZATION_MARKER
    )
    data_normalization_statements = [] if data_normalization_marker_completed else [
        "UPDATE users SET role='marketing' WHERE role='operator'",
        "UPDATE users SET role='analyst' WHERE role='viewer'",
        "UPDATE sms_tasks SET claim_token='' WHERE claim_token IS NULL",
        "UPDATE sms_tasks SET task_date=CAST(created_at AS DATE) WHERE task_date IS NULL",
        "UPDATE sms_tasks SET failure_class='' WHERE failure_class IS NULL",
        "UPDATE sms_tasks SET receipt_retry_count=0 WHERE receipt_retry_count IS NULL",
        "UPDATE smpp_configs SET tps=20 WHERE tps IS NULL",
        "UPDATE smpp_configs SET tps_scope='per_bind' WHERE tps_scope IS NULL OR tps_scope <> 'per_bind'",
        "UPDATE smpp_configs SET priority=100 WHERE priority IS NULL",
        "UPDATE smpp_configs SET reconnect_max_seconds=60 WHERE reconnect_max_seconds IS NULL",
        "UPDATE smpp_configs SET delivery_receipt_mode='optional' WHERE delivery_receipt_mode IS NULL OR delivery_receipt_mode=''",
        "UPDATE realtime_campaigns SET watch_filename_prefix='Android_4G_Switch_Somali_' WHERE watch_filename_prefix IS NULL OR watch_filename_prefix=''",
        """UPDATE batch_campaign_batches b SET
               task_count=s.task_count,
               delivered_count=s.delivered_count,
               failed_count=s.failed_count
           FROM (
               SELECT t.batch_id,COUNT(*) task_count,
                      SUM(CASE WHEN t.status IN ('delivered','accepted') THEN 1 ELSE 0 END) delivered_count,
                      SUM(CASE WHEN t.status IN ('failed','expired','cancelled') THEN 1 ELSE 0 END) failed_count
               FROM sms_tasks t
               JOIN batch_campaign_batches pending ON pending.id=t.batch_id
               WHERE COALESCE(pending.task_count,0)=0
               GROUP BY t.batch_id
           ) s
           WHERE b.id=s.batch_id AND COALESCE(b.task_count,0)=0""",
    ]
    data_normalization_complete = True
    for stmt in data_normalization_statements:
        try:
            conn.execute(stmt)
        except Exception as e:
            data_normalization_complete = False
            logger.warning("[DB] Skipping migration backfill: %s", e)

    if not data_normalization_marker_completed and data_normalization_complete:
        _record_db_marker(conn, _DB_DATA_NORMALIZATION_MARKER)
    logger.info(
        "[DB] data normalization complete=%s elapsed=%.3fs",
        data_normalization_marker_completed or data_normalization_complete,
        time.perf_counter() - data_normalization_started,
    )

    conn.commit()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sms_campaign ON sms_templates(campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_monitor_file ON monitor_files(filename, directory_path)",
        "CREATE INDEX IF NOT EXISTS idx_sms_log_msisdn ON sms_log(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_sms_log_status ON sms_log(status)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_status ON sms_tasks(status, next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_campaign ON sms_tasks(campaign_type, campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_dispatch_eligibility ON sms_tasks(campaign_type, campaign_id, dispatch_generation, status, next_attempt_at, created_at, id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_date ON sms_tasks(campaign_type, task_date, status, next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_message_id ON delivery_receipts(source_namespace,smsc_message_id)",
        "CREATE INDEX IF NOT EXISTS idx_exclusion_msisdn ON exclusion_users(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_created ON sms_tasks(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_keyset ON sms_tasks(id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_batch_status ON sms_tasks(batch_id,status)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_transport_smpp ON sms_tasks(transport_smpp_config_id)",
        "CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(campaign_type,campaign_id,file_hash)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_name_unique ON processed_files(campaign_type,campaign_id,filename)",
        # Receipt dedup is per-task: SMSC message ids may be reused across
        # tasks (simulator restarts its counter, some SMSCs recycle ids), so
        # the same (message_id, state) must be allowed on different tasks.
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_unique ON delivery_receipts(source_namespace,task_id,smsc_message_id,message_state)",
        "CREATE INDEX IF NOT EXISTS idx_receipt_task ON delivery_receipts(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_msg_task ON sms_task_message_ids(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_binding_lookup ON sms_message_bindings_v2(source_namespace,smsc_message_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_binding_task ON sms_message_bindings_v2(task_id,attempt_token)",
        "CREATE INDEX IF NOT EXISTS idx_sms_attempt_task ON sms_send_attempts(task_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_attempt_state ON sms_send_attempts(state,lease_until)",
        "CREATE INDEX IF NOT EXISTS idx_sms_parts_state ON sms_send_parts(attempt_token,state)",
        "CREATE INDEX IF NOT EXISTS idx_sms_fast_batch_state ON sms_fast_batches(state,updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_fast_item_task ON sms_fast_batch_items(task_id,state)",
        "CREATE INDEX IF NOT EXISTS idx_sms_fast_item_batch ON sms_fast_batch_items(batch_id,state)",
        "CREATE INDEX IF NOT EXISTS idx_capacity_policy_enabled ON campaign_capacity_policies(enabled,campaign_type,campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_batch_manifest_campaign ON batch_campaign_manifests(campaign_id,status,updated_at,id)",
        "CREATE INDEX IF NOT EXISTS idx_batch_batches_manifest ON batch_campaign_batches(manifest_id,status,batch_index)",
        "CREATE INDEX IF NOT EXISTS idx_realtime_reservation_expiry ON realtime_inbox_reservations(expires_at,campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_campaign_fairness_updated ON campaign_fairness_state(updated_at,campaign_type,campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_task_event_batches_campaign ON task_event_batches(campaign_type,campaign_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_chunks_status ON ingestion_chunks(status,lease_expires_at,id)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_chunks_batch ON ingestion_chunks(source_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_files_queue ON ingestion_files(status,next_retry_at,updated_at,id)",
        "CREATE INDEX IF NOT EXISTS idx_whitelist_versions_active ON realtime_whitelist_versions(campaign_id,status,version_no)",
        "CREATE INDEX IF NOT EXISTS idx_archive_partition ON task_archive_manifests(task_date,campaign_type,campaign_id,first_task_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_open ON system_events(resolved,severity,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_template_versions ON template_versions(template_id,version_no)",
        "CREATE INDEX IF NOT EXISTS idx_update_history_created ON update_history(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
    ]
    index_started = time.perf_counter()
    for idx in indexes:
        try:
            conn.execute(idx)
        except Exception as exc:
            logger.warning("[DB] Skipping index creation: %s", exc)
    _ensure_receipt_indexes(conn)
    logger.info(
        "[DB] index checks complete elapsed=%.3fs",
        time.perf_counter() - index_started,
    )

    try:
        marker = conn.execute(
            "SELECT value FROM app_settings WHERE key=?",
            ("sms_task_message_ids_backfill_v1",),
        ).fetchone()
        if not marker:
            conn.execute(
                """INSERT INTO sms_task_message_ids (smsc_message_id, task_id)
                   SELECT mid, id FROM (
                       SELECT id, unnest(from_json(smsc_message_ids, '["VARCHAR"]')) AS mid
                       FROM sms_tasks
                       WHERE smsc_message_ids IS NOT NULL
                         AND smsc_message_ids <> '[]'
                         AND json_valid(smsc_message_ids)
                   )
                   WHERE mid IS NOT NULL AND mid <> ''
                   ON CONFLICT (smsc_message_id) DO NOTHING"""
            )
            conn.execute(
                """INSERT INTO app_settings (key, value) VALUES (?, ?)
                   ON CONFLICT (key) DO UPDATE SET value=excluded.value""",
                ("sms_task_message_ids_backfill_v1", "completed"),
            )
            logger.info("[DB] Completed sms_task_message_ids backfill")
    except Exception as e:
        logger.warning("[DB] Skipping sms_task_message_ids backfill: %s", e)

    from services.runtime_service import normalize_detail_logging
    defaults = [
        ("batch_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "batch_campaigns")),
        ("realtime_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "realtime_campaigns")),
        ("system_mode", "production"),
        ("log_level", "INFO"),
        ("detail_logging", normalize_detail_logging(os.environ.get("MC_DETAIL_LOGGING", "0"))),
        ("sms_task_retention_days", "3"),
        ("realtime_file_retention_days", "1"),
        ("realtime_output_retention_days", "30"),
        ("realtime_match_chunk_size", os.environ.get("MC_REALTIME_MATCH_CHUNK_SIZE", "5000")),
        ("realtime_whitelist_build_chunk_size", os.environ.get("MC_REALTIME_WHITELIST_BUILD_CHUNK", "10000")),
        ("realtime_whitelist_build_memory_limit", os.environ.get("MC_REALTIME_WHITELIST_BUILD_MEMORY_LIMIT", "4GB")),
        ("realtime_whitelist_build_threads", os.environ.get("MC_REALTIME_WHITELIST_BUILD_THREADS", "1")),
        ("realtime_index_build_timeout", os.environ.get("MC_REALTIME_INDEX_BUILD_TIMEOUT", "7200")),
        ("realtime_whitelist_retention_days", os.environ.get("MC_REALTIME_WHITELIST_RETENTION_DAYS", "7")),
    ]
    for k, v in defaults:
        try:
            conn.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)", (k, v))
        except Exception:
            pass

    conn.commit()
    logger.info(
        "[DB] init_db complete elapsed=%.3fs",
        time.perf_counter() - started,
    )


def init_runtime_db() -> dict:
    """Validate the prepared canonical store without running maintenance.

    ``init_db`` remains an offline schema/migration operation for tests and
    the stopped-service cutover tool.  The web process must never repeat its
    data normalization, backfills, or index checks while starting against a
    multi-gigabyte store.
    """

    path = str(DATABASE_FILE)
    strict = _env_bool(os.environ.get("MC_REQUIRE_CANONICAL_STORE"), False)
    if strict and not str(os.environ.get("MC_CONTROL_DB_PATH") or "").strip():
        raise RuntimeError(
            "MC_CONTROL_DB_PATH is required in canonical production mode; "
            "the legacy DUCKDB_PATH selector is not a runtime fallback"
        )
    required = {
        "app_settings", "users", "smpp_configs", "campaigns",
        "batch_campaigns", "realtime_campaigns", "ingestion_files",
    }
    try:
        conn = get_connection().set_operation("database.runtime_schema_check")
    except Exception as exc:
        if not strict and (path == ":memory:" or not os.path.exists(path)):
            init_db()
            return {"path": path, "ready": True, "initialized": True}
        raise RuntimeError(
            f"canonical control database is unavailable: {path}; "
            "run tools/cutover_vnext_store.py before starting the service"
        ) from exc
    try:
        tables = {
            str(row[0]) for row in conn.execute(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_schema='main' AND table_type='BASE TABLE'"""
            ).fetchall()
        }
        missing = sorted(required.difference(tables))
        if missing:
            if not strict:
                conn.close()
                init_db()
                return {"path": path, "ready": True, "initialized": True}
            raise RuntimeError(
                f"canonical control database is incomplete ({', '.join(missing)}); "
                "run tools/cutover_vnext_store.py while the service is stopped"
            )
        marker = conn.execute(
            "SELECT value FROM app_settings WHERE key=?",
            ("db_canonical_ready_v1",),
        ).fetchone()
        ready = bool(marker and str(marker[0]) == "completed")
        if strict and not ready:
            raise RuntimeError(
                f"canonical control database has not completed migration: {path}; "
                "run tools/cutover_vnext_store.py before starting the service"
            )
        logger.info(
            "[DB] runtime canonical store ready path=%s marker=%s "
            "(startup maintenance skipped)",
            path,
            ready,
        )
        return {"path": path, "ready": ready, "initialized": False}
    finally:
        conn.close()
