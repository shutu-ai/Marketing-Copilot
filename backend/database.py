import os
import logging
import threading
import time
from contextlib import contextmanager

import duckdb

logger = logging.getLogger(__name__)

# Serialize mutations, but allow independent read connections to run while the
# SMS writer is committing. Every WrappedConnection is still protected against
# simultaneous use by multiple Python threads.
#
# The lock is re-entrant because a writer transaction holds it across all of
# its statements and commit/rollback.  Locking only one execute() at a time
# allowed two transactions to interleave and made DuckDB's optimistic
# write-write conflicts much more likely.
_write_lock = threading.RLock()
_write_state_lock = threading.Lock()
_write_holder = {
    "thread_id": None,
    "thread_name": "",
    "operation": "",
    "acquired_at": 0.0,
}

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


def is_transient_write_error(error: BaseException) -> bool:
    """Return whether *error* is safe to retry as a whole write transaction.

    DuckDB can surface contention as either the Python-side writer-gate
    timeout or a transaction conflict.  Keep this predicate in one place so
    HTTP handlers, upload services and background workers classify the same
    failures consistently.  We deliberately do not classify arbitrary
    ``TimeoutError`` instances (for example a whitelist build timeout) as a
    database failure.
    """
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            text = str(current).lower()
            if "duckdb write lock timeout" in text or "database write lock" in text:
                return True
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
            "could not set lock on file",
            "database is locked",
            "database busy",
            "write-write conflict",
            "transaction conflict",
        )):
            return True
        current = current.__cause__ or current.__context__
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


def write_transaction(callback, attempts: int = 3, operation: str = "write",
                      lock_timeout=None, base_delay: float = 0.05):
    """Run a complete short write transaction with bounded retry.

    The callback must not call ``commit`` or ``close``.  Retrying the whole
    transaction is important: after a DuckDB write-write conflict, replaying
    only the failed statement can leave earlier statements from the logical
    operation missing or partially committed.
    """
    attempts = max(1, min(5, int(attempts or 1)))
    for attempt in range(1, attempts + 1):
        conn = None
        transaction_started = time.perf_counter()
        transaction_result = "error"
        try:
            # Connection acquisition can itself fail while DuckDB is replaying
            # a WAL or another process briefly owns the file lock. Keep it
            # inside the retry scope so Web writes do not fail before the
            # transaction retry policy gets a chance to run.
            conn = get_connection()
            if hasattr(conn, "set_operation"):
                conn.set_operation(operation)
            conn.write_lock_timeout = lock_timeout
            conn.execute("BEGIN TRANSACTION")
            result = callback(conn)
            conn.commit()
            transaction_result = "success"
            return result
        except BaseException as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    logger.debug("DuckDB rollback failed after write error", exc_info=True)
            if not is_transient_write_error(exc) or attempt >= attempts:
                raise
            _inc_database_metric(
                "marketing_copilot_database_write_transaction_retries_total",
                labels={"operation": operation},
            )
            logger.warning(
                "DuckDB write transaction busy; retrying operation=%s attempt=%s/%s",
                operation, attempt + 1, attempts,
            )
            time.sleep(max(0.0, float(base_delay)) * attempt)
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


@contextmanager
def write_gate(operation: str = "write", timeout=None):
    """Hold the process writer gate across a multi-step file operation.

    Some operations, such as an online backup, must checkpoint and then copy
    the database after the DuckDB connection is closed (Windows keeps the
    file handle locked while a connection is open).  This helper keeps other
    application writers fenced during that gap without holding a DuckDB file
    handle itself.
    """
    _acquire_write_lock(operation, timeout)
    try:
        yield
    finally:
        _clear_write_holder()
        _write_lock.release()


def write_lock_status() -> dict:
    """Return non-sensitive diagnostics for the process-wide writer gate."""
    with _write_state_lock:
        thread_id = _write_holder["thread_id"]
        thread_name = _write_holder["thread_name"]
        operation = _write_holder["operation"]
        acquired_at = _write_holder["acquired_at"]
    age = max(0.0, time.monotonic() - acquired_at) if acquired_at else 0.0
    return {
        "held": bool(thread_id),
        "thread_id": thread_id,
        "thread_name": thread_name,
        "operation": operation,
        "age_seconds": round(age, 3),
    }


def duckdb_connection_config() -> dict:
    """Return offline-safe DuckDB settings for the application database."""
    extension_directory = os.environ.get("MC_DUCKDB_EXTENSION_DIR", "").strip()
    if not extension_directory:
        extension_directory = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "offline", "extensions"
        )
    if os.path.isdir(extension_directory):
        return {"extension_directory": extension_directory}
    return {}

DATABASE_FILE = os.environ.get(
    "DUCKDB_PATH",
    os.path.join(os.path.dirname(__file__), "marketing_copilot.duckdb")
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
        # Keep the business operation captured at first gate acquisition.
        # Later execute/commit phases must not relabel a long transaction as
        # merely ``commit`` in contention diagnostics.
        self._write_operation = ""
        self.write_lock_timeout = None

    def set_operation(self, operation: str):
        """Attach a bounded business operation name to subsequent DB writes."""
        self._operation = str(operation or "")
        return self

    def _ensure_write_lock(self, operation: str) -> float:
        operation = self._operation or operation
        if self._write_lock_held:
            _update_write_holder(self._write_operation or operation)
            return 0.0
        waited = _acquire_write_lock(operation, self.write_lock_timeout)
        self._write_lock_held = True
        self._write_operation = operation
        return waited

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
        write_wait = 0.0
        with self._connection_lock:
            if not read_only:
                write_wait = self._ensure_write_lock("execute")
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
            _observe_database_metric(
                "marketing_copilot_database_rollback_seconds",
                time.perf_counter() - started,
            )

    def close(self):
        try:
            with self._connection_lock:
                if self._write_lock_held:
                    self._rollback_underlying()
                    self._release_write_lock()
                self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None and self._write_lock_held:
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


def get_connection():
    return WrappedConnection(
        duckdb.connect(DATABASE_FILE, config=duckdb_connection_config())
    )


def reconnect(path: str = None):
    global DATABASE_FILE
    if path:
        DATABASE_FILE = path


def set_database_file(path: str):
    global DATABASE_FILE
    DATABASE_FILE = path


def get_database_file() -> str:
    return DATABASE_FILE


def init_db():
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

    try:
        conn.execute("ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS batch_id INTEGER")
    except Exception as e:
        logger.warning("[DB] Skipping sms_tasks.batch_id migration: %s", e)
    migrations = [
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS claim_token TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS task_date DATE DEFAULT CURRENT_DATE",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS sms_template_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS last_smpp_status INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS failure_class TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS receipt_retry_count INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS tps INTEGER",
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
        "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS whitelist_version_id INTEGER",
         "ALTER TABLE ingestion_files ADD COLUMN IF NOT EXISTS manifest_id BIGINT",
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
    for stmt in migrations:
        try:
            conn.execute(stmt)
        except Exception as e:
            logger.warning("[DB] Skipping migration: %s", e)
    for stmt in [
        "DELETE FROM app_settings WHERE key='realtime_watch_dir'",
        "DELETE FROM app_settings WHERE key='realtime_watch_filename_prefix'",
        "ALTER TABLE realtime_campaigns DROP COLUMN IF EXISTS first_listen_offset",
    ]:
        try:
            conn.execute(stmt)
        except Exception as e:
            logger.warning("[DB] Skipping obsolete realtime cleanup: %s", e)
    try:
        constraints = conn.execute(
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
                    detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
                    UNIQUE(campaign_type, campaign_id, filename)
                )"""
            )
            conn.execute(
                """INSERT INTO processed_files_new
                   (id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                     total_rows,created_tasks,error_detail,whitelist_version_id,processing_started_at,last_progress_at,
                     processing_phase,processed_rows,matched_rows,current_chunk_index,total_chunks,worker_id,retry_count,
                     last_error_class,last_error_at,next_retry_at,max_retry_count,detected_at,completed_at)
                   SELECT id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                           total_rows,created_tasks,error_detail,whitelist_version_id,processing_started_at,last_progress_at,
                           processing_phase,processed_rows,matched_rows,current_chunk_index,total_chunks,worker_id,retry_count,
                           last_error_class,last_error_at,next_retry_at,max_retry_count,detected_at,completed_at
                   FROM processed_files
                   QUALIFY row_number() OVER (
                       PARTITION BY campaign_type,campaign_id,filename
                       ORDER BY detected_at DESC,id DESC
                   )=1"""
            )
            conn.execute("DROP TABLE processed_files")
            conn.execute("ALTER TABLE processed_files_new RENAME TO processed_files")
    except Exception as e:
        logger.warning("[DB] Skipping processed_files uniqueness migration: %s", e)
    for stmt in [
        "UPDATE users SET role='marketing' WHERE role='operator'",
        "UPDATE users SET role='analyst' WHERE role='viewer'",
        "UPDATE sms_tasks SET claim_token='' WHERE claim_token IS NULL",
        "UPDATE sms_tasks SET task_date=CAST(created_at AS DATE) WHERE task_date IS NULL",
        "UPDATE sms_tasks SET failure_class='' WHERE failure_class IS NULL",
        "UPDATE sms_tasks SET receipt_retry_count=0 WHERE receipt_retry_count IS NULL",
        "UPDATE smpp_configs SET tps=20 WHERE tps IS NULL",
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
    ]:
        try:
            conn.execute(stmt)
        except Exception as e:
            logger.warning("[DB] Skipping migration backfill: %s", e)

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
        "DROP INDEX IF EXISTS idx_delivery_message_id",
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
        "DROP INDEX IF EXISTS idx_receipt_unique",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_unique ON delivery_receipts(source_namespace,task_id,smsc_message_id,message_state)",
        "CREATE INDEX IF NOT EXISTS idx_receipt_task ON delivery_receipts(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_msg_task ON sms_task_message_ids(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_binding_lookup ON sms_message_bindings_v2(source_namespace,smsc_message_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_binding_task ON sms_message_bindings_v2(task_id,attempt_token)",
        "CREATE INDEX IF NOT EXISTS idx_sms_attempt_task ON sms_send_attempts(task_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_attempt_state ON sms_send_attempts(state,lease_until)",
        "CREATE INDEX IF NOT EXISTS idx_sms_parts_state ON sms_send_parts(attempt_token,state)",
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
    for idx in indexes:
        try:
            conn.execute(idx)
        except Exception:
            pass

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
