import os
import threading
import logging

import duckdb

logger = logging.getLogger(__name__)

_lock = threading.Lock()

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
    "failed_task_archive": "seq_failed_task_archive",
    "template_versions": "seq_template_versions",
    "update_history": "seq_update_history",
    "users": "seq_users",
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

    def execute(self, sql, params=None):
        sql = sql.replace("datetime('now','localtime')", "CURRENT_TIMESTAMP")
        self._last_sql = sql
        with _lock:
            if params is not None:
                if not isinstance(params, (list, tuple)):
                    params = [params]
                self.conn.execute(sql, params)
            else:
                self.conn.execute(sql)
        return self

    def fetchone(self):
        row = self.conn.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in self.conn.description] if self.conn.description else []
        return DuckRow(cols, list(row))

    def fetchall(self):
        rows = self.conn.fetchall()
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
        with _lock:
            self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def get_connection():
    return WrappedConnection(duckdb.connect(DATABASE_FILE))


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
           created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS batch_campaign_batches (
            id INTEGER DEFAULT nextval('seq_batch_campaign_batches') PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            batch_index INTEGER NOT NULL DEFAULT 0,
            start_row INTEGER NOT NULL DEFAULT 0,
            end_row INTEGER NOT NULL DEFAULT 0,
            total_count INTEGER NOT NULL DEFAULT 0,
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
            customer_list_filename TEXT DEFAULT '',
            customer_list_path TEXT DEFAULT '',
            row_count INTEGER DEFAULT 0,
            campaign_dir TEXT DEFAULT '',
            use_custom_directory BOOLEAN NOT NULL DEFAULT FALSE,
            watch_dir TEXT NOT NULL DEFAULT '',
            watch_filename_prefix TEXT NOT NULL DEFAULT 'Android_4G_Switch_Somali_',
            sms_template_id INTEGER DEFAULT NULL,
            smpp_config_id INTEGER DEFAULT NULL,
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
            delivered_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS delivery_receipts (
            id INTEGER DEFAULT nextval('seq_delivery_receipts') PRIMARY KEY,
            task_id INTEGER,
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
        """CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER DEFAULT nextval('seq_processed_files') PRIMARY KEY,
            campaign_type TEXT NOT NULL, campaign_id INTEGER NOT NULL,
            filename TEXT NOT NULL, file_hash TEXT NOT NULL,
            file_path TEXT NOT NULL DEFAULT '', file_size BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'detected', total_rows INTEGER NOT NULL DEFAULT 0,
            created_tasks INTEGER NOT NULL DEFAULT 0, error_detail TEXT NOT NULL DEFAULT '',
            detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
            UNIQUE(campaign_type, campaign_id, filename)
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
            role TEXT NOT NULL DEFAULT 'operator',
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
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS watch_dir TEXT DEFAULT ''",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS watch_filename_prefix TEXT DEFAULT 'Android_4G_Switch_Somali_'",
       "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS phone_prefix TEXT DEFAULT ''",
        "ALTER TABLE realtime_campaigns ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
        "ALTER TABLE batch_campaigns ADD COLUMN IF NOT EXISTS smpp_config_id INTEGER",
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
                    detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
                    UNIQUE(campaign_type, campaign_id, filename)
                )"""
            )
            conn.execute(
                """INSERT INTO processed_files_new
                   (id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                    total_rows,created_tasks,error_detail,detected_at,completed_at)
                   SELECT id,campaign_type,campaign_id,filename,file_hash,file_path,file_size,status,
                          total_rows,created_tasks,error_detail,detected_at,completed_at
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
        "UPDATE sms_tasks SET claim_token='' WHERE claim_token IS NULL",
        "UPDATE sms_tasks SET task_date=CAST(created_at AS DATE) WHERE task_date IS NULL",
        "UPDATE sms_tasks SET failure_class='' WHERE failure_class IS NULL",
        "UPDATE sms_tasks SET receipt_retry_count=0 WHERE receipt_retry_count IS NULL",
        "UPDATE smpp_configs SET tps=20 WHERE tps IS NULL",
        "UPDATE smpp_configs SET priority=100 WHERE priority IS NULL",
        "UPDATE smpp_configs SET reconnect_max_seconds=60 WHERE reconnect_max_seconds IS NULL",
        "UPDATE smpp_configs SET delivery_receipt_mode='optional' WHERE delivery_receipt_mode IS NULL OR delivery_receipt_mode=''",
        "UPDATE realtime_campaigns SET watch_filename_prefix='Android_4G_Switch_Somali_' WHERE watch_filename_prefix IS NULL OR watch_filename_prefix=''",
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
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_date ON sms_tasks(campaign_type, task_date, status, next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_message_id ON delivery_receipts(smsc_message_id)",
        "CREATE INDEX IF NOT EXISTS idx_exclusion_msisdn ON exclusion_users(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(campaign_type,campaign_id,file_hash)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_name_unique ON processed_files(campaign_type,campaign_id,filename)",
        # Receipt dedup is per-task: SMSC message ids may be reused across
        # tasks (simulator restarts its counter, some SMSCs recycle ids), so
        # the same (message_id, state) must be allowed on different tasks.
        "DROP INDEX IF EXISTS idx_receipt_unique",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_unique ON delivery_receipts(task_id,smsc_message_id,message_state)",
        "CREATE INDEX IF NOT EXISTS idx_receipt_task ON delivery_receipts(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_msg_task ON sms_task_message_ids(task_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id,created_at)",
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
        conn.execute(
            """INSERT OR IGNORE INTO sms_task_message_ids (smsc_message_id, task_id)
               SELECT mid, id FROM (
                   SELECT id, unnest(from_json(smsc_message_ids, '["VARCHAR"]')) AS mid
                   FROM sms_tasks
                   WHERE smsc_message_ids IS NOT NULL AND smsc_message_ids <> '[]'
               )
               WHERE mid IS NOT NULL AND mid <> ''"""
        )
    except Exception as e:
        logger.warning("[DB] Skipping sms_task_message_ids backfill: %s", e)

    from services.runtime_service import normalize_detail_logging
    defaults = [
        ("batch_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "batch_campaigns")),
        ("realtime_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "realtime_campaigns")),
        ("system_mode", "production"),
        ("log_level", "INFO"),
        ("detail_logging", normalize_detail_logging(os.environ.get("MC_DETAIL_LOGGING", "0"))),
    ]
    for k, v in defaults:
        try:
            conn.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)", (k, v))
        except Exception:
            pass

    conn.commit()
