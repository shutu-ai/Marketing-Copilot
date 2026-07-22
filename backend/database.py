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
    "upload_history": "seq_upload_history",
    "whitelist_users": "seq_whitelist_users",
    "campaigns": "seq_campaigns",
    "sms_templates": "seq_sms_templates",
    "monitor_configs": "seq_monitor_configs",
    "monitor_files": "seq_monitor_files",
    "rabbitmq_configs": "seq_rabbitmq_configs",
    "whitelist_subscriptions": "seq_whitelist_subscriptions",
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
            print(f"[DB] Skipping sequence {seq}: {e}")

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
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS upload_history (
            id INTEGER DEFAULT nextval('seq_upload_history') PRIMARY KEY,
            filename TEXT NOT NULL,
            total_rows INTEGER NOT NULL DEFAULT 0,
            success_rows INTEGER NOT NULL DEFAULT 0,
            failed_rows INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            import_mode TEXT NOT NULL DEFAULT 'append',
            error_detail TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
        )""",
        """CREATE TABLE IF NOT EXISTS whitelist_users (
            id INTEGER DEFAULT nextval('seq_whitelist_users') PRIMARY KEY,
            msisdn TEXT NOT NULL, region TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
            upload_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (upload_id) REFERENCES upload_history(id)
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
        """CREATE TABLE IF NOT EXISTS whitelist_subscriptions (
            id INTEGER DEFAULT nextval('seq_whitelist_subscriptions') PRIMARY KEY,
            whitelist_id INTEGER NOT NULL, topic_pattern TEXT NOT NULL,
            campaign_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (whitelist_id) REFERENCES whitelist_users(id)
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
            sms_template_id INTEGER DEFAULT NULL,
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
            sms_template_id INTEGER DEFAULT NULL,
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
            content TEXT NOT NULL,
            source_addr TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL DEFAULT '{}',
            source_file TEXT NOT NULL DEFAULT '',
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
        """CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER DEFAULT nextval('seq_processed_files') PRIMARY KEY,
            campaign_type TEXT NOT NULL, campaign_id INTEGER NOT NULL,
            filename TEXT NOT NULL, file_hash TEXT NOT NULL,
            file_path TEXT NOT NULL DEFAULT '', file_size BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'detected', total_rows INTEGER NOT NULL DEFAULT 0,
            created_tasks INTEGER NOT NULL DEFAULT 0, error_detail TEXT NOT NULL DEFAULT '',
            detected_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP), completed_at TEXT,
            UNIQUE(campaign_type, campaign_id, file_hash)
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
    ]

    for stmt in ddl:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"[DB] Skipping DDL: {e}")

    try:
        conn.execute("ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS batch_id INTEGER")
    except Exception as e:
        print(f"[DB] Skipping sms_tasks.batch_id migration: {e}")
    migrations = [
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS claim_token TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS last_smpp_status INTEGER",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS failure_class TEXT",
        "ALTER TABLE sms_tasks ADD COLUMN IF NOT EXISTS receipt_retry_count INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS tps INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS priority INTEGER",
        "ALTER TABLE smpp_configs ADD COLUMN IF NOT EXISTS reconnect_max_seconds INTEGER",
    ]
    for stmt in migrations:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"[DB] Skipping migration: {e}")
    for stmt in [
        "UPDATE sms_tasks SET claim_token='' WHERE claim_token IS NULL",
        "UPDATE sms_tasks SET failure_class='' WHERE failure_class IS NULL",
        "UPDATE sms_tasks SET receipt_retry_count=0 WHERE receipt_retry_count IS NULL",
        "UPDATE smpp_configs SET tps=20 WHERE tps IS NULL",
        "UPDATE smpp_configs SET priority=100 WHERE priority IS NULL",
        "UPDATE smpp_configs SET reconnect_max_seconds=60 WHERE reconnect_max_seconds IS NULL",
    ]:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"[DB] Skipping migration backfill: {e}")

    conn.commit()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_whitelist_msisdn ON whitelist_users(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_whitelist_upload ON whitelist_users(upload_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_campaign ON sms_templates(campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_monitor_file ON monitor_files(filename, directory_path)",
        "CREATE INDEX IF NOT EXISTS idx_sms_log_msisdn ON sms_log(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_sms_log_status ON sms_log(status)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_status ON sms_tasks(status, next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS idx_sms_tasks_campaign ON sms_tasks(campaign_type, campaign_id)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_message_id ON delivery_receipts(smsc_message_id)",
        "CREATE INDEX IF NOT EXISTS idx_exclusion_msisdn ON exclusion_users(msisdn)",
        "CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(campaign_type,campaign_id,file_hash)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_unique ON delivery_receipts(smsc_message_id,message_state)",
        "CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_events_open ON system_events(resolved,severity,created_at)",
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_template_versions ON template_versions(template_id,version_no)",
    ]
    for idx in indexes:
        try:
            conn.execute(idx)
        except Exception:
            pass

    defaults = [
        ("batch_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "batch_campaigns")),
        ("realtime_base_dir", os.path.join(os.path.dirname(__file__), "uploads", "realtime_campaigns")),
        ("system_mode", "production"),
    ]
    for k, v in defaults:
        try:
            conn.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)", (k, v))
        except Exception:
            pass

    conn.commit()
