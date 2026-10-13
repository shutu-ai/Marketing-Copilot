# Linux deployment

## SLES 12 SP5 offline package (Python 3.11.5, no Docker or Node.js on target)

Run the builder in an internet-connected **SLES 12 SP5-compatible WSL/Linux
environment** (glibc <= 2.22), with Python 3.11, compiler build prerequisites,
Node.js/npm and curl. For example, import and use an approved SLES 12 SP5 rootfs
in WSL. Do not use a current Ubuntu WSL: its newer glibc would make the Python
binary unusable on SLES 12. Node is used only to create `frontend/dist`; the
generated archive contains static frontend files only, so the disconnected SLES
target does not install or run Node.js/npm.

The project keeps a single packaging script for the application package. The
application package is deployed separately from the standalone Python runtime:

```bash
cd /path/to/MarketingCopilot
bash deploy/build-sles12sp5-system-python-manual.sh --output release
```

It produces `marketing-copilot-sles12sp5-system-python-<ts>.tar.gz` containing
backend source, frontend dist, the offline wheelhouse and `run.sh`, with
`backend/release.json` version metadata for the online-update feature. The same
tar.gz can be deployed manually on the target (see
`SLES12SP5-system-python-manual-deployment.md`) or uploaded through
「系统设置 → 系统更新」as the update package.

The build requires an existing offline wheelhouse archive
(`marketing-copilot-sles12sp5-wheelhouse-*.tar.gz`). It is looked up in this
order: `--wheelhouse FILE` → `cache/wheelhouse/` (stable, git-ignored) →
`release/`. Keep the archive in `cache/wheelhouse/` so builds do not depend on
the scratch `release/` directory. When Python dependencies change, regenerate
it with the packaging skill helper
(`sles12sp5-deploy-package/scripts/prepare_wheelhouse_wsl.ps1`), or recover the
removed `deploy/build-sles12sp5-wheelhouse.sh` from git history.

The archive does **not** bundle a Python runtime. Install the separate
`sles12sp5-python-3.11.5-*.tar.gz` package first; the application defaults to
`/opt/marketing-copilot/python/bin/python3.11`. Wheels are
`manylinux2014_x86_64` (glibc 2.17), compatible with SLES 12 SP5 glibc 2.22.

Use Python 3.11 or 3.12. The pinned `pandas==2.2.2` dependency is not suitable for a Python 3.13 installation without a local C/C++ build toolchain.

```bash
sudo useradd --system --home /opt/marketing-copilot --shell /usr/sbin/nologin marketing-copilot
sudo mkdir -p /opt/marketing-copilot
sudo cp -a backend frontend deploy /opt/marketing-copilot/
sudo chown -R marketing-copilot:marketing-copilot /opt/marketing-copilot

sudo -u marketing-copilot bash -lc 'cd /opt/marketing-copilot/frontend && npm ci && npm run build'

sudo -u marketing-copilot python3.12 -m venv /opt/marketing-copilot/venv
sudo -u marketing-copilot /opt/marketing-copilot/venv/bin/pip install -r /opt/marketing-copilot/backend/requirements.txt

sudo install -d -o marketing-copilot -g marketing-copilot -m 0750 \
  /opt/marketing-copilot/data /opt/marketing-copilot/logs \
  /opt/marketing-copilot/run /opt/marketing-copilot/backups /opt/marketing-copilot/updates

sudo cp /opt/marketing-copilot/deploy/marketing-copilot.env.example /etc/marketing-copilot.env
sudo cp /opt/marketing-copilot/deploy/marketing-copilot.service /etc/systemd/system/
sudo cp /opt/marketing-copilot/deploy/marketing-copilot-backup.service /etc/systemd/system/
sudo cp /opt/marketing-copilot/deploy/marketing-copilot-backup.timer /etc/systemd/system/
sudo chmod 0640 /etc/marketing-copilot.env
sudo chown root:marketing-copilot /etc/marketing-copilot.env
sudo systemctl daemon-reload
sudo systemctl enable --now marketing-copilot.service marketing-copilot-backup.timer
```

The deployment uses only Python, Uvicorn and systemd. It does not require Docker. The service uses one Uvicorn process because DuckDB requires one application writer; `MC_TASK_WORKERS` controls the internal SMS worker threads. `StateDirectory`, `LogsDirectory`, and `RuntimeDirectory` create writable directories for the service account.

Verify after installation:

```bash
systemctl status marketing-copilot.service --no-pager
systemctl status marketing-copilot-backup.timer --no-pager
curl --fail http://127.0.0.1:19090/api/ready
curl --fail http://127.0.0.1:19090/api/health
curl --fail http://127.0.0.1:19090/metrics
sudo -u marketing-copilot /opt/marketing-copilot/venv/bin/python -m tools.backup_database
sudo -u marketing-copilot bash -c 'set -a; source /etc/marketing-copilot.env; cd /opt/marketing-copilot/backend; /opt/marketing-copilot/venv/bin/python -m tools.deployment_check --require-linux'
```

## Capacity monitoring

Capacity monitoring is built into the Python application:

- `/capacity` provides the operator dashboard.
- `/api/metrics/status` returns Worker, queue and SMSC pool state as JSON.
- `/metrics` exposes Prometheus-compatible text for an existing external monitoring service.
- `/api/smpp/stats` and `/api/metrics/status` expose independent transport
  measurements: `send_message_tps_1m` is the rate at which `send_message()`
  wrote submit PDUs to the socket, while `submit_sm_resp_tps_1m` is the rate
  at which the SMSC responses completed. These are intentionally separate
  from the durable task-submission rate.
- `MC_DUCKDB_WRITE_LOCK_TIMEOUT_SECONDS` bounds the application-level writer
  gate. DuckDB-mode writes are serialized for the duration of each transaction
  to prevent optimistic write-write conflicts; long read-heavy realtime
  matching is kept outside the final task-write transaction.
- `MC_DUCKDB_READ_RETRIES` and `MC_DUCKDB_READ_RETRY_DELAY_MS` provide bounded
  retries for transient DuckDB file/WAL/lock read failures. Web/API reads return
  a retryable 503 when the database remains unavailable instead of leaving a
  connection or returning an unstructured server error.
- `MC_SMS_WRITE_TRANSACTION_SIZE` bounds each DuckDB transaction used by the
  asynchronous SMS result writer. It is intentionally independent of
  `MC_SMS_WRITE_BATCH_SIZE`, so a large collection window does not create one
  long writer lock hold. Transient lock failures stay in the durable result
  journal and are retried after restart.
- `MC_SMS_WRITE_TRANSIENT_LOG_INTERVAL_SECONDS` rate-limits repeated transient
  lock-contention warnings (default `60`); it does not disable retries.
- `MC_BATCH_CANCEL_BATCH_SIZE` and `MC_BATCH_CANCEL_CAMPAIGNS_PER_RUN` bound
  overdue-task cleanup to short, rotating transactions (defaults `250` rows
  across `4` campaigns per scheduler run).
- `/api/metrics/status` includes `database.write_lock` and the metrics gauges
  `marketing_copilot_database_write_lock_held` and
  `marketing_copilot_database_write_lock_age_seconds` for identifying a long
  writer transaction.
- The Python scheduler evaluates queue depth, failure rate and SMSC availability every minute.
- Open and resolved alerts are stored in the operations exception center.
- When `MC_ALERT_WEBHOOK_URL` is set, Python sends JSON `firing` and `resolved` notifications directly without Alertmanager.

The webhook receiver must return an HTTP 2xx response. Keep the URL empty when notifications are not required.
