# Manual offline deployment with standalone SLES12SP5 Python 3.11.5

Install the separate `sles12sp5-python-3.11.5-*.tar.gz` runtime package first.
This application package does not include Python or OpenSSL. It contains the
Marketing Copilot source, frontend dist, offline wheels, and launcher only.
The default runtime is `/opt/marketing-copilot/python/bin/python3.11`.
Systemd service files, Docker, Podman, Node.js, and npm are not required on the
target.

## 1. Verify the standalone Python runtime

```bash
/opt/marketing-copilot/python/bin/python3.11 - <<'PY'
import ssl, sys
assert sys.version_info[:3] == (3, 11, 5), sys.version
assert ssl.OPENSSL_VERSION_INFO >= (1, 1, 1), ssl.OPENSSL_VERSION
print(sys.version)
print(ssl.OPENSSL_VERSION)
PY
```

The command must succeed. Python must also have the `venv` module. If the
runtime is installed elsewhere, set `MC_PYTHON_BIN` in `marketing-copilot.env`
before running `run.sh`.

## 2. Verify and unpack

```bash
sha256sum -c checksums.sha256
mkdir -p /opt/marketing-copilot-manual
cp -a backend frontend offline marketing-copilot.env.example run.sh README.md /opt/marketing-copilot-manual/
cd /opt/marketing-copilot-manual
```

## 3. Configure

```bash
cd /opt/marketing-copilot-manual
cp marketing-copilot.env.example marketing-copilot.env
```

The packaged `marketing-copilot.env.example` is preconfigured for a single
directory deployment: data, logs, locks, backups, and frontend files all live
under `$APP_HOME` subdirectories. `$APP_HOME` is the directory containing
`run.sh`. No path edits are required unless you want to move the data or log
directories elsewhere.

Logging is controlled by two settings:

- `MC_LOG_LEVEL`: application log level, default `INFO`. Use `WARNING` in
  production to keep only warnings and errors.
- `MC_DETAIL_LOGGING`: set to `1` to enable per-message detail logs (SMS body,
  RabbitMQ payload). Default `0` keeps these high-frequency logs off.

The admin UI requires a login (default `admin / Huawei@123`, change it in
production via `MC_ADMIN_PASSWORD`). The optional online-update feature lives
in the "System Settings" → "System Update" tab: it accepts the same tar.gz package, verifies it,
backs up the database, swaps the app directories and health-checks with
automatic rollback. `run.sh` prepares `$APP_HOME/updates` and exports the
`MC_UPDATE_MODE=manual` settings automatically. See
`Docs/前端在线更新与认证.md` for details.

## 3.1 One-time stopped-service storage cutover

Before the first start of this release, stop the service and run the cutover
once. It copies the old control-plane database to the canonical control path,
materializes the VNext task read index, and builds the spool recovery index.
The full scans happen here, outside application startup:

```bash
cd /opt/marketing-copilot-manual/backend
./../venv/bin/python -m tools.cutover_vnext_store \
  --source /opt/marketing-copilot/data/marketing_copilot.duckdb \
  --control-target /opt/marketing-copilot/data/control/vnext-control.duckdb \
  --projection-target /opt/marketing-copilot/data/vnext/projection/vnext-projection.duckdb \
  --spool-root /opt/marketing-copilot/data/vnext/spool \
  --manifest /opt/marketing-copilot/data/vnext/cutover-manifest.json \
  --min-business-day 2026-10-19 \
  --replace
```

The cutoff is inclusive: records before `2026-10-19` are not imported into the
new task index or control database. Review the manifest counts before starting.
Keep the old database as an offline rollback archive; the running service uses
only `MC_CONTROL_DB_PATH`, the VNext projection, and its read/recovery
sidecars. VNext ages records older than seven business days in bounded idle
maintenance batches; it does not scan the old database or journals at startup.
Do not run the command while the service owns either DuckDB file.

## Local version and rollback CLI

The Web UI is not required for local recovery. Run the CLI with the bundled
virtual environment from the application host:

```bash
cd /opt/marketing-copilot
bin/marketing-copilot-cli version
bin/marketing-copilot-cli status
bin/marketing-copilot-cli backups
bin/marketing-copilot-cli history
```

If the Web UI or backend cannot start after an update, inspect the backups and
restore the newest application backup:

```bash
cd /opt/marketing-copilot
bin/marketing-copilot-cli rollback --yes
bin/marketing-copilot-cli status
```

`rollback` uses the same backup and detached-runner workflow as the Web UI,
stops/drains the service before swapping files, and health-checks the restored
version. Run it as the service account or root, depending on who controls the
systemd unit. The command does not delete the database; it creates a fresh
database backup before starting the rollback.

## 4. Run manually

```bash
cd /opt/marketing-copilot-manual
./run.sh
```

On first run, `run.sh` creates `venv` if it is missing and installs Python
dependencies from `offline/wheels` using `--no-index`. If `venv` already exists
but core dependencies are missing, it repairs the environment from the same
offline wheelhouse before starting the service.

The service remains attached to the current terminal and listens on port 19090.
Verify in a second terminal:

```bash
curl --fail http://127.0.0.1:19090/api/ready
curl --fail http://127.0.0.1:19090/api/health
```

Stop it with `Ctrl+C`. For background execution, use the organization-approved
process supervisor; this package intentionally does not create or enable one.

## Run as a systemd background service

The package ships with an installer tool that creates a systemd unit for this
manual layout, so the service can run as a managed daemon with automatic
restart and boot startup:

```bash
cd /opt/marketing-copilot-manual/backend
sudo /opt/marketing-copilot-manual/venv/bin/python -m tools.install_systemd_service
```

The tool detects the layout, reuses `run.sh` and `marketing-copilot.env`, sets
`KillMode=control-group` with a 90-second stop grace period, installs a
separate oneshot update-runner unit and projection-recovery unit, installs a
polkit rule for the service account, flips `MC_UPDATE_MODE=systemd` (with a
`.bak` backup), and starts/enables the main unit. Review first with:

```bash
cd /opt/marketing-copilot-manual/backend
/opt/marketing-copilot-manual/venv/bin/python -m tools.install_systemd_service --dry-run
```

Useful options: `--user <account>` (default `marketing-copilot`),
`--service-name <name>.service`, `--no-start`, `--no-enable`, `--no-polkit`,
`--force`, `--exec-start <command>`.

`run.sh` also checks for the unit automatically: when it is missing and
`run.sh` is executed as root, the unit is created (without starting it) using
the owner of the app directory as the service account. Set
`MC_SKIP_SYSTEMD_CHECK=1` to disable this check.

If the projection reports `projection-recovery-required.json`, keep the main
service stopped and run the isolated recovery unit. It builds a fresh,
unindexed projection from a WAL-free copy of historical progress evidence plus
immutable progress journals, derives and validates the canonical tables, then
atomically promotes it while retaining the old projection as a rollback copy.
It deliberately does not import old indexes or WAL, so malformed task-index
rows cannot poison the rebuild and historical aggregate counts are retained:

```bash
sudo systemctl stop marketing-copilot.service
sudo systemctl start marketing-copilot-projection-recovery.service
sudo systemctl status marketing-copilot-projection-recovery.service --no-pager
```

If the recovery unit fails after `bulk-rebuild-state.json` and
`recovery-state.json` report a validated output, resume only the atomic
promotion instead of replaying the journals:

```bash
sudo /opt/marketing-copilot/venv/bin/python -m tools.recover_projection \
  --service-unit marketing-copilot.service \
  --root /opt/marketing-copilot/data/vnext \
  --output-root /opt/marketing-copilot/data/.vnext-recovery-<timestamp> \
  --promote-existing
```

The recovery path never opens the SMS spool or SMPP transport and never
resubmits a message.

## Optional unified SQLite projection

This package includes the candidate single-transaction SQLite projection.
Upgrading application files does not migrate an existing DuckDB projection.
See `SQLITE-PROJECTION.md` for validation limits and the offline migration plan.
With the service stopped, run from the installed `backend` directory:

```bash
/opt/marketing-copilot/venv/bin/python -m tools.migrate_sqlite_projection \
  --root /opt/marketing-copilot/data/vnext/projection
```

Enable `MC_VNEXT_PROJECTION_ENGINE=sqlite` only after verifying the migration
and completing workload acceptance. Existing migrated databases are selected
automatically. Downgrading to the old DuckDB is blocked because its state and
consumption offsets may be stale. Legacy DuckDB recovery/maintenance commands
refuse migrated directories; do not delete SQLite files or WAL to bypass this.
