# Manual offline deployment with system Python 3.11.5

This package does not include Python, OpenSSL, systemd service files, Docker,
Podman, Node.js, or npm. It runs in the foreground using an existing target
`python3.11` installation.

## 1. Verify system Python

```bash
python3.11 - <<'PY'
import ssl, sys
assert sys.version_info[:3] == (3, 11, 5), sys.version
assert ssl.OPENSSL_VERSION_INFO >= (1, 1, 1), ssl.OPENSSL_VERSION
print(sys.version)
print(ssl.OPENSSL_VERSION)
PY
```

The command must succeed. Python must also have the `venv` module. If the
Python executable is not named `python3.11`, set `PYTHON_BIN` before running
`run.sh`.

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
in the "System Update" page: it accepts the same tar.gz package, verifies it,
backs up the database, swaps the app directories and health-checks with
automatic rollback. `run.sh` prepares `$APP_HOME/updates` and exports the
`MC_UPDATE_MODE=manual` settings automatically. See
`Docs/前端在线更新与认证.md` for details.

## 4. Run manually

```bash
cd /opt/marketing-copilot-manual
./run.sh
```

On first run, `run.sh` creates `venv` if it is missing and installs Python
dependencies from `offline/wheels` using `--no-index`. If `venv` already exists
but core dependencies are missing, it repairs the environment from the same
offline wheelhouse before starting the service.

The service remains attached to the current terminal and listens on port 8000.
Verify in a second terminal:

```bash
curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
```

Stop it with `Ctrl+C`. For background execution, use the organization-approved
process supervisor; this package intentionally does not create or enable one.

## Run as a systemd background service

The package ships with an installer tool that creates a systemd unit for this
manual layout, so the service can run as a managed daemon with automatic
restart and boot startup:

```bash
sudo /opt/marketing-copilot-manual/venv/bin/python -m tools.install_systemd_service
```

The tool detects the layout, reuses `run.sh` and `marketing-copilot.env`, sets
`KillMode=process` (required for online self-updates), installs a polkit rule
for the service account, flips `MC_UPDATE_MODE=systemd` (with a `.bak` backup),
and starts/enables the unit. Review first with:

```bash
cd /opt/marketing-copilot-manual
venv/bin/python -m tools.install_systemd_service --dry-run
```

Useful options: `--user <account>` (default `marketing-copilot`),
`--service-name <name>.service`, `--no-start`, `--no-enable`, `--no-polkit`,
`--force`, `--exec-start <command>`.

`run.sh` also checks for the unit automatically: when it is missing and
`run.sh` is executed as root, the unit is created (without starting it) using
the owner of the app directory as the service account. Set
`MC_SKIP_SYSTEMD_CHECK=1` to disable this check.
