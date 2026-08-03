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
