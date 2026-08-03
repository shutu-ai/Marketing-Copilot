# Linux deployment

## SLES 12 SP5 offline package (Python 3.11.5, no Docker or Node.js on target)

Run the builder in an internet-connected **SLES 12 SP5-compatible WSL/Linux
environment** (glibc <= 2.22), with Python 3.11, compiler build prerequisites,
Node.js/npm and curl. For example, import and use an approved SLES 12 SP5 rootfs
in WSL. Do not use a current Ubuntu WSL: its newer glibc would make the Python
binary unusable on SLES 12. Node is used only to create `frontend/dist`; the
generated archive contains static frontend files only, so the disconnected SLES
target does not install or run Node.js/npm.

The preferred no-target-compilation method is the official manylinux2014 build
image. Docker/Podman is used **only on the build machine**; the SLES target has
no container runtime. With the existing AlmaLinux 8 WSL and Podman:

```bash
bash deploy/build-sles12sp5-manylinux2014.sh --engine podman --output release
```

It compiles the bundled Python 3.11.5/OpenSSL runtime against glibc 2.17 inside
the image, which is compatible with SLES 12 SP5 glibc 2.22.

```bash
cd /path/to/MarketingCopilot
bash deploy/build-sles12sp5-offline.sh --output release

# Copy the archive to the disconnected SLES 12 SP5 host, then:
tar -xzf marketing-copilot-sles12sp5-*.tar.gz
cd marketing-copilot-sles12sp5-*
sudo bash install.sh
```

If only the Python dependency archive is needed now, it can be built in the
existing AlmaLinux 8 WSL environment (no SLES-compatible compiler is required):

```bash
bash deploy/build-sles12sp5-wheelhouse.sh --output release
```

It creates `marketing-copilot-sles12sp5-wheelhouse-*.tar.gz`, including the
application wheelhouse and an explicitly separate, empty-dependency SMSC
simulator directory.

The archive contains a precompiled CPython 3.11.5/OpenSSL 1.1.1w runtime and an
offline `cp311` wheelhouse. The target only copies files and creates a virtual
environment; it does not compile Python or require a compiler, development RPMs,
Node.js, npm, or network access. Wheels are constrained to `manylinux2014_x86_64`
(glibc 2.17), compatible with SLES 12 SP5 glibc 2.22. The installer checks
SHA-256 before installation and never accesses the network.

Use `sudo bash install.sh --with-smsc-simulator` to install the optional local
SMSC simulator service. It is separate from application dependencies and uses
only the bundled Python standard library. It remains disabled until explicitly
enabled with `systemctl enable --now marketing-copilot-smsc-simulator.service`.

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
  /var/lib/marketing-copilot /var/lib/marketing-copilot/backups /var/log/marketing-copilot /run/marketing-copilot

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
curl --fail http://127.0.0.1:8000/api/ready
curl --fail http://127.0.0.1:8000/api/health
curl --fail http://127.0.0.1:8000/metrics
sudo -u marketing-copilot /opt/marketing-copilot/venv/bin/python -m tools.backup_database
sudo -u marketing-copilot bash -c 'set -a; source /etc/marketing-copilot.env; cd /opt/marketing-copilot/backend; /opt/marketing-copilot/venv/bin/python -m tools.deployment_check --require-linux'
```

## Capacity monitoring

Capacity monitoring is built into the Python application:

- `/capacity` provides the operator dashboard.
- `/api/metrics/status` returns Worker, queue and SMSC pool state as JSON.
- `/metrics` exposes Prometheus-compatible text for an existing external monitoring service.
- The Python scheduler evaluates queue depth, failure rate and SMSC availability every minute.
- Open and resolved alerts are stored in the operations exception center.
- When `MC_ALERT_WEBHOOK_URL` is set, Python sends JSON `firing` and `resolved` notifications directly without Alertmanager.

The webhook receiver must return an HTTP 2xx response. Keep the URL empty when notifications are not required.
