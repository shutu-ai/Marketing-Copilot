# Standalone SLES 12 SP5 Python runtime

This package installs the Python runtime used by Marketing Copilot separately
from the application package. It does not install the application, application
dependencies, systemd units, or project data.

## Installation

Run on the SLES 12 SP5 target as `root`:

```bash
tar -xzf sles12sp5-python-3.11.5-*.tar.gz
cd sles12sp5-python-3.11.5-*
bash install.sh
```

The default installation path is:

```text
/opt/marketing-copilot/python/bin/python3.11
```

To use another path:

```bash
bash install.sh --prefix /opt/marketing-copilot
```

The installer verifies the archive checksums and requires Python 3.11.5 with
working `ssl`, `_ctypes`/`ctypes`, and `venv`. It does not modify the system
Python or `PATH`. `_ctypes` is required by pandas and must be compiled into
the runtime; it cannot be supplied by an application wheel.

## Application package contract

The Marketing Copilot package should create its project virtual environment
with:

```bash
/opt/marketing-copilot/python/bin/python3.11 -m venv /opt/marketing-copilot/venv
```

Application wheels remain in the application package. The runtime package is
installed once and is not uploaded through the application's online update
feature. The online updater must continue to replace only application trees;
it must never remove or replace `/opt/marketing-copilot/python`.

## Compatibility

The runtime must be built and tested on SLES 12 SP5 or an environment with an
equivalent or older glibc baseline. A Python binary built on a newer Linux
distribution is not automatically compatible with SLES 12 SP5 merely because
it reports Python 3.11.

When compiling Python from source, install the SLES development package
`libffi-devel` before running `configure`; otherwise the build may silently
omit `_ctypes`.
