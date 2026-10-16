# Marketing Copilot Linux pytest test bundle

This is a test-only offline bundle. It is deliberately separate from the
Marketing Copilot SLES12SP5 application deployment package and must not be
copied into `release/`, `offline/`, or any production application archive.

Target:

- CPython 3.11 / cp311
- Linux x86_64
- manylinux2014/glibc 2.17-compatible wheel selection

Use only with a fresh run-local venv created by the approved standalone
CPython runtime:

```bash
python -m pip install --no-index --find-links wheels -r requirements-test.txt
python -m pytest --version
```

This bundle contains test tooling only. It does not contain application
source, application runtime, application dependencies, service units, or
credentials, and it does not connect to external test services.
