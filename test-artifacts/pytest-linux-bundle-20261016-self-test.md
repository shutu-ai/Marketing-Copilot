# Linux pytest test bundle self-test

Date: 2026-10-16

Result: PASS

- Test host: AlmaLinux-8 WSL, native ext4 filesystem.
- Interpreter: the delivered standalone CPython 3.11.5 runtime.
- Installation mode: fresh run-local venv, `pip --no-index --find-links` only.
- Pytest: 8.3.5.
- Test dependencies: pluggy 1.5.0, iniconfig 2.0.0, packaging 24.2, pygments 2.18.0.
- Checks passed: runtime `_ctypes`/SSL/venv contract, pytest startup, all five package imports, venv `sys.base_prefix`, archive SHA.
- Application deployment package: not modified.
- External test service/VPN: not used.

Bundle archive SHA256:

`5256b48ec5bdfe1ffb4a9adece74376f7b9a3516a6875be668893475d4b28a6d`
