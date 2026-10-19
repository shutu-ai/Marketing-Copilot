# Agent instructions

## Repository guidance

- Preserve unrelated user changes in the working tree.
- Do not commit or push unless the user explicitly requests it.
- Do not include runtime data, databases, logs, uploads, backups, or secrets in deployment artifacts.

## CodeGraph

If the repository contains a `.codegraph/` directory, use CodeGraph before `grep`, `find`, or reading source files to understand or locate code. Prefer:

```text
codegraph explore "<symbol names or question>"
```

If `.codegraph/` is absent, skip CodeGraph.

## WSL and SLES12SP5 test environment

- Verified WSL distribution: `AlmaLinux-8`.
- Python target: CPython `3.11` / `cp311`.
- SLES12SP5 wheel target: `manylinux2014_x86_64` with glibc 2.17 compatibility.
- Prefer existing offline archives in `cache/wheelhouse/` before attempting a full dependency download.
- The PowerShell WSL wrapper may fail when Windows paths are converted incorrectly. If that happens, invoke the corresponding `.sh` script directly from WSL to diagnose the path-conversion issue; this workaround does not make `/mnt/<drive>` a valid formal performance or acceptance workspace.
- Repository shell scripts may have CRLF line endings when checked out on Windows. When executing them directly in WSL, normalize a temporary copy with `tr -d '\r'` and remove the temporary copy afterward.
- Network access to the dependency mirror may time out. Do not discard an existing wheelhouse; use it as the base and add only the missing wheel when possible.
- The SLES12SP5 application package default is `MC_UPDATE_MODE=systemd`. Use `manual` only when a manual/nohup deployment is explicitly requested.

### Performance and acceptance filesystem rules

- Any formal throughput, latency, concurrency, long-stability, resource, database I/O, WAL/checkpoint, audit, or recovery acceptance test must run on the WSL distribution's native Linux filesystem (`ext4`), regardless of its test name or gate number.
- This includes current gates such as G0/G3/G4/G6 and any future equivalent tests.
- Keep source, virtualenv, database, WAL, logs, and test temporary directories on the same native `ext4` filesystem.
- `/mnt/<drive>` may be used for source synchronization, quick smoke tests, and copying artifacts, but its quantitative results must not be used as the Linux performance baseline.
- OS-specific compatibility tests must also run on their target OS separately; for example, Windows file-lock behavior must be tested on Windows and reported separately from ext4 results.
- Formal runs must record `df -T .`, Python version, and the dependency freeze.

## Standard validation commands

Run backend tests from the repository root:

```powershell
.\.p0-test-venv\Scripts\python.exe -m pytest backend/tests -q
```

Build the frontend from `frontend/`:

```powershell
npm run build
```

Build the SLES12SP5 application package from the repository root with the appropriate offline wheelhouse:

```text
bash deploy/build-sles12sp5-system-python-manual.sh --output release --wheelhouse <wheelhouse-archive>
```

After packaging, verify `backend/release.json`, `frontend/dist/index.html`, `offline/wheels/`, `run.sh`, and `checksums.sha256`, and confirm that runtime data is absent.
