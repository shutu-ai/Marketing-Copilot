# VNext Release Artifact Identity and Safety Audit

Run ID: `vnext-clean-release-20261017-a`

## Archive identity

| Archive | SHA256 | Members | Internal files checked | Sidecar |
|---|---:|---:|---:|---|
| Formal final application | `1fc278f43a01a6de6b4b32c62fe6c821514a51981133a2c733a0ef4065151551` | 195 | 179/179 | PASS |
| Independent test bundle | `090cd296ca3ea81e168eff8e1db5d47555cc9f6c501f0cb9ec459ed12566bce5` | 26 | 19/19 | PASS |
| Standalone CPython runtime | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | 2,558 | 2,374/2,374 | PASS |

The machine-readable version of this audit is `test-artifacts/vnext-clean-release-20261017-a/release-artifact-audit.json`.

## Formal package contract

Present and verified:

- `backend/release.json` and root `release.json`;
- `frontend/dist/index.html`;
- `offline/requirements.txt` and 33 application wheels;
- `offline/extensions/1.2.0/linux_amd64_gcc4/core_functions.duckdb_extension`;
- `run.sh` and `checksums.sha256`.

The two release JSON files are identical. Both contain:

```text
git_sha = c588cd2fc6967be6ee3d84f8e34e379530f969cd
source_tree_sha = 5a1b431bf9fcf23613071727514f259bc8f6faba
SOURCE_WORKTREE_CLEAN_AT_FREEZE = true
version = 20261017-vnext-clean-final
```

Formal forbidden-content audit: `0` items. The role-based audit found no tests/tools, pytest runner, backup files, private env files, keys/certificates, databases, WAL/journal, logs, backups, uploads, monitor state, or local diagnostics. A source file named `spool.py` and the `.duckdb_extension` are not treated as forbidden roles.

## Test-bundle isolation

The test bundle contains only the canary/static test overlay, five pytest dependency wheels, runner/readme, and `bundle.json`. It contains no formal `release.json`, `offline/` application wheels, frontend dist, runtime archive, database, WAL, logs, credentials, or real configuration.

```text
source_commit = c588cd2fc6967be6ee3d84f8e34e379530f969cd
formal_deployment_package_modified = false
runtime_data_included = false
r2_authorized = false
```

## Archive safety

Safety was checked before extraction and again through the machine audit. All three archives have one relative root, no absolute or drive-qualified member names, no `..` traversal, no symlink/hardlink members, and no special files. Internal checksum manifests were checked against every regular file, not only selected paths.

The finalizer’s controlled change set was exactly:

```text
backend/release.json
release.json
checksums.sha256
```

The intermediate SHA was `c259e7e886fafac3c0bf614eee3ddb8677b61800590a25027b0e50d97da5b2f1`; the final SHA is different by design.

## Input and dependency provenance

| Input | SHA256 | Check |
|---|---|
| Wheelhouse archive | `504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda` | 33/33 packaged app wheels and requirements match |
| DuckDB extension | `61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640` | package member hash matches |
| CPython runtime | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | sidecar and internal manifest match |
| Frozen frontend lockfile | `d079e511d0106c37fc02ba9e68dffc7144f85ce9e50511adb1abc463cd047d77` | clean clone build input |

The SLES target declaration remains CPython 3.11.5 / cp311 and manylinux2014 x86_64 / glibc 2.17-compatible. No dependency version was changed for this task.

## Source-to-package byte identity

The following source files were compared from the frozen snapshot to the final formal archive. Git blob identity and file/package SHA256 are recorded together.

| File | Git blob SHA | File and package SHA256 |
|---|---|---|
| `backend/vnext/runtime.py` | `f2351386fa1abf4fecbfb36a4271ed77111751ad` | `7541b29ded69dd32f5a196af9b56c59bbad93c3d34bfcd7f0b08c7da302fdc9d` |
| `backend/vnext/spool.py` | `22f0131f12b2cbb1661825009f11964fc7ddcc17` | `0e266ce6c52472f223a3f3933a68db7d2782af29220f89bdfd256906583253b2` |
| `backend/vnext/worker.py` | `2ab490fef084b778ed6b5757f688ac52e79687df` | `80e8f50ecbf69dbcbf55a059ee2b09c2e1119c55d0d3befc9b5fe6b7b20ecb15` |
| `backend/vnext/projection_writer.py` | `c567c15dea528bfd44b46e56563ca7b4446334bb` | `f220c165f88d04972f8746a538a801dd6053bf0cf849e2f0ac445f097f407b00` |
| `backend/vnext/smpp_transport.py` | `77ea35af0dfc57f88b97fcfb0bc866ffb4050073` | `65d4af860137ec9353cfef179ef90a05412aba6aa03a4c03165bcc661a1705d2` |
| `backend/vnext/diagnostics.py` | `5baad8d433ce72badeb23d9ce2775deee6b8627d` | `97549fe215547af7c8694163901d2ad33a614b2190c5371648c274d96545d6a9` |
| `backend/services/vnext_service.py` | `0e93fd367ec2268fc3fb931ad99074b9ab6c522e` | `e4a6419611e7b6a70cdf2119f91bf5d71ec6a436ec75435a71e0a745663c5f38` |
| `backend/routers/vnext.py` | `da115b17019659ec95aca5efa45866c36296fe3e` | `16405ad3d45ff82ce1ac6d3a1693e91d0c58db54fd901b980bc120e544181fc2` |
| `backend/main.py` | `1da177320776a7eccd04c910ffddff0441fa8777` | `8e3234e78e604d14e76e84ad7f1c8989fe5de2a7f7448bf7635e72003f1045b7` |

All file SHA256 values matched the corresponding package member.

## Local offline checks

- Run filesystem: WSL AlmaLinux-8 native ext4 (`df -T` recorded in the validation log).
- Runtime: CPython 3.11.5, OpenSSL 1.1.1v, `_ctypes/ctypes` PASS.
- Formal dependency installation: `pip --no-index --no-cache-dir --find-links offline/wheels -r offline/requirements.txt` PASS; `pip check` PASS.
- Imports: `ssl`, `_ctypes`, `ctypes`, `duckdb==1.2.0`, and `smpplib` PASS.
- Test overlay installation: five pytest wheels, `pip check` PASS.
- Focused test: `test_vnext_static_audit.py`, 12 passed in 0.17s. No server, SMSC, EulerOS, R2/P5500, R7, or production database was used.

This audit establishes package identity and offline installability. It does not establish EulerOS execution of this new candidate or production approval.
