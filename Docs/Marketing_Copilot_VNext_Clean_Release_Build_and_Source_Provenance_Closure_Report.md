# Marketing Copilot VNext — Clean Release Build & Source Provenance Closure

Run ID: `vnext-clean-release-20261017-a`  
Execution date: 2026-10-17  
Scope: offline release engineering, package identity, archive safety, and local smoke only.

## Conclusion

`CLEAN_RELEASE_BUILD_CLOSURE=PASS`.

The candidate was built from the frozen full Git SHA `c588cd2fc6967be6ee3d84f8e34e379530f969cd`, with tree SHA `5a1b431bf9fcf23613071727514f259bc8f6faba`. The original working tree was not used for packaging. The final application archive and the independent test-only archive both carry the same full source SHA, and the final application archive has no `-dirty` suffix.

The candidate is documented and locally installable offline. It has not been re-tested on EulerOS, connected to a real SMSC, or approved for production.

## Frozen source and build inputs

- `HEAD` and `origin/main` at preflight: `c588cd2fc6967be6ee3d84f8e34e379530f969cd`.
- Frozen source snapshot: WSL AlmaLinux-8 ext4 path `/tmp/mc-vnext-clean-release-20261017-a/source`.
- Freeze-time Git status, tracked diff, and index diff: clean.
- The isolated build hydrated only the allowlisted DuckDB extension under `source/cache/` and the built frontend `dist`; these are external/generated inputs, not source edits.
- Original worktree preflight summary: 101 status entries, including 2 tracked modifications and 99 untracked entries. No user files were reset, cleaned, committed, or pushed.
- Frontend was built from a separate clean Windows clone of the same frozen SHA because WSL had no native Node and Windows Node cannot use an ext4 UNC current directory. `npm run build` exited 0; Node `v24.19.0`, npm `12.0.2`; 49 dist files; `frontend/dist/index.html` SHA256 `5a25d52be0adfc536c8d8d8e9a28de817bea5537a2d3e7b0e64e42232d2cfc78`.
- Frozen `frontend/package-lock.json` SHA256: `d079e511d0106c37fc02ba9e68dffc7144f85ce9e50511adb1abc463cd047d77`.
- Wheelhouse SHA256: `504043e2e0fbf31e6c8dca572392519836289f0f415de933309bcc6ad5578bda`.
- DuckDB 1.2.0 `core_functions.duckdb_extension` SHA256: `61b0fc7741f6fda664025c8fc3b330d821c64c6c77407ed24a31316226cf4640`.
- Standalone CPython 3.11.5 runtime SHA256: `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97`.

## Build identities

The original builders were run from the clean frozen snapshot without modification:

- Formal builder SHA256: `4df3378edc6955389c25f355c7a706128b5aa00348d2740093fe1ac739fa330c`.
- Test-bundle builder SHA256: `918e5085d7f40bbd328ce2990f3e87ea86763f72df488b3dc4e75e300596fae1`.
- Finalizer SHA256: `247e125ad9ec1bc7b2259ee44a1a5e29462d951e39794e5fe9c7f8ad9a5be9ce`.
- Finalizer mode: `original_builder_plus_audited_metadata_finalizer`.
- The finalizer extracted a newly audited intermediate archive, changed only `backend/release.json`, root `release.json`, and `checksums.sha256`, then emitted a different final archive. It did not edit the tarball in place.

The intermediate application archive SHA256 is `c259e7e886fafac3c0bf614eee3ddb8677b61800590a25027b0e50d97da5b2f1`. Its short `git_sha` was intentionally retained as intermediate evidence; it was not presented as the final identity.

## Delivered artifacts

All artifacts below are under `test-artifacts/vnext-clean-release-20261017-a/release-candidate/`.

| Artifact | SHA256 | Identity evidence |
|---|---|---|
| `marketing-copilot-sles12sp5-system-python-20261017-vnext-clean-final.tar.gz` | `1fc278f43a01a6de6b4b32c62fe6c821514a51981133a2c733a0ef4065151551` | `release.json.git_sha` is the full frozen SHA |
| `marketing-copilot-vnext-runtime-canary-test-20261017-vnext-clean-test.tar.gz` | `090cd296ca3ea81e168eff8e1db5d47555cc9f6c501f0cb9ec459ed12566bce5` | `bundle.json.source_commit` is the same full frozen SHA |
| `inputs/sles12sp5-python-3.11.5-20260917-ctypes4.tar.gz` | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | standalone runtime, not merged into app package |

Formal `release.json` and `backend/release.json` are byte-identical and contain version `20261017-vnext-clean-final`, full `git_sha`, source tree SHA, and `SOURCE_WORKTREE_CLEAN_AT_FREEZE=true`.

## Verification results

- Formal archive: 195 members; 179 internal checksum entries, all 179 matched; 33 application wheels; required metadata, frontend index, requirements, run script, wheel directory, and DuckDB extension present.
- Formal archive safety: 0 unsafe names, 0 symlinks/hardlinks, 0 special members; forbidden-content count 0.
- Test archive: 26 members; 19 internal checksum entries, all matched; five pytest wheels; no formal deployment package, runtime, database, WAL, logs, credentials, or real configuration; `formal_deployment_package_modified=false`; `r2_authorized=false`.
- Runtime archive: 2,558 members; 2,374 internal checksum entries, all matched; sidecar matched.
- Wheelhouse-to-application wheel and requirements comparison: PASS. The packaged extension hash matches the allowlisted input hash.
- VNext business source byte-hash comparison: all nine audited backend modules match the frozen snapshot byte-for-byte; details are in the identity/safety audit.
- Ext4 offline smoke: PASS. CPython 3.11.5, OpenSSL 1.1.1v, `_ctypes/ctypes`, DuckDB 1.2.0, and smpplib imported successfully. Formal wheels installed with `pip --no-index`; `pip check` reported no broken requirements. The separate pytest wheels also passed `pip check`; 12 focused static-audit tests passed.
- `BYTE_FOR_BYTE_REPRODUCIBLE=NOT_PROVEN`: the metadata `build_time` is not normalized, so archive gzip bytes are not claimed reproducible. Payload, manifest, source, and input identity checks passed.

## Status boundaries

| Status | Result |
|---|---|
| `CLEAN_SOURCE_SNAPSHOT` | `PASS` |
| `SOURCE_PROVENANCE_EXACT` | `YES` |
| `RELEASE_PACKAGE_INTEGRITY` | `PASS` |
| `OFFLINE_INSTALL_SMOKE` | `PASS` |
| `CLEAN_RELEASE_BUILD_CLOSURE` | `PASS` |
| `CURRENT_VNEXT_LOOPBACK_CANARY` | `PASS_REPORTED_EVIDENCE_GAP` (historical evidence; not upgraded here) |
| `LATEST_EULEROS_R2_P5500` | `PASS` for the historical tested package only |
| `LATEST_EULEROS_R7` | `PASS` for the historical tested package only |
| `NEW_RELEASE_CANDIDATE_EULEROS_VERIFIED` | `NO` |
| `HISTORICAL_D7_R2_NATIVE_FAILURE` | `PRESERVED` |
| `RCA_LEVEL` | `RCA_STRONGLY_INDICATED` |
| `NATIVE_FAULTING_FRAME` | `UNAVAILABLE` |
| `RELEASE_CANDIDATE_DOCUMENTED` | `YES` |
| `REAL_SMSC_APPROVAL` | `NOT_GRANTED` |
| `PRODUCTION_APPROVAL` | `NOT_GRANTED` |

The formal builder is the manual/nohup-style application package builder. The repository deployment default remains `MC_UPDATE_MODE=systemd`; this task did not select, change, or exercise a live deployment mode.

## Evidence locations

- [Machine receipt](../test-artifacts/vnext-clean-release-20261017-a/release-provenance-receipt.json)
- [Machine artifact audit](../test-artifacts/vnext-clean-release-20261017-a/release-artifact-audit.json)
- [Artifact identity and safety audit](Marketing_Copilot_VNext_Release_Artifact_Identity_and_Safety_Audit.md)
- [Release-candidate directory](../test-artifacts/vnext-clean-release-20261017-a/release-candidate/)

No `git commit` or `git push` was executed.
