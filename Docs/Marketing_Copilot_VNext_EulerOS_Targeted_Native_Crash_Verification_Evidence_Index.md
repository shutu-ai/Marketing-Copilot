# EulerOS Targeted Native Crash Verification Evidence Index

Run ID: `vnext-targeted-native-20261017-r3f7c2`

Baseline: `8f14e82d57988559370cb1a978f99313c92d6754`

## Returned evidence archive

| Artifact | Location | SHA256 | Verification |
| --- | --- | --- | --- |
| sanitized server evidence | `test-artifacts/vnext-targeted-native-20261017-r3f7c2/transfer/targeted-native-20261017-evidence-complete.tar.gz` | `676b70352619035021dc6c3365a09314e6ddce1271e0d2bf86e035b83e1df0f4` | outer sidecar and local copy verified |

The archive's internal `checksums.sha256` has 93 rows. Local verification passed for every row. Archive names were checked for absolute paths and `..` traversal; no symlink entries were present. A filename audit found no DuckDB database, WAL, Spool, progress Journal, or core file.

## Package identity

| Package | SHA256 | Scope |
| --- | --- | --- |
| formal application | `bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44` | deployment package; no tests/tools |
| standalone CPython runtime | `11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97` | offline CPython 3.11.5 runtime |
| test-only B2 bundle | `32fd0fde2447be49ff48c487082ef700f7694c4e72d3ecb789302566f683c813` | launcher, acceptance tools, pytest wheels |

The server-side evidence records the same three archive hashes, formal `release.json`, test `bundle.json`, EulerOS/runtime preflight, pip freeze/check, and exact launcher request files. The test bundle states `formal_deployment_package_modified=false`, `runtime_data_included=false`, and `r2_authorized=false`.

## Evidence map

| Evidence path in archive | Purpose |
| --- | --- |
| `b0/` | EulerOS, ext4, runtime, dependency and debugger capability preflight |
| `d0/` | native lifecycle diagnostic and guard summary |
| `b1-r0/` | R0 canary, guard/resource samples, launcher request, phase and role metadata |
| `b1-r1/` | R1 canary, guard/resource samples, launcher request, phase and role metadata |
| `b2/` | 300-message/42-second targeted canary, guard/resource samples, phase and role metadata |
| `r2-decision.json` | machine-readable `R2_DECISION=SKIP` and B0–B2 counters |
| `package-archives.sha256` | server-side package parity |

No evidence file contains credentials, message content, MSISDNs, full database/WAL/Spool/Journal data, or a core. Empty faulthandler files are retained to prove the role-specific destinations were initialized; an empty log is not treated as a native stack.

## Decision

```text
R2_DECISION=SKIP
R2_DECISION_REASON=safe shared-disk margin below the 5 GiB guard; B2 provides no new discriminating evidence for the historical 2500-message failure
NATIVE_FAULTING_FRAME=UNAVAILABLE
```
