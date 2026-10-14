# OCC Equal-Window 2x2 G3/G4/G5 Acceptance

## Outcome

**Local status:** `MORE_EVIDENCE_NEEDED`

**Production decision:** `NO_GO`
**Source SHA:** `c9890a76040f92dabd50c4b50754d44716d6daca`

The result is deliberately conservative. A loopback SMPP simulator can validate
local mechanics, but it cannot qualify SLES12SP5 deployment behavior or a real
SMSC route.

## G0 baseline

| Item | Result |
|---|---|
| Source revision | `c9890a76040f92dabd50c4b50754d44716d6daca` |
| OS/filesystem | AlmaLinux-8 WSL, ext4 `/tmp` |
| Runtime | CPython 3.11, DuckDB 1.2.0 |
| Harness CLI | `local_e2e_acceptance.py --help` passed |
| Live environment | `LIVE_ENV_UNVERIFIED` |
| Worktree | Dirty with unrelated user files and runtime artifacts; none are part of this evidence commit |
| Production access | Not used |

The required 32-test OCC/reliability regression subset passed during the repair
cycle. That is a code regression signal, not a substitute for the four-arm
acceptance matrix.

## Equal-window matrix

All intended G3 runs use the same envelope: 4 SMPP binds, window 50, route TPS
200, task workers 8, ingestion workers 2, watch-copy workers 2, queue capacity
2000, loopback SMSC, disabled DLR, isolated DB/journal/watch directories, and a
new process per run.

| Arm | Current-source runs | Current result | Gate interpretation |
|---|---:|---|---|
| serialized + reliable | 0/3 | `NOT_RUN` | Historical passes excluded |
| OCC + reliable | 0/3 | `NOT_RUN` | Historical passes excluded |
| serialized + fast | 0/3 | `NOT_RUN` | Historical passes excluded |
| OCC + fast | 1 current run + discarded probes | `FAIL` | Missing mirror rows, OCC conflicts, incomplete closure |

Current OCC/fast raw run directories include:

- `/tmp/mc-occ-2x2-giYoXB/runs/marketing-copilot-stress-1k-full-5f52f748`
- Additional worktree probes under `/tmp/mc-occ-2x2-giYoXB/runs/` are diagnostic only and are not current evidence.

Historical PASS JSON files are indexed in
`Docs/OCC-2x2-Concurrency-Performance-Results.json` with
`current_evidence: false`. They were generated at the earlier `1511d4c` source
revision and are retained only for RCA comparison.

## Gate status

| Gate | Status | Required evidence still missing |
|---|---|---|
| G0 | `PASS` | None for the local baseline |
| G3 | `FAIL` | Four current arms × three runs; current OCC/fast must converge |
| G4 | `NOT_RUN` | Cross-campaign 50K-vs-1K isolation and stop-fence evidence |
| G5 | `NOT_RUN` | Fault matrix, restart/recovery evidence, effective 600-second soak |
| SLES12SP5 | `NOT_RUN` | Target package/runtime execution and health checks |
| REAL_SMSC | `NOT_RUN` | Approved canary with real route, rollback, and DLR evidence |

## Invariant disposition

- Duplicate send: not verified for the current failed arm.
- Lost mapping: failed as an audit/state invariant because processed-file rows
  disappeared during the current OCC/fast contention.
- Unknown auto-retry: failed for the current arm; UNKNOWN retries were observed
  after processed-file state loss. This is not an authorization to retry unknown
  SMPP submissions in production.
- Late submit after stop: not run under the current source.

## Acceptance rule

No production or canary approval is granted until the current source produces
three independent PASS runs for every G3 arm, then passes the G4/G5 gates and the
separate SLES12SP5/real-SMSC gates. Any missing gate remains `NOT_RUN` or
`MORE_EVIDENCE_NEEDED`; it is never inferred from old artifacts.
