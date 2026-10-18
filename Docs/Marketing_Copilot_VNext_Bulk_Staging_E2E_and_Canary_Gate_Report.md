# Marketing Copilot VNext — Bulk Staging E2E and Canary Gates

Task: `MC-VNEXT-STAGING-BULK-INGEST-OPTIMIZATION-20261018`
Final decision: `CANARY_READY=NO`.

## Scope decision

R2 verified A2 as the strongest isolated benchmark arm, but the 500K default-batch result did not establish a strong enough end-to-end production win for R3/R4 promotion. Therefore the production ingest path was not switched and the expensive candidate-specific R4a/R4c gates were not started. The unchanged baseline was still exercised through local loopback and recovery checks to make sure the instrumentation/refactoring did not alter correctness contracts.

No connection was made to the supplied remote server. The task explicitly requires separate authorization and VPN for EulerOS synthetic-loopback testing; server credentials are not copied into this report or any artifact.

## Local native-ext4 evidence

All formal local runs used the WSL `AlmaLinux-8` native ext4 Run root, not `/mnt/d`.

| Check | Result | Evidence |
|---|---|---|
| 400-task single-segment loopback smoke | PASS: 400 accepted, 400 PDU, 400 responses, 800 Journal/Projection events, 0 unknown | `/tmp/mc-vnext-staging-20261018.MSdK4g/e2e-smoke.json` |
| Multipart loopback smoke | PASS: 80 tasks, 120 PDU/responses, 200 events, 0 unknown | `/tmp/mc-vnext-staging-20261018.MSdK4g/e2e-multipart-smoke.json` |
| T7-4 through T7-9 recovery matrix | PASS: 6/6 scenarios | `/tmp/mc-vnext-staging-20261018.MSdK4g/t7-native.json` |
| Full Windows backend pytest | PASS: 422 passed, 5 skipped, 4 warnings | command run from repository root |

The multipart result correctly distinguishes tasks from PDU and response counts; it does not assume `task=PDU`. T7 covers pause/resume, route isolation, projection hint queue pressure, journal hard limit/GC, day reclaim, and writer restart/cross-day no-redispatch behavior.

These local checks are regression evidence only. They are not a claim about EulerOS throughput, 8 GiB hardware capacity, 16 GiB hardware capacity, or 600-second sustained service behavior.

## Required gates

```text
SOURCE_AND_ENV_IDENTITY=PASS
STAGING_INTERNAL_RCA=PASS
A0_A4_CORRECTNESS=PASS
STAGING_AB_BENEFIT=NO_CHANGE
PRODUCT_CANDIDATE_CORRECTNESS=NOT_VERIFIED
T3_50K_PAIRED=NOT_VERIFIED
MULTIPART_AND_RECOVERY=NOT_VERIFIED
EULEROS_600S_800PDU=BLOCKED_ENV
PROD_8G_CAPACITY=NOT_VERIFIED
PROD_16G_CAPACITY=NOT_VERIFIED
SINGLE_WRITER_CAPACITY_ESTIMATE=MEASURED
CANARY_READY=NO
```

`A0_A4_CORRECTNESS=PASS` covers the isolated adapter contract only. `PRODUCT_CANDIDATE_CORRECTNESS` and `MULTIPART_AND_RECOVERY` are not signed as candidate gates because A2 did not enter R3; unchanged baseline checks passed as listed above.

## Why the canary remains NO-GO

The local adapter evidence identified A2 as the strongest experimental arm, but its 500K default-batch wall improvement was only 3.4%; this is not sufficient to authorize a production mode switch. The actual EulerOS 600-second / approximately 800-PDU/s gate was not authorized or run. No assertion is made for 8 GiB or 16 GiB production capacity.

The safe follow-up is to obtain explicit target-environment authorization, approve/package the columnar dependency if it is acceptable for the target, integrate A2 behind a controlled mode switch, and rerun the same paired correctness and sustained-drain gates before considering any production mode change.
