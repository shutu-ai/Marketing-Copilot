# Production sizing comparison matrix

Task: `MC-VNEXT-PRODUCTION-SIZING-SINGLE-WRITER-THROUGHPUT-CLOSURE-20261017`

The target deployment specification was not confirmed and the EulerOS server was not re-authorized for this task. The rows below are therefore local ext4 evidence or historical evidence, not a production sign-off.

| Profile / run | RAM interpretation | CPU / I/O | Guard | Result | Use |
|---|---|---|---|---|---|
| `PROD_8G` model | 8 GiB total, initial 6 GiB application RSS | CPU quota and disk I/O unconfirmed | 6 GiB application-only RSS | `NOT_VERIFIED` | sizing model only |
| `PROD_16G` model | 16 GiB total, initial 12 GiB application RSS | CPU quota and disk I/O unconfirmed | 12 GiB application-only RSS | `NOT_VERIFIED` | sizing model only |
| `LOW_MEM_2G` | stress profile only | local/target-specific | 2 GiB, not a production gate | `NOT_VERIFIED` | bounded-degradation experiment |
| WSL local T3 baseline | 15 GiB WSL memory, not deployment RAM | affinity 0-3, default priority, ext4 | 6 GiB application-only | `PASS` | local synthetic evidence |
| WSL local T3 optimization candidate | same local envelope | affinity 0-3, default priority, ext4 | 6 GiB application-only | `FAIL` | rolled-back experiment evidence |
| historical EulerOS T3 | 2 GiB guard, 4 CPU, low priority | constrained shared host | 2 GiB harness/run guard | `FAIL` drain 742.003s | historical RCA only |
| historical EulerOS T4 | 2 GiB guard, 4 CPU, low priority | constrained shared host | RSS 2,099,128 KiB vs 2,097,152 KiB | `FAIL` | historical RCA only |

The final answer is `NO_GO`: the local correctness path is healthy, but a 600-second high-pressure deployment-profile gate and the actual 8/16 GiB target remain unverified.
