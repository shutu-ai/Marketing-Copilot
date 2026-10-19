# Marketing Copilot VNext UI metric contract

## Purpose

The VNext data plane has a different evidence model from the legacy `sms_tasks`
flow. The UI must use the VNext projection and live transport snapshots without
reintroducing legacy `accepted`, `delivered`, or `submitted` values as if they
were current send results.

## Canonical counters

| UI meaning | VNext field | Unit | Meaning |
| --- | --- | --- | --- |
| Sent | `socket_written` | logical message event | A `submit_sm` payload was written to the SMPP socket and the event was durably recorded. |
| Sent PDU parts | `socket_written_parts` | `submit_sm` part | Physical PDU parts written to sockets; use this for throughput/capacity. |
| SMSC accepted | `submit_sm_resp_success` | PDU part | SMSC returned a successful `submit_sm_resp`; it is not handset delivery. |
| SMSC rejected | `submit_sm_resp_error` | PDU part | SMSC returned an error response. |
| Unknown | `unknown` | PDU part | The response or terminal outcome could not be classified. |
| Write failed | `write_failed` | PDU part | The socket write did not complete successfully. |
| Delivered | unavailable | — | Handset DLR is not connected in VNext; do not render a zero-valued delivered KPI. |

The dashboard’s “today sent”, campaign sent count, task statistics, and trend
series therefore use `socket_written`. SMPP capacity and rate panels use
`socket_written_parts_1m` and `socket_written_tps_1m` because one logical
message can contain multiple PDU parts.

## Data lineage

```text
worker -> progress journal -> projection writer -> projection status snapshot
                                      |
                                      +-> dashboard / campaign statistics

worker transport snapshot -> SMPP capacity/rate statistics
```

The application process reads the projection status snapshot and transport
snapshots. It does not open the projection database from the request path.
Daily totals are cached by the projection writer for a short interval, and
live rate counters are in-memory rolling buckets. No statistics query, lock,
or network operation is added to the worker send loop.

## UI compatibility rules

- When `pipeline=vnext`, show socket-written send evidence and SMSC response
  evidence only.
- Hide legacy task actions and legacy accepted/delivered/submitted cards in
  VNext views; those actions target the old database workflow.
- Keep legacy labels and fields available only when `pipeline=legacy`, so an
  old deployment remains operable during migration.
- Surface projection lag as an operational freshness indicator, not as a
  change to the send count.
- Treat `delivery_receipts_available=false` as an explicit capability state,
  not as a delivery failure count.

## Performance guardrails

Metric alignment must not reduce dispatch throughput or add backpressure:

- send-path event writes remain journal/spool operations already required for
  correctness;
- rate counters are O(1) in-memory bucket updates;
- dashboard daily aggregation is performed by the projection writer and
  published in its status snapshot;
- UI-only reads may be cached or stale by the reported projection lag, but
  must never block SMPP binds, workers, or socket writes.

