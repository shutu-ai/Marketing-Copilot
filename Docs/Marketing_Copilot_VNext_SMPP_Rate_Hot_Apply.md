# VNext SMPP rate hot-apply contract

## Scope

Only `tps` is hot-applied while VNext is running. The update is persisted first,
then broadcast from the application parent to every live business worker. Each
worker updates the existing `AsyncSmppTransport` rate bucket without closing
the SMPP socket, changing task cursors, or replaying a Spool/Journal record.

The application fixes `tps_scope` to `per_bind`. The Web form does not expose a
scope selector, and older database rows are normalized to `per_bind` during
startup.

The following changes remain restart-bound:

- host, port, system ID, password, bind type, source address and TON/NPI;
- `pool_size`, `window_size`, enquire/timeout/reconnect settings;
- enable/disable, route priority and delivery-receipt mode.

The API response reports `hot_applied=true` and `effective_state=applied` only
when all live workers accepted the control message. A failed or partial worker
broadcast leaves the new database value persisted but marks
`restart_required=true`, so a safe restart is required to converge the
runtime.

## TPS semantics

`per_bind` uses one credit bucket per physical Bind. With `pool_size=4` and
`tps=200`, the configured ceiling is 800 PDU/s. This is an application pacing
ceiling, not proof that the SMSC contract, network, response window, or
production workload can sustain 800 PDU/s.

Through the Web SMPP settings page, `tps` can be changed and takes effect
without a service restart when it is the only changed field. `window_size` can
also be edited and persisted, but changing it is restart-bound so that the
in-flight semaphore is rebuilt safely; the API returns
`restart_required=true`/`effective_state=pending_restart` until the restart.

## Production validation rule

Production validation must consume only already-enqueued real tasks. It must
not create synthetic destinations, insert test tasks, reset cursors, replay
Spool files, or resend unknown results. If the existing real queue cannot
supply a target rate, the result is `UPSTREAM_LIMITED`, not a failed reason to
inject traffic.
