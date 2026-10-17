# 12 — Native-unit journal and recoverable cleanup

Extends the accepted local store and Jetstream2 Journal seam. Docker remains the
pilot baseline. This increment exposes no worker-creation CLI and authorizes no
cloud resources. It implements the simulated prerequisite for issues #3 and #4;
it does not close their live acceptance criteria or change their assignments.

## Durable authority

Native accounts and reservations share the existing locked, fsynced authority
file, checkpoint and held-restore protocol. Version 3 is written only when a native
account is configured; v1/v2 local ledgers remain compatible. Older binaries must
reject v3. Native units never enter local CPU-second totals. Each immutable account
pins provider, unit, scope, allowance and an adapter policy. A request commits its
identity, random reservation token, payload/configuration identity, finite native
reservation and absolute deadline before provider effects. One unsettled native
request is allowed per store; duplicate identical requests reuse their record.
Unknown charges continue to consume their reservation after resource deletion.
Only explicit, evidenced provider settlement can release the difference; estimates
and observed resource state cannot settle charges. Settlement above the ceiling is
recorded as an overrun and prevents further admission, not discarded or clamped.
No automatic dollar or ACCESS-credit conversion or retained-storage accounting is
implied. The initial adapter creates only an image-backed server and owned port.

## Effects and recovery

Per-request process locks serialize creator and supervisor effects, alongside the
existing execution lease used by cutover. No filesystem shared across hosts is
supported. Persist separate port/server attempt markers before POST. An interrupted
or failed POST is never retried; later discovery uses the exact durable reservation
token, owner, request, project and pinned resource attributes. Tags alone do not
authorize deletion. Multiple candidates or incomplete/paginated inventories refuse
cleanup and remain visible. Every recorded resource is freshly ownership-checked
before DELETE. Delete responses require a separate absence observation.

The independently invoked supervisor lists durable requests, checks absolute
expiry, reconciles exact ownership and performs teardown without requester or guest
cooperation. API outages, unexpected storage, foreign resources and ambiguous
absence remain liabilities. A late resource after an ambiguous absence is eligible
for subsequent reconciliation; unknown requests never free their reservation.
A failed cleanup retries through a later supervisor invocation, not a new create.
Admission must include a named supervisor, finite approval expiry and explicit
supervised-experiment exception. This does not establish host/control-plane-loss
self-expiry or guarantee an SU cap during provider/network/supervisor outage.

Held restored ledgers can inspect and clean up; they cannot admit or settle stale
native accounting. Activation refuses any unresolved native reservation. Native
state is part of the exact checkpoint hash and recovery report.

## Evidence required

Use a simulated HTTPS OpenStack provider to test concurrent admission, repeated
requests, ambiguous port/server creation, persisted-ID loss, process death before
and after effects, worker loss, API loss, deadlines, duplicate/foreign resources,
partial deletion, stale saves, accounting uncertainty and held restore. At least
one test kills the original creator process and launches cleanup in a different
process using only durable state. Label this same-host process evidence, not an
independent host or live provider. Live qualification still needs separately
approved allocation/capacity, pinned inputs, supervisor failure-domain acceptance,
provider observation and native settlement evidence.
