# Native SU accounting and independent cleanup preparation

This increment supplies the durable journal and cleanup supervisor behind the
existing Jetstream2 worker seam. Docker remains the accepted pilot baseline.
No live worker, cloud cluster or paid dependency was created. It follows accepted
PR #6 and recovery PR #7 (merged main `abfcfa280a0982e03b80a50e0349a5ef99a13a41`).
It contributes to Ben Markwart's existing issues [#3](https://github.com/EduCloud-Ecosystem/outfitter/issues/3)
and [#4](https://github.com/EduCloud-Ecosystem/outfitter/issues/4); their assignments
and live acceptance gates remain unchanged. Issue #5 follows live lifecycle acceptance.

## Operator contract

The existing private local ledger now holds native accounts and reservations as
well as local CPU-second accounts. It writes version 3 when a native account is
first configured; older binaries refuse it. Existing v1/v2 Docker state remains
readable without changing its budgets. Checkpoint, held restore, one-way fencing,
state-path binding and the process-shared execution lease apply to both units.
There is one total limit of 512 accounts and 512 requests, with reserved space for
cleanup and bounded metadata. There is no second database or network filesystem.

The adapter library's `ConfigureJournal` accepts an immutable configuration, a
dated per-flavor rate, a native allowance, named supervisor and finite approval
expiry. This is an operator assertion for a supervised experiment, not automatic
qualification of that supervisor. `OpenJournal` only opens an existing policy.
No executable command configures a cloud permit or launches a cloud worker.
`CreateWorker` remains a library seam requiring the durable journal. It reserves
before POST and records separate port and server attempts. Process locks prevent
concurrent creator/cleanup effects; request IDs, resource IDs and absolute deadlines
cannot be reused for a different placement.

The cleanup-only command is:

```
outfitter --state-dir PRIVATE_STATE jetstream2-reap --config PRIVATE_CONFIG
```

It authenticates with allocation-scoped application credentials from the existing
environment variables, never guest credentials. An external service may invoke
it every 15 seconds after separate deployment approval. Each invocation has a
two-minute bound; per-worker teardown has a 30-second bound. Sweep JSON reports
per-request failures and the command exits nonzero when cleanup remains unresolved.
It creates no worker, network, floating IP, volume or snapshot. Do not deploy it
from this document alone: the independent host and state-access plan remain gates.

The supervisor recovers missing IDs using the exact persisted random reservation
token, owner, request, deterministic name, project and image/flavor/network pins.
It refuses duplicate matches, foreign ownership, unknown attached storage and
incomplete inventory. It checks recorded resources again before deletion and
confirms absence separately. Discovery scans one bounded project inventory and
refuses pagination; large allocations may require a reviewed paginated inventory
implementation before qualification. Repeated cleanup cannot retry a create.

## Accounting and recovery limits

| Record | Meaning | Can release funds? |
| --- | --- | --- |
| Local CPU seconds | Docker's existing conservative CPU reservation | Only its existing local completion contract |
| Native reservation (`milli_su`) | 1,000 milli-SU = 1 provider SU; finite liability | No |
| `estimated` observation | Cumulative estimate with dated evidence | No |
| `provider_observed` observation | Cumulative provider sample, possibly delayed | No |
| Explicit provider settlement | Immutable amount plus retained evidence SHA-256 | Only after unambiguous effects and verified cleanup |
| Retained storage | Separate inventory/quota/retention responsibility | Never assumed removed or priced by SU settlement |

Observations append by immutable event ID and are cumulative, not additive charges.
At most 64 observations are retained per request; exhausting that optional history
refuses a new observation without blocking cleanup. Unknown or ambiguous work keeps
its full reservation after deletion. Ambiguous requests cannot currently be settled
automatically, even after later discovery; resolution needs a future reviewed
provider-evidence protocol. Reported overruns are retained and stop new native
admission. Settlement is a trusted operator/library operation; the software cannot
prove the provenance of an operator-supplied evidence digest. Keep its source report
outside the repository and review it before settlement.

Restored state remains held. Cleanup is permitted, but admission and settlement
are refused; activation refuses unresolved native liabilities. A checkpoint copied
to a second machine does not by itself provide a current independent cleanup
ledger. This implementation supports one private local filesystem and separately
recoverable processes. It does **not** provide a replicated ledger, remote fencing,
provider lease, guest self-expiry, or host/control-plane-loss guarantee. A provider
API outage can delay deletion and exceed the reservation. Mark the supervised
exception and resulting liability visibly; do not promise a hard provider SU cap.

## Finite live-test decision package — not authorized

Review [the proposed manifest](manifests/jetstream2-live-test.proposed.json).
Its null fields are blockers, not defaults. Greg Chism is the proposed named
operator/cleanup owner; confirmation remains pending. The draft slot is October 20,
2026, 10:00–10:15 a.m. Pacific, with a 10:10 absolute worker expiry. Changing the
slot requires a newly reviewed manifest; an expired file is never rolled forward.

Proposed envelope: one `m3.tiny` CPU worker, concurrency one, one create attempt,
10-minute lifetime, 1 SU aggregate reservation/ceiling, 20 GB ephemeral root disk,
no additional cloud storage, networks, floating IPs or snapshots. The current
published flavor is 1 vCPU, 3 GB RAM, 20 GB local disk, at 1 SU/hour. Ten minutes
therefore estimates 0.166667 SU; the remaining reservation is contingency, not a
promise that an outage cannot exceed it. Rate checked October 17, 2026 against the
[provider flavor table](https://docs.jetstream-cloud.org/general/vmsizes/).

ACCESS credits and provider SUs remain separately labeled. The provider currently
states a 1:1 exchange, but usage reports may lag by 12–24 hours. Stopped and
suspended machines still consume SUs. Deletion is the intended cleanup action;
shutdown is not accepted as teardown. An estimate never becomes a settled charge
because the worker disappeared. Sources: [credits and SU usage](https://docs.jetstream-cloud.org/general/access/),
[instance management](https://docs.jetstream-cloud.org/ui/horizon/manage/).

Use an existing approved CPU allocation if Greg selects one. `SOC250004` and Ben's
approved allocation are candidates from the existing assignment; neither has been
validated or selected in this increment. Membership, CPU entitlement, balance,
quota and available capacity must be recorded separately. Allocation membership is
required, and quota is shared allocation-wide. Sources:
[allocations](https://docs.jetstream-cloud.org/alloc/overview/),
[quotas](https://docs.jetstream-cloud.org/general/quotas/).

The proposed guest is `Featured-Ubuntu24`; its exact UUID and content hash must be
resolved in the selected region because featured images change over time. Pin an
existing private network/security group and confirm an approved private route for
the synthetic workload. No public-IP workaround is authorized. Sources:
[featured images](https://docs.jetstream-cloud.org/general/featured/),
[application credentials](https://docs.jetstream-cloud.org/ui/cli/auth/).

Decision required before any live run: confirm the operator, allocation, finite
manifest and supervised-expiry exception; approve existing independent cleanup
capacity and an operational durable-state plan. If that needs a paid host or new
allocation, prepare the exact cost/resource request first and obtain a separate
decision. No purchase, subscription, provisioning or production deployment is
included. Kubernetes remains an optional future evaluation; PAIR adds no dependency.

## Live procedure after the separate gates are met

1. Freeze accepted source, dated rate, configuration pins and the manifest digest.
   Run read-only preflight and separately check allocation balance and capacity.
   Verify the independent supervisor can read the current durable request authority.
2. Review the finite live runner and private synthetic-workload handoff before
   enabling creation. Persist the reservation and token before the single attempt;
   capture IDs privately. Do not retry a lost response.
3. Record the deterministic synthetic result (`42`), or retain failed/unknown work.
   At expiry, let the independent supervisor inventory and delete only the exact
   owned server and port. A fresh operator/provider session verifies both absent
   and confirms no extra resources with the same reservation token.
4. If teardown is unconfirmed by 10:15, halt admissions and invoke the named
   operator's rescue procedure. During API loss keep retrying bounded cleanup when
   service returns; record elapsed exposure and any estimate above the ceiling.
   The 11:00 decision-window end does not magically stop provider charges.
5. Retain logs, exact IDs, inventory and accounting separately from workload data.
   Reconcile provider reporting when available; otherwise retain the reservation.
   Report simulation, same-host process recovery, independent-host observation,
   live provider behavior and settled usage as separate evidence categories.
