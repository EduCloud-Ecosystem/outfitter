# Outfitter development assignment for Ben Markwart

October 13, 2026. Greg's selected workstream: build Outfitter with Jetstream2
as its first live provider. The repository is still specification-stage. This
assignment defines the implementation and acceptance work; it does not record
Ben's availability or a completed deployment.

## Outcome

Deliver a Go broker that reserves a finite compute allowance, provisions one
disposable Jetstream2 worker, runs a bounded synthetic workload, records usage,
and verifies teardown. Then demonstrate a narrow handoff to the existing
Waypoint/Cairn course workflow. Keep Docker as the local conformance baseline;
move Jetstream2 ahead of Codespaces. No other cloud adapter is required for this
milestone.

Start with one worker for a synthetic course, not one VM per submission. This
provides a useful infrastructure unit for Waypoint while Cairn retains its
existing grading sandbox. Worker provisioning, workload execution and course
acceptance must have separate results.

## Allocation and test configuration

Use either Ben's own allocation after approval or Greg's existing SOC250004.
The allocation is configuration, never a hard-coded provider assumption. Ben's
proposed allocation must not be represented as awarded or available.

The ACCESS My Projects page on October 13 shows SOC250004 active through
January 22, 2027, with approximately 35.2K CPU SUs and 224K GPU SUs remaining.
These are rounded, changing project balances, not an Outfitter test allowance.
The preferred first live workload is CPU-only on a resource with CPU access.
Verify the selected project's current balance, resource entitlement, quotas and
available flavor before launching. Existing project users share that capacity.

At kickoff, record the project and region, pinned image/flavor, network IDs,
operator, test SU ceiling, maximum runtime, maximum one concurrent instance,
cleanup owner and first-test date. Ben needs his own authorized project access
and application credential; keep credentials outside the repository. A new
allocation proposal should request resources for bounded broker/lifecycle tests
and a synthetic course rehearsal, with a budget derived from selected flavors,
run counts and maximum durations.

## Component boundaries

| Component | Responsibility in this milestone |
| --- | --- |
| Outfitter | Provider resources, placement reservations, native-unit accounting, expiry and owned-resource cleanup |
| Waypoint | Configure/enroll the supplied worker; course admission, ownership, fencing, transfer and retained coursework |
| Cairn | Assignments, roster, grading sandbox and result verification |
| Portage and Belay | No changes required for the first compute lifecycle test |

Agree a versioned placement handoff with the Waypoint workstream: request ID,
opaque course attribution, resource requirements, isolation requirement,
lifetime, reservation, provider resource IDs and an operator-private connection
reference. A provisioned VM is not automatically enrolled or authorized to run a
course. Use a reviewed file/CLI handoff first; a remote broker service is deferred.
Do not replicate Waypoint's course ownership authority in Outfitter.

Persistent course data is outside the disposable test scope. Before later hosting
real courses, define how course closure/export and retained storage interact with
the infrastructure deadline. The broker must refuse a deployment whose retention
and expiry requirements conflict; a TTL must not silently destroy coursework.

## Build sequence

October 15 clarification: the [Spore prior-art review](PRIOR-ART-SPORE-2026-10-15.md)
recognizes existing lifecycle and per-instance spending controls. Reuse suitable
patterns, while keeping implementation focused on shared durable reservations,
Docker conformance, Jetstream2 accounting/cleanup and the placement handoff below.
No AWS adapter, Spore port or dependency, chat/MCP feature, capacity watcher or
general job orchestrator is required. Future AWS reuse evaluation does not block
this assignment or change its acceptance criteria. EC2 guest-side lifecycle
behavior does not establish a Jetstream2 expiry mechanism.

1. **Contract and read-only preflight.** Review specs 01–05 and 08. Resolve the
   placement handoff, state model, reservation store and expiry mechanism. Add a
   Jetstream2 preflight that verifies project/region, identity, flavors, images,
   quotas and network references without creating resources. Record any missing
   allocation-balance or usage API explicitly. Default unknown capabilities to
   refusal; review the old negative-capability scheme before implementing it.
2. **Broker and Docker baseline.** Implement the shared lifecycle and conformance
   harness, atomic durable reservations, request deduplication, and a Docker
   adapter. No no-op budget guard on an executable provisioning path. Persist
   intent before provider effects. Concurrent requests must not reserve the same
   remaining allowance twice; restart must not reset balances or deadlines.
3. **Jetstream2 adapter and first live smoke test.** Use OpenStack APIs behind the
   adapter. Create one approved disposable worker, verify readiness and a known
   synthetic result, then delete and independently confirm cleanup. Start live
   testing once this bounded path and its cleanup controls are ready; do not wait
   for an instructor UI, additional providers or an allocation planner.
4. **Failure and expiry acceptance.** Exercise a lost create response, crash after
   provider creation, duplicate request, provider timeout, quota/budget refusal,
   broker termination, repeated teardown and leftover owned resources. Reconcile
   uncertain outcomes before retrying creation. Keep unresolved liabilities
   reserved. A partial failure must not be reported as successful cleanup.
5. **Waypoint/Cairn integration demonstration.** Hand the accepted placement to a
   pinned Waypoint release and run one synthetic course workflow. Record which
   steps remain operator-mediated. Verify course shutdown/export as applicable,
   then provider teardown. Deliver the runbook, evidence and scoped follow-ups.

## Accounting and cleanup requirements

Keep Jetstream2 service units, ACCESS credits, storage quotas and optional dollar
estimates distinct. Use dated per-flavor rates and observed resource states;
the older blanket GPU multiplier in spec 02 is not a sufficient current rate
card. Record estimates, observed resource usage and provider-settled charges
separately. Unknown charges are not zero. Outfitter can enforce its own reserved
test allowance; it cannot guarantee the entire allocation balance against other
users' independent spending.

Jetstream2 distinguishes stopped, suspended and shelved resources. Guest shutdown
is not proof of resource deletion or zero SU consumption. The disposable test
must inventory the server and any owned ports, floating IPs, volumes and snapshots;
verify each intended deletion and list anything deliberately retained. Reuse
shared networks without claiming ownership of them. Tags help discovery, but
deletion requires recorded resource IDs and verified ownership. Do not implement
spec 03's broad orphan deletion against unrelated allocation resources.

Prove an expiry mechanism that survives broker loss. Do not assume Jetstream2
offers a native lease or that a guest timer can delete its own VM. Keep cloud
management credentials out of workload guests. If only an external cleanup
service is feasible, declare the limitation and its independent failure domain;
use the explicit exception mechanism in spec 01 for any supervised experiment.
Such an experiment does not pass the default self-expiry contract. Provider
outages can prevent deletion; preserve the liability and expose the failure
instead of promising a hard ceiling beyond the demonstrated failure model.

## Acceptance evidence

- Source revisions, sanitized configuration and dated rate card.
- Conformance cases with passed, failed and not-run results; Go tests, race tests
  for concurrent reservations, vet and the selected lint checks.
- No create for absent/expired reservations, excessive cost, unknown required
  capability or unavailable ledger; no duplicate VM on ambiguous retry.
- Exact resource ownership and cleanup evidence from a fresh observer after
  normal completion, interruption and broker loss.
- Estimated versus observed versus settled usage with unresolved amounts retained.
- One repeatable live lifecycle run and a separate Waypoint/Cairn integration
  result. A VM reaching ACTIVE alone does not satisfy course readiness.

Ben's first review should contain the contract, preflight result, expiry design
and a dated estimate for the first live test. Deliver subsequent work in scoped,
DCO-signed PRs. No fixed completion date is assumed before availability and the
expiry approach are known. Codespaces, HPC, Pegasus, automatic migration,
multi-provider optimization and instructor dashboards remain deferred.

## Current provider references

Verified October 13, 2026; recheck at live-test kickoff.

- [ACCESS My Projects](https://allocations.access-ci.org/) — authenticated
  SOC250004 resource/balance observation; project data is not a public rate card.
- [Jetstream2 application credentials](https://docs.jetstream-cloud.org/ui/cli/auth/)
  — allocation-specific authentication and separate user/client credentials.
- [Instance flavors](https://docs.jetstream-cloud.org/general/vmsizes/) — resource
  categories and per-flavor SU rates; CPU/GPU/large-memory access is distinct.
- [Quotas](https://docs.jetstream-cloud.org/general/quotas/) — shared project limits
  and current usage inspection.
- [Instance management](https://docs.jetstream-cloud.org/ui/horizon/manage/)
  — state-dependent SU consumption and storage consequences.

The source review and ACCESS observation do not establish available host capacity,
Ben's project membership, provider self-expiry or a successful live test.
