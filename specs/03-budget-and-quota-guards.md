# 03 — Budget & quota guards

**Invariant: no compute is ever provisioned without a ceiling, a lifetime, and a
teardown path.** Enforced in three places so neither a crashed requester nor a
crashed broker can leak spend.

> **Revised 2026-08-08** after a prior-art scan (full scan with sources:
> `routing-gateway-brokering-landscape-2026-08-08.md`, Cowork project). Three
> additions, all borrowed from systems that already survive this problem at
> scale: reserve-then-reconcile (§2, from Slurm), the provider-side dead-man's
> switch (§4, from the observation that every OSS reaper leaks), and
> reconciliation-by-diff (§5). The original in/out-of-band structure is intact.

## 1. On the way in (broker)

- Reject a placement with no budget ceiling or no lifetime.
- Pre-check projected cost (`Estimate`) against the remaining course/term
  budget; refuse if it would breach.
- Enforce quotas: max concurrent placements per course/student.

## 2. Reserve at admission, reconcile at teardown

**The naive check is wrong under concurrency.** "Estimate, compare to remaining
budget, provision, charge on teardown" lets forty students pass the same check
simultaneously against the same unspent balance and collectively blow a course
budget that each of them individually fit inside. This is not hypothetical — it
is the standard failure of every check-then-act budget guard, and LiteLLM has a
live instance of it on the inference side (BerriAI/litellm#27735, budget
enforcement reading stale cached spend).

**Slurm's accounting has solved this and should be copied directly.**
`sacctmgr`'s hierarchical associations debit the *maximum possible* cost at
submit time and credit back the unused remainder when the job exits; jobs are
refused at submit when the reservation would exceed `GrpTRESMins`. The mapping
onto Outfitter is one-to-one:

| Slurm | Outfitter |
|---|---|
| Cluster → Account → sub-Account → User | institution → course → assignment → student |
| `GrpTRESMins` (hard lifetime cap, refuse at submit) | the budget ceiling |
| `GrpTRESRunMins` (cap on in-flight burn) | max concurrent committed spend |
| `MaxWall` | placement lifetime |
| QOS (policy layered orthogonally to accounts) | the instructor-set tier |

**Requirement:** admission debits `ceiling` (the bounded worst case for the
placement's full lifetime), not `Estimate`'s expected value. Teardown credits
back `ceiling − actual`. The reservation must be written atomically against the
same record the next admission check reads — a write-through counter, not a
cached read. A placement whose reservation cannot be written is refused.

**Consequence to accept deliberately:** reserving worst-case makes the guard
conservative — a course can be refused capacity it would statistically have had
room for. That is the correct direction to be wrong in, and it is why the
credit-back on teardown matters: the reservation must be released promptly, or
conservatism compounds into unusability.

## 3. Out of band (reaper)

- **Lifetime:** teardown at `provisioned_at + lifetime`, always.
- **Idle:** teardown when `Usage` shows idle beyond a threshold.
- **Budget breach:** if accumulating cost crosses the ceiling, teardown and mark
  the placement `Terminated(reason=budget)`.
- The reaper works only from persisted placement records + `Teardown`, so it
  survives requester or broker restarts.

**The reaper must not share a failure domain with the provisioner.** This is the
shape `jupyterhub-idle-culler` (BSD-3) settled on and it is the right one: a
separate service polling the record store, not an in-process goroutine that dies
with the thing it is supposed to outlive.

**But a cluster-side timer beats an external reaper, and a lease beats both**
(revised 2026-08-08). Surviving *restart* is a weaker property than surviving
*loss*. Three tiers; an adapter should reach for the strongest its provider
supports (`specs/01` §4):

1. **Lease** (strongest). Chameleon Cloud's Blazar model: every host, VLAN and
   floating IP is obtained as a time-bounded reservation, so there is no
   un-expiring resource to reap — the reservation system *is* the teardown path.
2. **Cluster-side timer.** SkyPilot's autostop runs on the provisioned resource
   itself, and its `wait_for: none` mode is a hard wall-clock TTL that fires
   regardless of running jobs. The control plane can be gone entirely.
3. **External reaper.** The jupyterhub-idle-culler pattern — the fallback, not the
   design target, and the only one of the three a network partition can defeat.

**A stop verb is a pricing decision.** Where a provider bills differently per state
(Jetstream2: suspended 75%, stopped 50%, shelved 0% — `specs/02` §2), "teardown" is
under-specified. The reaper must choose the cheapest state satisfying the policy and
record which verb it issued, or idle-culling will appear to work while still billing
three-quarters of the rate.

## 4. The provider-side dead-man's switch

**Every OSS teardown mechanism surveyed leaks** — sweepers (`aws-nuke`,
`cloud-nuke`) are filter-and-age based and dangerous as a primary path;
Kubernetes finalizers wedge objects indefinitely when their controller dies.

**The most reliable teardown is not in the control plane at all.** Set the TTL
provider-side, at creation, so it fires whether or not Outfitter is alive:

- EC2 / VM: `InstanceInitiatedShutdownBehavior=terminate` plus in-guest
  `shutdown -h +N`
- Kubernetes: `activeDeadlineSeconds` (and TTL-after-finished for jobs)
- Cloud Run / Fargate: native max-duration
- Slurm: `MaxWall` on the association
- Docker (reference adapter): a supervising timeout, not a bare `docker run`

**Requirement on the adapter interface (`specs/01`):** conformance must assert
that an adapter's provisioned resource terminates on its own after `lifetime`
**with the broker stopped**. An adapter that can only be torn down by an active
broker does not satisfy the invariant and does not pass.

Pegasus/HTCondor's `periodic_remove` is worth studying as a declarative
expression language for exactly this predicate.

## 5. Reconciliation by diff, not by trust

Teardown must be **idempotent**, and correctness cannot rest on the broker's own
state being accurate — the partition that loses a teardown is the same partition
that loses the record of having attempted it.

A reconciliation loop lists provider-side resources by their attribution tags and
diffs that against the ledger:

- **In provider, not in ledger** → orphan. Tear down; record it; this is the
  leak the invariant exists to prevent, and its rate is the health metric for
  the whole guard.
- **In ledger, not in provider** → already gone. Release the reservation (§2)
  and close the record.

The orphan count belongs in the exported report (`specs/02`), not only in logs.
A guard that never reports its own misses is indistinguishable from one that
never misses.

## 6. Failure posture

Fail closed. If cost or usage can't be determined, prefer teardown over
open-ended spend. Every teardown reason is recorded for the ledger and
reporting.

Fail-closed applies to admission too: if the reservation store is unreachable,
refuse to provision. Provisioning without a recorded reservation is the one
state from which the ceiling is unrecoverable.

## 7. What is genuinely novel here, and what is borrowed

Borrowed and settled — do not reinvent: the hierarchical account model and
reserve-then-reconcile (Slurm, GPLv2, decades of production), the external idle
culler (JupyterHub), template-level ceilings the consumer cannot raise (Coder's
`max_ttl` — the instructor sets it, the student cannot).

Genuinely uncontested — **with the claim narrowed 2026-08-08 after stress-testing
it.** The original wording ("nobody blocks provisioning on a predicted monetary
ceiling") was too absolute and would not survive a reviewer who knows the field.
Single-provider hard caps now exist:

- **Google Cloud Spend Caps** (public preview, 2026) genuinely hard-block: attach
  cap enforcement to a budget and the capped service returns **403 Permission
  Denied** within ~2 minutes of the threshold, liftable only by manual request.
  Limits: **one service at a time**, no project-wide cap, mutually exclusive with
  Pub/Sub budget notifications.
- **AWS Budgets Actions** can attach a Deny SCP or IAM policy, or stop EC2/RDS, on
  an actual *or forecasted* threshold. Real blocking, but gated on billing-data
  latency (hours) and account-scoped — not per-student.
- **Cloud Custodian** (Apache-2.0, CNCF) never blocks a create call; it is
  reactive. Its `mark-for-op` TTL tagging and `offhours` scheduling are good
  *teardown* primitives worth borrowing for §3, but they are not admission control.

**The defensible claim:** nobody blocks provisioning on a predicted monetary
ceiling *across heterogeneous providers, with per-`{course, assignment, student}`
attribution*. Slurm's `GrpTRESMins` does it in TRES-minutes on one provider; GCP
does it for one service; nothing does it in dollars across five. State it this way —
the narrowed claim is both true and stronger, because it names the two systems that
come closest and says exactly where they stop.

**Where the gate lives, structurally.** Borrow SkyPilot's **Admin Policy** shape: a
single server-side `validate_and_mutate(workload, config)` hook on the broker API,
unbypassable because client-side copies are advisory only. Its shipped examples map
almost one-to-one onto what this spec needs — `EnforceAutostopPolicy` (refuse any
launch without an idle timeout), `SetMaxAutostopIdleMinutesPolicy` (cap the ceiling
a caller may request), `GPUStaticQuotaPolicy` (per-user quota by inspecting existing
placements), `RateLimitLaunchPolicy`. One server-side gate is the correct
architecture; scattering ceiling checks across adapters is not.

The hard part remains §2 plus `specs/02`'s estimator, not the plumbing.
