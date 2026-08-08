# 01 — Provider adapter interface

The plugin seam. Every compute backend implements one interface; the broker knows
only this interface.

> **Revised 2026-08-08** after an analogue scan (sources:
> `outfitter-waypoint-landscape-2026-08-08.md`, Cowork project). Four changes, all
> borrowed from systems already running this problem at scale, and all landing
> *before* M1 writes any Go — which is the only reason they are cheap. In order of
> how expensive they'd be to retrofit: the stateless/stateful split (§1), negative
> capability declaration (§2), cluster-side TTL (§4), and a lifecycle conformance
> suite modelled on Crossplane's `uptest` rather than per-adapter tests (§6).

## 1. Two halves, not one interface

**The broker must be able to enumerate and price candidate placements without
touching a provider.** SkyPilot splits exactly here — `sky/clouds/` holds stateless,
cheap-to-construct cloud objects the optimizer enumerates freely, and
`sky/provision/<provider>/` holds the imperative provisioning logic — and the split
is what lets its optimizer consider 25 backends per request without side effects.

Outfitter's original single `Adapter` interface conflated the two. Split it:

```go
// Describer is stateless, cheap to construct, and free of side effects.
// The broker may construct and call these freely while planning.
type Describer interface {
    Name() string
    Capabilities() Capabilities                              // see §2
    Estimate(ctx, Workload, Lifetime) (CostEstimate, error)  // pure; no charges
    RateCard() RateCard                                      // see specs/02 §2
}

// Provisioner holds real state and talks to a real provider.
type Provisioner interface {
    Provision(ctx, Workload, Lifetime, Reservation) (Placement, error)
    Status(ctx, handle string) (Placement, error)
    Usage(ctx, handle string) (UsageSample, error)
    Teardown(ctx, handle string) error                        // idempotent
}
```

`Provision` now takes a `Reservation` (`specs/03` §2) — an adapter cannot be handed
a workload that has not already been debited against a ceiling. This makes the
invariant structural rather than a rule the broker is trusted to follow.

## 2. Capabilities declare what an adapter CANNOT do

The original `Capabilities()` implied a positive feature list (`gpu?`,
`persistence?`, `max lifetime`). SkyPilot's `CloudImplementationFeatures` inverts
this: **an adapter declares the features it does not support**, and the broker
filters on absence.

The inversion matters because it fails safe on extension. Adding a new capability
to a positive-list interface silently marks every existing adapter as *not* having
it — usually correct. Adding one to a negative-list interface silently marks every
adapter as *having* it — dangerously wrong, but *loud*: the conformance suite (§6)
fails for every adapter that doesn't actually implement it, on the next run. A
lowest-common-denominator interface, the alternative, would erase exactly the
provider differences this broker exists to arbitrate between.

```go
type Unsupported int
const (
    UnsupportedStop Unsupported = iota    // cannot pause; only run or destroy
    UnsupportedGPU
    UnsupportedPersistence
    UnsupportedMultiNode
    UnsupportedSelfExpiry                 // see §4 — this one is special
    UnsupportedUsageSampling              // cannot report usage; cost is derived only
)
```

`UnsupportedUsageSampling` is the honest declaration behind `specs/02` §4's
`estimated` provenance: an adapter that cannot sample usage produces `estimated`
ledger lines by construction, and the ledger can assert that rather than infer it.

## 3. Rules (unchanged)

- **Idempotent teardown.** Calling `Teardown` on an already-gone handle is success.
- **No hidden spend.** `Provision` must accept a `Lifetime`.
- **`Estimate` is pure.** No provisioning, no charges.
- **Capabilities are honest.** The broker filters providers by declared caps.

## 4. Teardown must not depend on the broker being alive

`specs/03` §4 already requires a provider-side dead-man's switch. This section is
the adapter-interface half of that requirement, and the scan produced a sharper
model for it than the external-reaper pattern originally catalogued.

**SkyPilot's autostop runs cluster-side, not from a central reaper** — the client
can die and teardown still happens. Its `wait_for: none` mode converts an
idle-culler into a **hard wall-clock TTL that fires regardless of running jobs**.
That is strictly stronger than jupyterhub-idle-culler's external poller, because it
survives control-plane loss rather than merely surviving control-plane restart.

**Chameleon Cloud goes further still**, and is worth naming as the ideal: via
OpenStack Blazar, every bare-metal host, VLAN, and floating IP is obtained as a
**time-bounded lease**. There is no un-expiring resource to reap, because the
reservation system *is* the teardown path. Where a provider offers a lease
primitive, an adapter should use it in preference to any teardown call.

**Requirement:** an adapter declaring `UnsupportedSelfExpiry` is admissible only
under an explicit deployment-level override, and the broker must log every such
placement as carrying reaper-dependent teardown risk. The default posture is that
self-expiry is mandatory, and §6's conformance suite tests it with the broker
process killed.

## 5. Isolation tier is a provisioning parameter

Cross-reference `specs/08-isolation-tiers.md`. `Workload` carries an
`IsolationTier` field; an adapter that cannot honour the requested tier declares so
through §2's capability set and is filtered out at planning time rather than
silently downgrading. **Silently satisfying a request for a stronger boundary with
a weaker one is the single worst failure this interface could permit** — a
downgrade must be a refusal, never a substitution.

## 6. Conformance: one provider-agnostic lifecycle suite

The original spec's table-driven suite is right in shape but too shallow. Model it
on **Crossplane's `uptest`** — the only project in this space with a genuine
provider-agnostic conformance harness (Terraform/Pulumi have per-provider
acceptance *frameworks*, where each provider writes its own tests; that is not the
same thing and does not produce comparable guarantees).

The suite every adapter must pass:

1. `Estimate` returns a bounded worst case, with no side effects (assert no
   provider-side resource appeared).
2. Provision → `Status` reaches `Running`.
3. `Usage` returns a sample, **or** the adapter declares
   `UnsupportedUsageSampling`.
4. Teardown → `Status` reaches `Terminated`.
5. Teardown again → success (idempotence).

Plus **three assertions no existing harness in the surveyed field makes**, which
are where Outfitter's actual invariant lives:

6. **No resource is reachable without a recorded reservation.** Attempt a
   `Provision` with a nil/expired `Reservation`; assert refusal, and assert no
   provider-side resource was created.
7. **Teardown completes with the broker process killed mid-flight.** Provision,
   `SIGKILL` the broker, wait past `Lifetime`, then assert from a fresh process
   that the provider-side resource is gone. An adapter that only tears down under
   an active broker fails (`specs/03` §4).
8. **A requested isolation tier is honoured or refused, never downgraded**
   (§5).

`docker` is the reference implementation and the baseline the suite is validated
on.
