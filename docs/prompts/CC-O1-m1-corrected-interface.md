# CC-O1 — M1, built against the corrected adapter interface

*Claude Code prompt. Authored in Cowork, 2026-08-08. `specs/01` was revised this
same day after an analogue scan (full scan:
`outfitter-waypoint-landscape-2026-08-08.md`, Cowork project) that found four
corrections worth making **before** M1 writes any Go. This prompt is M1 as
originally scoped, built against the revised spec rather than the old one.

The corrections, and why each is cheap now and expensive later:
1. **Stateless/stateful split.** The broker must enumerate and price candidates
   without touching a provider. SkyPilot splits `clouds/` (stateless, cheap to
   construct, freely enumerable by the optimizer) from `provision/` (imperative),
   and that split is what lets its optimizer consider 25 backends per request with
   no side effects. Retrofitting this means rewriting every adapter.
2. **Negative capability declaration.** An adapter declares what it *cannot* do,
   not what it can. A lowest-common-denominator positive interface would erase
   exactly the provider differences this broker exists to arbitrate between.
   Hardest of the four to retrofit.
3. **`Provision` takes a `Reservation`**, so an unbudgeted workload is
   structurally unrepresentable rather than merely disallowed by a rule the broker
   is trusted to follow.
4. **Conformance modelled on Crossplane's `uptest`** — a provider-agnostic
   lifecycle suite, not per-adapter tests. Terraform and Pulumi have per-provider
   acceptance *frameworks* where each provider writes its own tests; that is not
   the same thing and does not produce comparable guarantees.

**Read `specs/01` in full before starting.** It is the normative version; this
prompt is the build order, not a restatement.*

---

## 1. Read first

- `specs/01-provider-adapter-interface.md` (revised) — the contract, normative
- `specs/03-budget-and-quota-guards.md` §2 (reserve-then-reconcile) and §7 (the
  `validate_and_mutate` single-gate architecture) — `Reservation` comes from here
- `specs/02-cost-model-and-reporting.md` §2 (normalized unit, state-dependent rate
  cards) — `RateCard` and `Estimate`'s worst-case obligation come from here
- `specs/08-isolation-tiers.md` §5 — `IsolationTier` on `Workload`
- `DESIGN.md`, `ROADMAP.md` (M1 and the new M1.5)

## 2. Build order

Roughly the original M1, adjusted for the split:

1. **Go module layout** — `cmd/outfitter`, `internal/broker`, `internal/provider`,
   `internal/cost`, `internal/policy`, per ROADMAP M1. Consider mirroring
   SkyPilot's separation inside `internal/provider`: a stateless describe/price
   package and a stateful provision package, rather than one package per adapter
   holding both.
2. **The two interfaces** (`Describer`, `Provisioner`) and the supporting types
   (`Workload` with `IsolationTier`, `Capabilities` as the negative-declaration
   set, `Reservation`, `RateCard`, `CostEstimate`, `Placement`, `UsageSample`).
3. **The conformance harness** — write this *before* the docker adapter, so the
   adapter is developed against it rather than validated after the fact.
4. **The `docker` reference adapter**, passing conformance.
5. **Broker skeleton** — plan (enumerate `Describer`s, filter by capability, price)
   → reserve → provision → status → teardown.

## 3. The conformance suite is the deliverable that matters most

Per `specs/01` §6. The standard lifecycle (estimate with no side effects →
provision → running → usage-or-declared-unsupported → teardown → terminated →
idempotent re-teardown) plus the three assertions that encode Outfitter's actual
invariant and that no surveyed harness makes:

- **No resource is reachable without a recorded reservation.** Attempt `Provision`
  with a nil or expired `Reservation`; assert refusal *and* assert no provider-side
  resource was created.
- **Teardown completes with the broker SIGKILLed mid-flight.** Provision, kill the
  broker process, wait past `Lifetime`, then assert from a fresh process that the
  provider-side resource is gone. This is the test that makes `specs/03` §4's
  dead-man's-switch requirement real rather than aspirational, and it is the one
  most likely to be quietly softened into something easier. Do not soften it — if
  the docker adapter cannot pass it as written, that is a finding about the docker
  adapter (it likely needs a supervising timeout rather than a bare `docker run`,
  per `specs/03` §4), not a reason to weaken the assertion.
- **An isolation tier is honoured or refused, never downgraded.**

## 4. Explicitly out of scope

Do not build in this prompt: the cost ledger (M2), the reaper (M2), the codespaces
adapter (M2), the allocation planner (M4/M5), or any real cloud adapter. If the
interface work suggests changes to `specs/02`'s ledger design, **report them
rather than implementing them** — the ledger's provenance model has already been
specified and should not be re-derived incidentally here.

Also do not build an allocation-request/approval portal or a usage warehouse:
ROADMAP's new build-vs-integrate section records ColdFront (AGPLv3) and Open XDMoD
(LGPL-3) as adopt-not-build for those, and starting a homegrown version inside M1
would be exactly the duplication that section exists to prevent.

## 5. Report

- Confirm the conformance suite exists and the docker adapter passes all of it,
  including the three new assertions — with the SIGKILL test's actual output, since
  that one is the load-bearing claim.
- Any place the negative-capability model was awkward in Go (it is an unusual
  shape; if a positive set plus a "declared complete" marker reads better in
  practice, say so with reasoning rather than silently inverting the spec).
- Anything in `specs/01` that turned out to be wrong or underspecified once it met
  real code — flag it for a spec revision instead of diverging quietly. The spec
  was written from an analogue scan, not from implementation experience, and this
  is the first contact with reality.
- `go test ./...`, `go vet ./...`, and whatever linter the repo settles on, green.
