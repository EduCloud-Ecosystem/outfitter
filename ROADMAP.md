# Roadmap — Outfitter

Status legend: ☐ not started · ◐ in progress · ☑ done

## M0 — Spec-first scaffold (this commit)
- ☑ Repo, remote, governance (Apache-2.0 + DCO), `DESIGN.md`, `specs/00–05`
- ☑ Stack decision (Go) recorded
- ☑ Claude Code kickoff + handoff

## M1 — Skeleton & the contract
- ☐ Go module layout (`cmd/outfitter`, `internal/broker`, `internal/provider`, `internal/cost`, `internal/policy`)
- ☐ Provider adapter interface + conformance test harness (`specs/01`)
- ☐ `docker` reference adapter passing conformance
- ☐ Broker skeleton: Estimate → Provision → Status → Teardown

**M1 scope amended 2026-08-08** after an analogue scan (full scan:
`outfitter-waypoint-landscape-2026-08-08.md`, Cowork project). Four corrections to
`specs/01`, all cheap now and expensive later, so they land before any Go is
written:
- ☐ Split the adapter into stateless `Describer` (enumerate + price, no side
  effects) and stateful `Provisioner` — the broker must plan without touching a
  provider (SkyPilot's `clouds/` vs `provision/` split)
- ☐ Capabilities declare what an adapter **cannot** do, not what it can
  (SkyPilot's `CloudImplementationFeatures`) — hardest of the four to retrofit
- ☐ `Provision` takes a `Reservation`, so an unbudgeted workload is structurally
  unrepresentable rather than merely disallowed
- ☐ Conformance modelled on Crossplane's `uptest` (provider-agnostic lifecycle
  suite), with three assertions no surveyed harness makes: no resource without a
  reservation; **teardown completes with the broker SIGKILLed mid-flight**;
  isolation tier honoured or refused, never downgraded

## M1.5 — Isolation tiers (`specs/08`, new)
- ☐ `IsolationTier` on `Workload`; adapters declare which tiers they can honour
- ☐ gVisor (`runsc`) as the `T-standard` default for the docker adapter
- ☐ Kata RuntimeClass path for `T-strong` (long-lived workspaces, arbitrary
  instructor Dockerfiles, GPU)
- ☐ Conformance assertion: a tier request is satisfied or refused, never silently
  downgraded

## M2 — First real provider + guards
- ☐ `codespaces` adapter (pairs with Cairn's GitHub path)
- ☐ Budget ceiling + lifetime + reaper (idle/teardown) (`specs/03`)
- ☐ Cost ledger + normalized unit + one rate card (`specs/02`)

## M3 — Cairn integration
- ☐ Broker API Cairn can call for grading sandboxes (`specs/04`)
- ☐ Per-course / per-assignment attribution + exportable cost report

## M4 — Research/grant compute
- ☐ `hpc` (SSH/Slurm) adapter
- ☐ `jetstream2` (OpenStack / ACCESS) adapter
- ☐ `pegasus` adapter
- ☐ Multi-provider policy selection
- ☐ Planner P1: telemetry policy with predictive pre-provisioning (`specs/06`)

## M5 — LLM allocation planner (post-pilot)
- ☐ Planner P2: LLM plans validated by the deterministic plan checker
- ☐ Shadow scoring against P1; promotion by non-inferiority gate
- ☐ Operations lane through Portage (system telemetry only); decision log;
  confirm thresholds; kill switch

> Dates intentionally omitted until the MVP consumer is fixed (`specs/05`,
> open question in `DESIGN.md §8`). Sequence is firmer than calendar here.

---

## Build vs. integrate vs. skip (added 2026-08-08)

From the analogue scan. The point of this section is to stop Outfitter building
things that already exist in mature open-source form, so the effort concentrates on
the part that is genuinely uncontested (`specs/03` §7).

**Do not build — adopt instead:**

| Concern | Adopt | Why |
|---|---|---|
| Allocation request → approval → expiry → renewal portal | **ColdFront** (AGPLv3, University at Buffalo CCR) | Exactly this, in production at HPC centres, with a Slurm plugin that writes scheduler account associations from allocation attributes. AGPLv3 needs a decision, but as a separately-deployed service, not linked code. |
| Usage warehouse + chargeback reporting | **Open XDMoD** (LGPL-3) | Cloud/Job/Storage realms already modelled; ACCESS-native, so campus RCS staff already read it (`specs/02` §4). |
| Normalized cost unit | **Posit's `(GB RAM + vCPU)/2 × hours`** | One line; settles a design argument (`specs/02` §2). |

**Build — nobody has it:** the monetary ceiling itself. Reserve-then-reconcile
against a *predicted* dollar cost, per `{course, assignment, student}`, across
heterogeneous adapters, with pre-provision denial. This is the whole contribution.

**Neither fork nor depend on SkyPilot — port three ideas.** Depending means
inheriting Python, a Ray backend, 25 unwanted clouds, an open-core vendor whose
proprietary tier is exactly the multi-tenancy a classroom needs, and no
first-class Docker-on-owned-hardware path. Forking means owning ~200k lines. Port
instead: capability flags (M1), cluster-side autostop with hard-TTL mode
(`specs/03` §3), and `validate_and_mutate` as the single pre-provision gate
(`specs/03` §7).

**Skip:** Nomad (BUSL; resource quotas are Enterprise-only), Karpenter (K8s-only,
resource caps never dollars), Komiser/Vantage/CAST AI (visibility only), building a
Terraform-style IaC provider model (different problem).

## Admin — the ACCESS/CloudBank path (added 2026-08-08)

Highest-ROI non-engineering item on this roadmap, and it is nearly free.

- **Now (weeks 0–4): file an ACCESS *Explore* allocation.** Abstract-only, ~2-week
  turnaround, 400,000 credits, and instructors running classroom activities are
  *named as eligible*. Exchanges into Jetstream2 at 1:1. This gives Outfitter a real
  second adapter (OpenStack) with a documented, state-dependent rate card to
  validate the ledger against, at zero cost and near-zero paperwork.
- **Fall 2026: apply to CloudBank** (NSF-funded, brokers AWS/GCP/Azure credits
  through ACCESS/NAIRR) — no fee, and it levies no indirect costs on cloud spend.
  Gives hyperscaler adapters with real billing exports to validate the FOCUS mapping
  against, without a purchase order.
- **Defer:** Discover/Accelerate until a deployed pilot exists to write a project
  description about; Maximize (6-month panel cycle) is out of scope until 2027.

**One constraint worth designing around:** ACCESS deliberately does **not** extend
education allocations past course end. That is the same invariant this repository
enforces, which makes ACCESS an unusually good validation target — build the
adapter so course-end teardown is driven by allocation expiry and the two systems
agree by construction.
