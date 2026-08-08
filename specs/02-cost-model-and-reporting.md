# 02 — Cost model & reporting

> **Revised 2026-08-08** after a prior-art scan (sources:
> `routing-gateway-brokering-landscape-2026-08-08.md`, Cowork project). Two
> additions: the estimated-vs-reconciled distinction (§4), which the ledger has
> to carry structurally because some providers will never tell us the per-student
> number; and FOCUS as the line-item schema (§5) rather than inventing one. The
> normalized-unit approach is unchanged.

## 1. The problem

Providers price in different units: Codespaces core-hours, HPC service units,
Jetstream2/ACCESS credits, a flat campus rate, or $0 for owned hardware. A course
needs one number it can reason about and report.

## 2. Approach

- **Normalized unit.** Define an internal `ComputeUnit`; each provider has a
  **rate card** mapping its native usage → `ComputeUnit` → currency.

  **Adopt Posit Cloud's formula rather than inventing one** (revised 2026-08-08):
  compute hours = `(GB RAM + vCPUs) / 2 × wall-hours`. It is the cleanest
  provider-agnostic unit found in the analogue survey, it makes Docker, Codespaces
  and Jetstream2 commensurable *before* dollars are known, and it settles a design
  argument in one line.

  **A rate card is a function of resource STATE, not just size.** Jetstream2 bills
  1 SU per vCPU-core-hour, 2× for large-memory and GPU flavours, and then:
  **suspended instances bill 75%, stopped 50%, shelved 0%**. A rate card modelling
  only "running vs. gone" misprices every idle student VM. `RateCard`
  (`specs/01` §1) must express a per-state multiplier — which makes the reaper's
  choice of *which* stop verb to issue (`specs/03` §3) a cost decision, not only a
  lifecycle one.

  **ACCESS credits are a real cross-provider currency** and worth modelling as a
  first-class unit alongside dollars: 1 credit ≈ 1 core-hour or 1 GB storage,
  exchanged at fixed per-resource rates (Jetstream2 exchanges 1:1). A course funded
  by an allocation has a ceiling denominated in credits; forcing it through a dollar
  conversion loses the unit the institution actually accounts in.
- **Attribution.** Every placement carries opaque `course/assignment/student`
  labels; usage rolls up along those dimensions.
- **Ledger.** Append-only usage events (`handle, provider, sampled usage, ts`) →
  cost computed from the rate card. Privacy-minimal: IDs are opaque, no records.
- **Reports.** Export per-course / per-assignment / per-term cost, suitable for
  grant budgets and F&A conversations, as CSV/JSON.

## 3. Bounding cost before provisioning — the hard part

`specs/03`'s invariant is only enforceable if cost can be **bounded before
provisioning**, and that is this spec's genuinely difficult obligation.
`Estimate()` must return not just an expected value but a defensible *worst
case* over the placement's lifetime, because that worst case is what admission
reserves (`specs/03` §2).

The providers differ in how hard this is, and the difficulty is worth stating
plainly rather than discovering per-adapter:

| Provider | Bounding worst case |
|---|---|
| `docker` (owned hardware) | trivial — effectively $0, bounded by lifetime |
| `codespaces` | tractable — machine type × lifetime, published rates |
| `hpc` (Slurm) | tractable — the scheduler already enforces `MaxWall`/`GrpTRESMins`; adopt its bound rather than computing a second one |
| `jetstream2` | tractable — SU rate × flavour × lifetime |
| `pegasus` | **may be unbounded** — a DAG whose fan-out isn't known until it runs |

**Position: refuse to admit a workload whose cost cannot be bounded.** For an
unbounded DAG that means either a caller-supplied hard ceiling that the adapter
enforces by killing the DAG at the boundary, or refusal. It does not mean
admitting it with an estimate and hoping. This follows directly from the
fail-closed posture and should not be quietly relaxed when the Pegasus adapter
is written (M4).

Reuse for the estimator: seed from `InfoSciCourses/compute-cost-simulator` (its
cost model and curriculum/dashboard work) rather than rebuilding. Infracost's
cloud-SKU price database (Apache-2.0) is a reusable source for the rate cards
where the provider is a commercial cloud.

## 4. Estimated vs. reconciled — a structural distinction, not a footnote

**Some providers will never report per-student cost.** GitHub Codespaces bills
the *organisation* against its spending limit; there is no per-seat line item to
read back. The same is true wherever a shared allocation is drawn down by many
users — including ACCESS/Jetstream2 education allocations, which have no
per-student sub-allocation at all (the PI adds students to one pool and the
platform tracks nothing below it).

Where the provider won't tell us, the per-student number must be *derived* —
our own start/stop timestamps × the rate card — and it will drift from the
invoice.

**Requirement:** every ledger line carries its provenance, and reports state
which they are:

- `reserved` — debited at admission, worst case (`specs/03` §2)
- `estimated` — derived from our own observation × rate card; the provider
  cannot confirm it
- `reconciled` — confirmed against a provider-reported figure

A report that silently mixes derived and confirmed numbers is not usable for a
grant budget or an F&A conversation, which is the whole point of producing it.
Drift between `estimated` and `reconciled` at the course level is itself a
reportable quantity, and a growing gap is a signal that a rate card is stale.

**This is also the differentiator worth naming.** Nobody in the education
compute space exposes per-`{course, assignment, student}` attribution —
Codespaces for Education, Colab, Deepnote, and Posit Cloud all use opaque
per-user quotas against a shared pool with no instructor-visible ledger. The
recurring "compute suddenly unavailable, one week left in the semester" failure
is a direct consequence. Attribution is the product, not a reporting feature
bolted onto it.

**The Codespaces defect, stated precisely, because it is the motivating case.**
GitHub Codespaces for Education offers an *org-level* spending limit only. Admins
can cap the number of codespaces per person, restrict machine types, and set
maximum idle timeout and retention — but there is **no per-student monetary
budget**, and hitting the org limit stops *the whole class at once*. One student's
runaway container ends everyone's semester. Cite this by name in grant narratives
rather than describing it abstractly.

**Reuse rather than rebuild the reporting half** (revised 2026-08-08).
**Open XDMoD** (LGPL-3, University at Buffalo) already ingests Slurm/PBS/LSF/SGE
accounting plus Cloud and Storage realms, and is the engine behind ACCESS XDMoD —
so campus research-computing staff already know how to read its output, and its
schema is the closest existing thing to a research-computing FOCUS. Before building
bespoke chargeback reporting, evaluate emitting into Open XDMoD's realms instead.
The per-`{course, assignment, student}` attribution layer is the genuinely new
part; the warehouse under it is not.

## 5. Line-item schema: adopt FOCUS

Use the **FinOps Open Cost and Usage Specification (FOCUS)** (FinOps Foundation,
CC-BY) as the ledger's line-item schema rather than inventing one. It already
defines the vendor-neutral column set for billing records — the exact problem
§2's normalized unit solves internally — and adopting it means exported reports
are ingestible by existing FinOps tooling without a bespoke parser, and legible
to an institutional finance office that has seen the format before.

OpenCost (CNCF, Apache-2.0) is the reference implementation of the
resource-seconds × price → allocation-by-label model, and is worth reading for
the allocation logic even though it is Kubernetes-scoped and
reporting-only.

## 6. Open

- Persistent ledger DB vs. emitting events to a shared EduCloud store
  (DESIGN §8).
- Rate-card format + who maintains it (per-institution override likely). Note
  that a stale rate card degrades `estimated` lines silently — §4's drift metric
  is the intended detector, and the rate card needs an explicit
  last-verified date the way model pins do.
