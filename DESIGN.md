# Design Doc — Outfitter (EduCloud Compute Provisioning & Brokering)

*Working draft · spec-first · started 2026-07-21*

## 1. Purpose

Outfitter is the compute plane of EduCloud. It answers one request in many
environments: *"run this workload, under this budget, and tell me what it cost."*
Its consumers are Cairn (for sandboxed grading and student environments) and,
later, instructors and other EduCloud services directly.

It is a **broker/control plane with provider adapters**, deliberately shaped like
Cairn's Git-host adapter layer so the two compose and so contributors move between
them without relearning the model.

## 2. Non-goals

- **Not a scheduler or a cloud.** Outfitter places workloads onto existing compute
  (Codespaces, a Slurm cluster, an OpenStack cloud, a Pegasus site). It does not
  build a new cluster or reimplement Slurm/Kubernetes.
- **Not model routing.** Portage handles LLM/inference routing. Outfitter provisions
  machines and environments, not model calls.
- **Not identity.** Waypoint/Keycloak owns identity and federation. Outfitter
  consumes tokens; it does not mint them.
- **Not a data store for student records.** Usage and cost metadata only.

## 3. Actors & core flows

- **Requester** (Cairn, later an instructor UI/CLI) asks for a *placement*: a
  workload spec + a budget + a lifetime.
- **Broker** selects an eligible provider (policy + availability + cost estimate),
  provisions through that provider's adapter, and returns a handle.
- **Reaper** enforces lifetime/idle/teardown and budget ceilings out of band.
- **Ledger** records usage events and computes normalized cost for reporting.
- **Planner** (staged; `specs/06`) decides placements, swaps, and
  pre-provisioning from system-level knowledge (ledger, telemetry, budgets,
  course calendar). It acts only through the broker API, inside the budget
  and policy envelope, with every decision logged.

Core lifecycle: `Estimate → Provision → Status(poll) → Teardown`, with `Cost`
observable throughout and a hard `Reap` path that can teardown from outside the
request.

## 4. Provider adapters (the plugin seam)

A provider is anything that can hand back an addressable, teardownable unit of
compute. First-class targets (see `specs/01`):

| Adapter | Backs | Notes |
|---|---|---|
| `docker` | local dev, CI | reference adapter; the conformance baseline |
| `codespaces` | GitHub Codespaces | REST API; natural pairing with Cairn's GitHub path |
| `hpc` | campus clusters | SSH + Slurm (`sbatch`/`squeue`/`scancel`); batch grading |
| `jetstream2` | NSF ACCESS / Jetstream2 | OpenStack API; grant-funded burst capacity |
| `pegasus` | Pegasus WMS | reproducible scientific workflows |

Every adapter implements the same interface and ships a **conformance test** run
against a contract, so "does provider X behave?" is a test, not a guess.

## 5. Cost model & reporting

Providers price in wildly different units (Codespaces core-hours, HPC service
units, Jetstream2 ACCESS credits, a flat campus rate, $0 for owned hardware).
Outfitter normalizes these to a common internal unit with a per-provider rate card,
attributes usage down to course / assignment / student, and exports reports usable
for grant budgets and F&A discussions. The estimator seeds from the existing
`InfoSciCourses/compute-cost-simulator` cost model — that work is folded in here as
the estimation input rather than rebuilt (see `specs/02`).

## 6. Budget & quota guards

No provisioning path runs without (a) a budget ceiling, (b) a lifetime, and (c) a
teardown path. Guards are enforced by the broker on the way in and by the reaper
out of band, so a crashed requester still can't leak spend. Details in `specs/03`.

## 7. Stack decision — Go

**Chosen: Go.** Rationale:

- **Architectural symmetry with Cairn.** Cairn is a Go control plane with a
  host-adapter interface; Outfitter is a control plane with a provider-adapter
  interface. Same shape, same idioms, shared contributor muscle memory, and Cairn
  can call Outfitter natively.
- **Operational fit.** Single static binary, trivial to deploy next to Cairn/Waypoint
  (Coolify), good concurrency for polling many long-running provisioning jobs and
  running the reaper as background goroutines.
- **The Python-first providers are still reachable.** Jetstream2 (OpenStack) and
  Pegasus have Python-first SDKs, but both expose REST/CLI surfaces. Adapters call
  those APIs directly or shell out to the provider CLI; we do not need the whole
  runtime to be Python to talk to them.

Portage and Belay are Python because their work is ML/eval-shaped. Outfitter's work
is control-plane-shaped, so it follows Cairn. If a provider proves to *require*
heavy Python, that stays quarantined inside that one adapter (subprocess/service),
not the core.

> This is a recommendation recorded for the build; it is revisitable in `specs/01`
> if the first two adapters argue otherwise.

## 8. Open questions

- Is the first real consumer Cairn's grading sandboxes, or student dev environments?
  (Shapes the MVP adapter order — see `specs/05`.)
- Does Outfitter own a persistent ledger DB, or emit events to a shared EduCloud
  store? (Privacy-minimal either way.)
- How much of `compute-cost-simulator` transfers directly vs. informs a rewrite?

## 9. Allocation intelligence

Resource management is an AI role in this platform, by design. The planner
(`specs/06`) holds system-level knowledge and swaps resources automatically:
provider choice from live telemetry, workload migration when conditions
change, pre-provisioning ahead of course deadlines, allocation pacing across
a term. It runs in three gated stages (static policy, telemetry policy, LLM
planner), each promoted only after beating the previous stage in shadow on
cost per verified run and deadline adherence. The envelope stays
deterministic and outside the planner's reach: budget ceilings, lifetimes,
teardown, policy modes, an append-only decision log, human confirmation above
thresholds, and a kill switch back to the previous stage.
