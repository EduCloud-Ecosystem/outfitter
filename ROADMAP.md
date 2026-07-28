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
