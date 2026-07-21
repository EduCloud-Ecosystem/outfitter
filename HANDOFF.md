# HANDOFF — Outfitter build

## What exists now (M0)
Spec-first scaffold only: `DESIGN.md`, `specs/00–05`, governance (Apache-2.0 + DCO),
Go stack decision, remote `github.com/EduCloud-Ecosystem/outfitter`. **No code yet.**

## Buy-vs-build resolution
- **Provisioning primitives: buy/reuse.** Talk to providers through their own APIs
  (Codespaces REST, Slurm, OpenStack, Pegasus). Do not reimplement schedulers.
- **The broker, policy, cost model: build.** That normalization across providers is
  the thing that doesn't exist and is EduCloud-specific.
- **Cost estimator: reuse.** Seed from `InfoSciCourses/compute-cost-simulator`.

## Phases & acceptance
- **M1 skeleton** — adapter interface + conformance harness + `docker` adapter +
  broker lifecycle. *Accept:* `outfitter` runs a Docker workload through
  Estimate→Provision→Status→Teardown; conformance green.
- **M2 provider + guards** — `codespaces` adapter; budget ceiling + lifetime +
  reaper; cost ledger + one rate card. *Accept:* a placement with a $ ceiling
  provisions a Codespace, is reaped on lifetime/idle, and shows attributed cost.
- **M3 Cairn integration** — broker API for grading sandboxes; per-course/assignment
  attribution + export. *Accept:* Cairn can request and tear down a graded sandbox
  via Outfitter; cost report exports.
- **M4 research compute** — `hpc`, `jetstream2`, `pegasus` adapters; policy
  selection across providers.

## Standing rules
Spec-first · budget-safe by default · privacy-minimal · conformance per adapter ·
DCO sign-off · keep the core provider-agnostic.

## Open decisions (resolve at M1 kickoff)
1. First consumer: grading sandboxes vs. student dev environments? (`specs/05`)
2. Persistent ledger DB vs. emit events to a shared EduCloud store?
3. How much of `compute-cost-simulator` transfers directly?
