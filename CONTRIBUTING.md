# Contributing to Outfitter

Outfitter is the **compute-provisioning and brokering layer** for EduCloud. This
repository is spec-first: the design lives in `DESIGN.md` and `specs/` before code
lands. Read those first.

## Ground rules

1. **Sign your commits (DCO).** Use `git commit -s`. See `DCO`.
2. **Spec before code.** A change to provisioning behavior, the adapter interface,
   or the cost model starts as a spec edit under `specs/`, then implementation.
3. **Providers are plugins.** No provider-specific assumptions leak into the core
   broker. Everything a provider needs lives behind the adapter interface
   (`specs/01`).
4. **Privacy-minimal.** Store usage and cost metadata only. Never student records.
5. **Budget-safe by default.** No code path provisions compute without a budget
   ceiling and a teardown path (`specs/03`).

## Workflow

- Branch from `main`, open a PR, keep it scoped.
- Match the ecosystem: Apache-2.0, DCO sign-off, Go toolchain (see `DESIGN.md §Stack`).
- Tests accompany behavior. Adapters ship with a conformance test against the
  interface contract.

## Getting oriented

`README.md` → `DESIGN.md` → `specs/00-overview.md` → `KICKOFF-PROMPT.md`.
