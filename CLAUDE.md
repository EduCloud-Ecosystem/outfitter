# CLAUDE.md — operating instructions for Outfitter

Outfitter is the **compute-provisioning & brokering layer** of EduCloud. Read
`DESIGN.md` and `specs/00-overview.md` before writing code.

## Standing rules
- **Spec before code.** Behavior changes start in `specs/`, then implementation.
- **Go.** Single module, standard layout. Keep the core provider-agnostic; provider
  quirks live inside that provider's adapter only.
- **Budget-safe by default.** No provisioning path without a ceiling, a lifetime,
  and a teardown path. The reaper must be able to teardown out of band.
- **Privacy-minimal.** Usage + cost metadata only. Never student records.
- **Adapters ship with a conformance test** against the interface contract.
- **DCO sign-off** on every commit (`git commit -s`). Apache-2.0.

## Layout (target)
```
cmd/outfitter/         entrypoint (CLI + server)
internal/broker/       placement: estimate → provision → status → teardown
internal/provider/     adapter interface + docker|codespaces|hpc|jetstream2|pegasus
internal/policy/       budget, quota, lifetime, idle-reap
internal/cost/         normalized unit, rate cards, attribution, reports
specs/                 the reasoning record (00–05, extend as you go)
```

## Definition of done for a change
Spec updated · code + tests · adapter conformance green (if provider) · budget guard
covered · `git commit -s`.
