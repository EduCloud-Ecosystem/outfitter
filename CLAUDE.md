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

## EduCloud ecosystem — shared conventions

Outfitter's siblings: `cairn` (classroom/LMS), `belay` (AI tutor), `portage`
(LLM routing engine), `waypoint` (hosting/identity), `educloud` (umbrella
strategy/SYSTEM.md). `portage-local` is a separate personal deployment, on
hold, not part of this ecosystem's active work.

**Platform invariant:** no component trusts a vendor's self-report. Verify
pricing/precision/catalog/uptime/allocation-terms claims against the live
API/service or the program's own current docs before writing them into a
decision — this applies here especially to ACCESS/Jetstream2 allocation
terms and ally-cloud pricing.

**CC-* prompt namespace** (`CC-O*` here, plus `CC-P*`/`CC-CA*`/`CC-B*`/
`CC-W*` in siblings, `CC-HB*`/`CC-C*` historical) is shared platform-wide —
two real collisions have happened (CC-P9/P10, CC-C1). Before numbering a
new prompt: read `../educloud/DOCUMENTATION.md`'s namespace tracker, cross-
check this repo's own `docs/prompts/` directory (ground truth over the
registry), use one higher than the max of both, update the registry. The
`/new-cc-prompt` command does this automatically.

Prompt shape: real title; italicized context line (what's known/verified vs.
what this prompt resolves); numbered sections; closing numbered **Report**
section (never omit) covering anything unverified or diverged.

No `Co-Authored-By: Claude` trailers in commits — separate from the DCO
sign-off requirement above, which stays. Check actual trailers, not prose:
`git log -5 --format='%(trailers:key=Co-Authored-By)'` should print nothing
— a plain `grep -c Co-Authored-By` over the log can false-positive on prose
describing the convention (caught in Cairn by CC-CA5, 2026-08-09). New
commits over amends; no force-push unasked.

Where things are: `../educloud/SYSTEM.md` (shared contracts),
`../educloud/DOCUMENTATION.md` (doc index + CC-* tracker). `specs/08`
(isolation tiers) is platform-level, not broker-only — Cairn and Belay both
consume it; `specs/07` is reserved for the endpoint manifest, don't claim it
for anything else.
