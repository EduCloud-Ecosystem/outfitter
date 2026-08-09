# EduCloud ecosystem — shared conventions

This repo (`outfitter`) is the compute broker of the EduCloud ecosystem, an
open-source GitHub Classroom replacement. Siblings: `cairn` (classroom/LMS),
`belay` (AI tutor), `portage` (LLM routing engine), `waypoint`
(hosting/identity), `educloud` (umbrella strategy/decisions/SYSTEM.md).
`portage-local` is a separate personal deployment, currently on hold — not
part of this ecosystem's active work.

## The platform invariant

No component trusts a model's — or a provider's — self-report. A
deterministic check decides success, always. Before citing a vendor's claim
(pricing, precision, model catalog, uptime, API behavior, allocation terms)
as fact in a doc or a decision, verify it against the live API/service or the
program's own documentation, not a summary of it. This matters here
specifically for ACCESS/Jetstream2 allocation terms and ally-cloud pricing —
get those from the program's current docs, not memory.

## The CC-* prompt namespace — check before you number

Claude Code prompts across all six repos share one numbering namespace:
`CC-P*` (Portage), `CC-CA*` (Cairn), `CC-B*` (Belay), `CC-O*` (Outfitter —
this repo), `CC-W*` (Waypoint), `CC-HB*`/`CC-C*` (historical, closed
series). **A number means one thing platform-wide — collisions have
happened twice already** (CC-P9/P10 reused by accident, CC-C1 reused by
accident). Before creating a new prompt:

1. Read `../educloud/DOCUMENTATION.md`'s "Prompt-number namespace" section
   for the next free number in your series (find the actual path — it's a
   sibling directory, don't assume the relative path is exactly `../educloud`
   without checking).
2. Also check this repo's own `docs/prompts/` directory directly — the
   registry can drift; the directory listing is ground truth.
3. Use one higher than the max of both, then update the registry.

The `/new-cc-prompt` custom command in this repo does steps 1-3
automatically — prefer it over doing this by hand.

Prompt files live in `docs/prompts/CC-<series><n>-<slug>.md` and follow this
shape: a real title (not a placeholder); an italicized context block
(`*Claude Code prompt. Authored [where/when], from [what triggered this —
a finding, an instruction, a prior prompt's report]`) stating plainly what's
already known/verified and what this prompt needs to resolve; numbered
sections (what to read first, what to do, in dependency order); a closing
numbered **Report** section listing exactly what the executor must report
back — including anything that diverged from the happy path or couldn't be
verified. Never omit the Report section.

## Git conventions

- No `Co-Authored-By: Claude` trailers in this repo's commits. If unsure,
  `git log -5 | grep -c Co-Authored-By` should return `0`.
- Prefer new commits over amends; never force-push without being asked.

## Where things are

- `../educloud/SYSTEM.md` — the eleven shared platform contracts every
  module implements against
- `../educloud/DOCUMENTATION.md` — the full doc index and the CC-* namespace
  tracker
- `DESIGN.md` — the broker/adapter model, Go stack rationale, and §9's
  allocation intelligence
- `ROADMAP.md` — M0–M5, plus the M1.5 and 2026-08-08 build-vs-integrate
  triage and ACCESS path; this is also where the Explore-vs-Discover ACCESS
  sequencing note lives (kept in sync with `educloud/DOCUMENTATION.md`'s
  known-gaps list — don't let the two drift apart again)
- `specs/00–06` — adapter interface, cost model, budget guards, Cairn
  integration, MVP scope, allocation planner
- `specs/08` — isolation tiers for untrusted student code. This one is
  **platform-level**, not broker-only: Cairn and Belay both consume it.
  `specs/07` is reserved (endpoint manifest, SYSTEM.md contract 9) — don't
  claim that number for anything else
- `docs/prompts/CC-O1` — prompt executed so far
