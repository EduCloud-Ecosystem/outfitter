# Kickoff prompt for Claude Code — Outfitter session 1

Paste this into a fresh Claude Code session opened in `~/dev/outfitter`.

---

You are building **Outfitter**, the compute-provisioning and brokering layer of
EduCloud. It is a Go control plane with pluggable provider adapters — the same
shape as Cairn's Git-host adapters. It provisions compute (Codespaces, HPC,
Jetstream2, Pegasus, Docker), enforces budgets, and reports normalized cost.

**Before writing code:** read `DESIGN.md`, then `specs/00-overview.md` through
`specs/05-mvp-scope.md`. Honor `CLAUDE.md`'s standing rules (spec-first,
budget-safe by default, privacy-minimal, adapters ship conformance tests, DCO
sign-off).

**Session 1 goal — M1 skeleton + the contract (see `ROADMAP.md`):**
1. Initialize the Go module and the layout in `CLAUDE.md`.
2. Implement the provider adapter interface from `specs/01` and a conformance test
   harness that any adapter can be run against.
3. Implement the `docker` reference adapter and make it pass conformance.
4. Stand up the broker skeleton: `Estimate → Provision → Status → Teardown`, with a
   no-op budget guard wired in at the entry point (real guards are M2).
5. A `cmd/outfitter` entrypoint that can provision, show status, and teardown one
   local Docker workload end to end.

Do **not** start real cloud providers (Codespaces/HPC/Jetstream2/Pegasus) in
session 1 — the `docker` adapter proves the contract first. Stop and summarize when
`outfitter` runs a Docker workload through the full lifecycle with conformance
green. Commit with `-s`.

Scope later sessions one milestone at a time from `ROADMAP.md`.
