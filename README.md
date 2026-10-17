# Outfitter

> Working name — the compute-provisioning and brokering layer of EduCloud.
> (Repo/module name may change at the publish step; set in one atomic commit.)

**Outfitter provisions and brokers the compute that EduCloud courses run on.**
It gives the classroom layer (Cairn) and instructors one API to place workloads —
sandboxed grading jobs, student dev environments, batch runs — onto whatever
compute a program actually has: GitHub Codespaces, campus HPC, NSF ACCESS
(Jetstream2), Pegasus workflows, or plain Docker for local dev. It tracks what was
used and what it cost, and it will not spend past a budget you set.

> **Status: pre-alpha, local research proof of concept.** The reviewed-local
> Docker broker now executes pinned recipes with durable finite reservations and
> verified owned cleanup. It requires an explicit supervised-watchdog exception;
> stronger isolation, cloud provider acceptance and institutional readiness remain
> separate gates. Start with [the runnable contract](specs/09-reviewed-local-research-broker.md)
> and [operator runbook](docs/LOCAL-RESEARCH-BROKER.md).
> Jetstream2 has a [read-only preflight and gated API seam](docs/JETSTREAM2-PREFLIGHT.md);
> live worker provisioning remains disabled pending native-unit accounting and
> independent-supervisor integration.

## Why

EduCloud replaces GitHub Classroom without locking educators to one vendor. The
Git host is already a plugin in Cairn. Compute is the same problem one layer down:
a course might grade in Codespaces this term, on a Slurm cluster the next, and on
Jetstream2 when a grant pays for it. Instructors should not rewrite their course to
move, and no one should get a surprise bill.

Outfitter targets that gap:

- **Provider-agnostic.** Codespaces, HPC (SSH/Slurm), Jetstream2 (OpenStack),
  Pegasus, and local Docker sit behind one adapter interface. The compute backend
  is a plugin, not a foundation — the same design that makes Cairn host-agnostic.
- **Budget-safe by default.** Every provisioning request carries a ceiling and a
  teardown path. Idle environments get reclaimed. No path spends silently.
- **Honest cost reporting.** One normalized cost model across very different
  providers, attributed per course / assignment / student, exportable for grant
  reporting and F&A conversations.
- **Privacy-minimal.** Outfitter stores usage and cost metadata, never student
  records — the same stance as the rest of EduCloud.
- **AI-managed allocation, staged.** A planner with system-level knowledge
  (ledger, telemetry, budgets, course calendar) chooses and swaps providers
  inside the budget envelope: deterministic rules first, an LLM stage after
  the classroom pilot, each stage promoted only by shadow scoring
  (`specs/06`).

## Where it sits

```
      Cairn (classroom control plane, Go)
        │  "give me a graded sandbox / a student environment"
        ▼
   ┌─────────── Outfitter (this repo) ───────────┐
   │  broker API → policy (budget, quota, idle)  │
   │  cost model + attribution + reporting       │
   │  provider adapters ↓                        │
   └── Codespaces · HPC · Jetstream2 · Pegasus · Docker ──┘
```

Identity/federation come from Waypoint (Keycloak). Model routing is Portage's job,
not Outfitter's — Outfitter provisions machines, not model calls.

## Read in this order

| | |
|---|---|
| `DESIGN.md` | Architecture, the broker/adapter model, the stack decision, non-goals. Start here. |
| `specs/00-overview.md` | The problem, the actors, and the MVP cut. |
| `specs/01`–`06` | Adapter interface, cost model, budget/quota guards, Cairn integration, MVP scope, and the allocation planner (AI-managed compute in three gated stages). |
| `KICKOFF-PROMPT.md` | The prompt to open a Claude Code build session. |
| `HANDOFF.md` | Build phases, acceptance criteria, standing rules. |

## License

Apache-2.0 (code), DCO sign-off required. See `LICENSING.md`.
