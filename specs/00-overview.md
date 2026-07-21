# 00 — Overview

## Problem
EduCloud courses need compute — to grade submissions in sandboxes and to give
students working environments. That compute lives in different places for
different programs and different terms: GitHub Codespaces, a campus HPC cluster,
NSF ACCESS (Jetstream2), Pegasus workflow sites, or just Docker locally. Today,
moving between them means rewriting how a course runs, and there is no consistent
way to cap spend or report cost. Outfitter removes that coupling.

## What Outfitter is
A **broker/control plane** that accepts a *placement request* (workload + budget +
lifetime), provisions it through a **provider adapter**, enforces budget/lifetime
guards, and records **normalized cost** attributed to course/assignment/student.

## Actors
- **Requester** — Cairn today (grading sandboxes, student environments); an
  instructor CLI/UI later.
- **Broker** — selects an eligible provider, provisions, returns a handle.
- **Reaper** — enforces lifetime/idle/budget out of band.
- **Ledger** — records usage, computes cost, exports reports.

## Boundaries (see DESIGN §2)
Not a scheduler/cloud · not model routing (Portage) · not identity (Waypoint) ·
not a student-record store.

## The MVP cut
`docker` adapter proves the contract → `codespaces` is the first real provider →
guards + cost → Cairn integration. Research providers (HPC/Jetstream2/Pegasus) come
after the classroom path works. Firmed up in `05-mvp-scope.md`.
