# 04 — Cairn integration

Cairn (classroom control plane) is Outfitter's first consumer.

## Two request shapes
1. **Grading sandbox** — short-lived, resource-capped, one per submission/run.
   Cairn asks Outfitter to provision, runs the grader, reads the result, and the
   placement is torn down (by Cairn or by lifetime).
2. **Student environment** — longer-lived dev environment (e.g. a Codespace) a
   student uses across a session; idle-reaped and budget-capped.

## Interface
Outfitter exposes a small broker API (in-process library first, then a service Cairn
calls). Cairn passes a `Workload` + `Attribution` (its course/assignment/student
IDs, opaque to Outfitter) + budget + lifetime; gets back a `Placement` handle and an
endpoint; polls `Status`; and can `Teardown`.

## Division of labor
- **Cairn** owns roster, assignment logic, the grader, and what "done" means.
- **Outfitter** owns *where it runs*, *that it stays within budget*, and *what it
  cost*. Cairn never talks to a provider directly — that coupling is exactly what
  Outfitter removes.

## Alignment
Mirror Cairn's adapter idioms so the two feel like one system. Identity/token from
Waypoint/Keycloak flows through; Outfitter consumes, never mints.
