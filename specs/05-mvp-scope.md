# 05 — MVP scope

## The one decision that sets the MVP
**Which consumer first: grading sandboxes or student dev environments?**
- *Grading sandboxes* → optimize short-lived, high-churn, `docker`-then-HPC path.
- *Student environments* → optimize longer-lived, idle-reaped, `codespaces` path.

Recommended default: **grading sandboxes**, because it's the tightest loop, exercises
provision/teardown churn hardest, and is Cairn's most-used path. (Confirm with Greg;
this is the open question in DESIGN §8.)

## In scope for MVP (M1–M3)
- `docker` reference adapter + conformance harness.
- `codespaces` adapter (first real provider).
- Broker lifecycle: Estimate → Provision → Status → Teardown.
- Budget ceiling + lifetime + idle reaper.
- Normalized cost + one rate card + per-course/assignment attribution + export.
- In-process broker API Cairn can call.

## Out of scope for MVP (defer to M4+)
- HPC (Slurm), Jetstream2 (OpenStack), Pegasus adapters.
- Multi-provider policy/optimization (pick-cheapest-eligible).
- Standalone service + authn surface (start in-process with Cairn).
- Instructor-facing UI.

## Definition of MVP done
Cairn requests a graded sandbox through Outfitter; it provisions on Docker (and, on a
configured course, Codespaces), runs within a budget ceiling, is reaped on
lifetime/idle, and produces an attributed, exportable cost line. Conformance green;
commits DCO-signed.
