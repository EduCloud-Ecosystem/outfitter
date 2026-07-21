# 02 — Cost model & reporting

## The problem
Providers price in different units: Codespaces core-hours, HPC service units,
Jetstream2/ACCESS credits, a flat campus rate, or $0 for owned hardware. A course
needs one number it can reason about and report.

## Approach
- **Normalized unit.** Define an internal `ComputeUnit`; each provider has a **rate
  card** mapping its native usage → `ComputeUnit` → currency.
- **Attribution.** Every placement carries opaque `course/assignment/student`
  labels; usage rolls up along those dimensions.
- **Ledger.** Append-only usage events (`handle, provider, sampled usage, ts`) →
  cost computed from the rate card. Privacy-minimal: IDs are opaque, no records.
- **Reports.** Export per-course / per-assignment / per-term cost, suitable for
  grant budgets and F&A conversations, as CSV/JSON.

## Reuse
Seed the estimator from `InfoSciCourses/compute-cost-simulator` (its cost model and
curriculum/dashboard work) rather than rebuilding. `Estimate()` in the adapter
interface (`specs/01`) is the live counterpart of that simulator.

## Open
Persistent ledger DB vs. emitting events to a shared EduCloud store (DESIGN §8).
Rate-card format + who maintains it (per-institution override likely).
