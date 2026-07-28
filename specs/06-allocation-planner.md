# 06 — Allocation planner (AI-managed compute inside the envelope)

The broker executes placements. The planner decides them. This spec adds the
component that gives Outfitter system-level allocation intelligence: the
ability to choose providers, swap workloads between them, and provision ahead
of predicted load, automatically, inside limits it cannot alter.

## 1. Why a planner

Static provider order (specs/01, M2) answers "where can this run." It cannot
answer the questions a real term produces: grading load spikes at assignment
deadlines; a provider's quota runs low mid-week; Jetstream2 has a maintenance
window; the ACCESS allocation should last the semester rather than the month.
Answering those requires knowledge of the whole system at once: the ledger,
provider telemetry, budget state, and the course calendar. That is the
planner's job, and it is a stated goal of the platform that AI holds it.

## 2. What the planner knows

- The ledger: usage and normalized cost by course, assignment, and provider
  (specs/02).
- Provider telemetry: health, queue depth, quota and allocation balance,
  maintenance windows, per-provider rate cards.
- Budget state: remaining ceilings per course and per term (specs/03).
- The course calendar, via Cairn's attribution labels: assignment ids and due
  times. Deadlines predict grading load. No student content and no student
  identity are needed or accepted.
- Policy modes per lane (SYSTEM.md contract 2).

## 3. What the planner can do

Every action is an ordinary broker call, so conformance tests, budget guards,
lifetimes, and teardown apply unchanged. The planner has no second actuator.

- Choose the provider for a placement from the eligible set.
- Swap a workload class between providers when conditions change (quota
  pressure, health, price, maintenance).
- Pre-provision capacity ahead of a predicted deadline spike, with a lifetime
  that ends after the deadline window.
- Retire idle capacity early.
- Recommend, with reasons, actions above its own authority (see §5).

## 4. Three stages, each gated by the last

- **P0 — static policy.** Ordered provider lists. This is M2 as already
  specified. No intelligence.
- **P1 — telemetry policy.** Deterministic rules plus forecasting over the
  ledger and calendar: cheapest eligible healthy provider, predictive
  pre-provisioning before deadlines, allocation pacing across the term. No
  LLM. This stage alone covers most of the operational value.
- **P2 — LLM planner.** An LLM receives structured system state (telemetry
  summaries, budget state, calendar, current placements) and returns an
  allocation plan: placements, swaps, pre-provisions, each with a stated
  reason. A deterministic plan checker validates every item before execution:
  schema, projected cost against ceilings, policy-mode legality, and a
  blast-radius limit on how much may change per cycle. Illegal items are
  rejected item by item; the rest execute. The planner's own inference routes
  through Portage on an operations lane (system telemetry only, never student
  content; sovereign-preferred).

## 5. The envelope (hard, not planner-modifiable)

1. Budget ceilings, lifetimes, and teardown paths (specs/03) bind every
   planner action. The planner cannot raise a ceiling.
2. Policy modes bind placement: a sovereign lane's workloads cannot be placed
   on providers that do not exist in its configuration.
3. Human confirmation is required above thresholds: actions whose projected
   cost exceeds the per-cycle limit, swaps that would interrupt live
   student-facing sessions, and the first use of any newly enabled provider
   class.
4. Every decision, action, and rejection is appended to a decision log with
   the planner's stated reason. The log is append-only and the planner cannot
   write anywhere else.
5. A kill switch reverts to the previous stage (P2 to P1, P1 to P0)
   immediately and without migration work, because every stage acts through
   the same broker API.

## 6. The gate for promotion and for staying promoted

A stage runs in shadow first: it produces plans that are logged but not
executed, scored against what the live stage did. It is promoted only when it
beats the live stage on cost per verified grading run and deadline adherence
without raising the stall or failure rate, on the same non-inferiority
discipline Portage applies to router retraining. The comparison re-runs each
term; a stage that stops earning its place is demoted. Provider outages are
logged as unavailable, never as planner failures.

## 7. Why this claim is safe to make

The worst case of a bad plan is a suboptimal placement: it costs somewhat
more or runs somewhat slower, the ledger records it, and the gate demotes the
planner. A bad plan cannot overspend (ceilings), cannot move student data
(policy modes), cannot hide (decision log), and cannot persist (shadow
scoring and demotion). That is the platform's general position stated for
compute: AI decides within the envelope; deterministic systems define the
envelope and verify the results.

## 8. Sequencing

P0 ships in M2. P1 lands with M4 alongside the research-provider adapters,
when there is more than one real provider to choose between. P2 follows the
classroom pilot, once a term of ledger and calendar data exists to score it
against. Nothing in M1 through M3 blocks on this spec.
