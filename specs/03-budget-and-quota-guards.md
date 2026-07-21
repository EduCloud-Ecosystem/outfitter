# 03 — Budget & quota guards

**Invariant: no compute is ever provisioned without a ceiling, a lifetime, and a
teardown path.** Enforced in two places so a crashed requester can't leak spend.

## On the way in (broker)
- Reject a placement with no budget ceiling or no lifetime.
- Pre-check projected cost (`Estimate`) against the remaining course/term budget;
  refuse if it would breach.
- Enforce quotas: max concurrent placements per course/student.

## Out of band (reaper)
- **Lifetime:** teardown at `provisioned_at + lifetime`, always.
- **Idle:** teardown when `Usage` shows idle beyond a threshold.
- **Budget breach:** if accumulating cost crosses the ceiling, teardown and mark the
  placement `Terminated(reason=budget)`.
- The reaper works only from persisted placement records + `Teardown`, so it
  survives requester or broker restarts.

## Failure posture
Fail closed. If cost/usage can't be determined, prefer teardown over open-ended
spend. Every teardown reason is recorded for the ledger and reporting.
