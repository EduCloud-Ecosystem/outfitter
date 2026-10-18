# 16 — One finite offline experiment in native admission

This increment binds the accepted metadata evaluator to the actual `CreateWorker`
path, using only the existing loopback-only rescue creator, simulated port/server
APIs, two durable B acknowledgements and B-only cleanup. Guest execution is
`NOT_RUN`. Live readiness remains `BLOCKED`; no live constructor, provisioning
command, credential lookup, deployment switch, guest transport or bootstrap is
added. This does not approve live testing or alter Ben's assignments.

## Immutable envelope and bounded evidence

An optional experiment is stored separately from the native account's policy.
Its versioned, canonical envelope binds the existing source authority and owner,
account, full configuration/native-policy digests, compatibility profile/snapshot
digests, exact worker request (identity, owner, lifetime and native amount), a
preselected native reservation ID, start/expiry, and exactly one total attempt.
Compatibility already hashes native policy; the experiment is not added to that
policy and therefore introduces no circular digest.

Embed only a bounded synthetic profile (2 KiB) and snapshot (4 KiB), each at most
eight rules. The complete envelope is at most 8 KiB. Use existing strict JSON,
normalization and missing/null semantics; retain canonical bytes and SHA-256.
The generic ledger rejects opaque envelope bytes that would change under outer
JSON serialization (including insignificant whitespace and unescaped HTML text),
before any write can invalidate the retained digest.
Existing native policy and native worker state limits remain 16 KiB each. The
worker state carries only the experiment digest and closed live/guest labels,
not a second copy of compatibility evidence. Overflow fails closed before any
corresponding effect. Envelope metadata contains no credentials or guest payload.

Configuration requires an already configured rescue journal, the exact active
source authority and account, no prior native reservation in that account, valid
bounded inputs and binding consistency. Time or metadata mismatch is not a grant;
it will refuse admission. An identical configuration is idempotent; replacing,
removing or replenishing it is forbidden. A journal opened before configuration
must still enforce the stored envelope on subsequent admission.

## One serialized attempt

Use the existing ledger lock, atomic file replacement and fsync transaction.
The account's consumed marker and native reservation are committed together.
The envelope, preselected reservation ID and request binding are immutable. Each
consumed marker must retain its exact reservation; every experiment reservation
must point back to that marker and digest. Store updates cannot remove or reset
an existing experiment, including after settlement. Native accounting still
retains unknown charges; consuming an attempt never supplies a charge or refund.

Admission checks active source authority, exact request/configuration/policy and
all compatibility bindings, `start <= now < expiry`, request lifetime and amount,
reservation identity and `deadline <= expiry`. The existing policy expiry,
budget, unresolved-request, request-collision and overrun checks remain. Evaluate
the exact stored compatibility inputs inside serialized admission. Any UNKNOWN,
MISMATCH, malformed, stale, future, before-start or expired input refuses the
reservation without consuming the attempt or creating a provider resource.

A repeated exact request is lookup-only before provider preflight; it cannot
resume a partial creation. Changed payloads/IDs cannot obtain a second attempt.
Restart and settlement do not replenish the attempt. Existing execution leases,
held restore, source fencing, checkpoint integrity and path binding remain in
force. This is local serialization, not global prevention of hostile administrator
rollback or identical-path copied hosts.

## Checks at the actual effect boundary

Before each port/server POST, reload the retained envelope and reservation,
validate their identity and active authority, and reevaluate exact bound
compatibility and time/lifetime constraints. Run this check after delayed rescue
ACKs and immediately after the final pre-POST fault boundary. The request context
also retains the durable worker deadline. An expired/unknown/mismatched check
stops remaining effects; it does not erase consumed attempts, intent, ACKs, known
resources or unresolved liabilities. The second POST still requires the saved
port ID and its own durable B ACK. No retry, resume or automatic new identity is
introduced.

Only the current injected loopback rescue client may dispatch an experiment.
Ordinary constructors and journals without the required experiment enforcement
cannot dispatch a record carrying its digest. B remains independent of A's
ledger and is the sole cleanup executor. It can remove eligible lost/late effects
from retained signed scope while A is held, corrupt or unavailable. Absence is
observational only; no terminal absence or refunds are asserted. The 64-sweep
retention limit and all earlier rescue limitations remain.

## Required evidence

Exercise the actual creator and simulated HTTP endpoints: exactly one port and
server on a matching experiment; zero-effect refusals; expiry/staleness during
ACK; every identity/digest/time/lifetime binding; concurrent processes, restart,
settlement and changed IDs unable to gain another attempt. Kill creator processes
at reservation, scope/ACK, POST and response boundaries and retain liabilities.
Verify held/restore/checkpoint-tamper fences preserve the consumed attempt and
that B cleans eligible lost/late effects without A accounting access.

Retain earlier failed controls and 607/231 compatibility/220 rescue evidence.
Run existing race, Docker, recovery and consumer checks, independently review and
fix findings, then require exact-head CI before merge. Report local/simulated
results separately from live qualification and keep guest execution NOT RUN.
