# 21 — Disabled assessment owned-data lifecycle

This source/test increment starts from Outfitter merge
`de65bdab9303395226ebfb35e0e0740ab19b4d8e` and implements the Outfitter side of
EduCloud A1 through the existing journal and Watch/Reap paths. The reviewed
protocol digest is `bc6d083b31fa03abe9bda3461c83092602e2cfee8fa483ac4d1341046dda11f6`.
The canonical research-assessment contract, backlog and accepted A0/A2/A4/A5
evidence remain authoritative. This increment cannot qualify a host or admit
production content. Ben's assignments and every earlier failure remain intact.

## Production boundary

The fixed command `owned-data OPERATION` identifies prepare/ingest/seal before
reading stdin, opening state, or constructing a provider. Those content
operations always return `ASSESSMENT_UPLOAD_DISABLED`. There is no public
enabling argument, environment variable, configuration, header, boundary factory
or v6 initializer. The permitted bounded control parser requires the JSON
operation to match argv and rejects unknown/duplicate fields and non-integer
numbers. Production status/cancel/reconcile can expose or conservatively deny
known scopes; they cannot establish a storage boundary or trust an observation.

Positive tests inject a private synthetic filesystem/clock boundary directly in
the broker package. The constructor and fixture seeding live only in `_test.go`.
Separate Go test-helper processes call the same real lifecycle and Watch/Reap
functions; there is no persistent alternate test server or copied lifecycle.
Ordinary output has `test_permission: none`. Only a test-built peer with current
lifelines can expose `synthetic_ingest_only`; all upload/execution/readiness and
cleanup-trust flags remain false.

## Existing authority, finite immutable records

Fresh test-only `Data.Version = 6` adds `owned_data_scopes` in `ledger.json`.
It uses the existing lock, atomic serializer, fsync and metadata bounds. It never
upgrades a v1–v5 authority. Older binaries reject v6. No new scheduler, budget
ledger, compute reservation, attempt or provider record is created by an upload.
The test-only constructor durably publishes the existing `initialized`
tombstone before its first journal. A lost journal cannot turn the same state
directory into a fresh ordinary authority; open/create and recovery controls
must preserve the remaining objects and refuse replacement.
All Projects, Runs, NativeAccounts and NativeRuns remain semantically byte-exact
across every v6 update. Native/compute/consumer/project mutation and supported
checkpoint/restore/activation/export paths refuse v6 before effects.

The scope is a server-generated 32-lowercase-hex upload-session identity, distinct
from the manifest's original assessment ID. It immutably binds that original
assessment ID, unchanged manifest digest/expiry, known project, paired Waypoint
authority, boundary, admitting-owner epoch, writer epoch and shared-clock epoch.
Never rewrite a manifest to satisfy the new scope grammar. No caller path,
filename, source hash, token, command, content or raw diagnostic is retained.

The new envelope declares a positive upload ceiling, no larger than manifest
scratch and the stricter injected policy. It is not observed input size, a
physical disk quota or an allowance. Selected scope/control expiry is canonical
whole-second UTC, byte-identical in both, and no later than manifest expiry,
project end, initiating authorization expiry or injected policy. Select the
minimum instant, round down, and refuse if no future lifetime remains. Manifest
v1 bytes and whole-second format remain unchanged. Broker-created times use
canonical UTC with six fractional digits. Compare timestamps as instants.

At first prepare capture the injected wall/tick/epoch and persist the fixed
shared monotonic deadline once. Every command checks both absolute and tick
expiry. Wall rollback, tick rollback/unknown epoch, or replacement deny; neither
command restart nor delayed watcher startup can rebase the deadline. The actual
suspend-inclusive host clock is not implemented or qualified here.

Scope states are preparing, accepting, sealed, cleaning and cleanup_unresolved.
Deny, writer-started, watch-started, upload-complete and observation-saturation
latches never reset. No scope or tombstone is removed or ID reused. Observations
are append-only, at most eight, and always synthetic with false trust. Even
observed removal ends in cleanup_unresolved, never closed or verified.

Aggregate runs/native/scopes remains at most 512. Each record remains within
96 KiB and the whole journal within 64 MiB. Before admission, reserve serialized
space for the largest denial/observation form. Metadata saturation cannot remove
cleanup eligibility; an unrecordable observation preserves unknown status and
prior evidence without claiming success.

## Lifelines, one writer and cleanup

The Waypoint admitting process owns the sole noninherited owner pipe writer.
The existing per-scope watch child owns the sole watch pipe writer. Prepare
temporarily forwards owner-read/watch-write, then closes its copies. A bounded
readiness pipe binds scope, authorities, owner/watch epochs, generation, expiry
and original clock deadline after the watcher acquires its lease. Stale ready
files and PIDs cannot supply readiness. Ordinary prepare/status/ingest/seal exit
is not owner restart; a lost owner/watch pipe is.

Ingest holds the existing shared execution lease and a stable scope writer lease
before irrevocably claiming writer_started. Validate current journal, bindings,
both read lifelines and original clock deadline before claim, before opening
content and between bounded I/O. Lost/ambiguous claim acknowledgements cannot be
retried. Read exactly declared content_length and EOF, sync and close content,
then durably publish upload_complete while retaining the writer lease. Seal is
a separate command requiring current lifelines and exclusive writer drain.
A free lease or dead writer without upload_complete cannot seal.

Cancel/expiry/restart first latch denial under a short journal transaction. Never
hold the journal lock across stream I/O, pipe waits, drain or removal. Cleanup
takes the same stable writer lease exclusively without blocking, verifies owned
filesystem identities, and removes only the exact owned scope. Persisted private
filesystem identity bindings, if needed to identify root/scope/lease replacement,
are bounded internal metadata excluded from control/status. Lease tombstones
remain after content removal. Missing/replaced/foreign/symlink/special files or
uncertain ownership mean refusal and unresolved cleanup, never broad recursion.

Blocking I/O can retain a writer past denial. The watcher records unresolved
drain rather than claiming unlink or a PID observation revoked access. Synthetic
local pipes/locks only cover cooperating processes; privileged descriptor
forgery, arbitrary copied/restored authority, kernel-stuck I/O, host loss,
independent failure domains and production encryption remain unqualified.

## Verification and handoff

Freeze closed canonical control/status/observation fixtures for later Waypoint
integration. Exercise real functions in separate test-helper/watch processes:
ownership before bytes; loss between ACK/claim; writer races; exact-length
short/overlong/interrupted streams; cancellation and lease drain; immutable
expiry, fractional caps, rollback and restart; uncertain journal/I/O; foreign
and replaced roots/scopes/leases; observation saturation and cleanup at limits.
Use two known projects and meaningful prior unresolved/charged accounting
controls. Assert no content markers or source identifiers in retained state,
status, errors, output sidecars or supported recovery/export destinations.

The ordinary executable must refuse content before stdin consumption and cannot
initialize/migrate v6 or enable via imported verification/encryption claims.
Run focused tests as source develops. Coordinate one conservative full local
suite with the root, then bounded existing hosted CI. The root owns immutable
independent review and merge. No live provider, engine, model, real data, new
allowance, capacity purchase, actual counters or assessment benchmark is run.

## Metadata reservation encoding

Reserve the longest valid encoded denial record, including boolean values:
JSON `false` occupies one more byte than `true`. The reservation must cover a
denied scope with a watch epoch and all private object identities, eight
observations, no writer claim or completed upload, and an unset saturation
latch. Independently constructed valid failure records must fit both the
record and aggregate reservation; testing the reservation helper against its
own output does not establish that bound.

## Private close evidence

`content_close_confirmed` and `content_close_uncertain` are private monotone
journal booleans, both initially false. Confirmation is published only after
an actual successful content descriptor close, while retaining the writer
lease through publication and acknowledgement. Uncertainty is sticky and
overrides confirmation. Failed/ambiguous publication remains held. Neither
field is accepted from callers or exposed as a public trust flag.

Any `writer_started` scope without confirmed close remains unknown for drain,
access revocation and removal, including a crash before content identity was
published. Lease freedom and process disappearance cannot clear that hold.
Deferred successful close after interrupted input can record confirmation
without reopening denial. The original failure reason and observations remain.

