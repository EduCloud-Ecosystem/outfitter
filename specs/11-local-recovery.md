# 11 — Local checkpoints and fenced recovery

This increment coordinates with Waypoint's offline service lock and artifact/SQLite
checkpoint. It does not add cloud backup, remote fencing, budget reset, or live
provider acceptance. Ben's provider milestones and assignments remain unchanged.

## Authority and migration

Ledger version 2 binds an authority instance to its canonical local state path.
Version 1 migrates in place under its existing lock, retaining owner, reservations,
charges, request identities and diagnostic references. Older binaries reject v2.
A copied v2 directory cannot be opened at a different path. This is a local-host
operational guard, not protection from a privileged operator rewriting files,
rolling back the original path, or cloning a machine at the same path.

Admission is `active`, `held` or permanently `fenced`. Held/fenced ledgers allow
status, cancellation, reconciliation and reaping, but no new projects or requests.
Existing requests remain readable through status; run is refused even for a duplicate ID and never replays work.

## Commands (JSON stdout; failure exit 2)

```
outfitter --state-dir SOURCE checkpoint --output NEW_PRIVATE_DIRECTORY
outfitter --state-dir NEW_DEST restore --checkpoint DIRECTORY --checkpoint-sha256 SHA256
outfitter --state-dir DEST recovery-status
outfitter --state-dir DEST activate --source-state-dir SOURCE --checkpoint DIRECTORY --checkpoint-sha256 SHA256
```

Checkpoint takes one ledger transaction, validates authority and referenced output
blobs, and writes a new private directory exclusively. A manifest records the
ledger and output hashes/sizes. Its SHA-256 is the checkpoint identity; retain it
separately in the caller's trusted recovery manifest. Embedded hashes alone do not
establish authenticity. Missing/corrupt diagnostic blobs refuse a complete
checkpoint without blocking normal cleanup. Workload snapshots, watchdog markers
and executable replay artifacts are not copied. Existing destinations are refused.
Partial writes are retained for inspection and cannot be used as a completed
checkpoint or silently overwritten.

Restore verifies the separately supplied digest and every referenced file, then
creates a new authority instance in `held` mode, with the original accounting and
provider owner. No jobs are dispatched or refunded. Input paths remain provenance;
restore does not reinterpret them as permission to replay. Recovery-status lists
projects and unresolved request IDs, statuses, reserved CPU-seconds and deadlines.

All four commands return schema version 1 with `authority_id`, `state_path`,
`admission`, `authority_sha256`, `projects` (project-status objects), `unresolved`
(request_id, project_id, status, reserved_cpu_seconds, deadline), plus
`checkpoint_sha256`, `source_authority_id` and `source_state_path` when applicable.
Fenced state also reports `fence` with `destination_authority_id`,
`destination_state_path` and `checkpoint_sha256`. `authority_sha256` fingerprints
the exact current validated authority, including admission state; a fence changes
it. A same-pair activation retry verifies the durable fence rather than treating
the newly fenced source as a fresh active snapshot.

## Cutover protocol

Waypoint must hold its offline lock before coordinating the checkpoint and cutover.
Each broker Run holds a shared execution lease throughout reservation and effects.
Activation takes exclusive nonblocking execution leases and ledger locks for both
states in deterministic path order. It refuses in-flight source work, any active/unresolved provider liability,
missing source, changed accounting, stale checkpoints,
modified held destinations and a different fence target.

While holding both locks, activation durably writes a one-way source fence bound
to this destination authority ID, path and checkpoint digest. Only after that
write and directory sync succeed does it activate the destination. A crash can
leave both states non-admitting; it cannot leave both admitting. Retrying the same
pair/checkpoint is idempotent. Cleanup changes after a partial cutover can make the
checkpoint stale and keep the destination held; do not reset either ledger to
work around this failure. Status/cleanup remain available at both paths.

Unknown jobs retain their exact reservations and identities in held restores.
Activation requires source reconciliation and a fresh checkpoint with no active or
unresolved jobs. It does not itself contact a provider, replay or settle work. Without a current
source authority, only held recovery is supported in this increment.

Acceptance includes legacy migration, copied-state rejection, output integrity,
unknown liability retention, stale checkpoints, active-run exclusion, one-way
fencing, double activation and a simulated crash after fencing. Local filesystem
durability assumes supported flock, atomic rename and fsync; network filesystems
and remote-host disaster recovery remain outside this contract.
