# Local broker checkpoint and recovery

This is a coordinated **same-host local cutover**, not a cloud backup service or
remote disaster-recovery claim. Waypoint owns its offline service lock, SQLite,
artifacts, configuration, access revocations and credential reset. Outfitter owns
the compute authority and its one-way admission fence. Run both procedures
together; copying only a broker ledger is not a complete research-workspace backup.

## Operator sequence

Build the current binary (`go build -o bin/outfitter ./cmd/outfitter`) and stop the
requesting service using its offline lock. Retain its coordinated recovery point.
Inspect broker state before checkpointing:

```sh
bin/outfitter --state-dir /private/source-broker recovery-status
bin/outfitter --state-dir /private/source-broker reap
bin/outfitter --state-dir /private/source-broker recovery-status
bin/outfitter --state-dir /private/source-broker checkpoint --output /private/new-checkpoint
```

`recovery-status` includes exact current `authority_sha256`, project allowances,
charges, reservations, unknown CPU-seconds and the unresolved request list. A
checkpoint may retain unresolved work for inspection, but it cannot be activated.
Reconcile the source and create a **fresh** checkpoint before resuming admission.
An ambiguous creation remains a liability even when no resource is observed;
neither restore nor activation overrides that rule.

Retain the returned `checkpoint_sha256` separately in the caller's trusted
manifest. The checkpoint's own embedded hashes alone do not prove authenticity.
Restore to a nonexistent private path, supplying that exact retained digest:

```sh
bin/outfitter --state-dir /private/replacement-broker restore \
  --checkpoint /private/new-checkpoint --checkpoint-sha256 RETAINED_SHA256
bin/outfitter --state-dir /private/replacement-broker recovery-status
```

The replacement is `held`: status, cancel, reconcile and reap are available;
project-init and run admission are refused, including duplicate requests. There
is no replay, budget reset or automatic refund. Completed receipts and diagnostic
bytes are preserved. Missing/corrupt diagnostic files refuse a complete checkpoint
while the source's authority and cleanup remain usable.

When Waypoint has verified its matching source state and sealed source service
admission, activate the replacement against the **current source authority**:

```sh
bin/outfitter --state-dir /private/replacement-broker activate \
  --source-state-dir /private/source-broker \
  --checkpoint /private/new-checkpoint --checkpoint-sha256 RETAINED_SHA256
```

Activation refuses changed source accounting, a changed held destination,
in-flight execution, any unresolved work, or an unavailable source. It takes
both authority locks, durably fences the source to the specific replacement ID,
then enables the replacement. Checkpoint and restore destinations are never
overwritten. Failed partial directories remain for operator inspection.

## Interrupted cutover

If activation stops between its two durable writes, the source is `fenced` and
the replacement is `held`. Both refuse admission. Retry the exact same activation
arguments after inspecting both recovery-status reports. The source's `fence`
reports the intended destination ID/path and checkpoint digest. Its fingerprint
changes when fenced; a same-pair retry verifies that fence, rather than requiring
the original active fingerprint. Another restored destination cannot activate.
Retrying after successful activation does not reset subsequent destination usage.

Without the current source, recovery remains held. Do not delete a fence, edit an
allowance, restore an older source over its current path, or initialize replacement
accounting to bypass refusal. Such edits are not a supported recovery mechanism.

## Version and durability boundary

Ledger v1 migrates **in place** to v2 under its existing lock, retaining provider
owner IDs, request identities, reservations, charges and outputs. The new ledger
binds its authority ID to its canonical local path. Copying v2 to another path
fails closed; use restore instead. Old binaries reject v2, so keep the updated
binary available for status and cleanup after migration. Prior v1 copies have no
path binding and must not be independently started as duplicate authorities.

This guard assumes a trusted local operator and filesystem with working flock,
atomic rename and fsync. It cannot prevent privileged file rewriting, rollback
at the original path, or machine clones with the same path. Network filesystems,
source loss, multi-host fencing and hosted recovery targets require further work.
The Docker watchdog still requires a functioning host and daemon. No provider
self-expiry, native-SU settlement, cloud execution or institutional acceptance is
established here.

## Executable acceptance

```sh
go test -race ./...
go vet ./...
python3 scripts/verify_recovery.py \
  --image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 \
  --output .runtime/recovery-acceptance.json
```

The image must already exist locally. The harness creates new isolated private
state and performs two bounded public Python runs, before and after cutover. It
checks held admission, unchanged receipts, conservative charges, stale source
refusal, same-pair retry and competing destination refusal. It never edits ledger
JSON or performs broad Docker cleanup. State and diagnostic artifacts remain
under its private `.runtime/recovery-*` directory for inspection.

Local evidence on October 17, 2026: **38/38 checks passed in 16.942 seconds**, with
zero observed accounting loss; the full Go race suite and vet passed. This is a
synthetic operator-driven exercise, not an independent second-machine recovery
or a service-owner-approved recovery target. An initial attempt using a tighter
10-second deadline was cancelled during cleanup under local load; its output,
full charge and verified cleanup were retained. The accepted exercise uses the
actual research-workspace 30-second bound.

Unit failure controls cover unknown reservation retention, migration, copied
authority rejection, missing diagnostics, manifest tampering, source/destination
changes, execution exclusion, concurrent activation, and simulated process loss
after durable source fencing. CI runs the same local Docker recovery exercise.
