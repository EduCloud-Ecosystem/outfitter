# 17 Storage writer ownership and finite write qualification

This is a proposed protocol and disposable qualification, not runtime behavior.
Whole-root bytes, entries and available capacity remain **UNKNOWN**;
`quota_enforced:false`. No capacity ledger, quota admission, active broker pin,
provider default, accepted store, pruning or cleanup policy changes are included.

## Scope and source boundary

Waypoint PR #39 was accepted at head
`e543ca4e8a388bb71fe96e11e12b6765829e4a29`, merge
`599ee0ce23942bbfc578124af12285484c357157`. Its 17-row contract pins Waypoint
implementation `7d5cf925ffacd16129c6851f1b3d4f8bc4c1a8a0` and the active broker
`5827c84e9952cc67b85e91ceff4282e0ead6923b`. This qualification extends only its
five broker rows: ledger, output, snapshot, watchdog and recovery. The accepted
contract and prior failed controls must remain byte-for-byte intact.

The implementation checkout starts at Outfitter main
`e9c18f970bae38aef7254b90f1465dadd64673d5`. Witnesses export the **active pinned
broker**, verify its source hashes and add test-only observations in a fresh
disposable tree. Main has subsequent native-account/consumer additions; findings
must not silently promote that source to Waypoint's active pin. Source comparison
is separate from executing the active revision. Waypoint owns SQLite, guard and
its recovery integration; this slice changes none of those files.

No Docker daemon, cloud, application credentials, accepted state root, live
provider or storage cleanup job is needed. Provider lifecycle calls use fakes.
If deterministic syscall boundaries require instrumentation, insert only declared
test hooks into the exported source, retain before/after hashes and hook text,
and label the result instrumented. Never describe a synthetic I/O error as
physical exhaustion or a power-loss test.

## Accounting vocabulary

Count logical regular-file bytes and namespace entries separately; include the
root, directories, lock files, sidecars, partial files and unknown entries.
Physical blocks, metadata, inode/free-space capacity and Docker daemon storage
remain separate UNKNOWN operands. A bounded output stream is not its Go JSON
encoded size. Filesystem occupancy is not CPU-seconds, native SUs or settled
provider charges.

An owner is an authority plus request/operation identity, writer role,
incarnation and exact destination identities. A PID, filename, process exit or
settled compute receipt alone is insufficient. A writer can be runnable,
blocked, detached, interrupted or unknown after its parent returns. Preserve
unknown owners, scratch and liabilities without treating absence as release.

The candidate inequality in both dimensions remains:

`observed occupancy + remaining owned growth + proposed new growth + remaining safety growth <= policy limit`

Any UNKNOWN term refuses a modeled capacity decision. Passing a model is not
enforcement. A completed append may consume only its own predeclared paths,
bytes, content and entry counts after an independently derived expectation and
matching terminal proof. Unrelated growth or same-length changed content cannot
consume another owner's allowance. Reusable replacement scratch is needed again
on every generation; retained old bytes never fund the next temporary write.

## Current write paths and failure boundaries

| Writer | Actual ordering at the active pin | Obligation still open |
| --- | --- | --- |
| Ledger | `ledger.lock` covers read, mutation, initial validation, output spills, compact validation, 96 KiB record and 64 MiB ledger checks, temp write/file sync/close, rename and directory sync | Sidecars can precede final refusal. One current ledger plus the next complete temp is a per-call envelope, not a bound on retained failed files or future calls |
| Output | Go JSON encode stdout/stderr, enforce 6 MiB + 512 bytes, make outputs directory, create temp, write/sync/close, rename to digest, sync directory | Same-digest replacement still writes a temp. An orphan can survive failed ledger publication. Diagnostic failure may be recorded while conservative compute accounting succeeds |
| Snapshot | Reserve CPU, create snapshots parent/request directory, walk input then recipe, size check, exclusive copy with a remaining+1 probe, then start watchdog | Outside ledger flock. Count 1024 walked entries plus shared snapshots parent and request directory. A growth refusal may retain 16 MiB + 1 byte. Partial trees persist. Copy has no context/deadline |
| Watchdog | CLI detaches a child and polls existence of a ready filename; `Watch` reads a record, writes/truncates six-byte ready file, then periodically reads/cancels/reconciles under separate ledger transactions | Readiness has no incarnation or durable ACK. No execution lease is held by `Watch`. Duplicate watchdogs, stale ready files, blocking flock, parent exit and cleanup retries do not establish a finite owner/write count |
| Recovery | Checkpoint locks ledger, writes referenced diagnostics, ledger and manifest to a fresh destination. Restore copies referenced files and publishes held authority. Activate takes execution leases then sorted ledger locks, fences source before publishing destination | Checkpoint is not a whole-root inventory or an all-writer barrier. Unknown files, orphan outputs, snapshots and ready files are excluded. Partial destination and both authorities remain occupied after uncertainty |

Opening a legacy ledger can migrate/spill it even when the eventual CLI command
is status. Duplicate project/run requests can rewrite the ledger. Do not label
all reads or retries storage-neutral. Output iteration order across records is
not a stable ownership order; expectations must be independent of map iteration.

## Proposed finite protocol

These requirements need a later coordinated implementation through existing
authority/operation bindings. They are not a second persistent capacity ledger.
Numeric per-file limits below are current code limits, not a whole-root quota.

1. **Declare each writer before effects.** Bind authority, immutable request,
   writer role, boot/process incarnation, phase, exact destination and bounded
   transition to the existing operation authority. Proposed fresh-profile limits
   are one snapshot attempt and one watchdog incarnation per request, one
   serialized ledger publisher per authority, and one outstanding checkpoint
   destination attempt per authority. These are proposed ownership constraints,
   not current guarantees or evidence that all owners are registered. Unknown
   historical owners or unmatched files prevent a complete envelope.
2. **Prevalidate exact serialization before spills.** Prepare bounded diagnostics,
   compact records and complete ledger bytes with the actual Go serializer before
   starting sidecar writes. Validate every compact record and aggregate ledger.
   A write plan binds each output's expected digest/length and the new ledger's
   reference set. Keep accounting and diagnostic persistence outcomes distinct;
   failed diagnostics cannot invent a successful/refunded compute outcome.
3. **Retain replacement headroom.** With observed old files already counted, a
   serialized transaction needs its new ledger (at most 64 MiB), one output temp
   (at most 6 MiB + 512), and every distinct new output it can publish before the
   ledger commits. For k planned distinct outputs, conservatively reserve
   `64 MiB + (k + 1) * (6 MiB + 512)` additional logical bytes plus exact entries
   and newly created directories; tighter measured plans may be smaller. This
   assumes at most one outstanding output scratch across the whole plan. A failed
   or unproven scratch release holds the plan; it cannot advance to another output
   or retry under the same distinct-digest budget. This covers only this writer family. Existing orphans and all other writers remain
   separate occupancy/liability; k and serializer maxima need checked arithmetic.
4. **Bound failure and retry generations.** Propose a single predeclared scratch
   incarnation per write plan and one retained failed incarnation per writer
   slot. Exhaustion holds new work; it never allocates an unbounded new temp name
   or removes an unknown file. Reuse requires the exact prior owner stopped,
   handles closed, identity unchanged and a durable phase outcome. A missing path
   alone is insufficient. Ledger publication itself requires this protocol;
   recording a new slot is not free and cannot recursively assume unbounded
   scratch. Bootstrap and safety reservations must be specified before adoption.
5. **Separate snapshot probing from destination growth.** Pin expected source
   paths/hashes before copying and verify the actual copy against them. Bound
   total input/recipe entries including both roots and destination parents.
   A future copy should probe excess input in memory before writing beyond its
   declared destination limit, observe cancellation at bounded intervals and
   keep the writer registered until all copy handles close. Today the extra byte
   and cancellation gap remain real counterexamples. No successful copy receipt
   may be derived from the output inventory itself.
6. **Give watchdogs durable ownership.** Start only the declared incarnation;
   readiness must attest the exact authority/request, incarnation, held writer
   lease and durable lifecycle state. A preexisting ready path is not that ACK.
   Bound cleanup/update attempts and their safety scratch before launch. Reaching
   a limit retains uncertainty and requires an explicit handoff; parent exit or
   a compute settlement cannot replenish it. Keep provider cleanup independent
   of a dead requester without silently multiplying owners.
7. **Fence every writer before storage cutover.** The candidate barrier must
   cover snapshots, watchdog startup/readiness/removal, ledger/output writes,
   migration, cancel/reconcile and recovery publications. Reuse an agreed lock
   hierarchy (execution/writer leases before ledger locks; multiple roots sorted)
   and refuse boundedly rather than deadlock or suspend essential cleanup.
   Checkpoint/cutover requires exclusive ownership plus a complete writer census;
   a zero count of known leases cannot prove unknown owners absent. Waypoint's
   stopped state is not a substitute for this broker proof.
8. **Reserve complete destination work before copying or sealing.** Count source,
   external checkpoint, held destination, referenced copies, ledger replacements,
   manifests, marker/lock files, directories and remaining safety writes. Unknown
   source entries remain charged at source and are not silently omitted from a
   whole-root claim. Revalidate before the first source fence. A fence followed
   by destination failure keeps both roots protected. Before destination rename,
   source is fenced and destination remains held. After rename but before successful
   directory sync, destination may already be active despite an error; durability
   is uncertain and the error cannot certify both roots non-admitting. A future
   protocol needs explicit destination admission fencing and verification, with
   enough reserved safety work to persist that outcome. No automatic replay,
   refund or deletion. One outstanding checkpoint is a proposed
   bound only; retained earlier checkpoints still occupy their own destinations.

Safety cancellation, revocation, cleanup evidence and conservative settlement
must have a finite reserved write path at ordinary limits. If that cannot be
proved, preserve held/unknown state and external rescue responsibility rather
than promise indefinite durable retries. A lease protocol alone supplies no
byte capacity. Complete coverage of SQLite/guard, shared filesystem and other
host writers is still required before any quota admission proposal.

## Disposable witnesses and evidence interpretation

The harness must execute pinned production functions, derive expected content
before observation, identify every injected boundary and retain failed runs.
Use explicit assertions for retained liability, old ledger identity, partial
files and process/lock lifetime. Tests pass when they reproduce a counterexample;
the corresponding safety property remains FAIL, not PASS. Unsupported operating
systems or missing prerequisites are NOT RUN and cannot be a passing empty run.

Required controls:

- Output spill before injected ledger publication failure, repeated distinct
  orphan growth, encoded-output amplification and same-digest temporary overlap.
  A separate lowered compact-record threshold exercises ordering only: record the
  test threshold and production 96 KiB cap. Ordinary CLI reachability of that cap
  remains NOT DEMONSTRATED; do not report a production-cap failure.
- File-sync/rename/directory-sync fault boundaries with prior authority or visible
  uncertain generation distinguished; diagnostic persistence failure does not
  erase conservative accounting.
- Exact snapshot entry accounting and deterministic growth between size check
  and copy; partial retained bytes and no new provider effect on refusal.
- Snapshot cancellation/observer completion is not terminal copy-writer proof.
- Actual `Watch` in a disposable process, with fake provider operations: readiness
  outside execution/ledger fencing, stale/duplicate owner behavior, blocked ledger
  update, requester exit, retained ambiguous liability and final process termination
  observed separately. The test supervisor starts and waits for both requester
  and detached watchdog processes. Actual CLI launcher/OS-parent handoff is NOT RUN.
  Do not infer real Docker or host-loss acceptance.
- Checkpoint includes only referenced outputs, preserves/excludes unknown source
  entries, can coexist with unfenced writers, and refuses incomplete referenced
  output without granting whole-root or destination capacity credit. Contrast a
  pre-publication cutover fault with a destination directory-sync fault after
  rename: the latter can return error while destination admission is already active.
- Finite candidate model controls reject wrong identity/incarnation, nonterminal
  ownership, content corruption, unowned growth, missing scratch and UNKNOWN.
  They exercise a proposed rule only and must be labeled unenforced.

Retain Waypoint's fifth headroom FAIL and all earlier review failures. Actual
ENOSPC/inode exhaustion, host crash/power loss, daemon storage, complete independent
writer discovery, enforced destination peak refusal and production quota
enforcement remain NOT RUN. Live Jetstream2 retains its engineering, operational
and approval gates; Ben's issues #1–#5 and all selections are unchanged.

## Acceptance for this slice

Independent review must verify the source pins, production connection, fault
placement, negative controls, preserved evidence and precise PASS/FAIL/NOT RUN
labels. A focused PR may accept this protocol/qualification only. Coordinate any
broader implementation, new profile or default change separately. Do not claim
storage readiness, capacity reclaimed or a repaired headroom witness.
