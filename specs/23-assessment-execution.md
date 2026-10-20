# Connected assessment execution in the existing authority

Status: disabled source implementation. Runtime qualification is NOT RUN.
Base: merged PR #20, `e0dc0249c818ab1d8b6c74a5555d9e1ec89c1900`.
Governing reviewed protocol revision 4 digest:
`6e4a799dbf7d8350f0f33a027668c9f582d8bd82bd5738551e55d4fd081d6722`.
Source-implementation approval and binding addenda digest:
`59c0a1f4c4219199acfc5cc8cbc61a3254e51b9f4d58b469c2acefedc006df14`.

## Authority and permanent identity

Use the existing Broker, JSON Store, journal/execution leases, atomic serializer,
`Store.Update`, `Calculate`, watch and reap paths. Fresh test-only journal version
7 adds `assessment_execution`, with schema, identity, scopes and attempts. The
`internal/assessmentexec` package holds only typed metadata and pure validation;
it is not a second authority, scheduler, queue, service or ledger.

Version 7 requires the new object and forbids v6 owned-data fields. Versions 1–6
forbid the new object. No initializer, migration, import activation, budget-grant
endpoint or conversion of v6 exists. Test fixtures alone seed a fresh private
authority with known logical projects, paired authority IDs, finite synthetic
budgets and explicit settled/unresolved legacy controls. Version, owner,
authority, Projects, Runs, NativeAccounts and NativeRuns stay immutable after
seeding. No legacy raw diagnostics may be introduced by the seed or an update.

Each assessment ID is permanently bound to one project, exact complete manifest
and scope in the same authority. Identical prepare is lookup-only. A second
scope with the same assessment ID collides even with entirely fresh attempt and
reservation IDs, after all charges settle, or after data removal. Scope metadata
and planned slots are never deleted, shrunk, appended, reordered or rebound.
The same map validation enforces this; no separate index becomes an authority.

Dispatch IDs are exactly 32 lowercase hexadecimal characters. The pure v2
codec's 80-character structural IDs and partial/empty reservations remain valid
metadata but are not executable. Prepare requires every fitting/held-out/
repetition slot, in manifest order, with one analysis stage. No hash, padding,
truncation, replacement ID, manifest rewrite or digest rewrite is permitted.
Every planned attempt/reservation ID is globally unique across all scopes,
admitted assessment attempts, ordinary Runs and NativeRuns, including slots
never started. Each admitted attempt is an append-only exact subset of one
permanent parent's plan, keyed by attempt ID with immutable handoff nonce.

## Closed wire and private metadata

The exact required JSON field sets are the explicit structs in
`internal/assessmentexec/types.go` and `wire.go`; no field uses `omitempty`.
All nullable fields are present as a value or explicit null. No unknown,
duplicate, case-aliased or omitted key is accepted. Integer tokens are decimal
0..2^53−1 (positive where specified), rejecting bools, fractions, exponents,
negative values and negative zero. UTC seconds and microseconds have exact
fixed formats. Digests are bare lowercase SHA-256; physical IDs are ID32.

| Shape | Schema |
| --- | --- |
| Authority | `outfitter.assessment-execution-authority/v1` |
| Scope | `outfitter.assessment-execution-scope/v1` |
| Attempt | `outfitter.assessment-execution-attempt/v1` |
| Capability mapping | `outfitter.assessment-execution-capability/v1` |
| Scope observation | `outfitter.assessment-execution-scope-observation/v1` |
| Attempt observation | `outfitter.assessment-execution-attempt-observation/v1` |
| Control | `outfitter.assessment-execution-control/v1` |
| Status | `outfitter.assessment-execution-status/v1` |
| Ready | `outfitter.assessment-execution-ready/v1` |
| Result | `outfitter.assessment-execution-result/v1` |
| Error | `outfitter.assessment-execution-error/v1` |

Control is at most 128 KiB; any single reply is at most 64 KiB, canonical JSON
followed by exactly one newline. No pagination or list-all response exists.
Status returns at most one attempt. A fixed argv operation must match the
control operation; the body never selects the operation.

| Operation | Non-null operation-specific fields; all others null |
| --- | --- |
| prepare | admitting_owner_epoch, manifest, declared_upload_ceiling_bytes, expires_at |
| ingest | expected_generation, admitting_owner_epoch, writer_epoch, content_length |
| seal | expected_generation, admitting_owner_epoch, writer_epoch |
| execute | expected_generation, admitting_owner_epoch, slot, handoff_nonce |
| status | optional slot only |
| cancel | reason cancelled/access_ended; optional slot |
| reconcile | optional slot only |

Every control carries the full ScopeBinding: paired authority IDs, scope,
assessment, project, manifest schema/digest, profile, capability reference and
both complete code bindings. Replies echo it exactly. Capability digest covers
its full canonical mapping, both image/environment/interpreter/code identities,
both fixed argv lists, implementation source identities, limits and ordering.
ID/version resolving to another digest refuses. This mapping is installed private
test code, never an uploaded authorization document or an enablement boolean.

Public replies exclude filesystem identities, resource identities, root/lease
names, argv, raw data/results/diagnostics, source filenames and data-content
hashes. Ready is an internal one-shot child acknowledgement; an imported Ready
does not establish live paired descriptors. Error is exactly schema/code, exits
2, and never exposes raw errors or proves non-admission after an uncertain write.
Success unions exit 0; HELD is not acceptance. NOT_ADMITTED is only a definitive
no-claim/no-effect refusal, with null attempt/receipts and zero reservation/charge.

Trust always says synthetic with upload/execution authorization, cleanup trust
and actual-resource acceptance false. Normalized v2 receipt projection is
read-only; an imported receipt cannot write back or resolve journal uncertainty.

## Resource and storage contract

One CPU, fixed 64 PIDs, timeout 1–1800 seconds, memory 32 MiB–1 GiB, input at
most 5,000,000,000 bytes and 1,000,000 rows, output at most 65,536 bytes, and
aggregate scope scratch at most 10,000,000,000 bytes. Keep at most 12 slots and
21,600 aggregate conservative CPU seconds. All are engineering ceilings, not
capacity, measured performance, an allocation or permission to use resources.

Input and output partitions equal the declared manifest ceilings. Work is the
nonnegative scratch remainder. Smaller input does not enlarge work authority.
All phases and slots share one scope: no per-directory/per-attempt multiplication.
Every staging/intermediate/result/tmp/shared-memory surface and unlinked-open
storage must remain bounded by a trusted enforcing capability before real use.
Logical byte counts do not qualify filesystem overhead or whole-host capacity.
A 10 GB tmpfs under 1 GiB memory is not an implementation. Current ordinary
300-second request, 16 MiB tmpfs/snapshot and diagnostics behavior stay unchanged.

## Transaction and concurrency boundary

Lock order is existing shared authority execution lease, exclusive scope
execution lease, nonblocking writer exclusion, then the short journal lock.
Never wait for filesystem/provider/IPC/collector operations under the journal
lock. Watchers latch denial under the journal lock, then try exclusion without
waiting while holding it. Exact opened descriptor identities and live owner/
watch endpoints are checked, not PIDs or unrelated pipe EOF.

One transaction validates full current bindings, complete plan, unused slot,
installed capability, live lifelines, monotonic clock, expiry/full remaining
interval, current authorization, existing finite CPU budget and metadata room;
it creates one permanent claim and the full CPU reservation before effects.
Only an unambiguous successful write and ACK allow that invocation to continue.
Readback after uncertainty is observation, never permission to resume effects.
An identical claim is lookup-only. Different bindings collide; no replay/reopen.

Waypoint handoff orders revocation-before-handoff (refuse) versus later revocation
(cancel the already authorized attempt). Broker checks immediately before every
effect. This is not distributed atomic zero-start after a later revocation.

Caller cancellation is a separate negative check from current authority,
lifelines and the reserved deadline. Check it before and inside forward
reader/collector and phase claims, and after the last blocking check before
opening input, constructing a collector/transport or invoking Create/Start.
Cancellation after that final sample can still race with the callback. A
committed claim is never erased when its callback is suppressed: an unconstructed
nil handle is not a close confirmation. Actually returned handles and successful
Create identities remain registered for checked original-owner cleanup even
when the caller has ended. Cancellation must not suppress that safety work.

## Finite transitions and lost acknowledgements

Scope starts at generation 1, normal counter 0 and safety counter 0; generation
is exactly 1 + both counters, at most 81. Normal capacity is 48 and safety is
32. Attempts similarly use 48 normal + 48 safety, at most generation 97.

**Counters and generation increment inside the semantic transaction before
persistence. A possibly committed transition counts even if rename, fsync or ACK
fails.** ACK controls continuation only; no omitted increment, decrement, rollback
or replay is allowed after uncertain acknowledgement. This binding addendum
supersedes earlier wording describing only acknowledged transitions as counted.

Fixed event enums and exact field masks permit only bounded, one-time effects.
No timestamp-only update, status label, heartbeat, repeated cancellation or
poll consumes either bank. With the approved ownership events, the design bounds
are 26 scope-normal, 26 scope-safety, 23 attempt-normal and 42 attempt-safety
changes. These are source-derived bounds, not a claim that tests executed a
longest legal trace. Any new event needs reviewed derivation rather than silently
spending unused margin. Forward effects never consume safety capacity. Writer
claim requires six normal
scope steps remaining; a slot claim requires one. Every reader/transport/create/
start requires its durable normal write-ahead marker and ACK first.

Compare observation tuples without ID/time/generation. Duplicate of any retained
tuple is a byte-identical no-op. Append at most eight distinct observations;
the ninth latches saturation once. Later observation-only calls remain no-ops,
but a genuinely new one-time close, denial, removal or settlement may still use
its reserved safety event without appending history. Never rewrite prior unknowns.
Clock high-water updates accompany real semantic events only; live watchers keep
an in-memory maximum. Loss/restart/rollback denies; it never resets the clock.

Attempt forward states progress claimed → preparing → running → checking once.
Safety progresses finishing → held → terminal, allowing direct moves toward
held/terminal. No terminal/held return to forward work. Terminal identity and
full recorded charge survive a later one-time uncertainty latch; that uncertainty
blocks new admission without reopening a resource or rewinding accounting.

## Fixed workload then independent checker

`Broker.RunAssessmentOnce` stays separate from ordinary retained-study execution.
One full reservation and one immutable deadline cover two nonoverlapping phases:
workload, then independent checker. No fresh checker budget, timer, retry or DAG.
Reader/collector and each phase's transport have write-ahead start markers and
separate close-confirmed/close-uncertain latches. Uncertainty is sticky.

Checker creation requires workload definitively stopped and removed, no possible
late create or outstanding create/start ACK, closed workload transport with no
uncertainty, current scope/lifelines/clock and retained reader exclusion.
Otherwise suppress the checker permanently and proceed to denial/cleanup.
Global reader/collector close stays false between phases. Final global close is
permitted only when all begun phases/handles are closed and checker completion
or irreversible suppression prevents any further phase. A final confirmation
permanently forbids reopening. Process exit, free leases or provisional resource
absence do not prove drain or create finality.

Input is the single owned sealed file. Fixed recipe/checker code is independently
identified and never copied with data into snapshots. Combined stdout/stderr is
bounded to the output ceiling in ephemeral memory; stderr counts but is discarded.
The checker receives sealed input plus bounded ephemeral output. A supplied output
writer is retired when Start returns; later writes, including synchronous
transport-close callbacks, refuse before retaining bytes or spending budget.
The capture's final wipe follows transport teardown on every return path.
Only closed quality/metrics remain afterward. No generic provider Record raw diagnostics,
sidecars, source hashes, retained Worker output, caches or ordinary export path.
Clearing references is not a cryptographic erasure/swap-exclusion claim.

Collectors bind exact phase resources and return exactly 18 fixed metrics for
workload, checker and attempt. Null unknown/unsupported counters stay null;
reservations never become measured usage. GPU metrics are unsupported. CPU
process versus sandbox and execution-only versus whole-attempt duration remain
explicit; scopes are not silently relabeled for the existing estimator.
Independent quality failure/uncertainty never becomes eligible fitting evidence.

## Accounting and serial admission

`Calculate` includes admitted assessment attempts in the existing project sum.
Hold = full reservation, zero charged, provenance reserved, unresolved true.
Settlement = exact full reservation charged, estimated_upper_bound, unresolved
false, after authoritative compute/reader/close guards. Unknown execution may
remain unknown after settlement. No partial charge, refund, double count or
invented success/exit/quality. Imported reported cleanup is not settlement proof.

`CanClaimNextSlotV7` examines every prior assessment attempt and scope across the
authority, plus frozen ordinary/native uncertainty. Active/held or incompletely
closed resources/readers/collectors/transports block another claim even with
available budget. CPU charge alone is not serial clearance. Possible late create,
missing start ACK, close uncertainty or unremoved resource remains blocking.
Healthy sealed retained input is not a hazard merely because data remains and
public trust is false. After a terminal fully charged attempt with all effects
definitively closed, a different predeclared slot may read the same input once
within the original lifetime and budget. It is never a retry of an old slot.

## Metadata capacity and acceptance controls

Reserved units are `len(Runs)+len(NativeRuns)+sum(1+len(scope.slots)) <= 512`.
Prepare reserves future attempt metadata units, not CPU. Admitting an attempt
uses its existing slot unit; deletion/settlement never frees one. Projects plus
native accounts stay at most 512. Each scope and actual/future attempt must fit
96 KiB and the full same ledger 64 MiB, including worst mixed old diagnostics.
Before allocation, independently check maximum scope/attempt and every public
wire discriminant/nullability form; no silent truncation or limit increase.

Controls must use the real Store serializer, Broker paths, lifeline/lease/helper
processes and fault seams. Required negatives include direct Store.Update plan/
scope/attempt deletion or rebinding; same assessment with a second scope and
fresh disjoint IDs; lost before/after rename/fsync/ACK; duplicate concurrent
claims; late create/start/close uncertainty; premature global close; slot 2 after
charged-but-uncertain slot 1; copied identity; clock rollback; saturated no-op
observations; and raw input/output/stderr poison-marker exclusion. Positive
controls include both unknown accounting forms and slot 2 on the original healthy
sealed input. Maxima use scalar counters, never 5 GB files or 1800-second sleeps.

Normal production argv prepare/ingest/seal/execute refuses before stdin/state/
effects. No flag/env/config/imported trust/public factory enables the boundary.
Ordinary public/direct commands refuse v7 before provider effects. Recovery,
restore, portable copies and raw workspace export refuse before destination
creation. Prior v1/v2 codec/control/v6 bytes and ordinary defaults remain unchanged.

## Required later completion

This source milestone does not complete the pilot. The streaming Python/R recipe
and checker source in Waypoint PR #54 is accepted; qualified installed runtimes
and execution integration remain required. Other remaining work includes an
actual enforcing writer/storage and collector capability, separately versioned
Waypoint current-auth integration
after AS1 acceptance, actual-provenance A4 held-out estimates, digest-approved A5
researcher/instructor drafts, and qualified host/operator reproduction and cleanup.
Target storage/encryption/swap/spool/backup mechanisms and real capacity remain
unselected. No engines, providers, subscriptions, images/models, real private or
student data, live Jetstream2, or renewed allowance is authorized by this work.

## Approved ownership completion addendum (2026-10-20)

The root-approved ownership API revision 5 adds only private journal fields.
Scope records add `prepare_ownership_close_confirmed`,
`writer_ownership_close_confirmed`, `seal_started`,
`seal_ownership_close_confirmed`, `watch_ownership_close_confirmed`,
`cleanup_owner_epoch`, `cleanup_ownership_close_confirmed`, and
`lease_close_uncertain`. Attempt records add `ownership_close_confirmed` and
`ownership_close_uncertain`. Boolean defaults are false, the cleanup epoch
starts null and binds once, and uncertainty is absorbing. Public controls,
status, Ready, results and normalized v1/v2 receipts remain unchanged.

A fresh fixed Store begin commits the original role marker or full attempt
reservation before opening any operation-owned descriptor. Existing clock,
root, capability, authorization and paired lifelines are checked under that
same journal lock through a narrow trusted read-only begin guard. There are no
provider, IPC or lease waits under the journal lock. Duplicate lookup returns
metadata without an operation or descriptor acquisition.

The Store mints a nonserializable operation wrapper pointing to private shared
state. Copies share acquisition, launch, close and commit consumption. Fixed
constructors register every returned descriptor before checking identity or
flock. Each owned unlock and close is attempted once, including partial
construction failures; the aggregate outcome is cached. Borrowed roots and
lifelines are not closed. Generic Store.Update cannot publish positive ownership
completion. Only the same live operation's fixed checked-close path can do so;
no callback boolean, imported receipt, persisted epoch, PID, free lease or raw
FD can reconstruct this origin. Failed or unknown physical close leaves the
prior unconfirmed marker held, even when a best-effort uncertainty write also
fails. The approved publication-ACK refinement below distinguishes a lost final
metadata reply after all physical closes definitively succeeded.

Execution claims first, acquires the existing global/scope/writer locks, rechecks
fresh authority while excluding only its exact live claim, then runs phases.
Finalization suppresses further phases, definitively drains their effects,
closes local reader and collector owners, releases all operation leases, commits
ownership completion, then permits terminal publication. Conservative full CPU
accounting never substitutes for ownership clearance. Newly confirmed global
reader/collector flags require phase-only finality; historical confirmation
followed by later uncertainty remains representable.

Prepare owns all setup and readiness handles and validates the actual bound
Ready frame internally before successful completion. Ingest claims its writer
marker before leases and completes ownership before upload completion. Seal
adds a one-shot begin before leases and promotes sealed only after full checked
acquisition, fresh guards and ownership completion. Aborted prepare/seal owners
may close only under absorbing denial and cannot enable forward admission.

The current positive watcher is a test-only goroutine. Its original opaque
handoff reservation is consumed before any duplicate/pipe/open and its launch
permission before launch. Parent and child own distinct registered descriptors.
The child retires monitoring and closes its watch lease/endpoints before
publishing completion and invoking existing cleanup. There is no external
watcher adoption/bootstrap/restart API or qualified OS watcher handoff.

Cleanup first checks metadata, then atomically repeats that check under begin.
Known active prepare, writer, seal, watcher or non-SerialClear attempt refuses
without an epoch, descriptor open, observation or poll write. Only quiescent,
already denied scope state can claim its first cleanup epoch. The same owner
performs exact removal while exclusions are held and releases all handles before
publishing completion and any positive observation. Normal owner completion
hooks or later existing reconcile/reap may initiate that first cleanup. Unexpected
busy/partial/crash/uncertain ACK after begin conservatively holds permanently;
completed repeats never reopen removed paths. No scheduler or second authority
is introduced. Saturated history never gains a ninth row or loses old unknowns;
closure alone cannot manufacture retained positive removal evidence.

Admission checks pending roles and sticky uncertainty across every scope and
project. Only the authentic healthy live watcher is exempt. Terminal and
SerialClear require checked attempt ownership completion. Initial records and
fixed event masks preserve monotonicity. Limits remain scope 48 normal / 32
safety, attempt 48 / 48, generations 81 / 97, 512 units, 96 KiB per unit and
64 MiB ledger. Approved upper bounds of 26 scope-normal, 26 scope-safety and 42
attempt-safety transitions are design arithmetic. Independent source event
traces support those bounds; this does not claim an executed longest trace.
Maximum-form, saturation, fault and close controls remain required.

### Final publication acknowledgement refinement (2026-10-20)

There is no existing durable/global Store write-error fence; ownedLock only
serializes and releases. When all actual required unlock/close acknowledgements
succeeded, a fixed private positive may be committed even if the final journal
write reports a post-rename/fsync/reply error. The original live operation caches
HELD and never repeats publication, close, acquisition, launch or forward work.
Encoded semantic counters remain counted. A later read may observe the old
unconfirmed image (held) or the truthful checked-close positive image. That read
is a fact observation, not authority to replay the old operation. A distinct
fresh claim still requires every current immutable/ordinary/native/ownership/
lifeline/policy/budget guard and its own definitive full-claim ACK before effects.
Actual physical uncertainty and any recorded sticky uncertainty override prior
positive flags and charges. Cleanup positive absence additionally requires
identity-bound actual removal and current same-owner quiescence. No field,
fence, recovery API, trusted-cleanup claim or production authority is added.
