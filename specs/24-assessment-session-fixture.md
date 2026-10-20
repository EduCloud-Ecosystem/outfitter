# Fixed assessment session fixture

This is a test-only, one-scope, two-slot lifecycle owner for the disabled v7
execution path in spec 23. Production constructors and CLI refusals do not
change. The fixture uses the existing Store, Broker, original opaque operations
and in-process watcher. Workload and checker are actual synthetic test
subprocesses, not an installed recipe, engine, sampler or OS watcher.

The private owner selector runs only in the Go test executable. Its entry
function performs and checks teardown before returning an exit code; only then
does the selector call `os.Exit`. There is no testing-harness stdout footer and
no raw error or captured content on protocol output. Missing prebuilt assets
must fail closed; connected consumers must not build the helper implicitly.

## Bootstrap and ownership

The parent supplies a fresh, already seeded v7 test authority and private roots.
Filesystem and pipe identities come from the actual opened objects. The
one-scope fixture has two immutable planned slots, one CPU per attempt and a
30-second reservation per slot. Its project budget is 60 CPU seconds. No scope
claim, budget reservation, payload read, role acquisition or Broker call occurs
while merely staging a command. Borrowed bootstrap roots and session transport
endpoints are distinct from core operation ownership.

The setup watch-write object becomes lent/ownership-unknown when the actual
prepare call is triggered. The session retains the same object and never
constructs an alias or retries its close. An ambiguous prepare aborts the
session with that liability held. Teardown attempts only definitely owned
endpoints; process exit and parent Wait do not convert unknown ownership into
core cleanup confirmation.

Scope expiry is the whole-second UTC floor of the earliest actual manifest,
authorization, project and policy expiry and `now + 30 minutes`. The latter is
an upper bound, never a replacement grant. The original manifest's identity
and digest remain fixed after the approved project/expiry fixture substitutions.

Session descriptors 0 through 8 are respectively primary command, primary
reply, bounded error-only stderr, owner-read, watch-read, setup watch-write,
single-upload read, safety command and safety reply. The original parent owns
the paired endpoints. All child session endpoints are private/CLOEXEC before
any phase child starts. Phase children may receive only reviewed stdio and
phase metadata/prior-output pipes, plus the existing cancellation ready/gate
pipes when that fixture is selected.

The v7 `ReserveWatch` duplicate uses atomic `F_DUPFD_CLOEXEC` with no `Dup`
fallback. The private helper accesses the original file through
`SyscallConn.Control`; it checks both the outer control error and native errno.
A successful duplicate is registered immediately in the existing child handle
registry, including a nonnil partial result accompanied by an error. Existing
parent/child close-once and uncertainty rules remain unchanged. Old v6 code is
not modified. Source targets are Linux and Darwin on amd64 and arm64;
unsupported targets refuse rather than downgrade to a non-atomic duplicate.

## Staging and handoff

Each of two independent lanes permits exactly one outstanding invocation. A
stage is uint32 big-endian N followed by N bytes of closed valid execution
Control JSON and one final LF, with N at most 131072 including LF. This is not a
claim that arbitrary input is canonical. The exact received bytes determine
the stage digest.

The inert stage ACK is 53 bytes: `EXACK001` (8), lane `P` or `S` (1), sequence
uint64 big-endian (8), N uint32 big-endian (4), and raw SHA-256 of the N staged
bytes (32). It grants no execution, ownership, readiness or cleanup authority.
Only the next exact byte `0x01` on that same command pipe permits dispatch.
There is no pipelining, stream resynchronization or retry after uncertainty.
The child rejects any input already queued at bounded checks before ACK,
after trigger and before reply. A check makes one nonblocking read and accepts
only EAGAIN; it does not establish when future concurrent input was sent or
prove that the parent consumed an ACK. The fixed trusted parent must obey the
grammar independently. There is no post-reply probe that could reject a
compliant parent that has already received the reply and begun its next stage.
The reply is exactly one existing closed canonical public reply, bounded at
65536 bytes including LF. A public Error remains only schema and code; lane
occupancy supplies correlation without public fields.

The future Python parent must authorize each trigger with fresh current
Store/Owner/Guard/identity-publication state, mark `db.effect()`, and perform
the last trusted expiry check after blocking publication/lock work. Its
selected primitive is one private prebound typed `ctypes` libc write call,
not Python's automatically retried `os.write`. Python EX2 implementation and
ABI qualification are outside this patch. A successful one-byte write is local
handoff, not command completion. Lost publication ACK or an exception after
possible delivery stays uncertain and must not replay the command.

The child command-read and reply-write pipes must already be nonblocking when
their Go file wrappers are initialized. Deadline support is checked before any
Broker effect. No duplicate `os.File` aliases or `File.Fd()` conversion may
silently change command/reply poll mode. Every stage, ACK and reply read/write
uses a checked absolute deadline; raw blocking inherited reads are forbidden.
After every deadline setup, the transport rechecks cancellation before the
next blocking operation so a future deadline cannot overwrite an earlier
cancellation wakeup. A Go test driver must supply already-nonblocking atomic
CLOEXEC duplicates wrapped once with `os.NewFile`, rather than passing its
original `os.Pipe` wrappers through Go's `File.Fd` spawn conversion. The
originals and duplicates each retain a distinct checked ownership path.

## Finite lifecycle

There are at most five primary opportunities: prepare, ingest, seal, and one
execute for each slot. Safety has ten: each slot cancel once and reconcile
twice, scope cancel once and reconcile twice, and one scope status. A failed
attempt consumes its opportunity. Valid definitive HELD from a known active
cancel is an operation outcome, not transport uncertainty or permission to
consume a cleanup epoch. Original core owners retain their drain path.
After a known cancellation, the remaining primary plan may end without being
executed only when the same Store currently records denial, checked cleanup,
all transient owner closes and SerialClear for every local attempt. Safety
EOF additionally requires its final status invocation. Normal process exit
still conveys transport completion only; it is not a durable ownership proof.

The absolute session lifetime is 120 seconds. Stage plus ACK and trigger has
one five-second deadline. Non-execute reply/body is limited to ten seconds;
execute reply is limited to forty, without extending its Go 30-second
reservation. All limits clamp to the original session deadline and never
renew per read. The sum of worst-case waits need not fit; exceeding a bound
conservatively stops the session rather than extending it.
The private bootstrap carries the exact microsecond UTC deadline sampled
before launch. The child uses that same `context.WithDeadline`; bootstrap and
constructor time do not restart it. The fixed synthetic policy is narrowed to
the earlier original grant or launch deadline, floored to whole seconds, and
scope expiry is derived from that actual cap. The parent's additional five
seconds allow only kill/reap and checked teardown. They never extend trigger,
admission, phase work or reply time. Real clock sampling and denied cleanup
remain available after the cap expires.

The lanes share no mutex across Broker, provider, body, reply or exit waits.
Malformed/truncated/extra frames, ACK/trigger/reply uncertainty, parent loss or
deadline expiry latch terminal state and close original owned transport once.
Cancel and drain actual owners before borrowed roots are closed. Actual child
Wait/reap, public terminal state or full conservative charge does not create
missing core close proof. Checked teardown precedes selector `os.Exit`; no
deferred cleanup is silently skipped.

## Qualification boundary

Watch-pipe EOF does not join the original watcher callback: its checked watch
close precedes synchronous cleanup. Before a denied scope reconcile, the fixed helper
joins that exact callback outside authority locks, bounded by the current
request and original session deadline. Callback return grants no cleanup proof;
the unchanged Broker and Store must still establish checked closure/removal.
A private causal test mode holds only the original watcher at existing test
snapshot/storage boundaries. Its first reconcile must observe unresolved
cleanup while the original owner remains active; the second distinct allowed
reconcile releases and joins it. Abort releases the gates without replay,
replacement ownership or additional command opportunities.

Focused controls must cover inert actual staging, late/absent trigger, blocked
stage and reply deadlines, parent loss, independent safety progress, actual
child descriptor isolation, atomic-duplicate failures and original-owner loss.
All builds, helpers and controls require a separately reserved finite window.
This source patch itself makes no runtime, Python integration, EX2 SQL envelope,
whole-lifecycle OS watcher, provider, storage or real-data acceptance claim.
The Go driver uses its already-built test executable and ordinary Go pipe
writes solely to test the child/core protocol. It does not qualify the future
Python single-call trigger or parent current-authorization/publication fence.
The two new shared fixture files contain only the fixed A2 manifest and public
synthetic canary body; their package-integrity pins are not runtime input hashes
or authority evidence. No standalone helper binary is built implicitly.
