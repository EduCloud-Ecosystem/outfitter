# Offline Jetstream2 rescue implementation record

**LIVE READINESS REMAINS BLOCKED.** This prototype exercises authenticated cleanup
handoffs between isolated local processes using a simulated HTTPS provider. It
does not qualify actual independent hosts, provider IAM, allocation access,
terminal late-effect handling or a hard spending cap. Operational fields in the
[decision packet](JETSTREAM2-OPERATOR-DECISION.md) remain unselected.

The [design candidate](JETSTREAM2-RESCUE-DESIGN-CANDIDATE.md) was accepted only as
a blocked design record in PR #10. Its independent review and final-head CI are
recorded in [spec 14](../specs/14-offline-independent-rescue.md). This follow-up
implements the limited offline protocol and requires a separate review and CI.

## Implemented boundary

The immutable native policy and each request record pin `offline-rescue-v1`, the
source authority, B's identity and distinct Ed25519 trust keys. `CreateWorker`
persists port intent and authenticated B acknowledgement before its single port
POST. Only a recorded successful port ID permits server intent, whose signed
scope includes that ID and the parent envelope hash. A second durable,
authenticated acknowledgement precedes the server POST. Partial repeats do not
resume creation. Both A teardown and A reaping refuse B-owned requests, including
from a held restored ledger. No automatic executor takeover exists.

B stores one reservation's immutable scope and ACK files, a signed directory
binding, and bounded signed observations. It has no ledger reference or budget,
admission, settlement, activation or receipt-ingestion operation. Signature input
and canonical encoding are specified in spec 14. Unknown fields, alternate or
duplicate JSON representations, special files, symbolic links, conflicting
bytes, corruption, wrong permissions and storage overflow refuse the operation.
File and directory fsync precede every ACK; an identical retry must resync before
returning the stored ACK.

The observer has dedicated resource validators. Discovery and fresh GET before
deletion require exact identity, project, name, image, flavor, reservation and
deadline; explicit empty volumes; and a present port device ID plus exact
network/security-group pins. Missing, changed, duplicated or incomplete evidence
refuses deletion. Server deletion precedes owned-port deletion. GET/DELETE are
separate operations; no atomicity is claimed.

All prototype constructors require an injected transport and literal loopback
HTTPS origin. Requests and catalog endpoints must stay on that origin. B permits
only GET/DELETE resource calls and a separately identifiable token POST. There is
no CLI, live/default transport fallback, credential-environment discovery or
deployable supervisor. Fixture authentication uses public synthetic values.

## Evidence and retained failures

The new tests use a simulated provider and private fixture keys. Process tests
kill A or B at 40 intent, scope-write, file-sync, directory-sync, ACK and POST
boundaries, then run B separately with A's directory unavailable. This is
same-machine process evidence. It does not simulate an actual filesystem losing
power, an independently administered host or production network partitions.

Additional controls cover missing/lost/forged/conflicting ACKs, A save errors,
B write/fsync errors, concurrent duplicate requests, lost/late ports and servers,
fresh-GET changes, missing/null required fields, partial/duplicate inventory,
held/corrupt A, clock disagreement/rollback/expiry races, forged receipts,
FIFO/symlink/corruption/quota failures and a B API-method audit. The original
Docker and native fault controls remain in place.

The first process-harness run failed in two ways that remain part of this record:

- Its initial expectation that every interrupted request could be completely
  cleaned was incorrect. An accepted server scope without a visible server keeps
  the known port and yields `UNKNOWN`. The revised test requires that refusal,
  preserved intent and unchanged liability. No deletion or accounting rule was
  relaxed to make the test pass.
- The harness attempted to run the same process-command object twice. Each
  observer sweep now starts a fresh process. This was a test-harness error, not
  evidence of failed host recovery.

Independent review of implementation `d2c852691f6c96d20de1da124b4635d1efc3131f`
found three P2 issues. They were fixed in
`d4cc13bf2d04347f15e8e82b6d1df1c5b2e7fc12` and retain dedicated regressions:

| Finding | Fix and regression evidence |
| --- | --- |
| A signed final receipt could be returned with a persistence error | Withhold signed output until file and directory sync succeed; inject write/file-sync/directory-sync failures and require no returned receipt |
| B's clock could roll back during ACK persistence and still authorize POST | Recheck loaded high-water, captured acceptance time and expiry immediately before returning ACK; injected rollback in each phase blocks its corresponding POST |
| Creation of B's directory did not sync its parent entry | Sync the parent and propagate sync/close failures before initialization succeeds; verified by source review, not a physical power-loss simulation |

The same revision rejects explicit default and proxy-discovering transports and
normalizes trailing-slash authentication paths. Targeted review regressions pass
under the race detector. Independent re-review of this exact source revision
returned **PASS** for the bounded offline scope with no remaining substantive
findings. The reviewer's separate failure probes and focused lifecycle, late
effect, storage, clock, held/corrupt-A and review regressions also passed under
the race detector. The parent-directory fix was verified in source; physical
power-loss durability remains untested.

The full uncached race suite on the preceding implementation revision passed
356 tests/subtests, including 200 offline rescue tests/subtests, with zero test
failures; static checks also passed. That result is retained as preliminary
evidence, not final-head acceptance. Its JSONL SHA-256 is
`1a52bc029cc2fc73c13481a2488960fff8b72331c3fc10a8205d1513705cf6a9`.

The full uncached race suite on fixed source `d4cc13b` subsequently passed **364
tests/subtests**, including **208 offline rescue tests/subtests**, across all seven
packages. Static checks, formatting, diff and local document-link checks passed.
The [structured evidence record](evidence/2026-10-17-offline-rescue.json) pins the
source hashes, review findings/fixes, test-log digest and remaining limits.
The focused follow-up PR must also pass CI on its final head before merge,
including the existing actual-Docker lifecycle, recovery and consumer checks.

## Limits that keep the live gate closed

- An absence observation is time-bounded. Unknown intent and liabilities remain
  held, and later eligible resources can be observed in subsequent sweeps. A
  server intent with no observed server retains its port; no terminal outcome or
  charge settlement is inferred.
- B retains two phase scopes and at most 64 sweeps, each with durable intent and
  signed outcome. Observation capacity is separate from reserved scope/ACK
  capacity. Exhaustion refuses further cleanup and retains all scope; there is
  no automatic pruning or claim of unlimited monitoring.
- The store is bound to its private canonical directory and pinned keys.
  Restarting B in that directory is tested; copying B to a different directory
  is refused. An identical-path copy on another host cannot be detected by this
  local mechanism. No distributed fencing or copied-host recovery is qualified.
- Clock high-water observations and ACK tolerance detect tested rollback/skew
  cases. They do not establish synchronized independent clocks or detect every
  forward jump. Fresh field checks do not make provider GET/DELETE atomic.
- The protocol version and immutable policy are pinned; deployment/binary
  attestation, provider-enforced credential restrictions, physical disk durability
  and a supported terminal creation outcome remain unqualified.
- No live runner, private guest workload handoff or provider settlement protocol
  is supplied. No live calls, provisioning, allocation use, spending, real data,
  pilot/default-route changes or Ben assignment changes are authorized by this
  work. Docker remains the accepted baseline; issue #5 retains its broader live
  lifecycle and second-operator gates.
