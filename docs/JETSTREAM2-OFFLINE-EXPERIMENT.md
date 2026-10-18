# One finite offline Jetstream2 experiment

This increment enforces one configured synthetic experiment in the actual
`CreateWorker` and native admission path. It can create one simulated port and
one simulated server through the existing injected loopback HTTPS rescue client.
Guest execution remains `NOT_RUN`; live readiness remains `BLOCKED`.

The [specification](../specs/16-offline-experiment-enforcement.md) requires a
separate immutable envelope binding the source authority, account, exact request,
preselected reservation, time window, lifetime, native amount, and all four
[compatibility](JETSTREAM2-OFFLINE-COMPATIBILITY.md) digests. Native policy is
unchanged, avoiding circular hashes. The envelope retains a bounded synthetic
profile and snapshot; their timestamps/completeness remain fixture assertions.

## Admission and effects

The original ledger serializes consumption of the single attempt with its native
reservation. The account marker and reservation cross-reference each other and
the envelope digest. Ordinary store updates cannot replace or remove the envelope
or reset consumption. Read-time validation checks the retained cross-references,
including when recovery writes the restored authority. Settlement can alter
financial headroom but cannot grant a new experiment attempt.

The actual creator checks exact request replays before resource preflight. They
only return the durable record, including after interruption or expiry; changed
requests cannot resume the remaining work. Held/fenced or corrupt authority still
refuses creation. An old journal handle reads the currently stored experiment.

Compatibility and experiment validity are checked inside admission and again
immediately before each resource POST, after the phase's durable B ACK and final
pre-POST fault boundary. Unknown, mismatched, stale, future or expired evidence
stops remaining effects. Accepted intent, ACKs, known IDs, the consumed attempt
and unresolved native liability remain durable. Each actual POST still uses the
original finite request context and existing exact resource ownership metadata.

The account envelope is at most 8 KiB, its profile 2 KiB and snapshot 4 KiB, with
at most eight rules in either evidence object. The worker record keeps only its
experiment digest and evidence/live/guest labels. The original 16 KiB native
policy and worker-state caps remain unchanged. A capacity failure refuses the
corresponding state transition or effect; it never relaxes a bound.

## Review and verification

Independent review found one persistence defect in initial source
`75c07cd48e69366039e6e17e4daaadf7d3253bbd`: opaque envelope bytes could change
during JSON serialization and invalidate their retained digest. Four negative
controls reproduced it. The fix rejects unstable bytes before writing; two
positive controls retain readable canonical envelopes. The earlier failed run
and one test-fixture initialization failure remain in the evidence record.

The reviewer accepted corrected source
`0172b14ef213ba98f0f39be328c93dd57784bcfb`, independently passing all 129 repository
experiment tests/subtests and nine additional probes with race detection.
The full local uncached race suite passed **736 tests/subtests** across seven
packages: **129 experiment**, **231 compatibility** and **220 rescue** cases are
included. Static checks, formatting, patch checks and local document links passed.
The [structured record](evidence/2026-10-17-offline-experiment.json) retains source
and log hashes, the original 723-test run, the finding and all correction evidence.

Fifteen creator SIGKILL boundaries exercise the committed reservation, phase
intent, signed scope, ACK exchange, POST and saved response. Additional controls
cover concurrent creator processes, stale evidence after ACK, rechecks at the
last pre-POST boundary, changed authority/state, late configuration, held/corrupt
A and late provider effects. Generic native-ledger controls separately exercise
zero settlement and activation while preserving the consumed attempt; the rescue
creator still cannot settle from an observational B receipt.

These results are local/simulated. The final PR must independently pass Broker CI,
including actual Docker lifecycle, recovery and synthetic consumer acceptance,
before merge. No test runs a guest or qualifies live Jetstream2 access.

## Cleanup and limitations

The existing [offline rescue protocol](JETSTREAM2-OFFLINE-RESCUE.md) remains the
sole cleanup path. Both signed scopes and durable ACKs are required. B receives
no experiment ledger reference and cannot admit, replenish, settle or refund A's
reservation. B can clean eligible lost or late effects using its own retained
scope while A is unavailable. An absence receipt remains observational and does
not establish terminal absence or discharge native liabilities.

This is same-host process and filesystem evidence against simulated endpoints.
The existing execution leases, held restore, checkpoint integrity and path fences
remain in force. There is no global defense against hostile administrator rollback
or an identical-path copy on another host. The 64-sweep limit and earlier rescue
limitations remain. Metadata matching establishes no guest boot, host capacity,
effective filtering or private reachability.

No live constructor, creation CLI, credential discovery, deployment switch,
SSH/cloud-init, public guest endpoint, guest transport or guest payload is added.
Docker remains the baseline. The original proposed live manifest, contributor
assignments and prior 607/231 compatibility/220 rescue evidence are preserved.
Live Jetstream2 testing still requires explicit user approval.
