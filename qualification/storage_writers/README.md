# Offline storage writer qualification

This qualifies a **proposed** ownership/finite-write protocol. It does not implement
that protocol or change broker behavior. Whole-root bytes, entries and available
capacity remain **UNKNOWN**; `quota_enforced:false`. The runtime never loads this
directory. See [spec 17](../../specs/17-storage-writer-qualification.md).

The harness exports Outfitter `5827c84e9952cc67b85e91ceff4282e0ead6923b`, the
active broker pin in Waypoint's accepted 17-row contract. It verifies all 11 source
hashes, adds the declared Go overlays and records every instrumentation replacement
and its source hash. The production functions run in disposable directories with a
fake provider. No Docker daemon, cloud credentials or accepted store is used.

From the repository root, with Python 3 and Go supporting the race detector:

```sh
python3 qualification/storage_writers/run.py --output /tmp/new-storage-writer-evidence
```

The output directory must be new. The repository argument is a Git object source,
never a state-store location. A temporary source export, Go build cache, input data,
state roots, checkpoints and restore destinations are created solely for the run;
normal fixture teardown removes only those disposable objects. Reports and raw logs
survive failure. Run serially on Linux or macOS; missing tools, skipped controls,
missing witness IDs or failures cannot produce a passing report. Go dependency and
toolchain downloads are disabled. The new CI job uses no Docker; the repository's
existing Docker acceptance job remains a separate check.

## Reading the evidence

The overall `PASS_COUNTEREXAMPLES_REPRODUCED` means the expected observations were
reproduced. It does **not** turn the properties marked `FAIL` into storage guarantees.
The 20 connected observations are grouped below. Every observation is executed
against the pinned production implementation with declared test hooks; the hooks
add barriers, observation points, or an error at the identified I/O boundary.

| Observation | Meaning and remaining obligation |
| --- | --- |
| Three distinct outputs survive failed ledger publication | Injected file-sync errors leave prior authority/30 unknown CPU-seconds intact and accumulate three orphan sidecars; no global retry/orphan bound proved |
| Lowered compact-record threshold | Test cap is **1 byte**, real cap is **98,304 bytes**. This exercises ordering after spill. A normal valid CLI request reaching the real cap is **NOT DEMONSTRATED** |
| NUL output encoding and same-digest rewrite | 1,048,576 raw bytes encode to 6,291,481 bytes; old digest plus new temp simultaneously hold 12,582,962 output bytes. This excludes ledger/other occupancy |
| Six publication faults | Separate ledger/output file sync, rename and directory sync failures. Ledger faults before rename retain old authority; after rename a new generation is visible but durability is unproved. Output failure still permits conservative compute settlement |
| Legacy open | A v1 fixture reopens as v2 and spills diagnostics; opening for a later read command is not necessarily storage-neutral |
| Snapshot entry refusal | 1,024 walked entries plus snapshots/request parents leave 1,026 entries on refusal; no fake provider create occurs |
| Source growth after stat | Real production copy retains 16 MiB + 1 byte before growth refusal. Expected content is constructed and hashed before copying |
| Cancelled copy and concurrent checkpoint | Checkpoint completes with unresolved compute while snapshot copy is blocked; the copy still writes after context cancellation. Terminal copy completion is observed separately |
| Watch readiness and lock blocking | Actual detached Watch process overwrites stale readiness while both execution/ledger locks are held; cancellation cannot complete until ledger flock releases |
| Duplicate Watch owners | Two real processes share one ready filename; one exit removes it while the other process is still alive |
| Requester exit | A real requester process exits; the independently supervised Watch process continues publishing at least three observed ledger generations, retaining 30 unknown CPU-seconds |
| Settled compute and cutover | Actual checkpoint/held restore/activation succeeds while a Watch process is paused after reading its record; that process subsequently writes readiness at the fenced source |
| Referenced-only checkpoint | Checkpoint succeeds while an exclusive execution lease is held. It copies exactly the referenced output, ledger and manifest, while preserving/excluding unknown, orphan, snapshot and stale-ready source files |
| Destination post-publication failure | An injected directory-sync failure after destination rename returns an activation error while source is fenced and destination visibly admits; both roots remain protected and durability is UNKNOWN. The existing pre-publication crash control separately leaves both non-admitting |
| Same-length corrupt checkpoint reference | Hash mismatch refuses sealing; partial destination remains and retrying its path refuses. No capacity credit follows |

The Watch helper is a child of the test supervisor, which starts it with `Setsid`
and calls `Wait` for terminal proof. The requester is a separate supervised process.
This establishes requester loss plus an independent writer, **not** the actual CLI
launcher's OS-parent/child handoff. The CLI's existence-only ready polling was
source-inspected but is **NOT RUN** here. Readiness barriers deliberately widen real
scheduling windows; they are not an assertion of normal timing.

The separate finite model has one accepted boundary case and 13 rejection cases:
authority/incarnation mismatch, nonterminal owner, open handles, same-length
corruption, extra/missing paths, unknown owners, unproven scratch reuse, UNKNOWN
bytes/entries, and byte/entry peak excess. It operates on declared inputs only;
**MODEL_ONLY_NOT_ENFORCED**, with no production ownership census or admission link.

## Provenance and source comparison

The copied contract is byte-exact from Waypoint PR #39 head
`e543ca4e8a388bb71fe96e11e12b6765829e4a29` (merge
`599ee0ce23942bbfc578124af12285484c357157`), path
`research-workspace/qualification/capacity/contract-v1.json`, SHA-256
`4e87eb642897e30cda868e361672c8a499328395c0b8506c9e7c6e87f0b5dbc2`.

The retained fifth failure is copied from that same head, path
`research-workspace/evidence/2026-10-17/storage-admission-blocked/observation.json`,
SHA-256 `763ec3e001ef50512fca925efe24b2f6878141f7b448b2ecb495a5b2b7db2957`.
It remains **FAIL/BLOCKER_REPRODUCED**; this slice does not repair the earlier
headroom failure or edit Waypoint's accepted evidence.

The report compares whole-file hashes at Outfitter main baseline
`e9c18f970bae38aef7254b90f1465dadd64673d5`. Main adds native accounts/runs,
consumer bindings/content checks, supported ledger versions, and recovery guards.
This is a source comparison, **not** a qualification of main or permission to
change Waypoint's active broker pin. The snapshot, Watch and output-spill ordering
reviewed here persists in that baseline; wider envelopes require separate review.

Actual physical exhaustion, failed temp removal/scratch accumulation, host/power
loss, Docker storage, complete independent
writer discovery, enforced destination peak refusal and production quota
admission remain **NOT RUN**. Live Jetstream2 retains engineering, operational and
Greg's separate approval gates. Provider defaults, selections and Ben's issues
#1–#5 are unchanged.
