# 20 — Assessment receipt projection and validation

This first assessment component implements typed measurement and cleanup receipts
for Waypoint's benchmark-manifest and measurement-preview path. It does not run
an assessment or change the accepted Python/R execution path. Base Outfitter
revision: `b7eb50c6321a1cb39ee0642564547a652c713a02`.

The canonical contract is EduCloud `docs/specs/research-assessment.md`, reviewed
at SHA-256 `c3ca67d5daffcd4404cc9a623284285eee7b25a9103c391dec6893b721de2bff`
(EduCloud PR #37).
EduCloud owns that cross-component contract; Waypoint owns manifest preparation,
membership, retention preview and report export. Outfitter owns its receipt
conversion, execution/accounting semantics and cleanup assertions. Portage owns
inference usage; no Cairn or other component implementation is changed here.

## Narrow integration

An explicit conversion/validation command consumes a sanitized manifest binding
and an existing broker or synthetic-consumer receipt. It executes before any
ledger opening or provider construction. Existing CLI commands, request hashes,
receipt encodings, accounting and provider defaults remain unchanged. There is
no new ledger, scheduler, persistence migration, renewed allowance or execution
selector. Input receipt validation is structural, not authentication of a worker,
user, authorizing operator or independent verifier.

The projection must validate the existing admitted/not-admitted, one-attempt,
status, cancellation, cleanup and unresolved-liability combinations. An admitted
unknown outcome retains its whole reservation. A conservative settled local
charge remains an estimated upper bound; neither a missing charge nor an unknown
counter is silently replaced by zero. Existing native observations/settlement
remain a distinct contract and cannot be coerced into local CPU seconds.

## Receipt semantics

The version tags are `educloud.measurement-receipt/v1` and
`educloud.cleanup-receipt/v1`. Project, assessment, attempt, stage, reservation
and immutable environment bindings are required and checked against the supplied
manifest and source receipt. Explicit units, scope, availability and source
accompany each metric. Unsupported or unknown CPU, memory, storage, I/O, GPU and
phase-duration counters remain unavailable with no numeric value. Broker wall
timestamps do not imply CPU consumption or isolated execution duration.

Reservations, conservative local accounting charges, measured consumption and
provider-reported charges remain separate. A full reservation charge cannot be
presented as measured use or an external provider bill. Failure, cancellation
and unknown execution remain visible independently of measured values.

Cleanup includes the observed scopes, termination/access-revocation/deletion
states, observation method/time, verifier availability, unresolved liabilities
and quarantine/reuse decision. Existing `cleanup_verified` only establishes the
legacy provider cleanup assertion: input snapshots, diagnostic files and recovery
copies can remain. It cannot qualify assessment source removal, access revocation
or independent cleanup. Therefore legacy projection retains assessment cleanup
as unresolved and reuse denied unless separately qualified evidence is supplied
under a future authorized adapter contract. Structurally valid or simulated input
never becomes authenticated or independent evidence through conversion.

This untrusted command rejects an embedded `outcome: verified`, including
synthetic receipts claiming successful checks, a verifier and timestamp. It also
rejects a claim with no remaining liabilities. Partial positive observations may
remain untrusted claims, but cannot establish an overall verified cleanup result.
Reserved/creating receipts cannot claim execution has started. All active states
reject terminal exit codes, finished timestamps and completed execution cleanup.
Cancellation need not imply a user request: expiry can also cancel an attempt.
Unknown execution with provider cleanup complete can retain accounting liability.

Cleanup observations are immutable receipt values. Reconciliation must retain
earlier failed/unresolved observations and link subsequent evidence; this pure
component does not overwrite a prior receipt or implement its own event store.

## Retention and strict decoding

The output is a closed allowlist. It excludes raw stdout/stderr, errors, source
paths, source filenames, source-content hashes, command arguments, credentials,
workload outputs and arbitrary extra metadata. A reviewed software/environment
digest is not a source-data identifier. Bound every input before decoding; reject
duplicate/unknown keys, trailing values, invalid types, invalid units, nonfinite
or negative numbers, contradictory unavailable/value combinations and identity
mismatches. Validation diagnostics use fixed bounded categories without echoing
rejected input. Canonical byte and digest rules must agree with Waypoint's shared
fixture before the wire interface is frozen.

## Pure command and frozen vectors

`outfitter assessment-receipts` accepts exactly one JSON object on stdin:

- `validate`: keys `operation`, `manifest`, `receipts`.
- `project_legacy`: keys `operation`, `manifest`, `binding`,
  `cleanup_receipt_id`, `source_receipt`.

No command arguments or state directory are accepted. Input is limited to 8 MiB,
the receipt pair to 128 KiB and output to 256 KiB. Successful output is canonical
JSON without a trailing newline. Errors contain fixed categories only. Every
result has `validation: valid_unverified`, `execution_authorized: false`,
`cleanup_trusted: false`, and `actual_resource_accepted: false`.

The complete closed manifest is checked structurally, without authorizing its
catalog, membership, expiry or reservation references. Its `reservations` list
can be empty or partial during preparation. A receipt must match exactly one
declared attempt/reservation/stage/role/size/repetition entry. Neither a single
valid pair nor full declared coverage proves batch admission. Legacy projection
also requires request ID = attempt ID = reservation ID and exact project/image.

Canonical JSON sorts ASCII keys recursively, preserves array order and uses
integer tokens only (absolute value at most 2^53-1), with no whitespace. Decimal
or exponent numeric tokens are rejected even if mathematically integral. All
retained strings have closed ASCII vocabularies or bounded identifier formats.
SHA-256 digests use bare lowercase hexadecimal. Cleanup's digest covers the
whole cleanup receipt; there is no embedded self digest. Generic legacy replay
information is unavailable (`reused: null`); consumer receipts retain their
boolean replay claim without generating a new attempt.

Shared vectors in `internal/assessment/testdata` are frozen with Waypoint:

| Object | SHA-256 of canonical bytes |
| --- | --- |
| Manifest | `506a3587d73e70c1d545631ea2dd455056db92570e394230d5f56d81a7738420` |
| Cleanup | `e0e747be8a561293893585aec0dbc48e36d90903cf14c34af7160999959e9cf5` |
| Measurement | `36805e95c8852613a944509c98d1ab5c4a6cba1b842c489a020ac85fc586a52d` |
| Receipt set | `27061101e45bcff439f0e3bfff871d4f3bfe82ec6e02afb7410492e1b318d809` |

## Verification and remaining gates

Use synthetic receipt/manifest fixtures and fake-only existing attempt records.
Cover a correct projection, all lifecycle/accounting states, missing counters,
typed-input attacks, mismatched bindings, partial cleanup/quarantine, preserved
failed observations, explicit unit separation and legacy wire compatibility.
Exercise the actual conversion/validation CLI with no engine/provider calls and
no ledger creation or mutation. Verify the shared canonical bytes and digest.

This slice does not add live measurement, sandbox tooling, runtime enforcement,
temporary upload/storage deletion, independent cleanup or worker authentication.
Benchmarks, real data, provider/Kubernetes/model tests, deployment and capacity
acceptance remain NOT RUN. Ben's issues, disabled Kubernetes/PAIR, historical
failed controls and existing accepted state/allowances remain unchanged.
