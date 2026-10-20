# Larger CPU assessment metadata codec

Status: source-only v2 contract. This codec does not admit or execute work.

## Version boundary

`outfitter assessment-receipts` adds the explicit `validate_v2` operation with
exactly `operation`, `manifest`, and `receipts` request keys. Its typed
`DecodeManifestV2` and `ValidateV2` path accepts only:

- `educloud.benchmark-manifest/v2`
- `educloud.measurement-receipt/v2`
- `educloud.cleanup-receipt/v2`
- `educloud.receipt-set/v2`
- result `outfitter.assessment-receipts-result/v2`

The execution profile is exactly
`{"id":"cpu-python-r-bounded","version":1}`. It is a workload capability
identifier, separate from ACCESS/NAIRR application-template selection. There is
no enablement, authorization, encryption, or trust field in this profile.

The v1 `validate`, `project_legacy`, `DecodeManifest`, owned-control/status and
v6 paths keep their existing schemas, limits and bytes. They reject v2. The
new operation rejects v1 and mixed-version receipts, with no fallback or
conversion. Imported historical evidence is never rewritten.

## Manifest and immutable resource limits

V2 keeps the v1 manifest sections, adds `execution_profile`, and adds only
`cpu_count_per_attempt`, `input_bytes`, and `input_rows` to its own `ceilings`.
The manifest is at most 64 KiB. Existing 8 MiB request, 128 KiB receipt-set and
256 KiB output bounds are unchanged. These sizes bound encoded metadata, not
storage capacity. Canonicalization and integer-only, closed-field decoding
follow specification 20, including duplicate-key and explicit-null checking.

| Ceiling | Allowed integer values |
| --- | --- |
| `cpu_count_per_attempt` | exactly 1 |
| `timeout_seconds_per_attempt` | 1–1800 seconds |
| `memory_bytes` | 33,554,432–1,073,741,824 bytes |
| `input_bytes` | 1–5,000,000,000 bytes |
| `scratch_bytes` | 2–10,000,000,000 bytes, aggregate per assessment |
| `input_rows` | 1–1,000,000 rows |
| `output_bytes` | 1–65,536 bytes |
| `max_attempts` | exactly `(fitting size count + 1) * repetitions`, at most 12 |
| `cpu_seconds` | exactly `max_attempts * cpu_count_per_attempt * timeout_seconds_per_attempt`, at most 21,600 |

The immutable profile capability fixes the process limit at 64; it is a code
constant, not another caller-selectable manifest field. GPU count is zero.
Allocated CPU and memory cannot exceed the unverified hardware descriptor.
GB means decimal bytes here; the memory maximum is binary 1 GiB. A hardware
descriptor does not prove capacity, and these limits do not grant an allowance.

Require `input_bytes + output_bytes <= scratch_bytes`; never clamp. The fixed
partitions are input = `input_bytes`, output = `output_bytes`, and work =
`scratch_bytes - input_bytes - output_bytes`. At the maxima, work is
4,999,934,464 bytes. Smaller actual input does not enlarge that work budget.
Scratch is shared by one assessment, not multiplied by attempts or directories.
Copies, staging, intermediates, open-but-unlinked data, and results must count
against their respective partitions in a later enforcing implementation.

Keep 1–3 ascending, distinct fitting sizes, one distinct held-out size, and
1–3 repetitions with distinct seeds. Each sample size is at most `input_rows`.
Keep the existing smaller-design annotation and research/instructional shapes.
Reservations may be empty or partial; each declared slot and attempt/reservation
identifier is unique. Input byte and row ceilings are independent and do not
prove that a recipe can attain either limit.

Software identity remains an immutable image, recipe digest and environment
identifier, with one analysis stage and no dependencies. Recipe names still
describe Python/R summary work; structural acceptance does not qualify the
current full-materialization implementations for large inputs. Expiry is
structural metadata here. Later execution must honor the earlier of the
project, current authorization, owner/watch, and assessment deadlines; a
1800-second ceiling never extends any existing authority lifetime.

## Receipt binding and conservative accounting

Each v2 binding has all v1 binding fields plus `manifest_schema` and
`execution_profile`. It must match the exact canonical v2 manifest digest,
schema/profile, project, assessment, attempt, reservation, software environment
and image, hardware, measurement method, and declared sample slot. The manifest
digest also binds the recipe identity/digest and every ceiling. Both receipts
have exactly the same binding. The measurement's cleanup digest binds the
complete cleanup receipt. No identifier or digest proves admission.

Observation source is only `unverified_observation` or `synthetic_fixture`;
`authenticated` and `independently_verified` must be false. There is no v2
legacy projection. Reservation source is exactly
`unverified_profile_reservation` for every lifecycle form.

| State | Reservation and accounting |
| --- | --- |
| Not admitted | Failed/not_admitted; zero reservation and charge; no unresolved accounting liability |
| Admitted, CPU accounting unsettled (including unknown execution) | Exactly CPU count × timeout reserved; zero settled charge; `reserved` provenance and unresolved liability; the entire reservation stays held |
| Admitted, settled | Full per-attempt reservation charged; `estimated_upper_bound` provenance; reported execution-resource cleanup complete |

An `unknown` execution remains unknown even when its conservative CPU accounting
is settled after reported execution-resource cleanup. That form charges the
exact full per-attempt ceiling with `estimated_upper_bound` provenance and
`unresolved_liability: false`; the retained reservation ceiling is the original
bound, not a second live hold. Unknown execution can also retain the full
unresolved hold with zero settled charge. Neither form invents an exit code or
quality result. No partial charge, refund, charged-plus-unresolved double count,
active-state settlement, replay, reopened attempt, or new grant is inferred.
A settled charge is per attempt, not
the aggregate manifest CPU budget. Failed/OOM/cancelled attempts retain their
failure category and conservative charge or hold. Success requires execution
started, exit code zero, no cancellation, and settled accounting. Quality
passed/failed remains only an `unverified_checker` claim; unknown uses `none`.
Provider charge stays unknown/unsupported, null, unit unknown, source none.

This preserves existing broker reconciliation semantics without changing the
accepted journal: unverified JSON can describe accounting settlement but cannot
clear an actual ambiguous-create, incomplete-create, active writer/reader/
checker, ownership, or cleanup hold. A later authoritative bridge must check
those guards using the actual boundary. Caller-reported deletion is never that
proof. Compute cleanup does not resolve temporary-data/recovery cleanup,
termination, access revocation, provider billing, or measurement availability.

## Metric scope and cleanup limits

Keep the complete named metric set and units in specification 20. Available
values are nonnegative safe integers from `unverified_collector`; unavailable
values are explicit null with source `none`. Never fill unknown consumption
with zero or reservation-derived CPU use. For v2:

- `cpu_consumed` scope is `process` or `sandbox`; sandbox means the enclosing
  sandbox/cgroup counter and must never be relabeled as process CPU.
- `execution_duration` scope is `execution_only` or `whole_attempt`; these are
  different measurements and a later A4 adapter must preserve the distinction.
- Other metric scopes remain `process`, `sandbox`, or `whole_attempt`.
- All three GPU metrics are `unsupported`, null, source `none` for this profile.

Cleanup uses the existing three ordered structural scope descriptors: execution
resource ID = attempt ID, temporary-data and recovery-copy IDs = assessment ID.
These IDs do **not** bind an actual A1 broker authority or an owned sealed-upload
scope. A later versioned execution/control contract must bind that exact
authority and owned scope before dispatch or trust. This codec cannot supply it.

Cleanup method is `unverified_scope_observation` version 1. Termination and
access revocation are `unknown` or `failed`, never `verified`; verifier ID must
be null. Deletion claims remain reported/unknown/failed, with not_applicable
allowed only for a non-admitted execution resource. Remaining liabilities are
derived in the existing fixed order. Outcome is unresolved; quarantine and
reuse-denied are true. A prior receipt digest or observation timestamp is only
imported metadata, never evidence of independent verification.

Every result has `validation: valid_unverified`, `execution_authorized: false`,
`cleanup_trusted: false`, and `actual_resource_accepted: false`.

## Verification and required follow-through

Shared v2 fixtures will include a request, canonical manifest/receipts/result,
digest index, an independently constructed maximum metadata form, and bounded
negative cases. Tests must preserve every old v1 fixture byte and cover numeric
edges, malformed types, duplicate/unknown/missing/null fields, profile/digest/
slot/version mismatches, charge dropping, false trust, and lifecycle forms.
Maximum forms contain scalar ceilings and at most 12 slots, never payload data
or a million row objects. The pure executable must leave an empty working
directory unchanged and never call engine/provider/state initialization.

Codec acceptance is not measured throughput, available capacity, scientific
quality, allocation allowance, execution qualification, or full pilot completion.
Required later work remains:

1. Waypoint consumes these exact shared bytes after its AS1 work is accepted.
2. A streaming recipe **and independent bounded checker**, with reviewed
   cardinality/parsing/spill bounds and new immutable software identities,
   precede larger-input use.
3. An enforcing aggregate writer/storage boundary and measurement bridge in
   the existing Broker/Store bind current authority, sealed input, reservation,
   expiry and cleanup. No v6 activation, second ledger, or upload-created grant.
4. Actual quality and metric provenance flow through a versioned A4 held-out
   estimate, digest-approved A5 researcher/instructor export, and cleanup.
5. A qualified host and operator complete researcher and instructor pilots,
   independent reproduction, and accessibility review within an approved allowance.

Current generic 300-second requests, 16 MiB tmpfs/snapshots, 70-second observer,
ordinary retention and all disabled runtime gates are unchanged. A 10 GB tmpfs
under 1 GiB task memory is not an implementation of the storage contract.
No large payload, long sleep, engine/model/provider workload, real data, or live
Jetstream2 testing is authorized by this codec.
