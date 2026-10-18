# 15 — Offline metadata compatibility

This increment implements only a pure `EvaluateCompatibility(profile, snapshot,
config, policy, now)` and bounded in-memory decoders. The accepted design review
of proposal `19efdcad86678d7199995e30e1f4133360fddf65` requires this narrowed scope.
No client, journal, environment, filesystem, CLI or creator integration changes.
No admission, cleanup, settlement, receipt ingestion or provider request occurs.
All reports remain `synthetic_offline`, `provisioning_enabled:false`,
`live_readiness:BLOCKED` and `boot_verified:false`.

## Inputs and evidence

Version 1 profiles bind full configuration and native-policy digests, a validity
interval, maximum snapshot age, exact flavor dimensions, image declarations,
stateful-group expectation, and a complete expected semantic rule set. The policy
must validate and contain the same configuration; profile expiry cannot exceed
its approval expiry. Existing policy values and schemas are unchanged.

Profiles require every declared JSON field, including explicit booleans and null
port endpoints. Malformed profiles are rejected. Snapshots require their version,
three binding digests and observation timestamp; resource observations can be
absent, explicit null or typed values. Absent/null required observations yield
UNKNOWN rather than fabricated zeros. Structurally malformed snapshots are
rejected. Snapshot timestamps and completeness are fixture assertions, without
provider authentication or attestation.

The schema mapping and immutable upstream pins are in
[the primary-source note](../docs/proposals/jetstream2-openstack-field-mapping.md).
Snapshot fields are an Outfitter envelope, not an exhaustive raw OpenStack schema.
Both JSON inputs are limited to 65,536 bytes and 128 rules, with nesting at most
16. Reject duplicate keys at every level, wrong-case/unknown keys, trailing data,
invalid types, invalid UTF-8, overflow and unsupported versions. Null rule array
elements are malformed; a null rules observation remains UNKNOWN. Errors and reports use fixed
reason codes; input text, identifiers, URLs, credentials and upstream bodies are
never echoed. Typed inputs receive the same validation and size limits.

## Resource comparisons

Compare exact configuration IDs and project/region/network, exact vCPU/RAM/root
and additional-ephemeral dimensions, image checksum, and group project. Initial
profiles require 1–16 vCPUs, 1–65,536 MiB RAM, 1–200 GiB root disk, and zero
additional ephemeral disk. This preserves existing broad resource ceilings.
Only explicitly observed `ordinary_image` disk mode is supported; another mode
is UNKNOWN. Missing/null dimensions are UNKNOWN; negative or excessive dimensions
mismatch. Image minima must be explicit nonnegative integers and fit RAM/root
respectively; equality and zero are valid declared minima. Compare image size in
bytes against positive root GiB using bounded arithmetic. This does not establish
actual guest resource sufficiency or expanded-image fit.

Image declaration vocabulary is deliberately small: architecture `x86_64` or
`aarch64`; firmware `bios` or `uefi`; disk format `raw` or `qcow2`; container format
`bare`. Exact supported values may match; a different supported value mismatches;
missing/null or unsupported observations are UNKNOWN. These are declaration
comparisons only. Actual boot, host suitability, capacity, private access and
network isolation remain unverified.

## Security rules

A snapshot must explicitly assert complete rules, state the group's statefulness,
and provide a non-null array. A complete empty array may match only an explicitly
empty profile. Missing, null or incomplete arrays remain UNKNOWN. Dynamic
security-group/address-group selectors are unsupported observations, not CIDRs.
Each rule has a unique nonempty ID, explicit direction/ethertype/protocol/port
bounds and all three selector fields. Missing fields are UNKNOWN. Null protocol
means `any`; both null port bounds mean unrestricted ports; explicit null remote
selectors preserve their own meaning and never imply a harmless rule.

The initial protocol vocabulary is any, TCP, UDP, ICMP and IPv6-ICMP. Accept their
canonical names and numbers (6, 17, 1, 58, as strings or integers);
normalize `icmpv6` to `ipv6-icmp`. Booleans/floats are malformed. Only TCP/UDP
support ordered ports 1–65,535, or both null for all ports; normalize the full
range to null/null. ICMP endpoints are type/code 0–255, allowing type with null
code, but never code without type. ICMP must match its IP family. Any protocol
requires null/null bounds. Protocol `0` remains unsupported because API prose
and the pinned Neutron source disagree about its meaning. Unsupported protocols
are UNKNOWN in snapshots and rejected in profiles.

Require explicit canonical CIDRs matching the ethertype. A null prefix maps to
that family's /0 only when both dynamic selectors are explicitly null. Nonempty
dynamic selectors remain UNKNOWN; malformed/contradictory selectors mismatch.
Profiles use explicit CIDRs only and reject host bits or noncanonical spelling.
Rule semantics are direction, family, protocol, port bounds and CIDR. Compare the
entire normalized ingress+egress set, order-independently; extra/missing/duplicate
semantics mismatch. Snapshot IDs are retained in its digest but not the semantic
comparison. Identical rules with different IDs are still duplicates.

Profiles must explicitly contain `allow_unrestricted_selectors` and
`allow_broad_egress`. A /0 needs the former; egress with /0, any protocol or both
null port bounds also needs the latter. These booleans authorize only a synthetic
comparison. No SSH, DNS, HTTPS, IPv6 or route defaults are introduced.

## Time and canonical binding

Normalize times to UTC before requiring years 2000–2100; preserve precision.
The evaluation interval is `not_before <= now < expires_at`. Snapshot time must
satisfy `not_before <= observed_at <= now`, and age must be at most the profile's
1–3,600 second maximum (equality is accepted). Invalid `now` rejects; expired,
future or stale evidence yields fixed nonmatching checks. No clock is read.

Canonical bytes are Go JSON encoding of fixed structs, UTC times and normalized,
lexically sorted rules. Sort snapshots by semantic rule bytes and then ID;
normalize supported aliases and CIDRs before hashing, without dropping missing
or null states. Supported aliases normalize even when another rule field is
missing or unsupported. Invalid UTF-8 in typed strings is rejected before
serialization. Input slice order must not be modified. Digests are SHA-256 over
`outfitter-metadata-compatibility-v1` + NUL + kind + NUL + canonical bytes, for
kinds `config`, `policy`, `profile` and `snapshot`. Normalize policy approval time
only; all its fields, including rescue policy if present, remain bound. Profile
binds config/policy digests; snapshot binds config/policy/profile digests.

Return all four digests and stable ordered check IDs with MATCH/MISMATCH/UNKNOWN
and fixed reason codes. Overall metadata match requires every check to MATCH.
A digest proves only byte binding, not provenance or authority. Failed decoding
returns a sanitized error; evaluation errors still return the closed-gate report.

## Verification and preserved boundaries

Cover canonical equivalence, every binding mutation, mixed resource identities,
size bounds/inequality/equality/zero/null/overflow/modes, image declarations,
rule completeness/duplicates/directions/families/ports/selectors, malformed JSON,
byte/rule/depth limits, time boundaries, secret sentinels and input immutability.
Audit imports and call sites for no I/O, and use fixtures to assert zero network
or accounting effects. Existing full race and Docker/recovery/consumer checks
must pass. Independent review and final-head CI are required before merge.

Preserve prior 376/220 test evidence, the original proposed live manifest,
unknown late effects and charges, 64-sweep retention and Ben's assignments.
Live enforcement, actual boot, independent hosts, IAM and all operational approval
remain separate. No matching report changes any of these gates.
