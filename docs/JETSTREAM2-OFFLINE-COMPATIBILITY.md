# Offline Jetstream2 metadata compatibility

This increment compares synthetic resource declarations with an explicit profile.
A matching result means only that the supplied metadata satisfies that profile.
Every report keeps `evidence_scope: synthetic_offline`,
`provisioning_enabled: false`, `live_readiness: BLOCKED` and `boot_verified: false`.
Image boot, available hosts, effective filtering and private reachability remain
unverified. No live approval or executable permit is produced.

The [contract](../specs/15-offline-metadata-compatibility.md) narrows the
[reviewed proposal](proposals/jetstream2-preflight-policy.md) to pure
`EvaluateCompatibility(profile, snapshot, config, policy, now)`, two bounded
in-memory decoders, and digest helpers. Its production files have no client,
ledger, filesystem or environment access and read no clock. Existing creator,
preflight command, accounting and cleanup behavior are unchanged.

## Comparisons and evidence

The profile pins full configuration and native policy, exact flavor dimensions,
image declarations, group statefulness and a complete ingress/egress rule set.
Versioned canonical digests bind those inputs and the supplied snapshot. Rules
are order-independent, and normalization does not modify caller-owned inputs.
Timestamps and completeness flags are unauthenticated fixture assertions.

Missing observations yield `UNKNOWN`; present nulls retain their field-specific
meaning. In particular, null security-rule protocol or port bounds may express
unrestricted access. Unrestricted CIDRs and broad egress require the corresponding
explicit profile flags; nulls never mean an empty or harmless rule. Dynamic
selectors and unsupported metadata remain unknown.
Complete empty rules differ from absent, null or incomplete rule evidence.

The [primary-source mapping](proposals/jetstream2-openstack-field-mapping.md) and
[source pins](proposals/jetstream2-openstack-source-pins.json) distinguish image
minimum declarations, root disk and additional ephemeral disk. Only positive-root,
ordinary image-backed fixtures are supported. Image minima and payload size are
separate comparisons; matching them does not prove boot or expanded-image fit.

Reports contain fixed check IDs, status/reason codes and digests. They do not echo
identifiers, URLs, raw input, secrets or upstream response bodies. Malformed
profiles/envelopes and over-limit inputs are rejected with fixed errors. Inputs
are capped at 64 KiB, 128 rules and 16 levels of JSON nesting.

## Review and local verification

Independent review of `826b2f632c1ca1f052e6bd02cb44317c47fb90cc` requested
three fixes: protocol `0` falsely matching `any`, null rule objects losing their
meaning, and checking local calendar years before UTC normalization. It also
identified incomplete-rule alias normalization and invalid UTF-8 replacement.
All five probes failed before correction; that failure record is retained.

The reviewer accepted corrected source
`eea4fa3302b6d09f5022f32db8ff5d43098d06fc`, including independent race-enabled
probes in an isolated copy. The full uncached race suite passed **607 tests and
subtests**, including **231 compatibility** and **220 rescue** cases, across
seven packages. Static checks, formatting, local links and patch checks passed.
The [structured evidence](evidence/2026-10-17-offline-compatibility.json) records
source/log hashes, findings, regressions and the earlier 570-test result. It also
retains the initial incomplete test harness build failure due to unused imports.

These are local synthetic results. The PR must still pass its exact-head Broker
verification, including actual Docker lifecycle, recovery and synthetic consumer
acceptance, before merge. No local result establishes live Jetstream2 readiness.

## Preserved live boundaries

This increment introduces no CLI, network transport, resource creation, permit
configuration, accounting mutation, settlement, guest handoff or live enforcement.
The earlier offline rescue evidence remains intact, including 376 tests/subtests
and 220 rescue cases on its corrected source. Unknown late effects and native
liabilities, the 64-sweep retention limit, the original proposed live manifest,
Docker baseline and Ben's assignments retain their existing meaning.

Live Jetstream2 work remains blocked on engineering and operational evidence and
explicit user approval. Matching metadata closes none of those gates.
