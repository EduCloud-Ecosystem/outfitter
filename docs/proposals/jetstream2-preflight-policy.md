# Draft Jetstream2 preflight policy compatibility proposal

**Proposed scope only. Implementation awaits coordination.** Add an offline
assessment of whether a pinned image, flavor and security group match an explicit
reviewed profile. This would resolve a prerequisite for the finite worker runner
without enabling provider dispatch. Live testing remains blocked by engineering
and operational evidence and explicit user approval.

The accepted source is `e34604ce2e4ab25c23366a8bea49683899425553`, the merge of
[PR 11](https://github.com/EduCloud-Ecosystem/outfitter/pull/11). Its reviewed head
`73ae1ab4aae941cf87de03f745820166e44e7784` passed
[CI run 39098498344](https://github.com/EduCloud-Ecosystem/outfitter/actions/runs/39098498344):
376 race tests/subtests, 26 Docker checks, 38 recovery checks and 19 synthetic
consumer checks. These results establish the offline rescue baseline only.

## Current gap

| Existing behavior | Missing comparison |
| --- | --- |
| Preflight pins the flavor ID and checks broad CPU, RAM and disk bounds | Exact dimensions approved for this particular experiment |
| Preflight checks image ID, active status and checksum | Image memory/disk requirements, architecture and boot compatibility |
| Preflight checks security-group ID and project | The group's complete rules against a reviewed rule set |
| The journal binds configuration, rate, allowance and approval expiry | A compatibility assessment bound to those same inputs |

These boundaries are visible in
[`Preflight`](https://github.com/EduCloud-Ecosystem/outfitter/blob/e34604ce2e4ab25c23366a8bea49683899425553/internal/provider/jetstream2/client.go#L216)
and [`Policy`](https://github.com/EduCloud-Ecosystem/outfitter/blob/e34604ce2e4ab25c23366a8bea49683899425553/internal/provider/jetstream2/journal.go#L23).
[`CreateWorker`](https://github.com/EduCloud-Ecosystem/outfitter/blob/e34604ce2e4ab25c23366a8bea49683899425553/internal/provider/jetstream2/worker.go#L66)
already runs resource preflight before admission. The first proposed slice would
supply a pure compatibility assessment; making creation enforce it would require
a separately reviewed integration. The current
[preflight command](https://github.com/EduCloud-Ecosystem/outfitter/blob/e34604ce2e4ab25c23366a8bea49683899425553/cmd/outfitter/main.go#L184)
accepts configuration, not a compatibility profile.

## Proposed offline contract

Implement a pure evaluator in the Jetstream2 adapter. Its inputs are an explicit
profile, a typed synthetic resource snapshot, the existing configuration and
native policy, and an injected evaluation time. It has no network client, ledger,
filesystem writer or credential input. It neither opens nor configures an account.

The profile would carry a schema version, the complete configuration binding,
exact flavor dimensions, required image properties, an exact normalized security
rule set, and a finite validity interval. Bind the policy digest and profile
digest separately. Native rates and allowances remain accounting inputs; matching
them does not validate provider pricing or establish a spending cap.

The snapshot would carry the same resource IDs, the observed fields needed for
the comparison, an observation time, and explicit completeness information.
Numeric zero, an empty collection and an absent field must remain distinguishable.
Before implementation, the spec must map each proposed image and rule field to
the relevant official API schema. Unsupported properties stay unknown; aliases
or defaults must never manufacture compatibility.

Return one stable check per requirement with `MATCH`, `MISMATCH` or `UNKNOWN`,
the input digests, and `evidence_scope: synthetic_offline`. Aggregate compatibility
requires every required check to match. Missing, stale, future-dated, incomplete,
duplicated or unsupported input cannot match. Every result retains
`provisioning_enabled: false` and `live_readiness: BLOCKED`, including a complete
match. This result is an assessment, not an executable permit or provider receipt.

Use an exact reviewed rule set for the initial security-group comparison. Reject
extra, missing or duplicate rules after documented canonicalization; do not infer
that broad ingress or egress is acceptable. IPv4/IPv6, protocol, port bounds and
remote selectors must be explicit. No real network policy or private route is
selected by the synthetic fixture.

## Acceptance criteria

1. A complete matching fixture produces a deterministic match with exact input
   bindings and both live/dispatch gates closed.
2. Individually changed resource IDs, flavor dimensions, image requirements,
   architecture, boot properties and security rules produce the appropriate
   mismatch. Missing evidence produces unknown, never success.
3. Missing/null fields, ambiguous aliases, invalid integers, malformed rules,
   duplicate JSON keys, trailing content and unknown schema fields are rejected.
   Decode at most 64 KiB per input and at most 128 rules; overflow stays visible.
4. Expired profiles, stale or future snapshots, differing configuration/policy
   digests and mixed snapshots refuse compatibility. The spec defines exact
   inclusive/exclusive time boundaries and canonical encoding before code.
5. The evaluator produces no provider calls, account admission, ledger mutation,
   settlement or cleanup. Native reservations and the two rescue ACK phases retain
   their existing behavior. Existing Docker, recovery and consumer checks pass.
6. Reports distinguish implemented local accounting from unverified live
   allocation, capacity, IAM, host independence and settlement. The historical
   preflight text can be updated to point to accepted offline evidence without
   promoting any live gate.

## Work boundary and follow through

The proposed change is one spec, a pure evaluator and bounded decoder, synthetic
fixtures, targeted tests, and a short evidence record. It adds no CLI, live
transport, policy migration, runner, guest handoff, provider mutation or deployable
supervisor. Implementation would require independent review and final-head CI.
A later decision would determine how to collect and bind fresh provider evidence
and enforce the result at the actual creation boundary.

Keep the original [proposed live manifest](../manifests/jetstream2-live-test.proposed.json)
unchanged, including its unresolved resource, operator and private-access fields.
Its SHA-256 is
`3c9b6e4bfbe0daca29131c7f6a8ae995a6a6988d2897c13089e8e0a939f1e6cc`.
No allocation, host, expenditure, schedule or Ben assignment changes accompany
this proposal. The [offline rescue limitations](../JETSTREAM2-OFFLINE-RESCUE.md)
remain in force, including unknown late effects, retained liabilities and finite
observation capacity.
