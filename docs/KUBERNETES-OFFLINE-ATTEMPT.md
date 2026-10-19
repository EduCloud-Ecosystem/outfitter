# Disabled offline Kubernetes attempt

This implements the narrow P2-A protocol in
[spec 19](../specs/19-offline-kubernetes-attempt.md). It is accessible only through
an internal constructor requiring an injected API. No live API implementation,
kubeconfig/credential reader, backend CLI option, host engine change or workload
is included. Tests create new temporary synthetic allowances; no accepted state
or earlier exercise allowance is reused. Docker/native/consumer callers remain
on their existing paths. Ben's assignments and live Jetstream2 approval are unchanged.

The existing ledger reserves CPU-seconds before a possible Pod create and binds
the backend, cluster digest, namespace name/UID and attempt ID to the request.
A durable one-time dispatch claim and immutable resource UID survive restart.
Exact duplicate runs read retained status without API contact. Changed bindings
are refused. A fresh request cannot reuse the same bound attempt identity.
Reaping is scoped to the configured backend and exact cluster/namespace target;
foreign records remain unchanged and cannot block eligible owned cleanup.
Explicit foreign cancellation and reconciliation remain refused.

Ledger version 5 identifies authorities containing offline placement records.
Legacy request/record JSON remains unchanged when placement fields are absent.
The prototype does not migrate any existing on-disk state. Tests cover retaining
an earlier local record and native account when an offline reservation is added
to the same synthetic ledger. Version-5 checkpoint/restore is refused; same-state
reopen, receipt/usage reads and owned cleanup are covered. Old binaries refuse
the new format rather than silently interpreting its records as Docker work.

The fake intent has exactly one ordinary container, `restartPolicy: Never`, no
controller and no init/sidecar container. It carries the existing resource and
output caps and absolute deadline. It is not a deployable Pod manifest: image
availability, artifact transfer, storage, effective PID/CPU/memory enforcement,
network policy and independent cleanup are unqualified runtime gates.

Creation can itself start a Pod. A dispatch claim records possible execution;
an ambiguous response remains unresolved even after a late owned UID is observed
and removed. A known UID cannot be replaced, and delete/log operations bind it.
API-object absence alone is insufficient: an additional UID-bound stopped-workload
observation is required. That observation is supplied only by the fake API; no
live method for obtaining or trusting it is provided. Missing termination evidence
or cleanup failure retains liability. No retry, replacement Pod or refund occurs.

Completed, cleaned-up attempts retain the full `estimated_upper_bound` CPU-second
charge. This is not measured usage, money, native allocation units or control-plane
overhead. Uncertain attempts retain their entire reservation. Local ledger guards
prevent API-level rebinding, record removal, claim reset, UID replacement and
charge reduction; they do not detect privileged filesystem rollback.

## Verification

The fake conformance tests cover reserve-before-create, exact identities, duplicate
and concurrent calls, cancellation/expiry, interrupted reservations and dispatches,
ambiguous create/late appearance, foreign UIDs and deletion races, lost API/node
observations, bounded combined output, cleanup failure and no refund/replay.
The full Go suite also checks the existing native and synthetic consumer paths.

An offline invocation, with actual Docker tests explicitly disabled, is:

```sh
env -u OUTFITTER_DOCKER_IMAGE -u OUTFITTER_NATIVE_TEST_ROLE \
  -u OUTFITTER_RESCUE_TEST_ROLE go test -race -p 1 -count=1 ./...
go vet ./...
```

The new package never opens a network connection or launches a process. Existing
Jetstream2 tests use their prior loopback/process fakes. Existing CI Docker jobs
remain separate baseline evidence; they do not qualify this Kubernetes prototype.

Self-review and verification results, including failed controls and their artifact
hashes, are indexed in [the evidence summary](evidence/2026-10-18-kubernetes-offline/verification.json).
Independent review and CI on the immutable final PR head remain merge gates.
Root owns review/merge and host engine operations; this change performs neither.

## Decision

**DEFER live Kubernetes adoption.** Cluster capacity/operator approval, a live
identity-enforcing transport, independently verified termination, artifact/state
consistency, isolation/limits, reviewed image availability and Docker-equivalent
two-project Python/R results remain NOT RUN. The local zero-workload foundation
does not close those gates. No production, inference, real student data, quota,
default routing, purchase or existing contributor assignment changes are authorized.
