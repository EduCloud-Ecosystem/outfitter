# 19 — Disabled offline Kubernetes single-Pod attempt

P2-A slice, based on EduCloud backlog `8dd3754c971dff2b38936e1d9af2c2c81d3ecad5`.
This is a synthetic protocol implementation with an injected API, not a live
backend. No Kubernetes client, credentials, CLI selection, engine operation,
workload, quota change, or reuse of a previous allowance is introduced.

## Durable identity before effects

An optional request placement binds `kubernetes-offline`, a cluster identity
digest, namespace name and UID, and a unique attempt ID. It participates in the
existing request hash and CPU-second reservation in the existing ledger. The
first such reservation uses ledger version 5; older records keep their existing
bytes and meaning. Unbound records remain the legacy local path. A broker or
adapter must refuse a foreign placement before any resource action. Consumer
requests cannot select this prototype. Version-5 checkpoint/restore is refused;
reopening the same existing state for status and owned cleanup is supported.

Creation is a one-shot durable claim within the reservation. A duplicate run is
status-only, including after interruption or restart; a changed request, backend,
cluster, namespace UID or attempt identity conflicts. No replacement Pod or
controller is created. Existing placement records, identity, claimed dispatch,
assigned Pod UID and charges cannot be removed, rebound or reduced by updates.

## Offline API contract

The injected API models exactly one named Pod, one ordinary container,
`restartPolicy: Never`, no init/sidecar containers and no controller. It receives
the bound cluster/namespace identity on every operation, and must refuse an
identity change. Pod observations must match that identity, name, request,
attempt, owner and request digest. The first owned UID is durably retained;
different UIDs are refused, including during deletion and output observation.
Deletion is conditional on the retained UID. Cancellation/context expiry and the
absolute reservation deadline are checked again immediately before dispatch.

CPU/memory/PID/output/lifetime caps are model inputs, not claims that a Kubernetes
Pod manifest enforces them. Input/recipe directories remain private snapshots;
artifact transfer and storage design are NOT RUN. The API has no implementation
other than test fakes; it does not accept kubeconfig, tokens or network endpoints.

Pod creation can itself start execution. A durable dispatch claim records that
possibility before the call; a missing/ambiguous response never proves that work
did not run. A late owned Pod may be removed after restart, but ambiguity remains
an unresolved full reservation forever in this slice. No retry or refund follows
absence, completion, cleanup failure, node/API loss or restart.

## Accounting and cleanup

Reuse the existing project allowance, active-record limits, bounded combined
output capture and diagnostic serialization. Successful observed completion and
verified cleanup charge the entire existing CPU-second reservation. The value is
`estimated_upper_bound`, not measured CPU, native units, money or cluster overhead.
Unknown outcomes retain the full reservation and liability.

API-object absence alone cannot certify workload termination. The fake API must
also provide an explicit UID-bound stopped-workload observation. This stronger
observation is deliberately an unimplemented live gate; API deletion or a force
delete cannot substitute for it. Missing/failed stop evidence leaves liability
open even when the object is absent. Foreign objects are never deleted or read
for output. Polling/waiting is bounded by the request context; cleanup by the
existing broker cleanup deadline. An independent real cleanup host is NOT RUN.

## Acceptance and limits

Tests use temporary synthetic state and an injected fake API. Cover reservation
before effects, duplicates/status-only, changed identities, cancellation/expiry
before dispatch, ambiguous create/late appearance, restart, foreign UID,
bounded output, missing stop evidence, cleanup failure, no refund/replay and
preservation of Docker/native/consumer behavior and legacy request encoding.
Run race tests and static checks. Existing CI Docker checks remain separate;
no local engine or workload is authorized by this slice. Root owns independent
review and merge. Kubernetes runtime qualification/adoption remains DEFERRED.

Semantic references checked October 18, 2026: [Pod lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/),
[Pod API](https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/), and
[UID deletion preconditions](https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/types.go).
These motivate the fake protocol; they do not qualify a live cluster.
