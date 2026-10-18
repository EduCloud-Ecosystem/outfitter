# 14 — Offline independent rescue prototype

This implements a limited, simulated prerequisite to the blocked
[rescue candidate](../docs/JETSTREAM2-RESCUE-DESIGN-CANDIDATE.md). It does not close
live qualification, provide terminal late-effect guarantees, establish provider
IAM or qualify actual independent hosts. Docker and normal native accounting
remain unchanged. No new command, default transport, credential discovery,
deployment service, executor takeover or second-host provisioning is added.

## Acceptance of the design record

PR #10 at `3085e8f33350b6ce558a254cfdb7bc3de7f4c1b6` was independently reviewed by
the coordinating chat's `research_broker` agent. Its reported conclusion was no
blocker to merging a DESIGN-ONLY/BLOCKED record, not implementation approval.
Final-head Broker verification run `39096489315` passed. PR #10 merged as
`afc4fe82ee368261d7662b380bf9f611b31aa182`. The implementation below requires its
own independent review and final-head CI.

## One accounting authority, one cleanup executor

An immutable native policy opts into `offline-rescue-v1`, pins B's observer ID,
the source authority and separately provisioned Ed25519 public trust keys for A
and B. These values are copied into and validated against each durable request.
The original A ledger remains the only admission/accounting authority. B has no
ledger reference, budget/admission/settlement operation or authority activation.
Even a held/restored A must refuse teardown and reaping for B-owned requests.
Changing the cleanup assignment or keys after reservation is forbidden.

The opt-in client requires an explicitly injected HTTPS fixture transport and a
literal loopback IP origin. Every request, including authentication and service
catalog endpoints, is constrained to that origin. Nil/default transports and
non-loopback origins are rejected. The ordinary constructor cannot enable this
mode. B permits only GET and DELETE resource operations; token POST is the sole
exception. No command can construct or deploy this observer.

## Durable phases in the actual creator

`CreateWorker`, rather than a caller wrapper, enforces this sequence:

1. Reserve durably on A; persist port-attempt intent and the signed port scope.
2. B authenticates, validates and fsyncs the immutable scope and its directory;
   it then signs, persists and fsyncs its ACK and directory before returning it.
3. A authenticates the exact ACK, persists it, rechecks expiry, and makes at most
   one port POST. A repeat invocation does not resume or retry a partially
   dispatched request.
4. Only a successful, durably saved port ID allows server intent. The signed
   server scope binds that ID and the exact signed port envelope's SHA-256.
5. Repeat the durable authenticated ACK protocol before the single server POST.

Any missing, lost, conflicting, forged, expired or non-durable ACK stops the
corresponding POST. Failure of A's save also stops it. Intent without a POST is
conservatively unresolved. An ambiguous port cannot authorize a server phase.
Both scopes bind source authority/account, request/owner/reservation, immutable
configuration, request hash, reservation amount, creation time/expiry, exact
resource name/description/metadata, observer and protocol version. No secrets or
workload data are included.

## Signed bytes and local durability

All protocol objects are fixed Go structs encoded with `encoding/json.Marshal`
without whitespace. Decoding rejects unknown fields, trailing data, oversized
objects and any bytes that differ from re-encoding; this also rejects duplicate
keys and alternate representations. An envelope contains a kind, base64 payload
bytes and base64 Ed25519 signature. Signature input is exactly
`outfitter-offline-rescue-v1` + NUL + kind + NUL + payload bytes. Envelope bytes
are canonical too. Trust keys come from caller-provisioned fixture configuration,
never from a received scope. ACKs bind the entire envelope digest, phase,
reservation, observer, parent digest and acceptance time.

B uses one private, path-bound directory for one retained reservation, two
immutable scopes and their ACKs. No automatic garbage collection or copied-store
recovery exists. A separately configured expected absolute directory and B key
must match its signed binding. This cannot detect a malicious identical-path
copy onto another host; cross-host fencing remains unqualified.

Use bounded regular-file reads, no-follow/nonblocking opens, descriptor-based
directory access, private permissions, exclusive creation, file fsync and
directory fsync. Reject special files, symlinks, corruption, unexpected entries,
conflicting bytes and quota excess. Fsync errors never yield ACKs. Recovering an
already identical file must fsync it and its directory again before ACK. Local
process locks serialize acceptance and sweeps; they are not distributed locks.

Observation storage has a fixed separate bound and reserves capacity for scope,
ACK and binding files. When full, preserve all unresolved scope and refuse further
cleanup until reviewed intervention; never delete evidence to claim success.
Each sweep reserves durable observation intent before destructive calls and
returns a signed outcome only after durable recording. A lost final receipt is
unknown, not proof that DELETE was never sent.

## Cleanup and clocks

B rejects phase acceptance at/after expiry. A checks its own clock and deadline
before POST and bounds HTTP contexts to it. ACK clock disagreement beyond a small
fixed tolerance refuses dispatch. Persisted clock high-water evidence rejects
rollback across B restart. These are fixture controls, not proof of synchronized
host clocks; forward jumps or lost clock evidence cannot qualify a live host.

After expiry B discovers only the exact scope. Both discovery and fresh GET
immediately before DELETE require present matching ID/name/project/image/flavor/
owner/request/reservation/deadline and explicit non-null empty server volumes.
Ports require present matching ID/name/project/network/description/security-group
pins and a present non-null device ID (empty or the owned server). Missing lists,
pagination, foreign matches and duplicate matches refuse all deletion. Port-only
scope cannot authorize a server. Delete the server first, observe its absence,
then freshly validate and delete the port and observe absence. GET and DELETE
are not atomic; provider mutation between them remains an explicit limitation.

An empty inventory, 404 or ended watch window is only a time-bounded observation.
Preserve scopes and liabilities indefinitely within the fixed retention bound;
later sweeps remain possible for newly visible eligible resources. Never produce
a terminal cleanup/settlement assertion. API errors, corrupt state or duplicates
remain unresolved. B may act on intact signed scope while A is held, corrupt or
unavailable; it cannot rebuild or modify A's accounting.

Receipts are signed observational records only, binding scope digests, observer,
reservation, sequence, time, outcome and validated IDs. Provide bounded validation,
but defer ledger ingestion and all settlement. No receipt changes a budget.

## Required offline evidence

Use separate processes and isolated A/B directories, fixture-only keys and an
explicitly trusted loopback TLS simulated provider. Test crashes at intent,
acceptance, ACK and POST boundaries; ACK loss/conflict/forgery and disk/fsync
failure; concurrent repeats; lost/late port blocking server phase; lost/late
server after absence/window; changing/foreign/duplicate pins; missing/null
volumes/device ID and incomplete lists; B restart with A corrupt/held/unavailable;
A cleanup refusal; clock skew/rollback/expiry races; forged receipts, FIFOs and
quota exhaustion. Audit B's resource methods and unchanged A accounting.

Report the actually exercised boundaries and all limitations in the implementation
record. Existing failed controls remain in the test suite. No source of offline
evidence promotes live readiness or changes Ben's issue assignments.
