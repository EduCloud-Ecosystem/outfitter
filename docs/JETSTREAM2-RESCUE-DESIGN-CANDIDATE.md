# Independent Jetstream2 rescue: design candidate

**DESIGN ONLY — not implemented, not accepted for live qualification.** The
smallest candidate worth testing is an immutable cleanup manifest delivered to
an independent host before each possible creation effect. That host may observe
and delete exactly scoped resources after expiry, but cannot admit work or write
the budget ledger. Evidence returns to the original authority for reconciliation.

This can preserve cleanup intent through loss of the original host. It does
**not yet safely close all unknown-effect cases**: the accepted provider contract
does not establish a maximum delay between accepting a creation request and
making its effect visible. An empty inventory or a finite observation window
therefore cannot prove final cleanup. Reject this candidate as a complete
solution to the live independent-host gate until that gap has a reviewed answer.
The [operator packet](JETSTREAM2-OPERATOR-DECISION.md) remains **BLOCKED**.

Prepared October 17, 2026 against accepted Outfitter
`273bb0148fead33c91fcb92e0cc9e0bacdb929c2`. This is a proposed design and offline
test plan, not a new provisioning contract. Any accepted behavior must first be
specified under `specs/`. No distributed database, shared writable ledger,
second-host provisioning or Kubernetes system is proposed.

## Preserve the existing controls

The accepted [journal](../internal/provider/jetstream2/journal.go) binds the
request, owner, random reservation identity, project/configuration, native-unit
reservation and absolute deadline before effects. The
[creator](../internal/provider/jetstream2/worker.go) durably records
`PortAttempted` before the port POST, and `ServerAttempted` plus the known port ID
before the server POST. A missing response does not authorize a retry.

The [supervisor](../internal/provider/jetstream2/supervisor.go) discovers resources
using exact identity, corroborates discoveries with attempted-effect records,
refuses duplicate, foreign or incomplete inventory, and rejects unknown or
nonempty attached storage. Cleanup runs under local process/file locks. Those
locks and a stale checkpoint do not coordinate a separate host.

The candidate must preserve these distinctions:

| Accepted control | Required treatment in the candidate |
| --- | --- |
| Reservation precedes effects | Original ledger remains the sole admission and accounting authority; export only after durable reservation |
| Separate port/server attempt records | Use two immutable phases; a blanket permission for any future server is too broad |
| Exact ownership and reservation token | Require the complete matching identity and configuration, never a name prefix or tag alone |
| Known port required before server attempt | Server phase includes that exact port ID and the hash of the port phase |
| Duplicate or incomplete inventory refuses cleanup | Preserve refusal; no automatic deletion of multiple possible matches |
| Missing/null or nonempty volume inventory refuses cleanup | Preserve explicit empty-volume requirement and do not add volume deletion |
| Late or lost response retains uncertainty | A manifest means an effect may exist; it does not prove execution, absence or finality |
| Corrupt state blocks operations | Invalid rescue data causes refusal and an alert; valid rescue data cannot repair accounting state |
| Deletion does not settle charges | Reconciliation evidence cannot release a reservation or erase ambiguity |

The comparison is against the pinned accepted commit above. The current
same-host fault coverage is in [journal tests](../internal/provider/jetstream2/journal_test.go),
[client tests](../internal/provider/jetstream2/client_test.go) and
[review regressions](../internal/provider/jetstream2/review_regressions_test.go).
It is not evidence of cross-host acceptance.

## Minimum handoff protocol to test offline

Call the original ledger host **A** and the proposed cleanup host **B**. B stores
immutable manifests and append-only observation receipts in its own bounded,
private files. It receives no ledger checkpoint capable of admission, no budget
writer and no authority-activation operation.

1. **Reserve on A.** Admit exactly one request using the existing durable
   reservation rules. Freeze its absolute expiry and approved configuration.
   Designate B as the only automated cleanup executor for this candidate mode.
2. **Prepare the port phase.** A records the attempted-effect intent, writes an
   immutable port manifest, and delivers it to B. B authenticates and validates
   it, durably stores it, and acknowledges its exact digest and identity. A
   durably records that acknowledgement and rechecks its deadline before the
   single port POST. Missing or uncertain acknowledgement means no POST.
3. **Prepare the server phase.** Only after A durably records a successful port
   response may it record server intent and export a second immutable manifest.
   This includes the exact port ID and parent manifest digest. B must persist
   and acknowledge it before the single server POST. An ambiguous port response
   cannot advance to this phase. Any failure retains the reservation.
4. **Observe and clean after expiry.** B can operate from its accepted manifests
   while A is unavailable. B serializes phases and cleanup locally, rejects new
   phase acceptance after expiry, and never extends expiry. A must still enforce
   its own deadline immediately before every creation call. Clock uncertainty
   blocks dispatch; a reviewed clock policy is an offline design prerequisite.
5. **Return evidence.** B retains authenticated, idempotent receipts until A can
   collect them. A validates their binding and records observations. Missing A
   does not authorize B to create a replacement ledger or settle charges.

Both phases must bind: schema/version and phase; ledger authority and account;
request and owner; random reservation identity; original request/configuration
hashes; project and region; exact network, image, flavor and security-group pins;
expected resource name and port description/server ownership metadata; creation
time and absolute expiry; observer identity; and a reviewed implementation pin.
There is at most one port and one server in scope. The server phase additionally
binds the known port ID and parent digest. No credentials or workload data belong
in these manifests.

The exact encoding, size limits, filesystem durability and authenticated
delivery/acknowledgement format require specification. A digest detects a change
but does not authenticate the sender. One candidate is a detached signature over
exact manifest bytes, verified with an independently provisioned trust key; a
key supplied by the manifest itself cannot establish trust. Acknowledgements
must attest durable storage, not merely receipt. Replayed requests cannot gain
new scope, identity, deadlines or resource counts. Rejected or conflicting
phases block dispatch and remain visible to the operator.

Two phases are necessary because a single advance authorization for both effects
would weaken the current attempt checks. A crash between recording intent and
POST may leave a manifest for an effect that never happened. The conservative
result is unresolved intent, never permission to retry or infer zero charge.

## Cleanup authority and evidence

B's application interface permits authentication, bounded inventory/inspection,
and deletion of the uniquely matched server followed by the owned port. It
exposes no creation, retry, admission, budget, settlement or authority-transfer
operation. Whether Jetstream2 can enforce equivalent credential restrictions is
**unverified**; an application method allowlist is not provider-enforced IAM.
Credential scope and custody remain qualification requirements.

Before deletion, B must freshly verify the exact project, reservation/owner/
request metadata, resource name, configuration and explicit empty-volume
inventory. The port must match the exact network and description and be
unattached or attached to the uniquely matched server. A server discovered from
only a port-phase manifest is outside scope and triggers refusal. A second
matching resource, changed identity, foreign attachment, partial inventory,
unexpected storage or API error retains uncertainty and triggers an alert.
No automatic name-only or administrator-wide cleanup is allowed.

The candidate needs one cleanup executor per request. A's existing automatic
reaper/teardown cannot remain independently active for these requests without a
reviewed coordination rule; local locks do not fence B. Initially, propose B as
that sole executor and A as a status/reconciliation client. This requires an
explicit future mode, not a claim about today's CLI. B failure stops automated
cleanup and escalates to the named operator. Unreviewed failover or another
ledger copy is not an acceptable recovery mechanism.

Receipts should bind the manifest digest and reservation identity, observer,
sequence/receipt ID, UTC observation time, API operation/status, validated
resource IDs, completeness/ownership result and a digest of sanitized evidence.
They must distinguish `UNKNOWN`, `MATCH_OBSERVED`, `DELETE_REQUESTED` and
`OBSERVED_ABSENT`; the last is a time-bounded observation, not a final settlement.
Store no tokens, secrets or workload content. Duplicate receipts are idempotent;
conflicting, missing or out-of-order evidence cannot silently advance state.

Existing [native observations and settlement](../internal/ledger/native.go)
preserve the reservation and refuse settlement while effects are ambiguous. They
do not currently implement authenticated rescue receipt ingestion. A future
ingestion path must retain those protections. If A is corrupt or held after
recovery, B may still clean resources within an intact authenticated manifest,
but accounting remains held until the original authority is safely reconciled.

## Unknown effects are the unresolved limit

Three cases must remain distinct:

| Case | Safe candidate outcome |
| --- | --- |
| Response lost; one fully matching resource appears later | Observe it, validate its phase/ownership, then attempt bounded cleanup; retain accounting ambiguity |
| No matching resource appears in complete inventory | Record only observed absence; retain intent and liability because delayed creation remains possible |
| Multiple matches, wrong ownership, incomplete inventory or unreadable manifest | Refuse destructive action and escalate with the uncertainty visible |

Cancellation of A's HTTP request does not establish cancellation of a provider
effect. Expiry authorizes cleanup; it does not itself stop an in-flight provider
operation. Deleting a known port is not documented here as a cancellation fence
for an unknown server creation. A later server or port can therefore appear
after an earlier absence observation or after B's finite watch window ends.

Bound each API request and sweep, preserve manifests beyond the watch window,
and escalate unresolved intent at its end. Never turn timeout, lost credentials,
host loss or empty inventory into `CLEANUP_COMPLETE`, a refund or a new admission.
These rules preserve uncertainty but cannot guarantee eventual cleanup or cap
provider charges. They are insufficient to close the live engineering blocker.

A complete design needs supported provider evidence of terminal request outcome,
a validated cancellation/fencing mechanism, or another reviewed way to cover
late effects without falsely closing the request. No such guarantee was
established in this review. Merely asking Greg to accept a 1 SU allowance does
not supply it. Keep the launch blocked while offline work evaluates this limit.

## Offline acceptance plan — not run

Use a simulated provider and two isolated local state directories/processes.
This tests failure boundaries without provisioning a second host. A later real
independent-host rehearsal is a separate gate, requiring authorization.

| Fault or boundary | Required assertion |
| --- | --- |
| Missing/lost/corrupt acknowledgement | No corresponding creation POST; any replay resolves to the same immutable scope |
| A killed after B acknowledges, before or after POST | B recovers intent without A; never retries creation or releases liability |
| B killed before acknowledgement or after durable acceptance | A refuses dispatch without acknowledgement; B restart recovers exactly accepted scope |
| Lost port response; inventory initially empty then late port appears | No server phase; absence stays unknown; unique validated late port can be cleaned |
| Lost server response with accepted server phase | Known port and exact server metadata remain required; ambiguous absence stays unresolved |
| Provider effect appears after deadline or watch window | No false final cleanup, refund or new admission; durable alert/evidence remains |
| Duplicate effects, foreign attachment or mismatched pins | No destructive action; explicit unresolved outcome |
| Missing/null volumes or paginated/truncated inventory | Refuse cleanup as the accepted implementation does |
| Truncated file, wrong signature/key/authority, changed expiry or replayed token | No dispatch or deletion from invalid data; no automatic repair |
| Phase acceptance races expiry or cleanup; clock uncertainty | No late dispatch or scope extension; all uncertainty remains visible |
| A's local cleanup attempted while B owns cleanup | Refuse competing executor; no reliance on separate hosts sharing a file lock |
| A's ledger corrupt or held; B has valid immutable scope | Scoped cleanup may proceed; no accounting reconstruction, activation or settlement |
| Duplicate/conflicting/out-of-order receipts | Idempotent valid observations; conflict cannot change budget, identity or ambiguity |
| API method audit | Observer sends no resource-creation POST, PUT or PATCH; auth POST is separately counted |

Passing these tests would justify further review of the limited rescue mechanism.
It would not prove live capacity, allocation permission, provider hard caps,
independent-host reliability, terminal outcomes or second-operator readiness.
Ben's assignments and issue #5's wider acceptance requirements remain unchanged.
