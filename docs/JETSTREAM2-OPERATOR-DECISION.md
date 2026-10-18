# Jetstream2 operator decision packet

**BLOCKED — independent-host cleanup is an unresolved engineering requirement;
no live test is authorized.** The accepted journal and process locks operate on
one host. No supported protocol gives a separate rescue host current durable
cleanup intent before provider effects. A documented CPU rate supports a proposed
one-worker experiment, but allocation access, exact resource pins and a reviewed
live runner are also missing. Continue the accepted local Docker baseline.

Prepared for Greg Chism on October 17, 2026, using accepted Outfitter
`273bb0148fead33c91fcb92e0cc9e0bacdb929c2`, including the native journal and cleanup
changes from PR #8. The [existing proposed manifest](manifests/jetstream2-live-test.proposed.json)
remains `PROPOSED_NOT_AUTHORIZED`. The [structured decision record](manifests/jetstream2-operator-decision.json)
separates published terms, unknown deployment facts and proposed limits.
The [independent rescue design candidate](JETSTREAM2-RESCUE-DESIGN-CANDIDATE.md)
is saved for offline review. It is not implemented or accepted as closing the
cleanup requirement, particularly for unknown and late-created effects.

## Decisions for Greg

1. **Defer or nominate an existing allocation.** Deferral needs no purchase,
   resource change or schedule. To continue preparation, select an approved
   Jetstream2 CPU allocation and confirm permission to use it for this test.
   `SOC250004` and Ben's approved allocation remain candidates, not entitlements.
2. **Name the operators and existing cleanup capacity.** Confirm the primary
   operator, independent observer/rescue operator and an existing approved
   cleanup host outside the requester's and worker's failure domains. If no
   suitable host is available, defer or request a separately priced proposal;
   no new host or subscription is included here.
3. **After the technical gates pass, approve a frozen manifest and window.**
   The proposed allowance is 1 SU for one attempt and at most ten minutes.
   Approval must explicitly address the possibility of continued charges during
   provider/API or supervisor failure. If a guaranteed provider-enforced hard cap
   is required, this implementation cannot yet meet that requirement.

These decisions do not replace the missing evidence below. There is no runnable
launch request to approve yet, and no automatic launch when a date arrives.

## Published provider terms

The following primary documentation was checked October 17, 2026. These are
provider terms, not observations of the selected allocation.

| Subject | Verified published term | Primary source |
| --- | --- | --- |
| CPU candidate | `m3.tiny`: 1 vCPU, 3 GB RAM, 20 GB local disk, 1 SU/hour | [Instance flavors](https://docs.jetstream-cloud.org/general/vmsizes/) |
| Units and reporting | 1 ACCESS credit exchanges for 1 Jetstream2 SU; reporting can lag 12–24 hours | [ACCESS credits and usage](https://docs.jetstream-cloud.org/general/access/) |
| Inactive states | Stopped uses 50% of the normal rate; suspended 75%; shelved 0%. Shutdown is not deletion | [Usage rules](https://docs.jetstream-cloud.org/general/access/) |
| Allocation and quota | Valid allocation membership is required; quotas are shared across its users. CPU, GPU and large-memory entitlements are separate | [Allocations](https://docs.jetstream-cloud.org/alloc/overview/), [quotas](https://docs.jetstream-cloud.org/general/quotas/), [resources](https://docs.jetstream-cloud.org/general/resources/) |
| Image and credentials | `Featured-Ubuntu24` exists as a maintained image name; images change. Application credentials are allocation-specific and private | [Featured images](https://docs.jetstream-cloud.org/general/featured/), [authentication](https://docs.jetstream-cloud.org/ui/cli/auth/) |
| Data | Protected data requires specific institutional authorization; this proposal uses public/synthetic data only | [Provider acceptable-use policy](https://docs.jetstream-cloud.org/general/policies/) |

The published rate supports a **nominal estimate** of `1 × 600 / 3600 =
0.166667 SU`. Outfitter rounds its minimum reservation calculation upward to
**167 milli-SU**, where 1,000 milli-SU = 1 SU. The proposed reservation is instead
**1,000 milli-SU (1 SU)**, providing contingency within the local allowance.
These are distinct from a provider bill, a settlement, or an ACCESS exchange.
No dollar price is inferred, and no credits have been exchanged.

**The provider-enforced maximum charge is unknown.** Outfitter can refuse new
admission and retain liability; it cannot force deletion while the provider API
is unavailable. The 1 SU allowance must not be described as a guaranteed maximum
charge. Reporting lag also prevents an immediately visible balance from proving
that the final charge is settled.

## Proposed finite experiment

| Item | Proposal requiring approval |
| --- | --- |
| Worker | One image-backed `m3.tiny`; concurrency 1; one creation attempt; no retry after an ambiguous response |
| Lifetime and allowance | At most 600 seconds; reserve 1,000 milli-SU; aggregate native account allowance 1,000 milli-SU |
| Storage and network | 20 GB ephemeral root; one explicitly owned port on an existing private network; no new volumes, snapshots, floating IPs or networks |
| Workload | Reviewed synthetic `print(6 * 7)` with expected output `42`; private access/transfer and guest execution are not yet implemented in the live seam |
| Candidate window | October 20, 2026, 10:00–10:10 a.m. America/Los_Angeles; still a draft, not scheduled |
| Cleanup | Separate approved supervisor attempts bounded cleanup every 15 seconds after expiry; fresh observer verifies exact server/port absence and empty reservation inventory |
| Escalation | At 10:15 a.m., unresolved cleanup goes to the named rescue operator; no new admissions. The proposed 11:00 a.m. decision-window end does not stop billing |

A future runner must reject an expired or altered manifest and bound its request
deadline by both the 600-second limit and the approved absolute expiry. The
existing journal refuses a request whose deadline exceeds the permit expiry;
it does not itself enforce the proposed 10:00 start time or a one-attempt total
after settlement. Those are runner requirements, not current features.

## Facts still required

| Fact | Current state | Evidence needed before launch |
| --- | --- | --- |
| Allocation ID and CPU entitlement | Unselected and unverified | Operator/PI confirmation and current allocation record |
| Remaining SUs and expiry | Unknown | Dated allocation balance, expiry and outstanding usage review |
| OpenStack project, site and region | Unknown; IU is only a preference | Selected allocation's project and exact regional service catalog |
| Image UUID, checksum and suitability | Unknown | Active image UUID plus SHA-256/SHA-512; CPU platform and image minimum RAM/disk compatibility |
| Flavor UUID and observed resources | Unknown | Selected region's flavor matching the published `m3.tiny` dimensions |
| Network, security group and private route | Unknown | Existing IDs, rule review and approved workload/observer reachability |
| Quota and current capacity | Unknown | Fresh free instance/core/RAM quota plus separate current capacity evidence; quota alone is insufficient |
| Operators and rescue coverage | Greg is proposed; nobody confirmed | Named primary and independent observer/rescue coverage for the window |
| Independent cleanup host | Unknown | Approved existing host, separate failure domain and credential custody |
| Durable cleanup intent on that host | Unsupported across hosts today | Reviewed pre-effect handoff and failure rehearsal, with one accounting authority and explicit treatment of late effects |
| Live runner and settlement report | Missing | Reviewed finite runner, workload receipt, provider usage evidence and explicit settlement procedure |

The read-only CLI is available, but a live preflight was **NOT RUN** because the
required selected configuration is absent. No credentials were searched for or
printed. The command authenticates with Keystone, then GETs the pinned flavor,
image, network, security group and compute quota. It performs no resource create
or delete and always reports `provisioning_enabled:false`. Its result cannot
establish allocation balance, CPU entitlement, host capacity, image boot
compatibility, effective firewall policy or independent cleanup readiness.
The existing mock preflight/authentication failure tests passed in this review;
they do not establish any of the live facts above.

## Cleanup ownership and remaining offline work

Accepted code reserves native SUs before resource creation, records random
reservation identity and absolute deadlines, and prevents another unresolved
native request from being admitted. Cleanup checks persisted ownership and pins,
refuses incomplete/duplicate/foreign inventories and unexpected storage, and
deletes only the owned server and port. Verified deletion still does not release
unknown charges. Ambiguous effects cannot currently be settled even after later
discovery. See the [native accounting contract](JETSTREAM2-NATIVE-ACCOUNTING.md).

Only these source-backed gaps warrant offline work for this experiment:

| Gap | Accepted source evidence | Bounded offline deliverable |
| --- | --- | --- |
| Finite operator runner and workload handoff | [CLI](../cmd/outfitter/main.go) exposes preflight/reaping, not permit configuration or creation; [worker](../internal/provider/jetstream2/worker.go) creates infrastructure without guest transfer/execution | Draft the permit/manifest contract first, then fixture-only orchestration tests for start/expiry, exactly one attempt, private recipe execution and retained uncertain receipts. Keep live dispatch disabled |
| Independent host intent and alerts | [Recovery](../internal/ledger/recovery.go) and [journal](../internal/provider/jetstream2/journal.go) use a private local filesystem and process locks; [reaper](../internal/provider/jetstream2/supervisor.go) is a single sweep | Review the [immutable rescue scope candidate](JETSTREAM2-RESCUE-DESIGN-CANDIDATE.md) and rehearse its failure cases offline. A copied old checkpoint or unreviewed shared filesystem is insufficient; the candidate does not yet close the late-effect gap |
| Preflight scope for the selected image/network | [Preflight](../internal/provider/jetstream2/client.go) checks image status/hash and group ownership but not image minimum resources, platform or security-group rules | Add bounded read-only checks and fixture failures once the required compatibility/rule policy is specified; still keep capacity and entitlement separate |
| Inventory size, conditional on the selected project | [Discovery](../internal/provider/jetstream2/supervisor.go) refuses pagination or over 1,000 records | If the selected project's inventory requires it, implement bounded pagination with loop/completeness/ownership tests before qualification; otherwise retain the explicit refusal |
| Ambiguous settlement | [Settlement](../internal/ledger/native.go) rejects ambiguous effects | Define a separately reviewed provider-evidence reconciliation protocol; retain the reservation until then. Do not invent a refund or automatically clear ambiguity |

No optional Kubernetes system is needed. The local Cairn consumer does not supply
a Jetstream2 runner, and production Cairn wiring remains outside this proposal.
Ben Markwart's [issues 1–5](https://github.com/EduCloud-Ecosystem/outfitter/issues?q=is%3Aissue+assignee%3Abmarkwart)
remain open and assigned to him; this packet does not reassign or close them.
Issue #5's broader live lifecycle and independent second-operator acceptance
remain separate gates.
