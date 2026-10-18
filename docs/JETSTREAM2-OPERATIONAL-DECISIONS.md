# Jetstream2 operational decisions after the offline experiment

**BLOCKED. Recommend one private CPU worker on an existing approved allocation,
with restricted SSH execution from an existing host and independent cleanup from
a second existing host.** Greg can select this architecture and nominate its
operators and resources. Software and operational qualification must still be
completed before a separate live request is executable. No credential access,
provisioning, deployment or live testing is approved by this packet.

Prepared for Greg Chism on October 17, 2026, against merged PR #13,
`2ce00f4950122890a6bcb792ee29e51127ad76c2`. The
[structured supplement](manifests/jetstream2-operational-decisions.blocked.json)
is a decision record, not a permit, configuration or runnable manifest. The
[original proposal](manifests/jetstream2-live-test.proposed.json),
[earlier operator packet](JETSTREAM2-OPERATOR-DECISION.md), design candidate and
historical evidence remain unchanged. Their implementation descriptions describe
their recorded revisions; the current boundary is below.

## Decisions Greg can make now

| Selection | Recommendation | Information to supply |
| --- | --- | --- |
| Architecture or deferral | Continue design of the private SSH route below, one `m3.tiny`, candidate `Featured-Ubuntu24`, synthetic workload only. Defer if a provider-enforced hard charge cap is mandatory | Select this route or defer; architecture approval alone permits no live operation |
| Allocation and accountable operator | Use an existing CPU allocation with PI permission. `SOC250004` or Ben's approved allocation are candidates only | Allocation nomination, permission owner and primary operator; no secrets or presumed entitlement |
| Existing hosts and coverage | Host A owns admission and private guest access; independent host B owns automated cleanup. Name a separate rescue operator and credential custodian | Candidate A/B hosts, approved private route, failure domains, operators and coverage. If unavailable, request a priced alternative before adding infrastructure |

Exact provider UUIDs, compatibility fields and service endpoints are evidence to
collect after authorization, not values Greg should guess. The calendar window
and explicit residual-cost acceptance belong in a later frozen live packet.
Ben's [issues 1–5](https://github.com/EduCloud-Ecosystem/outfitter/issues?q=is%3Aissue+assignee%3Abmarkwart)
were still open and assigned to `bmarkwart` on October 17; assignments and issue
acceptance remain unchanged. No Waypoint, Portage, Cairn or pilot deployment is
enabled here.

## Recommended guest route and alternatives

The proposed route is **A to the worker's fixed private address over SSH**, using
existing approved routing. B needs independent access to the provider management
APIs and the authenticated A–B handoff, not guest login. The current client uses
HTTPS service-catalog endpoints; a private guest does not make those management
endpoints private. No worker floating IP, public guest endpoint, new network,
volume or snapshot is included.

1. **Bootstrap a restricted identity.** Pin the exact Ubuntu image and a reviewed,
   nonsecret cloud-init document. It would install a fixed, root-owned receiver
   and create a dedicated non-root account without sudo, password login, agent
   forwarding, port forwarding or a terminal. Admit one per-experiment public
   login key with a forced command. The private login key stays on A; provider
   credentials and rescue signing keys never enter the guest, user-data,
   resource metadata or workload. No package download at boot is proposed;
   required Python, SSH and cloud-init versions must be present and qualified.
2. **Authenticate this boot.** The guest generates a fresh host key and retains
   its private part only in the guest. Only the public host key or fingerprint
   may reach the console. Before A connects, the operator obtains that public
   fingerprint through the approved
   authenticated provider console for the exact project/server and binds it to
   the reservation and private address. Require exact host-key checking. Missing
   or conflicting evidence stops execution; do not accept an unverified first
   connection. Console availability, output and access policy need qualification.
3. **Transfer and execute once.** Use a bounded SSH input stream to that fixed
   receiver, carrying only the pinned synthetic recipe and request identity.
   Proposed bounds are 16 KiB input, 30 seconds of execution within the remaining
   worker lifetime, and 4 KiB combined stdout/stderr. The receiver must verify
   the recipe digest, durably consume its one execution attempt before launch,
   and return a receipt bound to the request and reservation. No arbitrary
   shell, file-tree upload, dependency resolution or second execution after
   interruption. A lost receipt stays unknown. These are engineering targets,
   not current features.
4. **Verify the result separately.** The only recipe is `print(6 * 7)` with
   expected result `42`. Record output truncation, timeout and missing receipts
   as failures or unknowns. A successful result does not prove cleanup, settle
   SUs, qualify hostile student workloads or authorize another attempt. B retains
   cleanup responsibility even when guest access fails.

These mechanisms are supported in principle by the upstream
[cloud-init module reference](https://docs.cloud-init.io/en/latest/reference/modules.html#keys-to-console)
and [OpenSSH restrictions](https://man.openbsd.org/sshd.8#AUTHORIZED_KEYS_FILE_FORMAT),
checked October 17, 2026. They do not establish what the selected image or
Jetstream2 deployment actually supports. Bootstrap code, SSH transport and a
guest runner are **not added** by this documentation change.

| Alternative | Tradeoff and disposition |
| --- | --- |
| Recommended private SSH with nonsecret bootstrap | Reuses familiar tools and avoids a new service; needs a qualified private route, host identity and one-shot receiver |
| Prebuilt custom image with receiver | Reduces boot-time setup but adds an image build, provenance, patching and storage lifecycle; only reconsider if the maintained image cannot meet the route |
| Guest-initiated HTTPS job service | Avoids inbound guest access but introduces an authenticated queue/service, guest enrollment and another failure domain; defer |
| New public bastion, floating IP or network tunnel | Changes exposure, infrastructure and possibly cost; outside this proposal, requiring a new decision |

## Allocation and exact resource evidence

Public primary documentation was checked **October 17, 2026**. It establishes
candidate terms, not allocation access or available capacity. A future evidence
record must pin collection time, selected project/region, source, digest and
reviewed freshness limit. Missing or stale facts refuse admission.

| Subject | Published basis | Exact facts still required |
| --- | --- | --- |
| Account and CPU allocation | Valid allocation membership is required; resource quotas are shared across users. [Allocations](https://docs.jetstream-cloud.org/alloc/overview/), [quotas](https://docs.jetstream-cloud.org/general/quotas/) | Selected allocation and PI permission; membership, CPU entitlement, remaining SUs, expiry and outstanding usage; exact OpenStack project, region, auth URL and service catalog. IU remains a preference only |
| Flavor | `m3.tiny`: 1 vCPU, 3 GB RAM, 20 GB local storage, 1 SU/hour. [Flavor table](https://docs.jetstream-cloud.org/general/vmsizes/) | Region-specific flavor ID, vCPUs, RAM in MiB, root and extra ephemeral disk in GiB, and image minima. Do not turn the table's 20 GB into an assumed extra ephemeral disk or silently convert units |
| Image | `Featured-Ubuntu24` is maintained and changes over time. [Featured images](https://docs.jetstream-cloud.org/general/featured/) | Active, accessible UUID and SHA-256/SHA-512; architecture, firmware, formats, minimum RAM/disk and qualified installed packages. No mutable-name fallback or automatic custom image |
| Network and rules | A guest without a floating IP is not internet-reachable by that route; security groups still govern traffic. Default Horizon/CLI groups allow outbound traffic. [Networking](https://docs.jetstream-cloud.org/ui/cli/network/), [security groups](https://docs.jetstream-cloud.org/ui/horizon/security_group/) | Existing network/subnet/router/SG IDs, exact complete ingress and egress rules, statefulness and private route from A. Restrict SSH ingress to A's approved source; explicitly bound bootstrap infrastructure access and egress. No broad rule or public-IP workaround |
| Quota and capacity | Quota is an allocation-wide concurrency limit, not a capacity reservation. [Quotas](https://docs.jetstream-cloud.org/general/quotas/) | Fresh instance/core/RAM/port headroom and complete inventories, plus dated capacity evidence appropriate to this region. Actual boot remains unproven until the separately approved attempt |

The [existing OpenStack field mapping](proposals/jetstream2-openstack-field-mapping.md)
and [pinned research](proposals/jetstream2-openstack-source-pins.json) remain useful
schema references; they do not identify deployed provider versions. The
experiment's 8 KiB envelope, 2 KiB profile, 4 KiB snapshot, eight-rule limits and
16 KiB native policy/state limits remain. If the selected evidence cannot fit or
inventory is paginated/over the current 1,000-record bound, engineering must
resolve the refusal before qualification, not weaken bounds during a run.

## Credential custody and independent cleanup

Jetstream2 recommends private, allocation-specific application credentials for
each user and machine. [Provider authentication guidance](https://docs.jetstream-cloud.org/ui/cli/auth/)
was checked October 17. A and B need separate credentials under named custodians,
distinct rescue signing keys and independently retained verification keys.
Use approved existing secret storage; keep secrets out of Git, reports, console
output, metadata, bootstrap and guests. Guest login keys are a separate purpose.

A needs only the reviewed preflight and one port/server creation operations.
B needs authentication, exact inventory/inspection and scoped server/port
deletion; it receives no admission, guest-login, settlement or authority-transfer
capability. The operator's console access is separately scoped. Ordinary project
roles may be broader than these application interfaces. OpenStack supports
delegating roles and optional access rules, but access rules require service
configuration; `unrestricted:false` alone does not mean cleanup-only access.
[Keystone application credentials](https://docs.openstack.org/keystone/latest/user/application_credentials.html)
was checked October 17. Jetstream2 enforcement of the needed restrictions is
unknown. Obtain provider/operator review and verify it under a separately
authorized qualification plan; do not probe create/delete permissions as part of
read-only preflight. If least privilege cannot be demonstrated, keep launch
blocked and return the actual privilege exception for review.

B must survive loss of A and of the worker: separate host/storage, independent
service supervision, usable management-network access, retained signed scopes,
durable ACKs and a named operator who can respond. A second process or a second
VM sharing the relevant failure domain is insufficient. No host has been
selected, inspected, purchased or provisioned. B is the sole automated cleanup
executor; A's reaper cannot compete for those resources. A keeps the sole budget
authority; copying its ledger to B does not create a failover permission.

The production clock and recovery policy remains engineering work. The offline
protocol uses a one-second ACK tolerance and expiry guard. Propose monitored UTC
error of at most 250 ms per host, including uncertainty, with fresh checks before
admission, ACK acceptance, resource POSTs and sweeps. This target needs validation
for the actual hosts; synchronization status alone proves no bound. Missing
health, clock jumps, stale evidence or restart uncertainty must stop creation.
Recovery must retain consumed attempts, immutable scopes and held accounting;
no restored/copy-on-host state may silently become another creator or reset B.
Existing fences do not claim protection from hostile administrator rollback or
identical-path copies on another host.

## Finite window and late effects

Retain the earlier proposal's **one worker, one total create attempt, concurrency
one, at most 600 seconds and 1,000 milli-SU reserved**. Boot, verification,
transfer and execution must fit the original worker deadline. Start, latest
admission and absolute expiry must be frozen together so a fixed request lifetime
fits: `latest_admission + request_lifetime <= absolute_expiry`. A late start
cannot silently shorten a bound request, extend expiry or create a new identity.

The original October 20, 10:00–10:10 Pacific run slot, 10:15 escalation and 11:00
decision end remain historical proposals, **not scheduled or approved**. This
supplement selects no date. The future packet must choose a feasible window after
prerequisites pass; an expired proposal is never automatically rolled forward.

Propose B starts cleanup after expiry plus the existing clock guard and runs at
most once every 15 seconds. Preserve the **64 persisted sweep limit per scope**,
including retained interrupted attempts. It is not 64 new sweeps after a restart.
Alert the rescue operator immediately on refusal, credential/host/API failure or
unexpected resources, and escalate unresolved cleanup no later than five minutes
after expiry. Capacity exhaustion requires handoff before further automated work;
never reset observation state to evade the limit.

An empty inventory or `OBSERVED_ABSENT` receipt is only an observation. Lost or
late provider effects can appear after the watch window, after credential expiry
or after sweep 64. Preserve the signed intent, exact IDs, uncertainty and native
liability. The operator must have a reviewed continuation/escalation procedure,
coverage and credential renewal/revocation plan that does not depend on A or
grant another create attempt. Renewing cleanup access is a separate controlled
action, never an automatic extension of creation authority. A human handoff or
support ticket alone does not prove eventual deletion or terminal absence.

**The late-effect gate remains open.** Before live approval, engineering and the
operator must establish a reviewed way to cover that continuation or supported
provider evidence of terminal outcome/cancellation. If neither is available,
defer. Acceptance of a 1 SU reservation does not close this gap. Corrupt state,
foreign/duplicate resources, unknown volume attachments or partial inventory
remain refusals requiring exact operator investigation, not broad deletion.
No rescue receipt grants settlement or a refund.

## Exposure and cost decisions

At the published 1 SU/hour rate, 600 seconds estimates **0.166667 SU**; the code's
minimum rounded reservation is **167 milli-SU**. The proposed **1,000 milli-SU
reservation is 1 SU in the local ledger, not a provider-enforced charge cap**.
Provider/API or supervisor failure can extend consumption beyond it; no verified
maximum overrun is available. ACCESS credits, Jetstream2 SUs, estimates, reserved
liabilities, observed usage, settled charges and dollars remain separate.

Jetstream2 documents a 1:1 ACCESS-credit/SU exchange, 12–24-hour reporting delay,
and continued charges while stopped (50%) or suspended (75%); shelved is 0%.
The experiment still requires deletion. Neither shutdown, shelving, an expired
window nor an absence observation settles a charge.
[Provider usage rules](https://docs.jetstream-cloud.org/general/access/), checked
October 17, 2026. No exchange, usage reconciliation or dollar conversion was made.

| Cost category | Decision state |
| --- | --- |
| Additional subscription | **None demonstrated as necessary.** Existing approved allocation and hosts may suffice; their suitability is unverified |
| Worker allocation consumption | Proposed reservation 1 SU; nominal ten-minute CPU estimate above. Balance/entitlement and possible overrun still need review |
| Existing A/B hosts, private route, logs and operator time | **Unknown incremental cost and capacity.** Not covered by the worker's reservation; ask the owners for existing authorization and a bounded estimate |
| New cleanup/access host, VPN/bastion, network changes or backup storage | Conditional, not authorized. If existing capacity fails, return exact supplier, resource, native units/dollars, duration, recurring charges and teardown plan for a new decision |
| Optional products | Kubernetes, managed secrets/monitoring, a paid VPN and a job queue are not dependencies of this recommended design; no purchase proposed |
| Extra volumes, snapshots, floating IPs, GPU/large-memory workers, image builds or further attempts | Excluded. Their costs are unpriced here; any addition or broader scope needs a new resource/cost decision |

No assertion that networking, storage or operational support is free follows
from the CPU rate. Unexpected chargeable resources retain liability and trigger
escalation. A finite local allowance cannot guarantee a finite provider bill.

## Engineering prerequisites before an executable live packet

These are deliverables for software engineering, not choices to delegate to Greg.
Each needs a spec, implementation tied to the actual effect path, appropriate
failure tests and independent review. This documentation PR implements none.

| Prerequisite | Required acceptance |
| --- | --- |
| Production experiment and live launcher contract | Reviewed disabled-by-default path binding selected configuration, native policy, exact compatibility evidence, request/reservation, start/expiry and one total attempt; preserve atomic consumption and final pre-POST checks. No fallback to the generic client that bypasses rescue. The existing experiment/rescue constructors are loopback-only |
| Authenticated evidence collection | Collect and strictly map selected provider metadata with provenance, completeness and freshness; bind it to admission and both POSTs. Static compatibility remains insufficient for boot, effective filtering or route reachability; stale evidence refuses, without refreshing the immutable envelope in place |
| Guest route and bootstrap | Qualify the pinned image, nonsecret bootstrap, host-key provenance, private SSH route and bounded one-shot receiver; verify wrong key/digest, replay, interrupted transfer/execution, output overflow and timeout. Guest result and accounting remain separate |
| Independent A–B transport and recovery | Authenticate and durably acknowledge each phase on the selected independent host before its POST; preserve B-only cleanup, credential custody, clock checks, restore fences, corrupt-state refusal and alerts during loss of A/B |
| Late effects and sweep exhaustion | Review and rehearse escalation beyond the finite watch/64-sweep boundary, including provider/API outage and expiring cleanup access; retain unknown liabilities. No automatic refill, terminal absence claim or refund |
| Operator-facing qualification and accounting | Prepare exact-method read-only inspection and separately authorized live qualification plans, redacted evidence retention, abort/runbook and provider-charge reconciliation. Ambiguous settlement stays disabled until separately specified and verified |

The baseline [experiment](JETSTREAM2-OFFLINE-EXPERIMENT.md),
[compatibility](JETSTREAM2-OFFLINE-COMPATIBILITY.md) and
[rescue](JETSTREAM2-OFFLINE-RESCUE.md) evidence remains synthetic/offline.
PR #13's [CI run](https://github.com/EduCloud-Ecosystem/outfitter/actions/runs/39102893358)
passed 736 tests/subtests and Docker/recovery/consumer acceptance; none is guest
or live-provider qualification. Deployment remains disabled and guest execution
remains `NOT_RUN`.

## Separate authorization steps

1. **Architecture selection:** choose the route, allocation candidate and proposed
   hosts/operators above. This does not approve credential retrieval/creation,
   provider login, account changes, resource inspection, provisioning or a run.
2. **Credential and read-only evidence authorization:** after a concrete scope is
   prepared, approve access to the named allocation and specified credential
   handling. Authentication and selected GETs may then establish resource pins,
   balance/entitlement records and quota; live preflight still cannot prove
   capacity, guest boot or cleanup reliability. No credentials were accessed and
   no authenticated provider calls were made for this supplement.
3. **Operational qualification approval:** separately approve any use/configuration
   of A/B hosts, private-network checks, credential permission tests or rehearsal
   that exceeds read-only inspection. Creating an application credential or
   changing a security group is a mutation, even without a worker. Any resources
   or costs need their own bounded authorization; no implicit preparatory test.
4. **Finite live experiment approval:** after the software prerequisites pass and
   operational qualification is complete where possible without the worker,
   present a newly frozen manifest, exact revisions/digests, operators, absolute
   window, liability/escalation plan and remaining checks to perform during the
   one attempt. Greg explicitly approves provisioning and that live test, or
   defers. Selecting a design, merging this PR or reaching a date cannot trigger it.

Until those decisions and gates are satisfied, this lane can wait while Waypoint
and Portage continue. The accepted local Docker baseline remains available.
