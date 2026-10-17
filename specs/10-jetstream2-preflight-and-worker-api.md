# 10 — Jetstream2 read-only preflight and worker API seam

This follows Ben's October 13 assignment after the local Docker baseline. The
first executable command is `outfitter jetstream2-preflight --config FILE` with no
state-directory argument. It authenticates using allocation-specific application
credentials from `OS_APPLICATION_CREDENTIAL_ID` and
`OS_APPLICATION_CREDENTIAL_SECRET`; it never prints or persists them. Configuration
pins auth URL, project UUID, region, image UUID and checksum, flavor, network and
security-group IDs. HTTPS certificate verification and no-redirect HTTP clients
are mandatory. Authentication obtains a token; subsequent preflight calls only
GET resources and quota detail. It never creates a worker or network resource. Failed resource checks still
emit the structured report and exit nonzero. Resource mutation contexts are
bounded by the persisted reservation deadline, rechecked immediately before
every creation POST; known port IDs and liabilities survive refusal of a later
server creation.

Preflight verifies the token project, exact catalog region/public endpoints,
active image and expected content checksum, selected flavor, active network,
security group and free instance/core/RAM quota. It reports unknown allocation
balance, native SU usage, provider expiry and host capacity explicitly. Quota
availability is not a resource entitlement, allocation balance or capacity promise.
No broad GPU multiplier or dollar conversion is inferred.

The worker API package supplies gated create, inspect and owned teardown methods
for adapter integration tests. Every create requires a `Journal` implementation
that has committed the opaque request's finite native-unit reservation and
independent-supervisor approval BEFORE any provider POST. The default client has
no such journal and refuses creation. There is no live provisioning CLI. The
in-memory journal in the original seam tests is mock evidence only.
[Spec 12](12-native-unit-journal-and-cleanup.md) adds the shared durable journal and
cleanup-only supervisor; independent-host qualification and an allocation-backed
live run remain required.

The API creates an explicitly owned Neutron port, persists its ID, then creates
one image-backed Nova server using that port and exact image/flavor references.
It requests no floating IPs, volumes, snapshots or new networks. Metadata and a
port description record owner/request IDs. A lost POST response or failed durable
resource recording marks the request unknown; identical retries cannot recreate.
Teardown verifies recorded IDs and ownership before deletion and independently
observes server absence before deleting the owned port. Unknown or foreign
resources are never swept. Provider errors retain liabilities. Even verified deletion leaves native-unit
accounting `reserved_unsettled` with liability retained until the authoritative
native-unit journal settles it; deletion is never an assumed zero charge. No guest shutdown
is treated as deletion or self-expiry.

Configuration example (IDs and digest are operator-supplied):

```json
{"auth_url":"https://AUTH_HOST/v3","project_id":"PROJECT_UUID",
 "region":"REGION","image_id":"IMAGE_UUID","image_hash":"sha512:IMAGE_HASH",
 "flavor_id":"FLAVOR_ID","network_id":"NETWORK_UUID",
 "security_group_id":"SECURITY_GROUP_UUID"}
```

Sources checked October 17, 2026: [Jetstream2 application credentials](https://docs.jetstream-cloud.org/ui/cli/auth/),
[OpenStack Identity v3](https://docs.openstack.org/api-ref/identity/v3/),
[Compute API](https://docs.openstack.org/api-ref/compute/),
[Networking API](https://docs.openstack.org/api-ref/network/v2/),
[Image API v2](https://docs.openstack.org/api-ref/image/v2/).
No live allocation credentials or resources were used to validate this slice.
