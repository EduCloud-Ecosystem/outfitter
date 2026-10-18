# Draft OpenStack field mapping for offline preflight

This is a research companion to the [preflight policy proposal](jetstream2-preflight-policy.md)
at `19efdcad86678d7199995e30e1f4133360fddf65`. It supplies primary-source mappings
for design review; it does not approve implementation or alter the current policy,
schema, resource limits or cleanup liabilities. Public documentation and upstream
source were checked on October 17, 2026, Pacific time. These upstream definitions
are not evidence of the OpenStack versions or extensions deployed by Jetstream2.

The evaluator can compare declarations with a reviewed profile. A matching image
architecture, firmware setting or minimum size does not demonstrate a successful
boot, usable private route, working guest recipe or compatible available host.
Keep those outcomes unknown until separately qualified.

## Image and flavor fields

The image detail response is the object returned by `GET /v2/images/{image_id}`;
custom properties appear at its top level. Base properties may be null, while
additional string properties are emitted only when valued. Image property access
can also be restricted. [Image API](https://docs.openstack.org/api-ref/image/v2/#show-image)

| JSON field | Primary definition | Proposed comparison |
| --- | --- | --- |
| `/min_ram` | Integer, documented in MB; API reference allows null | Require a present nonnegative integer for a size check; compare against flavor RAM |
| `/min_disk` | Integer, documented in GB; API reference allows null | Require a present nonnegative integer; compare against flavor root disk |
| `/architecture` | Optional string declaration of architecture required from the hypervisor, such as `x86_64` | Exact approved declaration; absence stays unknown |
| `/hw_firmware_type` | Optional firmware declaration, `bios` or `uefi`; libvirt-specific | Exact approved declaration; do not infer a default |
| `/disk_format`, `/container_format` | Nullable strings from the selected cloud's image schema | Explicit profile values, without inferring boot compatibility |

Sources: [Image API](https://docs.openstack.org/api-ref/image/v2/#show-image),
[image property definitions](https://docs.openstack.org/glance/latest/admin/useful-image-properties.html#image-property-keys-and-values).
`architecture` and `hw_firmware_type` are metadata declarations consumed by other
services. Their presence establishes neither the guest's measured architecture
nor successful firmware/driver operation. For the initial fixture vocabulary,
propose `x86_64` literally; no automatic `amd64` or image-name inference.

Nova's `GET /flavors/{flavor_id}` response has `/flavor/ram` in MiB,
`/flavor/disk` for the root disk in GiB, and the separate
`/flavor/OS-FLV-EXT-DATA:ephemeral` for additional ephemeral disk in GiB.
`/flavor/vcpus` is an integer. A zero root-disk flavor has special sizing behavior;
it does not mean a zero-byte root disk.
[Compute API](https://docs.openstack.org/api-ref/compute/#show-flavor-details)

The terminology differs between the Glance API prose and Nova's flavor prose.
The pinned [Nova comparison](https://github.com/openstack/nova/blob/0ec0756a55f374ec585ea80485ff094718f53c78/nova/compute/api.py#L814)
compares RAM quantities directly and converts image `min_disk` and flavor
`root_gb` with the same GiB multiplier. It additionally compares image byte size
against a nonzero local root disk. Thus a minima-only match cannot claim complete
size compatibility. The evaluator should retain the original field/unit labels,
use the documented Nova comparison convention explicitly, and require a positive
root disk for its initial image-backed fixture. That restriction is an Outfitter
proposal, not a new provider rule.

The [Glance schema implementation](https://github.com/openstack/glance/blob/76306ffdb15a76456bc6106a659250e1ae69f4e1/glance/api/v2/images.py#L1995)
types the two minimum fields as integers, while the API response reference also
describes nullable values. Preserve this difference in fixtures. Missing/null
minima must not be converted to zero by the offline assessment. An explicit zero
is a declaration of no positive minimum in that field; it is not measured guest
resource sufficiency.

The existing Outfitter `Preflight.DiskGB` receives Nova `flavor.disk` but serializes
as `ephemeral_disk_gb`. Record that naming issue for coordinated follow-up; this
research does not rename the established field or migrate any consumer.

## Security group fields

`GET /v2.0/security-groups/{security_group_id}` returns
`/security_group/security_group_rules`, an array containing the associated rules.
The API can filter response fields, and policy controls visibility. Rules may also
be read individually through `/v2.0/security-group-rules/{id}`.
[Networking API](https://docs.openstack.org/api-ref/network/v2/#show-security-group)

Paths below are relative to each rule object:

| Field | API value |
| --- | --- |
| `id`, `security_group_id`, `project_id` | Identity strings; `tenant_id` is the project alias |
| `direction` | `ingress` or `egress` |
| `ethertype` | `IPv4` or `IPv6` |
| `protocol` | Named string, protocol number, or null |
| `port_range_min`, `port_range_max` | Integer or null; ICMP uses type/code rather than ports |
| `remote_ip_prefix` | CIDR or null |
| `remote_group_id` | Security-group UUID or null |
| `remote_address_group_id` | Address-group UUID or null, extension-dependent |

[Rule API](https://docs.openstack.org/api-ref/network/v2/#show-security-group-rule)

The pinned [Neutron definitions](https://github.com/openstack/neutron/blob/8e0b546521abde0d96e68169a60c043a5053e684/neutron/extensions/securitygroup.py#L156)
normalize `null` and `any` to null. The [pinned any-protocol constant](https://github.com/openstack/neutron/blob/8e0b546521abde0d96e68169a60c043a5053e684/neutron/common/_constants.py#L41-L46)
excludes protocol `0`, which remains the numeric string `0` in the converter.
This conflicts with the API prose describing `0` as all protocols. The offline
evaluator conservatively treats `0` as unsupported, never as `any`; these pins
do not establish the version or behavior deployed on Jetstream2.
The schema defaults protocol, port bounds
and remote selectors to null. The [rule validator](https://github.com/openstack/neutron/blob/8e0b546521abde0d96e68169a60c043a5053e684/neutron/db/securitygroups_db.py#L770)
permits both port bounds null; full port ranges can be normalized to this form.
For transport ports, nonnull bounds must be ordered and nonzero; ICMP type/code
instead permit 0 through 255, with code requiring a type. Other protocols cannot
carry port bounds.

Neutron [rejects multiple nonempty remote selectors](https://github.com/openstack/neutron/blob/8e0b546521abde0d96e68169a60c043a5053e684/neutron/db/securitygroups_db.py#L522).
Its [comparison logic](https://github.com/openstack/neutron/blob/8e0b546521abde0d96e68169a60c043a5053e684/neutron/db/securitygroups_db.py#L965)
treats an unrestricted CIDR and null prefix alike for duplicate detection. A null
prefix with a nonnull remote group must retain that group's restriction. The
[address-group extension](https://github.com/openstack/neutron-lib/blob/6893d1d8c22bac71813422563beb9374e3b902e0/neutron_lib/api/definitions/security_groups_remote_address_group.py#L19)
adds `remote_address_group_id`; absence cannot establish that the extension is
unsupported or that no selector exists.

## Proposed design refinements

These are Outfitter decisions for review, not claims about all OpenStack clouds:

- Correct blanket null rejection in the original proposal. Keep three distinct
  states: missing, explicit null, and typed value. Interpret a null only where the
  pinned schema gives it meaning. An unrestricted rule requires an explicit
  reviewed match; null never means an empty or harmless rule.
- Separate a strict, bounded Outfitter snapshot envelope from raw provider JSON.
  Glance intentionally allows additional metadata; closing every raw object to
  unknown fields would reject legitimate responses. A later capture adapter must
  retain completeness and extension evidence rather than silently discard fields
  affecting interpretation.
- For the smallest slice, assess an explicit synthetic subset of protocols and
  selectors. Mark unsupported variants unknown. Define a finite canonicalization
  table before tests, preserving protocol family, port versus ICMP meaning, and
  each selector's kind. Never silently merge IPv4 and IPv6 rules.
- Bind group identity and the complete semantic rule set, plus observed rule IDs.
  Reject duplicate IDs, duplicate semantic rules, extra/missing rules and conflicting
  project aliases. Nonnull group/address-group selectors need separately bound
  membership evidence or remain outside the initial supported subset.
- Capture the group-level `stateful` declaration and extension availability when
  required by the profile. Identical rule tuples alone cannot establish equivalent
  effective filtering. Private routing, port security, backend enforcement and
  connection behavior remain outside a rule-declaration match.
- Report image declarations, minimum-size comparisons and rule-declaration matches
  as separate checks. Retain `boot_verified: false`,
  `provisioning_enabled: false` and `live_readiness: BLOCKED` in every fixture result.

Suggested cases for the forthcoming spec include absent architecture, null and
zero image minima, an image larger than its declared minimum, zero flavor root
disk, root/ephemeral confusion, null protocol with restrictive remote group,
ICMP type zero, one missing port bound, extra IPv6 egress, extension-field absence,
and a matching declaration set with boot readiness still unknown.

The [source pin record](jetstream2-openstack-source-pins.json) records the upstream
revisions and SHA-256 digests inspected for these distinctions. No provider endpoint,
allocation credential or live resource was accessed, and no implementation changed.
